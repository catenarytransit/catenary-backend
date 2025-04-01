// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use catenary::enum_to_int::*;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::stops::dsl::stops as stops_table;
use crossbeam;
use diesel_async::RunQueryDsl;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use titlecase::titlecase;

pub async fn stops_into_postgres(
    gtfs: &gtfs_structures::Gtfs,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    stop_ids_to_route_types: &HashMap<String, HashSet<i16>>,
    stop_ids_to_route_ids: &HashMap<String, HashSet<String>>,
    stop_id_to_children_ids: &HashMap<String, HashSet<String>>,
    stop_id_to_children_route: &HashMap<String, HashSet<i16>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let mut stops_finished_chunks_array = Vec::new();

    for chunk in &gtfs.stops.iter().chunks(128) {
        let mut insertable_stops = Vec::new();

        for (stop_id, stop) in chunk {
            let name: Option<String> = titlecase_process_new(stop.name.as_ref()).map(|x| {
                x.replace("Fermata ", "")
                    .replace("LAX / Metro", "Kyler Chin LAX")
                    .replace("LAX/Metro", "Kyler Chin LAX")
            });
            let display_name: Option<String> = name.as_ref().map(|name| {
                name.clone()
                    .replace("Station ", "")
                    .replace(" Station", "")
                    .replace(", Bahnhof", "")
                    .replace(" Banhhof", "")
                    .replace("Estación de tren ", "")
                    .replace(" Metrolink", "")
                    .replace("Northbound", "N.B.")
                    .replace("Eastbound", "E.B.")
                    .replace("Southbound", "S.B.")
                    .replace("Westbound", "W.B.")
                    .replace(" (Railway) ", "")
                    .replace(" Light Rail", "")
            });

            let stop_pg = catenary::models::Stop {
                onestop_feed_id: feed_id.to_string(),
                chateau: chateau_id.to_string(),
                attempt_id: attempt_id.to_string(),
                gtfs_id: stop_id.clone(),
                name,
                name_translations: None,
                displayname: display_name,
                code: match feed_id {
                    "f-amtrak~sanjoaquin" => Some(stop_id.clone()),
                    _ => stop.code.clone(),
                },
                gtfs_desc: stop.description.clone(),
                gtfs_desc_translations: None,
                location_type: location_type_conversion(&stop.location_type),
                children_ids: match stop_id_to_children_ids.get(&stop.id) {
                    Some(children_ids) => children_ids
                        .iter()
                        .map(|x| Some(x.clone()))
                        .collect::<Vec<Option<String>>>(),
                    None => vec![],
                },
                location_alias: None,
                hidden: false,
                parent_station: stop.parent_station.clone(),
                zone_id: stop.zone_id.clone(),
                url: stop.url.clone(),
                point: match stop.latitude.is_some() && stop.longitude.is_some() {
                    true => Some(postgis_diesel::types::Point::new(
                        stop.longitude.unwrap(),
                        stop.latitude.unwrap(),
                        Some(4326),
                    )),
                    false => match stop.parent_station.is_some() {
                        true => {
                            let parent_station =
                                gtfs.stops.get(stop.parent_station.as_ref().unwrap());
                            match parent_station {
                                Some(parent_station) => match parent_station.latitude.is_some()
                                    && parent_station.longitude.is_some()
                                {
                                    true => Some(postgis_diesel::types::Point::new(
                                        parent_station.longitude.unwrap(),
                                        parent_station.latitude.unwrap(),
                                        Some(4326),
                                    )),
                                    false => None,
                                },
                                None => None,
                            }
                        }
                        false => None,
                    },
                },
                timezone: stop.timezone.clone(),
                level_id: stop.level_id.clone(),
                station_feature: false,
                wheelchair_boarding: availability_to_int(&stop.wheelchair_boarding),
                primary_route_type: match stop_ids_to_route_types.get(&stop.id) {
                    Some(route_types) => {
                        let route_types = route_types.iter().copied().collect::<Vec<i16>>();
                        Some(route_types[0])
                    }
                    None => None,
                },
                platform_code: stop.platform_code.clone(),
                routes: match stop_ids_to_route_ids.get(&stop.id) {
                    Some(route_ids) => route_ids
                        .iter()
                        .map(|x| Some(x.clone()))
                        .collect::<Vec<Option<String>>>(),
                    None => vec![],
                },
                children_route_types: match stop_id_to_children_route.get(&stop.id) {
                    Some(route_types) => route_types
                        .iter()
                        .map(|x| Some(*x))
                        .collect::<Vec<Option<i16>>>(),
                    None => vec![],
                },
                tts_name: stop.tts_name.clone(),
                tts_name_translations: None,
                platform_code_translations: None,
                route_types: match stop_ids_to_route_types.get(&stop.id) {
                    Some(route_types) => route_types
                        .iter()
                        .map(|x| Some(*x))
                        .collect::<Vec<Option<i16>>>(),
                    None => vec![],
                },
                allowed_spatial_query: false,
            };

            insertable_stops.push(stop_pg);
        }

        stops_finished_chunks_array.push(insertable_stops);
    }

    conn.build_transaction()
        .run::<(), diesel::result::Error, _>(|conn| {
            Box::pin(async move {
                for insertable_stops in stops_finished_chunks_array {
                    diesel::insert_into(stops_table)
                        .values(insertable_stops)
                        .execute(conn)
                        .await?;
                }

                Ok(())
            })
        })
        .await?;

    Ok(())
}

pub fn titlecase_process_new_nooption(input: &String) -> String {
    let mut string = input.to_owned();
    if string.len() >= 7
        && string
            .as_str()
            .chars()
            .all(|s| s.is_ascii_punctuation() || s.is_ascii())
    {
        //i don't want to accidently screw up Greek, Cryllic, Chinese, Japanese, or other writing systems
        string = titlecase(string.as_str());
    }
    string
}

pub fn titlecase_process_new(input: Option<&String>) -> Option<String> {
    input.map(titlecase_process_new_nooption)
}
