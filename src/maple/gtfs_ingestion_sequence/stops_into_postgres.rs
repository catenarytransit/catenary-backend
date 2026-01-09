// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use catenary::enum_to_int::*;
use catenary::name_shortening_hash_insert_elastic;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::stops::dsl::stops as stops_table;
use crossbeam;
use diesel_async::RunQueryDsl;
use elasticsearch::http::request::JsonBody;
use gtfs_structures::LocationType;
use gtfs_translations::TranslatableField;
use gtfs_translations::TranslationKey;
use gtfs_translations::TranslationLookup;
use gtfs_translations::TranslationResult;
use gtfs_translations::translation_csv_text_to_translations;
use itertools::Itertools;
use language_tags::LanguageTag;
use regex::Regex;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use titlecase::titlecase;

pub async fn stops_into_postgres_and_elastic(
    gtfs: &gtfs_structures::Gtfs,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    stop_ids_to_route_types: &HashMap<String, HashSet<i16>>,
    stop_ids_to_route_ids: &HashMap<String, HashSet<String>>,
    stop_id_to_children_ids: &HashMap<String, HashSet<String>>,
    stop_id_to_children_route: &HashMap<String, HashSet<i16>>,
    gtfs_translations: Option<&TranslationResult>,
    default_lang: &Option<String>,
    elasticclient: Option<&elasticsearch::Elasticsearch>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let mut stops_finished_chunks_array = Vec::new();

    let mut stops_finished_chunks_arrays_array_elasticsearch: Vec<Vec<JsonBody<_>>> = Vec::new();
    let platform_regex = Regex::new(r",? Platform \d+").unwrap();

    let avaliable_langs_to_check = gtfs_translations.map(|translations_export| {
        translations_export
            .possible_translations
            .iter()
            .map(|x| x.1.clone())
            .collect::<HashSet<LanguageTag>>()
    });

    for chunk in &gtfs.stops.iter().chunks(256) {
        let mut insertable_stops = Vec::new();
        let mut insertable_elastic: Vec<JsonBody<_>> = Vec::new();

        for (stop_id, stop) in chunk {
            let name: Option<String> = titlecase_process_new(stop.name.as_ref()).map(|x| {
                let cleaned = x.replace("Fermata ", "");
                platform_regex.replace_all(&cleaned, "").to_string()
            });
            let display_name: Option<String> = name.as_ref().map(|name| {
                name.clone()
                    .replace("Station ", "")
                    .replace(" Station", "")
                    .replace(", Bahnhof", "")
                    .replace(" Bahnhof", "")
                    .replace("Estación de tren ", "")
                    .replace(" Metrolink", "")
                    .replace("Northbound", "N.B.")
                    .replace("Eastbound", "E.B.")
                    .replace("Southbound", "S.B.")
                    .replace("Westbound", "W.B.")
                    .replace(" (Railway) ", "")
                    .replace(" Light Rail", "")
            });

            let point = match stop.latitude.is_some() && stop.longitude.is_some() {
                true => Some(postgis_diesel::types::Point::new(
                    stop.longitude.unwrap(),
                    stop.latitude.unwrap(),
                    Some(4326),
                )),
                false => match stop.parent_station.is_some() {
                    true => {
                        let parent_station = gtfs.stops.get(stop.parent_station.as_ref().unwrap());
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
            };

            if point.is_none() {
                continue;
            }

            if !(point.as_ref().unwrap().x >= -180.0
                && point.as_ref().unwrap().x <= 180.0
                && point.as_ref().unwrap().y >= -90.0
                && point.as_ref().unwrap().y <= 90.0)
            {
                println!(
                    "Deleted feed id {} stop id {} for being out of bounds",
                    &feed_id, &stop_id
                );
                continue;
            }

            let timezone = match feed_id {
                "f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle" => {
                    tz_search::lookup(point.as_ref().unwrap().y, point.as_ref().unwrap().x)
                }
                _ => match &stop.timezone {
                    Some(tz) => stop.timezone.clone(),
                    None => match -90.0 <= point.as_ref().unwrap().y
                        && point.as_ref().unwrap().y <= 90.0
                        && -180.0 <= point.as_ref().unwrap().x
                        && point.as_ref().unwrap().x <= 180.0
                    {
                        true => {
                            tz_search::lookup(point.as_ref().unwrap().y, point.as_ref().unwrap().x)
                        }
                        false => Some(String::from("Etc/GMT")),
                    },
                },
            };

            let mut name_translations: HashMap<String, String> = HashMap::new();
            let mut name_translations_shortened_locales_elastic: HashMap<String, String> =
                HashMap::new();

            if let Some(avaliable_langs_to_check) = &avaliable_langs_to_check {
                for lang in avaliable_langs_to_check {
                    if let Some(gtfs_translations) = &gtfs_translations {
                        let translated_result_by_record_id =
                            gtfs_translations.translations.get(&TranslationLookup {
                                language: lang.clone(),
                                field: TranslatableField::Stops(
                                    gtfs_translations::StopFields::Name,
                                ),
                                key: TranslationKey::Record(stop_id.clone()),
                            });

                        if let Some(translated_result) = translated_result_by_record_id {
                            name_translations.insert(lang.to_string(), translated_result.clone());

                            name_shortening_hash_insert_elastic(
                                &mut name_translations_shortened_locales_elastic,
                                &lang,
                                translated_result.as_str(),
                            );
                        }

                        if let Some(gtfs_name) = &stop.name {
                            let translated_result_by_value_lookup =
                                gtfs_translations.translations.get(&TranslationLookup {
                                    language: lang.clone(),
                                    field: TranslatableField::Stops(
                                        gtfs_translations::StopFields::Name,
                                    ),
                                    key: TranslationKey::Value(gtfs_name.clone()),
                                });

                            if let Some(translated_result) = translated_result_by_value_lookup {
                                name_translations
                                    .insert(lang.to_string(), translated_result.clone());

                                name_shortening_hash_insert_elastic(
                                    &mut name_translations_shortened_locales_elastic,
                                    &lang,
                                    translated_result.as_str(),
                                );
                            }
                        }
                    }
                }
            }

            if let Some(default_lang) = &default_lang {
                if let Some(name) = &name {
                    name_translations.insert(default_lang.clone(), name.clone());

                    if let Ok(lang_tag) = LanguageTag::parse(default_lang.as_str()) {
                        name_shortening_hash_insert_elastic(
                            &mut name_translations_shortened_locales_elastic,
                            &lang_tag,
                            name.as_str(),
                        );
                    }
                }
            } else {
                if let Some(name) = &name {
                    name_translations_shortened_locales_elastic
                        .insert("en".to_string(), name.clone());
                }
            }

            let jsonified_translations = serde_json::to_value(&name_translations).unwrap();
            let jsonified_translations_for_elastic =
                serde_json::to_value(&name_translations_shortened_locales_elastic).unwrap();

            let mut route_names_for_elastic: Vec<String> = vec![];

            match stop_ids_to_route_ids.get(&stop.id) {
                Some(route_ids) => {
                    for route_id in route_ids {
                        if let Some(route) = gtfs.routes.get(route_id) {
                            if let Some(long_name) = &route.long_name {
                                route_names_for_elastic.push(long_name.clone());
                            }
                            if let Some(short_name) = &route.short_name {
                                route_names_for_elastic.push(short_name.clone());
                            }
                        }
                    }
                }
                None => {}
            }

            let stop_pg = catenary::models::Stop {
                onestop_feed_id: feed_id.to_string(),
                chateau: chateau_id.to_string(),
                attempt_id: attempt_id.to_string(),
                gtfs_id: stop_id.clone(),
                name,
                name_translations: match name_translations.len() {
                    0 => None,
                    _ => Some(jsonified_translations),
                },
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
                point: point,
                timezone: timezone,
                level_id: stop.level_id.clone(),
                station_feature: false,
                wheelchair_boarding: availability_to_int(&stop.wheelchair_boarding),
                primary_route_type: match stop_ids_to_route_types.get(&stop.id) {
                    Some(route_types) => {
                        let route_types = route_types.iter().copied().collect::<Vec<i16>>();

                        match route_types.contains(&2) {
                            true => Some(2),
                            false => match route_types.contains(&1) {
                                true => Some(1),
                                false => match route_types.contains(&0) {
                                    true => Some(0),
                                    false => Some(route_types[0]),
                                },
                            },
                        }
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
                osm_station_id: None,
                osm_platform_id: None,
            };

            insertable_stops.push(stop_pg);

            let elastic_id = format!("{}_{}_{}", feed_id, attempt_id, stop_id);

            if stop.location_type != LocationType::StationEntrance
                && stop.location_type != LocationType::GenericNode
            {
                insertable_elastic
                    .push(json!({"index": {"_index": "stops", "_id": elastic_id}}).into());

                let mut agency_names: Vec<String> = vec![];

                match gtfs.agencies.len() {
                    0 => {}
                    1 => {
                        agency_names.push(gtfs.agencies[0].name.clone());
                    }
                    _ => {
                        let mut agency_ids_for_this_stop: HashSet<String> = HashSet::new();

                        match stop_ids_to_route_ids.get(&stop.id) {
                            Some(route_ids) => {
                                for route_id in route_ids {
                                    if let Some(route) = gtfs.routes.get(route_id) {
                                        if let Some(agency_id) = &route.agency_id {
                                            agency_ids_for_this_stop.insert(agency_id.to_string());
                                        }
                                    }
                                }
                            }
                            None => {}
                        }

                        for agency_id in agency_ids_for_this_stop {
                            if let Some(agency) = gtfs
                                .agencies
                                .iter()
                                .find(|x| x.id.as_ref() == Some(&agency_id))
                            {
                                agency_names.push(agency.name.clone());
                            }
                        }
                    }
                }

                insertable_elastic.push(
                    json!({
                        "stop_id": stop_id.clone(),
                        "chateau": chateau_id.to_string(),
                        "attempt_id": attempt_id.to_string(),
                        "onestop_feed_id": feed_id.to_string(),
                        "stop_name": jsonified_translations_for_elastic,
                        "point": {
                            "lat": point.as_ref().unwrap().y,
                            "lon": point.as_ref().unwrap().x,
                        },
                        "route_types": stop_ids_to_route_types.get(&stop.id),
                        "route_name_search": route_names_for_elastic,
                        "agency_name_search": agency_names,
                    })
                    .into(),
                );
            }
        }

        stops_finished_chunks_array.push(insertable_stops);
        stops_finished_chunks_arrays_array_elasticsearch.push(insertable_elastic);
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

    if let Some(elasticclient) = elasticclient {
        for chunk in stops_finished_chunks_arrays_array_elasticsearch {
            let response = elasticclient
                .bulk(elasticsearch::BulkParts::Index("stops"))
                .body(chunk)
                .send()
                .await?;

            let response_body = response.json::<serde_json::Value>().await?;

            let mut print_err = true;

            if response_body.get("errors").map(|x| x.as_bool()).flatten() == Some(false) {
                print_err = false;
            }

            if print_err {
                println!("elastic stop response: {:#?}", response_body);
            }
        }
    }

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
