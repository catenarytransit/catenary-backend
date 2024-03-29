// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license
use catenary::schema::gtfs::calendar::onestop_feed_id;
use diesel_async::RunQueryDsl;
use gtfs_structures::{BikesAllowedType, ExactTimes};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::DownloadedFeedsInformation;
use catenary::postgres_tools::CatenaryPostgresPool;

// take a feed id and throw it into postgres
pub async fn gtfs_process_feed(
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    this_download_data: &DownloadedFeedsInformation,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    //CREATE entry in gtfs.ingested_static
    use catenary::schema::gtfs::ingested_static::dsl::ingested_static;

    let ingestion_static_data = catenary::models::IngestedStatic {
        onestop_feed_id: feed_id.to_string(),
        attempt_id: attempt_id.to_string(),
        file_hash: this_download_data.hash.unwrap().clone().to_string(),
        ingest_start_unix_time_ms: chrono::Utc::now().timestamp_millis(),
        ingestion_version: crate::gtfs_handlers::MAPLE_INGESTION_VERSION,
        ingesting_in_progress: true,
        ingestion_successfully_finished: false,
        ingestion_errored: false,
        production: false,
        deleted: false,
        feed_expiration_date: None,
        feed_start_date: None,
        languages_avaliable: Vec::new(),
    };

    diesel::insert_into(ingested_static)
        .values(&ingestion_static_data)
        .execute(conn)
        .await?;

    //read the GTFS zip file
    let path = format!("gtfs_uncompressed/{}", feed_id);

    let gtfs = gtfs_structures::Gtfs::new(path.as_str())?;

    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        make_hashmap_stops_to_route_types_and_ids(&gtfs);

    let (stop_id_to_children_ids, stop_ids_to_children_route_types) =
        make_hashmaps_of_children_stop_info(&gtfs, &stop_ids_to_route_types);

    //identify colours of shapes based on trip id's route id
    let (shape_to_color_lookup, shape_to_text_color_lookup) = shape_to_colour(&feed_id, &gtfs);

    //insert agencies
    for agency in &gtfs.agencies {
        use catenary::schema::gtfs::agencies::dsl::agencies;

        let agency_row = catenary::models::Agency {
            static_onestop_id: feed_id.to_string(),
            agency_id: agency.id.clone().unwrap_or_else(|| "".to_string()),
            attempt_id: attempt_id.to_string(),
            agency_name: agency.name.clone(),
            agency_name_translations: None,
            agency_url_translations: None,
            agency_url: agency.url.clone(),
            agency_fare_url: agency.fare_url.clone(),
            agency_fare_url_translations: None,
            chateau: chateau_id.to_string(),
            agency_lang: agency.lang.clone(),
            agency_phone: agency.phone.clone(),
            agency_timezone: agency.timezone.clone(),
        };

        diesel::insert_into(agencies)
            .values(agency_row)
            .execute(conn)
            .await?;
    }

    //shove raw geometry into postgresql
    shapes_into_postgres(
        &gtfs,
        &shape_to_color_lookup,
        &shape_to_text_color_lookup,
        &feed_id,
        Arc::clone(&arc_conn_pool),
        &chateau_id,
        &attempt_id,
    )
    .await?;

    //insert routes

    //insert trip
    for (trip_id, trip) in &gtfs.trips {
        let mut stop_headsigns: HashSet<String> = HashSet::new();

        for stop_time in &trip.stop_times {
            if let Some(stop_headsign) = stop_time.stop_headsign.as_ref() {
                stop_headsigns.insert(stop_headsign.clone());
            }
        }

        let stop_headsigns = stop_headsigns
            .into_iter()
            .map(|stop_headsign| Some(stop_headsign))
            .collect::<Vec<Option<String>>>();

        let has_stop_headsigns = stop_headsigns.len() > 0;

        use gtfs_structures::DirectionType;

        let frequencies_vec = match trip.frequencies.len() {
            0 => None,
            _ => Some(
                trip.frequencies
                    .iter()
                    .map(|freq| {
                        Some(catenary::models::TripFrequencyModel {
                            start_time: freq.start_time as i32,
                            end_time: freq.end_time as i32,
                            headway_secs: freq.headway_secs as i32,
                            exact_times: match freq.exact_times {
                                Some(exact_times_num) => match exact_times_num {
                                    ExactTimes::FrequencyBased => false,
                                    ExactTimes::ScheduleBased => true,
                                },
                                None => false,
                            },
                        })
                    })
                    .collect(),
            ),
        };

        let trip_pg = catenary::models::Trip {
            onestop_feed_id: feed_id.to_string(),
            trip_id: trip_id.clone(),
            attempt_id: attempt_id.to_string(),
            service_id: trip.service_id.clone(),
            trip_headsign: trip.trip_headsign.clone(),
            trip_headsign_translations: None,
            route_id: trip.route_id.clone(),
            has_stop_headsigns: has_stop_headsigns,
            stop_headsigns: match stop_headsigns.len() {
                0 => None,
                _ => Some(stop_headsigns.clone()),
            },
            trip_short_name: trip.trip_short_name.clone(),
            direction_id: match trip.direction_id {
                Some(direction) => Some(match direction {
                    DirectionType::Outbound => 0,
                    DirectionType::Inbound => 1,
                }),
                None => None,
            },
            bikes_allowed: match trip.bikes_allowed {
                BikesAllowedType::NoBikeInfo => 0,
                BikesAllowedType::AtLeastOneBike => 1,
                BikesAllowedType::NoBikesAllowed => 2,
                BikesAllowedType::Unknown(unknown) => unknown,
            },
            block_id: trip.block_id.clone(),
            shape_id: trip.shape_id.clone(),
            wheelchair_accessible: match trip.wheelchair_accessible {
                gtfs_structures::Availability::Available => Some(1),
                gtfs_structures::Availability::NotAvailable => Some(2),
                gtfs_structures::Availability::Unknown(unknown) => Some(unknown),
                gtfs_structures::Availability::InformationNotAvailable => Some(0),
            },
            chateau: chateau_id.to_string(),
            frequencies: frequencies_vec,
        };

        use catenary::schema::gtfs::trips::dsl::trips;

        diesel::insert_into(trips)
            .values(trip_pg)
            .execute(conn)
            .await?;

        //inside insert stoptimes
    }

    //insert stops

    //calculate hull
    //submit hull

    // insert feed info

    Ok(())
}
