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
use gtfs_structures::ContinuousPickupDropOff;

pub struct GtfsSummary {
    pub feed_start_date: Option<String>,
    pub feed_end_date: Option<String>,
    pub languages_avaliable: Vec<String>,
    pub default_lang: Option<String>,
    pub general_timezone: String,
}

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

        let frequencies_vec: Option<Vec<Option<catenary::models::TripFrequencyModel>>> =
            match trip.frequencies.len() {
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
            frequencies: None,
        };

        use catenary::schema::gtfs::trips::dsl::trips;

        diesel::insert_into(trips)
            .values(trip_pg)
            .execute(conn)
            .await?;

        //insert trip frequencies into seperate table for now until custom types are fixed in diesel or i find a better solution
        use catenary::models::TripFrequencyTableRow;

        let frequencies_for_table = trip
            .frequencies
            .iter()
            .enumerate()
            .map(|(i, freq)| catenary::models::TripFrequencyTableRow {
                trip_id: trip_id.clone(),
                onestop_feed_id: feed_id.to_string(),
                attempt_id: attempt_id.to_string(),
                index: i as i16,
                start_time: freq.start_time,
                end_time: freq.end_time,
                headway_secs: freq.headway_secs,
                exact_times: match freq.exact_times {
                    Some(exact_times_num) => match exact_times_num {
                        ExactTimes::FrequencyBased => false,
                        ExactTimes::ScheduleBased => true,
                    },
                    None => false,
                },
            })
            .collect::<Vec<TripFrequencyTableRow>>();

        use catenary::schema::gtfs::trip_frequencies::dsl::trip_frequencies;

        diesel::insert_into(trip_frequencies)
            .values(frequencies_for_table)
            .execute(conn)
            .await?;

        //inside insert stoptimes

        let stop_times_pg = trip
            .stop_times
            .iter()
            .map(|stop_time| catenary::models::StopTime {
                onestop_feed_id: feed_id.to_string(),
                route_id: trip.route_id.clone(),
                stop_headsign_translations: None,
                trip_id: trip_id.clone(),
                attempt_id: attempt_id.to_string(),
                stop_id: stop_time.stop.id.clone(),
                stop_sequence: stop_time.stop_sequence as i32,
                arrival_time: stop_time.arrival_time,
                departure_time: stop_time.departure_time,
                stop_headsign: stop_time.stop_headsign.clone(),
                pickup_type: pickup_dropoff_to_i16(&stop_time.pickup_type),
                drop_off_type: pickup_dropoff_to_i16(&stop_time.drop_off_type),
                shape_dist_traveled: stop_time.shape_dist_traveled,
                timepoint: match stop_time.timepoint {
                    gtfs_structures::TimepointType::Exact => true,
                    gtfs_structures::TimepointType::Approximate => false,
                },
                chateau: chateau_id.to_string(),
                point: match stop_time.stop.latitude {
                    Some(latitude) => match stop_time.stop.longitude {
                        Some(longitude) => Some(postgis_diesel::types::Point {
                            srid: Some(4326),
                            x: longitude,
                            y: latitude,
                        }),
                        None => None,
                    },
                    None => None,
                },

                continuous_pickup: continuous_pickup_drop_off_to_i16(&stop_time.continuous_pickup),
                continuous_drop_off: continuous_pickup_drop_off_to_i16(
                    &stop_time.continuous_drop_off,
                ),
            })
            .collect::<Vec<catenary::models::StopTime>>();

        use catenary::schema::gtfs::stoptimes::dsl::stoptimes;

        diesel::insert_into(stoptimes)
            .values(stop_times_pg)
            .execute(conn)
            .await?;
    }

    //insert stops

    //calculate hull
    //submit hull

    // insert feed info

    Ok(())
}

pub fn pickup_dropoff_to_i16(x: &gtfs_structures::PickupDropOffType) -> i16 {
    match x {
        gtfs_structures::PickupDropOffType::Regular => 0,
        gtfs_structures::PickupDropOffType::NotAvailable => 1,
        gtfs_structures::PickupDropOffType::ArrangeByPhone => 2,
        gtfs_structures::PickupDropOffType::CoordinateWithDriver => 3,
        gtfs_structures::PickupDropOffType::Unknown(x) => *x,
    }
}

pub fn continuous_pickup_drop_off_to_i16(x: &ContinuousPickupDropOff) -> i16 {
    match x {
        ContinuousPickupDropOff::Continuous => 0,
        ContinuousPickupDropOff::NotAvailable => 1,
        ContinuousPickupDropOff::ArrangeByPhone => 2,
        ContinuousPickupDropOff::CoordinateWithDriver => 3,
        ContinuousPickupDropOff::Unknown(x) => *x,
    }
}
