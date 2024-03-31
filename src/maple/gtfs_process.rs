use crate::gtfs_handlers::colour_correction::fix_background_colour_rgb_feed_route;
use crate::gtfs_handlers::colour_correction::fix_foreground_colour_rgb;
use crate::gtfs_handlers::colour_correction::fix_foreground_colour_rgb_feed;
// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license
use crate::gtfs_handlers::gtfs_to_int::availability_to_int;
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::gtfs_ingestion_sequence::stops_into_postgres::stops_into_postgres;
use crate::DownloadedFeedsInformation;
use catenary::models::Route as RoutePgModel;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::chateaus::languages_avaliable;
use catenary::schema::gtfs::stoptimes::continuous_drop_off;
use chrono::NaiveDate;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use geo::polygon;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::FeedInfo;
use gtfs_structures::{BikesAllowedType, ExactTimes};
use gtfs_translations::translation_csv_text_to_translations;
use gtfs_translations::TranslationResult;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use titlecase::titlecase;

#[derive(Debug)]
pub struct GtfsSummary {
    pub feed_start_date: Option<NaiveDate>,
    pub feed_end_date: Option<NaiveDate>,
    pub languages_avaliable: HashSet<String>,
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
) -> Result<GtfsSummary, Box<dyn Error + Send + Sync>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    //read the GTFS zip file
    let path = format!("gtfs_uncompressed/{}", feed_id);

    let gtfs = gtfs_structures::Gtfs::new(path.as_str())?;

    // Read Translations.txt, don't fail if it doesn't exist
    let translation_path = format!("gtfs_uncompressed/{}/translations.txt", feed_id);
    let translation_data = std::fs::read_to_string(translation_path);

    let gtfs_translations: Option<TranslationResult> = match translation_data {
        Ok(data) => match translation_csv_text_to_translations(data.as_str()) {
            Ok(translations) => Some(translations),
            Err(_) => None,
        },
        Err(_) => None,
    };
    let mut gtfs_summary = GtfsSummary {
        feed_start_date: None,
        feed_end_date: None,
        languages_avaliable: HashSet::new(),
        default_lang: None,
        general_timezone: gtfs.agencies[0].timezone.clone(),
    };

    let feed_info: Option<FeedInfo> = match gtfs.feed_info.len() >= 1 {
        true => Some(gtfs.feed_info[0].clone()),
        false => None,
    };

    if let Some(feed_info) = &feed_info {
        gtfs_summary.feed_start_date = feed_info.start_date.clone();
        gtfs_summary.feed_end_date = feed_info.end_date.clone();

        if let Some(default_lang) = &feed_info.default_lang {
            gtfs_summary.default_lang = Some(default_lang.clone());
            gtfs_summary
                .languages_avaliable
                .insert(default_lang.clone());
        }
    }

    //copy the avaliable languages from the translations.txt file over
    if let Some(gtfs_translations) = &gtfs_translations {
        for avaliable_language in &gtfs_translations.avaliable_languages {
            gtfs_summary
                .languages_avaliable
                .insert(avaliable_language.as_str().to_string());
        }
    }

    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        make_hashmap_stops_to_route_types_and_ids(&gtfs);

    let (stop_id_to_children_ids, stop_ids_to_children_route_types) =
        make_hashmaps_of_children_stop_info(&gtfs, &stop_ids_to_route_types);

    //identify colours of shapes based on trip id's route id
    let (shape_to_color_lookup, shape_to_text_color_lookup) = shape_to_colour(&feed_id, &gtfs);

    // make reverse lookup for route ids to shape ids
    let route_ids_to_shape_ids: HashMap<String, HashSet<String>> = {
        let mut results: HashMap<String, HashSet<String>> = HashMap::new();

        for (trip_id, trip) in &gtfs.trips {
            if let Some(shape_id) = &trip.shape_id {
                results
                    .entry(trip.route_id.clone())
                    .and_modify(|current_list| {
                        current_list.insert(shape_id.clone());
                    })
                    .or_insert(HashSet::from_iter(vec![shape_id.clone()]));
            }
        }

        results
    };

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
            wheelchair_accessible: availability_to_int(&trip.wheelchair_accessible),
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
                            srid: Some(catenary::WGS_84_SRID),
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
    let _ = stops_into_postgres(
        &gtfs,
        &feed_id,
        Arc::clone(&arc_conn_pool),
        &chateau_id,
        &attempt_id,
        &stop_ids_to_route_types,
        &stop_ids_to_route_ids,
        &stop_id_to_children_ids,
        &stop_ids_to_children_route_types,
    )
    .await?;

    //insert routes

    let routes_pg: Vec<RoutePgModel> = gtfs
        .routes
        .iter()
        .map(|(route_id, route)| {
            let colour = fix_background_colour_rgb_feed_route(feed_id, route.color, route);
            let text_colour =
                fix_foreground_colour_rgb_feed(feed_id, route.color, route.text_color);

            let colour_pg = format!("#{:02x}{:02x}{:02x}", colour.r, colour.g, colour.b);
            let text_colour_pg = format!(
                "#{:02x}{:02x}{:02x}",
                text_colour.r, text_colour.g, text_colour.b
            );

            let route_pg = RoutePgModel {
                onestop_feed_id: feed_id.to_string(),
                route_id: route_id.clone(),
                attempt_id: attempt_id.to_string(),
                agency_id: route.agency_id.clone(),
                short_name: route.short_name.clone(),
                long_name: route.long_name.clone(),
                chateau: chateau_id.to_string(),
                color: Some(colour_pg),
                text_color: Some(text_colour_pg),
                short_name_translations: None,
                long_name_translations: None,
                gtfs_desc: route.desc.clone(),
                gtfs_desc_translations: None,
                route_type: crate::gtfs_handlers::gtfs_to_int::route_type_to_int(&route.route_type),
                url: route.url.clone(),
                url_translations: None,
                shapes_list: match route_ids_to_shape_ids.get(&route_id.clone()) {
                    Some(shapes_list) => Some(
                        shapes_list
                            .iter()
                            .map(|x| Some(x.clone()))
                            .collect::<Vec<Option<String>>>(),
                    ),
                    None => None,
                },
                gtfs_order: match route.order {
                    Some(x) => Some(x),
                    None => None,
                },
                continuous_drop_off: continuous_pickup_drop_off_to_i16(&route.continuous_drop_off),
                continuous_pickup: continuous_pickup_drop_off_to_i16(&route.continuous_pickup),
            };
            route_pg
        })
        .collect();

    diesel::insert_into(catenary::schema::gtfs::routes::dsl::routes)
        .values(routes_pg)
        .execute(conn)
        .await?;

    //calculate concave hull
    let hull = crate::gtfs_handlers::hull_from_gtfs::hull_from_gtfs(&gtfs);

    // insert feed info
    if let Some(feed_info) = &feed_info {
        use catenary::schema::gtfs::feed_info::dsl::feed_info as feed_table;

        let feed_info_pg = catenary::models::FeedInfo {
            onestop_feed_id: feed_id.to_string(),
            feed_publisher_name: feed_info.name.clone(),
            feed_publisher_url: feed_info.url.clone(),
            feed_lang: feed_info.lang.clone(),
            feed_start_date: feed_info.start_date,
            feed_end_date: feed_info.end_date,
            feed_version: feed_info.version.clone(),
            feed_contact_email: feed_info.contact_email.clone(),
            feed_contact_url: feed_info.contact_url.clone(),
            attempt_id: attempt_id.to_string(),
            default_lang: feed_info.default_lang.clone(),
            chateau: chateau_id.to_string(),
        };

        diesel::insert_into(feed_table)
            .values(feed_info_pg)
            .execute(conn)
            .await?;
    }
    //submit hull

    let hull_pg: Option<postgis_diesel::types::Polygon<postgis_diesel::types::Point>> = match hull {
        Some(polygon_geo) => Some(postgis_diesel::types::Polygon {
            rings: vec![polygon_geo
                .into_inner()
                .0
                .into_iter()
                .map(|coord| {
                    postgis_diesel::types::Point::new(coord.x, coord.y, Some(catenary::WGS_84_SRID))
                })
                .collect()],
            srid: Some(catenary::WGS_84_SRID),
        }),
        None => None,
    };

    let languages_avaliable_pg = gtfs_summary
        .languages_avaliable
        .iter()
        .map(|x| Some(x.clone()))
        .collect::<Vec<Option<String>>>();

    let static_feed_pg = catenary::models::StaticFeed {
        onestop_feed_id: feed_id.to_string(),
        chateau: chateau_id.to_string(),
        default_lang: match feed_info {
            Some(feed_info) => Some(feed_info.lang.clone()),
            None => None,
        },
        previous_chateau_name: chateau_id.to_string(),
        languages_avaliable: languages_avaliable_pg.clone(),
        hull: hull_pg.clone(),
    };

    //create the static feed entry
    let _ = diesel::insert_into(catenary::schema::gtfs::static_feeds::dsl::static_feeds)
        .values(&static_feed_pg)
        .on_conflict(catenary::schema::gtfs::static_feeds::dsl::onestop_feed_id)
        .do_update()
        .set((
            catenary::schema::gtfs::static_feeds::dsl::languages_avaliable
                .eq(languages_avaliable_pg),
            catenary::schema::gtfs::static_feeds::dsl::hull.eq(match hull_pg {
                Some(hull_pg) => Some(postgis_diesel::types::GeometryContainer::Polygon(hull_pg)),
                None => None,
            }),
        ))
        .execute(conn)
        .await?;

    Ok(gtfs_summary)
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
