// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use crate::gtfs_handlers::colour_correction::fix_background_colour_rgb_feed_route;
use crate::gtfs_handlers::colour_correction::fix_foreground_colour_rgb_feed;
// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::shape_colour_calculator::ShapeToColourResponse;
use crate::gtfs_handlers::stops_associated_items::*;
use crate::gtfs_ingestion_sequence::calendar_into_postgres::calendar_into_postgres;
use crate::gtfs_ingestion_sequence::extra_stop_to_stop_shapes_into_postgres::insert_stop_to_stop_geometry;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::gtfs_ingestion_sequence::stops_into_postgres::stops_into_postgres;
use crate::DownloadedFeedsInformation;
use catenary::enum_to_int::*;
use catenary::gtfs_schedule_protobuf::frequencies_to_protobuf;
use catenary::maple_syrup;
use catenary::models::{
    DirectionPatternMeta, DirectionPatternRow, ItineraryPatternMeta, ItineraryPatternRow,
    Route as RoutePgModel,
};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::route_id_transform;
use catenary::schedule_filtering::minimum_day_filter;
use chrono::NaiveDate;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::FeedInfo;
use gtfs_structures::Gtfs;
use gtfs_translations::translation_csv_text_to_translations;
use gtfs_translations::TranslationResult;
use prost::Message;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

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
    gtfs_unzipped_path: &str,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    this_download_data: &DownloadedFeedsInformation,
) -> Result<GtfsSummary, Box<dyn Error + Send + Sync>> {
    println!("Begin feed {} processing", feed_id);
    let start = Instant::now();
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    //read the GTFS zip file
    let path = format!("{}/{}", gtfs_unzipped_path, feed_id);

    let gtfs = gtfs_structures::Gtfs::new(path.as_str())?;

    let gtfs: Gtfs = match feed_id {
        "f-dpz8-ttc" => {
            use catenary::schedule_filtering::include_only_route_types;

            let route_types = vec![gtfs_structures::RouteType::Subway];

            let gtfs = include_only_route_types(gtfs, route_types);

            println!("Filtered TTC Subway");
            gtfs.print_stats();
            gtfs
        }
        "f-r6-nswtrainlink~sydneytrains~buswayswesternsydney~interlinebus" => {
            //there's 8184 school buses in the feed. I'm removing them lmfao.
            use catenary::schedule_filtering::include_only_route_types;

            let route_types = gtfs.routes.values().map(|x| x.route_type).collect::<HashSet<_>>()
                .into_iter()
                .filter(|x| *x != gtfs_structures::RouteType::Other(712))
                .collect::<Vec<_>>();

            let gtfs = include_only_route_types(gtfs, route_types);

            println!("Filtered NSW, removed school buses");
            gtfs.print_stats();
            gtfs
        },
        "f-uc~irvine~anteater~express" => {
            let mut gtfs = gtfs;

            gtfs.feed_info = vec![gtfs_structures::FeedInfo {
                name: "TransLoc, Inc.".to_string(),
                url: "https://transloc.com".to_string(),
                lang: "en".to_string(),
                start_date: Some(chrono::NaiveDate::from_ymd_opt(2024, 07, 10).unwrap()),
                end_date: Some(chrono::NaiveDate::from_ymd_opt(2099, 12, 31).unwrap()),
                version: None,
                default_lang: Some("en".to_string()),
                contact_email: None,
                contact_url: None,
            }];

            gtfs
        }
        _ => gtfs,
    };

    let today = chrono::Utc::now().naive_utc().date();

    let gtfs = minimum_day_filter(gtfs, today - chrono::Duration::days(30));

    println!(
        "Finished reading GTFS for {}, took {:?}ms",
        feed_id, gtfs.read_duration
    );

    // Read Translations.txt, don't fail if it doesn't exist
    let translation_path = format!("{}/{}/translations.txt", gtfs_unzipped_path, feed_id);
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
        general_timezone: match gtfs.agencies.len() {
            0 => String::from("Etc/UTC"),
            _ => gtfs.agencies[0].timezone.clone(),
        },
    };

    let start_reduction_timer = Instant::now();
    let reduction = maple_syrup::reduce(&gtfs);
    println!(
        "Reduced schedule for {} in {:?}",
        feed_id,
        start_reduction_timer.elapsed()
    );
    println!(
        "{} itineraries, {} trips, {:.2} ratio",
        reduction.itineraries.len(),
        reduction.trips_to_itineraries.len(),
        reduction.trips_to_itineraries.len() as f64 / reduction.itineraries.len() as f64
    );

    let feed_info: Option<FeedInfo> = match !gtfs.feed_info.is_empty() {
        true => Some(gtfs.feed_info[0].clone()),
        false => None,
    };

    if let Some(feed_info) = &feed_info {
        gtfs_summary.feed_start_date = feed_info.start_date;
        gtfs_summary.feed_end_date = feed_info.end_date;

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

    println!(
        "Making stop to route type and route id hashmaps for {}",
        feed_id
    );
    let timer_stop_id_table = Instant::now();
    let (stop_ids_to_route_types, stop_ids_to_route_ids) =
        make_hashmap_stops_to_route_types_and_ids(&gtfs);

    let (stop_id_to_children_ids, stop_ids_to_children_route_types) =
        make_hashmaps_of_children_stop_info(&gtfs, &stop_ids_to_route_types);

    println!(
        "Finished making stop to route type and route id hashmaps in {:?} for {}",
        timer_stop_id_table.elapsed(),
        feed_id
    );

    //identify colours of shapes based on trip id's route id
    // also make reverse lookup for route ids to shape ids
    let ShapeToColourResponse {
        shape_to_color_lookup,
        shape_to_text_color_lookup,
        shape_id_to_route_ids_lookup,
        route_ids_to_shape_ids,
    } = shape_to_colour(feed_id, &gtfs);

    //insert agencies
    let mut agency_id_already_done: HashSet<Option<&String>> = HashSet::new();

    for agency in &gtfs.agencies {
        use catenary::schema::gtfs::agencies::dsl::agencies;

        if !agency_id_already_done.contains(&agency.id.as_ref()) {
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

            agency_id_already_done.insert(agency.id.as_ref());
        } else {
            eprintln!("Warning! Duplicate agency id found: \n{:?}", agency);
        }
    }

    println!("Agency insertion done for {}", feed_id);

    drop(agency_id_already_done);

    println!("Inserting shapes for {}", feed_id);

    //shove raw geometry into postgresql

    shapes_into_postgres(
        &gtfs,
        &shape_to_color_lookup,
        &shape_to_text_color_lookup,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
        &shape_id_to_route_ids_lookup,
    )
    .await?;

    println!("Shapes inserted for {}", feed_id);

    //insert calendar

    println!("Inserting calendar for {}", feed_id);

    calendar_into_postgres(
        &gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
    )
    .await?;

    println!("Calendar inserted for {}", feed_id);
    println!("Inserting stops for {}", feed_id);

    //insert stops
    stops_into_postgres(
        &gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
        &stop_ids_to_route_types,
        &stop_ids_to_route_ids,
        &stop_id_to_children_ids,
        &stop_ids_to_children_route_types,
    )
    .await?;

    println!("Stops inserted for {}", feed_id);

    // insert trip and itineraries

    println!("Inserting directions for {}", feed_id);

    for (direction_pattern_id, direction_pattern) in &reduction.direction_patterns {
        let gtfs_shape_id = match &direction_pattern.gtfs_shape_id {
            Some(gtfs_shape_id) => gtfs_shape_id.clone(),
            None => direction_pattern_id.to_string(),
        };

        let first_itin_id = reduction
            .direction_pattern_id_to_itineraries
            .get(direction_pattern_id)
            .unwrap()
            .iter()
            .next()
            .expect("Expected Itin for direction id");

        let itin_pattern = reduction
            .itineraries
            .get(first_itin_id)
            .expect("Did not find itin pattern, crashing....");

        if direction_pattern.gtfs_shape_id.is_none() {
            //: postgis_diesel::types::LineString<postgis_diesel::types::Point>

            let stop_points = direction_pattern
                .stop_sequence
                .iter()
                .filter_map(|stop_id| gtfs.stops.get(stop_id.as_str()))
                .filter_map(|stop| match (stop.latitude, stop.longitude) {
                    (Some(latitude), Some(longitude)) => Some(postgis_diesel::types::Point {
                        y: latitude,
                        x: longitude,
                        srid: Some(4326),
                    }),
                    _ => None,
                })
                .collect::<Vec<postgis_diesel::types::Point>>();

            if stop_points.len() > 2 {
                let linestring: postgis_diesel::types::LineString<postgis_diesel::types::Point> =
                    postgis_diesel::types::LineString {
                        points: stop_points,
                        srid: Some(4326),
                    };

                //insert into shapes and shapes_not_bus

                let route = gtfs.routes.get(direction_pattern.route_id.as_str());

                if let Some(route) = route {
                    let _ = insert_stop_to_stop_geometry(
                        feed_id,
                        attempt_id,
                        chateau_id,
                        route,
                        *direction_pattern_id,
                        &linestring,
                        Arc::clone(&arc_conn_pool),
                    )
                    .await;
                }
            }
        }

        let direction_pattern_meta = DirectionPatternMeta {
            chateau: chateau_id.to_string(),
            direction_pattern_id: direction_pattern_id.to_string(),
            headsign_or_destination: direction_pattern
                .headsign_or_destination
                .clone()
                .unwrap_or_else(|| "".to_string()),
            gtfs_shape_id: Some(gtfs_shape_id.clone()),
            fake_shape: direction_pattern.gtfs_shape_id.is_none(),
            onestop_feed_id: feed_id.to_string(),
            attempt_id: attempt_id.to_string(),
            route_id: Some(itin_pattern.route_id.clone()),
            route_type: Some(itin_pattern.route_type),
            direction_id: itin_pattern.direction_id,
        };

        //insert stop list into DirectionPatternRow

        diesel::insert_into(
            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta,
        )
        .values(direction_pattern_meta)
        .execute(conn)
        .await?;

        let direction_pattern_rows: Vec<DirectionPatternRow> = itin_pattern
            .stop_sequences
            .iter()
            .enumerate()
            .map(|(stop_idx, stop_time)| DirectionPatternRow {
                attempt_id: attempt_id.to_string(),
                chateau: chateau_id.to_string(),
                direction_pattern_id: direction_pattern_id.to_string(),
                stop_id: stop_time.stop_id.clone(),
                stop_sequence: stop_idx as u32,
                onestop_feed_id: feed_id.to_string(),
                arrival_time_since_start: stop_time.arrival_time_since_start,
                departure_time_since_start: stop_time.departure_time_since_start,
                interpolated_time_since_start: stop_time.interpolated_time_since_start,
            })
            .collect();

        for dir_chunk in direction_pattern_rows.chunks(50) {
            diesel::insert_into(catenary::schema::gtfs::direction_pattern::dsl::direction_pattern)
                .values(dir_chunk)
                .execute(conn)
                .await?;
        }
    }

    println!("Directions inserted for {}", feed_id);
    println!("Inserting itineraries for {}", feed_id);

    for (itinerary_id, itinerary) in &reduction.itineraries {
        let itinerary_pg_meta = ItineraryPatternMeta {
            onestop_feed_id: feed_id.to_string(),
            chateau: chateau_id.to_string(),
            attempt_id: attempt_id.to_string(),
            timezone: itinerary.timezone.clone(),
            trip_headsign: itinerary
                .trip_headsign
                .clone()
                .map(|x| x.replace(" - Funded in part by/SB County Measure A", "")),
            trip_headsign_translations: None,
            itinerary_pattern_id: itinerary_id.to_string(),
            trip_ids: reduction
                .itineraries_to_trips
                .get(itinerary_id)
                .as_ref()
                .unwrap()
                .iter()
                .map(|trip_under_itin| Some(trip_under_itin.trip_id.to_string()))
                .collect::<Vec<Option<String>>>(),
            shape_id: itinerary.shape_id.clone(),
            route_id: itinerary.route_id.clone(),
            direction_pattern_id: Some(itinerary.direction_pattern_id.to_string()),
        };

        diesel::insert_into(
            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta,
        )
        .values(itinerary_pg_meta)
        .execute(conn)
        .await?;

        let itinerary_pg = itinerary
            .stop_sequences
            .iter()
            .enumerate()
            .map(|(stop_index, stop_sequence)| ItineraryPatternRow {
                onestop_feed_id: feed_id.to_string(),
                chateau: chateau_id.to_string(),
                attempt_id: attempt_id.to_string(),
                itinerary_pattern_id: itinerary_id.to_string(),
                stop_sequence: stop_index as i32,
                stop_id: stop_sequence.stop_id.clone(),
                gtfs_stop_sequence: stop_sequence.gtfs_stop_sequence as u32,
                arrival_time_since_start: stop_sequence.arrival_time_since_start,
                departure_time_since_start: stop_sequence.departure_time_since_start,
                interpolated_time_since_start: stop_sequence.interpolated_time_since_start,
            })
            .collect::<Vec<_>>();

        for itinerary_chunk in itinerary_pg.chunks(50) {
            diesel::insert_into(catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern)
                .values(itinerary_chunk)
                .execute(conn)
                .await?;
        }
    }

    println!("Itineraries inserted for {}", feed_id);

    //insert trips

    println!("Inserting trips for {}", feed_id);

    for (itinerary_id, compressed_trip_list) in &reduction.itineraries_to_trips {
        let trip_pg = compressed_trip_list
            .iter()
            .map(|compressed_trip_raw| catenary::models::CompressedTrip {
                onestop_feed_id: feed_id.to_string(),
                chateau: chateau_id.to_string(),
                attempt_id: attempt_id.to_string(),
                itinerary_pattern_id: itinerary_id.to_string(),
                trip_id: compressed_trip_raw.trip_id.to_string(),
                service_id: compressed_trip_raw.service_id.clone(),
                direction_id: reduction
                    .itineraries
                    .get(itinerary_id)
                    .unwrap()
                    .direction_id,
                start_time: compressed_trip_raw.start_time,
                trip_short_name: compressed_trip_raw.trip_short_name.clone(),
                block_id: compressed_trip_raw.block_id.clone(),
                wheelchair_accessible: compressed_trip_raw.wheelchair_accessible,
                bikes_allowed: compressed_trip_raw.bikes_allowed,
                has_frequencies: !compressed_trip_raw.frequencies.is_empty(),
                route_id: route_id_transform(feed_id, compressed_trip_raw.route_id.clone()),
                frequencies: match !compressed_trip_raw.frequencies.is_empty() {
                    true => {
                        let prost_message =
                            frequencies_to_protobuf(&compressed_trip_raw.frequencies);
                        Some(prost_message.encode_to_vec())
                    }
                    false => None,
                },
            })
            .collect::<Vec<_>>();

        for trip_chunk in trip_pg.chunks(50) {
            diesel::insert_into(catenary::schema::gtfs::trips_compressed::dsl::trips_compressed)
                .values(trip_chunk)
                .execute(conn)
                .await?;
        }
    }

    //insert routes

    println!("Inserting routes for {}", feed_id);

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
                route_id: route_id_transform(feed_id, route_id.to_string()),
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
                route_type: route_type_to_int(&route.route_type),
                url: route.url.clone(),
                url_translations: None,
                shapes_list: route_ids_to_shape_ids
                    .get(&route_id.clone())
                    .map(|shapes_list| {
                        shapes_list
                            .iter()
                            .map(|x| Some(x.clone()))
                            .collect::<Vec<Option<String>>>()
                    }),
                gtfs_order: route.order,
                continuous_drop_off: continuous_pickup_drop_off_to_i16(&route.continuous_drop_off),
                continuous_pickup: continuous_pickup_drop_off_to_i16(&route.continuous_pickup),
            };
            route_pg
        })
        .collect();

    for routes_chunk in routes_pg.chunks(50) {
        diesel::insert_into(catenary::schema::gtfs::routes::dsl::routes)
            .values(routes_chunk)
            .execute(conn)
            .await?;
    }

    println!("Routes inserted for {}", feed_id);

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

    println!("Insert hull for {}", feed_id);

    let hull_pg: Option<postgis_diesel::types::Polygon<postgis_diesel::types::Point>> =
        hull.map(|polygon_geo| postgis_diesel::types::Polygon {
            rings: vec![polygon_geo
                .into_inner()
                .0
                .into_iter()
                .map(|coord| {
                    postgis_diesel::types::Point::new(coord.x, coord.y, Some(catenary::WGS_84_SRID))
                })
                .collect()],
            srid: Some(catenary::WGS_84_SRID),
        });

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
            catenary::schema::gtfs::static_feeds::dsl::hull
                .eq(hull_pg.map(postgis_diesel::types::GeometryContainer::Polygon)),
        ))
        .execute(conn)
        .await?;

    let ingest_duration = start.elapsed();
    println!(
        "Finished {}, took {:.3}s",
        feed_id,
        ingest_duration.as_secs_f32()
    );

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
