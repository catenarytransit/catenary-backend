// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use crate::gtfs_handlers::colour_correction::fix_background_colour_rgb_feed_route;
use crate::gtfs_handlers::colour_correction::fix_foreground_colour_rgb_feed;
// Initial version 3 of ingest written by Kyler Chin
// Removal of the attribution is not allowed, as covered under the AGPL license
use crate::DownloadedFeedsInformation;
use crate::gtfs_handlers::colour_correction;
use crate::gtfs_handlers::shape_colour_calculator::ShapeToColourResponse;
use crate::gtfs_handlers::shape_colour_calculator::shape_to_colour;
use crate::gtfs_handlers::stops_associated_items::*;
use crate::gtfs_ingestion_sequence::calendar_into_postgres::calendar_into_postgres;
use crate::gtfs_ingestion_sequence::extra_stop_to_stop_shapes_into_postgres::insert_stop_to_stop_geometry;
use crate::gtfs_ingestion_sequence::shapes_into_postgres::shapes_into_postgres;
use crate::gtfs_ingestion_sequence::stops_into_postgres::stops_into_postgres_and_elastic;
use catenary::enum_to_int::*;
use catenary::gtfs_schedule_protobuf::frequencies_to_protobuf;
use catenary::maple_syrup;
use catenary::models::{
    DirectionPatternMeta, DirectionPatternRow, ItineraryPatternMeta, ItineraryPatternRow,
    Route as RoutePgModel,
};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::route_id_transform;
use catenary::schedule_filtering::include_only_route_types;
use catenary::schedule_filtering::minimum_day_filter;
use chrono::NaiveDate;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use geo::BoundingRect;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::FeedInfo;
use gtfs_structures::Gtfs;
use gtfs_translations::TranslationResult;
use gtfs_translations::translation_csv_text_to_translations;
use itertools::Itertools;
use prost::Message;
use regex::Regex;
use rgb::RGB;
use std::collections::BTreeSet;
use std::collections::HashMap;
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
    pub bbox: Option<geo::Rect>,
}

// take a feed id and throw it into postgres
pub async fn gtfs_process_feed(
    gtfs_unzipped_path: &str,
    feed_id: &str,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    chateau_id: &str,
    attempt_id: &str,
    this_download_data: &DownloadedFeedsInformation,
    elasticclient: &elasticsearch::Elasticsearch,
) -> Result<GtfsSummary, Box<dyn Error + Send + Sync>> {
    let regex_train_starting = regex::RegexBuilder::new(r"^(train)")
        .case_insensitive(true)
        .build()
        .unwrap();

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
            let route_types = vec![gtfs_structures::RouteType::Subway];

            let gtfs = include_only_route_types(gtfs, route_types, true);

            println!("Filtered TTC Subway");
            gtfs.print_stats();
            gtfs
        }
        "f-r6-nswtrainlink~sydneytrains~buswayswesternsydney~interlinebus" => {
            // There are 8184 school buses in the feed. These are being removed for processing.
            let routes = gtfs
                .routes
                .into_iter()
                .filter(|(_, route)| match route.desc {
                    Some(ref desc) => !desc.contains("School"),
                    _ => true,
                })
                .collect::<HashMap<String, gtfs_structures::Route>>();

            let route_ids_to_keep: BTreeSet<_> = routes.keys().cloned().collect();

            let trips = gtfs
                .trips
                .into_iter()
                .filter(|(_, trip)| route_ids_to_keep.contains(&trip.route_id))
                .collect::<HashMap<String, gtfs_structures::Trip>>();

            let mut gtfs = gtfs;
            gtfs.routes = routes;
            gtfs.trips = trips;

            println!("Filtered NSW, removed school buses");
            gtfs.print_stats();
            gtfs
        }
        "f-amtrak~sanjoaquin" => {
            let mut gtfs = gtfs;

            gtfs.trips.retain(|trip_id, trip| trip.route_id != "ACE");

            gtfs.routes.retain(|route_id, route| route_id != "ACE");

            for (trip_id, trip) in gtfs.trips.iter_mut() {
                if trip.trip_short_name.as_deref() == Some("AMSJ") {
                    trip.trip_short_name = Some(trip_id.to_string());
                }
            }

            gtfs
        }
        "f-los~angeles~international~airport~shuttle" => {
            let mut gtfs = gtfs;

            gtfs.routes.iter_mut().for_each(|(route_id, route)| {
                route.long_name = route.long_name.as_ref().map(|x| {
                    x.replace("Metro Connector GL", "Metro Connector C")
                        .to_string()
                });
            });

            gtfs
        }
        "f-gtfs~de" => crate::gtfs_handlers::gtfs_de_cleanup::gtfs_de_cleanup(gtfs),
        "f-u0-switzerland" => crate::gtfs_handlers::gtfs_de_cleanup::gtfs_ch_cleanup(gtfs),
        "f-nvbw" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([
                String::from("DB AG"),
                String::from("FlixTrain-de"),
                String::from("SNCF"),
                String::from("Schweizerische Bundesbahnen SBB"),
                String::from("Schweiz. Schifffahrtsgesellschaft Untersee und Rhein AG"),
                String::from("FlixBus-de"),
                String::from("NVBW"),
                String::from("OEBB Personenverkehr AG Kundenservice"),
                String::from("DB Regio AG Bayern"),
                String::from("DB Fernverkehr AG"),
                String::from("SNCF Voyages Deutschland"),
                String::from("MVV-Regionalbus"),
                String::from("U-Bahn München"),
                String::from("Straßenbahn München"),
                String::from("Regionalverkehr Oberbayer (überregional)"),
                String::from("Bus München"),
                String::from("NachtTram München"),
                String::from("NachtBus München"),
                String::from("Nahreisezug"),
            ]),
        ),
        "f-ahverkehrsverbund~schleswig~holstein~nah" => {
            crate::gtfs_handlers::remove_agencies::remove_agencies(
                gtfs,
                &Vec::from([String::from("DB Fernverkehr (Codesharing)")]),
            )
        }
        "f-u4-ruter~flybussen~stfoldkollektivtrafikk~hedmarktrafikk~oppla" => {
            crate::gtfs_handlers::remove_agencies::remove_agencies(
                gtfs,
                &Vec::from([String::from("Avinor")]),
            )
        }
        "f-sp9x-normandie" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([String::from("Nomad Train (SNCF, Région Normandie)")]),
        ),
        "f-gbwc-mobibreizh" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([String::from("TER BreizhGo")]),
        ),
        "f-Ostösterreich~Österreich" => crate::gtfs_handlers::remove_agencies::remove_agencies(
            gtfs,
            &Vec::from([String::from("Wiener Linien GmbH & Co KG")]),
        ),
        "f-dr5r-mtasubway" => {
            //https://www.mta.info/document/134521
            //Align the GTFS schedule schedule ids for the New York City Subway to the realtime ids by dropping the first underscore
            let mut gtfs = gtfs;

            let mut new_trips = HashMap::new();

            for (trip_id, trip) in gtfs.trips {
                let new_trip_id_split = trip_id.split_once('_');

                let matched_new_trip_id = match new_trip_id_split {
                    None => trip_id,
                    Some(new_trip_id_split) => new_trip_id_split.1.to_string(),
                };

                let mut trip = trip;

                trip.id = matched_new_trip_id.clone();

                new_trips.insert(matched_new_trip_id, trip);
            }

            gtfs.trips = new_trips;

            gtfs
        }
        _ => gtfs,
    };

    let mut gtfs = gtfs;

    if feed_id == "f-u0-sncf~tgv" || feed_id == "f-u0-sncf~ter" || feed_id == "f-u0-sncf~intercites"
    {
        gtfs.trips = gtfs
            .trips
            .into_iter()
            .map(|(trip_id, trip)| {
                let mut trip = trip;
                if trip.trip_short_name == None {
                    trip.trip_short_name = trip.trip_headsign.clone();
                    trip.trip_headsign = None;
                }
                (trip_id, trip)
            })
            .collect();
    }

    if feed_id == "f-uc~irvine~anteater~express" {
        gtfs.routes
            .retain(|route_id, route| route.long_name.as_deref() != Some("Emergency Management"));
    }

    if feed_id == "f-9q5-metro~losangeles~rail" {
        for (route_id, route) in gtfs.routes.iter_mut() {
            if (route.long_name.as_deref() == Some("Metro C Line")) {
                //      route.long_name = Some("Metro Chin Line".to_string())
            }

            if (route.long_name.as_deref() == Some("Metro K Line")) {
                //       route.long_name = Some("Metro Kyler Line".to_string())
            }
        }
    }

    if feed_id == "f-9mu-orangecountytransportationauthority" {
        gtfs.agencies = gtfs
            .agencies
            .into_iter()
            .map(|agency| {
                let mut agency = agency;
                //      agency.name = "Metro OC Bus, a subdivision of LA Metro".to_string();
                agency
            })
            .collect();

        gtfs.routes.remove("400");
        gtfs.routes.remove("401");
        gtfs.routes.remove("403");

        let mut trips_to_remove: Vec<String> = vec![];

        for (trip_id, trip) in gtfs.trips.iter() {
            if trip.route_id.as_str() == "401"
                || trip.route_id.as_str() == "400"
                || trip.route_id.as_str() == "403"
            {
                trips_to_remove.push(trip_id.clone());
            }
        }

        for trip_to_remove in trips_to_remove {
            gtfs.trips.remove(&trip_to_remove);
        }
    }

    if feed_id == "f-9q5-metro~losangeles" {
        for (route_id, route) in gtfs.routes.iter_mut() {
            if route.long_name.as_deref() == Some("Metro G Line (Orange) 901") {
                route.long_name = Some("G 901".to_string())
            }

            if route.long_name.as_deref() == Some("Metro J Line (Silver) 910/950") {
                route.long_name = Some("J 910/950".to_string())
            }
        }

        gtfs.routes = gtfs
            .routes
            .into_iter()
            //remove anything from the route id that is the hyphen - or after
            .map(|(route_id, mut route)| {
                if let Some(hyphen_index) = route_id.find("-") {
                    route.id = route_id[0..hyphen_index].to_string();
                }
                (route.id.clone(), route)
            })
            .collect();

        //apply the same to the route id in the trip

        gtfs.trips = gtfs
            .trips
            .into_iter()
            //remove anything from the route id that is the hyphen - or after
            .map(|(trip_id, mut trip)| {
                if let Some(hyphen_index) = trip.route_id.find("-") {
                    trip.route_id = trip.route_id[0..hyphen_index].to_string();
                }
                (trip_id, trip)
            })
            .collect();
    }

    if feed_id == "f-9q5-metro~losangeles~rail" {
        for (trip_id, trip) in gtfs.trips.iter_mut() {
            for stop_t in trip.stop_times.iter_mut() {
                if let Some(headsign) = &mut stop_t.stop_headsign {
                    if headsign.contains("-") {
                        let split = headsign.split("-").collect::<Vec<&str>>();

                        *headsign = split[1].to_string();
                    }
                }
            }
        }
    }

    let today = chrono::Utc::now().naive_utc().date();

    let number_of_days = match feed_id {
        "f-gtfs~de" => 7,
        _ => 20,
    };

    let gtfs = minimum_day_filter(gtfs, today - chrono::Duration::days(number_of_days));

    println!(
        "Finished reading GTFS for {}, took {:?}",
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

    let default_lang = match gtfs.feed_info.len() {
        0 => match gtfs.agencies.len() {
            0 => Some(String::from("en")),
            _ => gtfs.agencies[0].lang.clone(),
        },
        _ => gtfs.feed_info[0].default_lang.clone(),
    };

    let mut gtfs_summary = GtfsSummary {
        feed_start_date: None,
        feed_end_date: None,
        languages_avaliable: HashSet::new(),
        default_lang: default_lang.clone(),
        general_timezone: match gtfs.agencies.len() {
            0 => String::from("Etc/UTC"),
            _ => gtfs.agencies[0].timezone.clone(),
        },
        bbox: None,
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
    use catenary::schema::gtfs::agencies::dsl::agencies;

    let agency_rows: Vec<catenary::models::Agency> = gtfs
        .agencies
        .iter()
        .filter_map(|agency| {
            // agency id duplication check, might need better handling depending on data
            if agency.id.is_some() {
                Some(catenary::models::Agency {
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
                })
            } else {
                eprintln!("Warning: Agency found without an ID: {:?}", agency);
                None
            }
        })
        .collect();

    if !agency_rows.is_empty() {
        diesel::insert_into(agencies)
            .values(agency_rows)
            .execute(conn)
            .await?;
    }

    println!("Agency insertion done for {}", feed_id);

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
    stops_into_postgres_and_elastic(
        &gtfs,
        feed_id,
        Arc::clone(&arc_conn_pool),
        chateau_id,
        attempt_id,
        &stop_ids_to_route_types,
        &stop_ids_to_route_ids,
        &stop_id_to_children_ids,
        &stop_ids_to_children_route_types,
        gtfs_translations.as_ref(),
        &default_lang,
        &elasticclient,
    )
    .await?;

    println!("Stops inserted for {}", feed_id);

    // insert trip and itineraries

    println!("Inserting directions for {}", feed_id);

    for group in &reduction.direction_patterns.iter().chunks(20000) {
        let mut d_final: Vec<DirectionPatternMeta> = vec![];

        let mut d_rows: Vec<Vec<DirectionPatternRow>> = vec![];

        for (direction_pattern_id, direction_pattern) in group {
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
                    .filter(|stop| {
                        stop.latitude.is_some() && stop.longitude.is_some()
                            && 
                            //not within 1 degree of null is_null_island
                            !(
                                stop.latitude.unwrap() > -1.0
                                    && stop.latitude.unwrap() < 1.0
                                    && stop.longitude.unwrap() > -1.0
                                    && stop.longitude.unwrap() < 1.0
                            )
                    })
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
                    let linestring: postgis_diesel::types::LineString<
                        postgis_diesel::types::Point,
                    > = postgis_diesel::types::LineString {
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
                headsign_or_destination: match chateau_id {
                    "santacruzmetro" => match &direction_pattern.stop_headsigns_unique_list {
                        Some(list) => list.join(","),
                        _ => itin_pattern
                            .stop_sequences
                            .last()
                            .and_then(|x| x.stop_headsign.clone())
                            .unwrap_or_else(|| "".to_string()),
                    },
                    "montebellobuslines" => gtfs
                        .stops
                        .get(itin_pattern.stop_sequences.last().unwrap().stop_id.as_str())
                        .as_deref()
                        .unwrap()
                        .name
                        .clone()
                        .unwrap_or_default(),
                    _ => direction_pattern
                        .headsign_or_destination
                        .clone()
                        .unwrap_or_else(|| "".to_string()),
                },
                gtfs_shape_id: Some(gtfs_shape_id.clone()),
                fake_shape: direction_pattern.gtfs_shape_id.is_none(),
                onestop_feed_id: feed_id.to_string(),
                attempt_id: attempt_id.to_string(),
                route_id: Some(itin_pattern.route_id.clone()),
                route_type: Some(itin_pattern.route_type),
                direction_id: itin_pattern.direction_id,
                stop_headsigns_unique_list: itin_pattern.stop_headsigns_unique_list.as_ref().map(
                    |x| {
                        x.into_iter()
                            .map(|x| Some(x.to_string()))
                            .collect::<Vec<Option<String>>>()
                    },
                ),
            };

            d_final.push(direction_pattern_meta);

            //insert stop list into DirectionPatternRow

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
                    stop_headsign_idx: match &stop_time.stop_headsign {
                        Some(this_stop_headsign) => direction_pattern
                            .stop_headsigns_unique_list
                            .as_ref()
                            .map(|direction_headsigns| {
                                direction_headsigns
                                    .iter()
                                    .position(|x| x == this_stop_headsign)
                                    .map(|x| x as i16)
                            })
                            .flatten(),
                        None => None,
                    },
                })
                .collect();

            for dir_chunk in direction_pattern_rows.chunks(1000) {
                d_rows.push(dir_chunk.to_vec());
            }
        }

        conn.build_transaction()
            .run::<(), diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    for dir_chunk in d_final.chunks(100) {
                        diesel::insert_into(
                            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta,
                        )
                        .values(dir_chunk)
                        .execute(conn)
                        .await?;
                    }
    
                    for dir_chunk in d_rows {
                        diesel::insert_into(
                            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern,
                        )
                        .values(dir_chunk)
                        .execute(conn)
                        .await?;
                    }
    
                    Ok(())
                })
            })
            .await?;
    }

    println!("Directions inserted for {}", feed_id);
    println!("Inserting itineraries for {}", feed_id);
    for group in &reduction.itineraries.iter().chunks(20000) {
        let mut t_final: Vec<catenary::models::ItineraryPatternMeta> = vec![];
        let mut t_rows: Vec<Vec<catenary::models::ItineraryPatternRow>> = vec![];

        for (itinerary_id, itinerary) in group {
            let itinerary_pg_meta = ItineraryPatternMeta {
                onestop_feed_id: feed_id.to_string(),
                chateau: chateau_id.to_string(),
                attempt_id: attempt_id.to_string(),
                timezone: itinerary.timezone.clone(),
                trip_headsign: match chateau_id {
                    "santacruzmetro" => match &itinerary.stop_headsigns_unique_list {
                        None => itinerary
                            .stop_sequences
                            .last()
                            .and_then(|x| x.stop_headsign.clone()),
                        Some(list) => Some(list.join(",")),
                    },
                    "montebellobuslines" => gtfs
                        .stops
                        .get(itinerary.stop_sequences.last().unwrap().stop_id.as_str())
                        .as_deref()
                        .unwrap()
                        .name
                        .clone(),
                    _ => itinerary
                        .trip_headsign
                        .clone()
                        .map(|x| x.replace(" - Funded in part by/SB County Measure A", "")),
                },
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

            t_final.push(itinerary_pg_meta);

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
                    timepoint: stop_sequence.timepoint.into(),
                    stop_headsign_idx: match stop_sequence.stop_headsign {
                        Some(ref stop_headsign) => {
                            itinerary.stop_headsigns_unique_list.as_ref().and_then(|x| {
                                x.iter().position(|x| x == stop_headsign).map(|x| x as i16)
                            })
                        }
                        None => None,
                    },
                })
                .collect::<Vec<_>>();

            for itinerary_chunk in itinerary_pg.chunks(1000) {
                t_rows.push(itinerary_chunk.to_vec());
            }
        }

        conn.build_transaction()
            .run::<(), diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    for itinerary_chunk in t_final.chunks(1000) {
                        diesel::insert_into(
                            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta,
                        )
                        .values(itinerary_chunk)
                        .execute(conn)
                        .await?;
                    }

                    for itinerary_chunk in t_rows {
                        diesel::insert_into(
                            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern,
                        )
                        .values(&itinerary_chunk)
                        .execute(conn)
                        .await?;
                    }

                    Ok(())
                })
            })
            .await?;
    }

    println!("Itineraries inserted for {}", feed_id);

    //insert trips

    println!("Inserting trips for {}", feed_id);

    for group in &reduction.itineraries_to_trips.iter().chunks(15000) {
        let mut t_final: Vec<Vec<catenary::models::CompressedTrip>> = vec![];
        for (itinerary_id, compressed_trip_list) in group {
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
                    trip_short_name: compressed_trip_raw.trip_short_name.clone().map(|x| {
                        CompactString::from(
                            regex_train_starting.replace(x.as_str(), "").to_string(),
                        )
                    }),
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

            for trip_chunk in trip_pg.chunks(1000) {
                t_final.push(trip_chunk.to_vec());
            }
        }

        conn.build_transaction()
            .run::<(), diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    for trip_chunk in t_final {
                        diesel::insert_into(
                            catenary::schema::gtfs::trips_compressed::dsl::trips_compressed,
                        )
                        .values(trip_chunk)
                        .execute(conn)
                        .await?;
                    }

                    Ok(())
                })
            })
            .await?;
    }

    //insert routes

    println!("Inserting routes for {}", feed_id);

    let routes_pg: Vec<RoutePgModel> = gtfs
        .routes
        .iter()
        .map(|(route_id, route)| {
            let colour = fix_background_colour_rgb_feed_route(
                feed_id,
                route
                    .color
                    .unwrap_or_else(|| colour_correction::DEFAULT_BACKGROUND),
                route,
            );
            let text_colour = fix_foreground_colour_rgb_feed(
                feed_id,
                route
                    .color
                    .unwrap_or_else(|| colour_correction::DEFAULT_BACKGROUND),
                route.text_color.unwrap_or_else(|| RGB::new(0, 0, 0)),
            );

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

    conn.build_transaction()
        .run::<(), diesel::result::Error, _>(|conn| {
            Box::pin(async move {
                for route_chunk in routes_pg.chunks(50) {
                    diesel::insert_into(catenary::schema::gtfs::routes::dsl::routes)
                        .values(route_chunk)
                        .execute(conn)
                        .await?;
                }
                Ok(())
            })
        })
        .await?;

    println!("Routes inserted for {}", feed_id);

    //calculate concave hull
    let hull = crate::gtfs_handlers::hull_from_gtfs::hull_from_gtfs(&gtfs);

    let bbox = hull.as_ref().map(|hull| hull.bounding_rect()).flatten();

    gtfs_summary.bbox = bbox;

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
            rings: vec![
                polygon_geo
                    .into_inner()
                    .0
                    .into_iter()
                    .map(|coord| {
                        postgis_diesel::types::Point::new(
                            coord.x,
                            coord.y,
                            Some(catenary::WGS_84_SRID),
                        )
                    })
                    .collect(),
            ],
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
