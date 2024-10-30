// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

extern crate catenary;
use ahash::{AHashMap, AHashSet};
use catenary::aspen_dataset::*;
use catenary::postgres_tools::CatenaryPostgresPool;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use gtfs_realtime::FeedMessage;
use scc::HashMap as SccHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use crate::metrolink_california_additions::vehicle_pos_supplement;

const MAKE_VEHICLES_FEED_LIST: [&str; 9] = [
    "f-mta~nyc~rt~subway~1~2~3~4~5~6~7",
    "f-mta~nyc~rt~subway~a~c~e",
    "f-mta~nyc~rt~subway~b~d~f~m",
    "f-mta~nyc~rt~subway~g",
    "f-mta~nyc~rt~subway~j~z",
    "f-mta~nyc~rt~subway~l",
    "f-mta~nyc~rt~subway~n~q~r~w",
    "f-mta~nyc~rt~subway~sir",
    "f-bart~rt",
];

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkTrackData {
    #[serde(rename = "TrainDesignation")]
    train_designation: String,
    #[serde(rename = "RouteCode")]
    route_code: String,
    #[serde(rename = "PlatformName")]
    platform_name: String,
    #[serde(rename = "EventType")]
    event_type: String,
    #[serde(rename = "FormattedTrackDesignation")]
    formatted_track_designation: String,
    #[serde(rename = "TrainMovementTime")]
    train_movement_time: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkTrackDataCleaned {
    track_movement_time_arrival: Option<u64>,
    track_movement_time_departure: Option<u64>,
    stop_id: String,
    formatted_track_designation: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum MetrolinkEventType {
    Arrival,
    Departure,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkOutputTrackData {
    //cleaned 3 digit trip number -> stop_id -> MetrolinkTrackDataCleaned
    track_lookup: HashMap<String, HashMap<String, MetrolinkTrackDataCleaned>>,
}

#[derive(Clone, Debug)]
pub enum TrackData {
    //output Option<MetrolinkOutputTrackData> instead
    Metrolink(Option<MetrolinkOutputTrackData>),
    None,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkPosRaw {
    symbol: CompactString,
    direction: CompactString,
    lat: CompactString,
    long: CompactString,
    speed: CompactString,
    line: CompactString,
    ptc_time: CompactString,
    ptc_status: CompactString,
    delay_status: CompactString,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkPos {
    lat: f32,
    lon: f32,
    speed: f32,
    symbol: CompactString,
}

fn mph_to_mps(mph: &CompactString) -> Option<f32> {
    let mph: f32 = match mph.parse() {
        Ok(mph) => mph,
        Err(_) => return None,
    };

    Some(mph * 0.44704)
}

fn metrlink_coord_to_f32(coord: &CompactString) -> Option<f32> {
    //Split into 3 parts based on :
    let parts: Vec<&str> = coord.split(':').collect();

    if parts.len() != 3 {
        return None;
    }

    let degrees: f32 = match parts[0].parse() {
        Ok(degrees) => degrees,
        Err(_) => return None,
    };

    let sign = if degrees < 0. { -1.0 } else { 1.0 };

    let degrees = degrees.abs();

    let minutes: f32 = match parts[1].parse() {
        Ok(minutes) => minutes,
        Err(_) => return None,
    };

    let seconds: f32 = match parts[2].parse() {
        Ok(seconds) => seconds,
        Err(_) => return None,
    };

    let mut decimal = degrees + minutes / 60.0 + seconds / 3600.0;

    decimal = decimal * sign;

    Some(decimal)
}

pub async fn new_rt_data(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    authoritative_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    chateau_id: String,
    realtime_feed_id: String,
    has_vehicles: bool,
    has_trips: bool,
    has_alerts: bool,
    vehicles_response_code: Option<u16>,
    trips_response_code: Option<u16>,
    alerts_response_code: Option<u16>,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let start = std::time::Instant::now();

    let fetch_supplemental_data_positions_metrolink: Option<AHashMap<CompactString, MetrolinkPos>> =
        match realtime_feed_id.as_str() {
            "f-metrolinktrains~rt" => {
                let raw_data_req =
                    reqwest::get("https://rtt.metrolinktrains.com/trainlist.json").await;

                match raw_data_req {
                    Ok(metrolink_data) => {
                        let metrolink_data = metrolink_data.json::<Vec<MetrolinkPosRaw>>().await;

                        match metrolink_data {
                            Ok(metrolink_data) => {
                                let mut metrolink_positions: AHashMap<CompactString, MetrolinkPos> =
                                    AHashMap::new();

                                for pos in metrolink_data {
                                    let lat = metrlink_coord_to_f32(&pos.lat);
                                    let lon = metrlink_coord_to_f32(&pos.long);
                                    let speed = mph_to_mps(&pos.speed);

                                    if let (Some(lat), Some(lon), Some(speed)) = (lat, lon, speed) {
                                        metrolink_positions.insert(
                                            pos.symbol.clone(),
                                            MetrolinkPos {
                                                lat,
                                                lon,
                                                speed,
                                                symbol: pos.symbol.clone(),
                                            },
                                        );
                                    }
                                }

                                println!("Got {} metrolink positions", metrolink_positions.len());

                                Some(metrolink_positions)
                            }
                            Err(e) => {
                                println!("Error fetching metrolink data: {}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error fetching metrolink data, could not connect: {}", e);
                        None
                    }
                }
            }
            _ => None,
        };

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    let fetched_track_data: TrackData = fetch_track_data(&chateau_id).await;

    //println!("Forming pg connection");
    let conn = &mut conn_pre?;
    //println!("Connected to postges");

    // take all the gtfs rt data and merge it together

    let mut aspenised_vehicle_positions: AHashMap<String, AspenisedVehiclePosition> =
        AHashMap::new();
    let mut gtfs_vehicle_labels_to_ids: AHashMap<String, String> = AHashMap::new();
    let mut vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache> = AHashMap::new();
    let mut trip_updates: AHashMap<CompactString, AspenisedTripUpdate> = AHashMap::new();
    let mut trip_updates_lookup_by_trip_id_to_trip_update_ids: AHashMap<
        CompactString,
        Vec<CompactString>,
    > = AHashMap::new();

    //let alerts hashmap
    let mut alerts: AHashMap<String, AspenisedAlert> = AHashMap::new();

    let mut impacted_route_id_to_alert_ids: AHashMap<String, Vec<String>> = AHashMap::new();
    let impacted_stop_id_to_alert_ids: AHashMap<String, Vec<String>> = AHashMap::new();
    let mut impact_trip_id_to_alert_ids: AHashMap<String, Vec<String>> = AHashMap::new();
    let general_alerts: AHashMap<String, Vec<String>> = AHashMap::new();

    use catenary::schema::gtfs::chateaus as chateaus_pg_schema;
    use catenary::schema::gtfs::routes as routes_pg_schema;

    //get this chateau
    let start_chateau_query = Instant::now();
    let this_chateau = chateaus_pg_schema::dsl::chateaus
        .filter(chateaus_pg_schema::dsl::chateau.eq(&chateau_id))
        .first::<catenary::models::Chateau>(conn)
        .await?;
    let chateau_elapsed = start_chateau_query.elapsed();

    //get all routes inside chateau from postgres db
    //: Vec<catenary::models::Route>

    let start_routes_query = Instant::now();
    let routes: Vec<catenary::models::Route> = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await?;
    let routes_query_elapsed = start_routes_query.elapsed();

    //let (this_chateau, routes) = tokio::try_join!(this_chateau, routes)?;

    if routes.is_empty() {
        println!("No routes found for chateau {}", chateau_id);
    }

    let mut route_id_to_route: HashMap<String, catenary::models::Route> = HashMap::new();

    for route in routes {
        route_id_to_route.insert(route.route_id.clone(), route);
    }

    let route_id_to_route = route_id_to_route;

    //combine them together and insert them with the vehicles positions

    // trips can be left fairly raw for now, with a lot of data references

    // ignore alerts for now, as well as trip modifications

    //collect all trip ids that must be looked up
    //collect all common itinerary patterns and look those up

    let mut trip_ids_to_lookup: AHashSet<String> = AHashSet::new();

    for realtime_feed_id in this_chateau.realtime_feeds.iter().flatten() {
        if let Some(vehicle_gtfs_rt_for_feed_id) =
            authoritative_gtfs_rt.get(&(realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
        {
            let vehicle_gtfs_rt_for_feed_id = vehicle_gtfs_rt_for_feed_id.get();

            for vehicle_entity in vehicle_gtfs_rt_for_feed_id.entity.iter() {
                if let Some(vehicle_pos) = &vehicle_entity.vehicle {
                    if let Some(trip) = &vehicle_pos.trip {
                        if let Some(trip_id) = &trip.trip_id {
                            trip_ids_to_lookup.insert(trip_id.clone());
                        }
                    }
                }
            }
        }

        //trips updates trip id lookup
        if let Some(trip_gtfs_rt_for_feed_id) =
            authoritative_gtfs_rt.get(&(realtime_feed_id.clone(), GtfsRtType::TripUpdates))
        {
            let trip_gtfs_rt_for_feed_id = trip_gtfs_rt_for_feed_id.get();

            for trip_entity in trip_gtfs_rt_for_feed_id.entity.iter() {
                if let Some(trip_update) = &trip_entity.trip_update {
                    if let Some(trip_id) = &trip_update.trip.trip_id {
                        trip_ids_to_lookup.insert(trip_id.clone());
                    }
                }
            }
        }

        //now do the same for alerts updates
        if let Some(alert_gtfs_rt_for_feed_id) =
            authoritative_gtfs_rt.get(&(realtime_feed_id.clone(), GtfsRtType::Alerts))
        {
            let alert_gtfs_rt_for_feed_id = alert_gtfs_rt_for_feed_id.get();

            for alert_entity in alert_gtfs_rt_for_feed_id.entity.iter() {
                if let Some(alert) = &alert_entity.alert {
                    for entity in alert.informed_entity.iter() {
                        if let Some(trip) = &entity.trip {
                            if let Some(trip_id) = &trip.trip_id {
                                trip_ids_to_lookup.insert(trip_id.clone());
                            }
                        }
                    }
                }
            }
        }

        //now look up all the trips

        let trip_start = std::time::Instant::now();
        let trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(&chateau_id))
            .filter(
                catenary::schema::gtfs::trips_compressed::dsl::trip_id
                    .eq_any(trip_ids_to_lookup.iter()),
            )
            .load::<catenary::models::CompressedTrip>(conn)
            .await?;
        let trip_duration = trip_start.elapsed();

        let mut trip_id_to_trip: AHashMap<String, catenary::models::CompressedTrip> =
            AHashMap::new();

        for trip in trips {
            trip_id_to_trip.insert(trip.trip_id.clone(), trip);
        }

        let trip_id_to_trip = trip_id_to_trip;

        //also lookup all the headsigns from the trips via itinerary patterns

        let mut list_of_itinerary_patterns_to_lookup: AHashSet<String> = AHashSet::new();

        for trip in trip_id_to_trip.values() {
            list_of_itinerary_patterns_to_lookup.insert(trip.itinerary_pattern_id.clone());
        }

        let itin_lookup_start = std::time::Instant::now();

        let itinerary_patterns =
            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::chateau.eq(&chateau_id),
                )
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_id
                        .eq_any(list_of_itinerary_patterns_to_lookup.iter()),
                )
                .select(catenary::models::ItineraryPatternMeta::as_select())
                .load::<catenary::models::ItineraryPatternMeta>(conn)
                .await?;

        let itin_lookup_duration = itin_lookup_start.elapsed();

        let mut itinerary_pattern_id_to_itinerary_pattern_meta: AHashMap<
            String,
            catenary::models::ItineraryPatternMeta,
        > = AHashMap::new();

        for itinerary_pattern in itinerary_patterns {
            itinerary_pattern_id_to_itinerary_pattern_meta.insert(
                itinerary_pattern.itinerary_pattern_id.clone(),
                itinerary_pattern,
            );
        }

        let itinerary_pattern_id_to_itinerary_pattern_meta =
            itinerary_pattern_id_to_itinerary_pattern_meta;

        let mut route_ids_to_insert = AHashSet::new();

        for realtime_feed_id in this_chateau.realtime_feeds.iter().flatten() {
            if let Some(vehicle_gtfs_rt_for_feed_id) =
                authoritative_gtfs_rt.get(&(realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
            {
                let vehicle_gtfs_rt_for_feed_id = vehicle_gtfs_rt_for_feed_id.get();

                for vehicle_entity in vehicle_gtfs_rt_for_feed_id.entity.iter() {
                    if let Some(vehicle_pos) = &vehicle_entity.vehicle {
                        let recalculate_route_id: Option<String> = match &vehicle_pos.trip {
                            Some(trip) => match &trip.trip_id {
                                Some(trip_id) => {
                                    let compressed_trip = trip_id_to_trip.get(trip_id);
                                    match compressed_trip {
                                        Some(compressed_trip) => {
                                            let route =
                                                route_id_to_route.get(&compressed_trip.route_id);
                                            match route {
                                                Some(route) => Some(route.route_id.clone()),
                                                None => trip.route_id.clone(),
                                            }
                                        }
                                        None => trip.route_id.clone(),
                                    }
                                }
                                None => trip.route_id.clone(),
                            },
                            None => None,
                        };

                        let pos_aspenised = AspenisedVehiclePosition {
                            trip: vehicle_pos.trip.as_ref().map(|trip| {
                                AspenisedVehicleTripInfo {
                                    trip_id: trip.trip_id.clone(),
                                    direction_id: trip.direction_id,
                                   start_date: trip.start_date.clone(),
                                   start_time: trip.start_time.clone(),
                                    schedule_relationship: trip.schedule_relationship,
                                    route_id: recalculate_route_id.clone(),
                                    trip_headsign: match &trip.trip_id {
                                            Some(trip_id) => {
                                                let trip = trip_id_to_trip.get(&trip_id.clone());
                                                match trip {
                                                    Some(trip) => {
                                                        let itinerary_pattern = itinerary_pattern_id_to_itinerary_pattern_meta.get(&trip.itinerary_pattern_id);
                                                        match itinerary_pattern {
                                                            Some(itinerary_pattern) => {
                                                                itinerary_pattern.trip_headsign.clone()
                                                            },
                                                            None => None
                                                        }
                                                    },
                                                    None => None
                                                }
                                            },
                                            None => None
                                        }.map(|headsign| headsign.replace("-Exact Fare", "").replace(" - Funded in part by/SB County Measure A", "")),
                                    trip_short_name: match &trip.trip_id {
                                        Some(trip_id) => {
                                            let trip = trip_id_to_trip.get(&trip_id.clone());
                                            match trip {
                                                Some(trip) => {
                                                    trip.trip_short_name.as_ref().map(|x| x.to_string())
                                                },
                                                None => None
                                            }
                                        },
                                        None => None
                                    }
                                }
                            }),
                            position: vehicle_pos.position.as_ref().map(|position| CatenaryRtVehiclePosition {
                                latitude: position.latitude,
                                longitude: position.longitude,
                                bearing: position.bearing,
                                odometer: position.odometer,
                                speed: position.speed,
                                }),
                            timestamp: vehicle_pos.timestamp,
                            vehicle: vehicle_pos.vehicle.as_ref().map(|vehicle| AspenisedVehicleDescriptor {
                                id: vehicle.id.clone(),
                                label: match realtime_feed_id.as_str() {
                                    "f-trimet~rt" => vehicle.id.clone(),
                                    _ => vehicle.label.clone()
                                },
                                license_plate: vehicle.license_plate.clone(),
                                wheelchair_accessible: vehicle.wheelchair_accessible,
                                }),
                            route_type: match realtime_feed_id.as_str() {
                                "f-mta~nyc~rt~lirr" => 2,
                                "f-mta~nyc~rt~mnr" => 2,
                                "f-amtrak~rt" => 2,
                                _ => match &vehicle_pos.trip {
                                    Some(trip) => match &recalculate_route_id {
                                        Some(route_id) => {
                                            let route = route_id_to_route.get(route_id);
                                            match route {
                                                Some(route) => route.route_type,
                                                None => 3
                                            }
                                        },
                                        None => 3
                                    },
                                    None => 3
                                }
                            },
                            current_status: vehicle_pos.current_status,
                            current_stop_sequence: vehicle_pos.current_stop_sequence,
                            occupancy_status: vehicle_pos.occupancy_status,
                            occupancy_percentage: vehicle_pos.occupancy_percentage,
                            congestion_level: vehicle_pos.congestion_level
                        };

                        let pos_aspenised = vehicle_pos_supplement(
                            pos_aspenised,
                            &fetch_supplemental_data_positions_metrolink,
                            &chateau_id,
                        );

                        aspenised_vehicle_positions
                            .insert(vehicle_entity.id.clone(), pos_aspenised);

                        //insert the route cache

                        if let Some(trip) = &vehicle_pos.trip {
                            if let Some(route_id) = &trip.route_id {
                                route_ids_to_insert.insert(route_id.clone());
                            }

                            if let Some(trip_id) = &trip.trip_id {
                                let trip = trip_id_to_trip.get(trip_id);
                                if let Some(trip) = &trip {
                                    route_ids_to_insert.insert(trip.route_id.clone());
                                }
                            }
                        }
                    }
                }
            }

            //process trip updates
            if let Some(trip_updates_gtfs_rt_for_feed_id) =
                authoritative_gtfs_rt.get(&(realtime_feed_id.clone(), GtfsRtType::TripUpdates))
            {
                let trip_updates_gtfs_rt_for_feed_id = trip_updates_gtfs_rt_for_feed_id.get();

                //let make_supplimental_db = get_supplemental_data

                for trip_update_entity in trip_updates_gtfs_rt_for_feed_id.entity.iter() {
                    if let Some(trip_update) = &trip_update_entity.trip_update {
                        let trip_id = trip_update.trip.trip_id.clone();

                        let compressed_trip = match &trip_id {
                            Some(trip_id) => trip_id_to_trip.get(trip_id),
                            None => None,
                        };

                        let trip_update = AspenisedTripUpdate {
                            trip: trip_update.trip.clone().into(),
                            vehicle: trip_update.vehicle.clone().map(|x| x.into()),
                            trip_headsign: None,
                            stop_time_update: trip_update
                                .stop_time_update
                                .iter()
                                .map(|stu| AspenisedStopTimeUpdate {
                                    stop_sequence: stu.stop_sequence,
                                    stop_id: stu.stop_id.as_ref().map(|x| x.into()),
                                    arrival: stu.arrival.clone().map(|arrival| {
                                        AspenStopTimeEvent {
                                            delay: None,
                                            time: arrival.time,
                                            uncertainty: arrival.uncertainty,
                                        }
                                    }),
                                    departure: stu.departure.clone().map(|departure| {
                                        AspenStopTimeEvent {
                                            delay: None,
                                            time: departure.time,
                                            uncertainty: departure.uncertainty,
                                        }
                                    }),
                                    platform_string: match chateau_id.as_str() {
                                        "metrolinktrains" => {
                                            let mut track_resp = None;

                                            if let TrackData::Metrolink(Some(track_data_scax)) =
                                                &fetched_track_data
                                            {
                                                let mut metrolink_code = String::from("M");

                                                if let Some(compressed_trip) = compressed_trip {
                                                    if let Some(trip_short_name) =
                                                        compressed_trip.trip_short_name.as_ref()
                                                    {
                                                        metrolink_code.push_str(trip_short_name);
                                                    }
                                                }

                                                if let Some(train_data) = track_data_scax
                                                    .track_lookup
                                                    .get(&metrolink_code)
                                                {
                                                    if let Some(stop_id) = &stu.stop_id {
                                                        if let Some(train_and_stop_scax) =
                                                            train_data.get(stop_id)
                                                        {
                                                            track_resp = Some(
                                                                train_and_stop_scax
                                                                    .formatted_track_designation
                                                                    .clone()
                                                                    .replace("Platform ", ""),
                                                            );
                                                        }
                                                    }
                                                }
                                            }

                                            track_resp
                                        }
                                        _ => None,
                                    },
                                    schedule_relationship: stu.schedule_relationship,
                                    departure_occupancy_status: stu.departure_occupancy_status,
                                    stop_time_properties: stu
                                        .stop_time_properties
                                        .clone()
                                        .map(|x| x.into()),
                                })
                                .collect(),
                            timestamp: trip_update.timestamp,
                            delay: trip_update.delay,
                            trip_properties: trip_update.trip_properties.clone().map(|x| x.into()),
                        };

                        if trip_id.is_some() {
                            trip_updates_lookup_by_trip_id_to_trip_update_ids
                                .entry(trip_id.as_ref().unwrap().into())
                                .or_insert(vec![CompactString::new(&trip_update_entity.id)]);
                        }

                        trip_updates
                            .insert(CompactString::new(&trip_update_entity.id), trip_update);
                    }
                }
            }

            if let Some(alert_updates_gtfs_rt) =
                authoritative_gtfs_rt.get(&(realtime_feed_id.clone(), GtfsRtType::Alerts))
            {
                let alert_updates_gtfs_rt = alert_updates_gtfs_rt.get();

                for alert_entity in alert_updates_gtfs_rt.entity.iter() {
                    if let Some(alert) = &alert_entity.alert {
                        let alert_id = alert_entity.id.clone();

                        let alert: AspenisedAlert = alert.clone().into();

                        for informed_entity in alert.informed_entity.iter() {
                            if let Some(route_id) = &informed_entity.route_id {
                                impacted_route_id_to_alert_ids
                                    .entry(route_id.clone())
                                    .and_modify(|x| x.push(alert_id.clone()))
                                    .or_insert(vec![alert_id.clone()]);
                            }

                            if let Some(trip) = &informed_entity.trip {
                                if let Some(trip_id) = &trip.trip_id {
                                    impact_trip_id_to_alert_ids
                                        .entry(trip_id.clone())
                                        .and_modify(|x| x.push(alert_id.clone()))
                                        .or_insert(vec![alert_id.clone()]);
                                }

                                if let Some(route_id) = &trip.route_id {
                                    impacted_route_id_to_alert_ids
                                        .entry(route_id.clone())
                                        .and_modify(|x| x.push(alert_id.clone()))
                                        .or_insert(vec![alert_id.clone()]);
                                }
                            }
                        }

                        alerts.insert(alert_id.clone(), alert);
                    }
                }
            }
        }

        //insert the route cache

        for route_id in route_ids_to_insert.iter() {
            let route = route_id_to_route.get(&route_id.clone());
            if let Some(route) = route {
                vehicle_routes_cache.insert(
                    route.route_id.clone(),
                    AspenisedVehicleRouteCache {
                        route_desc: route.gtfs_desc.clone(),
                        route_short_name: route.short_name.clone(),
                        route_long_name: route.long_name.clone(),
                        route_type: route.route_type,
                        route_colour: route.color.clone(),
                        route_text_colour: route.text_color.clone(),
                    },
                );
            }
        }

        //vehicle labels to gtfs ids

        for (key, vehicle_gtfs) in aspenised_vehicle_positions.iter() {
            if let Some(vehicle_data) = &vehicle_gtfs.vehicle {
                if let Some(label) = &vehicle_data.label {
                    gtfs_vehicle_labels_to_ids.insert(label.clone(), key.clone());
                }
            }
        }
    }

    //Insert data back into process-wide authoritative_data_store

    match authoritative_data_store.entry(chateau_id.clone()) {
        scc::hash_map::Entry::Occupied(mut oe) => {
            let mut data = oe.get_mut();
            *data = AspenisedData {
                vehicle_positions: aspenised_vehicle_positions,
                vehicle_routes_cache: vehicle_routes_cache,
                trip_updates: trip_updates,
                trip_updates_lookup_by_trip_id_to_trip_update_ids:
                    trip_updates_lookup_by_trip_id_to_trip_update_ids,
                aspenised_alerts: alerts,
                impacted_routes_alerts: impacted_route_id_to_alert_ids,
                impacted_stops_alerts: AHashMap::new(),
                vehicle_label_to_gtfs_id: gtfs_vehicle_labels_to_ids,
                impacted_trips_alerts: impact_trip_id_to_alert_ids,
                last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
            }
        }
        scc::hash_map::Entry::Vacant(ve) => {
            ve.insert_entry(AspenisedData {
                vehicle_positions: aspenised_vehicle_positions,
                vehicle_routes_cache: vehicle_routes_cache,
                trip_updates: trip_updates,
                trip_updates_lookup_by_trip_id_to_trip_update_ids:
                    trip_updates_lookup_by_trip_id_to_trip_update_ids,
                aspenised_alerts: alerts,
                impacted_routes_alerts: impacted_route_id_to_alert_ids,
                impacted_stops_alerts: AHashMap::new(),
                vehicle_label_to_gtfs_id: gtfs_vehicle_labels_to_ids,
                impacted_trips_alerts: impact_trip_id_to_alert_ids,
                last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
            });
        }
    }

    println!("Updated Chateau {}", chateau_id,);

    Ok(true)
}

pub async fn fetch_track_data(chateau_id: &str) -> TrackData {
    match chateau_id {
        "metrolinktrains" => {
            let url = "https://rtt.metrolinktrains.com/StationScheduleList.json";

            match reqwest::get(url).await {
                Ok(r) => {
                    let response = r.json::<Vec<MetrolinkTrackData>>().await;

                    println!("{:?}", response);

                    match response {
                        Ok(response) => {
                            let mut track_lookup: HashMap<
                                String,
                                HashMap<String, MetrolinkTrackDataCleaned>,
                            > = HashMap::new();

                            for t in response {
                                if !track_lookup.contains_key(&t.train_designation) {
                                    track_lookup
                                        .insert(t.train_designation.clone(), HashMap::new());
                                }

                                let mut train_lookup_entry =
                                    track_lookup.get_mut(&t.train_designation).unwrap();

                                let stop_id_find =
                                    catenary::metrolink_ptc_to_stop_id::METROLINK_STOP_LIST
                                        .iter()
                                        .find(|x| x.1 == &t.platform_name);

                                if let Some((stop_id, _)) = stop_id_find {
                                    if !train_lookup_entry.contains_key(*stop_id) {
                                        train_lookup_entry.insert(
                                            stop_id.to_string(),
                                            MetrolinkTrackDataCleaned {
                                                track_movement_time_arrival: None,
                                                track_movement_time_departure: None,
                                                stop_id: stop_id.to_string(),
                                                formatted_track_designation: t
                                                    .formatted_track_designation
                                                    .clone(),
                                            },
                                        );
                                    }

                                    let train_and_stop_entry =
                                        train_lookup_entry.get_mut(&stop_id.to_string()).unwrap();

                                    match t.event_type.as_str() {
                                        "Arrival" => {
                                            train_and_stop_entry.track_movement_time_arrival =
                                                Some(catenary::metrolink_unix_fix(
                                                    &t.train_movement_time,
                                                ));
                                        }
                                        "Departure" => {
                                            train_and_stop_entry.track_movement_time_departure =
                                                Some(catenary::metrolink_unix_fix(
                                                    &t.train_movement_time,
                                                ));
                                        }
                                        _ => {}
                                    }
                                }
                            }

                            TrackData::Metrolink(Some(MetrolinkOutputTrackData {
                                track_lookup: track_lookup,
                            }))
                        }
                        Err(e) => {
                            println!("Error decoding Metrolink data: {}", e);
                            TrackData::Metrolink(None)
                        }
                    }
                }
                Err(e) => {
                    println!("Error fetching Metrolink data: {}", e);
                    TrackData::Metrolink(None)
                }
            }
        }
        _ => TrackData::None,
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_fetch_track_data() {
        let track_data = super::fetch_track_data("metrolinktrains").await;
        match track_data {
            super::TrackData::Metrolink(m_data) => {
                assert!(m_data.is_some());
            }
            _ => panic!("Expected Metrolink data"),
        }
    }
}
