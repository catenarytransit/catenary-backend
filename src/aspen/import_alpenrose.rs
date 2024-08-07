// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

extern crate catenary;
use ahash::{AHashMap, AHashSet};
use catenary::aspen_dataset::*;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use gtfs_rt::FeedMessage;
use scc::HashMap as SccHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

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
    event_type: String,
    #[serde(rename = "FormattedTrackDesignation")]
    formatted_track_designation: String,
    #[serde(rename = "TrainMovementTime")]
    train_movement_time: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkTrackDataCleaned {
    train_movement_time: u64,
    stop_id: String,
    trip_id: String,
    event_type: MetrolinkEventType,
    formatted_track_designation: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum MetrolinkEventType {
    Arrival,
    Departure,
}

#[derive(Clone, Debug)]
pub enum TrackData {
    Metrolink(Option<Vec<MetrolinkTrackData>>),
    None,
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

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    let fetched_track_data: TrackData = fetch_track_data(&chateau_id).await;

    println!("Forming pg connection");
    let conn = &mut conn_pre?;
    println!("Connected to postges");

    // take all the gtfs rt data and merge it together

    let mut aspenised_vehicle_positions: AHashMap<String, AspenisedVehiclePosition> =
        AHashMap::new();
    let mut vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache> = AHashMap::new();
    let mut trip_updates: AHashMap<String, AspenisedTripUpdate> = AHashMap::new();
    let mut trip_updates_lookup_by_trip_id_to_trip_update_ids: AHashMap<String, Vec<String>> =
        AHashMap::new();

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
                        aspenised_vehicle_positions.insert(vehicle_entity.id.clone(), AspenisedVehiclePosition {
                                trip: vehicle_pos.trip.as_ref().map(|trip| {
                                    AspenisedVehicleTripInfo {
                                        trip_id: trip.trip_id.clone(),
                                        direction_id: trip.direction_id,
                                       start_date: trip.start_date.clone(),
                                       start_time: trip.start_time.clone(),
                                        schedule_relationship: trip.schedule_relationship,
                                        route_id: match &trip.route_id {
                                            Some(route_id) => Some(route_id.clone()),
                                            None => match &trip.trip_id {
                                                Some(trip_id) => {
                                                    let trip = trip_id_to_trip.get(&trip_id.clone());
                                                    trip.map(|trip| trip.route_id.clone())
                                                },
                                                None => None
                                            }
                                        },
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
                                                        trip.trip_short_name.clone()
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
                                    label: vehicle.label.clone(),
                                    license_plate: vehicle.license_plate.clone(),
                                    wheelchair_accessible: vehicle.wheelchair_accessible,
                                    }),
                                route_type: match realtime_feed_id.as_str() {
                                    "f-mta~nyc~rt~lirr" => 2,
                                    "f-mta~nyc~rt~mnr" => 2,
                                    "f-amtrak~rt" => 2,
                                    _ => match &vehicle_pos.trip {
                                        Some(trip) => match &trip.route_id {
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
                            });

                        //insert the route cache

                        if let Some(trip) = &vehicle_pos.trip {
                            if let Some(route_id) = &trip.route_id {
                                route_ids_to_insert.insert(route_id.clone());
                            } else if let Some(trip_id) = &trip.trip_id {
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

                        let trip_update = AspenisedTripUpdate {
                            trip: trip_update.trip.clone().into(),
                            vehicle: trip_update.vehicle.clone().map(|x| x.into()),
                            stop_time_update: trip_update
                                .stop_time_update
                                .iter()
                                .map(|stu| AspenisedStopTimeUpdate {
                                    stop_sequence: stu.stop_sequence,
                                    stop_id: stu.stop_id.clone(),
                                    arrival: stu.arrival.clone().map(|arrival| {
                                        AspenStopTimeEvent {
                                            delay: arrival.delay,
                                            time: arrival.time,
                                            uncertainty: arrival.uncertainty,
                                        }
                                    }),
                                    departure: stu.departure.clone().map(|departure| {
                                        AspenStopTimeEvent {
                                            delay: departure.delay,
                                            time: departure.time,
                                            uncertainty: departure.uncertainty,
                                        }
                                    }),
                                    platform_string: None,
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
                                .entry(trip_id.as_ref().unwrap().clone())
                                .or_insert(vec![trip_update_entity.id.clone()]);
                        }

                        trip_updates.insert(trip_update_entity.id.clone(), trip_update);
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

        //Insert data back into process-wide authoritative_data_store

        authoritative_data_store
            .entry(chateau_id.clone())
            .and_modify(|data| {
                *data = AspenisedData {
                    vehicle_positions: aspenised_vehicle_positions.clone(),
                    vehicle_routes_cache: vehicle_routes_cache.clone(),
                    trip_updates: trip_updates.clone(),
                    trip_updates_lookup_by_trip_id_to_trip_update_ids:
                        trip_updates_lookup_by_trip_id_to_trip_update_ids.clone(),
                    aspenised_alerts: alerts.clone(),
                    impacted_routes_alerts: impacted_route_id_to_alert_ids.clone(),
                    impacted_stops_alerts: AHashMap::new(),
                    impacted_trips_alerts: impact_trip_id_to_alert_ids.clone(),
                    last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
                }
            })
            .or_insert(AspenisedData {
                vehicle_positions: aspenised_vehicle_positions.clone(),
                vehicle_routes_cache: vehicle_routes_cache.clone(),
                trip_updates: trip_updates.clone(),
                trip_updates_lookup_by_trip_id_to_trip_update_ids:
                    trip_updates_lookup_by_trip_id_to_trip_update_ids.clone(),
                aspenised_alerts: alerts.clone(),
                impacted_routes_alerts: impacted_route_id_to_alert_ids.clone(),
                impacted_stops_alerts: AHashMap::new(),
                impacted_trips_alerts: impact_trip_id_to_alert_ids.clone(),
                last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
            });

        println!(
            "Updated Chateau {} with realtime data from {}, took {} ms, with {} ms chateau lookup, {} ms route lookup, {} ms trips and {} ms itin lookup",
            chateau_id,
            realtime_feed_id,
            start.elapsed().as_millis(),
            chateau_elapsed.as_millis(),
            routes_query_elapsed.as_millis(),
            trip_duration.as_millis(),
            itin_lookup_duration.as_millis()
        );
    }
    Ok(true)
}

pub async fn fetch_track_data(chateau_id: &str) -> TrackData {
    match chateau_id {
        "metrolinktrains" => {
            let url = "https://rtt.metrolinktrains.com/StationScheduleList.json";
            let response = reqwest::get(url)
                .await
                .unwrap()
                .json::<Vec<MetrolinkTrackData>>()
                .await
                .unwrap();
            TrackData::Metrolink(Some(response))
        }
        _ => TrackData::None,
    }
}
