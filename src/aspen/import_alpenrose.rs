// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

extern crate catenary;
use catenary::aspen_dataset::*;
use catenary::parse_gtfs_rt_message;
use catenary::postgres_tools::CatenaryPostgresPool;
use dashmap::DashMap;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use gtfs_rt::FeedMessage;
use gtfs_rt::TripUpdate;
use prost::Message;
use scc::HashMap as SccHashMap;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use ahash::{AHashSet,AHashMap};
use tokio::sync::RwLock;

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

pub async fn new_rt_data(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    authoritative_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    chateau_id: String,
    realtime_feed_id: String,
    vehicles: Option<Vec<u8>>,
    trips: Option<Vec<u8>>,
    alerts: Option<Vec<u8>>,
    has_vehicles: bool,
    has_trips: bool,
    has_alerts: bool,
    vehicles_response_code: Option<u16>,
    trips_response_code: Option<u16>,
    alerts_response_code: Option<u16>,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    println!("Forming pg connection");
    let conn = &mut conn_pre?;
    println!("Connected to postges");

    let vehicles_gtfs_rt = match vehicles_response_code {
        Some(200) => match vehicles {
            Some(v) => match parse_gtfs_rt_message(&v.as_slice()) {
                Ok(v) => Some(v),
                Err(e) => {
                    println!("Error decoding vehicles: {}", e);
                    None
                }
            },
            None => None,
        },
        _ => None,
    };

    let trips_gtfs_rt = match trips_response_code {
        Some(200) => match trips {
            Some(t) => match parse_gtfs_rt_message(&t.as_slice()) {
                Ok(t) => Some(t),
                Err(e) => {
                    println!("Error decoding trips: {}", e);
                    None
                }
            },
            None => None,
        },
        _ => None,
    };

    let alerts_gtfs_rt = match alerts_response_code {
        Some(200) => match alerts {
            Some(a) => match parse_gtfs_rt_message(&a.as_slice()) {
                Ok(a) => Some(a),
                Err(e) => {
                    println!("Error decoding alerts: {}", e);
                    None
                }
            },
            None => None,
        },
        _ => None,
    };

    //get and update raw gtfs_rt data

    println!("Parsed FeedMessages for {}", realtime_feed_id);

    if let Some(vehicles_gtfs_rt) = &vehicles_gtfs_rt {
        authoritative_gtfs_rt
            .entry((realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
            .and_modify(|gtfs_data| *gtfs_data = vehicles_gtfs_rt.clone())
            .or_insert(vehicles_gtfs_rt.clone());
    }

    if let Some(trip_gtfs_rt) = &trips_gtfs_rt {
        authoritative_gtfs_rt
            .entry((realtime_feed_id.clone(), GtfsRtType::TripUpdates))
            .and_modify(|gtfs_data| *gtfs_data = trip_gtfs_rt.clone())
            .or_insert(trip_gtfs_rt.clone());
    }

    if let Some(alerts_gtfs_rt) = &alerts_gtfs_rt {
        authoritative_gtfs_rt
            .entry((realtime_feed_id.clone(), GtfsRtType::Alerts))
            .and_modify(|gtfs_data| *gtfs_data = alerts_gtfs_rt.clone())
            .or_insert(alerts_gtfs_rt.clone());
    }

    println!("Saved FeedMessages for {}", realtime_feed_id);

    let this_chateau_dashmap = authoritative_data_store.get(&realtime_feed_id);

    // take all the gtfs rt data and merge it together

    let mut aspenised_vehicle_positions: AHashMap<String, AspenisedVehiclePosition> = AHashMap::new();
    let mut vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache> = AHashMap::new();
    let mut trip_updates: AHashMap<String, TripUpdate> = AHashMap::new();
    let mut trip_updates_lookup_by_trip_id_to_trip_update_ids: AHashMap<String, Vec<String>> =
        AHashMap::new();

    use catenary::schema::gtfs::chateaus as chateaus_pg_schema;
    use catenary::schema::gtfs::routes as routes_pg_schema;
    use catenary::schema::gtfs::static_download_attempts as static_download_attempts_pg_schema;

    //get this chateau
    let this_chateau = chateaus_pg_schema::dsl::chateaus
        .filter(chateaus_pg_schema::dsl::chateau.eq(&chateau_id))
        .first::<catenary::models::Chateau>(conn)
        .await?;

    //get all routes inside chateau from postgres db
    let routes = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await?;

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

        let trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(
                catenary::schema::gtfs::trips_compressed::dsl::trip_id
                    .eq_any(trip_ids_to_lookup.iter()),
            )
            .load::<catenary::models::CompressedTrip>(conn)
            .await?;

        let mut trip_id_to_trip: AHashMap<String, catenary::models::CompressedTrip> = AHashMap::new();

        for trip in trips {
            trip_id_to_trip.insert(trip.trip_id.clone(), trip);
        }

        let trip_id_to_trip = trip_id_to_trip;

        //also lookup all the headsigns from the trips via itinerary patterns

        let mut list_of_itinerary_patterns_to_lookup: HashSet<String> = HashSet::new();

        for trip in trip_id_to_trip.values() {
            list_of_itinerary_patterns_to_lookup.insert(trip.itinerary_pattern_id.clone());
        }

        let itinerary_patterns =
            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_id
                        .eq_any(list_of_itinerary_patterns_to_lookup.iter()),
                )
                .select(catenary::models::ItineraryPatternMeta::as_select())
                .load::<catenary::models::ItineraryPatternMeta>(conn)
                .await?;

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
                                            },
                                        trip_short_name: match &trip.trip_id {
                                            Some(trip_id) => {
                                                let trip = trip_id_to_trip.get(&trip_id.clone());
                                                match trip {
                                                    Some(trip) => {
                                                        match &trip.trip_short_name {
                                                            Some(trip_short_name) => Some(trip_short_name.clone()),
                                                            None => None
                                                        }
                                                    },
                                                    None => None
                                                }
                                            },
                                            None => None
                                        }
                                    }
                                }),
                                position: match &vehicle_pos.position {
                                    Some(position) => Some(CatenaryRtVehiclePosition {
                                        latitude: position.latitude,
                                        longitude: position.longitude,
                                        bearing: position.bearing,
                                        odometer: position.odometer,
                                        speed: position.speed,
                                    }),
                                    None => None
                                },
                                timestamp: vehicle_pos.timestamp.clone(),
                                vehicle: match &vehicle_pos.vehicle {
                                    Some(vehicle) => Some(AspenisedVehicleDescriptor {
                                        id: vehicle.id.clone(),
                                        label: vehicle.label.clone(),
                                        license_plate: vehicle.license_plate.clone(),
                                        wheelchair_accessible: vehicle.wheelchair_accessible,
                                    }),
                                    None => None
                                }
                            });

                        //insert the route cache

                        if let Some(trip) = &vehicle_pos.trip {
                            if let Some(route_id) = &trip.route_id {
                                if !vehicle_routes_cache.contains_key(route_id) {
                                    let route = route_id_to_route.get(route_id);
                                    if let Some(route) = route {
                                        vehicle_routes_cache.insert(
                                            route_id.clone(),
                                            AspenisedVehicleRouteCache {
                                                route_short_name: route.short_name.clone(),
                                                route_long_name: route.long_name.clone(),
                                                // route_short_name_langs: route.short_name_translations.clone(),
                                                //route_long_name_langs: route.short_name_translations.clone(),
                                                route_colour: route.color.clone(),
                                                route_text_colour: route.text_color.clone(),
                                            },
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
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
                    raw_alerts: None,
                    impacted_routes_alerts: None,
                    impacted_stops_alerts: None,
                    impacted_routes_stops_alerts: None,
                    last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
                }
            })
            .or_insert(AspenisedData {
                vehicle_positions: aspenised_vehicle_positions.clone(),
                vehicle_routes_cache: vehicle_routes_cache.clone(),
                trip_updates: trip_updates.clone(),
                trip_updates_lookup_by_trip_id_to_trip_update_ids:
                    trip_updates_lookup_by_trip_id_to_trip_update_ids.clone(),
                raw_alerts: None,
                impacted_routes_alerts: None,
                impacted_stops_alerts: None,
                impacted_routes_stops_alerts: None,
                last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
            });

        println!(
            "Updated Chateau {} with realtime data from {}",
            chateau_id, realtime_feed_id
        );
    }
    Ok(true)
}
