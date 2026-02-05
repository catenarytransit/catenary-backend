// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

extern crate catenary;
use super::track_number::metrolinx_platforms::{
    ALL_METROLINX_STATIONS, ALL_UPEXPRESS_STATIONS, fetch_metrolinx_platforms,
    fetch_upexpress_platforms,
};
use super::track_number::*;
use crate::delay_calculation::calculate_delay;
use crate::metrolink_california_additions::vehicle_pos_supplement;
use crate::persistence;
use crate::route_type_overrides::apply_route_type_overrides;
use crate::stop_time_logic::find_closest_stop_time_update;
use ahash::{AHashMap, AHashSet};
use catenary::aspen_dataset::option_i32_to_occupancy_status;
use catenary::aspen_dataset::option_i32_to_schedule_relationship;
use catenary::aspen_dataset::*;
use catenary::postgres_tools::CatenaryPostgresPool;

use chrono::TimeZone;

use compact_str::CompactString;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use ecow::EcoString;

use catenary::compact_formats::{
    CompactFeedMessage, CompactItineraryPatternRow, CompactStopTimeUpdate,
};
use lazy_static::lazy_static;

use scc::HashIndex;
use scc::HashMap as SccHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

lazy_static! {
    static ref LAST_SAVE_TIME: SccHashMap<String, Instant> = SccHashMap::new();
    static ref START_TIME: SccHashMap<String, Instant> = SccHashMap::new();
}

const SAVE_INTERVAL: Duration = Duration::from_secs(60);
// Used to prevent data flickering when a feed momentarily drops a trip that was present
// in the previous fetch cycle.
const DROP_OLD_TRIPS_GRACE_PERIOD: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetrolinkPosRaw {
    pub symbol: CompactString,
    pub direction: CompactString,
    pub lat: CompactString,
    pub long: CompactString,
    pub speed: CompactString,
    pub line: CompactString,
    pub ptc_time: CompactString,
    pub ptc_status: CompactString,
    pub delay_status: CompactString,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetrolinkPos {
    pub lat: f32,
    pub lon: f32,
    pub speed: f32,
    pub symbol: CompactString,
}

// Feeds where the vehicle label is more reliable/stable than the vehicle ID.
const REALTIME_FEEDS_TO_USE_VEHICLE_IDS: [&str; 1] = ["f-ezzx-tbc~rt"];

fn mph_to_mps(mph: &CompactString) -> Option<f32> {
    let mph: f32 = match mph.parse() {
        Ok(mph) => mph,
        Err(_) => return None,
    };

    Some(mph * 0.44704)
}

fn metrlink_coord_to_f32(coord: &CompactString) -> Option<f32> {
    // Split into 3 parts based on : (degrees:minutes:seconds)
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
    authoritative_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    chateau_id: &str,
    realtime_feed_id: &str,
    has_vehicles: bool,
    has_trips: bool,
    has_alerts: bool,
    vehicles_response_code: Option<u16>,
    trips_response_code: Option<u16>,
    alerts_response_code: Option<u16>,
    pool: Arc<CatenaryPostgresPool>,
    redis_client: &redis::Client,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "Started processing for chateau {} and feed {}",
        chateau_id, realtime_feed_id
    );
    let start = std::time::Instant::now();

    START_TIME
        .entry_async(chateau_id.to_string())
        .await
        .or_insert(Instant::now());

    // Fetch existing data once - extracting only what we need to avoid holding the lock
    let (mut compressed_trip_internal_cache, previous_authoritative_data_store) =
        match authoritative_data_store.get_async(chateau_id).await {
            Some(data) => (
                data.compressed_trip_internal_cache.clone(),
                Some(data.get().clone()),
            ),
            None => (CompressedTripInternalCache::new(), None),
        };

    // Metrolink provides a separate JSON endpoint with higher fidelity data (PTC status, exact speed)
    // than their standard GTFS-RT feed. We fetch this to supplement the standard feed.
    let fetch_supplemental_data_positions_metrolink: Option<AHashMap<CompactString, MetrolinkPos>> =
        match realtime_feed_id {
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

    let fetch_supplemental_platforms_metrolinx: Option<AHashMap<(String, String), String>> =
        match chateau_id {
            "gotransit" => Some(
                fetch_metrolinx_platforms(
                    ALL_METROLINX_STATIONS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                )
                .await,
            ),
            "upexpress" => Some(
                fetch_upexpress_platforms(
                    ALL_UPEXPRESS_STATIONS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                )
                .await,
            ),
            _ => None,
        };

    let conn_pool = pool.as_ref();
    let mut conn_pre = conn_pool.get().await;

    let fetched_track_data: TrackData = fetch_track_data(&chateau_id, &pool).await;

    let mut conn = conn_pre;

    if let Err(e) = &conn {
        println!("Error with connecting to postgres");
        eprintln!("{:#?}", e);
        return Ok(false);
    }

    let conn = &mut conn.unwrap();

    let mut aspenised_vehicle_positions: AHashMap<String, AspenisedVehiclePosition> =
        AHashMap::new();
    let mut gtfs_vehicle_labels_to_ids: AHashMap<String, String> = AHashMap::new();
    let mut trip_id_to_vehicle_gtfs_rt_id: AHashMap<String, Vec<String>> = AHashMap::new();
    let mut vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache> = AHashMap::new();
    let mut trip_updates: AHashMap<CompactString, AspenisedTripUpdate> = AHashMap::new();
    let mut trip_updates_lookup_by_trip_id_to_trip_update_ids: AHashMap<
        CompactString,
        Vec<CompactString>,
    > = AHashMap::new();

    let mut trip_updates_lookup_by_route_id_to_trip_update_ids: AHashMap<
        CompactString,
        Vec<CompactString>,
    > = AHashMap::new();

    let mut alerts: AHashMap<String, AspenisedAlert> = AHashMap::new();

    let mut impacted_route_id_to_alert_ids: AHashMap<String, Vec<String>> = AHashMap::new();
    let mut impacted_stop_id_to_alert_ids: AHashMap<String, Vec<String>> = AHashMap::new();
    let mut impact_trip_id_to_alert_ids: AHashMap<String, Vec<String>> = AHashMap::new();
    let mut general_alerts: AHashMap<String, Vec<String>> = AHashMap::new();

    let mut stop_id_to_stop: AHashMap<CompactString, AspenisedStop> = AHashMap::new();
    let mut shape_id_to_shape: AHashMap<CompactString, Option<EcoString>> = AHashMap::new();
    let mut trip_modifications: AHashMap<CompactString, AspenisedTripModification> =
        AHashMap::new();
    let mut trip_id_to_trip_modification_ids: AHashMap<CompactString, Vec<EcoString>> =
        AHashMap::new();
    let mut stop_id_to_trip_modification_ids: AHashMap<CompactString, Vec<EcoString>> =
        AHashMap::new();
    let mut stop_id_to_non_scheduled_trip_ids: AHashMap<CompactString, Vec<EcoString>> =
        AHashMap::new();
    let mut stop_id_to_parent_id: AHashMap<CompactString, CompactString> = AHashMap::new();
    let mut parent_id_to_children_ids: AHashMap<CompactString, Vec<CompactString>> =
        AHashMap::new();

    let mut accumulated_itinerary_patterns: AHashMap<
        String,
        (
            catenary::models::ItineraryPatternMeta,
            Vec<CompactItineraryPatternRow>,
        ),
    > = AHashMap::new();

    use catenary::schema::gtfs::chateaus as chateaus_pg_schema;
    use catenary::schema::gtfs::routes as routes_pg_schema;

    let start_chateau_query = Instant::now();
    let this_chateau = chateaus_pg_schema::dsl::chateaus
        .filter(chateaus_pg_schema::dsl::chateau.eq(&chateau_id))
        .first::<catenary::models::Chateau>(conn)
        .await?;
    let chateau_elapsed = start_chateau_query.elapsed();

    let start_routes_query = Instant::now();
    let routes: Vec<catenary::models::Route> = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await?;
    let routes_query_elapsed = start_routes_query.elapsed();

    if routes.is_empty() {
        println!("No routes found for chateau {}", chateau_id);
    }

    let mut route_id_to_route: AHashMap<String, catenary::models::Route> = AHashMap::new();

    for route in routes {
        route_id_to_route.insert(route.route_id.clone(), route);
    }

    let route_id_to_route = route_id_to_route;

    // Santa Cruz Metro requires supplemental data for better vehicle tracking accuracy
    let santa_cruz_supp_data = match chateau_id {
        "santacruzmetro" => {
            let route_ids = &route_id_to_route
                .keys()
                .map(|x| x.to_string())
                .collect::<Vec<String>>();

            let fetch_santa_cruz_supp =
                catenary::santa_cruz::fetch_santa_cruz_clever_data(route_ids);

            match fetch_santa_cruz_supp.await {
                Ok(santa_cruz_supp) => {
                    let mut hashmap_by_vehicle = AHashMap::new();

                    for s in santa_cruz_supp {
                        hashmap_by_vehicle.insert(s.vid.clone(), s);
                    }

                    Some(hashmap_by_vehicle)
                }
                Err(e) => {
                    println!("Error fetching santa cruz data: {}", e);
                    None
                }
            }
        }
        _ => None,
    };

    // Collect all Trip IDs referenced in the RT feeds (Vehicles, TripUpdates, Alerts)
    // to perform a single batch lookup against the cache/DB.
    let mut trip_ids_to_lookup: AHashSet<String> = AHashSet::new();

    for realtime_feed_id in this_chateau.realtime_feeds.iter().flatten() {
        if let Some(vehicle_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
            .get_async(&(realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
            .await
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

        if let Some(trip_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
            .get_async(&(realtime_feed_id.clone(), GtfsRtType::TripUpdates))
            .await
        {
            let trip_gtfs_rt_for_feed_id = trip_gtfs_rt_for_feed_id.get();

            for trip_entity in trip_gtfs_rt_for_feed_id.entity.iter() {
                if let Some(trip_update) = &trip_entity.trip_update {
                    if let Some(trip_id) = &trip_update.trip.trip_id {
                        trip_ids_to_lookup.insert(trip_id.clone());
                    }

                    if let Some(trip_property) = &trip_update.trip_properties {
                        if let Some(trip_id) = &trip_property.trip_id {
                            trip_ids_to_lookup.insert(trip_id.clone());
                        }
                    }
                }
            }
        }

        if let Some(alert_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
            .get_async(&(realtime_feed_id.clone(), GtfsRtType::Alerts))
            .await
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

        // Remove trips not in the current lookup set using retain (avoids intermediate Vec allocation)
        compressed_trip_internal_cache
            .compressed_trips
            .retain(|k, _| trip_ids_to_lookup.contains(k.as_str()));

        let trip_start = std::time::Instant::now();

        // Optimize DB load by only fetching trips that aren't already in the internal cache
        let trip_ids_to_lookup_to_hit = trip_ids_to_lookup
            .iter()
            .filter(|x| {
                !compressed_trip_internal_cache
                    .compressed_trips
                    .contains_key(x.as_str())
            })
            .collect::<Vec<&String>>();

        let trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(&chateau_id))
            .filter(
                catenary::schema::gtfs::trips_compressed::dsl::trip_id
                    .eq_any(&trip_ids_to_lookup_to_hit),
            )
            .load::<catenary::models::CompressedTrip>(conn)
            .await?;

        let trip_duration = trip_start.elapsed();

        let mut trip_id_to_trip: AHashMap<String, catenary::models::CompressedTrip> =
            AHashMap::new();

        for trip in trips {
            trip_id_to_trip.insert(trip.trip_id.clone(), trip);
        }

        for (trip_id, trips_in_cache) in compressed_trip_internal_cache.compressed_trips {
            trip_id_to_trip.insert(trip_id.clone(), trips_in_cache.clone());
        }

        let trip_id_to_trip = trip_id_to_trip;

        let service_ids_to_lookup = trip_id_to_trip
            .iter()
            .map(|x| x.1.service_id.clone())
            .collect::<AHashSet<CompactString>>()
            .into_iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        let pool_cal = pool.clone();
        let service_ids_cal = service_ids_to_lookup.clone();
        let chateau_cal = chateau_id.to_string();
        let calendar_future = async move {
            let mut conn = pool_cal
                .get()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            catenary::schema::gtfs::calendar::dsl::calendar
                .filter(catenary::schema::gtfs::calendar::dsl::chateau.eq(&chateau_cal))
                .filter(catenary::schema::gtfs::calendar::dsl::service_id.eq_any(&service_ids_cal))
                .load::<catenary::models::Calendar>(&mut conn)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        };

        let pool_cd = pool.clone();
        let service_ids_cd = service_ids_to_lookup.clone();
        let chateau_cd = chateau_id.to_string();
        let calendar_dates_future = async move {
            let mut conn = pool_cd
                .get()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                .filter(catenary::schema::gtfs::calendar_dates::dsl::chateau.eq(&chateau_cd))
                .filter(
                    catenary::schema::gtfs::calendar_dates::dsl::service_id.eq_any(&service_ids_cd),
                )
                .load::<catenary::models::CalendarDate>(&mut conn)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        };

        let missing_trip_ids = trip_ids_to_lookup
            .iter()
            .filter(|x| !trip_id_to_trip.contains_key(x.as_str()))
            .cloned()
            .collect::<AHashSet<String>>();

        let mut stop_ids_to_lookup: AHashSet<String> = AHashSet::new();

        let mut vehicle_id_to_closest_temporal_stop_update: AHashMap<
            CompactString,
            CompactStopTimeUpdate,
        > = AHashMap::new();

        let current_time_unix_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Used for cache fallback
        let empty_scheduled_stops_option: Option<AHashSet<String>> = None;

        if let Some(trip_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
            .get_async(&(realtime_feed_id.clone(), GtfsRtType::TripUpdates))
            .await
        {
            let trip_gtfs_rt_for_feed_id = trip_gtfs_rt_for_feed_id.get();
            let ref_epoch = trip_gtfs_rt_for_feed_id.reference_epoch;

            for trip_entity in trip_gtfs_rt_for_feed_id.entity.iter() {
                if let Some(trip_update) = &trip_entity.trip_update {
                    if let Some(trip_id) = &trip_update.trip.trip_id {
                        if missing_trip_ids.contains(trip_id) {
                            if let Some(trip_id) = &trip_update.trip.trip_id {
                                let last_non_skipped_stop_id = trip_update
                                    .stop_time_update
                                    .iter()
                                    .filter(|x| x.schedule_relationship != Some(1))
                                    .last()
                                    .and_then(|x| x.stop_id.clone());

                                if let Some(last_non_skipped_stop_id) = last_non_skipped_stop_id {
                                    stop_ids_to_lookup.insert(last_non_skipped_stop_id.clone());
                                }
                            }
                        }

                        // Determine the "active" stop time update for interpolation.
                        // Logic:
                        // 1. If a stop time update brackets the current time (arrival < now < departure), use it.
                        // 2. Otherwise, sort by time and select the next chronological arrival/departure.
                        // 3. Fallback: If all departures are past, use the final stop.
                        let closest_stop_time_update = find_closest_stop_time_update(
                            &trip_update.stop_time_update,
                            current_time_unix_timestamp,
                            ref_epoch,
                        );

                        if let Some(closest_stop_time_update) = closest_stop_time_update {
                            let vehicle_id = trip_update
                                .vehicle
                                .as_ref()
                                .and_then(|vehicle| vehicle.id.clone());

                            if let Some(vehicle_id) = vehicle_id {
                                if chateau_id == "santacruzmetro" {
                                    println!(
                                        "Inserting santacruzmetro vehicle {}, {:?}",
                                        vehicle_id, closest_stop_time_update.stop_id
                                    );
                                }

                                vehicle_id_to_closest_temporal_stop_update
                                    .insert(vehicle_id.into(), closest_stop_time_update);
                            }
                        }
                    }
                }
            }
        }

        let mut list_of_itinerary_patterns_to_lookup: AHashSet<String> = AHashSet::new();
        for trip in trip_id_to_trip.values() {
            list_of_itinerary_patterns_to_lookup.insert(trip.itinerary_pattern_id.clone());
        }
        compressed_trip_internal_cache.compressed_trips = trip_id_to_trip.clone();

        // Execute all heavy DB lookups in parallel via tokio join
        let pool_stops = pool.clone();
        let stop_ids_stops = stop_ids_to_lookup.iter().cloned().collect::<Vec<_>>();
        let chateau_stops = chateau_id.to_string();
        let stops_future = async move {
            if stop_ids_stops.is_empty() {
                return Ok(vec![]);
            }
            let mut conn = pool_stops
                .get()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let stops_answer = catenary::schema::gtfs::stops::dsl::stops
                .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&chateau_stops))
                .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&stop_ids_stops))
                .select(catenary::models::Stop::as_select())
                .load::<catenary::models::Stop>(&mut conn)
                .await;

            match stops_answer {
                Ok(stops) => Ok(stops),
                Err(e) => {
                    println!("Error fetching stops: {}", e);
                    Ok(vec![])
                }
            }
        };

        let pool_ip = pool.clone();
        let list_ip = list_of_itinerary_patterns_to_lookup
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let list_ip_2 = list_ip.clone();
        let chateau_ip = chateau_id.to_string();

        let itinerary_patterns_future = async move {
            let mut conn = pool_ip
                .get()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::chateau.eq(&chateau_ip),
                )
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_id
                        .eq_any(&list_ip),
                )
                .select(catenary::models::ItineraryPatternMeta::as_select())
                .load::<catenary::models::ItineraryPatternMeta>(&mut conn)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        };

        let pool_ipr = pool.clone();
        let chateau_ipr = chateau_id.to_string();
        let itinerary_pattern_rows_future = async move {
            let mut conn = pool_ipr
                .get()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                .filter(catenary::schema::gtfs::itinerary_pattern::dsl::chateau.eq(&chateau_ipr))
                .filter(
                    catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern_id
                        .eq_any(&list_ip_2),
                )
                .select(catenary::models::ItineraryPatternRow::as_select())
                .load::<catenary::models::ItineraryPatternRow>(&mut conn)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        };

        // Query for stops with parent_station to build parent-child mappings
        let pool_parent_stops = pool.clone();
        let chateau_parent_stops = chateau_id.to_string();
        let parent_stops_future = async move {
            let mut conn = pool_parent_stops
                .get()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            catenary::schema::gtfs::stops::dsl::stops
                .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&chateau_parent_stops))
                .filter(catenary::schema::gtfs::stops::dsl::parent_station.is_not_null())
                .select((
                    catenary::schema::gtfs::stops::dsl::gtfs_id,
                    catenary::schema::gtfs::stops::dsl::parent_station,
                ))
                .load::<(String, Option<String>)>(&mut conn)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        };

        let join_start = std::time::Instant::now();
        let (
            calendar,
            calendar_dates,
            stops,
            itinerary_pattern_meta_list,
            itinerary_pattern_rows,
            parent_stops,
        ) = tokio::try_join!(
            calendar_future,
            calendar_dates_future,
            stops_future,
            itinerary_patterns_future,
            itinerary_pattern_rows_future,
            parent_stops_future
        )?;

        let itin_lookup_duration = join_start.elapsed();
        let itinerary_pattern_row_duration = join_start.elapsed();

        let calendar_structure =
            catenary::make_calendar_structure_from_pg_single_chateau(calendar, calendar_dates);

        // Build parent-child mappings from parent_stops query results
        for (stop_id, parent) in &parent_stops {
            if let Some(parent_id) = parent {
                stop_id_to_parent_id.insert(stop_id.clone().into(), parent_id.clone().into());
                parent_id_to_children_ids
                    .entry(parent_id.clone().into())
                    .or_default()
                    .push(stop_id.clone().into());
            }
        }

        let stop_id_to_stop_from_postgres: AHashMap<String, catenary::models::Stop> = stops
            .into_iter()
            .map(|stop| (stop.gtfs_id.clone(), stop))
            .collect();

        let mut itinerary_pattern_id_to_itinerary_pattern_meta: AHashMap<
            String,
            catenary::models::ItineraryPatternMeta,
        > = AHashMap::new();

        for meta in itinerary_pattern_meta_list {
            itinerary_pattern_id_to_itinerary_pattern_meta
                .insert(meta.itinerary_pattern_id.clone(), meta);
        }

        let itinerary_pattern_id_to_itinerary_pattern_meta =
            itinerary_pattern_id_to_itinerary_pattern_meta;

        let direction_patterns_to_get: AHashSet<String> =
            itinerary_pattern_id_to_itinerary_pattern_meta
                .values()
                .map(|x| x.direction_pattern_id.clone())
                .flatten()
                .collect();

        // Direction Patterns (dependent on previous result, so we run it now)
        let direction_patterns =
            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::dsl::chateau.eq(&chateau_id),
                )
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_id
                        .eq_any(&direction_patterns_to_get),
                )
                .select(catenary::models::DirectionPatternMeta::as_select())
                .load::<catenary::models::DirectionPatternMeta>(conn)
                .await?;

        let mut direction_pattern_id_to_direction_pattern_meta: AHashMap<
            String,
            catenary::models::DirectionPatternMeta,
        > = AHashMap::new();

        for direction_pattern in direction_patterns {
            direction_pattern_id_to_direction_pattern_meta.insert(
                direction_pattern.direction_pattern_id.clone(),
                direction_pattern,
            );
        }

        let direction_pattern_id_to_direction_pattern_meta =
            direction_pattern_id_to_direction_pattern_meta;

        // Group itinerary rows by pattern ID to allow efficient lookups,
        // and enforce strict sorting by stop_sequence for linear interpolation.
        let mut newly_added_patterns = AHashSet::new();

        for (id, meta) in itinerary_pattern_id_to_itinerary_pattern_meta {
            if !accumulated_itinerary_patterns.contains_key(&id) {
                accumulated_itinerary_patterns.insert(id.clone(), (meta, vec![]));
                newly_added_patterns.insert(id);
            }
        }

        for itinerary_pattern_row in itinerary_pattern_rows {
            if newly_added_patterns.contains(&itinerary_pattern_row.itinerary_pattern_id) {
                if let Some(entry) = accumulated_itinerary_patterns
                    .get_mut(&itinerary_pattern_row.itinerary_pattern_id)
                {
                    entry.1.push(CompactItineraryPatternRow {
                        stop_sequence: itinerary_pattern_row.stop_sequence,
                        arrival_time_since_start: itinerary_pattern_row.arrival_time_since_start,
                        departure_time_since_start: itinerary_pattern_row
                            .departure_time_since_start,
                        interpolated_time_since_start: itinerary_pattern_row
                            .interpolated_time_since_start,
                        stop_id: itinerary_pattern_row.stop_id,
                        gtfs_stop_sequence: itinerary_pattern_row.gtfs_stop_sequence,
                        timepoint: itinerary_pattern_row.timepoint,
                        stop_headsign_idx: itinerary_pattern_row.stop_headsign_idx,
                    });
                }
            }
        }
        for (_, rows) in accumulated_itinerary_patterns.values_mut() {
            rows.sort_by(|a, b| a.stop_sequence.cmp(&b.stop_sequence));
        }

        let mut itinerary_pattern_id_to_scheduled_stop_ids: AHashMap<
            String,
            Option<AHashSet<String>>,
        > = AHashMap::new();
        for (id, (_, rows)) in &accumulated_itinerary_patterns {
            let set: AHashSet<String> = rows.iter().map(|x| x.stop_id.to_string()).collect();
            itinerary_pattern_id_to_scheduled_stop_ids.insert(id.clone(), Some(set));
        }

        let mut route_ids_to_insert: AHashSet<String> = AHashSet::new();

        for realtime_feed_id in this_chateau.realtime_feeds.iter().flatten() {
            let vehicle_id_to_trip_update_start_time: AHashMap<String, String> = AHashMap::new();

            if let Some(vehicle_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
                .get_async(&(realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
                .await
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

                        let mut position = vehicle_pos.position.as_ref().map(|position| {
                            CatenaryRtVehiclePosition {
                                latitude: position.latitude,
                                longitude: position.longitude,
                                bearing: position.bearing,
                                odometer: position.odometer,
                                speed: match chateau_id {
                                    "vy~yhtymä~oyj" => position.speed.map(|x| x / 3.6),
                                    _ => position.speed,
                                },
                            }
                        });

                        if santa_cruz_supp_data.is_some() {
                            if let Some(santa_cruz_supp_data) = &santa_cruz_supp_data {
                                if let Some(santa_cruz_supp_data) =
                                    santa_cruz_supp_data.get(&vehicle_entity.id)
                                {
                                    position = Some(CatenaryRtVehiclePosition {
                                        latitude: santa_cruz_supp_data.lat,
                                        longitude: santa_cruz_supp_data.lon,
                                        bearing: Some(santa_cruz_supp_data.heading),
                                        odometer: position.as_ref().map(|x| x.odometer).flatten(),
                                        speed: Some(santa_cruz_supp_data.speed_metres_per_second),
                                    });
                                }
                            }
                        }

                        // Determine the next stop event to calculate delays relative to the schedule.
                        let current_stop_event = match &vehicle_pos.vehicle {
                            None => None,
                            Some(vehicle) => match &vehicle.id {
                                Some(vehicle_id) => vehicle_id_to_closest_temporal_stop_update
                                    .get(vehicle_id.as_str()),
                                None => None,
                            },
                        };

                        if chateau_id == "santacruzmetro" {
                            if let Some(current_stop_event) = current_stop_event {
                                println!(
                                    "santacruzmetro Current stop event, vehicle {}, route {:?}: {:?}",
                                    vehicle_entity.id, recalculate_route_id, current_stop_event
                                );
                            } else {
                                println!(
                                    "santacruzmetro Current stop event, vehicle {}, route {:?}: None",
                                    vehicle_entity.id, recalculate_route_id
                                );
                            }
                        }

                        let trip_headsign = vehicle_pos.trip.as_ref().and_then(|trip| {
                            trip.trip_id.as_ref().and_then(|trip_id| {
                                trip_id_to_trip.get(trip_id).and_then(|trip| {
                                    accumulated_itinerary_patterns
                                        .get(&trip.itinerary_pattern_id)
                                        .map(|x| &x.0)
                                        .and_then(|itinerary_pattern| {
                                            itinerary_pattern
                                                .direction_pattern_id
                                                .as_ref()
                                                .and_then(|direction_pattern_id: &String| {
                                                    direction_pattern_id_to_direction_pattern_meta
                                                        .get(direction_pattern_id.as_str())
                                                        .map(|direction_pattern| {
                                                            let mut headsign =
                                                                direction_pattern.headsign_or_destination.clone();

                                                            if let Some(stop_headsigns) =
                                                                &direction_pattern.stop_headsigns_unique_list
                                                            {
                                                                if let Some(current_stop_event) =
                                                                    current_stop_event
                                                                {
                                                                    if let Some(itinerary_pattern_rows) =
                                                                        accumulated_itinerary_patterns
                                                                            .get(&trip.itinerary_pattern_id)
                                                                            .map(|x| &x.1)
                                                                    {
                                                                        if let Some(matching_itinerary_row) =
                                                                            current_stop_event.stop_id.as_ref().and_then(|current_stop_id| {
                                                                                itinerary_pattern_rows.iter().find(|x| x.stop_id == *current_stop_id)
                                                                            })
                                                                        {
                                                                            if let Some(stop_headsign_idx) =
                                                                                matching_itinerary_row.stop_headsign_idx
                                                                            {
                                                                                // Access headsign directly by index - avoids allocating Vec<String>
                                                                                let mut idx = 0usize;
                                                                                for headsign_opt in stop_headsigns.iter() {
                                                                                    if let Some(text) = headsign_opt {
                                                                                        if idx == stop_headsign_idx as usize {
                                                                                            headsign = text.clone();
                                                                                            break;
                                                                                        }
                                                                                        idx += 1;
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }

                                                            headsign
                                                        })
                                                })
                                        })
                                })
                            })
                        })
                        .map(|headsign| {
                            headsign
                                .replace("-Exact Fare", "")
                                .replace(" - Funded in part by/SB County Measure A", "")
                        });

                        let start_date = vehicle_pos
                            .trip
                            .as_ref()
                            .map(|trip| match &trip.start_date {
                                Some(start_date) => {
                                    match catenary::THROW_AWAY_START_DATES.contains(&chateau_id) {
                                        true => None,
                                        false => match chrono::NaiveDate::parse_from_str(
                                            &start_date,
                                            "%Y%m%d",
                                        ) {
                                            Ok(start_date) => Some(start_date),
                                            Err(_) => None,
                                        },
                                    }
                                }
                                None => None,
                            })
                            .flatten();

                        let mut pos_aspenised = AspenisedVehiclePosition {
                            trip: vehicle_pos
                                .trip
                                .as_ref()
                                .map(|trip| AspenisedVehicleTripInfo {
                                    trip_id: trip.trip_id.clone(),
                                    direction_id: trip.direction_id,
                                    start_date: start_date,
                                    start_time: trip.start_time.clone(),
                                    schedule_relationship: option_i32_to_schedule_relationship(
                                        &trip.schedule_relationship,
                                    ),
                                    route_id: recalculate_route_id.clone(),
                                    trip_headsign: trip_headsign,
                                    trip_short_name: match &trip.trip_id {
                                        Some(trip_id) => {
                                            let trip = trip_id_to_trip.get(&trip_id.clone());
                                            match trip {
                                                Some(trip) => trip
                                                    .trip_short_name
                                                    .as_ref()
                                                    .map(|x| x.to_string()),
                                                None => None,
                                            }
                                        }
                                        None => None,
                                    },
                                    delay: None,
                                }),
                            position: position,
                            timestamp: vehicle_pos.timestamp,
                            vehicle: vehicle_pos.vehicle.as_ref().map(|vehicle| {
                                AspenisedVehicleDescriptor {
                                    id: match realtime_feed_id.as_str() {
                                        "f-c28-bctransit~victoriaregionaltransitsystem~rt" => {
                                            vehicle.id.as_ref().map(|x| {
                                                x.as_str().replace("313135", "").to_string()
                                            })
                                        }
                                        _ => vehicle.id.clone(),
                                    },
                                    label: match realtime_feed_id.as_str() {
                                        "f-trimet~rt" | "f-f24-octranspo~rt" => vehicle.id.clone(),
                                        "f-c28-bctransit~victoriaregionaltransitsystem~rt" => {
                                            vehicle.label.as_ref().map(|x| {
                                                x.as_str().replace("313135", "").to_string()
                                            })
                                        }
                                        _ => vehicle.label.clone(),
                                    },
                                    license_plate: vehicle.license_plate.clone(),
                                    wheelchair_accessible: vehicle.wheelchair_accessible,
                                }
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
                                                None => 3,
                                            }
                                        }
                                        None => 3,
                                    },
                                    None => 3,
                                },
                            },
                            current_status: vehicle_pos.current_status,
                            current_stop_sequence: vehicle_pos.current_stop_sequence,
                            occupancy_status: vehicle_pos.occupancy_status,
                            occupancy_percentage: vehicle_pos.occupancy_percentage,
                            congestion_level: vehicle_pos.congestion_level,
                        };

                        apply_route_type_overrides(
                            realtime_feed_id.as_str(),
                            vehicle_pos,
                            &mut pos_aspenised,
                        );

                        let pos_aspenised = vehicle_pos_supplement(
                            pos_aspenised,
                            &fetch_supplemental_data_positions_metrolink,
                            chateau_id,
                        );

                        let pos_aspenised = match REALTIME_FEEDS_TO_USE_VEHICLE_IDS
                            .contains(&realtime_feed_id.as_str())
                        {
                            true => pos_aspenised.replace_vehicle_label_with_vehicle_id(),
                            false => pos_aspenised,
                        };

                        aspenised_vehicle_positions
                            .insert(vehicle_entity.id.clone(), pos_aspenised);

                        trip_id_to_vehicle_gtfs_rt_id
                            .entry(vehicle_entity.id.clone())
                            .and_modify(|x| x.push(vehicle_entity.id.clone()))
                            .or_insert(vec![vehicle_entity.id.clone()]);

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

            if let Some(trip_updates_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
                .get_async(&(realtime_feed_id.clone(), GtfsRtType::TripUpdates))
                .await
            {
                let trip_updates_gtfs_rt_for_feed_id = trip_updates_gtfs_rt_for_feed_id.get();
                let ref_epoch = trip_updates_gtfs_rt_for_feed_id.reference_epoch;

                for trip_update_entity in trip_updates_gtfs_rt_for_feed_id.entity.iter() {
                    if let Some(trip_update) = &trip_update_entity.trip_update {
                        let trip_id = trip_update.trip.trip_id.clone();

                        let compressed_trip = match &trip_id {
                            Some(trip_id) => trip_id_to_trip.get(trip_id),
                            None => None,
                        };

                        let itinerary_rows = compressed_trip
                            .map(|compressed_trip| {
                                accumulated_itinerary_patterns
                                    .get(&compressed_trip.itinerary_pattern_id)
                                    .map(|x| &x.1)
                            })
                            .flatten();

                        let itinerary_meta = compressed_trip
                            .map(|compressed_trip| {
                                accumulated_itinerary_patterns
                                    .get(&compressed_trip.itinerary_pattern_id)
                                    .map(|x| &x.0)
                            })
                            .flatten();

                        let timezone = itinerary_meta
                            .map(|itinerary_meta| {
                                chrono_tz::Tz::from_str(itinerary_meta.timezone.as_str()).ok()
                            })
                            .flatten();

                        let scheduled_stop_ids_hashset = compressed_trip
                            .and_then(|compressed_trip| {
                                itinerary_pattern_id_to_scheduled_stop_ids
                                    .get(&compressed_trip.itinerary_pattern_id)
                            })
                            .unwrap_or(&empty_scheduled_stops_option);

                        if compressed_trip.is_none() {
                            for trip_update in trip_update.stop_time_update.iter() {
                                if let Some(stop_id) = &trip_update.stop_id {
                                    stop_id_to_non_scheduled_trip_ids
                                        .entry(stop_id.clone().into())
                                        .and_modify(|x| {
                                            x.push(trip_update_entity.id.clone().into())
                                        })
                                        .or_insert(vec![trip_update_entity.id.clone().into()]);
                                }
                            }
                        }

                        let last_non_cancelled_stop_id = trip_update
                            .stop_time_update
                            .iter()
                            .filter(|x| x.schedule_relationship != Some(1))
                            .last()
                            .and_then(|x| x.stop_id.clone());

                        let mut trip_headsign = match &trip_id {
                            Some(trip_id) => match missing_trip_ids.contains(trip_id) {
                                true => match last_non_cancelled_stop_id {
                                    Some(last_non_cancelled_stop_id) => {
                                        stop_id_to_stop_from_postgres
                                            .get(&last_non_cancelled_stop_id)
                                            .map(|s| {
                                                s.name
                                                    .as_ref()
                                                    .map(|name| CompactString::new(&name))
                                            })
                                            .flatten()
                                    }
                                    None => None,
                                },
                                false => None,
                            },
                            None => None,
                        };

                        let mut trip_descriptor: AspenRawTripInfo = trip_update.trip.clone().into();

                        if trip_descriptor.trip_id.is_some() {
                            let recalculate_route_id: Option<String> =
                                match &trip_update.trip.trip_id {
                                    Some(trip_id) => {
                                        let compressed_trip = trip_id_to_trip.get(trip_id);
                                        match compressed_trip {
                                            Some(compressed_trip) => {
                                                let route = route_id_to_route
                                                    .get(&compressed_trip.route_id);
                                                match route {
                                                    Some(route) => Some(route.route_id.clone()),
                                                    None => trip_update.trip.route_id.clone(),
                                                }
                                            }
                                            None => trip_update.trip.route_id.clone(),
                                        }
                                    }
                                    None => trip_update.trip.route_id.clone(),
                                };

                            if let Some(recalculate_route_id) = recalculate_route_id {
                                trip_descriptor.route_id = Some(recalculate_route_id);
                            }
                        }

                        if catenary::THROW_AWAY_START_DATES.contains(&chateau_id) {
                            trip_descriptor.start_date = None;
                        }

                        let mut stop_time_updates_vec = Vec::new();
                        for stu in &trip_update.stop_time_update {
                            let mut platform_resp = None;

                            match chateau_id {
                                "metrolinktrains" => {
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
                                        if let Some(train_data) =
                                            track_data_scax.track_lookup.get(&metrolink_code)
                                        {
                                            if let Some(stop_id) = &stu.stop_id {
                                                if let Some(train_and_stop_scax) =
                                                    train_data.get(stop_id)
                                                {
                                                    platform_resp = Some(
                                                        train_and_stop_scax
                                                            .formatted_track_designation
                                                            .clone()
                                                            .replace("Platform ", "")
                                                            .into(),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                "amtrak" => {
                                    if let TrackData::Amtrak(amtrak_track_multisource) =
                                        &fetched_track_data
                                    {
                                        if let Some(track_data_scax) =
                                            &amtrak_track_multisource.metrolink
                                        {
                                            let mut metrolink_code = String::from("A");
                                            if let Some(compressed_trip) = compressed_trip {
                                                if let Some(trip_short_name) =
                                                    compressed_trip.trip_short_name.as_ref()
                                                {
                                                    metrolink_code.push_str(trip_short_name);
                                                }
                                            }
                                            if let Some(train_data) =
                                                track_data_scax.track_lookup.get(&metrolink_code)
                                            {
                                                if let Some(stop_id) = &stu.stop_id {
                                                    if let Some(train_and_stop_scax) =
                                                        train_data.get(stop_id)
                                                    {
                                                        platform_resp = Some(
                                                            train_and_stop_scax
                                                                .formatted_track_designation
                                                                .clone()
                                                                .replace("Platform ", "")
                                                                .into(),
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                "nationalrailuk" => {
                                    if let TrackData::NationalRail(nr_data) = &fetched_track_data {
                                        if let (Some(trip_id), Some(stop_id)) =
                                            (&trip_descriptor.trip_id, &stu.stop_id)
                                        {
                                            if let Some(trip_platforms) =
                                                nr_data.get(trip_id.as_str())
                                            {
                                                if let Some(platform_info) = trip_platforms
                                                    .iter()
                                                    .find(|p| p.stop_id == *stop_id)
                                                {
                                                    platform_resp =
                                                        Some(platform_info.platform.clone().into());
                                                }
                                            }
                                        }
                                    }
                                }
                                "nmbs" | "sncb" => {
                                    // Extract platform from stop_id suffix pattern (e.g., "8833001_7" -> platform "7")
                                    if let Some(stop_id) = &stu.stop_id {
                                        // Check if this RT stop_id has an underscore suffix indicating platform
                                        if let Some((parent_part, platform_part)) =
                                            stop_id.rsplit_once('_')
                                        {
                                            // Verify the parent is in our scheduled stops or matches via parent relationship
                                            let is_valid_match = scheduled_stop_ids_hashset
                                                .as_ref()
                                                .map(|scheduled| {
                                                    scheduled.contains(parent_part)
                                                        || stop_id_to_parent_id
                                                            .get(stop_id.as_str())
                                                            .map(|rt_parent| {
                                                                scheduled
                                                                    .contains(rt_parent.as_str())
                                                            })
                                                            .unwrap_or(false)
                                                })
                                                .unwrap_or(false);

                                            if is_valid_match && !platform_part.is_empty() {
                                                platform_resp = Some(platform_part.into());
                                            }
                                        }
                                    }
                                }

                                "gotransit" => {
                                    if let Some(plats) = &fetch_supplemental_platforms_metrolinx {
                                        if let Some(trip_id) = &trip_id {
                                            if let Some(stop_id) = &stu.stop_id {
                                                if let Some(trip_number) = trip_id.split('-').last()
                                                {
                                                    if let Some(platform) = plats.get(&(
                                                        trip_number.to_string(),
                                                        stop_id.to_string(),
                                                    )) {
                                                        platform_resp =
                                                            Some(platform.clone().into());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                "upexpress" => {
                                    if let Some(plats) = &fetch_supplemental_platforms_metrolinx {
                                        if let Some(stop_id) = &stu.stop_id {
                                            // Prefer trip_short_name if available (matches user requirement),
                                            // otherwise fallback to parsing trip_id suffix.
                                            let trip_number_candidate = compressed_trip
                                                .and_then(|t| {
                                                    t.trip_short_name
                                                        .as_ref()
                                                        .map(|s| s.to_string())
                                                })
                                                .or_else(|| {
                                                    trip_id.as_ref().and_then(|id| {
                                                        id.split('-').last().map(|s| s.to_string())
                                                    })
                                                });

                                            if let Some(trip_number) = trip_number_candidate {
                                                if let Some(platform) =
                                                    plats.get(&(trip_number, stop_id.to_string()))
                                                {
                                                    platform_resp = Some(platform.clone().into());
                                                }
                                            }
                                        }
                                    }
                                }
                                "viarail" => {
                                    if let TrackData::ViaRail(Some(via_data)) = &fetched_track_data
                                    {
                                        if let Some(trip) = compressed_trip {
                                            if let Some(trip_short_name) = &trip.trip_short_name {
                                                if let Some(stop_id) = &stu.stop_id {
                                                    if let Some(train_data) = via_data
                                                        .track_lookup
                                                        .get(trip_short_name.as_str())
                                                    {
                                                        if let Some(platform) =
                                                            train_data.get(stop_id.as_str())
                                                        {
                                                            platform_resp =
                                                                Some(platform.clone().into());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }

                            stop_time_updates_vec.push(AspenisedStopTimeUpdate {
                                stop_sequence: stu.stop_sequence.map(|x| x as u16),
                                stop_id: stu.stop_id.as_ref().map(|x| x.into()),
                                old_rt_data: false,
                                arrival: stu.arrival.clone().map(|arrival| AspenStopTimeEvent {
                                    delay: None,
                                    time: match arrival.time {
                                        Some(diff) => {
                                            let time =
                                                (ref_epoch as i64) + (i32::from(diff) as i64);
                                            if time <= 0 { None } else { Some(time) }
                                        }
                                        None => None,
                                    },
                                    uncertainty: arrival.uncertainty,
                                }),
                                departure: stu.departure.clone().map(|departure| {
                                    AspenStopTimeEvent {
                                        delay: None,
                                        time: match departure.time {
                                            Some(diff) => {
                                                let time =
                                                    (ref_epoch as i64) + (i32::from(diff) as i64);
                                                if time <= 0 { None } else { Some(time) }
                                            }
                                            None => None,
                                        },
                                        uncertainty: departure.uncertainty,
                                    }
                                }),
                                platform_string: platform_resp,
                                schedule_relationship:
                                    option_i32_to_stop_time_schedule_relationship(
                                        &stu.schedule_relationship,
                                    ),
                                departure_occupancy_status: option_i32_to_occupancy_status(
                                    &stu.departure_occupancy_status,
                                ),
                                stop_time_properties: stu
                                    .stop_time_properties
                                    .clone()
                                    .map(|x| (*x).into()),
                            });
                        }

                        let stop_time_update = stop_time_updates_vec;

                        let new_stop_ids = stop_time_update
                            .iter()
                            .map(|stu| stu.stop_id.clone())
                            .flatten()
                            .collect::<BTreeSet<EcoString>>();

                        let old_data_to_add_to_start: Option<Vec<AspenisedStopTimeUpdate>> =
                            match &trip_id {
                                Some(trip_id) => {
                                    match &previous_authoritative_data_store {
                                        Some(previous_authoritative_data_store) => {
                                            let previous_trip_update_id = match previous_authoritative_data_store.trip_updates_lookup_by_trip_id_to_trip_update_ids.get(trip_id.as_str()) {
                                            Some(trip_updates_lookup_by_trip_id_to_trip_update_ids) => {
                                                let mut matching_ids = trip_updates_lookup_by_trip_id_to_trip_update_ids.iter().filter(
                                                    |possible_match_trip_id| {
                                                        previous_authoritative_data_store
                                                            .trip_updates
                                                            .get(possible_match_trip_id.as_str())
                                                            .map_or(false, |possible_old_trip| {
                                                                possible_old_trip.trip == trip_descriptor
                                                            })
                                                    },
                                                );
                                                
                                                let first_match = matching_ids.next();
                                                if first_match.is_some() && matching_ids.next().is_none() {
                                                    first_match.cloned()
                                                } else {
                                                    None
                                                }
                                            }
                                            None => None,
                                        };

                                            match previous_trip_update_id {
                                                Some(previous_trip_update_id) => {
                                                    let trip_update =
                                                        previous_authoritative_data_store
                                                            .trip_updates
                                                            .get(&previous_trip_update_id);

                                                    match trip_update {
                                                        Some(trip_update) => {
                                                            let old_stop_time_update = trip_update
                                                                .stop_time_update
                                                                .iter()
                                                                .filter(|old_stu| {
                                                                    match &old_stu.stop_id {
                                                                        Some(old_stu_stop_id) => {
                                                                            !new_stop_ids.contains(
                                                                                old_stu_stop_id
                                                                                    .as_str(),
                                                                            )
                                                                        }
                                                                        None => false,
                                                                    }
                                                                })
                                                                .cloned()
                                                                .map(|old_stu| {
                                                                    let mut old_stu =
                                                                        old_stu.clone();
                                                                    old_stu.old_rt_data = true;
                                                                    old_stu
                                                                })
                                                                .collect::<Vec<_>>();
                                                            Some(old_stop_time_update)
                                                        }
                                                        None => None,
                                                    }
                                                }
                                                None => None,
                                            }
                                        }
                                        None => None,
                                    }
                                }
                                None => None,
                            };

                        let stop_time_update = match old_data_to_add_to_start {
                            Some(old_data_to_add_to_start) => {
                                let new_vec = old_data_to_add_to_start
                                    .into_iter()
                                    .chain(stop_time_update.into_iter())
                                    .collect::<Vec<_>>();
                                new_vec
                            }
                            None => stop_time_update,
                        };

                        let delay = calculate_delay(
                            trip_update.delay,
                            &trip_update.trip.start_date,
                            scheduled_stop_ids_hashset,
                            itinerary_rows,
                            itinerary_meta,
                            &stop_time_update,
                            current_time_unix_timestamp,
                            compressed_trip,
                            &calendar_structure,
                            timezone,
                        );

                        let trip_update = AspenisedTripUpdate {
                            trip: trip_descriptor,
                            vehicle: trip_update.vehicle.clone().map(|x| x.into()),
                            trip_headsign: trip_headsign.clone(),
                            found_schedule_trip_id: compressed_trip.is_some(),
                            stop_time_update: stop_time_update,
                            timestamp: trip_update.timestamp,
                            delay: delay,
                            trip_properties: trip_update.trip_properties.clone().map(|x| x.into()),
                            last_seen: catenary::duration_since_unix_epoch().as_millis() as u64,
                        };

                        let trip_update = match REALTIME_FEEDS_TO_USE_VEHICLE_IDS
                            .contains(&realtime_feed_id.as_str())
                        {
                            true => trip_update.replace_vehicle_label_with_vehicle_id(),
                            false => trip_update,
                        };

                        if let Some(trip_properties) = &trip_update.trip_properties {
                            if let Some(trip_id) = &trip_properties.trip_id {
                                trip_updates_lookup_by_trip_id_to_trip_update_ids
                                    .entry(trip_id.clone().into())
                                    .and_modify(|x| {
                                        x.push(CompactString::new(&trip_update_entity.id))
                                    })
                                    .or_insert(vec![CompactString::new(&trip_update_entity.id)]);
                            }
                        }

                        if let Some(trip_id_modified) = &trip_update.trip.modified_trip {
                            if let Some(trip_id_modified) = &trip_id_modified.affected_trip_id {
                                trip_updates_lookup_by_trip_id_to_trip_update_ids
                                    .entry(trip_id_modified.clone().into())
                                    .and_modify(|x| {
                                        x.push(CompactString::new(&trip_update_entity.id))
                                    })
                                    .or_insert(vec![CompactString::new(&trip_update_entity.id)]);
                            }
                        }

                        if trip_id.is_some() {
                            trip_updates_lookup_by_trip_id_to_trip_update_ids
                                .entry(trip_id.as_ref().unwrap().into())
                                .and_modify(|x| x.push(CompactString::new(&trip_update_entity.id)))
                                .or_insert(vec![CompactString::new(&trip_update_entity.id)]);

                            if let Some(route_id) = &trip_update.trip.route_id {
                                trip_updates_lookup_by_route_id_to_trip_update_ids
                                    .entry(route_id.into())
                                    .and_modify(|x| {
                                        x.push(CompactString::new(&trip_update_entity.id))
                                    })
                                    .or_insert(vec![CompactString::new(&trip_update_entity.id)]);
                            }
                        }

                        if let Some(trip_properties) = &trip_update.trip_properties {
                            if let Some(trip_id_in_properties) = &trip_properties.trip_id {
                                if let Some(route_id) = &trip_update.trip.route_id {
                                    trip_updates_lookup_by_route_id_to_trip_update_ids
                                        .entry(route_id.clone().into())
                                        .and_modify(|x| {
                                            x.push(CompactString::new(&trip_update_entity.id))
                                        })
                                        .or_insert(vec![CompactString::new(
                                            &trip_update_entity.id,
                                        )]);
                                }

                                if missing_trip_ids.contains(trip_id_in_properties) {
                                    let vehicle_entity_ids =
                                        trip_id_to_vehicle_gtfs_rt_id.get(trip_id_in_properties);

                                    if let Some(vehicle_entity_ids) = vehicle_entity_ids {
                                        for vehicle_entity_id in vehicle_entity_ids {
                                            if let Some(vehicle_pos) = aspenised_vehicle_positions
                                                .get_mut(vehicle_entity_id)
                                            {
                                                if let Some(trip_assigned) = &mut vehicle_pos.trip {
                                                    trip_assigned.trip_headsign = trip_headsign
                                                        .clone()
                                                        .map(|x| x.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        trip_updates
                            .insert(CompactString::new(&trip_update_entity.id), trip_update);

                        // If a trip is missing from the static schedule, attempt to recover headsign
                        // information from the realtime update and propagate it to vehicle entities.
                        if let Some(trip_id) = &trip_id {
                            if missing_trip_ids.contains(trip_id) {
                                let vehicle_entity_ids = trip_id_to_vehicle_gtfs_rt_id.get(trip_id);

                                if let Some(vehicle_entity_ids) = vehicle_entity_ids {
                                    for vehicle_entity_id in vehicle_entity_ids {
                                        if let Some(vehicle_pos) =
                                            aspenised_vehicle_positions.get_mut(vehicle_entity_id)
                                        {
                                            if let Some(trip_assigned) = &mut vehicle_pos.trip {
                                                trip_assigned.trip_headsign =
                                                    trip_headsign.clone().map(|x| x.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some(shape) = &trip_update_entity.shape {
                        if let Some(shape_id) = &shape.shape_id {
                            shape_id_to_shape.insert(
                                shape_id.clone().into(),
                                shape.encoded_polyline.as_ref().map(|x| x.clone().into()),
                            );
                        }
                    }

                    if let Some(trip_modification) = &trip_update_entity.trip_modifications {
                        trip_modifications.insert(
                            trip_update_entity.id.clone().into(),
                            trip_modification.clone().into(),
                        );

                        for selected_trip in trip_modification.selected_trips.iter() {
                            for trip_id in selected_trip.trip_ids.iter() {
                                trip_id_to_trip_modification_ids
                                    .entry(trip_id.clone().into())
                                    .and_modify(|x| x.push(trip_update_entity.id.clone().into()))
                                    .or_insert(vec![trip_update_entity.id.clone().into()]);
                            }
                        }

                        for modification in trip_modification.modifications.iter() {
                            for stop_time_update in modification.replacement_stops.iter() {
                                if let Some(stop_id) = &stop_time_update.stop_id {
                                    stop_id_to_trip_modification_ids
                                        .entry(stop_id.clone().into())
                                        .and_modify(|x| {
                                            x.push(trip_update_entity.id.clone().into())
                                        })
                                        .or_insert(vec![trip_update_entity.id.clone().into()]);
                                }
                            }
                        }
                    }

                    if let Some(stop_rt) = &trip_update_entity.stop {
                        if let Some(stop_id) = &stop_rt.stop_id {
                            stop_id_to_stop.entry(stop_id.clone().into()).or_insert(
                                AspenisedStop {
                                    stop_id: stop_id.clone().into(),
                                    stop_name: stop_rt.stop_name.clone().map(|x| x.into()),
                                    stop_code: stop_rt.stop_code.clone().map(|x| x.into()),
                                    tts_stop_name: stop_rt.tts_stop_name.clone().map(|x| x.into()),
                                    stop_desc: stop_rt.stop_desc.clone().map(|x| x.into()),
                                    stop_lat: stop_rt.stop_lat,
                                    stop_lon: stop_rt.stop_lon,
                                    zone_id: stop_rt.zone_id.clone(),
                                    stop_url: stop_rt.stop_url.clone().map(|x| x.into()),
                                    parent_station: stop_rt.parent_station.clone(),
                                    stop_timezone: stop_rt.stop_timezone.clone(),
                                    wheelchair_boarding: stop_rt.wheelchair_boarding,
                                    level_id: stop_rt.level_id.clone(),
                                    platform_code: stop_rt.platform_code.clone().map(|x| x.into()),
                                },
                            );
                        }
                    }
                }
            }

            if let Some(alert_updates_gtfs_rt) = authoritative_gtfs_rt
                .get_async(&(realtime_feed_id.clone(), GtfsRtType::Alerts))
                .await
            {
                let alert_updates_gtfs_rt = alert_updates_gtfs_rt.get();

                for alert_entity in alert_updates_gtfs_rt.entity.iter() {
                    if let Some(alert) = &alert_entity.alert {
                        let alert_id = alert_entity.id.clone();
                        let aspenised_alert: AspenisedAlert = alert.clone().into();

                        let processed_alert =
                            crate::alerts_processing::process_alert(aspenised_alert, chateau_id);

                        alerts.insert(alert_id.clone(), processed_alert);
                    }
                }
            }
        }

        alerts = crate::alerts_processing::deduplicate_alerts(alerts);

        for (alert_id, alert) in alerts.iter() {
            crate::alerts_processing::index_alert(
                alert,
                alert_id,
                &mut impacted_route_id_to_alert_ids,
                &mut impact_trip_id_to_alert_ids,
            );
        }

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
                        agency_id: route.agency_id.clone(),
                    },
                );
            }
        }

        for (key, vehicle_gtfs) in aspenised_vehicle_positions.iter() {
            if let Some(vehicle_data) = &vehicle_gtfs.vehicle {
                if let Some(label) = &vehicle_data.label {
                    gtfs_vehicle_labels_to_ids.insert(label.clone(), key.clone());
                } else {
                    if let Some(id) = &vehicle_data.id {
                        gtfs_vehicle_labels_to_ids.insert(id.clone(), key.clone());
                    }
                }
            }
        }

        // println!(
        //     "Finished processing {} chateau took {:?} for route lookup, {:?} for trips, {:?} for itin meta, {:?} for itin rows",
        //     chateau_id,
        //     routes_query_elapsed,
        //     trip_duration,
        //     itin_lookup_duration,
        //     itinerary_pattern_row_duration
        // );
    }

    // Resolve trip delays after all initial processing is complete.
    // This allows us to link trip updates that were processed separately from the vehicle positions.
    for (k, v) in aspenised_vehicle_positions.iter_mut() {
        {
            let trip = v.trip.as_mut();

            if let Some(trip) = trip {
                if let Some(trip_id) = &trip.trip_id {
                    let trip_update_ids =
                        trip_updates_lookup_by_trip_id_to_trip_update_ids.get(trip_id.as_str());

                    if let Some(trip_update_ids) = trip_update_ids {
                        let possible_trip_updates = trip_update_ids
                            .iter()
                            .map(|update_id| trip_updates.get(update_id))
                            .filter(|x| x.is_some())
                            .map(|x| x.unwrap())
                            .collect::<Vec<_>>();

                        if possible_trip_updates.len() > 0 {
                            let trip_update = match possible_trip_updates.len() {
                                1 => Some(possible_trip_updates[0]),
                                _ => possible_trip_updates
                                    .iter()
                                    .filter(|possible_match| {
                                        possible_match.trip.start_date == trip.start_date.clone()
                                            && possible_match.trip.start_time
                                                == trip.start_time.clone()
                                    })
                                    .next()
                                    .map(|v| &**v),
                            };

                            if let Some(trip_update) = trip_update {
                                if let Some(delay) = trip_update.delay {
                                    trip.delay = Some(delay);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Explicitly shrink hashmaps to release memory back to the allocator,
    // critical for long-running processes handling high-volume RT feeds.
    aspenised_vehicle_positions.shrink_to_fit();
    vehicle_routes_cache.shrink_to_fit();
    trip_updates.shrink_to_fit();

    for (_, x) in trip_updates_lookup_by_trip_id_to_trip_update_ids.iter_mut() {
        {
            if x.len() > 1 {
                x.sort();
                x.dedup();
            }
        }
    }

    trip_updates_lookup_by_trip_id_to_trip_update_ids.shrink_to_fit();
    alerts.shrink_to_fit();
    impacted_route_id_to_alert_ids.shrink_to_fit();
    impacted_stop_id_to_alert_ids.shrink_to_fit();
    impact_trip_id_to_alert_ids.shrink_to_fit();
    general_alerts.shrink_to_fit();
    gtfs_vehicle_labels_to_ids.shrink_to_fit();
    compressed_trip_internal_cache
        .compressed_trips
        .shrink_to_fit();

    if let Some(previous_data) = &previous_authoritative_data_store {
        let current_time = catenary::duration_since_unix_epoch().as_millis() as u64;
        let start_time = START_TIME.get_async(chateau_id).await.map(|t| *t.get());

        if let Some(start_time) = start_time {
            if start_time.elapsed() > DROP_OLD_TRIPS_GRACE_PERIOD {
                // If we are past the grace period, rely solely on the new authoritative data.
            } else {
                // Within the grace period, preserve old trip updates not present in the new feed
                // to prevent UI flickering during transient data drops.
                for (trip_update_id, old_trip_update) in &previous_data.trip_updates {
                    if !trip_updates.contains_key(trip_update_id) {
                        trip_updates.insert(trip_update_id.clone(), old_trip_update.clone());

                        if let Some(trip_id) = &old_trip_update.trip.trip_id {
                            trip_updates_lookup_by_trip_id_to_trip_update_ids
                                .entry(trip_id.clone().into())
                                .and_modify(|x| x.push(trip_update_id.clone()))
                                .or_insert(vec![trip_update_id.clone()]);
                        }

                        if let Some(trip_properties) = &old_trip_update.trip_properties {
                            if let Some(trip_id) = &trip_properties.trip_id {
                                trip_updates_lookup_by_trip_id_to_trip_update_ids
                                    .entry(trip_id.clone().into())
                                    .and_modify(|x| x.push(trip_update_id.clone()))
                                    .or_insert(vec![trip_update_id.clone()]);
                            }
                        }

                        if let Some(trip_id_modified) = &old_trip_update.trip.modified_trip {
                            if let Some(trip_id_modified) = &trip_id_modified.affected_trip_id {
                                trip_updates_lookup_by_trip_id_to_trip_update_ids
                                    .entry(trip_id_modified.clone().into())
                                    .and_modify(|x| x.push(trip_update_id.clone()))
                                    .or_insert(vec![trip_update_id.clone()]);
                            }
                        }

                        if let Some(route_id) = &old_trip_update.trip.route_id {
                            trip_updates_lookup_by_route_id_to_trip_update_ids
                                .entry(route_id.clone().into())
                                .and_modify(|x| x.push(trip_update_id.clone()))
                                .or_insert(vec![trip_update_id.clone()]);
                        }

                        if !old_trip_update.found_schedule_trip_id {
                            for stop_time_update in &old_trip_update.stop_time_update {
                                if let Some(stop_id) = &stop_time_update.stop_id {
                                    stop_id_to_non_scheduled_trip_ids
                                        .entry(CompactString::new(stop_id))
                                        .and_modify(|x| {
                                            x.push(EcoString::from(trip_update_id.as_str()))
                                        })
                                        .or_insert(vec![EcoString::from(trip_update_id.as_str())]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let fast_hash_of_routes =
        catenary::fast_hash(&vehicle_routes_cache.iter().collect::<BTreeMap<_, _>>());

    let aspenised_data = AspenisedData {
        vehicle_positions: aspenised_vehicle_positions,
        vehicle_routes_cache: vehicle_routes_cache,
        vehicle_routes_cache_hash: fast_hash_of_routes,
        trip_updates: trip_updates,
        trip_updates_lookup_by_trip_id_to_trip_update_ids:
            trip_updates_lookup_by_trip_id_to_trip_update_ids,
        aspenised_alerts: alerts,
        impacted_routes_alerts: impacted_route_id_to_alert_ids,
        impacted_stops_alerts: AHashMap::new(),
        vehicle_label_to_gtfs_id: gtfs_vehicle_labels_to_ids,
        impacted_trips_alerts: impact_trip_id_to_alert_ids,
        compressed_trip_internal_cache,
        itinerary_pattern_internal_cache: ItineraryPatternInternalCache {
            itinerary_patterns: accumulated_itinerary_patterns,
            last_time_full_refreshed: chrono::Utc::now(),
        },
        last_updated_time_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
        trip_updates_lookup_by_route_id_to_trip_update_ids:
            trip_updates_lookup_by_route_id_to_trip_update_ids,
        trip_id_to_vehicle_gtfs_rt_id: trip_id_to_vehicle_gtfs_rt_id,
        stop_id_to_stop,
        shape_id_to_shape,
        trip_modifications: trip_modifications,
        trip_id_to_trip_modification_ids,
        stop_id_to_trip_modification_ids,
        stop_id_to_non_scheduled_trip_ids,
        stop_id_to_parent_id,
        parent_id_to_children_ids,
    };

    // Insert the aspenised data - clone only for persistence, move into map when possible
    let aspenised_data_for_persist = aspenised_data.clone();
    authoritative_data_store
        .entry_async(chateau_id.to_string())
        .await
        .and_modify(|d| *d = aspenised_data_for_persist.clone())
        .or_insert(aspenised_data);

    let should_save = match LAST_SAVE_TIME.get_async(chateau_id).await {
        Some(last_save) => Instant::now().duration_since(*last_save.get()) > SAVE_INTERVAL,
        None => true,
    };

    if should_save {
        if let Err(e) = persistence::save_chateau_data(chateau_id, &aspenised_data_for_persist) {
            eprintln!("Failed to save chateau data for {}: {}", chateau_id, e);
        } else {
            LAST_SAVE_TIME
                .entry_async(chateau_id.to_string())
                .await
                .and_modify(|t| *t = Instant::now())
                .or_insert(Instant::now());
        }
    }

    println!("Updated Chateau {}", chateau_id);

    Ok(true)
}

//Assisted-by: Gemini 3 via Google Antigravity
