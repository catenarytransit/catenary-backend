// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// Are you a contributor? Or just an academic researching our algorithms? Please contact kyler@catenarymaps.org

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
use crate::trip_id_assigner_with_known_route_and_known_trip_updates;
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

use diesel::dsl::sql;
use diesel::sql_types::Text;

const ALLOWED_CHATEAUX: &[&str] = &[
    "deutschland",
    "sncf",
    "nationalrailuk",
    "schweiz",
    "île~de~france~mobilités",
    "sncb",
    "tisséo",
    "vbb",
];

#[derive(Clone, Debug)]
struct NyctRtTripContext {
    route_id: Option<String>,
    direction_id: Option<u32>,
    start_date: Option<chrono::NaiveDate>,
}

fn is_nyct_subway_feed(chateau_id: &str, realtime_feed_id: &str) -> bool {
    chateau_id == "nyct"
        && matches!(
            realtime_feed_id,
            "f-mta~nyc~rt~subway~1~2~3~4~5~6~7"
                | "f-mta~nyc~rt~subway~a~c~e"
                | "f-mta~nyc~rt~subway~b~d~f~m"
                | "f-mta~nyc~rt~subway~g"
                | "f-mta~nyc~rt~subway~j~z"
                | "f-mta~nyc~rt~subway~l"
                | "f-mta~nyc~rt~subway~n~q~r~w"
                | "f-mta~nyc~rt~subway~sir"
        )
}

fn parse_gtfs_rt_date(date: &Option<String>) -> Option<chrono::NaiveDate> {
    date.as_ref()
        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y%m%d").ok())
}

fn index_trip_update_id(
    lookup: &mut AHashMap<CompactString, Vec<CompactString>>,
    trip_id: &str,
    entity_id: &str,
) {
    let entity_id = CompactString::new(entity_id);

    lookup
        .entry(CompactString::new(trip_id))
        .and_modify(|ids| {
            if !ids.iter().any(|id| id.as_str() == entity_id.as_str()) {
                ids.push(entity_id.clone());
            }
        })
        .or_insert(vec![entity_id]);
}

use catenary::compact_formats::{
    CompactFeedMessage, CompactItineraryPatternRow, CompactStopTimeUpdate,
};
use lazy_static::lazy_static;

use prost::Message;
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

//woah! Huge function! Kyler here to explain! Yes HUMAN KYLER! not a robot!
//There's two data stores, we have one for finished processed data, and one for incoming raw GTFS-realtime
//we just copy the internal data structs between those two, and calculate and add a few extra fields
//For example, the delay
//then we write some indexes, so that way the realtime data can quickly be found by certain IDs
pub async fn new_rt_data(
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    authoritative_trajectory_data_store: Arc<
        SccHashMap<String, catenary::aspen_dataset::AspenTrajectoryStore>,
    >,
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
    authoritative_nyct_subway_data_cache: Arc<
        tokio::sync::RwLock<
            Option<
                std::collections::HashMap<
                    String,
                    catenary::agency_specific_types::mta_subway::Trip,
                >,
            >,
        >,
    >,
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

    let mut darwin_trip_id_to_formation: AHashMap<
        String,
        crate::consist_cache_and_conversion::DarwinScheduleFormations,
    > = AHashMap::new();

    let mut darwin_trip_id_to_formation_v1: AHashMap<
        String,
        crate::consist_cache_and_conversion::DarwinScheduleFormationsV1,
    > = AHashMap::new();

    let mut metrolinx_trip_id_to_consist: AHashMap<
        String,
        crate::consist_cache_and_conversion::MetrolinxTrip,
    > = AHashMap::new();

    if chateau_id == "gotransit" || chateau_id == "upexpress" {
        let client = reqwest::Client::new();
        let metrolinx_res = client
            .get("https://metrolinx-train-go.transitspotter.com/get-trains/all")
            .send()
            .await;
        if let Ok(res) = metrolinx_res {
            if let Ok(data) = res
                .json::<crate::consist_cache_and_conversion::MetrolinxTrainResponse>()
                .await
            {
                for trip in data.trips {
                    if let Some(trip_id) = &trip.trip_id {
                        metrolinx_trip_id_to_consist.insert(trip_id.to_string(), trip);
                    }
                }
            } else {
                println!("Failed to parse Metrolinx train json");
            }
        } else {
            println!("Failed to fetch Metrolinx trains from transitspotter");
        }
    }

    if chateau_id == "nationalrailuk" {
        let darwin_url =
            std::env::var("DARWIN_RT_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
        let client = reqwest::Client::new();

        let rid_res = client
            .get(format!("{}/rid-to-trip-id", darwin_url))
            .send()
            .await;
        let form_res = client
            .get(format!("{}/formations", darwin_url))
            .send()
            .await;
        let form_v1_res = client
            .get(format!("{}/formations-v1", darwin_url))
            .send()
            .await;

        if let (Ok(r_resp), Ok(f_resp), Ok(fv1_resp)) = (rid_res, form_res, form_v1_res) {
            if let (Ok(r_data), Ok(f_data), Ok(fv1_data)) =
                (
                    r_resp.json::<AHashMap<String, String>>().await,
                    f_resp
                        .json::<AHashMap<
                            String,
                            crate::consist_cache_and_conversion::DarwinScheduleFormations,
                        >>()
                        .await,
                    fv1_resp
                        .json::<AHashMap<
                            String,
                            crate::consist_cache_and_conversion::DarwinScheduleFormationsV1,
                        >>()
                        .await,
                )
            {
                for (rid, trip_id) in r_data {
                    if let Some(formation) = f_data.get(&rid) {
                        darwin_trip_id_to_formation.insert(trip_id.clone(), formation.clone());
                    }
                    if let Some(formation_v1) = fv1_data.get(&rid) {
                        darwin_trip_id_to_formation_v1.insert(trip_id, formation_v1.clone());
                    }
                }
            }
        } else {
            println!("Failed to fetch Darwin formation data endpoints.");
        }
    }

    let conn_pool = pool.as_ref();
    let mut conn_pre = conn_pool.get().await;

    let fetched_track_data: TrackData = fetch_track_data(&chateau_id, &pool).await;

    match &fetched_track_data {
        TrackData::MetroNorthRailroad(x) => {
            match &x {
                Some(x) => {
                    //println!("Metro North Consist data debug: {:#?}", &x.consist_lookup);
                }
                _ => {
                    println!("")
                }
            };
        }
        TrackData::LongIslandRailroad(x) => {
            match &x {
                Some(x) => {
                    //println!("LIRR Consist data debug: {:#?}", &x.consist_lookup);
                }
                _ => {
                    println!("No track data found for LIRR");
                }
            };
        }
        _ => {}
    };

    let mut conn = conn_pre;

    if let Err(e) = &conn {
        println!("Error with connecting to postgres");
        eprintln!("{:#?}", e);
        return Ok(false);
    }

    let conn = &mut conn.unwrap();

    let mut assigned_dresden_trips = std::collections::HashMap::new();
    if chateau_id == "deutschland" && realtime_feed_id == "f-tlms~rt" {
        match trip_id_assigner_with_known_route_and_known_trip_updates::assign_trips_for_dresden(
            &authoritative_gtfs_rt,
            conn,
        )
        .await
        {
            Ok(trips) => {
                assigned_dresden_trips = trips;
                println!(
                    "Finished assigning trips for f-tlms~rt! Assigned {} trips.",
                    assigned_dresden_trips.len()
                );
            }
            Err(e) => {
                eprintln!("Error in assign_trips_for_dresden: {:?}", e);
            }
        }
    }

    //don't stress about this! We are just making a lot of hashmaps that will become the future authoriative version of the realtime data
    //you'll also see a lot of indexes here (indicies)
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

    //You may have seen this before, but it's basically importing our postgres schema so we can access our tables!
    use catenary::schema::gtfs::chateaus as chateaus_pg_schema;
    use catenary::schema::gtfs::routes as routes_pg_schema;

    // let's first query the metadata for the chateau
    let start_chateau_query = Instant::now();
    let this_chateau = chateaus_pg_schema::dsl::chateaus
        .filter(chateaus_pg_schema::dsl::chateau.eq(&chateau_id))
        .first::<catenary::models::Chateau>(conn)
        .await?;
    let chateau_elapsed = start_chateau_query.elapsed();

    let is_europe = diesel::select(diesel::dsl::sql::<diesel::sql_types::Bool>(&format!(
        "EXISTS(SELECT 1 FROM gtfs.chateaus WHERE chateau = '{}' AND ST_Intersects(hull, ST_MakeEnvelope(-28.4207529, 34.31734126604816, 40.85603290981834, 71.4754084, 4326)))",
        chateau_id
    )))
    .get_result::<bool>(conn)
    .await
    .unwrap_or(false);

    //we want all the routes for this chateau
    let start_routes_query = Instant::now();
    let routes: Vec<catenary::models::Route> = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await?;
    let routes_query_elapsed = start_routes_query.elapsed();

    //It's stranged to have a good transit agency with no routes! What is this, Irvine?
    //This is logged just so we know, but perhaps it is not a crash.
    //Some GTFS files may be considered valid with no data because of agencies without service during off-season!
    if routes.is_empty() {
        println!("No routes found for chateau {}", chateau_id);
    }

    //this is so we can quickly lookup the route information here by id
    let mut route_id_to_route: AHashMap<String, catenary::models::Route> = AHashMap::new();

    for route in routes {
        route_id_to_route.insert(route.route_id.clone(), route);
    }

    //lock this table so it can't be edited anymore!
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

    // Collects all Trip IDs referenced in the RT feeds (Vehicles, TripUpdates, Alerts)
    // to perform a single batch lookup against the cache/DB!
    let mut trip_ids_to_lookup: AHashSet<String> = AHashSet::new();
    let mut nyct_rt_trip_contexts: AHashMap<String, NyctRtTripContext> = AHashMap::new();

    for realtime_feed_id in this_chateau.realtime_feeds.iter().flatten() {
        if let Some(vehicle_gtfs_rt_for_feed_id) = authoritative_gtfs_rt
            .get_async(&(realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
            .await
        {
            let vehicle_gtfs_rt_for_feed_id = vehicle_gtfs_rt_for_feed_id.get();

            for vehicle_entity in vehicle_gtfs_rt_for_feed_id.entity.iter() {
                if let Some(vehicle_pos) = &vehicle_entity.vehicle {
                    let assigned_trip_id =
                        if chateau_id == "deutschland" && realtime_feed_id == "f-tlms~rt" {
                            vehicle_pos
                                .vehicle
                                .as_ref()
                                .and_then(|v| v.id.as_ref())
                                .and_then(|vid| assigned_dresden_trips.get(vid.as_str()).cloned())
                        } else {
                            None
                        };

                    if let Some(trip_id) = assigned_trip_id {
                        trip_ids_to_lookup.insert(trip_id);
                    } else if let Some(trip) = &vehicle_pos.trip {
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
                    if is_nyct_subway_feed(chateau_id, realtime_feed_id.as_str()) {
                        if let Some(trip_id) = &trip_update.trip.trip_id {
                            nyct_rt_trip_contexts
                                .entry(trip_id.clone())
                                .or_insert_with(|| NyctRtTripContext {
                                    route_id: trip_update.trip.route_id.clone(),
                                    direction_id: trip_update.trip.direction_id,
                                    start_date: parse_gtfs_rt_date(&trip_update.trip.start_date),
                                });
                        }
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

        let mut is_subway_feed_nyc = false;
        //let mut second_decode_nyc_subway: Option<catenary::mta_gtfs_rt::nyct::FeedMessage> = None;
        let mut second_decode_nyc_subway_sorted_by_trip_update_id: Option<
            std::collections::HashMap<String, catenary::mta_gtfs_rt::nyct::FeedEntity>,
        > = None;

        //New York City Subway specific, identify is it a subway feed?
        if chateau_id == "nyct"
            && matches!(
                realtime_feed_id.as_str(),
                "f-mta~nyc~rt~subway~1~2~3~4~5~6~7"
                    | "f-mta~nyc~rt~subway~a~c~e"
                    | "f-mta~nyc~rt~subway~b~d~f~m"
                    | "f-mta~nyc~rt~subway~g"
                    | "f-mta~nyc~rt~subway~j~z"
                    | "f-mta~nyc~rt~subway~l"
                    | "f-mta~nyc~rt~subway~n~q~r~w"
                    | "f-mta~nyc~rt~subway~sir"
            )
        {
            is_subway_feed_nyc = true;

            // A second decoder is used to extract the special track numbers and train ids

            let client = reqwest::Client::new();

            let url_to_fetch = catenary::mta_gtfs_rt::URLS_LOOKUP
                .iter()
                .find(|(id, _)| *id == realtime_feed_id.as_str())
                .map(|(_, url)| *url);

            if let Some(url) = url_to_fetch {
                let response = client.get(url).send().await;
                println!("Fetching NYCT subway for second decoder from url: {}", url);

                match response {
                    Ok(response) => {
                        if response.status().is_success() {
                            if let Ok(bytes) = response.bytes().await {
                                match catenary::mta_gtfs_rt::nyct::FeedMessage::decode(
                                    bytes.as_ref(),
                                ) {
                                    Ok(feed) => {
                                        // second_decode_nyc_subway = Some(feed);

                                        let mut table = std::collections::HashMap::new();

                                        for entity in feed.entity {
                                            table.insert(entity.id.clone(), entity.clone());
                                        }

                                        second_decode_nyc_subway_sorted_by_trip_update_id =
                                            Some(table);
                                    }
                                    Err(e) => {
                                        println!(
                                            "Error decoding NYCT subway feed for second decoder: {}",
                                            e
                                        );
                                    }
                                }
                            } else {
                                println!(
                                    "Error getting bytes from NYCT subway feed response for second decoder"
                                );
                            }
                        } else {
                            println!(
                                "Non-success status code {} when fetching NYCT subway feed for second decoder",
                                response.status()
                            );
                        }
                    }
                    Err(e) => {
                        println!("Error fetching NYCT subway feed for second decoder: {}", e);
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

        if chateau_id == "sncf" {
            if let TrackData::Sncf(Some(sncf_data)) = &fetched_track_data {
                let sncf_train_nums = sncf_data
                    .track_lookup
                    .keys()
                    .map(|x| x.as_str())
                    .collect::<Vec<&str>>();

                if !sncf_train_nums.is_empty() {
                    let missing_sncf_trips =
                        catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                            .filter(
                                catenary::schema::gtfs::trips_compressed::dsl::chateau
                                    .eq(chateau_id),
                            )
                            .filter(
                                catenary::schema::gtfs::trips_compressed::dsl::trip_short_name
                                    .eq_any(&sncf_train_nums),
                            )
                            .load::<catenary::models::CompressedTrip>(conn)
                            .await?;

                    let candidate_service_ids = missing_sncf_trips
                        .iter()
                        .map(|trip| trip.service_id.to_string())
                        .collect::<AHashSet<String>>()
                        .into_iter()
                        .collect::<Vec<String>>();

                    let candidate_calendars = catenary::schema::gtfs::calendar::dsl::calendar
                        .filter(catenary::schema::gtfs::calendar::dsl::chateau.eq(chateau_id))
                        .filter(
                            catenary::schema::gtfs::calendar::dsl::service_id
                                .eq_any(&candidate_service_ids),
                        )
                        .load::<catenary::models::Calendar>(conn)
                        .await?;

                    let candidate_calendar_dates =
                        catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                            .filter(
                                catenary::schema::gtfs::calendar_dates::dsl::chateau.eq(chateau_id),
                            )
                            .filter(
                                catenary::schema::gtfs::calendar_dates::dsl::service_id
                                    .eq_any(&candidate_service_ids),
                            )
                            .load::<catenary::models::CalendarDate>(conn)
                            .await?;

                    let calendar_structure =
                        catenary::make_calendar_structure_from_pg_single_chateau(
                            candidate_calendars,
                            candidate_calendar_dates,
                        );

                    use std::str::FromStr;
                    let tz = chrono_tz::Tz::from_str("Europe/Paris").unwrap_or(chrono_tz::UTC);
                    let today = chrono::Utc::now().with_timezone(&tz).naive_local().date();

                    let mut trips_by_short_name: AHashMap<
                        String,
                        Vec<catenary::models::CompressedTrip>,
                    > = AHashMap::new();
                    for trip in missing_sncf_trips {
                        if let Some(short_name) = &trip.trip_short_name {
                            trips_by_short_name
                                .entry(short_name.to_string())
                                .or_default()
                                .push(trip);
                        }
                    }

                    for (_, mut trips) in trips_by_short_name {
                        let mut active_trip = trips
                            .iter()
                            .find(|trip| {
                                calendar_structure
                                    .get(trip.service_id.as_str())
                                    .map(|service| catenary::datetime_in_service(service, today))
                                    .unwrap_or(false)
                            })
                            .cloned();

                        if active_trip.is_none() && trips.len() == 1 {
                            active_trip = Some(trips.pop().unwrap());
                        }

                        if let Some(trip) = active_trip {
                            trip_ids_to_lookup.insert(trip.trip_id.clone());
                            trip_id_to_trip.insert(trip.trip_id.clone(), trip);
                        }
                    }
                }
            }
        }

        let mut nyct_rt_trip_id_to_static_trip_ids: AHashMap<String, Vec<String>> = AHashMap::new();

        if chateau_id == "nyct" {
            let missing_nyct_trip_ids = trip_ids_to_lookup
                .iter()
                .filter(|id| {
                    !trip_id_to_trip.contains_key(id.as_str())
                        && nyct_rt_trip_contexts.contains_key(id.as_str())
                })
                .cloned()
                .collect::<Vec<String>>();

            if !missing_nyct_trip_ids.is_empty() {
                let route_ids_to_match = missing_nyct_trip_ids
                    .iter()
                    .filter_map(|id| nyct_rt_trip_contexts.get(id))
                    .filter_map(|ctx| ctx.route_id.clone())
                    .collect::<AHashSet<String>>()
                    .into_iter()
                    .collect::<Vec<String>>();

                let mut query =
                    catenary::schema::gtfs::trips_compressed::dsl::trips_compressed.into_boxed();

                query = query
                    .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(chateau_id))
                    .filter(
                        sql::<Text>("substr(trip_id, strpos(trip_id, '_') + 1)")
                            .eq_any(&missing_nyct_trip_ids),
                    );

                if !route_ids_to_match.is_empty() {
                    query = query.filter(
                        catenary::schema::gtfs::trips_compressed::dsl::route_id
                            .eq_any(&route_ids_to_match),
                    );
                }

                let nyct_candidate_trips =
                    query.load::<catenary::models::CompressedTrip>(conn).await?;

                let candidate_service_ids = nyct_candidate_trips
                    .iter()
                    .map(|trip| trip.service_id.to_string())
                    .collect::<AHashSet<String>>()
                    .into_iter()
                    .collect::<Vec<String>>();

                let nyct_candidate_calendars = catenary::schema::gtfs::calendar::dsl::calendar
                    .filter(catenary::schema::gtfs::calendar::dsl::chateau.eq(chateau_id))
                    .filter(
                        catenary::schema::gtfs::calendar::dsl::service_id
                            .eq_any(&candidate_service_ids),
                    )
                    .load::<catenary::models::Calendar>(conn)
                    .await?;

                let nyct_candidate_calendar_dates =
                    catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                        .filter(catenary::schema::gtfs::calendar_dates::dsl::chateau.eq(chateau_id))
                        .filter(
                            catenary::schema::gtfs::calendar_dates::dsl::service_id
                                .eq_any(&candidate_service_ids),
                        )
                        .load::<catenary::models::CalendarDate>(conn)
                        .await?;

                let nyct_calendar_structure =
                    catenary::make_calendar_structure_from_pg_single_chateau(
                        nyct_candidate_calendars,
                        nyct_candidate_calendar_dates,
                    );

                let mut candidates_by_rt_id: AHashMap<
                    String,
                    Vec<catenary::models::CompressedTrip>,
                > = AHashMap::new();

                for candidate in nyct_candidate_trips {
                    let suffix = candidate
                        .trip_id
                        .split_once('_')
                        .map(|(_, suffix)| suffix)
                        .unwrap_or(candidate.trip_id.as_str());

                    candidates_by_rt_id
                        .entry(suffix.to_string())
                        .or_default()
                        .push(candidate);
                }

                let mut fixed_id_count = 0usize;

                for rt_trip_id in missing_nyct_trip_ids {
                    let Some(candidates) = candidates_by_rt_id.get(&rt_trip_id) else {
                        continue;
                    };

                    let Some(ctx) = nyct_rt_trip_contexts.get(&rt_trip_id) else {
                        continue;
                    };

                    let mut narrowed = candidates
                        .iter()
                        .filter(|candidate| {
                            ctx.route_id
                                .as_ref()
                                .map(|route_id| candidate.route_id == *route_id)
                                .unwrap_or(true)
                        })
                        .filter(
                            |candidate| match (ctx.direction_id, candidate.direction_id) {
                                // Catenary stores static direction_id as Option<bool>.
                                // false -> 0, true -> 1.
                                (Some(rt_direction_id), Some(static_direction_id)) => {
                                    static_direction_id == (rt_direction_id != 0)
                                }
                                _ => true,
                            },
                        )
                        .collect::<Vec<_>>();

                    if let Some(start_date) = ctx.start_date {
                        let active = narrowed
                            .iter()
                            .copied()
                            .filter(|candidate| {
                                nyct_calendar_structure
                                    .get(candidate.service_id.as_str())
                                    .map(|service| {
                                        catenary::datetime_in_service(service, start_date)
                                    })
                                    .unwrap_or(false)
                            })
                            .collect::<Vec<_>>();

                        if !active.is_empty() {
                            narrowed = active;
                        }
                    }

                    narrowed.sort_by(|a, b| a.trip_id.cmp(&b.trip_id));
                    narrowed.dedup_by(|a, b| a.trip_id == b.trip_id);

                    if narrowed.len() == 1 {
                        let static_trip = (*narrowed[0]).clone();

                        nyct_rt_trip_id_to_static_trip_ids
                            .entry(rt_trip_id.clone())
                            .or_default()
                            .push(static_trip.trip_id.clone());

                        // Critical alias:
                        // RT key "093950_1..S03R" now resolves to static trip metadata
                        // "ASP26GEN-1039-Saturday-00_093950_1..S03R" or whichever
                        // service_id is active for start_date.
                        trip_id_to_trip.insert(rt_trip_id.clone(), static_trip);

                        fixed_id_count += 1;
                    } else {
                        println!(
                            "NYCT trip id suffix {} resolved to {} active static candidates; leaving unresolved",
                            rt_trip_id,
                            narrowed.len()
                        );
                    }
                }

                println!(
                    "Fixed {} NYCT subway realtime trip ids via trip_id suffix + service-date matching",
                    fixed_id_count
                );
            }
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
        let empty_scheduled_stops_option: Option<AHashSet<std::sync::Arc<str>>> = None;

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
                                    stop_ids_to_lookup.insert(last_non_skipped_stop_id.to_string());
                                }
                            }
                        }

                        // Also add ALL stop_ids to lookup so we have their coordinates for trajectories
                        if ALLOWED_CHATEAUX.contains(&chateau_id) {
                            for stu in &trip_update.stop_time_update {
                                if let Some(stop_id) = &stu.stop_id {
                                    stop_ids_to_lookup.insert(stop_id.to_string());
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

        // Populate stop_id_to_stop with fetched stops so trajectory calculation has coordinates
        for (stop_id, stop) in &stop_id_to_stop_from_postgres {
            stop_id_to_stop
                .entry(stop_id.as_str().into())
                .or_insert_with(|| AspenisedStop {
                    stop_id: Some(stop_id.clone()),
                    stop_name: stop.name.clone().map(|x| {
                        catenary::aspen_dataset::AspenTranslatedString {
                            translation: vec![catenary::aspen_dataset::AspenTranslation {
                                text: x,
                                language: None,
                            }],
                        }
                    }),
                    stop_code: stop.code.clone().map(|x| {
                        catenary::aspen_dataset::AspenTranslatedString {
                            translation: vec![catenary::aspen_dataset::AspenTranslation {
                                text: x,
                                language: None,
                            }],
                        }
                    }),
                    tts_stop_name: None,
                    stop_desc: stop.gtfs_desc.clone().map(|x| {
                        catenary::aspen_dataset::AspenTranslatedString {
                            translation: vec![catenary::aspen_dataset::AspenTranslation {
                                text: x,
                                language: None,
                            }],
                        }
                    }),
                    stop_lat: stop.point.clone().map(|p| p.y as f32),
                    stop_lon: stop.point.clone().map(|p| p.x as f32),
                    zone_id: stop.zone_id.clone(),
                    stop_url: stop.url.clone().map(|x| {
                        catenary::aspen_dataset::AspenTranslatedString {
                            translation: vec![catenary::aspen_dataset::AspenTranslation {
                                text: x,
                                language: None,
                            }],
                        }
                    }),
                    parent_station: stop.parent_station.clone(),
                    stop_timezone: stop.timezone.clone(),
                    wheelchair_boarding: None,
                    level_id: stop.level_id.clone(),
                    platform_code: stop.platform_code.clone().map(|x| {
                        catenary::aspen_dataset::AspenTranslatedString {
                            translation: vec![catenary::aspen_dataset::AspenTranslation {
                                text: x,
                                language: None,
                            }],
                        }
                    }),
                });
        }

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
            Option<AHashSet<std::sync::Arc<str>>>,
        > = AHashMap::new();
        for (id, (_, rows)) in &accumulated_itinerary_patterns {
            let set: AHashSet<std::sync::Arc<str>> = rows
                .iter()
                .map(|x| std::sync::Arc::from(x.stop_id.as_str()))
                .collect();
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
                        let assigned_trip_id = if chateau_id == "deutschland"
                            && realtime_feed_id == "f-tlms~rt"
                        {
                            vehicle_pos
                                .vehicle
                                .as_ref()
                                .and_then(|v| v.id.as_ref())
                                .and_then(|vid| assigned_dresden_trips.get(vid.as_str()).cloned())
                        } else {
                            None
                        };

                        let recalculate_route_id: Option<String> = match assigned_trip_id
                            .as_ref()
                            .or(vehicle_pos.trip.as_ref().and_then(|t| t.trip_id.as_ref()))
                        {
                            Some(trip_id) => {
                                let compressed_trip = trip_id_to_trip.get(trip_id);
                                match compressed_trip {
                                    Some(compressed_trip) => {
                                        let route =
                                            route_id_to_route.get(&compressed_trip.route_id);
                                        match route {
                                            Some(route) => Some(route.route_id.clone()),
                                            None => vehicle_pos
                                                .trip
                                                .as_ref()
                                                .and_then(|t| t.route_id.clone()),
                                        }
                                    }
                                    None => {
                                        vehicle_pos.trip.as_ref().and_then(|t| t.route_id.clone())
                                    }
                                }
                            }
                            None => vehicle_pos.trip.as_ref().and_then(|t| t.route_id.clone()),
                        };

                        let mut position = vehicle_pos.position.as_ref().map(|position| {
                            let bearing = match realtime_feed_id.as_str() {
                                "f-mta~nyc~rt~bustime" => {
                                    position.bearing.map(|b| (90.0 - b).rem_euclid(360.0))
                                }
                                _ => position.bearing,
                            };

                            CatenaryRtVehiclePosition {
                                latitude: position.latitude,
                                longitude: position.longitude,
                                bearing: bearing,
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
                            let tid = assigned_trip_id.as_ref().or(trip.trip_id.as_ref())?;
                            trip_id_to_trip.get(tid).and_then(|trip| {
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
                            trip: vehicle_pos.trip.as_ref().map(|trip| {
                                // Prefer the canonical trip_id from the scheduled data when
                                // available (e.g., for Foothill Transit where the realtime
                                // IDs may differ from the database IDs).
                                let corrected_trip_id =
                                    match assigned_trip_id.as_ref().or(trip.trip_id.as_ref()) {
                                        Some(trip_id) => match trip_id_to_trip.get(trip_id) {
                                            Some(static_trip) => Some(static_trip.trip_id.clone()),
                                            None => Some(trip_id.clone()),
                                        },
                                        None => None,
                                    };

                                AspenisedVehicleTripInfo {
                                    trip_id: corrected_trip_id,
                                    direction_id: trip.direction_id,
                                    start_date: start_date,
                                    start_time: trip.start_time.clone(),
                                    schedule_relationship: option_i32_to_schedule_relationship(
                                        &trip.schedule_relationship,
                                    ),
                                    route_id: recalculate_route_id.clone(),
                                    trip_headsign: trip_headsign,
                                    // Use the original realtime trip_id for lookup into the
                                    // scheduled data; the metadata itself (including
                                    // trip_short_name) comes from Postgres.
                                    trip_short_name: match assigned_trip_id
                                        .as_ref()
                                        .or(trip.trip_id.as_ref())
                                    {
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
                                }
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
                            consist: None,
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

                            if let Some(trip_id) =
                                assigned_trip_id.as_ref().or(trip.trip_id.as_ref())
                            {
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
                                        .entry(stop_id.as_ref().into())
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
                                            .get(last_non_cancelled_stop_id.as_ref())
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

                        if chateau_id == "île~de~france~mobilités" {
                            trip_descriptor.start_time = None;
                        }

                        // Normalize the trip_id in the realtime descriptor to the canonical
                        // trip_id from the scheduled data when available. This ensures that
                        // downstream consumers see IDs consistent with the database even when
                        // the GTFS-rt feed uses a slightly different identifier (e.g.,
                        // Foothill Transit).
                        if let Some(td_trip_id) = &trip_descriptor.trip_id {
                            if let Some(static_trip) = trip_id_to_trip.get(td_trip_id) {
                                // Only overwrite when the scheduled ID differs; otherwise
                                // leave the original as-is.
                                if static_trip.trip_id != *td_trip_id {
                                    trip_descriptor.trip_id = Some(static_trip.trip_id.clone());
                                }
                            }
                        }

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

                        let mut consist = None;
                        let mut helium_vehicle_summary_string: Option<String> = None;

                        let mut nyct_stus_copied: Option<
                            Vec<catenary::mta_gtfs_rt::nyct::trip_update::StopTimeUpdate>,
                        > = None;

                        //NYC subway consist analysis
                        if is_subway_feed_nyc {
                            let read_guard = authoritative_nyct_subway_data_cache.read().await;

                            if let Some(ref nyc_subway_data_cache) = *read_guard {
                                if let Some(second_decode_nyc_subway_sorted_by_trip_update_id) =
                                    &second_decode_nyc_subway_sorted_by_trip_update_id
                                {
                                    let nyc_protobuf_version_trip_update =
                                        second_decode_nyc_subway_sorted_by_trip_update_id
                                            .get(&trip_update_entity.id);

                                    if let Some(nyc_protobuf_version_trip_update) =
                                        &nyc_protobuf_version_trip_update
                                    {
                                        //extract the train id

                                        if let Some(nyc_protobuf_trip_update_section) =
                                            &nyc_protobuf_version_trip_update.trip_update
                                        {
                                            nyct_stus_copied = Some(
                                                nyc_protobuf_trip_update_section
                                                    .stop_time_update
                                                    .clone(),
                                            );

                                            if let Some(nyct_train_descriptor) =
                                                &nyc_protobuf_trip_update_section
                                                    .trip
                                                    .nyct_trip_descriptor
                                            {
                                                let nyct_train_id =
                                                    nyct_train_descriptor.train_id.clone();

                                                if let Some(nyct_train_id) = nyct_train_id {
                                                    match nyc_subway_data_cache.get(&nyct_train_id)
                                                    {
                                                        None => {
                                                            println!(
                                                                "This train not found in helium"
                                                            );
                                                        }
                                                        Some(associated_helium_data) => {
                                                            consist = Some(crate::consist_cache_and_conversion::map_nyct_trip_to_consist(&associated_helium_data));

                                                            if let (Some(lat), Some(lon)) = (
                                                                associated_helium_data
                                                                    .estimated_latitude,
                                                                associated_helium_data
                                                                    .estimated_longitude,
                                                            ) {
                                                                let vehicle_id =
                                                                    trip_update_entity.id.clone();

                                                                let vehicle_summary_string =
                                                                    associated_helium_data
                                                                        .get_consist_summary();
                                                                helium_vehicle_summary_string =
                                                                    Some(
                                                                        vehicle_summary_string
                                                                            .clone(),
                                                                    );

                                                                let pos_aspenised = AspenisedVehiclePosition {
                                                                    trip: Some(AspenisedVehicleTripInfo {
                                                                        trip_id: trip_descriptor.trip_id.clone(),
                                                                        direction_id: trip_update.trip.direction_id,
                                                                        start_date: trip_descriptor.start_date.clone(),
                                                                        start_time: trip_update.trip.start_time.clone(),
                                                                        schedule_relationship: option_i32_to_schedule_relationship(&trip_update.trip.schedule_relationship),
                                                                        route_id: trip_descriptor.route_id.clone(),
                                                                        trip_headsign: Some(associated_helium_data.headsign.clone()),
                                                                        trip_short_name: match &trip_id {
                                                                            Some(t_id) => trip_id_to_trip.get(t_id).and_then(|t| t.trip_short_name.as_ref().map(|x| x.to_string())),
                                                                            None => None,
                                                                        },
                                                                        delay: None,
                                                                    }),
                                                                    position: Some(CatenaryRtVehiclePosition {
                                                                        latitude: lat as f32,
                                                                        longitude: lon as f32,
                                                                        bearing: None,
                                                                        odometer: None,
                                                                        speed: None,
                                                                    }),
                                                                    timestamp: associated_helium_data.updated_at.map(|x| x as u64),
                                                                    vehicle: Some(AspenisedVehicleDescriptor {
                                                                        id: Some(vehicle_id.clone()),
                                                                        label: Some(vehicle_summary_string.clone()),
                                                                        license_plate: None,
                                                                        wheelchair_accessible: None,
                                                                    }),
                                                                    route_type: 1, // Standard GTFS Route Type for Subway
                                                                    current_status: None,
                                                                    current_stop_sequence: None,
                                                                    occupancy_status: None,
                                                                    occupancy_percentage: None,
                                                                    congestion_level: None,
                                                                    consist: consist.clone(),
                                                                };

                                                                aspenised_vehicle_positions.insert(
                                                                    vehicle_id.clone(),
                                                                    pos_aspenised,
                                                                );

                                                                if let Some(t_id) =
                                                                    &trip_descriptor.trip_id
                                                                {
                                                                    trip_id_to_vehicle_gtfs_rt_id
                                                                        .entry(t_id.clone())
                                                                        .and_modify(|x| {
                                                                            x.push(
                                                                                vehicle_id.clone(),
                                                                            )
                                                                        })
                                                                        .or_insert(vec![
                                                                            vehicle_id.clone(),
                                                                        ]);
                                                                }

                                                                if let Some(route_id) =
                                                                    &trip_descriptor.route_id
                                                                {
                                                                    route_ids_to_insert
                                                                        .insert(route_id.clone());
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                println!("No NYCT subway data found!");
                            }
                        }

                        let mut stop_time_updates_vec = Vec::new();

                        let mut propagated_delay: Option<i32> = None;

                        let base_midnight_ts = trip_descriptor.start_date.as_ref().and_then(|nd| {
                            timezone
                                .as_ref()
                                .and_then(|tz| {
                                    tz.from_local_datetime(&nd.and_hms_opt(12, 0, 0).unwrap())
                                        .single()
                                })
                                .map(|dt| dt.timestamp() - (12 * 3600))
                        });
                        let compressed_start_time_seconds =
                            compressed_trip.map(|ct| ct.start_time as i64).unwrap_or(0);

                        let merged_stus;
                        if is_europe {
                            if let Some(rows) = itinerary_rows {
                                let mut temp = Vec::new();
                                let mut rt_stu_map = std::collections::HashMap::new();
                                for stu_item in &trip_update.stop_time_update {
                                    if let Some(seq) = stu_item.stop_sequence {
                                        rt_stu_map.insert(seq as i32, stu_item.clone());
                                    } else if let Some(ref sid) = stu_item.stop_id {
                                        if let Some(r) =
                                            rows.iter().find(|x| x.stop_id.as_str() == sid.as_ref())
                                        {
                                            rt_stu_map.insert(r.stop_sequence, stu_item.clone());
                                        }
                                    }
                                }
                                for row in rows {
                                    if let Some(stu_item) = rt_stu_map.remove(&row.stop_sequence) {
                                        temp.push(stu_item);
                                    } else {
                                        temp.push(
                                            catenary::compact_formats::CompactStopTimeUpdate {
                                                stop_sequence: Some(row.stop_sequence as u16),
                                                stop_id: Some(std::sync::Arc::from(
                                                    row.stop_id.as_str(),
                                                )),
                                                arrival: None,
                                                departure: None,
                                                departure_occupancy_status: None,
                                                schedule_relationship: None,
                                                stop_time_properties: None,
                                                //actual_track: None,
                                                //scheduled_track: None,
                                            },
                                        );
                                    }
                                }
                                let mut remaining: Vec<_> = rt_stu_map.into_values().collect();
                                temp.extend(remaining);
                                temp.sort_by_key(|s| s.stop_sequence.unwrap_or(u16::MAX));
                                merged_stus = Some(temp);
                            } else {
                                merged_stus = None;
                            }
                        } else {
                            merged_stus = None;
                        }

                        let stus_iter = match &merged_stus {
                            Some(v) => v,
                            None => &trip_update.stop_time_update,
                        };

                        for stu in stus_iter {
                            let mut resolved_stop_id: Option<std::sync::Arc<str>> =
                                stu.stop_id.clone();
                            let mut sched_arr_computed: Option<i64> = None;
                            let mut sched_dep_computed: Option<i64> = None;

                            if let Some(seq) = stu.stop_sequence {
                                if let Some(rows) = itinerary_rows {
                                    if let Some(matching_row) =
                                        rows.iter().find(|r| r.stop_sequence == (seq as i32))
                                    {
                                        if resolved_stop_id.is_none() {
                                            resolved_stop_id = Some(std::sync::Arc::from(
                                                matching_row.stop_id.as_str(),
                                            ));
                                        }
                                        if let Some(base_ms) = base_midnight_ts {
                                            if let Some(arr) = matching_row.arrival_time_since_start
                                            {
                                                sched_arr_computed = Some(
                                                    base_ms
                                                        + compressed_start_time_seconds
                                                        + arr as i64,
                                                );
                                            }
                                            if let Some(dep) =
                                                matching_row.departure_time_since_start
                                            {
                                                sched_dep_computed = Some(
                                                    base_ms
                                                        + compressed_start_time_seconds
                                                        + dep as i64,
                                                );
                                            }
                                        }
                                    }
                                }
                            }

                            let mut arr_clone = stu.arrival.clone();
                            let mut dep_clone = stu.departure.clone();

                            if is_europe {
                                if let Some(arr) = arr_clone.as_mut() {
                                    if let Some(d) = arr.delay {
                                        propagated_delay = Some(d);
                                    } else if arr.time.is_none() {
                                        if let Some(pd) = propagated_delay {
                                            arr.delay = Some(pd);
                                        }
                                    }
                                } else if let Some(pd) = propagated_delay {
                                    arr_clone = Some(Box::new(
                                        catenary::compact_formats::CompactStopTimeEvent {
                                            delay: Some(pd),
                                            time: None,
                                            uncertainty: None,
                                            scheduled_time: None,
                                        },
                                    ));
                                }

                                if let Some(dep) = dep_clone.as_mut() {
                                    if let Some(d) = dep.delay {
                                        propagated_delay = Some(d);
                                    } else if dep.time.is_none() {
                                        if let Some(pd) = propagated_delay {
                                            dep.delay = Some(pd);
                                        }
                                    }
                                } else if let Some(pd) = propagated_delay {
                                    dep_clone = Some(Box::new(
                                        catenary::compact_formats::CompactStopTimeEvent {
                                            delay: Some(pd),
                                            time: None,
                                            uncertainty: None,
                                            scheduled_time: None,
                                        },
                                    ));
                                }
                            }

                            let mut platform_resp = None;
                            let mut platform: Option<
                                catenary::formation_v1::AspenisedPlatformInfo,
                            > = None;

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
                                                    train_data.get(stop_id.as_ref())
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
                                                        train_data.get(stop_id.as_ref())
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
                                        if let Some(lirr_mnr_data) =
                                            &amtrak_track_multisource.lirr_mnr
                                        {
                                            if let Some(trip) = compressed_trip {
                                                if let Some(trip_short_name) = &trip.trip_short_name
                                                {
                                                    if let Some(stop_id) = &stu.stop_id {
                                                        if let Some(train_data) = lirr_mnr_data
                                                            .track_lookup
                                                            .get(trip_short_name.as_str())
                                                        {
                                                            if let Some(track) =
                                                                train_data.get(stop_id.as_ref())
                                                            {
                                                                platform_resp =
                                                                    Some(track.clone().into());
                                                            }
                                                        }
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
                                                    .find(|p| p.stop_id == stop_id.as_ref())
                                                {
                                                    platform_resp =
                                                        Some(platform_info.platform.clone().into());
                                                }
                                            }
                                        }
                                    }
                                }
                                "île~de~france~mobilités" => {
                                    if let TrackData::IleDeFrance(idfm_data) = &fetched_track_data {
                                        if let (Some(trip_id), Some(stop_id)) =
                                            (&trip_descriptor.trip_id, &stu.stop_id)
                                        {
                                            if let Some(platform_info) = idfm_data
                                                .get(trip_id.as_str())
                                                .and_then(|trip_platforms| {
                                                    trip_platforms.iter().find(|platform| {
                                                        platform.stop_id == stop_id.as_ref()
                                                    })
                                                })
                                            {
                                                platform_resp = Some(
                                                    platform_info.platform_name.clone().into(),
                                                );
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
                                                            .get(stop_id.as_ref())
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
                                                            train_data.get(stop_id.as_ref())
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
                                "sncf" => {
                                    if let TrackData::Sncf(Some(sncf_data)) = &fetched_track_data {
                                        if let Some(trip) = compressed_trip {
                                            if let Some(trip_short_name) = &trip.trip_short_name {
                                                if let Some(stop_id) = &stu.stop_id {
                                                    let normalized_trip = crate::track_number::sncf_siri::normalize_train_num(trip_short_name);
                                                    if let Some(train_data) =
                                                        sncf_data.track_lookup.get(&normalized_trip)
                                                    {
                                                        if let Some(code) = crate::track_number::sncf_siri::extract_station_code(stop_id.as_ref()) {
                                                            if let Some(track) = train_data.get(&code) {
                                                                let platform_name = track.departure_platform
                                                                    .clone()
                                                                    .or_else(|| track.arrival_platform.clone());
                                                                
                                                                if let Some(platform_val) = platform_name {
                                                                    platform_resp = Some(platform_val.clone().into());
                                                                    platform = Some(catenary::formation_v1::AspenisedPlatformInfo {
                                                                        aimed: None,
                                                                        expected: Some(platform_val.into()),
                                                                        platform_sectors: None,
                                                                        is_changed: false,
                                                                    });
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                "metro~northrailroad" => {
                                    if let TrackData::MetroNorthRailroad(Some(mnr_data)) =
                                        &fetched_track_data
                                    {
                                        if let Some(trip) = compressed_trip {
                                            if let Some(trip_short_name) = &trip.trip_short_name {
                                                if let Some(stop_id) = &stu.stop_id {
                                                    if let Some(train_data) = mnr_data
                                                        .track_lookup
                                                        .get(trip_short_name.as_str())
                                                    {
                                                        if let Some(track) =
                                                            train_data.get(stop_id.as_ref())
                                                        {
                                                            platform_resp =
                                                                Some(track.clone().into());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                "longislandrailroad" => {
                                    if let TrackData::LongIslandRailroad(Some(lirr_data)) =
                                        &fetched_track_data
                                    {
                                        if let Some(trip) = compressed_trip {
                                            if let Some(trip_short_name) = &trip.trip_short_name {
                                                if let Some(stop_id) = &stu.stop_id {
                                                    if let Some(train_data) = lirr_data
                                                        .track_lookup
                                                        .get(trip_short_name.as_str())
                                                    {
                                                        if let Some(track) =
                                                            train_data.get(stop_id.as_ref())
                                                        {
                                                            platform_resp =
                                                                Some(track.clone().into());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                "nyct" => {
                                    if let Some(nyct_stus_copied) = &nyct_stus_copied {
                                        platform =
                                            Some(catenary::formation_v1::AspenisedPlatformInfo {
                                                aimed: None,
                                                expected: None,
                                                platform_sectors: None,
                                                is_changed: false,
                                            });

                                        let matching_nyct_protobuf_stu =
                                            nyct_stus_copied.iter().find(|x| {
                                                x.stop_id.as_deref() == stu.stop_id.as_deref()
                                                    && stu.stop_id.is_some()
                                            });

                                        if let Some(matching_nyct_protobuf_stu) =
                                            &matching_nyct_protobuf_stu
                                        {
                                            if let Some(nyct_stop_time_update) =
                                                &matching_nyct_protobuf_stu.nyct_stop_time_update
                                            {
                                                if let Some(sched) =
                                                    &nyct_stop_time_update.scheduled_track
                                                {
                                                    if let Some(platform) = &mut platform {
                                                        platform.expected =
                                                            Some(sched.clone().into());
                                                    }
                                                }

                                                if let Some(act) =
                                                    &nyct_stop_time_update.actual_track
                                                {
                                                    if let Some(platform) = &mut platform {
                                                        platform.aimed = Some(act.clone().into());
                                                        platform_resp = Some(act.clone().into());
                                                    }
                                                }

                                                if let Some(platform) = &mut platform {
                                                    if let (Some(aimed), Some(expected)) =
                                                        (&platform.aimed, &platform.expected)
                                                    {
                                                        if aimed != expected {
                                                            platform.is_changed = true;
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
                                stop_sequence: match chateau_id {
                                    "metra" => None,
                                    _ => stu.stop_sequence.map(|x| x as u16),
                                },
                                stop_id: resolved_stop_id.clone(),
                                old_rt_data: false,
                                platform_info: platform,
                                arrival: arr_clone.map(|arrival| AspenStopTimeEvent {
                                    delay: arrival.delay,
                                    time: match arrival.time {
                                        Some(diff) => {
                                            let time =
                                                (ref_epoch as i64) + (i32::from(diff) as i64);
                                            if time <= 0 { None } else { Some(time) }
                                        }
                                        None => {
                                            if let Some(delay) = arrival.delay {
                                                if let Some(sched) = arrival.scheduled_time {
                                                    let time = (ref_epoch as i64)
                                                        + (i32::from(sched) as i64)
                                                        + (delay as i64);
                                                    if time > 0 { Some(time) } else { None }
                                                } else if let Some(sched_arr_time) =
                                                    sched_arr_computed
                                                {
                                                    let time = sched_arr_time + (delay as i64);
                                                    if time > 0 { Some(time) } else { None }
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        }
                                    },
                                    uncertainty: arrival.uncertainty.map(|u| u as i16),
                                }),
                                departure: dep_clone.map(|departure| AspenStopTimeEvent {
                                    delay: departure.delay,
                                    time: match departure.time {
                                        Some(diff) => {
                                            let time =
                                                (ref_epoch as i64) + (i32::from(diff) as i64);
                                            if time <= 0 { None } else { Some(time) }
                                        }
                                        None => {
                                            if let Some(delay) = departure.delay {
                                                if let Some(sched) = departure.scheduled_time {
                                                    let time = (ref_epoch as i64)
                                                        + (i32::from(sched) as i64)
                                                        + (delay as i64);
                                                    if time > 0 { Some(time) } else { None }
                                                } else if let Some(sched_dep_time) =
                                                    sched_dep_computed
                                                {
                                                    let time = sched_dep_time + (delay as i64);
                                                    if time > 0 { Some(time) } else { None }
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        }
                                    },
                                    uncertainty: departure.uncertainty.map(|u| u as i16),
                                }),
                                platform_string: platform_resp,
                                schedule_relationship: stu.schedule_relationship.map(|x| x.into()),
                                departure_occupancy_status: option_u8_to_occupancy_status(
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
                            .collect::<BTreeSet<Arc<str>>>();

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
                                                                                old_stu_stop_id,
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

                        let lirr_data_opt = match &fetched_track_data {
                            TrackData::LongIslandRailroad(Some(d)) => Some(d),
                            TrackData::MetroNorthRailroad(Some(d)) => Some(d),
                            _ => None,
                        };

                        if let Some(lirr_data) = lirr_data_opt {
                            if let Some(trip) = compressed_trip {
                                if let Some(trip_short_name) = &trip.trip_short_name {
                                    if let Some(unified_consist) =
                                        lirr_data.consist_lookup.get(trip_short_name.as_str())
                                    {
                                        consist = Some(unified_consist.clone());
                                    }
                                }
                            }
                        }

                        // Apply the Darwin consist mapping
                        if chateau_id == "nationalrailuk" {
                            if let Some(trip_id_str) = &trip_id {
                                if let Some(formation) =
                                    darwin_trip_id_to_formation.get(trip_id_str)
                                {
                                    consist = Some(crate::consist_cache_and_conversion::map_darwin_formation_to_consist(
                                        trip_id_str,
                                        formation
                                    ));
                                } else if let Some(formation_v1) =
                                    darwin_trip_id_to_formation_v1.get(trip_id_str)
                                {
                                    consist = Some(crate::consist_cache_and_conversion::map_darwin_v1_formation_to_consist(
                                        trip_id_str,
                                        formation_v1
                                    ));
                                }
                            }
                        }

                        if chateau_id == "gotransit" || chateau_id == "upexpress" {
                            if let Some(trip_id_str) = &trip_id {
                                let metrolinx_id =
                                    compressed_trip.and_then(|t| t.trip_short_name.as_deref());

                                if let Some(trip_short_name) = &metrolinx_id {
                                    if let Some(metrolinx_trip) =
                                        metrolinx_trip_id_to_consist.get(*trip_short_name)
                                    {
                                        consist = Some(crate::consist_cache_and_conversion::map_metrolinx_trip_to_consist(
                                        trip_id_str,
                                        metrolinx_trip
                                    ));
                                    }
                                }
                            }
                        }

                        let mut trip_update_vehicle: Option<AspenisedVehicleDescriptor> =
                            trip_update.vehicle.clone().map(|x| x.into());

                        if let Some(trip_id_str) = &trip_id {
                            if let Some(vehicle_entity_ids) =
                                trip_id_to_vehicle_gtfs_rt_id.get(trip_id_str)
                            {
                                if let Some(vehicle_entity_id) = vehicle_entity_ids.get(0) {
                                    if let Some(vehicle_pos) =
                                        aspenised_vehicle_positions.get(vehicle_entity_id)
                                    {
                                        if let Some(vehicle_desc) = &vehicle_pos.vehicle {
                                            match trip_update_vehicle.as_mut() {
                                                Some(tu_veh) => {
                                                    if tu_veh.id.is_none() {
                                                        tu_veh.id = vehicle_desc.id.clone();
                                                    }
                                                    if tu_veh.label.is_none() {
                                                        tu_veh.label = vehicle_desc.label.clone();
                                                    }
                                                    if tu_veh.license_plate.is_none() {
                                                        tu_veh.license_plate =
                                                            vehicle_desc.license_plate.clone();
                                                    }
                                                    if tu_veh.wheelchair_accessible.is_none() {
                                                        tu_veh.wheelchair_accessible =
                                                            vehicle_desc.wheelchair_accessible;
                                                    }
                                                }
                                                None => {
                                                    trip_update_vehicle =
                                                        Some(vehicle_desc.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(vehicle_summary_string) = helium_vehicle_summary_string.clone()
                        {
                            match trip_update_vehicle.as_mut() {
                                Some(vehicle) => {
                                    vehicle.label = Some(vehicle_summary_string);
                                }
                                None => {
                                    trip_update_vehicle = Some(AspenisedVehicleDescriptor {
                                        id: Some(trip_update_entity.id.clone()),
                                        label: Some(vehicle_summary_string),
                                        license_plate: None,
                                        wheelchair_accessible: None,
                                    });
                                }
                            }
                        }

                        let trip_update = AspenisedTripUpdate {
                            trip: trip_descriptor,
                            vehicle: trip_update_vehicle,
                            trip_headsign: trip_headsign.clone(),
                            found_schedule_trip_id: compressed_trip.is_some(),
                            stop_time_update: stop_time_update,
                            timestamp: trip_update.timestamp,
                            delay: delay,
                            trip_properties: trip_update
                                .trip_properties
                                .clone()
                                .map(|x| (*x).into()),
                            last_seen: catenary::duration_since_unix_epoch().as_millis() as u64,
                            consist,
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

                        if let Some(original_rt_trip_id) = &trip_id {
                            // 1. Keep the realtime suffix lookup.
                            index_trip_update_id(
                                &mut trip_updates_lookup_by_trip_id_to_trip_update_ids,
                                original_rt_trip_id,
                                &trip_update_entity.id,
                            );

                            // 2. Also index the canonical static trip_id after normalization.
                            if let Some(canonical_trip_id) = &trip_update.trip.trip_id {
                                if canonical_trip_id != original_rt_trip_id {
                                    index_trip_update_id(
                                        &mut trip_updates_lookup_by_trip_id_to_trip_update_ids,
                                        canonical_trip_id,
                                        &trip_update_entity.id,
                                    );
                                }
                            }

                            // 3. Explicit NYCT aliases, useful if there are multiple future consumers.
                            if let Some(static_aliases) =
                                nyct_rt_trip_id_to_static_trip_ids.get(original_rt_trip_id)
                            {
                                for static_alias in static_aliases {
                                    index_trip_update_id(
                                        &mut trip_updates_lookup_by_trip_id_to_trip_update_ids,
                                        static_alias,
                                        &trip_update_entity.id,
                                    );
                                }
                            }

                            if let Some(route_id) = &trip_update.trip.route_id {
                                route_ids_to_insert.insert(route_id.clone());

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
                            (*trip_modification.clone()).into(),
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
                        let aspenised_alert: AspenisedAlert = (*alert.clone()).into();

                        let processed_alert =
                            crate::alerts_processing::process_alert(aspenised_alert, chateau_id);

                        alerts.insert(alert_id.clone(), processed_alert);
                    }
                }
            }
        }

        if chateau_id == "sncf" {
            match crate::sncf_siri_alerts::fetch_alerts(conn).await {
                Ok(sncf_siri_alerts) => {
                    println!(
                        "Loaded {} SNCF SIRI Situation Exchange alerts",
                        sncf_siri_alerts.len()
                    );
                    for (alert_id, alert) in sncf_siri_alerts {
                        let processed_alert =
                            crate::alerts_processing::process_alert(alert, chateau_id);
                        alerts.insert(alert_id, processed_alert);
                    }
                }
                Err(error) => {
                    eprintln!("Failed to load SNCF SIRI Situation Exchange alerts: {error}");

                    if let Some(previous_data) = &previous_authoritative_data_store {
                        for (alert_id, alert) in &previous_data.aspenised_alerts {
                            if alert_id.starts_with("sncf-siri-sx:") {
                                alerts.insert(alert_id.clone(), alert.clone());
                            }
                        }
                    }
                }
            }
        }

        if chateau_id == "sncf" {
            if let TrackData::Sncf(Some(sncf_data)) = &fetched_track_data {
                let mut train_num_to_trip_id: AHashMap<String, String> = AHashMap::new();
                for (trip_id, trip) in trip_id_to_trip.iter() {
                    if let Some(short_name) = &trip.trip_short_name {
                        let normalized =
                            crate::track_number::sncf_siri::normalize_train_num(short_name);
                        train_num_to_trip_id.insert(normalized, trip_id.clone());
                    }
                }

                for (train_num, stop_map) in sncf_data.track_lookup.iter() {
                    if let Some(trip_id) = train_num_to_trip_id.get(train_num) {
                        if !trip_updates_lookup_by_trip_id_to_trip_update_ids
                            .contains_key(trip_id.as_str())
                        {
                            if let Some(trip) = trip_id_to_trip.get(trip_id) {
                                let mut stop_time_update = Vec::new();
                                let itinerary =
                                    accumulated_itinerary_patterns.get(&trip.itinerary_pattern_id);

                                for (stop_code, track) in stop_map.iter() {
                                    let mut full_stop_id = stop_code.as_str();
                                    let mut stop_sequence = None;

                                    if let Some((_, rows)) = itinerary {
                                        for row in rows {
                                            if let Some(code) =
                                                crate::track_number::sncf_siri::extract_station_code(
                                                    row.stop_id.as_str(),
                                                )
                                            {
                                                if code == *stop_code {
                                                    full_stop_id = row.stop_id.as_str();
                                                    stop_sequence = Some(row.stop_sequence as u16);
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    let arrival_time =
                                        track.expected_arrival_time.or(track.aimed_arrival_time);
                                    let arrival = if arrival_time.is_some() {
                                        Some(catenary::aspen_dataset::AspenStopTimeEvent {
                                            time: arrival_time,
                                            delay: None,
                                            uncertainty: None,
                                        })
                                    } else {
                                        None
                                    };

                                    let departure_time = track
                                        .expected_departure_time
                                        .or(track.aimed_departure_time);
                                    let departure = if departure_time.is_some() {
                                        Some(catenary::aspen_dataset::AspenStopTimeEvent {
                                            time: departure_time,
                                            delay: None,
                                            uncertainty: None,
                                        })
                                    } else {
                                        None
                                    };

                                    let platform_string = track
                                        .departure_platform
                                        .as_ref()
                                        .or(track.arrival_platform.as_ref())
                                        .map(|s| ecow::EcoString::from(s.as_str()));

                                    stop_time_update.push(
                                        catenary::aspen_dataset::AspenisedStopTimeUpdate {
                                            stop_sequence,
                                            stop_id: Some(Arc::from(full_stop_id)),
                                            arrival,
                                            departure,
                                            departure_occupancy_status: None,
                                            schedule_relationship: None,
                                            stop_time_properties: None,
                                            platform_string,
                                            old_rt_data: false,
                                            platform_info: None,
                                        },
                                    );
                                }

                                let synthetic_trip_update_id =
                                    format!("sncf-synthetic-{}", trip_id);

                                let synthetic_trip_update = AspenisedTripUpdate {
                                    trip: AspenRawTripInfo {
                                        trip_id: Some(trip_id.clone()),
                                        route_id: Some(trip.route_id.clone()),
                                        direction_id: None,
                                        start_time: None,
                                        start_date: None,
                                        schedule_relationship: None,
                                        modified_trip: None,
                                    },
                                    vehicle: None,
                                    timestamp: Some(
                                        catenary::duration_since_unix_epoch().as_secs(),
                                    ),
                                    delay: None,
                                    stop_time_update,
                                    trip_properties: None,
                                    trip_headsign: None,
                                    consist: None,
                                    found_schedule_trip_id: true,
                                    last_seen: catenary::duration_since_unix_epoch().as_millis()
                                        as u64,
                                };

                                trip_updates.insert(
                                    CompactString::new(&synthetic_trip_update_id),
                                    synthetic_trip_update,
                                );

                                trip_updates_lookup_by_trip_id_to_trip_update_ids
                                    .entry(CompactString::new(trip_id))
                                    .and_modify(|x| {
                                        x.push(CompactString::new(&synthetic_trip_update_id))
                                    })
                                    .or_insert(vec![CompactString::new(&synthetic_trip_update_id)]);
                            }
                        }
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

    let mut vehicle_positions_rtree_by_route_type = AHashMap::new();
    let mut rtree_elements_by_route_type = AHashMap::new();
    for (key, vehicle) in aspenised_vehicle_positions.iter() {
        if let Some(pos) = &vehicle.position {
            rtree_elements_by_route_type
                .entry(vehicle.route_type)
                .or_insert_with(Vec::new)
                .push(catenary::aspen_dataset::AspenisedVehiclePositionBBox {
                    vehicle_id: key.clone(),
                    lon: pos.longitude as f64,
                    lat: pos.latitude as f64,
                });
        }
    }
    for (route_type, elements) in rtree_elements_by_route_type.into_iter() {
        vehicle_positions_rtree_by_route_type.insert(route_type, rstar::RTree::bulk_load(elements));
    }

    let fast_hash_of_routes =
        catenary::fast_hash(&vehicle_routes_cache.iter().collect::<BTreeMap<_, _>>());

    let aspenised_data = AspenisedData {
        vehicle_positions: aspenised_vehicle_positions,
        vehicle_positions_rtree_by_route_type,
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
    let existing_trajectory_store = authoritative_trajectory_data_store
        .get_async(chateau_id)
        .await;
    let (mut geometries, mut patterns, mut rtree_by_route_type) =
        if let Some(ref existing) = existing_trajectory_store {
            let store = existing.get();
            (
                store.geometries.clone(),
                store.patterns.clone(),
                store.rtree_by_route_type.clone(),
            )
        } else {
            (Vec::new(), Vec::new(), AHashMap::new())
        };
    let mut pattern_map: AHashMap<CompactString, u32> = AHashMap::new();
    for (idx, pat) in patterns.iter().enumerate() {
        pattern_map.insert(pat.pattern_id_str.clone(), idx as u32);
    }
    let mut static_changed = false;

    let mut trajectories = Vec::new();
    let mut pattern_to_trajectories = vec![Vec::new(); patterns.len()];
    let current_timestamp = catenary::duration_since_unix_epoch().as_millis() as u64;
    let current_secs = (current_timestamp / 1000) as i64;

    // Fetch shapes for all trips
    // Note: The shape IDs sent in the GTFS realtime dataset are only used for detours.
    // They are not related to the shape IDs required for timetable analysis and trajectory computation.
    // For trajectories, we need to look at the schedules instead.
    let mut shape_ids_to_fetch = std::collections::HashSet::new();
    if ALLOWED_CHATEAUX.contains(&chateau_id) {
        for (_, trip_update) in aspenised_data_for_persist.trip_updates.iter() {
            if let Some(trip_id) = &trip_update.trip.trip_id {
                if let Some(compressed_trip) = aspenised_data_for_persist
                    .compressed_trip_internal_cache
                    .compressed_trips
                    .get(trip_id.as_str())
                {
                    if let Some((meta, _)) = aspenised_data_for_persist
                        .itinerary_pattern_internal_cache
                        .itinerary_patterns
                        .get(&compressed_trip.itinerary_pattern_id)
                    {
                        if let Some(shape_id) = &meta.shape_id {
                            shape_ids_to_fetch.insert(shape_id.clone());
                        }
                    }
                }
            }
        }
    }

    let mut shape_linestrings = std::collections::HashMap::new();
    let shape_ids: Vec<String> = shape_ids_to_fetch.into_iter().collect();
    if shape_ids.len() > 0 {
        println!(
            "Chateau {}: Attempting to fetch {} unique shapes from DB",
            chateau_id,
            shape_ids.len()
        );
        match pool.get().await {
            Ok(mut conn_pre) => {
                use catenary::schema::gtfs::shapes::dsl::*;
                use diesel::ExpressionMethods;
                use diesel::QueryDsl;
                use diesel_async::RunQueryDsl;

                for chunk in shape_ids.chunks(500) {
                    println!(
                        "Chateau {}: Querying database for chunk of {} shapes. filter: onestop_feed_id = {}",
                        chateau_id,
                        chunk.len(),
                        chateau_id
                    );
                    match shapes
                        .filter(chateau.eq(chateau_id))
                        .filter(shape_id.eq_any(chunk))
                        .select((shape_id, linestring))
                        .load::<(
                            String,
                            postgis_diesel::types::LineString<postgis_diesel::types::Point>,
                        )>(&mut conn_pre)
                        .await
                    {
                        Ok(shapes_result) => {
                            println!(
                                "Chateau {}: Successfully retrieved {}/{} shapes for this chunk",
                                chateau_id,
                                shapes_result.len(),
                                chunk.len()
                            );
                            for (s_id, ls) in shapes_result {
                                shape_linestrings.insert(s_id, ls);
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Chateau {}: Error fetching shapes from database: {:?}",
                                chateau_id, e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Chateau {}: Failed to get database connection from pool: {:?}",
                    chateau_id, e
                );
            }
        }
    }

    if ALLOWED_CHATEAUX.contains(&chateau_id) {
        println!("Starting trajectory processing for {}", &chateau_id);

        let mut skipped_no_stops = 0;
        let mut skipped_no_trip_id = 0;
        let mut skipped_no_route_id = 0;
        let mut skipped_no_route_cache = 0;
        let mut skipped_too_few_trajectory_stops = 0;
        let mut skipped_too_few_shape_coords = 0;

        for (_trip_update_id, trip_update) in aspenised_data_for_persist.trip_updates.iter() {
            if trip_update.stop_time_update.is_empty() {
                skipped_no_stops += 1;
                continue;
            }

            let trip_id = match &trip_update.trip.trip_id {
                Some(id) => id,
                None => {
                    skipped_no_trip_id += 1;
                    continue;
                }
            };

            let route_id = match &trip_update.trip.route_id {
                Some(id) => id,
                None => {
                    skipped_no_route_id += 1;
                    continue;
                }
            };

            let route = match aspenised_data_for_persist
                .vehicle_routes_cache
                .get(route_id)
            {
                Some(r) => r,
                None => {
                    skipped_no_route_cache += 1;
                    continue;
                }
            };

            let route_type_str = match route.route_type {
                0 => "tram",
                1 => "subway",
                2 => "rail",
                3 => "bus",
                4 => "ferry",
                5 => "cable_car",
                6 => "gondola",
                7 => "funicular",
                11 => "trolleybus",
                12 => "monorail",
                _ => "other",
            };

            let mut trajectory_stops = Vec::new();
            for stu in &trip_update.stop_time_update {
                let arrival_time = stu.arrival.as_ref().and_then(|a| a.time).unwrap_or(0);
                let departure_time = stu
                    .departure
                    .as_ref()
                    .and_then(|d| d.time)
                    .unwrap_or(arrival_time);
                if arrival_time != 0 || departure_time != 0 {
                    trajectory_stops.push((stu, arrival_time, departure_time));
                }
            }

            if trajectory_stops.len() < 2 {
                skipped_too_few_trajectory_stops += 1;
                continue;
            }

            // Try to find the linestring for this trip's shape
            let mut trip_shape_coords = None;
            if let Some(compressed_trip) = aspenised_data_for_persist
                .compressed_trip_internal_cache
                .compressed_trips
                .get(trip_id.as_str())
            {
                if let Some((meta, _)) = aspenised_data_for_persist
                    .itinerary_pattern_internal_cache
                    .itinerary_patterns
                    .get(&compressed_trip.itinerary_pattern_id)
                {
                    if let Some(shape_id) = &meta.shape_id {
                        if let Some(ls) = shape_linestrings.get(shape_id) {
                            let coords: Vec<[f64; 2]> =
                                ls.points.iter().map(|p| [p.x, p.y]).collect();
                            if !coords.is_empty() {
                                trip_shape_coords = Some(coords);
                            }
                        }
                    }
                }
            }

            if trip_shape_coords.is_none() {
                skipped_too_few_shape_coords += 1;
            }

            let mut segments = Vec::new();
            let mut last_shape_idx = 0;
            for i in 0..trajectory_stops.len() - 1 {
                let from_stu = trajectory_stops[i].0;
                let to_stu = trajectory_stops[i + 1].0;

                let mut from_lat = 0.0;
                let mut from_lon = 0.0;
                if let Some(stop_id) = &from_stu.stop_id {
                    if let Some(stop) = aspenised_data_for_persist
                        .stop_id_to_stop
                        .get(stop_id.as_ref())
                    {
                        from_lat = stop.stop_lat.unwrap_or(0.0) as f64;
                        from_lon = stop.stop_lon.unwrap_or(0.0) as f64;
                    }
                }

                let mut to_lat = 0.0;
                let mut to_lon = 0.0;
                if let Some(stop_id) = &to_stu.stop_id {
                    if let Some(stop) = aspenised_data_for_persist
                        .stop_id_to_stop
                        .get(stop_id.as_ref())
                    {
                        to_lat = stop.stop_lat.unwrap_or(0.0) as f64;
                        to_lon = stop.stop_lon.unwrap_or(0.0) as f64;
                    }
                }

                let mut seg_coords = vec![[from_lon, from_lat], [to_lon, to_lat]];

                if let Some(ref coords) = trip_shape_coords {
                    fn dist_sq(a: &[f64; 2], b: &[f64; 2]) -> f64 {
                        let dx = a[0] - b[0];
                        let dy = a[1] - b[1];
                        dx * dx + dy * dy
                    }

                    let mut best_start = last_shape_idx;
                    let mut min_start_dist = f64::MAX;

                    for (idx, p) in coords.iter().enumerate().skip(last_shape_idx) {
                        let d_start = dist_sq(p, &seg_coords[0]);
                        if d_start < min_start_dist {
                            min_start_dist = d_start;
                            best_start = idx;
                        }
                    }

                    let mut best_end = best_start;
                    let mut min_end_dist = f64::MAX;

                    for (idx, p) in coords.iter().enumerate().skip(best_start) {
                        let d_end = dist_sq(p, &seg_coords[1]);
                        if d_end < min_end_dist {
                            min_end_dist = d_end;
                            best_end = idx;
                        }
                    }

                    last_shape_idx = best_end;

                    if best_start <= best_end {
                        seg_coords = coords[best_start..=best_end].to_vec();
                    }
                }

                segments.push(catenary::aspen_dataset::AspenisedTrajectorySegment {
                    from_stop_index: i,
                    to_stop_index: i + 1,
                    coordinates: seg_coords,
                });
            }

            let trip_short_name = trip_update
                .trip_properties
                .as_ref()
                .and_then(|p| p.trip_short_name.clone())
                .or_else(|| {
                    aspenised_data_for_persist
                        .compressed_trip_internal_cache
                        .compressed_trips
                        .get(trip_id.as_str())
                        .and_then(|t| t.trip_short_name.clone().map(|s| s.to_string()))
                });

            let pattern_id_str = if let Some(compressed_trip) = aspenised_data_for_persist
                .compressed_trip_internal_cache
                .compressed_trips
                .get(trip_id.as_str())
            {
                compressed_trip.itinerary_pattern_id.to_string()
            } else {
                format!(
                    "{}_dir_{}",
                    route_id,
                    trip_update.trip.direction_id.unwrap_or(0)
                )
            };

            let pattern_idx = if let Some(&idx) = pattern_map.get(pattern_id_str.as_str()) {
                idx
            } else {
                let new_idx = patterns.len() as u32;
                let geometry_id = geometries.len() as u32;

                let mut pattern_coordinates: Vec<[f64; 2]> = Vec::new();
                let mut segment_ranges = Vec::new();

                for seg in &segments {
                    let coord_start = pattern_coordinates.len() as u32;
                    pattern_coordinates.extend_from_slice(&seg.coordinates);
                    let coord_end = pattern_coordinates.len() as u32;
                    segment_ranges.push(SegmentRange {
                        from_stop_index: seg.from_stop_index as u16,
                        to_stop_index: seg.to_stop_index as u16,
                        coordinate_start: coord_start,
                        coordinate_end: coord_end,
                    });
                }

                let packed_coords = pattern_coordinates
                    .iter()
                    .map(|&[lon, lat]| {
                        [
                            (lon * 1_000_000.0).round() as i32,
                            (lat * 1_000_000.0).round() as i32,
                        ]
                    })
                    .collect::<Vec<[i32; 2]>>()
                    .into_boxed_slice();

                geometries.push(PackedGeometry {
                    coordinates: packed_coords,
                });

                patterns.push(PatternGeometry {
                    pattern_id_str: CompactString::new(&pattern_id_str),
                    route_id: CompactString::new(route_id),
                    mode: CompactString::new(route_type_str),
                    geometry_id,
                    segments: segment_ranges.into_boxed_slice(),
                    distance: 0.0,
                });

                // Calculate bounding boxes and insert into rtree_by_route_type
                let is_train = route.route_type == 1 || route.route_type == 2;
                if is_train {
                    for seg in &segments {
                        if seg.coordinates.is_empty() {
                            continue;
                        }
                        let mut min_lon = f64::MAX;
                        let mut min_lat = f64::MAX;
                        let mut max_lon = f64::MIN;
                        let mut max_lat = f64::MIN;
                        for coord in &seg.coordinates {
                            min_lon = min_lon.min(coord[0]);
                            min_lat = min_lat.min(coord[1]);
                            max_lon = max_lon.max(coord[0]);
                            max_lat = max_lat.max(coord[1]);
                        }
                        rtree_by_route_type
                            .entry(route.route_type)
                            .or_insert_with(|| rstar::RTree::new())
                            .insert(PatternBBox {
                                pattern_id: new_idx,
                                min_lon,
                                min_lat,
                                max_lon,
                                max_lat,
                            });
                    }
                } else {
                    let mut current_min_lon = f64::MAX;
                    let mut current_min_lat = f64::MAX;
                    let mut current_max_lon = f64::MIN;
                    let mut current_max_lat = f64::MIN;
                    let mut current_dist = 0.0;
                    let mut last_coord: Option<[f64; 2]> = None;

                    for seg in &segments {
                        for coord in &seg.coordinates {
                            current_min_lon = current_min_lon.min(coord[0]);
                            current_min_lat = current_min_lat.min(coord[1]);
                            current_max_lon = current_max_lon.max(coord[0]);
                            current_max_lat = current_max_lat.max(coord[1]);

                            if let Some(last) = last_coord {
                                let dx =
                                    (coord[0] - last[0]) * 111320.0 * (last[1].to_radians().cos());
                                let dy = (coord[1] - last[1]) * 111320.0;
                                current_dist += (dx * dx + dy * dy).sqrt();
                            }
                            last_coord = Some(*coord);

                            if current_dist >= 10000.0 {
                                rtree_by_route_type
                                    .entry(route.route_type)
                                    .or_insert_with(|| rstar::RTree::new())
                                    .insert(PatternBBox {
                                        pattern_id: new_idx,
                                        min_lon: current_min_lon,
                                        min_lat: current_min_lat,
                                        max_lon: current_max_lon,
                                        max_lat: current_max_lat,
                                    });
                                current_min_lon = f64::MAX;
                                current_min_lat = f64::MAX;
                                current_max_lon = f64::MIN;
                                current_max_lat = f64::MIN;
                                current_dist = 0.0;
                            }
                        }
                    }
                    if current_dist > 0.0 || (current_min_lon != f64::MAX) {
                        rtree_by_route_type
                            .entry(route.route_type)
                            .or_insert_with(|| rstar::RTree::new())
                            .insert(PatternBBox {
                                pattern_id: new_idx,
                                min_lon: current_min_lon,
                                min_lat: current_min_lat,
                                max_lon: current_max_lon,
                                max_lat: current_max_lat,
                            });
                    }
                }

                pattern_map.insert(CompactString::new(&pattern_id_str), new_idx);
                static_changed = true;
                new_idx
            };

            let trajectory_idx = trajectories.len() as u32;
            let mut stops_instances = Vec::new();
            for (stu, arr, dep) in &trajectory_stops {
                stops_instances.push(TrajectoryStopInstance {
                    stop_id: stu
                        .stop_id
                        .as_ref()
                        .map(|id| CompactString::new(id.as_ref())),
                    track: stu
                        .platform_string
                        .as_ref()
                        .map(|s| CompactString::new(s.as_str())),
                    arrival: *arr,
                    departure: *dep,
                });
            }

            let start_time_parsed = trip_update
                .trip
                .start_time
                .as_ref()
                .and_then(|s| parse_start_time(s));

            let service_date_parsed = trip_update
                .trip
                .start_date
                .and_then(|d| d.format("%Y%m%d").to_string().parse::<i32>().ok());

            let traj_instance = TrajectoryInstance {
                pattern_id: pattern_idx,
                trip_id: CompactString::new(trip_id),
                start_time: start_time_parsed,
                service_date: service_date_parsed,
                trip_short_name: trip_short_name.map(CompactString::new),
                stops: stops_instances.into_boxed_slice(),
            };

            trajectories.push(traj_instance);

            if (pattern_idx as usize) >= pattern_to_trajectories.len() {
                pattern_to_trajectories.resize_with(pattern_idx as usize + 1, Vec::new);
            }
            pattern_to_trajectories[pattern_idx as usize].push(trajectory_idx);
        }
        println!(
            "Trajectory computation for chateau {}: \
             total_trip_updates={}, \
             skipped_no_stops={}, \
             skipped_no_trip_id={}, \
             skipped_no_route_id={}, \
             skipped_no_route_cache={}, \
             skipped_too_few_trajectory_stops={}, \
             skipped_too_few_shape_coords={}, \
             built_trajectories={}",
            chateau_id,
            aspenised_data_for_persist.trip_updates.len(),
            skipped_no_stops,
            skipped_no_trip_id,
            skipped_no_route_id,
            skipped_no_route_cache,
            skipped_too_few_trajectory_stops,
            skipped_too_few_shape_coords,
            rtree_by_route_type
                .values()
                .map(|t| t.size())
                .sum::<usize>()
        );
    }

    let store = catenary::aspen_dataset::AspenTrajectoryStore {
        geometries,
        patterns,
        trajectories,
        pattern_to_trajectories,
        rtree_by_route_type,
    };

    authoritative_trajectory_data_store
        .entry_async(chateau_id.to_string())
        .await
        .and_modify(|d| *d = store.clone())
        .or_insert(store.clone());

    let should_save = match LAST_SAVE_TIME.get_async(chateau_id).await {
        Some(last_save) => Instant::now().duration_since(*last_save.get()) > SAVE_INTERVAL,
        None => true,
    };

    if should_save {
        if let Err(e) = persistence::save_chateau_data(chateau_id, &aspenised_data_for_persist) {
            eprintln!("Failed to save chateau data for {}: {}", chateau_id, e);
        } else {
            if let Err(e) = persistence::save_trajectory_data(chateau_id, &store, static_changed) {
                eprintln!("Failed to save trajectory data for {}: {}", chateau_id, e);
            }
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

fn parse_start_time(time_str: &str) -> Option<i32> {
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() >= 2 {
        let hrs: i32 = parts[0].parse().ok()?;
        let mins: i32 = parts[1].parse().ok()?;
        let secs: i32 = if parts.len() >= 3 {
            parts[2].parse().unwrap_or(0)
        } else {
            0
        };
        Some(hrs * 3600 + mins * 60 + secs)
    } else {
        None
    }
}

//Assisted-by: Gemini 3 via Google Antigravity
