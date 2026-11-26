use crate::EtcdConnectionIps;
use crate::aspen::lib::ChateauMetadataEtcd;
use crate::postgres_tools::CatenaryPostgresPool;
use ahash::AHashMap;
use ahash::AHashSet;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Bool;
use diesel::sql_types::*;
use diesel_async::RunQueryDsl;
use futures::stream::{StreamExt, TryStreamExt};
use geo::{Distance, Haversine};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

// Import logic from shared modules
use crate::aspen::lib::AspenRpcClient;
use crate::aspen_dataset::{AspenisedAlert, AspenisedTripUpdate};
use crate::departures_shared::{
    AlertIndex, ItinOption, StopDataResult, ValidTripSet, estimate_service_date,
    fetch_stop_data_for_chateau,
};
use crate::gtfs_schedule_protobuf::protobuf_to_frequencies;
use crate::make_calendar_structure_from_pg;

// Keep large ANY predicates small while bounding the total database fan-out per chateau.
const PATTERN_QUERY_BATCH_SIZE: usize = 128;
const PATTERN_QUERY_CONCURRENCY: usize = 4;
const COACH_ROUTE_TYPE: i16 = 200;
const INTERMEDIATE_ROUTE_LIMIT: usize = 10;

const LONG_DISTANCE_CHATEAUX: &[&str] = &[
    "sncf",
    "nationalrailuk",
    "sncb",
    "nederlandse~spoorwegen",
    "rejseplanen~dk~gtfs",
    "norge",
    "sverige",
    "lv",
    "ztp~krakow",
    "deutschland",
    "schweiz",
    "trenitalia",
    "kordis",
    "pražskáintegrovanádoprava",
    "koleje~dolnoslaskie",
    "pkp~intercity~pl",
    "renfeoperadora",
];

// This endpoint reads its trips from trips_compressed, so it never uses the
// potentially large itinerary_pattern_meta.trip_ids array. Explicitly select
// only the metadata needed to construct nearby departures.
#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = crate::schema::gtfs::itinerary_pattern_meta)]
struct NearbyItineraryPatternMeta {
    itinerary_pattern_id: String,
    route_id: CompactString,
    direction_pattern_id: Option<String>,
    timezone: String,
    row_count: i32,
}

// Each HTTP/WS load has its own trace; concurrent chateaux and batches add
// records without serializing database work.
#[derive(Serialize, Clone, Debug)]
pub struct NearbySqlTiming {
    pub chateau: Option<String>,
    pub query: &'static str,
    // Milliseconds since the request began. Concurrent queries overlap.
    pub started_ms: f64,
    pub pool_wait_ms: f64,
    pub query_ms: f64,
    pub input_ids: usize,
    pub returned_rows: usize,
    pub success: bool,
}

type SharedSqlTimings = Arc<std::sync::Mutex<Vec<NearbySqlTiming>>>;

fn sql_timing<Row>(
    request_started: Instant,
    chateau: Option<String>,
    query: &'static str,
    input_ids: usize,
    checkout_started: Instant,
    sql_started: Instant,
    result: &diesel::QueryResult<Vec<Row>>,
) -> NearbySqlTiming {
    let finished = Instant::now();
    NearbySqlTiming {
        chateau,
        query,
        started_ms: checkout_started
            .duration_since(request_started)
            .as_secs_f64()
            * 1000.0,
        pool_wait_ms: sql_started.duration_since(checkout_started).as_secs_f64() * 1000.0,
        query_ms: finished.duration_since(sql_started).as_secs_f64() * 1000.0,
        input_ids,
        returned_rows: result.as_ref().map_or(0, Vec::len),
        success: result.is_ok(),
    }
}

fn record_sql_timing<Row>(
    timings: &SharedSqlTimings,
    request_started: Instant,
    chateau: Option<String>,
    query: &'static str,
    input_ids: usize,
    checkout_started: Instant,
    sql_started: Instant,
    result: &diesel::QueryResult<Vec<Row>>,
) {
    let record = sql_timing(
        request_started,
        chateau,
        query,
        input_ids,
        checkout_started,
        sql_started,
        result,
    );
    timings
        .lock()
        .expect("nearby SQL timing mutex poisoned")
        .push(record);
}

fn snapshot_sql_timings(timings: &SharedSqlTimings) -> Vec<NearbySqlTiming> {
    let mut records = timings
        .lock()
        .expect("nearby SQL timing mutex poisoned")
        .clone();
    records.sort_by(|a, b| {
        a.started_ms
            .total_cmp(&b.started_ms)
            .then_with(|| a.chateau.cmp(&b.chateau))
            .then_with(|| a.query.cmp(b.query))
    });
    records
}

// A record is emitted for each SQL query, including every parallel batch.
// Capture the chateau and batch size before Diesel consumes query parameters.
macro_rules! timed_nearby_sql {
    ($timings:expr, $request_started:expr, $chateau:expr, $name:literal, $inputs:expr, $pool:expr, $query:expr, $row:ty) => {{
        let input_ids = $inputs;
        let timing_chateau: Option<String> = if ($chateau).is_empty() {
            None
        } else {
            Some(($chateau).to_string())
        };
        let checkout_started = Instant::now();
        let mut conn = ($pool).get().await.unwrap();
        let sql_started = Instant::now();
        let result = ($query).load::<$row>(&mut conn).await;
        record_sql_timing(
            &$timings,
            $request_started,
            timing_chateau,
            $name,
            input_ids,
            checkout_started,
            sql_started,
            &result,
        );
        result
    }};
}

async fn load_pattern_batches<Id, Row, Load, LoadFuture>(
    ids: &[Id],
    query_slots: &tokio::sync::Semaphore,
    load: Load,
) -> diesel::QueryResult<Vec<Row>>
where
    Id: Clone,
    Load: Fn(Vec<Id>) -> LoadFuture,
    LoadFuture: std::future::Future<Output = diesel::QueryResult<Vec<Row>>>,
{
    let batches: Vec<Vec<Row>> = futures::stream::iter(ids.chunks(PATTERN_QUERY_BATCH_SIZE))
        .map(|batch| {
            let load = &load;
            async move {
                // Acquire before checking out a connection. All simultaneous pattern
                // lookups in this chateau share this limit, including tokio::join! branches.
                let _slot = query_slots.acquire().await.expect("query semaphore closed");
                load(batch.to_vec()).await
            }
        })
        .buffer_unordered(PATTERN_QUERY_CONCURRENCY)
        .try_collect()
        .await?;

    // Propagate any failed batch rather than returning an incomplete set of rows.
    Ok(batches.into_iter().flatten().collect())
}

// Compute one policy per request. Station density counts physical OSM stations,
// rather than platforms or duplicate GTFS records at the same station.
#[derive(Clone, Copy, Debug)]
struct DepartureDistanceLimits {
    bus_m: f64,
    other_m: f64,
}

impl DepartureDistanceLimits {
    fn new(radius_m: f64, stop_count: usize, station_count: usize) -> Self {
        let other_m = radius_m.min(match station_count {
            201.. => 1000.0,
            101.. => 2000.0,
            _ => radius_m,
        });
        // The former output-only 2 km cap subsumes the old 4 km/2.5 km
        // bus tiers. Apply it here, then tighten it for dense stop clusters.
        let bus_m = other_m
            .min(match stop_count {
                801.. => 1000.0,
                501.. => 1500.0,
                51.. => 2000.0,
                _ => radius_m,
            })
            .min(match station_count {
                101.. => 1000.0,
                1.. => 2000.0,
                _ => radius_m,
            });
        Self { bus_m, other_m }
    }

    fn allows(self, route_type: i16, distance_m: f64) -> bool {
        // Coaches are not part of the nearby local departure board.
        if route_type == COACH_ROUTE_TYPE {
            return false;
        }
        let is_bus = matches!(route_type, 3 | 11 | 201..=209 | 700..=799 | 800);
        distance_m.is_finite() && distance_m <= if is_bus { self.bus_m } else { self.other_m }
    }

    fn allows_stop(
        self,
        primary: Option<i16>,
        route_types: &[Option<i16>],
        children_route_types: &[Option<i16>],
        distance_m: f64,
    ) -> bool {
        // Mixed stops must survive if any mode is eligible. The actual route
        // is checked again before loading itinerary rows and trips.
        let mut modes = route_types
            .iter()
            .chain(children_route_types)
            .flatten()
            .copied()
            .chain(primary)
            .peekable();
        if modes.peek().is_none() {
            return distance_m.is_finite() && distance_m <= self.other_m;
        }
        modes.any(|mode| self.allows(mode, distance_m))
    }
}

// A stop with no known modes must remain eligible (for example, a station
// parent or realtime-only stop). Exclude only explicitly coach-only stops.
fn has_non_coach_mode(
    primary: Option<i16>,
    route_types: &[Option<i16>],
    children_route_types: &[Option<i16>],
) -> bool {
    let mut modes = route_types
        .iter()
        .chain(children_route_types)
        .flatten()
        .copied()
        .chain(primary)
        .peekable();
    modes.peek().is_none() || modes.any(|mode| mode != COACH_ROUTE_TYPE)
}

#[derive(Deserialize, Clone, Debug)]
pub struct NearbyFromCoordsV3 {
    pub lat: f64,
    pub lon: f64,
    pub departure_time: Option<u64>,
    pub radius: Option<f64>,
    pub limit_per_station: Option<usize>,
    pub limit_per_headsign: Option<usize>,
    pub skip_realtime: Option<bool>,
    pub rt_timeout_ms: Option<u64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct StopOutputV3 {
    pub gtfs_id: CompactString,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub osm_station_id: Option<i64>,
    pub timezone: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct NearbyDeparturesV3Response {
    pub long_distance: Vec<StationDepartureGroupExport>,
    pub local: Vec<DepartureRouteGroupExportV3>,
    pub routes: HashMap<String, HashMap<String, RouteInfoExport>>,
    pub stops: HashMap<String, HashMap<String, StopOutputV3>>,
    pub debug: NearbyDeparturesDebug,
}

/// Durations are cumulative across chateaux, not critical-path wall time.
/// Event construction includes service-date and legacy realtime matching.
/// Output grouping includes both per-chateau grouping and final assembly.
#[derive(Serialize, Clone, Debug, Default)]
pub struct NearbyProcessingDebug {
    pub realtime_connection_ms: f64,
    pub realtime_rpc_ms: f64,
    pub realtime_overlay_ms: f64,
    pub realtime_additional_sql_ms: f64,
    pub service_date_processing_ms: f64,
    pub event_construction_ms: f64,
    pub output_grouping_ms: f64,
    pub legacy_realtime_matching_ms: f64,
    pub realtime_loader_total_ms: f64,
    pub compressed_trip_count: usize,
    pub events_before_overlay: usize,
    pub realtime_update_count: usize,
    pub events_after_time_filter: usize,
    pub legacy_realtime_update_count: usize,
    pub realtime_modification_count: usize,
    pub events_after_overlay: usize,
}

impl NearbyProcessingDebug {
    fn accumulate(&mut self, other: &Self) {
        self.realtime_connection_ms += other.realtime_connection_ms;
        self.realtime_rpc_ms += other.realtime_rpc_ms;
        self.realtime_overlay_ms += other.realtime_overlay_ms;
        self.realtime_additional_sql_ms += other.realtime_additional_sql_ms;
        self.service_date_processing_ms += other.service_date_processing_ms;
        self.event_construction_ms += other.event_construction_ms;
        self.output_grouping_ms += other.output_grouping_ms;
        self.legacy_realtime_matching_ms += other.legacy_realtime_matching_ms;
        self.realtime_loader_total_ms += other.realtime_loader_total_ms;
        self.compressed_trip_count += other.compressed_trip_count;
        self.events_before_overlay += other.events_before_overlay;
        self.realtime_update_count += other.realtime_update_count;
        self.events_after_time_filter += other.events_after_time_filter;
        self.legacy_realtime_update_count += other.legacy_realtime_update_count;
        self.realtime_modification_count += other.realtime_modification_count;
        self.events_after_overlay += other.events_after_overlay;
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct NearbyDeparturesDebug {
    pub total_time_ms: u128,
    pub db_connection_time_ms: u128,
    pub stops_fetch_time_ms: u128,
    pub etcd_connection_time_ms: u128,
    pub pipeline_processing_time_ms: u128,
    pub initial_stop_count: Option<usize>,
    pub initial_osm_station_count: Option<usize>,
    pub sql_queries: Vec<NearbySqlTiming>,
    #[serde(flatten)]
    pub processing: NearbyProcessingDebug,
    pub processing_by_chateau: BTreeMap<String, NearbyProcessingDebug>,
}

// --- Long Distance Structs ---
#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Clone, Debug)]
pub enum StationKey {
    Osm(i64),
    Parent(String),
    Stop(String),
}

#[derive(Serialize, Clone, Debug)]
pub struct StationDepartureGroupExport {
    pub station_name: String,
    pub osm_station_id: Option<i64>,
    pub distance_m: f64,
    pub departures: Vec<DepartureItem>,
    pub lat: f64,
    pub lon: f64,
    pub timezone: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct DepartureItem {
    pub scheduled_departure: Option<u64>,
    pub realtime_departure: Option<u64>,
    pub scheduled_arrival: Option<u64>,
    pub realtime_arrival: Option<u64>,
    pub service_date: chrono::NaiveDate,
    pub headsign: String,
    pub platform: Option<String>,
    pub trip_id: String,
    pub trip_short_name: Option<String>,
    pub route_id: String,
    pub stop_id: String,
    pub agency_id: String,
    pub cancelled: bool,
    pub delayed: bool,
    pub chateau_id: String,
    pub last_stop: bool,
    pub final_station_name: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct RouteInfoExport {
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub agency_name: Option<String>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub route_type: i32,
}

// --- Local Transport Structs ---
#[derive(Serialize, Clone, Debug)]
pub struct DepartureRouteGroupExportV3 {
    pub chateau_id: String,
    pub route_id: CompactString,
    pub color: Option<CompactString>,
    pub text_color: Option<CompactString>,
    pub short_name: Option<CompactString>,
    pub long_name: Option<String>,
    pub route_type: i16,
    pub agency_name: Option<String>,
    pub headsigns: HashMap<String, Vec<LocalDepartureItem>>,
    pub closest_distance: f64,
}
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct LocalRouteKey {
    pub chateau: String,
    pub route_id: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct LocalDepartureItem {
    pub trip_id: CompactString,
    pub departure_schedule: Option<u64>,
    pub departure_realtime: Option<u64>,
    pub arrival_schedule: Option<u64>,
    pub arrival_realtime: Option<u64>,
    pub stop_id: CompactString,
    pub stop_name: Option<String>,
    pub cancelled: bool,
    pub platform: Option<String>,
    pub service_date: chrono::NaiveDate,
    pub last_stop: bool,
}

/// Discover stops once, then reuse the snapshot for each phase.
pub async fn prepare_nearby(
    mut query: NearbyFromCoordsV3,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<PreparedNearby, String> {
    query.validate()?;
    let start = Instant::now();
    query
        .departure_time
        .get_or_insert_with(|| crate::duration_since_unix_epoch().as_secs());
    let connection_start = Instant::now();
    let mut conn = pool.get().await.map_err(|e| e.to_string())?;
    let db_connection_time = connection_start.elapsed();
    let input_point = geo::Point::new(query.lon, query.lat);
    let radius = query.radius.unwrap_or(5000.0);
    let spatial_resolution = crate::make_degree_length_as_distance_from_point(&input_point, radius);
    let where_query = format!(
        "ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
        query.lon, query.lat, spatial_resolution
    );
    let stops_start = Instant::now();
    let stops_result = crate::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query))
        .select(crate::models::Stop::as_select())
        .load::<crate::models::Stop>(&mut conn)
        .await;
    let stop_sql_timing = sql_timing(
        start,
        None,
        "nearby_spatial_stops",
        1,
        connection_start,
        stops_start,
        &stops_result,
    );
    let mut stops = stops_result.map_err(|e| e.to_string())?;
    // Coach-only stops must not inflate the density thresholds or trigger
    // direction, calendar, trip or realtime requests in any phase.
    stops.retain(|stop| {
        has_non_coach_mode(
            stop.primary_route_type,
            &stop.route_types,
            &stop.children_route_types,
        )
    });
    Ok(PreparedNearby {
        query,
        stops,
        start,
        db_connection_time,
        stops_fetch_time: stops_start.elapsed(),
        stop_sql_timing,
        priority_sql_timings: Arc::new(std::sync::Mutex::new(Vec::new())),
    })
}

pub struct PreparedNearby {
    query: NearbyFromCoordsV3,
    stops: Vec<crate::models::Stop>,
    start: Instant,
    db_connection_time: std::time::Duration,
    stops_fetch_time: std::time::Duration,
    stop_sql_timing: NearbySqlTiming,
    priority_sql_timings: SharedSqlTimings,
}

impl NearbyFromCoordsV3 {
    pub fn validate(&self) -> Result<(), String> {
        if !self.lat.is_finite()
            || !self.lon.is_finite()
            || !(-90.0..=90.0).contains(&self.lat)
            || !(-180.0..=180.0).contains(&self.lon)
            || self
                .radius
                .is_some_and(|r| !r.is_finite() || r <= 0.0 || r > 50_000.0)
            || self.limit_per_station.is_some_and(|n| n > 100)
            || self.limit_per_headsign.is_some_and(|n| n > 100)
            || self.rt_timeout_ms.is_some_and(|n| n > 10_000)
            || self.departure_time.is_some_and(|t| {
                i64::try_from(t)
                    .ok()
                    .and_then(|t| chrono::Utc.timestamp_opt(t, 0).single())
                    .is_none()
            })
        {
            return Err("Invalid nearby departure parameters".into());
        }
        Ok(())
    }
}

// OSM station groups are merged across chateaux in the response. Non-OSM
// station identifiers, like GTFS route identifiers, are chateau-scoped.
#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Clone, Debug)]
enum PriorityStation {
    Osm(i64),
    Feed(String, StationKey),
}

impl PriorityStation {
    fn new(chateau: &str, key: &StationKey) -> Self {
        match key {
            StationKey::Osm(id) => Self::Osm(*id),
            _ => Self::Feed(chateau.to_owned(), key.clone()),
        }
    }
}

fn station_key_for_stop(stop: &crate::models::Stop) -> StationKey {
    if let Some(id) = stop.osm_station_id {
        StationKey::Osm(id)
    } else if let Some(parent) = &stop.parent_station {
        StationKey::Parent(parent.clone())
    } else {
        StationKey::Stop(stop.gtfs_id.clone())
    }
}

// Match the response's station-group classification, including the National
// Rail exceptions, rather than treating every route of type 2 as a station.
fn is_station_group(
    chateau: &str,
    mode: i16,
    agency_id: Option<&str>,
    is_ld_chateau: bool,
    station_key: &StationKey,
) -> bool {
    let is_long_distance = mode == 2
        && is_ld_chateau
        && !(chateau == "nationalrailuk"
            && agency_id.is_some_and(|id| ["XR", "HX", "LO"].contains(&id)));
    mode != 0
        && mode != 1
        && (is_long_distance || (matches!(station_key, StationKey::Osm(_)) && mode != 3))
}

#[derive(Debug, Default)]
pub struct PrioritySelection {
    local_routes: HashSet<(String, String)>,
    station: Option<PriorityStation>,
    station_routes: HashSet<(String, String)>,
    next_routes: HashSet<(String, String)>,
}

impl PrioritySelection {
    fn contains_route(&self, chateau: &str, route: &str) -> bool {
        let key = (chateau.to_owned(), route.to_owned());
        self.local_routes.contains(&key) || self.station_routes.contains(&key)
    }

    fn contains_next(&self, chateau: &str, route: &str) -> bool {
        self.next_routes.contains(&(chateau.to_owned(), route.to_owned()))
    }

    fn contains_departure(
        &self,
        chateau: &str,
        route: &str,
        station_key: &StationKey,
        station_group: bool,
    ) -> bool {
        let key = (chateau.to_owned(), route.to_owned());
        if station_group {
            self.station_routes.contains(&key)
                && self.station.as_ref() == Some(&PriorityStation::new(chateau, station_key))
        } else {
            self.local_routes.contains(&key)
        }
    }
}

#[derive(Clone, Default)]
pub enum RouteSelection {
    #[default]
    All,
    Only(Arc<PrioritySelection>),
    Except(Arc<PrioritySelection>),
    Next(Arc<PrioritySelection>),
    ExceptBoth(Arc<PrioritySelection>),
}

impl RouteSelection {
    // Coarse filter before the itinerary query. A single railway route can
    // have departures in both phases, so Except cannot reject it by route ID.
    fn may_include_route(&self, chateau: &str, route: &str) -> bool {
        match self {
            Self::Only(priority) => priority.contains_route(chateau, route),
            Self::Next(priority) => priority.contains_next(chateau, route),
            _ => true,
        }
    }

    // Limit the first two loads to the selected routes/chateaux. The final
    // phase cannot exclude whole rail routes: they may serve other stations.
    fn included_route_ids(&self, chateau: &str) -> Option<Vec<String>> {
        let ids: HashSet<_> = match self {
            Self::Only(priority) => priority.local_routes.iter().chain(&priority.station_routes)
                .filter(|(feed, _)| feed == chateau)
                .map(|(_, route)| route.clone())
                .collect(),
            Self::Next(priority) => priority.next_routes.iter()
                .filter(|(feed, _)| feed == chateau)
                .map(|(_, route)| route.clone())
                .collect(),
            _ => return None,
        };
        Some(ids.into_iter().collect())
    }

    fn excluded_local_route_ids(&self, chateau: &str) -> Vec<String> {
        match self {
            Self::Except(priority) => priority.local_routes.iter()
                .filter(|(feed, _)| feed == chateau)
                .map(|(_, route)| route.clone())
                .collect(),
            Self::ExceptBoth(priority) => priority.local_routes.iter().chain(&priority.next_routes)
                .filter(|(feed, _)| feed == chateau)
                .map(|(_, route)| route.clone())
                .collect(),
            _ => Vec::new(),
        }
    }

    fn allows(
        &self,
        chateau: &str,
        route: &str,
        station_key: &StationKey,
        station_group: bool,
    ) -> bool {
        match self {
            Self::All => true,
            Self::Only(priority) => priority.contains_departure(chateau, route, station_key, station_group),
            Self::Except(priority) => !priority.contains_departure(chateau, route, station_key, station_group),
            Self::Next(priority) => !station_group && priority.contains_next(chateau, route),
            Self::ExceptBoth(priority) => {
                !priority.contains_departure(chateau, route, station_key, station_group)
                    && (station_group || !priority.contains_next(chateau, route))
            }
        }
    }
}

impl PreparedNearby {
    pub async fn load(
        &self,
        pool: Arc<CatenaryPostgresPool>,
        aspen_chateau_cache: Arc<crate::aspen::lib::AspenChateauCache>,
        selection: RouteSelection,
    ) -> Result<NearbyDeparturesV3Response, String> {
        let query = &self.query;
        let rt_timeout_ms = query.rt_timeout_ms.unwrap_or(2000);

        let start = self.start;
        let mut initial_sql_timings = vec![self.stop_sql_timing.clone()];
        initial_sql_timings.extend(snapshot_sql_timings(&self.priority_sql_timings));
        let sql_timings: SharedSqlTimings = Arc::new(std::sync::Mutex::new(initial_sql_timings));
        let db_connection_time = self.db_connection_time;
        let limit_per_station = query.limit_per_station.unwrap_or(10);
        let limit_per_headsign = query.limit_per_headsign.unwrap_or(20);
        let skip_realtime = query.skip_realtime.unwrap_or(false);

        let departure_time = query
            .departure_time
            .unwrap_or_else(|| crate::duration_since_unix_epoch().as_secs());
        let departure_time_chrono = chrono::Utc.timestamp_opt(departure_time as i64, 0).unwrap();

        let long_distance_chateaux: HashSet<&str> = LONG_DISTANCE_CHATEAUX.iter().copied().collect();

        let long_distance_chateaux_arc = Arc::new(long_distance_chateaux);

        let input_point = geo::Point::new(query.lon, query.lat);
        let radius = query.radius.unwrap_or(5000.0);
        let stops = &self.stops;
        let stops_fetch_time = self.stops_fetch_time;
        if stops.is_empty() {
            return Ok(NearbyDeparturesV3Response {
                long_distance: vec![],
                local: vec![],
                routes: HashMap::new(),
                stops: HashMap::new(),
                debug: NearbyDeparturesDebug {
                    total_time_ms: start.elapsed().as_millis(),
                    db_connection_time_ms: db_connection_time.as_millis(),
                    stops_fetch_time_ms: stops_fetch_time.as_millis(),
                    etcd_connection_time_ms: 0,
                    pipeline_processing_time_ms: 0,
                    initial_stop_count: Some(0),
                    initial_osm_station_count: Some(0),
                    sql_queries: snapshot_sql_timings(&sql_timings),
                    processing: NearbyProcessingDebug::default(),
                    processing_by_chateau: BTreeMap::new(),
                },
            });
        }

        // 4. Group Stops
        let mut station_groups: HashMap<(String, StationKey), Vec<crate::models::Stop>> =
            HashMap::new();
        let mut station_group_metadata: HashMap<(String, StationKey), (String, f64, f64, f64)> =
            HashMap::new();
        let mut stop_dist_map: HashMap<(String, String), f64> = HashMap::new();
        let mut stop_full_info_map: HashMap<(String, String), crate::models::Stop> = HashMap::new();

        let number_of_unique_osm_stations = stops
            .iter()
            .filter_map(|s| s.osm_station_id)
            .collect::<AHashSet<_>>()
            .len();

        let distance_limits =
            DepartureDistanceLimits::new(radius, stops.len(), number_of_unique_osm_stations);

        for stop in stops {
            let Some(point) = &stop.point else {
                continue;
            };
            let stop_point = geo::Point::new(point.x, point.y);
            let dist = Haversine.distance(input_point, stop_point);

            if !distance_limits.allows_stop(
                stop.primary_route_type,
                &stop.route_types,
                &stop.children_route_types,
                dist,
            ) {
                continue;
            }

            let stop_key = (stop.chateau.clone(), stop.gtfs_id.clone());
            stop_dist_map.insert(stop_key.clone(), dist);
            stop_full_info_map.insert(stop_key, stop.clone());

            let key = station_key_for_stop(stop);

            let map_key = (stop.chateau.clone(), key.clone());
            station_groups
                .entry(map_key.clone())
                .or_default()
                .push(stop.clone());

            let entry = station_group_metadata.entry(map_key).or_insert((
                stop.name.clone().unwrap_or_default(),
                stop_point.y(),
                stop_point.x(),
                dist,
            ));

            if dist < entry.3 {
                *entry = (
                    stop.name.clone().unwrap_or_default(),
                    stop_point.y(),
                    stop_point.x(),
                    dist,
                );
            }
        }

        let etcd_connection_time = std::time::Duration::from_secs(0);

        // 5. Fetch Data
        let mut chateau_stops: HashMap<String, HashSet<String>> = HashMap::new();
        for ((chateau, _), stops) in &station_groups {
            for stop in stops {
                chateau_stops
                    .entry(chateau.clone())
                    .or_default()
                    .insert(stop.gtfs_id.clone());
            }
        }

        let stop_to_key_map_all: Arc<HashMap<(String, String), StationKey>> = Arc::new(
            stops
                .iter()
                .map(|s| {
                    (
                        (s.chateau.clone(), s.gtfs_id.clone()),
                        station_key_for_stop(s),
                    )
                })
                .collect(),
        );
        let stop_full_info_map_arc = Arc::new(stop_full_info_map);
        let stop_dist_map_arc = Arc::new(stop_dist_map);

        let aspen_chateau_cache_arc = aspen_chateau_cache.clone();

        let mut chateau_futures = Vec::new();
        for (chateau, stop_ids) in chateau_stops {
            if selection.included_route_ids(&chateau).is_some_and(|ids| ids.is_empty()) {
                continue;
            }
            let pool = pool.clone();
            let stop_ids_vec: Vec<String> = stop_ids.into_iter().collect();
            let aspen_chateau_cache_clone = aspen_chateau_cache_arc.clone();
            let dep_time = departure_time_chrono;
            let chateau_clone = chateau.clone();
            let ld_arc_clone = long_distance_chateaux_arc.clone();
            let stop_to_key_map = stop_to_key_map_all.clone();
            // GTFS stop IDs are scoped to a chateau. Do not let another feed's
            // identically named stop overwrite its distance or mode metadata.
            let full_info_map_clone: Arc<HashMap<String, crate::models::Stop>> = Arc::new(
                stop_full_info_map_arc
                    .iter()
                    .filter(|((c, _), _)| c == &chateau)
                    .map(|((_, id), stop)| (id.clone(), stop.clone()))
                    .collect(),
            );
            let stop_dist_map_clone = Arc::new(
                stop_dist_map_arc
                    .iter()
                    .filter(|((c, _), _)| c == &chateau)
                    .map(|((_, id), distance)| (id.clone(), *distance))
                    .collect(),
            );
            let stop_platform_map = Arc::new(
                full_info_map_clone
                    .iter()
                    .map(|(id, stop)| (id.clone(), stop.platform_code.clone()))
                    .collect(),
            );
            let stop_name_map = Arc::new(
                full_info_map_clone
                    .iter()
                    .map(|(id, stop)| (id.clone(), stop.name.clone()))
                    .collect(),
            );

            let selection = selection.clone();
            let sql_timings_for_chateau = sql_timings.clone();
            chateau_futures.push(async move {
                fetch_chateau_data(
                    pool,
                    chateau_clone,
                    stop_ids_vec,
                    dep_time,
                    aspen_chateau_cache_clone,
                    ld_arc_clone,
                    stop_to_key_map,
                    stop_platform_map,
                    stop_name_map,
                    full_info_map_clone,
                    stop_dist_map_clone,
                    distance_limits,
                    skip_realtime,
                    rt_timeout_ms,
                    selection,
                    sql_timings_for_chateau,
                    start,
                )
                .await
            });
        }

        let pipeline_start = Instant::now();

        // Flatten results while streaming to keep peak memory lower.
        let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> =
            HashMap::new();
        let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> =
            HashMap::new();
        let mut local_route_meta: HashMap<LocalRouteKey, (crate::models::Route, String)> =
            HashMap::new();
        let mut routes_output: HashMap<String, HashMap<String, RouteInfoExport>> = HashMap::new();
        let mut stops_output: HashMap<String, HashMap<String, StopOutputV3>> = HashMap::new();

        let mut processing = NearbyProcessingDebug::default();
        let mut processing_by_chateau = BTreeMap::new();
        let mut chateau_stream = futures::stream::iter(chateau_futures).buffer_unordered(3);
        while let Some(res) = chateau_stream.next().await {
            if let Some((ld_part, local_part, route_part, stop_part, debug_chateau, chateau_debug)) = res {
                processing.accumulate(&chateau_debug);
                processing_by_chateau.insert(debug_chateau, chateau_debug);
                let grouping_started = Instant::now();
                let mut chateau_opt = None;
                if let Some((c, _)) = ld_part.keys().next() {
                    chateau_opt = Some(c.clone());
                } else if let Some(k) = local_part.keys().next() {
                    chateau_opt = Some(k.chateau.clone());
                }

                for (k, v) in ld_part {
                    chateau_opt = Some(k.0.clone());
                    ld_departures_by_group.entry(k).or_default().extend(v);
                }
                for (k, (route, agency_name, headsigns)) in local_part {
                    chateau_opt = Some(k.chateau.clone());
                    local_route_meta
                        .entry(k.clone())
                        .or_insert((route, agency_name));
                    for (h, items) in headsigns {
                        local_departures
                            .entry(k.clone())
                            .or_default()
                            .entry(h)
                            .or_default()
                            .extend(items);
                    }
                }
                if let Some(c) = chateau_opt {
                    routes_output
                        .entry(c.clone())
                        .or_default()
                        .extend(route_part);
                    stops_output.entry(c).or_default().extend(stop_part);
                }
                processing.output_grouping_ms += grouping_started.elapsed().as_secs_f64() * 1000.0;
            }
        }

        let pipeline_processing_time = pipeline_start.elapsed();
        let final_grouping_started = Instant::now();

        // A. Long Distance Output Generation
        let mut ld_output: Vec<StationDepartureGroupExport> = Vec::new();
        for ((chateau, key), meta) in station_group_metadata {
            if let Some(mut deps) = ld_departures_by_group.remove(&(chateau.clone(), key.clone())) {
                deps.sort_by_key(|d| {
                    d.realtime_departure
                        .or(d.realtime_arrival)
                        .or(d.scheduled_departure)
                        .or(d.scheduled_arrival)
                        .unwrap_or(u64::MAX)
                });
                deps.truncate(limit_per_station);

                let (osm_id, rep_id) = match key {
                    StationKey::Osm(id) => (Some(id), "".to_string()),
                    StationKey::Parent(ref id) => (None, id.clone()),
                    StationKey::Stop(ref id) => (None, id.clone()),
                };

                let timezone =
                    tz_search::lookup(meta.1, meta.2).unwrap_or_else(|| String::from("Etc/GMT"));

                ld_output.push(StationDepartureGroupExport {
                    station_name: meta.0,
                    osm_station_id: osm_id,
                    distance_m: meta.3,
                    departures: deps,
                    lat: meta.1,
                    lon: meta.2,
                    timezone,
                });
            }
        }

        // Merge OSM Stations logic
        let osm_ids_to_fetch: Vec<i64> =
            ld_output.iter().filter_map(|g| g.osm_station_id).collect();
        let mut osm_station_info_map: HashMap<i64, crate::models::OsmStation> = HashMap::new();

        processing.output_grouping_ms += final_grouping_started.elapsed().as_secs_f64() * 1000.0;
        if !osm_ids_to_fetch.is_empty() {
            let osm_stations_data: Vec<crate::models::OsmStation> = timed_nearby_sql!(
                sql_timings,
                start,
                "",
                "osm_stations",
                osm_ids_to_fetch.len(),
                pool,
                crate::schema::gtfs::osm_stations::dsl::osm_stations
                    .filter(crate::schema::gtfs::osm_stations::osm_id.eq_any(&osm_ids_to_fetch)),
                crate::models::OsmStation
            )
            .unwrap_or_default();
            for s in osm_stations_data {
                osm_station_info_map.insert(s.osm_id, s);
            }
        }

        let final_grouping_started = Instant::now();
        let mut merged_ld_output: Vec<StationDepartureGroupExport> =
            Vec::with_capacity(ld_output.len());
        let mut osm_station_map: HashMap<i64, usize> = HashMap::new();

        for group in ld_output {
            if let Some(osm_id) = group.osm_station_id {
                if let Some(&idx) = osm_station_map.get(&osm_id) {
                    let existing = &mut merged_ld_output[idx];
                    existing.departures.extend(group.departures);
                } else {
                    let idx = merged_ld_output.len();
                    let mut new_group = group.clone();
                    if let Some(osm_info) = osm_station_info_map.get(&osm_id) {
                        if let Some(n) = &osm_info.name {
                            new_group.station_name = n.clone();
                        }
                        new_group.lat = osm_info.point.y;
                        new_group.lon = osm_info.point.x;
                        let osm_point = geo::Point::new(osm_info.point.x, osm_info.point.y);
                        new_group.distance_m = Haversine.distance(input_point, osm_point);
                    }
                    merged_ld_output.push(new_group);
                    osm_station_map.insert(osm_id, idx);
                }
            } else {
                merged_ld_output.push(group);
            }
        }

        for group in &mut merged_ld_output {
            if group.osm_station_id.is_some() {
                group.departures.sort_by_key(|d| {
                    d.realtime_departure
                        .or(d.realtime_arrival)
                        .or(d.scheduled_departure)
                        .or(d.scheduled_arrival)
                        .unwrap_or(u64::MAX)
                });
                group.departures.truncate(limit_per_station);
            }
        }
        let mut ld_output = merged_ld_output;
        ld_output.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap());

        // B. Local Output Generation
        let mut local_output: Vec<DepartureRouteGroupExportV3> = Vec::new();
        for (r_key, headsigns) in local_departures {
            let (route, a_name) = local_route_meta.get(&r_key).unwrap();
            let mut min_dist = 9999999.0;
            let mut sorted_headsigns = HashMap::new();

            for (h, mut items) in headsigns {
                items.sort_by_key(|i| {
                    i.departure_realtime
                        .or(i.arrival_realtime)
                        .or(i.departure_schedule)
                        .or(i.arrival_schedule)
                        .unwrap_or(u64::MAX)
                });
                items.truncate(limit_per_headsign);
                sorted_headsigns.insert(h, items);
            }

            for items in sorted_headsigns.values() {
                for item in items {
                    if let Some(d) =
                        stop_dist_map_arc.get(&(r_key.chateau.clone(), item.stop_id.to_string()))
                    {
                        if *d < min_dist {
                            min_dist = *d;
                        }
                    }
                }
            }

            local_output.push(DepartureRouteGroupExportV3 {
                chateau_id: r_key.chateau,
                route_id: CompactString::from(r_key.route_id),
                color: route.color.clone().map(CompactString::from),
                text_color: route.text_color.clone().map(CompactString::from),
                short_name: route.short_name.clone().map(CompactString::from),
                long_name: route.long_name.clone(),
                route_type: route.route_type as i16,
                agency_name: Some(a_name.clone()),
                headsigns: sorted_headsigns,
                closest_distance: min_dist,
            });
        }
        local_output.sort_by(|a, b| a.closest_distance.partial_cmp(&b.closest_distance).unwrap());

        processing.output_grouping_ms += final_grouping_started.elapsed().as_secs_f64() * 1000.0;
        Ok(NearbyDeparturesV3Response {
            long_distance: ld_output,
            local: local_output,
            routes: routes_output,
            stops: stops_output,
            debug: NearbyDeparturesDebug {
                total_time_ms: start.elapsed().as_millis(),
                db_connection_time_ms: db_connection_time.as_millis(),
                stops_fetch_time_ms: stops_fetch_time.as_millis(),
                etcd_connection_time_ms: etcd_connection_time.as_millis(),
                pipeline_processing_time_ms: pipeline_processing_time.as_millis(),
                initial_stop_count: Some(stops.len()),
                initial_osm_station_count: Some(number_of_unique_osm_stations),
                sql_queries: snapshot_sql_timings(&sql_timings),
                processing,
                processing_by_chateau,
            },
        })
    }
}

async fn fetch_chateau_data(
    pool: Arc<CatenaryPostgresPool>,
    chateau: String,
    stop_ids: Vec<String>,
    departure_time_chrono: chrono::DateTime<chrono::Utc>,
    aspen_chateau_cache: Arc<crate::aspen::lib::AspenChateauCache>,
    long_distance_chateaux: Arc<HashSet<&str>>,
    stop_to_key_map: Arc<HashMap<(String, String), StationKey>>,
    stop_platform_map: Arc<HashMap<String, Option<String>>>,
    stop_name_map: Arc<HashMap<String, Option<String>>>,
    stop_full_info_map: Arc<HashMap<String, crate::models::Stop>>,
    stop_dist_map: Arc<HashMap<String, f64>>,
    distance_limits: DepartureDistanceLimits,
    skip_realtime: bool,
    rt_timeout_ms: u64,
    selection: RouteSelection,
    sql_timings: SharedSqlTimings,
    request_started: Instant,
) -> Option<(
    HashMap<(String, StationKey), Vec<DepartureItem>>,
    HashMap<
        LocalRouteKey,
        (
            crate::models::Route,
            String,
            HashMap<String, Vec<LocalDepartureItem>>,
        ),
    >,
    HashMap<String, RouteInfoExport>,
    HashMap<String, StopOutputV3>,
    String,
    NearbyProcessingDebug,
)> {
    let mut processing = NearbyProcessingDebug::default();
    let query_slots = tokio::sync::Semaphore::new(PATTERN_QUERY_CONCURRENCY);
    let priority_route_ids = selection.included_route_ids(&chateau);
    let excluded_local_route_ids = selection.excluded_local_route_ids(&chateau);

    // 1. Determine Date Window
    let lookback_days = match chateau.as_str() {
        "sncb" | "schweiz" | "sncf" | "deutschland" => 2,
        _ => 14,
    };
    let date_window_start =
        departure_time_chrono.date_naive() - chrono::Duration::days(lookback_days);
    let date_window_end = departure_time_chrono.date_naive() + chrono::Duration::days(1);

    // 2. Optimization: Filter Redundant Stops for Local Transport
    // CRITICAL FIX: Do NOT apply this optimization for Long Distance Chateaux (e.g. SNCF)
    // This fixes the issue where agencies like SNCF/RER were being dropped because they looked like "local" stops
    // but didn't map correctly in direction_patterns.
    let is_ld_chateau = long_distance_chateaux.contains(chateau.as_str());

    let mut final_stop_ids = Vec::new();
    let mut stops_to_check = Vec::new();

    if is_ld_chateau {
        // For LD agencies, check ALL stops to prevent data loss at complex stations like Gare du Nord
        final_stop_ids = stop_ids.clone();
    } else {
        for stop_id in &stop_ids {
            if let Some(stop) = stop_full_info_map.get(stop_id) {
                match stop.primary_route_type {
                    Some(0) | Some(1) | Some(3) => stops_to_check.push(stop_id.clone()),
                    _ => final_stop_ids.push(stop_id.clone()),
                }
            } else {
                final_stop_ids.push(stop_id.clone());
            }
        }

        if !stops_to_check.is_empty() {
            let direction_rows: Vec<crate::models::DirectionPatternRow> = timed_nearby_sql!(
                    sql_timings,
                    request_started,
                    &chateau,
                    "nearest_direction_rows",
                    stops_to_check.len(),
                    pool,
                    crate::schema::gtfs::direction_pattern::dsl::direction_pattern
                        .filter(crate::schema::gtfs::direction_pattern::chateau.eq(chateau.clone()))
                        .filter(
                            crate::schema::gtfs::direction_pattern::stop_id.eq_any(&stops_to_check),
                        )
                        .select(crate::models::DirectionPatternRow::as_select()),
                    crate::models::DirectionPatternRow
                )
            .unwrap_or_default();

            let mut best_stops: HashMap<(String, Option<i16>), (f64, String)> = HashMap::new();
            for row in direction_rows {
                if let Some(dist) = stop_dist_map.get(row.stop_id.as_str()) {
                    let key = (row.direction_pattern_id, row.stop_headsign_idx);
                    match best_stops.entry(key) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            if *dist < e.get().0 {
                                e.insert((*dist, row.stop_id.to_string()));
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert((*dist, row.stop_id.to_string()));
                        }
                    }
                }
            }
            final_stop_ids.extend(best_stops.values().map(|(_, s)| s.clone()));
        }
    }

    // A stop can be selected for several direction/headsign pairs. Keep batches
    // disjoint so splitting SQL ANY predicates cannot introduce duplicate rows.
    final_stop_ids.sort_unstable();
    final_stop_ids.dedup();

    let t_v3_fetch = Instant::now();

    // 3. Optimized Fetch Pipeline (The "OSM-ification")
    // Instead of fetching all trips then filtering, we calculate Active Services first.

    // A. Fetch Direction & Itinerary Meta
    let direction_rows_list: Vec<crate::models::DirectionPatternRow> =
        load_pattern_batches(&final_stop_ids, &query_slots, |stop_batch| async {
            timed_nearby_sql!(
                sql_timings,
                request_started,
                &chateau,
                "direction_rows",
                stop_batch.len(),
                pool,
                crate::schema::gtfs::direction_pattern::dsl::direction_pattern
                    .filter(crate::schema::gtfs::direction_pattern::chateau.eq(&chateau))
                    .filter(crate::schema::gtfs::direction_pattern::stop_id.eq_any(stop_batch))
                    .select(crate::models::DirectionPatternRow::as_select()),
                crate::models::DirectionPatternRow
            )
        })
        .await
        .unwrap_or_default();

    let direction_pattern_ids: Vec<String> = direction_rows_list
        .iter()
        .map(|r| r.direction_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Start with itinerary occurrences at the *selected stops*, not every
    // itinerary belonging to a direction serving one of these stops.
    // The existing (chateau, stop_id) index makes this an efficient anchor.
    // Joining on the complete feed/attempt/pattern key prevents accidental
    // matches across GTFS imports sharing an itinerary_pattern_id.
    let direction_stop_ids: Vec<String> = direction_rows_list
        .iter()
        .map(|r| r.stop_id.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let matching_rows_and_metadata: Vec<(
        crate::models::ItineraryPatternRow,
        NearbyItineraryPatternMeta,
    )> = load_pattern_batches(&direction_stop_ids, &query_slots, |stop_batch| async {
        use crate::schema::gtfs::{itinerary_pattern as ip, itinerary_pattern_meta as im, routes};
        let input_ids = stop_batch.len();
        let mut query = ip::table
            .inner_join(
                im::table.on(ip::onestop_feed_id
                    .eq(im::onestop_feed_id)
                    .and(ip::attempt_id.eq(im::attempt_id))
                    .and(ip::itinerary_pattern_id.eq(im::itinerary_pattern_id)))
            )
            .inner_join(routes::table.on(
                routes::chateau.eq(im::chateau)
                    .and(routes::onestop_feed_id.eq(im::onestop_feed_id))
                    .and(routes::attempt_id.eq(im::attempt_id))
                    .and(routes::route_id.eq(im::route_id)),
            ))
            .filter(ip::chateau.eq(&chateau))
            .filter(im::chateau.eq(&chateau))
            .filter(routes::route_type.ne(COACH_ROUTE_TYPE))
            .filter(ip::stop_id.eq_any(stop_batch))
            .filter(im::direction_pattern_id.eq_any(&direction_pattern_ids))
            .select((
                crate::models::ItineraryPatternRow::as_select(),
                NearbyItineraryPatternMeta::as_select(),
            ))
            .into_boxed();
        if let Some(ids) = &priority_route_ids {
            query = query.filter(im::route_id.eq_any(ids));
        }
        // These three modes always produce local route groups (see
        // is_station_group). Other modes can still have station departures
        // in the final phase even when their local group was sent earlier.
        if !excluded_local_route_ids.is_empty() {
            query = query.filter(
                routes::route_type.ne_all([0_i16, 1, 3])
                    .or(im::route_id.ne_all(&excluded_local_route_ids)),
            );
        }
        timed_nearby_sql!(
            sql_timings,
            request_started,
            &chateau,
            "itinerary_rows_by_selected_stops",
            input_ids,
            pool,
            query,
            (
                crate::models::ItineraryPatternRow,
                NearbyItineraryPatternMeta
            )
        )
    })
    .await
    .unwrap_or_default();

    // Only materialize one copy of a pattern's metadata. Multiple stops and
    // repeated occurrences of the same stop must remain distinct row entries.
    let mut itin_meta_list = Vec::new();
    let mut itins_result = Vec::with_capacity(matching_rows_and_metadata.len());
    let mut seen_itinerary_ids = HashSet::new();
    for (row, meta) in matching_rows_and_metadata {
        if seen_itinerary_ids.insert(meta.itinerary_pattern_id.clone()) {
            itin_meta_list.push(meta);
        }
        itins_result.push(row);
    }

    // Keep the coarse route restriction before loading the route dictionaries.
    itin_meta_list.retain(|meta| selection.may_include_route(&chateau, meta.route_id.as_str()));

    // Resolve actual route modes before any itinerary-row or trip lookup.
    // primary_route_type cannot distinguish the bus and metro at a shared stop.
    let candidate_route_ids: Vec<CompactString> = itin_meta_list
        .iter()
        .map(|meta| meta.route_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let routes_result: Vec<crate::models::Route> = {
        timed_nearby_sql!(
            sql_timings,
            request_started,
            &chateau,
            "routes",
            candidate_route_ids.len(),
            pool,
            crate::schema::gtfs::routes::dsl::routes
                .filter(crate::schema::gtfs::routes::chateau.eq(&chateau))
                .filter(crate::schema::gtfs::routes::route_id.eq_any(&candidate_route_ids))
                .select(crate::models::Route::as_select()),
            crate::models::Route
        )
        .unwrap_or_default()
    };
    let route_types: HashMap<_, _> = routes_result
        .iter()
        .map(|route| (route.route_id.as_str(), route.route_type))
        .collect();
    let mut direction_distances: HashMap<&str, f64> = HashMap::new();
    for row in &direction_rows_list {
        if let Some(distance) = stop_dist_map.get(row.stop_id.as_str()) {
            direction_distances
                .entry(row.direction_pattern_id.as_str())
                .and_modify(|closest| *closest = closest.min(*distance))
                .or_insert(*distance);
        }
    }
    itin_meta_list.retain(|meta| {
        let distance = meta
            .direction_pattern_id
            .as_deref()
            .and_then(|id| direction_distances.get(id));
        match (route_types.get(meta.route_id.as_str()), distance) {
            (Some(mode), Some(distance)) => distance_limits.allows(*mode, *distance),
            _ => false,
        }
    });

    let filtered_itinerary_ids: Vec<String> = itin_meta_list
        .iter()
        .map(|m| m.itinerary_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Active services optimization removed (performance regression)

    // C. Fetch direction labels. Stop rows have already been fetched by
    // selected stop ID, so no itinerary-ID fan-out is needed here.
    let direction_meta_result: Vec<crate::models::DirectionPatternMeta> = load_pattern_batches(
        &direction_pattern_ids,
        &query_slots,
        |pattern_batch| async {
            timed_nearby_sql!(
                sql_timings,
                request_started,
                &chateau,
                "direction_metadata",
                pattern_batch.len(),
                pool,
                crate::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                    .filter(crate::schema::gtfs::direction_pattern_meta::chateau.eq(&chateau))
                    .filter(
                        crate::schema::gtfs::direction_pattern_meta::direction_pattern_id
                            .eq_any(pattern_batch)
                    )
                    .select(crate::models::DirectionPatternMeta::as_select()),
                crate::models::DirectionPatternMeta
            )
        },
    )
    .await
    .unwrap_or_default();

    // A retained itinerary can still contain a distant bus row when a closer
    // stop made its direction eligible. Exclude each row before fetching trips
    // and before the service-date/realtime loops.
    let routes_by_id: HashMap<_, _> = routes_result
        .iter()
        .map(|route| (route.route_id.as_str(), route))
        .collect();
    let itinerary_routes: HashMap<_, _> = itin_meta_list
        .iter()
        .filter_map(|meta| {
            routes_by_id
                .get(meta.route_id.as_str())
                .map(|route| (meta.itinerary_pattern_id.as_str(), *route))
        })
        .collect();
    itins_result.retain(|row| {
        let Some(route) = itinerary_routes.get(row.itinerary_pattern_id.as_str()) else {
            return false;
        };
        let Some(distance) = stop_dist_map.get(row.stop_id.as_str()) else {
            return false;
        };
        let Some(station_key) = stop_to_key_map.get(&(chateau.clone(), row.stop_id.to_string())) else {
            return false;
        };
        distance_limits.allows(route.route_type, *distance)
            && selection.allows(
                &chateau,
                route.route_id.as_str(),
                station_key,
                is_station_group(&chateau, route.route_type, route.agency_id.as_deref(), is_ld_chateau, station_key),
            )
    });

    // Preserve the route restriction after mode/distance filtering. This set
    // previously lived in the removed itinerary-row fetch block.
    let route_ids: Vec<CompactString> = itin_meta_list
        .iter()
        .map(|meta| meta.route_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Extract ACTUAL itinerary IDs that match the stop
    let actual_itinerary_ids: Vec<String> = itins_result
        .iter()
        .map(|r| r.itinerary_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Query only itineraries that actually serve these stops. The existing
    // (chateau, itinerary_pattern_id) index supports each batch directly.
    let trips_compressed_result: Vec<crate::models::CompressedTrip> = load_pattern_batches(
        &actual_itinerary_ids,
        &query_slots,
        |itinerary_batch| async {
            timed_nearby_sql!(
                sql_timings,
                request_started,
                &chateau,
                "compressed_trips",
                itinerary_batch.len(),
                pool,
                crate::schema::gtfs::trips_compressed::dsl::trips_compressed
                    .filter(crate::schema::gtfs::trips_compressed::chateau.eq(&chateau))
                    .filter(crate::schema::gtfs::trips_compressed::route_id.eq_any(&route_ids))
                    .filter(
                        crate::schema::gtfs::trips_compressed::itinerary_pattern_id
                            .eq_any(itinerary_batch),
                    )
                    .select(crate::models::CompressedTrip::as_select()),
                crate::models::CompressedTrip
            )
        },
    )
    .await
    .unwrap_or_default();

    // println!(
    //     "V3_OPT: Fetch for {} took {}ms. Found {} trips (filtered by {} itins).",
    //     chateau,
    //     t_v3_fetch.elapsed().as_millis(),
    //     trips_compressed_result.len(),
    //     actual_itinerary_ids.len()
    // );

    processing.compressed_trip_count = trips_compressed_result.len();

    // Fetch Calendar (Moved here to optimize performance)
    let mut service_ids_to_fetch = HashSet::new();
    for trip in &trips_compressed_result {
        service_ids_to_fetch.insert(trip.service_id.clone());
    }

    let service_ids_vec: Vec<CompactString> = service_ids_to_fetch.into_iter().collect();

    let (calendar, calendar_dates) = if !service_ids_vec.is_empty() {
        let pool_cal1 = pool.clone();
        let pool_cal2 = pool.clone();
        let chateau_cal1 = chateau.clone();
        let chateau_cal2 = chateau.clone();
        let s_ids1 = service_ids_vec.clone();
        let s_ids2 = service_ids_vec;

        let sql_timings_calendar = sql_timings.clone();
        let sql_timings_calendar_dates = sql_timings.clone();
        tokio::join!(
            async move {
                timed_nearby_sql!(
                    sql_timings_calendar,
                    request_started,
                    &chateau_cal1,
                    "calendar",
                    s_ids1.len(),
                    pool_cal1,
                    crate::schema::gtfs::calendar::dsl::calendar
                        .filter(crate::schema::gtfs::calendar::chateau.eq(chateau_cal1))
                        .filter(crate::schema::gtfs::calendar::service_id.eq_any(&s_ids1))
                        .select(crate::models::Calendar::as_select()),
                    crate::models::Calendar
                )
                .unwrap_or_default()
            },
            async move {
                timed_nearby_sql!(
                    sql_timings_calendar_dates,
                    request_started,
                    &chateau_cal2,
                    "calendar_dates",
                    s_ids2.len(),
                    pool_cal2,
                    crate::schema::gtfs::calendar_dates::dsl::calendar_dates
                        .filter(crate::schema::gtfs::calendar_dates::chateau.eq(chateau_cal2))
                        .filter(crate::schema::gtfs::calendar_dates::service_id.eq_any(&s_ids2))
                        .filter(
                            crate::schema::gtfs::calendar_dates::gtfs_date.ge(date_window_start)
                        )
                        .filter(crate::schema::gtfs::calendar_dates::gtfs_date.le(date_window_end))
                        .select(crate::models::CalendarDate::as_select()),
                    crate::models::CalendarDate
                )
                .unwrap_or_default()
            }
        )
    } else {
        (vec![], vec![])
    };

    let service_date_started = Instant::now();
    let calendar_struct =
        crate::make_calendar_structure_from_pg(vec![calendar], vec![calendar_dates])
            .unwrap_or_default();
    processing.service_date_processing_ms += service_date_started.elapsed().as_secs_f64() * 1000.0;
    let service_map = calendar_struct.get(&chateau);

    // Build Maps
    let mut itins: BTreeMap<String, Vec<crate::models::ItineraryPatternRow>> = BTreeMap::new();
    for itin in itins_result {
        itins
            .entry(itin.itinerary_pattern_id.clone())
            .or_default()
            .push(itin);
    }

    let mut itin_meta: BTreeMap<String, NearbyItineraryPatternMeta> = BTreeMap::new();
    for meta in itin_meta_list {
        itin_meta.insert(meta.itinerary_pattern_id.clone(), meta);
    }

    let mut direction_meta: BTreeMap<String, crate::models::DirectionPatternMeta> = BTreeMap::new();
    for dm in direction_meta_result {
        direction_meta.insert(dm.direction_pattern_id.clone(), dm);
    }

    let mut direction_rows: BTreeMap<String, Vec<crate::models::DirectionPatternRow>> =
        BTreeMap::new();
    for dr in direction_rows_list {
        direction_rows
            .entry(dr.direction_pattern_id.clone())
            .or_default()
            .push(dr);
    }

    let mut routes: BTreeMap<String, crate::models::Route> = BTreeMap::new();
    for r in routes_result {
        routes.insert(r.route_id.clone(), r);
    }

    // Fetch Agencies
    let agency_ids: Vec<String> = routes
        .values()
        .filter_map(|r| r.agency_id.clone())
        .collect();
    let agencies_list: Vec<crate::models::Agency> = timed_nearby_sql!(
        sql_timings,
        request_started,
        &chateau,
        "agencies",
        agency_ids.len(),
        pool,
        crate::schema::gtfs::agencies::dsl::agencies
            .filter(crate::schema::gtfs::agencies::chateau.eq(chateau.clone()))
            .filter(crate::schema::gtfs::agencies::agency_id.eq_any(&agency_ids))
            .select(crate::models::Agency::as_select()),
        crate::models::Agency
    )
    .unwrap_or_default();
    let agencies: BTreeMap<String, crate::models::Agency> = agencies_list
        .into_iter()
        .map(|a| (a.agency_id.clone(), a))
        .collect();

    // 4. Realtime Data Fetching (Optimized: Only send active trip IDs)
    let trips_compressed: BTreeMap<String, crate::models::CompressedTrip> = trips_compressed_result
        .into_iter()
        .map(|t| (t.trip_id.clone(), t))
        .collect();
    let trip_ids: Vec<String> = trips_compressed.keys().cloned().collect();

    let mut rt_data: Option<crate::aspen::lib::TripsSelectionResponse> = None;
    let mut rt_alerts: BTreeMap<String, crate::aspen_dataset::AspenisedAlert> = BTreeMap::new();

    if !skip_realtime {
        if let Some(meta) = aspen_chateau_cache.get(&chateau) {
            if !trip_ids.is_empty() {
                let timer_to_connect_to_aspen = std::time::Instant::now();
                let client = crate::aspen::lib::spawn_aspen_client_from_ip(&meta.socket).await;
                let time_to_connect_to_aspen = timer_to_connect_to_aspen.elapsed();
                processing.realtime_connection_ms += time_to_connect_to_aspen.as_secs_f64() * 1000.0;
                if let Ok(client) = client {

                    let timer_get_trips = std::time::Instant::now();
                    let timeout_result = tokio::time::timeout(
                        std::time::Duration::from_millis(rt_timeout_ms),
                        async {
                            tokio::join!(
                                client.get_all_trips_with_ids(
                                    tarpc::context::current(),
                                    chateau.clone(),
                                    trip_ids.clone()
                                ),
                                client.get_all_alerts(tarpc::context::current(), chateau.clone()),
                            )
                        },
                    )
                    .await;
                    let time_get_trips = timer_get_trips.elapsed();
                    processing.realtime_rpc_ms += time_get_trips.as_secs_f64() * 1000.0;

                    match timeout_result {
                        Ok((t, a)) => {
                            println!(
                                "nearby deps realtime data fetch chateau {}, aspen connect time {:?}, get trips time {:?}",
                                chateau.as_str(),
                                time_to_connect_to_aspen,
                                time_get_trips
                            );

                            if let Ok(Some(tr)) = t {
                                println!(
                                    "Nearby Departures {} rt trip_updates {}, trip_id_to_trip_updates {}, and stop_id_to_parent_id {}",
                                    chateau.as_str(),
                                    tr.trip_updates.len(),
                                    tr.trip_id_to_trip_update_ids.len(),
                                    tr.stop_id_to_parent_id.len()
                                );

                                processing.legacy_realtime_update_count = tr.trip_updates.len();
                                rt_data = Some(tr);
                            }
                            if let Ok(Some(al)) = a {
                                rt_alerts = al.into_iter().collect();
                            }
                        }
                        Err(_) => {
                            println!(
                                "realtime fetch timeout for chateau {} after {:?}",
                                chateau.as_str(),
                                time_get_trips
                            );
                        }
                    }
                }
            }
        } else {
            eprintln!("Failed to get etcd data for chateau {}", chateau.as_str());
        }
    }

    let mut board_events = Vec::new();

    // 5. Process Data into Objects
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> =
        HashMap::new();

    // Key: LocalRouteKey -> Value: Map<Headsign, List of items>
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> =
        HashMap::new();

    let mut local_route_meta_map: HashMap<LocalRouteKey, (crate::models::Route, String)> =
        HashMap::new();
    let mut route_info_map: HashMap<String, RouteInfoExport> = HashMap::new();
    let mut stop_output_map: HashMap<String, StopOutputV3> = HashMap::new();

    let mut itinerary_final_station_name = HashMap::new();
    if chateau == "île~de~france~mobilités" && !filtered_itinerary_ids.is_empty() {
        let itin_rows_all: Vec<crate::models::ItineraryPatternRow> = load_pattern_batches(
            &filtered_itinerary_ids,
            &query_slots,
            |itinerary_batch| async {
                timed_nearby_sql!(
                    sql_timings,
                    request_started,
                    &chateau,
                    "paris_terminal_itinerary_rows",
                    itinerary_batch.len(),
                    pool,
                    crate::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                        .filter(crate::schema::gtfs::itinerary_pattern::chateau.eq(&chateau))
                        .filter(
                            crate::schema::gtfs::itinerary_pattern::itinerary_pattern_id
                                .eq_any(itinerary_batch),
                        )
                        .select(crate::models::ItineraryPatternRow::as_select()),
                    crate::models::ItineraryPatternRow
                )
            },
        )
        .await
        .unwrap_or_default();

        let mut itinerary_last_stop_id = HashMap::new();
        for row in &itin_rows_all {
            if let Some(meta) = itin_meta.get(&row.itinerary_pattern_id) {
                if row.stop_sequence == meta.row_count - 1 {
                    itinerary_last_stop_id
                        .insert(row.itinerary_pattern_id.clone(), row.stop_id.to_string());
                }
            }
        }

        let last_stop_ids: Vec<String> = itinerary_last_stop_id
            .values()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if !last_stop_ids.is_empty() {
            let stop_names: Vec<(String, Option<String>)> = timed_nearby_sql!(
                sql_timings,
                request_started,
                &chateau,
                "paris_terminal_stop_names",
                last_stop_ids.len(),
                pool,
                crate::schema::gtfs::stops::dsl::stops
                    .filter(crate::schema::gtfs::stops::chateau.eq(chateau.clone()))
                    .filter(crate::schema::gtfs::stops::gtfs_id.eq_any(&last_stop_ids))
                    .select((
                        crate::schema::gtfs::stops::gtfs_id,
                        crate::schema::gtfs::stops::name,
                    )),
                (String, Option<String>)
            )
            .unwrap_or_default();

            let mut stop_names_map = HashMap::new();
            for (sid, name) in stop_names {
                if let Some(n) = name {
                    stop_names_map.insert(sid, n);
                }
            }

            for (itin_id, last_sid) in itinerary_last_stop_id {
                if let Some(name) = stop_names_map.get(&last_sid) {
                    itinerary_final_station_name.insert(itin_id, name.clone());
                }
            }
        }
    }

    let event_construction_started = Instant::now();
    let alerts_idx_timer = std::time::Instant::now();
    let alert_index = AlertIndex::new(&rt_alerts);
    println!(
        "alerts index formation time: {:?}",
        alerts_idx_timer.elapsed()
    );

    let relevant_stop_ids: HashSet<String> = stop_to_key_map
        .keys()
        .filter(|(c, _)| c == &chateau)
        .map(|(_, sid)| sid.clone())
        .collect();

    let mut total_time_added_looking_at_rt_trips = std::time::Duration::new(0, 0);
    let mut total_time_added_looking_at_rt_alerts = std::time::Duration::new(0, 0);

    for trip_id in trip_ids {
        let trip = trips_compressed.get(&trip_id);

        if let Some(trip) = trip {
            let itinerary_meta = match itin_meta.get(&trip.itinerary_pattern_id) {
                Some(m) => m,
                None => continue,
            };
            let route_id = &itinerary_meta.route_id;
            let _route = match routes.get(route_id.as_str()) {
                Some(r) => r,
                None => continue,
            };

            // Service Date Check
            let service_date_started = Instant::now();
            let mut valid_service_dates = Vec::new();
            if let Some(services) = service_map {
                if let Some(service) = services.get(trip.service_id.as_str()) {
                    let base_date = departure_time_chrono.date_naive() - chrono::Duration::days(1);
                    for i in 0..3 {
                        let d = base_date + chrono::Duration::days(i);
                        if crate::datetime_in_service(service, d) {
                            valid_service_dates.push(d);
                        }
                    }
                }
            }

            processing.service_date_processing_ms += service_date_started.elapsed().as_secs_f64() * 1000.0;

            let rows = match itins.get(&trip.itinerary_pattern_id) {
                Some(r) => r,
                None => continue,
            };

            for row in rows {
                if !relevant_stop_ids.contains(row.stop_id.as_str()) {
                    continue;
                }

                if let Some(_station_key) =
                    stop_to_key_map.get(&(chateau.clone(), row.stop_id.to_string()))
                {
                    let dep_time_offset = row
                        .departure_time_since_start
                        .or(row.arrival_time_since_start)
                        .or(row.interpolated_time_since_start)
                        .unwrap_or(0);

                    let is_last_stop = row.stop_sequence == itinerary_meta.row_count - 1;

                    for date in &valid_service_dates {
                        let tz = catenary_time::Tz::from_str_insensitive(&itinerary_meta.timezone)
                            .unwrap_or(catenary_time::UTC);
                        let midnight_ts = tz
                            .from_local_datetime(&date.and_hms_opt(12, 0, 0).unwrap())
                            .single()
                            .map(|t| t.timestamp() - 12 * 3600)
                            .unwrap_or(0);
                        let departure_ts =
                            midnight_ts + trip.start_time as i64 + dep_time_offset as i64;

                        let headsign = row
                            .stop_headsign_idx
                            .and_then(|idx| {
                                direction_meta
                                    .get(itinerary_meta.direction_pattern_id.as_ref()?)
                                    .and_then(|dm| dm.stop_headsigns_unique_list.as_ref())
                                    .and_then(|list| list.get(idx as usize))
                                    .cloned()
                                    .flatten()
                            })
                            .or(direction_meta
                                .get(
                                    itinerary_meta
                                        .direction_pattern_id
                                        .as_ref()
                                        .unwrap_or(&"".to_string()),
                                )
                                .map(|dm| dm.headsign_or_destination.clone()))
                            .unwrap_or_else(|| "Unknown".to_string());

                        // Realtime Logic
                        let mut rt_dep = None;
                        let mut rt_arr = None;
                        let mut rt_platform = None;
                        let mut is_cancelled = false;

                        let timer_looking_at_rt_alerts = std::time::Instant::now();
                        let relevant_alerts =
                            alert_index.search(route_id.as_str(), trip_id.as_str());
                        for alert in relevant_alerts {
                            if alert.effect == Some(1) {
                                // NO_SERVICE
                                let is_active = alert.active_period.iter().any(|ap| {
                                    let start = ap.start.unwrap_or(0);
                                    let end = ap.end.unwrap_or(u64::MAX);
                                    (departure_ts as u64) >= start && (departure_ts as u64) <= end
                                });

                                let applies_to_trip_without_a_referenced_stop = alert
                                    .informed_entity
                                    .iter()
                                    .filter(|e| e.stop_id.is_none())
                                    .any(|e| {
                                        let route_match = e
                                            .route_id
                                            .as_ref()
                                            .is_some_and(|r_id| *r_id == trip.route_id);
                                        let trip_match = e.trip.as_ref().is_some_and(|t| {
                                            t.trip_id
                                                .as_ref()
                                                .is_some_and(|t_id| *t_id == trip.trip_id)
                                        });

                                        route_match || trip_match
                                    });

                                if is_active && applies_to_trip_without_a_referenced_stop {
                                    // Under the GTFS specification, this is not allowed to mark the trip as cancelled
                                }
                            }
                        }
                        total_time_added_looking_at_rt_alerts +=
                            timer_looking_at_rt_alerts.elapsed();

                        let mut active_update: Option<&crate::aspen_dataset::AspenisedTripUpdate> =
                            None;
                        if let Some(data) = &rt_data {
                            let timer_looking_at_rt_trips = std::time::Instant::now();
                            if let Some(update_ids) =
                                data.trip_id_to_trip_update_ids.get(trip_id.as_str())
                            {
                                for uid in update_ids {
                                    if let Some(u) = data.trip_updates.get(uid) {
                                        // ModifiedTripSelector carries its own start_date.
                                        // Never infer a different service day for a dated update:
                                        // this can attach today's vehicle to yesterday's trip.
                                        if let Some(service_date) = u.trip.effective_start_date() {
                                            if service_date == *date {
                                                active_update = Some(u);
                                                break;
                                            }
                                            continue;
                                        }
                                        if u.trip.effective_start_date().is_none() {
                                            for stu in &u.stop_time_update {
                                                let update_time =
                                                    stu.departure.as_ref().and_then(|d| d.time).or(
                                                        stu.arrival.as_ref().and_then(|a| a.time),
                                                    );

                                                if let Some(u_time) = update_time {
                                                    if let Some(static_stop) = rows.iter().find(|r| {
                                                     if let Some(seq) = stu.stop_sequence {
                                                         return r.gtfs_stop_sequence == u32::from(seq);
                                                     }
                                                     if let Some(sid) = &stu.stop_id {
                                                         return crate::stop_matching::rt_stop_matches_scheduled_simple(sid, r.stop_id.as_str())
                                                             && rows.iter().filter(|candidate| {
                                                                 crate::stop_matching::rt_stop_matches_scheduled_simple(sid, candidate.stop_id.as_str())
                                                             }).take(2).count() == 1;
                                                     }
                                                     false
                                                 }) {
                                                     let offset = static_stop.departure_time_since_start
                                                        .or(static_stop.arrival_time_since_start)
                                                        .or(static_stop.interpolated_time_since_start)
                                                        .unwrap_or(0);

                                                     let tz = catenary_time::Tz::from_str_insensitive(&itinerary_meta.timezone).unwrap_or(catenary_time::UTC);

                                                     let implied_start_timestamp = (u_time as i64) - (trip.start_time as i64 + offset as i64);
                                                     // Allow for up to 20 hours of delay/early carrying over days, though matching exact date is usually enough
                                                     // effectively we check if the implied service date is the same as the candidate date
                                                     if let chrono::LocalResult::Single(dt) = tz.timestamp_opt(implied_start_timestamp, 0) {
                                                         if dt.date_naive() == *date {
                                                             active_update = Some(u);
                                                             break;
                                                         }
                                                     }
                                                 }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            total_time_added_looking_at_rt_trips +=
                                timer_looking_at_rt_trips.elapsed();
                        }

                        if let Some(update) = active_update {
                            let timer_looking_at_rt_alerts = std::time::Instant::now();
                            if let Some(stu) = crate::stop_matching::find_stop_time_update(
                                &update.stop_time_update,
                                row.gtfs_stop_sequence,
                                row.stop_id.as_str(),
                                rows.iter().map(|r| r.stop_id.as_str()),
                            ) {
                                if let Some(d) = &stu.departure {
                                    if let Some(t) = d.time {
                                        rt_dep = Some(t as u64);
                                    }
                                } else if let Some(a) = &stu.arrival {
                                    if let Some(t) = a.time {
                                        rt_dep = Some(t as u64);
                                    }
                                }
                                if let Some(a) = &stu.arrival {
                                    if let Some(t) = a.time {
                                        rt_arr = Some(t as u64);
                                    }
                                }
                                if let Some(p) = &stu.platform_string {
                                    rt_platform = Some(p.to_string());
                                }
                                if stu.schedule_relationship == Some(crate::aspen_dataset::AspenisedStopTimeScheduleRelationship::Skipped) { is_cancelled = true; }
                            }
                            if update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) { is_cancelled = true; }
                            total_time_added_looking_at_rt_alerts +=
                                timer_looking_at_rt_alerts.elapsed();
                        }

                        let display_platform = rt_platform.or_else(|| {
                            stop_platform_map
                                .get(row.stop_id.as_str())
                                .cloned()
                                .flatten()
                        });

                        let mut event = crate::departure_realtime::empty_event(
                            &chateau,
                            &trip_id,
                            route_id.as_str(),
                            row.stop_id.as_str(),
                        );
                        event.stop_sequence = Some(row.gtfs_stop_sequence);
                        event.trip_start_time = Some(trip.start_time);
                        event.scheduled_departure = u64::try_from(departure_ts).ok();
                        event.scheduled_arrival = u64::try_from(
                            midnight_ts
                                + i64::from(trip.start_time)
                                + i64::from(
                                    row.arrival_time_since_start.unwrap_or(dep_time_offset),
                                ),
                        )
                        .ok();
                        event.realtime_departure = rt_dep;
                        event.realtime_arrival = rt_arr;
                        event.service_date = Some(*date);
                        event.headsign = Some(headsign);
                        event.platform_code = display_platform;
                        event.trip_cancelled = is_cancelled;
                        event.trip_short_name = trip.trip_short_name.clone();
                        event.last_stop = is_last_stop;
                        event.final_station_name = itinerary_final_station_name
                            .get(&trip.itinerary_pattern_id)
                            .cloned();
                        board_events.push(event);
                    }
                }
            }
        }
    }

    println!(
        "Total time added looking at rt alerts: {} ms",
        total_time_added_looking_at_rt_alerts.as_millis()
    );
    println!(
        "Total time added looking at rt trips: {} ms",
        total_time_added_looking_at_rt_trips.as_millis()
    );

    processing.event_construction_ms = event_construction_started.elapsed().as_secs_f64() * 1000.0;
    processing.legacy_realtime_matching_ms =
        (total_time_added_looking_at_rt_alerts + total_time_added_looking_at_rt_trips)
            .as_secs_f64() * 1000.0;
    processing.events_before_overlay = board_events.len();

    // Use the same indexed overlay as station boards, before time filtering and
    // limits. Query every nearby stop: static direction pruning cannot predict
    // a detour or an added trip's stop calls.
    let mut route_dictionaries = BTreeMap::from([(chateau.clone(), routes)]);
    let mut agency_dictionaries = BTreeMap::from([(chateau.clone(), agencies)]);
    let mut realtime_stops = BTreeMap::new();
    if !skip_realtime {
        let mut realtime_timings = crate::departure_realtime_loader::DepartureRealtimeTimings::default();
        let realtime_loader_started = Instant::now();
        realtime_stops = crate::departure_realtime_loader::apply_departure_realtime_timed(
            &mut board_events,
            &BTreeMap::from([(chateau.clone(), stop_ids.iter().cloned().collect())]),
            &pool,
            &aspen_chateau_cache,
            &mut route_dictionaries,
            &mut agency_dictionaries,
            &mut BTreeMap::new(),
            false,
            &mut realtime_timings,
        )
        .await;
        processing.realtime_loader_total_ms = realtime_loader_started.elapsed().as_secs_f64() * 1000.0;
        processing.realtime_connection_ms += realtime_timings.connection_ms;
        processing.realtime_rpc_ms += realtime_timings.rpc_ms;
        processing.realtime_overlay_ms = realtime_timings.overlay_ms;
        processing.realtime_additional_sql_ms = realtime_timings.additional_sql_ms;
        processing.realtime_update_count = realtime_timings.update_count;
        processing.realtime_modification_count = realtime_timings.modification_count;
    }
    processing.events_after_overlay = board_events.len();
    let grouping_started = Instant::now();
    let routes = route_dictionaries.remove(&chateau).unwrap_or_default();
    let agencies = agency_dictionaries.remove(&chateau).unwrap_or_default();
    for event in board_events {
        if event.trip_deleted {
            continue;
        }
        let Some(route) = routes.get(&event.route_id) else {
            continue;
        };
        let rt_stop = realtime_stops
            .get(&chateau)
            .and_then(|stops| stops.get(&event.stop_id));
        let anchor = if stop_dist_map.contains_key(&event.stop_id) {
            event.stop_id.as_str()
        } else {
            match rt_stop.and_then(|stop| stop.parent_station.as_deref()) {
                Some(parent) if stop_dist_map.contains_key(parent) => parent,
                _ => continue,
            }
        };
        let Some(distance) = stop_dist_map.get(anchor) else {
            continue;
        };
        if !distance_limits.allows(route.route_type, *distance) {
            continue;
        }
        let Some(station_key) = stop_to_key_map.get(&(chateau.clone(), anchor.to_string())) else {
            continue;
        };
        let station_group = is_station_group(
            &chateau, route.route_type, route.agency_id.as_deref(), is_ld_chateau, station_key,
        );
        if !selection.allows(&chateau, &event.route_id, station_key, station_group) {
            continue;
        }
        let Some(time) = crate::departure_realtime::nearby_event_time(
            &event,
            departure_time_chrono.timestamp() as u64,
        ) else {
            continue;
        };
        processing.events_after_time_filter += 1;
        let Some(datetime) = chrono::Utc.timestamp_opt(time as i64, 0).single() else {
            continue;
        };
        let service_date = event.service_date.unwrap_or_else(|| {
            let timezone = stop_full_info_map
                .get(anchor)
                .and_then(|stop| stop.timezone.as_deref())
                .and_then(|tz| catenary_time::Tz::from_str_insensitive(tz).ok())
                .unwrap_or(catenary_time::UTC);
            datetime.with_timezone(&timezone).date_naive()
        });
        let agency_name = route
            .agency_id
            .as_ref()
            .and_then(|id| agencies.get(id))
            .map(|agency| agency.agency_name.clone());
        route_info_map
            .entry(event.route_id.clone())
            .or_insert_with(|| RouteInfoExport {
                short_name: route.short_name.as_ref().map(ToString::to_string),
                long_name: route.long_name.clone(),
                agency_name: agency_name.clone(),
                color: route.color.as_ref().map(ToString::to_string),
                text_color: route.text_color.as_ref().map(ToString::to_string),
                route_type: i32::from(route.route_type),
            });
        let headsign = event.headsign.clone().unwrap_or_else(|| "Unknown".into());
        let platform = event
            .platform_string_realtime
            .clone()
            .or(event.platform_code.clone())
            .or_else(|| stop_platform_map.get(&event.stop_id).cloned().flatten());
        let cancelled = event.trip_cancelled || event.stop_cancelled;
        if station_group {
            ld_departures_by_group
                .entry((chateau.clone(), station_key.clone()))
                .or_default()
                .push(DepartureItem {
                    scheduled_departure: event.scheduled_departure,
                    realtime_departure: event.realtime_departure,
                    scheduled_arrival: event.scheduled_arrival,
                    realtime_arrival: event.realtime_arrival,
                    service_date,
                    headsign,
                    platform,
                    trip_id: event.trip_id,
                    trip_short_name: event.trip_short_name.map(|s| s.to_string()),
                    route_id: event.route_id,
                    stop_id: event.stop_id,
                    agency_id: route.agency_id.clone().unwrap_or_default(),
                    cancelled,
                    delayed: event
                        .realtime_departure
                        .zip(event.scheduled_departure)
                        .is_some_and(|(rt, scheduled)| rt > scheduled + 60),
                    chateau_id: chateau.clone(),
                    last_stop: event.last_stop,
                    final_station_name: event.final_station_name,
                });
        } else {
            let key = LocalRouteKey {
                chateau: chateau.clone(),
                route_id: event.route_id,
            };
            local_route_meta_map
                .entry(key.clone())
                .or_insert((route.clone(), agency_name.unwrap_or_default()));
            local_departures
                .entry(key)
                .or_default()
                .entry(headsign)
                .or_default()
                .push(LocalDepartureItem {
                    trip_id: event.trip_id.into(),
                    departure_schedule: event.scheduled_departure,
                    departure_realtime: event.realtime_departure,
                    arrival_schedule: event.scheduled_arrival,
                    arrival_realtime: event.realtime_arrival,
                    stop_id: event.stop_id.clone().into(),
                    stop_name: rt_stop
                        .and_then(|s| s.stop_name.as_ref())
                        .and_then(|s| s.translation.as_slice().first())
                        .map(|s| s.text.clone())
                        .or_else(|| stop_name_map.get(anchor).cloned().flatten()),
                    cancelled,
                    platform,
                    service_date,
                    last_stop: event.last_stop,
                });
            if let Some(stop) = stop_full_info_map.get(anchor) {
                if let Some(point) = &stop.point {
                    stop_output_map
                        .entry(event.stop_id.clone())
                        .or_insert_with(|| StopOutputV3 {
                            gtfs_id: event.stop_id.as_str().into(),
                            name: rt_stop
                                .and_then(|s| s.stop_name.as_ref())
                                .and_then(|s| s.translation.as_slice().first())
                                .map(|s| s.text.clone())
                                .or_else(|| stop.name.clone())
                                .unwrap_or_default(),
                            lat: rt_stop
                                .and_then(|s| s.stop_lat)
                                .map(f64::from)
                                .unwrap_or(point.y),
                            lon: rt_stop
                                .and_then(|s| s.stop_lon)
                                .map(f64::from)
                                .unwrap_or(point.x),
                            osm_station_id: stop.osm_station_id,
                            timezone: stop.timezone.clone().unwrap_or_else(|| "Etc/UTC".into()),
                        });
                }
            }
        }
    }

    // Filter closest stop for Local
    for headsigns_map in local_departures.values_mut() {
        for items in headsigns_map.values_mut() {
            if items.is_empty() {
                continue;
            }
            let mut min_dist = f64::MAX;
            let mut best_cancelled = true;
            let mut best_stop_id: Option<CompactString> = None;
            for item in items.iter() {
                let anchor = realtime_stops
                    .get(&chateau)
                    .and_then(|stops| stops.get(item.stop_id.as_str()))
                    .and_then(|stop| stop.parent_station.as_deref());
                if let Some(dist) = stop_dist_map
                    .get(item.stop_id.as_str())
                    .or_else(|| anchor.and_then(|parent| stop_dist_map.get(parent)))
                {
                    // Removed detour calls remain visible as cancelled events.
                    // They must not hide the replacement stop's running calls.
                    if best_stop_id.is_none()
                        || (best_cancelled && !item.cancelled)
                        || (best_cancelled == item.cancelled && *dist < min_dist)
                    {
                        min_dist = *dist;
                        best_cancelled = item.cancelled;
                        best_stop_id = Some(item.stop_id.clone());
                    }
                }
            }
            if let Some(target) = best_stop_id {
                items.retain(|i| i.stop_id == target);
            }
        }
    }

    processing.output_grouping_ms = grouping_started.elapsed().as_secs_f64() * 1000.0;
    Some((
        ld_departures_by_group,
        local_departures
            .into_iter()
            .map(|(k, v)| {
                let (r, a) = local_route_meta_map.get(&k).unwrap().clone();
                (k, (r, a, v))
            })
            .collect(),
        route_info_map,
        stop_output_map,
        chateau,
        processing,
    ))
}

#[cfg(test)]
mod pattern_batch_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn batches_preserve_all_ids_at_boundaries_and_skip_empty_input() {
        for count in [0, 1, 127, 128, 129, 513] {
            let ids: Vec<usize> = (0..count).collect();
            let slots = tokio::sync::Semaphore::new(PATTERN_QUERY_CONCURRENCY);
            let calls = AtomicUsize::new(0);
            let mut rows = load_pattern_batches(&ids, &slots, |batch| {
                calls.fetch_add(1, Ordering::SeqCst);
                assert!(!batch.is_empty());
                assert!(batch.len() <= PATTERN_QUERY_BATCH_SIZE);
                async move { Ok(batch) }
            })
            .await
            .unwrap();
            rows.sort_unstable();
            assert_eq!(rows, ids);
            assert_eq!(
                AtomicUsize::load(&calls, Ordering::SeqCst),
                count.div_ceil(PATTERN_QUERY_BATCH_SIZE)
            );
        }
    }

    #[tokio::test]
    async fn simultaneous_loaders_share_the_concurrency_limit() {
        let ids: Vec<usize> = (0..PATTERN_QUERY_BATCH_SIZE * 9).collect();
        let slots = tokio::sync::Semaphore::new(PATTERN_QUERY_CONCURRENCY);
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let load = |batch: Vec<usize>| {
            let active = &active;
            let peak = &peak;
            async move {
                let running = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(running, Ordering::SeqCst);
                // Keep each batch in flight while the other batches are polled.
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(batch)
            }
        };
        let (first, second) = tokio::join!(
            load_pattern_batches(&ids, &slots, &load),
            load_pattern_batches(&ids, &slots, &load),
        );
        assert_eq!(first.unwrap().len(), ids.len());
        assert_eq!(second.unwrap().len(), ids.len());
        assert_eq!(
            AtomicUsize::load(&peak, Ordering::SeqCst),
            PATTERN_QUERY_CONCURRENCY
        );
        assert_eq!(AtomicUsize::load(&active, Ordering::SeqCst), 0);
        assert_eq!(slots.available_permits(), PATTERN_QUERY_CONCURRENCY);
    }

    #[tokio::test]
    async fn a_failed_batch_does_not_return_partial_rows() {
        let ids: Vec<usize> = (0..PATTERN_QUERY_BATCH_SIZE * 2).collect();
        let slots = tokio::sync::Semaphore::new(PATTERN_QUERY_CONCURRENCY);
        let result = load_pattern_batches(&ids, &slots, |batch| async move {
            if batch[0] == PATTERN_QUERY_BATCH_SIZE {
                Err(diesel::result::Error::NotFound)
            } else {
                Ok(batch)
            }
        })
        .await;
        assert!(matches!(result, Err(diesel::result::Error::NotFound)));
        assert_eq!(slots.available_permits(), PATTERN_QUERY_CONCURRENCY);
    }
}

#[cfg(test)]
mod distance_limit_tests {
    use super::{DepartureDistanceLimits, has_non_coach_mode};

    #[test]
    fn mixed_stop_keeps_metro_but_rejects_bus_beyond_dense_bus_radius() {
        let limits = DepartureDistanceLimits::new(5000.0, 801, 10);
        assert!(limits.allows_stop(Some(1), &[Some(1), Some(3)], &[], 1500.0));
        assert!(limits.allows(1, 1500.0));
        assert!(!limits.allows(3, 1500.0));
        assert!(!limits.allows_stop(Some(3), &[Some(3)], &[], 1500.0));
        // A parent's child modes can also preserve eligible rail service.
        assert!(limits.allows_stop(Some(3), &[], &[Some(1)], 1500.0));
    }

    #[test]
    fn bus_cutoffs_are_inclusive_and_tighten_with_density() {
        for (stops, stations, cutoff) in [
            (50, 0, 5000.0),
            (51, 0, 2000.0),
            (500, 0, 2000.0),
            (501, 0, 1500.0),
            (800, 0, 1500.0),
            (801, 0, 1000.0),
            (1, 100, 2000.0),
            (1, 101, 1000.0),
        ] {
            let limits = DepartureDistanceLimits::new(5000.0, stops, stations);
            assert!(
                limits.allows(3, cutoff),
                "{stops} stops, {stations} stations"
            );
            assert!(!limits.allows(3, cutoff + 0.1));
        }
    }

    #[test]
    fn extended_bus_modes_use_the_same_cutoff() {
        let limits = DepartureDistanceLimits::new(5000.0, 801, 0);
        for mode in [3, 11, 209, 700, 701, 799, 800] {
            assert!(limits.allows(mode, 1000.0));
            assert!(!limits.allows(mode, 1000.1));
        }
        for mode in [0, 1, 2, 100, 400, 900] {
            assert!(limits.allows(mode, 1500.0));
        }
    }

    #[test]
    fn coaches_are_excluded_but_shared_bus_stops_are_retained() {
        let limits = DepartureDistanceLimits::new(5000.0, 10, 1);
        assert!(!limits.allows(200, 0.0));
        assert!(!limits.allows_stop(Some(200), &[Some(200)], &[], 10.0));
        assert!(limits.allows_stop(Some(200), &[Some(200), Some(3)], &[], 10.0));
        assert!(!has_non_coach_mode(Some(200), &[Some(200)], &[Some(200)]));
        assert!(has_non_coach_mode(Some(200), &[Some(3)], &[]));
        assert!(has_non_coach_mode(Some(200), &[], &[Some(2)]));
        assert!(has_non_coach_mode(None, &[None], &[]));
        assert!(has_non_coach_mode(Some(201), &[], &[]));
    }

    #[test]
    fn requested_radius_caps_every_mode_including_unknown_stops() {
        let limits = DepartureDistanceLimits::new(500.0, 1000, 300);
        for mode in [0, 1, 2, 3, 700] {
            assert!(limits.allows(mode, 500.0));
            assert!(!limits.allows(mode, 500.1));
            assert!(!limits.allows(mode, f64::NAN));
            assert!(!limits.allows(mode, f64::INFINITY));
        }
        assert!(limits.allows_stop(None, &[], &[], 500.0));
        assert!(!limits.allows_stop(None, &[None], &[], 500.1));
    }

    #[test]
    fn station_density_tightens_the_non_bus_radius_at_existing_boundaries() {
        for (stations, cutoff) in [(100, 5000.0), (101, 2000.0), (200, 2000.0), (201, 1000.0)] {
            let limits = DepartureDistanceLimits::new(5000.0, 1, stations);
            assert!(limits.allows(1, cutoff));
            assert!(!limits.allows(1, cutoff + 0.1));
        }
    }
}

#[derive(Debug)]
struct RouteCandidate {
    chateau: String,
    route: String,
    mode: i16,
    distance: f64,
    station: Option<PriorityStation>,
}

fn priority_routes(
    mut candidates: Vec<RouteCandidate>,
    station_distances: &HashMap<PriorityStation, f64>,
) -> PrioritySelection {
    candidates.retain(|candidate| candidate.mode != COACH_ROUTE_TYPE);
    // Both Compose and web hoist only the closest station strictly within 1 km.
    // Use the station-group distance, not a route's nearest platform.
    let station = candidates.iter()
        .filter_map(|candidate| candidate.station.as_ref())
        .filter_map(|station| station_distances.get(station).map(|distance| (station, *distance)))
        .filter(|(_, distance)| *distance < 1000.0)
        .min_by(|(a, da), (b, db)| da.total_cmp(db).then_with(|| a.cmp(b)))
        .map(|(station, _)| station.clone());
    // Each hoisted display group replaces one priority bus slot.
    // Count distinct local routes, not their stops or directions.
    let hoisted_routes: HashSet<_> = candidates.iter()
        .filter(|candidate| {
            candidate.station.is_none()
                && ((matches!(candidate.mode, 1 | 7 | 12 | 400..=499)
                    && candidate.distance < 500.0)
                    || (matches!(candidate.mode, 0 | 5 | 900..=999)
                        && candidate.distance < 400.0))
        })
        .map(|candidate| (&candidate.chateau, &candidate.route))
        .collect();
    let bus_limit = 5usize.saturating_sub(hoisted_routes.len() + usize::from(station.is_some()));
    let mut selected = PrioritySelection { station, ..Default::default() };
    candidates.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| (&a.chateau, &a.route).cmp(&(&b.chateau, &b.route)))
    });
    let mut seen = HashSet::new();
    let mut buses = 0;
    let mut metro = false;
    for candidate in candidates {
        let key = (candidate.chateau, candidate.route);
        if let Some(station) = candidate.station {
            if selected.station.as_ref() == Some(&station) {
                selected.station_routes.insert(key);
            }
            continue;
        }
        if !seen.insert(key.clone()) {
            continue;
        }
        let is_bus = matches!(candidate.mode, 3 | 11 | 201..=209 | 700..=799 | 800);
        let is_metro = matches!(candidate.mode, 1 | 7 | 12 | 400..=499);
        let is_tram = matches!(candidate.mode, 0 | 5 | 900..=999);
        let hoisted = (is_metro && (!metro || candidate.distance < 500.0))
            || (is_tram && candidate.distance < 400.0);
        if hoisted || (is_bus && buses < bus_limit) {
            selected.local_routes.insert(key);
        } else if selected.next_routes.len() < INTERMEDIATE_ROUTE_LIMIT {
            selected.next_routes.insert(key);
        }
        if is_bus {
            buses += 1;
        }
        metro |= is_metro;
    }
    selected
}

impl PreparedNearby {
    /// Read only direction metadata here. Trip/calendar/realtime work is performed
    /// by load() after partitioning, with the same distance policy as Birch.
    pub async fn priority_selection(
        &self,
        pool: Arc<CatenaryPostgresPool>,
    ) -> Result<Arc<PrioritySelection>, String> {
        let point = geo::Point::new(self.query.lon, self.query.lat);
        let limits = DepartureDistanceLimits::new(
            self.query.radius.unwrap_or(5000.0),
            self.stops.len(),
            self.stops
                .iter()
                .filter_map(|s| s.osm_station_id)
                .collect::<HashSet<_>>()
                .len(),
        );
        let mut by_chateau: HashMap<String, HashMap<String, (f64, StationKey)>> = HashMap::new();
        let mut station_distances: HashMap<PriorityStation, f64> = HashMap::new();
        for stop in &self.stops {
            if let Some(p) = &stop.point {
                let distance = Haversine.distance(point, geo::Point::new(p.x, p.y));
                if limits.allows_stop(
                    stop.primary_route_type,
                    &stop.route_types,
                    &stop.children_route_types,
                    distance,
                ) {
                    let key = station_key_for_stop(stop);
                    station_distances
                        .entry(PriorityStation::new(&stop.chateau, &key))
                        .and_modify(|existing| *existing = existing.min(distance))
                        .or_insert(distance);
                    by_chateau
                        .entry(stop.chateau.clone())
                        .or_default()
                        .insert(stop.gtfs_id.clone(), (distance, key));
                }
            }
        }
        let priority_timings = self.priority_sql_timings.clone();
        let request_started = self.start;
        let parts: Vec<Vec<RouteCandidate>> = futures::stream::iter(by_chateau)
            .map(|(chateau, distances)| {
                let pool = pool.clone();
                let priority_timings = priority_timings.clone();
                async move {
                    use crate::schema::gtfs::{
                        direction_pattern as rows, direction_pattern_meta as meta, routes,
                    };
                    let ids: Vec<String> = distances.keys().cloned().collect();
                    let input_ids = ids.len();
                    let checkout_started = Instant::now();
                    let mut conn = pool.get().await.map_err(|e| e.to_string())?;
                    let sql_started = Instant::now();
                    let sql_result = rows::table
                        .inner_join(
                            meta::table.on(rows::chateau
                                .eq(meta::chateau)
                                .and(rows::onestop_feed_id.eq(meta::onestop_feed_id))
                                .and(rows::attempt_id.eq(meta::attempt_id))
                                .and(rows::direction_pattern_id.eq(meta::direction_pattern_id))),
                        )
                        .inner_join(routes::table.on(
                            routes::chateau.eq(meta::chateau)
                                .and(routes::onestop_feed_id.eq(meta::onestop_feed_id))
                                .and(routes::attempt_id.eq(meta::attempt_id))
                                .and(routes::route_id.nullable().eq(meta::route_id)),
                        ))
                        .filter(rows::chateau.eq(&chateau))
                        .filter(rows::stop_id.eq_any(ids))
                        .filter(routes::route_type.ne(COACH_ROUTE_TYPE))
                        .select((rows::stop_id, routes::route_id, routes::route_type, routes::agency_id))
                        .load::<(CompactString, CompactString, i16, Option<String>)>(&mut conn)
                        .await;
                    record_sql_timing(
                        &priority_timings,
                        request_started,
                        Some(chateau.clone()),
                        "priority_direction_route_join",
                        input_ids,
                        checkout_started,
                        sql_started,
                        &sql_result,
                    );
                    let rows = sql_result.map_err(|e| e.to_string())?;
                    Ok::<_, String>(
                        rows.into_iter()
                            .filter_map(|(stop, route, mode, agency_id)| {
                                let (distance, key) = distances.get(stop.as_str())?;
                                if !limits.allows(mode, *distance) {
                                    return None;
                                }
                                let station = is_station_group(
                                    &chateau, mode, agency_id.as_deref(),
                                    LONG_DISTANCE_CHATEAUX.contains(&chateau.as_str()), key,
                                ).then(|| PriorityStation::new(&chateau, key));
                                Some(RouteCandidate {
                                    chateau: chateau.clone(),
                                    route: route.to_string(),
                                    mode,
                                    distance: *distance,
                                    station,
                                })
                            })
                            .collect(),
                    )
                }
            })
            .buffer_unordered(3)
            .try_collect()
            .await?;
        let candidates: Vec<_> = parts.into_iter().flatten().collect();
        // The response uses the OSM station point rather than the closest
        // platform when available. Use the same distance for the hoist decision.
        let osm_ids: Vec<_> = candidates.iter()
            .filter_map(|candidate| match &candidate.station {
                Some(PriorityStation::Osm(id)) => Some(*id),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !osm_ids.is_empty() {
            let stations = timed_nearby_sql!(
                priority_timings,
                request_started,
                "",
                "priority_osm_stations",
                osm_ids.len(),
                pool,
                crate::schema::gtfs::osm_stations::table
                    .filter(crate::schema::gtfs::osm_stations::osm_id.eq_any(&osm_ids))
                    .select(crate::models::OsmStation::as_select()),
                crate::models::OsmStation
            ).unwrap_or_default();
            for station in stations {
                let distance = Haversine.distance(point, geo::Point::new(station.point.x, station.point.y));
                station_distances.insert(PriorityStation::Osm(station.osm_id), distance);
            }
        }
        Ok(Arc::new(priority_routes(candidates, &station_distances)))
    }
}

#[cfg(test)]
mod progressive_tests {
    use super::*;

    fn candidate(feed: &str, route: &str, mode: i16, distance: f64) -> RouteCandidate {
        RouteCandidate {
            chateau: feed.into(),
            route: route.into(),
            mode,
            distance,
            station: None,
        }
    }

    fn station_candidate(feed: &str, route: &str, id: i64, distance: f64) -> RouteCandidate {
        RouteCandidate {
            station: Some(PriorityStation::Osm(id)),
            ..candidate(feed, route, 2, distance)
        }
    }

    #[test]
    fn prioritises_hoisted_groups_and_remaining_bus_slots_globally() {
        let mut candidates = vec![
            station_candidate("rail", "train", 1, 900.0),
            candidate("city", "metro", 1, 100.0),
            candidate("city", "metro-2", 1, 499.0),
            candidate("city", "metro-far", 1, 500.0),
            candidate("city", "tram", 900, 399.0),
            candidate("city", "tram-far", 0, 400.0),
            candidate("other", "0", 700, 5.0),
        ];
        for n in 0..7 {
            candidates.push(candidate("bus", &n.to_string(), 3, 10.0 + n as f64));
        }
        // Opposite directions of a bus route do not consume two priority slots.
        candidates.push(candidate("bus", "0", 3, 10.5));
        let selected = priority_routes(candidates, &HashMap::from([(PriorityStation::Osm(1), 900.0)]));
        for key in [
            ("rail", "train"),
            ("city", "metro"),
            ("city", "metro-2"),
            ("city", "tram"),
            ("other", "0"),
        ] {
            assert!(selected.contains_route(key.0, key.1));
        }
        for key in [("city", "metro-far"), ("city", "tram-far"), ("bus", "0"), ("bus", "3"), ("bus", "4")] {
            assert!(!selected.contains_route(key.0, key.1));
        }
        assert_eq!(selected.local_routes.len() + selected.station_routes.len(), 5);
    }

    #[test]
    fn bus_budget_decreases_for_each_distinct_hoisted_group() {
        // Each group reserves a slot, including when all buses are closer.
        // Exact threshold distances do not hoist and the budget saturates at zero.
        for (extra, station_distance, expected_buses) in [
            (vec![], 1000.0, 5),
            (vec![station_candidate("rail", "train", 1, 999.0)], 999.0, 4),
            (vec![station_candidate("rail", "train", 1, 1000.0)], 1000.0, 5),
            (vec![candidate("city", "metro", 1, 499.0)], 1000.0, 4),
            (vec![candidate("city", "metro", 1, 500.0)], 1000.0, 5),
            (vec![candidate("city", "tram", 900, 399.0)], 1000.0, 4),
            (vec![candidate("city", "tram", 900, 400.0)], 1000.0, 5),
            (vec![
                station_candidate("rail", "train", 1, 999.0),
                candidate("city", "metro", 1, 499.0),
                candidate("city", "tram", 900, 399.0),
            ], 999.0, 2),
            (vec![
                candidate("city", "metro", 1, 100.0),
                candidate("city", "metro", 1, 200.0),
                candidate("city", "metro-2", 1, 300.0),
            ], 1000.0, 3),
            ((0..6).map(|i| candidate("city", &i.to_string(), 1, 100.0)).collect(), 1000.0, 0),
        ] {
            let mut candidates: Vec<_> = (0..7)
                .map(|i| candidate("bus", &i.to_string(), 3, 10.0 + i as f64))
                .collect();
            candidates.extend(extra);
            let priority = Arc::new(priority_routes(
                candidates,
                &HashMap::from([(PriorityStation::Osm(1), station_distance)]),
            ));
            let first = RouteSelection::Only(priority.clone());
            let second = RouteSelection::Except(priority);
            let stop = StationKey::Stop("stop".into());
            for i in 0..7 {
                let route = i.to_string();
                assert_eq!(first.allows("bus", &route, &stop, false), i < expected_buses);
                assert_eq!(second.allows("bus", &route, &stop, false), i >= expected_buses);
            }
        }
    }

    #[test]
    fn intermediate_phase_takes_next_ten_local_routes_and_preserves_remaining() {
        let mut candidates = vec![
            candidate("bus", "coach", 200, 1.0),
            station_candidate("rail", "train", 1, 250.0),
            station_candidate("rail", "train", 2, 500.0),
        ];
        for i in 0..18 {
            candidates.push(candidate("bus", &i.to_string(), 3, 20.0 + i as f64));
        }
        // Duplicate stops on a route do not consume the intermediate budget.
        candidates.push(candidate("bus", "4", 3, 25.0));
        let priority = Arc::new(priority_routes(candidates, &HashMap::from([
            (PriorityStation::Osm(1), 250.0),
            (PriorityStation::Osm(2), 500.0),
        ])));
        assert_eq!(priority.local_routes.len(), 4);
        assert_eq!(priority.next_routes.len(), 10);
        assert!(!priority.contains_route("bus", "coach"));
        assert!(!priority.contains_next("bus", "coach"));
        let first = RouteSelection::Only(priority.clone());
        let next = RouteSelection::Next(priority.clone());
        let last = RouteSelection::ExceptBoth(priority.clone());
        let legacy = RouteSelection::Except(priority);
        assert_eq!(first.included_route_ids("bus").unwrap().len(), 4);
        assert_eq!(next.included_route_ids("bus").unwrap().len(), 10);
        assert_eq!(last.excluded_local_route_ids("bus").len(), 14);
        assert_eq!(legacy.excluded_local_route_ids("bus").len(), 4);
        assert!(last.excluded_local_route_ids("rail").is_empty());
        let stop = StationKey::Stop("stop".into());
        for i in 0..18 {
            let route = i.to_string();
            assert_eq!(first.allows("bus", &route, &stop, false), i < 4);
            assert_eq!(next.allows("bus", &route, &stop, false), (4..14).contains(&i));
            assert_eq!(last.allows("bus", &route, &stop, false), i >= 14);
            assert_eq!(legacy.allows("bus", &route, &stop, false), i >= 4);
        }
        assert!(first.allows("rail", "train", &StationKey::Osm(1), true));
        assert!(!next.allows("rail", "train", &StationKey::Osm(2), true));
        assert!(last.allows("rail", "train", &StationKey::Osm(2), true));
        assert!(!last.allows("rail", "train", &StationKey::Osm(1), true));
    }

    #[test]
    fn nearest_metro_is_included_even_outside_hoist_threshold() {
        let selected = priority_routes(vec![
            candidate("a", "metro", 1, 700.0),
            candidate("b", "metro", 1, 800.0),
        ], &HashMap::new());
        assert!(selected.contains_route("a", "metro"));
        assert!(!selected.contains_route("b", "metro"));
    }

    #[test]
    fn phases_are_disjoint_exhaustive_and_scope_route_ids_by_feed() {
        let keys = Arc::new(PrioritySelection {
            local_routes: HashSet::from([("a".into(), "1".into())]),
            ..Default::default()
        });
        let first = RouteSelection::Only(keys.clone());
        let second = RouteSelection::Except(keys);
        let stop = StationKey::Stop("stop".into());
        for (feed, route) in [("a", "1"), ("b", "1"), ("a", "2")] {
            assert_ne!(first.allows(feed, route, &stop, false), second.allows(feed, route, &stop, false));
            assert!(RouteSelection::All.allows(feed, route, &stop, false));
        }
    }

    #[test]
    fn only_closest_station_is_hoisted_even_when_route_serves_both() {
        let distances = HashMap::from([
            (PriorityStation::Osm(1), 500.0),
            (PriorityStation::Osm(2), 600.0),
            (PriorityStation::Osm(3), 1000.0),
        ]);
        let priority = Arc::new(priority_routes(vec![
            station_candidate("a", "train", 1, 700.0),
            station_candidate("a", "train", 2, 100.0),
            station_candidate("b", "other-train", 1, 800.0),
            station_candidate("c", "far", 3, 50.0),
            candidate("a", "local-train", 2, 10.0),
        ], &distances));
        assert_eq!(priority.station, Some(PriorityStation::Osm(1)));
        let first = RouteSelection::Only(priority.clone());
        let second = RouteSelection::Except(priority);
        for (feed, route, station, expected) in [
            ("a", "train", 1, true),
            ("b", "other-train", 1, true),
            ("a", "train", 2, false),
            ("c", "far", 3, false),
            ("z", "added-route", 1, false),
        ] {
            let key = StationKey::Osm(station);
            assert_eq!(first.allows(feed, route, &key, true), expected);
            assert_eq!(second.allows(feed, route, &key, true), !expected);
        }
        assert!(!first.may_include_route("a", "local-train"));
        assert!(second.may_include_route("a", "train"));
    }

    #[test]
    fn station_at_or_beyond_one_kilometre_is_deferred() {
        let priority = priority_routes(
            vec![station_candidate("a", "train", 1, 900.0)],
            &HashMap::from([(PriorityStation::Osm(1), 1000.0)]),
        );
        assert!(priority.station.is_none());
        assert!(priority.station_routes.is_empty());
    }

    #[test]
    fn non_osm_station_keys_are_chateau_scoped() {
        let key = StationKey::Parent("station".into());
        let a = PriorityStation::new("a", &key);
        let b = PriorityStation::new("b", &key);
        assert_ne!(a, b);
        let priority = priority_routes(vec![
            RouteCandidate { station: Some(a.clone()), ..candidate("a", "train", 2, 300.0) },
            RouteCandidate { station: Some(b.clone()), ..candidate("b", "train", 2, 400.0) },
        ], &HashMap::from([(a.clone(), 300.0), (b, 400.0)]));
        assert_eq!(priority.station, Some(a));
        assert!(priority.contains_departure("a", "train", &key, true));
        assert!(!priority.contains_departure("b", "train", &key, true));
    }

    #[test]
    fn rejects_invalid_coordinates_timestamps_and_unbounded_limits() {
        let valid = r#"{"lat":34.0,"lon":-118.0}"#;
        let query: NearbyFromCoordsV3 = serde_json::from_str(valid).unwrap();
        assert!(query.validate().is_ok());
        for invalid in [
            r#"{"lat":91,"lon":0}"#,
            r#"{"lat":0,"lon":0,"radius":-1}"#,
            r#"{"lat":0,"lon":0,"departure_time":18446744073709551615}"#,
            r#"{"lat":0,"lon":0,"limit_per_headsign":10000}"#,
        ] {
            assert!(
                serde_json::from_str::<NearbyFromCoordsV3>(invalid)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }
}

#[cfg(test)]
mod nearby_sql_timing_tests {
    use super::*;

    #[test]
    fn query_record_keeps_input_and_result_counts() {
        let start = Instant::now();
        let checkout = Instant::now();
        let sql_start = Instant::now();
        let result: diesel::QueryResult<Vec<i32>> = Ok(vec![1, 2, 3]);
        let record = sql_timing(
            start,
            Some("test-chateau".to_owned()),
            "test_query",
            5,
            checkout,
            sql_start,
            &result,
        );
        assert_eq!(record.chateau.as_deref(), Some("test-chateau"));
        assert_eq!(record.query, "test_query");
        assert_eq!(record.input_ids, 5);
        assert_eq!(record.returned_rows, 3);
        assert!(record.success);
        assert!(record.query_ms >= 0.0);
        assert!(record.pool_wait_ms >= 0.0);
    }

    #[test]
    fn failed_queries_are_included_and_sorted_by_start() {
        let origin = Instant::now();
        let collection: SharedSqlTimings = Arc::new(std::sync::Mutex::new(Vec::new()));
        let failure: diesel::QueryResult<Vec<i32>> = Err(diesel::result::Error::NotFound);
        let start = Instant::now();
        record_sql_timing(
            &collection,
            origin,
            Some("test-chateau".to_owned()),
            "failed",
            8,
            start,
            Instant::now(),
            &failure,
        );
        let records = snapshot_sql_timings(&collection);
        assert_eq!(records.len(), 1);
        assert!(!records[0].success);
        assert_eq!(records[0].returned_rows, 0);
        assert_eq!(records[0].input_ids, 8);
    }
}

#[cfg(test)]
mod nearby_metadata_projection_tests {
    use super::*;

    #[test]
    fn processing_debug_accumulates_and_serializes() {
        let mut total = NearbyProcessingDebug::default();
        let one = NearbyProcessingDebug {
            realtime_overlay_ms: 12.5,
            events_before_overlay: 100,
            realtime_update_count: 3,
            ..Default::default()
        };
        total.accumulate(&one);
        total.accumulate(&one);
        let json = serde_json::to_value(total).unwrap();
        assert_eq!(json["realtime_overlay_ms"], 25.0);
        assert_eq!(json["events_before_overlay"], 200);
        assert_eq!(json["realtime_update_count"], 6);
        assert_eq!(json["realtime_rpc_ms"], 0.0);
    }

    #[test]
    fn nearby_metadata_sql_omits_trip_ids() {
        use crate::schema::gtfs::itinerary_pattern_meta::dsl as meta;

        let query = meta::itinerary_pattern_meta
            .filter(meta::chateau.eq("test-chateau"))
            .select(NearbyItineraryPatternMeta::as_select());

        // This checks the actual Diesel SELECT list, without a live database.
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();
        assert!(!sql.contains("trip_ids"), "unexpected trip_ids in: {sql}");
        for column in [
            "itinerary_pattern_id",
            "route_id",
            "direction_pattern_id",
            "timezone",
            "row_count",
        ] {
            assert!(sql.contains(column), "missing {column} in: {sql}");
        }
    }
}
