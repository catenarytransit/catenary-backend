use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
use ahash::AHashMap;
use ahash::AHashSet;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Bool;
use diesel::sql_types::*;
use diesel_async::RunQueryDsl;
use futures::stream::StreamExt;
use geo::HaversineDistance;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

// Import logic from shared modules
use crate::departures_shared::{
    AlertIndex, ItinOption, StopDataResult, ValidTripSet, estimate_service_date,
    fetch_stop_data_for_chateau,
};
use catenary::aspen::lib::AspenRpcClient;
use catenary::aspen_dataset::{AspenisedAlert, AspenisedTripUpdate};
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use catenary::make_calendar_structure_from_pg;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoordsV3 {
    lat: f64,
    lon: f64,
    departure_time: Option<u64>,
    radius: Option<f64>,
    limit_per_station: Option<usize>,
    limit_per_headsign: Option<usize>,
    skip_realtime: Option<bool>,
    rt_timeout_ms: Option<u64>,
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

#[derive(Serialize, Clone, Debug)]
pub struct NearbyDeparturesDebug {
    pub total_time_ms: u128,
    pub db_connection_time_ms: u128,
    pub stops_fetch_time_ms: u128,
    pub etcd_connection_time_ms: u128,
    pub pipeline_processing_time_ms: u128,
}

// --- Long Distance Structs ---
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
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
    pub cancelled: bool,
    pub delayed: bool,
    pub chateau_id: String,
    pub last_stop: bool,
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

#[actix_web::get("/nearbydeparturesfromcoordsv3")]
#[tracing::instrument(name = "nearby_from_coords_v3", skip(pool, etcd_connection_ips, etcd_connection_options, etcd_reuser), fields(lat = ?query.lat, lon = ?query.lon))]
pub async fn nearby_from_coords_v3(
    req: HttpRequest,
    query: Query<NearbyFromCoordsV3>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let rt_timeout_ms = query.rt_timeout_ms.unwrap_or(2000);

    let start = Instant::now();
    let limit_per_station = query.limit_per_station.unwrap_or(10);
    let limit_per_headsign = query.limit_per_headsign.unwrap_or(20);
    let skip_realtime = query.skip_realtime.unwrap_or(false);

    let conn_pool = pool.as_ref();
    let db_connection_start = Instant::now();
    let mut conn = match conn_pool.get().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().body("DB Connection Failed"),
    };
    let db_connection_time = db_connection_start.elapsed();

    let departure_time = query
        .departure_time
        .unwrap_or_else(|| catenary::duration_since_unix_epoch().as_secs());
    let departure_time_chrono = chrono::Utc.timestamp_opt(departure_time as i64, 0).unwrap();

    let long_distance_chateaux: HashSet<&str> = [
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
    ]
    .iter()
    .cloned()
    .collect();

    let long_distance_chateaux_arc = Arc::new(long_distance_chateaux);

    // 3. Fetch Nearby Stops
    let input_point = geo::Point::new(query.lon, query.lat);
    let radius = query.radius.unwrap_or(5000.0);
    let spatial_resolution =
        catenary::make_degree_length_as_distance_from_point(&input_point, radius);

    let where_query = format!(
        "ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
        query.lon, query.lat, spatial_resolution
    );

    let stops_fetch_start = Instant::now();
    let stops: Vec<catenary::models::Stop> = match catenary::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(&mut conn)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!("Stop fetch error: {:?}", e));
        }
    };
    let stops_fetch_time = stops_fetch_start.elapsed();

    if stops.is_empty() {
        return HttpResponse::Ok().json(NearbyDeparturesV3Response {
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
            },
        });
    }

    // 4. Group Stops
    let mut station_groups: HashMap<(String, StationKey), Vec<catenary::models::Stop>> =
        HashMap::new();
    let mut station_group_metadata: HashMap<(String, StationKey), (String, f64, f64, f64)> =
        HashMap::new();
    let mut stop_dist_map: HashMap<String, f64> = HashMap::new();
    let mut stop_full_info_map: HashMap<String, catenary::models::Stop> = HashMap::new();

    let number_of_osm_stations = stops.iter().filter(|s| s.osm_station_id.is_some()).count();

    for stop in &stops {
        let stop_point = geo::Point::new(
            stop.point.as_ref().unwrap().x,
            stop.point.as_ref().unwrap().y,
        );
        let dist = input_point.haversine_distance(&stop_point);

        // Distance filtering
        if stop.primary_route_type == Some(3) {
            if stops.len() > 100 && dist > 4000.0 {
                continue;
            }
            if stops.len() > 300 && dist > 2500.0 {
                continue;
            }
            if stops.len() > 500 && dist > 1500.0 {
                continue;
            }
            if stops.len() > 800 && dist > 1000.0 {
                continue;
            }
            if number_of_osm_stations > 50 && dist > 2000.0 {
                continue;
            }
        }
        if number_of_osm_stations > 100 && dist > 2500.0 {
            continue;
        }
        if number_of_osm_stations > 200 && dist > 3000.0 {
            continue;
        }
        if stop_dist_map.len() > 400 && dist > 3000.0 {
            continue;
        }

        stop_dist_map.insert(stop.gtfs_id.clone(), dist);
        stop_full_info_map.insert(stop.gtfs_id.clone(), stop.clone());

        let key = if let Some(osm_id) = stop.osm_station_id {
            StationKey::Osm(osm_id)
        } else if let Some(parent) = &stop.parent_station {
            StationKey::Parent(parent.clone())
        } else {
            StationKey::Stop(stop.gtfs_id.clone())
        };

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

    // 5. Connect to Etcd
    let etcd_connection_start = Instant::now();
    let etcd_result =
        catenary::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser)
            .await;

    let etcd = etcd_result.ok();
    let etcd_arc = Arc::new(etcd);
    let etcd_connection_time = etcd_connection_start.elapsed();

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

    let mut chateau_futures = Vec::new();
    for (chateau, stop_ids) in chateau_stops {
        let pool = pool.get_ref().clone();
        let stop_ids_vec: Vec<String> = stop_ids.into_iter().collect();
        let etcd_clone = etcd_arc.clone();
        let dep_time = departure_time_chrono;
        let chateau_clone = chateau.clone();
        let ld_arc_clone = long_distance_chateaux_arc.clone();

        let stop_to_key_map: HashMap<(String, String), StationKey> = stops
            .iter()
            .map(|s| {
                (
                    (s.chateau.clone(), s.gtfs_id.clone()),
                    if let Some(osm) = s.osm_station_id {
                        StationKey::Osm(osm)
                    } else if let Some(p) = &s.parent_station {
                        StationKey::Parent(p.clone())
                    } else {
                        StationKey::Stop(s.gtfs_id.clone())
                    },
                )
            })
            .collect();
        let stop_platform_map: HashMap<String, Option<String>> = stops
            .iter()
            .map(|s| (s.gtfs_id.clone(), s.platform_code.clone()))
            .collect();
        let stop_name_map: HashMap<String, Option<String>> = stops
            .iter()
            .map(|s| (s.gtfs_id.clone(), s.name.clone()))
            .collect();
        let full_info_map_clone = stop_full_info_map.clone();
        let stop_dist_map_clone = stop_dist_map.clone();

        chateau_futures.push(async move {
            fetch_chateau_data(
                pool,
                chateau_clone,
                stop_ids_vec,
                dep_time,
                etcd_clone,
                ld_arc_clone,
                stop_to_key_map,
                stop_platform_map,
                stop_name_map,
                full_info_map_clone,
                stop_dist_map_clone,
                skip_realtime,
                rt_timeout_ms,
            )
            .await
        });
    }

    let pipeline_start = Instant::now();
    let pipeline_results: Vec<
        Option<(
            HashMap<(String, StationKey), Vec<DepartureItem>>,
            HashMap<
                LocalRouteKey,
                (
                    catenary::models::Route,
                    String,
                    HashMap<String, Vec<LocalDepartureItem>>,
                ),
            >,
            HashMap<String, RouteInfoExport>,
            HashMap<String, StopOutputV3>,
        )>,
    > = futures::stream::iter(chateau_futures)
        .buffer_unordered(10)
        .collect()
        .await;
    let pipeline_processing_time = pipeline_start.elapsed();

    // Flatten results
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> =
        HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> =
        HashMap::new();
    let mut local_route_meta: HashMap<LocalRouteKey, (catenary::models::Route, String)> =
        HashMap::new();
    let mut routes_output: HashMap<String, HashMap<String, RouteInfoExport>> = HashMap::new();
    let mut stops_output: HashMap<String, HashMap<String, StopOutputV3>> = HashMap::new();

    for res in pipeline_results {
        if let Some((ld_part, local_part, route_part, stop_part)) = res {
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
        }
    }

    // A. Long Distance Output Generation
    let mut ld_output: Vec<StationDepartureGroupExport> = Vec::new();
    for ((chateau, key), meta) in station_group_metadata {
        if let Some(mut deps) = ld_departures_by_group.remove(&(chateau.clone(), key.clone())) {
            deps.sort_by_key(|d| d.scheduled_departure.unwrap_or(0));
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
    let osm_ids_to_fetch: Vec<i64> = ld_output.iter().filter_map(|g| g.osm_station_id).collect();
    let mut osm_station_info_map: HashMap<i64, catenary::models::OsmStation> = HashMap::new();

    if !osm_ids_to_fetch.is_empty() {
        let osm_stations_data: Vec<catenary::models::OsmStation> =
            catenary::schema::gtfs::osm_stations::dsl::osm_stations
                .filter(catenary::schema::gtfs::osm_stations::osm_id.eq_any(&osm_ids_to_fetch))
                .load::<catenary::models::OsmStation>(&mut conn)
                .await
                .unwrap_or_default();
        for s in osm_stations_data {
            osm_station_info_map.insert(s.osm_id, s);
        }
    }

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
                    new_group.distance_m = input_point.haversine_distance(&osm_point);
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
            group
                .departures
                .sort_by_key(|d| d.scheduled_departure.unwrap_or(0));
            group.departures.truncate(limit_per_station);
        }
    }
    let mut ld_output = merged_ld_output;
    ld_output.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap());

    // B. Local Output Generation
    let mut local_output: Vec<DepartureRouteGroupExportV3> = Vec::new();
    let has_rail = ld_output.iter().any(|g| g.osm_station_id.is_some());
    let many_stops = stops.len() > 50;
    let limit_buses = has_rail || many_stops;
    let bus_cutoff_m = 2000.0;

    for (r_key, headsigns) in local_departures {
        let (route, a_name) = local_route_meta.get(&r_key).unwrap();
        let mut min_dist = 9999999.0;
        let mut sorted_headsigns = HashMap::new();

        for (h, mut items) in headsigns {
            items.sort_by_key(|i| i.departure_schedule.unwrap_or(0));
            items.truncate(limit_per_headsign);
            sorted_headsigns.insert(h, items);
        }

        for (_, items) in &sorted_headsigns {
            for item in items {
                if let Some(d) = stop_dist_map.get(item.stop_id.as_str()) {
                    if *d < min_dist {
                        min_dist = *d;
                    }
                }
            }
        }

        if limit_buses && route.route_type == 3 && min_dist > bus_cutoff_m {
            continue;
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

    HttpResponse::Ok().json(NearbyDeparturesV3Response {
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
        },
    })
}

async fn fetch_chateau_data(
    pool: Arc<CatenaryPostgresPool>,
    chateau: String,
    stop_ids: Vec<String>,
    departure_time_chrono: chrono::DateTime<chrono::Utc>,
    etcd_arc: Arc<Option<etcd_client::Client>>,
    long_distance_chateaux: Arc<HashSet<&str>>,
    stop_to_key_map: HashMap<(String, String), StationKey>,
    stop_platform_map: HashMap<String, Option<String>>,
    stop_name_map: HashMap<String, Option<String>>,
    stop_full_info_map: HashMap<String, catenary::models::Stop>,
    stop_dist_map: HashMap<String, f64>,
    skip_realtime: bool,
    rt_timeout_ms: u64,
) -> Option<(
    HashMap<(String, StationKey), Vec<DepartureItem>>,
    HashMap<
        LocalRouteKey,
        (
            catenary::models::Route,
            String,
            HashMap<String, Vec<LocalDepartureItem>>,
        ),
    >,
    HashMap<String, RouteInfoExport>,
    HashMap<String, StopOutputV3>,
)> {
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
            let mut conn = pool.get().await.unwrap();
            let direction_rows: Vec<catenary::models::DirectionPatternRow> =
                catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
                    .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau.clone()))
                    .filter(
                        catenary::schema::gtfs::direction_pattern::stop_id.eq_any(&stops_to_check),
                    )
                    .select(catenary::models::DirectionPatternRow::as_select())
                    .load(&mut conn)
                    .await
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

    let t_v3_fetch = Instant::now();

    // 3. Optimized Fetch Pipeline (The "OSM-ification")
    // Instead of fetching all trips then filtering, we calculate Active Services first.

    // A. Fetch Direction & Itinerary Meta
    let mut dir_conn = pool.get().await.unwrap();
    let direction_rows_list: Vec<catenary::models::DirectionPatternRow> =
        catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
            .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau.clone()))
            .filter(catenary::schema::gtfs::direction_pattern::stop_id.eq_any(&final_stop_ids))
            .select(catenary::models::DirectionPatternRow::as_select())
            .load(&mut dir_conn)
            .await
            .unwrap_or_default();

    if direction_rows_list.is_empty() {
        return None;
    }

    let direction_pattern_ids: Vec<String> = direction_rows_list
        .iter()
        .map(|r| r.direction_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let mut meta_conn = pool.get().await.unwrap();
    let itin_meta_list: Vec<catenary::models::ItineraryPatternMeta> =
        catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(chateau.clone()))
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::direction_pattern_id
                    .eq_any(&direction_pattern_ids),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load(&mut meta_conn)
            .await
            .unwrap_or_default();

    let filtered_itinerary_ids: Vec<String> = itin_meta_list
        .iter()
        .map(|m| m.itinerary_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Active services optimization removed (performance regression)

    // C. Fetch Remaining Data (Filtered by Active Services)
    let direction_stop_ids: Vec<String> = direction_rows_list
        .iter()
        .map(|r| r.stop_id.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let route_ids: Vec<CompactString> = itin_meta_list.iter().map(|m| m.route_id.clone()).collect();
    let pool_clone = pool.clone();
    let chateau_clone_trips = chateau.clone();
    let chateau_clone_itin = chateau.clone();
    let chateau_clone_dm = chateau.clone();
    let chateau_clone_routes = chateau.clone();

    let (itins_result, direction_meta_result, routes_result) = tokio::join!(
        async {
            let mut conn = pool_clone.get().await.unwrap();
            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(chateau_clone_itin))
                .filter(
                    catenary::schema::gtfs::itinerary_pattern::itinerary_pattern_id
                        .eq_any(&filtered_itinerary_ids),
                )
                .filter(
                    catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(&direction_stop_ids),
                )
                .select(catenary::models::ItineraryPatternRow::as_select())
                .load::<catenary::models::ItineraryPatternRow>(&mut conn)
                .await
                .unwrap_or_default()
        },
        async {
            let mut conn = pool_clone.get().await.unwrap();
            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::chateau.eq(chateau_clone_dm),
                )
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::direction_pattern_id
                        .eq_any(&direction_pattern_ids),
                )
                .select(catenary::models::DirectionPatternMeta::as_select())
                .load::<catenary::models::DirectionPatternMeta>(&mut conn)
                .await
                .unwrap_or_default()
        },
        async {
            let mut conn = pool_clone.get().await.unwrap();
            catenary::schema::gtfs::routes::dsl::routes
                .filter(catenary::schema::gtfs::routes::chateau.eq(chateau_clone_routes))
                .filter(catenary::schema::gtfs::routes::route_id.eq_any(&route_ids))
                .select(catenary::models::Route::as_select())
                .load::<catenary::models::Route>(&mut conn)
                .await
                .unwrap_or_default()
        }
    );

    // Extract ACTUAL itinerary IDs that match the stop
    let actual_itinerary_ids: Vec<String> = itins_result
        .iter()
        .map(|r| r.itinerary_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let trips_compressed_raw: Vec<catenary::models::CompressedTrip> = {
        let mut conn = pool_clone.get().await.unwrap();
        catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(chateau_clone_trips))
            .filter(catenary::schema::gtfs::trips_compressed::route_id.eq_any(&route_ids))
            .select(catenary::models::CompressedTrip::as_select())
            .load::<catenary::models::CompressedTrip>(&mut conn)
            .await
            .unwrap_or_default()
    };

    let approved_itins: HashSet<&str> = actual_itinerary_ids.iter().map(|s| s.as_str()).collect();
    let trips_compressed_result: Vec<catenary::models::CompressedTrip> = trips_compressed_raw
        .into_iter()
        .filter(|t| approved_itins.contains(t.itinerary_pattern_id.as_str()))
        .collect();

    // println!(
    //     "V3_OPT: Fetch for {} took {}ms. Found {} trips (filtered by {} itins).",
    //     chateau,
    //     t_v3_fetch.elapsed().as_millis(),
    //     trips_compressed_result.len(),
    //     actual_itinerary_ids.len()
    // );

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

        tokio::join!(
            async move {
                let mut conn = pool_cal1.get().await.unwrap();
                catenary::schema::gtfs::calendar::dsl::calendar
                    .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau_cal1))
                    .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&s_ids1))
                    .select(catenary::models::Calendar::as_select())
                    .load::<catenary::models::Calendar>(&mut conn)
                    .await
                    .unwrap_or_default()
            },
            async move {
                let mut conn = pool_cal2.get().await.unwrap();
                catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                    .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(chateau_cal2))
                    .filter(catenary::schema::gtfs::calendar_dates::service_id.eq_any(&s_ids2))
                    .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.ge(date_window_start))
                    .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.le(date_window_end))
                    .select(catenary::models::CalendarDate::as_select())
                    .load::<catenary::models::CalendarDate>(&mut conn)
                    .await
                    .unwrap_or_default()
            }
        )
    } else {
        (vec![], vec![])
    };

    let calendar_struct =
        catenary::make_calendar_structure_from_pg(vec![calendar], vec![calendar_dates])
            .unwrap_or_default();
    let service_map = calendar_struct.get(&chateau);

    // Build Maps
    let mut itins: BTreeMap<String, Vec<catenary::models::ItineraryPatternRow>> = BTreeMap::new();
    for itin in itins_result {
        itins
            .entry(itin.itinerary_pattern_id.clone())
            .or_default()
            .push(itin);
    }

    let mut itin_meta: BTreeMap<String, catenary::models::ItineraryPatternMeta> = BTreeMap::new();
    for meta in itin_meta_list {
        itin_meta.insert(meta.itinerary_pattern_id.clone(), meta);
    }

    let mut direction_meta: BTreeMap<String, catenary::models::DirectionPatternMeta> =
        BTreeMap::new();
    for dm in direction_meta_result {
        direction_meta.insert(dm.direction_pattern_id.clone(), dm);
    }

    let mut direction_rows: BTreeMap<String, Vec<catenary::models::DirectionPatternRow>> =
        BTreeMap::new();
    for dr in direction_rows_list {
        direction_rows
            .entry(dr.direction_pattern_id.clone())
            .or_default()
            .push(dr);
    }

    let mut routes: BTreeMap<String, catenary::models::Route> = BTreeMap::new();
    for r in routes_result {
        routes.insert(r.route_id.clone(), r);
    }

    // Fetch Agencies
    let agency_ids: Vec<String> = routes
        .values()
        .filter_map(|r| r.agency_id.clone())
        .collect();
    let mut agency_conn = pool.get().await.unwrap();
    let agencies_list: Vec<catenary::models::Agency> =
        catenary::schema::gtfs::agencies::dsl::agencies
            .filter(catenary::schema::gtfs::agencies::chateau.eq(chateau.clone()))
            .filter(catenary::schema::gtfs::agencies::agency_id.eq_any(&agency_ids))
            .select(catenary::models::Agency::as_select())
            .load(&mut agency_conn)
            .await
            .unwrap_or_default();
    let agencies: BTreeMap<String, catenary::models::Agency> = agencies_list
        .into_iter()
        .map(|a| (a.agency_id.clone(), a))
        .collect();

    // 4. Realtime Data Fetching (Optimized: Only send active trip IDs)
    let trips_compressed: BTreeMap<String, catenary::models::CompressedTrip> =
        trips_compressed_result
            .into_iter()
            .map(|t| (t.trip_id.clone(), t))
            .collect();
    let trip_ids: Vec<String> = trips_compressed.keys().cloned().collect();

    let mut rt_data: Option<catenary::aspen::lib::TripsSelectionResponse> = None;
    let mut rt_alerts: BTreeMap<String, catenary::aspen_dataset::AspenisedAlert> = BTreeMap::new();

    if !skip_realtime {
        if let Some(etcd) = etcd_arc.as_ref() {
            if !trip_ids.is_empty() {
                let mut etcd_clone = etcd.clone();
                let start_realtime_fetch = std::time::Instant::now();
                if let Ok(resp) = etcd_clone
                    .get(format!("/aspen_assigned_chateaux/{}", chateau), None)
                    .await
                {
                    let etcd_time = start_realtime_fetch.elapsed();
                    if let Some(kv) = resp.kvs().first() {
                        if let Ok(meta) =
                            catenary::bincode_deserialize::<ChateauMetadataEtcd>(kv.value())
                        {
                            let timer_to_connect_to_aspen = std::time::Instant::now();
                            if let Ok(client) =
                                catenary::aspen::lib::spawn_aspen_client_from_ip(&meta.socket).await
                            {
                                let time_to_connect_to_aspen = timer_to_connect_to_aspen.elapsed();

                                let timer_get_trips = std::time::Instant::now();
                                let timeout_result = tokio::time::timeout(
                                    std::time::Duration::from_millis(rt_timeout_ms),
                                    async {
                                        tokio::join!(
                                            client.get_all_trips_with_route_ids(
                                                tarpc::context::current(),
                                                chateau.clone(),
                                                route_ids
                                                    .clone()
                                                    .into_iter()
                                                    .map(|x| x.into())
                                                    .collect()
                                            ),
                                            client.get_all_alerts(
                                                tarpc::context::current(),
                                                chateau.clone()
                                            )
                                        )
                                    },
                                )
                                .await;
                                let time_get_trips = timer_get_trips.elapsed();

                                match timeout_result {
                                    Ok((t, a)) => {
                                        println!(
                                            "nearby deps realtime data fetch chateau {}, etcd time {:?}, aspen connect time {:?}, get trips time {:?}",
                                            chateau.as_str(),
                                            etcd_time,
                                            time_to_connect_to_aspen,
                                            time_get_trips
                                        );

                                        if let Ok(Some(tr)) = t {
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
                    }
                }
            }
        }
    }

    // 5. Process Data into Objects
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> =
        HashMap::new();

    // Key: LocalRouteKey -> Value: Map<Headsign, List of items>
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> =
        HashMap::new();

    let mut local_route_meta_map: HashMap<LocalRouteKey, (catenary::models::Route, String)> =
        HashMap::new();
    let mut route_info_map: HashMap<String, RouteInfoExport> = HashMap::new();
    let mut stop_output_map: HashMap<String, StopOutputV3> = HashMap::new();

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
    let departure_time = departure_time_chrono.timestamp();

    let mut total_time_added_looking_at_rt_trips = std::time::Duration::new(0, 0);
    let mut total_time_added_looking_at_rt_alerts = std::time::Duration::new(0, 0);

    for trip_id in trip_ids {
        let trip = trips_compressed.get(&trip_id).unwrap();
        let itinerary_meta = match itin_meta.get(&trip.itinerary_pattern_id) {
            Some(m) => m,
            None => continue,
        };
        let route_id = &itinerary_meta.route_id;
        let route = match routes.get(route_id.as_str()) {
            Some(r) => r,
            None => continue,
        };

        // Determine Category
        let mut is_long_distance = false;
        if route.route_type == 2 && long_distance_chateaux.contains(chateau.as_str()) {
            is_long_distance = true;
            if chateau == "nationalrailuk" {
                if let Some(aid) = &route.agency_id {
                    if ["XR", "HX", "LO"].contains(&aid.as_str()) {
                        is_long_distance = false;
                    }
                }
            }
            if chateau == "upexpress" {
                is_long_distance = false;
            }
        }

        // Service Date Check
        let mut valid_service_dates = Vec::new();
        if let Some(services) = service_map {
            if let Some(service) = services.get(trip.service_id.as_str()) {
                let base_date = departure_time_chrono.date_naive() - chrono::Duration::days(1);
                for i in 0..3 {
                    let d = base_date + chrono::Duration::days(i);
                    if catenary::datetime_in_service(service, d) {
                        valid_service_dates.push(d);
                    }
                }
            }
        }

        let rows = match itins.get(&trip.itinerary_pattern_id) {
            Some(r) => r,
            None => continue,
        };

        for row in rows {
            if !relevant_stop_ids.contains(row.stop_id.as_str()) {
                continue;
            }

            if let Some(station_key) =
                stop_to_key_map.get(&(chateau.clone(), row.stop_id.to_string()))
            {
                let dep_time_offset = row
                    .departure_time_since_start
                    .or(row.arrival_time_since_start)
                    .or(row.interpolated_time_since_start)
                    .unwrap_or(0);

                let mut is_last_stop = false;
                /*if let Some(dir_id) = &itinerary_meta.direction_pattern_id {
                    if let Some(d_rows) = direction_rows.get(dir_id) {
                        if let Some(last) = d_rows.last() {
                            if last.stop_sequence == row.gtfs_stop_sequence {
                                is_last_stop = true;
                            }
                        }
                    }
                }*/

                for date in &valid_service_dates {
                    let tz = chrono_tz::Tz::from_str_insensitive(&itinerary_meta.timezone)
                        .unwrap_or(chrono_tz::UTC);
                    let midnight_ts = tz
                        .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
                        .single()
                        .map(|t| t.timestamp())
                        .unwrap_or(0);
                    let departure_ts =
                        midnight_ts + trip.start_time as i64 + dep_time_offset as i64;

                    if departure_ts < (departure_time as i64)
                        || departure_ts > (departure_time as i64 + 18 * 3600)
                    {
                        continue;
                    }

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

                    let agency_name = route
                        .agency_id
                        .clone()
                        .and_then(|aid| agencies.get(&aid).map(|a| a.agency_name.clone()));

                    // Realtime Logic
                    let mut rt_dep = None;
                    let mut rt_arr = None;
                    let mut rt_platform = None;
                    let mut is_cancelled = false;
                    let mut is_delayed = false;

                    let timer_looking_at_rt_alerts = std::time::Instant::now();
                    let relevant_alerts = alert_index.search(route_id.as_str(), trip_id.as_str());
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
                                        .map_or(false, |r_id| *r_id == trip.route_id);
                                    let trip_match = e.trip.as_ref().map_or(false, |t| {
                                        t.trip_id
                                            .as_ref()
                                            .map_or(false, |t_id| *t_id == trip.trip_id)
                                    });

                                    route_match || trip_match
                                });

                            if is_active && applies_to_trip_without_a_referenced_stop {
                                is_cancelled = true;
                            }
                        }
                    }
                    total_time_added_looking_at_rt_alerts += timer_looking_at_rt_alerts.elapsed();

                    let mut active_update: Option<&catenary::aspen_dataset::AspenisedTripUpdate> =
                        None;
                    if let Some(data) = &rt_data {
                        let timer_looking_at_rt_trips = std::time::Instant::now();
                        if let Some(update_ids) =
                            data.trip_id_to_trip_update_ids.get(trip_id.as_str())
                        {
                            for uid in update_ids {
                                if let Some(u) = data.trip_updates.get(uid) {
                                    if u.trip.start_date == Some(*date) {
                                        active_update = Some(u);
                                        break;
                                    }
                                    if u.trip.start_date.is_none() {
                                        for stu in &u.stop_time_update {
                                            let update_time = stu
                                                .departure
                                                .as_ref()
                                                .and_then(|d| d.time)
                                                .or(stu.arrival.as_ref().and_then(|a| a.time));

                                            if let Some(u_time) = update_time {
                                                if let Some(static_stop) = rows.iter().find(|r| {
                                                     if let Some(seq) = stu.stop_sequence {
                                                         if r.gtfs_stop_sequence == seq as u32 { return true; }
                                                     }
                                                     if let Some(sid) = &stu.stop_id {
                                                          return crate::stop_matching::rt_stop_matches_scheduled_simple(sid, r.stop_id.as_str());
                                                     }
                                                     false
                                                 }) {
                                                     let offset = static_stop.departure_time_since_start
                                                        .or(static_stop.arrival_time_since_start)
                                                        .or(static_stop.interpolated_time_since_start)
                                                        .unwrap_or(0);
                                                     
                                                     let tz = chrono_tz::Tz::from_str_insensitive(&itinerary_meta.timezone).unwrap_or(chrono_tz::UTC);
                                                     
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
                        total_time_added_looking_at_rt_trips += timer_looking_at_rt_trips.elapsed();
                    }

                    if let Some(update) = active_update {
                        let timer_looking_at_rt_alerts = std::time::Instant::now();
                        if let Some(stu) = update.stop_time_update.iter().find(|s| {
                            s.stop_sequence == Some(row.gtfs_stop_sequence as u16)
                                || s.stop_id
                                    .as_ref()
                                    .map(|sid| {
                                        crate::stop_matching::rt_stop_matches_scheduled_simple(
                                            sid,
                                            row.stop_id.as_str(),
                                        )
                                    })
                                    .unwrap_or(false)
                        }) {
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
                            if stu.schedule_relationship == Some(catenary::aspen_dataset::AspenisedStopTimeScheduleRelationship::Skipped) { is_cancelled = true; }
                        }
                        if update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) { is_cancelled = true; }
                        total_time_added_looking_at_rt_alerts +=
                            timer_looking_at_rt_alerts.elapsed();
                    }

                    if let Some(d) = rt_dep {
                        if d > departure_ts as u64 + 60 {
                            is_delayed = true;
                        }
                    }
                    let display_platform = rt_platform.or_else(|| {
                        stop_platform_map
                            .get(row.stop_id.as_str())
                            .cloned()
                            .flatten()
                    });

                    if !route_info_map.contains_key(&route_id.to_string()) {
                        route_info_map.insert(
                            route_id.to_string(),
                            RouteInfoExport {
                                short_name: route.short_name.clone().map(|x| x.to_string()),
                                long_name: route.long_name.clone(),
                                agency_name: agency_name.clone(),
                                color: route.color.clone().map(|x| x.to_string()),
                                text_color: route.text_color.clone().map(|x| x.to_string()),
                                route_type: route.route_type as i32,
                            },
                        );
                    }

                    let is_subway_or_tram = route.route_type == 0 || route.route_type == 1;

                    if !is_subway_or_tram
                        && (is_long_distance
                            || (matches!(station_key, StationKey::Osm(_)) && route.route_type != 3))
                    {
                        let item = DepartureItem {
                            scheduled_departure: Some(departure_ts as u64),
                            realtime_departure: rt_dep,
                            scheduled_arrival: Some(
                                (midnight_ts
                                    + trip.start_time as i64
                                    + row.arrival_time_since_start.unwrap_or(dep_time_offset)
                                        as i64) as u64,
                            ),
                            realtime_arrival: rt_arr,
                            service_date: *date,
                            headsign: headsign,
                            platform: display_platform.clone(),
                            trip_id: trip_id.to_string(),
                            trip_short_name: trip.trip_short_name.clone().map(|x| x.to_string()),
                            route_id: route_id.to_string(),
                            stop_id: row.stop_id.to_string(),
                            cancelled: is_cancelled,
                            delayed: is_delayed,
                            chateau_id: chateau.clone(),
                            last_stop: is_last_stop,
                        };
                        ld_departures_by_group
                            .entry((chateau.clone(), station_key.clone()))
                            .or_default()
                            .push(item);
                    } else {
                        let r_key = LocalRouteKey {
                            chateau: chateau.clone(),
                            route_id: route_id.to_string(),
                        };
                        local_route_meta_map
                            .entry(r_key.clone())
                            .or_insert((route.clone(), agency_name.unwrap_or_default()));

                        let item = LocalDepartureItem {
                            trip_id: CompactString::from(trip_id.as_str()),
                            departure_schedule: Some(departure_ts as u64),
                            departure_realtime: rt_dep,
                            arrival_schedule: Some(
                                (midnight_ts
                                    + trip.start_time as i64
                                    + row.arrival_time_since_start.unwrap_or(dep_time_offset)
                                        as i64) as u64,
                            ),
                            arrival_realtime: rt_arr,
                            stop_id: CompactString::from(row.stop_id.as_str()),
                            stop_name: stop_name_map.get(row.stop_id.as_str()).cloned().flatten(),
                            cancelled: is_cancelled,
                            platform: display_platform,
                            service_date: *date,
                            last_stop: is_last_stop,
                        };
                        local_departures
                            .entry(r_key)
                            .or_default()
                            .entry(headsign)
                            .or_default()
                            .push(item);

                        if !stop_output_map.contains_key(row.stop_id.as_str()) {
                            if let Some(s) = stop_full_info_map.get(row.stop_id.as_str()) {
                                stop_output_map.insert(
                                    row.stop_id.to_string(),
                                    StopOutputV3 {
                                        gtfs_id: CompactString::from(s.gtfs_id.as_str()),
                                        name: s.name.clone().unwrap_or_default(),
                                        lat: s.point.as_ref().unwrap().y,
                                        lon: s.point.as_ref().unwrap().x,
                                        osm_station_id: s.osm_station_id,
                                        timezone: s
                                            .timezone
                                            .clone()
                                            .unwrap_or("ETC/Utc".to_string()),
                                    },
                                );
                            }
                        }
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

    // Filter closest stop for Local
    for (_route_key, headsigns_map) in local_departures.iter_mut() {
        for (_headsign, items) in headsigns_map.iter_mut() {
            if items.is_empty() {
                continue;
            }
            let mut min_dist = f64::MAX;
            let mut best_stop_id: Option<CompactString> = None;
            for item in items.iter() {
                if let Some(dist) = stop_dist_map.get(item.stop_id.as_str()) {
                    if *dist < min_dist {
                        min_dist = *dist;
                        best_stop_id = Some(item.stop_id.clone());
                    }
                }
            }
            if let Some(target) = best_stop_id {
                items.retain(|i| i.stop_id == target);
            }
        }
    }

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
    ))
}
