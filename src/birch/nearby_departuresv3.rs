// Copyright
// Catenary Transit Initiatives
// Nearby Departures V3 (Hybrid: Long Distance Station-Grouped + Local Route-Headsign-Grouped)

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
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Bool;
use diesel::dsl::sql;
use diesel::sql_types::*;
use diesel_async::RunQueryDsl;
use futures::stream::StreamExt;
use geo::HaversineDistance;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use compact_str::CompactString;
use chrono::NaiveDate;
use chrono::TimeZone;

// Import logic from shared modules
use crate::departures_shared::{
    fetch_stop_data_for_chateau, ValidTripSet, estimate_service_date,
    ItinOption, StopDataResult
};
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use catenary::make_calendar_structure_from_pg;
use catenary::aspen_dataset::{AspenisedTripUpdate, AspenisedAlert};
use catenary::aspen::lib::AspenRpcClient;


#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoordsV3 {
    lat: f64,
    lon: f64,
    departure_time: Option<u64>,
    radius: Option<f64>,
    limit_per_station: Option<usize>,
}

#[derive(Serialize, Clone, Debug)]
pub struct StopOutputV3 {
    pub gtfs_id: CompactString,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub osm_station_id: Option<i64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct NearbyDeparturesV3Response {
    pub long_distance: Vec<StationDepartureGroupExport>,
    pub local: Vec<DepartureRouteGroupExportV3>,
    // Map<Chateau, Map<RouteId, RouteInfo>>
    pub routes: HashMap<String, HashMap<String, RouteInfoExport>>,
    // Map<Chateau, Map<StopId, StopOutputV3>>
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
    pub agency_name: Option<String>, // Added for convenience
    // Key: Headsign Name
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
    pub last_stop: bool,
}

#[actix_web::get("/nearbydeparturesfromcoordsv3")]
pub async fn nearby_from_coords_v3(
    req: HttpRequest,
    query: Query<NearbyFromCoordsV3>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let start = Instant::now();
    let limit_per_station = query.limit_per_station.unwrap_or(10);
    
    let conn_pool = pool.as_ref();
    let db_connection_start = Instant::now();
    let mut conn = match conn_pool.get().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().body("DB Connection Failed"),
    };
    let db_connection_time = db_connection_start.elapsed();

    let departure_time = query.departure_time.unwrap_or_else(|| catenary::duration_since_unix_epoch().as_secs());
    let departure_time_chrono = chrono::Utc.timestamp_opt(departure_time as i64, 0).unwrap();
    
    // Agencies that are *candidates* for Long Distance (combined with Route Type 2 logic)
    let long_distance_chateaux: HashSet<&str> = [
        "sncf", "nationalrailuk", "sncb", "nederlandse~spoorwegen",
        "rejseplanen~dk~gtfs", "norge", "sverige", "lv", "ztp~krakow",
        "deutschland", "schweiz", "trenitalia", "kordis",
        "pražskáintegrovanádoprava", "koleje~dolnoslaskie",
        "pkp~intercity~pl", "renfeoperadora"
    ].iter().cloned().collect();

    let long_distance_chateaux_arc = Arc::new(long_distance_chateaux);


    // 3. Fetch Nearby Stops (ALL agencies)
    let input_point = geo::Point::new(query.lon, query.lat);
    let radius = query.radius.unwrap_or(5000.0);
    let spatial_resolution = catenary::make_degree_length_as_distance_from_point(&input_point, radius);
    
    let where_query = format!(
        "ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
        query.lon, query.lat, spatial_resolution
    );

    let stops_fetch_start = Instant::now();
    let stops: Vec<catenary::models::Stop> = match catenary::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query))
        // No chateau filter anymore!
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(&mut conn)
        .await 
    {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().body(format!("Stop fetch error: {:?}", e)),
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
            }
        });
    }

    // 4. Group Stops & Helpers
    // We reuse station logic for everyone to find representative stops if needed, but primarily for LD.
    let mut station_groups: HashMap<(String, StationKey), Vec<catenary::models::Stop>> = HashMap::new();
    let mut station_group_metadata: HashMap<(String, StationKey), (String, f64, f64, f64)> = HashMap::new();
    let mut stop_dist_map: HashMap<String, f64> = HashMap::new(); // StopID -> Dist
    let mut stop_full_info_map: HashMap<String, catenary::models::Stop> = HashMap::new();

    // Create a platform lookup map
    let stop_platform_map: HashMap<String, Option<String>> = stops.iter()
        .map(|s| (s.gtfs_id.clone(), s.platform_code.clone()))
        .collect();

    // Create a name lookup map
    let stop_name_map: HashMap<String, Option<String>> = stops.iter()
        .map(|s| (s.gtfs_id.clone(), s.name.clone()))
        .collect();

    for stop in &stops {
        // Calculate distance
        let stop_point = geo::Point::new(stop.point.as_ref().unwrap().x, stop.point.as_ref().unwrap().y);
        let dist = input_point.haversine_distance(&stop_point);

        if stops.len() > 800 && dist > 1500.0 && stop.primary_route_type == Some(3) {
            continue;
        }

        stop_dist_map.insert(stop.gtfs_id.clone(), dist);
        stop_full_info_map.insert(stop.gtfs_id.clone(), stop.clone());

        /* Grouping Logic (Primarily for Long Distance, but useful structure) */
        let key = if let Some(osm_id) = stop.osm_station_id {
            StationKey::Osm(osm_id)
        } else if let Some(parent) = &stop.parent_station {
            StationKey::Parent(parent.clone())
        } else {
            StationKey::Stop(stop.gtfs_id.clone())
        };

        let map_key = (stop.chateau.clone(), key.clone());
        station_groups.entry(map_key.clone()).or_default().push(stop.clone());

        let entry = station_group_metadata.entry(map_key).or_insert((
            stop.name.clone().unwrap_or_default(),
            stop_point.y(), 
            stop_point.x(),
            dist
        ));
        
        if dist < entry.3 {
            *entry = (stop.name.clone().unwrap_or_default(), stop_point.y(), stop_point.x(), dist);
        }
    }


    // 5. Connect to Etcd (Aspen) early for concurrency
    let etcd_connection_start = Instant::now();
    let etcd_reuser = etcd_reuser.as_ref();
    let mut etcd = None;
    {
        let etcd_reuser_contents = etcd_reuser.read().await;
        if let Some(client) = etcd_reuser_contents.as_ref() {
            if client.clone().status().await.is_ok() {
                etcd = Some(client.clone());
            }
        }
    }
    
    if etcd.is_none() {
         // Try connect
         let new_client = etcd_client::Client::connect(
             etcd_connection_ips.ip_addresses.as_slice(),
             etcd_connection_options.as_ref().as_ref().to_owned(),
         ).await;
         if let Ok(c) = new_client {
             etcd = Some(c);
         }
    }
    let etcd_arc = Arc::new(etcd);
    let etcd_connection_time = etcd_connection_start.elapsed();

    // 5. Fetch Data (Pipelined Static + Realtime)
    let mut chateau_stops: HashMap<String, HashSet<String>> = HashMap::new();
    for ((chateau, _), stops) in &station_groups {
        for stop in stops {
            chateau_stops.entry(chateau.clone()).or_default().insert(stop.gtfs_id.clone());
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
        
        let stop_to_key_map: HashMap<String, StationKey> = stops.iter().map(|s| (s.gtfs_id.clone(), if let Some(osm) = s.osm_station_id { StationKey::Osm(osm) } else if let Some(p) = &s.parent_station { StationKey::Parent(p.clone()) } else { StationKey::Stop(s.gtfs_id.clone()) } )).collect();
        let stop_platform_map: HashMap<String, Option<String>> = stops.iter().map(|s| (s.gtfs_id.clone(), s.platform_code.clone())).collect();
        let stop_name_map: HashMap<String, Option<String>> = stops.iter().map(|s| (s.gtfs_id.clone(), s.name.clone())).collect();
        let full_info_map_clone = stop_full_info_map.clone();

        let stop_dist_map_clone = stop_dist_map.clone();

        chateau_futures.push(async move {
            fetch_chateau_data(pool, chateau_clone, stop_ids_vec, dep_time, etcd_clone, ld_arc_clone, stop_to_key_map, stop_platform_map, stop_name_map, full_info_map_clone, stop_dist_map_clone).await
        });

    }

    let pipeline_start = Instant::now();
    let pipeline_results: Vec<Option<(HashMap<(String, StationKey), Vec<DepartureItem>>, HashMap<LocalRouteKey, (catenary::models::Route, String, HashMap<String, Vec<LocalDepartureItem>>)>, HashMap<String, RouteInfoExport>, HashMap<String, StopOutputV3>)>> = futures::stream::iter(chateau_futures)
        .buffer_unordered(10)
        .collect()
        .await;
    let pipeline_processing_time = pipeline_start.elapsed();

    // Flatten results into structure
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> = HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> = HashMap::new();
    let mut local_route_meta: HashMap<LocalRouteKey, (catenary::models::Route, String)> = HashMap::new(); // Route, AgencyName
    let mut routes_output: HashMap<String, HashMap<String, RouteInfoExport>> = HashMap::new();
    let mut stops_output: HashMap<String, HashMap<String, StopOutputV3>> = HashMap::new();

    for res in pipeline_results {
        if let Some((ld_part, local_part, route_part, stop_part)) = res {
             let mut chateau_opt = None;
             
             if let Some((c, _)) = ld_part.keys().next() { chateau_opt = Some(c.clone()); }
             else if let Some(k) = local_part.keys().next() { chateau_opt = Some(k.chateau.clone()); }
             
             for (k, v) in ld_part {
                 chateau_opt = Some(k.0.clone());
                 ld_departures_by_group.entry(k).or_default().extend(v);
             }
             for (k, (route, agency_name, headsigns)) in local_part {
                 chateau_opt = Some(k.chateau.clone());
                 local_route_meta.entry(k.clone()).or_insert((route, agency_name));
                 for (h, items) in headsigns {
                     local_departures.entry(k.clone()).or_default().entry(h).or_default().extend(items);
                 }
             }
             
             if let Some(c) = chateau_opt {
                 routes_output.entry(c.clone()).or_default().extend(route_part);
                 stops_output.entry(c).or_default().extend(stop_part);
             }
        }
    }


    // 7. Format Outputs
    
    // A. Long Distance
    let mut ld_output: Vec<StationDepartureGroupExport> = Vec::new();
    for ((chateau, key), meta) in station_group_metadata {
        if let Some(mut deps) = ld_departures_by_group.remove(&(chateau.clone(), key.clone())) {
            deps.sort_by_key(|d| d.scheduled_departure.unwrap_or(0));
            deps.truncate(limit_per_station); // Limit LD departures

            let (osm_id, rep_id) = match key {
                StationKey::Osm(id) => (Some(id), "".to_string()),
                StationKey::Parent(ref id) => (None, id.clone()),
                StationKey::Stop(ref id) => (None, id.clone()),
            };
            let rep_id_fixed = if rep_id.is_empty() {
                 station_groups.get(&(chateau.clone(), key.clone())).and_then(|v| v.as_slice().first()).map(|s| s.gtfs_id.clone()).unwrap_or_default()
            } else { rep_id };

            let timezone = tz_search::lookup(meta.1, meta.2)
                .unwrap_or_else(|| String::from("Etc/GMT"));

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
    // MERGE OSM STATIONS logic
    // We might have multiple Chateaux reporting the same OSM station (e.g. Amtrak, Metrolink both at Union Station)
    // We want to merge these into a single StationDepartureGroupExport.
    
    // 1. Fetch authoritative OSM Station Data
    let osm_ids_to_fetch: Vec<i64> = ld_output.iter().filter_map(|g| g.osm_station_id).collect();
    let mut osm_station_info_map: HashMap<i64, catenary::models::OsmStation> = HashMap::new();
    
    if !osm_ids_to_fetch.is_empty() {
        let osm_stations_data: Vec<catenary::models::OsmStation> = catenary::schema::gtfs::osm_stations::dsl::osm_stations
            .filter(catenary::schema::gtfs::osm_stations::osm_id.eq_any(&osm_ids_to_fetch))
            .load::<catenary::models::OsmStation>(&mut conn)
            .await
            .unwrap_or_default();
            
        for s in osm_stations_data {
            osm_station_info_map.insert(s.osm_id, s);
        }
    }


    let mut merged_ld_output: Vec<StationDepartureGroupExport> = Vec::with_capacity(ld_output.len());
    let mut osm_station_map: HashMap<i64, usize> = HashMap::new(); // OSM ID -> Index in merged_ld_output

    for group in ld_output {
        if let Some(osm_id) = group.osm_station_id {
            if let Some(&idx) = osm_station_map.get(&osm_id) {
                // Merge into existing
                let existing = &mut merged_ld_output[idx];
                
                // Append departures
                existing.departures.extend(group.departures);
                
                // NOTE: We do NOT need to update distance/name here because we handle it below for ALL items involving this OSM ID
                // actually we check if we need to update the distance to be the MINIMUM of all contributing stops
                // But we will use the OSM Node coordinate for the final distance calculation.
            } else {
                // New OSM station
                let idx = merged_ld_output.len();
                let mut new_group = group.clone();
                
                // Override with Authoritative Data if available
                if let Some(osm_info) = osm_station_info_map.get(&osm_id) {
                     if let Some(n) = &osm_info.name {
                         new_group.station_name = n.clone();
                     }
                     new_group.lat = osm_info.point.y;
                     new_group.lon = osm_info.point.x;
                     
                     // Recalculate distance based on OSM Node location
                     let osm_point = geo::Point::new(osm_info.point.x, osm_info.point.y);
                     new_group.distance_m = input_point.haversine_distance(&osm_point);
                     
                     // Timezone lookup? Theoretically we should re-lookup if the location changed significantly, 
                     // but likely the first stop's timezone is fine or we do a new lookup.
                     // For performance we skip new timezone lookup unless we want to be very precise.
                }

                merged_ld_output.push(new_group);
                osm_station_map.insert(osm_id, idx);
            }
        } else {
            // Not an OSM station, just add it
            merged_ld_output.push(group);
        }
    }

    // Sort departures within merged groups (since we just appended)
    for group in &mut merged_ld_output {
        if group.osm_station_id.is_some() {
             group.departures.sort_by_key(|d| d.scheduled_departure.unwrap_or(0));
             group.departures.truncate(limit_per_station);
        }
    }

    let mut ld_output = merged_ld_output;
    ld_output.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap());

    // B. Local
    let mut local_output: Vec<DepartureRouteGroupExportV3> = Vec::new();
    
    // Logic to limit buses if we have rail or many stops
    let has_rail = ld_output.iter().any(|g| g.osm_station_id.is_some());
    let many_stops = stops.len() > 50;
    let limit_buses = has_rail || many_stops;
    let bus_cutoff_m = 2000.0;

    for (r_key, headsigns) in local_departures {
        let (route, a_name) = local_route_meta.get(&r_key).unwrap();
        
        // Calculate closest distance for this route
        let mut min_dist = 9999999.0;
        // Optimization: check if we even need to process headsigns if it's a bus and might be far? 
        // We need min_dist to decide.
        
        let mut sorted_headsigns = HashMap::new();
        for (h, mut items) in headsigns {
            items.sort_by_key(|i| i.departure_schedule.unwrap_or(0));
            items.truncate(10); 
            sorted_headsigns.insert(h, items);
        }

        for (_, items) in &sorted_headsigns {
            for item in items {
                if let Some(d) = stop_dist_map.get(item.stop_id.as_str()) {
                    if *d < min_dist { min_dist = *d; }
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
    // Sort local by distance?
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
    stop_to_key_map: HashMap<String, StationKey>,
    stop_platform_map: HashMap<String, Option<String>>,
    stop_name_map: HashMap<String, Option<String>>,

    stop_full_info_map: HashMap<String, catenary::models::Stop>,
    stop_dist_map: HashMap<String, f64>,
) -> Option<(HashMap<(String, StationKey), Vec<DepartureItem>>, HashMap<LocalRouteKey, (catenary::models::Route, String, HashMap<String, Vec<LocalDepartureItem>>)>, HashMap<String, RouteInfoExport>, HashMap<String, StopOutputV3>)> {
    
    // OPTIMIZATION: Filter redundant stops for Local Transport (Bus/Tram/Subway)
    // We only want the closest stop for each direction/headsign combination to avoid DB spam.
    
    let mut final_stop_ids = Vec::new();
    let mut stops_to_check = Vec::new();

    for stop_id in &stop_ids {
        if let Some(stop) = stop_full_info_map.get(stop_id) {
            match stop.primary_route_type {
                Some(0) | Some(1) | Some(3) => stops_to_check.push(stop_id.clone()),
                _ => final_stop_ids.push(stop_id.clone()),
            }
        } else {
             // Fallback
             final_stop_ids.push(stop_id.clone());
        }
    }

    if !stops_to_check.is_empty() {
        // Fetch directions for these stops to see which ones are redundant
        let mut conn = pool.get().await.unwrap(); // Connection just for this optimization query
        
        let direction_rows: Vec<catenary::models::DirectionPatternRow> = catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
             .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau.clone()))
             .filter(catenary::schema::gtfs::direction_pattern::stop_id.eq_any(&stops_to_check))
             .select(catenary::models::DirectionPatternRow::as_select())
             .load(&mut conn)
             .await
             .unwrap_or_default();

        // Map: (DirectionPatternId, HeadsignIdx) -> (MinDist, StopId)
        // We use String and Option<i16> as key
        let mut best_stops: HashMap<(String, Option<i16>), (f64, String)> = HashMap::new();
        
        // Also keep track of stops that have NO direction info? 
        // If a stop is not in direction_pattern, it won't be in direction_rows.
        // If we strictly filter, we might drop it. But if it's not in direction_pattern, 
        // likely it's not useful for our direction-based grouping anyway.
        // V2 logic implies we only care about stops that resolved to a direction.
        
        for row in direction_rows {
            if let Some(dist) = stop_dist_map.get(row.stop_id.as_str()) {
                let key = (row.direction_pattern_id, row.stop_headsign_idx);
                
                match best_stops.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if *dist < e.get().0 {
                            e.insert((*dist, row.stop_id.to_string()));
                        }
                    },
                    std::collections::hash_map::Entry::Vacant(e) => {
                         e.insert((*dist, row.stop_id.to_string()));
                    }
                }
            }
        }
        
        let winners: HashSet<String> = best_stops.values().map(|(_, s)| s.clone()).collect();
        final_stop_ids.extend(winners);
        
        // Safety: If a stop was in stops_to_check but didn't appear in ANY direction pattern,
        // it gets dropped here. This is consistent with V2 logic which ignores such stops for local transport views.
    } else {
        // No local transport stops to check
    }

    // 1. Fetch Static using refined list
    let (
        chateau_id,
        itins,
        itin_meta,
        direction_meta, 
        trips_compressed,
        direction_rows, 
        routes,
        agencies,
        _,
        calendar,
        calendar_dates
    ) = fetch_stop_data_for_chateau(pool, chateau.clone(), final_stop_ids, false).await;
    
    let mut rt_data: Option<catenary::aspen::lib::TripsSelectionResponse> = None;
    // We will just access rt_data.trip_updates directly later
    // let mut rt_trips = AHashMap::new(); 
    let mut rt_alerts = Vec::new();

    
    let etcd_opt = etcd_arc.as_ref();
    let processing_start = Instant::now();
    let mut trip_ids = Vec::new();

    let calendar_struct = make_calendar_structure_from_pg(vec![calendar.clone()], vec![calendar_dates.clone()]).unwrap_or_default();
    let service_map = calendar_struct.get(&chateau);
    
    
    // 2. Identify Active Trips

    // ... logic to find active trips ...
    
    // START OF REFACTOR to fetch Max Sequences & Identify Active Trips & Fetch RT concurrently
    // I will extract active IDs first (CPU), then dispatch RT fetch + Max Seq Fetch (IO/DB)
    
    if !trips_compressed.is_empty() {
            if let Some(services) = service_map {
                let base_date = departure_time_chrono.date_naive() - chrono::Duration::days(1);
                for (tid, trip) in &trips_compressed {
                    if let Some(service) = services.get(trip.service_id.as_str()) {
                            let mut active = false;
                            for i in 0..3 { 
                                let d = base_date + chrono::Duration::days(i);
                                if catenary::datetime_in_service(service, d) {
                                active = true; break;
                                }
                            }
                            if active { trip_ids.push(tid.clone()); }
                    }
                }
            }
            
            if let Some(etcd) = etcd_opt {
                if !trip_ids.is_empty() {
                    let mut etcd_clone = etcd.clone();
                    // Get Metadata
                    let meta_res = etcd_clone.get(format!("/aspen_assigned_chateaux/{}", chateau), None).await;
                    match meta_res {
                        Ok(resp) => {
                            if let Some(kv) = resp.kvs().first() {
                                match catenary::bincode_deserialize::<ChateauMetadataEtcd>(kv.value()) {
                                    Ok(meta) => {
                                        // Connect Aspen
                                        match catenary::aspen::lib::spawn_aspen_client_from_ip(&meta.socket).await {
                                            Ok(client) => {
                                                let t_fut = client.get_all_trips_with_ids(tarpc::context::current(), chateau.clone(), trip_ids.clone());
                                                let a_fut = client.get_all_alerts(tarpc::context::current(), chateau.clone());
                                                let (t, a) = tokio::join!(t_fut, a_fut);
                                                
                                                match t {
                                                    Ok(Some(tr)) => {
                                                        println!("RT_DEBUG [{}]: Got {} trip updates", chateau, tr.trip_updates.len());
                                                        rt_data = Some(tr);
                                                    }
                                                    Ok(None) => {
                                                        println!("RT_DEBUG [{}]: Aspen returned None for trips", chateau);
                                                    }
                                                    Err(e) => {
                                                        eprintln!("RT_DEBUG [{}]: Failed to get trips from aspen: {:?}", chateau, e);
                                                    }
                                                }
                                                match a {
                                                    Ok(Some(al)) => {
                                                        rt_alerts = al.into_values().collect();
                                                    }
                                                    Ok(None) => {}
                                                    Err(e) => {
                                                        eprintln!("RT_DEBUG [{}]: Failed to get alerts from aspen: {:?}", chateau, e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("RT_DEBUG [{}]: Failed to connect to aspen at {:?}: {:?}", chateau, meta.socket, e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("RT_DEBUG [{}]: Failed to deserialize chateau metadata: {:?}", chateau, e);
                                    }
                                }
                            } else {
                                println!("RT_DEBUG [{}]: No etcd kv found for chateau", chateau);
                            }
                        }
                        Err(e) => {
                            eprintln!("RT_DEBUG [{}]: Etcd get failed: {:?}", chateau, e);
                        }
                    }
                } else {
                    println!("RT_DEBUG [{}]: No trip_ids to fetch", chateau);
                }
            } else {
                println!("RT_DEBUG [{}]: No etcd connection available", chateau);
            }
    }

    
    // 4. Process Data into Objects
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> = HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> = HashMap::new();
    let mut local_route_meta_map: HashMap<LocalRouteKey, (catenary::models::Route, String)> = HashMap::new();
    let mut route_info_map: HashMap<String, RouteInfoExport> = HashMap::new(); 
    let mut stop_output_map: HashMap<String, StopOutputV3> = HashMap::new();

    let departure_time = departure_time_chrono.timestamp();
    
    // We iterate active trip_ids and look them up in trips_compressed
    for trip_id in trip_ids {
        let trip = match trips_compressed.get(&trip_id) {
            Some(t) => t,
            None => continue,
        };

        
        // A. Get Route & Meta
        let itinerary_meta = match itin_meta.get(&trip.itinerary_pattern_id) {
            Some(m) => m,
            None => continue,
        };
        
        let route_id = &itinerary_meta.route_id;
        let route = match routes.get(route_id.as_str()) {
            Some(r) => r,
            None => continue,
        };

        // B. Determine Category
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
        }
        
        // C. Service Date (Which specific date?)
        // We know it is active in the 3 day window. We need the exact date(s) to calc timestamp.
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


        // D. Process Itinerary Rows
        let rows = match itins.get(&trip.itinerary_pattern_id) {
            Some(r) => r,
            None => continue,
        };
        
        for row in rows {
            if let Some(station_key) = stop_to_key_map.get(row.stop_id.as_str()) {
                    let dep_time_offset = row.departure_time_since_start
                    .or(row.arrival_time_since_start)
                    .or(row.interpolated_time_since_start)
                    .unwrap_or(0);
                    let trip_start = trip.start_time;
                    
                    let mut is_last_stop = false;
                    if let Some(dir_id) = &itinerary_meta.direction_pattern_id {
                        if let Some(d_rows) = direction_rows.get(dir_id) {
                            if let Some(last) = d_rows.last() {
                                if last.stop_sequence == row.gtfs_stop_sequence {
                                    is_last_stop = true;
                                }
                            }
                        }
                    }


                    for date in &valid_service_dates {
                    let timezone_str = &itinerary_meta.timezone;
                    let tz = chrono_tz::Tz::from_str_insensitive(&timezone_str).unwrap_or(chrono_tz::UTC);
                    
                    let midnight_ts = tz.from_local_datetime(&date.and_hms_opt(0,0,0).unwrap())
                        .single().map(|t| t.timestamp()).unwrap_or(0);

                    let departure_ts = midnight_ts + trip_start as i64 + dep_time_offset as i64;
                    // Filter 18h
                     if departure_ts < (departure_time as i64) || departure_ts > (departure_time as i64 + 18 * 3600) {
                            continue;
                     }

                    // Determine Headsign
                    let headsign = row.stop_headsign_idx
                            .and_then(|idx| direction_meta.get(itinerary_meta.direction_pattern_id.as_ref()?)
                                .and_then(|dm| dm.stop_headsigns_unique_list.as_ref())
                                .and_then(|list| list.get(idx as usize))
                                .cloned().flatten())
                            .or(direction_meta.get(itinerary_meta.direction_pattern_id.as_ref().unwrap_or(&"".to_string()))
                                .map(|dm| dm.headsign_or_destination.clone()))
                            .unwrap_or_else(|| "Unknown".to_string());

                    // Agency Name
                    let agency_name = route.agency_id.clone().and_then(|aid| agencies.get(&aid).map(|a| a.agency_name.clone()));
                    
                    // Realtime Data Lookup
                    let mut rt_dep = None;
                    let mut rt_arr = None;
                    let mut rt_platform = None; // Placeholder
                    let mut is_cancelled = false;
                    let mut is_delayed = false;
                    
                    let mut active_update: Option<&catenary::aspen_dataset::AspenisedTripUpdate> = None;

                    if let Some(data) = &rt_data {
                         if let Some(update_ids) = data.trip_id_to_trip_update_ids.get(trip_id.as_str()) {
                             if !update_ids.is_empty() {
                                 // V2-style: Check if the first update uses dates to decide matching strategy
                                 let does_trip_set_use_dates = data.trip_updates
                                     .get(&update_ids[0])
                                     .map(|u| u.trip.start_date.is_some())
                                     .unwrap_or(false);
                                 
                                 let trip_offset = dep_time_offset as u64;
                                 
                                 // Filter updates by date matching
                                 let matching_updates: Vec<&catenary::aspen_dataset::AspenisedTripUpdate> = update_ids
                                     .iter()
                                     .filter_map(|uid| data.trip_updates.get(uid))
                                     .filter(|update| {
                                         if does_trip_set_use_dates {
                                             // Direct date match
                                             update.trip.start_date == Some(*date)
                                         } else {
                                             // V2-style date inference using RT timestamps
                                             let naive_date_approx_guess = update.stop_time_update.iter()
                                                 .filter(|x| x.departure.is_some() || x.arrival.is_some())
                                                 .filter_map(|x| {
                                                     x.departure.as_ref().and_then(|d| d.time)
                                                         .or_else(|| x.arrival.as_ref().and_then(|a| a.time))
                                                 })
                                                 .min();
                                             
                                             match naive_date_approx_guess {
                                                 Some(least_num) => {
                                                     let rt_least_naive_date = tz.timestamp_opt(least_num, 0).single();
                                                     if let Some(rt_time) = rt_least_naive_date {
                                                         let approx_service_date_start = rt_time - chrono::Duration::seconds(trip_offset as i64);
                                                         let approx_service_date = approx_service_date_start.date_naive();
                                                         
                                                         // Score dates within 1 day window
                                                         let day_before = approx_service_date - chrono::Duration::days(2);
                                                         let mut best_date: Option<chrono::NaiveDate> = None;
                                                         let mut best_score = i64::MAX;
                                                         
                                                         for i in 0..5 {
                                                             let check_day = day_before + chrono::Duration::days(i);
                                                             if let Some(services) = service_map {
                                                                 if let Some(service) = services.get(trip.service_id.as_str()) {
                                                                     if catenary::datetime_in_service(service, check_day) {
                                                                         let day_in_tz_midnight = tz.from_local_datetime(&check_day.and_hms_opt(12, 0, 0).unwrap())
                                                                             .single()
                                                                             .map(|t| t - chrono::Duration::hours(12));
                                                                         
                                                                         if let Some(midnight) = day_in_tz_midnight {
                                                                             let time_delta = (rt_time.timestamp() - midnight.timestamp()).abs();
                                                                             if time_delta < best_score {
                                                                                 best_score = time_delta;
                                                                                 best_date = Some(check_day);
                                                                             }
                                                                         }
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                         
                                                         best_date == Some(*date)
                                                     } else {
                                                         false
                                                     }
                                                 }
                                                 None => false,
                                             }
                                         }
                                     })
                                     .collect();
                                 
                                 if !matching_updates.is_empty() {
                                     active_update = Some(matching_updates[0]);
                                 }
                             }
                         }
                    }

                    if let Some(update) = active_update {
                        if let Some(stu) = update.stop_time_update.iter().find(|s| s.stop_id.as_deref() == Some(row.stop_id.as_str())) {
                            if let Some(d) = &stu.departure { if let Some(t) = d.time { rt_dep = Some(t as u64); } }
                            if let Some(a) = &stu.arrival { if let Some(t) = a.time { rt_arr = Some(t as u64); } }
                            
                            if let Some(p) = &stu.platform_string {
                                rt_platform = Some(p.to_string());
                            }

                            if stu.schedule_relationship == Some(catenary::aspen_dataset::AspenisedStopTimeScheduleRelationship::Skipped) { 
                                is_cancelled = true; 
                            }
                        }
                        if update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                is_cancelled = true;
                        }
                    }
                    
                    let display_platform = rt_platform.or_else(|| stop_platform_map.get(row.stop_id.as_str()).cloned().flatten());
                    
                     // Status
                    if let Some(d) = rt_dep {
                             if d > departure_ts as u64 + 60 { is_delayed = true; }
                    }


                    // --- Populate Route Info ---
                    if !route_info_map.contains_key(&route_id.to_string()) {
                        route_info_map.insert(route_id.to_string(), RouteInfoExport {
                            short_name: route.short_name.clone().map(|x| x.to_string()),
                            long_name: route.long_name.clone(),
                            agency_name: agency_name.clone(),
                            color: route.color.clone().map(|x| x.to_string()),
                            text_color: route.text_color.clone().map(|x| x.to_string()),
                            route_type: route.route_type as i32,
                        });
                    }

                    let is_subway_or_tram = route.route_type == 0 || route.route_type == 1;

                    if !is_subway_or_tram && (is_long_distance || (matches!(station_key, StationKey::Osm(_)) && route.route_type != 3)) {
                             let item = DepartureItem {
                                 scheduled_departure: Some(departure_ts as u64),
                                 realtime_departure: rt_dep,
                                 scheduled_arrival: Some( (midnight_ts + trip_start as i64 + row.arrival_time_since_start.unwrap_or(dep_time_offset) as i64) as u64 ),
                                 realtime_arrival: rt_arr,
                                 service_date: *date,
                                 headsign: headsign,
                                 platform: display_platform.clone(),
                                 trip_id: trip_id.to_string(),
                                 route_id: route_id.to_string(),
                                 stop_id: row.stop_id.to_string(),
                                 cancelled: is_cancelled,
                                 delayed: is_delayed,
                                 chateau_id: chateau.clone(),
                                 last_stop: is_last_stop,
                             };
                             ld_departures_by_group.entry((chateau.clone(), station_key.clone())).or_default().push(item);
                        } else {
                            // LOCAL
                            let r_key = LocalRouteKey {
                                chateau: chateau.clone(),
                                route_id: route_id.to_string(),
                            };
                            
                            local_route_meta_map.entry(r_key.clone()).or_insert((route.clone(), agency_name.unwrap_or_default()));

                            let item = LocalDepartureItem {
                                trip_id: CompactString::from(trip_id.as_str()),
                                departure_schedule: Some(departure_ts as u64),
                                departure_realtime: rt_dep,
                                arrival_schedule: Some((midnight_ts + trip_start as i64 + row.arrival_time_since_start.unwrap_or(dep_time_offset) as i64) as u64),
                                arrival_realtime: rt_arr,
                                stop_id: CompactString::from(row.stop_id.as_str()),
                                stop_name: stop_name_map.get(row.stop_id.as_str()).cloned().flatten(),
                                cancelled: is_cancelled,
                                platform: display_platform,
                                last_stop: is_last_stop,
                            };
                            
                            local_departures.entry(r_key)
                                .or_default()
                                .entry(headsign)
                                .or_default()
                                .push(item);

                            // Populate Stop Output
                            if !stop_output_map.contains_key(row.stop_id.as_str()) {
                                if let Some(s) = stop_full_info_map.get(row.stop_id.as_str()) {
                                    stop_output_map.insert(row.stop_id.to_string(), StopOutputV3 {
                                        gtfs_id: CompactString::from(s.gtfs_id.as_str()),
                                        name: s.name.clone().unwrap_or_default(),
                                        lat: s.point.as_ref().unwrap().y,
                                        lon: s.point.as_ref().unwrap().x,
                                        osm_station_id: s.osm_station_id,
                                    });
                                }
                            }
                        }


                    }
            }
        
       }

    } // End trip loop

    // Post-process local_departures to isolate closest stop per headsign (Local Transport isolation)
    // Matches v2 logic: "only show the instance of departure from the closest stop"
    for (_route_key, headsigns_map) in local_departures.iter_mut() {
        for (_headsign, items) in headsigns_map.iter_mut() {
             if items.is_empty() { continue; }
             
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

    
    Some((ld_departures_by_group, local_departures.into_iter().map(|(k, v)| {
          let (r, a) = local_route_meta_map.get(&k).unwrap().clone();
          (k, (r, a, v))
    }).collect(), route_info_map, stop_output_map))
}

