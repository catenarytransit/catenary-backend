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
pub struct NearbyDeparturesV3Response {
    pub long_distance: Vec<StationDepartureGroupExport>,
    pub local: Vec<DepartureRouteGroupExportV3>,
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
}

#[derive(Serialize, Clone, Debug)]
pub struct DepartureItem {
    pub scheduled_departure: Option<u64>,
    pub realtime_departure: Option<u64>,
    pub scheduled_arrival: Option<u64>,
    pub realtime_arrival: Option<u64>,
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub agency_name: Option<String>,
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
    pub stop_id: CompactString,
    pub stop_name: Option<String>,
    pub cancelled: bool,
    pub platform: Option<String>,
    pub last_stop: bool,
    // Add other fields if needed for use, keeping it lean for now
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
    let mut conn = match conn_pool.get().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().body("DB Connection Failed"),
    };

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

    if stops.is_empty() {
        return HttpResponse::Ok().json(NearbyDeparturesV3Response { long_distance: vec![], local: vec![] });
    }

    // 4. Group Stops & Helpers
    // We reuse station logic for everyone to find representative stops if needed, but primarily for LD.
    let mut station_groups: HashMap<(String, StationKey), Vec<catenary::models::Stop>> = HashMap::new();
    let mut station_group_metadata: HashMap<(String, StationKey), (String, f64, f64, f64)> = HashMap::new();
    let mut stop_dist_map: HashMap<String, f64> = HashMap::new(); // StopID -> Dist

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
        stop_dist_map.insert(stop.gtfs_id.clone(), dist);

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


        chateau_futures.push(async move {
            fetch_chateau_data(pool, chateau_clone, stop_ids_vec, dep_time, etcd_clone, ld_arc_clone, stop_to_key_map, stop_platform_map, stop_name_map).await
        });

    }

    // Results is now a list of (Partial LD, Partial Local)
    let pipeline_results: Vec<Option<(HashMap<(String, StationKey), Vec<DepartureItem>>, HashMap<LocalRouteKey, (catenary::models::Route, String, HashMap<String, Vec<LocalDepartureItem>>)>)>> = futures::stream::iter(chateau_futures)
        .buffer_unordered(10)
        .collect()
        .await;

    // Flatten results into structure
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> = HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> = HashMap::new();
    let mut local_route_meta: HashMap<LocalRouteKey, (catenary::models::Route, String)> = HashMap::new(); // Route, AgencyName


    for res in pipeline_results {
        if let Some((ld_part, local_part)) = res {
             for (k, v) in ld_part {
                 ld_departures_by_group.entry(k).or_default().extend(v);
             }
             for (k, (route, agency_name, headsigns)) in local_part {
                 local_route_meta.entry(k.clone()).or_insert((route, agency_name));
                 for (h, items) in headsigns {
                     local_departures.entry(k.clone()).or_default().entry(h).or_default().extend(items);
                 }
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

            ld_output.push(StationDepartureGroupExport {
                station_name: meta.0,
                osm_station_id: osm_id,
                distance_m: meta.3,
                departures: deps,
                lat: meta.1,
                lon: meta.2,
            });
        }
    }
    ld_output.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap());

    // B. Local
    let mut local_output: Vec<DepartureRouteGroupExportV3> = Vec::new();
    for (r_key, headsigns) in local_departures {
        let (route, a_name) = local_route_meta.get(&r_key).unwrap();
        
        let mut sorted_headsigns = HashMap::new();
        for (h, mut items) in headsigns {
            items.sort_by_key(|i| i.departure_schedule.unwrap_or(0));
            items.truncate(10); // Limit? Maybe high limit for local or pagination
            sorted_headsigns.insert(h, items);
        }

        // Calculate closest distance for this route
        // We know the stops involved. We can look them up in `station_group_metadata` but that's convoluted.
        // Simplified: iterate headsigns -> items -> stop_id -> dist_map
        let mut min_dist = 9999999.0;
        for (_, items) in &sorted_headsigns {
            for item in items {
                if let Some(d) = stop_dist_map.get(item.stop_id.as_str()) {
                    if *d < min_dist { min_dist = *d; }
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
    // Sort local by distance?
    local_output.sort_by(|a, b| a.closest_distance.partial_cmp(&b.closest_distance).unwrap());

    HttpResponse::Ok().json(NearbyDeparturesV3Response {
        long_distance: ld_output,
        local: local_output,
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
) -> Option<(HashMap<(String, StationKey), Vec<DepartureItem>>, HashMap<LocalRouteKey, (catenary::models::Route, String, HashMap<String, Vec<LocalDepartureItem>>)>)> {
    
    // 1. Fetch Static
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
    ) = fetch_stop_data_for_chateau(pool, chateau.clone(), stop_ids, false).await;
    
    let mut rt_trips = AHashMap::new();
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
                    if let Ok(resp) = meta_res {
                        if let Some(kv) = resp.kvs().first() {
                            if let Ok(meta) = catenary::bincode_deserialize::<ChateauMetadataEtcd>(kv.value()) {
                                // Connect Aspen
                                if let Ok(client) = catenary::aspen::lib::spawn_aspen_client_from_ip(&meta.socket).await {
                                    let t_fut = client.get_all_trips_with_ids(tarpc::context::current(), chateau.clone(), trip_ids.clone());
                                    let a_fut = client.get_all_alerts(tarpc::context::current(), chateau.clone());
                                    let (t, a) = tokio::join!(t_fut, a_fut);
                                    
                                    if let Ok(Some(tr)) = t {
                                        rt_trips = tr.trip_updates;
                                    }
                                    if let Ok(Some(al)) = a {
                                        rt_alerts = al.into_values().collect();
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    
    // 4. Process Data into Objects
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> = HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> = HashMap::new();
    let mut local_route_meta_map: HashMap<LocalRouteKey, (catenary::models::Route, String)> = HashMap::new(); 

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
                    
                    if let Some(update) = rt_trips.get(trip_id.as_str()) {
                        if let Some(stu) = update.stop_time_update.iter().find(|s| s.stop_id.as_deref() == Some(row.stop_id.as_str())) {
                            if let Some(d) = &stu.departure { if let Some(t) = d.time { rt_dep = Some(t as u64); } }
                            if let Some(a) = &stu.arrival { if let Some(t) = a.time { rt_arr = Some(t as u64); } }
                            
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

                    if is_long_distance {
                             let item = DepartureItem {
                                 scheduled_departure: Some(departure_ts as u64),
                                 realtime_departure: rt_dep,
                                 scheduled_arrival: Some( (midnight_ts + trip_start as i64 + row.arrival_time_since_start.unwrap_or(dep_time_offset) as i64) as u64 ),
                                 realtime_arrival: rt_arr,
                                 route_short_name: route.short_name.clone().map(|x| x.to_string()),
                                 route_long_name: route.long_name.clone(),
                                 agency_name: agency_name,
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
                        }


                    }
            }
        
       }

    } // End trip loop

    
    Some((ld_departures_by_group, local_departures.into_iter().map(|(k, v)| {
          let (r, a) = local_route_meta_map.get(&k).unwrap().clone();
          (k, (r, a, v))
    }).collect()))
}

