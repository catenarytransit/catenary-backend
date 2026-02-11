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
use futures::FutureExt;
use futures::stream::StreamExt;
use geo::HaversineDistance;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
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
pub struct NearbyFromCoordsV3 {
    pub lat: f64,
    pub lon: f64,
    pub departure_time: Option<u64>,
    pub radius: Option<f64>,
    pub limit_per_station: Option<usize>,
    pub limit_per_headsign: Option<usize>,
    pub skip_realtime: Option<bool>,
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

#[derive(Clone)]
pub struct ChateauIntermediate {
    pub chateau: String,
    pub valid_trips: HashMap<String, Vec<ValidTripSet>>,
    pub routes: BTreeMap<String, catenary::models::Route>,
    pub agencies: BTreeMap<String, catenary::models::Agency>,
    pub service_map: Option<BTreeMap<String, catenary::CalendarUnified>>,
    pub stop_to_key_map: HashMap<(String, String), StationKey>,
    pub stop_platform_map: HashMap<String, Option<String>>,
    pub stop_name_map: HashMap<String, Option<String>>,
    pub stop_full_info_map: HashMap<String, catenary::models::Stop>,
    pub stop_dist_map: HashMap<String, f64>,
    pub direction_meta: BTreeMap<String, catenary::models::DirectionPatternMeta>,
    pub departure_time: i64,
    pub long_distance_chateaux: Arc<HashSet<&'static str>>,
    pub query_point: geo::Point<f64>,
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
    let start = Instant::now();
    let limit_per_station = query.limit_per_station.unwrap_or(10);
    let limit_per_headsign = query.limit_per_headsign.unwrap_or(20);

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
    let shared_meta = Arc::new(station_group_metadata);
    let shared_dist = Arc::new(stop_dist_map);
    let shared_info = Arc::new(stop_full_info_map);

    let chunk_intermediates = fetch_chunk_intermediates(
        NearbyDeparturesContext {
            pool: pool.get_ref().clone(),
            etcd_connection_ips: etcd_connection_ips.get_ref().clone(),
            etcd_connection_options: etcd_connection_options.get_ref().clone(),
            etcd_reuser: etcd_reuser.get_ref().clone(),
        },
        query.into_inner(),
        station_groups, // consumes map
        shared_meta.clone(),
        shared_dist.clone(),
        shared_info.clone(),
        input_point,
        db_connection_time,
        stops_fetch_time,
        start,
    )
    .await;

    let final_response = finalize_chunk_response(
        &chunk_intermediates,
        etcd_arc,
        false, // skip_realtime = false
        limit_per_station,
        limit_per_headsign,
        start,
        input_point,
        shared_meta,
        shared_dist,
        pool.get_ref().clone(),
    )
    .await;

    HttpResponse::Ok().json(final_response)
}

async fn fetch_chateau_intermediate(
    pool: Arc<CatenaryPostgresPool>,
    chateau: String,
    stop_ids: Vec<String>,
    departure_time_chrono: chrono::DateTime<chrono::Utc>,
    long_distance_chateaux: Arc<HashSet<&'static str>>,
    stop_to_key_map: HashMap<(String, String), StationKey>,
    stop_platform_map: HashMap<String, Option<String>>,
    stop_name_map: HashMap<String, Option<String>>,
    stop_full_info_map: HashMap<String, catenary::models::Stop>,
    stop_dist_map: HashMap<String, f64>,
    query_point: geo::Point<f64>,
) -> Option<ChateauIntermediate> {
    // 1. Determine Date Window
    let lookback_days = match chateau.as_str() {
        "sncb" | "schweiz" | "sncf" | "deutschland" => 2,
        _ => 14,
    };
    let date_window_start =
        departure_time_chrono.date_naive() - chrono::Duration::days(lookback_days);
    let date_window_end = departure_time_chrono.date_naive() + chrono::Duration::days(1);

    // 2. Optimization: Filter Redundant Stops for Local Transport
    let is_ld_chateau = long_distance_chateaux.contains(chateau.as_str());

    let mut final_stop_ids = Vec::new();
    let mut stops_to_check = Vec::new();

    if is_ld_chateau {
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

    let direction_stop_ids: Vec<String> = direction_rows_list
        .iter()
        .map(|r| r.stop_id.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let route_ids: Vec<CompactString> = itin_meta_list.iter().map(|m| m.route_id.clone()).collect();
    let pool_clone = pool.clone();
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

    let actual_itinerary_ids: Vec<String> = itins_result
        .iter()
        .map(|r| r.itinerary_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let trips_compressed_raw: Vec<catenary::models::CompressedTrip> = {
        let chateau_clone_trips = chateau.clone();
        let mut conn = pool.get().await.unwrap();
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

    println!(
        "V3_OPT: Fetch for {} took {}ms. Found {} trips (filtered).",
        chateau,
        t_v3_fetch.elapsed().as_millis(),
        trips_compressed_result.len()
    );

    // Fetch Calendar
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
    let service_map = calendar_struct.get(&chateau).cloned();

    // Map Construction
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

    // Pre-calculate Valid Trip Sets (Dates included)
    let trips_compressed: BTreeMap<String, catenary::models::CompressedTrip> =
        trips_compressed_result
            .into_iter()
            .map(|t| (t.trip_id.clone(), t))
            .collect();

    let seek_back = chrono::TimeDelta::new(5400, 0).unwrap();
    let seek_forward = chrono::TimeDelta::new(3600 * 12, 0).unwrap(); // 12 hours forward

    let mut valid_trips: HashMap<String, Vec<ValidTripSet>> = HashMap::new();

    for trip in trips_compressed.values() {
        let itinerary_meta = match itin_meta.get(&trip.itinerary_pattern_id) {
            Some(m) => m,
            None => continue,
        };

        if let Some(r) = routes.get(itinerary_meta.route_id.as_str()) {
            // Agency blacklist logic for National Rail if needed can be done later or here
            // birch didn't discard here, just flagged "is_long_distance" later.
            // We keep everything valid.
        } else {
            continue;
        }

        let timezone_str = &itinerary_meta.timezone;

        let rows = match itins.get(&trip.itinerary_pattern_id) {
            Some(r) => r,
            None => continue,
        };

        if rows.is_empty() {
            continue;
        }

        let row_ref = &rows[0];
        let time_since_start = row_ref
            .departure_time_since_start
            .or(row_ref.arrival_time_since_start)
            .or(row_ref.interpolated_time_since_start)
            .unwrap_or(0);

        // Frequencies decoding
        let frequency: Option<catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto> =
            trip.frequencies.as_ref().map(|data| {
                <catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto as prost::Message>::decode(
                    data.as_ref(),
                )
                .unwrap()
            });
        let freq_converted = frequency.map(|x| protobuf_to_frequencies(&x));

        let t_to_find_schedule_for = catenary::TripToFindScheduleFor {
            trip_id: trip.trip_id.clone(),
            chateau: chateau.clone(),
            timezone: chrono_tz::Tz::from_str(timezone_str).unwrap_or(chrono_tz::UTC),
            time_since_start_of_service_date: chrono::TimeDelta::new(time_since_start.into(), 0)
                .unwrap(),
            frequency: freq_converted.clone(),
            itinerary_id: trip.itinerary_pattern_id.clone(),
            direction_id: itinerary_meta
                .direction_pattern_id
                .clone()
                .unwrap_or_default(),
        };

        if let Some(service) = service_map
            .as_ref()
            .and_then(|sm| sm.get(trip.service_id.as_str()))
        {
            let dates = catenary::find_service_ranges(
                service,
                &t_to_find_schedule_for,
                departure_time_chrono,
                seek_back,
                seek_forward,
            );

            if !dates.is_empty() {
                let itin_options: Vec<ItinOption> = rows
                    .iter()
                    .map(|r| ItinOption {
                        arrival_time_since_start: r.arrival_time_since_start,
                        departure_time_since_start: r.departure_time_since_start,
                        interpolated_time_since_start: r.interpolated_time_since_start,
                        stop_id: CompactString::from(r.stop_id.as_str()),
                        gtfs_stop_sequence: r.gtfs_stop_sequence as u32,
                        trip_headsign: itinerary_meta.trip_headsign.clone(),
                        trip_headsign_translations: itinerary_meta
                            .trip_headsign_translations
                            .clone(),
                    })
                    .collect();

                for date in dates {
                    let t = ValidTripSet {
                        chateau_id: chateau.clone(),
                        trip_id: CompactString::from(trip.trip_id.as_str()),
                        timezone: chrono_tz::Tz::from_str(timezone_str).ok(),
                        frequencies: freq_converted.clone(),
                        trip_service_date: date.0,
                        itinerary_options: Arc::new(itin_options.clone()),
                        reference_start_of_service_date: date.1,
                        itinerary_pattern_id: trip.itinerary_pattern_id.clone(),
                        direction_pattern_id: itinerary_meta
                            .direction_pattern_id
                            .clone()
                            .unwrap_or_default(),
                        route_id: CompactString::from(itinerary_meta.route_id.as_str()),
                        trip_start_time: trip.start_time as u32,
                        trip_short_name: trip.trip_short_name.clone().map(CompactString::from),
                        service_id: trip.service_id.clone(),
                    };
                    valid_trips.entry(trip.trip_id.clone()).or_default().push(t);
                }
            }
        }
    }

    Some(ChateauIntermediate {
        chateau,
        valid_trips,
        routes,
        agencies,
        service_map,
        stop_to_key_map,
        stop_platform_map,
        stop_name_map,
        stop_full_info_map,
        stop_dist_map,
        direction_meta,
        departure_time: departure_time_chrono.timestamp(),
        long_distance_chateaux,
        query_point,
    })
}

// --- WebSocket Support ---

#[derive(Clone)]
pub struct NearbyDeparturesContext {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
}

pub async fn get_nearby_departures_stream(
    context: NearbyDeparturesContext,
    params: NearbyFromCoordsV3,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = (NearbyDeparturesV3Response, bool)> + Send>> {
    let start = Instant::now();
    let pool_arc = context.pool.clone();
    let conn_pool = pool_arc.as_ref();
    let db_connection_start = Instant::now();
    let mut conn = match conn_pool.get().await {
        Ok(c) => c,
        Err(_) => return Box::pin(futures::stream::iter(Vec::new())),
    };
    let db_connection_time = db_connection_start.elapsed();

    // 3. Fetch Nearby Stops
    let input_point = geo::Point::new(params.lon, params.lat);
    let radius = params.radius.unwrap_or(5000.0);
    let spatial_resolution =
        catenary::make_degree_length_as_distance_from_point(&input_point, radius);

    let where_query = format!(
        "ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
        params.lon, params.lat, spatial_resolution
    );

    let stops_fetch_start = Instant::now();
    let stops: Vec<catenary::models::Stop> = match catenary::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(&mut conn)
        .await
    {
        Ok(s) => s,
        Err(_) => return Box::pin(futures::stream::iter(Vec::new())),
    };
    let stops_fetch_time = stops_fetch_start.elapsed();

    // Default empty response helper
    let empty_resp = || NearbyDeparturesV3Response {
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
    };

    if stops.is_empty() {
        return Box::pin(futures::stream::iter(vec![(empty_resp(), false)]));
    }

    // 4. Group Stops & Sort
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

        // Distance filtering (reused)
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

    // Sort groups specifically for chunking
    let mut sorted_group_keys: Vec<(String, StationKey)> =
        station_group_metadata.keys().cloned().collect();
    sorted_group_keys.sort_by(|a, b| {
        let meta_a = station_group_metadata.get(a).unwrap();
        let meta_b = station_group_metadata.get(b).unwrap();
        meta_a.3.partial_cmp(&meta_b.3).unwrap()
    });

    let mut chunk1 = HashMap::new();
    let mut chunk2 = HashMap::new();

    // Chunking Logic
    for key in sorted_group_keys {
        let meta = station_group_metadata.get(&key).unwrap();
        let dist = meta.3;
        let should_be_in_chunk1 = dist < 1500.0;
        if should_be_in_chunk1 {
            if let Some(stops) = station_groups.get(&key) {
                chunk1.insert(key, stops.clone());
            }
        } else {
            if let Some(stops) = station_groups.get(&key) {
                chunk2.insert(key, stops.clone());
            }
        }
    }

    let shared_meta = Arc::new(station_group_metadata);
    let shared_dist = Arc::new(stop_dist_map);
    let shared_info = Arc::new(stop_full_info_map);

    let stream = futures::stream::iter(vec![chunk1, chunk2])
        .enumerate()
        .then(move |(idx, chunk)| {
            let context = context.clone();
            let params = params.clone();
            let meta = shared_meta.clone();
            let dist = shared_dist.clone();
            let info = shared_info.clone();
            let input_point = input_point.clone();

            async move {
                let chunk_intermediates = fetch_chunk_intermediates(
                    context.clone(),
                    params.clone(),
                    chunk,
                    meta.clone(),
                    dist.clone(),
                    info,
                    input_point,
                    db_connection_time,
                    stops_fetch_time,
                    start,
                )
                .await;

                // Etcd Connect (for hydration phase)
                let etcd_reuser = context.etcd_reuser.as_ref();
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
                    let new_client = etcd_client::Client::connect(
                        context.etcd_connection_ips.ip_addresses.as_slice(),
                        context.etcd_connection_options.as_ref().clone(),
                    )
                    .await;
                    if let Ok(c) = new_client {
                        etcd = Some(c);
                    }
                }
                let etcd_arc = Arc::new(etcd);

                let static_response = finalize_chunk_response(
                    &chunk_intermediates,
                    etcd_arc.clone(),
                    true,
                    params.limit_per_station.unwrap_or(10),
                    params.limit_per_headsign.unwrap_or(20),
                    start,
                    input_point,
                    meta.clone(),
                    dist.clone(),
                    context.pool.clone(),
                )
                .await;

                let mut results = vec![(static_response, false)];

                let skip_realtime = params.skip_realtime.unwrap_or(false);
                if !skip_realtime {
                    let hydrated_response = finalize_chunk_response(
                        &chunk_intermediates,
                        etcd_arc.clone(),
                        false,
                        params.limit_per_station.unwrap_or(10),
                        params.limit_per_headsign.unwrap_or(20),
                        start,
                        input_point,
                        meta,
                        dist,
                        context.pool.clone(),
                    )
                    .await;
                    results.push((hydrated_response, true));
                }

                futures::stream::iter(results)
            }
        })
        .flatten();

    Box::pin(stream)
}

async fn fetch_chunk_intermediates(
    context: NearbyDeparturesContext,
    params: NearbyFromCoordsV3,
    station_groups: HashMap<(String, StationKey), Vec<catenary::models::Stop>>,
    station_group_metadata: Arc<HashMap<(String, StationKey), (String, f64, f64, f64)>>,
    stop_dist_map: Arc<HashMap<String, f64>>,
    stop_full_info_map: Arc<HashMap<String, catenary::models::Stop>>,
    input_point: geo::Point<f64>,
    db_connection_time: std::time::Duration,
    stops_fetch_time: std::time::Duration,
    total_start_time: Instant,
) -> Vec<Option<ChateauIntermediate>> {
    if station_groups.is_empty() {
        return Vec::new();
    }

    let departure_time = params
        .departure_time
        .unwrap_or_else(|| catenary::duration_since_unix_epoch().as_secs());
    let departure_time_chrono = chrono::Utc.timestamp_opt(departure_time as i64, 0).unwrap();

    let long_distance_chateaux: HashSet<&'static str> = [
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

    // Group stops by chateau for parallel fetch
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
        let pool = context.pool.clone();
        let stop_ids_vec: Vec<String> = stop_ids.into_iter().collect();
        let dep_time = departure_time_chrono;
        let c_clone = chateau.clone();
        let ld_arc_clone = long_distance_chateaux_arc.clone();
        let input_point_clone = input_point.clone();

        // Optimized stopping list construction
        let mut relevant_stops = Vec::new();
        for ((c, _), s_vec) in &station_groups {
            if c == &chateau {
                relevant_stops.extend(s_vec.clone());
            }
        }

        let stop_to_key_map: HashMap<(String, String), StationKey> = relevant_stops
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
        let stop_platform_map: HashMap<String, Option<String>> = relevant_stops
            .iter()
            .map(|s| (s.gtfs_id.clone(), s.platform_code.clone()))
            .collect();
        let stop_name_map: HashMap<String, Option<String>> = relevant_stops
            .iter()
            .map(|s| (s.gtfs_id.clone(), s.name.clone()))
            .collect();

        // Use maps from Arc
        let full_info_map_unwrap = stop_full_info_map.as_ref().clone();
        let stop_dist_map_unwrap = stop_dist_map.as_ref().clone();

        chateau_futures.push(async move {
            fetch_chateau_intermediate(
                pool,
                c_clone,
                stop_ids_vec,
                dep_time,
                ld_arc_clone,
                stop_to_key_map,
                stop_platform_map,
                stop_name_map,
                full_info_map_unwrap,
                stop_dist_map_unwrap,
                input_point_clone,
            )
            .await
        });
    }

    let pipeline_results: Vec<_> = futures::stream::iter(chateau_futures)
        .buffer_unordered(32)
        .collect()
        .await;

    pipeline_results
}

async fn render_chateau_response(
    intermediate: ChateauIntermediate,
    etcd_arc: Arc<Option<etcd_client::Client>>,
    skip_realtime: bool,
) -> (
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
) {
    let mut rt_data: Option<catenary::aspen::lib::TripsSelectionResponse> = None;
    let mut rt_alerts: BTreeMap<String, catenary::aspen_dataset::AspenisedAlert> = BTreeMap::new();
    let chateau = intermediate.chateau.clone();

    if !skip_realtime {
        // Collect trip IDs
        let mut route_ids = HashSet::new();
        for trip_set in intermediate.valid_trips.values() {
            for trip in trip_set {
                route_ids.insert(trip.route_id.clone());
            }
        }

        if let Some(etcd) = etcd_arc.as_ref() {
            if !route_ids.is_empty() {
                let mut etcd_clone = etcd.clone();
                if let Ok(resp) = etcd_clone
                    .get(format!("/aspen_assigned_chateaux/{}", chateau), None)
                    .await
                {
                    if let Some(kv) = resp.kvs().first() {
                        if let Ok(meta) =
                            catenary::bincode_deserialize::<ChateauMetadataEtcd>(kv.value())
                        {
                            if let Ok(client) =
                                catenary::aspen::lib::spawn_aspen_client_from_ip(&meta.socket).await
                            {
                                let (t, a) = tokio::join!(
                                    client.get_all_trips_with_route_ids(
                                        tarpc::context::current(),
                                        chateau.clone(),
                                        route_ids.into_iter().map(|x| x.into()).collect()
                                    ),
                                    client
                                        .get_all_alerts(tarpc::context::current(), chateau.clone())
                                );
                                if let Ok(Some(tr)) = t {
                                    rt_data = Some(tr);
                                }
                                if let Ok(Some(al)) = a {
                                    rt_alerts = al.into_iter().collect();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> =
        HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> =
        HashMap::new();
    let mut local_route_meta_map: HashMap<LocalRouteKey, (catenary::models::Route, String)> =
        HashMap::new();
    let mut route_info_map: HashMap<String, RouteInfoExport> = HashMap::new();
    let mut stop_output_map: HashMap<String, StopOutputV3> = HashMap::new();

    let alert_index = AlertIndex::new(&rt_alerts);
    let relevant_stop_ids: HashSet<String> = intermediate
        .stop_to_key_map
        .keys()
        .filter(|(c, _)| c == &chateau)
        .map(|(_, sid)| sid.clone())
        .collect();

    // Iterate over valid_trips
    for (trip_id, trip_set_list) in intermediate.valid_trips {
        for valid_trip in trip_set_list {
            let route_id = &valid_trip.route_id;
            let route = match intermediate.routes.get(route_id.as_str()) {
                Some(r) => r,
                None => continue,
            };

            // Category
            let mut is_long_distance = false;
            if route.route_type == 2
                && intermediate
                    .long_distance_chateaux
                    .contains(chateau.as_str())
            {
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

            for row in valid_trip.itinerary_options.iter() {
                if !relevant_stop_ids.contains(row.stop_id.as_str()) {
                    continue;
                }

                if let Some(station_key) = intermediate
                    .stop_to_key_map
                    .get(&(chateau.clone(), row.stop_id.to_string()))
                {
                    let dep_time_offset = row
                        .departure_time_since_start
                        .or(row.arrival_time_since_start)
                        .or(row.interpolated_time_since_start)
                        .unwrap_or(0);

                    let is_last_stop = false;
                    let date = valid_trip.trip_service_date;
                    let midnight_ts = valid_trip.reference_start_of_service_date.timestamp();

                    let departure_ts =
                        midnight_ts + valid_trip.trip_start_time as i64 + dep_time_offset as i64;

                    if departure_ts < (intermediate.departure_time as i64)
                        || departure_ts > (intermediate.departure_time as i64 + 18 * 3600)
                    {
                        continue;
                    }

                    let headsign = row
                        .trip_headsign
                        .clone()
                        .or(intermediate
                            .direction_meta
                            .get(&valid_trip.direction_pattern_id)
                            .map(|dm| dm.headsign_or_destination.clone()))
                        .unwrap_or_else(|| "Unknown".to_string());

                    let agency_name = route.agency_id.clone().and_then(|aid| {
                        intermediate
                            .agencies
                            .get(&aid)
                            .map(|a| a.agency_name.clone())
                    });

                    let mut rt_dep = None;
                    let mut rt_arr = None;
                    let mut rt_platform = None;
                    let mut is_cancelled = false;
                    let mut is_delayed = false;

                    // Alerts
                    let relevant_alerts = alert_index.search(route_id.as_str(), trip_id.as_str());
                    for alert in relevant_alerts {
                        if alert.effect == Some(1) {
                            let is_active = alert.active_period.iter().any(|ap| {
                                let start = ap.start.unwrap_or(0);
                                let end = ap.end.unwrap_or(u64::MAX);
                                (departure_ts as u64) >= start && (departure_ts as u64) <= end
                            });
                            if is_active {
                                is_cancelled = true;
                            }
                        }
                    }

                    // Trip Updates
                    let mut active_update: Option<&catenary::aspen_dataset::AspenisedTripUpdate> =
                        None;
                    if let Some(data) = &rt_data {
                        let empty_service_map = BTreeMap::new();
                        let service_map_ref = intermediate
                            .service_map
                            .as_ref()
                            .unwrap_or(&empty_service_map);

                        if let Some(update_ids) =
                            data.trip_id_to_trip_update_ids.get(trip_id.as_str())
                        {
                            for uid in update_ids {
                                if let Some(u) = data.trip_updates.get(uid) {
                                    if estimate_service_date(
                                        &valid_trip,
                                        u,
                                        row,
                                        service_map_ref,
                                        &chateau,
                                    ) {
                                        active_update = Some(u);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(update) = active_update {
                        if let Some(stu) = update.stop_time_update.iter().find(|s| {
                            s.stop_sequence == Some(row.gtfs_stop_sequence as u16)
                                || s.stop_id
                                    .as_ref()
                                    .map(|sid| {
                                        catenary::stop_matching::rt_stop_matches_scheduled_simple(
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
                    }

                    if let Some(d) = rt_dep {
                        if d > (departure_ts as u64 + 60) {
                            is_delayed = true;
                        }
                    }
                    let display_platform = rt_platform.or_else(|| {
                        intermediate
                            .stop_platform_map
                            .get(row.stop_id.as_str())
                            .cloned()
                            .flatten()
                    });

                    if !route_info_map.contains_key(route_id.as_str()) {
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
                                    + valid_trip.trip_start_time as i64
                                    + row.arrival_time_since_start.unwrap_or(dep_time_offset)
                                        as i64) as u64,
                            ),
                            realtime_arrival: rt_arr,
                            service_date: date,
                            headsign: headsign,
                            platform: display_platform.clone(),
                            trip_id: trip_id.to_string(),
                            trip_short_name: valid_trip
                                .trip_short_name
                                .clone()
                                .map(|x| x.to_string()),
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
                                    + valid_trip.trip_start_time as i64
                                    + row.arrival_time_since_start.unwrap_or(dep_time_offset)
                                        as i64) as u64,
                            ),
                            arrival_realtime: rt_arr,
                            stop_id: CompactString::from(row.stop_id.as_str()),
                            stop_name: intermediate
                                .stop_name_map
                                .get(row.stop_id.as_str())
                                .cloned()
                                .flatten(),
                            cancelled: is_cancelled,
                            platform: display_platform,
                            service_date: date,
                            last_stop: is_last_stop,
                        };
                        local_departures
                            .entry(r_key)
                            .or_default()
                            .entry(headsign)
                            .or_default()
                            .push(item);

                        if !stop_output_map.contains_key(row.stop_id.as_str()) {
                            if let Some(s) =
                                intermediate.stop_full_info_map.get(row.stop_id.as_str())
                            {
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

    // Filter closest stop for Local
    for (_route_key, headsigns_map) in local_departures.iter_mut() {
        for (_headsign, items) in headsigns_map.iter_mut() {
            if items.is_empty() {
                continue;
            }
            let mut min_dist = f64::MAX;
            let mut best_stop_id: Option<CompactString> = None;
            for item in items.iter() {
                if let Some(dist) = intermediate.stop_dist_map.get(item.stop_id.as_str()) {
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

    (
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
    )
}

async fn finalize_chunk_response(
    intermediates: &[Option<ChateauIntermediate>],
    etcd_arc: Arc<Option<etcd_client::Client>>,
    skip_realtime: bool,
    limit_per_station: usize,
    limit_per_headsign: usize,
    start_time: Instant,
    input_point: geo::Point<f64>,
    station_group_metadata: Arc<HashMap<(String, StationKey), (String, f64, f64, f64)>>,
    stop_dist_map: Arc<HashMap<String, f64>>,
    context_pool: Arc<CatenaryPostgresPool>,
) -> NearbyDeparturesV3Response {
    let mut ld_departures_by_group: HashMap<(String, StationKey), Vec<DepartureItem>> =
        HashMap::new();
    let mut local_departures: HashMap<LocalRouteKey, HashMap<String, Vec<LocalDepartureItem>>> =
        HashMap::new();
    let mut local_route_meta: HashMap<LocalRouteKey, (catenary::models::Route, String)> =
        HashMap::new();
    let mut routes_output: HashMap<String, HashMap<String, RouteInfoExport>> = HashMap::new();
    let mut stops_output: HashMap<String, HashMap<String, StopOutputV3>> = HashMap::new();

    for inter_opt in intermediates {
        if let Some(inter) = inter_opt {
            let (l_ld, l_loc, l_rt, l_st) =
                render_chateau_response(inter.clone(), etcd_arc.clone(), skip_realtime).await;

            for (k, v) in l_ld {
                ld_departures_by_group.entry(k).or_default().extend(v);
            }
            for (k, (r, a, h_map)) in l_loc {
                local_route_meta.entry(k.clone()).or_insert((r, a));
                for (h, items) in h_map {
                    local_departures
                        .entry(k.clone())
                        .or_default()
                        .entry(h)
                        .or_default()
                        .extend(items);
                }
            }
            routes_output
                .entry(inter.chateau.clone())
                .or_default()
                .extend(l_rt);
            stops_output
                .entry(inter.chateau.clone())
                .or_default()
                .extend(l_st);
        }
    }

    // Finalize LD
    let mut ld_output = Vec::new();
    for ((chateau, key), deps) in &mut ld_departures_by_group {
        if let Some(meta) = station_group_metadata.get(&(chateau.clone(), key.clone())) {
            deps.sort_by_key(|d| d.scheduled_departure.unwrap_or(0));
            deps.truncate(limit_per_station);

            let (osm_id, _rep_id) = match key {
                StationKey::Osm(id) => (Some(*id), "".to_string()),
                StationKey::Parent(id) => (None, id.clone()),
                StationKey::Stop(id) => (None, id.clone()),
            };
            let timezone =
                tz_search::lookup(meta.1, meta.2).unwrap_or_else(|| String::from("Etc/GMT"));
            ld_output.push(StationDepartureGroupExport {
                station_name: meta.0.clone(),
                osm_station_id: osm_id,
                distance_m: meta.3,
                departures: deps.clone(),
                lat: meta.1,
                lon: meta.2,
                timezone,
            });
        }
    }

    // OSM Fetching
    let osm_ids_to_fetch: Vec<i64> = ld_output.iter().filter_map(|g| g.osm_station_id).collect();
    let mut osm_station_info_map = HashMap::new();
    if !osm_ids_to_fetch.is_empty() {
        if let Ok(mut conn) = context_pool.get().await {
            if let Ok(data) = catenary::schema::gtfs::osm_stations::dsl::osm_stations
                .filter(catenary::schema::gtfs::osm_stations::osm_id.eq_any(&osm_ids_to_fetch))
                .load::<catenary::models::OsmStation>(&mut conn)
                .await
            {
                for s in data {
                    osm_station_info_map.insert(s.osm_id, s);
                }
            }
        }
    }

    // Merge OSM
    let mut merged_ld_output: Vec<StationDepartureGroupExport> =
        Vec::with_capacity(ld_output.len());
    let mut osm_station_map: HashMap<i64, usize> = HashMap::new();
    for group in ld_output {
        if let Some(osm_id) = group.osm_station_id {
            if let Some(&idx) = osm_station_map.get(&osm_id) {
                merged_ld_output[idx].departures.extend(group.departures);
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
    let mut ld_sorted = merged_ld_output;
    ld_sorted.sort_by(|a, b| a.distance_m.partial_cmp(&b.distance_m).unwrap());

    // Local Finalize
    let mut local_output = Vec::new();
    let has_rail = ld_sorted.iter().any(|g| g.osm_station_id.is_some());
    let many_stops = station_group_metadata.len() > 50;
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

    NearbyDeparturesV3Response {
        long_distance: ld_sorted,
        local: local_output,
        routes: routes_output,
        stops: stops_output,
        debug: NearbyDeparturesDebug {
            total_time_ms: start_time.elapsed().as_millis(),
            db_connection_time_ms: 0,
            stops_fetch_time_ms: 0,
            etcd_connection_time_ms: 0,
            pipeline_processing_time_ms: 0,
        },
    }
}
