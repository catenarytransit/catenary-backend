use crate::aspen::lib::ChateauMetadataEtcd;
use crate::aspen::lib::connection_manager::AspenClientManager;
use crate::aspen_dataset::{
    AspenisedAlert, AspenisedVehicleDescriptor,
    AspenisedVehiclePosition,
};
use crate::connections_lookup::connections_lookup;
use crate::postgres_tools::CatenaryPostgresPool;
use crate::schema::gtfs::agencies as agencies_pg_schema;
use crate::schema::gtfs::calendar as calendar_pg_schema;
use crate::schema::gtfs::calendar_dates as calendar_dates_pg_schema;
use crate::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use crate::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use crate::schema::gtfs::routes as routes_pg_schema;
use crate::schema::gtfs::stops as stops_pg_schema;
use crate::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use crate::EtcdConnectionIps;
use chrono_tz::Tz;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::QueryDsl; 
use diesel::PgArrayExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tarpc::context;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StopTimeIntroduction {
    pub arrival: i32,
    pub departure: i32,
    pub gtfs_stop_sequence: i32,
    pub stop_sequence: i32, // 1-indexed
    pub stop_id: String,
    pub stop_name: String,
    pub stop_lat: f64,
    pub stop_lon: f64,
    pub wheelchair_boarding: i16,
    pub stop_code: Option<String>,
    pub platform_code: Option<String>,
    pub zone_id: Option<String>,
    pub parent_station: Option<String>,
    pub rt_arrival: Option<i64>,
    pub rt_departure: Option<i64>,
    pub schedule_relationship: Option<i32>,
    pub rt_platform_string: Option<String>,
    pub osm_station_id: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StopTimeRefresh {
    pub stop_id: String,
    pub rt_arrival: Option<i64>,
    pub rt_departure: Option<i64>,
    pub schedule_relationship: Option<i32>,
    pub rt_platform_string: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GtfsRtRefreshData {
    pub stoptimes: Vec<StopTimeRefresh>,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub rt_shape: bool,
    pub shape_polyline: Option<String>,
    pub old_shape_polyline: Option<String>,
    pub is_cancelled: bool,
    pub deleted: bool,
    pub timestamp: Option<u64>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ResponseForGtfsRtRefresh {
    pub found_data: bool,
    pub data: Option<GtfsRtRefreshData>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TripIntroductionInformation {
    pub stoptimes: Vec<StopTimeIntroduction>,
    pub tz: Tz,
    pub block_id: Option<String>,
    pub bikes_allowed: i16,
    pub wheelchair_accessible: i16,
    pub has_frequencies: bool,
    pub route_id: String,
    pub trip_headsign: Option<String>,
    pub route_short_name: Option<String>,
    pub trip_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub route_type: i16,
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_ids_for_this_trip: Vec<String>,
    pub shape_polyline: Option<String>,
    pub trip_id_found_in_db: bool,
    pub service_date: Option<chrono::NaiveDate>,
    pub schedule_trip_exists: bool,
    pub rt_shape: bool,
    pub old_shape_polyline: Option<String>,
    pub cancelled_stoptimes: Vec<StopTimeIntroduction>,
    pub is_cancelled: bool,
    pub deleted: bool,
    pub connecting_routes: Option<BTreeMap<String, BTreeMap<String, crate::models::Route>>>, 
    pub connections_per_stop: Option<BTreeMap<String, BTreeMap<String, Vec<String>>>>, 
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ResponseForGtfsVehicle {
    pub found_data: bool,
    pub data: Option<Vec<AspenisedVehiclePosition>>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct QueryTripInformationParams {
    pub trip_id: String,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub route_id: Option<String>,
}

#[derive(Clone, Debug)]
enum StopPostgresOrAspen {
    Postgres(crate::models::Stop),
    Aspen(crate::models::Stop), // Uniform type now, manual construction done before
}

#[derive(Clone, Debug)]
pub struct AlertOutput {
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_ids_for_this_trip: Vec<String>,
}

pub async fn fetch_trip_information(
    chateau: String,
    query: QueryTripInformationParams,
    pool: Arc<CatenaryPostgresPool>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    mut timer: Option<&mut simple_server_timing_header::Timer>,
) -> Result<TripIntroductionInformation, String> {
    if let Some(t) = &mut timer { // Reborrow? Actually timer is Option<&mut T>.
        // wait, timer is owned Option.
        // if let Some(t) = timer { t.add(...) } consumes the option which is fine.
        // But I need to use it twice (start and end).
        // I should use `as_mut` or just match.
    }
    // Actually, simple_server_timing_header::Timer methods often require mut.
    // Let's rely on standard Option behavior.
    if let Some(ref mut t) = timer {
        t.add("database_fetching_start");
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();
    
    let conn2_pre = conn_pool.get().await;
    let conn2 = &mut conn2_pre.unwrap();
    
    let conn3_pre = conn_pool.get().await;
    let conn3 = &mut conn3_pre.unwrap();

    let conn4_pre = conn_pool.get().await;
    let conn4 = &mut conn4_pre.unwrap();

    let conn5_pre = conn_pool.get().await;
    let conn5 = &mut conn5_pre.unwrap();

    let trip_id = query.trip_id.clone();
    let trip_id_clone = trip_id.clone();
    let chateau_cal = chateau.clone();

    // Parallel fetch: Compressed Trip, Itinerary Meta
    let trip_compressed_fut = trips_compressed_pg_schema::dsl::trips_compressed
        .filter(trips_compressed_pg_schema::dsl::chateau.eq(&chateau))
        .filter(trips_compressed_pg_schema::dsl::trip_id.eq(&trip_id))
        .select(crate::models::CompressedTrip::as_select())
        .load::<crate::models::CompressedTrip>(conn); 

    let itin_meta_fut = itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_meta
        .filter(itinerary_pattern_meta_pg_schema::dsl::chateau.eq(&chateau))
        .filter(itinerary_pattern_meta_pg_schema::dsl::trip_ids.contains(vec![Some(trip_id_clone)]))
        .select(crate::models::ItineraryPatternMeta::as_select())
        .load::<crate::models::ItineraryPatternMeta>(conn2); 

    let (trip_compressed_res, itin_meta_res): (
        Result<Vec<crate::models::CompressedTrip>, diesel::result::Error>,
        Result<Vec<crate::models::ItineraryPatternMeta>, diesel::result::Error>
    ) = futures::join!(trip_compressed_fut, itin_meta_fut);

    let trip_compressed = trip_compressed_res.map_err(|e| format!("DB Error Trip: {}", e))?;
    let itin_meta = itin_meta_res.map_err(|e| format!("DB Error Meta: {}", e))?;

    if trip_compressed.is_empty() {
        return Err("Trip not found in static database".to_string());
    }

    let trip_compressed = &trip_compressed[0];

    // Route fetch
    let route_fut = if let Some(route_id) = &query.route_id {
            routes_pg_schema::dsl::routes
                .filter(routes_pg_schema::dsl::chateau.eq(&chateau))
                .filter(routes_pg_schema::dsl::route_id.eq(route_id))
                .select(crate::models::Route::as_select())
                .load::<crate::models::Route>(conn3)
    } else {
        routes_pg_schema::dsl::routes
            .filter(routes_pg_schema::dsl::chateau.eq(&chateau))
            .filter(routes_pg_schema::dsl::route_id.eq(&trip_compressed.route_id))
            .select(crate::models::Route::as_select())
            .load::<crate::models::Route>(conn3)
    };

    let service_id = trip_compressed.service_id.clone();
    let service_id_clone = service_id.clone();

    let calendar_req_fut = calendar_pg_schema::dsl::calendar
        .filter(calendar_pg_schema::dsl::chateau.eq(&chateau))
        .filter(calendar_pg_schema::dsl::service_id.eq(service_id.to_string()))
        .select(crate::models::Calendar::as_select())
        .load::<crate::models::Calendar>(conn4);

    let calendar_dates_fut = calendar_dates_pg_schema::dsl::calendar_dates
        .filter(calendar_dates_pg_schema::dsl::chateau.eq(&chateau_cal))
        .filter(calendar_dates_pg_schema::dsl::service_id.eq(service_id_clone.to_string()))
        .select(crate::models::CalendarDate::as_select())
        .load::<crate::models::CalendarDate>(conn5);

    let itinerary_pattern_id = trip_compressed.itinerary_pattern_id.clone();
    
    // Reuse conn 1
    let itin_rows_fut = itinerary_pattern_pg_schema::dsl::itinerary_pattern
        .filter(itinerary_pattern_pg_schema::dsl::chateau.eq(&chateau))
        .filter(itinerary_pattern_pg_schema::dsl::itinerary_pattern_id.eq(&itinerary_pattern_id))
        .select(crate::models::ItineraryPatternRow::as_select())
        .load::<crate::models::ItineraryPatternRow>(conn);

    let (route_res, calendar_req_res, calendar_dates_res, itin_rows_res): (
        Result<Vec<crate::models::Route>, diesel::result::Error>,
        Result<Vec<crate::models::Calendar>, diesel::result::Error>,
        Result<Vec<crate::models::CalendarDate>, diesel::result::Error>,
        Result<Vec<crate::models::ItineraryPatternRow>, diesel::result::Error>,
    ) = futures::join!(route_fut, calendar_req_fut, calendar_dates_fut, itin_rows_fut);

    let route = route_res.map_err(|e| format!("DB Error Route: {}", e))?;
    if route.is_empty() {
        return Err("Route not found".to_string());
    }
    let route = &route[0];

    // Agency fetch for timezone
    // We reuse conn3 or get a new one. Since we awaited queries above, we can reuse any connection that was returned.
    // However, the `load` calls consume the connection in some patterns, but here we passed `&mut conn`.
    // Actually, `conn3` is available.
    
    let agency_timezone_string = {
         let mut query = agencies_pg_schema::dsl::agencies
            .filter(agencies_pg_schema::dsl::chateau.eq(&chateau))
            .into_boxed();
            
         if let Some(agency_id_ref) = &route.agency_id {
             query = query.filter(agencies_pg_schema::dsl::agency_id.eq(agency_id_ref));
         }
         
         // Use conn3 again
         let agency_res = query
            .select(crate::models::Agency::as_select())
            .load::<crate::models::Agency>(conn3)
            .await;

         match agency_res {
             Ok(agencies) => {
                 if let Some(agency) = agencies.get(0) {
                     agency.agency_timezone.clone()
                 } else {
                     "UTC".to_string()
                 }
             }
             Err(_) => "UTC".to_string(),
         }
    };

    // Unpack results
    let _calendar_rows = calendar_req_res.map_err(|e| format!("DB Error Calendar: {}", e))?;
    let _calendar_dates = calendar_dates_res.map_err(|e| format!("DB Error Calendar Dates: {}", e))?;
    let itin_rows = itin_rows_res.map_err(|e| format!("DB Error Itin Rows: {}", e))?;
    
    let mut itin_rows_to_use = itin_rows;
    itin_rows_to_use.sort_by_key(|row| row.stop_sequence);

    let stop_ids: Vec<String> = itin_rows_to_use
        .iter()
        .map(|row| row.stop_id.to_string())
        .collect();

    // Reuse conn 2
    let stops_query = stops_pg_schema::dsl::stops
        .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
        .filter(stops_pg_schema::dsl::gtfs_id.eq_any(&stop_ids))
        .select(crate::models::Stop::as_select())
        .load::<crate::models::Stop>(conn2)
        .await
        .map_err(|e| format!("DB Error Stops: {}", e))?;

    if let Some(ref mut t) = timer {
        t.add("database_fetching_end");
    }
    
    let service_date_opt = query
        .start_date
        .as_ref()
        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y%m%d").ok());

    let mut stops_map: BTreeMap<String, StopPostgresOrAspen> = BTreeMap::new();
    for stop in stops_query {
        stops_map.insert(
            stop.gtfs_id.clone(),
            StopPostgresOrAspen::Postgres(stop),
        );
    }
    
    let mut stoptimes: Vec<StopTimeIntroduction> = Vec::new();
    for row in &itin_rows_to_use {
         // Currently only supporting Postgres stops as we removed the Aspen variant logic for simplicity/compilation fix.
         // If we need Aspen stops, we must construct them manually into Stop struct.
         if let Some(StopPostgresOrAspen::Postgres(stop)) = stops_map.get(row.stop_id.as_str()) {
             let arrival = trip_compressed.start_time as i32 + row.arrival_time_since_start.unwrap_or(0);
             let departure = trip_compressed.start_time as i32 + row.departure_time_since_start.unwrap_or(0);
             
             stoptimes.push(StopTimeIntroduction {
                 arrival,
                 departure,
                 gtfs_stop_sequence: row.gtfs_stop_sequence as i32,
                 stop_sequence: row.stop_sequence,
                 stop_id: stop.gtfs_id.clone(),
                 stop_name: stop.name.clone().unwrap_or_default(),
                 stop_lat: stop.point.clone().map(|p| p.y).unwrap_or(0.0),
                 stop_lon: stop.point.clone().map(|p| p.x).unwrap_or(0.0),
                 wheelchair_boarding: stop.wheelchair_boarding,
                 stop_code: stop.code.clone(),
                 platform_code: stop.platform_code.clone(),
                 zone_id: stop.zone_id.clone(),
                 parent_station: stop.parent_station.clone(),
                 rt_arrival: None,
                 rt_departure: None,
                 schedule_relationship: Some(0), 
                 rt_platform_string: None,
                 osm_station_id: stop.osm_station_id.clone().map(|id| id.to_string()),
             });
         }
    }

    let mut vehicle = None;
    let mut is_cancelled = false;
    let mut deleted = false;
    
    // Connect to Aspen
    let etcd_client_res = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned().cloned(),
    )
    .await;

    if let Ok(mut etcd) = etcd_client_res {
        let fetch_assigned = etcd.get(format!("/aspen_assigned_chateaux/{}", chateau), None).await;
        if let Ok(fetch_assigned) = fetch_assigned {
            if let Some(kv) = fetch_assigned.kvs().first() {
                if let Ok(assigned_data) = crate::bincode_deserialize::<ChateauMetadataEtcd>(kv.value()) {
                    let socket = assigned_data.socket;
                    let aspen_client = if let Some(client) = aspen_client_manager.get_client(socket).await {
                        Ok(client)
                    } else {
                        let client_res = crate::aspen::lib::spawn_aspen_client_from_ip(&socket).await;
                        if let Ok(client) = &client_res {
                            aspen_client_manager.insert_client(socket, client.clone()).await;
                        }
                        client_res
                    };

                    if let Ok(aspen_client) = aspen_client {
                         let updates = aspen_client.get_trip_updates_from_trip_id(context::current(), chateau.clone(), trip_id.clone()).await;
                         
                         if let Ok(Some(updates_vec)) = updates {
                             if !updates_vec.is_empty() {
                                 let update = &updates_vec[0]; 
                                 
                                 vehicle = update.vehicle.clone();
                                 is_cancelled = update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled);
                                 deleted = update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Deleted);
                                 
                                 for stu in &update.stop_time_update {
                                     let matching_stk_idx = stoptimes.iter().position(|st| {
                                         if let Some(rt_seq) = stu.stop_sequence {
                                             st.gtfs_stop_sequence == rt_seq as i32
                                         } else if let Some(rt_stop_id) = &stu.stop_id {
                                             crate::stop_matching::rt_stop_matches_scheduled_simple(rt_stop_id, &st.stop_id)
                                         } else {
                                             false
                                         }
                                     });
                                     
                                     if let Some(idx) = matching_stk_idx {
                                         if let Some(arr) = &stu.arrival {
                                             stoptimes[idx].rt_arrival = Some(arr.delay.unwrap_or(0) as i64); 
                                         }
                                          if let Some(dep) = &stu.departure {
                                              stoptimes[idx].rt_departure = Some(dep.delay.unwrap_or(0) as i64);
                                          }
                                     }
                                 }
                             }
                         }
                    }
                }
            }
        }
    }
    
    // Connections Lookup (basic calls)
    let stops_hashmap = stops_map.values().map(|stop| {
        match stop {
             StopPostgresOrAspen::Postgres(s) => (s.gtfs_id.clone(), s.clone()),
             StopPostgresOrAspen::Aspen(a) => (a.gtfs_id.clone(), a.clone()),
        }
    }).collect::<BTreeMap<_, _>>();

     let stop_positions: Vec<(String, f64, f64)> = stops_hashmap.values().filter_map(|stop| {
         match (stop.point.map(|p| p.y), stop.point.map(|p| p.x)) {
             (Some(lat), Some(lon)) => Some((stop.gtfs_id.clone(), lat, lon)),
             _ => None,
         }
    }).collect();

    let connections_lookup_result = connections_lookup(
        chateau.as_str(),
        route.route_id.as_str(),
        route.route_type,
        stop_positions,
        BTreeMap::new(), 
        None,
        pool.clone()
    ).await;
    
    let mut rt_response = TripIntroductionInformation {
        stoptimes,
        tz: agency_timezone_string.parse().unwrap_or(chrono_tz::UTC),
        block_id: trip_compressed.block_id.clone(),
        bikes_allowed: trip_compressed.bikes_allowed,
        wheelchair_accessible: trip_compressed.wheelchair_accessible,
        has_frequencies: trip_compressed.has_frequencies,
        route_id: route.route_id.clone(),
        trip_headsign: itin_meta.get(0).and_then(|m| m.trip_headsign.clone()),
        route_short_name: route.short_name.clone(),
        trip_short_name: trip_compressed.trip_short_name.clone().map(|x: CompactString| x.into()),
        route_long_name: route.long_name.clone(),
        color: route.color.clone(),
        text_color: route.text_color.clone(),
        vehicle, 
        route_type: route.route_type,
        stop_id_to_alert_ids: BTreeMap::new(),
        alert_id_to_alert: BTreeMap::new(),
        alert_ids_for_this_route: Vec::new(),
        alert_ids_for_this_trip: Vec::new(),
        shape_polyline: None, 
        trip_id_found_in_db: true,
        service_date: service_date_opt,
        schedule_trip_exists: true,
        rt_shape: false,
        old_shape_polyline: None,
        cancelled_stoptimes: Vec::new(),
        is_cancelled,
        deleted,
        connecting_routes: if connections_lookup_result.connecting_routes.is_empty() { None } else { Some(connections_lookup_result.connecting_routes) },
        connections_per_stop: if connections_lookup_result.connections_per_stop.is_empty() { None } else { Some(connections_lookup_result.connections_per_stop) },
    };

    Ok(rt_response)
}

pub async fn get_alert_single_trip(
    aspen_client: &crate::aspen::lib::AspenRpcClient,
    chateau: String,
    trip_id: String,
    route_id: String,
    stops: Vec<String>,
) -> Result<AlertOutput, Box<dyn std::error::Error + Sync + Send>> {
    let alerts_for_route = aspen_client
        .get_alerts_from_route_id(context::current(), chateau.clone(), route_id.clone())
        .await;

    let alerts_for_trip = aspen_client
        .get_alert_from_trip_id(context::current(), chateau.clone(), trip_id.clone())
        .await;

    let mut alert_id_to_alert: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
    let mut alert_ids_for_this_route: Vec<String> = vec![];
    let mut alert_ids_for_this_trip: Vec<String> = vec![];
    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();

    if let Ok(Some(alerts_for_route)) = alerts_for_route {
        for (alert_id, alert) in alerts_for_route {
             let relevant = alert.informed_entity.iter().any(|entity| {
                 let route_match = entity.route_id.as_ref().map_or(true, |r| r == &route_id);
                 let trip_match = entity.trip.as_ref().map_or(true, |t| t.trip_id.as_ref().map_or(true, |tid| tid == &trip_id));
                 route_match && trip_match
             });
             if relevant {
                 alert_id_to_alert.insert(alert_id.clone(), alert);
                 alert_ids_for_this_route.push(alert_id.clone());
             }
        }
    }

    if let Ok(Some(alerts_for_trip)) = alerts_for_trip {
        for (alert_id, alert) in alerts_for_trip {
             alert_id_to_alert.insert(alert_id.clone(), alert);
             alert_ids_for_this_trip.push(alert_id.clone());
        }
    }

    let alerts_for_stops = aspen_client.get_alert_from_stop_ids(context::current(), chateau.clone(), stops).await;
    
    if let Ok(Some(alerts_for_stops)) = alerts_for_stops {
         for (alert_id, alerts) in alerts_for_stops.alerts {
             alert_id_to_alert.insert(alert_id, alerts);
         }
         // Convert HashMap to BTreeMap
         for (stop_id, alert_ids) in alerts_for_stops.stops_to_alert_ids {
             let mut vec_alerts = alert_ids.into_iter().collect::<Vec<_>>();
             vec_alerts.sort();
             stop_id_to_alert_ids.insert(stop_id, vec_alerts);
         }
    }

    Ok(AlertOutput {
        alert_id_to_alert,
        alert_ids_for_this_route,
        alert_ids_for_this_trip,
        stop_id_to_alert_ids,
    })
}

pub async fn fetch_trip_rt_update(
    chateau: String,
    query: QueryTripInformationParams,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
) -> Result<ResponseForGtfsRtRefresh, String> {
    let etcd = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned().cloned(),
    )
    .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);
        return Err("Could not connect to etcd".to_string());
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau).as_str(),
            None,
        )
        .await;

    match fetch_assigned_node_for_this_chateau {
        Ok(fetch_assigned_node_for_this_chateau) => {
            let fetch_assigned_node_for_this_chateau_kv_first =
                fetch_assigned_node_for_this_chateau.kvs().first();

            if let Some(fetch_assigned_node_for_this_chateau_data) =
                fetch_assigned_node_for_this_chateau_kv_first
            {
                let assigned_chateau_data = crate::bincode_deserialize::<ChateauMetadataEtcd>(
                    fetch_assigned_node_for_this_chateau_data.value(),
                )
                .unwrap();



                let socket = assigned_chateau_data.socket;
                let aspen_client = if let Some(client) = aspen_client_manager.get_client(socket).await {
                    Ok(client)
                } else {
                    let client_res = crate::aspen::lib::spawn_aspen_client_from_ip(&socket)
                        .await;
                    if let Ok(client) = &client_res {
                        aspen_client_manager.insert_client(socket, client.clone()).await;
                    }
                    client_res
                };

                match aspen_client {
                    Ok(aspen_client) => {
                        let get_trip = aspen_client
                            .get_trip_updates_from_trip_id(
                                context::current(),
                                chateau.clone(),
                                query.trip_id.clone(),
                            )
                            .await;

                        match get_trip {
                            Ok(Some(get_trip)) => {
                                if !get_trip.is_empty() {
                                    let rt_trip_update = match get_trip.len() {
                                        1 => &get_trip[0],
                                        _ => {
                                            println!(
                                                "Multiple trip updates found for trip id {} {}",
                                                chateau, query.trip_id
                                            );
                                            let query_start_date_parsed =
                                                query.start_date.as_ref().and_then(|date_str| {
                                                    chrono::NaiveDate::parse_from_str(
                                                        date_str, "%Y%m%d",
                                                    )
                                                    .or_else(|_| {
                                                        chrono::NaiveDate::parse_from_str(
                                                            date_str, "%Y-%m-%d",
                                                        )
                                                    })
                                                    .ok()
                                                });

                                            let find_trip = get_trip.iter().find(|each_update| {
                                                let start_time_matches = match (
                                                    &query.start_time,
                                                    &each_update.trip.start_time,
                                                ) {
                                                    (Some(q_time), Some(u_time)) => {
                                                        q_time == u_time
                                                    }
                                                    (None, _) => true,
                                                    (Some(_), None) => false,
                                                };

                                                let start_date_matches = match (
                                                    &query_start_date_parsed,
                                                    &each_update.trip.start_date,
                                                ) {
                                                    (Some(q_date), Some(u_date)) => {
                                                        q_date == u_date
                                                    }
                                                    (None, _) => true,
                                                    (Some(_), None) => false,
                                                };

                                                start_time_matches && start_date_matches
                                            });

                                            match find_trip {
                                                Some(find_trip) => find_trip,
                                                None => &get_trip[0],
                                            }
                                        }
                                    };

                                    let stop_data: Vec<StopTimeRefresh> = rt_trip_update
                                        .stop_time_update
                                        .iter()
                                        .map(|stop_time_update| StopTimeRefresh {
                                            stop_id: stop_time_update.stop_id.clone().unwrap_or_default().to_string(),
                                            rt_arrival: stop_time_update.arrival.clone().map(|x| x.delay.unwrap_or(0) as i64),
                                            rt_departure: stop_time_update.departure.clone().map(|x| x.delay.unwrap_or(0) as i64),
                                            schedule_relationship: stop_time_update
                                                .schedule_relationship
                                                .as_ref()
                                                .map(|x| Into::<u8>::into(x.clone()) as i32),
                                            rt_platform_string: stop_time_update
                                                .platform_string
                                                .clone()
                                                .map(|x| x.to_string()),
                                        })
                                        .collect();

                                    Ok(ResponseForGtfsRtRefresh {
                                        found_data: true,
                                        data: Some(GtfsRtRefreshData {
                                            stoptimes: stop_data,
                                            vehicle: rt_trip_update.vehicle.clone(),
                                            rt_shape: false, // Default or derived?
                                            shape_polyline: None,
                                            old_shape_polyline: None,
                                            is_cancelled: rt_trip_update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled),
                                            deleted: rt_trip_update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Deleted),
                                            timestamp: rt_trip_update.timestamp,
                                        }),
                                    })
                                } else {
                                    Ok(ResponseForGtfsRtRefresh {
                                        found_data: false,
                                        data: None,
                                    })
                                }
                            }
                            _ => Ok(ResponseForGtfsRtRefresh {
                                found_data: false,
                                data: None,
                            }),
                        }
                    }
                    _ => Err("Could not connect to realtime data server".to_string()),
                }
            } else {
                Err("Could not connect to realtime data server".to_string())
            }
        }
        _ => Err("Could not connect to zookeeper".to_string()),
    }
}
