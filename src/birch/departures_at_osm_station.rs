// Copyright
// Catenary Transit Initiatives
// Departures at OSM Station Endpoint
// Attribution cannot be removed

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
use catenary::CalendarUnified;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::TripsSelectionResponse;
use catenary::aspen_dataset::AspenisedStopTimeScheduleRelationship;
use catenary::aspen_dataset::AspenisedTripUpdate;
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use catenary::make_calendar_structure_from_pg;
use catenary::make_degree_length_as_distance_from_point;
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::Datelike;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel::sql_types::*;
use diesel_async::RunQueryDsl;
use ecow::EcoString;
use futures::StreamExt;
use futures::future::join_all;
use geo::coord;
use rayon::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::join;

// Use shared types from departures_shared module
use crate::departures_shared::{
    AlertIndex, ItinOption, MovedStopData, ValidTripSet, estimate_service_date,
    fetch_stop_data_for_chateau,
};

#[derive(Deserialize, Clone, Debug)]
pub struct DeparturesAtOsmStationQuery {
    pub osm_station_id: i64,
    pub greater_than_time: Option<u64>,
    pub less_than_time: Option<u64>,
    pub include_shapes: Option<bool>,
}

#[derive(Serialize, Clone, Debug)]
pub struct OsmStationInfoForResponse {
    pub osm_id: i64,
    pub osm_type: String,
    pub name: Option<String>,
    pub name_translations: Option<serde_json::Value>,
    pub station_type: Option<String>,
    pub railway_tag: Option<String>,
    pub mode_type: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Serialize, Clone, Debug)]
struct StopInfoResponse {
    chateau: String,
    stop_id: String,
    stop_name: String,
    stop_lat: f64,
    stop_lon: f64,
    stop_code: Option<String>,
    level_id: Option<String>,
    platform_code: Option<String>,
    parent_station: Option<String>,
    children_ids: Vec<String>,
    timezone: String,
    stop_name_translations: Option<HashMap<String, String>>,
}

#[derive(Serialize, Clone, Debug)]
struct StopEvent {
    scheduled_arrival: Option<u64>,
    scheduled_departure: Option<u64>,
    realtime_arrival: Option<u64>,
    realtime_departure: Option<u64>,
    trip_modified: bool,
    stop_cancelled: bool,
    trip_cancelled: bool,
    trip_deleted: bool,
    trip_id: String,
    headsign: Option<String>,
    route_id: String,
    chateau: String,
    stop_id: String,
    uses_primary_stop: bool,
    unscheduled_trip: bool,
    moved_info: Option<MovedStopData>,
    platform_string_realtime: Option<String>,
    level_id: Option<String>,
    platform_code: Option<String>,
    vehicle_number: Option<String>,
    trip_short_name: Option<CompactString>,
    service_date: Option<NaiveDate>,
    last_stop: bool,
    scheduled_trip_shape_id: Option<CompactString>,
}

#[derive(Serialize, Clone, Debug)]
pub struct DeparturesAtOsmStationDebug {
    pub total_time_ms: u64,
    pub etcd_connection_time_ms: u64,
    pub db_connection_time_ms: u64,
    pub initial_osm_query_ms: u64,
    pub stop_data_fetch_ms: u64,
    pub aspen_data_fetch_ms: u64,
    pub event_generation_ms: u64,
}

#[derive(Serialize, Clone, Debug)]
struct DeparturesAtOsmStationResponse {
    osm_station: Option<OsmStationInfoForResponse>,
    stops: Vec<StopInfoResponse>,
    events: Vec<StopEvent>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
    shapes: BTreeMap<EcoString, BTreeMap<EcoString, String>>,
    alerts: BTreeMap<String, BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>>,
    agencies: BTreeMap<String, BTreeMap<String, catenary::models::Agency>>,
    debug: DeparturesAtOsmStationDebug,
}

#[actix_web::get("/departures_at_osm_station")]
#[tracing::instrument(name = "departures_at_osm_station", skip(pool, etcd_connection_ips, etcd_connection_options, etcd_reuser), fields(osm_id = ?query.osm_station_id))]
pub async fn departures_at_osm_station(
    req: HttpRequest,
    query: Query<DeparturesAtOsmStationQuery>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let start = Instant::now();
    let etcd_reuser = etcd_reuser.as_ref();

    let mut etcd = None;
    {
        let etcd_reuser_contents = etcd_reuser.read().await;
        let mut client_is_healthy = false;
        if let Some(client) = etcd_reuser_contents.as_ref() {
            let mut client = client.clone();

            if client.status().await.is_ok() {
                etcd = Some(client.clone());
                client_is_healthy = true;
            }
        }

        if !client_is_healthy {
            drop(etcd_reuser_contents);
            let new_client = etcd_client::Client::connect(
                etcd_connection_ips.ip_addresses.as_slice(),
                etcd_connection_options.as_ref().as_ref().to_owned(),
            )
            .await
            .unwrap();
            etcd = Some(new_client.clone());
            let mut etcd_reuser_write_lock = etcd_reuser.write().await;
            *etcd_reuser_write_lock = Some(new_client);
        }
    }

    let mut etcd = etcd.unwrap();

    let etcd_connection_time = start.elapsed();
    let db_timer = Instant::now();

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let db_connection_time = db_timer.elapsed();
    let initial_osm_query_timer = Instant::now();

    let now_secs = catenary::duration_since_unix_epoch().as_secs();
    let greater_than_time = query.greater_than_time.unwrap_or(now_secs - 3600);
    let less_than_time = query.less_than_time.unwrap_or(now_secs + 60 * 60 * 24);
    let requested_window_secs = less_than_time.saturating_sub(greater_than_time);
    let include_shapes = query.include_shapes.unwrap_or(true);

    let grace_secs: i64 = 5 * 60;

    let min_lookahead = chrono::TimeDelta::hours(12);
    let max_lookahead = chrono::TimeDelta::days(5);
    let req_lookahead = chrono::TimeDelta::seconds(requested_window_secs as i64 + grace_secs)
        .max(min_lookahead)
        .min(max_lookahead);

    // First, fetch the OSM station info
    let osm_station_query = catenary::schema::gtfs::osm_stations::dsl::osm_stations
        .filter(catenary::schema::gtfs::osm_stations::osm_id.eq(query.osm_station_id))
        .select(catenary::models::OsmStation::as_select())
        .load::<catenary::models::OsmStation>(conn);

    // Find all GTFS stops linked to this OSM station
    let stops_query = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::osm_station_id.eq(query.osm_station_id))
        .filter(catenary::schema::gtfs::stops::allowed_spatial_query.eq(true))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(conn);

    let (osm_station_result, stops_result) = tokio::join!(osm_station_query, stops_query);

    let initial_osm_query_time = initial_osm_query_timer.elapsed();

    println!(
        "Initial OSM queries took {} ms",
        initial_osm_query_time.as_millis()
    );

    let osm_station_info = match &osm_station_result {
        Ok(stations) if !stations.is_empty() => {
            let station = &stations[0];
            Some(OsmStationInfoForResponse {
                osm_id: station.osm_id,
                osm_type: station.osm_type.clone(),
                name: station.name.clone(),
                name_translations: station.name_translations.clone(),
                station_type: station.station_type.clone(),
                railway_tag: station.railway_tag.clone(),
                mode_type: station.mode_type.clone(),
                lat: station.point.y,
                lon: station.point.x,
            })
        }
        _ => None,
    };

    let linked_stops = match stops_result {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Stop query error: {:?}", e);
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Failed to query stops"}));
        }
    };

    if linked_stops.is_empty() {
        return HttpResponse::Ok().json(DeparturesAtOsmStationResponse {
            osm_station: osm_station_info,
            stops: vec![],
            events: vec![],
            routes: BTreeMap::new(),
            shapes: BTreeMap::new(),
            alerts: BTreeMap::new(),
            agencies: BTreeMap::new(),
            debug: DeparturesAtOsmStationDebug {
                total_time_ms: start.elapsed().as_millis() as u64,
                etcd_connection_time_ms: etcd_connection_time.as_millis() as u64,
                db_connection_time_ms: db_connection_time.as_millis() as u64,
                initial_osm_query_ms: initial_osm_query_time.as_millis() as u64,
                stop_data_fetch_ms: 0,
                aspen_data_fetch_ms: 0,
                event_generation_ms: 0,
            },
        });
    }

    // Build stops_to_search map: chateau_id -> HashSet<stop_id> for O(1) lookups
    let mut stops_to_search: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for stop in &linked_stops {
        stops_to_search
            .entry(stop.chateau.clone())
            .or_default()
            .insert(stop.gtfs_id.clone());

        // Include children
        for child_id in stop.children_ids.iter().filter_map(|x| x.clone()) {
            stops_to_search
                .entry(stop.chateau.clone())
                .or_default()
                .insert(child_id);
        }
    }

    // Use the first stop for timezone determination
    let primary_stop = &linked_stops[0];
    let stop_tz_txt = match &primary_stop.timezone {
        Some(tz) => tz.clone(),
        None => {
            if let Some(point_raw) = &primary_stop.point {
                match (-90.0..=90.0).contains(&point_raw.y)
                    && (-180.0..=180.0).contains(&point_raw.x)
                {
                    true => tz_search::lookup(point_raw.y, point_raw.x)
                        .unwrap_or_else(|| String::from("Etc/GMT")),
                    false => String::from("Etc/GMT"),
                }
            } else {
                String::from("Etc/GMT")
            }
        }
    };

    let stop_tz = chrono_tz::Tz::from_str_insensitive(&stop_tz_txt).unwrap_or(chrono_tz::UTC);

    let gt_utc = chrono::DateTime::from_timestamp(greater_than_time as i64, 0).unwrap_or_default();
    let greater_than_date_time = gt_utc.with_timezone(&stop_tz);

    let mut itins_btreemap_by_chateau: BTreeMap<
        String,
        BTreeMap<String, Vec<catenary::models::ItineraryPatternRow>>,
    > = BTreeMap::new();
    let mut itin_meta_btreemap_by_chateau: BTreeMap<
        String,
        BTreeMap<String, catenary::models::ItineraryPatternMeta>,
    > = BTreeMap::new();
    let mut direction_meta_btreemap_by_chateau: BTreeMap<
        String,
        BTreeMap<String, catenary::models::DirectionPatternMeta>,
    > = BTreeMap::new();
    let mut trip_compressed_btreemap_by_chateau: BTreeMap<
        String,
        BTreeMap<String, catenary::models::CompressedTrip>,
    > = BTreeMap::new();

    let mut direction_to_rows_by_chateau: BTreeMap<
        String,
        BTreeMap<String, Vec<catenary::models::DirectionPatternRow>>,
    > = BTreeMap::new();

    let mut routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>> = BTreeMap::new();
    let mut shapes: BTreeMap<EcoString, BTreeMap<EcoString, String>> = BTreeMap::new();
    let mut agencies: BTreeMap<String, BTreeMap<String, catenary::models::Agency>> =
        BTreeMap::new();

    let mut calender_responses: Vec<_> = vec![];
    let mut calendar_dates_responses: Vec<_> = vec![];

    let stop_data_fetch_timer = Instant::now();

    let stops_stream = futures::stream::iter(stops_to_search.iter())
        .map(|(chateau_id_to_search, stop_ids_to_search)| {
            let pool = pool.get_ref().clone();
            let chateau_id = chateau_id_to_search.clone();
            let stop_ids: Vec<String> = stop_ids_to_search.iter().cloned().collect();
            let include_shapes = include_shapes;

            let lookback_days = match chateau_id.as_str() {
                "sncb"
                | "schweiz"
                | "sncf"
                | "deutschland"
                | "nederlandse~spoorwegen"
                | "nationalrailuk" => 2,
                "île~de~france~mobilités" => 2,
                "bus~dft~gov~uk" => 8,
                _ => 14,
            };

            let check_start_date =
                greater_than_date_time.date_naive() - chrono::Duration::days(lookback_days);
            let check_end_date = (greater_than_date_time + req_lookahead)
                .date_naive()
                .succ_opt()
                .unwrap();

            async move {
                fetch_chateau_data_optimized(
                    pool,
                    chateau_id,
                    stop_ids,
                    include_shapes,
                    Some((check_start_date, check_end_date)),
                )
                .await
            }
        })
        .buffer_unordered(10);

    // Fetch realtime data from aspen
    let mut chateau_metadata = HashMap::new();
    let mut etcd_futures = Vec::new();
    for chateau_id in stops_to_search.keys() {
        let mut etcd_clone = etcd.clone();
        let chateau_id = chateau_id.clone();
        etcd_futures.push(async move {
            let etcd_data = etcd_clone
                .get(
                    format!("/aspen_assigned_chateaux/{}", chateau_id.clone()).as_str(),
                    None,
                )
                .await;
            (chateau_id, etcd_data)
        });
    }

    let (results, etcd_results) = join!(stops_stream.collect::<Vec<_>>(), join_all(etcd_futures));

    for (
        chateau_id,
        itins_btreemap,
        itin_meta_btreemap,
        direction_meta_btreemap,
        trip_compressed_btreemap,
        direction_rows_for_chateau,
        routes_btreemap,
        agencies_btreemap,
        shape_polyline_for_chateau,
        calendar,
        calendar_dates,
    ) in results
    {
        itins_btreemap_by_chateau.insert(chateau_id.clone(), itins_btreemap);
        itin_meta_btreemap_by_chateau.insert(chateau_id.clone(), itin_meta_btreemap);
        direction_meta_btreemap_by_chateau.insert(chateau_id.clone(), direction_meta_btreemap);
        trip_compressed_btreemap_by_chateau.insert(chateau_id.clone(), trip_compressed_btreemap);
        direction_to_rows_by_chateau.insert(chateau_id.clone(), direction_rows_for_chateau);
        routes.insert(chateau_id.clone(), routes_btreemap);
        agencies.insert(chateau_id.clone(), agencies_btreemap);
        shapes.insert(chateau_id.clone().into(), shape_polyline_for_chateau);
        calender_responses.push(calendar);
        calendar_dates_responses.push(calendar_dates);
    }

    let stop_data_fetch_time = stop_data_fetch_timer.elapsed();

    let calendar_structure =
        make_calendar_structure_from_pg(calender_responses, calendar_dates_responses)
            .unwrap_or_default();

    // OPTIMIZATION: pre-calculate active service IDs
    // This dramatically reduces the number of trips we need to fetch status for
    // and the number of trips we need to calculate complex schedules for.
    let mut active_services_by_chateau: HashMap<String, HashSet<String>> = HashMap::new();

    // We want to verify service availability in the requested window [greater_than_date_time, greater_than_date_time + req_lookahead]
    // We add a buffer of 1 day on either side to account for trips spanning midnight or timezone weirdness
    let check_start_date = greater_than_date_time.date_naive().pred_opt().unwrap();
    let check_end_date = (greater_than_date_time + req_lookahead)
        .date_naive()
        .succ_opt()
        .unwrap();

    for (chateau_id, services_map) in &calendar_structure {
        let mut active_services = HashSet::new();
        for (service_id, service) in services_map {
            let mut is_active = false;
            let mut d = check_start_date;
            while d <= check_end_date {
                if catenary::datetime_in_service(service, d) {
                    is_active = true;
                    break;
                }
                d = d.succ_opt().unwrap();
            }

            if is_active {
                active_services.insert(service_id.clone());
            }
        }
        active_services_by_chateau.insert(chateau_id.clone(), active_services);
    }

    for (chateau_id, etcd_data) in etcd_results {
        if let Ok(etcd_data) = etcd_data {
            if let Some(first_value) = etcd_data.kvs().first() {
                let this_chateau_metadata =
                    catenary::bincode_deserialize::<ChateauMetadataEtcd>(first_value.value())
                        .unwrap();

                chateau_metadata.insert(chateau_id.clone(), this_chateau_metadata);
            }
        }
    }

    let mut events: Vec<StopEvent> = vec![];
    let mut chateau_to_trips_aspenised: HashMap<String, TripsSelectionResponse> = HashMap::new();
    let mut chateau_to_nonscheduled_trips_aspenised: HashMap<String, Vec<AspenisedTripUpdate>> =
        HashMap::new();
    let mut alerts: BTreeMap<String, BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>> =
        BTreeMap::new();

    let aspen_data_fetch_timer = Instant::now();

    let mut aspen_futures = Vec::new();
    for (chateau_id, trips_compressed_data) in &trip_compressed_btreemap_by_chateau {
        if let Some(chateau_metadata_for_c) = chateau_metadata.get(chateau_id) {
            let chateau_id = chateau_id.clone();

            // Filter trips based on active services
            let active_services = active_services_by_chateau.get(&chateau_id);

            let trips_to_get = trips_compressed_data
                .iter()
                .filter(|(_, trip)| {
                    // If we have active services info, use it. If not, fallback to including it (safety)
                    match active_services {
                        Some(set) => set.contains(trip.service_id.as_str()),
                        None => true,
                    }
                })
                .map(|(k, _)| k.clone())
                .collect::<Vec<String>>();

            let socket = chateau_metadata_for_c.socket.clone();

            let stops_to_search_for_this_chateau = stops_to_search.get(chateau_id.as_str()).clone();

            aspen_futures.push(async move {
                let client = match catenary::aspen::lib::spawn_aspen_client_from_ip(&socket).await {
                    Ok(client) => client,
                    Err(e) => {
                        eprintln!("Error creating aspen client for {}: {:?}", chateau_id, e);
                        return (chateau_id, None, None, None);
                    }
                };

                let alerts_future =
                    client.get_all_alerts(tarpc::context::current(), chateau_id.clone());
                let trips_future = client.get_all_trips_with_ids(
                    tarpc::context::current(),
                    chateau_id.clone(),
                    trips_to_get,
                );

                let stops_to_search_for_this_chateau: Vec<String> =
                    match stops_to_search_for_this_chateau {
                        Some(stops_set) => stops_set.iter().cloned().collect(),
                        None => vec![],
                    };

                let nonscheduled_trips_future = client
                    .get_nonscheduled_trips_updates_from_stop_ids(
                        tarpc::context::current(),
                        chateau_id.clone(),
                        stops_to_search_for_this_chateau,
                    );

                let (alerts_res, trips_res, nonscheduled_trips_res) =
                    tokio::join!(alerts_future, trips_future, nonscheduled_trips_future);

                let alerts_data = match alerts_res {
                    Ok(Some(a)) => Some(a),
                    Ok(None) => None,
                    Err(e) => {
                        eprintln!("Error fetching alerts for {}: {:?}", chateau_id, e);
                        None
                    }
                };

                let trips_data = match trips_res {
                    Ok(Some(t)) => Some(t),
                    Ok(None) => None,
                    Err(e) => {
                        eprintln!("Error getting trip updates for {}: {:?}", chateau_id, e);
                        None
                    }
                };

                let nonscheduled_trips_data = match nonscheduled_trips_res {
                    Ok(Some(t)) => Some(t),
                    Ok(None) => None,
                    Err(e) => {
                        eprintln!(
                            "Error getting nonscheduled trip updates for {}: {:?}",
                            chateau_id, e
                        );
                        None
                    }
                };

                (chateau_id, alerts_data, trips_data, nonscheduled_trips_data)
            });
        }
    }

    let aspen_results = join_all(aspen_futures).await;

    for (chateau_id, alerts_opt, trips_opt, nonscheduled_trips_data_opt) in aspen_results {
        if let Some(all_alerts) = alerts_opt {
            alerts.insert(chateau_id.clone(), all_alerts.into_iter().collect());
        }
        if let Some(trips) = trips_opt {
            chateau_to_trips_aspenised.insert(chateau_id.clone(), trips);
        }

        if let Some(nonscheduled_trips_data) = nonscheduled_trips_data_opt {
            chateau_to_nonscheduled_trips_aspenised
                .insert(chateau_id.clone(), nonscheduled_trips_data);
        }
    }

    let aspen_data_fetch_time = aspen_data_fetch_timer.elapsed();
    let event_generation_timer = Instant::now();

    // Build stop info responses
    let stop_info_responses: Vec<StopInfoResponse> = linked_stops
        .iter()
        .map(|stop| StopInfoResponse {
            chateau: stop.chateau.clone(),
            stop_id: stop.gtfs_id.clone(),
            stop_name: stop.name.clone().unwrap_or_default(),
            stop_lat: stop.point.as_ref().map(|p| p.y).unwrap_or(0.0),
            stop_lon: stop.point.as_ref().map(|p| p.x).unwrap_or(0.0),
            stop_code: stop.code.clone(),
            level_id: stop.level_id.clone(),
            platform_code: stop.platform_code.clone(),
            parent_station: stop.parent_station.clone(),
            children_ids: stop.children_ids.iter().filter_map(|x| x.clone()).collect(),
            timezone: stop_tz_txt.clone(),
            stop_name_translations: catenary::serde_value_to_translated_hashmap(
                &stop.name_translations,
            ),
        })
        .collect();

    // Process trip data and create events
    // Filter alerts
    let mut filtered_alerts: BTreeMap<
        String,
        BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>,
    > = BTreeMap::new();
    for (chateau_id, chateau_alerts) in &alerts {
        let relevant_routes: BTreeSet<String> = routes
            .get(chateau_id)
            .map_or_else(BTreeSet::new, |r| r.keys().cloned().collect());
        let relevant_trips: BTreeSet<String> = trip_compressed_btreemap_by_chateau
            .get(chateau_id)
            .map_or_else(BTreeSet::new, |t| t.keys().cloned().collect());
        let relevant_stops: BTreeSet<String> = stops_to_search
            .get(chateau_id)
            .map_or_else(BTreeSet::new, |s| s.iter().cloned().collect());

        let chateau_filtered_alerts = chateau_alerts
            .iter()
            .filter(|(_alert_id, alert)| {
                alert.informed_entity.iter().any(|entity| {
                    if let Some(stop_id) = &entity.stop_id {
                        return relevant_stops.contains(stop_id);
                    }

                    let route_match = entity
                        .route_id
                        .as_ref()
                        .map_or(false, |r_id| relevant_routes.contains(r_id));
                    let trip_match = entity.trip.as_ref().map_or(false, |t| {
                        t.trip_id
                            .as_ref()
                            .map_or(false, |t_id| relevant_trips.contains(t_id))
                    });

                    // An entity is relevant if it matches a route, trip, or stop we are looking at.
                    // If an entity selector is broad (e.g., no specific route/trip/stop), we should include it if it's for the agency.
                    let is_general_alert = entity.route_id.is_none()
                        && entity.trip.is_none()
                        && entity.stop_id.is_none();

                    route_match || trip_match || is_general_alert
                })
            })
            .map(|(id, alert)| (id.clone(), alert.clone()))
            .collect::<BTreeMap<_, _>>();

        if !chateau_filtered_alerts.is_empty() {
            filtered_alerts.insert(chateau_id.clone(), chateau_filtered_alerts);
        }
    }
    alerts = filtered_alerts;

    let mut alert_indices: HashMap<String, AlertIndex> = HashMap::new();
    for (chateau_id, chateau_alerts) in &alerts {
        alert_indices.insert(chateau_id.clone(), AlertIndex::new(chateau_alerts));
    }

    let itins_btreemap_by_chateau = std::sync::Arc::new(itins_btreemap_by_chateau);
    let itin_meta_btreemap_by_chateau = std::sync::Arc::new(itin_meta_btreemap_by_chateau);
    let direction_meta_btreemap_by_chateau =
        std::sync::Arc::new(direction_meta_btreemap_by_chateau);
    let calendar_structure = std::sync::Arc::new(calendar_structure);
    let stops_to_search = std::sync::Arc::new(stops_to_search);
    let alert_indices = std::sync::Arc::new(alert_indices);
    let chateau_to_trips_aspenised = std::sync::Arc::new(chateau_to_trips_aspenised);
    let events_result = web::block(move || {
        trip_compressed_btreemap_by_chateau.par_iter().flat_map(|(chateau_id, trips_compressed_data)| {
            let active_services = active_services_by_chateau.get(chateau_id);
            let itins_btreemap_by_chateau = itins_btreemap_by_chateau.clone();
            let itin_meta_btreemap_by_chateau = itin_meta_btreemap_by_chateau.clone();
            let direction_meta_btreemap_by_chateau = direction_meta_btreemap_by_chateau.clone();
            let calendar_structure = calendar_structure.clone();
            let stops_to_search = stops_to_search.clone();
            let alert_indices = alert_indices.clone();
            let chateau_to_trips_aspenised = chateau_to_trips_aspenised.clone();

            // Build cache
            let mut pattern_cache = HashMap::new();
            if let Some(itins) = itins_btreemap_by_chateau.get(chateau_id) {
                if let Some(metas) = itin_meta_btreemap_by_chateau.get(chateau_id) {
                    for (pattern_id, rows) in itins {
                        if rows.is_empty() { continue; }
                        
                        // Get Meta
                        let meta = match metas.get(pattern_id) {
                            Some(m) => m,
                            None => continue,
                        };
                        
                        // Parse Timezone
                        let timezone = match chrono_tz::Tz::from_str(meta.timezone.as_str()) {
                            Ok(tz) => tz,
                            Err(_) => continue,
                        };

                        // Get Direction Meta
                        let direction_meta = match direction_meta_btreemap_by_chateau
                            .get(chateau_id)
                            .and_then(|m| meta.direction_pattern_id.as_ref().and_then(|id| m.get(id))) 
                        {
                            Some(d) => d,
                            None => continue,
                        };

                        // Build Options
                        let options: Vec<ItinOption> = rows.iter().map(|itin_row| {
                             ItinOption {
                                arrival_time_since_start: itin_row.arrival_time_since_start,
                                departure_time_since_start: itin_row.departure_time_since_start,
                                interpolated_time_since_start: itin_row.interpolated_time_since_start,
                                stop_id: itin_row.stop_id.clone(),
                                gtfs_stop_sequence: itin_row.gtfs_stop_sequence,
                                trip_headsign: match &itin_row.stop_headsign_idx {
                                    Some(idx) => direction_meta
                                        .stop_headsigns_unique_list
                                        .as_ref()
                                        .and_then(|list| list.get(*idx as usize))
                                        .cloned()
                                        .flatten(),
                                    None => None,
                                },
                                trip_headsign_translations: None,
                            }
                        }).collect();

                        // Calculate Reference Time (from last stop)
                        // Original logic used rows.last()
                        let last_row = rows.last().unwrap(); // Safe because checked is_empty
                        let time_since_start = match last_row.departure_time_since_start {
                            Some(t) => t,
                            None => match last_row.arrival_time_since_start {
                                Some(t) => t,
                                None => last_row.interpolated_time_since_start.unwrap_or(0),
                            },
                        };

                        pattern_cache.insert(pattern_id.clone(), (
                            std::sync::Arc::new(options),
                            timezone,
                            time_since_start,
                            meta.clone(),
                            direction_meta.clone()
                        ));
                    }
                }
            }
            
            let pattern_cache = std::sync::Arc::new(pattern_cache);

            trips_compressed_data.par_iter().flat_map(move |(trip_id, trip_compressed)| {
                let mut local_events = Vec::new();

                // Optimization: Skip trips with inactive services
                if let Some(set) = active_services {
                    if !set.contains(trip_compressed.service_id.as_str()) {
                        return Vec::new();
                    }
                }

                let frequency: Option<catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto> =
                    trip_compressed
                        .frequencies
                        .as_ref()
                        .map(|data| prost::Message::decode(data.as_ref()).unwrap());

                let freq_converted = frequency.map(|x| protobuf_to_frequencies(&x));

                // Use Cache
                let (options, timezone, time_since_start, itin_for_this_trip, direction_meta) = match pattern_cache.get(&trip_compressed.itinerary_pattern_id) {
                    Some(x) => x,
                    None => return Vec::new(),
                };
                
                let direction_pattern_id = match &itin_for_this_trip.direction_pattern_id {
                    Some(id) => id,
                    None => return Vec::new(),
                };

                let time_delta = match chrono::TimeDelta::new((*time_since_start).into(), 0) {
                    Some(td) => td,
                    None => return Vec::new(),
                };

                let t_to_find_schedule_for = catenary::TripToFindScheduleFor {
                    trip_id: trip_id.clone(),
                    chateau: chateau_id.clone(),
                    timezone: *timezone,
                    frequency: freq_converted.clone(),
                    itinerary_id: itin_for_this_trip.itinerary_pattern_id.clone(),
                    direction_id: direction_pattern_id.clone(),
                    time_since_start_of_service_date: time_delta,
                };

                let service = match calendar_structure.get(chateau_id.as_str()) {
                    Some(cal) => cal.get(trip_compressed.service_id.as_str()),
                    None => return Vec::new(),
                };

                if let Some(service) = service {
                    let dates = catenary::find_service_ranges(
                        service,
                        &t_to_find_schedule_for,
                        greater_than_date_time.with_timezone(&chrono::Utc),
                        chrono::TimeDelta::new(86400, 0).unwrap(),
                        req_lookahead,
                    );

                    if !dates.is_empty() {
                        for date in dates {
                            let valid_trip = ValidTripSet {
                                chateau_id: chateau_id.clone(),
                                trip_id: (&trip_compressed.trip_id).into(),
                                timezone: Some(*timezone),
                                frequencies: freq_converted.clone(),
                                trip_service_date: date.0,
                                itinerary_options: options.clone(),
                                reference_start_of_service_date: date.1,
                                itinerary_pattern_id: itin_for_this_trip.itinerary_pattern_id.clone(),
                                direction_pattern_id: direction_pattern_id.clone(),
                                route_id: itin_for_this_trip.route_id.clone(),
                                trip_start_time: trip_compressed.start_time,
                                trip_short_name: trip_compressed.trip_short_name.clone(),
                                service_id: trip_compressed.service_id.clone(),
                            };

                            // Generate events for this valid trip
                            if valid_trip.frequencies.is_none() {
                                for itin_option in valid_trip.itinerary_options.iter() {
                                    // Check if this stop is in our search set (O(1) with HashSet)
                                    let stop_in_search = stops_to_search
                                        .get(chateau_id)
                                        .map(|stops| stops.contains(itin_option.stop_id.as_str()))
                                        .unwrap_or(false);

                                    if !stop_in_search {
                                        continue;
                                    }

                                    let mut trip_cancelled: bool = false;
                                    let mut trip_deleted: bool = false;
                                    let mut stop_cancelled: bool = false;
                                    let mut departure_time_rt: Option<u64> = None;
                                    let mut platform: Option<String> = None;
                                    let mut arrival_time_rt: Option<u64> = None;
                                    let mut trip_update_for_event: Option<&AspenisedTripUpdate> = None;
                                    let mut vehicle_num: Option<String> = None;

                                    if let Some(index) = alert_indices.get(chateau_id) {
                                        let relevant_alerts =
                                            index.search(&valid_trip.route_id, &valid_trip.trip_id);
                                        for alert in relevant_alerts {
                                            if alert.effect == Some(1) {
                                                let event_time =
                                                    valid_trip.reference_start_of_service_date.timestamp()
                                                        as u64
                                                        + valid_trip.trip_start_time as u64
                                                        + itin_option.departure_time_since_start.unwrap_or(0)
                                                            as u64;

                                                let is_active = alert.active_period.iter().any(|ap| {
                                                    let start = ap.start.unwrap_or(0);
                                                    let end = ap.end.unwrap_or(u64::MAX);
                                                    event_time >= start && event_time <= end
                                                });
                                                if is_active {
                                                    // Check if any informed entity has a stop_id
                                                    let has_stop_id = alert.informed_entity.iter().any(|e| e.stop_id.is_some());

                                                    if has_stop_id {
                                                        // If it has a stop_id, check if it matches OUR stop_id
                                                        let matches_stop = alert.informed_entity.iter().any(|e| {
                                                            match &e.stop_id {
                                                                Some(s) => s.as_str() == itin_option.stop_id.as_str(),
                                                                None => false,
                                                            }
                                                        });

                                                        if matches_stop {
                                                            stop_cancelled = true;
                                                        }
                                                    } else {
                                                        // No stop_id means it affects the whole trip/route
                                                        trip_cancelled = true;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if let Some(gtfs_trip_aspenised) =
                                        chateau_to_trips_aspenised.get(chateau_id.as_str())
                                    {
                                        if let Some(trip_update_ids) = gtfs_trip_aspenised
                                            .trip_id_to_trip_update_ids
                                            .get(valid_trip.trip_id.as_str())
                                        {
                                            if !trip_update_ids.is_empty() {
                                                let does_trip_set_use_dates = gtfs_trip_aspenised
                                                    .trip_updates
                                                    .get(&trip_update_ids[0])
                                                    .unwrap()
                                                    .trip
                                                    .start_date
                                                    .is_some();

                                                let trip_updates: Vec<(&String, &AspenisedTripUpdate)> =
                                                    trip_update_ids
                                                        .iter()
                                                        .map(|x| {
                                                            (
                                                                x,
                                                                gtfs_trip_aspenised
                                                                    .trip_updates
                                                                    .get(x)
                                                                    .unwrap(),
                                                            )
                                                        })
                                                        .filter(|(_x, trip_update)| {
                                                            match does_trip_set_use_dates {
                                                                true => {
                                                                    trip_update.trip.start_date
                                                                        == Some(valid_trip.trip_service_date)
                                                                }
                                                                false => estimate_service_date(
                                                                    &valid_trip,
                                                                    trip_update,
                                                                    itin_option,
                                                                    calendar_structure
                                                                        .get(chateau_id.as_str())
                                                                        .unwrap(),
                                                                    chateau_id,
                                                                ),
                                                            }
                                                        })
                                                        .collect();

                                                if !trip_updates.is_empty() {
                                                    trip_update_for_event = Some(trip_updates[0].1);
                                                    let trip_update = trip_updates[0].1;

                                                    if let Some(vehicle) = &trip_update.vehicle {
                                                        vehicle_num = vehicle.label.clone();
                                                    }

                                                    if trip_update.trip.schedule_relationship
                                                        == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled)
                                                    {
                                                        trip_cancelled = true;
                                                    } else if trip_update.trip.schedule_relationship
                                                        == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted)
                                                    {
                                                        trip_deleted = true;
                                                    }

                                                    if let Some(matching_stu) =
                                                        trip_update.stop_time_update.iter().find(|stu| {
                                                            stu.stop_sequence
                                                                == Some(itin_option.gtfs_stop_sequence as u16)
                                                                || stu.stop_id.as_ref().map(|sid| 
                                                                    crate::stop_matching::rt_stop_matches_scheduled_simple(sid, itin_option.stop_id.as_str())
                                                                ).unwrap_or(false)
                                                        })
                                                    {
                                                        if let Some(arr) = &matching_stu.arrival {
                                                            arrival_time_rt = arr.time.map(|t| t as u64);
                                                        }
                                                        if let Some(dep) = &matching_stu.departure {
                                                            departure_time_rt = dep.time.map(|t| t as u64);
                                                        }
                                                        if let Some(plat) = &matching_stu.platform_string {
                                                            platform = Some(plat.to_string());
                                                        }
                                                        if matching_stu.schedule_relationship
                                                            == Some(
                                                                AspenisedStopTimeScheduleRelationship::Skipped,
                                                            )
                                                        {
                                                            stop_cancelled = true;
                                                        }
                                                        
                                                        // LOGIC UPDATE: Handle partial schedules and late arrivals
                                                        let trip_base_time = valid_trip.reference_start_of_service_date.timestamp() as u64 
                                                            + valid_trip.trip_start_time as u64;

                                                        let abs_scheduled_arrival = itin_option.arrival_time_since_start
                                                            .map(|t| trip_base_time + t as u64);
                                                        let abs_scheduled_departure = itin_option.departure_time_since_start
                                                            .map(|t| trip_base_time + t as u64);

                                                        // If the GTFS schedule is only listing either arrival or departure but not both (aka waiting at a station), 
                                                        // set the rt departure time to the rt arrival time.
                                                        if (abs_scheduled_arrival.is_some() ^ abs_scheduled_departure.is_some()) {
                                                            if let Some(rt_arr) = arrival_time_rt {
                                                                departure_time_rt = Some(rt_arr);
                                                            }
                                                        }

                                                        // If the rt arrival time is after the GTFS schedule departure time, also set the rt departure time
                                                        if let Some(rt_arr) = arrival_time_rt {
                                                            if let Some(sched_dep) = abs_scheduled_departure {
                                                                if rt_arr > sched_dep {
                                                                    // Ensure rt_departure is at least rt_arr
                                                                    if departure_time_rt.unwrap_or(0) < rt_arr {
                                                                        departure_time_rt = Some(rt_arr);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let scheduled_departure_time = itin_option.departure_time_since_start.map(
                                        |departure_time_since_start| {
                                            valid_trip.reference_start_of_service_date.timestamp() as u64
                                                + valid_trip.trip_start_time as u64
                                                + departure_time_since_start as u64
                                        },
                                    );

                                    let scheduled_arrival_time =
                                        itin_option
                                            .arrival_time_since_start
                                            .map(|arrival_time_since_start| {
                                                valid_trip.reference_start_of_service_date.timestamp() as u64
                                                    + valid_trip.trip_start_time as u64
                                                    + arrival_time_since_start as u64
                                            });

                                    let is_last_stop = itin_option.gtfs_stop_sequence
                                        == valid_trip
                                            .itinerary_options
                                            .last()
                                            .map(|x| x.gtfs_stop_sequence)
                                            .unwrap_or(0);

                                    local_events.push(StopEvent {
                                        scheduled_arrival: scheduled_arrival_time,
                                        scheduled_departure: scheduled_departure_time,
                                        realtime_arrival: arrival_time_rt,
                                        realtime_departure: departure_time_rt,
                                        trip_modified: false,
                                        stop_cancelled,
                                        trip_cancelled,
                                        trip_deleted,
                                        stop_id: itin_option.stop_id.to_string(),
                                        trip_id: valid_trip.trip_id.to_string(),
                                        chateau: chateau_id.clone(),
                                        last_stop: is_last_stop,
                                        platform_code: None,
                                        headsign: itin_option.trip_headsign.clone().or_else(|| {
                                            Some(direction_meta.headsign_or_destination.clone())
                                        }),
                                        route_id: valid_trip.route_id.clone().to_string(),
                                        vehicle_number: vehicle_num,
                                        level_id: None,
                                        uses_primary_stop: true,
                                        unscheduled_trip: false,
                                        moved_info: None,
                                        platform_string_realtime: platform,
                                        trip_short_name: valid_trip.trip_short_name.clone(),
                                        service_date: Some(valid_trip.trip_service_date),
                                        scheduled_trip_shape_id: direction_meta.gtfs_shape_id.clone()
                                            .map(|x| x.into()),
                                    });
                                }
                            }
                        }
                    }
                }
                local_events
            })
        }).collect::<Vec<StopEvent>>()
    }).await;

    let mut events: Vec<StopEvent> = match events_result {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Error in event generation: {:?}", e);
            vec![]
        }
    };

    // Deduplicate events: keep only one trip per (scheduled_time, route, headsign), preferring realtime
    let mut unique_events: HashMap<(Option<u64>, String, Option<String>), StopEvent> =
        HashMap::new();

    for event in events {
        let key = (
            event.scheduled_departure,
            event.route_id.clone(),
            event.headsign.clone(),
        );

        match unique_events.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let existing = entry.get();
                let existing_has_rt =
                    existing.realtime_departure.is_some() || existing.realtime_arrival.is_some();
                let new_has_rt =
                    event.realtime_departure.is_some() || event.realtime_arrival.is_some();

                if !existing_has_rt && new_has_rt {
                    entry.insert(event);
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(event);
            }
        }
    }
    let mut events: Vec<StopEvent> = unique_events.into_values().collect();

    // Filter and sort events
    let gt = greater_than_time;
    let lt = less_than_time;
    events.retain(|e| {
        let t1 = e.realtime_departure;
        let t2 = e.realtime_arrival;
        let t3 = e.scheduled_departure;
        let t4 = e.scheduled_arrival;
        let in_range = |t: Option<u64>| t.map(|x| x >= gt && x <= lt).unwrap_or(false);
        in_range(t1) || in_range(t2) || in_range(t3) || in_range(t4)
    });

    events.sort_by(|a, b| {
        let time_a = a
            .scheduled_departure
            .or(a.realtime_departure)
            .or(a.scheduled_arrival)
            .or(a.realtime_arrival)
            .unwrap_or(0);
        let time_b = b
            .scheduled_departure
            .or(b.realtime_departure)
            .or(b.scheduled_arrival)
            .or(b.realtime_arrival)
            .unwrap_or(0);
        time_a.cmp(&time_b)
    });

    let event_generation_time = event_generation_timer.elapsed();

    HttpResponse::Ok().json(DeparturesAtOsmStationResponse {
        osm_station: osm_station_info,
        stops: stop_info_responses,
        events,
        routes,
        shapes: match include_shapes {
            true => shapes,
            false => BTreeMap::new(),
        },
        alerts,
        agencies,
        debug: DeparturesAtOsmStationDebug {
            total_time_ms: start.elapsed().as_millis() as u64,
            etcd_connection_time_ms: etcd_connection_time.as_millis() as u64,
            db_connection_time_ms: db_connection_time.as_millis() as u64,
            initial_osm_query_ms: initial_osm_query_time.as_millis() as u64,
            stop_data_fetch_ms: stop_data_fetch_time.as_millis() as u64,
            aspen_data_fetch_ms: aspen_data_fetch_time.as_millis() as u64,
            event_generation_ms: event_generation_time.as_millis() as u64,
        },
    })
}

async fn fetch_chateau_data_optimized(
    pool: Arc<CatenaryPostgresPool>,
    chateau_id: String,
    stop_ids: Vec<String>,
    include_shapes: bool,
    date_filter: Option<(NaiveDate, NaiveDate)>,
) -> crate::departures_shared::StopDataResult {
    let mut conn = pool.get().await.unwrap();

    // 1. Fetch Direction Pattern IDs containing these stops
    let direction_rows: Vec<catenary::models::DirectionPatternRow> =
        catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
            .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau_id.clone()))
            .filter(catenary::schema::gtfs::direction_pattern::stop_id.eq_any(&stop_ids))
            .select(catenary::models::DirectionPatternRow::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

    if direction_rows.is_empty() {
        return (
            chateau_id,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            vec![],
            vec![],
        );
    }

    let direction_pattern_ids: Vec<String> = direction_rows
        .iter()
        .map(|r| r.direction_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // 2. Fetch Meta based on Direction Patterns
    let mut conn_meta = pool.get().await.unwrap();
    let itin_meta_list: Vec<catenary::models::ItineraryPatternMeta> =
        catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(chateau_id.clone()))
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::direction_pattern_id
                    .eq_any(&direction_pattern_ids),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load(&mut conn_meta)
            .await
            .unwrap_or_default();

    let itinerary_ids: Vec<String> = itin_meta_list
        .iter()
        .map(|m| m.itinerary_pattern_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let route_ids: Vec<CompactString> = itin_meta_list.iter().map(|m| m.route_id.clone()).collect();

    let pool_clone = pool.clone();
    let chateau_clone_itin = chateau_id.clone();
    let chateau_clone_dm = chateau_id.clone();
    let chateau_clone_routes = chateau_id.clone();
    let chateau_clone_agencies = chateau_id.clone();
    let chateau_clone_dr = chateau_id.clone();
    let chateau_clone_shapes = chateau_id.clone();

    let direction_pattern_ids_clone = direction_pattern_ids.clone();

    // START PARALLEL FETCHES
    let (itins_result, direction_meta_result, direction_rows_result, routes_result) = tokio::join!(
        async {
            let mut conn = pool_clone.get().await.unwrap();
            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(chateau_clone_itin))
                .filter(
                    catenary::schema::gtfs::itinerary_pattern::itinerary_pattern_id
                        .eq_any(&itinerary_ids),
                )
                .filter(catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(&stop_ids))
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
                        .eq_any(&direction_pattern_ids_clone),
                )
                .select(catenary::models::DirectionPatternMeta::as_select())
                .load::<catenary::models::DirectionPatternMeta>(&mut conn)
                .await
                .unwrap_or_default()
        },
        async {
            let mut conn = pool_clone.get().await.unwrap();
            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
                .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau_clone_dr))
                .filter(
                    catenary::schema::gtfs::direction_pattern::direction_pattern_id
                        .eq_any(&direction_pattern_ids_clone),
                )
                .select(catenary::models::DirectionPatternRow::as_select())
                .load::<catenary::models::DirectionPatternRow>(&mut conn)
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

    // Fetch Agencies
    let agency_ids: Vec<String> = routes_result
        .iter()
        .filter_map(|r| r.agency_id.clone())
        .collect();
    let agencies_result: Vec<catenary::models::Agency> = if !agency_ids.is_empty() {
        let mut conn = pool.get().await.unwrap();
        catenary::schema::gtfs::agencies::dsl::agencies
            .filter(catenary::schema::gtfs::agencies::chateau.eq(chateau_clone_agencies))
            .filter(catenary::schema::gtfs::agencies::agency_id.eq_any(&agency_ids))
            .select(catenary::models::Agency::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };

    // Fetch Shapes if needed
    let mut shapes_map = BTreeMap::new();
    if include_shapes {
        let shape_ids: Vec<String> = direction_meta_result
            .iter()
            .filter_map(|m| m.gtfs_shape_id.clone())
            .collect();
        if !shape_ids.is_empty() {
            let mut conn = pool.get().await.unwrap();
            let shapes: Vec<catenary::models::Shape> = catenary::schema::gtfs::shapes::dsl::shapes
                .filter(catenary::schema::gtfs::shapes::chateau.eq(chateau_clone_shapes))
                .filter(catenary::schema::gtfs::shapes::shape_id.eq_any(&shape_ids))
                .load(&mut conn)
                .await
                .unwrap_or_default();

            for s in shapes {
                let poly = polyline::encode_coordinates(
                    geo::LineString::new(
                        s.linestring
                            .points
                            .iter()
                            .map(|p| coord! { x: p.x, y: p.y })
                            .collect(),
                    ),
                    5,
                )
                .unwrap();
                shapes_map.insert(s.shape_id.into(), poly);
            }
        }
    }

    // Fetch Trips (Compressed)
    let trips_result: Vec<catenary::models::CompressedTrip> = {
        let mut conn = pool.get().await.unwrap();
        let trips_raw: Vec<catenary::models::CompressedTrip> =
            catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::trips_compressed::route_id.eq_any(&route_ids))
                .select(catenary::models::CompressedTrip::as_select())
                .load(&mut conn)
                .await
                .unwrap_or_default();

        let valid_itins: HashSet<&str> = itinerary_ids.iter().map(|s| s.as_str()).collect();
        trips_raw
            .into_iter()
            .filter(|t| valid_itins.contains(t.itinerary_pattern_id.as_str()))
            .collect()
    };

    // Fetch Calendar
    let service_ids: Vec<CompactString> = trips_result
        .iter()
        .map(|t| t.service_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let (calendar, calendar_dates) = if !service_ids.is_empty() {
        let pool_cal = pool.clone();
        let pool_cd = pool.clone();
        let chateau_cal = chateau_id.clone();
        let chateau_cd = chateau_id.clone();
        let s_ids_1 = service_ids.clone();
        let s_ids_2 = service_ids.clone();
        let (start_date, end_date) = date_filter.unwrap_or((
            chrono::Utc::now().date_naive() - chrono::Duration::days(1),
            chrono::Utc::now().date_naive() + chrono::Duration::days(1),
        ));

        tokio::join!(
            async move {
                let mut conn = pool_cal.get().await.unwrap();
                catenary::schema::gtfs::calendar::dsl::calendar
                    .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau_cal))
                    .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&s_ids_1))
                    .select(catenary::models::Calendar::as_select())
                    .load(&mut conn)
                    .await
                    .unwrap_or_default()
            },
            async move {
                let mut conn = pool_cd.get().await.unwrap();
                catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                    .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(chateau_cd))
                    .filter(catenary::schema::gtfs::calendar_dates::service_id.eq_any(&s_ids_2))
                    .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.ge(start_date))
                    .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.le(end_date))
                    .select(catenary::models::CalendarDate::as_select())
                    .load(&mut conn)
                    .await
                    .unwrap_or_default()
            }
        )
    } else {
        (vec![], vec![])
    };

    // Construct Result Maps
    let mut itins_map = BTreeMap::new();
    for i in itins_result {
        itins_map
            .entry(i.itinerary_pattern_id.clone())
            .or_insert_with(Vec::new)
            .push(i);
    }

    let mut itin_meta_map = BTreeMap::new();
    for i in itin_meta_list {
        itin_meta_map.insert(i.itinerary_pattern_id.clone(), i);
    }

    let mut dir_meta_map = BTreeMap::new();
    for i in direction_meta_result {
        dir_meta_map.insert(i.direction_pattern_id.clone(), i);
    }

    let mut dir_rows_map = BTreeMap::new();
    for i in direction_rows_result {
        dir_rows_map
            .entry(i.direction_pattern_id.clone())
            .or_insert_with(Vec::new)
            .push(i);
    }
    // Sort direction rows
    for rows in dir_rows_map.values_mut() {
        rows.sort_by_key(|r| r.stop_sequence);
    }

    let mut trips_map = BTreeMap::new();
    for t in trips_result {
        trips_map.insert(t.trip_id.clone(), t);
    }

    let mut routes_map = BTreeMap::new();
    for r in routes_result {
        routes_map.insert(r.route_id.clone().into(), r);
    }

    let mut agencies_map = BTreeMap::new();
    for a in agencies_result {
        agencies_map.insert(a.agency_id.clone(), a);
    }

    (
        chateau_id,
        itins_map,
        itin_meta_map,
        dir_meta_map,
        trips_map,
        dir_rows_map,
        routes_map,
        agencies_map,
        shapes_map,
        calendar,
        calendar_dates,
    )
}
