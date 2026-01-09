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
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use tokio::join;

// Use shared types from departures_shared module
use crate::departures_shared::{AlertIndex, ItinOption, MovedStopData, ValidTripSet, fetch_stop_data_for_chateau, estimate_service_date};

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
struct DeparturesAtOsmStationResponse {
    osm_station: Option<OsmStationInfoForResponse>,
    stops: Vec<StopInfoResponse>,
    events: Vec<StopEvent>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
    shapes: BTreeMap<EcoString, BTreeMap<EcoString, String>>,
    alerts: BTreeMap<String, BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>>,
    agencies: BTreeMap<String, BTreeMap<String, catenary::models::Agency>>,
}

#[actix_web::get("/departures_at_osm_station")]
pub async fn departures_at_osm_station(
    req: HttpRequest,
    query: Query<DeparturesAtOsmStationQuery>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
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

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

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
    let osm_station_result: diesel::prelude::QueryResult<Vec<catenary::models::OsmStation>> =
        catenary::schema::gtfs::osm_stations::dsl::osm_stations
            .filter(catenary::schema::gtfs::osm_stations::osm_id.eq(query.osm_station_id))
            .select(catenary::models::OsmStation::as_select())
            .load::<catenary::models::OsmStation>(conn)
            .await;

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

    // Find all GTFS stops linked to this OSM station
    let stops_result: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
        catenary::schema::gtfs::stops::dsl::stops
            .filter(catenary::schema::gtfs::stops::osm_station_id.eq(query.osm_station_id))
            .filter(catenary::schema::gtfs::stops::allowed_spatial_query.eq(true))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(conn)
            .await;

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

    let mut futures = Vec::new();
    for (chateau_id_to_search, stop_ids_to_search) in &stops_to_search {
        let pool = pool.get_ref().clone();
        let chateau_id = chateau_id_to_search.clone();
        let stop_ids: Vec<String> = stop_ids_to_search.iter().cloned().collect();
        let include_shapes = include_shapes;

        futures.push(async move {
            fetch_stop_data_for_chateau(pool, chateau_id, stop_ids, include_shapes).await
        });
    }

    let results = join_all(futures).await;

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

    let calendar_structure =
        make_calendar_structure_from_pg(calender_responses, calendar_dates_responses)
            .unwrap_or_default();

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

    let etcd_results = join_all(etcd_futures).await;

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

    let mut aspen_futures = Vec::new();
    for (chateau_id, trips_compressed_data) in &trip_compressed_btreemap_by_chateau {
        if let Some(chateau_metadata_for_c) = chateau_metadata.get(chateau_id) {
            let chateau_id = chateau_id.clone();
            let trips_to_get = trips_compressed_data
                .keys()
                .cloned()
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

                let stops_to_search_for_this_chateau: Vec<String> = match stops_to_search_for_this_chateau {
                    Some(stops_set) => {
                        stops_set.iter().cloned().collect()
                    }
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
    let mut alert_indices: HashMap<String, AlertIndex> = HashMap::new();
    for (chateau_id, chateau_alerts) in &alerts {
        alert_indices.insert(chateau_id.clone(), AlertIndex::new(chateau_alerts));
    }

    for (chateau_id, trips_compressed_data) in &trip_compressed_btreemap_by_chateau {
        let mut valid_trips: HashMap<String, Vec<ValidTripSet>> = HashMap::new();

        for (trip_id, trip_compressed) in trips_compressed_data.iter() {
            let frequency: Option<catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto> =
                trip_compressed
                    .frequencies
                    .as_ref()
                    .map(|data| prost::Message::decode(data.as_ref()).unwrap());

            let freq_converted = frequency.map(|x| protobuf_to_frequencies(&x));

            let itin_for_this_trip = match itin_meta_btreemap_by_chateau
                .get(chateau_id)
                .and_then(|m| m.get(&trip_compressed.itinerary_pattern_id))
            {
                Some(x) => x,
                None => continue,
            };

            let itinerary_rows = match itins_btreemap_by_chateau
                .get(chateau_id)
                .and_then(|m| m.get(&trip_compressed.itinerary_pattern_id))
            {
                Some(x) => x,
                None => continue,
            };

            let direction_pattern_id = match &itin_for_this_trip.direction_pattern_id {
                Some(id) => id,
                None => continue,
            };

            let direction_meta = match direction_meta_btreemap_by_chateau
                .get(chateau_id)
                .and_then(|m| m.get(direction_pattern_id.as_str()))
            {
                Some(x) => x,
                None => continue,
            };

            let itin_ref = match itinerary_rows.last() {
                Some(x) => x,
                None => continue,
            };

            let time_since_start = match itin_ref.departure_time_since_start {
                Some(departure_time_since_start) => departure_time_since_start,
                None => match itin_ref.arrival_time_since_start {
                    Some(arrival) => arrival,
                    None => itin_ref.interpolated_time_since_start.unwrap_or(0),
                },
            };

            let timezone = match chrono_tz::Tz::from_str(itin_for_this_trip.timezone.as_str()) {
                Ok(tz) => tz,
                Err(_) => continue,
            };

            let time_delta = match chrono::TimeDelta::new(time_since_start.into(), 0) {
                Some(td) => td,
                None => continue,
            };

            let t_to_find_schedule_for = catenary::TripToFindScheduleFor {
                trip_id: trip_id.clone(),
                chateau: chateau_id.clone(),
                timezone,
                frequency: freq_converted.clone(),
                itinerary_id: itin_for_this_trip.itinerary_pattern_id.clone(),
                direction_id: direction_pattern_id.clone(),
                time_since_start_of_service_date: time_delta,
            };

            let service = match calendar_structure.get(chateau_id.as_str()) {
                Some(cal) => cal.get(trip_compressed.service_id.as_str()),
                None => continue,
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
                        let t = ValidTripSet {
                            chateau_id: chateau_id.clone(),
                            trip_id: (&trip_compressed.trip_id).into(),
                            timezone: chrono_tz::Tz::from_str(itin_for_this_trip.timezone.as_str())
                                .ok(),
                            frequencies: freq_converted.clone(),
                            trip_service_date: date.0,
                            itinerary_options: itinerary_rows
                                .iter()
                                .map(|itin_row| ItinOption {
                                    arrival_time_since_start: itin_row.arrival_time_since_start,
                                    departure_time_since_start: itin_row.departure_time_since_start,
                                    interpolated_time_since_start: itin_row
                                        .interpolated_time_since_start,
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
                                })
                                .collect::<Vec<_>>(),
                            reference_start_of_service_date: date.1,
                            itinerary_pattern_id: itin_ref.itinerary_pattern_id.clone(),
                            direction_pattern_id: itin_for_this_trip
                                .direction_pattern_id
                                .clone()
                                .unwrap(),
                            route_id: itin_for_this_trip.route_id.clone(),
                            trip_start_time: trip_compressed.start_time,
                            trip_short_name: trip_compressed.trip_short_name.clone(),
                            service_id: trip_compressed.service_id.clone(),
                        };

                        match valid_trips.entry(trip_compressed.trip_id.clone()) {
                            std::collections::hash_map::Entry::Occupied(mut oe) => {
                                oe.get_mut().push(t);
                            }
                            std::collections::hash_map::Entry::Vacant(ve) => {
                                ve.insert(vec![t]);
                            }
                        }
                    }
                }
            }
        }

        // Loop through valid trips and create events
        for valid_trips_vec in valid_trips.values() {
            for valid_trip in valid_trips_vec.iter() {
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
                                    let event_time = valid_trip
                                        .reference_start_of_service_date
                                        .timestamp()
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
                                        trip_cancelled = true;
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
                                                    gtfs_trip_aspenised.trip_updates.get(x).unwrap(),
                                                )
                                            })
                                            .filter(|(_x, trip_update)| {
                                                match does_trip_set_use_dates {
                                                    true => {
                                                        trip_update.trip.start_date
                                                            == Some(valid_trip.trip_service_date)
                                                    }
                                                    false => estimate_service_date(
                                                        valid_trip,
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

                                        if let Some(matching_stu) = trip_update
                                            .stop_time_update
                                            .iter()
                                            .find(|stu| {
                                                stu.stop_sequence
                                                    == Some(itin_option.gtfs_stop_sequence as u16)
                                                    || stu.stop_id.as_ref().map(|s| s.as_str())
                                                        == Some(itin_option.stop_id.as_str())
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
                                                == Some(AspenisedStopTimeScheduleRelationship::Skipped)
                                            {
                                                stop_cancelled = true;
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

                        let scheduled_arrival_time = itin_option.arrival_time_since_start.map(
                            |arrival_time_since_start| {
                                valid_trip.reference_start_of_service_date.timestamp() as u64
                                    + valid_trip.trip_start_time as u64
                                    + arrival_time_since_start as u64
                            },
                        );

                        let is_last_stop = itin_option.gtfs_stop_sequence
                            == valid_trip
                                .itinerary_options
                                .last()
                                .map(|x| x.gtfs_stop_sequence)
                                .unwrap_or(0);

                        events.push(StopEvent {
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
                                direction_meta_btreemap_by_chateau
                                    .get(chateau_id.as_str())
                                    .and_then(|m| m.get(&valid_trip.direction_pattern_id))
                                    .map(|d| d.headsign_or_destination.clone())
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
                            scheduled_trip_shape_id: direction_meta_btreemap_by_chateau
                                .get(chateau_id.as_str())
                                .and_then(|m| m.get(&valid_trip.direction_pattern_id))
                                .and_then(|d| d.gtfs_shape_id.clone())
                                .map(|x| x.into()),
                        });
                    }
                }
            }
        }
    }

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

    events.sort_by_key(|x| {
        x.realtime_departure.unwrap_or(
            x.realtime_arrival.unwrap_or(
                x.scheduled_departure
                    .unwrap_or(x.scheduled_arrival.unwrap_or(0)),
            ),
        )
    });

    let response = DeparturesAtOsmStationResponse {
        osm_station: osm_station_info,
        stops: stop_info_responses,
        events,
        routes,
        shapes,
        alerts,
        agencies,
    };

    HttpResponse::Ok().json(response)
}
