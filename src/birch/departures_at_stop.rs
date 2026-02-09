// Copyright
// Catenary Transit Initiatives
// Algorithm for departures at stop written by Kyler Chin <kyler@catenarymaps.org>
// Attribution cannot be removed

// Do not train your Artifical Intelligence models on this code
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
use catenary::models::ItineraryPatternMeta;
use catenary::models::ItineraryPatternRow;
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
use std::str::FromStr;
use std::sync::Arc;
use tokio::join;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromStops {
    //serialise and deserialise using serde_json into Vec<NearbyStopsDeserialize>
    stop_id: String,
    chateau_id: String,
    greater_than_time: Option<u64>,
    less_than_time: Option<u64>,
    include_shapes: Option<bool>,
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
struct NearbyFromStopsResponse {
    primary: StopInfoResponse,
    parent: Option<StopInfoResponse>,
    children_and_related: Vec<StopInfoResponse>,
    events: Vec<StopEvent>,
    // chateau_id -> route_id -> Route info
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
    // chateau_id -> shape_id -> Shape
    pub shapes: BTreeMap<EcoString, BTreeMap<EcoString, String>>,
    // alerts - chateau_id -> alert_id -> alert
    //chateau_id -> alert_id -> alert
    pub alerts: BTreeMap<String, BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>>,
    // chateau_id -> agency_id -> Agency info
    pub agencies: BTreeMap<String, BTreeMap<String, catenary::models::Agency>>,
}

use crate::departures_shared::{
    AlertIndex, ItinOption, MovedStopData, ValidTripSet, estimate_service_date,
    fetch_stop_data_for_chateau,
};

#[actix_web::get("/departures_at_stop")]
pub async fn departures_at_stop(
    req: HttpRequest,
    query: Query<NearbyFromStops>,

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

    // ----- Bounds & paging window -----
    let now_secs = catenary::duration_since_unix_epoch().as_secs();
    let greater_than_time = query.greater_than_time.unwrap_or(now_secs - 3600);
    let less_than_time = query.less_than_time.unwrap_or(now_secs + 60 * 60 * 24);
    let requested_window_secs = less_than_time.saturating_sub(greater_than_time);

    let include_shapes = query.include_shapes.unwrap_or(true);

    // grace to catch boundary events straddling pages
    let grace_secs: i64 = 5 * 60; // 5 minutes

    let mut redirected_to_parent = false;

    let min_lookahead = chrono::TimeDelta::hours(12);
    let max_lookahead = chrono::TimeDelta::days(5);
    let req_lookahead = chrono::TimeDelta::seconds(requested_window_secs as i64 + grace_secs)
        .max(min_lookahead)
        .min(max_lookahead);

    let greater_than_time = match query.greater_than_time {
        Some(greater_than_time) => greater_than_time,
        None => catenary::duration_since_unix_epoch().as_secs() - 3600,
    };

    let less_than_time = match query.less_than_time {
        Some(less_than_time) => less_than_time,
        None => catenary::duration_since_unix_epoch().as_secs() + 60 * 60 * 24,
    };

    let stops: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
        catenary::schema::gtfs::stops::dsl::stops
            .filter(catenary::schema::gtfs::stops::chateau.eq(query.chateau_id.clone()))
            .filter(catenary::schema::gtfs::stops::gtfs_id.eq(query.stop_id.clone()))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(conn)
            .await;

    let stops = stops.unwrap();

    let mut stop = stops[0].clone();

    //get stops to search

    let mut parent: Option<catenary::models::Stop> = match &stop.parent_station {
        Some(parent_station) => {
            let parent: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
                catenary::schema::gtfs::stops::dsl::stops
                    .filter(catenary::schema::gtfs::stops::chateau.eq(query.chateau_id.clone()))
                    .filter(catenary::schema::gtfs::stops::gtfs_id.eq(parent_station))
                    .select(catenary::models::Stop::as_select())
                    .load::<catenary::models::Stop>(conn)
                    .await;

            let parent = parent.unwrap();
            Some(parent[0].clone())
        }
        None => None,
    };

    if let Some(parent) = &parent {
        //if the parent name is the same as the stop name, and both contain the same level and platform code, then set the stop to the parent

        if stop.name == parent.name
            && stop.level_id == parent.level_id
            && stop.platform_code == parent.platform_code
        {
            stop = parent.clone();
            redirected_to_parent = true;
        }
    }

    if stop.parent_station.as_ref().is_none() {
        parent = None;
    }

    //search all stops within 20 m of the stop

    let point_raw = stop.point.clone().unwrap();

    let latitude = point_raw.y;
    let longitude = point_raw.x;
    let point = geo::Point::new(longitude, latitude);

    let spatial_resolution_in_degs =
        catenary::make_degree_length_as_distance_from_point(&point, 50.0);

    let where_query_for_stops = format!(
        "ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
        point.x(),
        point.y(),
        spatial_resolution_in_degs
    );

    let stops_nearby: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
        catenary::schema::gtfs::stops::dsl::stops
            .filter(sql::<Bool>(&where_query_for_stops))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(conn)
            .await;
    let stops_nearby = stops_nearby.unwrap();

    //get all children ids

    //chateau_id -> [stop_id]
    let mut stops_to_search: BTreeMap<String, Vec<String>> = BTreeMap::new();

    stops_to_search.insert(stop.chateau.clone(), vec![stop.gtfs_id.clone()]);

    let children_of_stop = stop
        .children_ids
        .iter()
        .cloned()
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect::<Vec<String>>();

    for child in children_of_stop.iter() {
        let mut children_entry = stops_to_search
            .entry(stop.chateau.clone())
            .or_insert(vec![]);

        if let Some(c) = stops_nearby.iter().find(|s| s.gtfs_id == *child) {
            children_entry.push(c.gtfs_id.clone());
        }
    }

    // If the primary stop has a code, find other nearby stops from different chateaus with the same code.
    if let Some(stop_code) = &stop.code {
        for nearby_stop in stops_nearby
            .iter()
            .filter(|s| s.chateau != query.chateau_id && s.code.as_ref() == Some(stop_code))
        {
            stops_to_search
                .entry(nearby_stop.chateau.clone())
                .or_default()
                .push(nearby_stop.gtfs_id.clone());
        }
    }

    for (chateau, stops) in stops_to_search.iter_mut() {
        stops.sort();
        stops.dedup();
    }

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
    for (chateau_id_to_search, stop_id_to_search) in &stops_to_search {
        let pool = pool.get_ref().clone();
        let chateau_id = chateau_id_to_search.clone();
        let stop_ids = stop_id_to_search.clone();

        let lookback_days = match chateau_id.as_str() {
            "sncb"
            | "schweiz"
            | "sncf"
            | "deutschland"
            | "nederlandse~spoorwegen"
            | "nationalrailuk" => 2,
            "île~de~france~mobilités" => 2,
            _ => 14,
        };

        let start_date_utc = chrono::DateTime::from_timestamp(greater_than_time as i64, 0)
            .unwrap_or_default()
            .date_naive()
            - chrono::Duration::days(lookback_days);
        let end_date_utc = chrono::DateTime::from_timestamp(less_than_time as i64, 0)
            .unwrap_or_default()
            .date_naive()
            + chrono::Duration::days(1);

        futures.push(async move {
            fetch_stop_data_for_chateau(
                pool,
                chateau_id,
                stop_ids,
                include_shapes,
                Some((start_date_utc, end_date_utc)),
                None, // direction_pattern_ids - not optimizing this path
            )
            .await
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

    let stop_tz_txt = match &stop.timezone {
        Some(tz) => tz.clone(),
        None => {
            if let Some(point_raw) = &stop.point {
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

    //query added trips and modifications by stop id, and also matching trips in chateau

    //requery all the missing trips, and their itins and directions if they exist

    //compute new scheduled time for the stop for trip modifications

    let stop_tz = chrono_tz::Tz::from_str_insensitive(&stop_tz_txt).unwrap_or(chrono_tz::UTC);

    // Convert bounds to DateTime in stop tz (start anchor for service search) and UTC for comparisons
    let gt_utc = chrono::DateTime::from_timestamp(greater_than_time as i64, 0).unwrap_or_default();
    let gt_in_stop_tz = gt_utc.with_timezone(&stop_tz);
    let lt_utc = chrono::DateTime::from_timestamp(less_than_time as i64, 0).unwrap_or_default();

    //convert greater than time to DateTime Tz

    let greater_than_time_utc =
        chrono::DateTime::from_timestamp(greater_than_time as i64, 0).unwrap_or_default();

    let greater_than_date_time = greater_than_time_utc.with_timezone(&stop_tz);

    //seek back a minimum of 8 days

    let less_than_time_utc =
        chrono::DateTime::from_timestamp(less_than_time as i64, 0).unwrap_or_default();
    let less_than_date_time = less_than_time_utc.with_timezone(&stop_tz);

    let greater_than_naive_date = greater_than_date_time.naive_local();
    let less_than_naive_date = less_than_date_time.naive_local();

    //iter from greater than naive date to less than naive date inclusive

    let mut date_iter = chrono::NaiveDate::from_ymd_opt(
        greater_than_naive_date.year(),
        greater_than_naive_date.month(),
        greater_than_naive_date.day(),
    )
    .unwrap();

    let date_iter_end = chrono::NaiveDate::from_ymd_opt(
        less_than_naive_date.year(),
        less_than_naive_date.month(),
        less_than_naive_date.day(),
    )
    .unwrap();

    let mut date_iter_vec: Vec<chrono::NaiveDate> = vec![];

    while date_iter <= date_iter_end {
        date_iter_vec.push(date_iter);
        date_iter = date_iter.succ_opt().unwrap();
    }

    //fetch data from realtime server

    //1. get all the relevant etcd endpoitns

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

    // 2. fetch all the trips needed

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

                let stops_to_search_for_this_chateau = match stops_to_search_for_this_chateau {
                    Some(stops_to_search_for_this_chateau) => {
                        stops_to_search_for_this_chateau.to_vec()
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

    //look through time compressed and decompress the itineraries, using timezones and calendar calcs

    for (chateau_id, trips_compressed_data) in &trip_compressed_btreemap_by_chateau {
        //call catenary::find_service_ranges

        let mut valid_trips: HashMap<String, Vec<ValidTripSet>> = HashMap::new();

        for (trip_id, trip_compressed) in trips_compressed_data.iter() {
            let service_id = trip_compressed.service_id.clone();

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
                    //println!(
                    //    "trip: {}, route_id{}, itin_rows {:#?}, Adding dates {:?}",
                    //    trip_id.clone(),
                    //    trip_compressed.route_id.clone(),
                    //    itinerary_rows,
                    //    dates
                    //);

                    for date in dates {
                        let t = ValidTripSet {
                            chateau_id: chateau_id.clone(),
                            trip_id: (&trip_compressed.trip_id).into(),
                            timezone: chrono_tz::Tz::from_str(itin_for_this_trip.timezone.as_str())
                                .ok(),
                            frequencies: freq_converted.clone(),
                            trip_service_date: date.0,
                            //TODO, fix eventually, cannot be all the itin rows
                            itinerary_options: std::sync::Arc::new(
                                itinerary_rows
                                    .iter()
                                    .map(|itin_row| ItinOption {
                                        arrival_time_since_start: itin_row.arrival_time_since_start,
                                        departure_time_since_start: itin_row
                                            .departure_time_since_start,
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
                            ),
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
                            std::collections::hash_map::Entry::Vacant(mut ve) => {
                                ve.insert(vec![t]);
                            }
                        }
                    }
                }
            }
        }

        //loop through valid trips and assign event now

        for valid_trips in valid_trips.values() {
            for valid_trip in valid_trips.iter() {
                if valid_trip.frequencies.is_none() {
                    //ensure these are filtered later TODO
                    for itin_option in valid_trip.itinerary_options.iter() {
                        let mut trip_cancelled: bool = false;
                        let mut trip_deleted: bool = false;
                        let mut stop_cancelled: bool = false;

                        let mut departure_time_rt: Option<u64> = None;
                        let mut platform: Option<String> = None;
                        let mut arrival_time_rt: Option<u64> = None;

                        let mut trip_update_for_event: Option<&AspenisedTripUpdate> = None;

                        if let Some(index) = alert_indices.get(chateau_id) {
                            let relevant_alerts =
                                index.search(&valid_trip.route_id, &valid_trip.trip_id);
                            for alert in relevant_alerts {
                                if alert.effect == Some(1) {
                                    // NO_SERVICE
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
                                        let has_stop_id = alert
                                            .informed_entity
                                            .iter()
                                            .any(|e| e.stop_id.is_some());

                                        if has_stop_id {
                                            // If it has a stop_id, check if it matches OUR stop_id
                                            let matches_stop =
                                                alert.informed_entity.iter().any(|e| {
                                                    match &e.stop_id {
                                                        Some(s) => {
                                                            s.as_str()
                                                                == itin_option.stop_id.as_str()
                                                        }
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

                        let mut vehicle_num: Option<String> = None;

                        if let Some(gtfs_trip_aspenised) =
                            chateau_to_trips_aspenised.get(chateau_id.as_str())
                        {
                            if let Some(trip_update_ids) = gtfs_trip_aspenised
                                .trip_id_to_trip_update_ids
                                .get(valid_trip.trip_id.as_str())
                            {
                                if !trip_update_ids.is_empty() {
                                    // let trip_update_id = trip_rt[0].clone();

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

                                    if trip_updates.len() > 0 {
                                        trip_update_for_event = Some(trip_updates[0].1);

                                        let trip_update = trip_updates[0].1;
                                        let trip_update_id = trip_updates[0].0;

                                        if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                            trip_cancelled = true;
                                        } else if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted)
                                        {
                                            trip_deleted = true;
                                        } else {
                                            let relevant_stop_time_update =
                                                trip_update.stop_time_update.iter().find(|x| {
                                                    x.stop_id
                                                        .as_ref()
                                                        .map(|compare| compare.as_str())
                                                        == Some(&itin_option.stop_id)
                                                });

                                            if let Some(relevant_stop_time_update) =
                                                relevant_stop_time_update
                                            {

                                                if let (Some(departure), Some(arrival)) =
                                                    (&relevant_stop_time_update.departure, &relevant_stop_time_update.arrival) {
                                                        if let Some(time) = departure.time {
                                                            departure_time_rt = Some(time as u64);
                                                        
                                                        }

                                                        if let Some(time) = arrival.time {
                                                            arrival_time_rt = Some(time as u64);
                                                        }
                                                    } else {
                                                        
                                                if let Some(departure) =
                                                    &relevant_stop_time_update.departure
                                                {
                                                    if let Some(time) = departure.time {
                                                        departure_time_rt = Some(time as u64);
                                                    }
                                                } else {
                                                    if let Some(arrival) =
                                                        &relevant_stop_time_update.arrival
                                                    {
                                                        if let Some(time) = arrival.time {
                                                            departure_time_rt = Some(time as u64);
                                                        }
                                                    }
                                                }
                                                    }


                                                if let Some(platform_id) =
                                                    &relevant_stop_time_update.platform_string
                                                {
                                                    platform = Some(platform_id.to_string());
                                                }

                                                if let Some(trip_vehicle) = &trip_update.vehicle {
                                                    if let Some(label) = &trip_vehicle.label {
                                                        
                                                        vehicle_num = Some(label.to_string());
                                                    } else {
                                                        if let Some(id) = &trip_vehicle.id {
                                                            vehicle_num = Some(id.to_string());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(trip_update) = trip_update_for_event {
                            if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                trip_cancelled = true;
                            }

                            if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted) {
                               trip_deleted = true;
                            }
                        }

                        //add to stop event list

                        let midnight_of_service_date_unix_time =
                            valid_trip.reference_start_of_service_date.timestamp() as u64;

                        let last_stop_in_direction = direction_to_rows_by_chateau
                            .get(chateau_id.as_str())
                            .unwrap()
                            .get(&valid_trip.direction_pattern_id)
                            .unwrap()
                            .last()
                            .unwrap();

                        let itin_option_contains_multiple_of_last_stop = valid_trip
                            .itinerary_options
                            .iter()
                            .filter(|itin_option| {
                                itin_option.stop_id == last_stop_in_direction.stop_id
                            })
                            .count()
                            > 1;

                        let direction_meta = direction_meta_btreemap_by_chateau
                            .get(chateau_id.as_str())
                            .unwrap()
                            .get(&valid_trip.direction_pattern_id)
                            .unwrap();

                        events.push(StopEvent {
                            last_stop: match itin_option_contains_multiple_of_last_stop {
                                true => {
                                    itin_option.gtfs_stop_sequence
                                        == last_stop_in_direction.stop_sequence
                                }
                                false => last_stop_in_direction.stop_id == itin_option.stop_id,
                            },
                            scheduled_arrival: itin_option
                                .arrival_time_since_start
                                .map(|x| x as u64)
                                .map(|x| {
                                    x + midnight_of_service_date_unix_time
                                        + valid_trip.trip_start_time as u64
                                }),
                            scheduled_departure: itin_option
                                .departure_time_since_start
                                .map(|x| x as u64)
                                .map(|x| {
                                    x + midnight_of_service_date_unix_time
                                        + valid_trip.trip_start_time as u64
                                }),
                            chateau: chateau_id.clone(),
                            trip_id: valid_trip.trip_id.clone().to_string(),
                            stop_id: itin_option.stop_id.clone().to_string(),
                            stop_cancelled: stop_cancelled,
                            trip_cancelled: trip_cancelled,
                            trip_deleted: trip_deleted,
                            trip_modified: false,
                            realtime_arrival: arrival_time_rt,
                            realtime_departure: departure_time_rt,
                            platform_code: None,
                            headsign: match &direction_meta.stop_headsigns_unique_list {
                                Some(headsign_list) => {
                                    let matching_direction_rows = direction_to_rows_by_chateau
                                        .get(chateau_id.as_str())
                                        .unwrap()
                                        .get(&valid_trip.direction_pattern_id)
                                        .unwrap()
                                        .iter()
                                        .filter(|x| x.stop_id == itin_option.stop_id)
                                        .collect::<Vec<_>>();

                                    let matching_direction_row = match matching_direction_rows.len()
                                    {
                                        0 => None,
                                        1 => Some(matching_direction_rows[0]),
                                        _ => {
                                            let matching_direction_rows = matching_direction_rows
                                                .iter()
                                                .filter(|x| {
                                                    x.stop_sequence
                                                        == itin_option.gtfs_stop_sequence
                                                })
                                                .map(|x| *x)
                                                .collect::<Vec<_>>();

                                            match matching_direction_rows.len() {
                                                0 => None,
                                                1 => Some(matching_direction_rows[0]),
                                                _ => None,
                                            }
                                        }
                                    };

                                    let matching_headsign = match matching_direction_row {
                                        Some(matching_direction_row) => {
                                            match &matching_direction_row.stop_headsign_idx {
                                                Some(stop_headsign_idx) => match headsign_list
                                                    .get(*stop_headsign_idx as usize)
                                                {
                                                    Some(x) => x.clone(),
                                                    None => None,
                                                },
                                                None => None,
                                            }
                                        }
                                        None => None,
                                    };

                                    matching_headsign
                                }
                                None => match &itin_option.trip_headsign {
                                    Some(headsign) => Some(headsign.to_string()),
                                    None => Some(
                                        direction_meta_btreemap_by_chateau
                                            .get(chateau_id.as_str())
                                            .unwrap()
                                            .get(&valid_trip.direction_pattern_id)
                                            .unwrap()
                                            .headsign_or_destination
                                            .clone(),
                                    ),
                                },
                            },
                            route_id: valid_trip.route_id.clone().to_string(),
                            vehicle_number: vehicle_num,
                            level_id: None,
                            uses_primary_stop: true,
                            unscheduled_trip: false,
                            moved_info: None,
                            platform_string_realtime: platform,
                            trip_short_name: valid_trip.trip_short_name.clone(),
                            service_date: Some(valid_trip.trip_service_date.clone()),
                            scheduled_trip_shape_id: direction_meta_btreemap_by_chateau
                                .get(chateau_id.as_str())
                                .unwrap()
                                .get(&valid_trip.direction_pattern_id)
                                .map(|d| d.gtfs_shape_id.clone())
                                .flatten()
                                .map(|x| x.into()),
                        })
                    }
                } else {
                    let frequencies = valid_trip.frequencies.as_ref().unwrap();

                    // trip start time array
                    let mut trip_start_times: Vec<u32> = frequencies
                        .iter()
                        .flat_map(|frequency| {
                            (frequency.start_time..=frequency.end_time)
                                .step_by(frequency.headway_secs as usize)
                                .collect::<Vec<u32>>()
                        })
                        .collect();

                    trip_start_times.sort();
                    trip_start_times.dedup();

                    for scheduled_frequency_start_time in trip_start_times {
                        // We must iterate through the specific options for this stop (usually 1, but can be more)
                        for itin_option in valid_trip.itinerary_options.iter() {
                            let mut trip_cancelled: bool = false;
                            let mut trip_deleted: bool = false;
                            let mut stop_cancelled: bool = false;

                            let mut departure_time_rt: Option<u64> = None;
                            let mut arrival_time_rt: Option<u64> = None;
                            let mut platform: Option<String> = None;
                            let mut vehicle_num: Option<String> = None;

                            let mut trip_update_for_event: Option<&AspenisedTripUpdate> = None;

                            // 1. Realtime Data Fetching (With Frequency Start Time Filter)
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
                                                // Frequency Specific Logic: Filter by start_time
                                                .filter(|(_x, trip_update)| {
                                                    trip_update
                                                        .trip
                                                        .start_time
                                                        .as_ref()
                                                        .and_then(|trip_update_start_time| {
                                                            catenary::convert_hhmmss_to_seconds(
                                                                trip_update_start_time,
                                                            )
                                                        })
                                                        .map_or(false, |seconds| {
                                                            scheduled_frequency_start_time
                                                                == seconds
                                                        })
                                                })
                                                .filter(|(_x, trip_update)| {
                                                    match does_trip_set_use_dates {
                                                        true => {
                                                            trip_update.trip.start_date
                                                                == Some(
                                                                    valid_trip.trip_service_date,
                                                                )
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

                                            if trip_update.trip.schedule_relationship
                                                == Some(
                                                    catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled,
                                                )
                                            {
                                                trip_cancelled = true;
                                            } else if trip_update.trip.schedule_relationship
                                                == Some(
                                                    catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted,
                                                )
                                            {
                                                trip_deleted = true;
                                            } else {
                                                let relevant_stop_time_update = trip_update
                                                    .stop_time_update
                                                    .iter()
                                                    .find(|x| {
                                                        x.stop_id.as_ref().map(|compare| compare.as_str())
                                                            == Some(&itin_option.stop_id)
                                                    });

                                                if let Some(relevant_stop_time_update) =
                                                    relevant_stop_time_update
                                                {
                                                    if let Some(departure) =
                                                        &relevant_stop_time_update.departure
                                                    {
                                                        if let Some(time) = departure.time {
                                                            departure_time_rt = Some(time as u64);
                                                        }
                                                    }
                                                    if let Some(arrival) =
                                                        &relevant_stop_time_update.arrival
                                                    {
                                                        if let Some(time) = arrival.time {
                                                            arrival_time_rt = Some(time as u64);
                                                        }
                                                    }

                                                    if let Some(platform_id) =
                                                        &relevant_stop_time_update.platform_string
                                                    {
                                                        platform = Some(platform_id.to_string());
                                                    }

                                                     if let Some(schedule_relationship) = &relevant_stop_time_update.schedule_relationship {
                                                            match &schedule_relationship {
                                                                AspenisedStopTimeScheduleRelationship::Skipped => {
                                                                    stop_cancelled = true;
                                                                },
                                                                _ => {

                                                                }
                                                            }
                                                        }
                                                }

                                                if let Some(trip_vehicle) = &trip_update.vehicle {
                                                    if let Some(label) = &trip_vehicle.label {
                                                        vehicle_num = Some(label.to_string());
                                                    } else if let Some(id) = &trip_vehicle.id {
                                                        vehicle_num = Some(id.to_string());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // 2. Alert Logic (Recalculated for Frequency Offset)
                            if let Some(index) = alert_indices.get(chateau_id) {
                                let relevant_alerts =
                                    index.search(&valid_trip.route_id, &valid_trip.trip_id);
                                for alert in relevant_alerts {
                                    if alert.effect == Some(1) {
                                        // NO_SERVICE
                                        // Use the frequency specific start time for the event time calculation
                                        let event_time = valid_trip
                                            .reference_start_of_service_date
                                            .timestamp()
                                            as u64
                                            + scheduled_frequency_start_time as u64
                                            + itin_option.departure_time_since_start.unwrap_or(0)
                                                as u64;

                                        let is_active = alert.active_period.iter().any(|ap| {
                                            let start = ap.start.unwrap_or(0);
                                            let end = ap.end.unwrap_or(u64::MAX);
                                            event_time >= start && event_time <= end
                                        });
                                        if is_active {
                                            // Check if any informed entity has a stop_id
                                            let has_stop_id = alert
                                                .informed_entity
                                                .iter()
                                                .any(|e| e.stop_id.is_some());

                                            if has_stop_id {
                                                // If it has a stop_id, check if it matches OUR stop_id
                                                let matches_stop = alert
                                                    .informed_entity
                                                    .iter()
                                                    .any(|e| match &e.stop_id {
                                                        Some(s) => {
                                                            s.as_str()
                                                                == itin_option.stop_id.as_str()
                                                        }
                                                        None => false,
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

                            let midnight_of_service_date_unix_time =
                                valid_trip.reference_start_of_service_date.timestamp() as u64;

                            let last_stop_in_direction = direction_to_rows_by_chateau
                                .get(chateau_id.as_str())
                                .unwrap()
                                .get(&valid_trip.direction_pattern_id)
                                .unwrap()
                                .last()
                                .unwrap();

                            let itin_option_contains_multiple_of_last_stop = valid_trip
                                .itinerary_options
                                .iter()
                                .filter(|opt| opt.stop_id == last_stop_in_direction.stop_id)
                                .count()
                                > 1;

                            let direction_meta = direction_meta_btreemap_by_chateau
                                .get(chateau_id.as_str())
                                .unwrap()
                                .get(&valid_trip.direction_pattern_id)
                                .unwrap();

                            // 3. Push Event
                            if !trip_deleted {
                                events.push(StopEvent {
                                    last_stop: match itin_option_contains_multiple_of_last_stop {
                                        true => {
                                            itin_option.gtfs_stop_sequence
                                                == last_stop_in_direction.stop_sequence
                                        }
                                        false => {
                                            last_stop_in_direction.stop_id == itin_option.stop_id
                                        }
                                    },
                                    scheduled_arrival: itin_option
                                        .arrival_time_since_start
                                        .map(|x| x as u64)
                                        .map(|x| {
                                            x + midnight_of_service_date_unix_time
                                                + scheduled_frequency_start_time as u64
                                        }),
                                    scheduled_departure: itin_option
                                        .departure_time_since_start
                                        .map(|x| x as u64)
                                        .map(|x| {
                                            x + midnight_of_service_date_unix_time
                                                + scheduled_frequency_start_time as u64
                                        }),
                                    chateau: chateau_id.clone(),
                                    trip_id: valid_trip.trip_id.clone().to_string(),
                                    stop_id: itin_option.stop_id.clone().to_string(),
                                    stop_cancelled: stop_cancelled,
                                    trip_cancelled: trip_cancelled,
                                    trip_deleted: trip_deleted,
                                    trip_modified: false,
                                    realtime_arrival: arrival_time_rt,
                                    realtime_departure: departure_time_rt,
                                    platform_code: None,
                                    headsign: match &direction_meta.stop_headsigns_unique_list {
                                        Some(headsign_list) => {
                                            let matching_direction_rows =
                                                direction_to_rows_by_chateau
                                                    .get(chateau_id.as_str())
                                                    .unwrap()
                                                    .get(&valid_trip.direction_pattern_id)
                                                    .unwrap()
                                                    .iter()
                                                    .filter(|x| x.stop_id == itin_option.stop_id)
                                                    .collect::<Vec<_>>();

                                            let matching_direction_row =
                                                match matching_direction_rows.len() {
                                                    0 => None,
                                                    1 => Some(matching_direction_rows[0]),
                                                    _ => {
                                                        let matching_direction_rows =
                                                            matching_direction_rows
                                                                .iter()
                                                                .filter(|x| {
                                                                    x.stop_sequence
                                                                        == itin_option
                                                                            .gtfs_stop_sequence
                                                                })
                                                                .map(|x| *x)
                                                                .collect::<Vec<_>>();

                                                        match matching_direction_rows.len() {
                                                            0 => None,
                                                            1 => Some(matching_direction_rows[0]),
                                                            _ => None,
                                                        }
                                                    }
                                                };

                                            match matching_direction_row {
                                                Some(matching_direction_row) => {
                                                    match &matching_direction_row.stop_headsign_idx
                                                    {
                                                        Some(stop_headsign_idx) => {
                                                            match headsign_list
                                                                .get(*stop_headsign_idx as usize)
                                                            {
                                                                Some(x) => x.clone(),
                                                                None => None,
                                                            }
                                                        }
                                                        None => None,
                                                    }
                                                }
                                                None => None,
                                            }
                                        }
                                        None => match &itin_option.trip_headsign {
                                            Some(headsign) => Some(headsign.to_string()),
                                            None => Some(
                                                direction_meta_btreemap_by_chateau
                                                    .get(chateau_id.as_str())
                                                    .unwrap()
                                                    .get(&valid_trip.direction_pattern_id)
                                                    .unwrap()
                                                    .headsign_or_destination
                                                    .clone(),
                                            ),
                                        },
                                    },
                                    route_id: valid_trip.route_id.clone().to_string(),
                                    vehicle_number: vehicle_num,
                                    level_id: None,
                                    uses_primary_stop: true,
                                    unscheduled_trip: false,
                                    moved_info: None,
                                    platform_string_realtime: platform,
                                    trip_short_name: valid_trip.trip_short_name.clone(),
                                    service_date: Some(valid_trip.trip_service_date.clone()),
                                    scheduled_trip_shape_id: direction_meta_btreemap_by_chateau
                                        .get(chateau_id.as_str())
                                        .unwrap()
                                        .get(&valid_trip.direction_pattern_id)
                                        .map(|d| d.gtfs_shape_id.clone())
                                        .flatten()
                                        .map(|x| x.into()),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    //look through unscheduled trips

    for (chateau_id, unscheduled_trips) in &chateau_to_nonscheduled_trips_aspenised {
        let matching_stop_ids = &stops_to_search.get(chateau_id.as_str());

        for trip_update in unscheduled_trips {
            if let Some(matching_stu) =
                trip_update
                    .stop_time_update
                    .iter()
                    .find(|stu| match &stu.stop_id {
                        Some(stu_stop_id) => {
                            stu_stop_id.as_str() == stop.gtfs_id.as_str()
                                || match matching_stop_ids {
                                    None => false,
                                    Some(matching_stop_ids) => {
                                        matching_stop_ids.contains(&(stu_stop_id.to_string()))
                                    }
                                }
                        }
                        None => false,
                    })
            {
                let realtime_arrival = matching_stu
                    .arrival
                    .as_ref()
                    .map(|a| a.time.map(|t| t as u64))
                    .flatten();
                let realtime_departure = matching_stu
                    .departure
                    .as_ref()
                    .map(|d| d.time.map(|t| t as u64))
                    .flatten();

                let vehicle_number = trip_update
                    .vehicle
                    .as_ref()
                    .map(|x| x.label.clone())
                    .flatten();

                let service_date = trip_update.trip.start_date;

                let mut trip_cancelled = false;
                let mut stop_cancelled = false;
                let mut trip_deleted = false;

                let route_id = trip_update.trip.route_id.clone().unwrap_or(String::new());

                let mut headsign = None;

                let mut short_name = None;

                if let Some(trip_properties) = &trip_update.trip_properties {
                    if let Some(trip_headsign) = &trip_properties.trip_headsign {
                        headsign = Some(trip_headsign.clone());
                    }

                    if let Some(trip_short_name) = &trip_properties.trip_short_name {
                        short_name = Some(trip_short_name.into());
                    }
                }

                if let Some(schedule_relationship) = &matching_stu.schedule_relationship {
                    match schedule_relationship {
                        AspenisedStopTimeScheduleRelationship::Skipped => {
                            stop_cancelled = true;
                        }
                        _ => {}
                    }
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

                let stopevent = StopEvent {
                    scheduled_arrival: None,
                    scheduled_departure: None,
                    realtime_arrival: realtime_arrival,
                    realtime_departure: realtime_departure,
                    trip_modified: false,
                    uses_primary_stop: true,
                    unscheduled_trip: true,
                    moved_info: None,
                    platform_string_realtime: None,
                    headsign: headsign,
                    trip_short_name: short_name,
                    trip_id: trip_update.trip.trip_id.clone().unwrap_or(String::new()),
                    level_id: None,
                    route_id: route_id,
                    platform_code: None,
                    last_stop: false,
                    scheduled_trip_shape_id: None,
                    chateau: chateau_id.clone(),
                    stop_id: match &matching_stu.stop_id {
                        Some(sid) => sid.to_string(),
                        None => String::new(),
                    },
                    vehicle_number: vehicle_number,
                    service_date: service_date,
                    trip_cancelled: trip_cancelled,
                    stop_cancelled: stop_cancelled,
                    trip_deleted: trip_deleted,
                };

                events.push(stopevent);
            }
        }
    }

    //sort the event list

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

    let response = NearbyFromStopsResponse {
        primary: StopInfoResponse {
            chateau: stop.chateau,
            stop_id: stop.gtfs_id,
            stop_name: stop.name.unwrap_or_default(),
            stop_lat: stop.point.unwrap().y,
            stop_lon: stop.point.unwrap().x,
            stop_code: stop.code,
            level_id: stop.level_id,
            platform_code: stop.platform_code,
            parent_station: stop.parent_station,
            children_ids: vec![],
            timezone: stop_tz_txt.clone(),
            stop_name_translations: catenary::serde_value_to_translated_hashmap(
                &stop.name_translations,
            ),
        },
        parent: match parent {
            Some(parent) => Some(StopInfoResponse {
                chateau: parent.chateau,
                stop_id: parent.gtfs_id,
                stop_name: parent.name.unwrap_or_default(),
                stop_lat: parent.point.unwrap().y,
                stop_lon: parent.point.unwrap().x,
                stop_code: parent.code,
                level_id: parent.level_id,
                platform_code: parent.platform_code,
                parent_station: parent.parent_station,
                children_ids: vec![],
                timezone: stop_tz_txt.clone(),
                stop_name_translations: catenary::serde_value_to_translated_hashmap(
                    &stop.name_translations,
                ),
            }),
            None => None,
        },
        children_and_related: vec![],
        events: events,
        routes: routes,
        shapes: shapes,
        alerts: alerts,
        agencies: agencies,
    };

    HttpResponse::Ok().json(response)
}
