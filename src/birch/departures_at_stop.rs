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
use amtrak_gtfs_rt::asm::Stop;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::TripsSelectionResponse;
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
use geo::coord;
use futures::future::join_all;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItinOption {
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub interpolated_time_since_start: Option<i32>,
    pub stop_id: CompactString,
    pub gtfs_stop_sequence: u32,
    pub trip_headsign: Option<String>,
    pub trip_headsign_translations: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ValidTripSet {
    pub chateau_id: String,
    pub trip_id: CompactString,
    pub frequencies: Option<Vec<gtfs_structures::Frequency>>,
    pub trip_service_date: NaiveDate,
    pub itinerary_options: Vec<ItinOption>,
    pub reference_start_of_service_date: chrono::DateTime<chrono_tz::Tz>,
    pub itinerary_pattern_id: String,
    pub direction_pattern_id: String,
    pub route_id: CompactString,
    pub timezone: Option<chrono_tz::Tz>,
    pub trip_start_time: u32,
    pub trip_short_name: Option<CompactString>,
    pub service_id: CompactString,
}

// should be able to detect when a stop has detoured to this stop or detoured away from this stop

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromStops {
    //serialise and deserialise using serde_json into Vec<NearbyStopsDeserialize>
    stop_id: String,
    chateau_id: String,
    greater_than_time: Option<u64>,
    less_than_time: Option<u64>,
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
    service_date: NaiveDate,
    last_stop: bool,
    scheduled_trip_shape_id: Option<CompactString>,
}

#[derive(Serialize, Clone, Debug)]
pub struct MovedStopData {
    stop_id: String,
    scheduled_arrival: Option<u64>,
    scheduled_departure: Option<u64>,
    realtime_arrival: Option<u64>,
    realtime_departure: Option<u64>,
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
}

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

    let mut calender_responses: Vec<_> = vec![];
    let mut calendar_dates_responses: Vec<_> = vec![];

    let mut futures = Vec::new();
    for (chateau_id_to_search, stop_id_to_search) in &stops_to_search {
        let pool = Arc::clone(&pool);
        let chateau_id = chateau_id_to_search.clone();
        let stop_ids = stop_id_to_search.clone();

        futures.push(async move {
            let mut conn = pool.get().await.unwrap();

            let itins = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(&stop_ids))
                .select(catenary::models::ItineraryPatternRow::as_select())
                .load::<catenary::models::ItineraryPatternRow>(&mut conn)
                .await
                .unwrap_or_default();

            let mut itins_btreemap = BTreeMap::<String, Vec<catenary::models::ItineraryPatternRow>>::new();
            for itin in itins.iter() {
                itins_btreemap
                    .entry(itin.itinerary_pattern_id.clone())
                    .or_insert_with(Vec::new)
                    .push(itin.clone());
            }

            let itinerary_list: Vec<String> = itins_btreemap.keys().cloned().collect();

            let itin_meta = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::itinerary_pattern_meta::itinerary_pattern_id.eq_any(&itinerary_list))
                .select(catenary::models::ItineraryPatternMeta::as_select())
                .load::<catenary::models::ItineraryPatternMeta>(&mut conn)
                .await
                .unwrap_or_default();

            let mut itin_meta_btreemap = BTreeMap::<String, catenary::models::ItineraryPatternMeta>::new();
            for itin in &itin_meta {
                itin_meta_btreemap.insert(itin.itinerary_pattern_id.clone(), itin.clone());
            }

            let direction_ids_to_search: Vec<String> = itin_meta
                .iter()
                .filter_map(|x| x.direction_pattern_id.clone())
                .collect();

            let direction_meta = catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                .filter(catenary::schema::gtfs::direction_pattern_meta::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::direction_pattern_meta::direction_pattern_id.eq_any(&direction_ids_to_search))
                .select(catenary::models::DirectionPatternMeta::as_select())
                .load::<catenary::models::DirectionPatternMeta>(&mut conn)
                .await
                .unwrap_or_default();

            let mut direction_meta_btreemap = BTreeMap::<String, catenary::models::DirectionPatternMeta>::new();
            let mut shape_ids_to_fetch_for_this_chateau = BTreeSet::new();
            for direction in &direction_meta {
                if let Some(shape_id) = &direction.gtfs_shape_id {
                    shape_ids_to_fetch_for_this_chateau.insert(shape_id.clone());
                }
                direction_meta_btreemap.insert(direction.direction_pattern_id.clone(), direction.clone());
            }

            let shapes_result = catenary::schema::gtfs::shapes::dsl::shapes
                .filter(catenary::schema::gtfs::shapes::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::shapes::shape_id.eq_any(&shape_ids_to_fetch_for_this_chateau))
                .load::<catenary::models::Shape>(&mut conn)
                .await
                .unwrap_or_default();

            let mut shape_polyline_for_chateau: BTreeMap<EcoString, String> = BTreeMap::new();
            for db_shape in shapes_result {
                let shape_polyline = polyline::encode_coordinates(
                    geo::LineString::new(
                        db_shape.linestring.points.iter().map(|point| coord! { x: point.x, y: point.y }).collect::<Vec<_>>(),
                    ),
                    5,
                ).unwrap();
                shape_polyline_for_chateau.insert(db_shape.shape_id.clone().into(), shape_polyline);
            }

            let direction_row_query = catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
                .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::direction_pattern::direction_pattern_id.eq_any(&direction_ids_to_search))
                .select(catenary::models::DirectionPatternRow::as_select())
                .load::<catenary::models::DirectionPatternRow>(&mut conn)
                .await
                .unwrap_or_default();

            let mut direction_rows_for_chateau = BTreeMap::<String, Vec<catenary::models::DirectionPatternRow>>::new();
            for direction_row in direction_row_query {
                let entry = direction_rows_for_chateau.entry(direction_row.direction_pattern_id.clone()).or_insert_with(Vec::new);
                entry.push(direction_row);
                entry.sort_by_key(|x| x.stop_sequence);
            }

            let route_ids: Vec<CompactString> = itin_meta.iter().map(|x| x.route_id.clone()).collect();

            let routes_ret = catenary::schema::gtfs::routes::dsl::routes
                .filter(catenary::schema::gtfs::routes::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::routes::route_id.eq_any(&route_ids))
                .select(catenary::models::Route::as_select())
                .load::<catenary::models::Route>(&mut conn)
                .await
                .unwrap_or_default();

            let mut routes_btreemap = BTreeMap::<String, catenary::models::Route>::new();
            for route in &routes_ret {
                routes_btreemap.insert(route.route_id.clone().into(), route.clone());
            }

            let trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::trips_compressed::itinerary_pattern_id.eq_any(&itinerary_list))
                .select(catenary::models::CompressedTrip::as_select())
                .load::<catenary::models::CompressedTrip>(&mut conn)
                .await
                .unwrap_or_default();

            let mut trip_compressed_btreemap = BTreeMap::<String, catenary::models::CompressedTrip>::new();
            for trip in &trips {
                trip_compressed_btreemap.insert(trip.trip_id.clone(), trip.clone());
            }

            let service_ids_to_search: BTreeSet<CompactString> = trips.iter().map(|x| x.service_id.clone()).collect();

            let calendar = catenary::schema::gtfs::calendar::dsl::calendar
                .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&service_ids_to_search))
                .select(catenary::models::Calendar::as_select())
                .load::<catenary::models::Calendar>(&mut conn)
                .await
                .unwrap_or_default();

            let calendar_dates = catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::calendar_dates::service_id.eq_any(&service_ids_to_search))
                .select(catenary::models::CalendarDate::as_select())
                .load::<catenary::models::CalendarDate>(&mut conn)
                .await
                .unwrap_or_default();

            (
                chateau_id,
                itins_btreemap,
                itin_meta_btreemap,
                direction_meta_btreemap,
                trip_compressed_btreemap,
                direction_rows_for_chateau,
                routes_btreemap,
                shape_polyline_for_chateau,
                calendar,
                calendar_dates,
            )
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
        shapes.insert(chateau_id.clone().into(), shape_polyline_for_chateau);
        calender_responses.push(calendar);
        calendar_dates_responses.push(calendar_dates);
    }

    let calendar_structure =
        make_calendar_structure_from_pg(calender_responses, calendar_dates_responses).unwrap();

    let point_raw = stop.point.clone().unwrap();
    let stop_tz_txt = match &stop.timezone {
        Some(tz) => tz.clone(),
        None => {
            match (-90.0..=90.0).contains(&point_raw.y) && (-180.0..=180.0).contains(&point_raw.x) {
                true => tz_search::lookup(point_raw.y, point_raw.x)
                    .unwrap_or_else(|| String::from("Etc/GMT")),
                false => String::from("Etc/GMT"),
            }
        }
    };

    //query added trips and modifications by stop id, and also matching trips in chateau

    //requery all the missing trips, and their itins and directions if they exist

    //compute new scheduled time for the stop for trip modifications

    let stop_tz_txt = match &stop.timezone {
        Some(tz) => tz.clone(),
        None => {
            match -90.0 <= point_raw.y
                && point_raw.y <= 90.0
                && -180.0 <= point_raw.x
                && point_raw.x <= 180.0
            {
                true => tz_search::lookup(point_raw.y, point_raw.x)
                    .unwrap_or_else(|| String::from("Etc/GMT")),
                false => String::from("Etc/GMT"),
            }
        }
    };

    let stop_tz = chrono_tz::Tz::from_str_insensitive(&stop_tz_txt).unwrap();

    // Convert bounds to DateTime in stop tz (start anchor for service search) and UTC for comparisons
    let gt_utc = chrono::DateTime::from_timestamp(greater_than_time as i64, 0).unwrap();
    let gt_in_stop_tz = gt_utc.with_timezone(&stop_tz);
    let lt_utc = chrono::DateTime::from_timestamp(less_than_time as i64, 0).unwrap();

    //convert greater than time to DateTime Tz

    let greater_than_time_utc =
        chrono::DateTime::from_timestamp(greater_than_time as i64, 0).unwrap();

    let greater_than_date_time = greater_than_time_utc.with_timezone(&stop_tz);

    //seek back a minimum of 8 days

    let less_than_time_utc = chrono::DateTime::from_timestamp(less_than_time as i64, 0).unwrap();
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

    let mut alerts: BTreeMap<String, BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>> =
        BTreeMap::new();

    let mut aspen_futures = Vec::new();
    for (chateau_id, trips_compressed_data) in &trip_compressed_btreemap_by_chateau {
        if let Some(chateau_metadata_for_c) = chateau_metadata.get(chateau_id) {
            let chateau_id = chateau_id.clone();
            let trips_to_get = trips_compressed_data.keys().cloned().collect::<Vec<String>>();
            let socket = chateau_metadata_for_c.socket.clone();

            aspen_futures.push(async move {
                let client = match catenary::aspen::lib::spawn_aspen_client_from_ip(&socket).await {
                    Ok(client) => client,
                    Err(e) => {
                        eprintln!("Error creating aspen client for {}: {:?}", chateau_id, e);
                        return (chateau_id, None, None);
                    }
                };

                let alerts_future = client.get_all_alerts(tarpc::context::current(), chateau_id.clone());
                let trips_future = client.get_all_trips_with_ids(tarpc::context::current(), chateau_id.clone(), trips_to_get);

                let (alerts_res, trips_res) = tokio::join!(alerts_future, trips_future);

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

                (chateau_id, alerts_data, trips_data)
            });
        }
    }

    let aspen_results = join_all(aspen_futures).await;

    for (chateau_id, alerts_opt, trips_opt) in aspen_results {
        if let Some(all_alerts) = alerts_opt {
            alerts.insert(chateau_id.clone(), all_alerts.into_iter().collect());
        }
        if let Some(trips) = trips_opt {
            chateau_to_trips_aspenised.insert(chateau_id, trips);
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
                    let route_match = entity
                        .route_id
                        .as_ref()
                        .map_or(false, |r_id| relevant_routes.contains(r_id));
                    let trip_match = entity.trip.as_ref().map_or(false, |t| {
                        t.trip_id
                            .as_ref()
                            .map_or(false, |t_id| relevant_trips.contains(t_id))
                    });
                    let stop_match = entity
                        .stop_id
                        .as_ref()
                        .map_or(false, |s_id| relevant_stops.contains(s_id));

                    // An entity is relevant if it matches a route, trip, or stop we are looking at.
                    // If an entity selector is broad (e.g., no specific route/trip/stop), we should include it if it's for the agency.
                    let is_general_alert = entity.route_id.is_none()
                        && entity.trip.is_none()
                        && entity.stop_id.is_none();

                    route_match || trip_match || stop_match || is_general_alert
                })
            })
            .map(|(id, alert)| (id.clone(), alert.clone()))
            .collect::<BTreeMap<_, _>>();

        if !chateau_filtered_alerts.is_empty() {
            filtered_alerts.insert(chateau_id.clone(), chateau_filtered_alerts);
        }
    }
    alerts = filtered_alerts;

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

            let itin_for_this_trip = itin_meta_btreemap_by_chateau
                .get(chateau_id)
                .unwrap()
                .get(&trip_compressed.itinerary_pattern_id)
                .unwrap();

            let itinerary_rows = itins_btreemap_by_chateau
                .get(chateau_id)
                .unwrap()
                .get(&trip_compressed.itinerary_pattern_id)
                .unwrap();

            let direction_meta = direction_meta_btreemap_by_chateau
                .get(chateau_id)
                .unwrap()
                .get(
                    itin_for_this_trip
                        .direction_pattern_id
                        .as_ref()
                        .unwrap()
                        .as_str(),
                )
                .unwrap();

            let itin_ref = itinerary_rows.last().unwrap();

            let time_since_start = match itin_ref.departure_time_since_start {
                Some(departure_time_since_start) => departure_time_since_start,
                None => match itin_ref.arrival_time_since_start {
                    Some(arrival) => arrival,
                    None => itin_ref.interpolated_time_since_start.unwrap_or(0),
                },
            };

            let t_to_find_schedule_for = catenary::TripToFindScheduleFor {
                trip_id: trip_id.clone(),
                chateau: chateau_id.clone(),
                timezone: chrono_tz::Tz::from_str(itin_for_this_trip.timezone.as_str()).unwrap(),
                frequency: freq_converted.clone(),
                itinerary_id: itin_for_this_trip.itinerary_pattern_id.clone(),
                direction_id: itin_for_this_trip.direction_pattern_id.clone().unwrap(),
                time_since_start_of_service_date: chrono::TimeDelta::new(
                    time_since_start.into(),
                    0,
                )
                .unwrap(),
            };

            let service = calendar_structure
                .get(chateau_id.as_str())
                .unwrap()
                .get(trip_compressed.service_id.as_str());

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
                                            .unwrap()[*idx as usize]
                                            .clone(),
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
                        let mut is_cancelled: bool = false;
                        let mut deleted: bool = false;

                        let mut departure_time_rt: Option<u64> = None;
                        let mut platform: Option<String> = None;
                        let mut arrival_time_rt: Option<u64> = None;

                        let mut trip_update_for_event: Option<&AspenisedTripUpdate> = None;

                        if let Some(chateau_alerts) = alerts.get(chateau_id) {
                            for alert in chateau_alerts.values() {
                                let effect_is_no_service = alert.effect == Some(1); // NO_SERVICE
                                if effect_is_no_service {
                                    let applies_to_trip = alert.informed_entity.iter().any(|e| {
                                        let route_match = e
                                            .route_id
                                            .as_ref()
                                            .map_or(false, |r_id| *r_id == valid_trip.route_id);
                                        let trip_match = e.trip.as_ref().map_or(false, |t| {
                                            t.trip_id
                                                .as_ref()
                                                .map_or(false, |t_id| *t_id == valid_trip.trip_id)
                                        });
                                        route_match || trip_match
                                    });

                                    if applies_to_trip {
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
                                            is_cancelled = true;
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
                                            .filter(
                                                |(x, trip_update)| match does_trip_set_use_dates {
                                                    true => {
                                                        trip_update.trip.start_date
                                                            == Some(
                                                                valid_trip.trip_service_date
                                                            )
                                                    }
                                                    false => {
                                                        //if there is only 1 trip update, assign it to the current service date

                                                        //what is the current trip offset from the reference start of service date

                                                        let trip_offset = itin_option.departure_time_since_start.unwrap_or(
                                                            itin_option.arrival_time_since_start.unwrap_or(
                                                                itin_option.interpolated_time_since_start.unwrap_or(0)
                                                            )
                                                        );

                                                        // get current naive date in the timezone from the earliest item in the trip update

                                                        let naive_date_approx_guess = trip_update.stop_time_update.iter()
                                                        .filter(|x| x.departure.is_some() || x.arrival.is_some())
                                                        .filter_map(|x| {
                                                            if let Some(departure) = &x.departure {
                                                                Some(departure.time)
                                                            } else {
                                                                if let Some(arrival) = &x.arrival {
                                                                    Some(arrival.time)
                                                                } else {
                                                                    None
                                                                }
                                                            }
                                                        }).flatten().min();

                                                        match naive_date_approx_guess {
                                                            Some(least_num) => {
                                                                let tz = valid_trip.timezone.as_ref().unwrap();

                                                                let rt_least_naive_date = tz.timestamp(least_num as i64, 0);

                                                                let approx_service_date_start = rt_least_naive_date - chrono::Duration::seconds(trip_offset as i64);

                                                                let approx_service_date = approx_service_date_start.date();

                                                                //score dates within 1 day of the service date
                                                                let mut vec_possible_dates: Vec<(chrono::Date<chrono_tz::Tz>, i64)> = vec![];

                                                                //iter from day before to day after

                                                                let day_before = approx_service_date - chrono::Duration::days(2);

                                                                for day in day_before.naive_local().iter_days().take(3) {
                                                                   //check service id for trip id, then check if calendar is allowed

                                                                     let service_id = valid_trip.service_id.as_str();

                                                                    let service = calendar_structure
                                                                    .get(chateau_id.as_str())
                                                                    .unwrap().get(service_id);

                                                                    if let Some(service) = service {
                                                                        if catenary::datetime_in_service(&service, day) {
                                                                            let day_in_tz_midnight = day.and_hms(12, 0, 0).and_local_timezone(*tz).unwrap() - chrono::Duration::hours(12);

                                                                            let time_delta = rt_least_naive_date.signed_duration_since(day_in_tz_midnight);
    
                                                                            vec_possible_dates.push((day_in_tz_midnight.date(), time_delta.num_seconds().abs()));
                                                                        }
                                                                    }
                                                                }

                                                                let best_service_date = vec_possible_dates.iter().min_by_key(|x| x.1);

                                                                match best_service_date {
                                                                    Some(best_service_date) => {
                                                                        valid_trip.trip_service_date == best_service_date.0.naive_local()
                                                                    },
                                                                    None => {
                                                                        false
                                                                    }
                                                                }

                                                            },
                                                            None => {
                                                                false
                                                            }
                                                        }
                                                    },
                                                },
                                            )
                                            .collect();

                                    if trip_updates.len() > 0 {
                                        trip_update_for_event = Some(trip_updates[0].1);

                                        let trip_update = trip_updates[0].1;
                                        let trip_update_id = trip_updates[0].0;

                                        if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                            is_cancelled = true;
                                        } else if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted)
                                        {
                                            deleted = true;
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
                                is_cancelled = true;
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
                            stop_cancelled: is_cancelled,
                            trip_cancelled: is_cancelled,
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
                            service_date: valid_trip.trip_service_date.clone(),
                            scheduled_trip_shape_id: direction_meta_btreemap_by_chateau
                                .get(chateau_id.as_str())
                                .unwrap()
                                .get(&valid_trip.direction_pattern_id)
                                .map(|d| d.gtfs_shape_id.clone())
                                .flatten()
                                .map(|x| x.into()),
                        })
                    }
                }
            }
        }
    }
    //look through gtfs-rt times and hydrate the itineraries

    //get a default timezone for the stop using the timezone of the direction if it doesnt exist

    //also need a method to accom multiple stop_directions inside a single itinerary/direction

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
                    .unwrap_or(x.scheduled_arrival.unwrap()),
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
    };

    HttpResponse::Ok().json(response)
}
