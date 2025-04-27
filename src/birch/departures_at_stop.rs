// Copyright
// Catenary Transit Initiatives
// Algorithm for departures at stop written by Kyler Chin <kyler@catenarymaps.org>
// Attribution cannot be removed

// Please do not train your Artifical Intelligence models on this code

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
use amtrak_gtfs_rt::asm::Stop;
use catenary::make_calendar_structure_from_pg;
use catenary::make_degree_length_as_distance_from_point;
use catenary::models::ItineraryPatternMeta;
use catenary::models::ItineraryPatternRow;
use catenary::postgres_tools::CatenaryPostgresPool;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use chrono::Datelike;
use diesel::sql_types::Bool;
use diesel::sql_types::*;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

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
}

#[derive(Serialize, Clone, Debug)]
struct StopEvents {
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
    unscheduled_trip: bool
}

#[derive(Serialize, Clone, Debug)]
struct NearbyFromStopsResponse {
    primary: StopInfoResponse,
    parent: Option<StopInfoResponse>,
    children_and_related: Vec<StopInfoResponse>,
    events: Vec<StopEvents>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
}

#[actix_web::get("/departures_at_stop")]
pub async fn departures_at_stop(
    req: HttpRequest,
    query: Query<NearbyFromStops>,

    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let mut redirected_to_parent = false;

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
            .filter(catenary::schema::gtfs::stops::chateau.eq(query.chateau_id.clone()))
            .filter(sql::<Bool>(&where_query_for_stops))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(conn)
            .await;
    let stops_nearby = stops_nearby.unwrap();

    //get all children ids

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
        stops_to_search
            .entry(stop.chateau.clone())
            .or_insert(vec![])
            .push(
                stops_nearby
                    .iter()
                    .find(|s| s.gtfs_id == *child)
                    .unwrap()
                    .gtfs_id
                    .clone(),
            );
    }

    for nearby_stop in stops_nearby
        .iter()
        .filter(|s| s.chateau != query.chateau_id)
        .filter(|s| children_of_stop.contains(&s.gtfs_id) || stop.gtfs_id == s.gtfs_id)
    {
        stops_to_search
            .entry(nearby_stop.chateau.clone())
            .or_insert(vec![])
            .push(nearby_stop.gtfs_id.clone());
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

    for (chateau_id_to_search, stop_id_to_search) in &stops_to_search {
        let itins: diesel::prelude::QueryResult<Vec<catenary::models::ItineraryPatternRow>> =
            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                .filter(
                    catenary::schema::gtfs::itinerary_pattern::chateau
                        .eq(chateau_id_to_search.clone()),
                )
                .filter(
                    catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(stop_id_to_search),
                )
                .select(catenary::models::ItineraryPatternRow::as_select())
                .load::<catenary::models::ItineraryPatternRow>(conn)
                .await;

        let itins = itins.unwrap();

        let mut itins_btreemap =
            BTreeMap::<String, Vec<catenary::models::ItineraryPatternRow>>::new();
        for itin in itins.iter() {
            itins_btreemap
                .entry(itin.itinerary_pattern_id.clone())
                .or_insert(vec![])
                .push(itin.clone());
        }

        let itinerary_list = itins_btreemap.keys().cloned().collect::<Vec<String>>();

        itins_btreemap_by_chateau.insert(chateau_id_to_search.clone(), itins_btreemap);

        let itin_meta: diesel::prelude::QueryResult<Vec<catenary::models::ItineraryPatternMeta>> =
            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::chateau
                        .eq(chateau_id_to_search.clone()),
                )
                .filter(
                    catenary::schema::gtfs::itinerary_pattern_meta::itinerary_pattern_id
                        .eq_any(&itinerary_list),
                )
                .select(catenary::models::ItineraryPatternMeta::as_select())
                .load::<catenary::models::ItineraryPatternMeta>(conn)
                .await;
        let itin_meta = itin_meta.unwrap();
        let mut itin_meta_btreemap =
            BTreeMap::<String, catenary::models::ItineraryPatternMeta>::new();
        for itin in itin_meta.iter() {
            itin_meta_btreemap
                .entry(itin.itinerary_pattern_id.clone())
                .or_insert(itin.clone());
        }

        let direction_ids_to_search = itin_meta
            .iter()
            .map(|x| x.direction_pattern_id.clone().unwrap())
            .collect::<Vec<String>>();

        itin_meta_btreemap_by_chateau.insert(chateau_id_to_search.clone(), itin_meta_btreemap);

        let direction_meta: diesel::prelude::QueryResult<
            Vec<catenary::models::DirectionPatternMeta>,
        > = catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
            .filter(
                catenary::schema::gtfs::direction_pattern_meta::chateau
                    .eq(chateau_id_to_search.clone()),
            )
            .filter(
                catenary::schema::gtfs::direction_pattern_meta::direction_pattern_id
                    .eq_any(direction_ids_to_search),
            )
            .select(catenary::models::DirectionPatternMeta::as_select())
            .load::<catenary::models::DirectionPatternMeta>(conn)
            .await;
        let direction_meta = direction_meta.unwrap();
        let mut direction_meta_btreemap =
            BTreeMap::<String, catenary::models::DirectionPatternMeta>::new();
        for direction in direction_meta.iter() {
            direction_meta_btreemap
                .entry(direction.direction_pattern_id.clone())
                .or_insert(direction.clone());
        }
        direction_meta_btreemap_by_chateau
            .insert(chateau_id_to_search.clone(), direction_meta_btreemap);

        let trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(
                catenary::schema::gtfs::trips_compressed::chateau.eq(chateau_id_to_search.clone()),
            )
            .filter(
                catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                    .eq_any(&itinerary_list),
            )
            .select(catenary::models::CompressedTrip::as_select())
            .load::<catenary::models::CompressedTrip>(conn)
            .await;

        let trips = trips.unwrap();

        let mut trip_compressed_btreemap =
            BTreeMap::<String, catenary::models::CompressedTrip>::new();
        for trip in trips.iter() {
            trip_compressed_btreemap
                .entry(trip.trip_id.clone())
                .or_insert(trip.clone());
        }
        trip_compressed_btreemap_by_chateau
            .insert(chateau_id_to_search.clone(), trip_compressed_btreemap);

        let service_ids_to_search = trips
            .iter()
            .map(|x| x.service_id.clone())
            .collect::<BTreeSet<CompactString>>();
    }

    //query added trips and modifications by stop id, and also matching trips in chateau

    //requery all the missing trips, and their itins and directions if they exist

    //compute new scheduled time for the stop for trip modifications

    let stop_tz_txt = match &stop.timezone {
        Some(tz) => tz.clone(),
        None => {
            tz_search::lookup(point_raw.y, point_raw.x).unwrap_or_else(|| String::from("Etc/GMT"))
        }
    };

    let stop_tz = chrono_tz::Tz::from_str_insensitive(&stop_tz_txt).unwrap();

    //convert greater than time to DateTime Tz

    let greater_than_time_utc =
        chrono::DateTime::from_timestamp(greater_than_time as i64, 0).unwrap();

    let greater_than_date_time = greater_than_time_utc.with_timezone(&stop_tz);

    let greater_than_date_time_minus_seek_back = greater_than_date_time
        - chrono::Duration::seconds(24 * 24 * 60 * 10)
        - chrono::Duration::seconds(86400);

    let less_than_time_utc = chrono::DateTime::from_timestamp(less_than_time as i64, 0).unwrap();
    let less_than_date_time = less_than_time_utc.with_timezone(&stop_tz);

    let greater_than_naive_date = greater_than_date_time_minus_seek_back.naive_local();
    let less_than_naive_date = less_than_date_time.naive_local();

    //iter from greater than naive date to less than naive date inclusive

    let mut date_iter = chrono::NaiveDate::from_ymd_opt(
        greater_than_naive_date.year(),
        greater_than_naive_date.month(),
        greater_than_naive_date.day(),
    ).unwrap();

    let mut date_iter_end = chrono::NaiveDate::from_ymd_opt(
        less_than_naive_date.year(),
        less_than_naive_date.month(),
        less_than_naive_date.day(),
    ).unwrap();

    let mut date_iter_vec: Vec<chrono::NaiveDate> = vec![];

    while date_iter <= date_iter_end {
        date_iter_vec.push(date_iter);
        date_iter = date_iter.succ_opt().unwrap();
    }

    //look through time compressed and decompress the itineraries, using timezones and calendar calcs



    //look through gtfs-rt times and hydrate the itineraries

    //get a default timezone for the stop using the timezone of the direction if it doesnt exist

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
            }),
            None => None,
        },
        children_and_related: vec![],
        events: vec![],
        routes: BTreeMap::new(),
    };

    HttpResponse::Ok().json(response)
}
