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
use catenary::postgres_tools::CatenaryPostgresPool;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
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

    let stop = stops[0].clone();

    //get all children ids

    let mut combined_ids_to_search = vec![stop.gtfs_id.clone()];

    for child in stop.children_ids.clone() {
        combined_ids_to_search.push(child.unwrap());
    }

    // search through itineraries

    let itins_btreemap_by_chateau: BTreeMap<String, BTreeMap<String, Vec<catenary::models::ItineraryPatternRow>>> = BTreeMap::new();
    let itin_meta_btreemap_by_chateau: BTreeMap<String, BTreeMap<String, Vec<catenary::models::ItineraryPatternMeta>>> = BTreeMap::new();

    let itins: diesel::prelude::QueryResult<Vec<catenary::models::ItineraryPatternRow>> =
        catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
            .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(query.chateau_id.clone()))
            .filter(
                catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(combined_ids_to_search),
            )
            .select(catenary::models::ItineraryPatternRow::as_select())
            .load::<catenary::models::ItineraryPatternRow>(conn)
            .await;

    let itins = itins.unwrap();

    let itins_ids = itins
        .iter()
        .map(|x| x.itinerary_pattern_id.clone())
        .collect::<Vec<String>>();

    let itin_meta: diesel::prelude::QueryResult<Vec<catenary::models::ItineraryPatternMeta>> =
        catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::chateau
                    .eq(query.chateau_id.clone()),
            )
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::itinerary_pattern_id
                    .eq_any(itins_ids.clone()),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load::<catenary::models::ItineraryPatternMeta>(conn)
            .await;

    let itin_meta = itin_meta.unwrap();

    let stop_tz_txt = match stop.timezone {
        Some(timezone) => timezone,
        None => itin_meta[0].timezone.clone(),
    };

    //get all matching directions

    let mut direction_ids = itin_meta
        .iter()
        .map(|x| x.direction_pattern_id.as_ref().unwrap().clone())
        .collect::<Vec<String>>();
    direction_ids.sort();
    direction_ids.dedup();

    let direction_meta =
        catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
            .filter(
                catenary::schema::gtfs::direction_pattern_meta::chateau
                    .eq(query.chateau_id.clone()),
            )
            .filter(
                catenary::schema::gtfs::direction_pattern_meta::direction_pattern_id
                    .eq_any(direction_ids.clone()),
            )
            .select(catenary::models::DirectionPatternMeta::as_select())
            .load::<catenary::models::DirectionPatternMeta>(conn)
            .await;

    let direction_meta = direction_meta.unwrap();

    let trips: diesel::prelude::QueryResult<Vec<catenary::models::CompressedTrip>> =
        catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(query.chateau_id.clone()))
            .filter(
                catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                    .eq_any(itins_ids.clone()),
            )
            .select(catenary::models::CompressedTrip::as_select())
            .load::<catenary::models::CompressedTrip>(conn)
            .await;

    let trips = trips.unwrap();

    let routes_to_get = itin_meta
        .iter()
        .map(|x| x.route_id.clone())
        .collect::<Vec<CompactString>>();

    let routes: diesel::prelude::QueryResult<Vec<catenary::models::Route>> =
        catenary::schema::gtfs::routes::dsl::routes
            .filter(catenary::schema::gtfs::routes::chateau.eq(query.chateau_id.clone()))
            .filter(catenary::schema::gtfs::routes::route_id.eq_any(routes_to_get.clone()))
            .select(catenary::models::Route::as_select())
            .load::<catenary::models::Route>(conn)
            .await;

    let routes = routes.unwrap();

    let mut routes_map = BTreeMap::new();

    routes_map.insert(
        query.chateau_id.clone(),
        routes
            .iter()
            .map(|x| (x.route_id.clone(), x.clone()))
            .collect::<BTreeMap<String, catenary::models::Route>>(),
    );

    let service_ids = trips
        .iter()
        .map(|x| x.service_id.clone())
        .collect::<Vec<CompactString>>();

    let calendars: diesel::prelude::QueryResult<Vec<catenary::models::Calendar>> =
        catenary::schema::gtfs::calendar::dsl::calendar
            .filter(catenary::schema::gtfs::calendar::chateau.eq(query.chateau_id.clone()))
            .filter(catenary::schema::gtfs::calendar::service_id.eq_any(service_ids.clone()))
            .select(catenary::models::Calendar::as_select())
            .load::<catenary::models::Calendar>(conn)
            .await;

    let calendar_dates: diesel::prelude::QueryResult<Vec<catenary::models::CalendarDate>> =
        catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
            .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(query.chateau_id.clone()))
            .filter(catenary::schema::gtfs::calendar_dates::service_id.eq_any(service_ids.clone()))
            .select(catenary::models::CalendarDate::as_select())
            .load::<catenary::models::CalendarDate>(conn)
            .await;

    let calendar_dates = calendar_dates.unwrap();
    let calendars = calendars.unwrap();

    let calendar_structure =
        catenary::make_calendar_structure_from_pg_single_chateau(calendars, calendar_dates);

    // starting service date

    let maximum_offset_to_seek_back = itins
        .iter()
        .map(|x| match x.departure_time_since_start {
            Some(departure_time_since_start) => departure_time_since_start,
            None => match x.arrival_time_since_start {
                Some(arrival_time_since_start) => arrival_time_since_start,
                None => match x.interpolated_time_since_start {
                    Some(interpolated_time_since_start) => interpolated_time_since_start,
                    None => 0,
                },
            },
        })
        .max()
        .unwrap_or(0);

    let stop_tz = chrono_tz::Tz::from_str_insensitive(&stop_tz_txt).unwrap();

    //convert greater than time to DateTime Tz

    let greater_than_time_utc = chrono::DateTime::from_timestamp(
        greater_than_time as i64,
        0,
    );

    let greater_than_date_time = greater_than_time_utc.with_timezone(&stop_tz);

    let greater_than_date_time_minus_seek_back = greater_than_date_time
        - chrono::Duration::seconds(maximum_offset_to_seek_back as i64) - chrono::Duration::seconds(86400);

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
            timezone: stop_tz_txt,
        },
        parent: None,
        children_and_related: vec![],
        events: vec![],
        routes: routes_map,
    };

    HttpResponse::Ok().json(response)
}
