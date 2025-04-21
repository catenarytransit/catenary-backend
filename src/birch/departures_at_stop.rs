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
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel_async::RunQueryDsl;
use serde_derive::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromStops {
    //serialise and deserialise using serde_json into Vec<NearbyStopsDeserialize>
    stop_id: String,
    chateau_id: String,
    departure_time: Option<u64>,
}

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

struct NearbyFromStopsResponse {
    primary: StopInfoResponse,
    parent: Option<StopInfoResponse>,
    children_and_related: Vec<StopInfoResponse>,
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

    let departure_time = match query.departure_time {
        Some(departure_time) => departure_time,
        None => catenary::duration_since_unix_epoch().as_secs(),
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

    //get the start of the trip and the offset for the current stop

    //look through time compressed and decompress the itineraries, using timezones and calendar calcs

    //look through gtfs-rt times and hydrate the itineraries

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
            timezone: stop.timezone.unwrap(),
        },
        parent: None,
        children_and_related: vec![],
    };

    HttpResponse::Ok().body("Hello!")
}
