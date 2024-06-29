use actix_web::web;
use actix_web::web::Query;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoords {
    lat: f64,
    lon: f64,
    timestamp_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DepartingTrip {
    chateau_id: String,
    trip_id: String,
    gtfs_frequency_start_time: Option<String>,
    gtfs_schedule_start_day: String,
    is_frequency: String,
    departure_schedule_s: u64,
    departure_realtime_s: u64,
    arrival_schedule_s: u64,
    arrival_realtime_s: u64,
    stop_sequence: Option<u16>,
    stop_id: String,
    route_type: i16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DepartingTripsDataAnswer {
    number_of_stops_searched_through: usize,
}

#[actix_web::get("/nearbydeparturesfromcoords")]
pub async fn nearby_from_coords(
    req: HttpRequest,
    query: Query<NearbyFromCoords>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    // get all the nearby stops from the coords

    // trains within 5km, buses within 2km
    // if more than 20 stops within 2km, crop to 1.5km

    //https://postgis.net/docs/ST_DWithin.html

    // let stops = sql_query("")

    //Example query all stops within 0.1deg of Los Angeles Union Station
    // SELECT chateau, name FROM gtfs.stops WHERE ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT(-118.235570 34.0855904)', 0.1) AND allowed_spatial_query = TRUE;

    let where_query_for_stops = format!("ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', 0.1) AND allowed_spatial_query = TRUE",
    query.lon, query.lat);

    let stops = catenary::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query_for_stops))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(conn)
        .await;

    // search through itineraries matching those stops and then put them in a hashmap of stop to itineraries

    //get the start of the trip and the offset for the current stop

    //look through time compressed and decompress the itineraries, using timezones and calendar calcs

    //look through gtfs-rt times and `hydrate the itineraries

    match stops {
        Ok(stops) => {
            let answer = DepartingTripsDataAnswer {
                number_of_stops_searched_through: stops.len(),
            };

            let stringified_answer = serde_json::to_string(&answer).unwrap();

            HttpResponse::Ok().body(stringified_answer)
        }
        Err(stops_err) => HttpResponse::InternalServerError().body(format!("Error: {}", stops_err)),
    }
}

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromStops {
    //serialise and deserialise using serde_json into Vec<NearbyStopsDeserialize>
    stops: String,
}

#[derive(Deserialize, Clone, Debug)]
struct NearbyStopsDeserialize {
    stop_id: String,
    chateau_id: String,
    timestamp_seconds: u64,
}

#[actix_web::get("/nearbydeparturesfromstops/")]
pub async fn nearby_from_stops(req: HttpRequest, query: Query<NearbyFromStops>) -> impl Responder {
    // search through itineraries matching those stops and then put them in a hashmap of stop to itineraries

    //get the start of the trip and the offset for the current stop

    //look through time compressed and decompress the itineraries, using timezones and calendar calcs

    //look through gtfs-rt times and hydrate the itineraries

    HttpResponse::Ok().body("Hello!")
}
