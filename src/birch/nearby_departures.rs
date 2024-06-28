use actix_web::web;
use actix_web::web::Query;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoords {
    lat: f64,
    lon: f64,
    timestamp_seconds: u64,
}

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

#[actix_web::get("/nearbydeparturesfromcoords/")]
pub async fn nearby_from_coords(
    req: HttpRequest,
    query: Query<NearbyFromCoords>,
    sqlx_pool: web::Data<sqlx::PgPool>,
) -> impl Responder {
    // get all the nearby stops from the coords

    // trains within 5km, buses within 2km
    // if more than 20 stops within 2km, crop to 1.5km

    //https://postgis.net/docs/ST_DWithin.html

   // let stops = sql_query("")

   //Example query all stops within 1.0km of Los Angeles Union Station
   // SELECT chateau, name FROM gtfs.stops WHERE ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT(-118.235570 34.0855904)', 1000) AND allowed_spatial_query = TRUE;

    // search through itineraries matching those stops and then put them in a hashmap of stop to itineraries

    //get the start of the trip and the offset for the current stop

    //look through time compressed and decompress the itineraries, using timezones and calendar calcs

    //look through gtfs-rt times and `hydrate the itineraries

    HttpResponse::Ok().body("Hello!")
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
