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
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use geo::HaversineDestination;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoords {
    lat: f64,
    lon: f64,
    timestamp_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DepartingTrip {
    pub chateau_id: String,
    pub trip_id: String,
    pub gtfs_frequency_start_time: Option<String>,
    pub gtfs_schedule_start_day: String,
    pub is_frequency: String,
    pub departure_schedule_s: Option<u64>,
    pub departure_realtime_s: Option<u64>,
    pub arrival_schedule_s: Option<u64>,
    pub arrival_realtime_s: Option<u64>,
    pub stop_sequence: Option<u16>,
    pub stop_id: String,
    pub route_type: i16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DepartingTripsDataAnswer {
    pub number_of_stops_searched_through: usize,
    pub bus_limited_metres: f64,
    pub rail_and_other_limited_metres: f64,
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

    let input_point = geo::Point::new(query.lon, query.lat);

    // i dont want to accidently create a point which is outside 180 or -180

    let direction = match input_point.x() > 0. {
        true => 90.,
        false => -90.,
    };

    let distance_calc_point = input_point.haversine_destination(direction, 2000.);

    let spatial_resolution_in_degs = f64::abs(distance_calc_point.x() - input_point.x());

    let where_query_for_stops = format!("ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
    query.lon, query.lat, spatial_resolution_in_degs);

    let stops = catenary::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query_for_stops))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(conn)
        .await;

    match stops {
        Ok(stops) => {
            let number_of_stops = stops.len();

            //collect chateau list

            let mut sorted_by_chateau: HashMap<String, HashMap<String, catenary::models::Stop>> =
                HashMap::new();

            // search through itineraries matching those stops and then put them in a hashmap of stop to itineraries

            //problem with this code: no understanding of what the current chateau list is
            //solution, search through chateaus and get current valid attempt number to search through?
            for stop in stops.iter() {
                //  result
                sorted_by_chateau
                    .entry(stop.chateau.clone())
                    .and_modify(|hashmap_under_chateau| {
                        hashmap_under_chateau.insert(stop.gtfs_id.clone(), stop.clone());
                    })
                    .or_insert({
                        let mut hashmap_under_chateau: HashMap<String, catenary::models::Stop> =
                            HashMap::new();

                        hashmap_under_chateau.insert(stop.gtfs_id.clone(), stop.clone());

                        hashmap_under_chateau
                    });
            }

            let sorted_by_chateau = sorted_by_chateau;

            //for each chateau

            for (chateau_id, hash_under_chateau) in sorted_by_chateau {
                // query for all the itinerary times, look at the closest stops for all of them,

                // get the closest stop for each itinerary
            }

            //get the start of the trip and the offset for the current stop

            //look through time compressed and decompress the itineraries, using timezones and calendar calcs

            //look through gtfs-rt times and hydrate the itineraries

            let answer = DepartingTripsDataAnswer {
                number_of_stops_searched_through: number_of_stops,
                bus_limited_metres: 2000.,
                rail_and_other_limited_metres: 2000.,
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
