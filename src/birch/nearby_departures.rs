use actix_web::web;
use actix_web::web::Query;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use ahash::AHashMap;
use catenary::models::ItineraryPatternRow;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use geo::HaversineDestination;
use geo::HaversineDistance;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::OccupiedEntry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoords {
    lat: f64,
    lon: f64,
    departure_time: Option<u64>,
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

    let departure_time = match query.departure_time {
        Some(departure_time) => departure_time,
        None => catenary::duration_since_unix_epoch().as_secs(),
    };

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

    let mut distance_limit = 4000;

    let distance_calc_point = input_point.haversine_destination(direction, 4000.);

    let spatial_resolution_in_degs = f64::abs(distance_calc_point.x() - input_point.x());

    let where_query_for_stops = format!("ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
    query.lon, query.lat, spatial_resolution_in_degs);

    let stops: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
        catenary::schema::gtfs::stops::dsl::stops
            .filter(sql::<Bool>(&where_query_for_stops))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(conn)
            .await;

    match stops {
        Ok(stops) => {
            let number_of_stops = stops.len();

            if number_of_stops > 500 {
                distance_limit = 3000;
            }

            if number_of_stops > 1000 {
                distance_limit = 2000;
            }

            if number_of_stops > 2000 {
                distance_limit = 1500;
            }

            //collect chateau list

            let mut sorted_by_chateau: AHashMap<String, AHashMap<String, catenary::models::Stop>> =
                AHashMap::new();

            let mut sorted_stop_proximity: AHashMap<String, Vec<(String, f64)>> = AHashMap::new();

            let mut stop_distance: AHashMap<(String, String), f64> = AHashMap::new();

            // search through itineraries matching those stops and then put them in a hashmap of stop to itineraries

            //problem with this code: no understanding of what the current chateau list is
            //solution, search through chateaus and get current valid attempt number to search through?
            for stop in stops.iter() {
                //  result

                let stop_point = stop.point.as_ref().unwrap();

                let stop_point_geo: geo::Point = (stop_point.x, stop_point.y).into();

                let haversine_distance = input_point.haversine_distance(&stop_point_geo);

                if haversine_distance < distance_limit as f64 {
                    sorted_by_chateau
                        .entry(stop.chateau.clone())
                        .and_modify(|hashmap_under_chateau| {
                            hashmap_under_chateau.insert(stop.gtfs_id.clone(), stop.clone());
                        })
                        .or_insert({
                            let mut hashmap_under_chateau: AHashMap<
                                String,
                                catenary::models::Stop,
                            > = AHashMap::new();

                            hashmap_under_chateau.insert(stop.gtfs_id.clone(), stop.clone());

                            hashmap_under_chateau
                        });

                    stop_distance.insert(
                        (stop.chateau.clone(), stop.gtfs_id.clone()),
                        haversine_distance,
                    );

                    sorted_stop_proximity
                        .entry(stop.chateau.clone())
                        .and_modify(|proximity_list| {
                            proximity_list.push((stop.gtfs_id.clone(), haversine_distance))
                        })
                        .or_insert(vec![(stop.gtfs_id.clone(), haversine_distance)]);
                }
            }

            let sorted_by_chateau = sorted_by_chateau;

            let sorted_stop_proximity: AHashMap<String, Vec<(String, f64)>> = sorted_stop_proximity
                .into_iter()
                .map(|(chateau_id, list)| {
                    let mut list = list;
                    list.sort_by(|a, b| a.partial_cmp(b).unwrap());

                    (chateau_id, list)
                })
                .collect();

            //for each chateau

            let mut futures_array = vec![];

            for (chateau_id, hash_under_chateau) in &sorted_by_chateau {
                let stop_id_vec = hash_under_chateau
                    .keys()
                    .map(|key| key.to_string())
                    .collect::<Vec<String>>();

                // query for all the itinerary times, look at the closest stops for all of them,

                let itineraries_searched =
                    catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                        .filter(
                            catenary::schema::gtfs::itinerary_pattern::chateau
                                .eq(chateau_id.to_string()),
                        )
                        .filter(
                            catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(stop_id_vec),
                        )
                        .select(ItineraryPatternRow::as_select())
                        .load(conn);

                futures_array.push(itineraries_searched);
            }

            let run_futures: Vec<Result<Vec<ItineraryPatternRow>, diesel::result::Error>> =
                FuturesUnordered::from_iter(futures_array)
                    .collect::<Vec<_>>()
                    .await;

            let mut patterns_found: HashMap<String, HashMap<String, Vec<ItineraryPatternRow>>> =
                HashMap::new();

            for r in run_futures {
                let r = r.unwrap();

                let mut pattern_per_stop: HashMap<String, Vec<ItineraryPatternRow>> =
                    HashMap::new();

                let chateau = r[0].chateau.clone();

                for ipr in r {
                    match pattern_per_stop.entry(ipr.stop_id.clone()) {
                        std::collections::hash_map::Entry::Occupied(mut oe) => {
                            oe.get_mut().push(ipr);
                        }
                        std::collections::hash_map::Entry::Vacant(ve) => {
                            ve.insert(vec![ipr]);
                        }
                    }
                }

                patterns_found.insert(chateau, pattern_per_stop);
            }

            for (chateau_id, hash_under_chateau) in &sorted_by_chateau {
                // get the closest stop for each itinerary by greedy search

                let mut itins_found: ahash::AHashSet<String> = ahash::AHashSet::new();

                let patterns_per_stop = patterns_found.get(chateau_id).unwrap();

                let sorted_stop_prox_for_this_chateau =
                    sorted_stop_proximity.get(&chateau_id.clone()).unwrap();

                for (stop_id, distance) in sorted_stop_prox_for_this_chateau {
                        if let Some(patterns_per_stop) = patterns_per_stop.get(stop_id) {
                            for pattern in patterns_per_stop {
                                if !itins_found.contains(&pattern.itinerary_pattern_id) {

                                    itins_found.insert(pattern.itinerary_pattern_id.clone());
                                }
                            }
                        }
                }
            }
            //get the start of the trip and the offset for the current stop

            //look through time compressed and decompress the itineraries, using timezones and calendar calcs

            //look through gtfs-rt times and hydrate the itineraries

            let answer = DepartingTripsDataAnswer {
                number_of_stops_searched_through: number_of_stops,
                bus_limited_metres: distance_limit as f64,
                rail_and_other_limited_metres: distance_limit as f64,
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
