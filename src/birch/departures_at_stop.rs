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

#[actix_web::get("/departures_at_stop/")]
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

    // search through itineraries

    let itins: diesel::prelude::QueryResult<Vec<catenary::models::ItineraryPatternRow>> =
        catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
            .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(query.chateau_id.clone()))
            .filter(catenary::schema::gtfs::itinerary_pattern::stop_id.eq(query.stop_id.clone()))
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

    HttpResponse::Ok().body("Hello!")
}
