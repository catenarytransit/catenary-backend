use actix_web::{web, HttpResponse, Responder};
use catenary::aspen_dataset::AspenStopTimeEvent;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use catenary::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use catenary::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use diesel::query_dsl::methods::FilterDsl;
use diesel::ExpressionMethods;
use diesel::query_dsl::methods::SelectDsl;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use diesel::SelectableHelper;

#[actix_web::get("/get_vehicle_metadata/{chateau}/{vehicle_id}")]
pub async fn get_vehicle_metadata(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[actix_web::get("/get_vehicle_information/{chateau}/{gtfs_rt_id}")]
pub async fn get_vehicle_information(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[derive(Deserialize, Serialize)]
struct TripIntroductionInformation {
    stops: Vec<StopTimeIntroduction>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct StopTimeIntroduction {
    pub stop_id: String,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub scheduled_arrival_time_unix_seconds: Option<u64>,
    pub scheduled_departure_time_unix_seconds: Option<u64>,
    pub rt_arrival: Option<AspenStopTimeEvent>,
    pub rt_departure: Option<AspenStopTimeEvent>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct QueryTripInformationParams {
    pub trip_id: String,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
}

#[actix_web::get("/get_trip_information/{chateau}/")]
pub async fn get_trip(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>,
    zk: web::Data<Arc<tokio_zookeeper::ZooKeeper>>,
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let chateau = path.into_inner();

    // connect to pool
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to database");
    }

    let conn = &mut conn_pre.unwrap();

    //ask postgres first
    let trip_compressed = trips_compressed_pg_schema::dsl::trips_compressed
        .filter(trips_compressed_pg_schema::dsl::chateau.eq(&chateau))
        .filter(trips_compressed_pg_schema::dsl::trip_id.eq(&query.trip_id))
        .select(catenary::models::CompressedTrip::as_select())
        .load(conn)
        .await;

    if let Err(trip_compressed_err) = &trip_compressed {
        eprintln!("{}", trip_compressed_err);
        return HttpResponse::InternalServerError().body("Error fetching trip compressed");
    }

    let trip_compressed = trip_compressed.unwrap();

    if trip_compressed.len() == 0 {
        return HttpResponse::NotFound().body("Compressed trip not found");
    }

    let trip_compressed = trip_compressed[0].clone();
    // get itin data and itin meta data

    let itin_meta = itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_meta
        .filter(itinerary_pattern_meta_pg_schema::dsl::chateau.eq(&chateau))
        .filter(itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_id.eq(&trip_compressed.itinerary_pattern_id))
        .select(catenary::models::ItineraryPatternMeta::as_select())
        .load(conn).await;

        if let Err(itin_meta) = &itin_meta {
            eprintln!("{}", itin_meta);
            return HttpResponse::InternalServerError().body("Error fetching itinerary pattern");
        }

        let itin_meta = itin_meta.unwrap();

        let itin_rows = itinerary_pattern_pg_schema::dsl::itinerary_pattern
        .filter(itinerary_pattern_pg_schema::dsl::chateau.eq(&chateau))
        .filter(itinerary_pattern_pg_schema::dsl::itinerary_pattern_id.eq(&trip_compressed.itinerary_pattern_id))
        .select(catenary::models::ItineraryPatternRow::as_select())
        .load(conn).await;

    HttpResponse::Ok().body("get_vehicle_metadata")
}
