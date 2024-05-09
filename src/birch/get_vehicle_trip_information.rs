use actix_web::{web, HttpResponse, Responder};
use catenary::aspen_dataset::AspenStopTimeEvent;
use catenary::postgres_tools::CatenaryPostgresPool;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    pub trip_id: Option<String>,
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

    HttpResponse::Ok().body("get_vehicle_metadata")
}
