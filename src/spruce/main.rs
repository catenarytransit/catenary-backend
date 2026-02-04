use actix::prelude::*;
use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, web};
use actix_web_actors::ws;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use chrono::Utc;
use std::sync::Arc;

mod trip_websocket;
use trip_websocket::TripWebSocket;

mod departures_shared;
mod map_coordinator;
mod nearby_departures;

use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, TripIntroductionInformation,
};
use map_coordinator::{BoundsInputV3, BulkFetchCoordinator, BulkFetchResponseV2};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "subscribe_trip")]
    SubscribeTrip {
        chateau: String,
        #[serde(flatten)]
        params: QueryTripInformationParams,
    },
    #[serde(rename = "unsubscribe_trip")]
    UnsubscribeTrip {
        chateau: String,
        #[serde(flatten)]
        params: QueryTripInformationParams,
    },
    #[serde(rename = "unsubscribe_all_trips")]
    UnsubscribeAllTrips,

    #[serde(rename = "update_map")]
    UpdateMap {
        #[serde(flatten)]
        params: MapViewportUpdate,
    },
    #[serde(rename = "nearby_departures")]
    NearbyDepartures {
        #[serde(flatten)]
        params: nearby_departures::NearbyFromCoordsV3,
        request_id: String,
    },
}

#[derive(Deserialize, Clone)]
pub struct MapViewportUpdate {
    pub chateaus: Vec<String>,
    pub categories: Vec<String>,
    pub bounds_input: BoundsInputV3,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "initial_trip")]
    InitialTrip { data: TripIntroductionInformation },
    #[serde(rename = "update_trip")]
    UpdateTrip { data: GtfsRtRefreshData },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "map_update")]
    MapUpdate(BulkFetchResponseV2),
    #[serde(rename = "nearby_departures_chunk")]
    NearbyDeparturesChunk {
        request_id: String,
        chunk_index: usize,
        total_chunks: usize,
        is_hydration: bool,
        data: nearby_departures::NearbyDeparturesV3Response,
    },
}

async fn index(
    req: HttpRequest,
    stream: web::Payload,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    aspen_client_manager: web::Data<Arc<AspenClientManager>>,
    coordinator: web::Data<Addr<BulkFetchCoordinator>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> Result<HttpResponse, Error> {
    ws::start(
        TripWebSocket::new(
            pool.as_ref().clone(),
            etcd_connection_ips.as_ref().clone(),
            etcd_connection_options.as_ref().clone(),
            aspen_client_manager.as_ref().clone(),
            coordinator.get_ref().clone(),
            etcd_reuser.get_ref().clone(),
        ),
        &req,
        stream,
    )
}

async fn index_root() -> HttpResponse {
    HttpResponse::Ok().body(format!(
        "Hello World from Catenary Spruce! {}",
        Utc::now().to_rfc3339()
    ))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "debug");
    // env_logger::init();

    let pool = Arc::new(make_async_pool().await.unwrap());

    let etcd_urls_string = std::env::var("ETCD_URLS").unwrap();
    let etcd_urls_vec: Vec<String> = etcd_urls_string.split(",").map(|x| x.to_string()).collect();
    let etcd_username = std::env::var("ETCD_USERNAME").unwrap();
    let etcd_password = std::env::var("ETCD_PASSWORD").unwrap();

    let etcd_connection_ips = Arc::new(EtcdConnectionIps {
        ip_addresses: etcd_urls_vec.clone(),
    });

    let etcd_connection_options = Arc::new(Some(
        etcd_client::ConnectOptions::new()
            .with_user(etcd_username.clone(), etcd_password.clone())
            .with_keep_alive(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(5),
            ),
    ));

    let worker_amount = std::env::var("WORKER_AMOUNT")
        .unwrap_or("2".to_string())
        .parse::<usize>()
        .unwrap();

    let aspen_client_manager = Arc::new(AspenClientManager::new());

    // Etcd Reuser for BulkFetchCoordinator
    // Note: The original code used a RwLock<Option<Client>>, we can create one here.
    // However, the `etcd_client::Client` is cloneable and handles valid connection pool internally usually?
    // But the code in `bulk_realtime_fetch_v3` manually checks status and reconnects.
    // Let's replicate strict behavior: create an initial client (or None) and wrap in RwLock.

    let etcd_reuser = Arc::new(tokio::sync::RwLock::new(None));

    // Start the coordinator actor
    let coordinator = BulkFetchCoordinator::new(
        etcd_connection_ips.clone(),
        etcd_connection_options.clone(),
        aspen_client_manager.clone(),
        etcd_reuser.clone(),
    )
    .start();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(etcd_connection_ips.clone()))
            .app_data(web::Data::new(etcd_connection_options.clone()))
            .app_data(web::Data::new(aspen_client_manager.clone()))
            .app_data(web::Data::new(coordinator.clone()))
            .app_data(web::Data::new(etcd_reuser.clone()))
            .route("/ws/", web::get().to(index))
            .service(nearby_departures::nearby_from_coords_v3)
            .route("/", web::get().to(index_root))
    })
    .workers(worker_amount)
    .bind(("127.0.0.1", 52771))?
    .run()
    .await
}
