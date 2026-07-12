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

mod live_websocket;
use live_websocket::LiveLocationsWebSocket;

mod chateau_rtree;
mod departures_shared;
mod map_coordinator;
mod nearby_departures;
pub mod trajectories;

use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, TripIntroductionInformation,
};
use map_coordinator::{
    BoundsInputV3, BulkFetchCoordinator, BulkFetchCoordinatorPool, BulkFetchResponseV2,
};
use serde::{Deserialize, Serialize};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

    #[serde(rename = "subscribe_trajectories")]
    SubscribeTrajectories {
        #[serde(flatten)]
        params: trajectories::ClientTrajectorySubscriptionParams,
    },
    #[serde(rename = "unsubscribe_trajectories")]
    UnsubscribeTrajectories,

    #[serde(rename = "subscribe_map_v2")]
    SubscribeMapV2 {
        #[serde(flatten)]
        params: SubscribeMapV2Params,
    },
    #[serde(rename = "unsubscribe_map_v2")]
    UnsubscribeMapV2,
    #[serde(rename = "ping")]
    Ping,
}

#[derive(Deserialize, Clone)]
pub struct SubscribeMapV2Params {
    pub categories: Vec<String>,
    pub bounds_input: BoundsInputV3,
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
    #[serde(rename = "buffer")]
    Buffer {
        timestamp: u64,
        client_reference: String,
        chateau: String,
        content: Vec<trajectories::TrajectoryWrapper>,
        chunk_index: usize,
        total_chunks: usize,
    },
    #[serde(rename = "pong")]
    Pong,
}

async fn index(
    req: HttpRequest,
    stream: web::Payload,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    aspen_chateau_cache: web::Data<Arc<catenary::aspen::lib::AspenChateauCache>>,
    aspen_client_manager: web::Data<Arc<AspenClientManager>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> Result<HttpResponse, Error> {
    ws::start(
        TripWebSocket::new(
            pool.as_ref().clone(),
            aspen_chateau_cache.as_ref().clone(),
            aspen_client_manager.as_ref().clone(),
            etcd_connection_ips.as_ref().clone(),
            etcd_connection_options.as_ref().clone(),
            etcd_reuser.get_ref().clone(),
        ),
        &req,
        stream,
    )
}

async fn index_live(
    req: HttpRequest,
    stream: web::Payload,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    aspen_chateau_cache: web::Data<Arc<catenary::aspen::lib::AspenChateauCache>>,
    aspen_client_manager: web::Data<Arc<AspenClientManager>>,
    coordinator_pool: web::Data<Arc<BulkFetchCoordinatorPool>>,
    chateau_rtree: web::Data<Arc<chateau_rtree::ChateauRTree>>,
) -> Result<HttpResponse, Error> {
    ws::start(
        LiveLocationsWebSocket::new(
            pool.as_ref().clone(),
            aspen_chateau_cache.as_ref().clone(),
            aspen_client_manager.as_ref().clone(),
            coordinator_pool.get_ref().clone(),
            chateau_rtree.get_ref().clone(),
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

    // Load Chateau RTree once at startup
    let chateau_rtree = Arc::new(chateau_rtree::ChateauRTree::load(&pool).await);

    let catenary_config = catenary::catenaryconfig::config();

    let etcd_urls_original = std::env::var("ETCD_URLS")
        .ok()
        .or_else(|| {
            catenary_config
                .spruce
                .etcd_urls
                .as_ref()
                .map(|urls| urls.join(","))
        })
        .or_else(|| {
            catenary_config
                .aspen
                .etcd_urls
                .as_ref()
                .map(|urls| urls.join(","))
        })
        .unwrap_or_else(|| "localhost:2379".to_string());
    let etcd_urls_vec: Vec<String> = etcd_urls_original
        .split(',')
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    let etcd_username = std::env::var("ETCD_USERNAME")
        .ok()
        .or_else(|| catenary_config.spruce.etcd_username.clone())
        .or_else(|| catenary_config.aspen.etcd_username.clone());
    let etcd_password = std::env::var("ETCD_PASSWORD")
        .ok()
        .or_else(|| catenary_config.spruce.etcd_password.clone())
        .or_else(|| catenary_config.aspen.etcd_password.clone());

    let etcd_connection_ips = Arc::new(EtcdConnectionIps {
        ip_addresses: etcd_urls_vec.clone(),
    });

    let etcd_connection_options = Arc::new(match (etcd_username, etcd_password) {
        (Some(username), Some(password)) => Some(
            etcd_client::ConnectOptions::new()
                .with_user(username, password)
                .with_keep_alive(
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(5),
                ),
        ),
        _ => None,
    });

    let worker_amount = std::env::var("WORKER_AMOUNT")
        .ok()
        .or_else(|| catenary_config.spruce.worker_amount.map(|v| v.to_string()))
        .unwrap_or_else(|| "2".to_string())
        .parse::<usize>()
        .unwrap_or(2);

    let aspen_client_manager = Arc::new(AspenClientManager::new());

    // Etcd Reuser for BulkFetchCoordinator
    // Note: The original code used a RwLock<Option<Client>>, we can create one here.
    // However, the `etcd_client::Client` is cloneable and handles valid connection pool internally usually?
    // But the code in `bulk_realtime_fetch_v3` manually checks status and reconnects.
    // Let's replicate strict behavior: create an initial client (or None) and wrap in RwLock.

    let etcd_reuser = Arc::new(tokio::sync::RwLock::new(None::<etcd_client::Client>));

    let aspen_chateau_cache = Arc::new(
        catenary::etcd_cache::EtcdCache::<catenary::aspen::lib::ChateauMetadataEtcd>::new(
            etcd_connection_ips.clone(),
            etcd_connection_options.clone(),
            "/aspen_assigned_chateaux/",
        )
        .await
        .unwrap(),
    );

    let coordinator_shard_count = std::env::var("COORDINATOR_SHARDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| worker_amount.max(16));

    println!(
        "Starting spruce with {} Actix workers and {} BulkFetchCoordinator shards",
        worker_amount, coordinator_shard_count
    );

    let mut coordinator_arbiters = Vec::with_capacity(coordinator_shard_count);
    let mut coordinator_addrs = Vec::with_capacity(coordinator_shard_count);

    for _ in 0..coordinator_shard_count {
        let arbiter = Arbiter::new();

        let addr = BulkFetchCoordinator::start_in_arbiter(&arbiter.handle(), {
            let aspen_chateau_cache = aspen_chateau_cache.clone();
            let aspen_client_manager = aspen_client_manager.clone();

            move |_| {
                BulkFetchCoordinator::new(aspen_chateau_cache.clone(), aspen_client_manager.clone())
            }
        });

        coordinator_arbiters.push(arbiter);
        coordinator_addrs.push(addr);
    }

    let coordinator_pool = Arc::new(BulkFetchCoordinatorPool::new(coordinator_addrs));

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(etcd_connection_ips.clone()))
            .app_data(web::Data::new(etcd_connection_options.clone()))
            .app_data(web::Data::new(aspen_chateau_cache.clone()))
            .app_data(web::Data::new(aspen_client_manager.clone()))
            .app_data(web::Data::new(coordinator_pool.clone()))
            .app_data(web::Data::new(etcd_reuser.clone()))
            .app_data(web::Data::new(chateau_rtree.clone()))
            .route("/ws/", web::get().to(index_live))
            .route("/ws/trip", web::get().to(index))
            .route("/ws/trip/", web::get().to(index))
            .route("/ws/live", web::get().to(index_live))
            .route("/ws/live/", web::get().to(index_live))
            .service(nearby_departures::nearby_from_coords_v3)
            .route("/", web::get().to(index_root))
    })
    .workers(worker_amount)
    .bind(("127.0.0.1", 52771))?
    .run();

    let result = server.await;
    drop(coordinator_arbiters);
    result
}
