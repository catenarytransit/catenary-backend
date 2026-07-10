use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use chrono::Utc;
use std::sync::Arc;

mod trip_websocket;
use trip_websocket::handle_trip_socket;

mod live_websocket;
use live_websocket::handle_live_socket;

mod chateau_rtree;
mod departures_shared;
mod map_coordinator;
mod nearby_departures;
pub mod trajectories;

use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, TripIntroductionInformation,
};
use map_coordinator::{BoundsInputV3, BulkFetchCoordinatorPool, BulkFetchResponseV2};
use serde::{Deserialize, Serialize};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub coordinator_pool: Arc<BulkFetchCoordinatorPool>,
    pub etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    pub chateau_rtree: Arc<chateau_rtree::ChateauRTree>,
}

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

async fn trip_ws_handler(
    ws: sockudo_ws::axum_integration::WebSocketUpgrade,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl axum::response::IntoResponse {
    let config = sockudo_ws::Config::builder()
        .max_payload_length(64 * 1024)
        .idle_timeout(60)
        .compression(sockudo_ws::Compression::Dedicated)
        .build();

    ws.config(config)
        .on_upgrade(|socket| handle_trip_socket(socket, state))
}

async fn live_ws_handler(
    ws: sockudo_ws::axum_integration::WebSocketUpgrade,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl axum::response::IntoResponse {
    let config = sockudo_ws::Config::builder()
        .max_payload_length(1024 * 1024 * 16) // 16MB because live_websocket sends large initial maps
        .idle_timeout(60)
        .compression(sockudo_ws::Compression::Dedicated)
        .build();

    ws.config(config)
        .on_upgrade(|socket| handle_live_socket(socket, state))
}

async fn index_root() -> impl axum::response::IntoResponse {
    format!(
        "Hello World from Catenary Spruce! {}",
        Utc::now().to_rfc3339()
    )
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
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

    let aspen_client_manager = Arc::new(AspenClientManager::new());
    let etcd_reuser = Arc::new(tokio::sync::RwLock::new(None));

    let coordinator_pool = Arc::new(BulkFetchCoordinatorPool::new(
        etcd_connection_ips.clone(),
        etcd_connection_options.clone(),
        aspen_client_manager.clone(),
        etcd_reuser.clone(),
    ));

    println!("Starting spruce on Axum...");

    let state = AppState {
        pool,
        etcd_connection_ips,
        etcd_connection_options,
        aspen_client_manager,
        coordinator_pool,
        etcd_reuser,
        chateau_rtree,
    };

    let app = axum::Router::new()
        .route("/ws/", axum::routing::get(live_ws_handler))
        .route("/ws/trip", axum::routing::get(trip_ws_handler))
        .route("/ws/trip/", axum::routing::get(trip_ws_handler))
        .route("/ws/live", axum::routing::get(live_ws_handler))
        .route("/ws/live/", axum::routing::get(live_ws_handler))
        .route(
            "/nearbydeparturesfromcoordsv3",
            axum::routing::get(nearby_departures::nearby_from_coords_v3),
        )
        .route("/", axum::routing::get(index_root))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:52771")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
