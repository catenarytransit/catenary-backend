use axum::{Router, extract::State, response::IntoResponse, routing::get};
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, TripIntroductionInformation,
    fetch_trip_information, fetch_trip_rt_update,
};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sockudo_ws::{Compression, Config, Message};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, Instant, interval};

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
    #[serde(rename = "ping")]
    Ping,
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
    #[serde(rename = "pong")]
    Pong,
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const UPDATE_INTERVAL: Duration = Duration::from_millis(300);

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub etcd_reuser: Arc<RwLock<Option<etcd_client::Client>>>,
}

pub struct SessionState {
    pub subscriptions: HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: Instant,
    pub aspen_endpoint_cache: HashMap<String, (std::net::SocketAddr, Instant)>,
    pub in_progress_trip_fetches: HashMap<(String, QueryTripInformationParams), Instant>,
}

async fn index_root() -> impl IntoResponse {
    format!(
        "Hello World from Catenary Ramonda! {}",
        Utc::now().to_rfc3339()
    )
}

async fn ws_handler(
    ws: sockudo_ws::axum_integration::WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let config = Config::builder()
        .max_payload_length(64 * 1024)
        .idle_timeout(60)
        .compression(Compression::Dedicated)
        .build();

    ws.config(config)
        .on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: sockudo_ws::axum_integration::WebSocket, state: AppState) {
    let (mut receiver, mut sender) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    let session = Arc::new(RwLock::new(SessionState {
        subscriptions: HashMap::new(),
        hb: Instant::now(),
        aspen_endpoint_cache: HashMap::new(),
        in_progress_trip_fetches: HashMap::new(),
    }));

    // Writer task
    let mut writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Background periodic task
    let session_bg = session.clone();
    let tx_bg = tx.clone();
    let state_bg = state.clone();

    let mut bg_task = tokio::spawn(async move {
        let mut update_ticker = interval(UPDATE_INTERVAL);
        let mut hb_ticker = interval(HEARTBEAT_INTERVAL);

        loop {
            tokio::select! {
                _ = update_ticker.tick() => {
                    let now = Instant::now();
                    let mut keys_to_fetch = Vec::new();

                    let mut session_guard = session_bg.write().await;
                    for ((chateau, params), _) in session_guard.subscriptions.iter() {
                        let key = (chateau.clone(), params.clone());
                        if let Some(started) = session_guard.in_progress_trip_fetches.get(&key) {
                            if started.elapsed() < Duration::from_secs(5) {
                                continue;
                            }
                        }
                        keys_to_fetch.push(key);
                    }

                    for key in keys_to_fetch {
                        session_guard.in_progress_trip_fetches.insert(key.clone(), now);
                        let (chateau_clone, params_clone) = key.clone();

                        let cached_socket = session_guard
                            .aspen_endpoint_cache
                            .get(&chateau_clone)
                            .and_then(|(socket, time)| {
                                if time.elapsed() < Duration::from_secs(300) {
                                    Some(*socket)
                                } else {
                                    None
                                }
                            });

                        let tx_clone = tx_bg.clone();
                        let session_clone = session_bg.clone();
                        let state_clone = state_bg.clone();

                        tokio::spawn(async move {
                            let fs = fetch_trip_rt_update(
                                chateau_clone.clone(),
                                params_clone.clone(),
                                state_clone.etcd_connection_ips.clone(),
                                state_clone.etcd_connection_options.clone(),
                                state_clone.aspen_client_manager.clone(),
                                state_clone.etcd_reuser.clone(),
                                cached_socket,
                            );

                            let result = fs.await;

                            let mut s_guard = session_clone.write().await;
                            s_guard.in_progress_trip_fetches.remove(&key);

                            match result {
                                Ok((response, used_socket)) => {
                                    if let Some(sock) = used_socket {
                                        s_guard.aspen_endpoint_cache.insert(
                                            chateau_clone.clone(),
                                            (sock, Instant::now()),
                                        );
                                    }

                                    if response.found_data {
                                        if let Some(data) = response.data {
                                            use std::collections::hash_map::DefaultHasher;
                                            use std::hash::{Hash, Hasher};

                                            let stoptimes_json =
                                                serde_json::to_string(&data.stoptimes)
                                                    .unwrap_or_default();
                                            let mut hasher = DefaultHasher::new();
                                            stoptimes_json.hash(&mut hasher);
                                            let hash = hasher.finish();

                                            if let Some(current_last_update) =
                                                s_guard.subscriptions.get_mut(&key)
                                            {
                                                if let Some(last) = current_last_update {
                                                    if *last == hash {
                                                        return;
                                                    }
                                                }
                                                *current_last_update = Some(hash);
                                            }

                                            let msg = ServerMessage::UpdateTrip { data };
                                            if let Ok(text) = serde_json::to_string(&msg) {
                                                let _ = tx_clone.send(Message::Text(text.into_bytes().into()));
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Ramonda fetch_trip_rt_update failed for chateau={} trip_id={}: {}",
                                        chateau_clone, params_clone.trip_id, e
                                    );
                                    s_guard.aspen_endpoint_cache.remove(&chateau_clone);
                                }
                            }
                        });
                    }
                }
                _ = hb_ticker.tick() => {
                    let last_hb = session_bg.read().await.hb;
                    if last_hb.elapsed() > CLIENT_TIMEOUT {
                        break;
                    }
                    let _ = tx_bg.send(Message::Ping(vec![].into()));
                }
            }
        }
    });

    // Reader task (current thread)
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Ping(msg)) => {
                session.write().await.hb = Instant::now();
                let _ = tx.send(Message::Pong(msg));
            }
            Ok(Message::Pong(_)) => {
                session.write().await.hb = Instant::now();
            }
            Ok(Message::Text(text)) => {
                if let Ok(text_str) = String::from_utf8(text.to_vec()) {
                    let msg: Result<ClientMessage, _> = serde_json::from_str(&text_str);
                    match msg {
                        Ok(ClientMessage::UnsubscribeTrip { chateau, params }) => {
                            session
                                .write()
                                .await
                                .subscriptions
                                .remove(&(chateau, params));
                        }
                        Ok(ClientMessage::UnsubscribeAllTrips) => {
                            session.write().await.subscriptions.clear();
                        }
                        Ok(ClientMessage::SubscribeTrip { chateau, params }) => {
                            session
                                .write()
                                .await
                                .subscriptions
                                .insert((chateau.clone(), params.clone()), None);

                            let state_clone = state.clone();
                            let tx_clone = tx.clone();

                            tokio::spawn(async move {
                                let fs = fetch_trip_information(
                                    chateau,
                                    params,
                                    state_clone.pool.clone(),
                                    state_clone.etcd_connection_ips.clone(),
                                    state_clone.etcd_connection_options.clone(),
                                    state_clone.aspen_client_manager.clone(),
                                    None,
                                    state_clone.etcd_reuser.clone(),
                                );

                                match fs.await {
                                    Ok(data) => {
                                        let msg = ServerMessage::InitialTrip { data };
                                        if let Ok(text) = serde_json::to_string(&msg) {
                                            let _ = tx_clone
                                                .send(Message::Text(text.into_bytes().into()));
                                        }
                                    }
                                    Err(e) => {
                                        let msg = ServerMessage::Error { message: e };
                                        if let Ok(text) = serde_json::to_string(&msg) {
                                            let _ = tx_clone
                                                .send(Message::Text(text.into_bytes().into()));
                                        }
                                    }
                                }
                            });
                        }
                        Ok(ClientMessage::Ping) => {
                            session.write().await.hb = Instant::now();
                            let msg = ServerMessage::Pong;
                            if let Ok(text) = serde_json::to_string(&msg) {
                                let _ = tx.send(Message::Text(text.into_bytes().into()));
                            }
                        }
                        Err(e) => {
                            let msg = ServerMessage::Error {
                                message: format!("Invalid message structure: {}", e),
                            };
                            if let Ok(text) = serde_json::to_string(&msg) {
                                let _ = tx.send(Message::Text(text.into_bytes().into()));
                            }
                        }
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
    }

    bg_task.abort();
    writer_task.abort();
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let pool = Arc::new(make_async_pool().await.unwrap());
    let catenary_config = catenary::catenaryconfig::config();

    let etcd_urls_original = std::env::var("ETCD_URLS")
        .ok()
        .or_else(|| {
            catenary_config
                .ramonda
                .etcd_urls
                .as_ref()
                .map(|urls| urls.join(","))
        })
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
        .or_else(|| catenary_config.ramonda.etcd_username.clone())
        .or_else(|| catenary_config.spruce.etcd_username.clone())
        .or_else(|| catenary_config.aspen.etcd_username.clone());
    let etcd_password = std::env::var("ETCD_PASSWORD")
        .ok()
        .or_else(|| catenary_config.ramonda.etcd_password.clone())
        .or_else(|| catenary_config.spruce.etcd_password.clone())
        .or_else(|| catenary_config.aspen.etcd_password.clone());

    let etcd_connection_ips = Arc::new(EtcdConnectionIps {
        ip_addresses: etcd_urls_vec,
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
        .or_else(|| catenary_config.ramonda.worker_amount.map(|v| v.to_string()))
        .or_else(|| catenary_config.spruce.worker_amount.map(|v| v.to_string()))
        .unwrap_or_else(|| "2".to_string())
        .parse::<usize>()
        .unwrap_or(2);

    let aspen_client_manager = Arc::new(AspenClientManager::new());
    let etcd_reuser: Arc<RwLock<Option<etcd_client::Client>>> = Arc::new(RwLock::new(None));

    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .or_else(|| catenary_config.ramonda.port)
        .unwrap_or(52772);

    println!(
        "Starting ramonda (Axum + sockudo-ws) on port {} with {} workers config (ignored by axum)",
        port, worker_amount
    );

    let state = AppState {
        pool,
        etcd_connection_ips,
        etcd_connection_options,
        aspen_client_manager,
        etcd_reuser,
    };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/ws/", get(ws_handler))
        .route("/", get(index_root))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    axum::serve(listener, app).await
}
