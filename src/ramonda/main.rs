use actix::prelude::*;
use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, web};
use actix_web_actors::ws;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use chrono::Utc;
use std::sync::Arc;

use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, TripIntroductionInformation,
    fetch_trip_information, fetch_trip_rt_update,
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

const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const CLIENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

pub struct RamondaWebSocket {
    pub pool: Arc<CatenaryPostgresPool>,
    pub aspen_chateau_cache:
        std::sync::Arc<catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub subscriptions: std::collections::HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: std::time::Instant,
    pub aspen_endpoint_cache:
        std::collections::HashMap<String, (std::net::SocketAddr, std::time::Instant)>,
    pub in_progress_trip_fetches:
        std::collections::HashMap<(String, QueryTripInformationParams), std::time::Instant>,
}

impl Actor for RamondaWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        self.start_periodic_updates(ctx);
    }
}

impl RamondaWebSocket {
    pub fn new(
        pool: Arc<CatenaryPostgresPool>,
        aspen_chateau_cache: std::sync::Arc<
            catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>,
        >,
        aspen_client_manager: Arc<AspenClientManager>,
    ) -> Self {
        Self {
            pool,
            aspen_chateau_cache,
            aspen_client_manager,
            subscriptions: std::collections::HashMap::new(),
            hb: std::time::Instant::now(),
            aspen_endpoint_cache: std::collections::HashMap::new(),
            in_progress_trip_fetches: std::collections::HashMap::new(),
        }
    }

    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if std::time::Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }

    fn start_periodic_updates(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(UPDATE_INTERVAL, |act, ctx| {
            let now = std::time::Instant::now();
            let mut keys_to_fetch = Vec::new();

            for ((chateau, params), _) in act.subscriptions.iter() {
                let key = (chateau.clone(), params.clone());
                if let Some(started) = act.in_progress_trip_fetches.get(&key) {
                    if started.elapsed() < std::time::Duration::from_secs(5) {
                        continue;
                    }
                }
                keys_to_fetch.push(key);
            }

            for key in keys_to_fetch {
                act.in_progress_trip_fetches.insert(key.clone(), now);
                let (chateau_clone, params_clone) = key.clone();

                let cached_socket =
                    act.aspen_endpoint_cache
                        .get(&chateau_clone)
                        .and_then(|(socket, time)| {
                            if time.elapsed() < std::time::Duration::from_secs(300) {
                                Some(socket.clone())
                            } else {
                                None
                            }
                        });

                let fs = fetch_trip_rt_update(
                    chateau_clone.clone(),
                    params_clone.clone(),
                    act.aspen_chateau_cache.clone(),
                    act.aspen_client_manager.clone(),
                    cached_socket,
                );

                let fut = async move {
                    fs.await
                };

                let fut = actix::fut::wrap_future(fut).map(
                    move |result,
                          act: &mut RamondaWebSocket,
                          ctx: &mut ws::WebsocketContext<Self>| {
                        act.in_progress_trip_fetches.remove(&key);
                        match result {
                            Ok((response, used_socket)) => {
                                if let Some(socket) = used_socket {
                                    act.aspen_endpoint_cache.insert(
                                        chateau_clone.clone(),
                                        (socket, std::time::Instant::now()),
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

                                        let key = (chateau_clone.clone(), params_clone.clone());
                                        if let Some(current_last_update) =
                                            act.subscriptions.get_mut(&key)
                                        {
                                            if let Some(last) = current_last_update {
                                                if *last == hash {
                                                    return;
                                                }
                                            }
                                            *current_last_update = Some(hash);
                                        }

                                        let msg = ServerMessage::UpdateTrip { data };
                                        let text = serde_json::to_string(&msg).unwrap();
                                        ctx.text(text);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "Ramonda fetch_trip_rt_update failed for chateau={} trip_id={}: {}",
                                    chateau_clone,
                                    params_clone.trip_id,
                                    e
                                );
                                act.aspen_endpoint_cache.remove(&chateau_clone);
                            }
                        }
                    },
                );
                ctx.spawn(fut);
            }
        });
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for RamondaWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = std::time::Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = std::time::Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                let msg: Result<ClientMessage, _> = serde_json::from_str(&text);
                match msg {
                    Ok(ClientMessage::UnsubscribeTrip { chateau, params }) => {
                        self.subscriptions.remove(&(chateau, params));
                    }
                    Ok(ClientMessage::UnsubscribeAllTrips) => {
                        self.subscriptions.clear();
                    }
                    Ok(ClientMessage::SubscribeTrip { chateau, params }) => {
                        self.subscriptions
                            .insert((chateau.clone(), params.clone()), None);

                        let fs = fetch_trip_information(
                            chateau,
                            params,
                            self.pool.clone(),
                            self.aspen_chateau_cache.clone(),
                            self.aspen_client_manager.clone(),
                            None,
                        );

                        let fut = async move { fs.await };

                        let fut = actix::fut::wrap_future(fut).map(
                            |result, _, ctx: &mut ws::WebsocketContext<Self>| match result {
                                Ok(data) => {
                                    let msg = ServerMessage::InitialTrip { data };
                                    let text = serde_json::to_string(&msg).unwrap();
                                    ctx.text(text);
                                }
                                Err(e) => {
                                    let msg = ServerMessage::Error { message: e };
                                    let text = serde_json::to_string(&msg).unwrap();
                                    ctx.text(text);
                                }
                            },
                        );
                        ctx.spawn(fut);
                    }
                    Ok(ClientMessage::Ping) => {
                        let msg = ServerMessage::Pong;
                        if let Ok(text) = serde_json::to_string(&msg) {
                            ctx.text(text);
                        }
                    }
                    Err(e) => {
                        let msg = ServerMessage::Error {
                            message: format!("Invalid message structure: {}", e),
                        };
                        if let Ok(text) = serde_json::to_string(&msg) {
                            ctx.text(text);
                        }
                    }
                }
            }
            Ok(ws::Message::Binary(_)) => (),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => ctx.stop(),
        }
    }
}

async fn index(
    req: HttpRequest,
    stream: web::Payload,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    aspen_chateau_cache: web::Data<
        std::sync::Arc<catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>>,
    >,
    aspen_client_manager: web::Data<Arc<AspenClientManager>>,
) -> Result<HttpResponse, Error> {
    ws::start(
        RamondaWebSocket::new(
            pool.as_ref().clone(),
            aspen_chateau_cache.as_ref().clone(),
            aspen_client_manager.as_ref().clone(),
        ),
        &req,
        stream,
    )
}

async fn index_root() -> HttpResponse {
    HttpResponse::Ok().body(format!(
        "Hello World from Catenary Ramonda! {}",
        Utc::now().to_rfc3339()
    ))
}

#[actix_web::main]
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
    let aspen_chateau_cache = Arc::new(
        catenary::etcd_cache::EtcdCache::<catenary::aspen::lib::ChateauMetadataEtcd>::new(
            etcd_connection_ips.clone(),
            etcd_connection_options.clone(),
            "/aspen_assigned_chateaux/",
        )
        .await
        .unwrap(),
    );

    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .or_else(|| catenary_config.ramonda.port)
        .unwrap_or(52772);

    println!(
        "Starting ramonda with {} Actix workers on port {}",
        worker_amount, port
    );

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(aspen_chateau_cache.clone()))
            .app_data(web::Data::new(aspen_client_manager.clone()))
            .route("/ws", web::get().to(index))
            .route("/ws/", web::get().to(index))
            .route("/", web::get().to(index_root))
    })
    .workers(worker_amount)
    .bind(("127.0.0.1", port))?
    .run();

    server.await
}
