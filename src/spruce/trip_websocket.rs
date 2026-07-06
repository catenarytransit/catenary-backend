use actix::prelude::*;
use actix_web_actors::ws;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::trip_logic::{
    QueryTripInformationParams, fetch_trip_information, fetch_trip_rt_update,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const UPDATE_INTERVAL: Duration = Duration::from_millis(300);

use crate::{ClientMessage, ServerMessage, nearby_departures};
use futures::StreamExt;

pub struct TripWebSocket {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub subscriptions: HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: Instant,

    pub etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    pub aspen_endpoint_cache: HashMap<String, (std::net::SocketAddr, Instant)>,
    pub in_progress_trip_fetches: HashMap<(String, QueryTripInformationParams), Instant>,
}

impl TripWebSocket {
    pub fn new(
        pool: Arc<CatenaryPostgresPool>,
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
        etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    ) -> Self {
        Self {
            pool,
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            subscriptions: HashMap::new(),
            hb: Instant::now(),
            etcd_reuser,
            aspen_endpoint_cache: HashMap::new(),
            in_progress_trip_fetches: HashMap::new(),
        }
    }

    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }

    fn start_periodic_updates(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(UPDATE_INTERVAL, |act, ctx| {
            let now = Instant::now();
            let mut keys_to_fetch = Vec::new();

            for ((chateau, params), _) in act.subscriptions.iter() {
                let key = (chateau.clone(), params.clone());
                if let Some(started) = act.in_progress_trip_fetches.get(&key) {
                    if started.elapsed() < Duration::from_secs(5) {
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
                            if time.elapsed() < Duration::from_secs(300) {
                                Some(socket.clone())
                            } else {
                                None
                            }
                        });

                let fs = fetch_trip_rt_update(
                    chateau_clone.clone(),
                    params_clone.clone(),
                    act.etcd_connection_ips.clone(),
                    act.etcd_connection_options.clone(),
                    act.aspen_client_manager.clone(),
                    act.etcd_reuser.clone(),
                    cached_socket,
                );

                let fut = async move { fs.await };

                let fut = actix::fut::wrap_future(fut).map(
                    move |result, act: &mut TripWebSocket, ctx: &mut ws::WebsocketContext<Self>| {
                        act.in_progress_trip_fetches.remove(&key);
                        match result {
                            Ok((response, used_socket)) => {
                                if let Some(socket) = used_socket {
                                    act.aspen_endpoint_cache
                                        .insert(chateau_clone.clone(), (socket, Instant::now()));
                                }

                                if response.found_data {
                                    if let Some(data) = response.data {
                                        use std::collections::hash_map::DefaultHasher;
                                        use std::hash::{Hash, Hasher};

                                        let stoptimes_json = serde_json::to_string(&data.stoptimes)
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
                            Err(_e) => {
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

impl Actor for TripWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        self.start_periodic_updates(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        actix::Running::Stop
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for TripWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
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
                            self.etcd_connection_ips.clone(),
                            self.etcd_connection_options.clone(),
                            self.aspen_client_manager.clone(),
                            None,
                            self.etcd_reuser.clone(),
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
                    Ok(ClientMessage::NearbyDepartures { params, request_id }) => {
                        let context = nearby_departures::NearbyDeparturesContext {
                            pool: self.pool.clone(),
                            etcd_connection_ips: self.etcd_connection_ips.clone(),
                            etcd_connection_options: self.etcd_connection_options.clone(),
                            etcd_reuser: self.etcd_reuser.clone(),
                        };

                        let request_id_clone = request_id.clone();

                        let fut = async move {
                            let stream =
                                nearby_departures::get_nearby_departures_stream(context, params)
                                    .await;
                            stream
                                .enumerate()
                                .map(move |(idx, (response, is_hydration))| {
                                    let msg = ServerMessage::NearbyDeparturesChunk {
                                        request_id: request_id_clone.clone(),
                                        chunk_index: idx,
                                        total_chunks: 2,
                                        is_hydration,
                                        data: response,
                                    };
                                    if let Ok(text) = serde_json::to_string(&msg) {
                                        text
                                    } else {
                                        "".to_string()
                                    }
                                })
                                .collect::<Vec<String>>()
                                .await
                        };

                        let fut = actix::fut::wrap_future(fut).map(
                            |messages, _, ctx: &mut ws::WebsocketContext<Self>| {
                                for text in messages {
                                    if !text.is_empty() {
                                        ctx.text(text);
                                    }
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
                    _ => {
                        let msg = ServerMessage::Error {
                            message: "This endpoint only supports trip information and routing. Please connect to /ws/live/ for live locations.".to_string(),
                        };
                        ctx.text(serde_json::to_string(&msg).unwrap());
                    }
                }
            }
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => (),
        }
    }
}
