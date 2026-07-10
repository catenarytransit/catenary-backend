use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::trip_logic::{
    QueryTripInformationParams, fetch_trip_information, fetch_trip_rt_update,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{AppState, ClientMessage, ServerMessage, nearby_departures};
use futures_util::{SinkExt, StreamExt};
use sockudo_ws::Message;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Instant, interval};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const UPDATE_INTERVAL: Duration = Duration::from_millis(300);

pub struct SessionState {
    pub subscriptions: HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: Instant,
    pub aspen_endpoint_cache: HashMap<String, (std::net::SocketAddr, Instant)>,
    pub in_progress_trip_fetches: HashMap<(String, QueryTripInformationParams), Instant>,
}

pub async fn handle_trip_socket(socket: sockudo_ws::axum_integration::WebSocket, state: AppState) {
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

    // Reader task
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
                        Ok(ClientMessage::NearbyDepartures { params, request_id }) => {
                            let context = nearby_departures::NearbyDeparturesContext {
                                pool: state.pool.clone(),
                                etcd_connection_ips: state.etcd_connection_ips.clone(),
                                etcd_connection_options: state.etcd_connection_options.clone(),
                                etcd_reuser: state.etcd_reuser.clone(),
                            };

                            let request_id_clone = request_id.clone();
                            let tx_clone = tx.clone();

                            tokio::spawn(async move {
                                let stream = nearby_departures::get_nearby_departures_stream(
                                    context, params,
                                )
                                .await;

                                let mut pinned_stream = std::pin::pin!(stream);
                                let mut idx = 0;
                                while let Some((response, is_hydration)) =
                                    pinned_stream.next().await
                                {
                                    let msg = ServerMessage::NearbyDeparturesChunk {
                                        request_id: request_id_clone.clone(),
                                        chunk_index: idx,
                                        total_chunks: 2,
                                        is_hydration,
                                        data: response,
                                    };
                                    if let Ok(text) = serde_json::to_string(&msg) {
                                        let _ =
                                            tx_clone.send(Message::Text(text.into_bytes().into()));
                                    }
                                    idx += 1;
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
                        _ => {
                            let msg = ServerMessage::Error {
                                message: "This endpoint only supports trip information and routing. Please connect to /ws/live/ for live locations.".to_string(),
                            };
                            if let Ok(text) = serde_json::to_string(&msg) {
                                let _ = tx.send(Message::Text(text.into_bytes().into()));
                            }
                        }
                    }
                }
            }
            Ok(Message::Binary(_)) => (),
            Ok(Message::Close(_)) => break,
            Err(_) => break,
        }
    }

    bg_task.abort();
    writer_task.abort();
}
