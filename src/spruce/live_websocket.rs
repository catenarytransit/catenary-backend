use crate::map_coordinator::{
    AspenisedVehiclePositionOutput, BoundsInputV3, BulkFetchCoordinatorPool, BulkFetchResponseV2,
    CategoryOfRealtimeVehicleData, EachCategoryPayloadV2, EachChateauResponseV2,
    PositionDataCategoryV2, PrecomputedChateauMap,
};
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::CatenaryPostgresPool;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::{AppState, ClientMessage, MapViewportUpdate, ServerMessage, trajectories};
use futures_util::{SinkExt, StreamExt};
use sockudo_ws::Message;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::{Instant, interval};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const MAP_UPDATE_COALESCE_INTERVAL: Duration = Duration::from_millis(50);
const MAP_UPDATE_BUILD_CONCURRENCY: usize = 4;

type SentMapState = (u64, BoundsInputV3, HashSet<String>);

pub struct SessionState {
    pub hb: Instant,
    pub client_viewport: Option<MapViewportUpdate>,
    pub client_viewport_v2: Option<crate::SubscribeMapV2Params>,
    pub subscribed_chateaus: HashSet<String>,
    pub sent_state: HashMap<String, SentMapState>,
    pub pending_map_updates: HashMap<String, Arc<PrecomputedChateauMap>>,
    pub map_build_in_progress: bool,
    pub trajectory_subscription: Option<crate::trajectories::ClientTrajectorySubscriptionParams>,
    pub trajectory_request_generation: u64,
    pub last_trajectory_sent_time: Option<Instant>,
    // Manage broadcast receivers
    pub chateau_receivers: HashMap<String, broadcast::Receiver<Arc<PrecomputedChateauMap>>>,
}

pub async fn handle_live_socket(socket: sockudo_ws::axum_integration::WebSocket, state: AppState) {
    let (mut receiver, mut sender) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    let session = Arc::new(RwLock::new(SessionState {
        hb: Instant::now(),
        client_viewport: None,
        client_viewport_v2: None,
        subscribed_chateaus: HashSet::new(),
        sent_state: HashMap::new(),
        pending_map_updates: HashMap::new(),
        map_build_in_progress: false,
        trajectory_subscription: None,
        trajectory_request_generation: 0,
        last_trajectory_sent_time: None,
        chateau_receivers: HashMap::new(),
    }));

    let mut writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    let session_bg = session.clone();
    let tx_bg = tx.clone();
    let state_bg = state.clone();

    let mut bg_task = tokio::spawn(async move {
        let mut coalesce_ticker = interval(MAP_UPDATE_COALESCE_INTERVAL);
        let mut traj_ticker = interval(Duration::from_secs(30));
        let mut hb_ticker = interval(HEARTBEAT_INTERVAL);
        let mut receiver_poll_ticker = interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = coalesce_ticker.tick() => {
                    let mut s_guard = session_bg.write().await;
                    if s_guard.map_build_in_progress || s_guard.pending_map_updates.is_empty() {
                        continue;
                    }

                    let is_v1 = s_guard.client_viewport.is_some();
                    let is_v2 = s_guard.client_viewport_v2.is_some();
                    if !is_v1 && !is_v2 {
                        continue;
                    }

                    s_guard.map_build_in_progress = true;
                    let updates: Vec<(String, Arc<PrecomputedChateauMap>)> = s_guard.pending_map_updates.drain().collect();
                    let sent_state = s_guard.sent_state.clone();
                    let v1_params = s_guard.client_viewport.clone();
                    let v2_params = s_guard.client_viewport_v2.clone();
                    let tx_clone = tx_bg.clone();
                    let session_clone = session_bg.clone();

                    tokio::spawn(async move {
                        let build_stream = futures_util::stream::iter(updates).map(move |(chateau_id, response)| {
                            let state_entry = sent_state.get(&chateau_id).cloned();
                            let v1 = v1_params.clone();
                            let v2 = v2_params.clone();

                            async move {
                                tokio::task::spawn_blocking(move || {
                                    if let Some(params) = v1 {
                                        build_map_update_message(chateau_id.clone(), response, params, state_entry)
                                    } else if let Some(params) = v2 {
                                        build_map_update_message_v2(chateau_id.clone(), response, params, state_entry)
                                    } else {
                                        None
                                    }
                                }).await.unwrap_or(None)
                            }
                        }).buffer_unordered(MAP_UPDATE_BUILD_CONCURRENCY);

                        let mut pinned_stream = std::pin::pin!(build_stream);
                        while let Some(opt_result) = pinned_stream.next().await {
                            if let Some((chateau_id, text, new_state)) = opt_result {
                                let _ = tx_clone.send(Message::Text(text.into_bytes().into()));
                                let mut sg = session_clone.write().await;
                                sg.sent_state.insert(chateau_id, new_state);
                            }
                        }

                        let mut sg = session_clone.write().await;
                        sg.map_build_in_progress = false;
                    });
                }
                _ = traj_ticker.tick() => {
                    let s_guard = session_bg.read().await;
                    if s_guard.trajectory_subscription.is_some() {
                        drop(s_guard);
                        trigger_trajectory_update(&session_bg, &tx_bg, &state_bg).await;
                    }
                }
                _ = hb_ticker.tick() => {
                    let last_hb = session_bg.read().await.hb;
                    if last_hb.elapsed() > CLIENT_TIMEOUT {
                        break;
                    }
                    let _ = tx_bg.send(Message::Ping(vec![].into()));
                }
                _ = receiver_poll_ticker.tick() => {
                    // Poll all active receivers non-blockingly
                    let mut s_guard = session_bg.write().await;
                    let mut to_process = Vec::new();

                    for (chateau_id, rx) in s_guard.chateau_receivers.iter_mut() {
                        while let Ok(map_update) = rx.try_recv() {
                            to_process.push((chateau_id.clone(), map_update));
                        }
                    }

                    for (chateau_id, map_update) in to_process {
                        s_guard.pending_map_updates.insert(chateau_id, map_update);
                    }
                }
            }
        }
    });

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
                        Ok(ClientMessage::UpdateMap { params }) => {
                            let new_chateaus: HashSet<String> =
                                params.chateaus.iter().cloned().collect();
                            let mut sg = session.write().await;
                            sg.client_viewport = Some(params.clone());
                            update_map_subscriptions(&mut sg, &tx, &state, new_chateaus).await;
                        }
                        Ok(ClientMessage::SubscribeTrajectories { params }) => {
                            session.write().await.trajectory_subscription = Some(params);
                            trigger_trajectory_update(&session, &tx, &state).await;
                        }
                        Ok(ClientMessage::UnsubscribeTrajectories) => {
                            let mut sg = session.write().await;
                            if let Some(params) = &sg.trajectory_subscription {
                                let update_timestamp = chrono::Utc::now().timestamp_millis() as u64;
                                let client_ref = params.client_reference.clone();
                                for chateau in &sg.subscribed_chateaus {
                                    let msg = ServerMessage::Buffer {
                                        timestamp: update_timestamp,
                                        client_reference: client_ref.clone(),
                                        chateau: chateau.clone(),
                                        content: vec![],
                                        chunk_index: 0,
                                        total_chunks: 0,
                                    };
                                    if let Ok(text) = serde_json::to_string(&msg) {
                                        let _ = tx.send(Message::Text(text.into_bytes().into()));
                                    }
                                }
                            }
                            sg.trajectory_subscription = None;
                        }
                        Ok(ClientMessage::SubscribeMapV2 { params }) => {
                            let mut new_chateaus = HashSet::new();
                            for ch in params
                                .bounds_input
                                .level8
                                .get_chateaus(&state.chateau_rtree, 8)
                            {
                                new_chateaus.insert(ch);
                            }
                            for ch in params
                                .bounds_input
                                .level12
                                .get_chateaus(&state.chateau_rtree, 12)
                            {
                                new_chateaus.insert(ch);
                            }
                            for ch in params
                                .bounds_input
                                .level7
                                .get_chateaus(&state.chateau_rtree, 7)
                            {
                                new_chateaus.insert(ch);
                            }
                            for ch in params
                                .bounds_input
                                .level5
                                .get_chateaus(&state.chateau_rtree, 5)
                            {
                                new_chateaus.insert(ch);
                            }

                            let mut sg = session.write().await;
                            sg.client_viewport_v2 = Some(params.clone());
                            update_map_subscriptions(&mut sg, &tx, &state, new_chateaus).await;
                        }
                        Ok(ClientMessage::UnsubscribeMapV2) => {
                            let mut sg = session.write().await;
                            sg.client_viewport_v2 = None;
                            sg.client_viewport = None;
                            update_map_subscriptions(&mut sg, &tx, &state, HashSet::new()).await;
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
                                message: "This endpoint only supports live locations.".to_string(),
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

async fn update_map_subscriptions(
    session: &mut tokio::sync::RwLockWriteGuard<'_, SessionState>,
    tx: &mpsc::UnboundedSender<Message>,
    state: &AppState,
    new_chateaus: HashSet<String>,
) {
    let removed: Vec<_> = session
        .subscribed_chateaus
        .difference(&new_chateaus)
        .cloned()
        .collect();
    let added: Vec<_> = new_chateaus
        .difference(&session.subscribed_chateaus)
        .cloned()
        .collect();

    for chateau_id in &removed {
        session.chateau_receivers.remove(chateau_id);
    }

    for chateau_id in &added {
        let rx = state.coordinator_pool.subscribe(chateau_id);
        session.chateau_receivers.insert(chateau_id.clone(), rx);
    }

    if let Some(params) = &session.trajectory_subscription {
        let update_timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let client_ref = params.client_reference.clone();
        for chateau_id in &removed {
            if !crate::trajectories::ALLOWED_CHATEAUX.contains(&chateau_id.as_str()) {
                continue;
            }
            let msg = ServerMessage::Buffer {
                timestamp: update_timestamp,
                client_reference: client_ref.clone(),
                chateau: chateau_id.clone(),
                content: vec![],
                chunk_index: 0,
                total_chunks: 0,
            };
            if let Ok(text) = serde_json::to_string(&msg) {
                let _ = tx.send(Message::Text(text.into_bytes().into()));
            }
        }
    }

    let chateaus_changed = session.subscribed_chateaus != new_chateaus;
    session.subscribed_chateaus = new_chateaus;

    // We can't await inside the lock easily for trigger_trajectory_update
    // so it will be polled by the bg task naturally since trajectory_subscription is still Some.
}

fn build_map_update_message(
    chateau_id: String,
    response: Arc<PrecomputedChateauMap>,
    params: MapViewportUpdate,
    sent_state_entry: Option<SentMapState>,
) -> Option<(String, String, SentMapState)> {
    if !params.chateaus.contains(&chateau_id) {
        return None;
    }

    let categories_requested = params
        .categories
        .iter()
        .filter_map(|category| match category.as_str() {
            "metro" => Some(CategoryOfRealtimeVehicleData::Metro),
            "bus" => Some(CategoryOfRealtimeVehicleData::Bus),
            "rail" => Some(CategoryOfRealtimeVehicleData::Rail),
            "other" => Some(CategoryOfRealtimeVehicleData::Other),
            _ => None,
        })
        .collect::<Vec<CategoryOfRealtimeVehicleData>>();

    let mut each_chateau_response = EachChateauResponseV2 {
        categories: Some(PositionDataCategoryV2::default()),
    };

    let (last_sent_time, last_sent_bounds, last_sent_categories) = match &sent_state_entry {
        Some((t, b, c)) => (*t, Some(b), Some(c)),
        None => (0, None, None),
    };

    let is_new_feed = response.last_updated_time_ms != last_sent_time;
    let mut has_any_updates = false;

    for category in &categories_requested {
        let category_str = match category {
            CategoryOfRealtimeVehicleData::Metro => "metro",
            CategoryOfRealtimeVehicleData::Bus => "bus",
            CategoryOfRealtimeVehicleData::Rail => "rail",
            CategoryOfRealtimeVehicleData::Other => "other",
        };

        let category_is_new = last_sent_categories
            .as_ref()
            .map_or(true, |c| !c.contains(category_str));

        let zoom = match category {
            CategoryOfRealtimeVehicleData::Metro => 8,
            CategoryOfRealtimeVehicleData::Rail => 7,
            CategoryOfRealtimeVehicleData::Bus => 12,
            CategoryOfRealtimeVehicleData::Other => 5,
        };

        let bounds = match category {
            CategoryOfRealtimeVehicleData::Metro => &params.bounds_input.level8,
            CategoryOfRealtimeVehicleData::Rail => &params.bounds_input.level7,
            CategoryOfRealtimeVehicleData::Bus => &params.bounds_input.level12,
            CategoryOfRealtimeVehicleData::Other => &params.bounds_input.level5,
        };

        let prev_bounds_for_level = last_sent_bounds.as_ref().map(|b| match category {
            CategoryOfRealtimeVehicleData::Metro => &b.level8,
            CategoryOfRealtimeVehicleData::Rail => &b.level7,
            CategoryOfRealtimeVehicleData::Bus => &b.level12,
            CategoryOfRealtimeVehicleData::Other => &b.level5,
        });

        let replace_all = is_new_feed || prev_bounds_for_level.is_none() || category_is_new;

        let bounds_changed = if let Some(pb) = prev_bounds_for_level {
            pb.min_x != bounds.min_x
                || pb.max_x != bounds.max_x
                || pb.min_y != bounds.min_y
                || pb.max_y != bounds.max_y
        } else {
            true
        };

        if !replace_all && !bounds_changed {
            continue;
        }

        has_any_updates = true;

        let precomputed_category = match category {
            CategoryOfRealtimeVehicleData::Metro => &response.metro,
            CategoryOfRealtimeVehicleData::Bus => &response.bus,
            CategoryOfRealtimeVehicleData::Rail => &response.rail,
            CategoryOfRealtimeVehicleData::Other => &response.other,
        };

        let mut vehicles_by_tile: BTreeMap<
            u32,
            BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>,
        > = BTreeMap::new();

        if replace_all {
            for (&x, y_map) in precomputed_category
                .tiles
                .range(bounds.min_x..=bounds.max_x)
            {
                let in_bounds_y = y_map.range(bounds.min_y..=bounds.max_y);
                for (&y, vehicles) in in_bounds_y {
                    vehicles_by_tile
                        .entry(x)
                        .or_default()
                        .insert(y, vehicles.clone());
                }
            }
        } else if let Some(prev_bounds) = prev_bounds_for_level {
            for (&x, y_map) in precomputed_category
                .tiles
                .range(bounds.min_x..=bounds.max_x)
            {
                let in_bounds_y = y_map.range(bounds.min_y..=bounds.max_y);
                for (&y, vehicles) in in_bounds_y {
                    let in_prev_bounds = x >= prev_bounds.min_x
                        && x <= prev_bounds.max_x
                        && y >= prev_bounds.min_y
                        && y <= prev_bounds.max_y;
                    if !in_prev_bounds {
                        vehicles_by_tile
                            .entry(x)
                            .or_default()
                            .insert(y, vehicles.clone());
                    }
                }
            }
        }

        let list_of_agency_ids = precomputed_category.agency_ids.clone();

        let payload = EachCategoryPayloadV2 {
            vehicle_positions: match vehicles_by_tile.is_empty() {
                false => Some(vehicles_by_tile),
                true => None,
            },
            last_updated_time_ms: response.last_updated_time_ms,
            replaces_all: replace_all,
            z_level: zoom,
            list_of_agency_ids,
        };

        match category {
            CategoryOfRealtimeVehicleData::Metro => {
                each_chateau_response.categories.as_mut().unwrap().metro = Some(payload);
            }
            CategoryOfRealtimeVehicleData::Rail => {
                each_chateau_response.categories.as_mut().unwrap().rail = Some(payload);
            }
            CategoryOfRealtimeVehicleData::Other => {
                each_chateau_response.categories.as_mut().unwrap().other = Some(payload);
            }
            CategoryOfRealtimeVehicleData::Bus => {
                each_chateau_response.categories.as_mut().unwrap().bus = Some(payload);
            }
        }
    }

    if !has_any_updates {
        return None;
    }

    let mut bulk_fetch_response = BulkFetchResponseV2 {
        chateaus: BTreeMap::new(),
    };
    bulk_fetch_response
        .chateaus
        .insert(chateau_id.clone(), each_chateau_response);

    let msg = ServerMessage::MapUpdate(bulk_fetch_response);
    let text = serde_json::to_string(&msg).ok()?;

    let categories_sent: HashSet<String> = params.categories.iter().cloned().collect();
    let new_state = (
        response.last_updated_time_ms,
        params.bounds_input.clone(),
        categories_sent,
    );

    Some((chateau_id, text, new_state))
}

fn build_map_update_message_v2(
    chateau_id: String,
    response: Arc<PrecomputedChateauMap>,
    params: crate::SubscribeMapV2Params,
    sent_state_entry: Option<SentMapState>,
) -> Option<(String, String, SentMapState)> {
    let mut each_chateau_response = EachChateauResponseV2 {
        categories: Some(PositionDataCategoryV2::default()),
    };

    let (last_sent_time, last_sent_bounds, last_sent_categories) = match &sent_state_entry {
        Some((t, b, c)) => (*t, Some(b), Some(c)),
        None => (0, None, None),
    };

    let is_new_feed = response.last_updated_time_ms != last_sent_time;
    let mut has_any_updates = false;

    let categories_requested = params
        .categories
        .iter()
        .filter_map(|category| match category.as_str() {
            "metro" => Some(CategoryOfRealtimeVehicleData::Metro),
            "bus" => Some(CategoryOfRealtimeVehicleData::Bus),
            "rail" => Some(CategoryOfRealtimeVehicleData::Rail),
            "other" => Some(CategoryOfRealtimeVehicleData::Other),
            _ => None,
        })
        .collect::<Vec<CategoryOfRealtimeVehicleData>>();

    for category in &categories_requested {
        let category_str = match category {
            CategoryOfRealtimeVehicleData::Metro => "metro",
            CategoryOfRealtimeVehicleData::Bus => "bus",
            CategoryOfRealtimeVehicleData::Rail => "rail",
            CategoryOfRealtimeVehicleData::Other => "other",
        };

        let category_is_new = last_sent_categories
            .as_ref()
            .map_or(true, |c| !c.contains(category_str));

        let zoom = match category {
            CategoryOfRealtimeVehicleData::Metro => 8,
            CategoryOfRealtimeVehicleData::Rail => 7,
            CategoryOfRealtimeVehicleData::Bus => 12,
            CategoryOfRealtimeVehicleData::Other => 5,
        };

        let bounds = match category {
            CategoryOfRealtimeVehicleData::Metro => &params.bounds_input.level8,
            CategoryOfRealtimeVehicleData::Rail => &params.bounds_input.level7,
            CategoryOfRealtimeVehicleData::Bus => &params.bounds_input.level12,
            CategoryOfRealtimeVehicleData::Other => &params.bounds_input.level5,
        };

        let prev_bounds_for_level = last_sent_bounds.as_ref().map(|b| match category {
            CategoryOfRealtimeVehicleData::Metro => &b.level8,
            CategoryOfRealtimeVehicleData::Rail => &b.level7,
            CategoryOfRealtimeVehicleData::Bus => &b.level12,
            CategoryOfRealtimeVehicleData::Other => &b.level5,
        });

        let replace_all = is_new_feed || prev_bounds_for_level.is_none() || category_is_new;

        let bounds_changed = if let Some(pb) = prev_bounds_for_level {
            pb.min_x != bounds.min_x
                || pb.max_x != bounds.max_x
                || pb.min_y != bounds.min_y
                || pb.max_y != bounds.max_y
        } else {
            true
        };

        if !replace_all && !bounds_changed {
            continue;
        }

        has_any_updates = true;

        let precomputed_category = match category {
            CategoryOfRealtimeVehicleData::Metro => &response.metro,
            CategoryOfRealtimeVehicleData::Bus => &response.bus,
            CategoryOfRealtimeVehicleData::Rail => &response.rail,
            CategoryOfRealtimeVehicleData::Other => &response.other,
        };

        let mut vehicles_by_tile: BTreeMap<
            u32,
            BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>,
        > = BTreeMap::new();

        if replace_all {
            for (&x, y_map) in precomputed_category
                .tiles
                .range(bounds.min_x..=bounds.max_x)
            {
                let in_bounds_y = y_map.range(bounds.min_y..=bounds.max_y);
                for (&y, vehicles) in in_bounds_y {
                    vehicles_by_tile
                        .entry(x)
                        .or_default()
                        .insert(y, vehicles.clone());
                }
            }
        } else if let Some(prev_bounds) = prev_bounds_for_level {
            for (&x, y_map) in precomputed_category
                .tiles
                .range(bounds.min_x..=bounds.max_x)
            {
                let in_bounds_y = y_map.range(bounds.min_y..=bounds.max_y);
                for (&y, vehicles) in in_bounds_y {
                    let in_prev_bounds = x >= prev_bounds.min_x
                        && x <= prev_bounds.max_x
                        && y >= prev_bounds.min_y
                        && y <= prev_bounds.max_y;
                    if !in_prev_bounds {
                        vehicles_by_tile
                            .entry(x)
                            .or_default()
                            .insert(y, vehicles.clone());
                    }
                }
            }
        }

        let list_of_agency_ids = precomputed_category.agency_ids.clone();

        let payload = EachCategoryPayloadV2 {
            vehicle_positions: match vehicles_by_tile.is_empty() {
                false => Some(vehicles_by_tile),
                true => None,
            },
            last_updated_time_ms: response.last_updated_time_ms,
            replaces_all: replace_all,
            z_level: zoom,
            list_of_agency_ids,
        };

        match category {
            CategoryOfRealtimeVehicleData::Metro => {
                each_chateau_response.categories.as_mut().unwrap().metro = Some(payload);
            }
            CategoryOfRealtimeVehicleData::Rail => {
                each_chateau_response.categories.as_mut().unwrap().rail = Some(payload);
            }
            CategoryOfRealtimeVehicleData::Other => {
                each_chateau_response.categories.as_mut().unwrap().other = Some(payload);
            }
            CategoryOfRealtimeVehicleData::Bus => {
                each_chateau_response.categories.as_mut().unwrap().bus = Some(payload);
            }
        }
    }

    if !has_any_updates {
        return None;
    }

    let mut bulk_fetch_response = BulkFetchResponseV2 {
        chateaus: BTreeMap::new(),
    };
    bulk_fetch_response
        .chateaus
        .insert(chateau_id.clone(), each_chateau_response);

    let msg = ServerMessage::MapUpdate(bulk_fetch_response);
    let text = serde_json::to_string(&msg).ok()?;

    let categories_sent: HashSet<String> = params.categories.iter().cloned().collect();
    let new_state = (
        response.last_updated_time_ms,
        params.bounds_input.clone(),
        categories_sent,
    );

    Some((chateau_id, text, new_state))
}

async fn trigger_trajectory_update(
    session: &Arc<RwLock<SessionState>>,
    tx: &mpsc::UnboundedSender<Message>,
    state: &AppState,
) {
    let mut s_guard = session.write().await;
    let Some(client_params) = s_guard.trajectory_subscription.clone() else {
        return;
    };

    let pool = state.pool.clone();
    let etcd_ips = state.etcd_connection_ips.clone();
    let etcd_opts = state.etcd_connection_options.clone();
    let manager = state.aspen_client_manager.clone();
    let etcd_reuser = state.etcd_reuser.clone();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let look_ahead_ms = 90000;
    let latency_buffer_ms = 15000;
    let start_time_ms = now_ms - latency_buffer_ms;
    let end_time_ms = now_ms + look_ahead_ms + latency_buffer_ms;

    let params = trajectories::TrajectorySubscriptionParams {
        bbox: client_params.bbox,
        zoom: client_params.zoom,
        modes: client_params.modes,
        start_time_ms,
        end_time_ms,
        precision: client_params.precision,
        client_reference: client_params.client_reference.clone(),
    };

    s_guard.trajectory_request_generation = s_guard.trajectory_request_generation.wrapping_add(1);
    let current_gen = s_guard.trajectory_request_generation;
    let update_timestamp = chrono::Utc::now().timestamp_millis() as u64;

    let chateaus = s_guard
        .subscribed_chateaus
        .clone()
        .into_iter()
        .filter(|x| crate::trajectories::ALLOWED_CHATEAUX.contains(&x.as_str()))
        .collect::<Vec<_>>();

    drop(s_guard);

    for ch in chateaus {
        let ch_clone = ch.clone();
        let pool_clone = pool.clone();
        let ips_clone = etcd_ips.clone();
        let opts_clone = etcd_opts.clone();
        let manager_clone = manager.clone();
        let reuser_clone = etcd_reuser.clone();
        let params_clone = params.clone();
        let client_ref = client_params.client_reference.clone();

        let client_ref_clone = client_ref.clone();
        let ch_clone2 = ch.clone();

        let tx_clone = tx.clone();
        let session_clone = session.clone();

        tokio::spawn(async move {
            let trajectories = trajectories::get_single_chateau_trajectories(
                pool_clone,
                ips_clone,
                opts_clone,
                manager_clone,
                reuser_clone,
                params_clone,
                ch_clone,
            )
            .await;

            let messages = tokio::task::spawn_blocking(move || {
                let chunks: Vec<_> = trajectories.chunks(200).collect();
                let total_chunks = chunks.len();
                let mut serialized_messages = Vec::new();

                if chunks.is_empty() {
                    let msg = ServerMessage::Buffer {
                        timestamp: update_timestamp,
                        client_reference: client_ref_clone.clone(),
                        chateau: ch_clone2.clone(),
                        content: vec![],
                        chunk_index: 0,
                        total_chunks: 0,
                    };
                    if let Ok(text) = serde_json::to_string(&msg) {
                        serialized_messages.push(text);
                    }
                } else {
                    for (i, chunk) in chunks.into_iter().enumerate() {
                        let msg = ServerMessage::Buffer {
                            timestamp: update_timestamp,
                            client_reference: client_ref_clone.clone(),
                            chateau: ch_clone2.clone(),
                            content: chunk.to_vec(),
                            chunk_index: i,
                            total_chunks,
                        };
                        if let Ok(text) = serde_json::to_string(&msg) {
                            serialized_messages.push(text);
                        }
                    }
                }
                serialized_messages
            })
            .await
            .unwrap_or_default();

            let mut sg = session_clone.write().await;
            if current_gen != sg.trajectory_request_generation {
                return;
            }

            for text in messages {
                let _ = tx_clone.send(Message::Text(text.into_bytes().into()));
            }
            sg.last_trajectory_sent_time = Some(Instant::now());
        });
    }
}
