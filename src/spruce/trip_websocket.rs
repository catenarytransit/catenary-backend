use crate::map_coordinator::{
    AspenisedVehiclePositionOutput, BoundsInputV3, BulkFetchCoordinatorPool, BulkFetchParamsV3,
    BulkFetchResponseV2, CategoryAskParamsV2, CategoryOfRealtimeVehicleData, ChateauUpdate,
    EachCategoryPayloadV2, EachChateauResponseV2, PositionDataCategoryV2, PrecomputedChateauMap,
    Subscribe, Unsubscribe, category_to_allowed_route_ids, convert_to_output,
};
use actix::prelude::*;
use actix_web_actors::ws;
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, TripIntroductionInformation,
    fetch_trip_information, fetch_trip_rt_update,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const UPDATE_INTERVAL: Duration = Duration::from_millis(300);
const MAP_UPDATE_COALESCE_INTERVAL: Duration = Duration::from_millis(50);
const MAP_UPDATE_BUILD_CONCURRENCY: usize = 4;

type SentMapState = (u64, BoundsInputV3, HashSet<String>);

#[derive(Message)]
#[rtype(result = "()")]
enum MapBuildEvent {
    Built {
        chateau_id: String,
        text: String,
        new_state: SentMapState,
    },
    Finished,
}

use crate::{ClientMessage, MapViewportUpdate, ServerMessage, nearby_departures, trajectories};
use futures::StreamExt;

// Messages moved to main.rs

pub struct TripWebSocket {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub subscriptions: HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: Instant,

    // Map related fields
    pub coordinator_pool: Arc<BulkFetchCoordinatorPool>,
    pub client_viewport: Option<MapViewportUpdate>,
    pub subscribed_chateaus: HashSet<String>,
    // ChateauID -> (LastTime, LastBounds, LastCategories)
    pub sent_state: HashMap<String, SentMapState>,
    pub map_update_generation: u64,
    pub pending_map_updates: HashMap<String, Arc<PrecomputedChateauMap>>,
    pub map_build_in_progress: bool,

    pub chateau_rtree: Arc<crate::chateau_rtree::ChateauRTree>,
    pub client_viewport_v2: Option<CategoryAskParamsV2>,

    pub etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    pub aspen_endpoint_cache: HashMap<String, (std::net::SocketAddr, Instant)>,
    pub in_progress_trip_fetches: HashMap<(String, QueryTripInformationParams), Instant>,

    // Trajectory fields
    pub trajectory_subscription: Option<trajectories::ClientTrajectorySubscriptionParams>,
    pub last_trajectory_sent_time: Option<Instant>,
}

impl TripWebSocket {
    pub fn new(
        pool: Arc<CatenaryPostgresPool>,
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
        coordinator_pool: Arc<BulkFetchCoordinatorPool>,
        etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
        chateau_rtree: Arc<crate::chateau_rtree::ChateauRTree>,
    ) -> Self {
        Self {
            pool,
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            subscriptions: HashMap::new(),
            hb: Instant::now(),
            coordinator_pool,
            client_viewport: None,
            subscribed_chateaus: HashSet::new(),
            sent_state: HashMap::new(),
            map_update_generation: 0,
            pending_map_updates: HashMap::new(),
            map_build_in_progress: false,
            chateau_rtree,
            client_viewport_v2: None,
            etcd_reuser,
            aspen_endpoint_cache: HashMap::new(),
            in_progress_trip_fetches: HashMap::new(),
            trajectory_subscription: None,
            last_trajectory_sent_time: None,
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

    fn start_map_update_coalescer(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(MAP_UPDATE_COALESCE_INTERVAL, |act, ctx| {
            if act.map_build_in_progress || act.pending_map_updates.is_empty() {
                return;
            }

            let params = if let Some(v1_params) = act.client_viewport.clone() {
                Some(v1_params)
            } else if let Some(v2_params) = act.client_viewport_v2.clone() {
                let mut categories = Vec::new();
                let mut bounds_input = BoundsInputV3 {
                    level5: crate::map_coordinator::BoundsInputPerLevel { min_x: 0, min_y: 0, max_x: 0, max_y: 0 },
                    level7: crate::map_coordinator::BoundsInputPerLevel { min_x: 0, min_y: 0, max_x: 0, max_y: 0 },
                    level8: crate::map_coordinator::BoundsInputPerLevel { min_x: 0, min_y: 0, max_x: 0, max_y: 0 },
                    level12: crate::map_coordinator::BoundsInputPerLevel { min_x: 0, min_y: 0, max_x: 0, max_y: 0 },
                };
                
                let mut populate = |sub: &Option<crate::map_coordinator::SubCategoryAskParamsV2>, name: &str, target_bbox: &mut crate::map_coordinator::BoundsInputPerLevel| {
                    if let Some(s) = sub {
                        categories.push(name.to_string());
                        if let (Some(min_x), Some(max_x), Some(min_y), Some(max_y)) = (s.prev_user_min_x, s.prev_user_max_x, s.prev_user_min_y, s.prev_user_max_y) {
                            target_bbox.min_x = min_x;
                            target_bbox.max_x = max_x;
                            target_bbox.min_y = min_y;
                            target_bbox.max_y = max_y;
                        }
                    }
                };

                populate(&v2_params.bus, "bus", &mut bounds_input.level12);
                populate(&v2_params.metro, "metro", &mut bounds_input.level8);
                populate(&v2_params.rail, "rail", &mut bounds_input.level7);
                populate(&v2_params.other, "other", &mut bounds_input.level5);

                Some(crate::MapViewportUpdate {
                    chateaus: act.subscribed_chateaus.iter().cloned().collect(),
                    categories,
                    bounds_input,
                })
            } else {
                None
            };

            let Some(params) = params else {
                return;
            };

            println!("DEBUG: start_map_update_coalescer running with chateaus: {:?}, updates pending: {}", params.chateaus, act.pending_map_updates.len());

            act.map_build_in_progress = true;

            let updates: Vec<(String, Arc<PrecomputedChateauMap>)> =
                act.pending_map_updates.drain().collect();
            let sent_state = act.sent_state.clone();

            let build_stream = futures::stream::iter(updates)
                .map(move |(chateau_id, response)| {
                    let state_entry = sent_state.get(&chateau_id).cloned();
                    let params_clone = params.clone();

                    async move {
                        match tokio::task::spawn_blocking(move || {
                            Self::build_map_update_message(
                                chateau_id,
                                response,
                                params_clone,
                                state_entry,
                            )
                            .map(|(chateau_id, text, new_state)| {
                                MapBuildEvent::Built {
                                    chateau_id,
                                    text,
                                    new_state,
                                }
                            })
                        })
                        .await
                        {
                            Ok(event) => event,
                            Err(e) => {
                                eprintln!("Failed to build map_update websocket message: {:?}", e);
                                None
                            }
                        }
                    }
                })
                .buffer_unordered(MAP_UPDATE_BUILD_CONCURRENCY)
                .filter_map(|event| async move { event })
                .chain(futures::stream::once(async { MapBuildEvent::Finished }));

            ctx.add_message_stream(build_stream);
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
                                        if let Some(ts) = data.timestamp {
                                            let key = (chateau_clone.clone(), params_clone.clone());
                                            if let Some(current_last_update) =
                                                act.subscriptions.get_mut(&key)
                                            {
                                                if let Some(last) = current_last_update {
                                                    if *last == ts {
                                                        return;
                                                    }
                                                }
                                                *current_last_update = Some(ts);
                                            }
                                        }

                                        let msg = ServerMessage::UpdateTrip { data };
                                        let text = serde_json::to_string(&msg).unwrap();
                                        ctx.text(text);
                                    }
                                }
                            }
                            Err(e) => {
                                act.aspen_endpoint_cache.remove(&chateau_clone);
                            }
                        }
                    },
                );

                ctx.spawn(fut);
            }
        });
    }

    fn update_map_subscriptions(
        &mut self,
        ctx: &mut ws::WebsocketContext<Self>,
        new_chateaus: HashSet<String>,
    ) {
        let to_unsubscribe: Vec<String> = self
            .subscribed_chateaus
            .difference(&new_chateaus)
            .cloned()
            .collect();
        let to_subscribe: Vec<String> = new_chateaus
            .difference(&self.subscribed_chateaus)
            .cloned()
            .collect();

        for ch in to_unsubscribe {
            let coordinator = self.coordinator_pool.for_chateau(&ch);
            coordinator.do_send(Unsubscribe {
                chateau_id: ch.clone(),
                recipient: ctx.address().recipient(),
            });
            self.sent_state.remove(&ch);
        }

        for ch in to_subscribe {
            let coordinator = self.coordinator_pool.for_chateau(&ch);
            coordinator.do_send(Subscribe {
                chateau_id: ch,
                recipient: ctx.address().recipient(),
            });
        }

        self.subscribed_chateaus = new_chateaus;
    }

    fn build_map_update_message(
        chateau_id: String,
        response: Arc<PrecomputedChateauMap>,
        params: MapViewportUpdate,
        sent_state_entry: Option<SentMapState>,
    ) -> Option<(String, String, SentMapState)> {
        println!(
            "DEBUG: build_map_update_message started for chateau {}",
            chateau_id
        );
        if !params.chateaus.contains(&chateau_id) {
            println!(
                "DEBUG: build_map_update_message dropped chateau {} (not in params.chateaus)",
                chateau_id
            );
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

    fn process_map_update(
        &mut self,
        chateau_id: &String,
        response: &Arc<PrecomputedChateauMap>,
        _ctx: &mut ws::WebsocketContext<Self>,
    ) {
        if !self.subscribed_chateaus.contains(chateau_id) {
            return;
        }

        self.pending_map_updates
            .insert(chateau_id.clone(), response.clone());
    }

    fn start_trajectory_updates(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(30), |act, ctx| {
            if act.trajectory_subscription.is_none() {
                return;
            }
            act.trigger_trajectory_update(ctx);
        });
    }

    fn trigger_trajectory_update(&self, ctx: &mut ws::WebsocketContext<Self>) {
        let Some(client_params) = self.trajectory_subscription.clone() else {
            return;
        };
        let pool = self.pool.clone();
        let etcd_ips = self.etcd_connection_ips.clone();
        let etcd_opts = self.etcd_connection_options.clone();
        let manager = self.aspen_client_manager.clone();
        let etcd_reuser = self.etcd_reuser.clone();

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
            client_reference: client_params.client_reference,
        };

        let chateaus = self.subscribed_chateaus.clone();
        let fut = async move {
            trajectories::get_trajectories(
                pool,
                etcd_ips,
                etcd_opts,
                manager,
                etcd_reuser,
                params,
                chateaus,
            )
            .await
        };

        let fut = actix::fut::wrap_future(fut).map(
            |result, act: &mut TripWebSocket, ctx: &mut ws::WebsocketContext<Self>| match result {
                Ok(trajectories) => {
                    let client_ref = act
                        .trajectory_subscription
                        .as_ref()
                        .map_or("".to_string(), |s| s.client_reference.clone());
                    let msg = ServerMessage::Buffer {
                        timestamp: chrono::Utc::now().timestamp_millis() as u64,
                        client_reference: client_ref,
                        content: trajectories,
                    };
                    if let Ok(text) = serde_json::to_string(&msg) {
                        ctx.text(text);
                    }
                    act.last_trajectory_sent_time = Some(Instant::now());
                }
                Err(err) => {
                    eprintln!("Error fetching trajectories: {}", err);
                }
            },
        );
        ctx.spawn(fut);
    }
}

impl Actor for TripWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        self.start_periodic_updates(ctx);
        self.start_map_update_coalescer(ctx);
        self.start_trajectory_updates(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> actix::Running {
        for ch in &self.subscribed_chateaus {
            let coordinator = self.coordinator_pool.for_chateau(ch);
            coordinator.do_send(Unsubscribe {
                chateau_id: ch.clone(),
                recipient: ctx.address().recipient(),
            });
        }
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

                        // Fetch initial data immediately
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
                    Ok(ClientMessage::UpdateMap { params }) => {
                        //println!(
                        //    "DEBUG: TripWebSocket received UpdateMap (chateaus: {:?})",
                        //    params.chateaus
                        //);
                        let new_chateaus: HashSet<String> =
                            params.chateaus.iter().cloned().collect();
                        self.map_update_generation = self.map_update_generation.wrapping_add(1);
                        self.client_viewport = Some(params.clone());
                        self.update_map_subscriptions(ctx, new_chateaus);

                        // Request updates for all subscribed chateaus (cached ones will come back instantly).
                        // Each chateau is routed to a stable coordinator shard.
                        for ch in &self.subscribed_chateaus {
                            let coordinator = self.coordinator_pool.for_chateau(ch);
                            coordinator.do_send(Subscribe {
                                chateau_id: ch.clone(),
                                recipient: ctx.address().recipient(),
                            });
                        }
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
                    Ok(ClientMessage::SubscribeTrajectories { params }) => {
                        self.trajectory_subscription = Some(params);
                        self.trigger_trajectory_update(ctx);
                    }
                    Ok(ClientMessage::UnsubscribeTrajectories) => {
                        self.trajectory_subscription = None;
                    }
                    Ok(ClientMessage::SubscribeMapV2 { params }) => {
                        self.client_viewport_v2 = Some(params.clone());
                        let mut new_chateaus = HashSet::new();

                        let get_bounds =
                            |sub: &Option<crate::map_coordinator::SubCategoryAskParamsV2>,
                             zoom: u8|
                             -> Option<(f64, f64, f64, f64)> {
                                if let Some(s) = sub {
                                    if let (Some(min_x), Some(max_x), Some(min_y), Some(max_y)) = (
                                        s.prev_user_min_x,
                                        s.prev_user_max_x,
                                        s.prev_user_min_y,
                                        s.prev_user_max_y,
                                    ) {
                                        let n = f64::powi(2.0, zoom as i32);
                                        let min_lon = (min_x as f64) / n * 360.0 - 180.0;
                                        let max_lon = ((max_x + 1) as f64) / n * 360.0 - 180.0;

                                        let min_y_f64 = min_y as f64;
                                        let max_y_f64 = (max_y + 1) as f64;

                                        let max_lat_rad = (std::f64::consts::PI
                                            * (1.0 - 2.0 * min_y_f64 / n))
                                            .sinh()
                                            .atan();
                                        let max_lat = max_lat_rad * 180.0 / std::f64::consts::PI;

                                        let min_lat_rad = (std::f64::consts::PI
                                            * (1.0 - 2.0 * max_y_f64 / n))
                                            .sinh()
                                            .atan();
                                        let min_lat = min_lat_rad * 180.0 / std::f64::consts::PI;

                                        return Some((min_lon, min_lat, max_lon, max_lat));
                                    }
                                }
                                None
                            };

                        let bounds_bus = get_bounds(&params.bus, 12);
                        let bounds_metro = get_bounds(&params.metro, 8);
                        let bounds_rail = get_bounds(&params.rail, 7);
                        let bounds_other = get_bounds(&params.other, 5);

                        println!(
                            "DEBUG: SubscribeMapV2 received! Raw params: {}",
                            serde_json::to_string(&params).unwrap_or_else(|_| "err".into())
                        );

                        println!(
                            "DEBUG: SubscribeMapV2 received! Bounds: Bus={:?}, Metro={:?}, Rail={:?}, Other={:?}",
                            bounds_bus, bounds_metro, bounds_rail, bounds_other
                        );

                        for bounds in [bounds_bus, bounds_metro, bounds_rail, bounds_other]
                            .into_iter()
                            .flatten()
                        {
                            let chateaus = self
                                .chateau_rtree
                                .locate_in_envelope(bounds.0, bounds.1, bounds.2, bounds.3);
                            new_chateaus.extend(chateaus);
                        }

                        println!(
                            "DEBUG: SubscribeMapV2 resolved to {} chateaus: {:?}",
                            new_chateaus.len(),
                            new_chateaus
                        );

                        self.map_update_generation = self.map_update_generation.wrapping_add(1);
                        self.update_map_subscriptions(ctx, new_chateaus);

                        // Request updates for all subscribed chateaus
                        for ch in &self.subscribed_chateaus {
                            let coordinator = self.coordinator_pool.for_chateau(ch);
                            println!("DEBUG: Sending Subscribe to coordinator for {}", ch);
                            coordinator.do_send(crate::map_coordinator::Subscribe {
                                chateau_id: ch.clone(),
                                recipient: ctx.address().recipient(),
                            });
                        }
                    }
                    Ok(ClientMessage::UnsubscribeMapV2) => {
                        self.client_viewport_v2 = None;
                        self.map_update_generation = self.map_update_generation.wrapping_add(1);
                        self.update_map_subscriptions(ctx, HashSet::new());
                    }
                    Err(e) => {
                        let msg = ServerMessage::Error {
                            message: format!("Invalid message: {}", e),
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

impl Handler<MapBuildEvent> for TripWebSocket {
    type Result = ();

    fn handle(&mut self, event: MapBuildEvent, ctx: &mut Self::Context) {
        match event {
            MapBuildEvent::Built {
                chateau_id,
                text,
                new_state,
            } => {
                if !self.subscribed_chateaus.contains(&chateau_id) {
                    return;
                }

                if let Some((current_last_updated_time_ms, _, _)) = self.sent_state.get(&chateau_id)
                {
                    if *current_last_updated_time_ms > new_state.0 {
                        return;
                    }
                }

                ctx.text(text);
                self.sent_state.insert(chateau_id, new_state);
            }
            MapBuildEvent::Finished => {
                self.map_build_in_progress = false;
            }
        }
    }
}

impl Handler<ChateauUpdate> for TripWebSocket {
    type Result = ();

    fn handle(&mut self, msg: ChateauUpdate, ctx: &mut Self::Context) {
        self.process_map_update(&msg.chateau_id, &msg.response, ctx);
    }
}
