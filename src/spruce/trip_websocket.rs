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
    pub client_viewport_v2: Option<crate::SubscribeMapV2Params>,

    pub etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    pub aspen_endpoint_cache: HashMap<String, (std::net::SocketAddr, Instant)>,
    pub in_progress_trip_fetches: HashMap<(String, QueryTripInformationParams), Instant>,

    pub trajectory_subscription: Option<crate::trajectories::ClientTrajectorySubscriptionParams>,
    pub trajectory_request_generation: u64,
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
            trajectory_request_generation: 0,
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

            let is_v1 = act.client_viewport.is_some();
            let is_v2 = act.client_viewport_v2.is_some();

            if !is_v1 && !is_v2 {
                return;
            }

            act.map_build_in_progress = true;

            let updates: Vec<(String, Arc<PrecomputedChateauMap>)> =
                act.pending_map_updates.drain().collect();
            let sent_state = act.sent_state.clone();

            let v1_params = act.client_viewport.clone();
            let v2_params = act.client_viewport_v2.clone();

            let build_stream = futures::stream::iter(updates)
                .map(move |(chateau_id, response)| {
                    let state_entry = sent_state.get(&chateau_id).cloned();
                    let v1_params_clone = v1_params.clone();
                    let v2_params_clone = v2_params.clone();

                    async move {
                        match tokio::task::spawn_blocking(move || {
                            if let Some(params) = v1_params_clone {
                                Self::build_map_update_message(
                                    chateau_id,
                                    response,
                                    params,
                                    state_entry,
                                )
                            } else if let Some(params) = v2_params_clone {
                                Self::build_map_update_message_v2(
                                    chateau_id,
                                    response,
                                    params,
                                    state_entry,
                                )
                            } else {
                                None
                            }
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
        let removed: Vec<_> = self
            .subscribed_chateaus
            .difference(&new_chateaus)
            .cloned()
            .collect();
        for chateau_id in &removed {
            let coordinator = self.coordinator_pool.for_chateau(chateau_id);
            coordinator.do_send(Unsubscribe {
                chateau_id: chateau_id.clone(),
                recipient: ctx.address().recipient(),
            });
        }

        let chateaus_changed = self.subscribed_chateaus != new_chateaus;
        self.subscribed_chateaus = new_chateaus;

        if chateaus_changed && self.trajectory_subscription.is_some() {
            self.trigger_trajectory_update(ctx);
        }
    }

    fn build_map_update_message(
        chateau_id: String,
        response: Arc<PrecomputedChateauMap>,
        params: MapViewportUpdate,
        sent_state_entry: Option<SentMapState>,
    ) -> Option<(String, String, SentMapState)> {
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

            let mut filtered_count = 0;
            for y_map in vehicles_by_tile.values() {
                for vehicles in y_map.values() {
                    filtered_count += vehicles.len();
                }
            }
            let precomputed_count = precomputed_category.raw_vehicles.len();

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

    fn start_trajectory_updates(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(30), |act, ctx| {
            if act.trajectory_subscription.is_none() {
                return;
            }
            act.trigger_trajectory_update(ctx);
        });
    }

    fn trigger_trajectory_update(&mut self, ctx: &mut ws::WebsocketContext<Self>) {
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
            client_reference: client_params.client_reference.clone(),
        };

        self.trajectory_request_generation = self.trajectory_request_generation.wrapping_add(1);
        let current_gen = self.trajectory_request_generation;
        let update_timestamp = chrono::Utc::now().timestamp_millis() as u64;

        let chateaus = self.subscribed_chateaus.clone().into_iter().filter(|x| crate::trajectories::ALLOWED_CHATEAUX.contains(&x.as_str())).collect::<Vec<_>>();

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

            let fut = async move {
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

                tokio::task::spawn_blocking(move || {
                    let chunks: Vec<_> = trajectories.chunks(100).collect();
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
                .unwrap_or_default()
            };

            let fut = actix::fut::wrap_future(fut).map(
                move |messages,
                      act: &mut TripWebSocket,
                      ctx: &mut ws::WebsocketContext<Self>| {
                    if current_gen != act.trajectory_request_generation {
                        return; // newer request was sent
                    }

                    for text in messages {
                        ctx.text(text);
                    }
                    act.last_trajectory_sent_time = Some(Instant::now());
                },
            );
            ctx.spawn(fut);
        }
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

                        let get_bounds = |bounds: &crate::map_coordinator::BoundsInputPerLevel,
                                          zoom: u8|
                         -> Option<(f64, f64, f64, f64)> {
                            let top_left =
                                slippy_map_tiles::Tile::new(zoom, bounds.min_x, bounds.min_y)?;
                            let bottom_right =
                                slippy_map_tiles::Tile::new(zoom, bounds.max_x, bounds.max_y)?;

                            Some((
                                top_left.left() as f64,
                                bottom_right.bottom() as f64,
                                bottom_right.right() as f64,
                                top_left.top() as f64,
                            ))
                        };

                        let bounds_bus = if params.categories.contains(&"bus".to_string()) {
                            get_bounds(&params.bounds_input.level12, 12)
                        } else {
                            None
                        };
                        let bounds_metro = if params.categories.contains(&"metro".to_string()) {
                            get_bounds(&params.bounds_input.level8, 8)
                        } else {
                            None
                        };
                        let bounds_rail = if params.categories.contains(&"rail".to_string()) {
                            get_bounds(&params.bounds_input.level7, 7)
                        } else {
                            None
                        };
                        let bounds_other = if params.categories.contains(&"other".to_string()) {
                            get_bounds(&params.bounds_input.level5, 5)
                        } else {
                            None
                        };

                        for bounds in [bounds_bus, bounds_metro, bounds_rail, bounds_other]
                            .into_iter()
                            .flatten()
                        {
                            let chateaus = self
                                .chateau_rtree
                                .locate_in_envelope(bounds.0, bounds.1, bounds.2, bounds.3);
                            new_chateaus.extend(chateaus);
                        }

                        self.map_update_generation = self.map_update_generation.wrapping_add(1);
                        self.update_map_subscriptions(ctx, new_chateaus);

                        for ch in &self.subscribed_chateaus {
                            let coordinator = self.coordinator_pool.for_chateau(ch);
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
