use crate::map_coordinator::{
    AspenisedVehiclePositionOutput, BoundsInputV3, BulkFetchCoordinator, BulkFetchParamsV3,
    BulkFetchResponseV2, CategoryOfRealtimeVehicleData, ChateauUpdate, EachCategoryPayloadV2,
    EachChateauResponseV2, PositionDataCategoryV2, Subscribe, Unsubscribe,
    category_to_allowed_route_ids, convert_to_output,
};
use actix::prelude::*;
use actix_web_actors::ws;
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::GetVehicleLocationsResponse;
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

use crate::{ClientMessage, MapViewportUpdate, ServerMessage, nearby_departures};
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
    pub coordinator: Addr<BulkFetchCoordinator>,
    pub client_viewport: Option<MapViewportUpdate>,
    pub subscribed_chateaus: HashSet<String>,
    // ChateauID -> (LastTime, LastBounds, LastCategories)
    pub sent_state: HashMap<String, (u64, BoundsInputV3, HashSet<String>)>,

    pub etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
}

impl TripWebSocket {
    pub fn new(
        pool: Arc<CatenaryPostgresPool>,
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
        coordinator: Addr<BulkFetchCoordinator>,
        etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    ) -> Self {
        Self {
            pool,
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            subscriptions: HashMap::new(),
            hb: Instant::now(),
            coordinator,
            client_viewport: None,
            subscribed_chateaus: HashSet::new(),
            sent_state: HashMap::new(),
            etcd_reuser,
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
            for ((chateau, params), _) in act.subscriptions.iter_mut() {
                let fs = fetch_trip_rt_update(
                    chateau.clone(),
                    params.clone(),
                    act.etcd_connection_ips.clone(),
                    act.etcd_connection_options.clone(),
                    act.aspen_client_manager.clone(),
                    act.etcd_reuser.clone(),
                );

                let params_clone = params.clone();
                let chateau_clone = chateau.clone();

                let fut = async move { fs.await };

                let fut = actix::fut::wrap_future(fut).map(
                    move |result, act: &mut TripWebSocket, ctx: &mut ws::WebsocketContext<Self>| {
                        match result {
                            Ok(response) => {
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
                                // Optionally send error
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
            self.coordinator.do_send(Unsubscribe {
                chateau_id: ch,
                recipient: ctx.address().recipient(),
            });
        }

        for ch in to_subscribe {
            self.coordinator.do_send(Subscribe {
                chateau_id: ch,
                recipient: ctx.address().recipient(),
            });
        }

        self.subscribed_chateaus = new_chateaus;
    }

    fn process_map_update(
        &mut self,
        chateau_id: &String,
        response: &Arc<GetVehicleLocationsResponse>,
        ctx: &mut ws::WebsocketContext<Self>,
    ) {
        if let Some(params) = &self.client_viewport {
            // Check if this chateau is still relevant? (It should be if we are subscribed)
            // But verify it's in the current params list to be safe
            if !params.chateaus.contains(chateau_id) {
                return;
            }

            let categories_requested = params
                .categories
                .iter()
                .map(|category| match category.as_str() {
                    "metro" => Some(CategoryOfRealtimeVehicleData::Metro),
                    "bus" => Some(CategoryOfRealtimeVehicleData::Bus),
                    "rail" => Some(CategoryOfRealtimeVehicleData::Rail),
                    "other" => Some(CategoryOfRealtimeVehicleData::Other),
                    _ => None,
                })
                .flatten()
                .collect::<Vec<CategoryOfRealtimeVehicleData>>();

            let mut each_chateau_response = EachChateauResponseV2 {
                categories: Some(PositionDataCategoryV2::default()),
            };

            let sent_state_entry = self.sent_state.get(chateau_id).cloned();
            let (last_sent_time, last_sent_bounds, last_sent_categories) = match &sent_state_entry {
                Some((t, b, c)) => (*t, Some(b), Some(c)),
                None => (0, None, None),
            };

            // If time mismatch, we have a new feed.
            // If time match, we might have a viewport change (Delta).
            let is_new_feed = response.last_updated_time_ms != last_sent_time;

            // Logic:
            // 1. If is_new_feed: Send ALL vehicles in current bounds. replaces_all = true.
            // 2. If !is_new_feed:
            //    Check delta between current params.bounds and last_sent_bounds.
            //    Send ONLY vehicles in (current - last). replaces_all = false.

            // Note: If !is_new_feed and last_sent_bounds == current_bounds, we send nothing.

            let mut has_any_updates = false;

            for category in &categories_requested {
                let category_str = match category {
                    CategoryOfRealtimeVehicleData::Metro => "metro",
                    CategoryOfRealtimeVehicleData::Bus => "bus",
                    CategoryOfRealtimeVehicleData::Rail => "rail",
                    CategoryOfRealtimeVehicleData::Other => "other",
                };

                // If this specific category was not sent last time, we must send full state for it
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

                // Determine if we need to send anything for this category
                let replace_all = is_new_feed || prev_bounds_for_level.is_none() || category_is_new;

                // If not replacing all, check if bounds changed
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

                let route_types_allowed = category_to_allowed_route_ids(category);

                let filtered_vehicle_positions = response
                    .vehicle_positions
                    .iter()
                    .filter(|vehicle_position| {
                        route_types_allowed.contains(&vehicle_position.1.route_type)
                    })
                    .map(|(a, b)| (a.clone(), convert_to_output(&b)))
                    .collect::<AHashMap<String, AspenisedVehiclePositionOutput>>();

                let mut vehicles_by_tile: BTreeMap<
                    u32,
                    BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>,
                > = BTreeMap::new();

                if replace_all {
                    // Send everything in current bounds
                    for (vehicle_id, vehicle_position) in filtered_vehicle_positions {
                        if let Some(pos) = &vehicle_position.position {
                            if pos.latitude != 0.0 && pos.longitude != 0.0 {
                                let (x, y) = slippy_map_tiles::lat_lon_to_tile(
                                    pos.latitude,
                                    pos.longitude,
                                    zoom,
                                );

                                let in_current_bounds = x >= bounds.min_x
                                    && x <= bounds.max_x
                                    && y >= bounds.min_y
                                    && y <= bounds.max_y;

                                if in_current_bounds {
                                    vehicles_by_tile
                                        .entry(x)
                                        .or_default()
                                        .entry(y)
                                        .or_default()
                                        .insert(vehicle_id, vehicle_position);
                                }
                            }
                        }
                    }
                } else {
                    // Delta send
                    if let Some(prev_bounds) = prev_bounds_for_level {
                        for (vehicle_id, vehicle_position) in filtered_vehicle_positions {
                            if let Some(pos) = &vehicle_position.position {
                                if pos.latitude != 0.0 && pos.longitude != 0.0 {
                                    let (x, y) = slippy_map_tiles::lat_lon_to_tile(
                                        pos.latitude,
                                        pos.longitude,
                                        zoom,
                                    );

                                    let in_current_bounds = x >= bounds.min_x
                                        && x <= bounds.max_x
                                        && y >= bounds.min_y
                                        && y <= bounds.max_y;

                                    // Check if it was in previous bounds
                                    let in_prev_bounds = x >= prev_bounds.min_x
                                        && x <= prev_bounds.max_x
                                        && y >= prev_bounds.min_y
                                        && y <= prev_bounds.max_y;

                                    if in_current_bounds && !in_prev_bounds {
                                        vehicles_by_tile
                                            .entry(x)
                                            .or_default()
                                            .entry(y)
                                            .or_default()
                                            .insert(vehicle_id, vehicle_position);
                                    }
                                }
                            }
                        }
                    }
                }

                let list_of_agency_ids = response.vehicle_route_cache.as_ref().map(|cache| {
                    cache
                        .values()
                        .filter_map(|route_cache| {
                            if route_types_allowed.contains(&route_cache.route_type) {
                                route_cache.agency_id.clone()
                            } else {
                                None
                            }
                        })
                        .collect::<BTreeSet<String>>()
                        .into_iter()
                        .collect::<Vec<String>>()
                });

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
            } // for category

            if has_any_updates {
                let mut bulk_fetch_response = BulkFetchResponseV2 {
                    chateaus: BTreeMap::new(),
                };
                bulk_fetch_response
                    .chateaus
                    .insert(chateau_id.clone(), each_chateau_response);

                let msg = ServerMessage::MapUpdate(bulk_fetch_response);
                if let Ok(text) = serde_json::to_string(&msg) {
                    ctx.text(text);
                }

                // Update state
                let categories_sent: HashSet<String> = params.categories.iter().cloned().collect();
                self.sent_state.insert(
                    chateau_id.clone(),
                    (
                        response.last_updated_time_ms,
                        params.bounds_input.clone(),
                        categories_sent,
                    ),
                );
            }
        }
    }
}

impl Actor for TripWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
        self.start_periodic_updates(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> actix::Running {
        for ch in &self.subscribed_chateaus {
            self.coordinator.do_send(Unsubscribe {
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
                        println!(
                            "DEBUG: TripWebSocket received UpdateMap (chateaus: {:?})",
                            params.chateaus
                        );
                        let new_chateaus: HashSet<String> =
                            params.chateaus.iter().cloned().collect();
                        self.update_map_subscriptions(ctx, new_chateaus);
                        self.client_viewport = Some(params);

                        // Request updates for all subscribed chateaus (cached ones will come back instantly)
                        for ch in &self.subscribed_chateaus {
                            self.coordinator.do_send(Subscribe {
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

impl Handler<ChateauUpdate> for TripWebSocket {
    type Result = ();

    fn handle(&mut self, msg: ChateauUpdate, ctx: &mut Self::Context) {
        self.process_map_update(&msg.chateau_id, &msg.response, ctx);
    }
}
