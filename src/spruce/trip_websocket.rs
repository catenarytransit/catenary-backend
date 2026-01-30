use crate::map_coordinator::{
    AspenisedVehiclePositionOutput, BulkFetchCoordinator, BulkFetchParamsV3, BulkFetchResponseV2,
    CategoryOfRealtimeVehicleData, ChateauUpdate, EachCategoryPayloadV2, EachChateauResponseV2,
    PositionDataCategoryV2, Subscribe, Unsubscribe, category_to_allowed_route_ids,
    convert_to_output,
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

    // New variant for map updates
    #[serde(rename = "update_map")]
    UpdateMap {
        #[serde(flatten)]
        params: BulkFetchParamsV3,
    },
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
    // Map response is usually just the BulkFetchResponseV2 JSON, but we can wrap it if consistent with ws
    // However, existing backend sends raw JSON for map? No, this is new WebSocket territory.
    // Let's stick to the convention if possible.
    // But the user requested "identical response" to the HTTP version.
    // The HTTP version returns BulkFetchResponseV2 directly.
    // If we wrap it in `type: update_map_response`, we change the shape.
    // BUT we are in a WebSocket multiplexing different things.
    // If I send bare BulkFetchResponseV2, the client has to detect it.
    // Let's assume the client can handle a new message type "map_update".
    #[serde(rename = "map_update")]
    MapUpdate(BulkFetchResponseV2), // Tuple variant to embed the response directly? Or named?
                                    // If I use untagged enum or custom serialization I could make it look exactly like HTTP body?
                                    // But then how to distinguish from other messages?
                                    // Let's explicitly use a type tag for consistency within THIS socket.
}

pub struct TripWebSocket {
    pub pool: Arc<CatenaryPostgresPool>,
    pub etcd_connection_ips: Arc<EtcdConnectionIps>,
    pub etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub aspen_client_manager: Arc<AspenClientManager>,
    pub subscriptions: HashMap<(String, QueryTripInformationParams), Option<u64>>,
    pub hb: Instant,

    // Map related fields
    pub coordinator: Addr<BulkFetchCoordinator>,
    pub map_params: Option<BulkFetchParamsV3>,
    pub subscribed_chateaus: HashSet<String>,
}

impl TripWebSocket {
    pub fn new(
        pool: Arc<CatenaryPostgresPool>,
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
        coordinator: Addr<BulkFetchCoordinator>,
    ) -> Self {
        Self {
            pool,
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            subscriptions: HashMap::new(),
            hb: Instant::now(),
            coordinator,
            map_params: None,
            subscribed_chateaus: HashSet::new(),
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
        &self,
        chateau_id: &String,
        response: &Arc<GetVehicleLocationsResponse>,
        ctx: &mut ws::WebsocketContext<Self>,
    ) {
        if let Some(params) = &self.map_params {
            if let Some(chateau_params) = params.chateaus.get(chateau_id) {
                // Reuse logic from bulk_realtime_fetch_v3 / map_websocket
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

                for category in &categories_requested {
                    let chateau_params_for_this_category = match category {
                        CategoryOfRealtimeVehicleData::Metro => {
                            &chateau_params.category_params.metro
                        }
                        CategoryOfRealtimeVehicleData::Bus => &chateau_params.category_params.bus,
                        CategoryOfRealtimeVehicleData::Rail => &chateau_params.category_params.rail,
                        CategoryOfRealtimeVehicleData::Other => {
                            &chateau_params.category_params.other
                        }
                    };

                    let mismatched_times = response.last_updated_time_ms
                        != chateau_params_for_this_category
                            .as_ref()
                            .map_or(0, |x| x.last_updated_time_ms);

                    let no_previous_tiles = match category {
                        CategoryOfRealtimeVehicleData::Metro => {
                            let bounds = &params.bounds_input.level8;
                            chateau_params_for_this_category.as_ref().map_or(true, |p| {
                                p.prev_user_min_x.is_none()
                                    || p.prev_user_max_x.is_none()
                                    || p.prev_user_min_y.is_none()
                                    || p.prev_user_max_y.is_none()
                            })
                        }
                        CategoryOfRealtimeVehicleData::Rail => {
                            let bounds = &params.bounds_input.level7;
                            chateau_params_for_this_category.as_ref().map_or(true, |p| {
                                p.prev_user_min_x.is_none()
                                    || p.prev_user_max_x.is_none()
                                    || p.prev_user_min_y.is_none()
                                    || p.prev_user_max_y.is_none()
                            })
                        }
                        CategoryOfRealtimeVehicleData::Bus => {
                            let bounds = &params.bounds_input.level12;
                            chateau_params_for_this_category.as_ref().map_or(true, |p| {
                                p.prev_user_min_x.is_none()
                                    || p.prev_user_max_x.is_none()
                                    || p.prev_user_min_y.is_none()
                                    || p.prev_user_max_y.is_none()
                            })
                        }
                        CategoryOfRealtimeVehicleData::Other => {
                            let bounds = &params.bounds_input.level5;
                            chateau_params_for_this_category.as_ref().map_or(true, |p| {
                                p.prev_user_min_x.is_none()
                                    || p.prev_user_max_x.is_none()
                                    || p.prev_user_min_y.is_none()
                                    || p.prev_user_max_y.is_none()
                            })
                        }
                    };

                    let replace_all_tiles = mismatched_times || no_previous_tiles;
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

                    let zoom = match category {
                        CategoryOfRealtimeVehicleData::Metro => 8,
                        CategoryOfRealtimeVehicleData::Rail => 7,
                        CategoryOfRealtimeVehicleData::Bus => 12,
                        CategoryOfRealtimeVehicleData::Other => 5,
                    };

                    match replace_all_tiles {
                        true => {
                            for (vehicle_id, vehicle_position) in filtered_vehicle_positions {
                                if let Some(pos) = &vehicle_position.position {
                                    let bounds = match category {
                                        CategoryOfRealtimeVehicleData::Metro => {
                                            &params.bounds_input.level8
                                        }
                                        CategoryOfRealtimeVehicleData::Rail => {
                                            &params.bounds_input.level7
                                        }
                                        CategoryOfRealtimeVehicleData::Bus => {
                                            &params.bounds_input.level12
                                        }
                                        CategoryOfRealtimeVehicleData::Other => {
                                            &params.bounds_input.level5
                                        }
                                    };

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
                        }
                        false => {
                            let (bounds, prev_bounds_params) = match category {
                                CategoryOfRealtimeVehicleData::Metro => (
                                    &params.bounds_input.level8,
                                    chateau_params_for_this_category.as_ref(),
                                ),
                                CategoryOfRealtimeVehicleData::Rail => (
                                    &params.bounds_input.level7,
                                    chateau_params_for_this_category.as_ref(),
                                ),
                                CategoryOfRealtimeVehicleData::Bus => (
                                    &params.bounds_input.level12,
                                    chateau_params_for_this_category.as_ref(),
                                ),
                                CategoryOfRealtimeVehicleData::Other => (
                                    &params.bounds_input.level5,
                                    chateau_params_for_this_category.as_ref(),
                                ),
                            };

                            if let Some(prev_bounds) = prev_bounds_params {
                                if let (
                                    Some(prev_min_x),
                                    Some(prev_max_x),
                                    Some(prev_min_y),
                                    Some(prev_max_y),
                                ) = (
                                    prev_bounds.prev_user_min_x,
                                    prev_bounds.prev_user_max_x,
                                    prev_bounds.prev_user_min_y,
                                    prev_bounds.prev_user_max_y,
                                ) {
                                    for (vehicle_id, vehicle_position) in filtered_vehicle_positions
                                    {
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
                                                let in_prev_bounds = x >= prev_min_x
                                                    && x <= prev_max_x
                                                    && y >= prev_min_y
                                                    && y <= prev_max_y;

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
                        replaces_all: replace_all_tiles,
                        z_level: zoom,
                        list_of_agency_ids,
                    };

                    match category {
                        CategoryOfRealtimeVehicleData::Metro => {
                            each_chateau_response.categories.as_mut().unwrap().metro =
                                Some(payload);
                        }
                        CategoryOfRealtimeVehicleData::Rail => {
                            each_chateau_response.categories.as_mut().unwrap().rail = Some(payload);
                        }
                        CategoryOfRealtimeVehicleData::Other => {
                            each_chateau_response.categories.as_mut().unwrap().other =
                                Some(payload);
                        }
                        CategoryOfRealtimeVehicleData::Bus => {
                            each_chateau_response.categories.as_mut().unwrap().bus = Some(payload);
                        }
                    }
                } // for category

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
                        let new_chateaus: HashSet<String> =
                            params.chateaus.keys().cloned().collect();
                        self.update_map_subscriptions(ctx, new_chateaus);
                        self.map_params = Some(params);

                        // Request updates for all subscribed chateaus (cached ones will come back instantly)
                        for ch in &self.subscribed_chateaus {
                            self.coordinator.do_send(Subscribe {
                                chateau_id: ch.clone(),
                                recipient: ctx.address().recipient(),
                            });
                        }
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
