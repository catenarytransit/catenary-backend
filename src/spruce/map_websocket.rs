
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use catenary::aspen::lib::AspenisedVehicleDescriptor;
use catenary::models::CatenaryRtVehiclePosition;
use catenary::aspen_dataset::{AspenisedVehiclePosition, AspenisedVehicleRouteCache}; 
use actix::prelude::*;
use actix_web_actors::ws;
use std::sync::Arc;
use std::time::{Duration, Instant};
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::{GetVehicleLocationsResponse, ChateauMetadataEtcd};
use tarpc::context;
use catenary::bincode_deserialize;
use futures::StreamExt;

#[derive(Eq, PartialEq, Hash, Debug, Serialize, Deserialize, Clone, Copy)]
pub enum CategoryOfRealtimeVehicleData {
    Metro,
    Bus,
    Rail,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash)]
pub struct AspenisedVehicleTripInfoOutput {
    pub trip_id: Option<String>,
    pub trip_headsign: Option<String>,
    pub route_id: Option<String>,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub schedule_relationship: Option<u8>,
    pub delay: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedVehiclePositionOutput {
    pub trip: Option<AspenisedVehicleTripInfoOutput>,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub position: Option<CatenaryRtVehiclePosition>,
    pub timestamp: Option<u64>,
    pub route_type: i16,
    pub current_stop_sequence: Option<u32>,
    pub current_status: Option<i32>,
    pub congestion_level: Option<i32>,
    pub occupancy_status: Option<i32>,
    pub occupancy_percentage: Option<u32>,
}

pub fn convert_to_output(input: &AspenisedVehiclePosition) -> AspenisedVehiclePositionOutput {
    let trip_new = match &input.trip {
        Some(trip) => Some(AspenisedVehicleTripInfoOutput {
            trip_id: trip.trip_id.clone(),
            trip_headsign: trip.trip_headsign.clone(),
            route_id: trip.route_id.clone(),
            trip_short_name: trip.trip_short_name.clone(),
            direction_id: trip.direction_id,
            start_time: trip.start_time.clone(),
            start_date: trip.start_date.map(|x| x.format("%Y%m%d").to_string()),
            schedule_relationship: trip.schedule_relationship.as_ref().map(|x| x.into()),
            delay: trip.delay,
        }),
        None => None,
    };

    AspenisedVehiclePositionOutput {
        trip: trip_new,
        vehicle: input.vehicle.clone(),
        position: input.position.clone(),
        timestamp: input.timestamp,
        route_type: input.route_type,
        current_stop_sequence: input.current_stop_sequence,
        current_status: input.current_status,
        congestion_level: input.congestion_level,
        occupancy_status: input.occupancy_status,
        occupancy_percentage: input.occupancy_percentage,
    }
}

pub fn category_to_allowed_route_ids(category: &CategoryOfRealtimeVehicleData) -> Vec<i16> {
    match category {
        CategoryOfRealtimeVehicleData::Metro => vec![0, 1, 5, 7, 12],
        CategoryOfRealtimeVehicleData::Bus => vec![3, 11],
        CategoryOfRealtimeVehicleData::Rail => vec![2],
        CategoryOfRealtimeVehicleData::Other => vec![4, 6],
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ChateauAskParamsV2 {
    pub category_params: CategoryAskParamsV2,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CategoryAskParamsV2 {
    pub bus: Option<SubCategoryAskParamsV2>,
    pub metro: Option<SubCategoryAskParamsV2>,
    pub rail: Option<SubCategoryAskParamsV2>,
    pub other: Option<SubCategoryAskParamsV2>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SubCategoryAskParamsV2 {
    pub last_updated_time_ms: u64,
    pub prev_user_min_x: Option<u32>,
    pub prev_user_max_x: Option<u32>,
    pub prev_user_min_y: Option<u32>,
    pub prev_user_max_y: Option<u32>,
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BoundsInputV3 {
    pub level5: BoundsInputPerLevel,
    pub level7: BoundsInputPerLevel,
    pub level8: BoundsInputPerLevel,
    pub level12: BoundsInputPerLevel,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BoundsInputPerLevel {
    pub min_x: u32,
    pub max_x: u32,
    pub min_y: u32,
    pub max_y: u32,
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BulkFetchParamsV3 {
    pub chateaus: BTreeMap<String, ChateauAskParamsV2>,
    pub categories: Vec<String>,
    pub bounds_input: BoundsInputV3,
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchResponseV2 {
    pub chateaus: BTreeMap<String, EachChateauResponseV2>,
}

#[derive(Serialize, Deserialize)]
pub struct EachChateauResponseV2 {
    pub categories: Option<PositionDataCategoryV2>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct PositionDataCategoryV2 {
    pub metro: Option<EachCategoryPayloadV2>,
    pub bus: Option<EachCategoryPayloadV2>,
    pub rail: Option<EachCategoryPayloadV2>,
    pub other: Option<EachCategoryPayloadV2>,
}

#[derive(Serialize, Deserialize)]
pub struct EachCategoryPayloadV2 {
     pub vehicle_positions:
        Option<BTreeMap<u32, BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>>>,
    pub last_updated_time_ms: u64,
    pub replaces_all: bool,
    pub z_level: u8,
    pub list_of_agency_ids: Option<Vec<String>>,
}

// Messages

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct ChateauUpdate {
    pub chateau_id: String,
    pub response: Arc<GetVehicleLocationsResponse>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Subscribe {
    pub chateau_id: String,
    pub recipient: Recipient<ChateauUpdate>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Unsubscribe {
    pub chateau_id: String,
    pub recipient: Recipient<ChateauUpdate>,
}

// Coordinator Actor

pub struct BulkFetchCoordinator {
    subscribers: HashMap<String, HashSet<Recipient<ChateauUpdate>>>,
    // chateau_id -> (last_response, last_success_time)
    cache: HashMap<String, (Arc<GetVehicleLocationsResponse>, Instant)>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
}

impl BulkFetchCoordinator {
    pub fn new(
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
        etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    ) -> Self {
        Self {
            subscribers: HashMap::new(),
            cache: HashMap::new(),
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            etcd_reuser,
        }
    }

    fn fetch_cycle(&mut self, ctx: &mut Context<Self>) {
        if self.subscribers.is_empty() {
             return;
        }
        println!("DEBUG: Coordinator fetch cycle starting for {} chateaus", self.subscribers.len());

        let chateaus_to_fetch: Vec<String> = self.subscribers.keys().cloned().collect();
        let etcd_reuser = self.etcd_reuser.clone();
        let etcd_ips = self.etcd_connection_ips.clone();
        let etcd_opts = self.etcd_connection_options.clone();
        let aspen_manager = self.aspen_client_manager.clone();
        
        let fut = async move {
            // Etcd Client Logic (reused from prompt)
            let mut etcd = None;
            {
                let etcd_reuser_contents = etcd_reuser.read().await;
                let mut client_is_healthy = false;
                if let Some(client) = etcd_reuser_contents.as_ref() {
                    let mut client = client.clone();
                    if client.status().await.is_ok() {
                        etcd = Some(client.clone());
                        client_is_healthy = true;
                    }
                }

                if !client_is_healthy {
                    drop(etcd_reuser_contents);
                    if let Ok(new_client) = etcd_client::Client::connect(
                        etcd_ips.ip_addresses.as_slice(),
                        etcd_opts.as_ref().as_ref().to_owned(),
                    )
                    .await {
                        etcd = Some(new_client.clone());
                        let mut etcd_reuser_write_lock = etcd_reuser.write().await;
                        *etcd_reuser_write_lock = Some(new_client);
                    }
                }
            }

            if etcd.is_none() {
                println!("DEBUG: Etcd connection failed");
                return Vec::new(); // Failed to connect to etcd
            }
            let mut etcd = etcd.unwrap();

            let mut results = Vec::new();

            for chateau_id in chateaus_to_fetch {
                let fetch_assigned_node = etcd
                    .get(
                        format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
                        None,
                    )
                    .await;

                if let Ok(resp) = fetch_assigned_node {
                    if !resp.kvs().is_empty() {
                        if let Ok(assigned_chateau_data) =
                            bincode_deserialize::<ChateauMetadataEtcd>(
                                resp.kvs().first().unwrap().value(),
                            )
                        {
                            let mut aspen_client = aspen_manager
                                .get_client(assigned_chateau_data.socket)
                                .await;

                            // If client is missing, try to connect immediately
                            if aspen_client.is_none() {
                                println!("DEBUG: Aspen client missing for {}, connecting...", chateau_id);
                                if let Ok(new_client) =
                                    catenary::aspen::lib::spawn_aspen_client_from_ip(
                                        &assigned_chateau_data.socket,
                                    )
                                    .await
                                {
                                    aspen_manager
                                        .insert_client(
                                            assigned_chateau_data.socket,
                                            new_client.clone(),
                                        )
                                        .await;
                                    aspen_client = Some(new_client);
                                } else {
                                    println!("DEBUG: Failed to spawn aspen client for {}", chateau_id);
                                }
                            }

                            if let Some(client) = aspen_client {
                                let route_types: Vec<i16> =
                                    vec![0, 1, 2, 3, 4, 5, 6, 7, 11, 12]; // superset of all categories

                                let response = client
                                    .get_vehicle_locations(
                                        context::current(),
                                        chateau_id.clone(),
                                        None,
                                        Some(route_types.clone()),
                                    )
                                    .await;

                                match response {
                                    Ok(Some(data)) => {
                                        println!("DEBUG: Got data for {}", chateau_id);
                                        results.push((chateau_id, data));
                                    }
                                    Err(e) => {
                                        println!("DEBUG: RPC failed for {}: {:?}", chateau_id, e);
                                        if let Ok(new_client) =
                                            catenary::aspen::lib::spawn_aspen_client_from_ip(
                                                &assigned_chateau_data.socket,
                                            )
                                            .await
                                        {
                                            aspen_manager
                                                .insert_client(
                                                    assigned_chateau_data.socket,
                                                    new_client.clone(),
                                                )
                                                .await;
                                            
                                            // Retry
                                            let response_retry = new_client
                                                .get_vehicle_locations(
                                                    context::current(),
                                                    chateau_id.clone(),
                                                    None,
                                                    Some(route_types),
                                                )
                                                .await;
                                            
                                            if let Ok(Some(data)) = response_retry {
                                                println!("DEBUG: Retry success for {}", chateau_id);
                                                results.push((chateau_id, data));
                                            } else {
                                                 println!("DEBUG: Retry failed for {}", chateau_id);
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                         println!("DEBUG: Response Ok(None) for {}", chateau_id);
                                    }
                                }
                            }
                        }
                    } else {
                        println!("DEBUG: No assigned node for {}", chateau_id);
                    }
                } else {
                    println!("DEBUG: Etcd fetch failed for {}", chateau_id);
                }
            }
            results
        };

        ctx.spawn(actix::fut::wrap_future(fut).map(|results, actor: &mut BulkFetchCoordinator, _| {
             println!("DEBUG: Coordinator cycle finished, got {} results", results.len());
             for (chateau_id, data) in results {
                 let data_arc = Arc::new(data);
                 
                 let is_new = if let Some((old_data, _)) = actor.cache.get(&chateau_id) {
                     old_data.last_updated_time_ms != data_arc.last_updated_time_ms
                 } else {
                     true
                 };

                 if is_new {
                     println!("DEBUG: New data for {}, broadcasting", chateau_id);
                     actor.cache.insert(chateau_id.clone(), (data_arc.clone(), Instant::now()));
                     if let Some(subs) = actor.subscribers.get(&chateau_id) {
                         for recipient in subs {
                             recipient.do_send(ChateauUpdate {
                                 chateau_id: chateau_id.clone(),
                                 response: data_arc.clone(),
                             });
                         }
                     }
                 }
             }
        }));
    }
}

impl Actor for BulkFetchCoordinator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_millis(300), |act, ctx| {
            act.fetch_cycle(ctx);
        });
    }
}

impl Handler<Subscribe> for BulkFetchCoordinator {
    type Result = ();

    fn handle(&mut self, msg: Subscribe, _: &mut Self::Context) {
        let subs = self.subscribers.entry(msg.chateau_id.clone()).or_default();
        subs.insert(msg.recipient.clone());

        // Send cached data immediately if available
        if let Some((data, _)) = self.cache.get(&msg.chateau_id) {
            msg.recipient.do_send(ChateauUpdate {
                chateau_id: msg.chateau_id,
                response: data.clone(),
            });
        }
    }
}

impl Handler<Unsubscribe> for BulkFetchCoordinator {
    type Result = ();

    fn handle(&mut self, msg: Unsubscribe, _: &mut Self::Context) {
        if let Some(subs) = self.subscribers.get_mut(&msg.chateau_id) {
            subs.remove(&msg.recipient);
            if subs.is_empty() {
                self.subscribers.remove(&msg.chateau_id);
            }
        }
    }
}

// Stats / Heartbeat for WebSocket

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct MapWebSocket {
    hb: Instant,
    coordinator: Addr<BulkFetchCoordinator>,
    params: Option<BulkFetchParamsV3>,
    subscribed_chateaus: HashSet<String>,
}

impl MapWebSocket {
    pub fn new(coordinator: Addr<BulkFetchCoordinator>) -> Self {
        Self {
            hb: Instant::now(),
            coordinator,
            params: None,
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

    fn update_subscriptions(&mut self, ctx: &mut ws::WebsocketContext<Self>, new_chateaus: HashSet<String>) {
        let to_unsubscribe: Vec<String> = self.subscribed_chateaus.difference(&new_chateaus).cloned().collect();
        let to_subscribe: Vec<String> = new_chateaus.difference(&self.subscribed_chateaus).cloned().collect();

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
    
    fn process_update(&self, chateau_id: &String, response: &Arc<GetVehicleLocationsResponse>, ctx: &mut ws::WebsocketContext<Self>) {
        if let Some(params) = &self.params {
            if let Some(chateau_params) = params.chateaus.get(chateau_id) {
                // Perform filtering and response generation
                // This logic is copied and adapted from bulk_realtime_fetch_v3
                
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
                        CategoryOfRealtimeVehicleData::Metro => &chateau_params.category_params.metro,
                        CategoryOfRealtimeVehicleData::Bus => &chateau_params.category_params.bus,
                        CategoryOfRealtimeVehicleData::Rail => &chateau_params.category_params.rail,
                        CategoryOfRealtimeVehicleData::Other => &chateau_params.category_params.other,
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
                    } // match replace_all_tiles
                    
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

                let mut bulk_fetch_response = BulkFetchResponseV2 {
                    chateaus: BTreeMap::new(),
                };
                bulk_fetch_response.chateaus.insert(chateau_id.clone(), each_chateau_response);

                if let Ok(text) = serde_json::to_string(&bulk_fetch_response) {
                    ctx.text(text);
                }
            }
        }
    }
}

impl Actor for MapWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
    }
    
    fn stopping(&mut self, ctx: &mut Self::Context) {
        for ch in &self.subscribed_chateaus {
            self.coordinator.do_send(Unsubscribe {
                chateau_id: ch.clone(),
                recipient: ctx.address().recipient(),
            });
        }
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MapWebSocket {
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
                // Parse params
                // Expected format: JSON of BulkFetchParamsV3
                match serde_json::from_str::<BulkFetchParamsV3>(&text) {
                    Ok(params) => {
                         let new_chateaus: HashSet<String> = params.chateaus.keys().cloned().collect();
                         
                         let chateaus_changed = new_chateaus != self.subscribed_chateaus;
                         let params_changed = self.params.as_ref() != Some(&params);

                         if chateaus_changed || params_changed {
                             println!("DEBUG: Received new params (chateaus: {:?})", new_chateaus);
                             self.update_subscriptions(ctx, new_chateaus);
                             self.params = Some(params);
                             
                             for ch in &self.subscribed_chateaus {
                                 self.coordinator.do_send(Subscribe {
                                    chateau_id: ch.clone(),
                                    recipient: ctx.address().recipient(),
                                 });
                             }
                         } else {
                             // println!("DEBUG: Params identical, skipping update.");
                         }
                    }
                    Err(e) => {
                        println!("DEBUG: Failed to parse params: {:?}", e);
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

impl Handler<ChateauUpdate> for MapWebSocket {
    type Result = ();

    fn handle(&mut self, msg: ChateauUpdate, ctx: &mut Self::Context) {
        println!("DEBUG: MapWebSocket received ChateauUpdate for {}", msg.chateau_id);
        self.process_update(&msg.chateau_id, &msg.response, ctx);
    }
}
