use actix::prelude::*;
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::aspen::lib::{ChateauMetadataEtcd, GetVehicleLocationsResponse};
use catenary::aspen_dataset::AspenisedVehicleDescriptor;
use catenary::aspen_dataset::CatenaryRtVehiclePosition;
use catenary::aspen_dataset::{AspenisedVehiclePosition, AspenisedVehicleRouteCache};
use catenary::bincode_deserialize;
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use slippy_map_tiles;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tarpc::context;

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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChateauAskParamsV2 {
    pub category_params: CategoryAskParamsV2,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CategoryAskParamsV2 {
    pub bus: Option<SubCategoryAskParamsV2>,
    pub metro: Option<SubCategoryAskParamsV2>,
    pub rail: Option<SubCategoryAskParamsV2>,
    pub other: Option<SubCategoryAskParamsV2>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubCategoryAskParamsV2 {
    pub last_updated_time_ms: u64,
    pub prev_user_min_x: Option<u32>,
    pub prev_user_max_x: Option<u32>,
    pub prev_user_min_y: Option<u32>,
    pub prev_user_max_y: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BoundsInputV3 {
    pub level5: BoundsInputPerLevel,
    pub level7: BoundsInputPerLevel,
    pub level8: BoundsInputPerLevel,
    pub level12: BoundsInputPerLevel,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BoundsInputPerLevel {
    pub min_x: u32,
    pub max_x: u32,
    pub min_y: u32,
    pub max_y: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

#[derive(Clone, Serialize, Deserialize)]
pub struct PrecomputedCategoryTiles {
    pub tiles: BTreeMap<u32, BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>>,
    pub agency_ids: Option<Vec<String>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrecomputedChateauMap {
    pub last_updated_time_ms: u64,
    pub metro: PrecomputedCategoryTiles,
    pub bus: PrecomputedCategoryTiles,
    pub rail: PrecomputedCategoryTiles,
    pub other: PrecomputedCategoryTiles,
}

pub fn precompute_chateau_map(response: &GetVehicleLocationsResponse) -> PrecomputedChateauMap {
    let mut metro_tiles = BTreeMap::new();
    let mut bus_tiles = BTreeMap::new();
    let mut rail_tiles = BTreeMap::new();
    let mut other_tiles = BTreeMap::new();

    let route_types_metro = category_to_allowed_route_ids(&CategoryOfRealtimeVehicleData::Metro);
    let route_types_bus = category_to_allowed_route_ids(&CategoryOfRealtimeVehicleData::Bus);
    let route_types_rail = category_to_allowed_route_ids(&CategoryOfRealtimeVehicleData::Rail);
    let route_types_other = category_to_allowed_route_ids(&CategoryOfRealtimeVehicleData::Other);

    for (vehicle_id, vehicle_position) in &response.vehicle_positions {
        let route_type = vehicle_position.route_type;
        let category = if route_types_metro.contains(&route_type) {
            Some(CategoryOfRealtimeVehicleData::Metro)
        } else if route_types_bus.contains(&route_type) {
            Some(CategoryOfRealtimeVehicleData::Bus)
        } else if route_types_rail.contains(&route_type) {
            Some(CategoryOfRealtimeVehicleData::Rail)
        } else if route_types_other.contains(&route_type) {
            Some(CategoryOfRealtimeVehicleData::Other)
        } else {
            None
        };

        if let Some(cat) = category {
            let zoom = match cat {
                CategoryOfRealtimeVehicleData::Metro => 8,
                CategoryOfRealtimeVehicleData::Rail => 7,
                CategoryOfRealtimeVehicleData::Bus => 12,
                CategoryOfRealtimeVehicleData::Other => 5,
            };

            if let Some(pos) = &vehicle_position.position {
                if pos.latitude != 0.0 && pos.longitude != 0.0 {
                    let (x, y) =
                        slippy_map_tiles::lat_lon_to_tile(pos.latitude, pos.longitude, zoom);
                    let output = convert_to_output(vehicle_position);

                    match cat {
                        CategoryOfRealtimeVehicleData::Metro => {
                            metro_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                        CategoryOfRealtimeVehicleData::Bus => {
                            bus_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                        CategoryOfRealtimeVehicleData::Rail => {
                            rail_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                        CategoryOfRealtimeVehicleData::Other => {
                            other_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                    };
                }
            }
        }
    }

    let get_agencies = |route_types: &[i16]| -> Option<Vec<String>> {
        response.vehicle_route_cache.as_ref().map(|cache| {
            cache
                .values()
                .filter_map(|route_cache| {
                    if route_types.contains(&route_cache.route_type) {
                        route_cache.agency_id.clone()
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<String>>()
                .into_iter()
                .collect()
        })
    };

    PrecomputedChateauMap {
        last_updated_time_ms: response.last_updated_time_ms,
        metro: PrecomputedCategoryTiles {
            tiles: metro_tiles,
            agency_ids: get_agencies(&route_types_metro),
        },
        bus: PrecomputedCategoryTiles {
            tiles: bus_tiles,
            agency_ids: get_agencies(&route_types_bus),
        },
        rail: PrecomputedCategoryTiles {
            tiles: rail_tiles,
            agency_ids: get_agencies(&route_types_rail),
        },
        other: PrecomputedCategoryTiles {
            tiles: other_tiles,
            agency_ids: get_agencies(&route_types_other),
        },
    }
}

// Messages

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct ChateauUpdate {
    pub chateau_id: String,
    pub response: Arc<PrecomputedChateauMap>,
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

#[derive(Message)]
#[rtype(result = "()")]
pub struct FetchResultMsg(pub String, pub Option<Arc<PrecomputedChateauMap>>);

#[derive(Message)]
#[rtype(result = "()")]
pub struct FetchChateauNow(pub String);

// Coordinator Actor
pub struct BulkFetchCoordinator {
    subscribers: HashMap<String, HashSet<Recipient<ChateauUpdate>>>,
    // chateau_id -> (last_response, last_success_time)
    cache: HashMap<String, (Arc<PrecomputedChateauMap>, Instant)>,
    in_progress_fetches: HashMap<String, Instant>,
    active_chateaus: Arc<RwLock<HashSet<String>>>,
    aspen_endpoint_cache: Arc<RwLock<HashMap<String, (std::net::SocketAddr, Instant)>>>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
}

#[derive(Clone)]
pub struct BulkFetchCoordinatorPool {
    shards: Arc<Vec<Addr<BulkFetchCoordinator>>>,
}

impl BulkFetchCoordinatorPool {
    pub fn new(shards: Vec<Addr<BulkFetchCoordinator>>) -> Self {
        assert!(
            !shards.is_empty(),
            "BulkFetchCoordinatorPool must have at least one shard"
        );

        Self {
            shards: Arc::new(shards),
        }
    }

    pub fn for_chateau(&self, chateau_id: &str) -> Addr<BulkFetchCoordinator> {
        let mut hasher = DefaultHasher::new();
        chateau_id.hash(&mut hasher);
        let shard_index = (hasher.finish() as usize) % self.shards.len();

        self.shards[shard_index].clone()
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }
}

fn is_chateau_active(active_chateaus: &Arc<RwLock<HashSet<String>>>, chateau_id: &str) -> bool {
    active_chateaus
        .read()
        .map(|active| active.contains(chateau_id))
        .unwrap_or(false)
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
            in_progress_fetches: HashMap::new(),
            active_chateaus: Arc::new(RwLock::new(HashSet::new())),
            aspen_endpoint_cache: Arc::new(RwLock::new(HashMap::new())),
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

        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut chateaus_to_fetch = Vec::new();

        for chateau_id in self.subscribers.keys() {
            if let Some(started) = self.in_progress_fetches.get(chateau_id) {
                if started.elapsed() < timeout {
                    continue;
                }
            }

            self.in_progress_fetches.insert(chateau_id.clone(), now);
            let cached_last_updated_time_ms = self
                .cache
                .get(chateau_id)
                .map(|(data, _)| data.last_updated_time_ms);

            chateaus_to_fetch.push((chateau_id.clone(), cached_last_updated_time_ms));
        }

        if chateaus_to_fetch.is_empty() {
            return;
        }

        let active_chateaus = self.active_chateaus.clone();
        let aspen_endpoint_cache = self.aspen_endpoint_cache.clone();
        let etcd_reuser = self.etcd_reuser.clone();
        let etcd_ips = self.etcd_connection_ips.clone();
        let etcd_opts = self.etcd_connection_options.clone();
        let aspen_manager = self.aspen_client_manager.clone();

        let fut = async move {
            let etcd_result = catenary::get_etcd_client(&etcd_ips, &etcd_opts, &etcd_reuser).await;

            let etcd = etcd_result.ok();
            if etcd.is_none() {
                return futures::stream::iter(
                    chateaus_to_fetch
                        .into_iter()
                        .map(|(id, _)| FetchResultMsg(id, None)),
                )
                .left_stream();
            }
            let etcd = etcd.unwrap();

            let tasks = chateaus_to_fetch.into_iter().map(
                move |(chateau_id, cached_last_updated_time_ms)| {
                    let mut etcd = etcd.clone();
                    let aspen_manager = aspen_manager.clone();
                    let etcd_reuser = etcd_reuser.clone();
                    let active_chateaus = active_chateaus.clone();
                    let aspen_endpoint_cache = aspen_endpoint_cache.clone();

                    async move {
                        if !is_chateau_active(&active_chateaus, &chateau_id) {
                            return FetchResultMsg(chateau_id, None);
                        }

                        let mut cached_socket: Option<std::net::SocketAddr> = None;
                        if let Ok(cache_read) = aspen_endpoint_cache.read() {
                            if let Some((socket, time)) = cache_read.get(&chateau_id) {
                                if time.elapsed() < Duration::from_secs(300) {
                                    cached_socket = Some(socket.clone());
                                }
                            }
                        }

                        let socket_to_use = if let Some(socket) = cached_socket.clone() {
                            socket
                        } else {
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
                                        if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                                            cache_write.insert(
                                                chateau_id.clone(),
                                                (
                                                    assigned_chateau_data.socket.clone(),
                                                    Instant::now(),
                                                ),
                                            );
                                        }
                                        assigned_chateau_data.socket
                                    } else {
                                        return FetchResultMsg(chateau_id, None);
                                    }
                                } else {
                                    println!("DEBUG: No assigned node for {}", chateau_id);
                                    return FetchResultMsg(chateau_id, None);
                                }
                            } else {
                                println!("DEBUG: Etcd fetch failed for {}", chateau_id);
                                catenary::invalidate_etcd_client(&etcd_reuser).await;
                                println!(
                                    "DEBUG: Flushed Etcd reuser due to failure for {}",
                                    chateau_id
                                );
                                return FetchResultMsg(chateau_id, None);
                            }
                        };

                        let mut aspen_client =
                            aspen_manager.get_client(socket_to_use.clone()).await;

                        if aspen_client.is_none() {
                            println!(
                                "DEBUG: Aspen client missing for {}, connecting...",
                                chateau_id
                            );

                            if let Ok(new_client) =
                                catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use)
                                    .await
                            {
                                aspen_manager
                                    .insert_client(socket_to_use.clone(), new_client.clone())
                                    .await;

                                aspen_client = Some(new_client);
                            } else {
                                println!("DEBUG: Failed to spawn aspen client for {}", chateau_id);
                                if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                                    cache_write.remove(&chateau_id);
                                }
                            }
                        }

                        if let Some(mut client) = aspen_client {
                            let timeout_duration = Duration::from_secs(2);

                            if !is_chateau_active(&active_chateaus, &chateau_id) {
                                return FetchResultMsg(chateau_id, None);
                            }

                            let mut skipped_preflight = false;
                            let mut remote_last_updated_time_ms = None;
                            if cached_last_updated_time_ms.is_none() {
                                skipped_preflight = true;
                            } else {
                                let last_updated_response = tokio::time::timeout(
                                    timeout_duration,
                                    client.last_updated_time_ms_for_chateau(
                                        context::current(),
                                        chateau_id.clone(),
                                    ),
                                )
                                .await;

                                let remote_val = match last_updated_response {
                                    Ok(Ok(value)) => value,
                                    Ok(Err(e)) => {
                                        if let Ok(new_client) =
                                            catenary::aspen::lib::spawn_aspen_client_from_ip(
                                                &socket_to_use,
                                            )
                                            .await
                                        {
                                            aspen_manager
                                                .insert_client(
                                                    socket_to_use.clone(),
                                                    new_client.clone(),
                                                )
                                                .await;
                                            client = new_client;
                                            match tokio::time::timeout(
                                                timeout_duration,
                                                client.last_updated_time_ms_for_chateau(
                                                    tarpc::context::current(),
                                                    chateau_id.clone(),
                                                ),
                                            )
                                            .await
                                            {
                                                Ok(Ok(value)) => value,
                                                _ => {
                                                    if let Ok(mut cache_write) =
                                                        aspen_endpoint_cache.write()
                                                    {
                                                        cache_write.remove(&chateau_id);
                                                    }
                                                    return FetchResultMsg(chateau_id, None);
                                                }
                                            }
                                        } else {
                                            if let Ok(mut cache_write) =
                                                aspen_endpoint_cache.write()
                                            {
                                                cache_write.remove(&chateau_id);
                                            }
                                            return FetchResultMsg(chateau_id, None);
                                        }
                                    }
                                    Err(_) => {
                                        if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                                            cache_write.remove(&chateau_id);
                                        }
                                        return FetchResultMsg(chateau_id, None);
                                    }
                                };
                                remote_last_updated_time_ms = remote_val;
                            }

                            if !skipped_preflight {
                                let Some(remote_last_updated_time_ms) = remote_last_updated_time_ms
                                else {
                                    return FetchResultMsg(chateau_id, None);
                                };
                                if cached_last_updated_time_ms == Some(remote_last_updated_time_ms)
                                {
                                    return FetchResultMsg(chateau_id, None);
                                }
                            }

                            if !is_chateau_active(&active_chateaus, &chateau_id) {
                                return FetchResultMsg(chateau_id, None);
                            }

                            let route_types: Vec<i16> = vec![0, 1, 2, 3, 4, 5, 6, 7, 11, 12];

                            let response = tokio::time::timeout(
                                timeout_duration,
                                client.get_vehicle_locations(
                                    context::current(),
                                    chateau_id.clone(),
                                    None,
                                    Some(route_types.clone()),
                                ),
                            )
                            .await;

                            match response {
                                Ok(Ok(Some(data))) => {
                                    return FetchResultMsg(
                                        chateau_id,
                                        Some(Arc::new(precompute_chateau_map(&data))),
                                    );
                                }
                                Ok(Ok(None)) => {
                                    println!("DEBUG: Response Ok(None) for {}", chateau_id);
                                    return FetchResultMsg(chateau_id, None);
                                }
                                Ok(Err(e)) => {
                                    println!("DEBUG: RPC failed for {}: {:?}", chateau_id, e);
                                    if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                                        cache_write.remove(&chateau_id);
                                    }
                                }
                                Err(_) => {
                                    println!("DEBUG: Timeout fetching data for {}", chateau_id);
                                }
                            }

                            if !is_chateau_active(&active_chateaus, &chateau_id) {
                                return FetchResultMsg(chateau_id, None);
                            }

                            if let Ok(new_client) =
                                catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use)
                                    .await
                            {
                                aspen_manager
                                    .insert_client(socket_to_use.clone(), new_client.clone())
                                    .await;

                                println!("DEBUG: Reconnected to {}, retrying...", chateau_id);

                                let response_retry = tokio::time::timeout(
                                    timeout_duration,
                                    new_client.get_vehicle_locations(
                                        context::current(),
                                        chateau_id.clone(),
                                        None,
                                        Some(route_types),
                                    ),
                                )
                                .await;

                                if let Ok(Ok(Some(data))) = response_retry {
                                    println!("DEBUG: Retry success for {}", chateau_id);
                                    return FetchResultMsg(
                                        chateau_id,
                                        Some(Arc::new(precompute_chateau_map(&data))),
                                    );
                                } else {
                                    println!("DEBUG: Retry failed for {}", chateau_id);
                                    if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                                        cache_write.remove(&chateau_id);
                                    }
                                }
                            }
                        }

                        FetchResultMsg(chateau_id, None)
                    }
                },
            );

            let stream = futures::stream::iter(tasks).buffer_unordered(10);
            stream.right_stream()
        };

        ctx.spawn(actix::fut::wrap_future(fut).map(
            |stream, _actor: &mut BulkFetchCoordinator, ctx: &mut Context<Self>| {
                ctx.add_message_stream(stream);
            },
        ));
    }
}

impl Actor for BulkFetchCoordinator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_millis(1000), |act, ctx| {
            act.fetch_cycle(ctx);
        });
    }
}

impl Handler<Subscribe> for BulkFetchCoordinator {
    type Result = ();

    fn handle(&mut self, msg: Subscribe, ctx: &mut Self::Context) {
        let subs = self.subscribers.entry(msg.chateau_id.clone()).or_default();
        subs.insert(msg.recipient.clone());

        if let Ok(mut active) = self.active_chateaus.write() {
            active.insert(msg.chateau_id.clone());
        }

        if let Some((data, _)) = self.cache.get(&msg.chateau_id) {
            msg.recipient.do_send(ChateauUpdate {
                chateau_id: msg.chateau_id,
                response: data.clone(),
            });
        } else {
            ctx.notify(FetchChateauNow(msg.chateau_id));
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

                if let Ok(mut active) = self.active_chateaus.write() {
                    active.remove(&msg.chateau_id);
                }

                self.cache.remove(&msg.chateau_id);
            }
        }
    }
}

impl Handler<FetchResultMsg> for BulkFetchCoordinator {
    type Result = ();

    fn handle(&mut self, msg: FetchResultMsg, _: &mut Self::Context) {
        let FetchResultMsg(chateau_id, data_opt) = msg;
        self.in_progress_fetches.remove(&chateau_id);

        if let Some(data_arc) = data_opt {
            if !self.subscribers.contains_key(&chateau_id) {
                return;
            }

            let is_new = if let Some((old_data, _)) = self.cache.get(&chateau_id) {
                old_data.last_updated_time_ms != data_arc.last_updated_time_ms
            } else {
                true
            };

            if is_new {
                self.cache
                    .insert(chateau_id.clone(), (data_arc.clone(), Instant::now()));

                if let Some(subs) = self.subscribers.get(&chateau_id) {
                    for recipient in subs {
                        recipient.do_send(ChateauUpdate {
                            chateau_id: chateau_id.clone(),
                            response: data_arc.clone(),
                        });
                    }
                }
            }
        }
    }
}

impl Handler<FetchChateauNow> for BulkFetchCoordinator {
    type Result = ();

    fn handle(&mut self, msg: FetchChateauNow, ctx: &mut Self::Context) {
        let chateau_id = msg.0;
        if self.in_progress_fetches.contains_key(&chateau_id) {
            return;
        }

        self.in_progress_fetches
            .insert(chateau_id.clone(), Instant::now());

        let active_chateaus = self.active_chateaus.clone();
        let aspen_endpoint_cache = self.aspen_endpoint_cache.clone();
        let etcd_reuser = self.etcd_reuser.clone();
        let etcd_ips = self.etcd_connection_ips.clone();
        let etcd_opts = self.etcd_connection_options.clone();
        let aspen_manager = self.aspen_client_manager.clone();

        let fut = async move {
            let etcd_result = catenary::get_etcd_client(&etcd_ips, &etcd_opts, &etcd_reuser).await;
            let etcd = etcd_result.ok();

            if etcd.is_none() {
                return FetchResultMsg(chateau_id, None);
            }
            let mut etcd = etcd.unwrap();

            if !is_chateau_active(&active_chateaus, &chateau_id) {
                return FetchResultMsg(chateau_id, None);
            }

            let mut cached_socket: Option<std::net::SocketAddr> = None;
            if let Ok(cache_read) = aspen_endpoint_cache.read() {
                if let Some((socket, time)) = cache_read.get(&chateau_id) {
                    if time.elapsed() < Duration::from_secs(300) {
                        cached_socket = Some(socket.clone());
                    }
                }
            }

            let socket_to_use = if let Some(socket) = cached_socket.clone() {
                socket
            } else {
                let fetch_assigned_node = etcd
                    .get(
                        format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
                        None,
                    )
                    .await;

                if let Ok(resp) = fetch_assigned_node {
                    if !resp.kvs().is_empty() {
                        if let Ok(assigned_chateau_data) = bincode_deserialize::<ChateauMetadataEtcd>(
                            resp.kvs().first().unwrap().value(),
                        ) {
                            if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                                cache_write.insert(
                                    chateau_id.clone(),
                                    (assigned_chateau_data.socket.clone(), Instant::now()),
                                );
                            }
                            assigned_chateau_data.socket
                        } else {
                            return FetchResultMsg(chateau_id, None);
                        }
                    } else {
                        return FetchResultMsg(chateau_id, None);
                    }
                } else {
                    catenary::invalidate_etcd_client(&etcd_reuser).await;
                    return FetchResultMsg(chateau_id, None);
                }
            };

            let mut aspen_client = aspen_manager.get_client(socket_to_use.clone()).await;

            if aspen_client.is_none() {
                if let Ok(new_client) =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use).await
                {
                    aspen_manager
                        .insert_client(socket_to_use.clone(), new_client.clone())
                        .await;
                    aspen_client = Some(new_client);
                } else {
                    if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                        cache_write.remove(&chateau_id);
                    }
                }
            }

            if let Some(mut client) = aspen_client {
                let timeout_duration = Duration::from_secs(2);

                if !is_chateau_active(&active_chateaus, &chateau_id) {
                    return FetchResultMsg(chateau_id, None);
                }

                // Cold cache! Skip last_updated check!
                let route_types: Vec<i16> = vec![0, 1, 2, 3, 4, 5, 6, 7, 11, 12];

                let response = tokio::time::timeout(
                    timeout_duration,
                    client.get_vehicle_locations(
                        tarpc::context::current(),
                        chateau_id.clone(),
                        None,
                        Some(route_types.clone()),
                    ),
                )
                .await;

                match response {
                    Ok(Ok(Some(data))) => {
                        return FetchResultMsg(
                            chateau_id,
                            Some(Arc::new(precompute_chateau_map(&data))),
                        );
                    }
                    Ok(Ok(None)) => {
                        return FetchResultMsg(chateau_id, None);
                    }
                    Ok(Err(e)) => {
                        if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                            cache_write.remove(&chateau_id);
                        }
                    }
                    Err(_) => {}
                }

                if !is_chateau_active(&active_chateaus, &chateau_id) {
                    return FetchResultMsg(chateau_id, None);
                }

                if let Ok(new_client) =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use).await
                {
                    aspen_manager
                        .insert_client(socket_to_use.clone(), new_client.clone())
                        .await;

                    let response_retry = tokio::time::timeout(
                        timeout_duration,
                        new_client.get_vehicle_locations(
                            tarpc::context::current(),
                            chateau_id.clone(),
                            None,
                            Some(route_types),
                        ),
                    )
                    .await;

                    if let Ok(Ok(Some(data))) = response_retry {
                        return FetchResultMsg(
                            chateau_id,
                            Some(Arc::new(precompute_chateau_map(&data))),
                        );
                    } else {
                        if let Ok(mut cache_write) = aspen_endpoint_cache.write() {
                            cache_write.remove(&chateau_id);
                        }
                    }
                }
            }
            FetchResultMsg(chateau_id, None)
        };

        ctx.spawn(actix::fut::wrap_future(fut).map(
            |res, actor: &mut BulkFetchCoordinator, ctx| {
                actor.handle(res, ctx);
            },
        ));
    }
}
