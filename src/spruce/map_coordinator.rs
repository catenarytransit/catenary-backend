use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::aspen::lib::{ChateauMetadataEtcd, GetVehicleLocationsResponse};
use catenary::aspen_dataset::AspenisedVehicleDescriptor;
use catenary::aspen_dataset::CatenaryRtVehiclePosition;
use catenary::aspen_dataset::{AspenisedVehiclePosition, AspenisedVehicleRouteCache};
use catenary::bincode_deserialize;
use dashmap::DashMap;
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use slippy_map_tiles;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast};

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
    pub z_level: Option<u8>,
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

impl BoundsInputPerLevel {
    pub fn get_chateaus(
        &self,
        rtree: &crate::chateau_rtree::ChateauRTree,
        zoom: u8,
    ) -> std::collections::HashSet<String> {
        if let (Some(top_left), Some(bottom_right)) = (
            slippy_map_tiles::Tile::new(zoom, self.min_x, self.min_y),
            slippy_map_tiles::Tile::new(zoom, self.max_x, self.max_y),
        ) {
            rtree.locate_in_envelope(
                top_left.left() as f64,
                bottom_right.bottom() as f64,
                bottom_right.right() as f64,
                top_left.top() as f64,
            )
        } else {
            std::collections::HashSet::new()
        }
    }
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
    pub raw_vehicles: Vec<AspenisedVehiclePositionOutput>,
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

    let mut metro_raw = Vec::new();
    let mut bus_raw = Vec::new();
    let mut rail_raw = Vec::new();
    let mut other_raw = Vec::new();

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
                            metro_raw.push(output.clone());
                            metro_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                        CategoryOfRealtimeVehicleData::Bus => {
                            bus_raw.push(output.clone());
                            bus_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                        CategoryOfRealtimeVehicleData::Rail => {
                            rail_raw.push(output.clone());
                            rail_tiles
                                .entry(x)
                                .or_insert_with(BTreeMap::new)
                                .entry(y)
                                .or_insert_with(BTreeMap::new)
                                .insert(vehicle_id.clone(), output);
                        }
                        CategoryOfRealtimeVehicleData::Other => {
                            other_raw.push(output.clone());
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
            raw_vehicles: metro_raw,
            agency_ids: get_agencies(&route_types_metro),
        },
        bus: PrecomputedCategoryTiles {
            tiles: bus_tiles,
            raw_vehicles: bus_raw,
            agency_ids: get_agencies(&route_types_bus),
        },
        rail: PrecomputedCategoryTiles {
            tiles: rail_tiles,
            raw_vehicles: rail_raw,
            agency_ids: get_agencies(&route_types_rail),
        },
        other: PrecomputedCategoryTiles {
            tiles: other_tiles,
            raw_vehicles: other_raw,
            agency_ids: get_agencies(&route_types_other),
        },
    }
}

pub struct BulkFetchCoordinatorPool {
    chateaus: DashMap<String, broadcast::Sender<Arc<PrecomputedChateauMap>>>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<RwLock<Option<etcd_client::Client>>>,
    aspen_endpoint_cache: Arc<RwLock<HashMap<String, (std::net::SocketAddr, Instant)>>>,
}

impl BulkFetchCoordinatorPool {
    pub fn new(
        etcd_connection_ips: Arc<EtcdConnectionIps>,
        etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
        aspen_client_manager: Arc<AspenClientManager>,
        etcd_reuser: Arc<RwLock<Option<etcd_client::Client>>>,
    ) -> Self {
        Self {
            chateaus: DashMap::new(),
            etcd_connection_ips,
            etcd_connection_options,
            aspen_client_manager,
            etcd_reuser,
            aspen_endpoint_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn subscribe(
        self: &Arc<Self>,
        chateau_id: &str,
    ) -> broadcast::Receiver<Arc<PrecomputedChateauMap>> {
        if let Some(entry) = self.chateaus.get(chateau_id) {
            return entry.value().subscribe();
        }

        let (tx, rx) = broadcast::channel(16);
        self.chateaus.insert(chateau_id.to_string(), tx.clone());

        let pool = self.clone();
        let chateau_clone = chateau_id.to_string();

        tokio::spawn(async move {
            pool.fetch_loop(chateau_clone, tx).await;
        });

        rx
    }

    async fn fetch_loop(
        &self,
        chateau_id: String,
        tx: broadcast::Sender<Arc<PrecomputedChateauMap>>,
    ) {
        let mut ticker = tokio::time::interval(Duration::from_millis(1000));
        let mut cached_last_updated_time_ms = None;
        let mut cached_map: Option<Arc<PrecomputedChateauMap>> = None;

        loop {
            ticker.tick().await;

            if tx.receiver_count() == 0 {
                self.chateaus.remove(&chateau_id);
                break;
            }

            let timeout_duration = Duration::from_secs(2);

            let mut cached_socket: Option<std::net::SocketAddr> = None;
            {
                let cache_read = self.aspen_endpoint_cache.read().await;
                if let Some((socket, time)) = cache_read.get(&chateau_id) {
                    if time.elapsed() < Duration::from_secs(300) {
                        cached_socket = Some(*socket);
                    }
                }
            }

            let etcd_result = catenary::get_etcd_client(
                &self.etcd_connection_ips,
                &self.etcd_connection_options,
                &self.etcd_reuser,
            )
            .await;

            let mut etcd = match etcd_result {
                Ok(c) => c,
                Err(_) => {
                    // Just retry next tick
                    continue;
                }
            };

            let socket_to_use = if let Some(socket) = cached_socket {
                socket
            } else {
                let fetch_assigned_node = etcd
                    .get(
                        format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
                        None,
                    )
                    .await;

                if let Ok(resp) = fetch_assigned_node {
                    if let Some(kv) = resp.kvs().first() {
                        if let Ok(assigned_chateau_data) =
                            bincode_deserialize::<ChateauMetadataEtcd>(kv.value())
                        {
                            let mut cache_write = self.aspen_endpoint_cache.write().await;
                            cache_write.insert(
                                chateau_id.clone(),
                                (assigned_chateau_data.socket, Instant::now()),
                            );
                            assigned_chateau_data.socket
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    catenary::invalidate_etcd_client(&self.etcd_reuser).await;
                    continue;
                }
            };

            let mut aspen_client = self.aspen_client_manager.get_client(socket_to_use).await;

            if aspen_client.is_none() {
                if let Ok(new_client) =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use).await
                {
                    self.aspen_client_manager
                        .insert_client(socket_to_use, new_client.clone())
                        .await;
                    aspen_client = Some(new_client);
                } else {
                    let mut cache_write = self.aspen_endpoint_cache.write().await;
                    cache_write.remove(&chateau_id);
                    continue;
                }
            }

            if let Some(mut client) = aspen_client {
                let mut skipped_preflight = false;
                let mut remote_last_updated_time_ms = None;

                if cached_last_updated_time_ms.is_none() {
                    skipped_preflight = true;
                } else {
                    let last_updated_response = tokio::time::timeout(
                        timeout_duration,
                        client.last_updated_time_ms_for_chateau(
                            tarpc::context::current(),
                            chateau_id.clone(),
                        ),
                    )
                    .await;

                    match last_updated_response {
                        Ok(Ok(value)) => {
                            remote_last_updated_time_ms = Some(value);
                        }
                        Ok(Err(_)) | Err(_) => {
                            if let Ok(new_client) =
                                catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use)
                                    .await
                            {
                                self.aspen_client_manager
                                    .insert_client(socket_to_use, new_client.clone())
                                    .await;
                                client = new_client;

                                if let Ok(Ok(value)) = tokio::time::timeout(
                                    timeout_duration,
                                    client.last_updated_time_ms_for_chateau(
                                        tarpc::context::current(),
                                        chateau_id.clone(),
                                    ),
                                )
                                .await
                                {
                                    remote_last_updated_time_ms = Some(value);
                                } else {
                                    let mut cache_write = self.aspen_endpoint_cache.write().await;
                                    cache_write.remove(&chateau_id);
                                    continue;
                                }
                            } else {
                                let mut cache_write = self.aspen_endpoint_cache.write().await;
                                cache_write.remove(&chateau_id);
                                continue;
                            }
                        }
                    }
                }

                if !skipped_preflight {
                    if let Some(remote_val) = remote_last_updated_time_ms {
                        if cached_last_updated_time_ms == remote_val {
                            if let Some(map) = &cached_map {
                                let _ = tx.send(map.clone());
                            }
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

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

                let mut data_opt = match response {
                    Ok(Ok(Some(data))) => Some(data),
                    Ok(Ok(None)) => None,
                    Ok(Err(_)) => {
                        let mut cache_write = self.aspen_endpoint_cache.write().await;
                        cache_write.remove(&chateau_id);
                        None
                    }
                    Err(_) => None,
                };

                if data_opt.is_none() {
                    // Retry once
                    if let Ok(new_client) =
                        catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_to_use).await
                    {
                        self.aspen_client_manager
                            .insert_client(socket_to_use, new_client.clone())
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
                            data_opt = Some(data);
                        } else {
                            let mut cache_write = self.aspen_endpoint_cache.write().await;
                            cache_write.remove(&chateau_id);
                        }
                    }
                }

                if let Some(data) = data_opt {
                    cached_last_updated_time_ms = Some(data.last_updated_time_ms);
                    let precomputed = Arc::new(precompute_chateau_map(&data));
                    cached_map = Some(precomputed.clone());
                    let _ = tx.send(precomputed);
                } else if let Some(map) = &cached_map {
                    // Fallback to sending old map so clients get something
                    let _ = tx.send(map.clone());
                }
            }
        }
    }
}
