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
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
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

        let chateaus_to_fetch: Vec<String> = self.subscribers.keys().cloned().collect();
        let etcd_reuser = self.etcd_reuser.clone();
        let etcd_ips = self.etcd_connection_ips.clone();
        let etcd_opts = self.etcd_connection_options.clone();
        let aspen_manager = self.aspen_client_manager.clone();

        let fut = async move {
            let etcd_result = catenary::get_etcd_client(&etcd_ips, &etcd_opts, &etcd_reuser).await;

            let etcd = etcd_result.ok();

            if etcd.is_none() {
                return Vec::new();
            }
            let etcd = etcd.unwrap();

            let tasks = chateaus_to_fetch.into_iter().map(|chateau_id| {
                let mut etcd = etcd.clone();
                let aspen_manager = aspen_manager.clone();
                let etcd_reuser = etcd_reuser.clone();

                async move {
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
                                let mut aspen_client =
                                    aspen_manager.get_client(assigned_chateau_data.socket).await;

                                if aspen_client.is_none() {
                                    println!(
                                        "DEBUG: Aspen client missing for {}, connecting...",
                                        chateau_id
                                    );
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
                                        println!(
                                            "DEBUG: Failed to spawn aspen client for {}",
                                            chateau_id
                                        );
                                    }
                                }

                                if let Some(client) = aspen_client {
                                    let route_types: Vec<i16> =
                                        vec![0, 1, 2, 3, 4, 5, 6, 7, 11, 12];
                                    let timeout_duration = Duration::from_secs(2);

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
                                            // println!("DEBUG: Got data for {}", chateau_id);
                                            return Some((chateau_id, data));
                                        }
                                        Ok(Ok(None)) => {
                                            println!("DEBUG: Response Ok(None) for {}", chateau_id);
                                        }
                                        Ok(Err(e)) => {
                                            println!(
                                                "DEBUG: RPC failed for {}: {:?}",
                                                chateau_id, e
                                            );
                                            // Retry logic proceeds below
                                        }
                                        Err(_) => {
                                            println!(
                                                "DEBUG: Timeout fetching data for {}",
                                                chateau_id
                                            );
                                            // Retry logic proceeds below
                                        }
                                    }

                                    // Retry logic
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

                                        println!(
                                            "DEBUG: Reconnected to {}, retrying...",
                                            chateau_id
                                        );
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
                                            return Some((chateau_id, data));
                                        } else {
                                            println!("DEBUG: Retry failed for {}", chateau_id);
                                        }
                                    }
                                }
                            }
                        } else {
                            println!("DEBUG: No assigned node for {}", chateau_id);
                        }
                    } else {
                        println!("DEBUG: Etcd fetch failed for {}", chateau_id);
                        catenary::invalidate_etcd_client(&etcd_reuser).await;
                        println!(
                            "DEBUG: Flushed Etcd reuser due to failure for {}",
                            chateau_id
                        );
                    }
                    None
                }
            });

            join_all(tasks).await.into_iter().flatten().collect()
        };

        ctx.spawn(actix::fut::wrap_future(fut).map(
            |results, actor: &mut BulkFetchCoordinator, _| {
                for (chateau_id, data) in results {
                    let data_arc = Arc::new(data);

                    let is_new = if let Some((old_data, _)) = actor.cache.get(&chateau_id) {
                        old_data.last_updated_time_ms != data_arc.last_updated_time_ms
                    } else {
                        true
                    };

                    if is_new {
                        actor
                            .cache
                            .insert(chateau_id.clone(), (data_arc.clone(), Instant::now()));
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

    fn handle(&mut self, msg: Subscribe, _: &mut Self::Context) {
        let subs = self.subscribers.entry(msg.chateau_id.clone()).or_default();
        subs.insert(msg.recipient.clone());

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
