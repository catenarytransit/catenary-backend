// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect
)]
use catenary::postgres_tools::make_async_pool;
use catenary::{aspen::lib::*, id_cleanup};
use clap::Parser;
use futures::{future, prelude::*};
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng,
};
use std::sync::Arc;
use std::thread;
use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tarpc::{
    context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Bincode,
};
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use uuid::Uuid;
mod leader_thread;
use leader_thread::aspen_leader_thread;
mod import_alpenrose;
use catenary::aspen_dataset::GtfsRtType;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::{aspen_dataset::*, gtfs_rt_rough_hash};
use crossbeam::deque::{Injector, Steal};
use futures::join;
use gtfs_rt::FeedMessage;
use scc::HashMap as SccHashMap;
use std::error::Error;
mod async_threads_alpenrose;
use catenary::gtfs_rt_rough_hash::rough_hash_of_gtfs_rt;
use catenary::parse_gtfs_rt_message;
use std::collections::HashSet;
use tokio_zookeeper::ZooKeeper;
use tokio_zookeeper::{Acl, CreateMode};

// This is the type that implements the generated World trait. It is the business logic
// and is used to start the server.
#[derive(Clone)]
pub struct AspenServer {
    pub addr: SocketAddr,
    pub this_tailscale_ip: IpAddr,
    pub worker_id: Arc<String>, // Worker Id for this instance of Aspen
    pub authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    // Backed up in redis as well, program can be shut down and restarted without data loss
    pub authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    pub conn_pool: Arc<CatenaryPostgresPool>,
    pub alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    pub alpenrose_to_process_queue_chateaus: Arc<Mutex<HashSet<String>>>,
    pub rough_hash_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>>,
    pub backup_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    pub backup_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
}

impl AspenRpc for AspenServer {
    async fn hello(self, _: context::Context, name: String) -> String {
        let sleep_time =
            Duration::from_millis(Uniform::new_inclusive(1, 10).sample(&mut thread_rng()));
        time::sleep(sleep_time).await;
        format!("Hello, {name}! You are connected from {}", self.addr)
    }

    async fn from_alpenrose(
        self,
        _: context::Context,
        chateau_id: String,
        realtime_feed_id: String,
        vehicles: Option<Vec<u8>>,
        trips: Option<Vec<u8>>,
        alerts: Option<Vec<u8>>,
        has_vehicles: bool,
        has_trips: bool,
        has_alerts: bool,
        vehicles_response_code: Option<u16>,
        trips_response_code: Option<u16>,
        alerts_response_code: Option<u16>,
        time_of_submission_ms: u64,
    ) -> bool {
        let vehicles_gtfs_rt = match vehicles_response_code {
            Some(200) => match vehicles {
                Some(v) => match parse_gtfs_rt_message(v.as_slice()) {
                    Ok(v) => Some(id_cleanup::gtfs_rt_cleanup(v)),
                    Err(e) => {
                        println!("Error decoding vehicles: {}", e);
                        None
                    }
                },
                None => None,
            },
            _ => None,
        };

        let vehicles_gtfs_rt =
            vehicles_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                _ => gtfs_rt_feed,
            });

        let trips_gtfs_rt = match trips_response_code {
            Some(200) => match trips {
                Some(t) => match parse_gtfs_rt_message(t.as_slice()) {
                    Ok(t) => Some(id_cleanup::gtfs_rt_cleanup(t)),
                    Err(e) => {
                        println!("Error decoding trips: {}", e);
                        None
                    }
                },
                None => None,
            },
            _ => None,
        };

        let trips_gtfs_rt = trips_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
            "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
            _ => gtfs_rt_feed,
        });

        let alerts_gtfs_rt = match alerts_response_code {
            Some(200) => match alerts {
                Some(a) => match parse_gtfs_rt_message(a.as_slice()) {
                    Ok(a) => Some(id_cleanup::gtfs_rt_cleanup(a)),
                    Err(e) => {
                        println!("Error decoding alerts: {}", e);
                        None
                    }
                },
                None => None,
            },
            _ => None,
        };

        //get and update raw gtfs_rt data

        //  println!("Parsed FeedMessages for {}", realtime_feed_id);

        let mut new_data = false;

        if !new_data {
            if let Some(vehicles_gtfs_rt) = &vehicles_gtfs_rt {
                let hash = rough_hash_of_gtfs_rt(&vehicles_gtfs_rt);

                let key = (realtime_feed_id.clone(), GtfsRtType::VehiclePositions);

                match self.rough_hash_of_gtfs_rt.get(&key) {
                    Some(existing_hash) => {
                        if existing_hash.get() != &hash {
                            new_data = true;
                        }
                    }
                    None => {
                        new_data = true;
                    }
                }

                self.rough_hash_of_gtfs_rt
                    .entry(key)
                    .and_modify(|gtfs_data| *gtfs_data = hash)
                    .or_insert(hash);
            }
        }

        if !new_data {
            if let Some(trips_gtfs_rt) = &trips_gtfs_rt {
                let hash = rough_hash_of_gtfs_rt(&trips_gtfs_rt);

                let key = (realtime_feed_id.clone(), GtfsRtType::TripUpdates);

                match self.rough_hash_of_gtfs_rt.get(&key) {
                    Some(existing_hash) => {
                        if existing_hash.get() != &hash {
                            new_data = true;
                        }
                    }
                    None => {
                        new_data = true;
                    }
                }

                self.rough_hash_of_gtfs_rt
                    .entry(key)
                    .and_modify(|gtfs_data| *gtfs_data = hash)
                    .or_insert(hash);
            }
        }

        if !new_data {
            if let Some(alerts_gtfs_rt) = &alerts_gtfs_rt {
                let hash = rough_hash_of_gtfs_rt(&alerts_gtfs_rt);

                let key = (realtime_feed_id.clone(), GtfsRtType::Alerts);

                match self.rough_hash_of_gtfs_rt.get(&key) {
                    Some(existing_hash) => {
                        if existing_hash.get() != &hash {
                            new_data = true;
                        }
                    }
                    None => {
                        new_data = true;
                    }
                }

                self.rough_hash_of_gtfs_rt
                    .entry(key)
                    .and_modify(|gtfs_data| *gtfs_data = hash)
                    .or_insert(hash);
            }
        }

        if let Some(vehicles_gtfs_rt) = vehicles_gtfs_rt {
            self.authoritative_gtfs_rt_store
                .entry((realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
                .and_modify(|gtfs_data| *gtfs_data = vehicles_gtfs_rt.clone())
                .or_insert(vehicles_gtfs_rt.clone());
        }

        if let Some(trip_gtfs_rt) = trips_gtfs_rt {
            self.authoritative_gtfs_rt_store
                .entry((realtime_feed_id.clone(), GtfsRtType::TripUpdates))
                .and_modify(|gtfs_data| *gtfs_data = trip_gtfs_rt.clone())
                .or_insert(trip_gtfs_rt.clone());
        }

        if let Some(alerts_gtfs_rt) = alerts_gtfs_rt {
            self.authoritative_gtfs_rt_store
                .entry((realtime_feed_id.clone(), GtfsRtType::Alerts))
                .and_modify(|gtfs_data| *gtfs_data = alerts_gtfs_rt.clone())
                .or_insert(alerts_gtfs_rt.clone());
        }

        //   println!("Saved FeedMessages for {}", realtime_feed_id);

        if (new_data) {
            let mut lock_chateau_queue = self.alpenrose_to_process_queue_chateaus.lock().await;

            if !lock_chateau_queue.contains(&chateau_id) {
                lock_chateau_queue.insert(chateau_id.clone());
                self.alpenrose_to_process_queue.push(ProcessAlpenroseData {
                    chateau_id,
                    realtime_feed_id,
                    has_vehicles,
                    has_trips,
                    has_alerts,
                    vehicles_response_code,
                    trips_response_code,
                    alerts_response_code,
                    time_of_submission_ms,
                });
            }
        } else {
            println!(
                "No new data for {} under chateau {}, rough hash is the same",
                realtime_feed_id, chateau_id
            );
        }

        true
    }

    async fn get_vehicle_locations(
        self,
        _: context::Context,
        chateau_id: String,
        existing_fasthash_of_routes: Option<u64>,
    ) -> Option<GetVehicleLocationsResponse> {
        match self.authoritative_data_store.get(&chateau_id) {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();

                let fast_hash_of_routes = catenary::fast_hash(
                    &aspenised_data
                        .vehicle_routes_cache
                        .values()
                        .collect::<Vec<&AspenisedVehicleRouteCache>>(),
                );

                Some(GetVehicleLocationsResponse {
                    vehicle_route_cache: match existing_fasthash_of_routes {
                        Some(existing_fasthash_of_routes) => {
                            match existing_fasthash_of_routes == fast_hash_of_routes {
                                true => None,
                                false => Some(aspenised_data.vehicle_routes_cache.clone()),
                            }
                        }
                        None => Some(aspenised_data.vehicle_routes_cache.clone()),
                    },
                    vehicle_positions: aspenised_data.vehicle_positions.clone(),
                    hash_of_routes: fast_hash_of_routes,
                    last_updated_time_ms: aspenised_data.last_updated_time_ms,
                })
            }
            None => None,
        }
    }

    async fn get_single_vehicle_location_from_gtfsid(self, _: context::Context, chateau_id: String, gtfs_id: String) -> Option<AspenisedVehiclePosition> {
       match self.authoritative_data_store.get(&chateau_id) {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();

                let vehicle_position = aspenised_data.vehicle_positions.get(&gtfs_id);

                match vehicle_position {
                    Some(vehicle_position) => {
                        Some(vehicle_position.clone())
                    }
                    None => {
                        println!("Vehicle position not found for gtfs id {}", gtfs_id);
                        None
                    }
                }
            }
            None => None,
        }
    }

    async fn get_trip_updates_from_trip_id(
        self,
        _: context::Context,
        chateau_id: String,
        trip_id: String,
    ) -> Option<Vec<AspenisedTripUpdate>> {
        match self.authoritative_data_store.get(&chateau_id) {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();

                let trip_updates_id_list = aspenised_data
                    .trip_updates_lookup_by_trip_id_to_trip_update_ids
                    .get(&trip_id);

                match trip_updates_id_list {
                    Some(trip_updates_id_list) => {
                        let mut trip_updates = Vec::new();

                        for trip_update_id in trip_updates_id_list {
                            let trip_update = aspenised_data.trip_updates.get(trip_update_id);

                            match trip_update {
                                Some(trip_update) => {
                                    trip_updates.push(trip_update.clone());
                                }
                                None => {
                                    println!(
                                        "Trip update not found for trip update id {}",
                                        trip_update_id
                                    );
                                }
                            }
                        }

                        Some(trip_updates)
                    }
                    None => {
                        println!("Trip id not found in trip updates lookup table");
                        None
                    }
                }
            }
            None => None,
        }
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Worker Id for this instance of Aspen
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    let channel_count = std::env::var("CHANNELS")
        .expect("channels not set")
        .parse::<usize>()
        .expect("channels not a number");
    let alpenrosethreadcount = std::env::var("ALPENROSETHREADCOUNT")
        .expect("alpenrosethreadcount not set")
        .parse::<usize>()
        .expect("alpenrosethreadcount not a number");

    //connect to postgres
    println!("Connecting to postgres");
    let conn_pool: CatenaryPostgresPool = make_async_pool().await.unwrap();
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);
    println!("Connected to postgres");

    let tailscale_ip = catenary::tailscale::interface().expect("no tailscale interface found");

    let server_addr = (tailscale_ip, 40427);

    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Bincode::default).await?;
    //tracing::info!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);

    //register the worker with the leader

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

    //if zookeeper /aspen_workers doesn't exist, create it

    let get_zk_aspen_workers_parent = zk.get_data("/aspen_workers").await.unwrap();

    if get_zk_aspen_workers_parent.is_none() {
        let _ = zk
            .create(
                "/aspen_workers",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();
    }

    //register that the worker exists
    let _ = zk
        .create(
            format!("/aspen_workers/{}", this_worker_id).as_str(),
            bincode::serialize(&tailscale_ip).unwrap(),
            Acl::open_unsafe(),
            CreateMode::Ephemeral,
        )
        .await
        .unwrap();

    let workers_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let chateau_list: Arc<Mutex<Option<ChateausLeaderHashMap>>> = Arc::new(Mutex::new(None));

    let process_from_alpenrose_queue = Arc::new(Injector::<ProcessAlpenroseData>::new());
    let raw_gtfs = Arc::new(SccHashMap::new());
    let authoritative_data_store = Arc::new(SccHashMap::new());
    let backup_data_store = Arc::new(SccHashMap::new());
    let backup_raw_gtfs = Arc::new(SccHashMap::new());
    let alpenrose_to_process_queue_chateaus = Arc::new(Mutex::new(HashSet::new()));
    let rough_hash_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>> =
        Arc::new(SccHashMap::new());
    //run both the leader and the listener simultaniously

    let workers_nodes_for_leader_thread = Arc::clone(&workers_nodes);
    let chateau_list_for_leader_thread = Arc::clone(&chateau_list);
    let this_worker_id_for_leader_thread = Arc::clone(&this_worker_id);
    let tailscale_ip_for_leader_thread = Arc::new(tailscale_ip);
    let arc_conn_pool_for_leader_thread = Arc::clone(&arc_conn_pool);
    let leader_thread_handler: tokio::task::JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> =
        tokio::task::spawn(aspen_leader_thread(
            workers_nodes_for_leader_thread,
            chateau_list_for_leader_thread,
            this_worker_id_for_leader_thread,
            tailscale_ip_for_leader_thread,
            arc_conn_pool_for_leader_thread,
        ));

    let b_alpenrose_to_process_queue = Arc::clone(&process_from_alpenrose_queue);
    let b_authoritative_gtfs_rt_store = Arc::clone(&raw_gtfs);
    let b_authoritative_data_store = Arc::clone(&authoritative_data_store);
    let b_conn_pool = Arc::clone(&arc_conn_pool);
    let b_thread_count = alpenrosethreadcount;
    let b_backup_data_store = Arc::clone(&backup_data_store);
    let b_backup_gtfs_rt_store = Arc::clone(&backup_raw_gtfs);

    let async_from_alpenrose_processor_handler: tokio::task::JoinHandle<
        Result<(), Box<dyn Error + Sync + Send>>,
    > = tokio::task::spawn(async_threads_alpenrose::alpenrose_process_threads(
        b_alpenrose_to_process_queue,
        b_authoritative_gtfs_rt_store,
        b_authoritative_data_store,
        b_conn_pool,
        b_thread_count,
        Arc::clone(&alpenrose_to_process_queue_chateaus),
    ));

    let tarpc_server: tokio::task::JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> =
        tokio::task::spawn({
            println!("Listening on port {}", listener.local_addr().port());

            move || async move {
                listener
                    // Ignore accept errors.
                    .filter_map(|r| future::ready(r.ok()))
                    .map(server::BaseChannel::with_defaults)
                    .map(|channel| {
                        let server = AspenServer {
                            addr: channel.transport().peer_addr().unwrap(),
                            this_tailscale_ip: tailscale_ip,
                            worker_id: Arc::clone(&this_worker_id),
                            authoritative_data_store: Arc::clone(&authoritative_data_store),
                            conn_pool: Arc::clone(&arc_conn_pool),
                            alpenrose_to_process_queue: Arc::clone(&process_from_alpenrose_queue),
                            authoritative_gtfs_rt_store: Arc::clone(&raw_gtfs),
                            backup_data_store: Arc::clone(&backup_data_store),
                            backup_gtfs_rt_store: Arc::clone(&backup_raw_gtfs),
                            alpenrose_to_process_queue_chateaus: Arc::clone(
                                &alpenrose_to_process_queue_chateaus,
                            ),
                            rough_hash_of_gtfs_rt: Arc::clone(&rough_hash_of_gtfs_rt),
                        };
                        channel.execute(server.serve()).for_each(spawn)
                    })
                    // Max n channels.
                    .buffer_unordered(channel_count)
                    .for_each(|_| async {})
                    .await;

                Ok(())
            }
        }());

    let result_series = tokio::try_join!(
        leader_thread_handler,
        async_from_alpenrose_processor_handler,
        tarpc_server,
    );

    match result_series {
        Ok(_) => {
            println!("All threads have exited successfully");
            Ok(())
        }
        Err(e) => {
            println!("Error: {:?}", e);
            Err(anyhow::Error::new(e))
        }
    }
}
