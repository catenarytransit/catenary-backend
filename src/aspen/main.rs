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
use catenary::aspen::lib::*;
use catenary::postgres_tools::make_async_pool;
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
use catenary::aspen_dataset::*;
use catenary::postgres_tools::CatenaryPostgresPool;
use crossbeam::deque::{Injector, Steal};
use futures::join;
use gtfs_rt::FeedMessage;
use scc::HashMap as SccHashMap;
mod async_threads_alpenrose;

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
        self.alpenrose_to_process_queue.push(ProcessAlpenroseData {
            chateau_id,
            realtime_feed_id,
            vehicles,
            trips,
            alerts,
            has_vehicles,
            has_trips,
            has_alerts,
            vehicles_response_code,
            trips_response_code,
            alerts_response_code,
            time_of_submission_ms,
        });
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

    let workers_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let chateau_list: Arc<Mutex<Option<ChateausLeaderHashMap>>> = Arc::new(Mutex::new(None));

    let process_from_alpenrose_queue = Arc::new(Injector::<ProcessAlpenroseData>::new());
    let raw_gtfs = Arc::new(SccHashMap::new());
    let authoritative_data_store = Arc::new(SccHashMap::new());

    //run both the leader and the listener simultaniously

    let workers_nodes_for_leader_thread = Arc::clone(&workers_nodes);
    let chateau_list_for_leader_thread = Arc::clone(&chateau_list);
    let this_worker_id_for_leader_thread = Arc::clone(&this_worker_id);
    let tailscale_ip_for_leader_thread = Arc::new(tailscale_ip);
    let arc_conn_pool_for_leader_thread = Arc::clone(&arc_conn_pool);
    let leader_thread_handler = tokio::task::spawn(aspen_leader_thread(
        workers_nodes_for_leader_thread,
        chateau_list_for_leader_thread,
        this_worker_id_for_leader_thread,
        tailscale_ip_for_leader_thread,
        arc_conn_pool_for_leader_thread,
    ));

    let async_from_alpenrose_processor_handler = tokio::task::spawn({
        let alpenrose_to_process_queue = Arc::clone(&process_from_alpenrose_queue);
        let authoritative_gtfs_rt_store = Arc::clone(&raw_gtfs);
        let authoritative_data_store = Arc::clone(&authoritative_data_store);
        let conn_pool = Arc::clone(&arc_conn_pool);
        let thread_count = alpenrosethreadcount.clone();
        move || async move {
            async_threads_alpenrose::alpenrose_process_threads(
                alpenrose_to_process_queue,
                authoritative_gtfs_rt_store,
                authoritative_data_store,
                conn_pool,
                thread_count,
            )
            .await;
        }
    }());

    let tarpc_server = tokio::task::spawn({
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
                    };
                    channel.execute(server.serve()).for_each(spawn)
                })
                // Max n channels.
                .buffer_unordered(channel_count)
                .for_each(|_| async {})
                .await;
        }
    }());

    let result_series = futures::join!(
        leader_thread_handler,
        async_from_alpenrose_processor_handler,
        tarpc_server
    );

    Ok(())
}
