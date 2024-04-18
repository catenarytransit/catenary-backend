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
use dashmap::DashMap;
use futures::{future, prelude::*};
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng,
};
use std::sync::Arc;
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
use catenary::aspen_dataset::GtfsRtDataStore;
use catenary::postgres_tools::CatenaryPostgresPool;

#[derive(Parser)]
struct Flags {
    /// Sets the port number to listen on.
    #[clap(long)]
    port: u16,
}

// This is the type that implements the generated World trait. It is the business logic
// and is used to start the server.
#[derive(Clone)]
pub struct AspenServer {
    pub addr: SocketAddr,
    pub this_tailscale_ip: IpAddr,
    pub worker_id: Arc<String>, // Worker Id for this instance of Aspen
    pub authoritative_data_store: Arc<DashMap<String, RwLock<catenary::aspen_dataset::AspenisedData>>>,
    pub conn_pool: Arc<CatenaryPostgresPool>,
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
    ) -> bool {
        import_alpenrose::new_rt_data(
            Arc::clone(&self.authoritative_data_store),
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
            Arc::clone(&self.conn_pool)
        ).await
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Worker Id for this instance of Aspen
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    //connect to postgres
    let conn_pool: CatenaryPostgresPool = make_async_pool().await.unwrap();
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    let flags = Flags::parse();
    //init_tracing("Tarpc Example Server")?;

    let tailscale_ip = catenary::tailscale::interface().expect("no tailscale interface found");

    let server_addr = (tailscale_ip, flags.port);

    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr,
         Bincode::default).await?;
    //tracing::info!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);

    let workers_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let chateau_list: Arc<Mutex<Option<ChateausLeaderHashMap>>> = Arc::new(Mutex::new(None));

    //run both the leader and the listener simultaniously
    futures::join!(
        {
            let workers_nodes = Arc::clone(&workers_nodes);
            let chateau_list = Arc::clone(&chateau_list);
            let this_worker_id = Arc::clone(&this_worker_id);
            let tailscale_ip = Arc::new(tailscale_ip.clone());
            let arc_conn_pool = Arc::clone(&arc_conn_pool);
            async {
                aspen_leader_thread(
                    workers_nodes,
                    chateau_list,
                    this_worker_id,
                    tailscale_ip,
                    arc_conn_pool,
                )
                .await;
            }
        },
        listener
            // Ignore accept errors.
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            // serve is generated by the service attribute. It takes as input any type implementing
            // the generated World trait.
            .map(|channel| {
                let server = AspenServer {
                    addr: channel.transport().peer_addr().unwrap(),
                    this_tailscale_ip: tailscale_ip,
                    worker_id: Arc::clone(&this_worker_id),
                    authoritative_data_store: Arc::new(DashMap::new()),
                    conn_pool: Arc::clone(&arc_conn_pool),
                };
                channel.execute(server.serve()).for_each(spawn)
            })
            // Max n channels.
            .buffer_unordered(32)
            .for_each(|_| async {})
    );

    Ok(())
}
