use catenary::aspen::lib::ChateauMetadataZookeeper;
use catenary::aspen::lib::ChateausLeaderHashMap;
use catenary::aspen::lib::RealtimeFeedMetadataZookeeper;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::ChateauDataNoGeometry;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel::sql_types::{Float, Integer};
use diesel::ExpressionMethods;
use diesel::Selectable;
use diesel::SelectableHelper;
use diesel_async::pooled_connection::bb8::PooledConnection;
use diesel_async::RunQueryDsl;
use std::collections::BTreeMap;
use std::error::Error;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_threadpool::Worker;
use tokio_zookeeper::ZooKeeper;
use tokio_zookeeper::{Acl, CreateMode};

pub async fn aspen_leader_thread(
    workers_nodes: Arc<Mutex<Vec<String>>>,
    feeds_list: Arc<Mutex<Option<ChateausLeaderHashMap>>>,
    this_worker_id: Arc<String>,
    tailscale_ip: Arc<IpAddr>,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    etcd_addresses: Arc<Vec<String>>,
    lease_id_for_this_worker: i64
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("starting leader thread");

    let mut etcd = etcd_client::Client::connect(etcd_addresses.as_slice(), None).await?;

    println!("Connected to etcd!");

    loop {
        // println!("loop");

        //if the current is the current worker id, do leader tasks
        // Read the DMFR dataset, divide it into chunks, and assign it to workers

        
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
