// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// AGPL 3.0

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
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref
)]

// https://en.wikipedia.org/wiki/Rhododendron_ferrugineum
use catenary::agency_secret::*;
use catenary::fast_hash;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use catenary::schema::gtfs::admin_credentials::last_updated_ms;
use dashmap::DashMap;
use diesel::dsl::exists;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel::sql_types::{Float, Integer};
use diesel::ExpressionMethods;
use diesel::Selectable;
use diesel::SelectableHelper;
use diesel_async::pooled_connection::bb8::PooledConnection;
use diesel_async::RunQueryDsl;
use dmfr_folder_reader::read_folders;
use futures::prelude::*;
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_zookeeper::*;
use uuid::Uuid;
mod custom_rt_feeds;
pub mod get_feed_metadata;
mod leader_job;
mod single_fetch_time;
use get_feed_metadata::RealtimeFeedFetch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    println!("Worker id {}", this_worker_id);

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

    //If the worker disconnects from zookeeper, that's okay because tasks will be reassigned.
    // When it reconnects, the same worker id can be used and feed instructions will be reassigned to it.
    // ingestion won't run when the worker is disconnected from zookeeper due the instructions be written to the worker's ehpehmeral node

    // last check time
    let last_check_time_ms: Option<u64> = None;
    let mut last_set_of_active_nodes_hash: Option<u64> = None;
    let mut last_updated_feeds_hash: Option<u64> = None;

    //connect to postgres
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

    println!("Connected to zookeeper!");

    let mut etcd_client = etcd_client::Client::connect(["localhost:2379"], None).await?;

    println!("Connected to etcd");

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let assignments_for_this_worker: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let last_updated_ms_for_this_worker: Option<u64> = None;

    let last_fetch_per_feed: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

    //make client for reqwest
    //allow various compression algorithms to be used during the download process, as enabled in Cargo.toml
    let client = reqwest::ClientBuilder::new()
        //timeout queries
        .timeout(Duration::from_secs(8))
        .connect_timeout(Duration::from_secs(5))
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .cookie_store(true)
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let mut etcd = etcd_client::Client::connect(["localhost:2379"], None).await?;

    let etcd_lease_id: i64 = rand::thread_rng().gen_range(0..i64::MAX);

    let make_lease = etcd
        .lease_grant(
            //30 seconds
            30,
            Some(etcd_client::LeaseGrantOptions::new().with_id(etcd_lease_id)),
        )
        .await?;

    //create parent node for workers

    loop {
        let is_online = online::tokio::check(Some(5)).await.is_ok();

        if is_online {
            //renew the etcd lease

            let _ = etcd.lease_keep_alive(etcd_lease_id).await?;

            // create this worker as an ephemeral node

            let etcd_this_worker_assignment = etcd
                .put(
                    format!("/alpenrose_workers/{}", this_worker_id).as_str(),
                    bincode::serialize(&etcd_lease_id).unwrap(),
                    Some(etcd_client::PutOptions::new().with_lease(etcd_lease_id)),
                )
                .await?;

            //each feed id ephemeral id contains the last time updated, with none meaning the data has not been assigned to the node yet

            /*
            THIS IS NOT NEEDED, there is no longer a directory structure in etcd v3, the entire thing is flattened

            let etcd_this_worker_assignment = etcd
                .put(
                    format!("/alpenrose_assignments/{}", this_worker_id).as_str(),
                    vec![],
                    Some(etcd_client::PutOptions::new().with_lease(etcd_lease_id)),
                )
                .await?;
            */

            let mut election_client = etcd.election_client();

            let leader = zk.watch().get_data("/alpenrose_leader").await.unwrap();

            let current_leader_election = election_client.leader("/alpenrose_leader").await;

            if let Ok(current_leader_election) = current_leader_election {
                let leader_kv = current_leader_election.kv();

                match leader_kv {
                    None => {
                        let attempt_to_become_leader = election_client
                            .campaign(
                                "/alpenrose_leader",
                                bincode::serialize(this_worker_id.as_ref()).unwrap(),
                                etcd_lease_id,
                            )
                            .await;
                    }
                    Some(leader_kv) => {
                        let leader_id: String = bincode::deserialize(leader_kv.value()).unwrap();

                        if &leader_id == this_worker_id.as_ref() {
                            // I AM THE LEADER!!!

                            println!("I AM THE LEADER!!!");

                            leader_job::perform_leader_job(
                                &mut etcd,
                                Arc::clone(&arc_conn_pool),
                                &mut last_set_of_active_nodes_hash,
                                &mut last_updated_feeds_hash,
                            )
                            .await?;
                        }
                    }
                }
            }

            //read from etcd to get the current assignments for this node

            let last_updated_assignment_time_zk_fetch = zk
                .get_data(format!("/alpenrose_assignments/{}", this_worker_id).as_str())
                .await
                .unwrap();

            if let Some(last_updated_assignment_time) = last_updated_assignment_time_zk_fetch {
                let last_updated_assignment_time =
                    bincode::deserialize(&last_updated_assignment_time.0).unwrap_or(None::<u64>);

                //is the time newer than the last time we updated the assignments for this worker node?
                if last_updated_assignment_time != last_updated_ms_for_this_worker {
                    let feed_ids = zk
                        .get_children(format!("/alpenrose_assignments/{}", this_worker_id).as_str())
                        .await
                        .unwrap();

                    if let Some(feed_ids) = feed_ids {
                        let mut assignments_for_this_worker_lock =
                            assignments_for_this_worker.write().await;

                        let hashset_of_feed_ids: HashSet<String> =
                            feed_ids.iter().map(|x| x.to_string()).collect();

                        for feed_id in feed_ids.iter() {
                            let assignment_data = zk
                                .get_data(
                                    format!(
                                        "/alpenrose_assignments/{}/{}",
                                        this_worker_id, feed_id
                                    )
                                    .as_str(),
                                )
                                .await
                                .unwrap();

                            if let Some(assignment_data) = assignment_data {
                                let realtime_feed_fetch: RealtimeFeedFetch =
                                    bincode::deserialize(&assignment_data.0).unwrap();

                                assignments_for_this_worker_lock
                                    .insert(feed_id.to_string(), realtime_feed_fetch);
                            }
                        }

                        //cleanup from hashmap this worker is no longer supposed to handle
                        assignments_for_this_worker_lock
                            .retain(|key, _value| hashset_of_feed_ids.contains(key));
                    }
                }
            }

            //get the feed data from the feeds assigned to this worker

            single_fetch_time::single_fetch_time(
                client.clone(),
                Arc::clone(&assignments_for_this_worker),
                Arc::clone(&last_fetch_per_feed),
            )
            .await?;
        } else {
            //revoke the lease

            let _ = etcd.lease_revoke(etcd_lease_id).await?;

            //delete the ephemeral node
            let delete_worker = zk
                .delete(
                    format!("/alpenrose_workers/{}", this_worker_id).as_str(),
                    None,
                )
                .await
                .unwrap();

            if delete_worker.is_err() {
                println!("Failed to delete worker node");
            }

            let delete_worker_assignments = zk
                .delete(
                    format!("/alpenrose_assignments/{}", this_worker_id).as_str(),
                    None,
                )
                .await
                .unwrap();

            if delete_worker_assignments.is_err() {
                println!("Failed to delete worker assignments node");
            }

            //delete the leader node if this node is the leader
            let leader = zk.watch().get_data("/alpenrose_leader").await.unwrap();

            if let Some((leader_str_bytes, leader_stats)) = leader {
                let leader_id: String = bincode::deserialize(&leader_str_bytes).unwrap();

                if &leader_id == this_worker_id.as_ref() {
                    let delete_leader = zk.delete("/alpenrose_leader", None).await.unwrap();

                    if delete_leader.is_err() {
                        println!("Failed to delete leader node");
                    }
                }
            }
        }
    }
}
