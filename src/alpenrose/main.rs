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
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use dashmap::DashMap;
use futures::prelude::*;
use rand::Rng;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::RwLock;
use uuid::Uuid;
mod custom_rt_feeds;
pub mod get_feed_metadata;
mod leader_job;
use std::io;
use zip::ZipArchive;
mod single_fetch_time;
use get_feed_metadata::RealtimeFeedFetch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    let start = Instant::now();

    let amtrak_gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read_from_url_async("https://content.amtrak.com/content/gtfs/GTFS.zip")
        .await
        .unwrap();

    let amtrak_gtfs = Arc::new(amtrak_gtfs);

    let rtc_quebec_gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read(
            "rtcquebec.zip",
        )
        .unwrap();

    let rtc_quebec_gtfs = Arc::new(rtc_quebec_gtfs);

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

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let assignments_for_this_worker: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let mut previously_known_updated_ms_for_this_worker: Option<u64> = None;

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

    let etcd_urls_original =
        std::env::var("ETCD_URLS").unwrap_or_else(|_| "localhost:2379".to_string());
    let etcd_urls = etcd_urls_original.split(',').collect::<Vec<&str>>();

    let etcd_username = std::env::var("ETCD_USERNAME");

    let etcd_password = std::env::var("ETCD_PASSWORD");

    let etcd_connection_options: Option<etcd_client::ConnectOptions> =
        match (etcd_username, etcd_password) {
            (Ok(username), Ok(password)) => {
                Some(etcd_client::ConnectOptions::new().with_user(username, password))
            }
            _ => None,
        };

    let arc_etcd_connection_options = Arc::new(etcd_connection_options.clone());

    let mut etcd =
        etcd_client::Client::connect(&etcd_urls, etcd_connection_options.clone()).await?;

    println!("Connected to etcd");

    let etcd_lease_id: i64 = rand::thread_rng().random_range(0..i64::MAX);

    let make_lease = etcd
        .lease_grant(
            //30 seconds
            30,
            Some(etcd_client::LeaseGrantOptions::new().with_id(etcd_lease_id)),
        )
        .await?;

    println!("etcd registered lease {}", etcd_lease_id);

    let chicago_gtfs = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip";

    let schedule_response = client.get(chicago_gtfs).send().await;

    let chicago_trips_str = Arc::new(match schedule_response {
        Ok(schedule_resp) => {
            // Create a ZIP archive from the bytes
            let schedule_bytes = schedule_resp.bytes().await;

            match schedule_bytes {
                Ok(schedule_bytes) => {
                    let mut archive = ZipArchive::new(io::Cursor::new(schedule_bytes));

                    match archive {
                        Ok(mut archive) => {
                            // Find and open the desired file
                            let mut trips_file = archive
                                .by_name("trips.txt")
                                .expect("trips.txt doesn't exist");
                            let mut buffer = Vec::new();
                            io::copy(&mut trips_file, &mut buffer).unwrap();

                            // Convert the buffer to a string
                            let trips_content = String::from_utf8(buffer).unwrap();

                            Some(trips_content)
                        }
                        Err(_) => None,
                    }
                }
                Err(e) => {
                    eprintln!("{:#?}", e);
                    None
                }
            }
        }
        Err(e) => {
            eprintln!("{:#?}", e);
            None
        }
    });

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

            let mut election_client = etcd.election_client();

            let current_leader_election = election_client.leader("/alpenrose_leader").await;

            match current_leader_election {
                Ok(current_leader_election) => {
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

                            println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);
                        }
                        Some(leader_kv) => {
                            let leader_id: String =
                                bincode::deserialize(leader_kv.value()).unwrap();

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
                Err(leader_election_err) => {
                    let attempt_to_become_leader = election_client
                        .campaign(
                            "/alpenrose_leader",
                            bincode::serialize(this_worker_id.as_ref()).unwrap(),
                            etcd_lease_id,
                        )
                        .await;

                    println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);

                    eprintln!("{:#?}", leader_election_err);

                    //fetch again and see if leader

                    let current_leader_election = election_client.leader("/alpenrose_leader").await;

                    if let Ok(current_leader_resp) = current_leader_election {
                        if let Some(leader_kv) = current_leader_resp.kv() {
                            let leader_id: String =
                                bincode::deserialize(leader_kv.value()).unwrap();

                            if &leader_id == this_worker_id.as_ref() {
                                // I AM THE LEADER!!!

                                println!("I AM THE LEADER on first try!!!");

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
            }

            //read from etcd to get the current assignments for this node

            let fetch_last_updated_assignments_for_this_worker_resp = etcd
                .get(
                    format!("/alpenrose_assignments_last_updated/{}", this_worker_id),
                    None,
                )
                .await?;

            let last_updated_worker_time_kv =
                fetch_last_updated_assignments_for_this_worker_resp.kvs();

            if let Some(last_updated_worker_time) = last_updated_worker_time_kv.first() {
                let last_updated_worker_time_value =
                    bincode::deserialize::<u64>(last_updated_worker_time.value()).unwrap();

                if Some(last_updated_worker_time_value)
                    != previously_known_updated_ms_for_this_worker
                {
                    previously_known_updated_ms_for_this_worker =
                        Some(last_updated_worker_time_value);

                    let mut assignments_for_this_worker_lock =
                        assignments_for_this_worker.write().await;

                    //fetch all the assignments

                    let prefix_search = format!("/alpenrose_assignments/{}/", this_worker_id);

                    let assignments = etcd
                        .get(
                            prefix_search.clone(),
                            Some(etcd_client::GetOptions::new().with_prefix()),
                        )
                        .await?
                        .take_kvs()
                        .into_iter()
                        .map(|each_kv| {
                            (
                                each_kv
                                    .key_str()
                                    .unwrap()
                                    .to_string()
                                    .replace(&prefix_search, ""),
                                bincode::deserialize::<RealtimeFeedFetch>(each_kv.value()).unwrap(),
                            )
                        })
                        .collect::<HashMap<String, RealtimeFeedFetch>>();

                    *assignments_for_this_worker_lock = assignments;
                }
            }

            //renew the lease
            let _ = etcd.lease_keep_alive(etcd_lease_id).await?;

            //get the feed data from the feeds assigned to this worker

            single_fetch_time::single_fetch_time(
                client.clone(),
                Arc::clone(&assignments_for_this_worker),
                Arc::clone(&last_fetch_per_feed),
                Arc::clone(&amtrak_gtfs),
                Arc::clone(&chicago_trips_str),
                Arc::clone(&rtc_quebec_gtfs),
                &etcd_urls,
                &etcd_connection_options,
            )
            .await?;
        } else {
            //revoke the lease

            let _ = etcd.lease_revoke(etcd_lease_id).await?;

            //end the program

            Err("No internet connection")?;
        }
    }
}
