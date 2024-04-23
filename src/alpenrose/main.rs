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
    clippy::iter_cloned_collect
)]

// https://en.wikipedia.org/wiki/Rhododendron_ferrugineum
use catenary::agency_secret::*;
use catenary::fast_hash;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use catenary::schema::gtfs::admin_credentials::last_updated_ms;
use dashmap::DashMap;
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
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_zookeeper::*;
use uuid::Uuid;
mod single_fetch_time;

#[derive(Serialize, Clone, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct RealtimeFeedFetch {
    pub feed_id: String,
    pub realtime_vehicle_positions: Option<String>,
    pub realtime_trip_updates: Option<String>,
    pub realtime_alerts: Option<String>,
    pub key_formats: Vec<KeyFormat>,
    pub passwords: Option<Vec<PasswordInfo>>,
    pub fetch_interval_ms: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

    //If the worker disconnects from zookeeper, that's okay because tasks will be reassigned.
    // When it reconnects, the same worker id can be used and feed instructions will be reassigned to it.
    // ingestion won't run when the worker is disconnected from zookeeper due the instructions be written to the worker's ehpehmeral node

    // last check time
    let last_check_time_ms: Option<u64> = None;
    let last_set_of_active_nodes_hash: Option<u64> = None;
    let last_updated_feeds_hash: Option<u64> = None;

    //connect to postgres
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

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
        //timeout queries after 20 seconds
        .timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(20))
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .cookie_store(true)
        .build()
        .unwrap();

    loop {
        //create parent node for workers

        let _ = zk
            .create(
                "/alpenrose_workers",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        // create this worker as an ephemeral node
        let this_worker_assignment = zk
            .create(
                format!("/alpenrose_workers/{}", this_worker_id).as_str(),
                vec![],
                Acl::open_unsafe(),
                CreateMode::Ephemeral,
            )
            .await
            .unwrap()
            .unwrap();

        let workers_assignments = zk
            .create(
                "/alpenrose_assignments",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        //each feed id ephemeral id contains the last time updated, with none meaning the data has not been assigned to the node yet
        let this_worker_assignments = zk
            .create(
                format!("/alpenrose_assignments/{}", this_worker_id).as_str(),
                bincode::serialize(&None::<u64>).unwrap(),
                Acl::open_unsafe(),
                CreateMode::Ephemeral,
            )
            .await
            .unwrap();

        let leader_exists = zk.exists("/alpenrose_leader").await.unwrap();

        if leader_exists.is_none() {
            //attempt to become leader
            let leader = zk
                .create(
                    "/alpenrose_leader",
                    bincode::serialize(&this_worker_id).unwrap(),
                    Acl::open_unsafe(),
                    CreateMode::Ephemeral,
                )
                .await
                .unwrap();

            if leader.is_err() {
                println!("Failed to become leader");
            }
        }

        let leader = zk.watch().get_data("/alpenrose_leader").await.unwrap();

        if let Some((leader_str_bytes, leader_stats)) = leader {
            let leader_id: String = bincode::deserialize(&leader_str_bytes).unwrap();

            if &leader_id == this_worker_id.as_ref() {
                //I am the leader!

                let can_refresh_data = match last_check_time_ms {
                    Some(last_check_time_ms) => {
                        let current_time_ms = chrono::Utc::now().timestamp_millis() as u64;
                        let time_since_last_check = current_time_ms - last_check_time_ms;
                        time_since_last_check > 60000
                    }
                    None => true,
                };

                //Get data from postgres
                let feeds = get_feed_metadata(Arc::clone(&arc_conn_pool)).await;

                match feeds {
                    Ok(feeds) => {
                        //sort into BTreeMap

                        let feeds_map: BTreeMap<String, RealtimeFeedFetch> = {
                            let mut feeds_map = BTreeMap::new();
                            for feed in feeds {
                                feeds_map.insert(feed.feed_id.clone(), feed);
                            }
                            feeds_map
                        };

                        let fast_hash_of_feeds = fast_hash(&feeds_map);

                        // list of current active worker nodes

                        let mut worker_nodes =
                            zk.watch().get_children("/alpenrose_workers").await.unwrap();

                        if let Some(worker_nodes) = &mut worker_nodes {
                            //sort worker nodes
                            worker_nodes.sort();

                            let fast_hash_of_worker_nodes = fast_hash(&worker_nodes);

                            // either the list of workers
                            if last_set_of_active_nodes_hash != Some(fast_hash_of_worker_nodes)
                                || last_updated_feeds_hash != Some(fast_hash_of_feeds)
                            {
                                // divide feeds between worker nodes

                                // feed id -> List of realtime fetch instructions
                                let mut assignments: BTreeMap<
                                    String,
                                    HashMap<String, RealtimeFeedFetch>,
                                > = BTreeMap::new();

                                for (index, (feed_id, realtime_instructions)) in
                                    feeds_map.iter().enumerate()
                                {
                                    let node_to_assign = &worker_nodes[index % worker_nodes.len()];

                                    //append to list
                                    assignments
                                        .entry(node_to_assign.to_string())
                                        .and_modify(|instructions| {
                                            instructions.insert(
                                                feed_id.clone(),
                                                realtime_instructions.clone(),
                                            );
                                        })
                                        .or_insert({
                                            let mut map = HashMap::new();
                                            map.insert(
                                                feed_id.clone(),
                                                realtime_instructions.clone(),
                                            );
                                            map
                                        });
                                }

                                //update assignments in zookeeper

                                for (worker_id, instructions_hashmap) in assignments.iter() {
                                    for (feed_id, realtime_instruction) in instructions_hashmap {
                                        let feed_id_str = feed_id.clone();

                                        //update each feed under the workers node's assignment
                                        let assignment = zk
                                            .create(
                                                format!(
                                                    "/alpenrose_assignments/{}/{}",
                                                    worker_id, feed_id_str
                                                )
                                                .as_str(),
                                                bincode::serialize(&realtime_instruction).unwrap(),
                                                Acl::open_unsafe(),
                                                CreateMode::Persistent,
                                            )
                                            .await?;

                                        match assignment {
                                            Ok(_) => {
                                                println!(
                                                    "Assigned feed {} to worker {}",
                                                    feed_id_str, worker_id
                                                );
                                            }
                                            Err(error::Create::NodeExists) => {
                                                let set_assignment = zk
                                                    .set_data(
                                                        format!(
                                                            "/alpenrose_assignments/{}/{}",
                                                            worker_id, feed_id_str
                                                        )
                                                        .as_str(),
                                                        None,
                                                        bincode::serialize(&realtime_instruction)
                                                            .unwrap(),
                                                    )
                                                    .await?;

                                                match set_assignment {
                                                    Ok(_) => {
                                                        println!(
                                                            "Reassigned feed {} to worker {}",
                                                            feed_id_str, worker_id
                                                        );
                                                    }
                                                    Err(err) => {
                                                        eprintln!("Error reassigning feed {} to worker {}: {:?}", feed_id_str, worker_id, err);
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                eprintln!(
                                                    "Error assigning feed {} to worker {}: {:?}",
                                                    feed_id_str, worker_id, err
                                                );
                                            }
                                        }
                                    }

                                    //update the worker's last updated time
                                    let worker_assignment_metadata = zk
                                        .create(
                                            format!("/alpenrose_assignments/{}", this_worker_id)
                                                .as_str(),
                                            bincode::serialize(&Some(
                                                catenary::duration_since_unix_epoch().as_millis(),
                                            ))
                                            .unwrap(),
                                            Acl::open_unsafe(),
                                            CreateMode::Persistent,
                                        )
                                        .await?;

                                    match worker_assignment_metadata {
                                        Ok(_) => {
                                            println!("Updated worker assignment metadata");
                                        }
                                        Err(error::Create::NodeExists) => {
                                            let set_worker_assignment_metadata = zk
                                                .set_data(
                                                    format!(
                                                        "/alpenrose_assignments/{}",
                                                        this_worker_id
                                                    )
                                                    .as_str(),
                                                    None,
                                                    bincode::serialize(&Some(
                                                        catenary::duration_since_unix_epoch()
                                                            .as_millis(),
                                                    ))
                                                    .unwrap(),
                                                )
                                                .await?;

                                            match set_worker_assignment_metadata {
                                                Ok(_) => {
                                                    println!(
                                                        "Reassigned worker assignment metadata"
                                                    );
                                                }
                                                Err(err) => {
                                                    eprintln!("Error reassigning worker assignment metadata: {:?}", err);
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "Error updating worker assignment metadata: {:?}",
                                                err
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("Error getting feed metadata: {:?}", err);
                    }
                }
            }
        }

        //read from zookeeper to get the current assignments for this node

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
                                format!("/alpenrose_assignments/{}/{}", this_worker_id, feed_id)
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
    }
}

async fn get_feed_metadata(
    arc_conn_pool: Arc<CatenaryPostgresPool>,
) -> Result<Vec<RealtimeFeedFetch>, Box<dyn Error + Sync + Send>> {
    //Get feed metadata from postgres
    let dmfr_result = read_folders("./transitland-atlas/")?;

    //get everything out of realtime feeds table and realtime password tables

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    use catenary::schema::gtfs::realtime_feeds as realtime_feeds_table;

    let realtime_feeds = realtime_feeds_table::dsl::realtime_feeds
        .select(catenary::models::RealtimeFeed::as_select())
        .load::<catenary::models::RealtimeFeed>(conn)
        .await?;

    //format realtime feeds into HashMap

    let mut realtime_feeds_hashmap: HashMap<String, catenary::models::RealtimeFeed> =
        HashMap::new();

    for realtime_feed in realtime_feeds {
        let feed_id = realtime_feed.onestop_feed_id.clone();
        realtime_feeds_hashmap.insert(feed_id, realtime_feed);
    }

    let realtime_passwords = catenary::schema::gtfs::realtime_passwords::table
        .select(catenary::models::RealtimePasswordRow::as_select())
        .load::<catenary::models::RealtimePasswordRow>(conn)
        .await?;

    //format realtime passwords into HashMap

    let mut realtime_passwords_hashmap: HashMap<String, PasswordFormat> = HashMap::new();

    for realtime_password in realtime_passwords {
        let feed_id = realtime_password.onestop_feed_id.clone();
        let password_raw_json = realtime_password.passwords.clone();

        let password = password_raw_json.map(|password_format| {
            serde_json::from_value::<PasswordFormat>(password_format).unwrap()
        });

        if let Some(password) = password {
            realtime_passwords_hashmap.insert(feed_id, password);
        }
    }

    let mut realtime_feed_fetches: Vec<RealtimeFeedFetch> = Vec::new();

    for (feed_id, realtime_feed_dmfr) in dmfr_result
        .feed_hashmap
        .iter()
        //filter dmfr database for only GTFS rt feeds
        .filter(|(_, feed)| match feed.spec {
            dmfr::FeedSpec::GtfsRt => true,
            _ => false,
        })
    {
        let vehicles_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_realtime_vehicle_positions {
                Some(url) => Some(url.to_string()),
                None => realtime_feed_dmfr
                    .urls
                    .realtime_vehicle_positions
                    .as_ref()
                    .map(|url| url.as_str().to_string()),
            },
            None => realtime_feed_dmfr
                .urls
                .realtime_vehicle_positions
                .as_ref()
                .map(|url| url.as_str().to_string()),
        };

        let trip_updates_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_realtime_trip_updates {
                Some(url) => Some(url.to_string()),
                None => realtime_feed_dmfr
                    .urls
                    .realtime_trip_updates
                    .as_ref()
                    .map(|url| url.as_str().to_string()),
            },
            None => realtime_feed_dmfr
                .urls
                .realtime_trip_updates
                .as_ref()
                .map(|url| url.as_str().to_string()),
        };

        let alerts_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_alerts {
                Some(url) => Some(url.to_string()),
                None => realtime_feed_dmfr
                    .urls
                    .realtime_alerts
                    .as_ref()
                    .map(|url| url.as_str().to_string()),
            },
            None => realtime_feed_dmfr
                .urls
                .realtime_alerts
                .as_ref()
                .map(|url| url.as_str().to_string()),
        };

        realtime_feed_fetches.push(RealtimeFeedFetch {
            feed_id: feed_id.clone(),
            realtime_vehicle_positions: vehicles_url,
            realtime_trip_updates: trip_updates_url,
            realtime_alerts: alerts_url,
            key_formats: realtime_passwords_hashmap
                .get(feed_id)
                .unwrap()
                .key_formats
                .clone(),
            passwords: realtime_passwords_hashmap
                .get(feed_id)
                .map(|password_format| password_format.passwords.clone()),
            fetch_interval_ms: match realtime_feeds_hashmap.get(feed_id) {
                Some(realtime_feed) => realtime_feed.fetch_interval_ms,
                None => None,
            },
        })
    }

    Ok(realtime_feed_fetches)
}
