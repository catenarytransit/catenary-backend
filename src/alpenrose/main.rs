// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// AGPL 3.0

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::assigning_clones,
    clippy::redundant_allocation,
    bool_comparison,
    bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map
)]

// https://en.wikipedia.org/wiki/Rhododendron_ferrugineum
use catenary::agency_secret::*;
use catenary::fast_hash;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
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
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio_zookeeper::*;
use uuid::Uuid;

// gtfs unix timestamps
struct LastDataFetched {
    realtime_vehicle_positions: Option<u64>,
    realtime_trip_updates: Option<u64>,
    realtime_alerts: Option<u64>,
}

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

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
struct InstructionsPerWorker {
    feeds: RealtimeFeedFetch,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

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

    loop {
        //create parent node for workers

        let workers = zk
            .create(
                "/alpenrose_workers",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
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

        let leader_exists = zk.watch().exists("/alpenrose_leader").await.unwrap();

        if leader_exists.is_none() {
            //attempt to become leader
            let leader = zk
                .create(
                    "/alpenrose_leader",
                    this_worker_id.as_bytes().to_vec(),
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
            let leader_id = String::from_utf8(leader_str_bytes).unwrap();

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
                                let mut assignments: BTreeMap<String, Vec<RealtimeFeedFetch>> =
                                    BTreeMap::new();

                                for (index, (feed_id, realtime_instructions)) in
                                    feeds_map.iter().enumerate()
                                {
                                    let node_to_assign = &worker_nodes[index % worker_nodes.len()];

                                    let _ = assignments.entry(node_to_assign.to_string());
                                    //append to list
                                }
                                // look at current assignments, delete old assignments

                                // assign feeds to worker nodes
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

        let password = match password_raw_json {
            Some(password_format) => {
                Some(serde_json::from_value::<PasswordFormat>(password_format).unwrap())
            }
            None => None,
        };

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
                None => match &realtime_feed_dmfr.urls.realtime_vehicle_positions {
                    Some(url) => Some(url.as_str().to_string()),
                    None => None,
                },
            },
            None => match &realtime_feed_dmfr.urls.realtime_vehicle_positions {
                Some(url) => Some(url.as_str().to_string()),
                None => None,
            },
        };

        let trip_updates_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_realtime_trip_updates {
                Some(url) => Some(url.to_string()),
                None => match &realtime_feed_dmfr.urls.realtime_trip_updates {
                    Some(url) => Some(url.as_str().to_string()),
                    None => None,
                },
            },
            None => match &realtime_feed_dmfr.urls.realtime_trip_updates {
                Some(url) => Some(url.as_str().to_string()),
                None => None,
            },
        };

        let alerts_url = match realtime_passwords_hashmap.get(feed_id) {
            Some(password_format) => match &password_format.override_alerts {
                Some(url) => Some(url.to_string()),
                None => match &realtime_feed_dmfr.urls.realtime_alerts {
                    Some(url) => Some(url.as_str().to_string()),
                    None => None,
                },
            },
            None => match &realtime_feed_dmfr.urls.realtime_alerts {
                Some(url) => Some(url.as_str().to_string()),
                None => None,
            },
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
            passwords: match realtime_passwords_hashmap.get(feed_id) {
                Some(password_format) => Some(password_format.passwords.clone()),
                None => None,
            },
            fetch_interval_ms: match realtime_feeds_hashmap.get(feed_id) {
                Some(realtime_feed) => realtime_feed.fetch_interval_ms,
                None => None,
            },
        })
    }

    Ok(realtime_feed_fetches)
}
