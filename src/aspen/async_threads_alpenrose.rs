use catenary::aspen::lib::*;
use catenary::aspen_dataset::GtfsRtType;
use catenary::compact_formats::CompactFeedMessage;
use catenary::postgres_tools::CatenaryPostgresPool;
use crossbeam::deque::{Injector, Steal};

use scc::HashMap as SccHashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::import_alpenrose::new_rt_data;

// This is a somewhat simple function!
// Because Aspen adds incoming new gtfs realtime data into the in-memory database, and then adds the notification to do the work
// All this does is spawn the number of worker threads initially configured by the environmental variables
// If a worker ever exits, it will spawn a new one!
// I hope you can improve on this code ! or just appreciate it !
// scroll down to read how the worker function works
// Initially written by Kyler

pub async fn alpenrose_process_threads(
    alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    conn_pool: Arc<CatenaryPostgresPool>,
    alpenrosethreadcount: usize,
    chateau_queue_list: Arc<Mutex<HashSet<String>>>,
    _lease_id_for_this_worker: i64,
    redis_client: redis::Client,
    authoritative_nyct_subway_data_cache: Arc<
        tokio::sync::RwLock<
            Option<
                std::collections::HashMap<
                    String,
                    catenary::agency_specific_types::mta_subway::Trip,
                >,
            >,
        >,
    >,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut set: JoinSet<()> = JoinSet::new();

    for _i in 0..alpenrosethreadcount {
        let alpenrose_to_process_queue = Arc::clone(&alpenrose_to_process_queue);
        let authoritative_gtfs_rt_store = Arc::clone(&authoritative_gtfs_rt_store);
        let authoritative_data_store = Arc::clone(&authoritative_data_store);
        let conn_pool = Arc::clone(&conn_pool);
        let chateau_queue_list = Arc::clone(&chateau_queue_list);
        let redis_client = redis_client.clone();

        // Clone into a differently named local before moving into the task.
        let nyct_cache = Arc::clone(&authoritative_nyct_subway_data_cache);

        set.spawn(async move {
            loop {
                let result = alpenrose_loop_process_thread(
                    alpenrose_to_process_queue.clone(),
                    authoritative_gtfs_rt_store.clone(),
                    authoritative_data_store.clone(),
                    conn_pool.clone(),
                    chateau_queue_list.clone(),
                    redis_client.clone(),
                    nyct_cache.clone(),
                )
                .await;

                match result {
                    Ok(()) => break,
                    Err(e) => {
                        eprintln!("Thread crashed: {:?}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            eprintln!("Task panicked: {:?}", e);

            // Spawn a replacement only for a panicked task.
            let alpenrose_to_process_queue = Arc::clone(&alpenrose_to_process_queue);
            let authoritative_gtfs_rt_store = Arc::clone(&authoritative_gtfs_rt_store);
            let authoritative_data_store = Arc::clone(&authoritative_data_store);
            let conn_pool = Arc::clone(&conn_pool);
            let chateau_queue_list = Arc::clone(&chateau_queue_list);
            let redis_client = redis_client.clone();
            let nyct_cache = Arc::clone(&authoritative_nyct_subway_data_cache);

            set.spawn(async move {
                loop {
                    let result = alpenrose_loop_process_thread(
                        alpenrose_to_process_queue.clone(),
                        authoritative_gtfs_rt_store.clone(),
                        authoritative_data_store.clone(),
                        conn_pool.clone(),
                        chateau_queue_list.clone(),
                        redis_client.clone(),
                        nyct_cache.clone(),
                    )
                    .await;

                    match result {
                        Ok(()) => break,
                        Err(e) => {
                            eprintln!("Thread crashed: {:?}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }

    Ok(())
}

// here's the code which does tasks inside!
// it takes a task to due from the first in first out double sided queue,
// then it runs the processing function
// we then unlock that this chateau because it has finished processing, and can be removed from the queue
// we do it like this so if a worker crashes, it doesn't prevent the chateau's new data state from not being processed again due to some locks
// also written by kyler!

pub async fn alpenrose_loop_process_thread(
    alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    conn_pool: Arc<CatenaryPostgresPool>,
    chateau_queue_list: Arc<Mutex<HashSet<String>>>,
    redis_client: redis::Client,
    authoritative_nyct_subway_data_cache: Arc<
        tokio::sync::RwLock<
            Option<
                std::collections::HashMap<
                    String,
                    catenary::agency_specific_types::mta_subway::Trip,
                >,
            >,
        >,
    >,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        // println!("From-Alpenrose process thread");
        match alpenrose_to_process_queue.steal() {
            Steal::Success(new_ingest_task) => {
                let feed_id = new_ingest_task.realtime_feed_id.clone();

                let rt_processed_status = new_rt_data(
                    Arc::clone(&authoritative_data_store),
                    Arc::clone(&authoritative_gtfs_rt_store),
                    new_ingest_task.chateau_id.as_str(),
                    new_ingest_task.realtime_feed_id.as_str(),
                    new_ingest_task.has_vehicles,
                    new_ingest_task.has_trips,
                    new_ingest_task.has_alerts,
                    new_ingest_task.vehicles_response_code,
                    new_ingest_task.trips_response_code,
                    new_ingest_task.alerts_response_code,
                    Arc::clone(&conn_pool),
                    &redis_client,
                    Arc::clone(&authoritative_nyct_subway_data_cache),
                )
                .await;

                let mut chateau_queue_list = chateau_queue_list.lock().await;

                chateau_queue_list.remove(&new_ingest_task.chateau_id.clone());

                drop(chateau_queue_list);

                if let Err(e) = &rt_processed_status {
                    eprintln!("Error processing RT data: {} {:?}", feed_id, e);
                }

                tokio::task::yield_now().await;
            }
            _ => {
                tokio::task::yield_now().await;
            }
        }
    }

    Ok(())
}
