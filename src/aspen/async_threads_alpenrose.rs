use catenary::aspen::lib::*;
use catenary::aspen_dataset::GtfsRtType;
use catenary::compact_formats::CompactFeedMessage;
use catenary::postgres_tools::CatenaryPostgresPool;
use crossbeam::deque::{Injector, Steal};

use ahash::AHashMap;
use futures::FutureExt;
use parking_lot::Mutex;
use scc::HashMap as SccHashMap;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{AcquireError, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::import_alpenrose::new_rt_data;

// This is a somewhat simple function!
// Because Aspen adds incoming new gtfs realtime data into the in-memory database, and then adds the notification to do the work
// All this does is spawn the number of worker threads initially configured by the environmental variables
// If a worker ever exits, it will spawn a new one!
// I hope you can improve on this code ! or just appreciate it !
// scroll down to read how the worker function works
// Initially written by Kyler

#[derive(Clone)]
pub struct AlpenroseWorkQueue {
    queue: Arc<Injector<ProcessAlpenroseData>>,
    available: Arc<Semaphore>,
}

impl AlpenroseWorkQueue {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Injector::new()),
            available: Arc::new(Semaphore::new(0)),
        }
    }

    /// Enqueues one item and makes exactly one unit of work available.
    ///
    /// The ordering matters: push first, then publish the permit.
    pub fn push(&self, item: ProcessAlpenroseData) {
        self.queue.push(item);
        self.available.add_permits(1);
    }

    /// Waits without consuming CPU until an item is available.
    pub async fn recv(&self) -> Result<ProcessAlpenroseData, AcquireError> {
        loop {
            // Use an owned permit so cancellation before `forget()` returns
            // the permit to the semaphore.
            let permit = self.available.clone().acquire_owned().await?;

            loop {
                match self.queue.steal() {
                    Steal::Success(item) => {
                        // Permanently consume the permit corresponding to this item.
                        permit.forget();
                        return Ok(item);
                    }

                    // Retry means Crossbeam observed a transient concurrent
                    // modification. There is known to be work because we hold
                    // a semaphore permit.
                    Steal::Retry => {
                        tokio::task::yield_now().await;
                    }

                    // This indicates a queue/permit accounting bug. Discard the
                    // invalid permit rather than entering another busy loop.
                    Steal::Empty => {
                        tracing::error!(
                            "Alpenrose queue invariant violated: \
                             acquired a permit but the Injector was empty"
                        );
                        permit.forget();
                        break;
                    }
                }
            }
        }
    }

    pub fn close(&self) {
        self.available.close();
    }
}

impl Default for AlpenroseWorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn alpenrose_process_threads(
    alpenrose_to_process_queue: AlpenroseWorkQueue,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    authoritative_trajectory_data_store: Arc<
        SccHashMap<String, catenary::aspen_dataset::AspenTrajectoryStore>,
    >,
    conn_pool: Arc<CatenaryPostgresPool>,
    alpenrosethreadcount: usize,
    chateau_queue_list: Arc<Mutex<HashMap<WorkKey, ChateauWorkState>>>,
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
        let alpenrose_to_process_queue = alpenrose_to_process_queue.clone();
        let authoritative_gtfs_rt_store = Arc::clone(&authoritative_gtfs_rt_store);
        let authoritative_data_store = Arc::clone(&authoritative_data_store);
        let authoritative_trajectory_data_store = Arc::clone(&authoritative_trajectory_data_store);
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
                    authoritative_trajectory_data_store.clone(),
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
            let alpenrose_to_process_queue = alpenrose_to_process_queue.clone();
            let authoritative_gtfs_rt_store = Arc::clone(&authoritative_gtfs_rt_store);
            let authoritative_data_store = Arc::clone(&authoritative_data_store);
            let authoritative_trajectory_data_store =
                Arc::clone(&authoritative_trajectory_data_store);
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
                        authoritative_trajectory_data_store.clone(),
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
    alpenrose_to_process_queue: AlpenroseWorkQueue,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    authoritative_trajectory_data_store: Arc<
        SccHashMap<String, catenary::aspen_dataset::AspenTrajectoryStore>,
    >,
    conn_pool: Arc<CatenaryPostgresPool>,
    chateau_queue_list: Arc<Mutex<HashMap<WorkKey, ChateauWorkState>>>,
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
        // Suspends the task when no work exists. No polling and no idle CPU.
        let new_ingest_task = match alpenrose_to_process_queue.recv().await {
            Ok(task) => task,
            Err(_) => break, // Queue closed
        };

        let feed_id = new_ingest_task.realtime_feed_id.clone();
        let chateau_id = new_ingest_task.chateau_id.clone();
        let key = chateau_id.clone();

        let processing_result = timeout(
            Duration::from_secs(300),
            AssertUnwindSafe(new_rt_data(
                Arc::clone(&authoritative_data_store),
                Arc::clone(&authoritative_trajectory_data_store),
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
            ))
            .catch_unwind(),
        )
        .await;

        let task_to_requeue = {
            let mut states = chateau_queue_list.lock();
            match states.get_mut(&key) {
                Some(state) if state.dirty => {
                    state.dirty = false;
                    Some(state.last_task.clone())
                }
                Some(_) => {
                    states.remove(&key);
                    None
                }
                None => None,
            }
        };

        if let Some(task) = task_to_requeue {
            alpenrose_to_process_queue.push(task);
        }

        match processing_result {
            Ok(Ok(Ok(_))) => {}
            Ok(Ok(Err(error))) => {
                tracing::error!(
                    feed_id = %feed_id,
                    chateau_id = %chateau_id,
                    error = ?error,
                    "Error processing realtime data"
                );
            }
            Ok(Err(payload)) => {
                let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                    Some(*s)
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    Some(s.as_str())
                } else {
                    None
                };
                tracing::error!(
                    feed_id = %feed_id,
                    chateau_id = %chateau_id,
                    panic_message = ?msg,
                    "Panic occurred while processing realtime data"
                );
            }
            Err(_elapsed) => {
                tracing::error!(
                    feed_id = %feed_id,
                    chateau_id = %chateau_id,
                    "Timeout processing realtime data"
                );
            }
        }
    }

    Ok(())
}
