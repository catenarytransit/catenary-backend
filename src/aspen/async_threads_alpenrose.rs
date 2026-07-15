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
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::{AcquireError, Semaphore};
use tokio::task::JoinSet;

use dashmap::DashMap;

use crate::import_alpenrose::new_rt_data;

use std::sync::atomic::{AtomicUsize, Ordering};

pub static ACTIVE_STAGES: LazyLock<DashMap<String, ActiveAlpenroseStage>> =
    LazyLock::new(DashMap::new);

#[derive(Clone, Debug)]
pub struct ActiveAlpenroseStage {
    pub feed_id: String,
    pub stage: &'static str,
    pub stage_started: Instant,
    pub job_started: Instant,
}

pub fn set_stage(chateau_id: &str, feed_id: &str, stage: &'static str, job_started: Instant) {
    ACTIVE_STAGES.insert(
        chateau_id.to_string(),
        ActiveAlpenroseStage {
            feed_id: feed_id.to_string(),
            stage,
            stage_started: Instant::now(),
            job_started,
        },
    );
}

// This is a somewhat simple function!
// Because Aspen adds incoming new gtfs realtime data into the in-memory database, and then adds the notification to do the work
// All this does is spawn the number of worker threads initially configured by the environmental variables
// If a worker ever exits, it will spawn a new one!
// I hope you can improve on this code ! or just appreciate it !
// scroll down to read how the worker function works
// Initially written by Kyler

pub static ACTIVE_ALPENROSE_JOBS: AtomicUsize = AtomicUsize::new(0);

struct ActiveJobGuard;

impl ActiveJobGuard {
    fn new() -> Self {
        ACTIVE_ALPENROSE_JOBS.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        ACTIVE_ALPENROSE_JOBS.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct AlpenroseWorkQueue {
    queue: Arc<Injector<ProcessAlpenroseData>>,
    available: Arc<Semaphore>,
    queued: Arc<AtomicUsize>,
}

impl AlpenroseWorkQueue {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Injector::new()),
            available: Arc::new(Semaphore::new(0)),
            queued: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Enqueues one item and makes exactly one unit of work available.
    ///
    /// The ordering matters: push first, then publish the permit.
    pub fn push(&self, item: ProcessAlpenroseData) {
        self.queue.push(item);
        let depth = self.queued.fetch_add(1, Ordering::Release) + 1;
        self.available.add_permits(1);

        tracing::debug!(
            queue_depth = depth,
            available_permits = self.available.available_permits(),
            "Enqueued Alpenrose work"
        );
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
                        let depth = self.queued.fetch_sub(1, Ordering::AcqRel) - 1;
                        tracing::debug!(queue_depth = depth, "Dequeued Alpenrose work");
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
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
    authoritative_trajectory_data_store: Arc<
        SccHashMap<String, catenary::aspen_dataset::AspenTrajectoryStore>,
    >,
    conn_pool: Arc<CatenaryPostgresPool>,
    alpenrosethreadcount: usize,
    chateau_queue_list: Arc<Mutex<HashMap<String, ChateauWorkState>>>,
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
    let concurrency = alpenrosethreadcount.max(1);
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut running = JoinSet::new();

    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;

            for entry in ACTIVE_STAGES.iter() {
                let value = entry.value();

                if value.job_started.elapsed() > Duration::from_secs(10) {
                    tracing::warn!(
                        chateau_id = entry.key(),
                        feed_id = value.feed_id,
                        stage = value.stage,
                        stage_elapsed_ms = value.stage_started.elapsed().as_millis(),
                        job_elapsed_ms = value.job_started.elapsed().as_millis(),
                        "Long-running Alpenrose job"
                    );
                }
            }
        }
    });

    loop {
        let task = match alpenrose_to_process_queue.recv().await {
            Ok(task) => task,
            Err(_) => break,
        };

        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("Alpenrose semaphore closed");

        let alpenrose_to_process_queue = alpenrose_to_process_queue.clone();
        let authoritative_gtfs_rt_store = Arc::clone(&authoritative_gtfs_rt_store);
        let authoritative_data_store = Arc::clone(&authoritative_data_store);
        let authoritative_trajectory_data_store = Arc::clone(&authoritative_trajectory_data_store);
        let conn_pool = Arc::clone(&conn_pool);
        let chateau_queue_list = Arc::clone(&chateau_queue_list);
        let redis_client = redis_client.clone();
        let nyct_cache = Arc::clone(&authoritative_nyct_subway_data_cache);

        let permits_for_log = Arc::clone(&permits);

        running.spawn(async move {
            let permit = permit;

            let result = process_one_alpenrose_task(
                alpenrose_to_process_queue,
                task,
                authoritative_gtfs_rt_store,
                authoritative_data_store,
                authoritative_trajectory_data_store,
                conn_pool,
                chateau_queue_list,
                redis_client,
                nyct_cache,
            )
            .await;

            tracing::info!(
                available_permits_before_drop = permits_for_log.available_permits(),
                "Alpenrose processing function returned; releasing permit"
            );

            drop(permit);

            tracing::info!(
                available_permits_after_drop = permits_for_log.available_permits(),
                "Alpenrose concurrency permit released"
            );

            if let Err(error) = result {
                tracing::error!(
                    error = ?error,
                    "Alpenrose processing task returned an error"
                );
            }
        });

        while let Some(result) = running.try_join_next() {
            if let Err(error) = result {
                tracing::error!(
                    error = ?error,
                    "Alpenrose processing task panicked"
                );
            }
        }
    }

    while let Some(result) = running.join_next().await {
        if let Err(error) = result {
            tracing::error!(
                error = ?error,
                "Alpenrose processing task panicked during shutdown"
            );
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

pub async fn process_one_alpenrose_task(
    alpenrose_to_process_queue: AlpenroseWorkQueue,
    new_ingest_task: ProcessAlpenroseData,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
    authoritative_trajectory_data_store: Arc<
        SccHashMap<String, catenary::aspen_dataset::AspenTrajectoryStore>,
    >,
    conn_pool: Arc<CatenaryPostgresPool>,
    chateau_queue_list: Arc<Mutex<HashMap<String, ChateauWorkState>>>,
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
    let feed_id = new_ingest_task.realtime_feed_id.clone();
    let chateau_id = new_ingest_task.chateau_id.clone();
    let key = chateau_id.clone();

    let _active_job_guard = ActiveJobGuard::new();
    let active = ACTIVE_ALPENROSE_JOBS.load(Ordering::Acquire);

    tracing::info!(
        active_jobs = active,
        queue_depth = alpenrose_to_process_queue.queued.load(Ordering::Relaxed),
        chateau_id,
        feed_id,
        "Starting Alpenrose job"
    );

    println!(
        "Worker thread: about to trigger new_rt_data for chateau: {}, feed: {}",
        chateau_id, feed_id
    );
    let processing_result = AssertUnwindSafe(new_rt_data(
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
    .catch_unwind()
    .await;

    println!(
        "Worker thread: finished new_rt_data for chateau: {}, feed: {}, result: {:?}",
        chateau_id, feed_id, processing_result
    );

    println!(
        "Alpenrose postprocess: waiting for chateau state lock: {}",
        chateau_id
    );

    let task_to_requeue = {
        let mut states = chateau_queue_list.lock();

        println!(
            "Alpenrose postprocess: acquired chateau state lock: {}",
            chateau_id
        );

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

    println!(
        "Alpenrose postprocess: released chateau state lock: {}, requeue={}",
        chateau_id,
        task_to_requeue.is_some()
    );

    if let Some(task) = task_to_requeue {
        println!(
            "Alpenrose postprocess: requeueing dirty chateau: {}",
            chateau_id
        );

        alpenrose_to_process_queue.push(task);

        println!(
            "Alpenrose postprocess: requeued dirty chateau: {}",
            chateau_id
        );
    }

    println!(
        "Alpenrose postprocess: handling processing result: {}",
        chateau_id
    );

    match processing_result {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::error!(
                feed_id = %feed_id,
                chateau_id = %chateau_id,
                error = ?error,
                "Error processing realtime data"
            );
        }
        Err(payload) => {
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
    }

    let active_before = ACTIVE_ALPENROSE_JOBS.load(Ordering::Acquire);
    let active_after = active_before.saturating_sub(1);

    tracing::info!(
        chateau_id = %chateau_id,
        feed_id = %feed_id,
        active_before,
        active_jobs = active_after,
        queue_depth = alpenrose_to_process_queue
            .queued
            .load(Ordering::Acquire),
        "Alpenrose job fully exited"
    );

    ACTIVE_STAGES.remove(&chateau_id);

    Ok(())
}
