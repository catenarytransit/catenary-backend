use catenary::aspen::lib::*;
use catenary::aspen_dataset::GtfsRtType;
use catenary::postgres_tools::CatenaryPostgresPool;
use crossbeam::deque::{Injector, Steal};
use gtfs_rt::FeedMessage;
use scc::HashMap as SccHashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::import_alpenrose::new_rt_data;

pub async fn alpenrose_process_threads(
    alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    conn_pool: Arc<CatenaryPostgresPool>,
    alpenrosethreadcount: usize,
) {
   // let mut handler_vec: Vec<thread::JoinHandle<_>> = vec![];

    for i in 0..alpenrosethreadcount {
        //handler_vec.push();
        tokio::task::spawn({
            let alpenrose_to_process_queue = Arc::clone(&alpenrose_to_process_queue);
            let authoritative_gtfs_rt_store = Arc::clone(&authoritative_gtfs_rt_store);
            let authoritative_data_store = Arc::clone(&authoritative_data_store);
            let conn_pool = Arc::clone(&conn_pool);
            move || async move {
                println!("Starting alpenrose task queue thread {}", i);
                alpenrose_loop_process_thread(
                    alpenrose_to_process_queue,
                    authoritative_gtfs_rt_store,
                    authoritative_data_store,
                    conn_pool,
                )
                .await
            }
        }());
    }

    //for handle in handler_vec.into_iter() {
        //handle.join().unwrap().await;
  //  }
}

pub async fn alpenrose_loop_process_thread(
    alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    conn_pool: Arc<CatenaryPostgresPool>,
) {
    loop {
        println!("From-Alpenrose process thread");
        if let Steal::Success(new_ingest_task) = alpenrose_to_process_queue.steal() {
            new_rt_data(
                Arc::clone(&authoritative_data_store),
                Arc::clone(&authoritative_gtfs_rt_store),
                new_ingest_task.chateau_id,
                new_ingest_task.realtime_feed_id,
                new_ingest_task.vehicles,
                new_ingest_task.trips,
                new_ingest_task.alerts,
                new_ingest_task.has_vehicles,
                new_ingest_task.has_trips,
                new_ingest_task.has_alerts,
                new_ingest_task.vehicles_response_code,
                new_ingest_task.trips_response_code,
                new_ingest_task.alerts_response_code,
                Arc::clone(&conn_pool),
            )
            .await;
        } else {
            thread::sleep(Duration::from_millis(1))
        }
    }
}
