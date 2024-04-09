// Copyright Kyler Chin <kyler@catenarymaps.org>
// Attribution cannot be removed

// AGPL 3.0

use std::thread;
use std::time::Duration;
use uuid::Uuid;
use tokio_zookeeper::*;
use futures::prelude::*;

fn main() {
    let this_worker_id = Arc::new(Uuid::new_v4());

    // infinite runtime with worker threads and a leader-candidate thread that attempts to be the leader
    // Uses zookeeper to elect the leader and assign feeds to insert
    // Service discovery via zookeeper

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
    .await
    .unwrap();

    //spawn the thread to listen to be a leader
    thread::spawn(|| {
        //read passwords and dynamically update passwords from postgres

        let dmfr_result = read_folders("./transitland-atlas/")?;
    });

    //worker thread
    thread::spawn(|| {
        
    });
}