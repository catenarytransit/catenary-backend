// Copyright Kyler Chin <kyler@catenarymaps.org>
// Attribution cannot be removed

// AGPL 3.0
use std::thread;
use std::time::Duration;
use uuid::Uuid;
use tokio_zookeeper::*;
use futures::prelude::*;
use dmfr_folder_reader::read_folders;
use std::sync::Arc;
use std::error::Error;

#[tokio::main]
async fn main() {
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
    .await
    .unwrap();

    loop {
        //Get data from postgres
        let feeds = get_feed_metadata();
    }
}

fn get_feed_metadata() -> Result<(), Box<dyn Error + Sync + Send>> {
    //Get feed metadata from postgres
    let dmfr_result = read_folders("./transitland-atlas/")?;

    Ok(())
}