// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// AGPL 3.0

// https://en.wikipedia.org/wiki/Rhododendron_ferrugineum
use catenary::agency_secret::*;
use diesel_async::pooled_connection::bb8::PooledConnection;
use dmfr_folder_reader::read_folders;
use futures::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio_zookeeper::*;
use uuid::Uuid;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use catenary::postgres_tools::CatenaryConn;

// gtfs unix timestamps
struct LastDataFetched {
    realtime_vehicle_positions: Option<u64>,
    realtime_trip_updates: Option<u64>,
    realtime_alerts: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

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
        //Get data from postgres
        let feeds = get_feed_metadata(Arc::clone(&arc_conn_pool));
    }
}

#[derive(Serialize, Clone, Deserialize, Debug)]
struct RealtimeFeedFetch {
    feed_id: String,
    realtime_vehicle_positions: Option<String>,
    realtime_trip_updates: Option<String>,
    realtime_alerts: Option<String>,
    key_formats: Vec<KeyFormat>,
    passwords: Vec<PasswordInfo>,
}

fn get_feed_metadata(pool: Arc<CatenaryPostgresPool>) -> Result<(), Box<dyn Error + Sync + Send>> {
    //Get feed metadata from postgres
    let dmfr_result = read_folders("./transitland-atlas/")?;

    //get everything out of realtime feeds table and realtime password tables

    

    Ok(())
}
