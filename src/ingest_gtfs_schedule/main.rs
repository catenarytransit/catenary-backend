use futures::StreamExt;
use serde_json::Error as SerdeError;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
mod dmfr;
use futures;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::Error as GtfsError;
use gtfs_structures::PickupDropOffType;
use clap::Parser;
use gtfs_structures::RouteType;
use postgis::{ewkb, LineString};
use std::error::Error;
use std::fs::File;
use rgb::RGB;
use std::io::copy;
use std::io::Write;
use std::ops::Deref;
use tokio_postgres::Client;
use tokio_postgres::{Error as PostgresError, NoTls};
use bb8_postgres::{PostgresConnectionManager};


extern crate fs_extra;
use fs_extra::dir::get_size;

pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    postgres: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args = Args::parse();

    let postgresstring = arguments::parse(std::env::args())
        .unwrap()
        .get::<String>("postgres");

    let postgresstring = match postgresstring {
        Some(s) => s,
        None => {
            panic!("You need a postgres string");
        }
    };

    
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(&postgresstring, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let _ = client
        .batch_execute(
            "
        CREATE EXTENSION IF NOT EXISTS postgis;

        DROP SCHEMA IF EXISTS gtfs CASCADE;

    CREATE SCHEMA IF NOT EXISTS gtfs;
    
    CREATE TABLE IF NOT EXISTS gtfs.static_feeds (
        onestop_feed_id text PRIMARY KEY,
        onestop_operator_id text,
        gtfs_agency_id text,
        name text ,
        url text ,
        timezone text,
        lang text,
        phone text,
        fare_url text,
        email text,
        only_realtime_ref text,
        operators text[],
        max_lat double precision NOT NULL,
        max_lon double precision NOT NULL,
        min_lat double precision NOT NULL,
        min_lon double precision NOT NULL
    );

    CREATE TABLE IF NOT EXISTS gtfs.realtime_feeds (
        onestop_feed_id text PRIMARY KEY,
        name text
    );

    CREATE TABLE IF NOT EXISTS gtfs.routes (
        route_id text NOT NULL,
        onestop_feed_id text NOT NULL,
        short_name text NOT NULL,
        long_name text NOT NULL,
        gtfs_desc text,
        route_type int NOT NULL,
        url text,
        agency_id text,
        gtfs_order int,
        color text,
        text_color text,
        continuous_pickup int,
        continuous_drop_off int,
        shapes_list text[],
        PRIMARY KEY (onestop_feed_id, route_id)
    );

    CREATE TABLE IF NOT EXISTS gtfs.shapes (
        onestop_feed_id text NOT NULL,
        shape_id text NOT NULL,
        linestring GEOMETRY(LINESTRING,4326) NOT NULL,
        color text,
        routes text[],
        PRIMARY KEY (onestop_feed_id,shape_id)
    );

    CREATE TABLE IF NOT EXISTS gtfs.trips (
        trip_id text NOT NULL,
        onestop_feed_id text NOT NULL,
        route_id text NOT NULL,
        service_id text NOT NULL,
        trip_headsign text,
        trip_short_name text,
        direction_id int,
        block_id text,
        shape_id text,
        wheelchair_accessible int,
        bikes_allowed int,
        PRIMARY KEY (onestop_feed_id, trip_id)
    );

    CREATE INDEX IF NOT EXISTS gtfs_static_geom_idx ON gtfs.shapes USING GIST (linestring);

    CREATE INDEX IF NOT EXISTS gtfs_static_feed_id ON gtfs.shapes (onestop_feed_id);

    CREATE INDEX IF NOT EXISTS gtfs_static_feed ON gtfs.routes (onestop_feed_id);
    
    ",
        ).await;

    println!("Finished making database");

    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {
        let mut feedhashmap: BTreeMap<String, dmfr::Feed> = BTreeMap::new();

        let mut operatorhashmap: BTreeMap<String, dmfr::Operator> = BTreeMap::new();

        let mut operator_to_feed_hashmap: BTreeMap<String, Vec<dmfr::OperatorAssociatedFeedsItem>> =
            BTreeMap::new();

        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(file_name) = entry.file_name().to_str() {
                    println!("{}", file_name);

                    let contents =
                        fs::read_to_string(format!("transitland-atlas/feeds/{}", file_name));

                    match contents {
                        Ok(contents) => {
                            let dmfrinfo: Result<
                                dmfr::DistributedMobilityFeedRegistry,
                                SerdeError,
                            > = serde_json::from_str(&contents);

                            match dmfrinfo {
                                Ok(dmfrinfo) => {
                  dmfrinfo.feeds.iter().for_each(|feed| {
                                        //println!("Feed {}: {:#?}", feed.id.clone(), feed);

                                        if !feedhashmap.contains_key(&feed.id) {
                                            //feedhashmap.insert(feed.id.clone(), feed.clone());
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        } 

                                        feed.operators.iter().for_each(|operator| {
                                            operatorhashmap.insert(
                                                operator.onestop_id.clone(),
                                                operator.clone(),
                                            );

                                            if operator_to_feed_hashmap
                                                .contains_key(&operator.onestop_id)
                                            {
                                                //combine the feeds for this operator together
                                                let mut existing_associated_feeds =
                                                    operator_to_feed_hashmap
                                                        .get(&operator.onestop_id)
                                                        .unwrap()
                                                        .clone();

                                                let existing_feed_ids = operator_to_feed_hashmap
                                                    .get(&operator.onestop_id)
                                                    .unwrap()
                                                    .iter()
                                                    .map(|associated_feed| {
                                                        associated_feed
                                                            .feed_onestop_id
                                                            .clone()
                                                            .unwrap()
                                                    })
                                                    .collect::<Vec<String>>();

                                                operator.associated_feeds.iter().for_each(
                                                    |associated_feed| {
                                                        if !existing_feed_ids.contains(
                                                            &associated_feed
                                                                .feed_onestop_id
                                                                .clone()
                                                                .unwrap_or_else(|| feed.id.clone()),
                                                        ) {
                                                            existing_associated_feeds
                                                                .push(associated_feed.clone());
                                                        }
                                                    },
                                                );

                                                operator_to_feed_hashmap.insert(
                                                    operator.onestop_id.clone(),
                                                    existing_associated_feeds,
                                                );
                                            } else {
                                                operator_to_feed_hashmap.insert(
                                                    operator.onestop_id.clone(),
                                                    operator.associated_feeds.clone(),
                                                );
                                            }
                                        });
                                    });

                                    dmfrinfo.operators.iter().for_each(|operator| {
                                        operatorhashmap
                                            .insert(operator.onestop_id.clone(), operator.clone());

                                        println!(
                                            "Operator {}: {:?}",
                                            operator.onestop_id.clone(),
                                            operator.associated_feeds
                                        );

                                        if operator_to_feed_hashmap
                                            .contains_key(&operator.onestop_id)
                                        {
                                            //combine the feeds for this operator together
                                            let mut existing_associated_feeds =
                                                operator_to_feed_hashmap
                                                    .get(&operator.onestop_id)
                                                    .unwrap()
                                                    .clone();

                                            let existing_feed_ids = operator_to_feed_hashmap
                                                .get(&operator.onestop_id)
                                                .unwrap()
                                                .iter()
                                                .filter(|associated_feed| {
                                                    associated_feed.feed_onestop_id.is_some()
                                                })
                                                .map(|associated_feed| {
                                                    associated_feed.feed_onestop_id.clone().unwrap()
                                                })
                                                .collect::<Vec<String>>();

                                            operator.associated_feeds.iter().for_each(
                                                |associated_feed| {
                                                    if !existing_feed_ids.contains(
                                                        &associated_feed
                                                            .feed_onestop_id
                                                            .clone()
                                                            .unwrap(),
                                                    ) {
                                                        existing_associated_feeds
                                                            .push(associated_feed.clone());
                                                    }
                                                },
                                            );

                                            operator_to_feed_hashmap.insert(
                                                operator.onestop_id.clone(),
                                                existing_associated_feeds,
                                            );
                                        } else {
                                            operator_to_feed_hashmap.insert(
                                                operator.onestop_id.clone(),
                                                operator.associated_feeds.clone(),
                                            );
                                        }
                                    });
                                }
                                Err(e) => {}
                            }
                        }
                        Err(e) => {}
                    }
                }


               
            }
        }
        
        
        let manager = PostgresConnectionManager::new(
            postgresstring.parse().unwrap(),
            NoTls,
        );

        let pool = bb8::Pool::builder().build(manager).await.unwrap();


    }

    Ok(())
}
