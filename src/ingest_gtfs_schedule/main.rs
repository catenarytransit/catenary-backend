use futures::StreamExt;
use serde_json::Error as SerdeError;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
mod dmfr;
use bb8_postgres::PostgresConnectionManager;
use clap::Parser;
use futures;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::Error as GtfsError;
use gtfs_structures::PickupDropOffType;
use gtfs_structures::RouteType;
use postgis::{ewkb, LineString};
use rgb::RGB;
use std::error::Error;
use std::fs::File;
use std::io::copy;
use std::io::Write;
use std::ops::Deref;
use tokio_postgres::Client;
use tokio_postgres::{Error as PostgresError, NoTls};

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
        )
        .await;

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

        let manager = PostgresConnectionManager::new(postgresstring.parse().unwrap(), NoTls);

        let pool = bb8::Pool::builder().build(manager).await.unwrap();

        futures::stream::iter(feedhashmap.clone().iter().into_iter().map(|(key, feed)| {
            let pool = pool.clone();
        async move {
            
            let mut client = pool.get().await.unwrap();

            println!("Feed in future {}: {:#?}", key, feed);

            let mut dothetask = true;

           if key.contains("~jp") || key.contains("germany~urban~transport") || key.contains("~gov~uk") {
               dothetask = false;
           }

           if dothetask {
            match feed.spec {
                dmfr::FeedSpec::Gtfs => {
                    //println!("{:?}", feed.urls);

                    if feed.urls.static_current.is_some() {
                        //check if folder exists in the directory

                        //process and upload routes, stops, headways, and shapes etc into postgres

                        //calculate the bounds of the feed,

                        //upload the feed id metadata

                        let file_path = format!("gtfs_uncompressed/{}/", key);


                        if path_exists(&file_path) {

                            //feed exists

                            println!("Starting read for {}", &key);

                            
                        let folder_size = get_size(&file_path).unwrap();
                        println!("size: {} kB", folder_size / 1000); 

                            let gtfs = gtfs_structures::GtfsReader::default()
                                .read_from_path(&file_path);

                            if gtfs.is_ok() {
                                let gtfs = gtfs.unwrap();

                                println!("read_duration: {:?}ms", gtfs.read_duration);

                                println!(
                                    "there are {} stops in the gtfs",
                                    gtfs.stops.len()
                                );

                                println!(
                                    "there are {} routes in the gtfs",
                                    gtfs.routes.len()
                                );

                                let mut least_lat: Option<f64> = None;
                                let mut least_lon: Option<f64> = None;

                                let mut most_lat: Option<f64> = None;
                                let mut most_lon: Option<f64> = None;

                                let timestarting = std::time::Instant::now();

                                let mut shapes_per_route: HashMap<String, Vec<String>> =
                                    HashMap::new();

                                for (stop_id, stop) in &gtfs.stops {
                                    //check if least_lat has a value

                                    if (*stop).deref().longitude.is_some() {
                                        let stop_lon = (*stop).deref().longitude.unwrap();

                                        if least_lon.is_some() {
                                            if stop_lon < least_lon.unwrap() {
                                                least_lon = Some(stop_lon);
                                            }
                                        } else {
                                            least_lon = Some(stop_lon);
                                        }

                                        if most_lon.is_some() {
                                            if stop_lon > most_lon.unwrap() {
                                                most_lon = Some(stop_lon);
                                            }
                                        } else {
                                            most_lon = Some(stop_lon);
                                        }
                                    }

                                    if (*stop).deref().latitude.is_some() {
                                        let stop_lat = (*stop).deref().latitude.unwrap();

                                        if least_lat.is_some() {
                                            if stop_lat < least_lat.unwrap() {
                                                least_lat = Some(stop_lat);
                                            }
                                        } else {
                                            least_lat = Some(stop_lat);
                                        }

                                        if most_lat.is_some() {
                                            if stop_lat > most_lat.unwrap() {
                                                most_lat = Some(stop_lat);
                                            }
                                        } else {
                                            most_lat = Some(stop_lat);
                                        }
                                    }
                                }

                                let mut shape_to_color_lookup: BTreeMap<String, RGB<u8>> = BTreeMap::new();

                                for (trip_id, trip) in &gtfs.trips {
                                    if trip.shape_id.is_some() {
                                        if !shape_to_color_lookup
                                            .contains_key(&trip.shape_id.as_ref().unwrap().clone())
                                        {
                                            if gtfs.routes.contains_key(&trip.route_id) {
                                                let route = gtfs
                                                    .routes
                                                    .get(&trip.route_id)
                                                    .unwrap();

                                                let color = route.color.clone();

                                                shape_to_color_lookup.insert(
                                                trip.shape_id.as_ref().unwrap().clone(),
                                                    color,
                                                );
                                            }
                                        }
                                    }
                                }

                                for (shape_id, shape) in &gtfs.shapes {
                                    let linestring = ewkb::LineStringT {
                                        srid: Some(4326),
                                        points: shape
                                            .iter()
                                            .map(|point| ewkb::Point {
                                                x: point.longitude,
                                                y: point.latitude,
                                                srid: Some(4326),
                                            })
                                            .collect(),
                                    };
                                    /*
                                      CREATE TABLE IF NOT EXISTS gtfs.shapes (
                                            onestop_feed_id text NOT NULL,
                                            shape_id text NOT NULL,
                                            linestring GEOMETRY(LINESTRING,4326) NOT NULL,
                                            color text,
                                            PRIMARY KEY (onestop_feed_id,shape_id)
                                        );
                                    */

                                    let color_to_upload =
                                        match shape_to_color_lookup.get(shape_id) {
                                            Some(color) => format!(
                                                "{:02x}{:02x}{:02x}",
                                                color.r, color.g, color.b
                                            ),
                                            None => String::from("3a3a3a"),
                                        };

                                        println!("uploading shape {:?} {:?}", &feed.id, &shape_id);

                                    let _ = client.query("INSERT INTO gtfs.shapes (onestop_feed_id, shape_id, linestring, color) VALUES ($1, $2, $3, $4)",
                                 &[
                                    &feed.id,
                                    &shape_id, 
                                 &linestring,
                                 &color_to_upload
                                 ]).await;
                                }

                                for (route_id, route) in &gtfs.routes {
                                    let route_type_number = match &route.route_type {
                                        RouteType::Tramway => 0,
                                        RouteType::Subway => 1,
                                        RouteType::Rail => 2,
                                        RouteType::Bus => 3,
                                        RouteType::Ferry => 4,
                                        RouteType::CableCar => 5,
                                        RouteType::Gondola => 6,
                                        RouteType::Funicular => 7,
                                        RouteType::Coach => 200,
                                        RouteType::Air => 1100,
                                        RouteType::Taxi => 1500,
                                        RouteType::Other(i) => *i,
                                    };

                                    let mut shape_id_array: Vec<String> =
                                        match shapes_per_route.get(route_id) {
                                            Some(shape_list) => shape_list.clone(),
                                            None => vec![],
                                        };

                                    shape_id_array.dedup();

                                    let shape_id_array = shape_id_array;

                                        println!("uploading route {:?} {}", &feed.id , &route_id);

                                    let _ = client
                                    .query(
                                        "INSERT INTO gtfs.routes
                                (
                                    route_id,
                                    onestop_feed_id,
                                    short_name,
                                    long_name,
                                    gtfs_desc,
                                    route_type,
                                    url,
                                    agency_id,
                                    gtfs_order,
                                    color,
                                    text_color,
                                    continuous_pickup,
                                    continuous_drop_off,
                                    shapes_list
                                )
                                VALUES (
                                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                                )
                                ",
                                        &[
                                            &route_id,
                                            &feed.id,
                                            &route.short_name,
                                            &route.long_name,
                                            &route.desc.clone().unwrap_or_else(|| "".to_string()),
                                            &route_type_number,
                                            &route.url,
                                            &route.agency_id.clone().unwrap_or_else(|| "".to_string()),
                                            &i32::try_from(route.order.unwrap_or_else(|| 0)).ok(),
                                            &(route.color.to_string()),
                                            &(route.text_color.to_string()),
                                            &(match route.continuous_pickup {
                                                ContinuousPickupDropOff::Continuous => 0,
                                                ContinuousPickupDropOff::NotAvailable => 1,
                                                ContinuousPickupDropOff::ArrangeByPhone => 2,
                                                ContinuousPickupDropOff::CoordinateWithDriver => 3,
                                                ContinuousPickupDropOff::Unknown(i) => i,
                                            }),
                                            &(match route.continuous_drop_off {
                                                ContinuousPickupDropOff::Continuous => 0,
                                                ContinuousPickupDropOff::NotAvailable => 1,
                                                ContinuousPickupDropOff::ArrangeByPhone => 2,
                                                ContinuousPickupDropOff::CoordinateWithDriver => 3,
                                                ContinuousPickupDropOff::Unknown(i) => i,
                                            }),
                                            &shape_id_array,
                                        ],
                                    );
                                }

                                println!("Uploading {} trips", gtfs.trips.len());
                            
                                let trips_insertion_multithread = futures::stream::iter(gtfs.trips.clone().into_iter().map(|(trip_id, trip)| {
                                    
                                    let pool = pool.clone();

                                    let feed_id = feed.id.clone();

                                    async move {

                                    let mut client = pool.get().await.unwrap();

                                   

                                        let insert_trips = client
                                            .query(
                                                "INSERT INTO gtfs.trips (onestop_feed_id, trip_id, service_id, route_id, trip_headsign, trip_short_name) VALUES ($1, $2, $3, $4, $5, $6);",
                                                &[
                                                    &feed_id,
                                                       &trip_id,
                                                     &trip.service_id,
                                 &trip.route_id,
                                      &trip.trip_headsign.unwrap_or_else(|| "".to_string()),
                                              &trip.trip_short_name.unwrap_or_else(|| "".to_string()),
                                                   ],
                                            ).await; 

                                        match insert_trips {
                                            Ok(_) => {},
                                            Err(e) => {
                                                println!("Error inserting trip {} {}: {:?}", &feed_id, &trip_id, e);
                                            }
                                        }
                                    }
                                }))
                                .buffer_unordered(10)
                                .collect::<Vec<()>>();

                                //okay finally upload the feed metadata

                                /*
                                
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
        max_lat double precision NOT NULL,
        max_lon double precision NOT NULL,
        min_lat double precision NOT NULL,
        min_lon double precision NOT NULL
         */

                                if gtfs.routes.len() > 0 as usize {
                                    let _ = client.query("INSERT INTO gtfs.static_feeds (onestop_feed_id,max_lat, max_lon, min_lat, min_lon) VALUES ($1, $2, $3, $4, $5)", &[
                                    &feed.id,
                                    &least_lat,
                                    &least_lon,
                                    &least_lat,
                                    &least_lon
                                ]).await;
                                }
                            }
                        }
                    }
                }
                _ => {
                    //do nothing
                    println!("skipping {}, does not match dmfr feed spec", &key);
                }
            }
        }
        }
        })).buffer_unordered(8).collect::<Vec<()>>().await;
    }

    Ok(())
}
