use csv::ReaderBuilder;
use std::error::Error;
use std::io::prelude::*;
//use serde::ser::StdError;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use std::fs::File;
//use std::path::Path;
use reqwest;
use std::ops::Deref;
use tokio::task::JoinHandle;

use gtfs_structures::Error as GtfsError;
use gtfs_structures::RouteType;

use std::io::{Read, Write};
//use std::net::TcpStream;
use std::fs::copy;

use std::collections::HashMap;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Error as PostgresError, NoTls, Row};

#[feature(async_await)]
use futures::future::join_all;

use postgis::{ewkb, LineString};

#[derive(Debug, Deserialize, Clone)]
struct Agency {
    agency: String,
    url: String,
    feed_id: String,
    operator_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let postgresstring = arguments::parse(std::env::args())
        .unwrap()
        .get::<String>("postgres");

    let postgresstring = match postgresstring {
        Some(s) => s,
        None => {
            println!("Postgres string not avaliable, using default");
            "host=localhost user=postgres".to_string()
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

    client
        .batch_execute(
            "
            CREATE EXTENSION IF NOT EXISTS postgis;

            DROP SCHEMA IF EXISTS gtfs_static CASCADE;

        CREATE SCHEMA IF NOT EXISTS gtfs_static;
        
        CREATE TABLE IF NOT EXISTS gtfs_static.agencies (
            onestop_feed_id text PRIMARY KEY,
            onestop_operator_id text,
            gtfs_agency_id text,
            name text NOT NULL,
            url text NOT NULL,
            timezone text NOT NULL,
            lang text,
            phone text,
            fare_url text,
            email text,
            max_lat double precision NOT NULL,
            max_lon double precision NOT NULL,
            min_lat double precision NOT NULL,
            min_lon double precision NOT NULL
        );

        CREATE TABLE IF NOT EXISTS gtfs_static.routes (
            route_id text NOT NULL,
            onestop_feed_id text NOT NULL,
            short_name text NOT NULL,
            long_name text NOT NULL,
            desc text,
            route_type int NOT NULL,
            url text,
            agency_id: text,
            order: int,
            color text,
            text_color text,
            continuous_pickup int,
            continuous_drop_off int,
        );

        CREATE TABLE IF NOT EXISTS gtfs_static.shapes (
            onestop_feed_id text NOT NULL,
            shape_id text NOT NULL,
            linestring GEOMETRY(LINESTRING,4326) NOT NULL,
            PRIMARY KEY (onestop_feed_id,shape_id)
        );
        
        ",
        )
        .await
        .unwrap();

    let file = File::open("./gtfs_schedules.csv");
    let mut contents = String::new();
    file.expect("read zip file failed!")
        .read_to_string(&mut contents);

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(contents.as_bytes());

    let mut agencies = Vec::new();
    for result in reader.deserialize() {
        let record: Agency = result?;
        agencies.push(record);
    }

    // Iterate over the paths.
    let mut tasks: Vec<JoinHandle<Result<(), ()>>> = vec![];

    let firstagencies = agencies.clone();

    for agency in firstagencies {
        // Copy each path into a new string
        // that can be consumed/captured by the task closure
        let path = agency.url.clone();

        // Create a Tokio task for each path
        tasks.push(tokio::spawn(async move {
            match reqwest::get(&path).await {
                Ok(resp) => {
                    match resp.bytes().await {
                        Ok(text) => {
                            println!("RESPONSE: {} KB from {}", text.len() / 1000, path);

                            //create folder if not exists
                            std::fs::create_dir_all("./gtfs_schedules")
                                .expect("create folder failed!");

                            //save to file

                            let mut file =
                                File::create(format!("./gtfs_schedules/{}.zip", agency.agency))
                                    .expect("create file failed!");

                            // Copy the response body into the file
                            let mut content = Cursor::new(text);
                            std::io::copy(&mut content, &mut file);

                            println!(
                                "save to file: {}",
                                format!("./gtfs_schedules/{}.zip", agency.agency)
                            );
                        }
                        Err(_) => eprintln!("ERROR reading {}", path),
                    }
                }
                Err(_) => eprintln!("ERROR downloading {}", path),
            }
            Ok(())
        }));
    }

    // Wait for them all to finish
    println!("Started {} tasks. Waiting...", tasks.len());
    join_all(tasks).await;

    for agency in agencies {
        println!("v2 agency: {}, url: {}", agency.agency, agency.url);

        let gtfs =
            gtfs_structures::Gtfs::from_path(format!("./gtfs_schedules/{}.zip", agency.agency))?;

        println!("Read duration read_duration: {:?}", gtfs.read_duration);

        println!("there are {} stops in the gtfs", gtfs.stops.len());

        println!("there are {} routes in the gtfs", gtfs.routes.len());

        let mut least_lat: Option<f64> = None;
        let mut least_lon: Option<f64> = None;

        let mut most_lat: Option<f64> = None;
        let mut most_lon: Option<f64> = None;

        let timestarting = std::time::Instant::now();

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

        println!(
            "Found bounding box for {}, ({},{}) and ({},{})",
            agency.agency,
            least_lat.unwrap(),
            least_lon.unwrap(),
            most_lat.unwrap(),
            most_lon.unwrap()
        );

        println!("That took {:?}", timestarting.elapsed());

        for (shape_id, shape_vec) in &gtfs.shapes {
            //println!("shape_id: {} has {} points", shape_id, shape_vec.len());

            //get the trips associated with this shape_id

            let mut trip_ids = Vec::new();
            let mut route_ids = Vec::new();

            for (trip_id, trip) in &gtfs.trips {
                let cloned_shape_id: String = shape_id.clone();

                if trip.shape_id == Some(cloned_shape_id) {
                    trip_ids.push(trip_id.clone());
                    route_ids.push(trip.route_id.clone());
                }
            }

            let cloned_shape_id_2: String = shape_id.clone();

            if gtfs.agencies[0].id == Some(String::from("Metrolink")) {
                let mut shape_to_route_pre = HashMap::new();

                let lines = ["91", "IEOC", "AV", "OC", "RIVER", "SB", "VT"];

                for line in lines.iter() {
                    let value = match *line {
                        "91" => "91 Line",
                        "IEOC" => "Inland Emp.-Orange Co. Line",
                        "AV" => "Antelope Valley Line",
                        "OC" => "Orange County Line",
                        "RIVER" => "Riverside Line",
                        "SB" => "San Bernardino Line",
                        "VT" => "Ventura County Line",
                        _ => "",
                    };
                    shape_to_route_pre.insert(line.to_string(), value.to_string());
                }

                let mut shape_to_route = HashMap::new();

                for preroute in shape_to_route_pre.iter() {
                    //for each preroute, add "{key}in" and "keyout" to the hashmap

                    let keyin = format!("{}in", preroute.0);
                    let keyout: String = format!("{}out", preroute.0);

                    shape_to_route.insert(keyin, preroute.1.clone());
                    shape_to_route.insert(keyout, preroute.1.clone());
                }

                //check if shape_id is in the hashmap

                //console hashmap

                println!("shape_to_route: {:?}", shape_to_route);

                //println!("shape_id: {}", cloned_shape_id_2.clone());

                if shape_to_route.contains_key(&format!("{}", cloned_shape_id_2.clone())) {
                    //println!("shape_id: {} has route_id: {}", shape_id, shape_to_route[shape_id]);
                    //route_ids.push(shape_to_route[shape_id].to_string().clone());
                    route_ids.push(
                        shape_to_route[cloned_shape_id_2.as_str()]
                            .to_string()
                            .clone(),
                    );
                }
            }

            route_ids.sort_unstable();
            route_ids.dedup();

            //list trips associated with this shape_id
            //let result = trip_ids.join(",");
            //println!("trip_id for shape: {}", result);

            //println!("there are {} trips for shape", trip_ids.len());
            //println!("the routes for shape {} are {:?}", shape_id, route_ids);
        }

        let _ = client
            .query(
                "INSERT INTO gtfs_static.agencies 
            (onestop_feed_id, onestop_operator_id, gtfs_agency_id, name, url, timezone, lang, phone, fare_url, email, 
                max_lat, min_lat, max_lon, min_lon)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                &[
                    &agency.feed_id,
                    &agency.operator_id,
                    &gtfs.agencies[0].id,
                    &gtfs.agencies[0].name,
                    &gtfs.agencies[0].url,
                    &gtfs.agencies[0].timezone,
                    &gtfs.agencies[0].lang,
                    &gtfs.agencies[0].phone,
                    &gtfs.agencies[0].fare_url,
                    &gtfs.agencies[0].email,
                    &(least_lat.unwrap()),
                    &(least_lon.unwrap()),
                    &(most_lat.unwrap()),
                    &(most_lon.unwrap()),
                ],
            )
            .await?;

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

            let _ = client
                .query(
                    "INSERT INTO gtfs_static.routes 
            (
                route_id,
                onestop_feed_id,
                short_name,
                long_name,
                desc,
                route_type,
                url,
                agency_id,
                order,
                color,
                text_color,
                continuous_pickup,
                continuous_drop_off,
            )
            ",
                    &[
                        &route_id,
                        &agency.feed_id,
                        &route.short_name,
                        &route.long_name,
                        &route.desc.unwrap_or_else(|| "".to_string()),
                        &(route_type_number
                            .to_sql(&tokio_postgres::types::Type::INT4, &mut BytesMut::new())),
                        &route.url,
                        &route.agency_id.unwrap_or_else(|| "".to_string()),
                        &route.order.unwrap_or_else(|| 0),
                        &(route.color.to_string()),
                        &(route.text_color.to_string()),
                        &((match route.continuous_pickup {
                            PickupDropOffType::Regular => 0,
                            PickupDropOffType::NotAvailable => 1,
                            PickupDropOffType::ArrangeByPhone => 2,
                            PickupDropOffType::CoordinateWithDriver => 3,
                            PickupDropOffType::Unknown(i) => *i,
                        })
                        .to_sql(&tokio_postgres::types::Type::INT4, &mut BytesMut::new())),
                        &((match route.continuous_drop_off {
                            PickupDropOffType::Regular => 0,
                            PickupDropOffType::NotAvailable => 1,
                            PickupDropOffType::ArrangeByPhone => 2,
                            PickupDropOffType::CoordinateWithDriver => 3,
                            PickupDropOffType::Unknown(i) => *i,
                        })
                        .to_sql(&tokio_postgres::types::Type::INT4, &mut BytesMut::new())),
                    ],
                )
                .await?;
        }
    }

    Ok(())
}
/*

fn convert_optional_f64_to_numeric(f64: Option<f64>) -> Option<Numeric> {
    match f64 {
        Some(f64) => Some(Numeric::from(f64)),
        None => None,
    }
}
 */
