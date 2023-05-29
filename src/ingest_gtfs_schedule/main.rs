use csv::ReaderBuilder;
use std::error::Error;
use std::io::prelude::*;
//use serde::ser::StdError;
use serde::Deserialize;
use std::io::Cursor;

use std::fs::File;
//use std::path::Path;
use reqwest;
use tokio::task::JoinHandle;

use gtfs_structures::Error as GtfsError;

use std::io::{Read, Write};
//use std::net::TcpStream;
use std::fs::copy;

use std::collections::HashMap;

#[feature(async_await)]
use futures::future::join_all;

#[derive(Debug, Deserialize, Clone)]
struct Agency {
    agency: String,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

        for (shape_id, shape_vec) in &gtfs.shapes {
            println!("shape_id: {} has {} points", shape_id, shape_vec.len());

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

            println!("there are {} trips for shape", trip_ids.len());
            println!("the routes for shape {} are {:?}", shape_id, route_ids);
        }
    }

    Ok(())
}
