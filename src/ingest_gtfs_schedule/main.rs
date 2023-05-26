use std::error::Error;
use std::io::prelude::*;
use csv::{ReaderBuilder};
use serde::ser::StdError;
use serde::Deserialize;

use std::fs::File;
use std::path::Path;
use reqwest;
use tokio::task::JoinHandle;

use std::io::{Read, Write};
use std::net::TcpStream;

#[feature(async_await)]
use futures::future::{join_all};

#[derive(Debug, Deserialize)]
struct Agency {
    agency: String,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<csv::Error>> {
    let mut file = File::open("./gtfs_schedules.csv");
    let mut contents = String::new();
    file.expect("read zip file failed!").read_to_string(&mut contents);

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(contents.as_bytes());

    let mut agencies = Vec::new();
    for result in reader.deserialize() {
        let record: Agency = match result {
            Ok(record) => record,
            Err(err) => {
                    return Err(Box::new(err));
            }
        };
        agencies.push(record);
    }

    // Iterate over the paths.
    let mut tasks: Vec<JoinHandle<Result<(), ()>>>= vec![];

    for agency in agencies {
        // Copy each path into a new string
    // that can be consumed/captured by the task closure
    let path = agency.url.clone();

    // Create a Tokio task for each path
    tasks.push(tokio::spawn(async move {
        match reqwest::get(&path).await {
            Ok(resp) => {
                match resp.text().await {
                    Ok(text) => {
                        println!("RESPONSE: {} KB from {}", text.len()/1000, path);

                        let gtfs = gtfs_structures::Gtfs::new(&path)?;
                        println!("there are {} stops in the gtfs", gtfs.stops.len());
                    }
                    Err(_) => println!("ERROR reading {}", path),
                }
            }
            Err(_) => println!("ERROR downloading {}", path),
        }
        Ok(())
    }));
    }

    // Wait for them all to finish
    println!("Started {} tasks. Waiting...", tasks.len());
    join_all(tasks).await;

    Ok(())
}