use std::error::Error;
use std::io::prelude::*;
use csv::{ReaderBuilder, ErrorKind};
use serde::Deserialize;

use std::fs::File;
use std::io::copy;
use reqwest;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct Agency {
    agency: String,
    url: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut file = File::open("./gtfs_schedules.csv")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

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

    //sprintln!("{:#?}", agencies);

    for agency in agencies {
        println!("{}: {}", agency.agency, agency.url);

        //make dir called temp
        //download gtfs into temp directory

        let file_path = Path::new("temp").join(agency.agency + "-gtfs.zip");
        let mut file = File::create(file_path)?;

        let mut response = reqwest::get(agency.url);
        match response {
            Ok(mut response) => {
                match copy(&mut response, &mut file) {
                    Ok(_) => {
                        println!("Downloaded {} gtfs", agency.agency);
                        copy(&mut response, &mut file)?;
                    },
                    Err(err) => println!("Error downloading {}: {}", agency.agency, err),
                }
            },
            Err(err) => println!("Error downloading {}: {}", agency.agency, err),
        };
        
    }

    Ok(())
}