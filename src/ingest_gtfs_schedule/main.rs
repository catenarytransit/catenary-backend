use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use csv::{ReaderBuilder, ErrorKind};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Agency {
    agency: String,
    url: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut file = File::open("./../../gtfs_schedules.csv")?;
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
                if err.kind() == ErrorKind::Deserialize {
                    continue;
                } else {
                    return Err(Box::new(err));
                }
            }
        };
        agencies.push(record);
    }

    println!("{:#?}", agencies);

    Ok(())
}