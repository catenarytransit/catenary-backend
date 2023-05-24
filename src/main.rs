use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use protobuf::{CodedInputStream, Message};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use rouille::{Request, Response};

#[derive(Debug)]
struct Agency {
    url: String,
    auth: Option<String>,
}

#[derive(Debug, Default)]
struct VehiclePositions {
    positions: HashMap<String, Vec<u8>>,
}

impl VehiclePositions {
    fn insert(&mut self, agency: &str, position: Vec<u8>) {
        self.positions.insert(agency.to_string(), position);
    }

    fn get(&self, agency: &str) -> Option<&Vec<u8>> {
        self.positions.get(agency)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the CSV file and parse the agencies
    let mut agencies = Vec::new();
    let mut reader = csv::Reader::from_path("rtvehiclepositions.csv")?;
    for result in reader.deserialize() {
        let record: HashMap<String, String> = result?;
        let url = record.get("url").ok_or("Missing URL")?.to_string();
        let auth = record.get("authentication").map(|s| s.to_string());
        agencies.push(Agency { url, auth });
    }

    // Create a shared state for the vehicle positions
    let vehicle_positions = Arc::new(Mutex::new(VehiclePositions::default()));

    // Spawn a thread to fetch the vehicle positions every 10 seconds
    let vehicle_positions_clone = vehicle_positions.clone();
    thread::spawn(move || loop {
        for agency in &agencies {
            let mut headers = HeaderMap::new();
            if let Some(auth) = &agency.auth {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(auth).expect("Invalid authorization header"),
                );
            }
            let response = reqwest::blocking::get(&agency.url)
                .and_then(|r| r.error_for_status())
                .and_then(|r| r.bytes())
                .expect("Failed to fetch GTFS-RT data");
            let mut feed = protobuf::rt::read_length_delimited_from_bytes(&response)
                .expect("Failed to decode GTFS-RT data");
            let mut vehicle_positions = vehicle_positions_clone.lock().unwrap();
            vehicle_positions.insert(&agency.url, feed.write_to_bytes().unwrap());
        }
        thread::sleep(Duration::from_secs(10));
    });

    // Start a web server to serve the vehicle positions
    rouille::start_server("localhost:8000", move |request| {
        let vehicle_positions = vehicle_positions.lock().unwrap();
        if let Some(agency) = request.url().split('/').nth(2) {
            if let Some(position) = vehicle_positions.get(agency) {
                return Response::from_data("application/octet-stream", position.clone());
            }
        }
        Response::empty_404()
    });

    Ok(())
}