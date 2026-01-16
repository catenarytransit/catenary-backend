// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};

#[derive(Serialize, Deserialize)]
struct RawAgency {
    agency_id: String,
    agency_name: String,
    agency_url: String,
    agency_timezone: String,
    agency_lang: Option<String>,
    agency_phone: Option<String>,
    agency_fare_url: Option<String>,
    agency_email: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct RawRoute {
    pub route_id: String,
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub route_desc: Option<String>,
    pub route_route_type: u8,
    pub route_url: Option<String>,
    pub agency_id: Option<String>,
    pub route_sort_order: Option<u32>,
    pub route_color: Option<String>,
    pub route_text_color: Option<String>,
    pub continuous_pickup: Option<u8>,
    pub continuous_drop_off: Option<u8>,
}

#[derive(Serialize, Deserialize)]
struct RawTrip {
    pub trip_id: String,
    pub service_id: String,
    pub route_id: String,
    pub shape_id: Option<String>,
    pub trip_headsign: Option<String>,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<u8>,
    pub block_id: Option<String>,
    pub wheelchair_accessible: Option<u8>,
    pub bikes_allowed: Option<u8>,
}

#[derive(Serialize, Deserialize)]
struct RawStopTime {
    pub trip_id: String,
    pub arrival_time: Option<String>,
    pub departure_time: Option<String>,
    pub stop_id: String,
    pub stop_sequence: u32,
    pub stop_headsign: Option<String>,
    pub pickup_type: Option<u8>,
    pub drop_off_type: Option<u8>,
}

/// Remove trips and stop_times for banned agencies from raw GTFS files.
/// This operates directly on the CSV files in the GTFS folder.
pub fn remove_banned_agencies(
    folder_path: &str,
    banned_agencies: &[&str],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Removing banned agencies from {}", folder_path);

    // Read agency.txt as csv
    let agency_file_path = format!("{}/agency.txt", folder_path);
    let agency_file = File::open(&agency_file_path)
        .map_err(|e| format!("Unable to open file: {} - {}", agency_file_path, e))?;

    let mut rdr = csv::Reader::from_reader(agency_file);

    let mut agencies_to_keep: Vec<RawAgency> = Vec::new();
    let mut banned_agency_ids: BTreeSet<String> = BTreeSet::new();

    for result in rdr.deserialize() {
        if let Ok(record) = result {
            let record: RawAgency = record;
            if banned_agencies.contains(&record.agency_name.as_str()) {
                banned_agency_ids.insert(record.agency_id.clone());
            } else {
                agencies_to_keep.push(record);
            }
        }
    }

    println!("Found {} agencies to remove", banned_agency_ids.len());
    println!("Banned Agency IDs: {:?}", banned_agency_ids);

    // Write agencies back to file
    let agency_new_path = format!("{}/agency_cleaned.txt", folder_path);
    let mut writer = csv::Writer::from_path(&agency_new_path)
        .map_err(|e| format!("Unable to create file: {}", e))?;

    for agency in agencies_to_keep {
        writer
            .serialize(agency)
            .map_err(|e| format!("Unable to write to file: {}", e))?;
    }
    writer
        .flush()
        .map_err(|e| format!("Unable to flush file: {}", e))?;
    fs::rename(&agency_new_path, &agency_file_path)?;

    let mut route_ids_to_remove: BTreeSet<String> = BTreeSet::new();

    // Read routes file
    let routes_file_path = format!("{}/routes.txt", folder_path);
    let routes_file = File::open(&routes_file_path)
        .map_err(|e| format!("Unable to open file: {} - {}", routes_file_path, e))?;
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(routes_file);

    let headers = rdr.headers()?.clone();
    let agency_id_idx = headers.iter().position(|h| h == "agency_id");
    let route_id_idx = headers
        .iter()
        .position(|h| h == "route_id")
        .ok_or("routes.txt missing route_id column")?;

    let mut routes = Vec::new();

    for result in rdr.records() {
        match result {
            Ok(record) => {
                let route_id = record.get(route_id_idx).unwrap_or("").to_string();
                let agency_id = if let Some(idx) = agency_id_idx {
                    record.get(idx).unwrap_or("")
                } else {
                    ""
                };

                // Check if agency is in banned agencies
                if banned_agency_ids.contains(agency_id) {
                    println!(
                        "Removing route {} due to banned agency {}",
                        route_id, agency_id
                    );
                    route_ids_to_remove.insert(route_id);
                } else {
                    routes.push(record);
                }
            }
            Err(e) => {
                eprintln!("Error reading route record: {}", e);
            }
        }
    }

    // Write routes back to file
    let routes_new_path = format!("{}/routes_cleaned.txt", folder_path);
    let mut writer = csv::Writer::from_path(&routes_new_path)
        .map_err(|e| format!("Unable to create file: {}", e))?;

    // Write header
    writer
        .write_record(&headers)
        .map_err(|e| format!("Unable to write header: {}", e))?;

    for route in routes {
        writer
            .write_record(&route)
            .map_err(|e| format!("Unable to write to file: {}", e))?;
    }
    writer
        .flush()
        .map_err(|e| format!("Unable to flush file: {}", e))?;
    fs::rename(&routes_new_path, &routes_file_path)?;

    println!("Found {} routes to remove", route_ids_to_remove.len());

    if route_ids_to_remove.is_empty() {
        println!("No routes to remove, skipping");
        return Ok(());
    }

    println!("Fixing trips");

    let mut trip_ids_to_remove = BTreeSet::new();

    // Read trips file
    let trips_file_path = format!("{}/trips.txt", folder_path);
    let trips_file = File::open(&trips_file_path)
        .map_err(|e| format!("Unable to open file: {} - {}", trips_file_path, e))?;

    let mut rdr = csv::Reader::from_reader(trips_file);

    let mut trips = Vec::new();

    for result in rdr.deserialize() {
        if let Ok(record) = result {
            let record: RawTrip = record;

            // Check if route is in banned routes
            if route_ids_to_remove.contains(&record.route_id) {
                trip_ids_to_remove.insert(record.trip_id.clone());
            } else {
                trips.push(record);
            }
        }
    }

    println!("Found {} trips to remove", trip_ids_to_remove.len());

    // Write trips back to file
    let trips_new_path = format!("{}/trips_cleaned.txt", folder_path);

    let mut writer = csv::Writer::from_path(&trips_new_path)
        .map_err(|e| format!("Unable to create file: {}", e))?;

    for trip in trips {
        writer
            .serialize(trip)
            .map_err(|e| format!("Unable to write to file: {}", e))?;
    }

    writer
        .flush()
        .map_err(|e| format!("Unable to flush file: {}", e))?;

    fs::rename(&trips_new_path, &trips_file_path)?;

    println!("Fixing stop_times");

    // Read stop_times file and at the same time, write to new file
    let stop_times_file_path = format!("{}/stop_times.txt", folder_path);

    let stop_times_file = File::open(&stop_times_file_path)
        .map_err(|e| format!("Unable to open file: {} - {}", stop_times_file_path, e))?;

    let mut rdr = csv::Reader::from_reader(stop_times_file);

    let stop_times_new_path = format!("{}/stop_times_cleaned.txt", folder_path);
    let mut writer = csv::Writer::from_path(&stop_times_new_path)
        .map_err(|e| format!("Unable to create file: {}", e))?;

    for result in rdr.deserialize() {
        if let Ok(record) = result {
            let record: RawStopTime = record;

            // Check if trip is in banned trips
            if trip_ids_to_remove.contains(&record.trip_id) {
                continue;
            } else {
                writer
                    .serialize(record)
                    .map_err(|e| format!("Unable to write to file: {}", e))?;
            }
        } else {
            eprintln!("Error reading record");
        }
    }

    writer
        .flush()
        .map_err(|e| format!("Unable to flush file: {}", e))?;

    fs::rename(&stop_times_new_path, &stop_times_file_path)?;

    println!("Finished removing banned agencies from {}", folder_path);

    Ok(())
}
