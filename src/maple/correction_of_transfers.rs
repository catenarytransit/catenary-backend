use csv::ReaderBuilder;
use csv::WriterBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct Stop {
    stop_id: String,
    // Add other fields from stops.txt if needed
}

#[derive(Debug, Deserialize, Serialize)]
struct Transfer {
    from_stop_id: String,
    to_stop_id: String,
    transfer_type: u8,
    min_transfer_time: Option<u16>,
    // Add other fields from transfers.txt if needed
}

pub fn filter_and_write_transfers(
    stops_file_path: &str,
    transfers_file_path: &str,
    output_file_path: &str,
) -> Result<(), Box<dyn Error>> {
    // Read stops.txt and collect valid stop IDs
    let stops_file = File::open(stops_file_path)?;
    let stops_reader = BufReader::new(stops_file);
    let mut stops_csv_reader = ReaderBuilder::new().from_reader(stops_reader);

    let mut valid_stop_ids = HashSet::new();
    for result in stops_csv_reader.deserialize::<Stop>() {
        match result {
            Ok(record) => {
                valid_stop_ids.insert(record.stop_id);
            }
            Err(e) => {
                eprintln!("Error reading stops.txt record: {}", e);
                // Consider whether to continue or return an error
            }
        }
    }

    // Read transfers.txt and filter rows with valid stop IDs
    let transfers_file = File::open(transfers_file_path)?;
    let transfers_reader = BufReader::new(transfers_file);
    let mut transfers_csv_reader = ReaderBuilder::new().from_reader(transfers_reader);

    let mut valid_transfers = Vec::new();
    for result in transfers_csv_reader.deserialize::<Transfer>() {
        match result {
            Ok(record) => {
                if valid_stop_ids.contains(&record.from_stop_id)
                    && valid_stop_ids.contains(&record.to_stop_id)
                {
                    valid_transfers.push(record);
                }
            }
            Err(e) => {
                eprintln!("Error reading transfers.txt record: {}", e);
                // Consider whether to continue or return an error
            }
        }
    }

    // Write the filtered transfers back to the output file
    let output_file = File::create(output_file_path)?;
    let mut writer = WriterBuilder::new().from_writer(BufWriter::new(output_file));

    // Write the filtered records
    for transfer in valid_transfers {
        writer.serialize(transfer)?;
    }

    // Ensure all buffered data is written to the file
    writer.flush()?;

    Ok(())
}
