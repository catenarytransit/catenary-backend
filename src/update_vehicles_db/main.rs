use std::error::Error;
use clap::Parser;
use serde::Deserialize;
use std::{
    fs, io,
    path::{Path, PathBuf},
};


#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct VehicleType {
    pub manufacturer: String,
    pub model: String,
    pub roster: Vec<Roster>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Roster {
    pub fleet_selection: FleetSelector,
    pub engine: Option<String>,
    pub transmission: Option<String>,
    pub notes: Option<String>,
    pub years: Option<Vec<u16>>,
    pub division: Option<String>,
}

//range numbers are inclusive on both sides.
//for example, to iterate, do
//for i in start_number..=end_number
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FleetSelector {
    pub start_number: Option<u32>,
    pub end_number: Option<u32>,
    pub start_text: Option<String>,
    pub end_text: Option<String>,
    pub use_numeric_sorting: bool,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RootOfVehicleFile {
    pub vehicles: Vec<VehicleType>,
}

fn find_json_files_recursive(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut json_files: Vec<PathBuf> = Vec::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let sub_dir_json_files = find_json_files_recursive(&path)?; // Recursive call for subdirectories
                json_files.extend(sub_dir_json_files); // Add JSON files from subdirectory
            } else if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                json_files.push(path); // Add JSON file path to the list
            }
        }
    }
    Ok(json_files)
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    vehicles_db_folder: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + std::marker::Send + Sync>> {
    let args = Args::parse();

    let json_files = find_json_files_recursive(Path::new(&args.vehicles_db_folder))?;

    if json_files.len() == 0 {
        println!("No JSON files found in the folder");
        return Ok(());
    }

    for file_path in json_files {
        let file_read_to_string = std::fs::read_to_string(&file_path).unwrap();
        let vehicles = serde_json::from_str::<RootOfVehicleFile>(&file_read_to_string);

        match vehicles {
            Ok(vehicles) => {
                println!("File: {:?}", file_path);
              //  println!("Data: {:#?}", vehicles);

              let cleaned_path = file_path.strip_prefix(&args.vehicles_db_folder).unwrap();
              let cleaned_path = cleaned_path.to_str().unwrap().replace(".json", "");

              println!("Cleaned Path: {:?}", cleaned_path);
            }
            Err(e) => {
                println!("Error parsing JSON file: {:?}", e);
            }
        }
    }

    Ok(())
}