use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::{read_dir, remove_file};
use std::io::BufRead;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

use dmfr::Feed;
use dmfr_dataset_reader::ReturnDmfrAnalysis;

fn delete_zip_files(dir_path: &str) -> std::io::Result<()> {
    for entry in read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "zip") {
            // don't crash if you can't delete the zips
            let _ = remove_file(path);
        }
    }
    Ok(())
}

// Extracts a sub zip file and uses it as the parent folder
pub fn extract_sub_zip(
    gtfs_uncompressed_temp_storage: &str,
    feed_id: &str,
    sub_folder: &str,
) -> Result<(), Box<dyn Error>> {
    let source_path = format!(
        "{}/{}/{}.zip",
        gtfs_uncompressed_temp_storage, feed_id, sub_folder
    );
    let target_path = format!("{}/{}", gtfs_uncompressed_temp_storage, feed_id);

    println!("Extracting feed {} inside {}", feed_id, source_path);

    // Attempt to open file and pass back error if failed
    let mut file = File::open(source_path)?;
    let mut buf: Vec<u8> = vec![];

    // read bytes and pass back error if unable to read
    let read = file.read_to_end(&mut buf)?;
    let target_dir = PathBuf::from(target_path.as_str());

    zip_extract::extract(Cursor::new(buf), &target_dir, true)?;

    //delete excess zip files
    delete_zip_files(target_path.as_str())?;

    Ok(())
}

pub fn remove_transloc_prefix(gtfs_uncompressed_temp_storage: &str, feed_id: &str) -> () {
    //for every file in the folder, read file, remove all "TL-" matches, write file

    let target_path = format!("{}/{}", gtfs_uncompressed_temp_storage, feed_id);

    let paths = fs::read_dir(target_path).unwrap();

    for path in paths {
        let path = path.unwrap().path();
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
        let file_path = path.to_str().unwrap().to_string();

        if file_name.ends_with(".txt") {
            //read first, then write back
            let file = File::open(&file_path).unwrap();
            let reader = std::io::BufReader::new(file);
            println!("Fixing Transloc file: {}", file_path);
            let mut finished_data = reader
                .lines()
                .map(|line| {
                    let line = line.as_ref().unwrap();
                    line.replace("TL-", "")
                })
                .collect::<Vec<String>>()
                .join("\n");

            let mut file = File::create(&file_path).unwrap();

            file.write_all(finished_data.as_bytes()).unwrap();
        }
    }
}

pub fn flatten_feed(
    gtfs_temp_storage: &str,
    gtfs_uncompressed_temp_storage: &str,
    feed_id: &str,
    feed: Rc<Feed>,
) -> Result<(), Box<dyn Error>> {
    let _ = fs::create_dir(gtfs_uncompressed_temp_storage);

    let source_path = format!("{}/{}.zip", &gtfs_temp_storage, &feed_id);
    let target_path = format!("{}/{}", &gtfs_uncompressed_temp_storage, &feed_id);

    // Attempt to open file and pass back error if failed
    let mut file = File::open(&source_path)?;
    let mut buf: Vec<u8> = vec![];

    // read bytes and pass back error if unable to read
    let read = file.read_to_end(&mut buf)?;
    let target_dir = PathBuf::from(&target_path);

    //extract file
    zip_extract::extract(Cursor::new(buf), &target_dir, true)?;

    if feed_id == "f-uc~irvine~anteater~express"
        || feed_id == "f-9muq-lagunabeach~ca~us"
        || feed_id == "f-los~angeles~international~airport~shuttle"
    {
        remove_transloc_prefix(gtfs_uncompressed_temp_storage, feed_id);
    }

    // go into folder and unnest folders
    if feed_id == "f-dr4-septa~rail" {
        extract_sub_zip(
            gtfs_uncompressed_temp_storage,
            "f-dr4-septa~rail",
            "google_rail",
        )?;
    }

    if feed_id == "f-dr4-septa~bus" {
        extract_sub_zip(
            gtfs_uncompressed_temp_storage,
            "f-dr4-septa~bus",
            "google_bus",
        )?;
    }

    //if feed id starts with f-r1-ptv
    if feed_id.contains("-ptv") {
        let subfolder_to_get = feed
            .urls
            .static_current
            .as_ref()
            .unwrap()
            .replace("http://data.ptv.vic.gov.au/downloads/gtfs.zip#", "")
            .replace(".zip", "");

        println!(
            "Extracting subfolder {} for Victoria Australia",
            subfolder_to_get.as_str()
        );

        let subfolder_answer = extract_sub_zip(
            gtfs_uncompressed_temp_storage,
            feed_id,
            subfolder_to_get.as_str(),
        );

        match &subfolder_answer {
            Ok(_) => {
                println!("Subfolder extracted successfully");
            }
            Err(e) => {
                println!("Error extracting subfolder: {:?}", e);
            }
        }

        subfolder_answer?;
    }

    //fix stop times for germany

    if feed_id == "f-gtfs~de" {
        let stop_times_path = format!("{}/stop_times.txt", &target_path);
        let stop_times_temp_path = format!("{}/stop_times_temp.txt", &target_path);

        println!("Fixing stop_times.txt for Germany");
        let start_timer = std::time::Instant::now();
        catenary::fix_stop_times_headsigns(&stop_times_path, &stop_times_temp_path)?;
        println!(
            "Fixed stop times for Germany, took {:?}",
            start_timer.elapsed()
        );

        //delete old stop_times.txt
        fs::remove_file(&stop_times_path)?;

        //rename stop_times_temp.txt to stop_times.txt
        fs::rename(&stop_times_temp_path, &stop_times_path)?;
    }

    //fix transfers txt

    let transfers_path = format!("{}/transfers.txt", &target_path);
    let stops_path = format!("{}/stops.txt", &target_path);
    let new_transfer_path = format!("{}/transfers_temp.txt", &target_path);

    if PathBuf::from(&transfers_path).exists() && PathBuf::from(&stops_path).exists() {
        //fix transfers
        println!("Fixing transfers.txt for {}", feed_id);
        let start_timer = std::time::Instant::now();
        crate::correction_of_transfers::filter_and_write_transfers(
            &stops_path,
            &transfers_path,
            &new_transfer_path,
        )?;
        //rename transfers_temp.txt to transfers.txt
        fs::rename(&new_transfer_path, &transfers_path)?;
        println!(
            "Fixed transfers for {}, took {:?}",
            feed_id,
            start_timer.elapsed()
        );
    }

    Ok(())
}
