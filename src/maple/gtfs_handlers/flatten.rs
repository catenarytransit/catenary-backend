use csv::ReaderBuilder;
use csv::WriterBuilder;
use dmfr::Feed;
use dmfr_dataset_reader::ReturnDmfrAnalysis;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::fs::{read_dir, remove_file};
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::rc::Rc;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct RawRiderCategoryRecord {
    pub id: String,
    pub name: String,
    pub is_default_fare_category: Option<u8>,
    pub eligibility_url: Option<String>,
}

fn fix_fares(
    gtfs_uncompressed_temp_storage: &str,
    feed_id: &str,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut valid_rider_categories = true;

    let rider_categories_path = format!(
        "{}/{}/rider_categories.txt",
        gtfs_uncompressed_temp_storage, feed_id
    );

    let rider_categories_file = fs::File::open(&rider_categories_path);

    if let Ok(rider_categories_file) = rider_categories_file {
        let rider_categories_file_reader = BufReader::new(rider_categories_file);
        let rider_categories_file_lines: Vec<String> = rider_categories_file_reader
            .lines()
            .map(|line| line.expect("Failed to read line!"))
            .collect();

        let first_line = rider_categories_file_lines[0]
            .split(",")
            .map(|x| x.to_string())
            .collect::<HashSet<String>>();

        if first_line.contains("is_default_fare_category")
            && first_line.contains("rider_category_name")
            && first_line.contains("rider_category_id")
        {
        } else {
            valid_rider_categories = false;
        }
    }

    if valid_rider_categories == false {
        let _ = fs::remove_file(&rider_categories_path);
    }

    Ok(())
}

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

fn remove_0x_from_colours(gtfs_uncompressed_temp_storage: &str, feed_id: &str) -> () {
    let target_path_of_routes_file =
        format!("{}/{}/routes.txt", gtfs_uncompressed_temp_storage, feed_id);

    let data_of_file = fs::read_to_string(&target_path_of_routes_file).unwrap();

    let new_data = data_of_file.replace("0x", "");

    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&target_path_of_routes_file)
        .unwrap();
    f.write_all(new_data.as_bytes()).unwrap();
    f.flush().unwrap();
}

fn flatten_if_single_subfolder(folder_path: &Path) -> Result<(), io::Error> {
    let entries = fs::read_dir(folder_path)?;
    let mut subfolders = Vec::new();

    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            subfolders.push(entry.path());
        }
    }

    if subfolders.len() == 1 {
        let subfolder_path = subfolders.into_iter().next().unwrap();
        let subfolder_name = subfolder_path.file_name().unwrap();

        let subfolder_entries = fs::read_dir(&subfolder_path)?;

        for entry in subfolder_entries {
            let entry = entry?;
            let source_path = entry.path();
            let file_name = source_path.file_name().unwrap();
            let destination_path = folder_path.join(file_name);
            fs::rename(&source_path, &destination_path)?;
        }

        fs::remove_dir(&subfolder_path)?;
        println!("Flattened folder: {:?}", folder_path);
    } else {
        println!(
            "Folder {:?} does not contain exactly one subfolder.",
            folder_path
        );
    }

    Ok(())
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
        //   remove_transloc_prefix(gtfs_uncompressed_temp_storage, feed_id);
    }

    if feed_id == "f-9qh-omnitrans" {
        remove_0x_from_colours(gtfs_uncompressed_temp_storage, feed_id);
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
        let subfolder_to_get = feed_id.rsplitn(2, '~').next().unwrap();

        println!(
            "Extracting subfolder {} for Victoria Australia",
            subfolder_to_get
        );

        // PTV has nested structure: {branch}/google_transit.zip
        let source_path = format!(
            "{}/{}/{}/google_transit.zip",
            gtfs_uncompressed_temp_storage, feed_id, subfolder_to_get
        );

        // Check if nested google_transit.zip exists, fallback to old format if not
        let subfolder_answer = if std::path::Path::new(&source_path).exists() {
            println!("Using nested google_transit.zip at {}", source_path);
            let mut file = File::open(&source_path)?;
            let mut buf: Vec<u8> = vec![];
            file.read_to_end(&mut buf)?;
            let target_dir =
                PathBuf::from(format!("{}/{}", gtfs_uncompressed_temp_storage, feed_id));
            zip_extract::extract(Cursor::new(buf), &target_dir, true)?;
            delete_zip_files(&format!("{}/{}", gtfs_uncompressed_temp_storage, feed_id))?;
            // Also remove the numbered folder after extraction
            let folder_to_remove = format!(
                "{}/{}/{}",
                gtfs_uncompressed_temp_storage, feed_id, subfolder_to_get
            );
            if std::path::Path::new(&folder_to_remove).is_dir() {
                let _ = fs::remove_dir_all(&folder_to_remove);
            }
            Ok(())
        } else {
            // Fallback to old format: {branch}.zip
            extract_sub_zip(gtfs_uncompressed_temp_storage, feed_id, subfolder_to_get)
        };

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

    //flatten if single subfolder

    let folder_path = PathBuf::from(&target_path);

    flatten_if_single_subfolder(&folder_path)?;

    //fix stop times for germany

    /*
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
    }*/

    // fix rider_categories.txt
    let _ = fix_fares(&gtfs_uncompressed_temp_storage, &feed_id);

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

        //check if transfers.txt is more than 1 line, if not, delete it

        let file = File::open(&transfers_path)?;

        let mut reader = std::io::BufReader::new(file);

        let mut line_count = 0;

        for _ in reader.lines() {
            line_count += 1;
        }

        if line_count < 2 {
            fs::remove_file(&transfers_path)?;
        }
    }

    Ok(())
}
