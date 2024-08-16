use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::{read_dir, remove_file};
use std::io::Cursor;
use std::io::Read;
use std::path::PathBuf;

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
    gtfs_temp_storage: &str,
    gtfs_uncompressed_temp_storage: &str,
    feed_id: &str,
    sub_folder: &str,
) -> Result<(), Box<dyn Error>> {
    let source_path = format!("{}/{}.zip", gtfs_temp_storage, feed_id);
    let target_path = format!("{}/{}", gtfs_uncompressed_temp_storage, feed_id);

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

pub fn flatten_feed(
    gtfs_temp_storage: &str,
    gtfs_uncompressed_temp_storage: &str,
    feed_id: &str,
) -> Result<(), Box<dyn Error>> {
    let _ = fs::create_dir(gtfs_uncompressed_temp_storage);

    let source_path = format!("{}/{}.zip", gtfs_temp_storage, feed_id);
    let target_path = format!("{}/{}", gtfs_uncompressed_temp_storage, feed_id);

    // Attempt to open file and pass back error if failed
    let mut file = File::open(source_path)?;
    let mut buf: Vec<u8> = vec![];

    // read bytes and pass back error if unable to read
    let read = file.read_to_end(&mut buf)?;
    let target_dir = PathBuf::from(target_path);

    //extract file
    zip_extract::extract(Cursor::new(buf), &target_dir, true)?;

    // go into folder and unnest folders
    if feed_id == "f-dr4-septa~rail" {
        extract_sub_zip(
            gtfs_temp_storage,
            gtfs_uncompressed_temp_storage,
            "f-dr4-septa~rail",
            "google_rail",
        )?;
    }

    if feed_id == "f-dr4-septa~bus" {
        extract_sub_zip(
            gtfs_temp_storage,
            gtfs_uncompressed_temp_storage,
            "f-dr4-septa~bus",
            "google_bus",
        )?;
    }

    Ok(())
}
