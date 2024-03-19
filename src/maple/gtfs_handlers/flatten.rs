use std::error::Error;
use std::fs;
//use std::path::PathBuf;

fn flatten_feed(feed_id: &str, zip_name: &str) -> Result<(), Box<dyn Error>> {
    let _ = fs::create_dir("gtfs_uncompressed");

    // unzip

    // go into folder and unnest folders
    
    Ok(())
}