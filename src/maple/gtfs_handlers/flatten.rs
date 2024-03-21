use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::io::Read;
use std::path::PathBuf;

pub fn flatten_feed(feed_id: &str) -> Result<(), Box<dyn Error>> {
    let _ = fs::create_dir("gtfs_uncompressed");

    let source_path = format!("gtfs_static_zips/{}.zip", feed_id);
    let target_path = format!("gtfs_uncompressed/{}", feed_id);

    // Attempt to open file and pass back error if failed
    let mut file = File::open(source_path)?;
    let mut buf: Vec<u8> = vec![];

    // read bytes and pass back error if unable to read
    let read = file.read_to_end(&mut buf)?;
    let target_dir = PathBuf::from(target_path);

    //extract file
    zip_extract::extract(Cursor::new(buf), &target_dir, true)?;

    // go into folder and unnest folders
    if feed_id == "f-dr4-septa~rail" || feed_id == "f-dr4-septa~bus" {}

    Ok(())
}
