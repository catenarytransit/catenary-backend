use std::fs::File;
use std::io::Read;
use std::io::Write;

pub fn fix_files_in_gtfs_directory(path: &str) -> Result<(), anyhow::Error> {
    //read stop file
    let stops = format!("{}/stops.txt", path);

    let mut f = File::open(&stops)?;

    let mut buffer: Vec<u8> = Vec::new();

    let _read = f.read_to_end(&mut buffer)?;

    let stops_string = String::from_utf8_lossy(&buffer);

    //write stops string back

    let mut f = File::create(&stops)?;

    f.write_all(stops_string.as_bytes())?;

    Ok(())
}
