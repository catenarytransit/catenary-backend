use catenary::fix_stop_times_headsigns;
use csv::StringRecord;
use csv::{ReaderBuilder, WriterBuilder};
use std::env;
use std::fs;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::{Read, Write};
use std::time::Instant;
use std::{fs::File, io::BufReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = arguments::parse(std::env::args());

    let path = arguments
        .expect("expected path to gtfs file like --path FILE.zip")
        .get::<String>("path")
        .expect("expected path to gtfs file like --path FILE.zip");

    // Unzip the file
    let start_unzip = Instant::now();
    let file = fs::File::open(&path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    //write to new folder maple_test_unzip/ in the current directory, not temp dir

    let new_unzipped_dir = env::current_dir().unwrap().join("maple_test_unzip");

    println!(
        "Unzipping '{}' to '{}'...",
        path,
        new_unzipped_dir.display()
    );

    fs::create_dir_all(&new_unzipped_dir)?;

    //wipe everything in the folder

    for entry in fs::read_dir(&new_unzipped_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = new_unzipped_dir.join(file.mangled_name());

        if file.is_dir() {
            fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    fs::create_dir_all(&p)?;
                }
            }
            let mut outfile = fs::File::create(&outpath)?;
            std::io::copy(&mut file, &mut outfile)?;
        }
    }

    let unzip_duration = Instant::now() - start_unzip;

    println!(
        "Unzipping completed in {:.3} seconds.",
        unzip_duration.as_secs_f64()
    );
    //make temporary folder

    let recomp_temp_dir = env::temp_dir().join("recompression_test");
    fs::create_dir_all(&recomp_temp_dir)?;

    let mut total_original_file_size = 0;

    total_original_file_size += fs::metadata(new_unzipped_dir.join("trips.txt"))?.len();
    total_original_file_size += fs::metadata(new_unzipped_dir.join("stop_times.txt"))?.len();

    // compress the folder
    let start_compress = Instant::now();
    let compressed_path = recomp_temp_dir.join("compressed.zip");
    let compressed_file = fs::File::create(&compressed_path)?;

    let mut zip = zip::ZipWriter::new(&compressed_file);

    let options = zip::write::SimpleFileOptions::default()
        .large_file(true)
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o755);

    zip.start_file("trips.txt", options)?;
    let mut trips_file = fs::File::open(new_unzipped_dir.join("trips.txt"))?;
    std::io::copy(&mut trips_file, &mut zip)?;

    zip.start_file("stop_times.txt", options)?;
    let mut stop_times_file = fs::File::open(new_unzipped_dir.join("stop_times.txt"))?;
    std::io::copy(&mut stop_times_file, &mut zip)?;

    zip.finish()?;
    let zip_compress_duration = Instant::now() - start_compress;

    //get compression ratio

    let original_size = fs::metadata(&path)?.len();
    let compressed_size = fs::metadata(&compressed_path)?.len();

    let zip_compression_ratio = compressed_size as f64 / original_size as f64;

    println!(
        "Compressed in {:.3} seconds, {} bytes",
        zip_compress_duration.as_secs_f64(),
        compressed_size
    );

    println!("Loading {}", path);
    let gtfs_reader = gtfs_structures::GtfsReader::default().trim_fields(true);

    let gtfs = gtfs_reader
        .read_from_path(&new_unzipped_dir)
        .expect("failed to load gtfs file");

    let start = std::time::Instant::now();
    let response = catenary::maple_syrup::reduce(&gtfs);
    let duration = start.elapsed();

    let route_count = gtfs.routes.len();
    let trip_count = gtfs.trips.len();

    let stop_times_count = gtfs
        .trips
        .values()
        .map(|trip| trip.stop_times.len())
        .sum::<usize>();

    let iten_count = response.itineraries.len();

    println!("Reduced schedule in {:?}", duration);
    println!(
        "{} has {} routes, {} trips and {} stop times, reduced to {} itineraries",
        path, route_count, trip_count, stop_times_count, iten_count
    );

    //write entire structure as bytecode

    let start_write = Instant::now();

    let new_zip_path = env::current_dir()?.join("reduced.gtfs.zip");
    let mut file = fs::File::create(&new_zip_path)?;

    let mut writer = BufWriter::new(file);

    let data = bincode::serde::encode_to_vec(&response, bincode::config::standard())?;

    writer.write_all(&data)?;

    let write_duration = Instant::now() - start_write;

    let total_reduced_file_size = fs::metadata(&new_zip_path)?.len();

    println!(
        "Wrote reduced schedule in {:.3} seconds, {} bytes",
        write_duration.as_secs_f64(),
        total_reduced_file_size
    );

    let catenary_compression_ratio =
        total_original_file_size as f64 / total_reduced_file_size as f64;

    const ALPHA: f64 = 1.0;

    println!("alpha constant: {}", ALPHA);

    //W = a * (new ratio / zip ratio) * (log(new time in s)) / (log(zip time in s))

    println!("Zip compression ratio: {}", zip_compression_ratio);
    println!("Catenary compression ratio: {}", catenary_compression_ratio);

    let zip_time_score = (zip_compress_duration.as_secs_f64()).log(10.0);
    let catenary_time_score = (duration.as_secs_f64()).log(10.0);

    let weissman_score = ALPHA
        * (catenary_compression_ratio / zip_compression_ratio)
        * (catenary_time_score / zip_time_score);

    println!("Weissman score: {}", weissman_score);

    Ok(())
}
