fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = arguments::parse(std::env::args());

    let path = arguments
        .expect("expected path to gtfs file like --path FILE.zip")
        .get::<String>("path")
        .expect("expected path to gtfs file like --path FILE.zip");

    println!("Loading {}", path);
    let gtfs = gtfs_structures::Gtfs::new(&path.as_str()).expect("failed to load gtfs file");

    let start = std::time::Instant::now();
    let response = catenary::maple_syrup::reduce(&gtfs);
    let duration = start.elapsed();

    let route_count = gtfs.routes.len();
    let trip_count = gtfs.trips.len();

    let stop_times_count = gtfs
        .trips
        .iter()
        .map(|(_, trip)| trip.stop_times.len())
        .sum::<usize>();

    let iten_count = response.itineraries.len();

    println!("Reduced schedule in {:?}", duration);
    println!(
        "{} has {} routes, {} trips and {} stop times, reduced to {} itineraries",
        path, route_count, trip_count, stop_times_count, iten_count
    );

    println!(
        "Compression ratio: {:.2}",
        (trip_count) as f64 / iten_count as f64
    );
    //    println!("Weissman score: ")

    Ok(())
}
