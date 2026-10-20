use anyhow::{Context, Result, ensure};
use clap::Parser;
use osr_ffi::{ffi, Profile};
use std::{collections::HashMap, fs::File, io::{BufRead, BufReader, BufWriter, Write}, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    osr_dir: PathBuf,
    #[arg(long)]
    stops: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 900)]
    max_seconds: u32,
    #[arg(long, default_value_t = 1200.0)]
    candidate_meters: f64,
}

struct Stop { lat: f64, lon: f64 }

fn meters(a: &Stop, b: &Stop) -> f64 {
    let dlat = (b.lat - a.lat).to_radians();
    let dlon = (b.lon - a.lon).to_radians();
    let h = (dlat/2.0).sin().powi(2) + a.lat.to_radians().cos()
        * b.lat.to_radians().cos() * (dlon/2.0).sin().powi(2);
    12_742_000.0 * h.sqrt().asin()
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.max_seconds > 0 && args.max_seconds <= 30_660, "invalid duration budget");
    ensure!(args.candidate_meters > 0.0, "invalid candidate radius");
    let input = BufReader::new(File::open(&args.stops)?);
    let mut stops = Vec::new();
    for (expected, line) in input.lines().enumerate() {
        let line = line?;
        let mut columns = line.split('\t');
        let id: usize = columns.next().context("missing stop index")?.parse()?;
        ensure!(id == expected, "stop indices not contiguous");
        let lat: f64 = columns.next().context("missing latitude")?.parse()?;
        let lon: f64 = columns.next().context("missing longitude")?.parse()?;
        ensure!(lat.is_finite() && lon.is_finite(), "invalid stop coordinate");
        stops.push(Stop { lat, lon });
    }
    const CELL: f64 = 0.02;
    let mut tiles: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
    for (i, stop) in stops.iter().enumerate() {
        let key = ((stop.lat / CELL).floor() as i32, (stop.lon / CELL).floor() as i32);
        tiles.entry(key).or_default().push(i);
    }
    let graph = ffi::open_router(args.osr_dir.to_str().context("non-UTF8 graph directory")?)?;
    let router = graph.as_ref().context("OSR returned null router")?;
    let options = ffi::RoutingOptions {
        max_cost: args.max_seconds,
        include_segments: false,
        ..ffi::RoutingOptions::default().profile(Profile::Foot)
    };
    if let Some(dir) = args.output.parent() { std::fs::create_dir_all(dir)?; }
    let mut output = BufWriter::new(File::create(&args.output)?);
    let mut edges = 0u64;
    for (i, origin) in stops.iter().enumerate() {
        let mut destinations = Vec::new();
        let mut indices = Vec::new();
        let y = (origin.lat / CELL).floor() as i32;
        let x = (origin.lon / CELL).floor() as i32;
        let lat_cells = (args.candidate_meters / (111_000.0 * CELL)).ceil() as i32 + 1;
        let lon_meter = 111_000.0 * origin.lat.to_radians().cos().abs();
        ensure!(lon_meter > 1.0, "near-polar routing requires a different spatial index");
        let lon_cells = (args.candidate_meters / (lon_meter * CELL)).ceil() as i32 + 1;
        for dy in -lat_cells..=lat_cells {
            for dx in -lon_cells..=lon_cells {
                if let Some(candidates) = tiles.get(&(y + dy, x + dx)) {
                    for &j in candidates {
                        let candidate = &stops[j];
                        if i != j && meters(origin, candidate) <= args.candidate_meters {
                            indices.push(j);
                            destinations.push(ffi::Coordinate::outdoor(candidate.lat, candidate.lon));
                        }
                    }
                }
            }
        }
        if destinations.is_empty() { continue; }
        let result = router.one_to_many(
            &ffi::Coordinate::outdoor(origin.lat, origin.lon), &destinations, &options, &[],
        )?;
        ensure!(result.len() == indices.len(), "OSR one-to-many result count mismatch");
        for (j, path) in indices.into_iter().zip(result) {
            if path.found && path.duration_seconds <= args.max_seconds {
                writeln!(output, "{i}\t{j}\t{}", path.duration_seconds)?;
                edges += 1;
            }
        }
        if i % 1000 == 0 {
            eprintln!("OSR origin {}/{}; {} edges", i, stops.len(), edges);
        }
    }
    output.flush()?;
    eprintln!("Computed {} OSR footpaths across {} stops", edges, stops.len());
    Ok(())
}
