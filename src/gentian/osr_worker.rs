use anyhow::{Context, Result, ensure};
use clap::Parser;
use osr_ffi::{Profile, ffi};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "catenaryconfig.toml")]
    config: PathBuf,
    #[arg(long, default_value = "127.0.0.1:40501")]
    listen: String,
    /// Concurrent native OSR search contexts sharing one read-only graph per region.
    #[arg(long, default_value_t = 3)]
    workers: usize,
}

#[derive(Deserialize)]
struct Config {
    osm_manager: Manager,
    #[serde(default)]
    osr_worker: WorkerConfig,
}
#[derive(Deserialize)]
struct Manager {
    data_dir: PathBuf,
}
#[derive(Default, Deserialize)]
struct WorkerConfig {
    #[serde(default)]
    excluded_regions: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StreetMode { #[default] Walk, Bike }

#[derive(Deserialize)]
struct Request {
    region: String,
    latitude: f64,
    longitude: f64,
    max_seconds: u32,
    /// 0 = forward routing (access), 1 = backward routing (egress).
    #[serde(default)]
    direction: u8,
    #[serde(default)]
    mode: StreetMode,
    #[serde(default)]
    pedestrian_speed: Option<f32>,
    #[serde(default)]
    cycling_speed: Option<f32>,
    stops: Vec<Stop>,
    #[serde(default)]
    include_geometry: bool,
}
#[derive(Clone, Deserialize)]
struct Stop {
    stop: u32,
    latitude: f64,
    longitude: f64,
}
#[derive(Serialize)]
struct Access {
    stop: u32,
    seconds: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    geometry: Option<Vec<[f64; 2]>>,
}
#[derive(Serialize)]
struct Response {
    reachable: Vec<Access>,
    error: Option<String>,
}

fn run_request(
    graphs: &BTreeMap<String, cxx::SharedPtr<ffi::Router>>,
    query: Request,
) -> Result<Vec<Access>> {
    ensure!(query.direction <= 1, "invalid OSR query direction");
    let (profile, selected_speed) = match query.mode {
        StreetMode::Walk => {
            ensure!(query.cycling_speed.is_none(), "cycling speed supplied for walking");
            let speed = query.pedestrian_speed.unwrap_or(1.2);
            ensure!(speed.is_finite() && (0.3..=10.0).contains(&speed),
                "pedestrian speed out of range");
            (Profile::Foot, speed)
        }
        StreetMode::Bike => {
            ensure!(query.pedestrian_speed.is_none(), "walking speed supplied for cycling");
            let speed = query.cycling_speed.unwrap_or(4.2);
            ensure!(speed.is_finite() && (0.7..=20.0).contains(&speed),
                "cycling speed out of range");
            (Profile::Bike, speed)
        }
    };
    let foot_speed = if matches!(query.mode, StreetMode::Walk) { selected_speed } else { 0.0 };
    let bike_speed = if matches!(query.mode, StreetMode::Bike) { selected_speed } else { 0.0 };
    ensure!(
        query.latitude.is_finite() && query.longitude.is_finite(),
        "invalid origin"
    );
    ensure!(
        (1..=3600).contains(&query.max_seconds),
        "invalid OSR duration budget"
    );
    ensure!(
        query.stops.len() <= 4096,
        "too many stops for one OSR query"
    );
    let graph = graphs
        .get(&query.region)
        .context("OSR region is not loaded")?;
    let router = graph.as_ref().context("OSR graph is null")?;
    let options = ffi::RoutingOptions {
        max_cost: query.max_seconds,
        include_segments: query.include_geometry,
        foot_speed_meters_per_second: foot_speed,
        bike_speed_meters_per_second: bike_speed,
        ..ffi::RoutingOptions::default().profile(profile)
    };
    // A backward query is from the arrival point to the stops in OSR search
    // order, yielding legal stop-to-destination egress costs.
    let mut targets = Vec::with_capacity(query.stops.len());
    for x in &query.stops {
        ensure!(
            x.latitude.is_finite() && x.longitude.is_finite(),
            "invalid target"
        );
        targets.push(ffi::Coordinate::outdoor(x.latitude, x.longitude));
    }
    // Use low-level native OSR matching. Access queries start at the user
    // (forward); egress queries search backward from the destination to
    // candidate stops. Destination candidates must be reverse-matched
    // relative to the chosen OSR search direction.
    let origin = ffi::Coordinate::outdoor(query.latitude, query.longitude);
    let mut source_matches = router.create_match_store();
    let mut target_matches = router.create_match_store();
    let origin_options = ffi::MatchOptions {
        profile: profile as u8,
        direction: query.direction,
        reverse: false,
        max_match_distance_meters: options.max_match_distance_meters,
        foot_speed_meters_per_second: options.foot_speed_meters_per_second,
        bike_speed_meters_per_second: options.bike_speed_meters_per_second,
    };
    let destination_options = ffi::MatchOptions {
        reverse: true,
        ..origin_options.clone()
    };
    let raw_origin = router.get_raw_match(&origin, 250.0)?;
    router.append_lookup_match(
        source_matches.pin_mut(),
        &origin,
        &origin_options,
        &raw_origin,
    )?;
    let mut target_indices = Vec::with_capacity(targets.len());
    for target in &targets {
        let raw = router.get_raw_match(target, 250.0)?;
        target_indices.push(router.append_lookup_match(
            target_matches.pin_mut(),
            target,
            &destination_options,
            &raw,
        )?);
    }
    let options = ffi::RoutingOptions {
        direction: query.direction,
        ..options
    };
    let paths = if target_indices.is_empty() {
        Vec::new()
    } else {
        router.route_one_to_many_cached(
            source_matches
                .as_ref()
                .context("null OSR source match store")?,
            0,
            target_matches
                .as_ref()
                .context("null OSR destination match store")?,
            &target_indices,
            &options,
        )?
    };
    let mut results = Vec::new();
    for (stop, path) in query.stops.into_iter().zip(paths) {
        if path.found && path.duration_seconds <= query.max_seconds {
            let geometry = query.include_geometry.then(|| {
                let mut points = Vec::new();
                points.push([query.longitude, query.latitude]);
                for segment in &path.segments {
                    for point in &segment.geometry {
                        let coordinate = [point.lon, point.lat];
                        if points.last() != Some(&coordinate) {
                            points.push(coordinate);
                        }
                    }
                }
                let end = [stop.longitude, stop.latitude];
                if points.last() != Some(&end) {
                    points.push(end);
                }
                points
            });
            results.push(Access {
                stop: stop.stop,
                seconds: path.duration_seconds,
                geometry,
            });
        }
    }
    results.sort_by_key(|x| (x.seconds, x.stop));
    Ok(results)
}

fn handle(stream: TcpStream, graphs: &BTreeMap<String, cxx::SharedPtr<ffi::Router>>) -> Result<()> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    let reader = BufReader::new(stream.try_clone()?);
    // Enforce the wire budget while reading. Checking after read_line could
    // allocate an unbounded String per concurrent client.
    let mut limited = reader.take(524_289);
    let mut input = Vec::new();
    let received = limited.read_until(b'\n', &mut input)?;
    ensure!(
        received > 0 && received <= 524_288 && input.last() == Some(&b'\n'),
        "OSR request exceeds 512KiB or is not newline terminated"
    );
    let response = match serde_json::from_slice::<Request>(&input) {
        Ok(req) => match run_request(graphs, req) {
            Ok(reachable) => Response {
                reachable,
                error: None,
            },
            Err(e) => Response {
                reachable: Vec::new(),
                error: Some(e.to_string()),
            },
        },
        Err(e) => Response {
            reachable: Vec::new(),
            error: Some(e.to_string()),
        },
    };
    let mut writer = BufWriter::new(stream);
    serde_json::to_writer(&mut writer, &response)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

/// Resolve graph directories before opening the shared, read-only routers.
fn region_paths(
    data_dir: &std::path::Path,
    excluded_regions: &BTreeSet<String>,
) -> Result<BTreeMap<String, PathBuf>> {
    let registry = data_dir.join("regions.geojson");
    let document: serde_json::Value = serde_json::from_slice(&fs::read(&registry)?)?;
    region_paths_from_registry(data_dir, &document, excluded_regions)
}

fn region_paths_from_registry(
    data_dir: &std::path::Path,
    document: &serde_json::Value,
    excluded_regions: &BTreeSet<String>,
) -> Result<BTreeMap<String, PathBuf>> {
    let features = document
        .get("features")
        .and_then(|x| x.as_array())
        .context("OSR regions registry missing features")?;
    let mut paths = BTreeMap::new();
    for feature in features {
        let id = feature
            .get("properties")
            .and_then(|x| x.get("id"))
            .and_then(|x| x.as_str())
            .context("OSR registry entry missing id")?;
        ensure!(
            !id.split('/').any(|p| p == ".." || p == "." || p.is_empty()),
            "invalid OSR id"
        );
        if excluded_regions.contains(id) {
            eprintln!("Skipping excluded OSR region: {id}");
            continue;
        }
        ensure!(
            paths
                .insert(id.to_owned(), data_dir.join("regions").join(id))
                .is_none(),
            "duplicate OSR region ID: {id}"
        );
    }
    ensure!(
        !paths.is_empty(),
        "no OSR regions enabled (check osr_worker.excluded_regions)"
    );
    Ok(paths)
}

/// Eagerly open exactly one immutable native router per configured region.
/// Match stores and all routing search state remain local to each request.
fn open_graphs(
    paths: &BTreeMap<String, PathBuf>,
) -> Result<BTreeMap<String, cxx::SharedPtr<ffi::Router>>> {
    let mut graphs = BTreeMap::new();
    for (id, dir) in paths {
        let router = ffi::open_shared_router(dir.to_str().context("non-UTF8 OSR path")?)?;
        ensure!(!router.is_null(), "OSR graph is null: {id}");
        eprintln!("Opened native OSR mmap: {id}");
        graphs.insert(id.clone(), router);
    }
    Ok(graphs)
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=16).contains(&args.workers),
        "--workers must be in 1..=16"
    );
    let cfg: Config = toml::from_str(&fs::read_to_string(&args.config)?)?;
    let paths = region_paths(&cfg.osm_manager.data_dir, &cfg.osr_worker.excluded_regions)?;
    // Open each graph once before accepting connections or spawning query threads.
    let graphs = Arc::new(open_graphs(&paths)?);
    let listener = TcpListener::bind(&args.listen)?;

    // Bounded queue provides backpressure under load. Workers share immutable
    // native routers, but create request-local match stores and search state.
    let (send, recv) = mpsc::sync_channel::<TcpStream>(args.workers * 4);
    let recv = Arc::new(Mutex::new(recv));
    for worker in 0..args.workers {
        let recv = Arc::clone(&recv);
        let graphs = Arc::clone(&graphs);
        thread::Builder::new()
            .name(format!("osr-query-{worker}"))
            .spawn(move || {
                loop {
                    // Do not hold the receiver mutex while running native OSR.
                    let stream = match recv.lock().expect("OSR queue poisoned").recv() {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    if let Err(error) = handle(stream, &graphs) {
                        eprintln!("OSR client: {error}");
                    }
                }
            })
            .context("spawning OSR worker")?;
    }
    eprintln!(
        "OSR worker listening at {} ({} concurrent queries)",
        args.listen, args.workers
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => send.send(stream).context("OSR worker queue closed")?,
            Err(error) => eprintln!("OSR accept: {error}"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusions_default_to_empty_for_existing_configs() -> Result<()> {
        let config: Config = toml::from_str("[osm_manager]\ndata_dir = '/tmp/osr-data'\n")?;
        assert!(config.osr_worker.excluded_regions.is_empty());
        Ok(())
    }

    #[test]
    fn excluded_regions_are_not_selected_for_mmap() -> Result<()> {
        let config: Config = toml::from_str(
            "[osm_manager]\ndata_dir = '/tmp/osr-data'\n\n[osr_worker]\nexcluded_regions = ['asia/japan']\n",
        )?;
        let registry = serde_json::json!({
            "features": [
                {"properties": {"id": "asia/japan"}},
                {"properties": {"id": "australia-oceania"}}
            ]
        });
        let paths = region_paths_from_registry(
            &config.osm_manager.data_dir,
            &registry,
            &config.osr_worker.excluded_regions,
        )?;
        assert_eq!(paths.len(), 1);
        assert!(!paths.contains_key("asia/japan"));
        assert_eq!(
            paths["australia-oceania"],
            PathBuf::from("/tmp/osr-data/regions/australia-oceania")
        );
        Ok(())
    }

    #[test]
    fn excluding_every_region_fails_fast() {
        let registry = serde_json::json!({
            "features": [{"properties": {"id": "asia/japan"}}]
        });
        let excluded = BTreeSet::from(["asia/japan".to_owned()]);
        let error =
            region_paths_from_registry(std::path::Path::new("/tmp/osr-data"), &registry, &excluded)
                .err()
                .expect("no enabled regions must fail startup");
        assert!(error.to_string().contains("no OSR regions enabled"));
    }

    #[test]
    fn unknown_region_is_rejected_without_opening_another_graph() {
        let request = Request {
            region: "missing".to_owned(),
            latitude: 0.0,
            longitude: 0.0,
            max_seconds: 120,
            direction: 0,
            stops: Vec::new(),
            include_geometry: false,
        };
        let error = run_request(&BTreeMap::new(), request)
            .err()
            .expect("unknown regions must fail");
        assert!(error.to_string().contains("OSR region is not loaded"));
    }

    #[test]
    fn concurrent_workers_reuse_the_same_native_router() -> Result<()> {
        // Point at an extracted OSR region to exercise actual native mmap handles.
        let Ok(data_dir) = std::env::var("OSR_TEST_DATA_DIR") else {
            return Ok(());
        };
        let mut paths = BTreeMap::new();
        paths.insert("test".to_owned(), PathBuf::from(data_dir));
        let graphs = Arc::new(open_graphs(&paths)?);
        let original = graphs["test"].as_ref().context("null test router")?;
        let expected_nodes = original.node_count();
        ensure!(expected_nodes > 0, "test graph has no nodes");

        thread::scope(|scope| {
            for _ in 0..4 {
                let graphs = Arc::clone(&graphs);
                scope.spawn(move || {
                    let router = graphs["test"].as_ref().expect("null shared router");
                    assert!(std::ptr::eq(router, original));
                    assert_eq!(router.node_count(), expected_nodes);
                });
            }
        });
        Ok(())
    }
}
