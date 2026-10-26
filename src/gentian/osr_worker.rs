use anyhow::{Context, Result, ensure};
use clap::Parser;
use osr_ffi::{Profile, ffi};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "catenaryconfig.toml")]
    config: PathBuf,
    #[arg(long, default_value = "127.0.0.1:40501")]
    listen: String,
    /// Concurrent native OSR search contexts. Each worker owns its router
    /// handles: CXX UniquePtr/router state is never shared across threads.
    #[arg(long, default_value_t = 3)]
    workers: usize,
}

#[derive(Deserialize)]
struct Config {
    osm_manager: Manager,
}
#[derive(Deserialize)]
struct Manager {
    data_dir: PathBuf,
}

#[derive(Deserialize)]
struct Request {
    region: String,
    latitude: f64,
    longitude: f64,
    max_seconds: u32,
    /// 0 = forward routing (access), 1 = backward routing (egress).
    #[serde(default)]
    direction: u8,
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
    graphs: &mut BTreeMap<String, cxx::UniquePtr<ffi::Router>>,
    paths: &BTreeMap<String, PathBuf>,
    query: Request,
) -> Result<Vec<Access>> {
    ensure!(query.direction <= 1, "invalid OSR query direction");
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
    if !graphs.contains_key(&query.region) {
        let dir = paths.get(&query.region).context("unknown OSR region")?;
        let router = ffi::open_router(dir.to_str().context("non-UTF8 OSR path")?)?;
        ensure!(!router.is_null(), "OSR graph is null: {}", query.region);
        eprintln!("Opened native OSR mmap: {}", query.region);
        graphs.insert(query.region.clone(), router);
    }
    let graph = graphs
        .get(&query.region)
        .context("OSR region is not loaded")?;
    let router = graph.as_ref().context("OSR graph is null")?;
    let options = ffi::RoutingOptions {
        max_cost: query.max_seconds,
        include_segments: query.include_geometry,
        ..ffi::RoutingOptions::default().profile(Profile::Foot)
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
        profile: Profile::Foot as u8,
        direction: query.direction,
        reverse: false,
        max_match_distance_meters: options.max_match_distance_meters,
        foot_speed_meters_per_second: options.foot_speed_meters_per_second,
    };
    let destination_options = ffi::MatchOptions {
        reverse: true,
        ..origin_options.clone()
    };
    let raw_origin = router.get_raw_match(&origin, 250.0)?;
    router.append_lookup_match(
        source_matches.pin_mut(), &origin, &origin_options, &raw_origin,
    )?;
    let mut target_indices = Vec::with_capacity(targets.len());
    for target in &targets {
        let raw = router.get_raw_match(target, 250.0)?;
        target_indices.push(router.append_lookup_match(
            target_matches.pin_mut(), target, &destination_options, &raw,
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
            source_matches.as_ref().context("null OSR source match store")?,
            0,
            target_matches.as_ref().context("null OSR destination match store")?,
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
                if points.last() != Some(&end) { points.push(end); }
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

fn handle(
    stream: TcpStream,
    graphs: &mut BTreeMap<String, cxx::UniquePtr<ffi::Router>>,
    paths: &BTreeMap<String, PathBuf>,
) -> Result<()> {
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
        Ok(req) => match run_request(graphs, paths, req) {
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

/// Resolve graph directories in the acceptor thread; CXX native routers are
/// opened independently inside their owning worker threads.
fn region_paths(data_dir: &std::path::Path) -> Result<BTreeMap<String, PathBuf>> {
    let registry = data_dir.join("regions.geojson");
    let document: serde_json::Value = serde_json::from_slice(&fs::read(&registry)?)?;
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
        ensure!(
            paths
                .insert(id.to_owned(), data_dir.join("regions").join(id))
                .is_none(),
            "duplicate OSR region ID: {id}"
        );
    }
    ensure!(!paths.is_empty(), "no OSR regions configured");
    Ok(paths)
}

/// The primary worker eagerly initializes the configured regions. Additional
/// workers initialize handles lazily per region. Native handles never cross
/// thread boundaries; read-only mmap pages share the Linux page cache.
fn open_graphs(
    paths: &BTreeMap<String, PathBuf>,
) -> Result<BTreeMap<String, cxx::UniquePtr<ffi::Router>>> {
    let mut graphs = BTreeMap::new();
    for (id, dir) in paths {
        let router = ffi::open_router(dir.to_str().context("non-UTF8 OSR path")?)?;
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
    let paths = Arc::new(region_paths(&cfg.osm_manager.data_dir)?);
    let listener = TcpListener::bind(&args.listen)?;

    // Bounded queue provides backpressure under load. Only TcpStreams, never
    // native CXX routers, cross thread boundaries.
    let (send, recv) = mpsc::sync_channel::<TcpStream>(args.workers * 4);
    let recv = Arc::new(Mutex::new(recv));
    let (ready_tx, ready_rx) = mpsc::channel::<Result<()>>();
    for worker in 0..args.workers {
        let recv = Arc::clone(&recv);
        let paths = Arc::clone(&paths);
        let ready = ready_tx.clone();
        thread::Builder::new()
            .name(format!("osr-query-{worker}"))
            .spawn(move || {
                // One eagerly initialized context retains the previous startup
                // validation behavior. Additional contexts initialize lazily,
                // avoiding duplicate native memory for OSR regions never queried.
                let mut graphs = if worker == 0 {
                    match open_graphs(&paths) {
                        Ok(g) => g,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    }
                } else {
                    BTreeMap::new()
                };
                let _ = ready.send(Ok(()));
                loop {
                    // Do not hold the receiver mutex while running native OSR.
                    let stream = match recv.lock().expect("OSR queue poisoned").recv() {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    if let Err(error) = handle(stream, &mut graphs, &paths) {
                        eprintln!("OSR client: {error}");
                    }
                }
            })
            .context("spawning OSR worker")?;
    }
    drop(ready_tx);
    // Do not advertise a listening worker until all routers have opened.
    for _ in 0..args.workers {
        ready_rx
            .recv()
            .context("OSR worker failed to initialize")??;
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
