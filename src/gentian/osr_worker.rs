use anyhow::{Context, Result, ensure, bail};
use clap::Parser;
use osr_ffi::{ffi, Profile};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    thread,
};

#[derive(Parser)]
struct Args {
    #[arg(long,default_value="catenaryconfig.toml")]
    config: PathBuf,
    #[arg(long,default_value="127.0.0.1:40501")]
    listen: String,
    /// Concurrent native OSR search contexts. Each worker owns its router
    /// handles: CXX UniquePtr/router state is never shared across threads.
    #[arg(long, default_value_t = 3)]
    workers: usize,
}

#[derive(Deserialize)]
struct Config { osm_manager: Manager }
#[derive(Deserialize)]
struct Manager { data_dir: PathBuf }

#[derive(Deserialize)]
struct Request {
    region: String,
    latitude: f64,
    longitude: f64,
    max_seconds: u32,
    stops: Vec<Stop>,
}
#[derive(Clone,Deserialize)]
struct Stop { stop: u32, latitude:f64, longitude:f64 }
#[derive(Serialize)]
struct Access {stop:u32,seconds:u32}
#[derive(Serialize)]
struct Response {reachable:Vec<Access>,error:Option<String>}

fn run_request(
    graphs: &BTreeMap<String,cxx::UniquePtr<ffi::Router>>,
    query:Request,
)->Result<Vec<Access>>{
    ensure!(query.latitude.is_finite() && query.longitude.is_finite(),"invalid origin");
    ensure!((1..=3600).contains(&query.max_seconds),"invalid OSR duration budget");
    ensure!(query.stops.len()<=4096,"too many stops for one OSR query");
    let graph=graphs.get(&query.region).context("OSR region is not loaded")?;
    let router=graph.as_ref().context("OSR graph is null")?;
    let options=ffi::RoutingOptions {
        max_cost:query.max_seconds,include_segments:false,
        ..ffi::RoutingOptions::default().profile(Profile::Foot)
    };
    let mut targets=Vec::with_capacity(query.stops.len());
    for x in &query.stops {
        ensure!(x.latitude.is_finite()&&x.longitude.is_finite(),"invalid target");
        targets.push(ffi::Coordinate::outdoor(x.latitude,x.longitude));
    }
    let paths=router.one_to_many(
        &ffi::Coordinate::outdoor(query.latitude,query.longitude),
        &targets,&options,&[]
    )?;
    let mut results=Vec::new();
    for (stop,path) in query.stops.into_iter().zip(paths) {
        if path.found&&path.duration_seconds<=query.max_seconds{
            results.push(Access{
                stop:stop.stop,
                seconds:path.duration_seconds,
            });
        }
    }
    results.sort_by_key(|x|(x.seconds,x.stop));
    Ok(results)
}

fn handle(stream:TcpStream,graphs:&BTreeMap<String,cxx::UniquePtr<ffi::Router>>)->Result<()>{
    stream.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    let mut reader=BufReader::new(stream.try_clone()?);
    let mut input=String::new();
    reader.read_line(&mut input)?;
    ensure!(input.len()<=524_288,"OSR request exceeds 512KiB");
    let response=match serde_json::from_str::<Request>(&input){
        Ok(req)=>match run_request(graphs,req){
            Ok(reachable)=>Response{reachable,error:None},
            Err(e)=>Response{reachable:Vec::new(),error:Some(e.to_string())},
        },
        Err(e)=>Response{reachable:Vec::new(),error:Some(e.to_string())},
    };
    let mut writer=BufWriter::new(stream);
    serde_json::to_writer(&mut writer,&response)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

/// Resolve graph directories in the acceptor thread; CXX native routers are
/// opened independently inside their owning worker threads.
fn region_paths(data_dir: &std::path::Path) -> Result<BTreeMap<String, PathBuf>> {
    let registry = data_dir.join("regions.geojson");
    let document: serde_json::Value = serde_json::from_slice(&fs::read(&registry)?)?;
    let features = document.get("features").and_then(|x| x.as_array())
        .context("OSR regions registry missing features")?;
    let mut paths = BTreeMap::new();
    for feature in features {
        let id = feature.get("properties").and_then(|x| x.get("id"))
            .and_then(|x| x.as_str()).context("OSR registry entry missing id")?;
        ensure!(!id.split('/').any(|p| p == ".." || p == "." || p.is_empty()),
            "invalid OSR id");
        ensure!(paths.insert(id.to_owned(), data_dir.join("regions").join(id)).is_none(),
            "duplicate OSR region ID: {id}");
    }
    ensure!(!paths.is_empty(), "no OSR regions configured");
    Ok(paths)
}

/// Loaded once on each worker thread, so native routing state never crosses
/// a Rust Send/Sync boundary. Linux page cache backs the same OSR graph files
/// across workers, but per-router mutable native memory is not shared.
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
    ensure!((1..=16).contains(&args.workers), "--workers must be in 1..=16");
    let cfg: Config = toml::from_str(&fs::read(&args.config)?)?;
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
        thread::Builder::new().name(format!("osr-query-{worker}")).spawn(move || {
            let graphs = match open_graphs(&paths) {
                Ok(g) => {
                    let _ = ready.send(Ok(()));
                    g
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return;
                }
            };
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
        }).context("spawning OSR worker")?;
    }
    drop(ready_tx);
    // Do not advertise a listening worker until all routers have opened.
    for _ in 0..args.workers {
        ready_rx.recv().context("OSR worker failed to initialize")??;
    }
    eprintln!("OSR worker listening at {} ({} concurrent queries)",
        args.listen, args.workers);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => send.send(stream).context("OSR worker queue closed")?,
            Err(error) => eprintln!("OSR accept: {error}"),
        }
    }
    Ok(())
}
