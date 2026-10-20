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
};

#[derive(Parser)]
struct Args {
    #[arg(long,default_value="catenaryconfig.toml")]
    config: PathBuf,
    #[arg(long,default_value="127.0.0.1:40501")]
    listen: String,
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
struct Access {stop:u32,minutes:u32,seconds:u32}
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
                minutes:path.duration_seconds.div_ceil(60),
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

fn main()->Result<()> {
    let args=Args::parse();
    let cfg:Config=toml::from_str(&fs::read_to_string(&args.config)?)?;
    let registry=cfg.osm_manager.data_dir.join("regions.geojson");
    let document:serde_json::Value=serde_json::from_slice(&fs::read(&registry)?)?;
    let features=document.get("features").and_then(|x|x.as_array())
        .context("OSR regions registry missing features")?;
    let mut graphs=BTreeMap::new();
    for feature in features {
        let id=feature.get("properties").and_then(|x|x.get("id"))
            .and_then(|x|x.as_str()).context("OSR registry entry missing id")?;
        ensure!(!id.split('/').any(|p|p==".."||p=="."||p.is_empty()),"invalid OSR id");
        let dir=cfg.osm_manager.data_dir.join("regions").join(id);
        let router=ffi::open_router(dir.to_str().context("non-UTF8 OSR path")?)?;
        ensure!(!router.is_null(),"OSR graph is null: {id}");
        eprintln!("Opened native OSR mmap: {id}");
        graphs.insert(id.to_owned(),router);
    }
    ensure!(!graphs.is_empty(),"no OSR regions loaded");
    let listener=TcpListener::bind(&args.listen)?;
    eprintln!("OSR worker listening at {}",args.listen);
    for stream in listener.incoming() {
        match stream {
            Ok(stream)=>if let Err(e)=handle(stream,&graphs){eprintln!("OSR client: {e}")},
            Err(e)=>eprintln!("OSR accept: {e}"),
        }
    }
    Ok(())
}