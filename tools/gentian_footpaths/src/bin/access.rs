use anyhow::{Context, Result, ensure};
use clap::Parser;
use osr_ffi::{ffi, Profile};
use serde::Serialize;
use std::{fs::File, io::{BufRead, BufReader}, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    osr_dir: PathBuf,
    #[arg(long)]
    stops: PathBuf,
    #[arg(long)]
    latitude: f64,
    #[arg(long)]
    longitude: f64,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t=1800.0)]
    radius_meters: f64,
    #[arg(long, default_value_t=900)]
    max_seconds: u32,
}
#[derive(Serialize)]
struct Access { stop: u32, minutes: u32, seconds: u32 }
fn meters(a:f64,b:f64,c:f64,d:f64)->f64{
    let dy=(c-a).to_radians();
    let dx=(d-b).to_radians();
    let v=(dy/2.0).sin().powi(2)+a.to_radians().cos()*c.to_radians().cos()*(dx/2.0).sin().powi(2);
    12_742_000.0*v.sqrt().asin()
}
fn main()->Result<()>{
    let args=Args::parse();
    ensure!(args.latitude.is_finite() && args.longitude.is_finite(),"invalid coordinates");
    let reader=BufReader::new(File::open(&args.stops)?);
    let mut candidates=Vec::new();
    let mut targets=Vec::new();
    for line in reader.lines() {
        let line=line?;
        let mut it=line.split('\t');
        let idx:u32=it.next().context("missing stop id")?.parse()?;
        let lat:f64=it.next().context("missing stop latitude")?.parse()?;
        let lon:f64=it.next().context("missing stop longitude")?.parse()?;
        if meters(args.latitude,args.longitude,lat,lon)<=args.radius_meters {
            candidates.push(idx);
            targets.push(ffi::Coordinate::outdoor(lat,lon));
        }
    }
    let router=ffi::open_router(args.osr_dir.to_str().context("non-UTF8 OSR path")?)?;
    let r=router.as_ref().context("missing OSR router")?;
    let opts=ffi::RoutingOptions{
        max_cost:args.max_seconds,include_segments:false,
        ..ffi::RoutingOptions::default().profile(Profile::Foot)
    };
    let result=r.one_to_many(&ffi::Coordinate::outdoor(args.latitude,args.longitude),&targets,&opts,&[])?;
    let mut output=Vec::new();
    for (idx,path) in candidates.into_iter().zip(result) {
        if path.found && path.duration_seconds<=args.max_seconds {
            output.push(Access{stop:idx,minutes:path.duration_seconds.div_ceil(60),seconds:path.duration_seconds});
        }
    }
    output.sort_by_key(|x|(x.seconds,x.stop));
    std::fs::write(&args.output,serde_json::to_vec_pretty(&output)?)?;
    eprintln!("OSR access: {} reachable stops",output.len());
    Ok(())
}