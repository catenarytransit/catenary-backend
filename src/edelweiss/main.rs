#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::{Context, Result};
use catenary::routing_common::route_manager::{
    Coordinates, Router, RouterSettings, RoutingResponse,
};
use catenary::routing_common::raptor_query::WalkingEndpoint;
use catenary::routing_common::timetable::LocationIdx;
use catenary::routing_common::raptor_query::Leg;
use clap::Parser;
use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Instant;
use std::{collections::{BTreeMap, BTreeSet}, fs, path::PathBuf, sync::Arc};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "routing_regions.toml")]
    regions_config: PathBuf,
    #[arg(long, default_value = "catenaryconfig.toml")]
    catenary_config: PathBuf,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, default_value = "127.0.0.1:40428")]
    listen: String,
    #[arg(long)]
    query: Option<PathBuf>,
    /// Write per-phase query timings as JSON; only available with --query.
    #[arg(long, requires = "query")]
    query_timings: Option<PathBuf>,
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, default_value = "127.0.0.1:40501")]
    osr_worker: String,
}

#[derive(Deserialize)]
struct Endpoint { stop:u32, seconds:u32 }

#[derive(Deserialize)]
struct Request {
    from: Coordinates,
    to: Coordinates,
    departure_unix_seconds:i64,
    #[serde(default)]
    access:Vec<Endpoint>,
    #[serde(default)]
    egress:Vec<Endpoint>,
    direct_walk_seconds:Option<u32>,
}

#[derive(Serialize)]
struct RouteStyle {
    route:u32, trip_id:String, feed_id:String, route_id:String,
    short_name:Option<String>, long_name:Option<String>,
    color:Option<String>,text_color:Option<String>,
}

#[derive(Serialize)]
struct Answer {
    routing:RoutingResponse,
    route_styles:Vec<RouteStyle>,
}

/// Milliseconds measured within the query engine, excluding process startup.
/// OSR access/egress and PostgreSQL metadata lookup are tracked separately
/// from the RAPTOR algorithm itself.
#[derive(Default, Serialize)]
struct QueryTimings {
    selection_ms: f64,
    access_ms: f64,
    egress_ms: f64,
    direct_walk_ms: f64,
    raptor_ms: f64,
    metadata_ms: f64,
    query_total_ms: f64,
}

fn milliseconds(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

struct State {router:Arc<Router>,database_url:Option<String>,osr_worker:String}

fn hydrate(
    tt:&catenary::routing_common::timetable::ZeroCopyTimetable<'_>,
    journeys:&[catenary::routing_common::raptor_query::Journey],
    url:Option<&str>,
) -> Result<Vec<RouteStyle>> {
    let Some(url)=url else {return Ok(Vec::new())};
    let mut client=Client::connect(url,NoTls)?;
    let mut requested=BTreeSet::new();
    for j in journeys {
        for leg in &j.legs {
            if let Leg::Ride{route,transport,..}=leg { requested.insert((*route,*transport)); }
        }
    }
    let mut styles=BTreeMap::new();
    for (route,transport) in requested {
        let trip=tt.trip_transport_offsets().windows(2).enumerate().find_map(|(idx,r)|{
            tt.trip_transport_ranges()[r[0] as usize..r[1] as usize]
                .iter().any(|x|x.transport.0==transport).then_some(idx)
        });
        let Some(trip)=trip else {continue};
        let Some(route_id_idx)=tt.trip_public_route_ids().get(trip) else {continue};
        let Some(public)=tt.route_ids().get(route_id_idx.idx()) else {continue};
        let Some(raw_route)=tt.get_string(public.id) else {continue};
        let Some((_,feed,attempt))=tt.source(public.source) else {continue};
        let gtfs_trip=tt.trip_ids().iter().find(|x|x.trip.idx()==trip)
            .and_then(|x|tt.get_string(x.id)).unwrap_or("");
        let rows=client.query(
            "SELECT short_name,long_name,color,text_color FROM gtfs.routes
             WHERE onestop_feed_id=$1 AND attempt_id=$2 AND route_id=$3 LIMIT 1",
            &[&feed,&attempt,&raw_route])?;
        let (short_name,long_name,color,text_color)=if let Some(row)=rows.first(){
            (row.get(0),row.get(1),row.get(2),row.get(3))
        } else {(None,None,None,None)};
        styles.insert(route,RouteStyle{
            route,trip_id:gtfs_trip.to_owned(),feed_id:feed.to_owned(),
            route_id:raw_route.to_owned(),short_name,long_name,color,text_color,
        });
    }
    Ok(styles.into_values().collect())
}


#[derive(Serialize)]
struct OsrStop {stop:u32,latitude:f64,longitude:f64}
#[derive(Serialize)]
struct OsrRequest<'a>{
    region:&'a str,latitude:f64,longitude:f64,max_seconds:u32,stops:Vec<OsrStop>,
}
#[derive(Deserialize)]
struct OsrAccess {stop:u32,seconds:u32}
#[derive(Deserialize)]
struct OsrResponse {reachable:Vec<OsrAccess>,error:Option<String>}

fn osr_access(
    router:&Router,
    addr:&str,
    transit_region:&str,
    osr_region:&str,
    point:Coordinates,
)->Result<Vec<WalkingEndpoint>>{
    let nearest=router.nearest_stops(transit_region,point,4096)?;
    let table=router.timetable_for(transit_region)?;
    let stops=nearest.into_iter().map(|(id,_)|{
        let c=table.coordinates()[id.idx()];
        OsrStop{stop:id.0,latitude:f64::from(c.lat),longitude:f64::from(c.lon)}
    }).collect();
    let request=OsrRequest{
        region:osr_region,latitude:point.lat,longitude:point.lon,
        max_seconds:1800,stops,
    };
    let mut stream=TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    serde_json::to_writer(&mut stream,&request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut line=String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let reply:OsrResponse=serde_json::from_str(&line)?;
    if let Some(err)=reply.error { anyhow::bail!("OSR query: {err}"); }
    Ok(reply.reachable.into_iter()
        .map(|x|WalkingEndpoint{stop:LocationIdx(x.stop),seconds:x.seconds}).collect())
}

fn direct_osr_seconds(
    addr:&str,
    region:&str,
    from:Coordinates,
    to:Coordinates,
)->Result<Option<u32>>{
    let request=OsrRequest{
        region,latitude:from.lat,longitude:from.lon,max_seconds:3600,
        stops:vec![OsrStop{stop:0,latitude:to.lat,longitude:to.lon}],
    };
    let mut stream=TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    serde_json::to_writer(&mut stream,&request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut line=String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let reply:OsrResponse=serde_json::from_str(&line)?;
    if let Some(error)=reply.error { anyhow::bail!("OSR direct route: {error}"); }
    Ok(reply.reachable.first().map(|x|x.seconds))
}

fn run_query(
    router: &Router,
    request: Request,
    url: Option<&str>,
    osr_worker: &str,
) -> Result<(Answer, QueryTimings)> {
    let query_start = Instant::now();
    let mut timings = QueryTimings::default();

    let phase = Instant::now();
    let selection = router.select(request.from, request.to)?;
    timings.selection_ms = milliseconds(phase);

    let phase = Instant::now();
    let from = if request.access.is_empty() {
        osr_access(router, osr_worker, &selection.transit_region,
            &selection.origin_osr_region, request.from)?
    } else {
        request.access.into_iter().map(|e| WalkingEndpoint {
            stop: LocationIdx(e.stop), seconds: e.seconds,
        }).collect()
    };
    timings.access_ms = milliseconds(phase);

    let phase = Instant::now();
    let to = if request.egress.is_empty() {
        osr_access(router, osr_worker, &selection.transit_region,
            &selection.destination_osr_region, request.to)?
    } else {
        request.egress.into_iter().map(|e| WalkingEndpoint {
            stop: LocationIdx(e.stop), seconds: e.seconds,
        }).collect()
    };
    timings.egress_ms = milliseconds(phase);

    let phase = Instant::now();
    let direct = if request.direct_walk_seconds.is_some() {
        request.direct_walk_seconds
    } else if selection.origin_osr_region == selection.destination_osr_region {
        direct_osr_seconds(osr_worker, &selection.origin_osr_region,
            request.from, request.to)?
    } else {
        None
    };
    timings.direct_walk_ms = milliseconds(phase);

    let phase = Instant::now();
    let routing = router.route_with_access(selection, request.departure_unix_seconds,
        from, to, direct)?;
    timings.raptor_ms = milliseconds(phase);

    let phase = Instant::now();
    let timetable = router.timetable_for(&routing.selection.transit_region)?;
    let route_styles = hydrate(&timetable, &routing.journeys, url)?;
    timings.metadata_ms = milliseconds(phase);
    timings.query_total_ms = milliseconds(query_start);

    Ok((Answer { routing, route_styles }, timings))
}

async fn route(state:web::Data<State>,payload:web::Json<Request>)->HttpResponse{
    let state=state.into_inner();
    let request=payload.into_inner();
    let result=web::block(move||run_query(&state.router,request,state.database_url.as_deref(),&state.osr_worker)).await;
    match result {
        Ok(Ok((answer, _timings)))=>HttpResponse::Ok().json(answer),
        Ok(Err(err))=>HttpResponse::UnprocessableEntity().body(err.to_string()),
        Err(err)=>HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[actix_web::main]
async fn main()->Result<()>{
    let args=Args::parse();
    let config=catenary::routing_common::routing_regions::RoutingRegionsConfig::load(&args.regions_config)?;
    let settings=RouterSettings{
        data_dir:args.data_dir.unwrap_or(config.data_dir),
        access_radius_meters:5000.0,max_boardings:5,max_duration_seconds:360 * 60,
    };
    let router=Arc::new(Router::load(&args.regions_config,&args.catenary_config,settings)?);
    if let Some(path)=args.query {
        let timing_output = args.query_timings.clone();
        let request:Request=serde_json::from_slice(&fs::read(&path)
            .with_context(||format!("reading {}",path.display()))?)?;
        // The synchronous postgres client creates and blocks on its own Tokio
        // runtime. Running it on an Actix worker thread would nest runtimes and
        // panic; use the same blocking pool as the HTTP route handler.
        let (response, timings) = web::block(move || {
            run_query(&router, request, args.database_url.as_deref(), &args.osr_worker)
        })
        .await
        .context("CLI route-query worker failed")??;
        if let Some(path) = &timing_output {
            fs::write(path, serde_json::to_vec_pretty(&timings)?)
                .with_context(|| format!("writing query timings to {}", path.display()))?;
        }
        eprintln!(
            "Edelweiss query: total {:.3} ms; RAPTOR {:.3} ms; OSR access {:.3} ms; OSR egress {:.3} ms; OSR direct {:.3} ms; metadata {:.3} ms",
            timings.query_total_ms, timings.raptor_ms, timings.access_ms,
            timings.egress_ms, timings.direct_walk_ms, timings.metadata_ms
        );
        println!("{}",serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let state=web::Data::new(State{router,database_url:args.database_url,osr_worker:args.osr_worker});
    HttpServer::new(move||App::new().app_data(state.clone()).route("/v1/route",web::post().to(route)))
        .bind(&args.listen)?.run().await?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn postgres_client_runs_outside_actix_runtime() {
        // The synchronous postgres crate starts an internal Tokio runtime.
        // Verify the blocking worker is outside the HTTP executor; an
        // unreachable port is sufficient and requires no database fixture.
        let result = web::block(|| {
            Client::connect(
                "host=127.0.0.1 port=1 user=postgres dbname=unavailable connect_timeout=1",
                NoTls,
            )
            .is_err()
        })
        .await;
        assert_eq!(result.unwrap(), true, "PostgreSQL should fail normally, not panic");
    }
}
