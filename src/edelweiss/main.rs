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
use std::{collections::{BTreeMap, BTreeSet}, fs, path::PathBuf, sync::Arc};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "routing_regions/config.toml")]
    regions_config: PathBuf,
    #[arg(long, default_value = "catenaryconfig.toml")]
    catenary_config: PathBuf,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, default_value = "127.0.0.1:40428")]
    listen: String,
    #[arg(long)]
    query: Option<PathBuf>,
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
}

#[derive(Deserialize)]
struct Endpoint { stop:u32, minutes:u32 }

#[derive(Deserialize)]
struct Request {
    from: Coordinates,
    to: Coordinates,
    departure_unix_seconds:i64,
    access:Vec<Endpoint>,
    egress:Vec<Endpoint>,
    direct_walk_minutes:Option<u32>,
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

struct State {router:Arc<Router>,database_url:Option<String>}

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

fn run_query(router:&Router,request:Request,url:Option<&str>)->Result<Answer>{
    let selection=router.select(request.from,request.to)?;
    let from=request.access.into_iter().map(|e| WalkingEndpoint{
        stop:LocationIdx(e.stop),minutes:e.minutes}).collect();
    let to=request.egress.into_iter().map(|e|WalkingEndpoint{
        stop:LocationIdx(e.stop),minutes:e.minutes}).collect();
    let routing=router.route_with_access(selection,request.departure_unix_seconds,from,to,
        request.direct_walk_minutes)?;
    let timetable=router.timetable_for(&routing.selection.transit_region)?;
    let route_styles=hydrate(&timetable,&routing.journeys,url)?;
    Ok(Answer{routing,route_styles})
}

async fn route(state:web::Data<State>,payload:web::Json<Request>)->HttpResponse{
    let state=state.into_inner();
    let request=payload.into_inner();
    let result=web::block(move||run_query(&state.router,request,state.database_url.as_deref())).await;
    match result {
        Ok(Ok(answer))=>HttpResponse::Ok().json(answer),
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
        access_radius_meters:1800.0,max_boardings:5,max_duration_minutes:360,
    };
    let router=Arc::new(Router::load(&args.regions_config,&args.catenary_config,settings)?);
    if let Some(path)=args.query {
        let request:Request=serde_json::from_slice(&fs::read(&path)
            .with_context(||format!("reading {}",path.display()))?)?;
        let response=run_query(&router,request,args.database_url.as_deref())?;
        println!("{}",serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let state=web::Data::new(State{router,database_url:args.database_url});
    HttpServer::new(move||App::new().app_data(state.clone()).route("/v1/route",web::post().to(route)))
        .bind(&args.listen)?.run().await?;
    Ok(())
}
