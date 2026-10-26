#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod motis_response;

use actix_web::{App, HttpResponse, HttpServer, web};
use anyhow::{Context, Result};
use catenary::routing_common::metadata_index::MetadataIndex;
use catenary::routing_common::raptor_query::Leg;
use catenary::routing_common::raptor_query::WalkingEndpoint;
use catenary::routing_common::route_manager::{
    Coordinates, Router, RouterSettings, RoutingResponse,
};
use catenary::routing_common::timetable::LocationIdx;
use clap::Parser;
use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Instant;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::Arc,
};

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
struct Endpoint {
    stop: u32,
    seconds: u32,
}

#[derive(Deserialize)]
struct Request {
    from: Coordinates,
    to: Coordinates,
    departure_unix_seconds: i64,
    #[serde(default)]
    access: Vec<Endpoint>,
    #[serde(default)]
    egress: Vec<Endpoint>,
    direct_walk_seconds: Option<u32>,
    #[serde(default = "default_detailed_legs")]
    detailed_legs: bool,
}
fn default_detailed_legs() -> bool { true }

#[derive(Serialize)]
struct RouteStyle {
    route: u32,
    trip_id: String,
    feed_id: String,
    route_id: String,
    short_name: Option<String>,
    long_name: Option<String>,
    color: Option<String>,
    text_color: Option<String>,
}

#[derive(Serialize)]
struct Answer {
    routing: RoutingResponse,
    route_styles: Vec<RouteStyle>,
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
    osr_parallel_wall_ms: f64,
    raptor_ms: f64,
    metadata_ms: f64,
    query_total_ms: f64,
}

fn milliseconds(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

struct State {
    router: Arc<Router>,
    database_url: Option<String>,
    osr_worker: String,
}

/// Collect one metadata association per RAPTOR route, matching the existing
/// BTreeMap overwrite order when several transports of one route are returned.
fn requested_route_metadata(
    tt: &catenary::routing_common::timetable::ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    journeys: &[catenary::routing_common::raptor_query::Journey],
) -> BTreeMap<u32, (String, String, String, String)> {
    let mut requested = BTreeSet::new();
    for journey in journeys {
        for leg in &journey.legs {
            if let Leg::Ride {
                route, transport, ..
            } = leg
            {
                requested.insert((*route, *transport));
            }
        }
    }

    let mut metadata = BTreeMap::new();
    for (route, transport) in requested {
        let Some(trip) = index.trip_for_transport(transport) else {
            continue;
        };
        let Some(public_id) = tt.trip_public_route_ids().get(trip) else {
            continue;
        };
        let Some(public) = tt.route_ids().get(public_id.idx()) else {
            continue;
        };
        let Some(raw_route) = tt.get_string(public.id) else {
            continue;
        };
        let Some((_, feed, attempt)) = tt.source(public.source) else {
            continue;
        };
        let trip_id = index
            .id_record_for_trip(trip)
            .and_then(|idx| tt.trip_ids().get(idx))
            .and_then(|id| tt.get_string(id.id))
            .unwrap_or("");
        metadata.insert(
            route,
            (
                trip_id.to_owned(),
                feed.to_owned(),
                attempt.to_owned(),
                raw_route.to_owned(),
            ),
        );
    }
    metadata
}

fn hydrate(
    tt: &catenary::routing_common::timetable::ZeroCopyTimetable<'_>,
    index: &MetadataIndex,
    journeys: &[catenary::routing_common::raptor_query::Journey],
    url: Option<&str>,
) -> Result<Vec<RouteStyle>> {
    let Some(url) = url else {
        return Ok(Vec::new());
    };
    let requested = requested_route_metadata(tt, index, journeys);
    if requested.is_empty() {
        return Ok(Vec::new());
    };

    // A single round trip for all route style keys. Keep source identities
    // (feed and attempt) together; raw route IDs can repeat across GTFS feeds.
    let mut feeds = Vec::with_capacity(requested.len());
    let mut attempts = Vec::with_capacity(requested.len());
    let mut routes = Vec::with_capacity(requested.len());
    for (_, feed, attempt, route) in requested.values() {
        feeds.push(feed.clone());
        attempts.push(attempt.clone());
        routes.push(route.clone());
    }
    let mut client = Client::connect(url, NoTls)?;
    let rows = client.query(
        "SELECT DISTINCT ON (r.onestop_feed_id, r.attempt_id, r.route_id) \
                r.onestop_feed_id, r.attempt_id, r.route_id, \
                r.short_name, r.long_name, r.color, r.text_color \
         FROM gtfs.routes AS r \
         JOIN unnest($1::text[], $2::text[], $3::text[]) \
              AS requested(feed, attempt, route_id) \
           ON r.onestop_feed_id = requested.feed \
          AND r.attempt_id = requested.attempt \
          AND r.route_id = requested.route_id \
         ORDER BY r.onestop_feed_id, r.attempt_id, r.route_id",
        &[&feeds, &attempts, &routes],
    )?;
    let mut style_by_key = BTreeMap::new();
    for row in rows {
        let feed: String = row.get(0);
        let attempt: String = row.get(1);
        let route: String = row.get(2);
        let fields: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = (row.get(3), row.get(4), row.get(5), row.get(6));
        style_by_key.insert((feed, attempt, route), fields);
    }

    Ok(requested
        .into_iter()
        .map(|(route, (trip_id, feed_id, attempt, route_id))| {
            let (short_name, long_name, color, text_color) = style_by_key
                .get(&(feed_id.clone(), attempt, route_id.clone()))
                .cloned()
                .unwrap_or((None, None, None, None));
            RouteStyle {
                route,
                trip_id,
                feed_id,
                route_id,
                short_name,
                long_name,
                color,
                text_color,
            }
        })
        .collect())
}

#[derive(Serialize)]
struct OsrStop {
    stop: u32,
    latitude: f64,
    longitude: f64,
}
#[derive(Serialize)]
struct OsrRequest<'a> {
    region: &'a str,
    direction: u8,
    latitude: f64,
    longitude: f64,
    max_seconds: u32,
    stops: Vec<OsrStop>,
}
#[derive(Deserialize)]
struct OsrAccess {
    stop: u32,
    seconds: u32,
}
#[derive(Deserialize)]
struct OsrResponse {
    reachable: Vec<OsrAccess>,
    error: Option<String>,
}

fn osr_access(
    router: &Router,
    addr: &str,
    transit_region: &str,
    osr_region: &str,
    point: Coordinates,
    direction: u8,
) -> Result<Vec<WalkingEndpoint>> {
    let nearest = router.nearest_stops(transit_region, point, 4096)?;
    let table = router.timetable_for(transit_region)?;
    let stops = nearest
        .into_iter()
        .map(|(id, _)| {
            let c = table.coordinates()[id.idx()];
            OsrStop {
                stop: id.0,
                latitude: f64::from(c.lat),
                longitude: f64::from(c.lon),
            }
        })
        .collect();
    let request = OsrRequest {
        region: osr_region,
        direction,
        latitude: point.lat,
        longitude: point.lon,
        max_seconds: 1800,
        stops,
    };
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    serde_json::to_writer(&mut stream, &request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let reply: OsrResponse = serde_json::from_str(&line)?;
    if let Some(err) = reply.error {
        anyhow::bail!("OSR query: {err}");
    }
    Ok(reply
        .reachable
        .into_iter()
        .map(|x| WalkingEndpoint {
            stop: LocationIdx(x.stop),
            seconds: x.seconds,
        })
        .collect())
}

fn direct_osr_seconds(
    addr: &str,
    region: &str,
    from: Coordinates,
    to: Coordinates,
) -> Result<Option<u32>> {
    let request = OsrRequest {
        region,
        direction: 0,
        latitude: from.lat,
        longitude: from.lon,
        max_seconds: 3600,
        stops: vec![OsrStop {
            stop: 0,
            latitude: to.lat,
            longitude: to.lon,
        }],
    };
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(45)))?;
    serde_json::to_writer(&mut stream, &request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let reply: OsrResponse = serde_json::from_str(&line)?;
    if let Some(error) = reply.error {
        anyhow::bail!("OSR direct route: {error}");
    }
    Ok(reply.reachable.first().map(|x| x.seconds))
}

fn run_query(
    router: &Router,
    request: Request,
    url: Option<&str>,
    osr_worker: &str,
) -> Result<(serde_json::Value, QueryTimings)> {
    let query_start = Instant::now();
    let origin = request.from;
    let destination = request.to;
    let detailed_legs = request.detailed_legs;
    let mut timings = QueryTimings::default();

    let phase = Instant::now();
    let selection = router.select(request.from, request.to)?;
    timings.selection_ms = milliseconds(phase);

    // Access, egress and direct walking have no data dependencies.
    // Use Rayon's existing bounded pool, rather than spawning up to three
    // operating-system threads per HTTP request. Native routers are isolated
    // by the OSR worker's own fixed-size worker pool.
    let osr_start = Instant::now();
    let (access, (egress, direct)) = rayon::join(
        || {
            let start = Instant::now();
            let result = if request.access.is_empty() {
                osr_access(
                    router,
                    osr_worker,
                    &selection.transit_region,
                    &selection.origin_osr_region,
                    request.from,
                    0,
                )
            } else {
                Ok(request
                    .access
                    .iter()
                    .map(|e| WalkingEndpoint {
                        stop: LocationIdx(e.stop),
                        seconds: e.seconds,
                    })
                    .collect())
            };
            (result, milliseconds(start))
        },
        || {
            rayon::join(
                || {
                    let start = Instant::now();
                    let result = if request.egress.is_empty() {
                        osr_access(
                            router,
                            osr_worker,
                            &selection.transit_region,
                            &selection.destination_osr_region,
                            request.to,
                            1,
                        )
                    } else {
                        Ok(request
                            .egress
                            .iter()
                            .map(|e| WalkingEndpoint {
                                stop: LocationIdx(e.stop),
                                seconds: e.seconds,
                            })
                            .collect())
                    };
                    (result, milliseconds(start))
                },
                || {
                    let start = Instant::now();
                    let result = if let Some(seconds) = request.direct_walk_seconds {
                        Ok(Some(seconds))
                    } else if selection.origin_osr_region == selection.destination_osr_region {
                        direct_osr_seconds(
                            osr_worker,
                            &selection.origin_osr_region,
                            request.from,
                            request.to,
                        )
                    } else {
                        Ok(None)
                    };
                    (result, milliseconds(start))
                },
            )
        },
    );
    timings.osr_parallel_wall_ms = milliseconds(osr_start);
    timings.access_ms = access.1;
    timings.egress_ms = egress.1;
    timings.direct_walk_ms = direct.1;
    let from = access.0?;
    let to = egress.0?;
    let direct = direct.0?;

    let phase = Instant::now();
    let routing =
        router.route_with_access(selection, request.departure_unix_seconds, from, to, direct)?;
    timings.raptor_ms = milliseconds(phase);

    let phase = Instant::now();
    let timetable = router.timetable_for(&routing.selection.transit_region)?;
    let index = router.metadata_for(&routing.selection.transit_region)?;
    let response = motis_response::build(
        &timetable, index, &routing.journeys,
        origin, destination,
        &routing.selection.origin_osr_region,
        &routing.selection.destination_osr_region,
        osr_worker, url, detailed_legs,
    )?;
    timings.metadata_ms = milliseconds(phase);
    timings.query_total_ms = milliseconds(query_start);

    Ok((
        response,
        timings,
    ))
}

async fn route(state: web::Data<State>, payload: web::Json<Request>) -> HttpResponse {
    let state = state.into_inner();
    let request = payload.into_inner();
    let result = web::block(move || {
        run_query(
            &state.router,
            request,
            state.database_url.as_deref(),
            &state.osr_worker,
        )
    })
    .await;
    match result {
        Ok(Ok((answer, _timings))) => HttpResponse::Ok().json(answer),
        Ok(Err(err)) => HttpResponse::UnprocessableEntity().body(err.to_string()),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = catenary::routing_common::routing_regions::RoutingRegionsConfig::load(
        &args.regions_config,
    )?;
    let settings = RouterSettings {
        data_dir: args.data_dir.unwrap_or(config.data_dir),
        access_radius_meters: 5000.0,
        max_boardings: 5,
        max_duration_seconds: 360 * 60,
    };
    let router = Arc::new(Router::load(
        &args.regions_config,
        &args.catenary_config,
        settings,
    )?);
    if let Some(path) = args.query {
        let timing_output = args.query_timings.clone();
        let request: Request = serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )?;
        // The synchronous postgres client creates and blocks on its own Tokio
        // runtime. Running it on an Actix worker thread would nest runtimes and
        // panic; use the same blocking pool as the HTTP route handler.
        let (response, timings) = web::block(move || {
            run_query(
                &router,
                request,
                args.database_url.as_deref(),
                &args.osr_worker,
            )
        })
        .await
        .context("CLI route-query worker failed")??;
        if let Some(path) = &timing_output {
            fs::write(path, serde_json::to_vec_pretty(&timings)?)
                .with_context(|| format!("writing query timings to {}", path.display()))?;
        }
        eprintln!(
            "Edelweiss query: total {:.3} ms; RAPTOR {:.3} ms; concurrent OSR wall {:.3} ms; OSR access {:.3} ms; OSR egress {:.3} ms; OSR direct {:.3} ms; metadata {:.3} ms",
            timings.query_total_ms,
            timings.raptor_ms,
            timings.osr_parallel_wall_ms,
            timings.access_ms,
            timings.egress_ms,
            timings.direct_walk_ms,
            timings.metadata_ms
        );
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let state = web::Data::new(State {
        router,
        database_url: args.database_url,
        osr_worker: args.osr_worker,
    });
    HttpServer::new(move || {
        App::new()
            .wrap(
                actix_cors::Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header(),
            )
            .app_data(state.clone())
            .route("/v1/route", web::post().to(route))
    })
    .bind(&args.listen)?
    .run()
    .await?;
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
        assert_eq!(
            result.unwrap(),
            true,
            "PostgreSQL should fail normally, not panic"
        );
    }
}
