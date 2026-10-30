#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod motis_response;

use actix_web::{App, HttpResponse, HttpServer, web};
use anyhow::{Context, Result};
use catenary::routing_common::profile_search::ProfileOptions;
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
use std::{fs, path::PathBuf, sync::Arc};

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

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StreetMode { Walk, Bike }
impl StreetMode { fn name(self) -> &'static str {
    match self { Self::Walk => "WALK", Self::Bike => "BIKE" }
} }
#[derive(Deserialize)]
struct Request {
    from: Coordinates,
    to: Coordinates,
    #[serde(default)]
    departure_unix_seconds: Option<i64>,
    #[serde(default)]
    arrival_unix_seconds: Option<i64>,
    #[serde(default, rename = "preTransitModes")]
    pre_transit_modes: Vec<StreetMode>,
    #[serde(default, rename = "postTransitModes")]
    post_transit_modes: Vec<StreetMode>,
    #[serde(default, rename = "pedestrianSpeed")]
    pedestrian_speed: Option<f32>,
    #[serde(default, rename = "cyclingSpeed")]
    cycling_speed: Option<f32>,
    #[serde(default, rename = "maxPreTransitTime")]
    max_pre_transit_time: Option<u32>,
    #[serde(default, rename = "maxPostTransitTime")]
    max_post_transit_time: Option<u32>,
    #[serde(default)]
    access: Vec<Endpoint>,
    #[serde(default)]
    egress: Vec<Endpoint>,
    direct_walk_seconds: Option<u32>,
    #[serde(default = "default_timetable_view")]
    timetable_view: bool,
    #[serde(default)]
    search_window_seconds: Option<u32>,
    #[serde(default)]
    max_search_window_seconds: Option<u32>,
    #[serde(default)]
    min_itineraries: Option<usize>,
    #[serde(default)]
    max_journey_duration_seconds: Option<u32>,
    #[serde(default = "default_detailed_legs")]
    detailed_legs: bool,
}
fn default_timetable_view() -> bool {
    true
}
fn default_detailed_legs() -> bool {
    true
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
    mode: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pedestrian_speed: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cycling_speed: Option<f32>,
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
    mode: StreetMode, max_seconds: u32,
    pedestrian_speed: f32, cycling_speed: f32,
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
        max_seconds,
        stops,
        mode: mode.name(),
        pedestrian_speed: matches!(mode, StreetMode::Walk).then_some(pedestrian_speed),
        cycling_speed: matches!(mode, StreetMode::Bike).then_some(cycling_speed),
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
        max_seconds: 1800,
        mode: "WALK",
        pedestrian_speed: None,
        cycling_speed: None,
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
    // Exactly one timestamp is required. Reject ambiguity explicitly.
    let (request_time, arrive_by) =
        match (request.departure_unix_seconds, request.arrival_unix_seconds) {
            (Some(t), None) if t > 0 => (t, false),
            (None, Some(t)) if t > 0 => (t, true),
            _ => anyhow::bail!(
                "supply exactly one positive departure_unix_seconds or arrival_unix_seconds"
            ),
        };
    let pedestrian_speed = request.pedestrian_speed.unwrap_or(1.2);
    let cycling_speed = request.cycling_speed.unwrap_or(4.2);
    anyhow::ensure!(pedestrian_speed.is_finite() && (0.3..=10.0).contains(&pedestrian_speed),
        "pedestrianSpeed must be between 0.3 and 10.0 m/s");
    anyhow::ensure!(cycling_speed.is_finite() && (0.7..=20.0).contains(&cycling_speed),
        "cyclingSpeed must be between 0.7 and 20.0 m/s");
    let modes = |items: &[StreetMode]| -> anyhow::Result<StreetMode> {
        match items { [] => Ok(StreetMode::Walk), [mode] => Ok(*mode),
            _ => anyhow::bail!("only one pre/post transit mode is currently supported") }
    };
    let access_mode = modes(&request.pre_transit_modes)?;
    let egress_mode = modes(&request.post_transit_modes)?;
    let access_budget = request.max_pre_transit_time.unwrap_or(900);
    let egress_budget = request.max_post_transit_time.unwrap_or(900);
    anyhow::ensure!((1..=3600).contains(&access_budget) && (1..=3600).contains(&egress_budget),
        "pre/post transit time budgets must be between 1 and 3600 seconds");
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
                    access_mode, access_budget, pedestrian_speed, cycling_speed,
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
                            egress_mode, egress_budget, pedestrian_speed, cycling_speed,
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
    let profile_defaults = ProfileOptions::default();
    let options = ProfileOptions {
        search_window_seconds: request
            .search_window_seconds
            .unwrap_or(profile_defaults.search_window_seconds),
        max_search_window_seconds: request
            .max_search_window_seconds
            .unwrap_or(profile_defaults.max_search_window_seconds),
        min_itineraries: request
            .min_itineraries
            .unwrap_or(profile_defaults.min_itineraries),
        max_journey_duration_seconds: request
            .max_journey_duration_seconds
            .unwrap_or(profile_defaults.max_journey_duration_seconds),
    };
    let mut routing = if request.timetable_view {
        router.route_profile_with_access(
            selection,
            request_time,
            from,
            to,
            direct,
            arrive_by,
            options,
        )?
    } else if arrive_by {
        router.route_arrive_by_with_access(selection, request_time, from, to, direct)?
    } else {
        router.route_with_access(selection, request_time, from, to, direct)?
    };
    timings.raptor_ms = milliseconds(phase);
    catenary::routing_common::profile_search::sort_journeys_for_response(
        &mut routing.journeys,
        arrive_by,
    );

    let phase = Instant::now();
    let timetable = router.timetable_for(&routing.selection.transit_region)?;
    let index = router.metadata_for(&routing.selection.transit_region)?;
    let response = motis_response::build(
        &timetable,
        index,
        &routing.journeys,
        origin,
        destination,
        &routing.selection.origin_osr_region,
        &routing.selection.destination_osr_region,
        osr_worker,
        url,
        detailed_legs,
    )?;
    timings.metadata_ms = milliseconds(phase);
    timings.query_total_ms = milliseconds(query_start);

    Ok((response, timings))
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

fn resolve_database_url(
    explicit: Option<String>,
    config_path: &std::path::Path,
) -> Result<Option<String>> {
    if let Some(url) = explicit.filter(|url| !url.trim().is_empty()) {
        return Ok(Some(url));
    }
    let contents = match fs::read_to_string(config_path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", config_path.display())),
    };
    let config: catenary::catenaryconfig::CatenaryConfig =
        toml::from_str(&contents).with_context(|| format!("parsing {}", config_path.display()))?;
    Ok(config
        .postgres
        .database_url
        .filter(|url| !url.trim().is_empty()))
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
    // Match the rest of the backend: --database-url / DATABASE_URL overrides
    // [postgres].database_url in the selected catenaryconfig.toml file.
    let database_url = resolve_database_url(args.database_url.clone(), &args.catenary_config)?;
    if database_url.is_none() {
        eprintln!(
            "Warning: no PostgreSQL URL configured; route colours and other GTFS metadata will be unavailable"
        );
    }
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
            run_query(&router, request, database_url.as_deref(), &args.osr_worker)
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
        database_url,
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

    #[test]
    fn configured_postgres_url_is_used_when_no_cli_override_exists() {
        let config = std::env::temp_dir().join(format!(
            "edelweiss-db-config-{}-{}.toml",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(
            &config,
            "[postgres]\ndatabase_url = 'postgres://configured/db'\n",
        )
        .unwrap();
        assert_eq!(
            resolve_database_url(None, &config).unwrap().as_deref(),
            Some("postgres://configured/db")
        );
        assert_eq!(
            resolve_database_url(Some("postgres://cli/db".into()), &config)
                .unwrap()
                .as_deref(),
            Some("postgres://cli/db")
        );
        std::fs::remove_file(config).unwrap();
    }

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
