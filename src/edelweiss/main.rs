#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod footpath_debug;
mod motis_response;
mod pagination;
mod startup;
mod websocket;

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, web};
use anyhow::{Context, Result};
use catenary::routing_common::native_osr::{Endpoint as StreetEndpoint, NativeOsr};
use catenary::routing_common::profile_search::ProfileOptions;
use catenary::routing_common::raptor_query::{Journey, RaptorStats, WalkingEndpoint};
use catenary::routing_common::route_manager::PongControl;
use catenary::routing_common::route_manager::{Coordinates, Router, RouterSettings};
use catenary::routing_common::timetable::LocationIdx;
use clap::Parser;
use osr_ffi::Profile;
use pagination::{PageOptions, Side};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
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
    /// Maximum concurrent in-process street searches across all requests.
    #[arg(long, default_value_t = 3)]
    osr_workers: usize,
    /// Opt-in, read-only transfer-closure inspection. Disabled in production by default.
    #[arg(long, env = "EDELWEISS_ENABLE_FOOTPATH_DEBUG")]
    enable_footpath_debug: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct Endpoint {
    stop: u32,
    seconds: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StreetMode {
    Walk,
    Bike,
}
impl StreetMode {
    fn profile(self) -> Profile {
        match self {
            Self::Walk => Profile::Foot,
            Self::Bike => Profile::BikeFast,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Walk => "WALK",
            Self::Bike => "BIKE",
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
struct Request {
    #[serde(flatten)]
    page: PageOptions,
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
    #[serde(default, rename = "maxDirectTime")]
    max_direct_time: Option<u32>,
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
    timeout_seconds: Option<u32>,
    #[serde(default)]
    max_journey_duration_seconds: Option<u32>,
    /// Relative duration cutoff for journeys with a faster nearby alternative.
    /// None uses ProfileOptions::default().
    #[serde(default)]
    extreme_duration_ratio: Option<f64>,
    /// Additive tolerance, in seconds, before filtering a slow alternative.
    #[serde(default)]
    extreme_duration_slack_seconds: Option<u32>,
    /// Compare journey durations only for nearby departure/arrival times.
    #[serde(default)]
    comparison_window_seconds: Option<u32>,
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
    // Includes metadata plus OSR geometry reconstruction and JSON assembly.
    response_build_ms: f64,
    query_total_ms: f64,
}

fn milliseconds(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Include the same measurements emitted by --query-timings in the HTTP
/// response, preserving the existing RAPTOR diagnostics in debugOutput.
/// The OSR wall time is the elapsed time for three concurrent phases, not
/// the sum of their individual durations.
fn attach_query_timings(response: &mut serde_json::Value, timings: &QueryTimings) {
    response["debugOutput"]["timings"] = serde_json::json!(timings);
}

struct State {
    router: Arc<Router>,
    database_url: Option<String>,
    osr: Arc<NativeOsr>,
}

fn osr_access(
    router: &Router,
    osr: &NativeOsr,
    transit_region: &str,
    osr_region: &str,
    point: Coordinates,
    direction: u8,
    mode: StreetMode,
    max_seconds: u32,
    pedestrian_speed: f32,
    cycling_speed: f32,
) -> Result<Vec<WalkingEndpoint>> {
    // Preserve complete OSR access/egress offsets; do not relax start footpaths.
    let speed = f64::from(match mode {
        StreetMode::Walk => pedestrian_speed,
        StreetMode::Bike => cycling_speed,
    });
    let radius = speed * f64::from(max_seconds)
        + 2.0 * catenary::routing_common::native_osr::MAX_ENDPOINT_MATCH_METERS;
    let nearest = router.nearest_stops(transit_region, point, 4096, radius)?;
    let tt = router.timetable_for(transit_region)?;
    let targets = nearest
        .iter()
        .map(|(stop, _)| StreetEndpoint::stop(&tt, *stop))
        .collect::<Result<Vec<_>>>()?;
    let paths = osr.route(
        osr_region,
        &tt,
        router.platform_metadata_for(transit_region)?,
        StreetEndpoint::position(point),
        &targets,
        mode.profile(),
        direction,
        max_seconds,
        pedestrian_speed,
        cycling_speed,
        false,
    )?;
    let mut result: Vec<_> = nearest
        .into_iter()
        .zip(paths)
        .filter(|(_, path)| path.found && path.duration_seconds <= max_seconds)
        .map(|((stop, _), path)| WalkingEndpoint {
            stop,
            seconds: path.duration_seconds,
        })
        .collect();
    result.sort_by_key(|x| (x.seconds, x.stop.0));
    Ok(result)
}

fn direct_osr_seconds(
    router: &Router,
    osr: &NativeOsr,
    transit_region: &str,
    region: &str,
    from: Coordinates,
    to: Coordinates,
    mode: StreetMode,
    max_seconds: u32,
    pedestrian_speed: f32,
    cycling_speed: f32,
) -> Result<Option<u32>> {
    let tt = router.timetable_for(transit_region)?;
    let paths = osr.route(
        region,
        &tt,
        router.platform_metadata_for(transit_region)?,
        StreetEndpoint::position(from),
        &[StreetEndpoint::position(to)],
        mode.profile(),
        0,
        max_seconds,
        pedestrian_speed,
        cycling_speed,
        false,
    )?;
    Ok(paths
        .first()
        .filter(|p| p.found && p.duration_seconds <= max_seconds)
        .map(|p| p.duration_seconds))
}

fn run_query(
    router: &Router,
    request: Request,
    url: Option<&str>,
    osr: &NativeOsr,
) -> Result<(serde_json::Value, QueryTimings)> {
    run_query_observed(
        router,
        request,
        url,
        osr,
        false,
        &AtomicBool::new(false),
        &mut |_| Ok(()),
    )
}

fn request_id(request: &Request) -> Result<String> {
    request.page.validate()?;
    if let Some(id) = &request.page.request_id {
        return Ok(id.clone());
    }
    let mut identity = request.clone();
    identity.page.request_id = None;
    pagination::digest(&identity)
}

fn query_key(request: &Request) -> Result<String> {
    let mut value = serde_json::to_value(request)?;
    let object = value.as_object_mut().unwrap();
    for field in [
        "request_id",
        "page_cursor",
        "number_of_results_before",
        "number_of_results_after",
        "departure_unix_seconds",
        "arrival_unix_seconds",
    ] {
        object.remove(field);
    }
    pagination::digest(&value)
}

fn run_query_observed(
    router: &Router,
    mut request: Request,
    url: Option<&str>,
    osr: &NativeOsr,
    streaming: bool,
    cancelled: &AtomicBool,
    observer: &mut dyn FnMut(serde_json::Value) -> Result<()>,
) -> Result<(serde_json::Value, QueryTimings)> {
    let query_start = Instant::now();
    request.page.validate()?;
    let key = query_key(&request)?;
    let request_id = request_id(&request)?;
    if let Some((side, time)) = request.page.cursor(&key)? {
        let (before, after) = (
            request.page.number_of_results_before.unwrap_or(0),
            request.page.number_of_results_after.unwrap_or(0),
        );
        anyhow::ensure!(
            match side {
                Side::Before => after == 0,
                Side::After => before == 0,
            },
            "page_cursor direction conflicts with before/after counts"
        );
        let count = if before + after > 0 {
            before.max(after)
        } else {
            request.min_itineraries.unwrap_or(5)
        };
        request.page.number_of_results_before = Some(if side == Side::Before { count } else { 0 });
        request.page.number_of_results_after = Some(if side == Side::After { count } else { 0 });
        request.departure_unix_seconds = (side == Side::After).then_some(time);
        request.arrival_unix_seconds = (side == Side::Before).then_some(time);
    }
    // Keep streaming output bounded even when a fast timetable can be swept
    // far beyond the original minimum before the wall budget expires.
    if streaming
        && request.page.number_of_results_before.is_none()
        && request.page.number_of_results_after.is_none()
    {
        let count = request.min_itineraries.unwrap_or(16);
        request.page.number_of_results_before = Some(if request.arrival_unix_seconds.is_some() {
            count
        } else {
            0
        });
        request.page.number_of_results_after = Some(if request.arrival_unix_seconds.is_some() {
            0
        } else {
            count
        });
    }
    request.page.validate()?;
    anyhow::ensure!(
        request.timetable_view
            || (!streaming
                && request.page.number_of_results_before.is_none()
                && request.page.number_of_results_after.is_none()
                && request.page.page_cursor.is_none()),
        "streaming and pagination require timetable_view=true"
    );
    anyhow::ensure!(
        !cancelled.load(Ordering::Relaxed),
        "routing request cancelled"
    );
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
    anyhow::ensure!(
        pedestrian_speed.is_finite() && (0.3..=10.0).contains(&pedestrian_speed),
        "pedestrianSpeed must be between 0.3 and 10.0 m/s"
    );
    anyhow::ensure!(
        cycling_speed.is_finite() && (0.7..=20.0).contains(&cycling_speed),
        "cyclingSpeed must be between 0.7 and 20.0 m/s"
    );
    let modes = |items: &[StreetMode]| -> anyhow::Result<StreetMode> {
        match items {
            [] => Ok(StreetMode::Walk),
            [mode] => Ok(*mode),
            _ => anyhow::bail!("only one pre/post transit mode is currently supported"),
        }
    };
    let access_mode = modes(&request.pre_transit_modes)?;
    let egress_mode = modes(&request.post_transit_modes)?;
    let access_budget = request.max_pre_transit_time.unwrap_or(900);
    let egress_budget = request.max_post_transit_time.unwrap_or(900);
    anyhow::ensure!(
        (1..=3600).contains(&access_budget) && (1..=3600).contains(&egress_budget),
        "pre/post transit time budgets must be between 1 and 3600 seconds"
    );
    let direct_budget = request.max_direct_time.unwrap_or(1800);
    anyhow::ensure!(
        (1..=3600).contains(&direct_budget),
        "maxDirectTime must be between 1 and 3600 seconds"
    );
    let mut timings = QueryTimings::default();

    let phase = Instant::now();
    let selection = router.select(request.from, request.to)?;
    timings.selection_ms = milliseconds(phase);

    // Access, egress and direct walking have no data dependencies.
    // Use Rayon's existing bounded pool, rather than spawning up to three
    // operating-system threads per HTTP request. Graphs are shared read-only;
    // NativeOsr bounds concurrent searches and keeps match stores call-local.
    let osr_start = Instant::now();
    let (access, (egress, direct)) = rayon::join(
        || {
            let start = Instant::now();
            let result = if request.access.is_empty() {
                osr_access(
                    router,
                    osr,
                    &selection.transit_region,
                    &selection.origin_osr_region,
                    request.from,
                    0,
                    access_mode,
                    access_budget,
                    pedestrian_speed,
                    cycling_speed,
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
                            osr,
                            &selection.transit_region,
                            &selection.destination_osr_region,
                            request.to,
                            1,
                            egress_mode,
                            egress_budget,
                            pedestrian_speed,
                            cycling_speed,
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
                            router,
                            osr,
                            &selection.transit_region,
                            &selection.origin_osr_region,
                            request.from,
                            request.to,
                            access_mode,
                            direct_budget,
                            pedestrian_speed,
                            cycling_speed,
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
        timeout_seconds: request
            .timeout_seconds
            .unwrap_or(profile_defaults.timeout_seconds),
        max_journey_duration_seconds: request
            .max_journey_duration_seconds
            .unwrap_or(profile_defaults.max_journey_duration_seconds),
        extreme_duration_ratio: request
            .extreme_duration_ratio
            .unwrap_or(profile_defaults.extreme_duration_ratio),
        extreme_duration_slack_seconds: request
            .extreme_duration_slack_seconds
            .unwrap_or(profile_defaults.extreme_duration_slack_seconds),
        comparison_window_seconds: request
            .comparison_window_seconds
            .unwrap_or(profile_defaults.comparison_window_seconds),
    };
    options.validate()?;
    let route_started = Instant::now();
    let deadline = route_started + Duration::from_secs(u64::from(options.timeout_seconds));
    let paged = request.page.number_of_results_before.is_some()
        || request.page.number_of_results_after.is_some();
    // Enrich only new/changed journeys, with one cache shared by partial and
    // final responses. Snapshot transport never forces repeated OSR geometry
    // or PostgreSQL metadata work for an unchanged itinerary.
    let timetable = router.timetable_for(&selection.transit_region)?;
    let index = router.metadata_for(&selection.transit_region)?;
    let platform = router.platform_metadata_for(&selection.transit_region)?;
    let mut cache = HashMap::<String, serde_json::Value>::new();
    let mut template = None;
    let mut metadata_total = 0.0;
    let mut rendering_total = 0.0;
    let mut render = |journeys: &[Journey], stats: &RaptorStats| -> Result<serde_json::Value> {
        anyhow::ensure!(
            !cancelled.load(Ordering::Relaxed),
            "routing request cancelled"
        );
        let start = Instant::now();
        let ids = journeys
            .iter()
            .map(pagination::journey_id)
            .collect::<Result<Vec<_>>>()?;
        let missing = journeys
            .iter()
            .zip(&ids)
            .filter(|(_, id)| !cache.contains_key(*id))
            .map(|(j, _)| j.clone())
            .collect::<Vec<_>>();
        if template.is_none() || !missing.is_empty() {
            let (response, metadata_ms) = motis_response::build(
                &timetable,
                index,
                platform,
                &missing,
                origin,
                destination,
                &selection.origin_osr_region,
                &selection.destination_osr_region,
                osr,
                url,
                detailed_legs,
                access_mode.name(),
                egress_mode.name(),
                access_mode.name(),
                pedestrian_speed,
                cycling_speed,
            )?;
            metadata_total += metadata_ms;
            for (is_direct, field) in [(false, "itineraries"), (true, "direct")] {
                for (journey, value) in missing
                    .iter()
                    .filter(|j| (j.boardings == 0) == is_direct)
                    .zip(response[field].as_array().unwrap())
                {
                    let id = pagination::journey_id(journey)?;
                    let mut value = value.clone();
                    value["id"] = serde_json::json!(id);
                    cache.insert(id, value);
                }
            }
            template = Some(response);
        }
        let mut response = template.as_ref().unwrap().clone();
        for (is_direct, field) in [(false, "itineraries"), (true, "direct")] {
            response[field] = serde_json::Value::Array(
                journeys
                    .iter()
                    .zip(&ids)
                    .filter(|(j, _)| (j.boardings == 0) == is_direct)
                    .map(|(_, id)| cache[id].clone())
                    .collect(),
            );
        }
        // Evict entries that disappeared from the current replacement snapshot.
        cache.retain(|id, _| ids.contains(id));
        let (before, after) = pagination::cursors(journeys, &key)?;
        response["requestId"] = serde_json::json!(request_id);
        response["previousPageCursor"] = serde_json::json!(before);
        response["nextPageCursor"] = serde_json::json!(after);
        response["debugOutput"] = serde_json::json!({
            "raptor":stats, "timetableView":request.timetable_view, "arriveBy":arrive_by,
            "routingEngine":if request.timetable_view { "nigiri-raptor-labels-pong-v5" } else { "point-raptor" },
        });
        rendering_total += milliseconds(start);
        Ok(response)
    };
    let mut completed = Vec::<Journey>::new();
    let mut side_stats = serde_json::Map::new();
    let mut last_snapshot = Vec::<String>::new();
    let mut publish = |journeys: &[Journey], stats: &RaptorStats| -> Result<()> {
        if !streaming {
            return Ok(());
        }
        let ids = journeys
            .iter()
            .map(pagination::journey_id)
            .collect::<Result<Vec<_>>>()?;
        if ids != last_snapshot {
            observer(render(journeys, stats)?)?;
            last_snapshot = ids;
        }
        Ok(())
    };
    let mut final_stats = RaptorStats::default();
    if request.timetable_view {
        // Each side gets a fresh RAPTOR state. Shared deadline and fair slices
        // prevent one side from consuming the other side's entire budget.
        let mut sides = if paged {
            let mut result = Vec::new();
            if let Some(n) = request.page.number_of_results_after.filter(|&n| n > 0) {
                result.push((Side::After, request_time, n));
            }
            if let Some(n) = request.page.number_of_results_before.filter(|&n| n > 0) {
                let time = if arrive_by {
                    request_time
                } else {
                    request_time
                        .checked_sub(1)
                        .context("before timestamp overflow")?
                };
                result.push((Side::Before, time, n));
            }
            result
        } else {
            vec![(
                if arrive_by { Side::Before } else { Side::After },
                request_time,
                options.min_itineraries,
            )]
        };
        if arrive_by {
            sides.reverse();
        }
        let total_sides = sides.len();
        for (i, &(side, time, count)) in sides.iter().enumerate() {
            anyhow::ensure!(
                !cancelled.load(Ordering::Relaxed),
                "routing request cancelled"
            );
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                final_stats.profile_budget_exhausted = true;
                break;
            }
            let control = PongControl {
                deadline: Some(Instant::now() + remaining / (total_sides - i) as u32),
                cancelled: Some(cancelled),
                continue_until_budget: streaming,
                emit_updates: streaming,
                require_reconstructed_count: paged,
            };
            let side_options = ProfileOptions {
                min_itineraries: count,
                // A one-second seed window avoids waiting for a broad profile
                // interval before fulfilling a count-based scrolling request.
                search_window_seconds: if paged {
                    1
                } else {
                    options.search_window_seconds
                },
                ..options
            };
            let reverse = side == Side::Before;
            let merge = |part: &[Journey]| -> Result<Vec<Journey>> {
                let mut combined = completed.clone();
                combined.extend(if paged {
                    pagination::select_page(part, side, time, count)
                } else {
                    part.to_vec()
                });
                let mut seen = HashSet::new();
                let mut unique = Vec::new();
                let mut has_direct = false;
                for journey in combined {
                    if journey.boardings == 0 {
                        if has_direct {
                            continue;
                        }
                        has_direct = true;
                    }
                    if seen.insert(pagination::journey_id(&journey)?) {
                        unique.push(journey);
                    }
                }
                catenary::routing_common::profile_search::sort_journeys_for_response(
                    &mut unique,
                    arrive_by,
                );
                Ok(unique)
            };
            let routing = router.route_profile_observed(
                selection.clone(),
                time,
                from.clone(),
                to.clone(),
                direct,
                reverse,
                side_options,
                &control,
                &mut |part, stats| publish(&merge(part)?, stats),
            )?;
            let merged = merge(&routing.journeys)?;
            completed = merged;
            final_stats.profile_budget_exhausted |= routing.raptor_stats.profile_budget_exhausted;
            side_stats.insert(
                if reverse { "before" } else { "after" }.into(),
                serde_json::to_value(&routing.raptor_stats)?,
            );
            let exhausted = final_stats.profile_budget_exhausted;
            final_stats = routing.raptor_stats;
            final_stats.profile_budget_exhausted |= exhausted;
            publish(&completed, &final_stats)?;
        }
    } else {
        let routing = if arrive_by {
            router.route_arrive_by_with_access(selection.clone(), request_time, from, to, direct)?
        } else {
            router.route_with_access(selection.clone(), request_time, from, to, direct)?
        };
        completed = routing.journeys;
        final_stats = routing.raptor_stats;
    }
    drop(publish);
    let mut response = render(&completed, &final_stats)?;
    drop(render);
    response["debugOutput"]["pages"] = serde_json::Value::Object(side_stats);
    timings.raptor_ms = milliseconds(route_started) - rendering_total;
    timings.metadata_ms = metadata_total;
    timings.response_build_ms = rendering_total;
    timings.query_total_ms = milliseconds(query_start);
    attach_query_timings(&mut response, &timings);

    Ok((response, timings))
}

async fn route_ws(
    state: web::Data<State>,
    request: HttpRequest,
    payload: web::Payload,
) -> Result<HttpResponse, actix_web::Error> {
    let state = state.into_inner();
    let runner: Arc<websocket::Runner> = Arc::new(move |query, request_id, cancelled, observer| {
        let mut request: Request = serde_json::from_value(query)?;
        anyhow::ensure!(
            request
                .page
                .request_id
                .as_ref()
                .is_none_or(|id| id == &request_id),
            "query request_id must match envelope requestId"
        );
        request.page.request_id = Some(request_id);
        let (answer, _) = run_query_observed(
            &state.router,
            request,
            state.database_url.as_deref(),
            &state.osr,
            true,
            &cancelled,
            observer,
        )?;
        Ok(answer)
    });
    websocket::start(request, payload, runner)
}

// These endpoints are served only after Router::load succeeds. Health checks
// deliberately avoid OSR and PostgreSQL calls, so they stay cheap under load.
async fn index() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "service": "edelweiss",
        "version": env!("CARGO_PKG_VERSION"),
        "endpoints": {
            "health": "/health",
            "route": "/v1/route",
            "routeWebSocket": "/v1/route/ws"
        }
    }))
}

async fn health() -> HttpResponse {
    HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(serde_json::json!({
            "service": "edelweiss",
            "status": "ok"
        }))
}

async fn route(state: web::Data<State>, payload: web::Json<Request>) -> HttpResponse {
    let state = state.into_inner();
    let request = payload.into_inner();
    let id = match request_id(&request) {
        Ok(id) => id,
        Err(error) => {
            return HttpResponse::UnprocessableEntity().json(serde_json::json!({
                "requestId":request.page.request_id, "error":error.to_string(),
            }));
        }
    };
    let result = web::block(move || {
        run_query(
            &state.router,
            request,
            state.database_url.as_deref(),
            &state.osr,
        )
    })
    .await;
    match result {
        Ok(Ok((answer, _timings))) => HttpResponse::Ok().json(answer),
        Ok(Err(err)) => HttpResponse::UnprocessableEntity()
            .json(serde_json::json!({"requestId":id,"error":err.to_string()})),
        Err(err) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"requestId":id,"error":err.to_string()})),
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
    let startup_started = Instant::now();
    let args = Args::parse();
    eprintln!(
        "Edelweiss startup: loading routing configuration {}",
        args.regions_config.display()
    );
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
    eprintln!("Edelweiss startup: resolving PostgreSQL configuration");
    let database_url = resolve_database_url(args.database_url.clone(), &args.catenary_config)?;
    if database_url.is_none() {
        eprintln!(
            "Warning: no PostgreSQL URL configured; route colours and other GTFS metadata will be unavailable"
        );
    }
    let parallel_started = Instant::now();
    eprintln!("Edelweiss startup: loading router and in-process OSR graphs concurrently");
    // Both loaders only read their input files. Use a dedicated scoped thread
    // so startup overlaps even when Rayon's pool has only one worker.
    let (router, osr) = startup::load_concurrently(
        || {
            let started = Instant::now();
            let result = Router::load(&args.regions_config, &args.catenary_config, settings);
            eprintln!(
                "Edelweiss startup: router load finished in {:.3}s (success: {})",
                started.elapsed().as_secs_f64(),
                result.is_ok(),
            );
            result
        },
        || {
            let started = Instant::now();
            let result = NativeOsr::load(&args.catenary_config, args.osr_workers);
            eprintln!(
                "Edelweiss startup: in-process OSR load finished in {:.3}s (success: {})",
                started.elapsed().as_secs_f64(),
                result.is_ok(),
            );
            result
        },
    );
    eprintln!(
        "Edelweiss startup: concurrent router/OSR load finished in {:.3}s",
        parallel_started.elapsed().as_secs_f64(),
    );
    // Join both loaders before propagating errors or checking graph matches.
    let router = Arc::new(router.context("loading timetable router")?);
    let osr = Arc::new(osr.context("loading in-process OSR graphs")?);
    for region in &router.transit {
        let tt = region.timetable.view()?;
        // Log incompatibility once at load, not for every candidate stop.
        // Distinct graph partitions legitimately need fresh matching.
        for street in router.street_region_ids() {
            if let Ok(reuse) = osr.has_precomputed_matches(street, &tt) {
                eprintln!(
                    "Edelweiss startup: timetable {} / OSR {}: precomputed matches {}",
                    region.id,
                    street,
                    if reuse {
                        "enabled"
                    } else {
                        "unavailable; matching on demand"
                    }
                );
            }
        }
    }
    if let Some(path) = args.query {
        let timing_output = args.query_timings.clone();
        let request: Request = serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )?;
        // The synchronous postgres client creates and blocks on its own Tokio
        // runtime. Running it on an Actix worker thread would nest runtimes and
        // panic; use the same blocking pool as the HTTP route handler.
        let (response, timings) =
            web::block(move || run_query(&router, request, database_url.as_deref(), &osr))
                .await
                .context("CLI route-query worker failed")??;
        if let Some(path) = &timing_output {
            fs::write(path, serde_json::to_vec_pretty(&timings)?)
                .with_context(|| format!("writing query timings to {}", path.display()))?;
        }
        eprintln!(
            "Edelweiss query: total {:.3} ms; RAPTOR {:.3} ms; concurrent OSR wall {:.3} ms; OSR access {:.3} ms; OSR egress {:.3} ms; OSR direct {:.3} ms; metadata {:.3} ms; response build {:.3} ms",
            timings.query_total_ms,
            timings.raptor_ms,
            timings.osr_parallel_wall_ms,
            timings.access_ms,
            timings.egress_ms,
            timings.direct_walk_ms,
            timings.metadata_ms,
            timings.response_build_ms
        );
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let state = web::Data::new(State {
        router,
        database_url,
        osr,
    });
    let enable_footpath_debug = args.enable_footpath_debug;
    eprintln!("Edelweiss startup: binding HTTP server to {}", args.listen);
    let server = HttpServer::new(move || {
        let app = App::new()
            .wrap(
                actix_cors::Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header(),
            )
            .app_data(state.clone())
            .route("/", web::get().to(index))
            .route("/health", web::get().to(health))
            .route("/v1/route", web::post().to(route))
            .route("/v1/route/ws", web::get().to(route_ws));
        if enable_footpath_debug {
            app.route(
                "/v1/debug/footpaths",
                web::post().to(footpath_debug::handle),
            )
        } else {
            app
        }
    })
    .bind(&args.listen)
    .with_context(|| format!("binding Edelweiss HTTP server to {}", args.listen))?
    .run();
    eprintln!(
        "Edelweiss startup: HTTP server starting on {} after {:.3}s; router is ready",
        args.listen,
        startup_started.elapsed().as_secs_f64(),
    );
    server.await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_identity_tracks_query_and_cursors_allow_new_counts() {
        let mut request: Request = serde_json::from_value(serde_json::json!({
            "from":{"lat":33.6,"lon":-117.8}, "to":{"lat":34.0,"lon":-118.2},
            "departure_unix_seconds":1793500000, "numberOfResultsBefore":2,
            "numberOfResultsAfter":6,
        }))
        .unwrap();
        request.page.validate().unwrap();
        let key = query_key(&request).unwrap();
        let id = request_id(&request).unwrap();
        assert_eq!(id, request_id(&request.clone()).unwrap());
        request.page.number_of_results_after = Some(10);
        request.departure_unix_seconds = Some(1793501000);
        assert_eq!(key, query_key(&request).unwrap());
        assert_ne!(id, request_id(&request).unwrap());
        request.page.request_id = Some("client-page-2".into());
        assert_eq!(request_id(&request).unwrap(), "client-page-2");
        request.to.lat += 0.1;
        assert_ne!(key, query_key(&request).unwrap());
    }

    #[actix_web::test]
    async fn service_endpoints_work_without_routing_dependencies() {
        let app = actix_web::test::init_service(
            App::new()
                .route("/", web::get().to(index))
                .route("/health", web::get().to(health)),
        )
        .await;
        let request = actix_web::test::TestRequest::get().uri("/").to_request();
        let response = actix_web::test::call_service(&app, request).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body: serde_json::Value = actix_web::test::read_body_json(response).await;
        assert_eq!(body["service"], "edelweiss");
        assert_eq!(body["endpoints"]["health"], "/health");
        assert_eq!(body["endpoints"]["route"], "/v1/route");

        let request = actix_web::test::TestRequest::get()
            .uri("/health")
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        let body: serde_json::Value = actix_web::test::read_body_json(response).await;
        assert_eq!(
            body,
            serde_json::json!({"service": "edelweiss", "status": "ok"})
        );
    }

    #[test]
    fn query_timings_are_included_in_response_without_erasing_raptor_diagnostics() {
        let timings = QueryTimings {
            selection_ms: 1.0,
            access_ms: 28.0,
            egress_ms: 95.0,
            direct_walk_ms: 12.0,
            osr_parallel_wall_ms: 100.0,
            raptor_ms: 625.0,
            metadata_ms: 8.0,
            response_build_ms: 20.0,
            query_total_ms: 740.0,
        };
        let mut response = serde_json::json!({
            "debugOutput": {
                "raptor": {"rounds": 3},
                "timetableView": true,
                "arriveBy": false
            },
            "itineraries": []
        });
        attach_query_timings(&mut response, &timings);

        assert_eq!(response["debugOutput"]["raptor"]["rounds"], 3);
        assert_eq!(response["debugOutput"]["timetableView"], true);
        assert_eq!(response["debugOutput"]["arriveBy"], false);
        assert_eq!(
            response["debugOutput"]["timings"],
            serde_json::to_value(&timings).unwrap()
        );
        assert_eq!(response["debugOutput"]["timings"]["raptor_ms"], 625.0);
        assert_eq!(
            response["debugOutput"]["timings"]["osr_parallel_wall_ms"],
            100.0
        );
        assert!(response["itineraries"].as_array().unwrap().is_empty());
    }

    #[test]
    fn optional_profile_quality_fields_deserialize_and_preserve_defaults() {
        let request: Request = serde_json::from_value(serde_json::json!({
            "from": {"lat": 33.6, "lon": -117.8},
            "to": {"lat": 32.9, "lon": -117.2},
            "departure_unix_seconds": 1_793_500_000_i64
        }))
        .unwrap();

        // Existing clients omit these parameters. Serde must accept them.
        assert_eq!(request.extreme_duration_ratio, None);
        assert_eq!(request.extreme_duration_slack_seconds, None);
        assert_eq!(request.comparison_window_seconds, None);

        let defaults = ProfileOptions::default();
        assert_eq!(
            request
                .extreme_duration_ratio
                .unwrap_or(defaults.extreme_duration_ratio),
            1.75,
        );
        assert_eq!(
            request
                .extreme_duration_slack_seconds
                .unwrap_or(defaults.extreme_duration_slack_seconds),
            1800,
        );
        assert_eq!(
            request
                .comparison_window_seconds
                .unwrap_or(defaults.comparison_window_seconds),
            5400,
        );
    }

    #[test]
    fn profile_quality_overrides_deserialize_and_validate() {
        let request: Request = serde_json::from_value(serde_json::json!({
            "from": {"lat": 33.6, "lon": -117.8},
            "to": {"lat": 32.9, "lon": -117.2},
            "arrival_unix_seconds": 1_793_500_000_i64,
            "extreme_duration_ratio": 2.0,
            "extreme_duration_slack_seconds": 900,
            "comparison_window_seconds": 3600
        }))
        .unwrap();

        assert_eq!(request.extreme_duration_ratio, Some(2.0));
        assert_eq!(request.extreme_duration_slack_seconds, Some(900));
        assert_eq!(request.comparison_window_seconds, Some(3600));

        let defaults = ProfileOptions::default();
        let options = ProfileOptions {
            extreme_duration_ratio: request
                .extreme_duration_ratio
                .unwrap_or(defaults.extreme_duration_ratio),
            extreme_duration_slack_seconds: request
                .extreme_duration_slack_seconds
                .unwrap_or(defaults.extreme_duration_slack_seconds),
            comparison_window_seconds: request
                .comparison_window_seconds
                .unwrap_or(defaults.comparison_window_seconds),
            ..defaults
        };
        options.validate().unwrap();
        assert_eq!(options.extreme_duration_ratio, 2.0);
        assert_eq!(options.extreme_duration_slack_seconds, 900);
        assert_eq!(options.comparison_window_seconds, 3600);
    }

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
            postgres::Client::connect(
                "host=127.0.0.1 port=1 user=postgres dbname=unavailable connect_timeout=1",
                postgres::NoTls,
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
