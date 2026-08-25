// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use crate::async_threads_alpenrose::set_stage;
use crate::persistence;
use ahash::{AHashMap, AHashSet};
use catenary::aspen_dataset::{
    AspenTrajectoryStore, AspenisedData, AspenisedStopTimeUpdate, AspenisedTrajectorySegment,
    PackedGeometry, PatternBBox, PatternGeometry, SegmentRange, TrajectoryInstance,
    TrajectoryStopInstance,
};
use catenary::postgres_tools::CatenaryPostgresPool;
use compact_str::CompactString;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use scc::HashMap as SccHashMap;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const ALLOWED_CHATEAUX: &[&str] = &[
    "deutschland",
    "sncf",
    "nationalrailuk",
    "schweiz",
    "île~de~france~mobilités",
    "sncb",
    "tisséo",
    "vbb",
    "danmark",
    "busÉireann",
    "nederland",
];

const LONG_BBOX_DISTANCE_METERS: f64 = 10_000.0;
const METERS_PER_DEGREE: f64 = 111_320.0;
const SHAPE_FETCH_BATCH_SIZE: usize = 128;

type ShapeLineString = postgis_diesel::types::LineString<postgis_diesel::types::Point>;

pub(crate) struct TrajectoryBuildResult {
    pub(crate) store: Arc<AspenTrajectoryStore>,
    pub(crate) static_changed: bool,
    pub(crate) cpu_time: Duration,
}

#[cfg(target_os = "linux")]
fn current_thread_cpu_time() -> Duration {
    Duration::try_from(rustix::time::clock_gettime(
        rustix::time::ClockId::ThreadCPUTime,
    ))
    .expect("CLOCK_THREAD_CPUTIME_ID returned a negative duration")
}

#[cfg(not(target_os = "linux"))]
fn current_thread_cpu_time() -> Duration {
    Duration::ZERO
}

pub(crate) async fn build_trajectory_store(
    chateau_id: &str,
    realtime_feed_id: &str,
    total_started: Instant,
    aspenised_data: &AspenisedData,
    authoritative_trajectory_data_store: &SccHashMap<String, Arc<AspenTrajectoryStore>>,
    pool: &CatenaryPostgresPool,
) -> TrajectoryBuildResult {
    let (mut geometries, mut patterns, mut rtree_by_route_type) =
        load_existing_static_trajectory_data(authoritative_trajectory_data_store, chateau_id).await;

    let mut cpu_time = Duration::ZERO;
    let cpu_started = current_thread_cpu_time();
    let mut pattern_map = pattern_index(&patterns);
    let mut static_changed = false;

    set_stage(
        chateau_id,
        realtime_feed_id,
        "build_trajectories",
        total_started,
    );

    let mut trajectories = Vec::new();
    let mut pattern_to_trajectories = vec![Vec::new(); patterns.len()];

    let (vehicle_trip_ids, vehicle_route_ids) = collect_vehicle_trip_and_route_ids(aspenised_data);
    let missing_patterns = collect_missing_patterns(
        chateau_id,
        aspenised_data,
        &vehicle_trip_ids,
        &vehicle_route_ids,
        &pattern_map,
    );
    cpu_time += current_thread_cpu_time().saturating_sub(cpu_started);

    if ALLOWED_CHATEAUX.contains(&chateau_id) {
        let MissingPatternBuildResult {
            patterns_built,
            cpu_time: pattern_build_cpu_time,
        } = build_missing_patterns(
            chateau_id,
            aspenised_data,
            &missing_patterns,
            pool,
            &mut geometries,
            &mut patterns,
            &mut pattern_map,
            &mut rtree_by_route_type,
        )
        .await;
        static_changed |= patterns_built > 0;
        cpu_time += pattern_build_cpu_time;
    }

    let cpu_started = current_thread_cpu_time();
    if ALLOWED_CHATEAUX.contains(&chateau_id) {
        process_trip_updates(
            chateau_id,
            aspenised_data,
            &vehicle_trip_ids,
            &vehicle_route_ids,
            &pattern_map,
            &mut trajectories,
            &mut pattern_to_trajectories,
        );
    }

    let store = AspenTrajectoryStore {
        geometries,
        patterns,
        trajectories,
        pattern_to_trajectories,
        rtree_by_route_type,
    };
    let (store, compacted_static_data) = persistence::compact_trajectory_store(store);
    static_changed |= compacted_static_data;
    cpu_time += current_thread_cpu_time().saturating_sub(cpu_started);

    TrajectoryBuildResult {
        store: Arc::new(store),
        static_changed,
        cpu_time,
    }
}

pub(crate) async fn publish_trajectory_store(
    authoritative_trajectory_data_store: &SccHashMap<String, Arc<AspenTrajectoryStore>>,
    chateau_id: &str,
    realtime_feed_id: &str,
    total_started: Instant,
    store: &Arc<AspenTrajectoryStore>,
) {
    set_stage(
        chateau_id,
        realtime_feed_id,
        "publish_trajectory_store",
        total_started,
    );

    authoritative_trajectory_data_store
        .entry_async(chateau_id.to_string())
        .await
        .and_modify(|current| *current = Arc::clone(store))
        .or_insert_with(|| Arc::clone(store));
}

pub(crate) fn save_trajectory_store(
    chateau_id: &str,
    store: &AspenTrajectoryStore,
    static_changed: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    persistence::save_trajectory_data(chateau_id, store, static_changed)
}

async fn load_existing_static_trajectory_data(
    authoritative_trajectory_data_store: &SccHashMap<String, Arc<AspenTrajectoryStore>>,
    chateau_id: &str,
) -> (
    Vec<PackedGeometry>,
    Vec<PatternGeometry>,
    AHashMap<i16, rstar::RTree<PatternBBox>>,
) {
    match authoritative_trajectory_data_store
        .get_async(chateau_id)
        .await
    {
        Some(existing) => {
            let store = existing.get();
            (
                store.geometries.clone(),
                store.patterns.clone(),
                store.rtree_by_route_type.clone(),
            )
        }
        None => (Vec::new(), Vec::new(), AHashMap::new()),
    }
}

fn pattern_index(patterns: &[PatternGeometry]) -> AHashMap<CompactString, u32> {
    patterns
        .iter()
        .enumerate()
        .map(|(idx, pattern)| (pattern.pattern_id_str.clone(), idx as u32))
        .collect()
}

fn collect_vehicle_trip_and_route_ids(
    aspenised_data: &AspenisedData,
) -> (AHashSet<String>, AHashSet<String>) {
    let mut vehicle_trip_ids = AHashSet::new();
    let mut vehicle_route_ids = AHashSet::new();

    for vehicle in aspenised_data.vehicle_positions.values() {
        if let Some(trip) = &vehicle.trip {
            if let Some(trip_id) = &trip.trip_id {
                vehicle_trip_ids.insert(trip_id.clone());
            }
            if let Some(route_id) = &trip.route_id {
                vehicle_route_ids.insert(route_id.clone());
            }
        }
    }

    (vehicle_trip_ids, vehicle_route_ids)
}

#[derive(Clone, Copy)]
enum TrajectorySkipReason {
    NoStops,
    NoTripId,
    NoRouteId,
    CoveredByVehicle,
    NoRouteCache,
    FilteredRouteType,
}

struct TrajectoryTripContext<'a> {
    trip_id: &'a String,
    route_id: &'a String,
    route_type: i16,
}

struct MissingPatternBuild<'a> {
    pattern_id: CompactString,
    trip_update: &'a catenary::aspen_dataset::AspenisedTripUpdate,
    route_id: &'a str,
    route_type: i16,
    shape_id: Option<&'a str>,
    trajectory_stop_count: usize,
}

#[derive(Default)]
struct TrajectorySkipCounts {
    no_stops: usize,
    no_trip_id: usize,
    no_route_id: usize,
    covered_by_vehicle: usize,
    no_route_cache: usize,
    filtered_route_type: usize,
    too_few_trajectory_stops: usize,
}

impl TrajectorySkipCounts {
    fn record(&mut self, reason: TrajectorySkipReason) {
        match reason {
            TrajectorySkipReason::NoStops => self.no_stops += 1,
            TrajectorySkipReason::NoTripId => self.no_trip_id += 1,
            TrajectorySkipReason::NoRouteId => self.no_route_id += 1,
            TrajectorySkipReason::CoveredByVehicle => self.covered_by_vehicle += 1,
            TrajectorySkipReason::NoRouteCache => self.no_route_cache += 1,
            TrajectorySkipReason::FilteredRouteType => self.filtered_route_type += 1,
        }
    }
}

struct MissingPatternBuildResult {
    patterns_built: usize,
    cpu_time: Duration,
}

fn trajectory_trip_context<'a>(
    chateau_id: &str,
    aspenised_data: &AspenisedData,
    trip_update: &'a catenary::aspen_dataset::AspenisedTripUpdate,
    vehicle_trip_ids: &AHashSet<String>,
    vehicle_route_ids: &AHashSet<String>,
) -> Result<TrajectoryTripContext<'a>, TrajectorySkipReason> {
    if trip_update.stop_time_update.is_empty() {
        return Err(TrajectorySkipReason::NoStops);
    }

    let trip_id = trip_update
        .trip
        .trip_id
        .as_ref()
        .ok_or(TrajectorySkipReason::NoTripId)?;
    let route_id = trip_update
        .trip
        .route_id
        .as_ref()
        .ok_or(TrajectorySkipReason::NoRouteId)?;

    if vehicle_trip_ids.contains(trip_id) || vehicle_route_ids.contains(route_id) {
        return Err(TrajectorySkipReason::CoveredByVehicle);
    }

    let route = aspenised_data
        .vehicle_routes_cache
        .get(route_id)
        .ok_or(TrajectorySkipReason::NoRouteCache)?;

    if is_special_chateau(chateau_id) && route.route_type != 2 {
        return Err(TrajectorySkipReason::FilteredRouteType);
    }

    Ok(TrajectoryTripContext {
        trip_id,
        route_id,
        route_type: route.route_type,
    })
}

fn trajectory_stop_count(
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
) -> usize {
    trip_update
        .stop_time_update
        .iter()
        .filter(|stop_time_update| trajectory_stop_times(stop_time_update).is_some())
        .count()
}

fn collect_missing_patterns<'a>(
    chateau_id: &str,
    aspenised_data: &'a AspenisedData,
    vehicle_trip_ids: &AHashSet<String>,
    vehicle_route_ids: &AHashSet<String>,
    pattern_map: &AHashMap<CompactString, u32>,
) -> Vec<MissingPatternBuild<'a>> {
    if !ALLOWED_CHATEAUX.contains(&chateau_id) {
        return Vec::new();
    }

    let mut missing_patterns: Vec<MissingPatternBuild<'a>> = Vec::new();
    let mut missing_pattern_index: AHashMap<CompactString, usize> = AHashMap::new();

    for trip_update in aspenised_data.trip_updates.values() {
        let Ok(context) = trajectory_trip_context(
            chateau_id,
            aspenised_data,
            trip_update,
            vehicle_trip_ids,
            vehicle_route_ids,
        ) else {
            continue;
        };

        let pattern_id = pattern_id_for_trip(
            aspenised_data,
            trip_update,
            context.trip_id,
            context.route_id,
        );
        if pattern_map.contains_key(pattern_id.as_ref()) {
            continue;
        }

        let stop_count = trajectory_stop_count(trip_update);
        if stop_count < 2 {
            continue;
        }

        let Some(shape_source) = trip_shape_source(aspenised_data, context.trip_id) else {
            // The compressed trip knows an itinerary pattern but its metadata is not
            // available yet. Do not cache a straight-line geometry that would outlive
            // this transient cache miss.
            continue;
        };

        let pattern_id = CompactString::new(pattern_id.as_ref());
        let candidate = MissingPatternBuild {
            pattern_id: pattern_id.clone(),
            trip_update,
            route_id: context.route_id.as_str(),
            route_type: context.route_type,
            shape_id: match shape_source {
                TripShapeSource::NoShape => None,
                TripShapeSource::Shape(shape_id) => Some(shape_id),
            },
            trajectory_stop_count: stop_count,
        };

        if let Some(&existing_idx) = missing_pattern_index.get(pattern_id.as_str()) {
            // Realtime feeds can provide partial stop updates. Geometry is static, so
            // choose the most complete representative for each new itinerary pattern
            // instead of whichever AHashMap entry happens to be visited first.
            if stop_count > missing_patterns[existing_idx].trajectory_stop_count {
                missing_patterns[existing_idx] = candidate;
            }
        } else {
            missing_pattern_index.insert(pattern_id, missing_patterns.len());
            missing_patterns.push(candidate);
        }
    }

    missing_patterns
}

async fn build_missing_patterns(
    chateau_id: &str,
    aspenised_data: &AspenisedData,
    missing_patterns: &[MissingPatternBuild<'_>],
    pool: &CatenaryPostgresPool,
    geometries: &mut Vec<PackedGeometry>,
    patterns: &mut Vec<PatternGeometry>,
    pattern_map: &mut AHashMap<CompactString, u32>,
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) -> MissingPatternBuildResult {
    let mut cpu_time = Duration::ZERO;
    let mut patterns_built = 0usize;
    let mut patterns_by_shape: AHashMap<String, Vec<usize>> =
        AHashMap::with_capacity(missing_patterns.len());

    let cpu_started = current_thread_cpu_time();
    for (idx, missing) in missing_patterns.iter().enumerate() {
        if let Some(shape_id) = missing.shape_id {
            patterns_by_shape
                .entry(shape_id.to_owned())
                .or_default()
                .push(idx);
        } else if materialize_missing_pattern(
            aspenised_data,
            missing,
            None,
            geometries,
            patterns,
            pattern_map,
            rtree_by_route_type,
        ) {
            patterns_built += 1;
        }
    }
    cpu_time += current_thread_cpu_time().saturating_sub(cpu_started);

    let shape_backed_pattern_count = patterns_by_shape.values().map(Vec::len).sum::<usize>();
    if patterns_by_shape.is_empty() {
        println!(
            "Chateau {} trajectory geometry cache: missing_patterns={}, built_patterns={}, unique_shapes_to_fetch=0",
            chateau_id,
            missing_patterns.len(),
            patterns_built,
        );
        return MissingPatternBuildResult {
            patterns_built,
            cpu_time,
        };
    }

    let mut shape_ids = patterns_by_shape.keys().cloned().collect::<Vec<_>>();
    shape_ids.sort_unstable();

    println!(
        "Chateau {} trajectory geometry cache: missing_patterns={}, shape_backed_patterns={}, unique_shapes_to_fetch={}",
        chateau_id,
        missing_patterns.len(),
        shape_backed_pattern_count,
        shape_ids.len(),
    );

    let mut conn_pre = match pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!(
                "Chateau {}: Failed to get database connection for {} missing trajectory shapes: {:?}",
                chateau_id,
                shape_ids.len(),
                error
            );
            return MissingPatternBuildResult {
                patterns_built,
                cpu_time,
            };
        }
    };

    use catenary::schema::gtfs::shapes::dsl::{
        chateau as db_chateau, linestring as db_linestring, shape_id as db_shape_id, shapes,
    };

    let mut loaded_shapes = 0usize;
    let mut unavailable_shape_backed_patterns = 0usize;

    for chunk in shape_ids.chunks(SHAPE_FETCH_BATCH_SIZE) {
        let fetched_shapes = match shapes
            .filter(db_chateau.eq(chateau_id))
            .filter(db_shape_id.eq_any(chunk))
            .select((db_shape_id, db_linestring))
            .load::<(String, ShapeLineString)>(&mut conn_pre)
            .await
        {
            Ok(rows) => rows.into_iter().collect::<AHashMap<_, _>>(),
            Err(error) => {
                unavailable_shape_backed_patterns += chunk
                    .iter()
                    .filter_map(|shape_id| patterns_by_shape.get(shape_id))
                    .map(Vec::len)
                    .sum::<usize>();
                eprintln!(
                    "Chateau {}: Error fetching trajectory shape batch of {} shapes: {:?}",
                    chateau_id,
                    chunk.len(),
                    error
                );
                continue;
            }
        };

        loaded_shapes += fetched_shapes.len();

        let cpu_started = current_thread_cpu_time();
        for requested_shape_id in chunk {
            let Some(pattern_indices) = patterns_by_shape.get(requested_shape_id) else {
                continue;
            };
            let Some(linestring) = fetched_shapes.get(requested_shape_id) else {
                unavailable_shape_backed_patterns += pattern_indices.len();
                continue;
            };
            if linestring.points.is_empty() {
                unavailable_shape_backed_patterns += pattern_indices.len();
                continue;
            }

            for &pattern_idx in pattern_indices {
                if materialize_missing_pattern(
                    aspenised_data,
                    &missing_patterns[pattern_idx],
                    Some(linestring),
                    geometries,
                    patterns,
                    pattern_map,
                    rtree_by_route_type,
                ) {
                    patterns_built += 1;
                }
            }
        }
        cpu_time += current_thread_cpu_time().saturating_sub(cpu_started);
        // fetched_shapes drops at the end of this iteration, bounding transient
        // raw PostGIS geometry retention to SHAPE_FETCH_BATCH_SIZE rows.
    }

    println!(
        "Chateau {} trajectory geometry cache: requested_shapes={}, loaded_shapes={}, built_patterns={}, unavailable_shape_backed_patterns={}",
        chateau_id,
        shape_ids.len(),
        loaded_shapes,
        patterns_built,
        unavailable_shape_backed_patterns,
    );

    MissingPatternBuildResult {
        patterns_built,
        cpu_time,
    }
}

#[allow(clippy::too_many_arguments)]
fn materialize_missing_pattern(
    aspenised_data: &AspenisedData,
    missing: &MissingPatternBuild<'_>,
    shape: Option<&ShapeLineString>,
    geometries: &mut Vec<PackedGeometry>,
    patterns: &mut Vec<PatternGeometry>,
    pattern_map: &mut AHashMap<CompactString, u32>,
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) -> bool {
    if pattern_map.contains_key(missing.pattern_id.as_str()) {
        return false;
    }

    let trajectory_stops = collect_trajectory_stops(missing.trip_update);
    if trajectory_stops.len() < 2 {
        return false;
    }

    let segments = build_segments(aspenised_data, &trajectory_stops, shape);
    let (_, inserted) = ensure_pattern(
        missing.pattern_id.as_str(),
        missing.route_id,
        missing.route_type,
        route_type_name(missing.route_type),
        &segments,
        geometries,
        patterns,
        pattern_map,
        rtree_by_route_type,
    );
    inserted
}

fn process_trip_updates(
    chateau_id: &str,
    aspenised_data: &AspenisedData,
    vehicle_trip_ids: &AHashSet<String>,
    vehicle_route_ids: &AHashSet<String>,
    pattern_map: &AHashMap<CompactString, u32>,
    trajectories: &mut Vec<TrajectoryInstance>,
    pattern_to_trajectories: &mut Vec<Vec<u32>>,
) {
    println!("Starting trajectory processing for {}", chateau_id);

    let mut skipped = TrajectorySkipCounts::default();
    let mut skipped_missing_pattern_geometry = 0usize;

    for trip_update in aspenised_data.trip_updates.values() {
        let context = match trajectory_trip_context(
            chateau_id,
            aspenised_data,
            trip_update,
            vehicle_trip_ids,
            vehicle_route_ids,
        ) {
            Ok(context) => context,
            Err(reason) => {
                skipped.record(reason);
                continue;
            }
        };

        let trajectory_stops = collect_trajectory_stops(trip_update);
        if trajectory_stops.len() < 2 {
            skipped.too_few_trajectory_stops += 1;
            continue;
        }

        let pattern_id = pattern_id_for_trip(
            aspenised_data,
            trip_update,
            context.trip_id,
            context.route_id,
        );
        let Some(&pattern_idx) = pattern_map.get(pattern_id.as_ref()) else {
            // A shape-backed missing pattern stays missing if its DB fetch failed.
            // This prevents a transient database failure from permanently caching a
            // straight line under the canonical itinerary-pattern ID.
            skipped_missing_pattern_geometry += 1;
            continue;
        };

        let trajectory_idx = trajectories.len() as u32;
        trajectories.push(build_trajectory_instance(
            trip_update,
            context.trip_id,
            pattern_idx,
            trip_short_name(aspenised_data, trip_update, context.trip_id),
            &trajectory_stops,
        ));

        if pattern_idx as usize >= pattern_to_trajectories.len() {
            pattern_to_trajectories.resize_with(pattern_idx as usize + 1, Vec::new);
        }
        pattern_to_trajectories[pattern_idx as usize].push(trajectory_idx);
    }

    println!(
        "Trajectory computation for chateau {}: total_trip_updates={}, skipped_no_stops={}, skipped_no_trip_id={}, skipped_no_route_id={}, skipped_covered_by_vehicle={}, skipped_no_route_cache={}, skipped_filtered_route_type={}, skipped_too_few_trajectory_stops={}, skipped_missing_pattern_geometry={}, built_trajectories={}",
        chateau_id,
        aspenised_data.trip_updates.len(),
        skipped.no_stops,
        skipped.no_trip_id,
        skipped.no_route_id,
        skipped.covered_by_vehicle,
        skipped.no_route_cache,
        skipped.filtered_route_type,
        skipped.too_few_trajectory_stops,
        skipped_missing_pattern_geometry,
        trajectories.len(),
    );
}

fn trajectory_stop_times(stop_time_update: &AspenisedStopTimeUpdate) -> Option<(i64, i64)> {
    let arrival_time = stop_time_update
        .arrival
        .as_ref()
        .and_then(|arrival| arrival.time)
        .unwrap_or(0);
    let departure_time = stop_time_update
        .departure
        .as_ref()
        .and_then(|departure| departure.time)
        .unwrap_or(arrival_time);

    (arrival_time != 0 || departure_time != 0).then_some((arrival_time, departure_time))
}

fn collect_trajectory_stops(
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
) -> Vec<(&AspenisedStopTimeUpdate, i64, i64)> {
    trip_update
        .stop_time_update
        .iter()
        .filter_map(|stop_time_update| {
            trajectory_stop_times(stop_time_update)
                .map(|(arrival, departure)| (stop_time_update, arrival, departure))
        })
        .collect()
}

#[derive(Clone, Copy)]
enum TripShapeSource<'a> {
    NoShape,
    Shape(&'a str),
}

fn trip_shape_source<'a>(
    aspenised_data: &'a AspenisedData,
    trip_id: &str,
) -> Option<TripShapeSource<'a>> {
    let Some(compressed_trip) = aspenised_data
        .compressed_trip_internal_cache
        .compressed_trips
        .get(trip_id)
    else {
        // No static compressed trip means there is no shape metadata to wait for.
        return Some(TripShapeSource::NoShape);
    };

    let (meta, _) = aspenised_data
        .itinerary_pattern_internal_cache
        .itinerary_patterns
        .get(&compressed_trip.itinerary_pattern_id)?;

    Some(match meta.shape_id.as_deref() {
        Some(shape_id) => TripShapeSource::Shape(shape_id),
        None => TripShapeSource::NoShape,
    })
}

fn build_segments(
    aspenised_data: &AspenisedData,
    trajectory_stops: &[(&AspenisedStopTimeUpdate, i64, i64)],
    trip_shape: Option<&ShapeLineString>,
) -> Vec<AspenisedTrajectorySegment> {
    let mut segments = Vec::with_capacity(trajectory_stops.len().saturating_sub(1));
    if trajectory_stops.len() < 2 {
        return segments;
    }

    if let Some(shape) = trip_shape.filter(|shape| !shape.points.is_empty()) {
        // Match each stop monotonically along the shape. The old implementation
        // re-scanned the current stop on every segment even though the previous
        // segment had already found its globally-nearest forward index. Reusing
        // that index halves the suffix scans while preserving the same greedy
        // monotonic matching rule.
        let first_stop_coordinate = stop_coordinates(aspenised_data, trajectory_stops[0].0);
        let mut last_shape_idx = nearest_shape_index(&shape.points, &first_stop_coordinate, 0);

        for i in 0..trajectory_stops.len() - 1 {
            let to_coordinate = stop_coordinates(aspenised_data, trajectory_stops[i + 1].0);
            let next_shape_idx = nearest_shape_index(&shape.points, &to_coordinate, last_shape_idx);
            let segment_coordinates = shape.points[last_shape_idx..=next_shape_idx]
                .iter()
                .map(|point| [point.x, point.y])
                .collect();

            segments.push(AspenisedTrajectorySegment {
                from_stop_index: i,
                to_stop_index: i + 1,
                coordinates: segment_coordinates,
            });
            last_shape_idx = next_shape_idx;
        }

        return segments;
    }

    for i in 0..trajectory_stops.len() - 1 {
        segments.push(AspenisedTrajectorySegment {
            from_stop_index: i,
            to_stop_index: i + 1,
            coordinates: vec![
                stop_coordinates(aspenised_data, trajectory_stops[i].0),
                stop_coordinates(aspenised_data, trajectory_stops[i + 1].0),
            ],
        });
    }

    segments
}

fn nearest_shape_index(
    points: &[postgis_diesel::types::Point],
    target: &[f64; 2],
    start_idx: usize,
) -> usize {
    debug_assert!(!points.is_empty());
    let start_idx = start_idx.min(points.len() - 1);
    let mut best_idx = start_idx;
    let mut best_distance = f64::MAX;

    for (offset, point) in points[start_idx..].iter().enumerate() {
        let dx = point.x - target[0];
        let dy = point.y - target[1];
        let distance = dx * dx + dy * dy;
        if distance < best_distance {
            best_distance = distance;
            best_idx = start_idx + offset;
        }
    }

    best_idx
}

fn stop_coordinates(
    aspenised_data: &AspenisedData,
    stop_time_update: &AspenisedStopTimeUpdate,
) -> [f64; 2] {
    let Some(stop_id) = &stop_time_update.stop_id else {
        return [0.0, 0.0];
    };
    let Some(stop) = aspenised_data.stop_id_to_stop.get(stop_id.as_ref()) else {
        return [0.0, 0.0];
    };

    [
        stop.stop_lon.unwrap_or(0.0) as f64,
        stop.stop_lat.unwrap_or(0.0) as f64,
    ]
}

fn trip_short_name(
    aspenised_data: &AspenisedData,
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
    trip_id: &str,
) -> Option<String> {
    trip_update
        .trip_properties
        .as_ref()
        .and_then(|properties| properties.trip_short_name.clone())
        .or_else(|| {
            aspenised_data
                .compressed_trip_internal_cache
                .compressed_trips
                .get(trip_id)
                .and_then(|trip| trip.trip_short_name.clone().map(|name| name.to_string()))
        })
}

fn pattern_id_for_trip<'a>(
    aspenised_data: &'a AspenisedData,
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
    trip_id: &str,
    route_id: &str,
) -> Cow<'a, str> {
    aspenised_data
        .compressed_trip_internal_cache
        .compressed_trips
        .get(trip_id)
        .map(|trip| Cow::Borrowed(trip.itinerary_pattern_id.as_str()))
        .unwrap_or_else(|| {
            Cow::Owned(format!(
                "{}_dir_{}",
                route_id,
                trip_update.trip.direction_id.unwrap_or(0)
            ))
        })
}

#[allow(clippy::too_many_arguments)]
fn ensure_pattern(
    pattern_id_str: &str,
    route_id: &str,
    route_type: i16,
    route_type_name: &str,
    segments: &[AspenisedTrajectorySegment],
    geometries: &mut Vec<PackedGeometry>,
    patterns: &mut Vec<PatternGeometry>,
    pattern_map: &mut AHashMap<CompactString, u32>,
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) -> (u32, bool) {
    if let Some(&idx) = pattern_map.get(pattern_id_str) {
        return (idx, false);
    }

    let new_idx = patterns.len() as u32;
    let geometry_id = geometries.len() as u32;
    let (packed_geometry, segment_ranges) = pack_pattern_geometry(segments);

    geometries.push(packed_geometry);
    patterns.push(PatternGeometry {
        pattern_id_str: CompactString::new(pattern_id_str),
        route_id: CompactString::new(route_id),
        mode: CompactString::new(route_type_name),
        geometry_id,
        segments: segment_ranges.into_boxed_slice(),
        distance: 0.0,
    });

    index_pattern_bboxes(new_idx, route_type, segments, rtree_by_route_type);
    pattern_map.insert(CompactString::new(pattern_id_str), new_idx);

    (new_idx, true)
}

fn pack_pattern_geometry(
    segments: &[AspenisedTrajectorySegment],
) -> (PackedGeometry, Vec<SegmentRange>) {
    let total_coordinates = segments
        .iter()
        .map(|segment| segment.coordinates.len())
        .sum::<usize>();
    let mut coordinates = Vec::with_capacity(total_coordinates);
    let mut segment_ranges = Vec::with_capacity(segments.len());

    for segment in segments {
        let coordinate_start = coordinates.len() as u32;
        coordinates.extend(segment.coordinates.iter().map(|&[lon, lat]| {
            [
                (lon * 1_000_000.0).round() as i32,
                (lat * 1_000_000.0).round() as i32,
            ]
        }));
        let coordinate_end = coordinates.len() as u32;
        segment_ranges.push(SegmentRange {
            from_stop_index: segment.from_stop_index as u16,
            to_stop_index: segment.to_stop_index as u16,
            coordinate_start,
            coordinate_end,
        });
    }

    (
        PackedGeometry {
            coordinates: coordinates.into_boxed_slice(),
        },
        segment_ranges,
    )
}

fn index_pattern_bboxes(
    pattern_id: u32,
    route_type: i16,
    segments: &[AspenisedTrajectorySegment],
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) {
    if route_type == 1 || route_type == 2 {
        index_train_pattern_bboxes(pattern_id, route_type, segments, rtree_by_route_type);
    } else {
        index_long_pattern_bboxes(pattern_id, route_type, segments, rtree_by_route_type);
    }
}

fn index_train_pattern_bboxes(
    pattern_id: u32,
    route_type: i16,
    segments: &[AspenisedTrajectorySegment],
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) {
    for segment in segments {
        let Some(bbox) = bounding_box(pattern_id, &segment.coordinates) else {
            continue;
        };
        rtree_by_route_type
            .entry(route_type)
            .or_insert_with(rstar::RTree::new)
            .insert(bbox);
    }
}

fn index_long_pattern_bboxes(
    pattern_id: u32,
    route_type: i16,
    segments: &[AspenisedTrajectorySegment],
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) {
    let mut current_min_lon = f64::MAX;
    let mut current_min_lat = f64::MAX;
    let mut current_max_lon = f64::MIN;
    let mut current_max_lat = f64::MIN;
    let mut current_dist = 0.0;
    let mut last_coord: Option<[f64; 2]> = None;

    for segment in segments {
        for coord in &segment.coordinates {
            current_min_lon = current_min_lon.min(coord[0]);
            current_min_lat = current_min_lat.min(coord[1]);
            current_max_lon = current_max_lon.max(coord[0]);
            current_max_lat = current_max_lat.max(coord[1]);

            if let Some(last) = last_coord {
                let dx = (coord[0] - last[0]) * METERS_PER_DEGREE * last[1].to_radians().cos();
                let dy = (coord[1] - last[1]) * METERS_PER_DEGREE;
                current_dist += (dx * dx + dy * dy).sqrt();
            }
            last_coord = Some(*coord);

            if current_dist >= LONG_BBOX_DISTANCE_METERS {
                insert_bbox(
                    pattern_id,
                    route_type,
                    current_min_lon,
                    current_min_lat,
                    current_max_lon,
                    current_max_lat,
                    rtree_by_route_type,
                );
                current_min_lon = f64::MAX;
                current_min_lat = f64::MAX;
                current_max_lon = f64::MIN;
                current_max_lat = f64::MIN;
                current_dist = 0.0;
            }
        }
    }

    if current_dist > 0.0 || current_min_lon != f64::MAX {
        insert_bbox(
            pattern_id,
            route_type,
            current_min_lon,
            current_min_lat,
            current_max_lon,
            current_max_lat,
            rtree_by_route_type,
        );
    }
}

fn bounding_box(pattern_id: u32, coordinates: &[[f64; 2]]) -> Option<PatternBBox> {
    if coordinates.is_empty() {
        return None;
    }

    let mut min_lon = f64::MAX;
    let mut min_lat = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut max_lat = f64::MIN;

    for coord in coordinates {
        min_lon = min_lon.min(coord[0]);
        min_lat = min_lat.min(coord[1]);
        max_lon = max_lon.max(coord[0]);
        max_lat = max_lat.max(coord[1]);
    }

    Some(PatternBBox {
        pattern_id,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
    })
}

#[allow(clippy::too_many_arguments)]
fn insert_bbox(
    pattern_id: u32,
    route_type: i16,
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
) {
    rtree_by_route_type
        .entry(route_type)
        .or_insert_with(rstar::RTree::new)
        .insert(PatternBBox {
            pattern_id,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        });
}

fn build_trajectory_instance(
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
    trip_id: &str,
    pattern_id: u32,
    trip_short_name: Option<String>,
    trajectory_stops: &[(&AspenisedStopTimeUpdate, i64, i64)],
) -> TrajectoryInstance {
    let stops = trajectory_stops
        .iter()
        .map(
            |(stop_time_update, arrival, departure)| TrajectoryStopInstance {
                stop_id: stop_time_update
                    .stop_id
                    .as_ref()
                    .map(|id| CompactString::new(id.as_ref())),
                track: stop_time_update
                    .platform_string
                    .as_ref()
                    .map(|platform| CompactString::new(platform.as_str())),
                arrival: *arrival,
                departure: *departure,
            },
        )
        .collect::<Vec<_>>()
        .into_boxed_slice();

    let start_time = trip_update
        .trip
        .start_time
        .as_ref()
        .and_then(|time| parse_start_time(time));
    let service_date = trip_update
        .trip
        .start_date
        .and_then(|date| date.format("%Y%m%d").to_string().parse::<i32>().ok());

    TrajectoryInstance {
        pattern_id,
        trip_id: CompactString::new(trip_id),
        start_time,
        service_date,
        trip_short_name: trip_short_name.map(CompactString::new),
        stops,
    }
}

fn route_type_name(route_type: i16) -> &'static str {
    match route_type {
        0 => "tram",
        1 => "subway",
        2 => "rail",
        3 => "bus",
        4 => "ferry",
        5 => "cable_car",
        6 => "gondola",
        7 => "funicular",
        11 => "trolleybus",
        12 => "monorail",
        _ => "other",
    }
}

fn is_special_chateau(chateau_id: &str) -> bool {
    chateau_id == "busÉireann" || chateau_id == "nederland"
}

fn parse_start_time(time_str: &str) -> Option<i32> {
    let mut parts = time_str.split(':');
    let hrs: i32 = parts.next()?.parse().ok()?;
    let mins: i32 = parts.next()?.parse().ok()?;
    let secs: i32 = parts.next()?.parse().ok()?;
    Some(hrs * 3600 + mins * 60 + secs)
}
