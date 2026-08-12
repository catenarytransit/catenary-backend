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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

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

type ShapeLineString = postgis_diesel::types::LineString<postgis_diesel::types::Point>;

pub(crate) struct TrajectoryBuildResult {
    pub(crate) store: Arc<AspenTrajectoryStore>,
    pub(crate) static_changed: bool,
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
    let shape_ids = collect_shape_ids(
        chateau_id,
        aspenised_data,
        &vehicle_trip_ids,
        &vehicle_route_ids,
    );
    let shape_linestrings = fetch_shape_linestrings(chateau_id, shape_ids, pool).await;

    if ALLOWED_CHATEAUX.contains(&chateau_id) {
        process_trip_updates(
            chateau_id,
            aspenised_data,
            &vehicle_trip_ids,
            &vehicle_route_ids,
            &shape_linestrings,
            &mut geometries,
            &mut patterns,
            &mut pattern_map,
            &mut trajectories,
            &mut pattern_to_trajectories,
            &mut rtree_by_route_type,
            &mut static_changed,
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

    TrajectoryBuildResult {
        store: Arc::new(store),
        static_changed,
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

fn collect_shape_ids(
    chateau_id: &str,
    aspenised_data: &AspenisedData,
    vehicle_trip_ids: &AHashSet<String>,
    vehicle_route_ids: &AHashSet<String>,
) -> HashSet<String> {
    let mut shape_ids_to_fetch = HashSet::new();

    if !ALLOWED_CHATEAUX.contains(&chateau_id) {
        return shape_ids_to_fetch;
    }

    let special_chateau = is_special_chateau(chateau_id);

    for trip_update in aspenised_data.trip_updates.values() {
        if let Some(trip_id) = &trip_update.trip.trip_id {
            if vehicle_trip_ids.contains(trip_id) {
                continue;
            }
        }

        if let Some(route_id) = &trip_update.trip.route_id {
            if vehicle_route_ids.contains(route_id) {
                continue;
            }
        }

        if special_chateau {
            let is_rail = trip_update
                .trip
                .route_id
                .as_ref()
                .and_then(|route_id| aspenised_data.vehicle_routes_cache.get(route_id))
                .map(|route| route.route_type == 2)
                .unwrap_or(false);
            if !is_rail {
                continue;
            }
        }

        let Some(trip_id) = &trip_update.trip.trip_id else {
            continue;
        };
        let Some(compressed_trip) = aspenised_data
            .compressed_trip_internal_cache
            .compressed_trips
            .get(trip_id.as_str())
        else {
            continue;
        };
        let Some((meta, _)) = aspenised_data
            .itinerary_pattern_internal_cache
            .itinerary_patterns
            .get(&compressed_trip.itinerary_pattern_id)
        else {
            continue;
        };
        if let Some(shape_id) = &meta.shape_id {
            shape_ids_to_fetch.insert(shape_id.clone());
        }
    }

    shape_ids_to_fetch
}

async fn fetch_shape_linestrings(
    chateau_id: &str,
    shape_ids_to_fetch: HashSet<String>,
    pool: &CatenaryPostgresPool,
) -> HashMap<String, ShapeLineString> {
    let mut shape_linestrings = HashMap::new();
    let shape_ids: Vec<String> = shape_ids_to_fetch.into_iter().collect();

    if shape_ids.is_empty() {
        return shape_linestrings;
    }

    println!(
        "Chateau {}: Attempting to fetch {} unique shapes from DB",
        chateau_id,
        shape_ids.len()
    );

    match pool.get().await {
        Ok(mut conn_pre) => {
            use catenary::schema::gtfs::shapes::dsl::*;

            for chunk in shape_ids.chunks(500) {
                println!(
                    "Chateau {}: Querying database for chunk of {} shapes. filter: onestop_feed_id = {}",
                    chateau_id,
                    chunk.len(),
                    chateau_id
                );

                match shapes
                    .filter(chateau.eq(chateau_id))
                    .filter(shape_id.eq_any(chunk))
                    .select((shape_id, linestring))
                    .load::<(String, ShapeLineString)>(&mut conn_pre)
                    .await
                {
                    Ok(shapes_result) => {
                        println!(
                            "Chateau {}: Successfully retrieved {}/{} shapes for this chunk",
                            chateau_id,
                            shapes_result.len(),
                            chunk.len()
                        );
                        shape_linestrings.extend(shapes_result);
                    }
                    Err(error) => {
                        eprintln!(
                            "Chateau {}: Error fetching shapes from database: {:?}",
                            chateau_id, error
                        );
                    }
                }
            }
        }
        Err(error) => {
            eprintln!(
                "Chateau {}: Failed to get database connection from pool: {:?}",
                chateau_id, error
            );
        }
    }

    shape_linestrings
}

#[allow(clippy::too_many_arguments)]
fn process_trip_updates(
    chateau_id: &str,
    aspenised_data: &AspenisedData,
    vehicle_trip_ids: &AHashSet<String>,
    vehicle_route_ids: &AHashSet<String>,
    shape_linestrings: &HashMap<String, ShapeLineString>,
    geometries: &mut Vec<PackedGeometry>,
    patterns: &mut Vec<PatternGeometry>,
    pattern_map: &mut AHashMap<CompactString, u32>,
    trajectories: &mut Vec<TrajectoryInstance>,
    pattern_to_trajectories: &mut Vec<Vec<u32>>,
    rtree_by_route_type: &mut AHashMap<i16, rstar::RTree<PatternBBox>>,
    static_changed: &mut bool,
) {
    println!("Starting trajectory processing for {}", chateau_id);

    let mut skipped_no_stops = 0;
    let mut skipped_no_trip_id = 0;
    let mut skipped_no_route_id = 0;
    let mut skipped_no_route_cache = 0;
    let mut skipped_too_few_trajectory_stops = 0;
    let mut skipped_too_few_shape_coords = 0;
    let special_chateau = is_special_chateau(chateau_id);

    for trip_update in aspenised_data.trip_updates.values() {
        if trip_update.stop_time_update.is_empty() {
            skipped_no_stops += 1;
            continue;
        }

        let trip_id = match &trip_update.trip.trip_id {
            Some(id) => id,
            None => {
                skipped_no_trip_id += 1;
                continue;
            }
        };

        let route_id = match &trip_update.trip.route_id {
            Some(id) => id,
            None => {
                skipped_no_route_id += 1;
                continue;
            }
        };

        if vehicle_trip_ids.contains(trip_id) || vehicle_route_ids.contains(route_id) {
            continue;
        }

        let route = match aspenised_data.vehicle_routes_cache.get(route_id) {
            Some(route) => route,
            None => {
                skipped_no_route_cache += 1;
                continue;
            }
        };

        if special_chateau && route.route_type != 2 {
            continue;
        }

        let trajectory_stops = collect_trajectory_stops(trip_update);
        if trajectory_stops.len() < 2 {
            skipped_too_few_trajectory_stops += 1;
            continue;
        }

        let trip_shape_coords = trip_shape_coordinates(aspenised_data, trip_id, shape_linestrings);
        if trip_shape_coords.is_none() {
            skipped_too_few_shape_coords += 1;
        }

        let segments = build_segments(
            aspenised_data,
            &trajectory_stops,
            trip_shape_coords.as_deref(),
        );

        let trip_short_name = trip_short_name(aspenised_data, trip_update, trip_id);
        let pattern_id_str = pattern_id_for_trip(aspenised_data, trip_update, trip_id, route_id);
        let (pattern_idx, inserted_pattern) = ensure_pattern(
            &pattern_id_str,
            route_id,
            route.route_type,
            route_type_name(route.route_type),
            &segments,
            geometries,
            patterns,
            pattern_map,
            rtree_by_route_type,
        );
        *static_changed |= inserted_pattern;

        let trajectory_idx = trajectories.len() as u32;
        trajectories.push(build_trajectory_instance(
            trip_update,
            trip_id,
            pattern_idx,
            trip_short_name,
            &trajectory_stops,
        ));

        if pattern_idx as usize >= pattern_to_trajectories.len() {
            pattern_to_trajectories.resize_with(pattern_idx as usize + 1, Vec::new);
        }
        pattern_to_trajectories[pattern_idx as usize].push(trajectory_idx);
    }

    println!(
        "Trajectory computation for chateau {}: \
         total_trip_updates={}, \
         skipped_no_stops={}, \
         skipped_no_trip_id={}, \
         skipped_no_route_id={}, \
         skipped_no_route_cache={}, \
         skipped_too_few_trajectory_stops={}, \
         skipped_too_few_shape_coords={}, \
         built_trajectories={}",
        chateau_id,
        aspenised_data.trip_updates.len(),
        skipped_no_stops,
        skipped_no_trip_id,
        skipped_no_route_id,
        skipped_no_route_cache,
        skipped_too_few_trajectory_stops,
        skipped_too_few_shape_coords,
        rtree_by_route_type
            .values()
            .map(|tree| tree.size())
            .sum::<usize>()
    );
}

fn collect_trajectory_stops(
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
) -> Vec<(&AspenisedStopTimeUpdate, i64, i64)> {
    trip_update
        .stop_time_update
        .iter()
        .filter_map(|stop_time_update| {
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

            (arrival_time != 0 || departure_time != 0).then_some((
                stop_time_update,
                arrival_time,
                departure_time,
            ))
        })
        .collect()
}

fn trip_shape_coordinates(
    aspenised_data: &AspenisedData,
    trip_id: &str,
    shape_linestrings: &HashMap<String, ShapeLineString>,
) -> Option<Vec<[f64; 2]>> {
    let compressed_trip = aspenised_data
        .compressed_trip_internal_cache
        .compressed_trips
        .get(trip_id)?;
    let (meta, _) = aspenised_data
        .itinerary_pattern_internal_cache
        .itinerary_patterns
        .get(&compressed_trip.itinerary_pattern_id)?;
    let shape_id = meta.shape_id.as_ref()?;
    let linestring = shape_linestrings.get(shape_id)?;
    let coordinates: Vec<[f64; 2]> = linestring
        .points
        .iter()
        .map(|point| [point.x, point.y])
        .collect();

    (!coordinates.is_empty()).then_some(coordinates)
}

fn build_segments(
    aspenised_data: &AspenisedData,
    trajectory_stops: &[(&AspenisedStopTimeUpdate, i64, i64)],
    trip_shape_coords: Option<&[[f64; 2]]>,
) -> Vec<AspenisedTrajectorySegment> {
    let mut segments = Vec::with_capacity(trajectory_stops.len().saturating_sub(1));
    let mut last_shape_idx = 0;

    for i in 0..trajectory_stops.len() - 1 {
        let from_stu = trajectory_stops[i].0;
        let to_stu = trajectory_stops[i + 1].0;
        let mut segment_coordinates = vec![
            stop_coordinates(aspenised_data, from_stu),
            stop_coordinates(aspenised_data, to_stu),
        ];

        if let Some(coords) = trip_shape_coords {
            let mut best_start = last_shape_idx;
            let mut min_start_dist = f64::MAX;

            for (idx, point) in coords.iter().enumerate().skip(last_shape_idx) {
                let distance = squared_distance(point, &segment_coordinates[0]);
                if distance < min_start_dist {
                    min_start_dist = distance;
                    best_start = idx;
                }
            }

            let mut best_end = best_start;
            let mut min_end_dist = f64::MAX;

            for (idx, point) in coords.iter().enumerate().skip(best_start) {
                let distance = squared_distance(point, &segment_coordinates[1]);
                if distance < min_end_dist {
                    min_end_dist = distance;
                    best_end = idx;
                }
            }

            last_shape_idx = best_end;
            if best_start <= best_end {
                segment_coordinates = coords[best_start..=best_end].to_vec();
            }
        }

        segments.push(AspenisedTrajectorySegment {
            from_stop_index: i,
            to_stop_index: i + 1,
            coordinates: segment_coordinates,
        });
    }

    segments
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

fn squared_distance(a: &[f64; 2], b: &[f64; 2]) -> f64 {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    dx * dx + dy * dy
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

fn pattern_id_for_trip(
    aspenised_data: &AspenisedData,
    trip_update: &catenary::aspen_dataset::AspenisedTripUpdate,
    trip_id: &str,
    route_id: &str,
) -> String {
    aspenised_data
        .compressed_trip_internal_cache
        .compressed_trips
        .get(trip_id)
        .map(|trip| trip.itinerary_pattern_id.to_string())
        .unwrap_or_else(|| {
            format!(
                "{}_dir_{}",
                route_id,
                trip_update.trip.direction_id.unwrap_or(0)
            )
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
    let mut pattern_coordinates = Vec::new();
    let mut segment_ranges = Vec::with_capacity(segments.len());

    for segment in segments {
        let coordinate_start = pattern_coordinates.len() as u32;
        pattern_coordinates.extend_from_slice(&segment.coordinates);
        let coordinate_end = pattern_coordinates.len() as u32;
        segment_ranges.push(SegmentRange {
            from_stop_index: segment.from_stop_index as u16,
            to_stop_index: segment.to_stop_index as u16,
            coordinate_start,
            coordinate_end,
        });
    }

    let coordinates = pattern_coordinates
        .iter()
        .map(|&[lon, lat]| {
            [
                (lon * 1_000_000.0).round() as i32,
                (lat * 1_000_000.0).round() as i32,
            ]
        })
        .collect::<Vec<[i32; 2]>>()
        .into_boxed_slice();

    (PackedGeometry { coordinates }, segment_ranges)
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
