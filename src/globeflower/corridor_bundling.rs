// ===========================================================================
// Corridor Bundling for Intercity Rail
// ===========================================================================
//
// This module implements Principal Component Analysis (PCA) based corridor
// detection and bundling to fix the "wobbly" appearance of parallel intercity
// rail tracks.
//
// The Problem:
// When 10+ intercity rail tracks run in parallel on the same corridor,
// point-by-point merging creates oscillations because:
// 1. Processing order affects weighted centroid
// 2. 20m resampling creates beat patterns with offset input vertices
// 3. Pairwise folding is non-transitive (A+B)+C ≠ (B+C)+A
//
// The Solution:
// 1. Detect corridors (groups of parallel intercity rail edges)
// 2. Compute principal axis using PCA
// 3. Fit a master polyline along the corridor centerline
// 4. Assign consistent offsets to each track
// 5. Replace geometries with smooth parallel lines
// ===========================================================================

use crate::graph_types::{EdgeRef, LineGraph, NodeRef};
use crate::route_registry::RouteId;
use crate::osm_rail_graph;
use ahash::{AHashMap, AHashSet};
use geo::Coord;
use std::rc::Rc;

/// Represents a detected corridor of parallel intercity rail tracks
#[derive(Debug)]
pub struct Corridor {
    pub id: usize,
    /// Edges that belong to this corridor
    pub edge_ids: Vec<usize>,
    /// The computed master polyline (centerline of corridor)
    pub master_polyline: Vec<Coord>,
    /// Principal direction unit vector (dx, dy)
    pub principal_direction: (f64, f64),
    /// Perpendicular direction unit vector
    pub perpendicular_direction: (f64, f64),
    /// Offset assignment for each route (positive = right of center, negative = left)
    pub route_offsets: AHashMap<RouteId, f64>,
    /// Total corridor width
    pub width: f64,
}

/// Configuration for corridor detection
pub struct CorridorConfig {
    /// Minimum corridor length in meters (shorter corridors are ignored)
    pub min_corridor_length: f64,
    /// Minimum number of parallel tracks to form a corridor
    pub min_parallel_tracks: usize,
    /// Maximum width of corridor in meters
    pub max_corridor_width: f64,
    /// Spacing between offset tracks in meters
    pub offset_spacing: f64,
    /// Minimum parallelism (dot product of directions, 0.95 = ~18 degrees)
    pub min_parallelism: f64,
}

impl Default for CorridorConfig {
    fn default() -> Self {
        Self {
            // Reduced from 500m to catch shorter station platform spreads
            min_corridor_length: 200.0,
            // Reduced from 3 to detect diverge-reconverge patterns with just 2 tracks
            min_parallel_tracks: 2,
            // Increased from 100m to handle wide terminal platforms (24+ platforms)
            max_corridor_width: 200.0,
            offset_spacing: 5.0,
            min_parallelism: 0.95,
        }
    }
}

/// Detect and bundle intercity rail corridors in the graph
///
/// Returns a list of detected corridors with their master polylines and offset assignments.
pub fn detect_and_bundle_corridors(graph: &LineGraph, config: &CorridorConfig) -> Vec<Corridor> {
    const INTERCITY_RAIL: i32 = 2;

    // Step 1: Collect all intercity rail edges with their geometries
    let intercity_edges: Vec<(usize, EdgeRef, Vec<Coord>)> = graph
        .nodes
        .iter()
        .flat_map(|node| {
            node.borrow()
                .adj_list
                .iter()
                .filter(|e| Rc::ptr_eq(&e.borrow().from, node))
                .filter(|e| e.borrow().routes.iter().any(|r| r.route_type == INTERCITY_RAIL))
                .map(|e| {
                    let edge_id = e.borrow().id;
                    let geom = e.borrow().geometry.clone();
                    (edge_id, Rc::clone(e), geom)
                })
                .collect::<Vec<_>>()
        })
        .collect();

    if intercity_edges.len() < config.min_parallel_tracks {
        return Vec::new();
    }

    // Step 2: Build spatial index for edge midpoints
    let edge_data: Vec<EdgeData> = intercity_edges
        .iter()
        .map(|(id, edge_ref, geom)| {
            let (midpoint, direction, length) = compute_edge_stats(geom);
            let routes: Vec<RouteId> = edge_ref
                .borrow()
                .routes
                .iter()
                .map(|r| r.line_id)
                .collect();
            EdgeData {
                id: *id,
                edge_ref: Rc::clone(edge_ref),
                geometry: geom.clone(),
                midpoint,
                direction,
                length,
                routes,
            }
        })
        .filter(|e| e.length >= config.min_corridor_length)
        .collect();

    if edge_data.len() < config.min_parallel_tracks {
        return Vec::new();
    }

    // Step 3: Find corridor candidates using spatial proximity
    let mut corridors = Vec::new();
    let mut used_edges: AHashSet<usize> = AHashSet::new();
    let mut corridor_id = 0;

    for (i, seed) in edge_data.iter().enumerate() {
        if used_edges.contains(&seed.id) {
            continue;
        }

        // Find all edges that are parallel and close to the seed
        let mut corridor_edges: Vec<&EdgeData> = vec![seed];

        for (j, candidate) in edge_data.iter().enumerate() {
            if i == j || used_edges.contains(&candidate.id) {
                continue;
            }

            // Check parallelism (dot product of directions)
            let dot = seed.direction.0 * candidate.direction.0
                + seed.direction.1 * candidate.direction.1;
            if dot.abs() < config.min_parallelism {
                continue;
            }

            // Check proximity (perpendicular distance between midpoints)
            let perp_dist = perpendicular_distance(
                seed.midpoint,
                candidate.midpoint,
                seed.direction,
            );
            if perp_dist > config.max_corridor_width {
                continue;
            }

            // Check overlap (edges should run along similar stretch)
            let along_dist = along_axis_distance(
                seed.midpoint,
                candidate.midpoint,
                seed.direction,
            );
            // Allow significant along-axis separation for long corridors
            let max_along = (seed.length + candidate.length) / 2.0;
            if along_dist > max_along {
                continue;
            }

            // ===========================================================
            // OSM VALIDATION FOR CORRIDOR BUNDLING
            // ===========================================================
            // Tracks should only be bundled if they actually belong to the same
            // physical corridor (share junction or same way).
            // Exception: RER A / Metro 1 case (parallel for >400m, <30m separation)
            if let Some(osm_index) = osm_rail_graph::get_osm_index() {
                // Check coverage first
                let p1 = [seed.midpoint.0, seed.midpoint.1];
                let p2 = [candidate.midpoint.0, candidate.midpoint.1];
                
                if osm_index.is_in_coverage(p1, 500.0) && osm_index.is_in_coverage(p2, 500.0) {
                    // Check if connected in OSM
                    // Use larger radius (50m) as these are midpoints
                    let is_connected = osm_index.share_junction(p1, p2, 50.0).is_some() 
                        || osm_index.on_same_way(p1, p2, 50.0);
                        
                    if !is_connected {
                        // Not connected in OSM - Check for "RER A / Metro 1" exception
                        // User specified: <30m distance and >400m length
                        let is_exception = perp_dist < 30.0 
                            && seed.length > 400.0 
                            && candidate.length > 400.0;
                            
                        if !is_exception {
                            // Distinct tracks, not meeting exception - DO NOT BUNDLE
                            continue;
                        }
                    }
                }
            }

            corridor_edges.push(candidate);
        }

        // Need at least min_parallel_tracks to form a corridor
        if corridor_edges.len() < config.min_parallel_tracks {
            continue;
        }

        // Mark all edges as used
        for edge in &corridor_edges {
            used_edges.insert(edge.id);
        }

        // Step 4: Compute corridor using PCA
        if let Some(corridor) = compute_corridor(corridor_id, &corridor_edges, config) {
            corridors.push(corridor);
            corridor_id += 1;
        }
    }

    println!(
        "  Detected {} intercity rail corridors with {} total edges",
        corridors.len(),
        used_edges.len()
    );

    corridors
}

/// Apply corridor geometries to the graph
///
/// Replaces wobbly edge geometries with smooth parallel lines offset from the master polyline.
pub fn apply_corridor_geometries(graph: &mut LineGraph, corridors: &[Corridor], config: &CorridorConfig) {
    let mut edges_updated = 0;

    // Build edge ID to EdgeRef map
    let mut edge_map: AHashMap<usize, EdgeRef> = AHashMap::new();
    for node in &graph.nodes {
        for edge_ref in &node.borrow().adj_list {
            if Rc::ptr_eq(&edge_ref.borrow().from, node) {
                edge_map.insert(edge_ref.borrow().id, Rc::clone(edge_ref));
            }
        }
    }

    for corridor in corridors {
        for edge_id in &corridor.edge_ids {
            if let Some(edge_ref) = edge_map.get(edge_id) {
                // Get routes on this edge to determine offset
                let routes: Vec<RouteId> = edge_ref
                    .borrow()
                    .routes
                    .iter()
                    .map(|r| r.line_id)
                    .collect();

                // Compute average offset for this edge's routes
                let mut total_offset = 0.0;
                let mut count = 0;
                for route_id in &routes {
                    if let Some(offset) = corridor.route_offsets.get(route_id) {
                        total_offset += offset;
                        count += 1;
                    }
                }
                let offset = if count > 0 {
                    total_offset / count as f64
                } else {
                    0.0
                };

                // Generate offset geometry
                let new_geom = generate_offset_polyline(
                    &corridor.master_polyline,
                    corridor.perpendicular_direction,
                    offset,
                );

                // Trim to match original endpoints
                let old_geom = edge_ref.borrow().geometry.clone();
                let trimmed_geom = trim_to_endpoints(&new_geom, &old_geom);

                edge_ref.borrow_mut().geometry = trimmed_geom;
                edges_updated += 1;
            }
        }
    }

    println!("  Updated {} edge geometries from corridor bundling", edges_updated);
}

// ===========================================================================
// Internal Helper Types and Functions
// ===========================================================================

struct EdgeData {
    id: usize,
    edge_ref: EdgeRef,
    geometry: Vec<Coord>,
    midpoint: (f64, f64),
    direction: (f64, f64), // Unit vector
    length: f64,
    routes: Vec<RouteId>,
}

/// Compute edge statistics: midpoint, direction unit vector, length
fn compute_edge_stats(geom: &[Coord]) -> ((f64, f64), (f64, f64), f64) {
    if geom.len() < 2 {
        return ((0.0, 0.0), (1.0, 0.0), 0.0);
    }

    // Calculate total length
    let mut length = 0.0;
    for i in 0..geom.len() - 1 {
        let dx = geom[i + 1].x - geom[i].x;
        let dy = geom[i + 1].y - geom[i].y;
        length += (dx * dx + dy * dy).sqrt();
    }

    // Find midpoint by traversing half the length
    let half_len = length / 2.0;
    let mut accum = 0.0;
    let mut midpoint = (geom[0].x, geom[0].y);
    for i in 0..geom.len() - 1 {
        let dx = geom[i + 1].x - geom[i].x;
        let dy = geom[i + 1].y - geom[i].y;
        let seg_len = (dx * dx + dy * dy).sqrt();
        if accum + seg_len >= half_len && seg_len > 0.0 {
            let frac = (half_len - accum) / seg_len;
            midpoint = (geom[i].x + dx * frac, geom[i].y + dy * frac);
            break;
        }
        accum += seg_len;
    }

    // Direction from first to last point
    let dx = geom.last().unwrap().x - geom.first().unwrap().x;
    let dy = geom.last().unwrap().y - geom.first().unwrap().y;
    let dir_len = (dx * dx + dy * dy).sqrt();
    let direction = if dir_len > 0.0 {
        (dx / dir_len, dy / dir_len)
    } else {
        (1.0, 0.0)
    };

    (midpoint, direction, length)
}

/// Calculate perpendicular distance from point to line through reference with given direction
fn perpendicular_distance(
    reference: (f64, f64),
    point: (f64, f64),
    direction: (f64, f64),
) -> f64 {
    let dx = point.0 - reference.0;
    let dy = point.1 - reference.1;
    // Perpendicular component = cross product with direction
    (dx * direction.1 - dy * direction.0).abs()
}

/// Calculate along-axis distance (projection onto direction)
fn along_axis_distance(
    reference: (f64, f64),
    point: (f64, f64),
    direction: (f64, f64),
) -> f64 {
    let dx = point.0 - reference.0;
    let dy = point.1 - reference.1;
    // Along-axis component = dot product with direction
    (dx * direction.0 + dy * direction.1).abs()
}

/// Compute corridor from a set of parallel edges using PCA
fn compute_corridor(id: usize, edges: &[&EdgeData], config: &CorridorConfig) -> Option<Corridor> {
    if edges.is_empty() {
        return None;
    }

    // Step 1: Sample points from all edges
    let mut all_points: Vec<(f64, f64)> = Vec::new();
    for edge in edges {
        for coord in &edge.geometry {
            all_points.push((coord.x, coord.y));
        }
    }

    if all_points.len() < 3 {
        return None;
    }

    // Step 2: Compute centroid
    let n = all_points.len() as f64;
    let cx: f64 = all_points.iter().map(|p| p.0).sum::<f64>() / n;
    let cy: f64 = all_points.iter().map(|p| p.1).sum::<f64>() / n;

    // Step 3: Compute 2D covariance matrix
    // [cov_xx, cov_xy]
    // [cov_xy, cov_yy]
    let mut cov_xx = 0.0;
    let mut cov_yy = 0.0;
    let mut cov_xy = 0.0;
    for p in &all_points {
        let dx = p.0 - cx;
        let dy = p.1 - cy;
        cov_xx += dx * dx;
        cov_yy += dy * dy;
        cov_xy += dx * dy;
    }
    cov_xx /= n;
    cov_yy /= n;
    cov_xy /= n;

    // Step 4: Compute principal eigenvector (2x2 analytic solution)
    // λ = (cov_xx + cov_yy ± sqrt((cov_xx - cov_yy)² + 4*cov_xy²)) / 2
    let trace = cov_xx + cov_yy;
    let det = cov_xx * cov_yy - cov_xy * cov_xy;
    let discriminant = (trace * trace - 4.0 * det).max(0.0).sqrt();
    let lambda1 = (trace + discriminant) / 2.0; // Larger eigenvalue

    // Eigenvector for λ1: (cov_xy, λ1 - cov_xx) or (λ1 - cov_yy, cov_xy)
    let (px, py) = if cov_xy.abs() > 1e-10 {
        let vx = cov_xy;
        let vy = lambda1 - cov_xx;
        let len = (vx * vx + vy * vy).sqrt();
        if len > 0.0 {
            (vx / len, vy / len)
        } else {
            (1.0, 0.0)
        }
    } else if cov_xx > cov_yy {
        (1.0, 0.0)
    } else {
        (0.0, 1.0)
    };

    let principal_direction = (px, py);
    let perpendicular_direction = (-py, px); // 90° rotation

    // Step 5: Project all points onto principal axis to find extent
    let mut min_t = f64::MAX;
    let mut max_t = f64::MIN;
    for p in &all_points {
        let t = (p.0 - cx) * px + (p.1 - cy) * py;
        min_t = min_t.min(t);
        max_t = max_t.max(t);
    }

    // Step 6: Create master polyline by binning along principal axis
    let corridor_length = max_t - min_t;
    if corridor_length < config.min_corridor_length {
        return None;
    }

    let bin_size = 20.0; // 20m bins for averaging
    let num_bins = ((corridor_length / bin_size).ceil() as usize).max(2);

    // For each bin, collect points and average their perpendicular positions
    let mut master_polyline: Vec<Coord> = Vec::with_capacity(num_bins);

    for i in 0..num_bins {
        let t = min_t + (i as f64 / (num_bins - 1) as f64) * corridor_length;
        let bin_start = t - bin_size / 2.0;
        let bin_end = t + bin_size / 2.0;

        // Collect perpendicular offsets of points in this bin
        let mut perp_sum = 0.0;
        let mut count = 0;
        for p in &all_points {
            let pt = (p.0 - cx) * px + (p.1 - cy) * py;
            if pt >= bin_start && pt <= bin_end {
                let perp = (p.0 - cx) * (-py) + (p.1 - cy) * px;
                perp_sum += perp;
                count += 1;
            }
        }

        let avg_perp = if count > 0 { perp_sum / count as f64 } else { 0.0 };

        // Convert back to coordinates
        let x = cx + t * px + avg_perp * (-py);
        let y = cy + t * py + avg_perp * px;
        master_polyline.push(Coord { x, y });
    }

    // Step 7: Smooth the master polyline
    master_polyline = chaikin_smooth(&master_polyline, 2);

    // Step 8: Assign offsets to routes
    let mut route_offsets: AHashMap<RouteId, f64> = AHashMap::new();
    let mut route_perp_sums: AHashMap<RouteId, (f64, usize)> = AHashMap::new();

    // Calculate average perpendicular position for each route
    for edge in edges {
        for coord in &edge.geometry {
            let perp = (coord.x - cx) * (-py) + (coord.y - cy) * px;
            for route_id in &edge.routes {
                let entry = route_perp_sums.entry(*route_id).or_insert((0.0, 0));
                entry.0 += perp;
                entry.1 += 1;
            }
        }
    }

    // Convert to average perpendicular offset
    let mut route_avg_perps: Vec<(RouteId, f64)> = route_perp_sums
        .into_iter()
        .map(|(id, (sum, count))| (id, sum / count as f64))
        .collect();

    // Sort by perpendicular offset (left to right)
    route_avg_perps.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Assign evenly-spaced offsets centered on the corridor
    let num_routes = route_avg_perps.len();
    let total_width = (num_routes as f64 - 1.0) * config.offset_spacing;
    let start_offset = -total_width / 2.0;

    for (i, (route_id, _)) in route_avg_perps.iter().enumerate() {
        let offset = start_offset + i as f64 * config.offset_spacing;
        route_offsets.insert(*route_id, offset);
    }

    // Collect edge IDs
    let edge_ids: Vec<usize> = edges.iter().map(|e| e.id).collect();

    Some(Corridor {
        id,
        edge_ids,
        master_polyline,
        principal_direction,
        perpendicular_direction,
        route_offsets,
        width: total_width,
    })
}

/// Generate a polyline offset from the master by perpendicular distance
fn generate_offset_polyline(
    master: &[Coord],
    perp_dir: (f64, f64),
    offset: f64,
) -> Vec<Coord> {
    master
        .iter()
        .map(|c| Coord {
            x: c.x + perp_dir.0 * offset,
            y: c.y + perp_dir.1 * offset,
        })
        .collect()
}

/// Trim/extend new geometry to match original endpoints
fn trim_to_endpoints(new_geom: &[Coord], old_geom: &[Coord]) -> Vec<Coord> {
    if new_geom.len() < 2 || old_geom.len() < 2 {
        return old_geom.to_vec();
    }

    let old_start = old_geom.first().unwrap();
    let old_end = old_geom.last().unwrap();

    // Find closest points on new geometry to old endpoints
    let start_idx = find_closest_point(new_geom, old_start);
    let end_idx = find_closest_point(new_geom, old_end);

    if start_idx >= end_idx {
        // Can't trim properly, return with replaced endpoints
        let mut result = new_geom.to_vec();
        result[0] = *old_start;
        *result.last_mut().unwrap() = *old_end;
        return result;
    }

    // Extract segment and replace endpoints
    let mut result: Vec<Coord> = new_geom[start_idx..=end_idx].to_vec();
    result[0] = *old_start;
    *result.last_mut().unwrap() = *old_end;

    result
}

/// Find index of closest point in polyline to target
fn find_closest_point(polyline: &[Coord], target: &Coord) -> usize {
    let mut best_idx = 0;
    let mut best_dist_sq = f64::MAX;

    for (i, c) in polyline.iter().enumerate() {
        let dx = c.x - target.x;
        let dy = c.y - target.y;
        let dist_sq = dx * dx + dy * dy;
        if dist_sq < best_dist_sq {
            best_dist_sq = dist_sq;
            best_idx = i;
        }
    }

    best_idx
}

/// Chaikin subdivision smoothing
fn chaikin_smooth(coords: &[Coord], iterations: usize) -> Vec<Coord> {
    let mut current = coords.to_vec();

    for _ in 0..iterations {
        if current.len() < 3 {
            break;
        }

        let mut new_coords = Vec::with_capacity(current.len() * 2);
        new_coords.push(current[0]); // Keep start

        for i in 0..current.len() - 1 {
            let p1 = current[i];
            let p2 = current[i + 1];

            // Q = 0.75 P1 + 0.25 P2
            let q = Coord {
                x: 0.75 * p1.x + 0.25 * p2.x,
                y: 0.75 * p1.y + 0.25 * p2.y,
            };

            // R = 0.25 P1 + 0.75 P2
            let r = Coord {
                x: 0.25 * p1.x + 0.75 * p2.x,
                y: 0.25 * p1.y + 0.75 * p2.y,
            };

            new_coords.push(q);
            new_coords.push(r);
        }

        new_coords.push(current[current.len() - 1]); // Keep end
        current = new_coords;
    }

    current
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perpendicular_distance() {
        // Line along x-axis through origin
        let reference = (0.0, 0.0);
        let direction = (1.0, 0.0);

        // Point directly above should have perpendicular distance of 5
        let point = (3.0, 5.0);
        let dist = perpendicular_distance(reference, point, direction);
        assert!((dist - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_chaikin_smooth() {
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
            Coord { x: 10.0, y: 10.0 },
        ];
        let smoothed = chaikin_smooth(&coords, 1);
        // After 1 iteration: start, Q1, R1, Q2, R2, end = 6 points
        assert_eq!(smoothed.len(), 6);
        // Start and end preserved
        assert!((smoothed[0].x - 0.0).abs() < 1e-10);
        assert!((smoothed.last().unwrap().y - 10.0).abs() < 1e-10);
    }
}
