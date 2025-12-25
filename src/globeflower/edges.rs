use crate::clustering::StopCluster;
use crate::geometry_utils::{average_polylines, average_polylines_weighted, is_contained};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{Coord, Length, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rstar::{AABB, PointDistance as RPointDistance, RTree, RTreeObject};
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Maximum length of a merged edge segment (approximately 500m in degrees at mid-latitudes).
/// Prevents excessive edge merging that causes "line creep" artifacts.
const MAX_COLLAPSED_SEG_LENGTH_DEGREES: f64 = 0.0045;

/// Convergence threshold for iterative segment collapsing.
const CONVERGENCE_THRESHOLD: f64 = 0.002;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NodeId {
    Cluster(usize),
    Intersection(usize),
}

#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub from: NodeId,
    pub to: NodeId,
    pub geometry: LineString<Point>,
    pub route_ids: Vec<(String, String)>, // (chateau, route_id) - chateau ensures uniqueness across different gtfs files
    pub weight: f64,                      // length in meters
    pub original_edge_index: Option<usize>, // For debugging/zippering traceability
}

pub async fn generate_edges(
    pool: &CatenaryPostgresPool,
    clusters: &[StopCluster],
) -> Result<Vec<GraphEdge>> {
    let mut conn = pool.get().await?;

    let mut stop_to_cluster: HashMap<String, usize> = HashMap::new();
    for c in clusters {
        for s in &c.stops {
            stop_to_cluster.insert(s.gtfs_id.clone(), c.cluster_id);
        }
    }

    use catenary::schema::gtfs::direction_pattern::dsl::*;

    // Optimized: Fetch only necessary columns to reduce memory usage
    // Reverting to full struct load to avoid type mismatch issues with partial tuples
    let patterns = direction_pattern
        .order((direction_pattern_id, stop_sequence))
        .load::<catenary::models::DirectionPatternRow>(&mut conn)
        .await?;

    let mut pattern_map: HashMap<String, Vec<catenary::models::DirectionPatternRow>> =
        HashMap::new();
    for p in patterns {
        pattern_map
            .entry(p.direction_pattern_id.clone())
            .or_default()
            .push(p);
    }

    use catenary::schema::gtfs::direction_pattern_meta::dsl as meta_dsl;
    use catenary::schema::gtfs::shapes::dsl as user_shapes_dsl;

    let metas = meta_dsl::direction_pattern_meta
        .load::<catenary::models::DirectionPatternMeta>(&mut conn)
        .await?;

    let mut pattern_to_shape: HashMap<String, String> = HashMap::new();
    let mut pattern_to_route: HashMap<String, (String, String)> = HashMap::new(); // (chateau, route_id)
    let mut relevant_shape_ids = HashSet::new();

    for m in metas {
        if let Some(sid) = m.gtfs_shape_id {
            pattern_to_shape.insert(m.direction_pattern_id.clone(), sid.clone());
            relevant_shape_ids.insert(sid);
        }
        if let Some(rid) = m.route_id {
            pattern_to_route.insert(
                m.direction_pattern_id.clone(),
                (m.chateau.clone(), rid.to_string()),
            );
        }
    }

    let loaded_shapes = user_shapes_dsl::shapes
        .filter(user_shapes_dsl::shape_id.eq_any(&relevant_shape_ids))
        .filter(user_shapes_dsl::stop_to_stop_generated.eq(false))
        .filter(user_shapes_dsl::route_type.eq_any(vec![0, 1, 2]))
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    let mut shape_map: HashMap<String, catenary::models::Shape> = HashMap::new();
    for s in loaded_shapes {
        shape_map.insert(s.shape_id.clone(), s);
    }

    let mut raw_edges: Vec<GraphEdge> = Vec::new();

    for (pat_id, sequence) in pattern_map {
        let shape_id = pattern_to_shape.get(&pat_id);
        if shape_id.is_none() {
            continue;
        }
        let shape = shape_map.get(shape_id.unwrap());
        if shape.is_none() {
            continue;
        }

        let geo_shape = convert_to_geo(&shape.unwrap().linestring);

        for i in 0..sequence.len() - 1 {
            let from_stop = &sequence[i].stop_id;
            let to_stop = &sequence[i + 1].stop_id;

            let from_c = stop_to_cluster.get(from_stop.as_str());
            let to_c = stop_to_cluster.get(to_stop.as_str());

            if let (Some(&fc), Some(&tc)) = (from_c, to_c) {
                if fc == tc {
                    continue;
                }

                let start_pt = convert_point(&clusters[fc].centroid);
                let end_pt = convert_point(&clusters[tc].centroid);

                // Find positions on shape
                let start_frac = geo_shape.line_locate_point(&start_pt).unwrap_or(0.0);
                let end_frac = geo_shape.line_locate_point(&end_pt).unwrap_or(1.0);

                if start_frac >= end_frac {
                    continue; // Skip backtracking/invalid segments
                }

                if let Some(segment) = get_line_substring(&geo_shape, start_frac, end_frac) {
                    #[allow(deprecated)]
                    let weight = segment.haversine_length();

                    raw_edges.push(GraphEdge {
                        from: NodeId::Cluster(fc),
                        to: NodeId::Cluster(tc),
                        geometry: convert_from_geo(&segment),
                        route_ids: pattern_to_route.get(&pat_id).cloned().into_iter().collect(),
                        weight,
                        original_edge_index: None,
                    });
                }
            }
        }
    }

    let merged_once = merge_edges(raw_edges);
    println!("Initial edge merge count: {}", merged_once.len());

    // Per-chateau processing: group edges by primary chateau and process each group separately
    let chateau_groups = partition_by_chateau(merged_once);
    println!("Partitioned into {} chateau groups", chateau_groups.len());

    let mut all_bundled: Vec<GraphEdge> = Vec::new();
    for (chateau_name, group) in chateau_groups {
        println!(
            "Processing chateau '{}' with {} edges...",
            chateau_name,
            group.len()
        );

        // Collapse shared segments within this chateau using Loom-style algorithm
        let collapsed = collapse_shared_segments(group, 0.0005, 0.0001, 10);
        let bundled = bundle_edges(collapsed);
        all_bundled.extend(bundled);
    }
    println!("Total bundled edges: {}", all_bundled.len());

    // Global intersection detection across all chateaus
    let intersections = detect_intersections(&all_bundled);
    println!("Detected {} intersection points.", intersections.len());

    let split_edges = split_edges_at_points(all_bundled, &intersections);
    println!("Split into {} segment edges.", split_edges.len());

    // Zippering: Snap clusters to passing edges
    let zippered_edges = snap_clusters_to_edges(split_edges, clusters, 50.0); // 50m threshold
    println!("Zippering resulted in {} edges.", zippered_edges.len());

    let merged = merge_edges(zippered_edges);

    // Apply post-processing pipeline as in loom:
    // smoothenOutliers(50) → simplify(1) → densify(5) → chaikin(1) → simplify(1)
    let processed: Vec<GraphEdge> = merged
        .into_iter()
        .map(|mut edge| {
            let geom = convert_to_geo(&edge.geometry);
            let processed = post_process_line(&geom);
            edge.geometry = convert_from_geo(&processed);
            edge
        })
        .collect();

    println!("Post-processed {} edges.", processed.len());
    Ok(processed)
}

fn merge_edges(raw_edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
    let mut groups: HashMap<(NodeId, NodeId), Vec<GraphEdge>> = HashMap::new();
    for edge in raw_edges {
        groups.entry((edge.from, edge.to)).or_default().push(edge);
    }

    let mut merged = Vec::new();
    for ((from, to), group) in groups {
        if group.is_empty() {
            continue;
        }

        // Collect all route IDs as (chateau, route_id) tuples
        let mut all_routes: HashSet<(String, String)> = HashSet::new();
        for e in &group {
            for r in &e.route_ids {
                all_routes.insert(r.clone());
            }
        }
        let merged_routes: Vec<(String, String)> = all_routes.into_iter().collect();

        // Actually average the geometries instead of just picking one
        // Weight by route count squared (matching loom's geomAvg approach)
        let avg_geom = if group.len() == 1 {
            convert_to_geo(&group[0].geometry)
        } else {
            let geoms: Vec<GeoLineString<f64>> =
                group.iter().map(|e| convert_to_geo(&e.geometry)).collect();
            let weights: Vec<f64> = group
                .iter()
                .map(|e| (e.route_ids.len().max(1) as f64).powi(2))
                .collect();
            average_polylines_weighted(&geoms, Some(&weights))
        };

        // Calculate average weight
        let avg_weight = group.iter().map(|e| e.weight).sum::<f64>() / group.len() as f64;

        merged.push(GraphEdge {
            from,
            to,
            geometry: convert_from_geo(&chaikin_smoothing(&avg_geom, 1)),
            route_ids: merged_routes,
            weight: avg_weight,
            original_edge_index: None,
        });
    }
    merged
}

/// Partition edges by their primary chateau for per-chateau processing.
/// Each edge is assigned to the chateau that appears most frequently in its route_ids.
/// Edges with no routes are grouped under "_unknown_".
fn partition_by_chateau(edges: Vec<GraphEdge>) -> HashMap<String, Vec<GraphEdge>> {
    let mut groups: HashMap<String, Vec<GraphEdge>> = HashMap::new();

    for edge in edges {
        // Determine primary chateau by counting occurrences
        let primary_chateau = if edge.route_ids.is_empty() {
            "_unknown_".to_string()
        } else {
            let mut chateau_counts: HashMap<&str, usize> = HashMap::new();
            for (chateau, _) in &edge.route_ids {
                *chateau_counts.entry(chateau.as_str()).or_default() += 1;
            }
            chateau_counts
                .into_iter()
                .max_by_key(|(_, count)| *count)
                .map(|(ch, _)| ch.to_string())
                .unwrap_or_else(|| "_unknown_".to_string())
        };

        groups.entry(primary_chateau).or_default().push(edge);
    }

    groups
}

fn chaikin_smoothing(line: &GeoLineString<f64>, iterations: usize) -> GeoLineString<f64> {
    let mut current = line.clone();
    for _ in 0..iterations {
        let mut new_coords = Vec::new();
        if current.0.is_empty() {
            break;
        }
        new_coords.push(current.0[0]); // Keep start

        for i in 0..current.0.len() - 1 {
            let p1 = current.0[i];
            let p2 = current.0[i + 1];

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

        new_coords.push(current.0[current.0.len() - 1]); // Keep end
        current = GeoLineString::new(new_coords);
    }
    current
}

/// Post-processing pipeline matching loom's approach:
/// smoothenOutliers(50) → simplify(1) → densify(5) → chaikin(1) → simplify(1)
fn post_process_line(line: &GeoLineString<f64>) -> GeoLineString<f64> {
    // Convert distances from metres to approximate degrees
    // 50m ≈ 0.00045 degrees, 5m ≈ 0.000045, 1m ≈ 0.000009
    let smoothed = smoothen_outliers(line, 0.00045);
    let simplified1 = simplify_line(&smoothed, 0.000009);
    let densified = densify_line(&simplified1, 0.000045);
    let chaikin = chaikin_smoothing(&densified, 1);
    simplify_line(&chaikin, 0.000009)
}

/// Remove outlier points that create sharp angles (matching loom's smoothenOutliers).
fn smoothen_outliers(line: &GeoLineString<f64>, threshold: f64) -> GeoLineString<f64> {
    if line.0.len() < 4 {
        return line.clone();
    }

    let mut result = Vec::new();
    result.push(line.0[0]);

    for i in 1..line.0.len().saturating_sub(2) {
        let prev = line.0[i - 1];
        let curr = line.0[i];
        let next = line.0[i + 1];

        // Calculate inner angle using dot product
        let v1 = (prev.x - curr.x, prev.y - curr.y);
        let v2 = (next.x - curr.x, next.y - curr.y);

        let len1 = (v1.0 * v1.0 + v1.1 * v1.1).sqrt();
        let len2 = (v2.0 * v2.0 + v2.1 * v2.1).sqrt();

        if len1 < 1e-10 || len2 < 1e-10 {
            result.push(curr);
            continue;
        }

        let dot = v1.0 * v2.0 + v1.1 * v2.1;
        let cos_angle = dot / (len1 * len2);
        let angle_deg = cos_angle.clamp(-1.0, 1.0).acos().to_degrees();

        // If angle < 35 degrees and point is close to neighbours, remove it
        let dist_prev = ((curr.x - prev.x).powi(2) + (curr.y - prev.y).powi(2)).sqrt();
        let dist_next = ((curr.x - next.x).powi(2) + (curr.y - next.y).powi(2)).sqrt();

        if angle_deg < 35.0 && (dist_prev < threshold || dist_next < threshold) {
            continue;
        }

        result.push(curr);
    }

    // Add last two points
    if line.0.len() >= 2 {
        result.push(line.0[line.0.len() - 2]);
    }
    result.push(line.0[line.0.len() - 1]);

    GeoLineString::new(result)
}

/// Simplify a line using Douglas-Peucker algorithm.
fn simplify_line(line: &GeoLineString<f64>, epsilon: f64) -> GeoLineString<f64> {
    use geo::Simplify;
    line.simplify(epsilon)
}

/// Densify a line by adding points so no segment is longer than max_dist.
fn densify_line(line: &GeoLineString<f64>, max_dist: f64) -> GeoLineString<f64> {
    if line.0.len() < 2 {
        return line.clone();
    }

    let mut result = Vec::new();
    result.push(line.0[0]);

    for i in 1..line.0.len() {
        let start = line.0[i - 1];
        let end = line.0[i];

        let dx = end.x - start.x;
        let dy = end.y - start.y;
        let dist = (dx * dx + dy * dy).sqrt();

        if dist > max_dist {
            let num_segments = (dist / max_dist).ceil() as usize;
            for j in 1..num_segments {
                let t = j as f64 / num_segments as f64;
                result.push(Coord {
                    x: start.x + dx * t,
                    y: start.y + dy * t,
                });
            }
        }

        result.push(end);
    }

    GeoLineString::new(result)
}

// Basic implementation of line substring
fn get_line_substring(
    line: &GeoLineString<f64>,
    start_frac: f64,
    end_frac: f64,
) -> Option<GeoLineString<f64>> {
    let mut coords = Vec::new();
    let num_points = line.0.len();
    if num_points < 2 {
        return None;
    }

    #[allow(deprecated)]
    let total_euclidean_len = line.lines().map(|l| l.euclidean_length()).sum::<f64>();

    let start_dist = start_frac * total_euclidean_len;
    let end_dist = end_frac * total_euclidean_len;

    let mut current_dist = 0.0;
    let mut started = false;

    // Helper to interpolate
    let interpolate = |l: geo::Line<f64>, dist_on_line: f64| -> Coord {
        #[allow(deprecated)]
        let len = l.euclidean_length();
        if len == 0.0 {
            return l.start;
        }
        let t = dist_on_line / len;
        Coord {
            x: l.start.x + (l.end.x - l.start.x) * t,
            y: l.start.y + (l.end.y - l.start.y) * t,
        }
    };

    for segment in line.lines() {
        #[allow(deprecated)]
        let seg_len = segment.euclidean_length();
        let next_dist = current_dist + seg_len;

        if !started {
            if next_dist >= start_dist {
                // This segment contains start
                let dist_on_seg = start_dist - current_dist;
                coords.push(interpolate(segment, dist_on_seg));
                started = true;

                // If it also contains end
                if next_dist >= end_dist {
                    let dist_on_seg_end = end_dist - current_dist;
                    coords.push(interpolate(segment, dist_on_seg_end));
                    break;
                } else {
                    coords.push(segment.end);
                }
            }
        } else {
            // Already started
            if next_dist >= end_dist {
                // This segment contains end
                let dist_on_seg = end_dist - current_dist;
                coords.push(interpolate(segment, dist_on_seg));
                break;
            } else {
                coords.push(segment.end);
            }
        }
        current_dist = next_dist;
    }

    if coords.len() < 2 {
        return None;
    }
    Some(GeoLineString::new(coords))
}

fn convert_to_geo(ls: &LineString<Point>) -> GeoLineString<f64> {
    ls.points.iter().map(|p| Coord { x: p.x, y: p.y }).collect()
}

fn convert_from_geo(ls: &GeoLineString<f64>) -> LineString<Point> {
    let points =
        ls.0.iter()
            .map(|c| Point {
                x: c.x,
                y: c.y,
                srid: Some(4326),
            })
            .collect();
    LineString {
        points,
        srid: Some(4326),
    }
}

fn convert_point(p: &Point) -> GeoPoint<f64> {
    GeoPoint::new(p.x, p.y)
}

use geo::algorithm::line_intersection::LineIntersection;
#[allow(deprecated)]
use geo::{Closest, EuclideanDistance, EuclideanLength, HaversineLength};

struct EdgeSegment {
    edge_idx: usize,
    segment_idx: usize,
    line: geo::Line<f64>,
}

impl RTreeObject for EdgeSegment {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        let (p1, p2) = self.line.points();
        AABB::from_corners([p1.x(), p1.y()], [p2.x(), p2.y()])
    }
}

fn detect_intersections(edges: &[GraphEdge]) -> Vec<GeoPoint<f64>> {
    let mut segments = Vec::new();
    let edge_geoms: Vec<GeoLineString<f64>> =
        edges.iter().map(|e| convert_to_geo(&e.geometry)).collect();

    for (i, edge_geom) in edge_geoms.iter().enumerate() {
        for (j, line) in edge_geom.lines().enumerate() {
            segments.push(EdgeSegment {
                edge_idx: i,
                segment_idx: j,
                line,
            });
        }
    }

    let tree = RTree::bulk_load(segments);
    let mut points: Vec<GeoPoint<f64>> = Vec::new();

    // Build node position lookup for shared-endpoint filtering (O(1) lookups instead of O(n))
    let node_positions = build_node_positions(edges);
    for segment in tree.iter() {
        let envelope = segment.envelope();
        for candidate in tree.locate_in_envelope_intersecting(&envelope) {
            // Skip same edge and enforce ordering to avoid duplicates
            if candidate.edge_idx <= segment.edge_idx {
                continue;
            }

            // Check if edges share an endpoint (loom's sharedNode check)
            // If so, skip intersections near that shared node
            let edge_a = &edges[segment.edge_idx];
            let edge_b = &edges[candidate.edge_idx];
            let shared_node = if edge_a.from == edge_b.from || edge_a.from == edge_b.to {
                Some(edge_a.from)
            } else if edge_a.to == edge_b.from || edge_a.to == edge_b.to {
                Some(edge_a.to)
            } else {
                None
            };

            if let Some(opts) =
                geo::algorithm::line_intersection::line_intersection(segment.line, candidate.line)
            {
                let intersection_points = match opts {
                    LineIntersection::SinglePoint { intersection, .. } => {
                        vec![GeoPoint::from(intersection)]
                    }
                    LineIntersection::Collinear { intersection } => {
                        vec![
                            GeoPoint::from(intersection.start),
                            GeoPoint::from(intersection.end),
                        ]
                    }
                };

                for int_pt in intersection_points {
                    // If edges share a node, skip intersections near that node
                    // Loom uses 100m threshold; 100m ≈ 0.0009 degrees
                    let skip = if let Some(node_id) = shared_node {
                        if let Some(np) = node_positions.get(&node_id) {
                            let dist = ((int_pt.x() - np.x()).powi(2)
                                + (int_pt.y() - np.y()).powi(2))
                            .sqrt();
                            dist < 0.0009
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !skip {
                        points.push(int_pt);
                    }
                }
            }
        }
    }

    // Deduplicate points with epsilon (15m matching loom's MAX_EQ_DISTANCE)
    let mut unique_points: Vec<GeoPoint<f64>> = Vec::new();
    points.sort_by(|a, b| {
        a.x()
            .partial_cmp(&b.x())
            .unwrap()
            .then(a.y().partial_cmp(&b.y()).unwrap())
    });

    if !points.is_empty() {
        unique_points.push(points[0]);
        for i in 1..points.len() {
            let p = points[i];
            let prev = unique_points.last().unwrap();
            // 15m in degrees ≈ 0.000135
            if (p.x() - prev.x()).abs() > 0.000135 || (p.y() - prev.y()).abs() > 0.000135 {
                unique_points.push(p);
            }
        }
    }

    unique_points
}

/// Helper to get approximate node position from edges
fn build_node_positions(edges: &[GraphEdge]) -> HashMap<NodeId, GeoPoint<f64>> {
    let mut positions = HashMap::new();
    for edge in edges {
        let geo_line = convert_to_geo(&edge.geometry);
        if let Some(first) = geo_line.points().next() {
            positions.entry(edge.from).or_insert(first);
        }
        if let Some(last) = geo_line.points().last() {
            positions.entry(edge.to).or_insert(last);
        }
    }
    positions
}

/// Snaps clusters to passing edges that are geometrically close (zippering).
///
/// This effectively "zippers" parallel lines (like local and express tracks) by
/// ensuring that if an express line passes by a local station (within `threshold`),
/// we split the express edge at that projection point and insert a node there.
///
/// This allows the `merge_edges` step to later combine the segments if they share the same
/// start/end nodes (which they now will).
pub fn snap_clusters_to_edges(
    edges: Vec<GraphEdge>,
    clusters: &[StopCluster],
    threshold_meters: f64,
) -> Vec<GraphEdge> {
    println!(
        "Snapping clusters to edges (threshold: {}m)...",
        threshold_meters
    );

    // 1. Index edges in R-Tree
    let mut edge_segments = Vec::new();
    for (edge_idx, edge) in edges.iter().enumerate() {
        let edge_geo = convert_to_geo(&edge.geometry);
        for (seg_idx, line) in edge_geo.lines().enumerate() {
            edge_segments.push(EdgeSegment {
                edge_idx,
                segment_idx: seg_idx, // Not strictly used but good for debug
                line,
            });
        }
    }
    let edge_tree = RTree::bulk_load(edge_segments);

    // 2. Identify split points
    // Map: edge_index -> Vec<(distance_ratio, NodeId, GeoPoint)>
    let mut split_points_by_edge: HashMap<usize, Vec<(f64, NodeId, GeoPoint<f64>)>> =
        HashMap::new();

    for (cluster_idx, cluster) in clusters.iter().enumerate() {
        let cluster_pt = GeoPoint::new(cluster.centroid.x, cluster.centroid.y);
        // Find edges likely to be close
        // We search with a slightly larger box to be safe
        let search_radius = threshold_meters * 1.5 / 111111.0; // rough deg conversion
        let search_aabb = AABB::from_corners(
            [
                cluster.centroid.x - search_radius,
                cluster.centroid.y - search_radius,
            ],
            [
                cluster.centroid.x + search_radius,
                cluster.centroid.y + search_radius,
            ],
        );

        for segment in edge_tree.locate_in_envelope_intersecting(&search_aabb) {
            let edge = &edges[segment.edge_idx];

            // Skip if the edge is already connected to this cluster
            if edge.from == NodeId::Cluster(cluster_idx) || edge.to == NodeId::Cluster(cluster_idx)
            {
                continue;
            }

            // Check precise distance
            let edge_geom = convert_to_geo(&edge.geometry);

            #[allow(deprecated)]
            let dist = edge_geom.euclidean_distance(&cluster_pt);

            // Convert deg distance to approx meters: * 111,111
            let dist_meters = dist * 111_111.0;

            if dist_meters < threshold_meters {
                // Found a match!
                use geo::algorithm::closest_point::ClosestPoint; // Ensure usage
                if let Closest::Intersection(p) = edge_geom.closest_point(&cluster_pt) {
                    split_points_by_edge
                        .entry(segment.edge_idx)
                        .or_default()
                        .push((
                            0.0, // Placeholder sort key
                            NodeId::Cluster(cluster_idx),
                            p,
                        ));
                }
            }
        }
    }

    // 3. Apply splits
    let mut final_edges = Vec::new();

    for (i, edge) in edges.iter().enumerate() {
        if let Some(mut splits) = split_points_by_edge.remove(&i) {
            // Calculate sort keys (distance along line)
            let edge_geom = convert_to_geo(&edge.geometry);
            #[allow(deprecated)]
            let total_len = edge_geom.euclidean_length();

            for item in &mut splits {
                // naive approach to find dist along line:
                let mut dist_so_far = 0.0;
                let mut best_dist = 0.0;
                let mut min_seg_dist = f64::MAX;

                for seg in edge_geom.lines() {
                    #[allow(deprecated)]
                    let seg_len = seg.euclidean_length();
                    #[allow(deprecated)]
                    let d = seg.euclidean_distance(&item.2); // distance from segment to point
                    if d < min_seg_dist {
                        min_seg_dist = d;
                        let start = seg.start;
                        let start_pt = GeoPoint::from(start);
                        #[allow(deprecated)]
                        let dist_from_start = start_pt.euclidean_distance(&item.2); // approx
                        best_dist = dist_so_far + dist_from_start;
                    }
                    dist_so_far += seg_len;
                }
                item.0 = best_dist;
            }

            // Sort by distance along line
            splits.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

            let mut current_start_node = edge.from;
            let mut start_ratio = 0.0;

            for (dist_deg, node_id, _pt) in &splits {
                let end_ratio = if total_len > 0.0 {
                    *dist_deg / total_len
                } else {
                    0.0
                };

                if end_ratio <= start_ratio + 1e-9 {
                    continue;
                }
                if end_ratio >= 1.0 - 1e-9 {
                    continue;
                }

                if let Some(sub_geom) = get_line_substring(&edge_geom, start_ratio, end_ratio) {
                    final_edges.push(GraphEdge {
                        from: current_start_node,
                        to: *node_id,
                        geometry: convert_from_geo(&sub_geom),
                        route_ids: edge.route_ids.clone(),
                        weight: 0.0, // Should recalc
                        original_edge_index: edge.original_edge_index,
                    });
                }

                current_start_node = *node_id;
                start_ratio = end_ratio;
            }

            // Final segment
            if start_ratio < 1.0 {
                if let Some(sub_geom) = get_line_substring(&edge_geom, start_ratio, 1.0) {
                    final_edges.push(GraphEdge {
                        from: current_start_node,
                        to: edge.to,
                        geometry: convert_from_geo(&sub_geom),
                        route_ids: edge.route_ids.clone(),
                        weight: 0.0,
                        original_edge_index: edge.original_edge_index,
                    });
                }
            }
        } else {
            final_edges.push(edge.clone());
        }
    }

    // Recalculate weights
    for edge in &mut final_edges {
        let geo_line = convert_to_geo(&edge.geometry);
        #[allow(deprecated)]
        let w = geo_line.haversine_length();
        edge.weight = w;
    }

    final_edges
}

struct IndexedPoint {
    index: usize,
    point: GeoPoint<f64>,
}

impl RTreeObject for IndexedPoint {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.point.x(), self.point.y()])
    }
}

fn split_edges_at_points(edges: Vec<GraphEdge>, points: &[GeoPoint<f64>]) -> Vec<GraphEdge> {
    println!("Splitting edges at {} intersection points...", points.len());
    let mut new_edges = Vec::new();

    // Bulk load points into R-Tree
    let indexed_points: Vec<IndexedPoint> = points
        .iter()
        .enumerate()
        .map(|(i, p)| IndexedPoint {
            index: i,
            point: *p,
        })
        .collect();

    let tree = RTree::bulk_load(indexed_points);

    for edge in edges {
        let geo_line = convert_to_geo(&edge.geometry);
        let mut split_points: Vec<(f64, usize)> = Vec::new();

        // Query R-Tree with edge envelope
        #[allow(deprecated)]
        let (min_x, max_x, min_y, max_y) = {
            let bounding_rect = geo_line.bounding_rect().unwrap_or(geo::Rect::new(
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 0.0, y: 0.0 },
            ));
            (
                bounding_rect.min().x,
                bounding_rect.max().x,
                bounding_rect.min().y,
                bounding_rect.max().y,
            )
        };

        let envelope = AABB::from_corners([min_x, min_y], [max_x, max_y]);

        for candidate in tree.locate_in_envelope_intersecting(&envelope) {
            let p = &candidate.point;
            let p_idx = candidate.index;

            // Project point onto line
            if let Some(frac) = geo_line.line_locate_point(p) {
                // Check if point is actually ON the line (distance check)
                // Relaxed threshold: 1e-4 degrees ≈ 10m to handle float precision
                #[allow(deprecated)]
                let dist = geo_line.euclidean_distance(p);
                if dist < 1e-4 {
                    // Avoid splitting at endpoints
                    if frac > 1e-4 && frac < 1.0 - 1e-4 {
                        split_points.push((frac, p_idx));
                    }
                }
            }
        }

        if split_points.is_empty() {
            new_edges.push(edge);
            continue;
        }

        split_points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Deduplicate split points
        split_points.dedup_by(|a, b| (a.0 - b.0).abs() < 1e-6);

        let mut current_frac = 0.0;
        let mut current_node = edge.from;

        for (frac, p_idx) in split_points {
            // Redundant safe-guard check
            if (frac - current_frac).abs() < 1e-5 {
                continue;
            }

            if let Some(geom) = get_line_substring(&geo_line, current_frac, frac) {
                #[allow(deprecated)]
                let weight = geom.haversine_length();
                new_edges.push(GraphEdge {
                    from: current_node,
                    to: NodeId::Intersection(p_idx),
                    geometry: convert_from_geo(&geom),
                    route_ids: edge.route_ids.clone(),
                    weight,
                    original_edge_index: edge.original_edge_index,
                });
            }
            current_node = NodeId::Intersection(p_idx);
            current_frac = frac;
        }

        // Final segment
        if (1.0 - current_frac).abs() > 1e-5 {
            if let Some(geom) = get_line_substring(&geo_line, current_frac, 1.0) {
                #[allow(deprecated)]
                let weight = geom.haversine_length();
                new_edges.push(GraphEdge {
                    from: current_node,
                    to: edge.to,
                    geometry: convert_from_geo(&geom),
                    route_ids: edge.route_ids.clone(),
                    weight,
                    original_edge_index: edge.original_edge_index,
                });
            }
        }
    }

    new_edges
}

fn bundle_edges(edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
    println!("Bundling {} edges (Loom-style)...", edges.len());
    let mut edge_groups: std::collections::HashMap<(NodeId, NodeId), Vec<GraphEdge>> =
        std::collections::HashMap::new();

    for edge in edges {
        edge_groups
            .entry((edge.from, edge.to))
            .or_default()
            .push(edge);
    }

    let mut final_edges = Vec::new();

    for ((u, v), mut group) in edge_groups {
        if group.is_empty() {
            continue;
        }

        // Fast path: single edge or pair - skip expensive containment checks
        if group.len() <= 2 {
            if group.len() == 1 {
                final_edges.push(group.into_iter().next().unwrap());
            } else {
                // Just average the two geometries
                let geoms: Vec<GeoLineString<f64>> =
                    group.iter().map(|e| convert_to_geo(&e.geometry)).collect();
                let weights: Vec<f64> = group
                    .iter()
                    .map(|e| (e.route_ids.len().max(1) as f64).powi(2))
                    .collect();
                let avg_geom = average_polylines_weighted(&geoms, Some(&weights));

                let mut all_routes: Vec<(String, String)> =
                    group.iter().flat_map(|e| e.route_ids.clone()).collect();
                all_routes.sort_unstable();
                all_routes.dedup();

                let total_weight: f64 = group.iter().map(|e| e.weight).sum();

                final_edges.push(GraphEdge {
                    from: u,
                    to: v,
                    geometry: convert_from_geo(&avg_geom),
                    route_ids: all_routes,
                    weight: total_weight,
                    original_edge_index: None,
                });
            }
            continue;
        }

        // Combine Included Geometries with iteration limit to avoid O(n²) worst case
        let threshold = 0.0005; // ~50m in degrees
        let max_iterations = group.len() * 2; // Reasonable limit
        let mut iterations = 0;

        let mut i = 0;
        while i < group.len() && iterations < max_iterations {
            iterations += 1;
            let mut absorbed = false;

            let geom_i = convert_to_geo(&group[i].geometry);
            let len_i = geom_i.euclidean_length();

            for j in 0..group.len() {
                if i == j {
                    continue;
                }
                let geom_j = convert_to_geo(&group[j].geometry);

                if geom_j.euclidean_length() >= len_i && is_contained(&geom_i, &geom_j, threshold) {
                    let new_routes = group[i].route_ids.clone();
                    group[j].route_ids.extend(new_routes);
                    group[j].route_ids.sort();
                    group[j].route_ids.dedup();
                    group[j].weight += group[i].weight;
                    absorbed = true;
                    break;
                }
            }

            if absorbed {
                group.remove(i);
            } else {
                i += 1;
            }
        }

        // Average remaining geometries
        if group.len() > 1 {
            let geoms: Vec<GeoLineString<f64>> =
                group.iter().map(|e| convert_to_geo(&e.geometry)).collect();
            let weights: Vec<f64> = group
                .iter()
                .map(|e| (e.route_ids.len().max(1) as f64).powi(2))
                .collect();
            let avg_geom = average_polylines_weighted(&geoms, Some(&weights));

            let mut all_routes: Vec<(String, String)> =
                group.iter().flat_map(|e| e.route_ids.clone()).collect();
            all_routes.sort_unstable();
            all_routes.dedup();

            let total_weight: f64 = group.iter().map(|e| e.weight).sum();

            final_edges.push(GraphEdge {
                from: u,
                to: v,
                geometry: convert_from_geo(&avg_geom),
                route_ids: all_routes,
                weight: total_weight,
                original_edge_index: None,
            });
        } else {
            final_edges.push(group.into_iter().next().unwrap());
        }
    }

    final_edges
}

fn collapse_shared_segments(
    mut edges: Vec<GraphEdge>,
    d_cut: f64,
    seg_len: f64,
    max_iters: usize,
) -> Vec<GraphEdge> {
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct SnappedNode {
        id: usize,
        point: [f64; 2],
    }

    struct SimpleGrid {
        cell_size: f64,
        cells: HashMap<(i32, i32), Vec<usize>>,
        nodes: HashMap<usize, SnappedNode>,
    }

    impl SimpleGrid {
        fn new(cell_size: f64) -> Self {
            Self {
                cell_size,
                cells: HashMap::new(),
                nodes: HashMap::new(),
            }
        }

        fn get_cell_coords(&self, x: f64, y: f64) -> (i32, i32) {
            (
                (x / self.cell_size).floor() as i32,
                (y / self.cell_size).floor() as i32,
            )
        }

        fn insert(&mut self, node: SnappedNode) {
            let coords = self.get_cell_coords(node.point[0], node.point[1]);
            self.cells.entry(coords).or_default().push(node.id);
            self.nodes.insert(node.id, node);
        }

        fn remove(&mut self, id: usize) {
            if let Some(node) = self.nodes.remove(&id) {
                let coords = self.get_cell_coords(node.point[0], node.point[1]);
                if let Some(indices) = self.cells.get_mut(&coords) {
                    if let Some(pos) = indices.iter().position(|&x| x == id) {
                        indices.swap_remove(pos);
                    }
                }
            }
        }

        fn query(&self, x: f64, y: f64, radius: f64) -> Vec<SnappedNode> {
            let mut results = Vec::new();
            let r_sq = radius * radius;

            let min_c = self.get_cell_coords(x - radius, y - radius);
            let max_c = self.get_cell_coords(x + radius, y + radius);

            for cx in min_c.0..=max_c.0 {
                for cy in min_c.1..=max_c.1 {
                    if let Some(indices) = self.cells.get(&(cx, cy)) {
                        for &id in indices {
                            if let Some(node) = self.nodes.get(&id) {
                                let dx = node.point[0] - x;
                                let dy = node.point[1] - y;
                                if dx * dx + dy * dy <= r_sq {
                                    results.push(*node);
                                }
                            }
                        }
                    }
                }
            }
            results
        }
    }

    for iter in 0..max_iters {
        edges.sort_by(|a, b| {
            b.weight
                .partial_cmp(&a.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut node_positions: HashMap<usize, (f64, f64, usize)> = HashMap::new();
        let mut new_edges = Vec::new();
        let mut node_grid = SimpleGrid::new(d_cut);
        let mut next_node_id = 0;

        // imgNds: Map from internal node ID → original NodeId (preserves Cluster identity)
        // This is critical for maintaining station identity through the collapse process
        let mut img_nds: HashMap<usize, NodeId> = HashMap::new();

        // Calculate len_old BEFORE edges are consumed by the for loop
        let len_old: f64 = edges.iter().map(|e| e.weight).sum();

        for edge in edges {
            let geom = convert_to_geo(&edge.geometry);
            let mut path_nodes: Vec<(usize, GeoPoint<f64>)> = Vec::new();

            // Simplify geometry (approx 0.5m) to reduce noise
            let simplified_geom = geom.simplify(0.000005);
            let densified = densify_geometry(&simplified_geom, seg_len);

            let last_pt_idx = densified.len().saturating_sub(1);
            let back_geom_pt = if !densified.is_empty() {
                densified[last_pt_idx]
            } else {
                GeoPoint::new(0.0, 0.0)
            };
            let front_geom_pt = if !densified.is_empty() {
                densified[0]
            } else {
                GeoPoint::new(0.0, 0.0)
            };

            for i in 0..densified.len() {
                let p = densified[i];
                let pt_arr = [p.x(), p.y()];

                // ITERATE CANDIDATES logic
                let d_cut_sq = d_cut * d_cut;
                let candidates = node_grid.query(pt_arr[0], pt_arr[1], d_cut);

                let mut best_match: Option<(usize, GeoPoint<f64>, SnappedNode)> = None;
                let mut min_dist_sq = d_cut_sq; // Start with max allowed

                for n in candidates {
                    let dist_sq =
                        (n.point[0] - pt_arr[0]).powi(2) + (n.point[1] - pt_arr[1]).powi(2);
                    if dist_sq >= min_dist_sq {
                        continue;
                    }

                    let dist = dist_sq.sqrt();

                    let dist_to_back = ((p.x() - back_geom_pt.x()).powi(2)
                        + (p.y() - back_geom_pt.y()).powi(2))
                    .sqrt();
                    let dist_to_front = ((p.x() - front_geom_pt.x()).powi(2)
                        + (p.y() - front_geom_pt.y()).powi(2))
                    .sqrt();

                    let d_span_allowed = (dist_to_back / 1.414);
                    let d_span_allowed_front = (dist_to_front / 1.414);

                    if dist > d_span_allowed || dist > d_span_allowed_front {
                        continue;
                    }

                    // Loom does not use an angle check here.
                    // It relies on dSpan and iterative process.

                    // Valid candidate, update best
                    min_dist_sq = dist_sq;
                    best_match = Some((n.id, GeoPoint::new(n.point[0], n.point[1]), n));
                }

                let (node_id, node_pt) = if let Some((bid, bpt, old_node)) = best_match {
                    let entry = node_positions.entry(bid).or_insert((bpt.x(), bpt.y(), 0));

                    entry.0 += pt_arr[0];
                    entry.1 += pt_arr[1];
                    entry.2 += 1;

                    let avg_x = entry.0 / entry.2 as f64;
                    let avg_y = entry.1 / entry.2 as f64;

                    // Loom-style: Update the node in the tree to the new average
                    // Loom-style: Update the node in the grid to the new average
                    // This ensures subsequent points snap to the MOVING average
                    node_grid.remove(old_node.id);
                    node_grid.insert(SnappedNode {
                        id: bid,
                        point: [avg_x, avg_y],
                    });

                    (bid, GeoPoint::new(avg_x, avg_y))
                } else {
                    let id = next_node_id;
                    next_node_id += 1;
                    node_grid.insert(SnappedNode { id, point: pt_arr });
                    node_positions.insert(id, (pt_arr[0], pt_arr[1], 1));
                    (id, p)
                };

                // Record imgNds mapping for first/last points (like loom's imgNds)
                // This preserves Cluster identity through the collapse process
                if i == 0 {
                    // First point maps to edge.from
                    img_nds.entry(node_id).or_insert(edge.from);
                }
                if i == last_pt_idx {
                    // Last point maps to edge.to
                    img_nds.entry(node_id).or_insert(edge.to);
                }

                path_nodes.push((node_id, node_pt));
            }

            for w in path_nodes.windows(2) {
                let (u_id, u_pt) = w[0];
                let (v_id, v_pt) = w[1];
                if u_id == v_id {
                    continue;
                }

                // Use the imgNds mapping to preserve original NodeId (especially Cluster)
                let from_node = img_nds
                    .get(&u_id)
                    .copied()
                    .unwrap_or(NodeId::Intersection(u_id));
                let to_node = img_nds
                    .get(&v_id)
                    .copied()
                    .unwrap_or(NodeId::Intersection(v_id));

                let segment_len = u_pt.haversine_distance(&v_pt);
                new_edges.push(GraphEdge {
                    from: from_node,
                    to: to_node,
                    geometry: convert_from_geo(&GeoLineString::new(vec![
                        Coord {
                            x: u_pt.x(),
                            y: u_pt.y(),
                        },
                        Coord {
                            x: v_pt.x(),
                            y: v_pt.y(),
                        },
                    ])),
                    route_ids: edge.route_ids.clone(),
                    weight: segment_len,
                    original_edge_index: edge.original_edge_index,
                });
            }
        }

        edges = simplify_graph_serial(new_edges);

        // Artifact removal: Remove very short edges (like Loom's removeEdgeArtifacts)
        // This contracts edges shorter than d_cut by merging their endpoints
        edges = remove_short_edge_artifacts(edges, d_cut);

        let len_new: f64 = edges.iter().map(|e| e.weight).sum();

        // Convergence/divergence check
        if len_old > 0.0 {
            let ratio = len_new / len_old;
            let convergence_ratio = (1.0 - ratio).abs();

            // Stop if converged
            if convergence_ratio < CONVERGENCE_THRESHOLD {
                println!(
                    "Converged at iteration {} (ratio: {:.6})",
                    iter, convergence_ratio
                );
                break;
            }

            // Stop if diverging (edges growing instead of shrinking)
            // ratio > 1.0 means total length increased, which shouldn't happen
            if ratio > 1.5 {
                println!(
                    "Divergence detected at iteration {} (length grew by {:.1}x), stopping early",
                    iter, ratio
                );
                break;
            }

            println!(
                "Iteration {}: length ratio {:.6} ({} edges)",
                iter,
                convergence_ratio,
                edges.len()
            );
        }
    }
    edges
}

fn densify_geometry(line: &GeoLineString<f64>, max_seg_len: f64) -> Vec<GeoPoint<f64>> {
    let mut points = Vec::new();
    if line.0.is_empty() {
        return points;
    }

    let coords = &line.0;
    points.push(GeoPoint::from(coords[0]));

    for i in 0..coords.len() - 1 {
        let p1 = GeoPoint::from(coords[i]);
        let p2 = GeoPoint::from(coords[i + 1]);
        let dist = ((p1.x() - p2.x()).powi(2) + (p1.y() - p2.y()).powi(2)).sqrt();

        if dist > max_seg_len {
            let num_segments = (dist / max_seg_len).ceil() as usize;
            for j in 1..num_segments {
                let frac = j as f64 / num_segments as f64;
                let x = p1.x() + (p2.x() - p1.x()) * frac;
                let y = p1.y() + (p2.y() - p1.y()) * frac;
                points.push(GeoPoint::new(x, y));
            }
        }
        points.push(p2);
    }
    points
}

/// Check if two edges have identical route sets (matching loom's lineEq concept).
/// This ensures we only merge degree-2 chains when the lines are actually the same.
fn routes_equal(a: &GraphEdge, b: &GraphEdge) -> bool {
    if a.route_ids.len() != b.route_ids.len() {
        return false;
    }
    let set_a: HashSet<_> = a.route_ids.iter().collect();
    let set_b: HashSet<_> = b.route_ids.iter().collect();
    set_a == set_b
}

/// Calculate the combined geometry length of a chain of edges (in degrees).
fn chain_geometry_length(edges: &[GraphEdge], indices: &[usize]) -> f64 {
    indices
        .iter()
        .map(|&i| {
            let geom = convert_to_geo(&edges[i].geometry);
            geom.euclidean_length()
        })
        .sum()
}

fn simplify_graph_serial(mut edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
    // Step 0: Consolidate parallel edges (identical u->v) to ensure true graph merging
    let mut edge_map: HashMap<(NodeId, NodeId), GraphEdge> = HashMap::new();
    for e in edges {
        let key = (e.from, e.to);
        if let Some(existing) = edge_map.get_mut(&key) {
            // Merge routes (avoid duplicates)
            for r in e.route_ids {
                if !existing.route_ids.contains(&r) {
                    existing.route_ids.push(r);
                }
            }
        } else {
            edge_map.insert(key, e);
        }
    }
    let edges: Vec<GraphEdge> = edge_map.into_values().collect();

    // Merge consecutive edges (degree-2 nodes)
    let mut adj: HashMap<usize, (Vec<usize>, Vec<usize>)> = HashMap::new();
    for (i, edge) in edges.iter().enumerate() {
        if let NodeId::Intersection(u) = edge.from {
            if let NodeId::Intersection(v) = edge.to {
                adj.entry(u).or_default().1.push(i);
                adj.entry(v).or_default().0.push(i);
            }
        }
    }

    let mut visited = vec![false; edges.len()];
    let mut simplified = Vec::new();

    let merge_chain = |indices: &[usize]| -> GraphEdge {
        let first = &edges[indices[0]];
        let last = &edges[indices[indices.len() - 1]];
        let mut coords = Vec::new();
        for (i, &idx) in indices.iter().enumerate() {
            let geom = &edges[idx].geometry;
            let pts = &geom.points;
            if i == 0 {
                for p in pts {
                    coords.push(Coord { x: p.x, y: p.y });
                }
            } else {
                for p in pts.iter().skip(1) {
                    coords.push(Coord { x: p.x, y: p.y });
                }
            }
        }
        let total_weight: f64 = indices.iter().map(|&i| edges[i].weight).sum();
        let mut routes = std::collections::HashSet::new();
        for &idx in indices {
            for r in &edges[idx].route_ids {
                routes.insert(r.clone());
            }
        }
        let route_vec = routes.into_iter().collect();
        GraphEdge {
            from: first.from,
            to: last.to,
            geometry: convert_from_geo(&post_process_line(&GeoLineString::new(coords))),
            route_ids: route_vec,
            weight: total_weight,
            original_edge_index: first.original_edge_index,
        }
    };

    let mut node_ids: Vec<usize> = adj.keys().cloned().collect();
    node_ids.sort();

    for u in node_ids {
        if let Some((in_list, out_list)) = adj.get(&u) {
            // Check if this node is a START of one or more chains
            if in_list.len() != 1 || out_list.len() != 1 {
                for &start_idx in out_list {
                    if visited[start_idx] {
                        continue;
                    }

                    let mut chain = vec![start_idx];
                    visited[start_idx] = true;

                    let mut curr = start_idx;
                    loop {
                        let curr_edge = &edges[curr];
                        if let NodeId::Intersection(v) = curr_edge.to {
                            if let Some((v_in, v_out)) = adj.get(&v) {
                                if v_in.len() == 1 && v_out.len() == 1 {
                                    let next = v_out[0];
                                    if visited[next] {
                                        break;
                                    }
                                    // Check route equality before merging (loom's lineEq)
                                    if !routes_equal(&edges[curr], &edges[next]) {
                                        break;
                                    }
                                    // Check max segment length constraint
                                    let mut test_chain = chain.clone();
                                    test_chain.push(next);
                                    if chain_geometry_length(&edges, &test_chain)
                                        > MAX_COLLAPSED_SEG_LENGTH_DEGREES
                                    {
                                        break;
                                    }
                                    visited[next] = true;
                                    chain.push(next);
                                    curr = next;
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                    simplified.push(merge_chain(&chain));
                }
            }
        }
    }

    for i in 0..edges.len() {
        if !visited[i] {
            let mut chain = vec![i];
            visited[i] = true;
            let mut curr = i;
            loop {
                let curr_edge = &edges[curr];
                if let NodeId::Intersection(v) = curr_edge.to {
                    if let Some((_, v_out)) = adj.get(&v) {
                        if !v_out.is_empty() {
                            let next = v_out[0];
                            if visited[next] {
                                break;
                            }
                            // Check route equality before merging (loom's lineEq)
                            if !routes_equal(&edges[curr], &edges[next]) {
                                break;
                            }
                            // Check max segment length constraint
                            let mut test_chain = chain.clone();
                            test_chain.push(next);
                            if chain_geometry_length(&edges, &test_chain)
                                > MAX_COLLAPSED_SEG_LENGTH_DEGREES
                            {
                                break;
                            }
                            visited[next] = true;
                            chain.push(next);
                            curr = next;
                            continue;
                        }
                    }
                }
                break;
            }
            simplified.push(merge_chain(&chain));
        }
    }

    simplified
}

/// Remove short edge artifacts by merging their endpoints (Loom's removeEdgeArtifacts).
/// This contracts edges shorter than d_cut by replacing both endpoints with their midpoint.
fn remove_short_edge_artifacts(edges: Vec<GraphEdge>, d_cut: f64) -> Vec<GraphEdge> {
    let mut result = edges;
    let mut changed = true;

    while changed {
        changed = false;

        // Find edges shorter than d_cut (in degrees)
        let mut short_edges: Vec<(usize, f64)> = Vec::new();
        for (i, edge) in result.iter().enumerate() {
            let geom = convert_to_geo(&edge.geometry);
            let len = geom.euclidean_length();
            if len < d_cut {
                short_edges.push((i, len));
            }
        }

        if short_edges.is_empty() {
            break;
        }

        // Sort by length (shortest first)
        short_edges.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take the shortest edge and contract it
        let (short_idx, _) = short_edges[0];
        let short_edge = &result[short_idx];

        // Don't contract if both endpoints are clusters (preserve station identity)
        if matches!(short_edge.from, NodeId::Cluster(_))
            && matches!(short_edge.to, NodeId::Cluster(_))
        {
            // Remove this edge but don't merge nodes
            result.remove(short_idx);
            changed = true;
            continue;
        }

        // Calculate midpoint
        let geom = convert_to_geo(&short_edge.geometry);
        let mid = if geom.0.len() >= 2 {
            let first = geom.0[0];
            let last = geom.0[geom.0.len() - 1];
            Coord {
                x: (first.x + last.x) / 2.0,
                y: (first.y + last.y) / 2.0,
            }
        } else if !geom.0.is_empty() {
            geom.0[0]
        } else {
            Coord { x: 0.0, y: 0.0 }
        };

        // Decide which node to keep (prefer Cluster over Intersection)
        let (keep_node, replace_node) = match (short_edge.from, short_edge.to) {
            (NodeId::Cluster(_), _) => (short_edge.from, short_edge.to),
            (_, NodeId::Cluster(_)) => (short_edge.to, short_edge.from),
            _ => (short_edge.from, short_edge.to),
        };

        // Remove the short edge and update all other edges
        let mut new_edges = Vec::new();
        for (i, edge) in result.into_iter().enumerate() {
            if i == short_idx {
                continue; // Skip the contracted edge
            }

            let mut e = edge;

            // Replace references to replace_node with keep_node
            if e.from == replace_node {
                e.from = keep_node;
                // Update geometry start point
                let mut geom = convert_to_geo(&e.geometry);
                if !geom.0.is_empty() {
                    geom.0[0] = mid;
                }
                e.geometry = convert_from_geo(&geom);
            }
            if e.to == replace_node {
                e.to = keep_node;
                // Update geometry end point
                let mut geom = convert_to_geo(&e.geometry);
                if !geom.0.is_empty() {
                    let last_idx = geom.0.len() - 1;
                    geom.0[last_idx] = mid;
                }
                e.geometry = convert_from_geo(&geom);
            }

            // Skip self-loops
            if e.from != e.to {
                new_edges.push(e);
            }
        }

        result = new_edges;
        changed = true;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::Coord;

    #[test]
    fn test_get_line_substring_simple() {
        // Horizontal line from 0,0 to 10,0
        let coords = vec![Coord { x: 0.0, y: 0.0 }, Coord { x: 10.0, y: 0.0 }];
        let line = GeoLineString::new(coords);

        // Middle 50%: 2.5 to 7.5
        let sub = get_line_substring(&line, 0.25, 0.75).expect("Should return substring");

        // Length should be 5.0
        // Euclidean implementation used in function.
        use geo::EuclideanLength; // Ensure trait is usable if needed, or rely on implementation detail using deprecated one or new one.
        // Actually, verifying coordinates is safer.
        let pts = sub.0;
        assert_eq!(pts.len(), 2);
        assert!((pts[0].x - 2.5).abs() < 1e-6);
        assert!((pts[1].x - 7.5).abs() < 1e-6);
    }

    #[test]
    fn test_get_line_substring_multi_segment() {
        // Line 0,0 -> 10,0 -> 10,10. Total len 20.
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
            Coord { x: 10.0, y: 10.0 },
        ];
        let line = GeoLineString::new(coords);

        // From 0.25 (x=5) to 0.75 (y=5).
        // 0.25 * 20 = 5. Point (5,0).
        // 0.75 * 20 = 15. Point (10,5).
        let sub = get_line_substring(&line, 0.25, 0.75).expect("Should return substring");

        // Should contain (5,0), (10,0), (10,5)
        let pts = sub.0;
        assert_eq!(pts.len(), 3);
        assert!((pts[0].x - 5.0).abs() < 1e-6);
        assert!((pts[0].y - 0.0).abs() < 1e-6);

        assert!((pts[1].x - 10.0).abs() < 1e-6);
        assert!((pts[1].y - 0.0).abs() < 1e-6);

        assert!((pts[2].x - 10.0).abs() < 1e-6);
        assert!((pts[2].y - 5.0).abs() < 1e-6);
    }
    #[test]
    fn test_chaikin_smoothing() {
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 10.0 },
            Coord { x: 20.0, y: 0.0 },
        ];
        let line = GeoLineString::new(coords);

        let smoothed = chaikin_smoothing(&line, 1);

        // Expected: Start, Q1, R1, Q2, R2, End -> 6 points
        assert_eq!(smoothed.0.len(), 6);
        let pts = smoothed.0;

        // Check first point (Start)
        assert_eq!(pts[0], Coord { x: 0.0, y: 0.0 });

        // Q1 = 0.75(0) + 0.25(10) = 2.5
        assert!((pts[1].x - 2.5).abs() < 1e-6);
        assert!((pts[1].y - 2.5).abs() < 1e-6);
    }

    #[test]
    fn test_split_edges_at_points_rtree() {
        // Edge from 0,0 to 10,0
        let coords = vec![
            Point {
                x: 0.0,
                y: 0.0,
                srid: Some(4326),
            },
            Point {
                x: 10.0,
                y: 0.0,
                srid: Some(4326),
            },
        ];
        let edge = GraphEdge {
            from: NodeId::Cluster(0),
            to: NodeId::Cluster(1),
            geometry: LineString {
                points: coords,
                srid: Some(4326),
            },
            route_ids: vec![("test_chateau".to_string(), "R1".to_string())],
            weight: 10.0,
            original_edge_index: None,
        };

        // Point at 5,0 (should split)
        let p1 = GeoPoint::new(5.0, 0.0);
        // Point at 5,5 (too far, should not split)
        let p2 = GeoPoint::new(5.0, 5.0);

        let result = split_edges_at_points(vec![edge], &[p1, p2]);

        // Should be split into 2 edges
        assert_eq!(result.len(), 2);

        // Verify geometry of split edges
        let e1 = &result[0];
        let e2 = &result[1];

        // e1: 0,0 -> 5,0
        assert_eq!(e1.geometry.points.len(), 2);
        assert!((e1.geometry.points[0].x - 0.0).abs() < 1e-6);
        assert!((e1.geometry.points[1].x - 5.0).abs() < 1e-6);

        // e2: 5,0 -> 10,0
        assert_eq!(e2.geometry.points.len(), 2);
        assert!((e2.geometry.points[0].x - 5.0).abs() < 1e-6);
        assert!((e2.geometry.points[1].x - 10.0).abs() < 1e-6);
    }
}
