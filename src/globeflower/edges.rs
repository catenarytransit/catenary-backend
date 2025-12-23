use crate::geometry_utils::{average_polylines, average_polylines_weighted, is_contained};
use crate::clustering::StopCluster;
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{LineString as GeoLineString, Point as GeoPoint, Coord, Length};
use postgis_diesel::types::{LineString, Point};
use std::collections::{HashMap, HashSet};

use serde::{Serialize, Deserialize};

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
    pub route_ids: Vec<String>,
    pub weight: f64, // length in meters
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
    let mut pattern_to_route: HashMap<String, String> = HashMap::new();
    let mut relevant_shape_ids = HashSet::new();

    for m in metas {
        if let Some(sid) = m.gtfs_shape_id {
            pattern_to_shape.insert(m.direction_pattern_id.clone(), sid.clone());
            relevant_shape_ids.insert(sid);
        }
        if let Some(rid) = m.route_id {
            pattern_to_route.insert(m.direction_pattern_id.clone(), rid.to_string());
        }
    }

    let loaded_shapes = user_shapes_dsl::shapes
        .filter(user_shapes_dsl::shape_id.eq_any(&relevant_shape_ids))
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
                        route_ids: vec![pattern_to_route.get(&pat_id).cloned().unwrap_or_default()],
                        weight,
                        original_edge_index: None,
                    });
                }
            }
        }
    }

    let merged_once = merge_edges(raw_edges);
    println!("Initial edge merge count: {}", merged_once.len());
    
    // Loom-style bundling
    let bundled = bundle_edges(merged_once);
    let intersections = detect_intersections(&bundled);
    println!("Detected {} intersection points.", intersections.len());
    
    let split_edges = split_edges_at_points(bundled, &intersections);
    println!("Split into {} segment edges.", split_edges.len());

    // Zippering: Snap clusters to passing edges
    let zippered_edges = snap_clusters_to_edges(split_edges, clusters, 50.0); // 50m threshold
    println!("Zippering resulted in {} edges.", zippered_edges.len());
    
    let merged = merge_edges(zippered_edges);
    
    // Apply post-processing pipeline as in loom:
    // smoothenOutliers(50) → simplify(1) → densify(5) → chaikin(1) → simplify(1)
    let processed: Vec<GraphEdge> = merged.into_iter().map(|mut edge| {
        let geom = convert_to_geo(&edge.geometry);
        let processed = post_process_line(&geom);
        edge.geometry = convert_from_geo(&processed);
        edge
    }).collect();
    
    println!("Post-processed {} edges.", processed.len());
    Ok(processed)
}

fn merge_edges(raw_edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
    let mut groups: HashMap<(NodeId, NodeId), Vec<GraphEdge>> = HashMap::new();
    for edge in raw_edges {
        groups.entry((edge.from, edge.to)).or_default().push(edge);
    }
    
    let mut merged = Vec::new();
    for ((from, to), mut group) in groups {
        if group.is_empty() { continue; }
        
        // Collect all route IDs
        let mut all_routes = HashSet::new();
        for e in &group {
            for r in &e.route_ids {
                all_routes.insert(r.clone());
            }
        }
        let merged_routes: Vec<String> = all_routes.into_iter().collect();
        
        // Pick representative geometry: Median length
        group.sort_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap_or(std::cmp::Ordering::Equal));
        let mid = group.len() / 2;
        let best_edge = &group[mid];
        
        merged.push(GraphEdge {
            from,
            to,
            geometry: convert_from_geo(&chaikin_smoothing(&convert_to_geo(&best_edge.geometry), 2)),
            route_ids: merged_routes,
            weight: best_edge.weight,
            original_edge_index: None,
        });
    }
    merged
}

fn chaikin_smoothing(line: &GeoLineString<f64>, iterations: usize) -> GeoLineString<f64> {
    let mut current = line.clone();
    for _ in 0..iterations {
        let mut new_coords = Vec::new();
        if current.0.is_empty() { break; }
        new_coords.push(current.0[0]); // Keep start
        
        for i in 0..current.0.len() - 1 {
            let p1 = current.0[i];
            let p2 = current.0[i+1];
            
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
        
        new_coords.push(current.0[current.0.len()-1]); // Keep end
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
fn get_line_substring(line: &GeoLineString<f64>, start_frac: f64, end_frac: f64) -> Option<GeoLineString<f64>> {
    let mut coords = Vec::new();
    let num_points = line.0.len();
    if num_points < 2 { return None; }
    
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
         if len == 0.0 { return l.start; }
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
    
    if coords.len() < 2 { return None; }
    Some(GeoLineString::new(coords))
}

fn convert_to_geo(ls: &LineString<Point>) -> GeoLineString<f64> {
    ls.points.iter().map(|p| Coord { x: p.x, y: p.y }).collect()
}

fn convert_from_geo(ls: &GeoLineString<f64>) -> LineString<Point> {
    let points = ls.0.iter().map(|c| Point { x: c.x, y: c.y, srid: Some(4326) }).collect();
    LineString { points, srid: Some(4326) }
}

fn convert_point(p: &Point) -> GeoPoint<f64> {
    GeoPoint::new(p.x, p.y)
}

use geo::algorithm::line_intersection::LineIntersection;
#[allow(deprecated)]
use geo::{EuclideanLength, HaversineLength, EuclideanDistance, Closest};
use rstar::{RTree, RTreeObject, AABB};



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
    let edge_geoms: Vec<GeoLineString<f64>> = edges.iter().map(|e| convert_to_geo(&e.geometry)).collect();

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

    // Iterate over all segments and query the tree for potential overlaps
    // We need to avoid duplicate checks. (i, j) vs (j, i).
    // RTree query returns references.
    
    // To avoid duplicates and self-intersection issues properly:
    // Only check if candidate.edge_idx > current.edge_idx.
    
    // We can iterate through the *original edges* again, query the tree with the whole edge bbox?
    // No, better to iterate all segments against the tree, then filter.
    
    for segment in tree.iter() {
        let envelope = segment.envelope();
        // Locate candidates
        for candidate in tree.locate_in_envelope_intersecting(&envelope) {
            if candidate.edge_idx <= segment.edge_idx {
                continue; // Enforce ordering to avoid dupes and self-checks
            }
            
            // Perform exact intersection test
            if let Some(opts) = geo::algorithm::line_intersection::line_intersection(segment.line, candidate.line) {
                 match opts {
                     LineIntersection::SinglePoint { intersection, .. } => {
                         points.push(GeoPoint::from(intersection));
                     }
                     LineIntersection::Collinear { intersection } => {
                         points.push(GeoPoint::from(intersection.start));
                         points.push(GeoPoint::from(intersection.end));
                     }
                 }
            }
        }
    }

    // Deduplicate points with epsilon
    let mut unique_points: Vec<GeoPoint<f64>> = Vec::new();
    // Sort to make deduplication O(N log N) instead of O(N^2)
    // Points usually few, but for safety.
    points.sort_by(|a, b| {
        a.x().partial_cmp(&b.x()).unwrap().then(a.y().partial_cmp(&b.y()).unwrap())
    });
    
    if !points.is_empty() {
        unique_points.push(points[0]);
        for i in 1..points.len() {
            let p = points[i];
            let prev = unique_points.last().unwrap();
            // Loom uses MAX_EQ_DISTANCE = 15m for point equality
            // In degrees: 15 / 111111 ≈ 0.000135
            if (p.x() - prev.x()).abs() > 0.000135 || (p.y() - prev.y()).abs() > 0.000135 {
                unique_points.push(p);
            }
        }
    }
    
    unique_points
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
    println!("Snapping clusters to edges (threshold: {}m)...", threshold_meters);
    
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
    let mut split_points_by_edge: HashMap<usize, Vec<(f64, NodeId, GeoPoint<f64>)>> = HashMap::new();
    
    for (cluster_idx, cluster) in clusters.iter().enumerate() {
        let cluster_pt = GeoPoint::new(cluster.centroid.x, cluster.centroid.y);
        // Find edges likely to be close
        // We search with a slightly larger box to be safe
        let search_radius = threshold_meters * 1.5 / 111111.0; // rough deg conversion
        let search_aabb = AABB::from_corners(
            [cluster.centroid.x - search_radius, cluster.centroid.y - search_radius],
            [cluster.centroid.x + search_radius, cluster.centroid.y + search_radius],
        );
        
        for segment in edge_tree.locate_in_envelope_intersecting(&search_aabb) {
             let edge = &edges[segment.edge_idx];
             
             // Skip if the edge is already connected to this cluster
             if edge.from == NodeId::Cluster(cluster_idx) || edge.to == NodeId::Cluster(cluster_idx) {
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
                    split_points_by_edge.entry(segment.edge_idx).or_default().push((
                        0.0, // Placeholder sort key
                        NodeId::Cluster(cluster_idx),
                        p
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
                 let end_ratio = if total_len > 0.0 { *dist_deg / total_len } else { 0.0 };
                 
                 if end_ratio <= start_ratio + 1e-9 { continue; }
                 if end_ratio >= 1.0 - 1e-9 { continue; }
                 
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
    let indexed_points: Vec<IndexedPoint> = points.iter().enumerate().map(|(i, p)| IndexedPoint {
        index: i,
        point: *p,
    }).collect();
    
    let tree = RTree::bulk_load(indexed_points);
    
    for edge in edges {
         let geo_line = convert_to_geo(&edge.geometry);
         let mut split_points: Vec<(f64, usize)> = Vec::new();
         
         // Query R-Tree with edge envelope
         #[allow(deprecated)]
         let (min_x, max_x, min_y, max_y) = {
             let bounding_rect = geo_line.bounding_rect().unwrap_or(geo::Rect::new(
                 Coord { x: 0.0, y: 0.0 }, 
                 Coord { x: 0.0, y: 0.0 }
             ));
             (bounding_rect.min().x, bounding_rect.max().x, bounding_rect.min().y, bounding_rect.max().y)
         };
         
         let envelope = AABB::from_corners([min_x, min_y], [max_x, max_y]);
         
         for candidate in tree.locate_in_envelope_intersecting(&envelope) {
             let p = &candidate.point;
             let p_idx = candidate.index;
             
             // Project point onto line
             if let Some(frac) = geo_line.line_locate_point(p) {
                   // Check if point is actually ON the line (distance check)
                   #[allow(deprecated)]
                   let dist = geo_line.euclidean_distance(p);
                   if dist < 1e-5 { // ~1 meter approx
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
    let mut edge_groups: std::collections::HashMap<(NodeId, NodeId), Vec<GraphEdge>> = std::collections::HashMap::new();

    for edge in edges {
        // Preserve edge direction - don't use undirected keys
        // This matches loom's behaviour where A→B and B→A are distinct edges
        edge_groups.entry((edge.from, edge.to)).or_default().push(edge);
    }

    let mut final_edges = Vec::new();

    for ((u, v), mut group) in edge_groups {
        if group.is_empty() { continue; }
        
        // 1. Combine Included Geometries (Loom-style)
        let mut i = 0;
        while i < group.len() {
            let mut absorbed = false;
            let mut j = 0;
            while j < group.len() {
                if i != j {
                    let geom_i = convert_to_geo(&group[i].geometry);
                    let geom_j = convert_to_geo(&group[j].geometry);
                    
                    // Threshold: ~50m approx in degrees
                    let threshold = 0.0005; 
                    
                    if geom_j.euclidean_length() >= geom_i.euclidean_length() 
                       && is_contained(&geom_i, &geom_j, threshold) 
                    {
                        let mut new_routes = group[i].route_ids.clone();
                        group[j].route_ids.append(&mut new_routes);
                        group[j].route_ids.sort();
                        group[j].route_ids.dedup();
                        group[j].weight += group[i].weight;
                        
                        absorbed = true;
                        break; 
                    }
                }
                j += 1;
            }
            
            if absorbed {
                group.remove(i);
            } else {
                i += 1;
            }
        }
        
        // 2. Average Combine (Loom-style) with weighted averaging
        if group.len() > 1 {
           let geoms: Vec<geo::LineString<f64>> = group.iter().map(|e| convert_to_geo(&e.geometry)).collect();
           
           // Weight by route count squared (as in loom's geomAvg)
           let weights: Vec<f64> = group.iter()
               .map(|e| (e.route_ids.len() as f64).powi(2))
               .collect();
           let avg_geom = average_polylines_weighted(&geoms, Some(&weights));
           
           let mut all_routes = Vec::new();
           let mut total_weight = 0.0;
           for e in &group {
               all_routes.extend(e.route_ids.clone());
               total_weight += e.weight;
           }
           all_routes.sort_unstable();
           all_routes.dedup();
           
            let new_edge = GraphEdge {
                from: u.clone(),
                to: v.clone(),
                geometry: convert_from_geo(&avg_geom),
                route_ids: all_routes,
                weight: total_weight,
                original_edge_index: None,
            };
            final_edges.push(new_edge);
        } else {
            final_edges.push(group.into_iter().next().unwrap());
        }
    }
    
    final_edges
}





#[cfg(test)]
mod tests {
    use super::*;
    use geo::Coord;

    #[test]
    fn test_get_line_substring_simple() {
        // Horizontal line from 0,0 to 10,0
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
        ];
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
            Point { x: 0.0, y: 0.0, srid: Some(4326) },
            Point { x: 10.0, y: 0.0, srid: Some(4326) },
        ];
        let edge = GraphEdge {
            from: NodeId::Cluster(0),
            to: NodeId::Cluster(1),
            geometry: LineString { points: coords, srid: Some(4326) },
            route_ids: vec!["R1".to_string()],
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
