// ===========================================================================
// OSM Topological Clustering
// ===========================================================================
//
// This module implements the topological analysis for railway
// networks. Instead of treating the network as a bag of edges, it treats it
// as a set of logical corridors.
//
// Core Algorithm:
// 1. Build a "Parallel Adjacency Graph" where nodes are OSM ways and edges
//    connect ways that are geometrically and topologically parallel.
// 2. Find Connected Components to identify disjoint Corridors.
// 3. Collapse each Corridor into a single "Centerline" geometry.
//
// Key Defenses:
// - Berlin Hbf: Rejects merging ways on different layers/levels or with
//   orthogonal angles (N-S vs E-W crossings).
// - York Station: Uses Polyline Averaging (not PCA) to strictly preserve curvature.
// ===========================================================================

use crate::osm_rail_graph::OsmRailIndex;
use ahash::{AHashMap, AHashSet};
use geo::Coord;
use std::collections::VecDeque;

/// A Cluster represents a logical corridor containing one or more OSM ways.
#[derive(Debug)]
pub struct CorridorCluster {
    pub id: usize,
    /// OSM Ways that form this cluster
    pub way_ids: Vec<i64>,
    /// The computed single centerline for this corridor
    pub centerline: Vec<Coord>,
    /// Average railway type (e.g. "rail", "subway")
    pub primary_type: String,
}

/// Main entry point: Build clusters from the OSM index
pub fn build_corridor_clusters(osm_index: &OsmRailIndex) -> Vec<CorridorCluster> {
    println!("Building topological corridor clusters...");

    // 1. Identification: Get all valid rail ways
    // This uses the newly added helper to get all ways from the index
    let all_way_ids = osm_index.get_all_way_ids();
    
    println!("Clustering {} railway ways...", all_way_ids.len());

    let (clusters, _) = clustering_impl(osm_index, &all_way_ids);
    clusters
}

/// Actual implementation that takes a list of way IDs (extracted by caller)
pub fn clustering_impl(
    osm_index: &OsmRailIndex, 
    all_way_ids: &[i64]
) -> (Vec<CorridorCluster>, AHashMap<i64, usize>) {
    let mut adj: AHashMap<i64, Vec<i64>> = AHashMap::new();
    let mut way_to_cluster: AHashMap<i64, usize> = AHashMap::new();
    let mut clusters: Vec<CorridorCluster> = Vec::new();
    
    // 1. Build Parallel Adjacency Graph
    for &way_a in all_way_ids {
        let geom_a = match osm_index.get_way_geometry(way_a) {
            Some(g) => g,
            None => continue,
        };
        
        let center_a = get_polyline_center(geom_a);
        
        // Find candidates within 100m (corridor width)
        let candidates = osm_index.ways_near_position(center_a, 100.0);
        
        for way_b in candidates {
            if way_a == way_b { continue; }
            if way_a > way_b { continue; } // Check each pair once
            
            let geom_b = match osm_index.get_way_geometry(way_b) {
                Some(g) => g,
                None => continue,
            };
            
            if are_ways_parallel(osm_index, way_a, geom_a, way_b, geom_b) {
                adj.entry(way_a).or_default().push(way_b);
                adj.entry(way_b).or_default().push(way_a);
            }
        }
    }
    
    // 2. Connected Components (Clustering)
    let mut visited: AHashSet<i64> = AHashSet::new();
    let mut cluster_id_counter = 0;
    
    for &seed_way in all_way_ids {
        if visited.contains(&seed_way) { continue; }
        
        // Start new cluster
        let mut cluster_ways: Vec<i64> = Vec::new();
        let mut queue: VecDeque<i64> = VecDeque::new();
        queue.push_back(seed_way);
        visited.insert(seed_way);
        
        while let Some(curr) = queue.pop_front() {
            cluster_ways.push(curr);
            way_to_cluster.insert(curr, cluster_id_counter);
            
            if let Some(neighbors) = adj.get(&curr) {
                for &neighbor in neighbors {
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back(neighbor);
                    }
                }
            }
        }
        
        // 3. Generate Centerline
        let centerline = if cluster_ways.is_empty() {
             Vec::new()
        } else {
             compute_cluster_centerline(osm_index, &cluster_ways)
        };
        
        let primary_type = osm_index.get_way_type(cluster_ways[0]).cloned().unwrap_or_else(|| "rail".to_string());
        
        clusters.push(CorridorCluster {
            id: cluster_id_counter,
            way_ids: cluster_ways,
            centerline,
            primary_type,
        });
        
        cluster_id_counter += 1;
    }
    
    (clusters, way_to_cluster)
}

/// Check if two ways are topologically and geometrically parallel
fn are_ways_parallel(
    index: &OsmRailIndex,
    id_a: i64,
    geom_a: &[[f64; 2]],
    id_b: i64,
    geom_b: &[[f64; 2]],
) -> bool {
    // 1. Shared Route Relation (Ground Truth)
    // If both ways belong to the same route relation (e.g. "Ligne 10"), strict geometric checks are relaxed.
    let shared_relation = index.share_route_relation(id_a, id_b);
    
    // 2. Geometry Sanity
    if geom_a.len() < 2 || geom_b.len() < 2 { return false; }

    // 3. Layer Defense (Berlin Hbf)
    // Only enforce strict layer check if NOT in a shared relation (tags can be missing/messy)
    if !shared_relation {
        let layer_a = index.get_way_layer(id_a).unwrap_or(0);
        let layer_b = index.get_way_layer(id_b).unwrap_or(0);
        if layer_a != layer_b {
            return false;
        }
    }
    
    // 4. Angle Defense (Berlin Crossing)
    // Compare direction vectors
    let dir_a = get_principal_direction(geom_a);
    let dir_b = get_principal_direction(geom_b);
    
    // Dot product: 1.0 = parallel, -1.0 = antiparallel, 0.0 = orthogonal
    let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;
    
    // Thresholds
    let (angle_thresh, dist_thresh) = if shared_relation {
        // Relaxed for shared relations (allow up to ~45 degrees, 0.707)
        // Distance: allow up to 100m (e.g. wide stations)
        (0.7, 100.0)
    } else {
        // Strict for unrelated tracks
        (0.9, 50.0)
    };

    if dot.abs() < angle_thresh {
        return false;
    }
    
    // 5. Distance Check
    let c_a = get_polyline_center(geom_a);
    let c_b = get_polyline_center(geom_b);
    let dist_sq = (c_a[0]-c_b[0]).powi(2) + (c_a[1]-c_b[1]).powi(2);
    
    if dist_sq > dist_thresh * dist_thresh {
        return false;
    }
    
    true
}

/// Compute a single centerline from multiple parallel ways (averaging)
fn compute_cluster_centerline(index: &OsmRailIndex, way_ids: &[i64]) -> Vec<Coord> {
    if way_ids.is_empty() { return Vec::new(); }
    if way_ids.len() == 1 {
        // Simple case: just return the geometry
        if let Some(g) = index.get_way_geometry(way_ids[0]) {
             return g.iter().map(|p| Coord { x: p[0], y: p[1] }).collect();
        }
        return Vec::new();
    }
    
    // Get all geometries
    let mut geoms: Vec<Vec<Coord>> = Vec::new();
    let mut reference_dir: Option<(f64, f64)> = None;
    
    for &wid in way_ids {
        if let Some(raw_geom) = index.get_way_geometry(wid) {
             let mut pts: Vec<Coord> = raw_geom.iter().map(|p| Coord { x: p[0], y: p[1] }).collect();
             
             // Orient all geometries to match the first one
             if let Some(ref_dir) = reference_dir {
                 let this_dir = get_principal_direction(raw_geom);
                 let dot = ref_dir.0 * this_dir.0 + ref_dir.1 * this_dir.1;
                 if dot < 0.0 {
                     pts.reverse();
                 }
             } else {
                 reference_dir = Some(get_principal_direction(raw_geom));
             }
             
             geoms.push(pts);
        }
    }
    
    if geoms.is_empty() { return Vec::new(); }
    
    // York Station Defense: Polyline Averaging
    // Instead of linear regression (which flattens curves), we:
    // 1. Resample all polylines to equidistant points (e.g. every 5 meters).
    // 2. Average the points at index i across all lines.
    // This strictly preserves the shared curvature.
    
    // Find average length
    let total_len: f64 = geoms.iter().map(|g| calc_len(g)).sum();
    let avg_len = total_len / geoms.len() as f64;
    
    let step = 5.0;
    let steps = (avg_len / step).ceil() as usize;
    
    let mut centerline = Vec::with_capacity(steps);
    
    for i in 0..=steps {
        let dist = i as f64 * step;
        if dist > avg_len { break; }
        
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut count = 0;
        
        for geom in &geoms {
            let pt = sample_along_polyline(geom, dist);
            sum_x += pt.x;
            sum_y += pt.y;
            count += 1;
        }
        
        if count > 0 {
            centerline.push(Coord {
                x: sum_x / count as f64,
                y: sum_y / count as f64,
            });
        }
    }
    
    centerline
}

// --- Geometry Helpers ---

fn get_principal_direction(geom: &[[f64; 2]]) -> (f64, f64) {
    if geom.len() < 2 { return (1.0, 0.0); }
    
    // Improved direction: Sample at 10% and 90% to avoid "U-shape" artifacts
    // and noisy endpoints
    let len = calc_len_array(geom);
    if len < 1.0 { return (1.0, 0.0); }
    
    let p1 = sample_point_array(geom, len * 0.1);
    let p2 = sample_point_array(geom, len * 0.9);
    
    let dx = p2[0] - p1[0];
    let dy = p2[1] - p1[1];
    let dist = (dx*dx + dy*dy).sqrt();
    
    if dist > 0.0 { (dx/dist, dy/dist) } else { (1.0, 0.0) }
}

fn get_polyline_center(geom: &[[f64; 2]]) -> [f64; 2] {
    if geom.is_empty() { return [0.0, 0.0]; }
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    for p in geom {
        sum_x += p[0];
        sum_y += p[1];
    }
    let n = geom.len() as f64;
    [sum_x / n, sum_y / n]
}

fn calc_len(geom: &[Coord]) -> f64 {
    let mut len = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let dx = geom[i+1].x - geom[i].x;
        let dy = geom[i+1].y - geom[i].y;
        len += (dx*dx + dy*dy).sqrt();
    }
    len
}

fn sample_along_polyline(geom: &[Coord], target_dist: f64) -> Coord {
    if geom.is_empty() { return Coord { x:0.0, y:0.0 }; }
    
    let mut accum = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let p1 = geom[i];
        let p2 = geom[i+1];
        let dx = p2.x - p1.x;
        let dy = p2.y - p1.y;
        let seg_len = (dx*dx + dy*dy).sqrt();
        
        if accum + seg_len >= target_dist {
            let remain = target_dist - accum;
            let t = if seg_len > 0.001 { remain / seg_len } else { 0.0 };
            return Coord {
                x: p1.x + dx * t,
                y: p1.y + dy * t,
            };
        }
        accum += seg_len;
    }
    
    *geom.last().unwrap()
}

fn calc_len_array(geom: &[[f64; 2]]) -> f64 {
    let mut len = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let dx = geom[i+1][0] - geom[i][0];
        let dy = geom[i+1][1] - geom[i][1];
        len += (dx*dx + dy*dy).sqrt();
    }
    len
}

fn sample_point_array(geom: &[[f64; 2]], target_dist: f64) -> [f64; 2] {
    if geom.is_empty() { return [0.0, 0.0]; }
    
    let mut accum = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let p1 = geom[i];
        let p2 = geom[i+1];
        let dx = p2[0] - p1[0];
        let dy = p2[1] - p1[1];
        let seg_len = (dx*dx + dy*dy).sqrt();
        
        if accum + seg_len >= target_dist {
            let remain = target_dist - accum;
            let t = if seg_len > 0.001 { remain / seg_len } else { 0.0 };
            return [
                p1[0] + dx * t,
                p1[1] + dy * t,
            ];
        }
        accum += seg_len;
    }
    
    *geom.last().unwrap()
}
