
// ===========================================================================
// OSM Topological Corridor Collapse
// ===========================================================================
//
// This module implements the topological analysis for railway
// networks. It replaces the old geometric averaging with a rigorous
// "Topological Corridor Clustering" approach.
//
// Algorithm:
// 1. Identify "Relevant Skeleton": 
//    Find all OSM Ways that are near the input edges.
// 2. Cluster Skeleton:
//    Use `osm_topology` to group parallel OSM Ways into Single-Track Corridors.
//    (Handles Berlin Hbf layers/angles, York Station curvature).
// 3. Map Match:
//    Project every input GTFS edge onto the nearest Corridor Cluster.
// 4. Graph Reconstruction:
//    Output the Cluster Centerlines as the final edges, preserving OSM Junctions.
// ===========================================================================

use crate::osm_rail_graph::OsmRailIndex;
use crate::osm_topology;
use crate::route_registry::RouteId;
use crate::support_graph::CompactedGraphEdge;
use ahash::{AHashMap, AHashSet};
use catenary::graph_formats::NodeId;
use geo::Coord;
use crate::geometry_utils;

/// Main entry point for topological collapse
pub fn collapse_with_osm_junctions(
    input_edges: Vec<CompactedGraphEdge>,
    d_cut: f64,
) -> Vec<CompactedGraphEdge> {
    // 1. Get OSM Index (Ground Truth)
    let osm_index = match crate::osm_rail_graph::get_osm_index() {
        Some(idx) => idx,
        None => {
            eprintln!("Warning: No OSM index available. Falling back to input edges.");
            return input_edges;
        }
    };

    println!("Starting Topological Corridor Collapse on {} edges...", input_edges.len());

    // 2. Identify Relevant OSM Ways (The Skeleton)
    // Scan all input edges to find which OSM ways cover them
    let mut relevant_way_ids: AHashSet<i64> = AHashSet::new();
    let sample_spacing = 50.0; // Check every 50m

    // Optimization: Pre-filter edges to avoid expansive search?
    // For now, simple scan.
    for edge in &input_edges {
        let mut dist = 0.0;
        let total_len = calc_len(&edge.geometry);
        // Ensure at least one check
        if total_len < sample_spacing {
            let mid = sample_along_polyline(&edge.geometry, total_len / 2.0);
            let nearby = osm_index.ways_near_position([mid.x, mid.y], d_cut.max(50.0));
            for w in nearby { relevant_way_ids.insert(w); }
        } else {
            while dist < total_len {
                let pt = sample_along_polyline(&edge.geometry, dist);
                // Search radius: d_cut * 2 to catch things slightly further
                let nearby = osm_index.ways_near_position([pt.x, pt.y], d_cut.max(50.0));
                for w in nearby {
                    relevant_way_ids.insert(w);
                }
                dist += sample_spacing;
            }
        }
        // Always check end
        let end = edge.geometry.last().unwrap();
        let nearby = osm_index.ways_near_position([end.x, end.y], d_cut.max(50.0));
        for w in nearby {
            relevant_way_ids.insert(w);
        }
    }
    
    let way_list: Vec<i64> = relevant_way_ids.into_iter().collect();
    println!("Identified {} relevant OSM ways for skeleton.", way_list.len());

    // 3. Cluster Skeleton into Corridors
    // This handles the "Berlin Hbf" and "York Station" logic
    let (clusters, way_to_cluster) = osm_topology::clustering_impl(osm_index, &way_list);
    println!("Collapsed skeleton into {} corridor clusters.", clusters.len());

    // 4. Map Match Input -> Clusters
    // We need to track the coverage interval (min_dist, max_dist) for each Route on each Cluster.
    // Map: ClusterID -> { (RouteId, RouteType) -> (min_dist, max_dist) }
    let mut cluster_routes: AHashMap<usize, AHashMap<(RouteId, i32), (f64, f64)>> = AHashMap::new();
    
    let mut unmatched_edges: Vec<CompactedGraphEdge> = Vec::new();
    
    // Multi-Cluster Map Matching
    // Instead of voting for a single "winner" cluster for the whole edge,
    // we scan along the edge and assign segments to different clusters.
    // This allows an edge to traverse multiple clusters (e.g. Station -> Line -> Station)
    // without "losing" the parts that don't match the winner.
    
    for edge in input_edges {
        let total_len = calc_len(&edge.geometry);
        if total_len < 1.0 { continue; } // Skip tiny edges
        
        // Sampling parameters
        let step = 10.0;
        let num_samples = (total_len / step).ceil() as usize;

        // 1. Scan edge: (dist, cluster_id)
        // Group consecutive samples into "Raw Intervals"
        let mut raw_intervals: Vec<(usize, f64, f64)> = Vec::new(); // (cid, start_dist, end_dist)
        
        let mut curr_cid: Option<usize> = None;
        let mut curr_start = 0.0;
        let mut last_sample_dist = 0.0;
        
        for i in 0..=num_samples {
             let dist = (i as f64 * step).min(total_len);
             let pt = sample_along_polyline(&edge.geometry, dist);
             
             // Find best cluster for this point
             let nearby_ways = osm_index.ways_near_position([pt.x, pt.y], 30.0);
             let mut best_cid = None;
             let mut min_cluster_dist = f64::MAX;
             
             // Only consider clusters that contain the nearby ways
             for w in nearby_ways {
                 if let Some(&cid) = way_to_cluster.get(&w) {
                     // Find distance to this cluster's centerline
                     if let Some(cluster) = clusters.get(cid) {
                         let d = geometry_utils::distance_point_to_polyline(&pt, &cluster.centerline);
                         if d < min_cluster_dist && d < 50.0 { // 50m max snap
                             min_cluster_dist = d;
                             best_cid = Some(cid);
                         }
                     }
                 }
             }
             
             // Run Length Encoding
             match (curr_cid, best_cid) {
                 (None, Some(new)) => {
                     // Start new interval
                     curr_cid = Some(new);
                     curr_start = dist; 
                 }
                 (Some(old), Some(new)) if old != new => {
                     // Transition: End old, Start new
                     // Split at midpoint for smoothness
                     let mid = (last_sample_dist + dist) / 2.0;
                     raw_intervals.push((old, curr_start, mid));
                     
                     curr_cid = Some(new);
                     curr_start = mid;
                 }
                 (Some(old), None) => {
                     // Transition to void: End old
                     let mid = (last_sample_dist + dist) / 2.0;
                     raw_intervals.push((old, curr_start, mid));
                     curr_cid = None;
                 }
                 _ => {} // Continue same state (Some->Some==, None->None)
             }
             
             last_sample_dist = dist;
        }
        
        // Close last interval if open
        if let Some(cid) = curr_cid {
            raw_intervals.push((cid, curr_start, total_len));
        }
        
        if raw_intervals.is_empty() {
             unmatched_edges.push(edge);
             continue;
        }
        
        // 2. Register intervals to Clusters
        for (cid, start_dist, end_dist) in raw_intervals {
             if end_dist - start_dist < 0.1 { continue; }
             
             let cluster_geom = &clusters[cid].centerline;
             
             // Project start/end of matched segment onto Cluster Centerline
             // to find the range [min_d, max_d] on the CLUSTER
             let pt_start = sample_along_polyline(&edge.geometry, start_dist);
             let pt_end = sample_along_polyline(&edge.geometry, end_dist);
             
             let c_start = geometry_utils::project_point_to_polyline(&pt_start, cluster_geom);
             let c_end = geometry_utils::project_point_to_polyline(&pt_end, cluster_geom);
             
             let (min_d, max_d) = if c_start < c_end {
                 (c_start, c_end)
             } else {
                 (c_end, c_start)
             };
             
             // Register coverage
             let entry = cluster_routes.entry(cid).or_default();
             for r in &edge.routes {
                 entry.entry(*r)
                      .and_modify(|range| {
                          range.0 = range.0.min(min_d);
                          range.1 = range.1.max(max_d);
                      })
                      .or_insert((min_d, max_d));
             }
        }
    }
    
    println!("Map matching complete. {} unmatched edges.", unmatched_edges.len());

    // 5. Graph Reconstruction using Dynamic Segmentation
    let mut final_edges: Vec<CompactedGraphEdge> = Vec::new();
    let mut node_id_counter = 0; 
    let mut cluster_node_positions: AHashMap<NodeId, Coord> = AHashMap::new();

    for cluster in clusters {
        let route_intervals = match cluster_routes.get(&cluster.id) {
            Some(r) if !r.is_empty() => r,
            _ => continue,
        };

        let full_geom = cluster.centerline;
        if full_geom.len() < 2 { continue; }
        
        // --- Dynamic Segmentation ---
        
        // 1. Gather all "Cut Points"
        // - OSM Junctions
        // - Route Start/End points
        
        #[derive(Debug, Clone, Copy)]
        struct CutPoint {
            dist: f64,
            is_junction: bool,
            junction_id: i64,
        }

        let mut cuts: Vec<CutPoint> = Vec::new();
        let total_path_len = calc_len_f64(&full_geom.iter().map(|c| [c.x, c.y]).collect::<Vec<_>>());

        // A. Add OSM Junctions
        let path: Vec<[f64; 2]> = full_geom.iter().map(|c| [c.x, c.y]).collect();
        let junctions = osm_index.junctions_along_path(&path, 20.0); 
        for (jid, _, dist) in junctions {
             // Constrain to geometry bounds
             let d = dist.max(0.0).min(total_path_len);
             cuts.push(CutPoint { dist: d, is_junction: true, junction_id: jid });
        }

        // B. Add Route Intervals (Start/End)
        for (_, (min_d, max_d)) in route_intervals {
            cuts.push(CutPoint { dist: *min_d, is_junction: false, junction_id: 0 });
            cuts.push(CutPoint { dist: *max_d, is_junction: false, junction_id: 0 });
        }

        // C. Always add Start (0.0) and End (total_len)
        cuts.push(CutPoint { dist: 0.0, is_junction: false, junction_id: 0 });
        cuts.push(CutPoint { dist: total_path_len, is_junction: false, junction_id: 0 });

        // Sort and Deduplicate
        cuts.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(std::cmp::Ordering::Equal));
        
        // Merge close cuts (within 1m) to avoid tiny segments
        // Prefer keeping 'is_junction' true if merging
        let mut merged_cuts: Vec<CutPoint> = Vec::new();
        if !cuts.is_empty() {
            let mut curr = cuts[0];
            for next in cuts.into_iter().skip(1) {
                if (next.dist - curr.dist).abs() < 1.0 {
                    // Merge
                    if next.is_junction {
                        curr.is_junction = true;
                        curr.junction_id = next.junction_id;
                    }
                } else {
                    merged_cuts.push(curr);
                    curr = next;
                }
            }
            merged_cuts.push(curr);
        }

        // 2. Pre-assign Node IDs to all cut points
        // This ensures adjacent segments share the same NodeId at their junction.
        // Previously, node IDs were generated per-segment, causing consecutive segments
        // to have different IDs for the same cut point, breaking graph connectivity.
        let cut_node_ids: Vec<NodeId> = merged_cuts
            .iter()
            .map(|cut| {
                if cut.junction_id != 0 {
                    NodeId::OsmJunction(cut.junction_id)
                } else {
                    let id = NodeId::Intersection(0, node_id_counter);
                    node_id_counter += 1;
                    id
                }
            })
            .collect();

        // Store positions for all cut points
        for (i, cut) in merged_cuts.iter().enumerate() {
            let pos = sample_along_polyline(&full_geom, cut.dist);
            cluster_node_positions.insert(cut_node_ids[i].clone(), pos);
        }

        // 3. Create Segments using pre-assigned node IDs
        for i in 0..merged_cuts.len().saturating_sub(1) {
            let start_cut = merged_cuts[i];
            let end_cut = merged_cuts[i + 1];

            let seg_len = end_cut.dist - start_cut.dist;
            if seg_len < 0.1 {
                continue;
            } // Skip degenerate segments

            let mid_dist = start_cut.dist + seg_len / 2.0;

            // 4. Determine Active Routes for this segment
            // A route is active if the segment is within its [min, max] interval
            let mut active_routes: Vec<(RouteId, i32)> = Vec::new();
            for (r_key, (min_d, max_d)) in route_intervals {
                // Use a small buffer to handle floating point issues at boundaries
                if mid_dist >= *min_d && mid_dist <= *max_d {
                    active_routes.push(*r_key);
                }
            }

            if active_routes.is_empty() {
                // DROP empty segments - routes define what's actually used
                continue;
            }

            // Extract Geometry
            let sub_geom = extract_sub_geom(&full_geom, start_cut.dist, end_cut.dist);
            if sub_geom.len() < 2 {
                continue;
            }

            // Use pre-assigned node IDs (now guaranteed to be shared between adjacent segments)
            let u = cut_node_ids[i].clone();
            let v = cut_node_ids[i + 1].clone();

            final_edges.push(CompactedGraphEdge {
                from: u,
                to: v,
                routes: active_routes,
                geometry: sub_geom,
                weight: seg_len,
            });
        }
    }

    // 6. Connectivity Restoration (Fix Missing Segments)
    // Scan unmatched edges. If their endpoints are close to any cluster node, snap them.
    // This bridges the gap between the "OSM Skeleton" and "Fallback Geometry".
    
    let snap_dist_sq = 20.0 * 20.0; // 20m snap tolerance
    
    // Build a simple list for brute-force check
    // Clone keys/values so we don't consume the map needed for lookups later
    let cluster_nodes: Vec<(NodeId, Coord)> = cluster_node_positions
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    
    let mut snapped_count = 0;
    
    for edge in &mut unmatched_edges {
        // Check FROM endpoint
        let start_pt = edge.geometry.first().unwrap();
        let mut best_u = None;
        let mut min_d_u = snap_dist_sq;
        
        for (nid, pos) in &cluster_nodes {
            let d_sq = (start_pt.x - pos.x).powi(2) + (start_pt.y - pos.y).powi(2);
            if d_sq < min_d_u {
                min_d_u = d_sq;
                best_u = Some(nid.clone());
            }
        }
        
        if let Some(nid) = best_u {
            edge.from = nid; // Connect!
            edge.geometry[0] = cluster_node_positions[&edge.from]; // Snap geometry
            snapped_count += 1;
        }
        
        // Check TO endpoint
        let end_pt = edge.geometry.last().unwrap();
        let mut best_v = None;
        let mut min_d_v = snap_dist_sq;
        
        for (nid, pos) in &cluster_nodes {
            let d_sq = (end_pt.x - pos.x).powi(2) + (end_pt.y - pos.y).powi(2);
            if d_sq < min_d_v {
                min_d_v = d_sq;
                best_v = Some(nid.clone());
            }
        }
        
        if let Some(nid) = best_v {
            edge.to = nid; // Connect!
            let last = edge.geometry.len() - 1;
            edge.geometry[last] = cluster_node_positions[&edge.to]; // Snap geometry
            snapped_count += 1;
        }
    }
    
    println!("Connectivity Restoration: Snapped {} endpoints of unmatched edges.", snapped_count);
    
    // 7. Output
    final_edges.extend(unmatched_edges);
    
    // 8. Deduplicate edges with same (from, to) pair
    // Multiple clusters can generate edges to similar endpoints with different geometries.
    // Merge these to prevent oscillating/duplicate segments in output.
    let pre_dedup_count = final_edges.len();
    let mut edge_map: AHashMap<(NodeId, NodeId), CompactedGraphEdge> = AHashMap::new();
    
    for edge in final_edges {
        // Normalize key so (A, B) and (B, A) are treated as the same edge
        let key = if edge.from < edge.to {
            (edge.from.clone(), edge.to.clone())
        } else {
            (edge.to.clone(), edge.from.clone())
        };
        
        edge_map.entry(key)
            .and_modify(|existing| {
                // Merge routes from duplicate
                for r in &edge.routes {
                    if !existing.routes.contains(r) {
                        existing.routes.push(*r);
                    }
                }
                // Keep geometry from edge with more routes (likely more authoritative)
                if edge.routes.len() > existing.routes.len() {
                    existing.geometry = edge.geometry.clone();
                    existing.weight = edge.weight;
                }
            })
            .or_insert(edge);
    }
    
    let final_edges: Vec<CompactedGraphEdge> = edge_map.into_values().collect();
    
    if pre_dedup_count != final_edges.len() {
        println!("Edge deduplication: {} -> {} edges ({} duplicates removed)", 
                 pre_dedup_count, final_edges.len(), pre_dedup_count - final_edges.len());
    }
    
    println!("Topological Collapse complete. Produced {} edges.", final_edges.len());
    final_edges
}

// --- Helpers which were previously in file ---

fn calc_len(geom: &[Coord]) -> f64 {
    let mut len = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let dx = geom[i+1].x - geom[i].x;
        let dy = geom[i+1].y - geom[i].y;
        len += (dx*dx + dy*dy).sqrt();
    }
    len
}

fn calc_len_f64(path: &[[f64; 2]]) -> f64 {
    let mut len = 0.0;
    for i in 0..path.len().saturating_sub(1) {
        let dx = path[i+1][0] - path[i][0];
        let dy = path[i+1][1] - path[i][1];
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

fn extract_sub_geom(geom: &[Coord], start_dist: f64, end_dist: f64) -> Vec<Coord> {
    // Collect points between start_dist and end_dist
    let mut result = Vec::new();
    
    // Add start point
    result.push(sample_along_polyline(geom, start_dist));
    
    let mut accum = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let p1 = geom[i];
        let p2 = geom[i+1];
        let dx = p2.x - p1.x;
        let dy = p2.y - p1.y;
        let seg_len = (dx*dx + dy*dy).sqrt();
        
        // If this segment is fully or partially inside range
        let seg_start = accum;
        let seg_end = accum + seg_len;
        
        if seg_end > start_dist && seg_start < end_dist {
            // Include intermediate vertex p2 if it's strictly inside
            if seg_end < end_dist {
                result.push(p2);
            }
        }
        accum += seg_len;
    }
    
    // Add end point
    result.push(sample_along_polyline(geom, end_dist));
    
    result
}
