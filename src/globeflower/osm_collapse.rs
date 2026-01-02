// ===========================================================================
// OSM-Accelerated Collapse Algorithm
// ===========================================================================
//
// This module provides a memory-efficient alternative to the iterative collapse
// algorithm. Instead of densifying edges at 5m spacing and iteratively collapsing,
// it uses OSM junctions as natural merge boundaries.
//
// Key benefits:
// - 10x fewer points (50m spacing vs 5m)
// - Single pass (no iteration)
// - Junction-bounded segments prevent runaway merges
// ===========================================================================

use crate::osm_rail_graph::OsmRailIndex;
use crate::route_registry::RouteId;
use crate::support_graph::CompactedGraphEdge;
use ahash::{AHashMap, AHashSet};
use catenary::graph_formats::NodeId;
use geo::Coord;

/// Junction-aware edge key for grouping merge candidates
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct JunctionPair {
    /// OSM junction ID at start (or hash of position if no junction)
    from_key: i64,
    /// OSM junction ID at end (or hash of position if no junction)  
    to_key: i64,
}

impl JunctionPair {
    fn new(from: i64, to: i64) -> Self {
        // Normalize ordering so (A,B) and (B,A) are the same pair
        if from <= to {
            Self { from_key: from, to_key: to }
        } else {
            Self { from_key: to, to_key: from }
        }
    }
    
    /// Create a key from position hashes when no OSM junction found
    fn from_positions(from_pos: [f64; 2], to_pos: [f64; 2]) -> Self {
        // Quantize positions to ~10m grid for hashing
        let from_hash = ((from_pos[0] / 10.0) as i64) ^ (((from_pos[1] / 10.0) as i64) << 20);
        let to_hash = ((to_pos[0] / 10.0) as i64) ^ (((to_pos[1] / 10.0) as i64) << 20);
        Self::new(from_hash, to_hash)
    }
}

/// Split an edge at OSM junctions along its path
/// 
/// Returns a list of edge segments, where each segment runs between junctions
/// or original endpoints.
pub fn split_edge_at_junctions(
    edge: &CompactedGraphEdge,
    osm_index: &OsmRailIndex,
    junction_tolerance_m: f64,
) -> Vec<CompactedGraphEdge> {
    let path: Vec<[f64; 2]> = edge.geometry.iter().map(|c| [c.x, c.y]).collect();
    
    if path.len() < 2 {
        return vec![edge.clone()];
    }
    
    // Find junctions along this edge's path
    let junctions = osm_index.junctions_along_path(&path, junction_tolerance_m);
    
    if junctions.is_empty() {
        // No junctions along path - return original edge
        return vec![edge.clone()];
    }
    
    // Split edge at each junction
    let mut segments: Vec<CompactedGraphEdge> = Vec::new();
    let mut current_start_idx = 0;
    let total_len = calc_path_length(&path);
    
    for (jct_id, jct_pos, dist_along) in &junctions {
        // Find the path index closest to this junction
        let split_idx = find_path_index_at_distance(&path, *dist_along);
        
        if split_idx <= current_start_idx || split_idx >= path.len() - 1 {
            continue; // Skip if junction is at/before start or at end
        }
        
        // Create segment from current_start to junction
        let mut seg_geom: Vec<Coord> = path[current_start_idx..=split_idx]
            .iter()
            .map(|p| Coord { x: p[0], y: p[1] })
            .collect();
        
        // Snap end to junction position
        if let Some(last) = seg_geom.last_mut() {
            *last = Coord { x: jct_pos[0], y: jct_pos[1] };
        }
        
        let seg_from = if current_start_idx == 0 {
            edge.from.clone()
        } else {
            // Use previous junction as from node
            NodeId::OsmJunction(*jct_id)
        };
        
        segments.push(CompactedGraphEdge {
            from: seg_from,
            to: NodeId::OsmJunction(*jct_id),
            geometry: seg_geom.clone(),
            routes: edge.routes.clone(),
            weight: calc_geom_length(&seg_geom),
        });
        
        current_start_idx = split_idx;
    }
    
    // Create final segment from last junction to end
    if current_start_idx < path.len() - 1 {
        let mut seg_geom: Vec<Coord> = path[current_start_idx..]
            .iter()
            .map(|p| Coord { x: p[0], y: p[1] })
            .collect();
        
        let last_jct_id = junctions.last().map(|(id, _, _)| *id).unwrap_or(0);
        
        segments.push(CompactedGraphEdge {
            from: NodeId::OsmJunction(last_jct_id),
            to: edge.to.clone(),
            geometry: seg_geom,
            routes: edge.routes.clone(),
            weight: 0.0, // Will be recalculated
        });
    }
    
    if segments.is_empty() {
        vec![edge.clone()]
    } else {
        segments
    }
}

/// Main entry point: OSM-accelerated collapse
/// 
/// This replaces the iterative collapse for areas with OSM coverage.
/// Memory usage is ~10x lower because:
/// - No dense 5m sampling (uses 50m between junctions)
/// - Single pass (no iteration)
/// - Junction-bounded merge decisions
pub fn collapse_with_osm_junctions(
    edges: Vec<CompactedGraphEdge>,
    osm_index: &OsmRailIndex,
    d_cut: f64,
) -> Vec<CompactedGraphEdge> {
    println!("OSM-accelerated collapse: {} edges, {} junctions available", 
             edges.len(), osm_index.junction_count());
    
    // Step 1: Split edges at OSM junctions
    let mut split_edges: Vec<CompactedGraphEdge> = Vec::new();
    let junction_tolerance = 25.0; // 25m tolerance for junction snapping
    
    for edge in &edges {
        let segments = split_edge_at_junctions(edge, osm_index, junction_tolerance);
        split_edges.extend(segments);
    }
    
    println!("  Split into {} junction-bounded segments", split_edges.len());
    
    // Step 2: Group edges by junction pair for merge detection
    let mut junction_groups: AHashMap<JunctionPair, Vec<usize>> = AHashMap::new();
    
    for (idx, edge) in split_edges.iter().enumerate() {
        let path: Vec<[f64; 2]> = edge.geometry.iter().map(|c| [c.x, c.y]).collect();
        if path.len() < 2 {
            continue;
        }
        
        // Get junction IDs or position hashes for endpoints
        let from_jct = osm_index.snap_to_junction(path[0], junction_tolerance);
        let to_jct = osm_index.snap_to_junction(*path.last().unwrap(), junction_tolerance);
        
        let key = match (from_jct, to_jct) {
            (Some((from_id, _)), Some((to_id, _))) => JunctionPair::new(from_id, to_id),
            (Some((from_id, _)), None) => {
                let to_hash = position_hash(*path.last().unwrap());
                JunctionPair::new(from_id, to_hash)
            }
            (None, Some((to_id, _))) => {
                let from_hash = position_hash(path[0]);
                JunctionPair::new(from_hash, to_id)
            }
            (None, None) => JunctionPair::from_positions(path[0], *path.last().unwrap()),
        };
        
        junction_groups.entry(key).or_default().push(idx);
    }
    
    // Step 3: Merge edges in same junction group that share OSM relations
    let mut merged_edges: Vec<CompactedGraphEdge> = Vec::new();
    let mut used: AHashSet<usize> = AHashSet::new();
    
    for (_key, indices) in junction_groups {
        if indices.len() == 1 {
            // Single edge in group - no merge needed
            merged_edges.push(split_edges[indices[0]].clone());
            used.insert(indices[0]);
            continue;
        }
        
        // Multiple edges between same junctions - check if they should merge
        // For now, merge all edges between same junction pair (they're parallel tracks)
        let first_edge = &split_edges[indices[0]];
        let mut merged = first_edge.clone();
        
        // Combine route lists from all edges in group
        for &idx in &indices[1..] {
            let other = &split_edges[idx];
            for route in &other.routes {
                if !merged.routes.iter().any(|(rid, _)| *rid == route.0) {
                    merged.routes.push(route.clone());
                }
            }
            used.insert(idx);
        }
        
        // Average geometries for merged parallel tracks
        if indices.len() > 1 {
            merged.geometry = average_geometries(
                &indices.iter().map(|&i| &split_edges[i].geometry).collect::<Vec<_>>()
            );
        }
        
        merged.weight = calc_geom_length(&merged.geometry);
        merged_edges.push(merged);
        used.insert(indices[0]);
    }
    
    // Add any edges that weren't in groups
    for (idx, edge) in split_edges.iter().enumerate() {
        if !used.contains(&idx) {
            merged_edges.push(edge.clone());
        }
    }
    
    println!("  Merged to {} edges", merged_edges.len());
    
    merged_edges
}

/// Densify at coarser 50m spacing (vs 5m in original algorithm)
pub fn coarse_densify(coords: &[[f64; 2]], spacing: f64) -> Vec<[f64; 2]> {
    if coords.is_empty() {
        return vec![];
    }
    
    let mut result = Vec::with_capacity(coords.len() * 2);
    result.push(coords[0]);
    
    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let dx = p2[0] - p1[0];
        let dy = p2[1] - p1[1];
        let dist = (dx * dx + dy * dy).sqrt();
        
        if dist > spacing {
            let num_segments = (dist / spacing).ceil() as usize;
            for j in 1..num_segments {
                let frac = j as f64 / num_segments as f64;
                result.push([p1[0] + dx * frac, p1[1] + dy * frac]);
            }
        }
        result.push(p2);
    }
    
    result
}

// === Helper functions ===

fn calc_path_length(path: &[[f64; 2]]) -> f64 {
    let mut len = 0.0;
    for i in 0..path.len().saturating_sub(1) {
        let dx = path[i + 1][0] - path[i][0];
        let dy = path[i + 1][1] - path[i][1];
        len += (dx * dx + dy * dy).sqrt();
    }
    len
}

fn calc_geom_length(geom: &[Coord]) -> f64 {
    let mut len = 0.0;
    for i in 0..geom.len().saturating_sub(1) {
        let dx = geom[i + 1].x - geom[i].x;
        let dy = geom[i + 1].y - geom[i].y;
        len += (dx * dx + dy * dy).sqrt();
    }
    len
}

fn find_path_index_at_distance(path: &[[f64; 2]], target_dist: f64) -> usize {
    let mut cumulative = 0.0;
    for i in 0..path.len().saturating_sub(1) {
        let dx = path[i + 1][0] - path[i][0];
        let dy = path[i + 1][1] - path[i][1];
        let seg_len = (dx * dx + dy * dy).sqrt();
        
        if cumulative + seg_len >= target_dist {
            return i + 1;
        }
        cumulative += seg_len;
    }
    path.len().saturating_sub(1)
}

fn position_hash(pos: [f64; 2]) -> i64 {
    // Quantize to ~10m grid
    ((pos[0] / 10.0) as i64) ^ (((pos[1] / 10.0) as i64) << 20)
}

fn average_geometries(geoms: &[&Vec<Coord>]) -> Vec<Coord> {
    if geoms.is_empty() {
        return vec![];
    }
    if geoms.len() == 1 {
        return geoms[0].clone();
    }
    
    // Use first geometry as base, interpolate others onto it
    let base = geoms[0];
    let mut result: Vec<Coord> = Vec::with_capacity(base.len());
    
    for (i, coord) in base.iter().enumerate() {
        let mut sum_x = coord.x;
        let mut sum_y = coord.y;
        let mut count = 1.0;
        
        // Find closest point on each other geometry
        for other in &geoms[1..] {
            if let Some(closest) = find_closest_point(other, *coord) {
                sum_x += closest.x;
                sum_y += closest.y;
                count += 1.0;
            }
        }
        
        result.push(Coord { x: sum_x / count, y: sum_y / count });
    }
    
    result
}

fn find_closest_point(geom: &[Coord], target: Coord) -> Option<Coord> {
    if geom.is_empty() {
        return None;
    }
    
    let mut best = geom[0];
    let mut best_dist_sq = f64::MAX;
    
    for coord in geom {
        let dx = coord.x - target.x;
        let dy = coord.y - target.y;
        let dist_sq = dx * dx + dy * dy;
        
        if dist_sq < best_dist_sq {
            best_dist_sq = dist_sq;
            best = *coord;
        }
    }
    
    Some(best)
}
