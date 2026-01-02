// ===========================================================================
// Station Spread Bundling for Intercity Rail
// ===========================================================================
//
// This module detects and fixes "station spreads" - where parallel tracks
// diverge at a station entrance, run parallel through platform areas,
// then reconverge at the station exit.
//
// The Problem:
// Wide terminal stations (Frankfurt Hbf, Paris Gare du Nord, etc.) can span
// 800m+ with 20+ parallel platform tracks. The standard corridor bundling
// treats these as geometric patterns, but station spreads are fundamentally
// TOPOLOGICAL patterns (diverge → parallel → reconverge).
//
// The Solution:
// 1. Detect divergence junctions (high-degree nodes with intercity rail)
// 2. Trace paths from diverging edges to find reconvergence
// 3. Compute station "spine" as weighted average of all paths
// 4. Align platform tracks as parallel offsets from spine
// ===========================================================================

use crate::graph_types::{EdgeRef, LineGraph, NodeRef};
use ahash::{AHashMap, AHashSet};
use geo::Coord;
use std::collections::VecDeque;
use std::rc::Rc;

/// Configuration for station spread detection
pub struct StationConfig {
    /// Maximum distance to look for reconvergence (e.g., 1500m for large terminals)
    pub max_station_span: f64,
    /// Minimum number of diverging tracks to detect as station spread
    pub min_diverging_tracks: usize,
    /// Maximum angle between diverging tracks (radians) to still count as station spread
    /// Tracks going opposite directions are NOT a station spread
    pub max_divergence_angle: f64,
    /// Offset spacing between aligned platform tracks (meters)
    pub platform_spacing: f64,
}

impl Default for StationConfig {
    fn default() -> Self {
        Self {
            max_station_span: 1500.0,      // 1.5km covers even largest terminals
            min_diverging_tracks: 2,        // Even 2 tracks can form a spread
            max_divergence_angle: 1.57,     // 90° max divergence (π/2)
            platform_spacing: 8.0,          // ~8m between track centerlines
        }
    }
}

/// Represents a detected station spread
#[derive(Debug)]
pub struct StationSpread {
    pub id: usize,
    /// Node where tracks diverge entering the station
    pub entry_node_id: usize,
    /// Node where tracks reconverge exiting the station
    pub exit_node_id: usize,
    /// Edge IDs of the platform tracks
    pub platform_edge_ids: Vec<usize>,
    /// Computed spine polyline (centerline through station)
    pub spine_polyline: Vec<Coord>,
    /// Total span of the station in meters
    pub span_meters: f64,
}

const INTERCITY_RAIL: i32 = 2;

/// Detect and fix station platform spreads in the graph
///
/// Returns the number of station spreads detected and fixed.
pub fn detect_and_fix_station_spreads(
    graph: &mut LineGraph,
    config: &StationConfig,
) -> usize {
    let spreads = detect_station_spreads(graph, config);
    let count = spreads.len();
    
    if count > 0 {
        println!("  Detected {} station spreads, applying spine alignment", count);
        apply_station_spine_alignment(graph, &spreads, config);
    }
    
    count
}

/// Detect station spread patterns (diverge → parallel → reconverge)
fn detect_station_spreads(graph: &LineGraph, config: &StationConfig) -> Vec<StationSpread> {
    let mut spreads = Vec::new();
    let mut used_nodes: AHashSet<usize> = AHashSet::new();
    let mut spread_id = 0;

    // Build node map for fast lookup
    let node_map: AHashMap<usize, NodeRef> = graph
        .nodes
        .iter()
        .map(|n| (n.borrow().id, Rc::clone(n)))
        .collect();

    // Find high-degree nodes with intercity rail edges
    let junction_nodes: Vec<NodeRef> = graph
        .nodes
        .iter()
        .filter(|n| {
            let node = n.borrow();
            // At least 3 edges, with multiple being intercity rail
            node.get_deg() >= 3
                && node
                    .adj_list
                    .iter()
                    .filter(|e| e.borrow().routes.iter().any(|r| r.route_type == INTERCITY_RAIL))
                    .count()
                    >= config.min_diverging_tracks
        })
        .map(Rc::clone)
        .collect();

    for entry_node in &junction_nodes {
        let entry_id = entry_node.borrow().id;
        if used_nodes.contains(&entry_id) {
            continue;
        }

        // Get intercity rail edges from this junction
        let intercity_edges: Vec<EdgeRef> = entry_node
            .borrow()
            .adj_list
            .iter()
            .filter(|e| e.borrow().routes.iter().any(|r| r.route_type == INTERCITY_RAIL))
            .map(Rc::clone)
            .collect();

        if intercity_edges.len() < config.min_diverging_tracks {
            continue;
        }

        // Group edges by rough direction (find tracks going "same way")
        let mut direction_groups: Vec<Vec<EdgeRef>> = Vec::new();
        
        for edge in &intercity_edges {
            let edge_dir = get_edge_direction_from_node(edge, entry_node);
            
            // Find existing group with similar direction
            let mut found_group = false;
            for group in &mut direction_groups {
                if let Some(first_edge) = group.first() {
                    let group_dir = get_edge_direction_from_node(first_edge, entry_node);
                    let dot = edge_dir.0 * group_dir.0 + edge_dir.1 * group_dir.1;
                    
                    // If angle < 90°, same general direction
                    if dot > 0.0 {
                        group.push(Rc::clone(edge));
                        found_group = true;
                        break;
                    }
                }
            }
            
            if !found_group {
                direction_groups.push(vec![Rc::clone(edge)]);
            }
        }

        // For each direction group with 2+ tracks, look for reconvergence
        for group in direction_groups {
            if group.len() < config.min_diverging_tracks {
                continue;
            }

            // Trace each edge forward to find reconvergence
            if let Some((exit_node, platform_edges, span)) = 
                find_station_reconvergence(&group, entry_node, config, &node_map)
            {
                let exit_id = exit_node.borrow().id;
                
                // Compute spine polyline as average of all paths
                let spine = compute_station_spine(&platform_edges, entry_node, &exit_node);
                
                spreads.push(StationSpread {
                    id: spread_id,
                    entry_node_id: entry_id,
                    exit_node_id: exit_id,
                    platform_edge_ids: platform_edges.iter().map(|e| e.borrow().id).collect(),
                    spine_polyline: spine,
                    span_meters: span,
                });
                
                spread_id += 1;
                used_nodes.insert(entry_id);
                used_nodes.insert(exit_id);
            }
        }
    }

    spreads
}

/// Find reconvergence of diverging edges within max_station_span
fn find_station_reconvergence(
    diverging_edges: &[EdgeRef],
    entry_node: &NodeRef,
    config: &StationConfig,
    _node_map: &AHashMap<usize, NodeRef>,
) -> Option<(NodeRef, Vec<EdgeRef>, f64)> {
    // BFS from each diverging edge's "other" node
    // Track which nodes each path reaches
    let mut path_reaches: Vec<AHashMap<usize, (f64, Vec<EdgeRef>)>> = Vec::new();
    let entry_id = entry_node.borrow().id;

    for edge in diverging_edges {
        let start_node = edge.borrow().get_other_nd(entry_node);
        let mut visited: AHashMap<usize, (f64, Vec<EdgeRef>)> = AHashMap::new();
        let mut queue: VecDeque<(NodeRef, f64, Vec<EdgeRef>)> = VecDeque::new();
        
        let initial_edges = vec![Rc::clone(edge)];
        queue.push_back((start_node, calc_polyline_length(&edge.borrow().geometry), initial_edges));

        while let Some((current, dist, path)) = queue.pop_front() {
            if dist > config.max_station_span {
                continue;
            }

            let current_id = current.borrow().id;
            if current_id == entry_id {
                continue;
            }

            // Check if we've visited this node with shorter path
            if let Some((prev_dist, _)) = visited.get(&current_id) {
                if *prev_dist <= dist {
                    continue;
                }
            }
            visited.insert(current_id, (dist, path.clone()));

            // Explore neighbors via intercity rail edges
            for edge_ref in &current.borrow().adj_list {
                if !edge_ref.borrow().routes.iter().any(|r| r.route_type == INTERCITY_RAIL) {
                    continue;
                }
                
                let neighbor = edge_ref.borrow().get_other_nd(&current);
                let edge_len = calc_polyline_length(&edge_ref.borrow().geometry);
                let new_dist = dist + edge_len;

                if new_dist <= config.max_station_span {
                    let mut new_path = path.clone();
                    new_path.push(Rc::clone(edge_ref));
                    queue.push_back((neighbor, new_dist, new_path));
                }
            }
        }

        path_reaches.push(visited);
    }

    // Find node reachable by ALL paths
    if path_reaches.is_empty() {
        return None;
    }

    let first_reaches = &path_reaches[0];
    for (node_id, (dist, _)) in first_reaches {
        // Check if all other paths reach this node
        let all_reach = path_reaches[1..].iter().all(|reaches| reaches.contains_key(node_id));
        
        if all_reach {
            // Found reconvergence! Collect all platform edges
            let mut all_edges: Vec<EdgeRef> = Vec::new();
            let mut max_dist = *dist;
            
            for reaches in &path_reaches {
                if let Some((d, edges)) = reaches.get(node_id) {
                    max_dist = max_dist.max(*d);
                    for e in edges {
                        if !all_edges.iter().any(|existing| Rc::ptr_eq(existing, e)) {
                            all_edges.push(Rc::clone(e));
                        }
                    }
                }
            }

            // Get the exit node
            if let Some((_, edges)) = first_reaches.get(node_id) {
                if let Some(last_edge) = edges.last() {
                    let exit = if last_edge.borrow().from.borrow().id == *node_id {
                        Rc::clone(&last_edge.borrow().from)
                    } else {
                        Rc::clone(&last_edge.borrow().to)
                    };
                    
                    return Some((exit, all_edges, max_dist));
                }
            }
        }
    }

    None
}

/// Compute station spine as weighted average of all platform track geometries
fn compute_station_spine(
    platform_edges: &[EdgeRef],
    entry_node: &NodeRef,
    exit_node: &NodeRef,
) -> Vec<Coord> {
    if platform_edges.is_empty() {
        return vec![];
    }

    let entry_pos = entry_node.borrow().pos;
    let exit_pos = exit_node.borrow().pos;
    
    // Simple spine: straight line from entry to exit for now
    // TODO: Could compute proper weighted average of geometries
    vec![
        Coord { x: entry_pos[0], y: entry_pos[1] },
        Coord { x: exit_pos[0], y: exit_pos[1] },
    ]
}

/// Apply station spine alignment to platform edge geometries
fn apply_station_spine_alignment(
    graph: &mut LineGraph,
    spreads: &[StationSpread],
    config: &StationConfig,
) {
    // Build edge ID to EdgeRef map
    let mut edge_map: AHashMap<usize, EdgeRef> = AHashMap::new();
    for node in &graph.nodes {
        for edge_ref in &node.borrow().adj_list {
            if Rc::ptr_eq(&edge_ref.borrow().from, node) {
                edge_map.insert(edge_ref.borrow().id, Rc::clone(edge_ref));
            }
        }
    }

    for spread in spreads {
        if spread.spine_polyline.len() < 2 {
            continue;
        }

        let spine_start = spread.spine_polyline.first().unwrap();
        let spine_end = spread.spine_polyline.last().unwrap();
        
        // Compute spine direction
        let dx = spine_end.x - spine_start.x;
        let dy = spine_end.y - spine_start.y;
        let spine_len = (dx * dx + dy * dy).sqrt();
        if spine_len < 1.0 {
            continue;
        }
        
        let spine_dir = (dx / spine_len, dy / spine_len);
        let perp_dir = (-spine_dir.1, spine_dir.0);

        // Assign offsets to each platform edge
        let num_platforms = spread.platform_edge_ids.len();
        let total_width = (num_platforms as f64 - 1.0) * config.platform_spacing;
        let start_offset = -total_width / 2.0;

        for (i, edge_id) in spread.platform_edge_ids.iter().enumerate() {
            if let Some(edge_ref) = edge_map.get(edge_id) {
                let offset = start_offset + (i as f64) * config.platform_spacing;
                
                // Generate offset geometry from spine
                let new_geom: Vec<Coord> = spread
                    .spine_polyline
                    .iter()
                    .map(|c| Coord {
                        x: c.x + perp_dir.0 * offset,
                        y: c.y + perp_dir.1 * offset,
                    })
                    .collect();

                // Update edge geometry, preserving original endpoints
                let mut edge = edge_ref.borrow_mut();
                if new_geom.len() >= 2 && edge.geometry.len() >= 2 {
                    let orig_start = edge.geometry[0];
                    let orig_end = edge.geometry[edge.geometry.len() - 1];
                    
                    edge.geometry = new_geom;
                    edge.geometry[0] = orig_start;
                    let last_idx = edge.geometry.len() - 1;
                    edge.geometry[last_idx] = orig_end;
                }
            }
        }
    }
}

/// Helper: Get direction vector from a node along an edge
fn get_edge_direction_from_node(edge: &EdgeRef, node: &NodeRef) -> (f64, f64) {
    let edge_borrow = edge.borrow();
    let node_pos = node.borrow().pos;

    let target_pos = if Rc::ptr_eq(&edge_borrow.from, node) {
        if edge_borrow.geometry.len() >= 2 {
            [edge_borrow.geometry[1].x, edge_borrow.geometry[1].y]
        } else {
            edge_borrow.to.borrow().pos
        }
    } else {
        if edge_borrow.geometry.len() >= 2 {
            let idx = edge_borrow.geometry.len() - 2;
            [edge_borrow.geometry[idx].x, edge_borrow.geometry[idx].y]
        } else {
            edge_borrow.from.borrow().pos
        }
    };

    let dx = target_pos[0] - node_pos[0];
    let dy = target_pos[1] - node_pos[1];
    let len = (dx * dx + dy * dy).sqrt();
    if len < 1e-10 {
        return (1.0, 0.0);
    }
    (dx / len, dy / len)
}

/// Helper: Calculate polyline length
fn calc_polyline_length(coords: &[Coord]) -> f64 {
    if coords.len() < 2 {
        return 0.0;
    }
    
    let mut length = 0.0;
    for i in 0..coords.len() - 1 {
        let dx = coords[i + 1].x - coords[i].x;
        let dy = coords[i + 1].y - coords[i].y;
        length += (dx * dx + dy * dy).sqrt();
    }
    length
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StationConfig::default();
        assert_eq!(config.max_station_span, 1500.0);
        assert_eq!(config.min_diverging_tracks, 2);
        assert_eq!(config.platform_spacing, 8.0);
    }
}
