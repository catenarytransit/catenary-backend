
// ===========================================================================
// OSM Topological Conflation (Node-First)
// ===========================================================================
//
// This module replaces the old "Cluster & Match" approach with a robust
// "Node-First Conflation" pipeline.
//
// Key Goal: Preserve the connectivity of the input GTFS graph while forcing
// it to conform to the OSM railway topology where possible.
//
// Algorithm:
// 1. Gather all unique Node IDs from the input graph.
// 2. Snap each Node to the nearest OSM Railway Topology (if within d_cut).
//    - Assign a NEW Globally Unique Node ID (from global counter).
//    - Track mapping: OldNodeID -> (NewInternalNodeID, Option<OsmGraphPos>)
// 3. For each input edge (u, v):
//    - Let u', v' be the conflated nodes.
//    - If both u' and v' are on OSM:
//         - Find Shortest Path on OSM Graph between u' and v'.
//         - If path found: Replace original edge with OSM path segments.
//         - If path NOT found (e.g. graph disconnect): Fallback to rubber-sheeting.
//    - If one or both are unmatched:
//         - "Rubber-sheet" the original geometry to connect u' and v'.
//         - This ensures no gaps appear even at transition from Matched->Unmatched.
// 4. Deduplication:
//    - Merge parallel edges that resulted from mapping multiple GTFS edges to the same OSM path.
// ===========================================================================

use crate::osm_rail_graph::{self, GraphPosition, OsmPathSegment};
use crate::support_graph::CompactedGraphEdge;
use ahash::{AHashMap};
use catenary::graph_formats::NodeId;
use geo::Coord;

/// Main entry point for conflation
pub fn conflate_graph(
    input_edges: Vec<CompactedGraphEdge>,
    d_cut: f64,
    next_node_id_counter: &mut usize,
) -> Vec<CompactedGraphEdge> {
    // 1. Get OSM Index (Ground Truth)
    let osm_index = match osm_rail_graph::get_osm_index() {
        Some(idx) => idx,
        None => {
            eprintln!("Warning: No OSM index available. Falling back to input edges.");
            return input_edges;
        }
    };

    println!("Starting Topological Conflation on {} edges...", input_edges.len());

    // 2. Collect Unique Input Nodes & Positions
    // We need the geometry of edges to know node positions
    let mut input_nodes: AHashMap<NodeId, [f64; 2]> = AHashMap::new();
    for edge in &input_edges {
        if let Some(first) = edge.geometry.first() {
             input_nodes.entry(edge.from.clone()).or_insert([first.x, first.y]);
        }
        if let Some(last) = edge.geometry.last() {
             input_nodes.entry(edge.to.clone()).or_insert([last.x, last.y]);
        }
    }
    
    println!("Identified {} unique input nodes.", input_nodes.len());

    // 3. Snap Nodes & Assign Global IDs
    struct ConflatedNode {
        id: NodeId,               // The FINAL NodeId to be used in output (Globally Unique)
        pos: [f64; 2],            // The FINAL position (snapped or original)
        graph_pos: Option<GraphPosition>, // Unmatched if None
    }
    
    let mut node_mapping: AHashMap<NodeId, ConflatedNode> = AHashMap::new();
    let mut snapped_count = 0;
    
    for (old_id, raw_pos) in input_nodes {
        // Attempt snap
        // Use d_cut for matching, but minimum 50m for robustness
        let graph_pos = osm_index.snap_to_topology(raw_pos, d_cut.max(50.0));
        
        let final_pos = if let Some(gp) = &graph_pos {
            snapped_count += 1;
            gp.pos
        } else {
            raw_pos
        };
        
        // Generate NEW Globally Unique ID
        // This fixes the "Jumping Lines" bug caused by local 0-based counters
        let new_numeric_id = *next_node_id_counter;
        *next_node_id_counter += 1;
        let new_id = NodeId::Intersection(0, new_numeric_id);
        
        node_mapping.insert(old_id, ConflatedNode {
            id: new_id,
            pos: final_pos,
            graph_pos,
        });
    }
    
    println!("Snapped {}/{} nodes to OSM topology.", snapped_count, node_mapping.len());

    // 4. Re-route Edges
    let mut conflated_edges: Vec<CompactedGraphEdge> = Vec::with_capacity(input_edges.len());
    let mut routing_success = 0;
    
    for edge in input_edges {
        let u_info = &node_mapping[&edge.from];
        let v_info = &node_mapping[&edge.to];
        
        let mut path_found = false;
        
        // Strategy: If both endpoints matched OSM, try to route on OSM.
        if let (Some(u_gp), Some(v_gp)) = (&u_info.graph_pos, &v_info.graph_pos) {
            // Find topological path (sequence of segments)
            if let Some(segments) = osm_index.shortest_path(u_gp, v_gp) {
                // Determine directionality: usually bidirectional on rail unless explicitly oneway?
                // For now assuming we can traverse.
                
                // Convert segments to physical edges
                // A single logical edge might become multiple physical segments (e.g. crossing junctions)
                // We typically need to introduce new intermediate nodes for these split points.
                
                let num_segs = segments.len();
                let mut prev_node_id = u_info.id.clone();
                // prev_pos is implicit
                
                for (i, seg) in segments.iter().enumerate() {
                    let is_last = i == num_segs - 1;
                    
                    let target_node_id = if is_last {
                        v_info.id.clone()
                    } else {
                        // Create intermediate node
                        let nid = *next_node_id_counter;
                        *next_node_id_counter += 1;
                        NodeId::Intersection(0, nid)
                    };
                    
                    // Extract Geometry
                    let geom_pts = if let Some(way_geom) = osm_index.get_way_geometry(seg.way_id) {
                         extract_geometry_slice(way_geom, seg.start_dist, seg.end_dist)
                    } else {
                        // Fallback (shouldn't happen)
                        vec![
                            Coord{x: u_info.pos[0], y: u_info.pos[1]}, 
                            Coord{x: v_info.pos[0], y: v_info.pos[1]} // Crude line
                        ]
                    };
                    
                    // Add Edge
                    conflated_edges.push(CompactedGraphEdge {
                        from: prev_node_id.clone(),
                        to: target_node_id.clone(),
                        routes: edge.routes.clone(), // All segments inherit the full route list
                        geometry: geom_pts,
                        weight: edge.weight, // Replicate weight or split it? Replicating is safer for connectivity checks.
                    });
                    
                    prev_node_id = target_node_id;
                }
                
                path_found = true;
                routing_success += 1;
            }
        }
        
        if !path_found {
            // Unmatched or Routing Failed (Gap in OSM)
            // Fallback: "Rubber-sheet" original edge
            // We keep the original geometry, but Snap the endpoints to the Conflated Nodes.
            // This ensures NO GAPS even if we transition Matched->Unmatched.
            
            let mut new_geom = edge.geometry.clone();
            
            // Force snap endpoints
            if let Some(first) = new_geom.first_mut() {
                first.x = u_info.pos[0];
                first.y = u_info.pos[1];
            }
            if let Some(last) = new_geom.last_mut() {
                last.x = v_info.pos[0];
                last.y = v_info.pos[1];
            }
            
            conflated_edges.push(CompactedGraphEdge {
                from: u_info.id.clone(),
                to: v_info.id.clone(),
                routes: edge.routes.clone(),
                geometry: new_geom,
                weight: edge.weight,
            });
        }
    }
    
    println!("Graph Conflation: Routed {}/{} edges over OSM topology.", routing_success, conflated_edges.len());
    
    // 5. Deduplicate Pairs
    // Since multiple input GTFS edges (e.g. Line 1, Line 2) might route over the exact same OSM path,
    // we will now have multiple identical edges (same from, to, geometry, but different route info).
    // We merge them.
    
    let pre_dedup = conflated_edges.len();
    let mut edge_map: AHashMap<(NodeId, NodeId), CompactedGraphEdge> = AHashMap::new();

    for edge in conflated_edges {
        // Normalize key
        let key = if edge.from < edge.to {
            (edge.from.clone(), edge.to.clone())
        } else {
            (edge.to.clone(), edge.from.clone())
        };
        
        edge_map.entry(key)
            .and_modify(|existing| {
                // Merge routes
                for r in &edge.routes {
                    if !existing.routes.contains(r) {
                        existing.routes.push(*r);
                    }
                }
                // Keep geometry with more points (usually better resolution)
                if edge.geometry.len() > existing.geometry.len() {
                    existing.geometry = edge.geometry.clone();
                    existing.weight = edge.weight;
                }
            })
            .or_insert(edge);
    }
    
    let final_edges: Vec<CompactedGraphEdge> = edge_map.into_values().collect();
    println!("Deduplication: {} -> {} edges.", pre_dedup, final_edges.len());
    
    final_edges
}

/// Helper to Extract a sub-section of a polyline given start/end distances
fn extract_geometry_slice(geom: &Vec<[f64; 2]>, start_dist: f64, end_dist: f64) -> Vec<Coord> {
    if geom.is_empty() { return Vec::new(); }
    
    // Ensure start < end for processing, reverse later if needed
    let reverse = start_dist > end_dist;
    let (d_min, d_max) = if reverse { (end_dist, start_dist) } else { (start_dist, end_dist) };
    
    let mut result = Vec::new();
    let mut dist = 0.0;
    
    // 1. Find start point
    let mut started = false;
    
    for i in 0..geom.len().saturating_sub(1) {
        let p1 = geom[i];
        let p2 = geom[i+1];
        let seg_len = ((p2[0]-p1[0]).powi(2) + (p2[1]-p1[1]).powi(2)).sqrt();
        
        let d_next = dist + seg_len;
        
        // Check if interval overlaps with segment
        if d_next >= d_min && dist <= d_max {
            // Segment overlaps with target range
            
            // If this is the first segment we touch, add the precise start point
            if !started {
                let start_t = ((d_min - dist) / seg_len).max(0.0).min(1.0);
                result.push(Coord {
                    x: p1[0] + (p2[0]-p1[0])*start_t,
                    y: p1[1] + (p2[1]-p1[1])*start_t,
                });
                started = true;
            }
            
            // If this segment fully contains the end of the range
            if d_next >= d_max {
                let end_t = ((d_max - dist) / seg_len).max(0.0).min(1.0);
                result.push(Coord {
                    x: p1[0] + (p2[0]-p1[0])*end_t,
                    y: p1[1] + (p2[1]-p1[1])*end_t,
                });
                break; // We are done
            } else {
                // We go past this segment, add full p2
                result.push(Coord { x: p2[0], y: p2[1] });
            }
        }
        
        dist = d_next;
    }
    
    if reverse {
        result.reverse();
    }
    
    // Safety check: always return at least 2 points
    if result.len() < 2 {
        // Fallback line
        // ... hard to recover without context, just return what we have (caller will handle single point?)
        // Or duplicate point?
        if result.len() == 1 {
            result.push(result[0]);
        }
    }
    
    result
}
