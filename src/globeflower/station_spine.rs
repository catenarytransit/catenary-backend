use crate::osm_rail_graph::{OsmRailIndex, OsmStationInfo};
use crate::geometry_utils::{sub_2d, normalize_2d, dot_product_2d};
use ahash::{AHashSet, AHashMap};

use geo::{LineString, Point, CoordsIter, EuclideanLength};
use crate::geometry_utils::average_polylines;

/// Represents the calculated "spine" or principal axis of a station (can be curved)
#[derive(Debug, Clone)]
pub struct StationSpine {
    /// The curved spine geometry
    pub geometry: LineString<f64>,
    /// Approximate primary direction (for initial filtering)
    pub principal_dir: [f64; 2],
    /// Width/Spread of the station tracks roughly
    pub spread: f64,
}

impl StationSpine {
    /// Calculate principal curved spines for a station
    pub fn from_station(station: &OsmStationInfo, osm_index: &OsmRailIndex) -> Vec<Self> {
        let bbox = if let Some(b) = station.bbox { b } else { return vec![]; };
        
        // Expand bbox slightly
        let margin = 50.0;
        let search_nodes = osm_index.get_nodes_in_bbox(
            bbox.0 - margin, bbox.1 - margin, 
            bbox.2 + margin, bbox.3 + margin
        );
        
        let member_set: AHashSet<i64> = station.members.iter().copied().collect();
        let mut node_pos: AHashMap<i64, [f64; 2]> = AHashMap::new();
        for n in search_nodes {
            node_pos.insert(n.node_id, n.pos);
        }
        
        // 1. Reconstruct logical segments from member ways
        let mut segments: Vec<([f64; 2], [f64; 2])> = Vec::new(); // (start, end)
        
        let nodes_vec: Vec<(&i64, &[f64; 2])> = node_pos.iter().collect();
        // Naive O(N^2) reconstruction of segments based on shared way ID and proximity
        for i in 0..nodes_vec.len() {
            let (id_a, pos_a) = nodes_vec[i];
            let ways_a = if let Some(w) = osm_index.get_node_ways(*id_a) { w } else { continue; };
            
            // Only consider ways that are part of the station members AND are intercity rail
            if !ways_a.iter().any(|w| {
                if !member_set.contains(w) { return false; }
                
                // Checkway type
                if let Some(rtype) = osm_index.get_way_type(*w) {
                    // Only generate spines for rail (intercity) and narrow_gauge (often regional)
                    // Exclude subway, tram, light_rail as requested
                    matches!(rtype.as_str(), "rail" | "narrow_gauge")
                } else {
                    false
                }
            }) { continue; }
            
            for j in (i+1)..nodes_vec.len() {
                let (id_b, pos_b) = nodes_vec[j];
                
                let dx = pos_b[0] - pos_a[0];
                let dy = pos_b[1] - pos_a[1];
                let dist_sq = dx*dx + dy*dy;
                
                if dist_sq > 100.0 * 100.0 || dist_sq < 1.0 { continue; }
                
                let ways_b = if let Some(w) = osm_index.get_node_ways(*id_b) { w } else { continue; };
                
                if ways_a.iter().any(|wa| {
                    ways_b.contains(wa) && 
                    member_set.contains(wa) &&
                    osm_index.get_way_type(*wa).map_or(false, |t| matches!(t.as_str(), "rail" | "narrow_gauge"))
                }) {
                    segments.push((*pos_a, *pos_b));
                }
            }
        }
        
        if segments.is_empty() { return vec![]; }
        
        // 2. Chain segments into Polylines (Greedy stitching)
        let mut chains: Vec<Vec<[f64; 2]>> = Vec::new();
        let mut used_segments = vec![false; segments.len()];
        
        // Sort segments to maybe help ordering? Unnecessary for basic greedy.
        
        for i in 0..segments.len() {
            if used_segments[i] { continue; }
            used_segments[i] = true;
            
            let mut chain = vec![segments[i].0, segments[i].1];
            let mut changed = true;
            
            // Grow chain at both ends
            while changed {
                changed = false;
                let head = chain[0];
                let tail = *chain.last().unwrap();
                let head_dir = normalize_2d([chain[1][0] - chain[0][0], chain[1][1] - chain[0][1]]);
                let tail_dir = normalize_2d([tail[0] - chain[chain.len()-2][0], tail[1] - chain[chain.len()-2][1]]);
                
                for j in 0..segments.len() {
                    if used_segments[j] { continue; }
                    let (s, e) = segments[j];
                    
                    // Try attach to tail
                    let d_tail_s = (s[0]-tail[0]).hypot(s[1]-tail[1]);
                    if d_tail_s < 1.0 {
                        // Check angle
                        let dir = normalize_2d([e[0]-s[0], e[1]-s[1]]);
                        if dot_product_2d(tail_dir, dir) > 0.8 {
                            chain.push(e);
                            used_segments[j] = true;
                            changed = true;
                            break;
                        }
                    }
                    let d_tail_e = (e[0]-tail[0]).hypot(e[1]-tail[1]);
                    if d_tail_e < 1.0 {
                         let dir = normalize_2d([s[0]-e[0], s[1]-e[1]]); // reversed
                         if dot_product_2d(tail_dir, dir) > 0.8 {
                            chain.push(s);
                            used_segments[j] = true;
                            changed = true;
                            break;
                         }
                    }
                    
                    // Try attach to head (prepend)
                    let d_head_s = (s[0]-head[0]).hypot(s[1]-head[1]); // s matches head
                    if d_head_s < 1.0 {
                         let dir = normalize_2d([s[0]-e[0], s[1]-e[1]]); // pointing away from e to s
                         // head_dir points INTO chain (0->1). We want s->e to point into chain? 
                         // No, head_dir is 0->1. We want new segment X->head to align.
                         // X->head direction is e->s (since e is head).
                         // The segment is s->e. 
                         let seg_dir = normalize_2d([e[0]-s[0], e[1]-s[1]]);
                         if dot_product_2d(head_dir, seg_dir) > 0.8 {
                             chain.insert(0, s);
                             used_segments[j] = true;
                             changed = true;
                             break;
                         }
                    }
                     // e matches head
                    let d_head_e = (e[0]-head[0]).hypot(e[1]-head[1]);
                    if d_head_e < 1.0 {
                         let seg_dir = normalize_2d([s[0]-e[0], s[1]-e[1]]); // s->e is backwards relative to head->
                         // segment is s->e. head is e. Chain goes e->...
                         // We want s->e direction to match head->next direction?
                         // Chain: s -> e -> old_head -> ...
                         // Direction s->e should match e->old_head.
                         if dot_product_2d(head_dir, seg_dir) > 0.8 {
                             chain.insert(0, s);
                             used_segments[j] = true;
                             changed = true;
                             break;
                         }
                    }
                }
            }
            if chain.len() > 2 {
                chains.push(chain);
            }
        }
        
        // 3. Convert to LineStrings and Cluster by Direction
        let mut linestrings: Vec<LineString<f64>> = chains.iter()
            .map(|c| LineString::from(c.iter().map(|p| (p[0], p[1])).collect::<Vec<_>>()))
            .collect();
            
        if linestrings.is_empty() { return vec![]; }
        
        // Simple clustering: Since we want "a curved spine", we can't filter by a single direction vector easily.
        // But typically, a station has 1 or 2 main directions.
        // Let's take the longest chain as the "seed" and find all compatible chains (parallel-ish).
        
        linestrings.sort_by(|a, b| b.euclidean_length().partial_cmp(&a.euclidean_length()).unwrap());
        
        let mut station_spines = Vec::new();
        let mut used_lines = vec![false; linestrings.len()];
        
        for i in 0..linestrings.len() {
            if used_lines[i] { continue; }
            if linestrings[i].euclidean_length() < 50.0 { continue; } // Ignore short bits
            
            let seed = &linestrings[i];
            let mut group = vec![seed.clone()];
            used_lines[i] = true;
            
            // Find parallel lines
            // We use Frechet distance or just "average distance" check + angle check?
            // Simplified: Compare start-to-end vectors for rough alignment.
            let p1 = seed.points().next().unwrap();
            let p2 = seed.points().last().unwrap();
            let seed_dir = normalize_2d([p2.x() - p1.x(), p2.y() - p1.y()]);
            
            for j in (i+1)..linestrings.len() {
                if used_lines[j] { continue; }
                let l = &linestrings[j];
                let lp1 = l.points().next().unwrap();
                let lp2 = l.points().last().unwrap();
                let l_dir = normalize_2d([lp2.x() - lp1.x(), lp2.y() - lp1.y()]);
                
                if dot_product_2d(seed_dir, l_dir).abs() > 0.8 {
                    // Check proximity? Assuming station bbox is small enough (it is).
                    group.push(l.clone());
                    used_lines[j] = true;
                }
            }
            
            // Average this group
            let spine_geom = average_polylines(&group);
            station_spines.push(StationSpine {
                geometry: spine_geom,
                principal_dir: seed_dir,
                spread: 10.0, // Placeholder
            });
        }
        
        station_spines
    }
}

