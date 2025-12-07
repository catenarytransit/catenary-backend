use super::utils::haversine_distance;
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use catenary::routing_common::osm_graph::{StreetData, edge_flags, permissions};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
    cost: u32,
    node: u32,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap instead of a max-heap.
impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        // Notice that the we flip the ordering on costs.
        // In case of a tie we compare positions - this step is necessary
        // to make implementations of `PartialEq` and `Ord` consistent.
        other
            .cost
            .cmp(&self.cost)
            .then_with(|| self.node.cmp(&other.node))
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn compute_osm_walk(
    start_node_idx: u32,
    target_node_idx: u32,
    street_data: &StreetData,
) -> Option<u32> {
    if start_node_idx == target_node_idx {
        return Some(0);
    }

    // Limits
    const MAX_COST_SECONDS: u32 = 600; // 10 minutes max walk for internal transfer? 
    // The user said "500m" max dist in rebuild_patterns.
    // 500m @ 1.0m/s = 500s. So 600s is safe.

    // A* Search
    let mut dist: HashMap<u32, u32> = HashMap::new();
    let mut heap = BinaryHeap::new();

    // Heuristic function (Haversine / Max Speed)
    // Max walk speed approx 1.5 m/s? Or maybe 2 m/s to be admissible/conservative (lower bound on cost).
    // Walking is usually 1.1 - 1.4 m/s.
    // To be admissible (heuristic <= actual cost), we need to divide distance by MAX possible speed.
    // Let's us 2.0 m/s as a safe upper bound on walk speed.
    let target_node = &street_data.nodes[target_node_idx as usize];
    let target_lat = target_node.lat;
    let target_lon = target_node.lon;

    let heuristic = |n_idx: u32| -> u32 {
        let n = &street_data.nodes[n_idx as usize];
        let d = haversine_distance(n.lat, n.lon, target_lat, target_lon);
        (d / 2.0) as u32
    };

    dist.insert(start_node_idx, 0);
    heap.push(State {
        cost: 0 + heuristic(start_node_idx),
        node: start_node_idx,
    });

    while let Some(State { cost: _, node: u }) = heap.pop() {
        if u == target_node_idx {
            return dist.get(&u).copied();
        }

        let current_dist = *dist.get(&u).unwrap_or(&u32::MAX);

        // If we popped a worse path, skip
        // (Actually with consistent heuristic this shouldn't happen much but good practice)

        if current_dist > MAX_COST_SECONDS {
            continue;
        }

        // Expand
        let u_node = &street_data.nodes[u as usize];
        let mut edge_idx = u_node.first_edge_idx as usize;

        // We need to iterate edges. But street_data edges are flat array.
        // We don't have "next_edge_idx" or "count" in Node?
        // Checking `osm_graph.rs` schema:
        // `pub struct Node { ... pub first_edge_idx: u32, ... }`
        // The edges are sorted by source node?
        // "All nodes in the graph, sorted by NodeID (0 to N). Used ... to find the start of the edge list."
        // Usually this implies edges for node i are from `nodes[i].first_edge_idx` to `nodes[i+1].first_edge_idx`.

        let next_node_start = if (u as usize) + 1 < street_data.nodes.len() {
            street_data.nodes[u as usize + 1].first_edge_idx as usize
        } else {
            street_data.edges.len()
        };

        for e_ptr in edge_idx..next_node_start {
            let edge = &street_data.edges[e_ptr];

            // Check permissions (Walk Forward)
            // permissions::WALK_FWD = 1
            if (edge.permissions & permissions::WALK_FWD) == 0 {
                continue;
            }

            let v = edge.target_node;
            let len_m = edge.distance_mm as f64 / 1000.0;

            // Calculate walk seconds
            // Base speed 1.4 m/s (approx 5 km/h)
            // Adjust for grade? For now simple constant speed.
            let duration = (len_m / 1.35).ceil() as u32; // 1.35 m/s average walk
            let duration = std::cmp::max(1, duration);

            let next_dist = current_dist + duration;

            if next_dist < *dist.get(&v).unwrap_or(&u32::MAX) {
                dist.insert(v, next_dist);
                heap.push(State {
                    cost: next_dist + heuristic(v),
                    node: v,
                });
            }
        }
    }

    None
}
