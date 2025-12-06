use crate::graph_loader::GraphManager;
use catenary::routing_common::osm_graph::StreetData;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

#[derive(Copy, Clone, PartialEq)]
struct State {
    cost: u32,
    node_idx: u32,
}

impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost
            .cmp(&self.cost)
            .then_with(|| self.node_idx.cmp(&other.node_idx))
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct OsmRouter<'a> {
    graph: &'a GraphManager,
}

impl<'a> OsmRouter<'a> {
    pub fn new(graph: &'a GraphManager) -> Self {
        Self { graph }
    }

    pub fn find_reachable_stops(
        &self,
        partition_id: u32,
        start_lat: f64,
        start_lon: f64,
        stops: &[(u32, f64, f64)], // (stop_idx, lat, lon)
        max_duration_seconds: u32,
        walk_speed_mps: f64,
    ) -> Option<Vec<(u32, u32, Vec<(f64, f64)>)>> {
        // (stop_idx, duration, geometry)
        let partition = match self.graph.get_street_partition(partition_id) {
            Some(p) => p,
            None => return None,
        };

        // 1. Find nearest node to start
        let start_node_idx = match self.find_nearest_node(&partition, start_lat, start_lon) {
            Some(idx) => idx,
            None => return Some(Vec::new()), // Partition exists but start node too far
        };

        // 2. Snap stops to nodes
        let mut node_to_stops: HashMap<u32, Vec<u32>> = HashMap::new();
        for &(stop_idx, s_lat, s_lon) in stops {
            if let Some(n_idx) = self.find_nearest_node(&partition, s_lat, s_lon) {
                node_to_stops.entry(n_idx).or_default().push(stop_idx);
            }
        }

        // 3. Run Dijkstra
        let mut dist: HashMap<u32, u32> = HashMap::new();
        let mut heap = BinaryHeap::new();
        let mut predecessors: HashMap<u32, u32> = HashMap::new(); // curr -> prev

        dist.insert(start_node_idx, 0);
        heap.push(State {
            cost: 0,
            node_idx: start_node_idx,
        });

        let mut results = Vec::new();
        let max_cost = (max_duration_seconds as f64 * walk_speed_mps * 1000.0) as u32; // in mm?
        // Wait, Edge distance is in mm.
        // max_duration * speed = distance in meters.
        // * 1000 = mm.

        while let Some(State { cost, node_idx }) = heap.pop() {
            if cost > *dist.get(&node_idx).unwrap_or(&u32::MAX) {
                continue;
            }

            // Check if this node is a stop
            if let Some(stop_indices) = node_to_stops.get(&node_idx) {
                for &stop_idx in stop_indices {
                    // Reconstruct geometry
                    let geometry = self.reconstruct_geometry(
                        &partition,
                        start_node_idx,
                        node_idx,
                        &predecessors,
                    );
                    let duration = (cost as f64 / 1000.0 / walk_speed_mps).ceil() as u32;
                    let duration = if duration == 0 { 1 } else { duration };
                    results.push((stop_idx, duration, geometry));
                }
            }

            if cost >= max_cost {
                continue;
            }

            let node = &partition.nodes[node_idx as usize];
            let start_edge = node.first_edge_idx as usize;
            // How to know end edge?
            // The edges are flattened. We need to know the next node's first_edge_idx.
            // Or store num_edges in Node.
            // Looking at osm_graph.rs, Node has first_edge_idx.
            // We assume nodes are sorted by ID, so we can look at next node.
            // But `nodes` is a Vec.
            let end_edge = if (node_idx as usize) + 1 < partition.nodes.len() {
                partition.nodes[node_idx as usize + 1].first_edge_idx as usize
            } else {
                partition.edges.len()
            };

            for edge_idx in start_edge..end_edge {
                let edge = &partition.edges[edge_idx];
                // Check permissions (walking)
                // Assuming permissions::WALK_FWD is bit 0.
                // We need to check if we can traverse.
                // For now, assume all edges are walkable.
                // TODO: Check permissions.

                let next_cost = cost + edge.distance_mm;
                if next_cost < *dist.get(&edge.target_node).unwrap_or(&u32::MAX) {
                    dist.insert(edge.target_node, next_cost);
                    predecessors.insert(edge.target_node, node_idx);
                    heap.push(State {
                        cost: next_cost,
                        node_idx: edge.target_node,
                    });
                }
            }
        }

        Some(results)
    }

    fn find_nearest_node(&self, partition: &StreetData, lat: f64, lon: f64) -> Option<u32> {
        // Simple linear scan for now.
        // TODO: Use spatial index.
        let mut best_dist = f64::MAX;
        let mut best_node = None;

        for (idx, node) in partition.nodes.iter().enumerate() {
            let dist = self.haversine_distance(lat, lon, node.lat, node.lon);
            if dist < best_dist {
                best_dist = dist;
                best_node = Some(idx as u32);
            }
        }

        // Limit snap distance? e.g. 500m
        if best_dist > 500.0 {
            return None;
        }

        best_node
    }

    fn haversine_distance(&self, lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        let r = 6371000.0;
        let dlat = (lat2 - lat1).to_radians();
        let dlon = (lon2 - lon1).to_radians();
        let a = (dlat / 2.0).sin().powi(2)
            + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        r * c
    }

    fn reconstruct_geometry(
        &self,
        partition: &StreetData,
        start_node: u32,
        end_node: u32,
        predecessors: &HashMap<u32, u32>,
    ) -> Vec<(f64, f64)> {
        // This is getting complicated to reconstruct full geometry with intermediate points.
        // For now, let's just return the list of node coordinates.
        // It's a "rough" geometry (straight lines between nodes).
        // If we want detailed geometry, we need to fetch from `geometries`.

        // Let's redo:
        let mut node_path = Vec::new();
        let mut curr = end_node;
        node_path.push(curr);
        while curr != start_node {
            if let Some(&prev) = predecessors.get(&curr) {
                node_path.push(prev);
                curr = prev;
            } else {
                break;
            }
        }
        node_path.reverse(); // Now [Start, ..., End]

        let mut full_geom = Vec::new();
        for i in 0..node_path.len() - 1 {
            let u = node_path[i];
            let v = node_path[i + 1];

            // Find edge u -> v
            let u_node = &partition.nodes[u as usize];
            let start_edge = u_node.first_edge_idx as usize;
            let end_edge_idx = if (u as usize) + 1 < partition.nodes.len() {
                partition.nodes[u as usize + 1].first_edge_idx as usize
            } else {
                partition.edges.len()
            };

            for edge_idx in start_edge..end_edge_idx {
                let edge = &partition.edges[edge_idx];
                if edge.target_node == v {
                    // Append geometry
                    if edge.geometry_id < partition.geometries.len() as u32 {
                        let geom = &partition.geometries[edge.geometry_id as usize];
                        // geom.coords is [lat, lon, lat, lon...]
                        for chunk in geom.coords.chunks(2) {
                            if chunk.len() == 2 {
                                full_geom.push((chunk[0] as f64, chunk[1] as f64));
                            }
                        }
                    } else {
                        // Fallback to u -> v straight line
                        // u is already added? No.
                        // We should add u, then intermediate, then v?
                        // Usually geometry includes start and end nodes?
                        // Let's assume yes.
                        // But we need to be careful not to duplicate points.
                        // If geom is [u, p1, p2, v], and next is [v, p3, w].
                        // We will have ... u, p1, p2, v, v, p3, w ...
                        // We should skip the first point of the next segment if it matches.
                    }
                    break;
                }
            }
        }

        // If full_geom is empty (e.g. start == end), add start node.
        if full_geom.is_empty() && !node_path.is_empty() {
            let node = &partition.nodes[node_path[0] as usize];
            full_geom.push((node.lat, node.lon));
        }

        full_geom
    }
}
