use crate::geometry_utils;
use crate::osm_loader::OsmRailIndex;
use crate::osm_types::{AtomicEdge, AtomicEdgeId, LineId, OsmNodeId, RailMode};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, trace, warn};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Result of map-matching a GTFS shape to OSM edges
#[derive(Debug, Clone)]
pub struct MatchedShape {
    pub line_id: LineId,
    /// Ordered sequence of atomic edges the shape traverses
    pub edges: Vec<AtomicEdgeId>,
    /// Observed transitions: (from_node, in_edge, out_edge)
    pub transitions: Vec<(OsmNodeId, AtomicEdgeId, AtomicEdgeId)>,
    /// Match quality score (0.0 = poor, 1.0 = excellent)    
    pub quality: f64,
}

/// Configuration for map-matching
#[derive(Debug, Clone)]
pub struct MatchConfig {
    /// Maximum distance to snap shape points to edges (meters)
    pub snap_radius_m: f64,
    /// Penalty for changing z-class (layer) during matching
    pub z_class_change_penalty: f64,
    /// Penalty for mode mismatch
    pub mode_mismatch_penalty: f64,
    /// Maximum gap to bridge with shortest path (meters)
    pub max_gap_m: f64,
}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            snap_radius_m: 50.0,
            z_class_change_penalty: 1000.0,
            mode_mismatch_penalty: f64::INFINITY,
            max_gap_m: 500.0,
        }
    }
}

/// Map-matcher for GTFS shapes to OSM rail infrastructure
pub struct MapMatcher<'a> {
    index: &'a OsmRailIndex,
    config: MatchConfig,
}

impl<'a> MapMatcher<'a> {
    pub fn new(index: &'a OsmRailIndex, config: MatchConfig) -> Self {
        Self { index, config }
    }

    /// Match a GTFS shape (sequence of lon/lat points) to OSM edges
    pub fn match_shape(
        &self,
        line_id: LineId,
        shape_points: &[(f64, f64)],
        mode: RailMode,
    ) -> Option<MatchedShape> {
        if shape_points.len() < 2 {
            return None;
        }

        // Sample the shape at regular intervals for more stable matching
        let sampled = geometry_utils::sample_along_polyline(shape_points, 20.0);
        if sampled.len() < 2 {
            return None;
        }

        // Find candidate edges for each sample point
        let candidates: Vec<Vec<(&AtomicEdge, f64)>> = sampled
            .iter()
            .map(|&(lon, lat)| self.find_candidates(lon, lat, mode))
            .collect();

        // Build edge sequence using greedy matching with shortest-path stitching
        let edges = self.stitch_candidates(&candidates)?;
        if edges.is_empty() {
            return None;
        }

        // Extract transitions
        let transitions = self.extract_transitions(&edges);

        // Calculate match quality
        let quality = self.calculate_quality(&edges, &sampled);

        Some(MatchedShape {
            line_id,
            edges,
            transitions,
            quality,
        })
    }

    /// Find candidate edges near a point
    fn find_candidates(&self, lon: f64, lat: f64, mode: RailMode) -> Vec<(&AtomicEdge, f64)> {
        let nearby = self
            .index
            .edges_near_point(lon, lat, self.config.snap_radius_m);

        let mut candidates: Vec<(&AtomicEdge, f64)> = nearby
            .into_iter()
            .filter_map(|edge| {
                // Check mode compatibility
                if edge.mode != mode {
                    return None;
                }

                // Project point onto edge
                let (_, dist, _) =
                    geometry_utils::project_point_to_polyline((lon, lat), &edge.geometry)?;
                if dist <= self.config.snap_radius_m {
                    Some((edge, dist))
                } else {
                    None
                }
            })
            .collect();

        // Sort by distance
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates
    }

    /// Stitch candidate matches into a coherent edge sequence
    fn stitch_candidates(
        &self,
        candidates: &[Vec<(&AtomicEdge, f64)>],
    ) -> Option<Vec<AtomicEdgeId>> {
        if candidates.is_empty() {
            return None;
        }

        let mut result = Vec::new();
        let mut current_edge: Option<AtomicEdgeId> = None;
        let mut current_z_class = None;

        for (i, cands) in candidates.iter().enumerate() {
            if cands.is_empty() {
                continue;
            }

            // Find best candidate considering continuity
            let best = if let Some(curr_id) = current_edge {
                self.find_best_continuation(&cands, curr_id, current_z_class)
            } else {
                cands.first().map(|(e, d)| (e.id, *d))
            };

            if let Some((edge_id, _)) = best {
                if let Some(curr_id) = current_edge {
                    if curr_id != edge_id {
                        // Need to bridge from current to new edge
                        if let Some(path) = self.find_path(curr_id, edge_id) {
                            for path_edge in path {
                                if result.last() != Some(&path_edge) {
                                    result.push(path_edge);
                                }
                            }
                        }
                        current_edge = Some(edge_id);
                        if let Some(idx) = self.index.edge_index.get(&edge_id) {
                            current_z_class = Some(self.index.edges[*idx].z_class);
                        }
                    }
                } else {
                    result.push(edge_id);
                    current_edge = Some(edge_id);
                    if let Some(idx) = self.index.edge_index.get(&edge_id) {
                        current_z_class = Some(self.index.edges[*idx].z_class);
                    }
                }
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Find best continuation edge considering z-class and connectivity
    fn find_best_continuation(
        &self,
        candidates: &[(&AtomicEdge, f64)],
        current_id: AtomicEdgeId,
        current_z: Option<crate::osm_types::ZClass>,
    ) -> Option<(AtomicEdgeId, f64)> {
        let current_edge_idx = self.index.edge_index.get(&current_id)?;
        let current_edge = &self.index.edges[*current_edge_idx];

        // Check if any candidate is adjacent to current edge
        let current_endpoints = [current_edge.from, current_edge.to];

        let mut scored: Vec<_> = candidates
            .iter()
            .map(|(edge, dist)| {
                let mut score = *dist;

                // Bonus for same edge (continuity)
                if edge.id == current_id {
                    score -= 20.0;
                }

                // Bonus for adjacent edges
                if current_endpoints.contains(&edge.from) || current_endpoints.contains(&edge.to) {
                    score -= 10.0;
                }

                // Penalty for z-class change
                if let Some(z) = current_z {
                    if edge.z_class != z {
                        score += self.config.z_class_change_penalty;
                    }
                }

                (edge.id, score)
            })
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.first().map(|&(id, s)| (id, s))
    }

    /// Find shortest path between two edges using A* on the track graph
    fn find_path(&self, from: AtomicEdgeId, to: AtomicEdgeId) -> Option<Vec<AtomicEdgeId>> {
        let from_idx = *self.index.edge_index.get(&from)?;
        let to_idx = *self.index.edge_index.get(&to)?;
        let from_edge = &self.index.edges[from_idx];
        let to_edge = &self.index.edges[to_idx];

        // Try both endpoints of from_edge as start
        let start_nodes = [from_edge.from, from_edge.to];
        let end_nodes = [to_edge.from, to_edge.to];

        let mut best_path: Option<Vec<AtomicEdgeId>> = None;
        let mut best_cost = f64::INFINITY;

        for &start in &start_nodes {
            for &end in &end_nodes {
                if let Some((path, cost)) = self.astar(start, end, from, to, from_edge.z_class) {
                    if cost < best_cost {
                        best_cost = cost;
                        best_path = Some(path);
                    }
                }
            }
        }

        best_path
    }

    /// A* pathfinding between nodes
    fn astar(
        &self,
        start: OsmNodeId,
        goal: OsmNodeId,
        start_edge: AtomicEdgeId,
        goal_edge: AtomicEdgeId,
        preferred_z: crate::osm_types::ZClass,
    ) -> Option<(Vec<AtomicEdgeId>, f64)> {
        if start == goal {
            return Some((vec![goal_edge], 0.0));
        }

        #[derive(Clone, Copy)]
        struct State {
            node: OsmNodeId,
            cost: f64,
        }

        impl PartialEq for State {
            fn eq(&self, other: &Self) -> bool {
                self.node == other.node
            }
        }
        impl Eq for State {}
        impl PartialOrd for State {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for State {
            fn cmp(&self, other: &Self) -> Ordering {
                other
                    .cost
                    .partial_cmp(&self.cost)
                    .unwrap_or(Ordering::Equal)
            }
        }

        let goal_pos = self.index.node_position(goal)?;
        let heuristic = |node: OsmNodeId| -> f64 {
            if let Some(pos) = self.index.node_position(node) {
                geometry_utils::polyline_length(&[pos, goal_pos])
            } else {
                0.0
            }
        };

        let mut open = BinaryHeap::new();
        let mut came_from: HashMap<OsmNodeId, (OsmNodeId, AtomicEdgeId)> = HashMap::new();
        let mut g_score: HashMap<OsmNodeId, f64> = HashMap::new();

        g_score.insert(start, 0.0);
        open.push(State {
            node: start,
            cost: heuristic(start),
        });

        let max_iterations = 1000;
        let mut iterations = 0;

        while let Some(State { node, .. }) = open.pop() {
            iterations += 1;
            if iterations > max_iterations {
                return None;
            }

            if node == goal {
                // Reconstruct path
                let mut path = vec![goal_edge];
                let mut current = goal;
                while let Some(&(prev, edge)) = came_from.get(&current) {
                    if edge != start_edge && path.last() != Some(&edge) {
                        path.push(edge);
                    }
                    current = prev;
                    if current == start {
                        break;
                    }
                }
                path.push(start_edge);
                path.reverse();
                return Some((path, *g_score.get(&goal).unwrap_or(&f64::INFINITY)));
            }

            let current_g = *g_score.get(&node).unwrap_or(&f64::INFINITY);

            for edge in self.index.edges_at_node(node) {
                let neighbor = edge.other_end(node)?;
                let mut edge_cost = edge.length_m;

                // Penalize z-class changes
                if edge.z_class != preferred_z {
                    edge_cost += self.config.z_class_change_penalty;
                }

                let tentative_g = current_g + edge_cost;
                let neighbor_g = *g_score.get(&neighbor).unwrap_or(&f64::INFINITY);

                if tentative_g < neighbor_g {
                    came_from.insert(neighbor, (node, edge.id));
                    g_score.insert(neighbor, tentative_g);
                    open.push(State {
                        node: neighbor,
                        cost: tentative_g + heuristic(neighbor),
                    });
                }
            }
        }

        None
    }

    /// Extract transition tuples from edge sequence
    fn extract_transitions(
        &self,
        edges: &[AtomicEdgeId],
    ) -> Vec<(OsmNodeId, AtomicEdgeId, AtomicEdgeId)> {
        let mut transitions = Vec::new();

        for window in edges.windows(2) {
            let from_id = window[0];
            let to_id = window[1];

            let from_idx = match self.index.edge_index.get(&from_id) {
                Some(i) => *i,
                None => continue,
            };
            let to_idx = match self.index.edge_index.get(&to_id) {
                Some(i) => *i,
                None => continue,
            };

            let from_edge = &self.index.edges[from_idx];
            let to_edge = &self.index.edges[to_idx];

            // Find shared node
            let shared_node = if from_edge.to == to_edge.from || from_edge.to == to_edge.to {
                Some(from_edge.to)
            } else if from_edge.from == to_edge.from || from_edge.from == to_edge.to {
                Some(from_edge.from)
            } else {
                None
            };

            if let Some(node) = shared_node {
                transitions.push((node, from_id, to_id));
            }
        }

        transitions
    }

    /// Calculate match quality as fraction of shape covered
    fn calculate_quality(&self, edges: &[AtomicEdgeId], shape: &[(f64, f64)]) -> f64 {
        if edges.is_empty() || shape.is_empty() {
            return 0.0;
        }

        let shape_len = geometry_utils::polyline_length(shape);
        if shape_len == 0.0 {
            return 0.0;
        }

        let matched_len: f64 = edges
            .iter()
            .filter_map(|id| {
                let idx = self.index.edge_index.get(id)?;
                Some(self.index.edges[*idx].length_m)
            })
            .sum();

        (matched_len / shape_len).min(1.5).max(0.0) / 1.5
    }
}
