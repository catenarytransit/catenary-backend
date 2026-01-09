use crate::geometry_utils;
use crate::osm_loader::OsmRailIndex;
use crate::osm_types::{AtomicEdge, AtomicEdgeId, LineId, OsmNodeId, RailMode};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, trace, warn};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

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

/// Reusable context for pathfinding to avoid allocations
struct PathFinder {
    min_heap: BinaryHeap<State>,
    came_from: HashMap<OsmNodeId, (OsmNodeId, AtomicEdgeId)>,
    g_score: HashMap<OsmNodeId, f64>,
}

impl PathFinder {
    fn new() -> Self {
        Self {
            min_heap: BinaryHeap::new(),
            came_from: HashMap::new(),
            g_score: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.min_heap.clear();
        self.came_from.clear();
        self.g_score.clear();
    }
}

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
    pathfinder: std::cell::RefCell<PathFinder>,
}

impl<'a> MapMatcher<'a> {
    pub fn new(index: &'a OsmRailIndex, config: MatchConfig) -> Self {
        Self {
            index,
            config,
            pathfinder: std::cell::RefCell::new(PathFinder::new()),
        }
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
        let sampled = geometry_utils::sample_along_polyline(shape_points, 40.0);
        if sampled.len() < 2 {
            return None;
        }

        // Find candidate edges for each sample point
        let candidates: Vec<Vec<(&AtomicEdge, f64)>> = sampled
            .iter()
            .map(|&(lon, lat)| self.find_candidates(lon, lat, mode))
            .collect();

        // Use Viterbi algorithm to find the most likely sequence of edges
        let edges = self.viterbi_match(&candidates)?;
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
        // Optimized: Use a slightly larger radius for initial R-tree query but filter strictly
        // The R-tree query is fast, filtering is fast.
        let nearby = self
            .index
            .edges_near_point(lon, lat, self.config.snap_radius_m * 1.5);

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

        // Sort by distance and keep top K
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates.truncate(10); // Keep top 10 candidates max
        candidates
    }

    /// Viterbi algorithm to find the optimal path through candidate states
    fn viterbi_match(&self, candidates: &[Vec<(&AtomicEdge, f64)>]) -> Option<Vec<AtomicEdgeId>> {
        if candidates.is_empty() {
            return None;
        }

        // DP State: (Edge ID, Score, Predecessor Index in prev layer)
        // We store the index of the candidate in the previous layer to reconstruct the path
        struct ViterbiState {
            edge_id: AtomicEdgeId,
            score: f64, // Log probability (maximized)
            parent_idx: Option<usize>,
            edge_idx_in_layer: usize,
        }

        // Initialize first layer
        // layers[t] = Vec<ViterbiState>
        let mut layers: Vec<Vec<ViterbiState>> = Vec::with_capacity(candidates.len());

        // Initial probabilities based purely on emission (distance)
        let first_layer: Vec<ViterbiState> = candidates[0]
            .iter()
            .enumerate()
            .map(|(idx, (edge, dist))| {
                // Emission probability: exp(-dist^2 / sigma^2)
                // Log prob: -dist^2 / sigma^2
                // We can drop sigma^2 as it is constant for maximization, but let's keep it somewhat realistic
                // sigma = 20m roughly
                let sigma2 = 400.0;
                let score = -dist.powi(2) / sigma2;
                ViterbiState {
                    edge_id: edge.id,
                    score,
                    parent_idx: None,
                    edge_idx_in_layer: idx,
                }
            })
            .collect();

        if first_layer.is_empty() {
            return None;
        }
        layers.push(first_layer);

        // Iterate through time steps
        for t in 1..candidates.len() {
            let prev_layer = &layers[t - 1];
            let curr_candidates = &candidates[t];

            if curr_candidates.is_empty() {
                return None;
            }

            let mut current_layer = Vec::with_capacity(curr_candidates.len());
            let step_dist = 40.0; // We sampled at 40m

            for (curr_idx, (curr_edge, curr_dist)) in curr_candidates.iter().enumerate() {
                let mut best_score = f64::NEG_INFINITY;
                let mut best_parent_idx = None;

                // Emission score
                let sigma2 = 400.0;
                let emission_score = -curr_dist.powi(2) / sigma2;

                for (prev_idx, prev_state) in prev_layer.iter().enumerate() {
                    // Transition score
                    let transition_score = if curr_edge.id == prev_state.edge_id {
                        0.0 // Staying on same edge is very likely
                    } else {
                        // Check connectivity
                        let prev_idx_in_index = self.index.edge_index.get(&prev_state.edge_id);
                        let curr_idx_in_index = self.index.edge_index.get(&curr_edge.id);

                        if let (Some(&p_idx), Some(&c_idx)) = (prev_idx_in_index, curr_idx_in_index)
                        {
                            let p_edge = &self.index.edges[p_idx];
                            let c_edge = &self.index.edges[c_idx];

                            // Check if directly connected
                            if p_edge.to == c_edge.from {
                                -0.01 // Connected
                            } else if p_edge.from == c_edge.to
                                || p_edge.from == c_edge.from
                                || p_edge.to == c_edge.to
                            {
                                -0.01 // Connected (reversed/joined)
                            } else {
                                // Calculate network distance
                                let dist =
                                    self.find_path_dist_cached(prev_state.edge_id, curr_edge.id);
                                if dist.is_infinite() {
                                    -1000.0 // Impossible transition
                                } else {
                                    // Transition prob: |network_dist - euclidean_dist|
                                    let diff = (dist - step_dist).abs();
                                    let beta2 = 1000.0; // variance for transition
                                    -diff.powi(2) / beta2
                                }
                            }
                        } else {
                            -1000.0
                        }
                    };

                    let total_score = prev_state.score + transition_score + emission_score;

                    if total_score > best_score {
                        best_score = total_score;
                        best_parent_idx = Some(prev_idx);
                    }
                }

                current_layer.push(ViterbiState {
                    edge_id: curr_edge.id,
                    score: best_score,
                    parent_idx: best_parent_idx,
                    edge_idx_in_layer: curr_idx,
                });
            }
            layers.push(current_layer);
        }

        // Backtrack
        let last_layer = layers.last()?;
        let mut best_final_state = Option::<&ViterbiState>::None;
        for state in last_layer {
            if best_final_state.map_or(true, |best| state.score > best.score) {
                best_final_state = Some(state);
            }
        }

        let mut current_state = best_final_state?;
        if current_state.score < -10000.0 {
            // Path broken
            return None;
        }

        let mut path_indices: Vec<(usize, usize)> = Vec::new(); // (layer_idx, state_idx_in_layer)
        let last_layer_idx = layers.len() - 1;
        path_indices.push((last_layer_idx, current_state.edge_idx_in_layer));

        let mut curr_layer_idx = last_layer_idx;
        while curr_layer_idx > 0 {
            if let Some(parent_idx) = current_state.parent_idx {
                curr_layer_idx -= 1;
                current_state = &layers[curr_layer_idx][parent_idx];
                path_indices.push((curr_layer_idx, current_state.edge_idx_in_layer));
            } else {
                break;
            }
        }
        path_indices.reverse();

        // Convert to edge ID sequence
        let mut result_edges = Vec::new();
        let mut prev_edge_id: Option<AtomicEdgeId> = None;

        for (layer_idx, state_idx) in path_indices {
            let edge_id = layers[layer_idx][state_idx].edge_id;

            if let Some(prev) = prev_edge_id {
                if prev != edge_id {
                    // Patch connection if strictly necessary
                    if let Some(path) = self.find_path(prev, edge_id) {
                        for path_edge in path {
                            if result_edges.last() != Some(&path_edge) {
                                result_edges.push(path_edge);
                            }
                        }
                    }
                }
            } else {
                result_edges.push(edge_id);
            }
            prev_edge_id = Some(edge_id);
            if result_edges.last() != Some(&edge_id) {
                result_edges.push(edge_id);
            }
        }

        if result_edges.is_empty() {
            None
        } else {
            Some(result_edges)
        }
    }

    /// Wrapper for find_path that returns distance and uses cached context
    fn find_path_dist_cached(&self, from: AtomicEdgeId, to: AtomicEdgeId) -> f64 {
        if from == to {
            return 0.0;
        }
        let from_idx = match self.index.edge_index.get(&from) {
            Some(i) => *i,
            None => return f64::INFINITY,
        };
        let to_idx = match self.index.edge_index.get(&to) {
            Some(i) => *i,
            None => return f64::INFINITY,
        };

        let f_edge = &self.index.edges[from_idx];
        let t_edge = &self.index.edges[to_idx];

        if f_edge.to == t_edge.from || f_edge.to == t_edge.to {
            return 0.0;
        }
        if f_edge.from == t_edge.from || f_edge.from == t_edge.to {
            return 0.0;
        }

        if let Some((_, cost)) = self.find_path_internal(from, to, true) {
            cost
        } else {
            f64::INFINITY
        }
    }

    /// Find shortest path between two edges using A* on the track graph
    fn find_path(&self, from: AtomicEdgeId, to: AtomicEdgeId) -> Option<Vec<AtomicEdgeId>> {
        self.find_path_internal(from, to, false).map(|(p, _)| p)
    }

    fn find_path_internal(
        &self,
        from: AtomicEdgeId,
        to: AtomicEdgeId,
        dist_only: bool,
    ) -> Option<(Vec<AtomicEdgeId>, f64)> {
        let from_idx = *self.index.edge_index.get(&from)?;
        let to_idx = *self.index.edge_index.get(&to)?;
        let from_edge = &self.index.edges[from_idx];
        let to_edge = &self.index.edges[to_idx];

        // Try both endpoints of from_edge as start
        let start_nodes = [from_edge.from, from_edge.to];
        let end_nodes = [to_edge.from, to_edge.to];

        let mut best_path: Option<Vec<AtomicEdgeId>> = None;
        let mut best_cost = f64::INFINITY;

        // We reuse the pathfinder context, clearing it inside astar
        // Note: nesting astar calls would break this, but we don't nest.

        for &start in &start_nodes {
            for &end in &end_nodes {
                if let Some((path, cost)) = self.astar(start, end, from, to, from_edge.z_class) {
                    if cost < best_cost {
                        best_cost = cost;
                        if !dist_only {
                            best_path = Some(path);
                        }
                    }
                }
            }
        }

        if best_cost.is_infinite() {
            None
        } else {
            Some((best_path.unwrap_or_default(), best_cost))
        }
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

        let goal_pos = self.index.node_position(goal)?;
        let heuristic = |node: OsmNodeId| -> f64 {
            if let Some(pos) = self.index.node_position(node) {
                geometry_utils::polyline_length(&[pos, goal_pos])
            } else {
                0.0
            }
        };

        // Borrow reusable buffers
        let mut pf = self.pathfinder.borrow_mut();
        pf.clear();

        // Destructure to allow simultaneous mutable borrows of fields
        let PathFinder {
            min_heap: open,
            came_from,
            g_score,
            ..
        } = &mut *pf;

        g_score.insert(start, 0.0);
        open.push(State {
            node: start,
            cost: heuristic(start),
        });

        // Reduced max iterations for performance
        let max_iterations = 500;
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
                // Need to clone result out of borrowed context
                let cost = *g_score.get(&goal).unwrap_or(&f64::INFINITY);
                return Some((path, cost));
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
