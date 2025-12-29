use crate::edges::{GraphEdge, NodeId, convert_to_geo};
use crate::geometry_utils::{get_ortho_line_at_dist, intersection};
use anyhow::Result;
use catenary::graph_formats::TurnRestriction;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use ordered_float::OrderedFloat;
use std::collections::{BinaryHeap, HashMap, HashSet};

// ============================================================================
// Configuration Constants (matching C++ TopoConfig defaults)
// ============================================================================

/// Maximum deviation in path length for a turn to still be valid (meters)
const MAX_LENGTH_DEV: f64 = 100.0;

/// Penalty added for making a sharp turn (180 degrees) (meters equivalent)
const TURN_PENALTY: f64 = 50.0;

/// Angle threshold (in radians) below which a turn is considered a "full turn"
/// In C++: fullTurnAngle (default around 0.2 = ~11 degrees)
const FULL_TURN_ANGLE: f64 = 0.2;

/// Maximum gap allowed when checking if edges are consecutive (meters)
const MAX_GAP_METERS: f64 = 50.0;

/// Map from (chateau, route_id) to list of Shape Geometries
pub type RouteShapeMap = HashMap<(String, String), Vec<GeoLineString<f64>>>;

// ============================================================================
// Restriction Graph Data Structures (matching C++ RestrGraph)
// ============================================================================

/// A node in the restriction graph
#[derive(Clone, Debug)]
struct RestrNode {
    pos: [f64; 2],
    /// Map from line_id -> (from_edge_idx -> set of blocked to_edge_idx)
    restrs: HashMap<String, HashMap<usize, HashSet<usize>>>,
}

/// An edge in the restriction graph (directed)
#[derive(Clone, Debug)]
struct RestrEdge {
    from_node: usize,
    to_node: usize,
    geom: GeoLineString<f64>,
    length: f64,
    lines: HashSet<String>,
}

/// The restriction inference graph
struct RestrGraph {
    nodes: Vec<RestrNode>,
    /// Edges are stored in Options to allow deletion (splitting)
    edges: Vec<Option<RestrEdge>>,
    /// Adjacency list: node_idx -> list of edge indices going OUT from node
    adj_out: HashMap<usize, Vec<usize>>,
    /// Adjacency list: node_idx -> list of edge indices going IN to node
    adj_in: HashMap<usize, Vec<usize>>,
}

impl RestrGraph {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            adj_out: HashMap::new(),
            adj_in: HashMap::new(),
        }
    }

    fn add_node(&mut self, pos: [f64; 2]) -> usize {
        let idx = self.nodes.len();
        self.nodes.push(RestrNode {
            pos,
            restrs: HashMap::new(),
        });
        idx
    }

    fn add_edge(
        &mut self,
        from_node: usize,
        to_node: usize,
        geom: GeoLineString<f64>,
        lines: HashSet<String>,
    ) -> usize {
        #[allow(deprecated)]
        let length = geom.haversine_length();
        let idx = self.edges.len();
        self.edges.push(Some(RestrEdge {
            from_node,
            to_node,
            geom,
            length,
            lines,
        }));
        self.adj_out.entry(from_node).or_default().push(idx);
        self.adj_in.entry(to_node).or_default().push(idx);
        idx
    }

    fn del_edge(&mut self, edge_idx: usize) {
        if let Some(edge) = &self.edges[edge_idx] {
            // Remove from adjacency lists
            if let Some(outs) = self.adj_out.get_mut(&edge.from_node) {
                outs.retain(|&x| x != edge_idx);
            }
            if let Some(ins) = self.adj_in.get_mut(&edge.to_node) {
                ins.retain(|&x| x != edge_idx);
            }
        }
        self.edges[edge_idx] = None;
    }

    /// Add a turn restriction: for the given line, forbid going from `from_edge` to `to_edge`
    fn add_restriction(
        &mut self,
        line_id: &str,
        node_idx: usize,
        from_edge: usize,
        to_edge: usize,
    ) {
        self.nodes[node_idx]
            .restrs
            .entry(line_id.to_string())
            .or_default()
            .entry(from_edge)
            .or_default()
            .insert(to_edge);
    }
}

// ============================================================================
// Dijkstra Cost Function (matching C++ CostFunc)
// ============================================================================

/// Cost function for Dijkstra that accounts for:
/// - Edge length
/// - Turn penalties for sharp turns
/// - Turn restrictions
/// - Line-specific routing
struct CostFunc<'a> {
    line_id: &'a str,
    max_cost: f64,
    turn_penalty: f64,
    full_turn_angle: f64,
    graph: &'a RestrGraph,
}

impl<'a> CostFunc<'a> {
    fn new(line_id: &'a str, max_cost: f64, graph: &'a RestrGraph) -> Self {
        Self {
            line_id,
            max_cost,
            turn_penalty: TURN_PENALTY,
            full_turn_angle: FULL_TURN_ANGLE,
            graph,
        }
    }

    fn inf(&self) -> f64 {
        self.max_cost
    }

    /// Calculate cost of traversing from `from_edge` through `node_idx` to `to_edge`
    fn edge_cost(&self, from_edge: Option<usize>, node_idx: usize, to_edge: usize) -> f64 {
        let Some(to_e) = &self.graph.edges[to_edge] else {
            return self.inf();
        };

        // If edge doesn't contain the line we're routing for, infinite cost
        if !to_e.lines.contains(self.line_id) {
            return self.inf();
        }

        // Check if there's a start edge
        let Some(from_edge_idx) = from_edge else {
            // No from edge (start of path) - just return the edge length
            return to_e.length;
        };

        let Some(from_e) = &self.graph.edges[from_edge_idx] else {
            return self.inf();
        };

        // If from edge doesn't contain the line, infinite cost
        if !from_e.lines.contains(self.line_id) {
            return self.inf();
        }

        // Check for turn restriction at this node
        let node = &self.graph.nodes[node_idx];
        if let Some(line_restrs) = node.restrs.get(self.line_id) {
            if let Some(blocked_to) = line_restrs.get(&from_edge_idx) {
                if blocked_to.contains(&to_edge) {
                    return self.inf();
                }
            }
        }

        // Don't allow going back the same edge
        if from_e.from_node == to_e.to_node && from_e.to_node == to_e.from_node {
            return self.inf();
        }

        let mut cost = 0.0;

        // Calculate turn penalty based on angle
        if from_e.geom.0.len() >= 2 && to_e.geom.0.len() >= 2 {
            let node_pos = self.graph.nodes[node_idx].pos;
            let from_point = from_e.geom.0[from_e.geom.0.len().saturating_sub(2)];
            let to_point = to_e.geom.0.get(1).copied().unwrap_or(to_e.geom.0[0]);

            // Calculate inner product (cos of angle) - similar to C++ innerProd
            let v1 = [from_point.x - node_pos[0], from_point.y - node_pos[1]];
            let v2 = [to_point.x - node_pos[0], to_point.y - node_pos[1]];

            let dot = v1[0] * v2[0] + v1[1] * v2[1];
            let len1 = (v1[0] * v1[0] + v1[1] * v1[1]).sqrt();
            let len2 = (v2[0] * v2[0] + v2[1] * v2[1]).sqrt();

            if len1 > 0.0 && len2 > 0.0 {
                let cos_angle = dot / (len1 * len2);
                // If cos_angle is close to 1, it's a sharp U-turn
                // C++ uses innerProd which returns the angle directly, so we do similarly
                if cos_angle > (1.0 - self.full_turn_angle) {
                    cost += self.turn_penalty;
                }
            }
        }

        // Final cost = turn penalty + edge length
        cost + to_e.length
    }
}

// ============================================================================
// Dijkstra's Algorithm
// ============================================================================

/// State for Dijkstra priority queue
#[derive(Clone, Copy, PartialEq, Eq)]
struct DijkstraState {
    cost: OrderedFloat<f64>,
    node: usize,
    via_edge: usize,
}

impl Ord for DijkstraState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Run Dijkstra from a set of starting edges to a set of target edges
/// Returns the minimum cost path, or infinity if no valid path exists
fn dijkstra(
    graph: &RestrGraph,
    start_node: usize,
    is_target: impl Fn(usize) -> bool,
    cost_func: &CostFunc,
) -> Option<f64> {
    let mut heap = BinaryHeap::new();
    let mut dist: HashMap<(usize, usize), f64> = HashMap::new();

    // Start from start_node with 0 cost, via conceptual "no edge"
    // Actually we need to start traversing edges FROM the start node.
    // The cost includes edge length.

    // Initial state: We are AT start_node. Cost is 0.
    // We explore outgoing edges.

    // BUT CostFunc expects `edge_cost(from_edge, node, to_edge)`.
    // If we are just starting, `from_edge` is None.

    // Let's add initial outgoing edges to heap
    if let Some(out_edges) = graph.adj_out.get(&start_node) {
        for &edge_idx in out_edges {
            if let Some(edge) = &graph.edges[edge_idx] {
                let c = cost_func.edge_cost(None, start_node, edge_idx);
                if c < cost_func.inf() {
                    heap.push(DijkstraState {
                        cost: OrderedFloat(c),
                        node: edge.to_node,
                        via_edge: edge_idx,
                    });
                    dist.insert((edge.to_node, edge_idx), c);
                }
            }
        }
    }

    let mut min_cost = f64::MAX;

    while let Some(DijkstraState {
        cost,
        node,
        via_edge,
    }) = heap.pop()
    {
        let cost_val = cost.0;

        if cost_val >= min_cost {
            continue;
        }

        if is_target(node) {
            if cost_val < min_cost {
                min_cost = cost_val;
            }
            // Continue to find potentially shorter paths? Dijkstra guarantees first hit is shortest?
            // Yes, if we pop from PQ.
            return Some(min_cost);
        }

        if let Some(&d) = dist.get(&(node, via_edge)) {
            if cost_val > d {
                continue;
            }
        }

        if let Some(out_edges) = graph.adj_out.get(&node) {
            for &next_edge_idx in out_edges {
                let next_cost = cost_val + cost_func.edge_cost(Some(via_edge), node, next_edge_idx);
                if next_cost >= cost_func.inf() {
                    continue;
                }

                let Some(next_edge) = &graph.edges[next_edge_idx] else {
                    continue;
                };
                let next_node = next_edge.to_node;

                if dist
                    .get(&(next_node, next_edge_idx))
                    .map_or(true, |&d| next_cost < d)
                {
                    dist.insert((next_node, next_edge_idx), next_cost);
                    heap.push(DijkstraState {
                        cost: OrderedFloat(next_cost),
                        node: next_node,
                        via_edge: next_edge_idx,
                    });
                }
            }
        }
    }

    if min_cost < f64::MAX {
        Some(min_cost)
    } else {
        None
    }
}

// ============================================================================
// Main Restriction Inference Logic
// ============================================================================

pub async fn infer_from_db(
    pool: &CatenaryPostgresPool,
    edges: &[GraphEdge],
) -> Result<Vec<TurnRestriction>> {
    println!("Loading shapes for restriction inference (Dijkstra mode)...");
    let mut conn = pool.get().await?;

    // Import diesel traits locally to avoid conflict with geo
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;

    use catenary::schema::gtfs::direction_pattern_meta::dsl as meta_dsl;

    // Load relationships: Pattern -> Shape, Pattern -> Route
    let metas = meta_dsl::direction_pattern_meta
        .load::<catenary::models::DirectionPatternMeta>(&mut conn)
        .await?;

    let mut pattern_to_shape: HashMap<String, String> = HashMap::new();
    let mut pattern_to_route: HashMap<String, (String, String)> = HashMap::new();
    let mut _relevant_shape_ids = HashSet::new();

    for m in metas {
        if let Some(sid) = m.gtfs_shape_id {
            pattern_to_shape.insert(m.direction_pattern_id.clone(), sid.clone());
            _relevant_shape_ids.insert(sid);
        }
        if let Some(rid) = m.route_id {
            pattern_to_route.insert(
                m.direction_pattern_id.clone(),
                (m.chateau.clone(), rid.to_string()),
            );
        }
    }

    // Load Shapes
    use catenary::schema::gtfs::shapes::dsl as user_shapes_dsl;
    let loaded_shapes = user_shapes_dsl::shapes
        .filter(user_shapes_dsl::stop_to_stop_generated.eq(false))
        .filter(user_shapes_dsl::route_type.eq_any(vec![0, 1, 2])) // Rail only
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    let mut shape_map: HashMap<String, catenary::models::Shape> = HashMap::new();
    for s in loaded_shapes {
        shape_map.insert(s.shape_id.clone(), s);
    }

    // Build RouteShapeMap
    let mut route_shapes: RouteShapeMap = HashMap::new();

    for (pat_id, shape_id) in &pattern_to_shape {
        if let Some(route_key) = pattern_to_route.get(pat_id) {
            if let Some(shape) = shape_map.get(shape_id) {
                let geo = convert_to_geo(&shape.linestring);
                route_shapes.entry(route_key.clone()).or_default().push(geo);
            }
        }
    }

    Ok(infer_restrictions_dijkstra(
        edges,
        &route_shapes,
        MAX_LENGTH_DEV,
    ))
}

/// Type aliases for handle maps matching C++ _handlesA/_handlesB
/// Maps: GraphEdgeIdx -> set of RestrNode indices
type HandleMap = HashMap<usize, HashSet<usize>>;

/// Infer turn restrictions using Dijkstra-based path cost analysis
/// This matches the C++ RestrInferrer algorithm
pub fn infer_restrictions_dijkstra(
    edges: &[GraphEdge],
    route_shapes: &RouteShapeMap,
    max_length_deviation: f64,
) -> Vec<TurnRestriction> {
    println!("Inferring turn restrictions (Dijkstra)...");

    // Build restriction graph from edges
    let (mut graph, restr_edge_map, node_map) = build_restriction_graph(edges, route_shapes);

    // Add handles - returns separate A (1/3) and B (2/3) handle maps
    let (handles_a, handles_b) = add_handles(&mut graph, edges, &restr_edge_map, route_shapes);

    let mut restrictions = Vec::new();

    // Build adjacency for the original edges (treating edges as undirected for iteration)
    // C++ iterates over all adjacencies, checking each pair
    let mut adj: HashMap<NodeId, Vec<usize>> = HashMap::new();
    for (i, edge) in edges.iter().enumerate() {
        adj.entry(edge.from).or_default().push(i);
        adj.entry(edge.to).or_default().push(i);
    }

    let all_nodes: HashSet<NodeId> = adj.keys().cloned().collect();
    let mut restrictions_count = 0;

    for node in &all_nodes {
        let Some(adjacent_indices) = adj.get(node) else {
            continue;
        };

        // Check every pair of adjacent edges at this node (matching C++ lines 86-112)
        for &edg1_idx in adjacent_indices {
            for &edg2_idx in adjacent_indices {
                if edg1_idx == edg2_idx {
                    continue;
                }

                let edg1 = &edges[edg1_idx];
                let edg2 = &edges[edg2_idx];

                // Get routes on edg1
                for route_key in &edg1.route_ids {
                    // Check if edg2 has the same route (C++ line 92: hasLine check)
                    if !edg2.route_ids.contains(route_key) {
                        continue;
                    }

                    let line_id = format!("{}_{}", route_key.0, route_key.1);

                    // NOTE: C++ direction filtering (lines 96-103) would go here
                    // For now we skip it since GTFS edges are bidirectional by default
                    // The restriction check below handles this implicitly

                    // C++ line 106: Check BOTH directions - only add restriction if BOTH fail
                    let check1 = check_path_for_turn(
                        &graph,
                        edges,
                        &handles_a,
                        &handles_b,
                        edg1_idx,
                        edg2_idx,
                        *node,
                        &line_id,
                        max_length_deviation,
                    );
                    let check2 = check_path_for_turn(
                        &graph,
                        edges,
                        &handles_a,
                        &handles_b,
                        edg2_idx,
                        edg1_idx,
                        *node,
                        &line_id,
                        max_length_deviation,
                    );

                    if !check1 && !check2 {
                        restrictions.push(TurnRestriction {
                            from_edge_index: edg1_idx,
                            to_edge_index: edg2_idx,
                            route_id: route_key.clone(),
                        });
                        restrictions_count += 1;
                    }
                }
            }
        }
    }

    // C++ lines 116-145: Clear erroneous/superfluous exceptions
    // If a line on an edge has ALL its continuations blocked, something is wrong
    restrictions = cleanup_dead_restrictions(restrictions, edges, &adj);

    println!(
        "Inferred {} turn restrictions (Dijkstra) after cleanup.",
        restrictions_count
    );
    restrictions
}

/// Clean up restrictions that would block ALL continuations for a line
/// Matching C++ lines 116-145
fn cleanup_dead_restrictions(
    mut restrictions: Vec<TurnRestriction>,
    edges: &[GraphEdge],
    adj: &HashMap<NodeId, Vec<usize>>,
) -> Vec<TurnRestriction> {
    // Build a set of blocked turns for quick lookup
    let mut blocked: HashSet<(usize, usize, String)> = HashSet::new();
    for r in &restrictions {
        let key = (
            r.from_edge_index,
            r.to_edge_index,
            format!("{}_{}", r.route_id.0, r.route_id.1),
        );
        blocked.insert(key);
    }

    // Check each node
    let all_nodes: HashSet<NodeId> = adj.keys().cloned().collect();
    let mut to_remove: HashSet<(usize, usize, String)> = HashSet::new();

    for node in &all_nodes {
        let Some(adjacent_indices) = adj.get(node) else {
            continue;
        };

        for &edg1_idx in adjacent_indices {
            let edg1 = &edges[edg1_idx];

            for route_key in &edg1.route_ids {
                let line_id = format!("{}_{}", route_key.0, route_key.1);

                // Count other edges with this line, and how many are connected
                let mut other_occs = 0;
                let mut other_cons = 0;

                for &edg2_idx in adjacent_indices {
                    if edg1_idx == edg2_idx {
                        continue;
                    }

                    let edg2 = &edges[edg2_idx];
                    if edg2.route_ids.contains(route_key) {
                        other_occs += 1;

                        // Check if connection is allowed (in either direction)
                        let key1 = (edg1_idx, edg2_idx, line_id.clone());
                        let key2 = (edg2_idx, edg1_idx, line_id.clone());
                        if !blocked.contains(&key1) || !blocked.contains(&key2) {
                            other_cons += 1;
                        }
                    }
                }

                // If there are other edges with this line but NO connections allowed,
                // delete all restrictions from this edge for this line
                if other_cons == 0 && other_occs > 0 {
                    for &edg2_idx in adjacent_indices {
                        let key = (edg1_idx, edg2_idx, line_id.clone());
                        to_remove.insert(key);
                    }
                }
            }
        }
    }

    // Remove the erroneous restrictions
    let removed_count = to_remove.len();
    restrictions.retain(|r| {
        let key = (
            r.from_edge_index,
            r.to_edge_index,
            format!("{}_{}", r.route_id.0, r.route_id.1),
        );
        !to_remove.contains(&key)
    });

    if removed_count > 0 {
        println!("Cleaned up {} erroneous restrictions.", removed_count);
    }

    restrictions
}

/// Check if a valid path exists for a turn from edg1 to edg2 at shared_node
/// Matching C++ RestrInferrer::check (lines 313-358)
#[allow(clippy::too_many_arguments)]
fn check_path_for_turn(
    graph: &RestrGraph,
    edges: &[GraphEdge],
    handles_a: &HandleMap,
    handles_b: &HandleMap,
    edg1_idx: usize,
    edg2_idx: usize,
    shared_node: NodeId,
    line_id: &str,
    max_dev: f64,
) -> bool {
    let edg1 = &edges[edg1_idx];
    let edg2 = &edges[edg2_idx];

    // Determine which handles to use based on shared node position
    // C++ lines 322-348: if sharedNd == edg1->getFrom() use handlesA, else handlesB
    let from_handles = if shared_node == edg1.from {
        handles_a.get(&edg1_idx)
    } else {
        handles_b.get(&edg1_idx)
    };

    let to_handles = if shared_node == edg2.from {
        handles_a.get(&edg2_idx)
    } else {
        handles_b.get(&edg2_idx)
    };

    // Collect "from" edges (incoming edges to handle nodes)
    let mut from_edges: HashSet<usize> = HashSet::new();
    if let Some(handle_nodes) = from_handles {
        for &nd in handle_nodes {
            if let Some(in_edges) = graph.adj_in.get(&nd) {
                from_edges.extend(in_edges.iter().filter(|&&e| {
                    graph.edges[e]
                        .as_ref()
                        .map(|edge| edge.lines.contains(line_id))
                        .unwrap_or(false)
                }));
            }
        }
    }

    // Collect "to" edges (incoming edges to handle nodes - same as C++ getAdjListIn)
    let mut to_edges: HashSet<usize> = HashSet::new();
    if let Some(handle_nodes) = to_handles {
        for &nd in handle_nodes {
            // C++ uses getAdjListIn() for BOTH from and to edges
            if let Some(in_edges) = graph.adj_in.get(&nd) {
                to_edges.extend(in_edges.iter().filter(|&&e| {
                    graph.edges[e]
                        .as_ref()
                        .map(|edge| edge.lines.contains(line_id))
                        .unwrap_or(false)
                }));
            }
        }
    }

    if from_edges.is_empty() || to_edges.is_empty() {
        // No handles found - assume path exists (conservative)
        return true;
    }

    // Calculate expected distance (C++ lines 319-320)
    let edg1_geom = convert_to_geo(&edg1.geometry);
    let edg2_geom = convert_to_geo(&edg2.geometry);
    #[allow(deprecated)]
    let cur_d = edg1_geom.haversine_length() * 0.33 + edg2_geom.haversine_length() * 0.33;

    // curdist + maxL is the infinity threshold (C++ lines 353-355)
    let eps = 0.1;
    let max_cost = cur_d + max_dev + eps;
    let cost_func = CostFunc::new(line_id, max_cost, graph);

    // Run EDijkstra from "from_edges" to "to_edges" (C++ line 357)
    let min_cost = dijkstra_edges(graph, &from_edges, &to_edges, &cost_func);

    // C++ line 357: return cost - curD < maxLengthDev
    min_cost.map(|cost| cost - cur_d < max_dev).unwrap_or(false)
}

/// Dijkstra variant that starts from a set of edges and ends at a set of edges
/// Matching C++ EDijkstra::shortestPath behavior
/// 
/// In C++, EDijkstra finds shortest path from a set of "from" edges to a set of "to" edges.
/// The cost includes traversing the from edge and ends when we reach (not after traversing) a to edge.
fn dijkstra_edges(
    graph: &RestrGraph,
    from_edges: &HashSet<usize>,
    to_edges: &HashSet<usize>,
    cost_func: &CostFunc,
) -> Option<f64> {
    let mut heap = BinaryHeap::new();
    let mut dist: HashMap<(usize, usize), f64> = HashMap::new();

    // Initialize with starting edges
    for &edge_idx in from_edges {
        let Some(edge) = &graph.edges[edge_idx] else {
            continue;
        };

        // Cost to traverse this starting edge (no "from" edge)
        let c = cost_func.edge_cost(None, edge.from_node, edge_idx);
        if c < cost_func.inf() {
            // Check if this starting edge is also a target edge
            if to_edges.contains(&edge_idx) {
                return Some(c);
            }
            
            heap.push(DijkstraState {
                cost: OrderedFloat(c),
                node: edge.to_node,
                via_edge: edge_idx,
            });
            dist.insert((edge.to_node, edge_idx), c);
        }
    }

    while let Some(DijkstraState {
        cost,
        node,
        via_edge,
    }) = heap.pop()
    {
        let cost_val = cost.0;

        if let Some(&d) = dist.get(&(node, via_edge)) {
            if cost_val > d {
                continue;
            }
        }

        if let Some(out_edges) = graph.adj_out.get(&node) {
            for &next_edge_idx in out_edges {
                let next_cost = cost_val + cost_func.edge_cost(Some(via_edge), node, next_edge_idx);
                if next_cost >= cost_func.inf() {
                    continue;
                }

                // Check if we've reached a target edge
                // We return when we CAN reach a target, not after traversing it
                if to_edges.contains(&next_edge_idx) {
                    return Some(next_cost);
                }

                let Some(next_edge) = &graph.edges[next_edge_idx] else {
                    continue;
                };
                let next_node = next_edge.to_node;

                if dist
                    .get(&(next_node, next_edge_idx))
                    .map_or(true, |&d| next_cost < d)
                {
                    dist.insert((next_node, next_edge_idx), next_cost);
                    heap.push(DijkstraState {
                        cost: OrderedFloat(next_cost),
                        node: next_node,
                        via_edge: next_edge_idx,
                    });
                }
            }
        }
    }

    None
}

/// Build a restriction graph for Dijkstra-based checking
/// Returns: (graph, edge_map, node_map)
fn build_restriction_graph(
    edges: &[GraphEdge],
    _route_shapes: &RouteShapeMap,
) -> (
    RestrGraph,
    HashMap<usize, Vec<usize>>,
    HashMap<NodeId, usize>,
) {
    let mut graph = RestrGraph::new();
    let mut restr_edge_map: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut node_map: HashMap<NodeId, usize> = HashMap::new();

    // Create nodes
    for edge in edges {
        let from_geo = convert_to_geo(&edge.geometry);
        let to_geo = convert_to_geo(&edge.geometry);

        let from_pos = from_geo.0.get(0).map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
        let to_pos = to_geo
            .0
            .get(to_geo.0.len().saturating_sub(1))
            .map(|c| [c.x, c.y])
            .unwrap_or([0.0, 0.0]);

        if !node_map.contains_key(&edge.from) {
            let idx = graph.add_node(from_pos);
            node_map.insert(edge.from, idx);
        }
        if !node_map.contains_key(&edge.to) {
            let idx = graph.add_node(to_pos);
            node_map.insert(edge.to, idx);
        }
    }

    // Create edges (both directions for bidirectional graph)
    for (edge_idx, edge) in edges.iter().enumerate() {
        let from_node = node_map[&edge.from];
        let to_node = node_map[&edge.to];
        let geom = convert_to_geo(&edge.geometry);

        let lines: HashSet<String> = edge
            .route_ids
            .iter()
            .map(|(chateau, rid)| format!("{}_{}", chateau, rid))
            .collect();

        // Add forward edge
        let idx1 = graph.add_edge(from_node, to_node, geom.clone(), lines.clone());
        restr_edge_map.entry(edge_idx).or_default().push(idx1);

        // Add reverse edge
        let mut rev_geom = geom.clone();
        rev_geom.0.reverse();
        let idx2 = graph.add_edge(to_node, from_node, rev_geom, lines);
        restr_edge_map.entry(edge_idx).or_default().push(idx2);
    }

    (graph, restr_edge_map, node_map)
}

// ============================================================================
// Edge Splitting / Handles Logic
// ============================================================================

fn edge_rpl(graph: &mut RestrGraph, node_idx: usize, old_edge: usize, new_edge: usize) {
    if old_edge == new_edge {
        return;
    }

    // We need to mutate the node's restrictions
    // Since we can't easily iterate and mutate, we'll collect updates first
    let node = &mut graph.nodes[node_idx];

    for restr_map in node.restrs.values_mut() {
        // Replace in keys (from_edge)
        if let Some(blocked) = restr_map.remove(&old_edge) {
            restr_map.insert(new_edge, blocked);
        }

        // Replace in values (to_edge set)
        for blocked_set in restr_map.values_mut() {
            if blocked_set.remove(&old_edge) {
                blocked_set.insert(new_edge);
            }
        }
    }
}

/// Splits edges based on route shape intersections, creating "handles" for precise restriction checking
/// Returns: (handles_a, handles_b) - A handles at 1/3 position, B handles at 2/3 position
/// Matches C++ _handlesA/_handlesB separation
fn add_handles(
    graph: &mut RestrGraph,
    edges: &[GraphEdge],
    restr_edge_map: &HashMap<usize, Vec<usize>>,
    route_shapes: &RouteShapeMap,
) -> (HandleMap, HandleMap) {
    let mut handles_a: HandleMap = HashMap::new(); // Handles at 1/3 position
    let mut handles_b: HandleMap = HashMap::new(); // Handles at 2/3 position

    // Configuration for handle checking
    let max_dist = 100.0;
    let aggr_dist = 20.0;

    // Request: (GraphEdgeIdx, RestrEdgeIdx, Fraction, SplitPoint, IsHandleA)
    let mut split_requests: Vec<(usize, usize, f64, GeoPoint<f64>, bool)> = Vec::new();

    for (orig_idx, orig_edge) in edges.iter().enumerate() {
        let geom = convert_to_geo(&orig_edge.geometry);
        #[allow(deprecated)]
        let len_meters = geom.haversine_length();

        // Skip very short edges
        if len_meters < 1.0 {
            continue;
        }

        let check_pos = (len_meters / 2.0).min(2.0 * aggr_dist);

        // A handles at 1/3, B handles at 2/3 (matching C++ lines 243-254)
        let hndl_la_check = get_ortho_line_at_dist(&geom, check_pos, max_dist);
        let hndl_lb_check = get_ortho_line_at_dist(&geom, len_meters - check_pos, max_dist);

        let hndl_la = get_ortho_line_at_dist(&geom, len_meters / 3.0, max_dist);
        let hndl_lb = get_ortho_line_at_dist(&geom, len_meters * 2.0 / 3.0, max_dist);

        for route_key in &orig_edge.route_ids {
            let line_id = format!("{}_{}", route_key.0, route_key.1);
            let Some(shapes) = route_shapes.get(route_key) else {
                continue;
            };

            for shape in shapes {
                // Closure to check and collect intersections
                let mut check_and_collect =
                    |hndl_opt: Option<geo::Line<f64>>,
                     check_opt: Option<geo::Line<f64>>,
                     is_handle_a: bool| {
                        let Some(hndl_line) = hndl_opt else { return };
                        let Some(check_line) = check_opt else { return };

                        let hndl_ls = GeoLineString::from(vec![hndl_line.start, hndl_line.end]);
                        let check_ls = GeoLineString::from(vec![check_line.start, check_line.end]);

                        if !intersection(&check_ls, shape).is_empty() {
                            let isects = intersection(&hndl_ls, shape);
                            let final_isects = if isects.is_empty() {
                                intersection(&check_ls, shape)
                            } else {
                                isects
                            };

                            for isect in final_isects {
                                if let Some(restr_indices) = restr_edge_map.get(&orig_idx) {
                                    for &r_idx in restr_indices {
                                        if let Some(r_edge) = &graph.edges[r_idx] {
                                            if r_edge.lines.contains(&line_id) {
                                                let frac = r_edge
                                                    .geom
                                                    .line_locate_point(&isect)
                                                    .unwrap_or(0.0);
                                                split_requests.push((
                                                    orig_idx,
                                                    r_idx,
                                                    frac,
                                                    isect,
                                                    is_handle_a,
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    };
                check_and_collect(hndl_la, hndl_la_check, true); // A handles
                check_and_collect(hndl_lb, hndl_lb_check, false); // B handles
            }
        }
    }

    // Group splits by RestrEdge index
    // RestrEdgeIdx -> (GraphEdgeIdx, SplitsWithHandleType)
    let mut splits_by_restr_edge: HashMap<usize, (usize, Vec<(f64, GeoPoint<f64>, bool)>)> =
        HashMap::new();

    for (g_idx, r_idx, frac, pt, is_a) in split_requests {
        if frac > 0.01 && frac < 0.99 {
            splits_by_restr_edge
                .entry(r_idx)
                .or_insert((g_idx, Vec::new()))
                .1
                .push((frac, pt, is_a));
        }
    }

    // Apply splits and collect handles into A and B maps
    for (edge_idx, (g_idx, mut splits)) in splits_by_restr_edge {
        splits.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        splits.dedup_by(|a, b| (a.0 - b.0).abs() < 1e-4);

        if splits.is_empty() {
            continue;
        }

        let Some(orig_restr_edge) = graph.edges[edge_idx].clone() else {
            continue;
        };
        let mut last_node = orig_restr_edge.from_node;
        let mut last_frac = 0.0;

        for (frac, pt, is_handle_a) in splits {
            if (frac - last_frac).abs() < 1e-6 {
                continue;
            }

            let new_node_id = graph.add_node([pt.x(), pt.y()]);

            let sub_geom = get_sub_geom(&orig_restr_edge.geom, last_frac, frac);
            let new_edge_idx = graph.add_edge(
                last_node,
                new_node_id,
                sub_geom,
                orig_restr_edge.lines.clone(),
            );

            // Add handle to appropriate map based on position (A or B)
            if is_handle_a {
                handles_a.entry(g_idx).or_default().insert(new_node_id);
            } else {
                handles_b.entry(g_idx).or_default().insert(new_node_id);
            }

            edge_rpl(graph, last_node, edge_idx, new_edge_idx);
            edge_rpl(graph, new_node_id, edge_idx, new_edge_idx);

            last_node = new_node_id;
            last_frac = frac;
        }

        // Final segment
        let sub_geom = get_sub_geom(&orig_restr_edge.geom, last_frac, 1.0);
        let final_edge_idx = graph.add_edge(
            last_node,
            orig_restr_edge.to_node,
            sub_geom,
            orig_restr_edge.lines.clone(),
        );

        edge_rpl(graph, last_node, edge_idx, final_edge_idx);
        edge_rpl(graph, orig_restr_edge.to_node, edge_idx, final_edge_idx);

        graph.del_edge(edge_idx);
    }

    (handles_a, handles_b)
}

fn get_sub_geom(geom: &GeoLineString<f64>, start_frac: f64, end_frac: f64) -> GeoLineString<f64> {
    if geom.0.is_empty() {
        return geom.clone();
    }
    #[allow(deprecated)]
    let total_len = geom.haversine_length();
    let start_dist = total_len * start_frac;
    let end_dist = total_len * end_frac;

    let mut coords = Vec::new();
    let mut dist = 0.0;

    if start_frac <= 0.001 {
        coords.push(geom.0[0]);
    }

    for segment in geom.lines() {
        #[allow(deprecated)]
        let seg_len = segment.haversine_length();
        let current_end_dist = dist + seg_len;

        let overlap_start = dist.max(start_dist);
        let overlap_end = current_end_dist.min(end_dist);

        if overlap_start < overlap_end {
            let p1 = segment.start;
            let p2 = segment.end;

            if overlap_start > dist || coords.is_empty() {
                let ratio = if seg_len > 0.0 {
                    (overlap_start - dist) / seg_len
                } else {
                    0.0
                };
                let p = Coord {
                    x: p1.x + (p2.x - p1.x) * ratio,
                    y: p1.y + (p2.y - p1.y) * ratio,
                };
                coords.push(p);
            }

            let ratio = if seg_len > 0.0 {
                (overlap_end - dist) / seg_len
            } else {
                1.0
            };
            let p = Coord {
                x: p1.x + (p2.x - p1.x) * ratio,
                y: p1.y + (p2.y - p1.y) * ratio,
            };
            coords.push(p);
        }

        dist += seg_len;
        if dist >= end_dist {
            break;
        }
    }

    GeoLineString(coords)
}

// ============================================================================
// Legacy Shape-Projection Based Inference (kept for comparison)
// ============================================================================

/// Legacy: Infer restrictions using shape projection
/// This is the original simpler approach
#[allow(dead_code)]
pub fn infer_restrictions_legacy(
    edges: &[GraphEdge],
    route_shapes: &RouteShapeMap,
) -> Vec<TurnRestriction> {
    println!("Inferring turn restrictions (legacy)...");
    let mut restrictions = Vec::new();

    let mut adj_out: HashMap<NodeId, Vec<usize>> = HashMap::new();
    let mut adj_in: HashMap<NodeId, Vec<usize>> = HashMap::new();

    for (i, edge) in edges.iter().enumerate() {
        adj_out.entry(edge.from).or_default().push(i);
        adj_in.entry(edge.to).or_default().push(i);
    }

    let all_nodes: HashSet<NodeId> = adj_in.keys().chain(adj_out.keys()).cloned().collect();
    let mut restrictions_count = 0;

    for node in all_nodes {
        let incoming = adj_in.get(&node);
        let outgoing = adj_out.get(&node);

        if incoming.is_none() || outgoing.is_none() {
            continue;
        }

        let incoming_indices = incoming.unwrap();
        let outgoing_indices = outgoing.unwrap();

        for &u_idx in incoming_indices {
            for &v_idx in outgoing_indices {
                if u_idx == v_idx {
                    continue;
                }

                let u = &edges[u_idx];
                let v = &edges[v_idx];

                let u_routes: HashSet<&(String, String)> = u.route_ids.iter().collect();
                let v_routes: HashSet<&(String, String)> = v.route_ids.iter().collect();

                let shared_routes: Vec<&(String, String)> =
                    u_routes.intersection(&v_routes).cloned().collect();

                if shared_routes.is_empty() {
                    continue;
                }

                let u_geom = convert_to_geo(&u.geometry);
                let v_geom = convert_to_geo(&v.geometry);

                for route_key in shared_routes {
                    if let Some(shapes) = route_shapes.get(route_key) {
                        let mut valid_any = false;

                        for shape in shapes {
                            if is_consecutive(&u_geom, &v_geom, shape) {
                                valid_any = true;
                                break;
                            }
                        }

                        if !valid_any {
                            restrictions.push(TurnRestriction {
                                from_edge_index: u_idx,
                                to_edge_index: v_idx,
                                route_id: route_key.clone(),
                            });
                            restrictions_count += 1;
                        }
                    }
                }
            }
        }
    }

    println!(
        "Inferred {} turn restrictions (legacy).",
        restrictions_count
    );
    restrictions
}

fn is_consecutive(
    u_geom: &GeoLineString<f64>,
    v_geom: &GeoLineString<f64>,
    shape: &GeoLineString<f64>,
) -> bool {
    let range_u = get_projection_range(u_geom, shape);
    let range_v = get_projection_range(v_geom, shape);

    if range_u.is_none() || range_v.is_none() {
        return true;
    }

    let (_, max_u) = range_u.unwrap();
    let (min_v, _) = range_v.unwrap();

    #[allow(deprecated)]
    let shape_len = shape.haversine_length();

    let gap_frac = min_v - max_u;
    let gap_meters = gap_frac * shape_len;

    if gap_meters > MAX_GAP_METERS {
        return false;
    }

    if gap_meters < -MAX_GAP_METERS {
        return false;
    }

    true
}

fn get_projection_range(
    segment: &GeoLineString<f64>,
    shape: &GeoLineString<f64>,
) -> Option<(f64, f64)> {
    if segment.0.is_empty() || shape.0.is_empty() {
        return None;
    }

    let coords = &segment.0;
    let start_p = &coords[0];
    let end_p = coords.last().unwrap();

    let mut min_frac = 1.0;
    let mut max_frac = 0.0;

    for p in [start_p, end_p] {
        let p_geo = GeoPoint::from(*p);
        let frac = shape.line_locate_point(&p_geo).unwrap_or(0.0);
        if frac < min_frac {
            min_frac = frac;
        }
        if frac > max_frac {
            max_frac = frac;
        }
    }

    Some((min_frac, max_frac))
}
