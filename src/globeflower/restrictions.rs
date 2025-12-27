use crate::edges::{GraphEdge, NodeId, convert_to_geo};
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
    edges: Vec<RestrEdge>,
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
        self.edges.push(RestrEdge {
            from_node,
            to_node,
            geom,
            length,
            lines,
        });
        self.adj_out.entry(from_node).or_default().push(idx);
        self.adj_in.entry(to_node).or_default().push(idx);
        idx
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
        let to_e = &self.graph.edges[to_edge];

        // If edge doesn't contain the line we're routing for, infinite cost
        if !to_e.lines.contains(self.line_id) {
            return self.inf();
        }

        // Check if there's a start edge
        let Some(from_edge_idx) = from_edge else {
            // No from edge (start of path) - just return the edge length
            return to_e.length;
        };

        let from_e = &self.graph.edges[from_edge_idx];

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
fn dijkstra_shortest_path(
    from_edges: &[usize],
    to_edges: &HashSet<usize>,
    cost_func: &CostFunc,
) -> f64 {
    if from_edges.is_empty() || to_edges.is_empty() {
        return cost_func.inf();
    }

    let mut heap = BinaryHeap::new();
    // Key: (node_idx, last_edge_idx) -> best cost to reach this state
    let mut dist: HashMap<(usize, usize), f64> = HashMap::new();

    // Initialize with starting edges
    for &edge_idx in from_edges {
        let edge = &cost_func.graph.edges[edge_idx];
        let start_cost = cost_func.edge_cost(None, edge.from_node, edge_idx);
        if start_cost < cost_func.inf() {
            let to_node = edge.to_node;
            heap.push(DijkstraState {
                cost: OrderedFloat(start_cost),
                node: to_node,
                via_edge: edge_idx,
            });
            dist.insert((to_node, edge_idx), start_cost);
        }
    }

    let mut best_to_target = cost_func.inf();

    while let Some(DijkstraState {
        cost,
        node,
        via_edge,
    }) = heap.pop()
    {
        let cost_val = cost.0;

        // Early termination if we've exceeded the best we can do
        if cost_val >= best_to_target {
            continue;
        }

        // Check if we've reached a target edge
        if to_edges.contains(&via_edge) {
            if cost_val < best_to_target {
                best_to_target = cost_val;
            }
            continue; // Don't explore further from target
        }

        // Skip if we've found a better path to this state
        if let Some(&d) = dist.get(&(node, via_edge)) {
            if cost_val > d {
                continue;
            }
        }

        // Explore outgoing edges
        if let Some(out_edges) = cost_func.graph.adj_out.get(&node) {
            for &next_edge_idx in out_edges {
                let next_edge = &cost_func.graph.edges[next_edge_idx];
                let edge_cost = cost_func.edge_cost(Some(via_edge), node, next_edge_idx);

                if edge_cost >= cost_func.inf() {
                    continue;
                }

                let next_cost = cost_val + edge_cost;
                if next_cost >= cost_func.inf() || next_cost >= best_to_target {
                    continue;
                }

                let next_node = next_edge.to_node;
                let key = (next_node, next_edge_idx);

                if dist.get(&key).map_or(true, |&d| next_cost < d) {
                    dist.insert(key, next_cost);
                    heap.push(DijkstraState {
                        cost: OrderedFloat(next_cost),
                        node: next_node,
                        via_edge: next_edge_idx,
                    });
                }
            }
        }
    }

    best_to_target
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

    Ok(infer_restrictions_dijkstra(edges, &route_shapes))
}

/// Infer turn restrictions using Dijkstra-based path cost analysis
/// This matches the C++ RestrInferrer algorithm
pub fn infer_restrictions_dijkstra(
    edges: &[GraphEdge],
    route_shapes: &RouteShapeMap,
) -> Vec<TurnRestriction> {
    println!("Inferring turn restrictions (Dijkstra)...");

    // Build restriction graph from edges
    let restr_graph = build_restriction_graph(edges, route_shapes);

    let mut restrictions = Vec::new();

    // Build adjacency for the original edges
    let mut adj_out: HashMap<NodeId, Vec<usize>> = HashMap::new();
    let mut adj_in: HashMap<NodeId, Vec<usize>> = HashMap::new();

    for (i, edge) in edges.iter().enumerate() {
        adj_out.entry(edge.from).or_default().push(i);
        adj_in.entry(edge.to).or_default().push(i);
    }

    let all_nodes: HashSet<NodeId> = adj_in.keys().chain(adj_out.keys()).cloned().collect();

    let mut restrictions_count = 0;

    for node in all_nodes {
        let Some(incoming_indices) = adj_in.get(&node) else {
            continue;
        };
        let Some(outgoing_indices) = adj_out.get(&node) else {
            continue;
        };

        for &u_idx in incoming_indices {
            for &v_idx in outgoing_indices {
                if u_idx == v_idx {
                    continue;
                }

                let u = &edges[u_idx];
                let v = &edges[v_idx];

                // Find shared routes
                let u_routes: HashSet<&(String, String)> = u.route_ids.iter().collect();
                let v_routes: HashSet<&(String, String)> = v.route_ids.iter().collect();

                let shared_routes: Vec<&(String, String)> =
                    u_routes.intersection(&v_routes).cloned().collect();

                if shared_routes.is_empty() {
                    continue;
                }

                // For each shared route, check validity using Dijkstra
                for route_key in shared_routes {
                    let line_id = format!("{}_{}", route_key.0, route_key.1);

                    // Check both directions (u -> v and v -> u)
                    let valid = check_path_exists(
                        &restr_graph,
                        &line_id,
                        u_idx,
                        v_idx,
                        &u.geometry,
                        &v.geometry,
                    ) || check_path_exists(
                        &restr_graph,
                        &line_id,
                        v_idx,
                        u_idx,
                        &v.geometry,
                        &u.geometry,
                    );

                    if !valid {
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

    println!(
        "Inferred {} turn restrictions (Dijkstra).",
        restrictions_count
    );
    restrictions
}

/// Build a restriction graph for Dijkstra-based checking
fn build_restriction_graph(edges: &[GraphEdge], route_shapes: &RouteShapeMap) -> RestrGraph {
    let mut graph = RestrGraph::new();

    // Map from NodeId to node index in restriction graph
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

        // Create line set for this edge
        let lines: HashSet<String> = edge
            .route_ids
            .iter()
            .map(|(chateau, rid)| format!("{}_{}", chateau, rid))
            .collect();

        // Add forward edge
        graph.add_edge(from_node, to_node, geom.clone(), lines.clone());

        // Add reverse edge
        let mut rev_geom = geom.clone();
        rev_geom.0.reverse();
        graph.add_edge(to_node, from_node, rev_geom, lines);
    }

    graph
}

/// Check if a valid path exists from edge `from_idx` to edge `to_idx` for the given line
fn check_path_exists(
    graph: &RestrGraph,
    line_id: &str,
    _from_idx: usize,
    _to_idx: usize,
    from_geom: &postgis_diesel::types::LineString<postgis_diesel::types::Point>,
    to_geom: &postgis_diesel::types::LineString<postgis_diesel::types::Point>,
) -> bool {
    let from_geo = convert_to_geo(from_geom);
    let to_geo = convert_to_geo(to_geom);

    // Calculate the expected path length
    #[allow(deprecated)]
    let expected_len = from_geo.haversine_length() * 0.33 + to_geo.haversine_length() * 0.33;

    // Max allowed cost = expected_len + MAX_LENGTH_DEV
    let max_cost = expected_len + MAX_LENGTH_DEV + 0.1; // Small epsilon

    let cost_func = CostFunc::new(line_id, max_cost, graph);

    // Find edges in the restriction graph that correspond to from_idx and to_idx
    // For this simplified version, we use the edge positions to find matching edges
    let from_edges = find_edges_near_geometry(graph, &from_geo, line_id);
    let to_edges: HashSet<usize> = find_edges_near_geometry(graph, &to_geo, line_id)
        .into_iter()
        .collect();

    if from_edges.is_empty() || to_edges.is_empty() {
        // If we can't find matching edges, assume valid to avoid false positives
        return true;
    }

    let path_cost = dijkstra_shortest_path(&from_edges, &to_edges, &cost_func);

    // Valid if path cost is within allowed deviation
    path_cost - expected_len < MAX_LENGTH_DEV
}

/// Find edges in the restriction graph that are near the given geometry and contain the line
fn find_edges_near_geometry(
    graph: &RestrGraph,
    geom: &GeoLineString<f64>,
    line_id: &str,
) -> Vec<usize> {
    let mut result = Vec::new();

    if geom.0.is_empty() {
        return result;
    }

    let start_opt = geom.0.get(0);
    let end_opt = geom.0.get(geom.0.len().saturating_sub(1));

    let (Some(start), Some(end)) = (start_opt, end_opt) else {
        return result;
    };

    for (idx, edge) in graph.edges.iter().enumerate() {
        if !edge.lines.contains(line_id) {
            continue;
        }

        // Check if edge geometry overlaps with the given geometry
        if edge.geom.0.is_empty() {
            continue;
        }

        let edge_start_opt = edge.geom.0.get(0);
        let edge_end_opt = edge.geom.0.get(edge.geom.0.len().saturating_sub(1));

        let (Some(edge_start), Some(edge_end)) = (edge_start_opt, edge_end_opt) else {
            continue;
        };

        // Simple proximity check (within ~50m)
        let threshold = 0.0005; // ~50m in degrees

        // Check if start/end points are close
        let start_close = (start.x - edge_start.x).abs() < threshold
            && (start.y - edge_start.y).abs() < threshold;
        let end_close =
            (end.x - edge_end.x).abs() < threshold && (end.y - edge_end.y).abs() < threshold;

        if start_close || end_close {
            result.push(idx);
        }
    }

    result
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
