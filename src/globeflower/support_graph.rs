use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rstar::{AABB, RTree, RTreeObject};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

// ===========================================================================
// Rc-based Line Graph (matches C++ UndirGraph<LineNodePL, LineEdgePL>)
// ===========================================================================

type NodeRef = Rc<RefCell<LineNode>>;
type EdgeRef = Rc<RefCell<LineEdge>>;

#[derive(Debug)]
struct LineNode {
    id: usize,
    pos: [f64; 2],
    adj_list: Vec<EdgeRef>,
}

impl LineNode {
    fn new(id: usize, pos: [f64; 2]) -> Self {
        Self {
            id,
            pos,
            adj_list: Vec::new(),
        }
    }

    fn get_deg(&self) -> usize {
        self.adj_list.len()
    }
}

#[derive(Debug)]
struct LineEdge {
    from: NodeRef,
    to: NodeRef,
    routes: HashSet<(String, String)>,
    geometry: Vec<Coord>,
}

impl LineEdge {
    fn get_other_nd(&self, n: &NodeRef) -> NodeRef {
        if Rc::ptr_eq(&self.from, n) {
            Rc::clone(&self.to)
        } else {
            Rc::clone(&self.from)
        }
    }
}

struct LineGraph {
    nodes: Vec<NodeRef>,
    next_node_id: usize,
}

impl LineGraph {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            next_node_id: 0,
        }
    }

    fn add_nd(&mut self, pos: [f64; 2]) -> NodeRef {
        let id = self.next_node_id;
        self.next_node_id += 1;
        let node = Rc::new(RefCell::new(LineNode::new(id, pos)));
        self.nodes.push(Rc::clone(&node));
        node
    }

    /// Get edge between two nodes, if it exists
    fn get_edg(&self, from: &NodeRef, to: &NodeRef) -> Option<EdgeRef> {
        let from_borrow = from.borrow();
        for edge in &from_borrow.adj_list {
            let edge_borrow = edge.borrow();
            if Rc::ptr_eq(&edge_borrow.to, to) || Rc::ptr_eq(&edge_borrow.from, to) {
                return Some(Rc::clone(edge));
            }
        }
        None
    }

    /// Add edge between two nodes
    fn add_edg(&mut self, from: &NodeRef, to: &NodeRef) -> EdgeRef {
        let edge = Rc::new(RefCell::new(LineEdge {
            from: Rc::clone(from),
            to: Rc::clone(to),
            routes: HashSet::new(),
            geometry: vec![
                Coord {
                    x: from.borrow().pos[0],
                    y: from.borrow().pos[1],
                },
                Coord {
                    x: to.borrow().pos[0],
                    y: to.borrow().pos[1],
                },
            ],
        }));
        from.borrow_mut().adj_list.push(Rc::clone(&edge));
        to.borrow_mut().adj_list.push(Rc::clone(&edge));
        edge
    }

    fn get_nds(&self) -> &[NodeRef] {
        &self.nodes
    }

    fn num_edges(&self) -> usize {
        let mut count = 0;
        for node in &self.nodes {
            let node_borrow = node.borrow();
            for edge in &node_borrow.adj_list {
                // Count each edge once (when from == node)
                if Rc::ptr_eq(&edge.borrow().from, node) {
                    count += 1;
                }
            }
        }
        count
    }
    fn clear(&mut self) {
        // Break cycles manually
        for node in &self.nodes {
            node.borrow_mut().adj_list.clear();
        }
        self.nodes.clear();
    }
}

// ===========================================================================
// Spatial Index for Nodes (matches C++ NodeGeoIdx)
// ===========================================================================

struct NodeGeoIdx {
    cell_size: f64,
    cells: HashMap<(i32, i32), Vec<NodeRef>>,
}

impl NodeGeoIdx {
    fn new(cell_size: f64) -> Self {
        Self {
            cell_size,
            cells: HashMap::new(),
        }
    }

    fn get_cell_coords(&self, x: f64, y: f64) -> (i32, i32) {
        (
            (x / self.cell_size).floor() as i32,
            (y / self.cell_size).floor() as i32,
        )
    }

    fn add(&mut self, pos: [f64; 2], node: NodeRef) {
        let coords = self.get_cell_coords(pos[0], pos[1]);
        self.cells.entry(coords).or_default().push(node);
    }

    fn remove(&mut self, node: &NodeRef) {
        let pos = node.borrow().pos;
        let coords = self.get_cell_coords(pos[0], pos[1]);
        if let Some(nodes) = self.cells.get_mut(&coords) {
            nodes.retain(|n| !Rc::ptr_eq(n, node));
        }
    }

    /// Get nodes within radius of point
    fn get(&self, pos: [f64; 2], radius: f64) -> Vec<NodeRef> {
        let mut result = Vec::new();
        let r_sq = radius * radius;
        let min_c = self.get_cell_coords(pos[0] - radius, pos[1] - radius);
        let max_c = self.get_cell_coords(pos[0] + radius, pos[1] + radius);

        for cx in min_c.0..=max_c.0 {
            for cy in min_c.1..=max_c.1 {
                if let Some(nodes) = self.cells.get(&(cx, cy)) {
                    for node in nodes {
                        let node_pos = node.borrow().pos;
                        let dx = node_pos[0] - pos[0];
                        let dy = node_pos[1] - pos[1];
                        if dx * dx + dy * dy <= r_sq {
                            result.push(Rc::clone(node));
                        }
                    }
                }
            }
        }
        result
    }
}

// ===========================================================================
// Build Support Graph (public API)
// ===========================================================================

/// Build the Support Graph from raw Shapes (Thesis Section 3.2).
pub async fn build_support_graph(pool: &CatenaryPostgresPool) -> Result<Vec<GraphEdge>> {
    let mut conn = pool.get().await?;

    use catenary::schema::gtfs::shapes::dsl::*;

    let loaded_shapes = shapes
        .filter(route_type.eq_any(vec![0, 1, 2])) // Tram, Subway, Rail
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    println!(
        "Loaded {} raw shapes for Support Graph.",
        loaded_shapes.len()
    );

    // Convert shapes to initial edges
    let mut raw_edges = Vec::new();
    let mut node_id_counter = 0;

    for shape in loaded_shapes {
        let geom = convert_to_geo(&shape.linestring);
        if geom.0.is_empty() {
            continue;
        }

        let start_node = NodeId::Intersection(node_id_counter);
        node_id_counter += 1;
        let end_node = NodeId::Intersection(node_id_counter);
        node_id_counter += 1;

        let route_ids = vec![(String::from("shape"), shape.shape_id)];
        #[allow(deprecated)]
        let weight = geom.haversine_length();

        raw_edges.push(GraphEdge {
            from: start_node,
            to: end_node,
            geometry: convert_from_geo(&geom),
            route_ids,
            weight,
            original_edge_index: None,
        });
    }

    // Collapse shared segments using C++-style algorithm
    // Parameters: d_cut=~55m, seg_len=~55m (same as d_cut for efficiency), max_iters=7
    let collapsed = collapse_shared_segments(raw_edges, 0.0005, 0.0005, 7, &mut node_id_counter);

    Ok(collapsed)
}

// ===========================================================================
// Collapse Shared Segments (C++ Port of MapConstructor::collapseShrdSegs)
// ===========================================================================

fn collapse_shared_segments(
    mut edges: Vec<GraphEdge>,
    d_cut: f64,
    seg_len: f64,
    max_iters: usize,
    next_node_id_counter: &mut usize,
) -> Vec<GraphEdge> {
    use std::cmp::Ordering;

    println!("Building Support Graph via iterative collapse (Topological Mode)...");

    for iter in 0..max_iters {
        println!("Iteration {}/{}...", iter + 1, max_iters);

        // Create new graph for this iteration (matches C++: `LineGraph tgNew`)
        let mut tg_new = LineGraph::new();

        // Spatial index for node collapse candidates
        let mut geo_idx = NodeGeoIdx::new(d_cut);

        // Map from old node IDs to new nodes (matches C++: `imgNds`)
        let mut img_nds: HashMap<NodeId, NodeRef> = HashMap::new();

        // Sort edges by length (longest first) - matches C++ sorting
        edges.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(Ordering::Equal));

        for edge in &edges {
            let mut last: Option<NodeRef> = None;
            let mut my_nds: HashSet<usize> = HashSet::new(); // Blocking set (by node id)

            // Build polyline including endpoints
            let geom = convert_to_geo(&edge.geometry);

            // Simplify then densify (matches C++: simplify(pl, 0.5), densify(..., SEGL))
            use geo::algorithm::simplify::Simplify;
            let simplified = geom.simplify(0.5e-5); // ~0.5 meters in degrees
            let coords: Vec<[f64; 2]> = if simplified.0.is_empty() {
                geom.0.iter().map(|c| [c.x, c.y]).collect()
            } else {
                simplified.0.iter().map(|c| [c.x, c.y]).collect()
            };

            // Densify
            let pl_dense = densify_coords(&coords, seg_len);

            let num_lines = edge.route_ids.len();
            let mut i = 0;
            let pl_len = pl_dense.len();
            let mut front_pos: Option<[f64; 2]> = None;
            let mut front: Option<NodeRef> = None; // For FROM coverage handling
            let mut img_from_covered = false;
            let mut img_to_covered = false;
            
            // C++ line 232: back = e->getTo() - the original TO endpoint position
            // This is used for span constraint to prevent shortcuts
            let to_endpoint_pos = *pl_dense.last().unwrap_or(&[0.0, 0.0]);

            for point in &pl_dense {
                // For span constraints: use front_pos (first node of this path)
                // and to_endpoint_pos (original TO endpoint, cleared at last point)
                // This matches C++: front and back are passed to ndCollapseCand
                let span_a_pos = front_pos;
                let span_b_pos = if i == pl_len - 1 { None } else { Some(to_endpoint_pos) };
                
                // Find or create node with span constraints (matches C++: ndCollapseCand)
                let cur = nd_collapse_cand(
                    &my_nds,
                    num_lines,
                    d_cut,
                    *point,
                    span_a_pos,
                    span_b_pos,
                    &mut geo_idx,
                    &mut tg_new,
                );

                // Track image nodes for FROM/TO
                if i == 0 {
                    if !img_nds.contains_key(&edge.from) {
                        img_nds.insert(edge.from.clone(), Rc::clone(&cur));
                        img_from_covered = true;
                    }
                }
                if i == pl_len - 1 {
                    if !img_nds.contains_key(&edge.to) {
                        img_nds.insert(edge.to.clone(), Rc::clone(&cur));
                        img_to_covered = true;
                    }
                }

                let cur_id = cur.borrow().id;
                my_nds.insert(cur_id);
                i += 1;

                // Skip self-edges (matches C++: `if (last == cur) continue`)
                if let Some(ref last_node) = last {
                    if Rc::ptr_eq(last_node, &cur) {
                        continue;
                    }
                }

                // Check if we covered FROM/TO
                if let Some(from_node) = img_nds.get(&edge.from) {
                    if Rc::ptr_eq(from_node, &cur) {
                        img_from_covered = true;
                    }
                }
                if let Some(to_node) = img_nds.get(&edge.to) {
                    if Rc::ptr_eq(to_node, &cur) {
                        img_to_covered = true;
                    }
                }

                // Create edge if we have a previous node
                if let Some(ref last_node) = last {
                    // Check for existing edge (matches C++: `getEdg(last, cur)`)
                    let new_e = if let Some(existing) = tg_new.get_edg(last_node, &cur) {
                        existing
                    } else {
                        tg_new.add_edg(last_node, &cur)
                    };

                    // Merge route info
                    for r in &edge.route_ids {
                        new_e.borrow_mut().routes.insert(r.clone());
                    }
                }

                // Track front position and node (C++: if (!front) front = cur)
                if front_pos.is_none() {
                    front_pos = Some(cur.borrow().pos);
                    front = Some(Rc::clone(&cur));
                }
                last = Some(cur);

                // Early termination if we reached TO
                if let Some(to_node) = img_nds.get(&edge.to) {
                    if let Some(ref last_node) = last {
                        if Rc::ptr_eq(last_node, to_node) {
                            break;
                        }
                    }
                }
            }

            // Handle case where FROM wasn't covered by the dense path
            if !img_from_covered {
                if let (Some(from_node), Some(front_node)) = (img_nds.get(&edge.from), &front) {
                    if !Rc::ptr_eq(from_node, front_node) {
                        let new_e = if let Some(existing) = tg_new.get_edg(from_node, front_node) {
                            existing
                        } else {
                            tg_new.add_edg(from_node, front_node)
                        };
                        for r in &edge.route_ids {
                            new_e.borrow_mut().routes.insert(r.clone());
                        }
                    }
                }
            }

            // Handle case where TO wasn't covered by the dense path
            if !img_to_covered {
                if let (Some(to_node), Some(last_node)) = (img_nds.get(&edge.to), &last) {
                    if !Rc::ptr_eq(to_node, last_node) {
                        let new_e = if let Some(existing) = tg_new.get_edg(last_node, to_node) {
                            existing
                        } else {
                            tg_new.add_edg(last_node, to_node)
                        };
                        for r in &edge.route_ids {
                            new_e.borrow_mut().routes.insert(r.clone());
                        }
                    }
                }
            }
        }

        let num_edges = tg_new.num_edges();
        println!("  Graph built with {} segments. Simplifying...", num_edges);

        // Soft cleanup (C++: lines 367-377) - combine nearby junctions before finalizing geometry
        soft_cleanup(&mut tg_new, seg_len);

        // =====================================================================
        // CRITICAL: Write edge geoms from node positions (C++: lines 379-387)
        // This is the key step - edges just connect nodes, geometry is straight line
        // =====================================================================
        for node in tg_new.get_nds() {
            let adj_list = node.borrow().adj_list.clone();
            for edge_ref in adj_list {
                let is_from = Rc::ptr_eq(&edge_ref.borrow().from, node);
                if !is_from {
                    continue;
                }
                
                let from_pos = edge_ref.borrow().from.borrow().pos;
                let to_pos = edge_ref.borrow().to.borrow().pos;
                
                edge_ref.borrow_mut().geometry = vec![
                    Coord { x: from_pos[0], y: from_pos[1] },
                    Coord { x: to_pos[0], y: to_pos[1] },
                ];
            }
        }

        // Contract degree-2 nodes (matches C++ re-collapse phase)
        collapse_degree_2_nodes_serial(&mut tg_new);

        // Apply polish fixes: fold parallel edges, edge artifact removal, intersection reconstruction
        apply_polish_fixes(&mut tg_new, d_cut);

        println!("  Simplification complete.");

        // Convert back to Vec<GraphEdge>
        let mut new_edges = Vec::new();
        let mut len_new = 0.0;

        for node in tg_new.get_nds() {
            let node_borrow = node.borrow();
            for edge in &node_borrow.adj_list {
                let edge_borrow = edge.borrow();
                // Only process edges where this node is "from"
                if !Rc::ptr_eq(&edge_borrow.from, node) {
                    continue;
                }

                // Use the stored geometry from degree-2 contractions, not just endpoints
                let ls = GeoLineString::new(edge_borrow.geometry.clone());
                #[allow(deprecated)]
                let weight = ls.haversine_length();

                let from_id = edge_borrow.from.borrow().id;
                let to_id = edge_borrow.to.borrow().id;

                new_edges.push(GraphEdge {
                    from: NodeId::Intersection(from_id),
                    to: NodeId::Intersection(to_id),
                    geometry: convert_from_geo(&ls),
                    route_ids: edge_borrow.routes.iter().cloned().collect(),
                    weight,
                    original_edge_index: None,
                });
                len_new += weight;
            }
        }

        // Convergence check - use RELATIVE threshold like C++ (0.2%)
        let len_old: f64 = edges.iter().map(|e| e.weight).sum();
        let relative_diff = if len_old > 0.0 {
            (1.0 - len_new / len_old).abs()
        } else {
            0.0
        };
        println!(
            "  Iteration {} complete. Relative delta: {:.6} (threshold: 0.002)",
            iter + 1,
            relative_diff
        );

        *next_node_id_counter = tg_new.next_node_id;
        edges = new_edges;

        // CRITICAL: Break Reference Cycles
        // The LineGraph uses Rc<RefCell<...>> which creates cycles (Node <-> Edge).
        // If we don't clear it manually, the graph from this iteration will leak
        // and stay in memory forever, eventually causing OOM.
        tg_new.clear();


        if relative_diff < 0.002 {
            println!("Converged.");
            break;
        }
    }

    edges
}

/// Find or create a node for collapsing (matches C++: ndCollapseCand)
/// 
/// Parameters:
/// - blocking_set: nodes on the current edge that should not be reused
/// - num_lines: number of routes on the current edge (used for line-aware distance)  
/// - d_cut: base distance cutoff for merging
/// - point: the point to find/create a node for
/// - span_a_pos: optional position of first endpoint (front of current path)
/// - span_b_pos: optional position of second endpoint (original TO endpoint)
/// - geo_idx: spatial index for nodes
/// - graph: the line graph being built
fn nd_collapse_cand(
    blocking_set: &HashSet<usize>,
    num_lines: usize,
    d_cut: f64,
    point: [f64; 2],
    span_a_pos: Option<[f64; 2]>,
    span_b_pos: Option<[f64; 2]>,
    geo_idx: &mut NodeGeoIdx,
    graph: &mut LineGraph,
) -> NodeRef {
    // Use line-aware distance cutoff
    let effective_d_cut = max_d(num_lines, d_cut);
    let neighbors = geo_idx.get(point, effective_d_cut);

    let mut best: Option<NodeRef> = None;
    let mut best_dist = f64::INFINITY;

    // C++ lines 90-96: Calculate distance constraints from span endpoints
    // A candidate can only be accepted if it's closer to the point than the 
    // span endpoints are (divided by sqrt(2) for geometric tolerance)
    let d_span_a = if let Some(sa_pos) = span_a_pos {
        let dx = point[0] - sa_pos[0];
        let dy = point[1] - sa_pos[1];
        (dx * dx + dy * dy).sqrt() / std::f64::consts::SQRT_2
    } else {
        f64::INFINITY
    };
    
    let d_span_b = if let Some(sb_pos) = span_b_pos {
        let dx = point[0] - sb_pos[0];
        let dy = point[1] - sb_pos[1];
        (dx * dx + dy * dy).sqrt() / std::f64::consts::SQRT_2
    } else {
        f64::INFINITY
    };

    for nd_test in neighbors {
        let nd_id = nd_test.borrow().id;
        
        // Skip nodes in blocking set (already on this edge's path)
        if blocking_set.contains(&nd_id) {
            continue;
        }
        
        // Skip degree-0 nodes
        if nd_test.borrow().get_deg() == 0 {
            continue;
        }

        let nd_pos = nd_test.borrow().pos;
        let dx = nd_pos[0] - point[0];
        let dy = nd_pos[1] - point[1];
        let d = (dx * dx + dy * dy).sqrt();

        // C++ line 104: d < dSpanA && d < dSpanB && d < dMax && d < dBest
        // The candidate must be closer than the span endpoints (prevents shortcuts)
        if d < d_span_a && d < d_span_b && d < effective_d_cut && d < best_dist {
            best_dist = d;
            best = Some(nd_test);
        }
    }

    if let Some(nd_min) = best {
        // Update node position to centroid
        let old_pos = nd_min.borrow().pos;
        let new_pos = [(old_pos[0] + point[0]) / 2.0, (old_pos[1] + point[1]) / 2.0];

        geo_idx.remove(&nd_min);
        nd_min.borrow_mut().pos = new_pos;
        geo_idx.add(new_pos, Rc::clone(&nd_min));

        nd_min
    } else {
        // Create new node
        let new_node = graph.add_nd(point);
        geo_idx.add(point, Rc::clone(&new_node));
        new_node
    }
}

/// Contract degree-2 nodes with matching routes (matches C++ re-collapse phase)
/// This is a single-pass implementation to avoid O(N^2) behavior on large graphs.
fn collapse_degree_2_nodes_serial(graph: &mut LineGraph) {
    let mut total_contracted = 0;

    // Collect all nodes to iterate over
    // We strictly iterate over the initial set of nodes once
    // We hold Rc references directly to avoid O(N) lookup by ID.
    let nodes: Vec<NodeRef> = graph.nodes.iter().map(Rc::clone).collect();

    for node in nodes {
        // Check if node is degree 2
        // If it was "removed" (adj list cleared) in a previous iteration, degree will be 0
        if node.borrow().get_deg() != 2 {
            continue;
        }

        let adj = node.borrow().adj_list.clone();
        if adj.len() != 2 {
            continue;
        }

        let edge_a = &adj[0];
        let edge_b = &adj[1];

        // Ensure routes match (C++ lineEq)
        if !routes_equal(&edge_a.borrow().routes, &edge_b.borrow().routes) {
            continue;
        }

        let other_a = edge_a.borrow().get_other_nd(&node);
        let other_b = edge_b.borrow().get_other_nd(&node);

        // Check for existing edge between the other two nodes
        // If one exists, we might need to handle it carefully or skip
        // C++ logic: if (ex) { ... if long enough support ... else dont contract }
        // For simplicity, we skip contraction if an edge already exists to prevent
        // complicating the graph topology or creating multi-edges where not supported
        if graph.get_edg(&other_a, &other_b).is_some() {
            continue;
        }

        // C++: MAX_COLLAPSED_SEG_LENGTH check (500m ~ 0.0045 degrees)
        // Don't contract if the resulting edge would be too long
        // Calculate distance between the two endpoints that would be connected
        {
            let pos_a = other_a.borrow().pos;
            let pos_b = other_b.borrow().pos;
            let dx = pos_a[0] - pos_b[0];
            let dy = pos_a[1] - pos_b[1];
            let dist = (dx * dx + dy * dy).sqrt();
            if dist > MAX_COLLAPSED_SEG_LENGTH {
                continue;
            }
        }

        // Combine edges: create new edge from other_a to other_b
        let merged_routes: HashSet<(String, String)> = edge_a
            .borrow()
            .routes
            .union(&edge_b.borrow().routes)
            .cloned()
            .collect();

        // Build combined geometry
        let new_geom;
        {
            let ea = edge_a.borrow();
            let eb = edge_b.borrow();

            // Determine orientation and combine geometries
            // Geometry goes from other_a through node to other_b
            let geom_a = if Rc::ptr_eq(&ea.to, &node) {
                ea.geometry.clone()
            } else {
                let mut g = ea.geometry.clone();
                g.reverse();
                g
            };

            let geom_b = if Rc::ptr_eq(&eb.from, &node) {
                eb.geometry.clone()
            } else {
                let mut g = eb.geometry.clone();
                g.reverse();
                g
            };

            // Combine: geom_a ends at node, geom_b starts at node
            let mut combined = geom_a;
            if !combined.is_empty() && !geom_b.is_empty() {
                combined.pop(); // Remove duplicate node point
            }
            combined.extend(geom_b);
            new_geom = combined;
        }

        // Create new edge
        let new_edge = Rc::new(RefCell::new(LineEdge {
            from: Rc::clone(&other_a),
            to: Rc::clone(&other_b),
            routes: merged_routes,
            geometry: new_geom,
        }));

        // Update adjacency lists
        other_a
            .borrow_mut()
            .adj_list
            .retain(|e| !Rc::ptr_eq(e, edge_a));
        other_b
            .borrow_mut()
            .adj_list
            .retain(|e| !Rc::ptr_eq(e, edge_b));

        // Clear the middle node's adj list (effectively disconnecting it)
        node.borrow_mut().adj_list.clear();

        // Add new edge
        other_a.borrow_mut().adj_list.push(Rc::clone(&new_edge));
        other_b.borrow_mut().adj_list.push(Rc::clone(&new_edge));

        total_contracted += 1;
    }

    // Single cleanup pass at the end
    if total_contracted > 0 {
        println!("  Contracted {} degree-2 nodes.", total_contracted);
        graph.nodes.retain(|n| n.borrow().get_deg() > 0);
    }
}

/// Check if two route sets are equal (matches C++ lineEq simplified)
fn routes_equal(a: &HashSet<(String, String)>, b: &HashSet<(String, String)>) -> bool {
    a == b
}

/// Check if two route sets have any overlap (C++: used before merging)
fn routes_overlap(a: &HashSet<(String, String)>, b: &HashSet<(String, String)>) -> bool {
    !a.is_disjoint(b)
}

/// Line-aware distance cutoff (C++: maxD)
/// NOTE: In C++ this logic is commented out and just returns d.
fn max_d(_num_lines: usize, d_cut: f64) -> f64 {
    d_cut
}

/// Maximum length for a collapsed segment (500m in degrees, ~0.0045)
/// Prevents degree-2 contraction from creating excessively long edges
const MAX_COLLAPSED_SEG_LENGTH: f64 = 0.0045;

/// Densify coordinates to have points no further than max_seg_len apart
fn densify_coords(coords: &[[f64; 2]], max_seg_len: f64) -> Vec<[f64; 2]> {
    if coords.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(coords.len() * 2);
    result.push(coords[0]);

    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let dx = p2[0] - p1[0];
        let dy = p2[1] - p1[1];
        let dist = (dx * dx + dy * dy).sqrt();

        if dist > max_seg_len {
            let num_segments = (dist / max_seg_len).ceil() as usize;
            for j in 1..num_segments {
                let frac = j as f64 / num_segments as f64;
                result.push([p1[0] + dx * frac, p1[1] + dy * frac]);
            }
        }
        result.push(p2);
    }

    result
}

fn haversine_dist(p1: GeoPoint<f64>, p2: GeoPoint<f64>) -> f64 {
    use geo::HaversineDistance;
    p1.haversine_distance(&p2)
}

// ===========================================================================
// Polish Fixes: geomAvg, fold_edges, intersection reconstruction
// ===========================================================================

/// Weighted geometry averaging (C++: geomAvg)
/// Averages two polylines with weights based on number of lines (squared)
fn geom_avg(
    geom_a: &[Coord],
    weight_a: usize,
    geom_b: &[Coord],
    weight_b: usize,
) -> Vec<Coord> {
    if geom_a.is_empty() {
        return geom_b.to_vec();
    }
    if geom_b.is_empty() {
        return geom_a.to_vec();
    }

    // Weights are squared (as in C++)
    let wa = (weight_a * weight_a) as f64;
    let wb = (weight_b * weight_b) as f64;
    let total_weight = wa + wb;

    if total_weight == 0.0 {
        return geom_a.to_vec();
    }

    // Resample both geometries to same number of points
    let num_points = geom_a.len().max(geom_b.len()).max(5);
    let resampled_a = resample_polyline(geom_a, num_points);
    let resampled_b = resample_polyline(geom_b, num_points);

    // Weighted average of corresponding points
    let mut result = Vec::with_capacity(num_points);
    for i in 0..num_points {
        let pa = resampled_a[i];
        let pb = resampled_b[i];
        result.push(Coord {
            x: (pa.x * wa + pb.x * wb) / total_weight,
            y: (pa.y * wa + pb.y * wb) / total_weight,
        });
    }

    // Simplify the result
    simplify_coords(&result, 0.5e-6)
}

/// Resample a polyline to have exactly n points
fn resample_polyline(coords: &[Coord], n: usize) -> Vec<Coord> {
    if coords.len() < 2 || n < 2 {
        return coords.to_vec();
    }

    // Calculate total length
    let mut total_len = 0.0;
    for i in 0..coords.len() - 1 {
        let dx = coords[i + 1].x - coords[i].x;
        let dy = coords[i + 1].y - coords[i].y;
        total_len += (dx * dx + dy * dy).sqrt();
    }

    if total_len == 0.0 {
        return vec![coords[0]; n];
    }

    let mut result = Vec::with_capacity(n);
    result.push(coords[0]);

    let step = total_len / (n - 1) as f64;
    let mut cur_len = 0.0;
    let mut seg_idx = 0;
    let mut seg_start_len = 0.0;

    for i in 1..n - 1 {
        let target_len = step * i as f64;

        // Advance through segments until we find the one containing target_len
        while seg_idx < coords.len() - 1 {
            let dx = coords[seg_idx + 1].x - coords[seg_idx].x;
            let dy = coords[seg_idx + 1].y - coords[seg_idx].y;
            let seg_len = (dx * dx + dy * dy).sqrt();

            if seg_start_len + seg_len >= target_len {
                // Found the segment
                let frac = (target_len - seg_start_len) / seg_len;
                result.push(Coord {
                    x: coords[seg_idx].x + dx * frac,
                    y: coords[seg_idx].y + dy * frac,
                });
                break;
            }

            seg_start_len += seg_len;
            seg_idx += 1;
        }

        // If we ran out of segments, use last point
        if seg_idx >= coords.len() - 1 {
            result.push(*coords.last().unwrap());
        }
    }

    result.push(*coords.last().unwrap());
    result
}

/// Simplify coordinates using Douglas-Peucker
fn simplify_coords(coords: &[Coord], epsilon: f64) -> Vec<Coord> {
    if coords.len() <= 2 {
        return coords.to_vec();
    }
    let ls = GeoLineString::new(coords.to_vec());
    use geo::algorithm::simplify::Simplify;
    ls.simplify(epsilon).0
}

/// Average node positions based on incident edge endpoints (C++: averageNodePositions)
fn average_node_positions(graph: &LineGraph) {
    for node in &graph.nodes {
        let deg = node.borrow().get_deg();
        if deg == 0 {
            continue;
        }

        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut count = 0;

        let adj_list = node.borrow().adj_list.clone();
        for edge_ref in adj_list {
            let edge = edge_ref.borrow();
            let geom = &edge.geometry;
            if geom.is_empty() {
                continue;
            }

            // If this node is the "to" end, use the back of geometry
            // If this node is the "from" end, use the front of geometry
            let pt = if Rc::ptr_eq(&edge.to, node) {
                &geom[geom.len() - 1]
            } else {
                &geom[0]
            };
            sum_x += pt.x;
            sum_y += pt.y;
            count += 1;
        }

        if count > 0 {
            node.borrow_mut().pos = [sum_x / count as f64, sum_y / count as f64];
        }
    }
}

/// Reconstruct intersections by cutting back edge geometries (C++: reconstructIntersections)
/// Cuts back edge geometry by max_aggr_distance from endpoints, then reconnects to node positions
fn reconstruct_intersections(graph: &LineGraph, max_aggr_distance: f64) {
    // First average node positions
    average_node_positions(graph);

    // Then cut back edge geometries
    for node in &graph.nodes {
        let adj_list = node.borrow().adj_list.clone();
        for edge_ref in adj_list {
            let edge_from_ptr = {
                let edge = edge_ref.borrow();
                Rc::clone(&edge.from)
            };

            // Only process edges where this node is 'from'
            if !Rc::ptr_eq(&edge_from_ptr, node) {
                continue;
            }

            // Get geometry and cut it back
            let mut edge = edge_ref.borrow_mut();
            let geom = &edge.geometry;
            if geom.len() < 2 {
                continue;
            }

            // Calculate total length
            let total_len = calc_polyline_length(geom);
            if total_len <= 2.0 * max_aggr_distance {
                // Edge too short, just connect endpoints directly
                let from_pos = edge.from.borrow().pos;
                let to_pos = edge.to.borrow().pos;
                edge.geometry = vec![
                    Coord { x: from_pos[0], y: from_pos[1] },
                    Coord { x: to_pos[0], y: to_pos[1] },
                ];
                continue;
            }

            // Cut back from both ends
            let start_frac = max_aggr_distance / total_len;
            let end_frac = 1.0 - max_aggr_distance / total_len;

            let cut_geom = get_polyline_segment(geom, start_frac, end_frac);

            // Prepend from node position, append to node position
            let from_pos = edge.from.borrow().pos;
            let to_pos = edge.to.borrow().pos;

            let mut new_geom = Vec::with_capacity(cut_geom.len() + 2);
            new_geom.push(Coord { x: from_pos[0], y: from_pos[1] });
            new_geom.extend(cut_geom);
            new_geom.push(Coord { x: to_pos[0], y: to_pos[1] });

            edge.geometry = new_geom;
        }
    }
}

/// Calculate polyline length
fn calc_polyline_length(coords: &[Coord]) -> f64 {
    let mut len = 0.0;
    for i in 0..coords.len().saturating_sub(1) {
        let dx = coords[i + 1].x - coords[i].x;
        let dy = coords[i + 1].y - coords[i].y;
        len += (dx * dx + dy * dy).sqrt();
    }
    len
}

/// Get a segment of a polyline between two fractional positions
fn get_polyline_segment(coords: &[Coord], start_frac: f64, end_frac: f64) -> Vec<Coord> {
    if coords.len() < 2 || start_frac >= end_frac {
        return coords.to_vec();
    }

    let total_len = calc_polyline_length(coords);
    if total_len == 0.0 {
        return coords.to_vec();
    }

    let start_dist = start_frac * total_len;
    let end_dist = end_frac * total_len;

    let mut result = Vec::new();
    let mut cur_dist = 0.0;
    let mut started = false;

    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let dx = p2.x - p1.x;
        let dy = p2.y - p1.y;
        let seg_len = (dx * dx + dy * dy).sqrt();

        let seg_end_dist = cur_dist + seg_len;

        // Check if start point is in this segment
        if !started && start_dist >= cur_dist && start_dist <= seg_end_dist {
            let frac = (start_dist - cur_dist) / seg_len;
            result.push(Coord {
                x: p1.x + dx * frac,
                y: p1.y + dy * frac,
            });
            started = true;
        }

        // If we've started, add intermediate points
        if started && cur_dist >= start_dist && seg_end_dist <= end_dist {
            result.push(p2);
        }

        // Check if end point is in this segment
        if started && end_dist >= cur_dist && end_dist <= seg_end_dist {
            let frac = (end_dist - cur_dist) / seg_len;
            result.push(Coord {
                x: p1.x + dx * frac,
                y: p1.y + dy * frac,
            });
            break;
        }

        cur_dist = seg_end_dist;
    }

    if result.is_empty() && !coords.is_empty() {
        result.push(coords[0]);
    }

    result
}

/// Contract very short edges (edge artifact removal) (C++: lines 421-481 of collapseShrdSegs)
/// Repeatedly merges nodes connected by edges shorter than max_aggr_distance
fn contract_short_edges(graph: &mut LineGraph, max_aggr_distance: f64) {
    let mut total_contracted = 0;
    let max_iterations = 100;

    for _ in 0..max_iterations {
        let mut contracted_this_iter = false;
        let nodes: Vec<NodeRef> = graph.nodes.iter().map(Rc::clone).collect();

        for node in nodes {
            if node.borrow().get_deg() == 0 {
                continue;
            }

            let adj_list = node.borrow().adj_list.clone();
            for edge_ref in &adj_list {
                let (to_node, from_deg, to_deg) = {
                    let edge = edge_ref.borrow();

                    // Only process edges where this node is 'from'
                    if !Rc::ptr_eq(&edge.from, &node) {
                        continue;
                    }

                    // Check endpoint distance (geometry is just straight line at this point)
                    let from_pos = edge.from.borrow().pos;
                    let to_pos = edge.to.borrow().pos;
                    let dx = from_pos[0] - to_pos[0];
                    let dy = from_pos[1] - to_pos[1];
                    let dist = (dx * dx + dy * dy).sqrt();

                    // Skip if too long
                    if dist >= max_aggr_distance {
                        continue;
                    }

                    (
                        Rc::clone(&edge.to),
                        edge.from.borrow().get_deg(),
                        edge.to.borrow().get_deg(),
                    )
                };

                // Skip self-loops
                if Rc::ptr_eq(&node, &to_node) {
                    continue;
                }

                // C++ lines 460-472: Don't contract cases where:
                // 1) One node is terminus (deg 1), other is deg 2, and lines match
                // 2) Both nodes are deg 2 and all lines match
                // This avoids deleting edges that were left during deg-2 contraction
                // to avoid too-long edges
                if from_deg == 1 && to_deg == 2 {
                    let to_adj = to_node.borrow().adj_list.clone();
                    if to_adj.len() == 2 {
                        if routes_equal(&to_adj[0].borrow().routes, &to_adj[1].borrow().routes) {
                            continue;
                        }
                    }
                } else if from_deg == 2 && to_deg == 1 {
                    let from_adj = node.borrow().adj_list.clone();
                    if from_adj.len() == 2 {
                        if routes_equal(&from_adj[0].borrow().routes, &from_adj[1].borrow().routes) {
                            continue;
                        }
                    }
                } else if from_deg == 2 && to_deg == 2 {
                    let from_adj = node.borrow().adj_list.clone();
                    let to_adj = to_node.borrow().adj_list.clone();
                    if from_adj.len() == 2 && to_adj.len() == 2 {
                        if routes_equal(&from_adj[0].borrow().routes, &from_adj[1].borrow().routes)
                            && routes_equal(&to_adj[0].borrow().routes, &to_adj[1].borrow().routes)
                        {
                            continue;
                        }
                    }
                }

                // COMBINE NODES: Move all edges from 'node' to 'to_node'
                // The 'to_node' will be the surviving node
                
                // Calculate new position as centroid
                let new_pos = {
                    let from_pos = node.borrow().pos;
                    let to_pos = to_node.borrow().pos;
                    [(from_pos[0] + to_pos[0]) / 2.0, (from_pos[1] + to_pos[1]) / 2.0]
                };
                to_node.borrow_mut().pos = new_pos;

                // Move edges from 'node' to 'to_node'
                let node_adj = node.borrow().adj_list.clone();
                for other_edge_ref in &node_adj {
                    // Skip the connecting edge we're contracting
                    if Rc::ptr_eq(other_edge_ref, edge_ref) {
                        continue;
                    }

                    let other_node = other_edge_ref.borrow().get_other_nd(&node);
                    
                    // Skip if other_node is to_node (would create self-loop)
                    if Rc::ptr_eq(&other_node, &to_node) {
                        continue;
                    }

                    // Check if to_node already has an edge to other_node
                    let existing = graph.get_edg(&to_node, &other_node);
                    
                    if let Some(existing_edge) = existing {
                        // Fold edges: merge routes (geometry is just straight line)
                        {
                            let routes_b: HashSet<(String, String)> = other_edge_ref.borrow().routes.clone();
                            let mut ee = existing_edge.borrow_mut();
                            for r in routes_b {
                                ee.routes.insert(r);
                            }
                            // Update geometry to straight line between new endpoints
                            let from_pos = ee.from.borrow().pos;
                            let to_pos = ee.to.borrow().pos;
                            ee.geometry = vec![
                                Coord { x: from_pos[0], y: from_pos[1] },
                                Coord { x: to_pos[0], y: to_pos[1] },
                            ];
                        }

                        // Remove other_edge from other_node's adj list
                        other_node.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                    } else {
                        // Create new edge from to_node to other_node
                        let new_edge = Rc::new(RefCell::new(LineEdge {
                            from: Rc::clone(&to_node),
                            to: Rc::clone(&other_node),
                            routes: other_edge_ref.borrow().routes.clone(),
                            geometry: vec![
                                Coord { x: new_pos[0], y: new_pos[1] },
                                Coord { x: other_node.borrow().pos[0], y: other_node.borrow().pos[1] },
                            ],
                        }));
                        
                        // Add to adj lists
                        to_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                        other_node.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                        other_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                    }
                }

                // Remove the connecting edge from to_node's adj list
                to_node.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, edge_ref));

                // Clear the contracted node
                node.borrow_mut().adj_list.clear();

                total_contracted += 1;
                contracted_this_iter = true;
                break;
            }

            if contracted_this_iter {
                break;
            }
        }

        if !contracted_this_iter {
            break;
        }
    }

    if total_contracted > 0 {
        println!("  Edge artifact removal: contracted {} short edges.", total_contracted);
        graph.nodes.retain(|n| n.borrow().get_deg() > 0);
    }
}

/// Get orthogonal line at a fraction along a polyline (for handle-based restrictions)
/// Returns a line segment perpendicular to the polyline at the given position
fn get_ortho_line_at(coords: &[Coord], frac: f64, length: f64) -> Option<(Coord, Coord)> {
    if coords.len() < 2 {
        return None;
    }

    let total_len = calc_polyline_length(coords);
    if total_len == 0.0 {
        return None;
    }

    let target_dist = frac * total_len;
    let mut cur_dist = 0.0;

    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let dx = p2.x - p1.x;
        let dy = p2.y - p1.y;
        let seg_len = (dx * dx + dy * dy).sqrt();

        if cur_dist + seg_len >= target_dist {
            // Found the segment
            let local_frac = (target_dist - cur_dist) / seg_len;
            let pt = Coord {
                x: p1.x + dx * local_frac,
                y: p1.y + dy * local_frac,
            };

            // Calculate perpendicular direction
            let perp_dx = -dy / seg_len;
            let perp_dy = dx / seg_len;

            let half_len = length / 2.0;
            return Some((
                Coord {
                    x: pt.x - perp_dx * half_len,
                    y: pt.y - perp_dy * half_len,
                },
                Coord {
                    x: pt.x + perp_dx * half_len,
                    y: pt.y + perp_dy * half_len,
                },
            ));
        }

        cur_dist += seg_len;
    }

    None
}

/// Fold parallel edges by averaging their geometries (C++: foldEdges)
/// When two edges share a node and go to the same place, average their geometries
fn fold_edges_for_node(graph: &mut LineGraph, node: &NodeRef) -> bool {
    let adj_list = node.borrow().adj_list.clone();
    if adj_list.len() < 2 {
        return false;
    }

    // Check for pairs of edges going to the same node
    for i in 0..adj_list.len() {
        for j in i + 1..adj_list.len() {
            let edge_a = &adj_list[i];
            let edge_b = &adj_list[j];

            let other_a = edge_a.borrow().get_other_nd(node);
            let other_b = edge_b.borrow().get_other_nd(node);

            // If they go to the same node, fold them
            if Rc::ptr_eq(&other_a, &other_b) {
                // Average geometries weighted by route count
                // Orient both geometries to start from 'node'
                let (geom_a, weight_a) = {
                    let e = edge_a.borrow();
                    (get_geometry_oriented_from(&e, node), e.routes.len())
                };
                let (geom_b, weight_b) = {
                    let e = edge_b.borrow();
                    (get_geometry_oriented_from(&e, node), e.routes.len())
                };

                let avg_geom = geom_avg(&geom_a, weight_a, &geom_b, weight_b);

                // Merge routes from both edges into edge_a
                {
                    let routes_b: HashSet<(String, String)> = edge_b.borrow().routes.clone();
                    let mut ea = edge_a.borrow_mut();
                    
                    // Update geometry, respecting original edge direction
                    if Rc::ptr_eq(&ea.to, node) {
                        // Edge points TO node, but avg_geom points FROM node
                        let mut g = avg_geom;
                        g.reverse();
                        ea.geometry = g;
                    } else {
                        ea.geometry = avg_geom;
                    }

                    for r in routes_b {
                        ea.routes.insert(r);
                    }
                }

                // Remove edge_b from adjacency lists
                node.borrow_mut()
                    .adj_list
                    .retain(|e| !Rc::ptr_eq(e, edge_b));
                other_b
                    .borrow_mut()
                    .adj_list
                    .retain(|e| !Rc::ptr_eq(e, edge_b));

                return true;
            }
        }
    }

    false
}

/// Apply polish fixes to the graph - INSIDE the iteration loop
/// Based on C++ collapseShrdSegs lines 421-481 (edge artifact removal)
fn apply_polish_fixes(graph: &mut LineGraph, max_aggr_distance: f64) {
    // C++ does edge artifact removal (combineNodes for short edges) in the inner loop
    // The complex operations (averageNodePositions, reconstructIntersections) are 
    // done OUTSIDE collapseShrdSegs in TopoMain.cpp
    
    // Edge artifact removal: contract very short edges (C++: lines 421-481)
    contract_short_edges(graph, max_aggr_distance);

    // Clean up orphaned nodes
    graph.nodes.retain(|n| n.borrow().get_deg() > 0);
}

/// Soft cleanup: combine nearby high-degree nodes (C++ lines 367-377)
/// This fixes "soap bubble" artifacts where junctions are too close but distinct
fn soft_cleanup(graph: &mut LineGraph, threshold: f64) {
    let mut total_contracted = 0;
    
    // We can't modify the graph while iterating, so we collect candidates first
    // In C++ they iterate and break on success, restarting or continuing carefully
    // distinct from contract_short_edges, this targets non-degree-2 nodes
    
    // Simple greedy approach: find one, contract, repeat
    loop {
        let mut found = false;
        let nodes: Vec<NodeRef> = graph.nodes.iter().map(Rc::clone).collect();

        for node in nodes {
            // C++: if ((from->getDeg() == 2 || to->getDeg() == 2)) continue;
            // We only want to merge junctions (deg != 2), usually deg 1 or >2
            // Merging deg 2 is handled by collapse_degree_2_nodes_serial
            if node.borrow().get_deg() == 2 {
                continue;
            }

            let adj_list = node.borrow().adj_list.clone();
            for edge_ref in &adj_list {
                // Only process outgoing edges to avoid double counting
                if !Rc::ptr_eq(&edge_ref.borrow().from, &node) {
                    continue;
                }

                let to_node = Rc::clone(&edge_ref.borrow().to);
                
                if to_node.borrow().get_deg() == 2 {
                    continue;
                }

                // Check distance/length
                let from_pos = node.borrow().pos;
                let to_pos = to_node.borrow().pos;
                let dx = from_pos[0] - to_pos[0];
                let dy = from_pos[1] - to_pos[1];
                let dist = (dx * dx + dy * dy).sqrt();

                if dist > threshold {
                    continue;
                }

                // Found a candidate to contract!
                // Contract 'node' into 'to_node' similar to contract_short_edges

                // Calculate new position as centroid
                let new_pos = [(from_pos[0] + to_pos[0]) / 2.0, (from_pos[1] + to_pos[1]) / 2.0];
                to_node.borrow_mut().pos = new_pos;

                // Move edges from 'node' to 'to_node'
                let node_adj = node.borrow().adj_list.clone();
                for other_edge_ref in &node_adj {
                    // Skip the connecting edge we're contracting
                    if Rc::ptr_eq(other_edge_ref, edge_ref) {
                        continue;
                    }

                    let other_node = other_edge_ref.borrow().get_other_nd(&node);
                    
                    // Skip if other_node is to_node (would create self-loop)
                    if Rc::ptr_eq(&other_node, &to_node) {
                        continue;
                    }

                    let existing = graph.get_edg(&to_node, &other_node);
                    
                    if let Some(existing_edge) = existing {
                        // Fold edges
                        {
                            let routes_b: HashSet<(String, String)> = other_edge_ref.borrow().routes.clone();
                            let mut ee = existing_edge.borrow_mut();
                            for r in routes_b {
                                ee.routes.insert(r);
                            }
                            // Geometry update deferred to end of loop
                        }
                        other_node.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                    } else {
                        // Create new edge
                        let new_edge = Rc::new(RefCell::new(LineEdge {
                            from: Rc::clone(&to_node),
                            to: Rc::clone(&other_node),
                            routes: other_edge_ref.borrow().routes.clone(),
                            geometry: vec![], // Will be set at end of loop
                        }));
                        
                        to_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                        other_node.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                        other_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                    }
                }

                // Remove the connecting edge
                to_node.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, edge_ref));
                node.borrow_mut().adj_list.clear();

                found = true;
                total_contracted += 1;
                break;
            }
            if found { break; }
        }
        if !found { break; }
    }
    
    if total_contracted > 0 {
        println!("  Soft cleanup: contracted {} nearby junctions.", total_contracted);
        graph.nodes.retain(|n| n.borrow().get_deg() > 0);
    }
}

/// Remove orphan lines - lines that don't connect to anything meaningful
/// (C++: mc.removeOrphanLines)
fn remove_orphan_lines(graph: &mut LineGraph) {
    // Remove edges where routes are empty
    for node in &graph.nodes {
        let edges_to_remove: Vec<EdgeRef> = node
            .borrow()
            .adj_list
            .iter()
            .filter(|e| {
                let edge = e.borrow();
                Rc::ptr_eq(&edge.from, node) && edge.routes.is_empty()
            })
            .cloned()
            .collect();

        for edge_ref in edges_to_remove {
            let to_node = edge_ref.borrow().to.clone();
            node.borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, &edge_ref));
            to_node.borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, &edge_ref));
        }
    }
}

/// Clean up geometries to ensure they connect properly to nodes (C++: cleanUpGeoms)
fn clean_up_geoms(graph: &LineGraph) {
    for node in &graph.nodes {
        let adj_list = node.borrow().adj_list.clone();
        for edge_ref in adj_list {
            // Only process edges where we're the 'from' node
            let is_from = {
                let edge = edge_ref.borrow();
                Rc::ptr_eq(&edge.from, node)
            };
            if !is_from {
                continue;
            }

            let mut edge = edge_ref.borrow_mut();
            if edge.geometry.len() < 2 {
                continue;
            }

            // Project from/to nodes onto the geometry
            let from_pos = edge.from.borrow().pos;
            let to_pos = edge.to.borrow().pos;

            // Find closest points on geometry to the node positions
            let from_coord = Coord { x: from_pos[0], y: from_pos[1] };
            let to_coord = Coord { x: to_pos[0], y: to_pos[1] };

            let start_frac = project_point_on_line(&edge.geometry, &from_coord);
            let end_frac = project_point_on_line(&edge.geometry, &to_coord);

            if start_frac < end_frac {
                edge.geometry = get_polyline_segment(&edge.geometry, start_frac, end_frac);
            }

            // Ensure endpoints match node positions
            if !edge.geometry.is_empty() {
                edge.geometry[0] = from_coord;
                *edge.geometry.last_mut().unwrap() = to_coord;
            }
        }
    }
}

/// Project a point onto a polyline, returning the fraction along the line
fn project_point_on_line(coords: &[Coord], point: &Coord) -> f64 {
    if coords.len() < 2 {
        return 0.0;
    }

    let total_len = calc_polyline_length(coords);
    if total_len == 0.0 {
        return 0.0;
    }

    let mut best_frac = 0.0;
    let mut best_dist = f64::INFINITY;
    let mut cur_dist = 0.0;

    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let dx = p2.x - p1.x;
        let dy = p2.y - p1.y;
        let seg_len = (dx * dx + dy * dy).sqrt();

        if seg_len == 0.0 {
            continue;
        }

        // Project point onto segment
        let t = ((point.x - p1.x) * dx + (point.y - p1.y) * dy) / (seg_len * seg_len);
        let t_clamped = t.clamp(0.0, 1.0);

        let proj_x = p1.x + dx * t_clamped;
        let proj_y = p1.y + dy * t_clamped;

        let dist = ((point.x - proj_x).powi(2) + (point.y - proj_y).powi(2)).sqrt();

        if dist < best_dist {
            best_dist = dist;
            best_frac = (cur_dist + seg_len * t_clamped) / total_len;
        }

        cur_dist += seg_len;
    }

    best_frac
}

/// Helper: Get geometry oriented to start from a specific node
fn get_geometry_oriented_from(edge: &LineEdge, node: &NodeRef) -> Vec<Coord> {
    if Rc::ptr_eq(&edge.from, node) {
        edge.geometry.clone()
    } else if Rc::ptr_eq(&edge.to, node) {
        let mut g = edge.geometry.clone();
        g.reverse();
        g
    } else {
        // Should not happen if logic is correct
        edge.geometry.clone()
    }
}
