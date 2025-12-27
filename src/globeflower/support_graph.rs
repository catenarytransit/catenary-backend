use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rstar::{AABB, RTree, RTreeObject};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::{Rc, Weak};

// ===========================================================================
// Rc-based Line Graph (matches C++ UndirGraph<LineNodePL, LineEdgePL>)
// ===========================================================================

type NodeRef = Rc<RefCell<LineNode>>;
type EdgeRef = Rc<RefCell<LineEdge>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum RouteDirection {
    Both,
    Forward,  // From -> To
    Backward, // To -> From
}

/// Line occurrence on an edge (matches C++ LineOcc)
/// direction: None = bidirectional, Some(node) = directed towards that node
#[derive(Clone, Debug)]
pub struct LineOcc {
    pub line_id: String,
    /// Direction the line travels. None = bidirectional.
    /// Some(weak_ref) = directed towards the node pointed to.
    /// Using Weak to avoid reference cycles.
    pub direction: Option<Weak<RefCell<LineNode>>>,
}

impl LineOcc {
    fn new(line_id: String, direction: Option<Weak<RefCell<LineNode>>>) -> Self {
        Self { line_id, direction }
    }

    /// Create a bidirectional line occurrence
    fn new_bidirectional(line_id: String) -> Self {
        Self {
            line_id,
            direction: None,
        }
    }
}

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
    routes: Vec<LineOcc>,
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
            routes: Vec::new(),
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
    cells: AHashMap<(i32, i32), Vec<NodeRef>>,
}

impl NodeGeoIdx {
    fn new(cell_size: f64) -> Self {
        Self {
            cell_size,
            cells: AHashMap::new(),
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
            if let Some(pos) = nodes.iter().position(|n| Rc::ptr_eq(n, node)) {
                nodes.swap_remove(pos);
            }
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

    /// Find the best node within radius using a scoring function (avoids Vec allocation).
    ///
    /// The callback receives (node_ref, node_id, node_pos, distance_squared) and returns:
    /// - Some(score) to consider this node as a candidate (lower score = better)
    /// - None to skip this node
    ///
    /// Returns the node with the lowest score, or None if no candidates.
    fn find_best_in_radius<F>(
        &self,
        pos: [f64; 2],
        radius: f64,
        mut score_fn: F,
    ) -> Option<(NodeRef, f64)>
    where
        F: FnMut(&NodeRef, usize, [f64; 2], f64) -> Option<f64>,
    {
        let r_sq = radius * radius;
        let min_c = self.get_cell_coords(pos[0] - radius, pos[1] - radius);
        let max_c = self.get_cell_coords(pos[0] + radius, pos[1] + radius);

        let mut best: Option<(NodeRef, f64)> = None;

        for cx in min_c.0..=max_c.0 {
            for cy in min_c.1..=max_c.1 {
                if let Some(nodes) = self.cells.get(&(cx, cy)) {
                    for node in nodes {
                        let node_borrow = node.borrow();
                        let node_pos = node_borrow.pos;
                        let node_id = node_borrow.id;
                        let node_deg = node_borrow.get_deg();
                        drop(node_borrow); // Release borrow before callback

                        let dx = node_pos[0] - pos[0];
                        let dy = node_pos[1] - pos[1];
                        let dist_sq = dx * dx + dy * dy;

                        if dist_sq > r_sq {
                            continue;
                        }

                        // Skip degree-0 nodes (matches C++ ndTest->getDeg() == 0 check)
                        if node_deg == 0 {
                            continue;
                        }

                        if let Some(score) = score_fn(node, node_id, node_pos, dist_sq) {
                            match &best {
                                None => best = Some((Rc::clone(node), score)),
                                Some((_, best_score)) if score < *best_score => {
                                    best = Some((Rc::clone(node), score));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        best
    }
}

// ===========================================================================
// Build Support Graph (public API)
// ===========================================================================

/// Build the Support Graph from raw Shapes (Thesis Section 3.2).
pub async fn build_support_graph(pool: &CatenaryPostgresPool) -> Result<Vec<GraphEdge>> {
    let mut conn = pool.get().await?;

    use catenary::schema::gtfs::shapes::dsl::*;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl; // Needed for filter, eq_any etc

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

    // =========================================================================
    // C++ TopoMain.cpp Pipeline (lines 128-185)
    // =========================================================================

    // C++ defaults: maxAggrDistance=50m, segmentLength=5m
    // Convert meters to degrees: 1 degree ≈ 111,111 meters
    let tight_d_cut = 0.00009; // 10 meters in degrees (C++: dCut=10)
    let normal_d_cut = 0.00045; // 50 meters in degrees (C++: maxAggrDistance=50)

    // CRITICAL OPTIMIZATION:
    // C++ uses "segmentLength = 5". On Lat/Lng data, this is 5 DEGREES (~500km).
    // This effectively disables densification for almost all segments in C++.
    // Rust was using 0.000045 (5 meters), causing massive densification (millions of points).
    // We relax this to 0.005 (~500 meters) to match C++ behaviour of only processing
    // exceptionally long segments.
    let seg_len = 0.005;

    // Step 1: Build initial graph from raw edges
    println!(
        "Building initial LineGraph from {} raw edges...",
        raw_edges.len()
    );
    let mut graph = build_initial_linegraph(&raw_edges, &mut node_id_counter);

    // Step 2: averageNodePositions (C++ line 128)
    println!("Averaging node positions...");
    average_node_positions(&mut graph);

    // Step 3: removeNodeArtifacts(false) - contract degree-2 nodes (C++ line 131)
    println!("Pre-collapse: removing node artifacts...");
    collapse_degree_2_nodes_serial(&mut graph, normal_d_cut);

    // Step 4: cleanUpGeoms (C++ line 133)
    println!("Cleaning up geometries...");
    clean_up_geoms(&graph);

    // Step 5: removeEdgeArtifacts - contract short edges (C++ line 142)
    println!("Pre-collapse: removing edge artifacts...");
    contract_short_edges(&mut graph, normal_d_cut);

    // Step 6: Two passes of collapseShrdSegs (C++ lines 145-146)
    println!("Running collapse pass 1 (tight threshold ~10m)...");
    let collapsed =
        collapse_shared_segments_from_graph(&graph, tight_d_cut, seg_len, 50, &mut node_id_counter);

    println!("Running collapse pass 2 (normal threshold ~50m)...");
    let collapsed =
        collapse_shared_segments(collapsed, normal_d_cut, seg_len, 50, &mut node_id_counter);

    Ok(collapsed)
}

// ===========================================================================
// Build Initial LineGraph from GraphEdges
// ===========================================================================

/// Build initial LineGraph by snapping together geometrically close endpoints.
/// This creates shared nodes where lines meet, matching C++ input format.
fn build_initial_linegraph(edges: &[GraphEdge], _next_node_id: &mut usize) -> LineGraph {
    let mut graph = LineGraph::new();
    let snap_threshold = 0.00002; // ~2 meters - snap endpoints this close together

    // Spatial index for finding nearby nodes
    let mut geo_idx = NodeGeoIdx::new(snap_threshold);

    for edge in edges {
        let geom = convert_to_geo(&edge.geometry);
        if geom.0.is_empty() {
            continue;
        }

        let from_pos = geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
        let to_pos = geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);

        // Find or create FROM node
        let from_node = {
            let neighbors = geo_idx.get(from_pos, snap_threshold);
            if let Some(existing) = neighbors.first() {
                Rc::clone(existing)
            } else {
                let node = graph.add_nd(from_pos);
                geo_idx.add(from_pos, Rc::clone(&node));
                node
            }
        };

        // Find or create TO node
        let to_node = {
            let neighbors = geo_idx.get(to_pos, snap_threshold);
            if let Some(existing) = neighbors.first() {
                Rc::clone(existing)
            } else {
                let node = graph.add_nd(to_pos);
                geo_idx.add(to_pos, Rc::clone(&node));
                node
            }
        };

        // Skip self-loops
        if Rc::ptr_eq(&from_node, &to_node) {
            continue;
        }

        // Create edge with geometry
        let new_edge = graph.add_edg(&from_node, &to_node);
        new_edge.borrow_mut().geometry = geom.0.iter().map(|c| Coord { x: c.x, y: c.y }).collect();

        // Add routes
        for (chateau, rid) in &edge.route_ids {
            let full_id = format!("{}_{}", chateau, rid);
            new_edge
                .borrow_mut()
                .routes
                .push(LineOcc::new_bidirectional(full_id));
        }
    }

    graph
}

/// Variant of collapse_shared_segments that takes a LineGraph directly
fn collapse_shared_segments_from_graph(
    graph: &LineGraph,
    d_cut: f64,
    seg_len: f64,
    max_iters: usize,
    next_node_id_counter: &mut usize,
) -> Vec<GraphEdge> {
    // Convert LineGraph to Vec<GraphEdge> format
    let mut edges = Vec::new();

    for node in &graph.nodes {
        let node_borrow = node.borrow();
        for edge in &node_borrow.adj_list {
            let edge_borrow = edge.borrow();
            if !Rc::ptr_eq(&edge_borrow.from, node) {
                continue;
            }

            let ls = GeoLineString::new(edge_borrow.geometry.clone());
            #[allow(deprecated)]
            let weight = ls.haversine_length();

            let from_id = edge_borrow.from.borrow().id;
            let to_id = edge_borrow.to.borrow().id;

            edges.push(GraphEdge {
                from: NodeId::Intersection(from_id),
                to: NodeId::Intersection(to_id),
                geometry: convert_from_geo(&ls),
                route_ids: edge_borrow
                    .routes
                    .iter()
                    .map(|r| ("shape".to_string(), r.line_id.clone()))
                    .collect(),
                weight,
                original_edge_index: None,
            });
        }
    }

    collapse_shared_segments(edges, d_cut, seg_len, max_iters, next_node_id_counter)
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
        let mut img_nds: AHashMap<NodeId, NodeRef> = AHashMap::new();
        // Set of image nodes (for artifact removal check) - matches C++: `imgNdsSet`
        let mut img_nds_set: AHashSet<usize> = AHashSet::new();

        println!("Edge sorting by length...");

        // Sort edges by length (longest first) - matches C++ sorting
        edges.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(Ordering::Equal));

        println!("Edge sorting by length complete.");

        for edge in &edges {
            let mut last: Option<NodeRef> = None;
            let mut my_nds: AHashSet<usize> = AHashSet::new(); // Blocking set (by node id)
            let mut affected_nodes: Vec<NodeRef> = Vec::new(); // C++ line 230: affectedNodes

            // Build polyline including endpoints (C++ lines 237-245)
            // CRITICAL: Must prepend FROM position and append TO position!
            let geom = convert_to_geo(&edge.geometry);
            let from_pos = geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
            let to_pos = geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);

            // Build full polyline: FROM -> geometry -> TO (matching C++ pl construction)
            let mut pl: Vec<[f64; 2]> = Vec::with_capacity(geom.0.len() + 2);
            pl.push(from_pos); // C++ line 240: pl.push_back(*e->getFrom()->pl().getGeom())
            for coord in &geom.0 {
                pl.push([coord.x, coord.y]);
            }
            pl.push(to_pos); // C++ line 242: pl.push_back(*e->getTo()->pl().getGeom())

            // Simplify then densify (matches C++: simplify(pl, 0.5), densify(..., SEGL))
            use geo::algorithm::simplify::Simplify;
            let pl_geo =
                GeoLineString::new(pl.iter().map(|p| Coord { x: p[0], y: p[1] }).collect());
            let simplified = pl_geo.simplify(0.5e-5); // ~0.5 meters in degrees
            let coords: Vec<[f64; 2]> = if simplified.0.is_empty() {
                pl
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
                let span_b_pos = if i == pl_len - 1 {
                    None
                } else {
                    Some(to_endpoint_pos)
                };

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
                        img_nds_set.insert(cur.borrow().id);
                        img_from_covered = true;
                    }
                }
                if i == pl_len - 1 {
                    if !img_nds.contains_key(&edge.to) {
                        img_nds.insert(edge.to.clone(), Rc::clone(&cur));
                        img_nds_set.insert(cur.borrow().id);
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
                    // Merge route info (assume Both for initial shapes unless detailed)
                    for (chateau, rid) in &edge.route_ids {
                        let full_id = format!("{}_{}", chateau, rid);
                        // Check if already exists? simple append for now, unique later
                        if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                            new_e
                                .borrow_mut()
                                .routes
                                .push(LineOcc::new_bidirectional(full_id));
                        }
                    }
                }

                // Track front position and node (C++: if (!front) front = cur)
                if front_pos.is_none() {
                    front_pos = Some(cur.borrow().pos);
                    front = Some(Rc::clone(&cur));
                }

                // Track affected nodes for artifact removal (C++ line 298)
                affected_nodes.push(Rc::clone(&cur));
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
                        for (chateau, rid) in &edge.route_ids {
                            let full_id = format!("{}_{}", chateau, rid);
                            if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                                new_e
                                    .borrow_mut()
                                    .routes
                                    .push(LineOcc::new_bidirectional(full_id));
                            }
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
                        for (chateau, rid) in &edge.route_ids {
                            let full_id = format!("{}_{}", chateau, rid);
                            if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                                new_e
                                    .borrow_mut()
                                    .routes
                                    .push(LineOcc::new_bidirectional(full_id));
                            }
                        }
                    }
                }
            }

            // =====================================================================
            // Affected Nodes Artifact Removal (C++ lines 341-364)
            // Check all affected nodes for artifact edges (edges connecting two
            // deg > 2 nodes under segment length). Combine with nearest high-degree
            // neighbor to prevent "soap bubble" artifacts at major stations.
            // =====================================================================
            for affected_node in &affected_nodes {
                // Skip if this is a primary image node (FROM/TO mapping)
                if img_nds_set.contains(&affected_node.borrow().id) {
                    continue;
                }

                let mut d_min = seg_len;
                let mut comb: Option<NodeRef> = None;

                // Find nearest high-degree neighbor to merge with
                let adj = affected_node.borrow().adj_list.clone();
                for edge_ref in &adj {
                    let b = edge_ref.borrow().get_other_nd(affected_node);

                    // C++ line 351: if ((a->getDeg() < 3 && b->getDeg() < 3)) continue;
                    // Only consider merging if at least one node is high-degree (junction)
                    if affected_node.borrow().get_deg() < 3 && b.borrow().get_deg() < 3 {
                        continue;
                    }

                    let a_pos = affected_node.borrow().pos;
                    let b_pos = b.borrow().pos;
                    let dx = a_pos[0] - b_pos[0];
                    let dy = a_pos[1] - b_pos[1];
                    let d_cur = (dx * dx + dy * dy).sqrt();

                    if d_cur <= d_min {
                        d_min = d_cur;
                        comb = Some(b);
                    }
                }

                // Merge affected node into comb (C++ line 362: combineNodes(a, comb, &tgNew))
                if let Some(comb_node) = comb {
                    if !Rc::ptr_eq(affected_node, &comb_node) {
                        // CRITICAL: Must actually combine the nodes, not just move position!
                        // This rewires all edges from affected_node to comb_node
                        if combine_nodes(affected_node, &comb_node, &mut tg_new, &mut geo_idx) {
                            // Successfully merged - geo_idx update handled by combine_nodes
                        }
                    }
                }
            }
        }

        let num_edges = tg_new.num_edges();
        println!("  Graph built with {} segments. Simplifying...", num_edges);

        // =====================================================================
        // Phase 1: Soft cleanup (C++: lines 367-377)
        // Combine nearby junctions before finalizing geometry
        // C++ doesn't use explicit threshold here - it just uses edges that exist
        // (which are already short from the collapse process)
        // We pass d_cut as the threshold (in degrees) to match C++ behaviour
        // =====================================================================
        println!("Soft cleanup...");
        soft_cleanup(&mut tg_new, d_cut);

        // =====================================================================
        // Phase 2: Write edge geoms (C++: lines 379-387)
        // SET ALL EDGES TO STRAIGHT LINES between their endpoints
        // This is critical - geometries are simplified to straight lines
        // =====================================================================
        println!("Writing edge geoms...");
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
                    Coord {
                        x: from_pos[0],
                        y: from_pos[1],
                    },
                    Coord {
                        x: to_pos[0],
                        y: to_pos[1],
                    },
                ];
            }
        }

        // =====================================================================
        // Phase 3: Re-collapse (C++: lines 389-419)
        // Contract degree-2 nodes
        // =====================================================================
        println!("  Re-collapse degree-2 nodes...");
        collapse_degree_2_nodes_serial(&mut tg_new, d_cut);

        // =====================================================================
        // Phase 4: Edge artifact removal (C++: lines 421-481)
        // Remove short edges that shouldn't exist
        // =====================================================================
        println!("  Contract short edges...");
        contract_short_edges(&mut tg_new, d_cut);

        // =====================================================================
        // Phase 5: Re-collapse again (C++: lines 483-502)
        // May have introduced new degree-2 nodes
        // =====================================================================
        println!("  Re-collapse degree-2 nodes...");
        collapse_degree_2_nodes_serial(&mut tg_new, d_cut);

        // =====================================================================
        // Phase 6: Polish fixes (C++: lines 164-186)
        // Multiple passes to clean up all artifacts
        // =====================================================================
        println!("  Polish fixes...");
        apply_polish_fixes(&mut tg_new, seg_len);

        // First reconstruction pass (C++: line 164)
        println!("  First reconstruction pass...");
        reconstruct_intersections(&mut tg_new, seg_len);

        // Remove orphan lines (C++: line 178)
        println!("  Remove orphan lines...");
        remove_orphan_lines(&mut tg_new);

        // C++: removeNodeArtifacts(true) - contract degree-2 nodes again (line 180)
        println!("  Contract degree-2 nodes...");
        collapse_degree_2_nodes_serial(&mut tg_new, d_cut);

        // Second reconstruction pass (C++: line 182)
        println!("  Second reconstruction pass...");
        reconstruct_intersections(&mut tg_new, seg_len);

        // Remove orphan lines again (C++: line 185)
        println!("  Remove orphan lines again...");
        remove_orphan_lines(&mut tg_new);

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
                    // Map back strictly to strings.
                    // Note: Direction info is effectively lost in GraphEdge export
                    // if GraphEdge doesn't support it, but it was used for simplification.
                    route_ids: edge_borrow
                        .routes
                        .iter()
                        .map(|ro| ("_unknown_".to_string(), ro.line_id.clone()))
                        .collect(),
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
    blocking_set: &AHashSet<usize>,
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

    // C++ lines 90-96: Calculate distance constraints from span endpoints
    // A candidate can only be accepted if it's closer to the point than the
    // span endpoints are (divided by sqrt(2) for geometric tolerance)
    let d_span_a_sq = if let Some(sa_pos) = span_a_pos {
        let dx = point[0] - sa_pos[0];
        let dy = point[1] - sa_pos[1];
        (dx * dx + dy * dy) / 2.0 // sqrt(2)^2 = 2
    } else {
        f64::INFINITY
    };

    let d_span_b_sq = if let Some(sb_pos) = span_b_pos {
        let dx = point[0] - sb_pos[0];
        let dy = point[1] - sb_pos[1];
        (dx * dx + dy * dy) / 2.0 // sqrt(2)^2 = 2
    } else {
        f64::INFINITY
    };

    let d_cut_sq = effective_d_cut * effective_d_cut;

    // Use callback-based search to avoid Vec allocation (major perf win)
    let best =
        geo_idx.find_best_in_radius(point, effective_d_cut, |_node, nd_id, nd_pos, dist_sq| {
            // Skip nodes in blocking set (already on this edge's path)
            if blocking_set.contains(&nd_id) {
                return None;
            }

            // C++ line 104: d < dSpanA && d < dSpanB && d < dMax && d < dBest
            // Compare squared distances to avoid sqrt
            if dist_sq < d_span_a_sq && dist_sq < d_span_b_sq && dist_sq < d_cut_sq {
                Some(dist_sq) // Score by distance (lower = better)
            } else {
                None
            }
        });

    if let Some((nd_min, _score)) = best {
        // Update node position to centroid
        let old_pos = nd_min.borrow().pos;
        let new_pos = [(old_pos[0] + point[0]) / 2.0, (old_pos[1] + point[1]) / 2.0];

        // OPTIMIZATION: Only update grid if cell changes
        let old_cell = geo_idx.get_cell_coords(old_pos[0], old_pos[1]);
        let new_cell = geo_idx.get_cell_coords(new_pos[0], new_pos[1]);

        if old_cell != new_cell {
            geo_idx.remove(&nd_min);
            nd_min.borrow_mut().pos = new_pos;
            geo_idx.add(new_pos, Rc::clone(&nd_min));
        } else {
            // Just update position, no need to touch the map/vec
            nd_min.borrow_mut().pos = new_pos;
        }

        nd_min
    } else {
        // Create new node
        let new_node = graph.add_nd(point);
        geo_idx.add(point, Rc::clone(&new_node));
        new_node
    }
}

/// Check if line direction matches relative to a shared node
///
/// Returns true if `dir` (a direction on an edge) points TOWARDS `target_node`.
/// With pointer-based directions:
/// - None = bidirectional (counts as "towards" any node)
/// - Some(weak_ref) = directed towards the node that weak_ref points to
fn is_directed_towards(dir: &Option<Weak<RefCell<LineNode>>>, target_node: &NodeRef) -> bool {
    match dir {
        None => true, // Bidirectional counts as towards anywhere
        Some(weak_ref) => {
            if let Some(node_ref) = weak_ref.upgrade() {
                Rc::ptr_eq(&node_ref, target_node)
            } else {
                false // Weak ref expired - treat as not matching
            }
        }
    }
}

/// Check if line direction matches relative to a shared node (Incoming logic)
///
/// Returns:
/// - 0: Bidirectional
/// - 1: Incoming (directed towards node)
/// - -1: Outgoing (directed away from node)
fn direction_at_node(edge: &EdgeRef, dir: &Option<Weak<RefCell<LineNode>>>, node: &NodeRef) -> i8 {
    match dir {
        None => 0, // Bidirectional
        Some(weak_ref) => {
            if let Some(direction_node) = weak_ref.upgrade() {
                if Rc::ptr_eq(&direction_node, node) {
                    1 // Points TO node -> Incoming
                } else {
                    -1 // Points AWAY from node -> Outgoing
                }
            } else {
                0 // Weak ref expired - treat as bidirectional
            }
        }
    }
}

/// Replace node references in line directions (matches C++ LineGraph::nodeRpl)
/// Replaces all references to old_node with new_node in the edge's route directions
fn node_rpl(edge: &EdgeRef, old_node: &NodeRef, new_node: &NodeRef) {
    let mut edge_mut = edge.borrow_mut();
    for route in &mut edge_mut.routes {
        if let Some(ref weak_ref) = route.direction {
            if let Some(node_ref) = weak_ref.upgrade() {
                if Rc::ptr_eq(&node_ref, old_node) {
                    route.direction = Some(Rc::downgrade(new_node));
                }
            }
        }
    }
}

/// Combine node 'a' into node 'b' (matches C++ MapConstructor::combineNodes lines 754-837)
///
/// - Deletes 'a' and the connecting edge {a, b}
/// - 'b' becomes the contracted node at the centroid position
/// - All edges from 'a' are moved to 'b'
/// - Returns true if successful
fn combine_nodes(
    a: &NodeRef,
    b: &NodeRef,
    graph: &mut LineGraph,
    geo_idx: &mut NodeGeoIdx,
) -> bool {
    // Get connecting edge
    let connecting = graph.get_edg(a, b);
    if connecting.is_none() {
        return false;
    }
    let connecting = connecting.unwrap();

    // Update b's position to centroid of a and b
    let a_pos = a.borrow().pos;
    let b_pos = b.borrow().pos;
    let new_pos = [(a_pos[0] + b_pos[0]) / 2.0, (a_pos[1] + b_pos[1]) / 2.0];

    geo_idx.remove(b);
    b.borrow_mut().pos = new_pos;
    geo_idx.add(new_pos, Rc::clone(b));

    // Process edges where 'a' is the FROM node
    let a_adj = a.borrow().adj_list.clone();
    for old_edge in &a_adj {
        if !Rc::ptr_eq(&old_edge.borrow().from, a) {
            continue; // Only process FROM edges in this pass
        }
        if Rc::ptr_eq(old_edge, &connecting) {
            continue;
        }

        let other_node = old_edge.borrow().to.clone();

        // Safety check: shouldn't happen if graph is well-formed
        if Rc::ptr_eq(&other_node, b) {
            continue;
        }

        // Check if edge from b to other_node already exists
        if let Some(existing_edge) = graph.get_edg(b, &other_node) {
            // Fold edges: merge routes AND geometry (C++: foldEdges)
            let routes_from_old = old_edge.borrow().routes.clone();
            let geom_from_old = old_edge.borrow().geometry.clone();
            let mut ee = existing_edge.borrow_mut();

            // Average geometries (C++: geomAvg)
            if !geom_from_old.is_empty() && !ee.geometry.is_empty() {
                ee.geometry = geom_avg(&ee.geometry, 1, &geom_from_old, 1);
            } else if ee.geometry.is_empty() {
                ee.geometry = geom_from_old;
            }

            for r in routes_from_old {
                if !ee.routes.iter().any(|er| er.line_id == r.line_id) {
                    ee.routes.push(r);
                }
            }

            // Update route directions - replace references to 'a' with 'b'
            drop(ee);
            node_rpl(&existing_edge, a, b);

            // Remove old_edge from other_node's adjacency
            other_node
                .borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, old_edge));
        } else {
            // Create new edge from b to other_node, PRESERVING GEOMETRY (C++: std::move(oldE->pl()))
            let new_edge = Rc::new(RefCell::new(LineEdge {
                from: Rc::clone(b),
                to: Rc::clone(&other_node),
                routes: old_edge.borrow().routes.clone(),
                geometry: old_edge.borrow().geometry.clone(), // PRESERVE geometry!
            }));

            // Update route directions
            node_rpl(&new_edge, a, b);

            b.borrow_mut().adj_list.push(Rc::clone(&new_edge));
            other_node
                .borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, old_edge));
            other_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
        }
    }

    // Process edges where 'a' is the TO node
    for old_edge in &a_adj {
        if !Rc::ptr_eq(&old_edge.borrow().to, a) {
            continue; // Only process TO edges in this pass
        }
        if Rc::ptr_eq(old_edge, &connecting) {
            continue;
        }

        let other_node = old_edge.borrow().from.clone();

        if Rc::ptr_eq(&other_node, b) {
            continue;
        }

        // Check if edge from other_node to b already exists
        if let Some(existing_edge) = graph.get_edg(&other_node, b) {
            // Fold edges: merge routes AND geometry (C++: foldEdges)
            let routes_from_old = old_edge.borrow().routes.clone();
            let geom_from_old = old_edge.borrow().geometry.clone();
            let mut ee = existing_edge.borrow_mut();

            // Average geometries (C++: geomAvg)
            if !geom_from_old.is_empty() && !ee.geometry.is_empty() {
                ee.geometry = geom_avg(&ee.geometry, 1, &geom_from_old, 1);
            } else if ee.geometry.is_empty() {
                ee.geometry = geom_from_old;
            }

            for r in routes_from_old {
                if !ee.routes.iter().any(|er| er.line_id == r.line_id) {
                    ee.routes.push(r);
                }
            }

            drop(ee);
            node_rpl(&existing_edge, a, b);

            other_node
                .borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, old_edge));
        } else {
            // Create new edge from other_node to b, PRESERVING GEOMETRY
            let new_edge = Rc::new(RefCell::new(LineEdge {
                from: Rc::clone(&other_node),
                to: Rc::clone(b),
                routes: old_edge.borrow().routes.clone(),
                geometry: old_edge.borrow().geometry.clone(), // PRESERVE geometry!
            }));

            node_rpl(&new_edge, a, b);

            other_node
                .borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, old_edge));
            other_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
            b.borrow_mut().adj_list.push(Rc::clone(&new_edge));
        }
    }

    // Remove connecting edge from b's adjacency list
    b.borrow_mut()
        .adj_list
        .retain(|e| !Rc::ptr_eq(e, &connecting));

    // Remove 'a' from geo_idx and clear its adjacency list (effectively "deleting" it)
    geo_idx.remove(a);
    a.borrow_mut().adj_list.clear();

    true
}

/// Check if a connection occurs at a node for a given line between two edges.
/// Matches C++ LineNodePL::connOccurs (LineNodePL.cpp:128-137)
///
/// Returns true unless there's an explicit connection exception stored at the node.
/// Since we don't have connection exception data yet, this always returns true.
fn conn_occurs(_node: &NodeRef, _line_id: &str, _edge_a: &EdgeRef, _edge_b: &EdgeRef) -> bool {
    // C++ implementation checks _connEx map for explicit banned connections.
    // If no exception exists, connection is allowed.
    // TODO: Implement connection exception tracking if needed for turn restrictions.
    true
}

/// Matches C++ MapConstructor::lineEq
///
/// Checks if two edges can be merged based on their routes and directions.
fn line_eq(a: &EdgeRef, b: &EdgeRef) -> bool {
    let a_routes = &a.borrow().routes;
    let b_routes = &b.borrow().routes;

    // 1. Same number of lines
    if a_routes.len() != b_routes.len() {
        return false;
    }

    // Identify shared node
    let shr_nd = if Rc::ptr_eq(&a.borrow().from, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().from, &b.borrow().to)
    {
        Rc::clone(&a.borrow().from)
    } else if Rc::ptr_eq(&a.borrow().to, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().to, &b.borrow().to)
    {
        Rc::clone(&a.borrow().to)
    } else {
        // Should not happen if called correctly on adjacent edges
        return false;
    };

    // 2. Check each line validation
    for ra in a_routes {
        // Find corresponding line in b
        let rb = match b_routes.iter().find(|r| r.line_id == ra.line_id) {
            Some(r) => r,
            None => return false,
        };

        // C++ line 57: if (!shrNd->pl().connOccurs(ra.line, a, b)) return false;
        // Check if connection is allowed at the shared node (no excluded connections).
        // Currently always returns true since we don't have connection exception data.
        if !conn_occurs(&shr_nd, &ra.line_id, a, b) {
            return false;
        }

        let dir_a = direction_at_node(a, &ra.direction, &shr_nd);
        let dir_b = direction_at_node(b, &rb.direction, &shr_nd);

        // Logic from C++:
        // if (ra.direction == 0 && rb.direction == 0) found = true;
        // if (ra.direction == shrNd && rb.direction != 0 && rb.direction != shrNd) found = true;
        // if (ra.direction != shrNd && ra.direction != 0 && rb.direction == shrNd) found = true;

        // Translated to 0 (Both), 1 (Incoming to shrNd), -1 (Outgoing from shrNd)

        // Case 1: Both are bidirectional
        if dir_a == 0 && dir_b == 0 {
            continue;
        }

        // Case 2: A is incoming to shared, B is outgoing from shared
        // (A points to shared, B points away from shared -> Valid flow through)
        if dir_a == 1 && dir_b == -1 {
            continue;
        }

        // Case 3: A is outgoing from shared, B is incoming to shared
        // (B points to shared, A points away -> Valid flow through)
        if dir_a == -1 && dir_b == 1 {
            continue;
        }

        // If none matched, continuity is broken (e.g. head-to-head or tail-to-tail one-ways)
        return false;
    }

    true
}

/// Matches C++ MapConstructor::supportEdge
/// Split a long blocking edge 'ex' into two edges with a new support node in the middle.
fn support_edge(ex: &EdgeRef, graph: &mut LineGraph) {
    let ex_borrow = ex.borrow();
    let from = Rc::clone(&ex_borrow.from);
    let to = Rc::clone(&ex_borrow.to);
    let routes = ex_borrow.routes.clone();
    let geom = ex_borrow.geometry.clone();

    // Split geometry in half
    let geom_len = geom.len();
    if geom_len < 2 {
        return;
    }

    // Simple split index (approximate middle)
    // C++ uses geometric split at 0.5 length fraction.
    // Here we just pick middle vertex index for simplicity, or interpolate if needed.
    // Given the density, vertex split is likely fine.
    let mid_idx = geom_len / 2;

    // Create support node
    let mid_coord = geom[mid_idx];
    let sup_nd = graph.add_nd([mid_coord.x, mid_coord.y]); // IDs are internal

    // Create edge A: from -> sup_nd
    let geom_a = geom[0..=mid_idx].to_vec();
    let edge_a = Rc::new(RefCell::new(LineEdge {
        from: Rc::clone(&from),
        to: Rc::clone(&sup_nd),
        routes: routes.clone(),
        geometry: geom_a,
    }));

    // Create edge B: sup_nd -> to
    let geom_b = geom[mid_idx..].to_vec();
    let edge_b = Rc::new(RefCell::new(LineEdge {
        from: Rc::clone(&sup_nd),
        to: Rc::clone(&to),
        routes: routes.clone(),
        geometry: geom_b,
    }));

    // Update Adjacency Lists
    // 1. Remove ex from 'from' and 'to'
    from.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, ex));
    to.borrow_mut().adj_list.retain(|e| !Rc::ptr_eq(e, ex));

    // 2. Add new edges
    from.borrow_mut().adj_list.push(Rc::clone(&edge_a));
    sup_nd.borrow_mut().adj_list.push(Rc::clone(&edge_a));

    sup_nd.borrow_mut().adj_list.push(Rc::clone(&edge_b));
    to.borrow_mut().adj_list.push(Rc::clone(&edge_b));

    // C++ _origEdgs tracking (MapConstructor.cpp line 733-734: combContEdgs)
    // Used for edge provenance tracking in restriction inference and freeze/unfreeze operations.
    // The tracking maps new edges back to their original source edges for turn restriction inference.
    // Not critical for core geometry algorithm - topology is preserved without it.
}

/// Combine two edges into one, removing the intermediate node 'n'.
/// Directions are merged.
fn combine_edges(edge_a: &EdgeRef, edge_b: &EdgeRef, n: &NodeRef, graph: &mut LineGraph) {
    let ea_borrow = edge_a.borrow();
    let eb_borrow = edge_b.borrow();

    let other_a = ea_borrow.get_other_nd(n);
    let other_b = eb_borrow.get_other_nd(n);

    // Merge geometry and handle directions
    // Orientation: We want new edge to be other_a -> other_b

    let mut new_geom = Vec::new();

    // Check orientation of A relative to n
    let a_to_n = Rc::ptr_eq(&ea_borrow.to, n);
    // If a.to == n, then A is other_a -> n (Correct direction for prefix)
    // If a.from == n, then A is n -> other_a (Need comparison)

    if a_to_n {
        new_geom.extend(ea_borrow.geometry.iter().cloned());
    } else {
        new_geom.extend(ea_borrow.geometry.iter().rev().cloned());
    }

    // Remove duplicate join point
    if !new_geom.is_empty() {
        new_geom.pop();
    }

    // Check orientation of B relative to n
    let b_from_n = Rc::ptr_eq(&eb_borrow.from, n);
    // If b.from == n, then B is n -> other_b (Correct direction for suffix)

    if b_from_n {
        new_geom.extend(eb_borrow.geometry.iter().cloned());
    } else {
        new_geom.extend(eb_borrow.geometry.iter().rev().cloned());
    }

    let mut new_routes = Vec::new();

    // foldEdges logic: merge routes
    // A lines
    for ra in &ea_borrow.routes {
        // Find rb
        if let Some(rb) = eb_borrow.routes.iter().find(|r| r.line_id == ra.line_id) {
            // Merging directions
            let dir_a = direction_at_node(edge_a, &ra.direction, n);
            // dir_a == 1 (Incoming to n)
            // dir_a == -1 (Outgoing from n)

            let dir_b = direction_at_node(edge_b, &rb.direction, n);

            // Resulting direction on new edge (other_a -> other_b)
            // If Flow is other_a -> n -> other_b
            // Then incoming to n (A) is Forward relative to A ?
            //   If A is other_a->n, Forward is incoming to n.
            //   If B is n->other_b, Forward is outgoing from n (towards other_b).

            // If A(Incoming to n) and B(Outgoing from n) => Continuous Forward flow
            // new direction: towards other_b

            // If A(Outgoing from n) and B(Incoming to n) => Continuous Backward flow
            // new direction: towards other_a

            let new_dir: Option<Weak<RefCell<LineNode>>> = if dir_a == 1 && dir_b == -1 {
                // Forward: towards other_b
                Some(Rc::downgrade(&other_b))
            } else if dir_a == -1 && dir_b == 1 {
                // Backward: towards other_a
                Some(Rc::downgrade(&other_a))
            } else {
                None // Bidirectional
            };

            new_routes.push(LineOcc {
                line_id: ra.line_id.clone(),
                direction: new_dir,
            });
        }
    }

    // Create new edge
    let new_edge = Rc::new(RefCell::new(LineEdge {
        from: Rc::clone(&other_a),
        to: Rc::clone(&other_b),
        routes: new_routes,
        geometry: new_geom,
    }));

    // Drop borrows before mutating graph
    drop(ea_borrow);
    drop(eb_borrow);

    // Update adj lists
    other_a
        .borrow_mut()
        .adj_list
        .retain(|e| !Rc::ptr_eq(e, edge_a));
    other_a.borrow_mut().adj_list.push(Rc::clone(&new_edge));

    other_b
        .borrow_mut()
        .adj_list
        .retain(|e| !Rc::ptr_eq(e, edge_b));
    other_b.borrow_mut().adj_list.push(Rc::clone(&new_edge));

    // Clear n
    n.borrow_mut().adj_list.clear();
}

/// Contract degree-2 nodes with matching routes (matches C++ re-collapse phase)
/// This is a single-pass implementation to avoid O(N^2) behaviour on large graphs.
fn collapse_degree_2_nodes_serial(graph: &mut LineGraph, d_cut: f64) {
    let mut total_contracted = 0;

    // Collect all nodes to iterate over
    let nodes: Vec<NodeRef> = graph.nodes.iter().map(Rc::clone).collect();

    // Convert d_cut (degrees) to meters for threshold comparison
    // C++ uses: 2 * maxD(numLines, dCut) which is 2 * dCut
    // 1 degree ≈ 111,111 meters at equator
    let support_threshold_meters = 2.0 * d_cut * 111111.0;

    for node in nodes {
        // Check if node is degree 2
        let deg = node.borrow().get_deg();
        if deg != 2 {
            continue;
        }

        let adj = node.borrow().adj_list.clone();
        if adj.len() != 2 {
            continue;
        }

        let edge_a = &adj[0];
        let edge_b = &adj[1];

        // Ensure routes match (C++ lineEq)
        if !line_eq(edge_a, edge_b) {
            continue;
        }

        let other_a = edge_a.borrow().get_other_nd(&node);
        let other_b = edge_b.borrow().get_other_nd(&node);

        let mut do_contract = true;

        // Check for existing edge between the other two nodes
        if let Some(ex) = graph.get_edg(&other_a, &other_b) {
            // C++ Logic:
            // if (ex && ex->pl().longerThan(2 * maxD(...))) -> supportEdge(ex)
            // else -> continue (don't contract)

            #[allow(deprecated)]
            let ex_len = {
                let geom = &ex.borrow().geometry;
                let ls = GeoLineString::new(geom.clone());
                ls.haversine_length()
            };

            // Threshold: if blocking edge is long (> 2 * d_cut in meters), split it.
            if ex_len > support_threshold_meters {
                println!("Splitting blocking edge to allow contraction...");
                support_edge(&ex, graph);
                // After splitting, 'ex' is effectively gone/replaced.
                // We can proceed with contraction of node.
            } else {
                do_contract = false;
            }
        }

        if !do_contract {
            continue;
        }

        // C++: MAX_COLLAPSED_SEG_LENGTH check (500m ~ 0.0045 degrees)
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

        combine_edges(edge_a, edge_b, &node, graph);
        total_contracted += 1;
    }

    if total_contracted > 0 {
        println!("  Contracted {} degree-2 nodes.", total_contracted);
        graph.nodes.retain(|n| n.borrow().get_deg() > 0);
    }
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
fn geom_avg(geom_a: &[Coord], weight_a: usize, geom_b: &[Coord], weight_b: usize) -> Vec<Coord> {
    if geom_a.is_empty() {
        return geom_b.to_vec();
    }
    if geom_b.is_empty() {
        return geom_a.to_vec();
    }

    // Handle single-point geometries - just return weighted average point
    if geom_a.len() < 2 || geom_b.len() < 2 {
        // Just average the available points
        let pa = geom_a.first().unwrap();
        let pb = geom_b.first().unwrap();
        let wa = (weight_a * weight_a) as f64;
        let wb = (weight_b * weight_b) as f64;
        let total_weight = wa + wb;
        if total_weight == 0.0 {
            return geom_a.to_vec();
        }
        return vec![Coord {
            x: (pa.x * wa + pb.x * wb) / total_weight,
            y: (pa.y * wa + pb.y * wb) / total_weight,
        }];
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

    // Use the actual resampled lengths (in case resampling failed)
    let actual_len = resampled_a.len().min(resampled_b.len());
    if actual_len == 0 {
        return geom_a.to_vec();
    }

    // Weighted average of corresponding points
    let mut result = Vec::with_capacity(actual_len);
    for i in 0..actual_len {
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

                    // Skip if endpoint distance too long
                    if dist >= max_aggr_distance {
                        continue;
                    }

                    // ALSO check actual geometry length (C++: e->pl().getPolyline().shorterThan())
                    // This catches curved edges where endpoints are close but path is long
                    let geom_len = calc_polyline_length(&edge.geometry);
                    if geom_len >= max_aggr_distance {
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
                        if line_eq(&to_adj[0], &to_adj[1]) {
                            continue;
                        }
                    }
                } else if from_deg == 2 && to_deg == 1 {
                    let from_adj = node.borrow().adj_list.clone();
                    if from_adj.len() == 2 {
                        if line_eq(&from_adj[0], &from_adj[1]) {
                            continue;
                        }
                    }
                } else if from_deg == 2 && to_deg == 2 {
                    let from_adj = node.borrow().adj_list.clone();
                    let to_adj = to_node.borrow().adj_list.clone();
                    if from_adj.len() == 2 && to_adj.len() == 2 {
                        if line_eq(&from_adj[0], &from_adj[1]) && line_eq(&to_adj[0], &to_adj[1]) {
                            continue;
                        }
                    }
                }

                // GEOMETRY CHECK: Prevent folding edges with vastly different lengths
                // This matches C++ contractNodes lines 593-606
                // If merging would create edges with very different lengths, these are lines
                // crossing at angles - don't merge them!
                let node_adj_check = node.borrow().adj_list.clone();
                let mut dont_contract = false;
                for other_edge_ref in &node_adj_check {
                    if Rc::ptr_eq(other_edge_ref, edge_ref) {
                        continue;
                    }

                    let other_neighbor = other_edge_ref.borrow().get_other_nd(&node);
                    if Rc::ptr_eq(&other_neighbor, &to_node) {
                        continue;
                    }

                    // Check if to_node already has connection to other_neighbor
                    if let Some(existing_edge) = graph.get_edg(&to_node, &other_neighbor) {
                        let len_old = calc_polyline_length(&other_edge_ref.borrow().geometry);
                        let len_new = calc_polyline_length(&existing_edge.borrow().geometry);

                        // If lengths differ significantly, these are lines at angles - don't merge!
                        if (len_new - len_old).abs() > max_aggr_distance * 2.0 {
                            dont_contract = true;
                            break;
                        }
                    }
                }

                if dont_contract {
                    continue;
                }

                // COMBINE NODES: Move all edges from 'node' to 'to_node'
                // The 'to_node' will be the surviving node

                // Calculate new position: favor the higher degree node to prevent shifting main lines
                // when contracting spurs. If degrees equal, use centroid.
                let new_pos = if to_node.borrow().get_deg() > node.borrow().get_deg() {
                    to_node.borrow().pos
                } else if node.borrow().get_deg() > to_node.borrow().get_deg() {
                    node.borrow().pos
                } else {
                    let from_pos = node.borrow().pos;
                    let to_pos = to_node.borrow().pos;
                    [
                        (from_pos[0] + to_pos[0]) / 2.0,
                        (from_pos[1] + to_pos[1]) / 2.0,
                    ]
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
                            let routes_b: Vec<LineOcc> = other_edge_ref.borrow().routes.clone();
                            let mut ee = existing_edge.borrow_mut();

                            // Check if direction needs flipping
                            // existing: A -> B
                            // other: C -> B (where C is being merged into A) -> effectively A -> B?
                            // Logic: compare 'other_node' position
                            let other_node_is_to_existing = Rc::ptr_eq(&ee.to, &other_node);
                            let other_node_is_to_other =
                                Rc::ptr_eq(&other_edge_ref.borrow().to, &other_node);

                            let flip = other_node_is_to_existing != other_node_is_to_other;

                            for r in routes_b {
                                // C++ foldEdges lines 917-941: map direction to new edge
                                // Direction logic based on shared node:
                                // - If direction is None (bidirectional): stays None
                                // - If direction points to from_node (being merged): map to to_node
                                // - If direction points away: keep pointing to other_node

                                let new_dir = if r.direction.is_none() {
                                    None // Bidirectional stays bidirectional
                                } else if flip {
                                    // When flipping orientation, swap direction pointer
                                    // If it pointed to from_node, now point to to_node
                                    if let Some(ref weak_ref) = r.direction {
                                        if let Some(dir_node) = weak_ref.upgrade() {
                                            if Rc::ptr_eq(&dir_node, &node) {
                                                // Was pointing to merged node, now point to merged target
                                                Some(Rc::downgrade(&to_node))
                                            } else if Rc::ptr_eq(&dir_node, &other_node) {
                                                Some(Rc::downgrade(&other_node))
                                            } else {
                                                r.direction.clone()
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    r.direction.clone()
                                };

                                // Check if this line already exists
                                let existing_r =
                                    ee.routes.iter_mut().find(|r| r.line_id == r.line_id);
                                if let Some(existing_r) =
                                    ee.routes.iter_mut().find(|er| er.line_id == r.line_id)
                                {
                                    // If directions conflict, make bidirectional
                                    let same_direction = match (&existing_r.direction, &new_dir) {
                                        (None, None) => true,
                                        (Some(a), Some(b)) => {
                                            if let (Some(na), Some(nb)) = (a.upgrade(), b.upgrade())
                                            {
                                                Rc::ptr_eq(&na, &nb)
                                            } else {
                                                false
                                            }
                                        }
                                        _ => false,
                                    };
                                    if !same_direction {
                                        existing_r.direction = None; // Become bidirectional
                                    }
                                } else {
                                    ee.routes.push(LineOcc {
                                        line_id: r.line_id.clone(),
                                        direction: new_dir,
                                    });
                                }
                            }

                            // Average geometry instead of straight line (C++: foldEdges/geomAvg)
                            let geom_from_old = other_edge_ref.borrow().geometry.clone();
                            if !geom_from_old.is_empty() && !ee.geometry.is_empty() {
                                ee.geometry = geom_avg(&ee.geometry, 1, &geom_from_old, 1);
                            } else if ee.geometry.is_empty() {
                                ee.geometry = geom_from_old;
                            }
                            // If both empty, leave as is - reconstruct_intersections will fix
                        }

                        // Remove other_edge from other_node's adj list
                        other_node
                            .borrow_mut()
                            .adj_list
                            .retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                    } else {
                        // Create new edge from to_node to other_node, PRESERVING GEOMETRY
                        let new_edge = Rc::new(RefCell::new(LineEdge {
                            from: Rc::clone(&to_node),
                            to: Rc::clone(&other_node),
                            routes: other_edge_ref.borrow().routes.clone(),
                            geometry: other_edge_ref.borrow().geometry.clone(), // PRESERVE geometry!
                        }));

                        // Add to adj lists
                        to_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                        other_node
                            .borrow_mut()
                            .adj_list
                            .retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                        other_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                    }
                }

                // Remove the connecting edge from to_node's adj list
                to_node
                    .borrow_mut()
                    .adj_list
                    .retain(|e| !Rc::ptr_eq(e, edge_ref));

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
        println!(
            "  Edge artifact removal: contracted {} short edges.",
            total_contracted
        );
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
                    let routes_b: Vec<LineOcc> = edge_b.borrow().routes.clone();
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

                    // For fold_edges_for_node, both edges connect (node, other_node).
                    // We need to check if they are oriented the same way relative to 'node'.
                    let ea_to_node = Rc::ptr_eq(&ea.to, node);
                    let eb_to_node = Rc::ptr_eq(&edge_b.borrow().to, node);

                    let flip = ea_to_node != eb_to_node;

                    for r in routes_b {
                        let new_dir = r.direction.clone();
                        // For pointer directions, flipping doesn't change the pointer target,
                        // it's just about whether we need to interpret it differently.
                        // Since we're merging into edge_a, the direction pointer stays valid.

                        if let Some(existing_r) =
                            ea.routes.iter_mut().find(|er| er.line_id == r.line_id)
                        {
                            // If directions conflict, make bidirectional
                            let same_direction = match (&existing_r.direction, &new_dir) {
                                (None, None) => true,
                                (Some(a), Some(b)) => {
                                    if let (Some(na), Some(nb)) = (a.upgrade(), b.upgrade()) {
                                        Rc::ptr_eq(&na, &nb)
                                    } else {
                                        false
                                    }
                                }
                                _ => false,
                            };
                            if !same_direction {
                                existing_r.direction = None; // Become bidirectional
                            }
                        } else {
                            ea.routes.push(LineOcc {
                                line_id: r.line_id.clone(),
                                direction: new_dir,
                            });
                        }
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
/// Soft cleanup: combine nearby high-degree nodes (C++ lines 367-377)
/// This fixes "soap bubble" artifacts where junctions are too close but distinct
fn soft_cleanup(graph: &mut LineGraph, threshold: f64) {
    let mut total_contracted = 0;

    // C++ Strategy: Iterate all nodes/edges and merge if "safe".
    // "Safe" means:
    // 1. Shorter than threshold (maxAggrDistance)
    // 2. Geometry check: merging wouldn't fold edges with vastly different geometries.

    // We iterate a snapshot of nodes to avoid concurrent modification issues during iteration.
    let nodes: Vec<NodeRef> = graph.nodes.iter().map(Rc::clone).collect();

    for node in nodes {
        // Skip if node is already effectively deleted (deg 0/isolated) although get_deg handles it.
        if node.borrow().get_deg() == 0 {
            continue;
        }

        // CRITICAL: Skip degree-2 nodes - they are part of linear tracks, not junctions
        // This matches C++ lines 374: if ((from->getDeg() == 2 || to->getDeg() == 2)) continue;
        // Merging degree-2 nodes causes curved tracks to be straightened incorrectly
        if node.borrow().get_deg() == 2 {
            continue;
        }

        let adj_list = node.borrow().adj_list.clone();

        // Find best candidate to merge INTO.
        // C++ iterates outgoing edges.

        let mut best_candidate: Option<(NodeRef, f64)> = None;

        for edge_ref in &adj_list {
            let e = edge_ref.borrow();
            // Process only outgoing edges to avoid double checking pairs
            if !Rc::ptr_eq(&e.from, &node) {
                continue;
            }

            // Check if distance/geometry length is small enough
            // Use actual geometry length, not just displacement
            let dist = calc_polyline_length(&e.geometry);

            if dist > threshold {
                continue;
            }

            let to_node = Rc::clone(&e.to);
            if Rc::ptr_eq(&to_node, &node) {
                continue;
            } // safe-guard

            // CRITICAL: Skip if target is degree-2 (would straighten curves)
            // This matches C++ lines 374: if ((from->getDeg() == 2 || to->getDeg() == 2)) continue;
            if to_node.borrow().get_deg() == 2 {
                continue;
            }

            // GEOMETRY CHECK (The "Angle Check"):
            // Check if we would fold edges with vastly different geoms.
            // If we merge `node` into `to_node`, `node`'s neighbors become `to_node`'s neighbors.
            // If `to_node` ALREADY has a connection to a neighbor, we merge edges.
            // We must ensure the new edge is not wildly different in length from the old edge.

            let mut safe_to_merge = true;
            for old_edge_ref in &adj_list {
                if Rc::ptr_eq(old_edge_ref, edge_ref) {
                    continue;
                }

                let old_neighbor = old_edge_ref.borrow().get_other_nd(&node);

                // If we merge `node` -> `to_node`, the new edge is `to_node` -> `old_neighbor`
                if let Some(new_edge_ref) = graph.get_edg(&to_node, &old_neighbor) {
                    let len_old = calc_polyline_length(&old_edge_ref.borrow().geometry);
                    let len_new = calc_polyline_length(&new_edge_ref.borrow().geometry);

                    if (len_new - len_old).abs() > threshold * 2.0 {
                        safe_to_merge = false;
                        break;
                    }
                }
            }

            if !safe_to_merge {
                continue;
            }

            // If multiple candidates, pick shortest? C++ takes first valid.
            // Let's pick shortest to be safe.
            if let Some((_, best_dist)) = best_candidate {
                if dist < best_dist {
                    best_candidate = Some((to_node, dist));
                }
            } else {
                best_candidate = Some((to_node, dist));
            }
        }

        if let Some((to_node, _)) = best_candidate {
            // Contract 'node' into 'to_node'

            // Get fresh reference to edge connecting them
            let edge_ref = {
                let adj = node.borrow().adj_list.clone();
                let mut found_edge = None;
                for e in adj {
                    if Rc::ptr_eq(&e.borrow().to, &to_node) && Rc::ptr_eq(&e.borrow().from, &node) {
                        found_edge = Some(e);
                        break;
                    }
                }
                if found_edge.is_none() {
                    continue;
                }
                found_edge.unwrap()
            };

            let from_pos = node.borrow().pos;
            let to_pos = to_node.borrow().pos;

            // Calculate new position as centroid
            let new_pos = [
                (from_pos[0] + to_pos[0]) / 2.0,
                (from_pos[1] + to_pos[1]) / 2.0,
            ];
            to_node.borrow_mut().pos = new_pos;

            // Move edges from 'node' to 'to_node'
            let node_adj = node.borrow().adj_list.clone();
            for other_edge_ref in &node_adj {
                // Skip the connecting edge we're contracting
                if Rc::ptr_eq(other_edge_ref, &edge_ref) {
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
                        let routes_b: Vec<LineOcc> = other_edge_ref.borrow().routes.clone();
                        let mut ee = existing_edge.borrow_mut();

                        // Check flipping
                        let other_node_is_to_existing = Rc::ptr_eq(&ee.to, &other_node);
                        let other_node_is_to_other =
                            Rc::ptr_eq(&other_edge_ref.borrow().to, &other_node);

                        let flip = other_node_is_to_existing != other_node_is_to_other;

                        for r in routes_b {
                            let new_dir = r.direction.clone();
                            // With pointer directions, we just keep the pointer - nodeRpl fixes any stale refs

                            if let Some(existing_r) =
                                ee.routes.iter_mut().find(|er| er.line_id == r.line_id)
                            {
                                // If directions conflict, make bidirectional
                                let same_direction = match (&existing_r.direction, &new_dir) {
                                    (None, None) => true,
                                    (Some(a), Some(b)) => {
                                        if let (Some(na), Some(nb)) = (a.upgrade(), b.upgrade()) {
                                            Rc::ptr_eq(&na, &nb)
                                        } else {
                                            false
                                        }
                                    }
                                    _ => false,
                                };
                                if !same_direction {
                                    existing_r.direction = None; // Become bidirectional
                                }
                            } else {
                                ee.routes.push(LineOcc {
                                    line_id: r.line_id.clone(),
                                    direction: new_dir,
                                });
                            }
                        }
                        // Geometry update deferred to end of loop
                    }
                    other_node
                        .borrow_mut()
                        .adj_list
                        .retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                } else {
                    // Create new edge
                    // IMPORTANT: Geometry might be empty/straight line, will be fixed by clean_up_geoms or reconstruct
                    let new_edge = Rc::new(RefCell::new(LineEdge {
                        from: Rc::clone(&to_node),
                        to: Rc::clone(&other_node),
                        routes: other_edge_ref.borrow().routes.clone(),
                        geometry: vec![], // Will be set at end of loop by clean_up_geoms?
                                          // Actually soft_cleanup calls run before "write edge geoms" in C++
                                          // so geometry is just endpoints.
                    }));

                    to_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                    other_node
                        .borrow_mut()
                        .adj_list
                        .retain(|e| !Rc::ptr_eq(e, other_edge_ref));
                    other_node.borrow_mut().adj_list.push(Rc::clone(&new_edge));
                }
            }

            // Remove the connecting edge
            to_node
                .borrow_mut()
                .adj_list
                .retain(|e| !Rc::ptr_eq(e, &edge_ref));
            node.borrow_mut().adj_list.clear();

            total_contracted += 1;
        }
    }

    if total_contracted > 0 {
        println!(
            "  Soft cleanup: contracted {} nearby nodes.",
            total_contracted
        );
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
            to_node
                .borrow_mut()
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
            let from_coord = Coord {
                x: from_pos[0],
                y: from_pos[1],
            };
            let to_coord = Coord {
                x: to_pos[0],
                y: to_pos[1],
            };

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

/// Average node positions based on connected edge geometries (C++: averageNodePositions)
fn average_node_positions(graph: &mut LineGraph) {
    for node in &graph.nodes {
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut count = 0;

        for edge_ref in &node.borrow().adj_list {
            let edge = edge_ref.borrow();
            let geom = &edge.geometry;
            if geom.is_empty() {
                continue;
            }

            // Check if we are connected to the start or end of the edge geometry
            // The edge object has from/to, but the geometry might be oriented differently?
            // Usually geometry follows from->to
            if Rc::ptr_eq(&edge.to, node) {
                // Connected to the END of this edge
                let last = geom.last().unwrap();
                sum_x += last.x;
                sum_y += last.y;
            } else {
                // Connected to the START of this edge
                let first = geom.first().unwrap();
                sum_x += first.x;
                sum_y += first.y;
            }
            count += 1;
        }

        if count > 0 {
            node.borrow_mut().pos = [sum_x / count as f64, sum_y / count as f64];
        }
    }
}

/// Reconstruct intersections (C++: reconstructIntersections)
/// 1. Average node positions
/// 2. Trim/Extend edge geometries to meet exactly at the new node positions
fn reconstruct_intersections(graph: &mut LineGraph, max_aggr_distance: f64) {
    // 1. Center nodes based on current edge geometries
    average_node_positions(graph);

    // 2. Adjust edge geometries to meet at new node positions
    for node in &graph.nodes {
        // Process each connected edge
        let adj_list = node.borrow().adj_list.clone();
        for edge_ref in adj_list {
            // To avoid double processing and borrow issues, we only process when we are the 'from' node.
            // But we need to access BOTH nodes to get their positions.

            let (from_pos, to_pos, is_from) = {
                let e = edge_ref.borrow();
                let is_from = Rc::ptr_eq(&e.from, node);
                (e.from.borrow().pos, e.to.borrow().pos, is_from)
            };

            if !is_from {
                continue;
            }

            let mut edge = edge_ref.borrow_mut();

            // Cut off the ends (C++: maxAggrDistance)
            let total_len = calc_polyline_length(&edge.geometry);

            // If geometry is empty (e.g. from soft_cleanup), we just create straight line
            if edge.geometry.is_empty() {
                edge.geometry = vec![
                    Coord {
                        x: from_pos[0],
                        y: from_pos[1],
                    },
                    Coord {
                        x: to_pos[0],
                        y: to_pos[1],
                    },
                ];
                continue;
            }

            if total_len > 2.0 * max_aggr_distance {
                // Trim both ends
                let start_frac = max_aggr_distance / total_len;
                let end_frac = (total_len - max_aggr_distance) / total_len;

                edge.geometry = get_polyline_segment(&edge.geometry, start_frac, end_frac);
            } else {
                // Too short to trim, Replace with empty capable of accepting points
                edge.geometry.clear();
            }

            // Insert explicit Start point (fromNode)
            edge.geometry.insert(
                0,
                Coord {
                    x: from_pos[0],
                    y: from_pos[1],
                },
            );

            // Insert explicit End point (toNode)
            edge.geometry.push(Coord {
                x: to_pos[0],
                y: to_pos[1],
            });
        }
    }
}
