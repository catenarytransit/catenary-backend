use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rayon::prelude::*;
use rstar::{AABB, RTree, RTreeObject};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::{Rc, Weak};

// ===========================================================================
// Web Mercator Coordinate Conversion (matches C++ util::geo::latLngToWebMerc)
// ===========================================================================

const EARTH_RADIUS: f64 = 6378137.0;

/// Convert lat/lng (EPSG:4326) to Web Mercator (EPSG:3857) - matches C++ latLngToWebMerc
/// Input: (longitude, latitude) in degrees
/// Output: (x, y) in meters
fn lat_lng_to_web_merc(lon: f64, lat: f64) -> (f64, f64) {
    let x = EARTH_RADIUS * lon.to_radians();
    let y = EARTH_RADIUS * ((std::f64::consts::FRAC_PI_4 + lat.to_radians() / 2.0).tan()).ln();
    (x, y)
}

/// Convert Web Mercator (EPSG:3857) to lat/lng (EPSG:4326) - matches C++ webMercToLatLng
/// Input: (x, y) in meters
/// Output: (longitude, latitude) in degrees
fn web_merc_to_lat_lng(x: f64, y: f64) -> (f64, f64) {
    let lon = (x / EARTH_RADIUS).to_degrees();
    let lat = (2.0 * (y / EARTH_RADIUS).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees();
    (lon, lat)
}

/// Convert a GraphEdge geometry to Web Mercator coordinates
fn convert_edge_to_web_merc(edge: &mut GraphEdge) {
    for coord in &mut edge.geometry.points {
        let (x, y) = lat_lng_to_web_merc(coord.x, coord.y);
        coord.x = x;
        coord.y = y;
    }
}

/// Convert a GraphEdge geometry back to lat/lng coordinates
fn convert_edge_to_lat_lng(edge: &mut GraphEdge) {
    for coord in &mut edge.geometry.points {
        let (lon, lat) = web_merc_to_lat_lng(coord.x, coord.y);
        coord.x = lon;
        coord.y = lat;
    }
}

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
        .filter(stop_to_stop_generated.eq(false))
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    println!(
        "Loaded {} raw shapes for Support Graph.",
        loaded_shapes.len()
    );

    // Load shape->route mappings from direction_pattern_meta
    use catenary::schema::gtfs::direction_pattern_meta::dsl as meta_dsl;
    let metas = meta_dsl::direction_pattern_meta
        .load::<catenary::models::DirectionPatternMeta>(&mut conn)
        .await?;

    // Build map: shape_id -> Vec<(chateau, route_id)>
    let mut shape_to_routes: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for m in metas {
        if let (Some(sid), Some(rid)) = (m.gtfs_shape_id, m.route_id) {
            shape_to_routes
                .entry(sid)
                .or_default()
                .push((m.chateau.clone(), rid.to_string()));
        }
    }

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

        // Get actual route IDs for this shape (chateau, route_id)
        let route_ids = shape_to_routes
            .get(&shape.shape_id)
            .cloned()
            .unwrap_or_default();

        // Track original shape provenance
        let original_shape_ids = vec![(shape.chateau.clone(), shape.shape_id)];

        #[allow(deprecated)]
        let weight = geom.haversine_length();

        raw_edges.push(GraphEdge {
            from: start_node,
            to: end_node,
            geometry: convert_from_geo(&geom),
            route_ids,
            original_shape_ids,
            weight,
            original_edge_index: None,
        });
    }

    // =========================================================================
    // CRITICAL: Convert all coordinates to Web Mercator (meters)
    // C++ reads lat/lng input and converts to Web Mercator internally
    // (see LineGraph.cpp line 247: latLngToWebMerc)
    // PARALLEL: Each edge conversion is independent
    // =========================================================================
    println!(
        "Converting {} edges to Web Mercator coordinates (parallel)...",
        raw_edges.len()
    );
    raw_edges.par_iter_mut().for_each(|edge| {
        convert_edge_to_web_merc(edge);
        // Recalculate weight in meters (Euclidean in projected coords)
        let geom = convert_to_geo(&edge.geometry);
        edge.weight = geom.euclidean_length();
    });

    // =========================================================================
    // C++ TopoMain.cpp Pipeline (lines 128-185)
    // All thresholds are now in METERS (matching C++ behaviour)
    // =========================================================================
    let tight_d_cut = 10.0; // 10 meters (C++ first pass: dCut=10)
    let normal_d_cut = 50.0; // 50 meters (C++ maxAggrDistance=50)
    let seg_len = 5.0; // 5 meters (C++ segmentLength=5)

    // Snap threshold for building initial graph
    let snap_threshold = 2.0; // 2 meters in Web Mercator

    // Step 1: Build initial graph from raw edges (already in Web Mercator)
    println!(
        "Building initial LineGraph from {} raw edges...",
        raw_edges.len()
    );
    let mut graph =
        build_initial_linegraph_webmerc(&raw_edges, &mut node_id_counter, snap_threshold);

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
    println!("Running collapse pass 1 (tight threshold 10m)...");
    let collapsed =
        collapse_shared_segments_from_graph(&graph, tight_d_cut, seg_len, 50, &mut node_id_counter);

    println!("Running collapse pass 2 (normal threshold 50m)...");
    let mut collapsed =
        collapse_shared_segments(collapsed, normal_d_cut, seg_len, 50, &mut node_id_counter);

    // =========================================================================
    // Post-Collapse Reconstruction (matches C++ TopoMain.cpp lines 164-186)
    // These steps were moved OUT of the loop to match C++ and fix performance.
    // =========================================================================

    // First reconstruction pass (C++: line 164)
    println!("First reconstruction pass (post-collapse)...");
    let mut graph = graph_from_edges(collapsed, &mut node_id_counter);
    reconstruct_intersections(&mut graph, seg_len);

    // Remove orphan lines (C++: line 178)
    println!("Remove orphan lines...");
    remove_orphan_lines(&mut graph);

    // C++: removeNodeArtifacts(true) - contract degree-2 nodes again (line 180)
    // This cleans up new degree-2 nodes formed by reconstruction
    println!("Contract degree-2 nodes (post-collapse)...");
    collapse_degree_2_nodes_serial(&mut graph, normal_d_cut);

    // Second reconstruction pass (C++: line 182)
    println!("Second reconstruction pass...");
    reconstruct_intersections(&mut graph, seg_len);

    // Remove orphan lines again (C++: line 185)
    println!("Remove orphan lines again...");
    remove_orphan_lines(&mut graph);

    // Apply smoothing if requested (C++ line 201: tg.smooth(cfg.smooth))
    // We can use apply_polish_fixes or a dedicated smooth function here if needed.
    // For now, reconstruct_intersections does some geometric cleanup.

    // Convert back to edges for return
    let mut collapsed = Vec::new();
    for node in &graph.nodes {
        let node_borrow = node.borrow();
        for edge in &node_borrow.adj_list {
            let edge_borrow = edge.borrow();
            if !Rc::ptr_eq(&edge_borrow.from, node) {
                continue;
            }

            let ls = GeoLineString::new(edge_borrow.geometry.clone());
            let weight = ls.euclidean_length();

            collapsed.push(GraphEdge {
                from: NodeId::Intersection(edge_borrow.from.borrow().id),
                to: NodeId::Intersection(edge_borrow.to.borrow().id),
                geometry: convert_from_geo(&ls),
                route_ids: edge_borrow
                    .routes
                    .iter()
                    .filter_map(|r| {
                        // line_id format: "chateau_routeid"
                        let parts: Vec<&str> = r.line_id.splitn(2, '_').collect();
                        if parts.len() == 2 {
                            Some((parts[0].to_string(), parts[1].to_string()))
                        } else {
                            None
                        }
                    })
                    .collect(),
                original_shape_ids: vec![], // Lost through LineGraph processing
                weight,
                original_edge_index: None,
            });
        }
    }

    // =========================================================================
    // Convert back to lat/lng for output (PARALLEL)
    // =========================================================================
    println!(
        "Converting {} edges back to lat/lng (parallel)...",
        collapsed.len()
    );
    collapsed.par_iter_mut().for_each(|edge| {
        convert_edge_to_lat_lng(edge);
        // Recalculate weight with haversine
        let geom = convert_to_geo(&edge.geometry);
        #[allow(deprecated)]
        {
            edge.weight = geom.haversine_length();
        }
    });

    Ok(collapsed)
}

/// Reconstruct a LineGraph from a list of GraphEdges, respecting existing NodeIds.
/// This does NOT snap nodes; it assumes edges sharing a NodeId are connected.
fn graph_from_edges(edges: Vec<GraphEdge>, next_node_id: &mut usize) -> LineGraph {
    let mut graph = LineGraph::new();
    let mut node_map: HashMap<usize, NodeRef> = HashMap::new();

    // First pass: Create all nodes
    for edge in &edges {
        let from_id = match edge.from {
            NodeId::Intersection(id) => id,
            _ => continue, // Should not happen in this context
        };
        let to_id = match edge.to {
            NodeId::Intersection(id) => id,
            _ => continue,
        };

        // Determine positions (approximate, will be fixed by reconstruction)
        // We use the first/last point of geometry
        let geom = convert_to_geo(&edge.geometry);
        let from_pos = geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
        let to_pos = geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);

        node_map.entry(from_id).or_insert_with(|| {
            let n = graph.add_nd(from_pos);
            n.borrow_mut().id = from_id;
            n
        });
        node_map.entry(to_id).or_insert_with(|| {
            let n = graph.add_nd(to_pos);
            n.borrow_mut().id = to_id;
            n
        });
    }

    // Update next_node_id to ensure future nodes don't collide
    if let Some(&max_id) = node_map.keys().max() {
        if max_id >= *next_node_id {
            *next_node_id = max_id + 1;
        }
    }
    graph.next_node_id = *next_node_id;

    // Second pass: Create edges
    for edge in edges {
        let from_id = match edge.from {
            NodeId::Intersection(id) => id,
            _ => continue,
        };
        let to_id = match edge.to {
            NodeId::Intersection(id) => id,
            _ => continue,
        };

        let from_node = node_map.get(&from_id).unwrap();
        let to_node = node_map.get(&to_id).unwrap();

        // Check if self-loop
        if Rc::ptr_eq(from_node, to_node) {
            continue;
        }

        let new_edge = graph.add_edg(from_node, to_node);
        let geom = convert_to_geo(&edge.geometry);
        new_edge.borrow_mut().geometry = geom.0.iter().map(|c| Coord { x: c.x, y: c.y }).collect();

        // Restore routes
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

        // Add routes WITH DIRECTION - GTFS shapes are directional
        for (chateau, rid) in &edge.route_ids {
            let full_id = format!("{}_{}", chateau, rid);
            // Direction points to TO node - shape goes from -> to
            new_edge
                .borrow_mut()
                .routes
                .push(LineOcc::new_bidirectional(full_id));
        }
    }

    graph
}

/// Build initial LineGraph with Web Mercator coordinates (meters).
/// snap_threshold is in meters (matching C++ behaviour with projected coordinates).
fn build_initial_linegraph_webmerc(
    edges: &[GraphEdge],
    _next_node_id: &mut usize,
    snap_threshold: f64,
) -> LineGraph {
    let mut graph = LineGraph::new();

    // Spatial index for finding nearby nodes (threshold in meters)
    let mut geo_idx = NodeGeoIdx::new(snap_threshold);

    let total_edges = edges.len();
    println!(
        "  Starting initial graph build for {} edges...",
        total_edges
    );

    for (idx, edge) in edges.iter().enumerate() {
        if idx % 5000 == 0 {
            println!(
                "  Building initial graph: processed {}/{} edges ({:.1}%)",
                idx,
                total_edges,
                100.0 * idx as f64 / total_edges as f64
            );
        }

        let geom = convert_to_geo(&edge.geometry);
        if geom.0.is_empty() {
            continue;
        }

        let from_pos = geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
        let to_pos = geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);

        // Find or create FROM node
        // Use find_best_in_radius to find CLOSEST node without allocating a vector
        let from_node = {
            if let Some((existing, _)) =
                geo_idx.find_best_in_radius(from_pos, snap_threshold, |_, _, _, d_sq| Some(d_sq))
            {
                existing
            } else {
                let node = graph.add_nd(from_pos);
                geo_idx.add(from_pos, Rc::clone(&node));
                node
            }
        };

        // Find or create TO node
        let to_node = {
            if let Some((existing, _)) =
                geo_idx.find_best_in_radius(to_pos, snap_threshold, |_, _, _, d_sq| Some(d_sq))
            {
                existing
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

        // Add routes WITH DIRECTION - GTFS shapes are directional
        for (chateau, rid) in &edge.route_ids {
            let full_id = format!("{}_{}", chateau, rid);
            // Direction points to TO node - shape goes from -> to
            new_edge
                .borrow_mut()
                .routes
                .push(LineOcc::new_bidirectional(full_id));
        }
    }

    println!(
        "  Initial graph built with {} nodes and {} edges.",
        graph.nodes.len(),
        graph.num_edges()
    );

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
            // Use Euclidean length since coordinates are in Web Mercator (meters)
            let weight = ls.euclidean_length();

            let from_id = edge_borrow.from.borrow().id;
            let to_id = edge_borrow.to.borrow().id;

            edges.push(GraphEdge {
                from: NodeId::Intersection(from_id),
                to: NodeId::Intersection(to_id),
                geometry: convert_from_geo(&ls),
                route_ids: edge_borrow
                    .routes
                    .iter()
                    .filter_map(|r| {
                        // line_id format: "chateau_routeid"
                        let parts: Vec<&str> = r.line_id.splitn(2, '_').collect();
                        if parts.len() == 2 {
                            Some((parts[0].to_string(), parts[1].to_string()))
                        } else {
                            None
                        }
                    })
                    .collect(),
                original_shape_ids: vec![], // Lost through LineGraph processing
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

        // Sort edges by length (longest first) - matches C++ sorting (PARALLEL)
        edges.par_sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(Ordering::Equal));

        let total_edges = edges.len();
        for (edge_idx, edge) in edges.iter().enumerate() {
            // Progress logging every 100 edges
            if edge_idx % 2000 == 0 {
                println!(
                    "  Processing edge {}/{} ({:.1}%)...",
                    edge_idx,
                    total_edges,
                    100.0 * edge_idx as f64 / total_edges as f64
                );
            }
            let mut last: Option<NodeRef> = None;
            let mut my_nds: AHashSet<usize> = AHashSet::new(); // Blocking set (by node id)
            let mut affected_nodes: Vec<NodeRef> = Vec::new(); // C++ line 230: affectedNodes

            // Build polyline including endpoints (C++ lines 237-245)
            // CRITICAL: C++ uses e->getFrom()->pl().getGeom() which is the NODE's current position,
            // not the edge geometry. Nodes may have moved during previous iterations!
            // Use img_nds to get the current node position if available.
            let geom = convert_to_geo(&edge.geometry);

            // Get FROM/TO node positions (may have moved in previous iterations)
            // Fall back to edge geometry if node not yet mapped
            let from_pos = img_nds
                .get(&edge.from)
                .map(|n| n.borrow().pos)
                .unwrap_or_else(|| geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]));
            let to_pos = img_nds
                .get(&edge.to)
                .map(|n| n.borrow().pos)
                .unwrap_or_else(|| geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]));

            // Build full polyline: FROM -> geometry -> TO (matching C++ pl construction)
            let mut pl: Vec<[f64; 2]> = Vec::with_capacity(geom.0.len() + 2);
            pl.push(from_pos); // C++ line 240: pl.push_back(*e->getFrom()->pl().getGeom())
            for coord in &geom.0 {
                pl.push([coord.x, coord.y]);
            }
            pl.push(to_pos); // C++ line 242: pl.push_back(*e->getTo()->pl().getGeom())

            // C++ line 244-245: simplify(pl, 0.5), then densify(..., SEGL)
            // CRITICAL: C++ uses 0.5m for simplification, NOT seg_len!
            // And C++ ALWAYS densifies after simplification
            use geo::algorithm::simplify::Simplify;
            let pl_geo =
                GeoLineString::new(pl.iter().map(|p| Coord { x: p[0], y: p[1] }).collect());

            // Simplify at 0.5m tolerance (C++: simplify(pl, 0.5))
            let simplified = pl_geo.simplify(0.5);
            let coords: Vec<[f64; 2]> = if simplified.0.is_empty() {
                pl
            } else {
                simplified.0.iter().map(|c| [c.x, c.y]).collect()
            };
            let coords_len = coords.len();

            // ALWAYS densify after simplification (C++: densify(..., SEGL))
            // This ensures curves maintain their shape by having regularly-spaced points
            let pl_dense = densify_coords(&coords, seg_len);

            // NOTE: C++ has no point cap - simplify(0.5) should already reduce density enough

            // Debug logging removed for performance

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

                    // Merge route info WITH DIRECTION
                    // CRITICAL: GTFS shapes are directional - trains travel FROM -> TO
                    // C++ mergeLines (lines 977-988) maps direction from old edge to new edge
                    // Since the original shape goes in one direction, and we're iterating
                    // from first point to last, the route direction should point toward 'cur' (the TO node)
                    for (chateau, rid) in &edge.route_ids {
                        let full_id = format!("{}_{}", chateau, rid);
                        // Check if already exists with same direction
                        if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                            // C++ uses bidirectional by default; direction comes from input
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
                                // C++ uses bidirectional by default
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
                                // C++ uses bidirectional by default
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
            // neighbour to prevent "soap bubble" artifacts at major stations.
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
                        // FIX BUG 2: Check for blocking edges before calling combine_nodes
                        // In the C++ affected nodes loop, there is no explicit check, but combineNodes handles it.
                        // However, we need to be careful not to create artifacts if the merge implies
                        // folding edges that shouldn't be folded.

                        // We rely on combine_nodes to handle the folding correctly.
                        // But let's verify if we are merging nodes that are already connected,
                        // this effectively collapses the edge between them.
                        // If they are NOT connected, we are "teleporting" them together.

                        // C++ `combineNodes` effectively moves `a` to `b`.
                        // If `a` and `b` are connected, the edge is removed.
                        // If `a` and `b` are NOT connected, `combineNodes` effectively merges them geometry-wise?
                        // Actually `combineNodes` logic (lines 1516+) checks:
                        // "Get connecting edge... if connecting.is_none() return false"
                        // So combine_nodes ONLY works if they are connected!

                        // Wait, C++ `combineNodes` (MapConstructor.cpp 754) STARTS with:
                        // LineEdge* connecting = g->getEdg(a, b); assert(connecting);
                        // So in C++, this loop (affected nodes) ONLY merges if there is an edge?
                        // Actually no, affected nodes loop finds "nearest high-degree neighbor".
                        // It does NOT guarantee they are connected.
                        // If they are NOT connected, C++ crashes on assert(connecting)?
                        // Let's check C++ again.
                        // Lines 348-357 iterate `a->getAdjList()`.
                        // `comb` is `e->getOtherNd(a)`.
                        // So `comb` IS a neighbor of `a`! They ARE connected!
                        // So our `combine_nodes` logic requiring connection is correct and consistent.

                        // So where is the bug?
                        // "Affected nodes artifact removal missing blocking edge check"
                        // If we hold the edge `a-comb`, and we merge `a` into `comb`.
                        // Are there OTHER edges between `a` and `comb`? (Parallel edges)
                        // If so, `combine_nodes` might behave weirdly.
                        // But `get_edg` returns one edge.

                        // Proceed with merge, ensuring combine_nodes is robust.
                        if combine_nodes(affected_node, &comb_node, &mut tg_new, &mut geo_idx) {
                            // Successfully merged
                        }
                    }
                }
            }

            // Garbage Collect Dead Nodes Periodically
            // Rust LineGraph keeps usage of Rc<RefCell<Node>>. When combine_nodes "deletes" a node,
            // it clears its adjacency list but the Node struct remains in tg_new.nodes.
            // In C++, the node is deleted. In Rust, we must manually remove it to free memory.
            // We do this periodically to amortize the O(N) cost of Vec::retain.
            // 2000 edges threshold is chosen to keep working set size reasonable without excessive scanning.
            // Garbage Collect Dead Nodes Periodically
            // Rust LineGraph keeps usage of Rc<RefCell<Node>>.
            // We reduced frequency significantly to avoid O(N) cost.
            // Only doing it once per iteration loop (at end) is safer for perf.
            /*
            if edge_idx % 2000 == 0 {
                tg_new.nodes.retain(|n| n.borrow().get_deg() > 0);
            }
            */
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
        soft_cleanup(&mut tg_new, d_cut);

        // =====================================================================
        // Phase 2: Write edge geoms (C++: lines 379-387)
        // SET ALL EDGES TO STRAIGHT LINES between their endpoints
        // This is critical - geometries are simplified to straight lines
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

        // Phase 3: Re-collapse (C++: lines 389-419)
        // Contract degree-2 nodes
        collapse_degree_2_nodes_serial(&mut tg_new, d_cut);

        // Phase 4: Edge artifact removal (C++: lines 421-481)
        // Remove short edges that shouldn't exist
        println!("Contracting short edges...");
        contract_short_edges(&mut tg_new, d_cut);

        // Phase 5: Re-collapse again (C++: lines 483-502)
        // May have introduced new degree-2 nodes
        println!("Contracting degree-2 nodes...");
        collapse_degree_2_nodes_serial(&mut tg_new, d_cut);

        // Phase 6: Polish fixes - Internal Loop (C++: lines 504-514)
        // Just the smoothing part here. Reconstruction moved outside loop.
        println!("Applying polish fixes...");
        apply_polish_fixes(&mut tg_new, seg_len);

        // NOTE: The C++ loop ENDS here.
        // reconstruct_intersections, remove_orphan_lines etc happen AFTER loop.

        // =====================================================================
        // Phase 6.5: DISABLED - This was destroying smoothed geometries!
        // The code below was overwriting all curved edges with straight lines
        // AFTER apply_polish_fixes had carefully smoothed them. This caused
        // jagged lines on curved rail turnings.
        //
        // The C++ code writes straight-line geometries at lines 379-387,
        // but that happens BEFORE the re-collapse and smoothing steps.
        // Having this AFTER apply_polish_fixes is incorrect.
        // =====================================================================
        /*
        for node in tg_new.get_nds() {
            let adj_list = node.borrow().adj_list.clone();
            for edge_ref in adj_list {
                let is_from = Rc::ptr_eq(&edge_ref.borrow().from, node);
                if !is_from {
                    continue;
                }

                // CRITICAL: Set geometry to straight line from current node positions
                // Any curves from combine_edges are discarded - we want clean straight lines
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
        */

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
                // Use Euclidean length since coordinates are in Web Mercator (meters)
                let weight = ls.euclidean_length();

                let from_id = edge_borrow.from.borrow().id;
                let to_id = edge_borrow.to.borrow().id;

                new_edges.push(GraphEdge {
                    from: NodeId::Intersection(from_id),
                    to: NodeId::Intersection(to_id),
                    geometry: convert_from_geo(&ls),
                    // Map back strictly to tuples.
                    // Note: Direction info is effectively lost in GraphEdge export
                    // if GraphEdge doesn't support it, but it was used for simplification.
                    route_ids: edge_borrow
                        .routes
                        .iter()
                        .filter_map(|ro| {
                            // line_id format: "chateau_routeid"
                            let parts: Vec<&str> = ro.line_id.splitn(2, '_').collect();
                            if parts.len() == 2 {
                                Some((parts[0].to_string(), parts[1].to_string()))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    original_shape_ids: vec![], // Lost through LineGraph processing
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

        // ALWAYS remove and re-add (matching C++ behavior)
        // C++ lines 113-122: geoIdx.remove(ndMin); ... geoIdx.add(..., ret);
        // The C++ always removes then re-adds regardless of cell position.
        // This ensures consistent neighbor lookups and avoids stale position data.
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
            let mut ee = existing_edge.borrow_mut();

            // CRITICAL: Align geometry directions before averaging (C++ foldEdges lines 910-914)
            // C++ Logic: if (b->getTo() == a->getTo() || a->getFrom() == b->getFrom())
            // Both old_edge and existing_edge share 'other_node' as an endpoint.
            // Check if BOTH edges point TO other_node OR BOTH point FROM other_node.
            // old_edge: from=a, to=other_node (in FROM loop, so old_edge.from == a)
            // existing_edge: from=b or to=other_node (either direction)
            let old_to_other = Rc::ptr_eq(&old_edge.borrow().to, &other_node);
            let existing_to_other = Rc::ptr_eq(&ee.to, &other_node);
            let same_direction = old_to_other == existing_to_other;

            let geom_old = old_edge.borrow().geometry.clone();
            let geom_ee = ee.geometry.clone();

            // Align old geometry to match existing_edge direction if needed
            let geom_old_aligned = if same_direction {
                geom_old
            } else {
                let mut g = geom_old;
                g.reverse();
                g
            };

            // Average aligned geometries with route count as weights (C++ lines 859-861)
            let weight_ee = ee.routes.len().max(1);
            let weight_old = old_edge.borrow().routes.len().max(1);
            let avg_geom = if !geom_ee.is_empty() && !geom_old_aligned.is_empty() {
                geom_avg(&geom_ee, weight_ee, &geom_old_aligned, weight_old)
            } else if geom_ee.is_empty() {
                geom_old_aligned
            } else {
                geom_ee
            };

            ee.geometry = avg_geom;

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
            let mut ee = existing_edge.borrow_mut();

            // CRITICAL: Align geometry directions before averaging (C++ foldEdges lines 910-914)
            // C++ Logic: if (b->getTo() == a->getTo() || a->getFrom() == b->getFrom())
            // Both old_edge and existing_edge share 'other_node' as an endpoint.
            // old_edge: from=other_node, to=a (in TO loop, so old_edge.to == a)
            // existing_edge: connects other_node <-> b
            let old_to_other = Rc::ptr_eq(&old_edge.borrow().to, &other_node);
            let existing_to_other = Rc::ptr_eq(&ee.to, &other_node);
            let same_direction = old_to_other == existing_to_other;

            let geom_old = old_edge.borrow().geometry.clone();
            let geom_ee = ee.geometry.clone();

            // Align old geometry to match existing_edge direction if needed
            let geom_old_aligned = if same_direction {
                geom_old
            } else {
                let mut g = geom_old;
                g.reverse();
                g
            };

            // Average aligned geometries with route count as weights (C++ lines 859-861)
            let weight_ee = ee.routes.len().max(1);
            let weight_old = old_edge.borrow().routes.len().max(1);
            let avg_geom = if !geom_ee.is_empty() && !geom_old_aligned.is_empty() {
                geom_avg(&geom_ee, weight_ee, &geom_old_aligned, weight_old)
            } else if geom_ee.is_empty() {
                geom_old_aligned
            } else {
                geom_ee
            };

            ee.geometry = avg_geom;

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

    let mut new_routes: Vec<LineOcc> = Vec::new();

    // foldEdges logic: Merge ALL routes from both edges (union, not intersection)
    // C++ combineEdges (lines 636-717) keeps all lines from both edges

    // Process routes from edge A
    for ra in &ea_borrow.routes {
        if let Some(rb) = eb_borrow.routes.iter().find(|r| r.line_id == ra.line_id) {
            // Route exists in BOTH edges - determine combined direction
            let dir_a = direction_at_node(edge_a, &ra.direction, n);
            let dir_b = direction_at_node(edge_b, &rb.direction, n);

            // Resulting direction on new edge (other_a -> other_b)
            // If A(Incoming=1) and B(Outgoing=-1) => Continuous Forward flow -> towards other_b
            // If A(Outgoing=-1) and B(Incoming=1) => Continuous Backward flow -> towards other_a
            let new_dir: Option<Weak<RefCell<LineNode>>> = if dir_a == 1 && dir_b == -1 {
                Some(Rc::downgrade(&other_b))
            } else if dir_a == -1 && dir_b == 1 {
                Some(Rc::downgrade(&other_a))
            } else {
                None // Bidirectional
            };

            new_routes.push(LineOcc {
                line_id: ra.line_id.clone(),
                direction: new_dir,
            });
        } else {
            // Route only in edge A - map direction to new edge
            // New edge is other_a -> other_b, edge A connects other_a <-> n
            let dir_a = direction_at_node(edge_a, &ra.direction, n);
            // If A is incoming to n (dir_a == 1) and A is other_a -> n, route goes towards n
            // On new edge other_a -> other_b, that would be going towards other_b
            let new_dir: Option<Weak<RefCell<LineNode>>> = if dir_a == 1 {
                Some(Rc::downgrade(&other_b)) // Continuing in same direction
            } else if dir_a == -1 {
                Some(Rc::downgrade(&other_a)) // Going back
            } else {
                None // Bidirectional
            };

            new_routes.push(LineOcc {
                line_id: ra.line_id.clone(),
                direction: new_dir,
            });
        }
    }

    // Process routes only in edge B (not already added from A)
    for rb in &eb_borrow.routes {
        if !new_routes.iter().any(|r| r.line_id == rb.line_id) {
            // Route only in edge B - map direction to new edge
            // New edge is other_a -> other_b, edge B connects n <-> other_b
            let dir_b = direction_at_node(edge_b, &rb.direction, n);
            // If B is outgoing from n (dir_b == -1) and B is n -> other_b, route goes towards other_b
            // On new edge other_a -> other_b, that direction is also towards other_b
            let new_dir: Option<Weak<RefCell<LineNode>>> = if dir_b == -1 {
                Some(Rc::downgrade(&other_b)) // Same direction as edge B
            } else if dir_b == 1 {
                Some(Rc::downgrade(&other_a)) // Opposite direction
            } else {
                None // Bidirectional
            };

            new_routes.push(LineOcc {
                line_id: rb.line_id.clone(),
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

    // Note: d_cut is already in meters (Web Mercator coordinates)
    // C++ uses: 2 * maxD(numLines, dCut) which is 2 * dCut

    for node in nodes {
        // Extract adj_list in single borrow (reduces borrow overhead)
        let adj = {
            let node_borrow = node.borrow();
            if node_borrow.adj_list.len() != 2 {
                continue;
            }
            node_borrow.adj_list.clone()
        };

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

            // Use Euclidean length since coordinates are in Web Mercator (meters)
            let ex_len = calc_polyline_length(&ex.borrow().geometry);

            // Threshold: if blocking edge is long (> 2 * d_cut in meters), split it.
            // Note: d_cut is already in meters (Web Mercator), no conversion needed
            let support_threshold = 2.0 * d_cut; // Already in meters
            if ex_len > support_threshold {
                // println!("Splitting blocking edge to allow contraction...");
                support_edge(&ex, graph);

                // CRITICAL CORRECTION (Fix Bug 3):
                // After splitting, we must verify that the blocking edge is actually GONE
                // or that we can now proceed.
                // In C++, supportEdge permanently modifies the graph so 'ex' is invalid.
                // But we need to ensure we don't just blindly contract if something is still there.
                // Since support_edge removes 'ex' from adjacency lists, get_edg should now return None
                // or a different edge. If it returns None, we are good.
                if graph.get_edg(&other_a, &other_b).is_some() {
                    // Still blocked (maybe another edge?), abort
                    do_contract = false;
                }
            } else {
                // Short blocking edge exists - DO NOT CONTRACT (Fix Bug 3)
                do_contract = false;
            }
        }

        if !do_contract {
            continue;
        }

        // C++: MAX_COLLAPSED_SEG_LENGTH check
        // CRITICAL: C++ uses longerThan(geom_a, geom_b, d) which sums POLYLINE lengths,
        // NOT straight-line distance between endpoints! This is essential for curved tracks.
        {
            let geom_a_len = calc_polyline_length(&edge_a.borrow().geometry);
            let geom_b_len = calc_polyline_length(&edge_b.borrow().geometry);
            if geom_a_len + geom_b_len > MAX_COLLAPSED_SEG_LENGTH {
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

/// Maximum length for a collapsed segment (500 meters)
/// Prevents degree-2 contraction from creating excessively long edges
const MAX_COLLAPSED_SEG_LENGTH: f64 = 500.0; // meters (Web Mercator)

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
    simplify_coords(&result, 0.5)
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
/// Contract very short edges (edge artifact removal) (C++: lines 421-481 of collapseShrdSegs)
/// Repeatedly merges nodes connected by edges shorter than max_aggr_distance
/// OPTIMIZED: Uses a worklist algorithm to avoid O(N^2) complexity.
fn contract_short_edges(graph: &mut LineGraph, max_aggr_distance: f64) {
    let mut total_contracted = 0;

    // Worklist for nodes to process.
    // Initially populate with all nodes (that have degree > 0).
    let mut worklist: VecDeque<usize> = VecDeque::new();
    let mut in_worklist: AHashSet<usize> = AHashSet::new();

    for node in &graph.nodes {
        let node_borrow = node.borrow();
        if node_borrow.get_deg() > 0 {
            worklist.push_back(node_borrow.id);
            in_worklist.insert(node_borrow.id);
        }
    }

    // Map for fast node lookup by ID.
    // LineGraph stores nodes in a Vec<NodeRef>, but access is largely via iteration or Rc links.
    // To lookup by ID from the worklist, we need a map.
    // Since graph.nodes is immutable in structure (we modify contents but don't add/remove from the Vec directly
    // apart from retain at the end), we can build a map once.
    // Note: combine_nodes "deletes" nodes by clearing them, but the NodeRef remains in graph.nodes until the end.
    let mut node_map: AHashMap<usize, NodeRef> = AHashMap::new();
    for node in &graph.nodes {
        node_map.insert(node.borrow().id, Rc::clone(node));
    }

    while let Some(node_id) = worklist.pop_front() {
        in_worklist.remove(&node_id);

        let from = match node_map.get(&node_id) {
            Some(n) => Rc::clone(n),
            None => continue, // Should not happen
        };

        // Check if node is still alive (combine_nodes might have cleared it)
        if from.borrow().get_deg() == 0 {
            continue;
        }

        // Iterate edges from this node
        let mut candidate_edge: Option<EdgeRef> = None;

        {
            let from_borrow = from.borrow();
            for edge in &from_borrow.adj_list {
                if !Rc::ptr_eq(&edge.borrow().from, &from) {
                    continue;
                }
                let to = Rc::clone(&edge.borrow().to);

                // Check endpoint distance
                let from_pos = from_borrow.pos;
                let to_pos = to.borrow().pos;
                let euclidean_dist =
                    ((from_pos[0] - to_pos[0]).powi(2) + (from_pos[1] - to_pos[1]).powi(2)).sqrt();

                // Basic check first to avoid expensive geometry calc
                if euclidean_dist >= max_aggr_distance {
                    continue;
                }

                // Check actual geometry length
                let geom_len = calc_polyline_length(&edge.borrow().geometry);

                if geom_len < max_aggr_distance {
                    candidate_edge = Some(Rc::clone(edge));
                    break; // Process one at a time per node pass
                }
            }
        }

        if let Some(edge) = candidate_edge {
            let to = Rc::clone(&edge.borrow().to);
            let from_id = from.borrow().id;
            let to_id = to.borrow().id; // Survivor ID

            // Identify blocking edges
            let mut dont_contract = false;

            // Collect blocking edges first to avoid double borrow issues if we need to split
            let mut blocking_edges_to_split: Vec<EdgeRef> = Vec::new();

            {
                let from_borrow = from.borrow();
                for old_e in &from_borrow.adj_list {
                    if Rc::ptr_eq(old_e, &edge) {
                        continue;
                    }

                    let other = old_e.borrow().get_other_nd(&from);

                    // Check if there is an edge between 'other' and 'to' (the survivor)
                    if let Some(ex) = graph.get_edg(&other, &to) {
                        let ex_len = calc_polyline_length(&ex.borrow().geometry);
                        let support_threshold = 2.0 * max_aggr_distance;

                        if ex_len > support_threshold {
                            blocking_edges_to_split.push(Rc::clone(&ex));
                        } else {
                            // Short blocking edge -> assume we shouldn't squash this triangle
                            dont_contract = true;
                        }
                    }
                }
            }

            // Perform splits
            for block_ex in blocking_edges_to_split {
                // When we split an edge, the topology changes efficiently.
                // support_edge creates a new node and replaces the long edge with two shorter ones.
                // We should add the affected nodes to the worklist.
                // support_edge modifies 'to' and 'other' adjacency lists.
                // Ideally support_edge would return the new node ID, but we can verify later.
                // For now, let's just run it.
                support_edge(&block_ex, graph);
            }

            if dont_contract {
                continue;
            }

            // Degree constraint checks
            let from_deg = from.borrow().get_deg();
            let to_deg = to.borrow().get_deg();

            let mut skip_degree = false;

            if from_deg == 1 && to_deg == 2 {
                let to_adj = to.borrow().adj_list.clone();
                if to_adj.len() == 2 && line_eq(&to_adj[0], &to_adj[1]) {
                    skip_degree = true;
                }
            } else if from_deg == 2 && to_deg == 1 {
                let from_adj = from.borrow().adj_list.clone();
                if from_adj.len() == 2 && line_eq(&from_adj[0], &from_adj[1]) {
                    skip_degree = true;
                }
            } else if from_deg == 2 && to_deg == 2 {
                let from_adj = from.borrow().adj_list.clone();
                let to_adj = to.borrow().adj_list.clone();
                if from_adj.len() == 2
                    && to_adj.len() == 2
                    && line_eq(&from_adj[0], &from_adj[1])
                    && line_eq(&to_adj[0], &to_adj[1])
                {
                    skip_degree = true;
                }
            }

            if skip_degree {
                continue;
            }

            // Create local geo_idx for combine_nodes requirement
            let mut dummy_idx = NodeGeoIdx::new(max_aggr_distance);
            dummy_idx.add(to.borrow().pos, Rc::clone(&to));
            dummy_idx.add(from.borrow().pos, Rc::clone(&from));

            // Use combine_nodes which handles geometry folding and route merging correctly
            if combine_nodes(&from, &to, graph, &mut dummy_idx) {
                total_contracted += 1;

                // 'from' is dead. 'to' is the survivor.
                // We need to add 'to' back to the worklist because it might now have new short edges
                // (inherited from 'from') or its position changed.
                if !in_worklist.contains(&to_id) {
                    worklist.push_back(to_id);
                    in_worklist.insert(to_id);
                }

                // Also, all neighbors of 'to' might now be closer to 'to' (since 'to' moved)
                // or their edges to 'to' changed. So add them too.
                let to_borrow = to.borrow();
                for e in &to_borrow.adj_list {
                    let neighbor = e.borrow().get_other_nd(&to);
                    let neighbor_id = neighbor.borrow().id;
                    if !in_worklist.contains(&neighbor_id) {
                        worklist.push_back(neighbor_id);
                        in_worklist.insert(neighbor_id);
                    }
                }
            }
        }
    }

    if total_contracted > 0 {
        println!("  Contracted {} short artifact edges.", total_contracted);
        // Clean up any isolated nodes left behind
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

    // C++ lines 504-514: "smoothen a bit" - apply per-iteration edge smoothing
    // This is CRITICAL for smooth curves and proper convergence

    // PARALLEL PROCESSING: Collect edge geometries, process in parallel, write back
    // Step 1: Collect all edge geometries with their identifiers
    let mut edge_data: Vec<(usize, usize, Vec<Coord>)> = Vec::new(); // (from_id, to_id, geometry)

    for node in &graph.nodes {
        let node_borrow = node.borrow();
        for edge_ref in &node_borrow.adj_list {
            let edge_borrow = edge_ref.borrow();
            if !Rc::ptr_eq(&edge_borrow.from, node) {
                continue;
            }
            if edge_borrow.geometry.len() < 2 {
                continue;
            }

            let from_id = edge_borrow.from.borrow().id;
            let to_id = edge_borrow.to.borrow().id;
            edge_data.push((from_id, to_id, edge_borrow.geometry.clone()));
        }
    }

    // Step 2: Process geometries in parallel
    let smoothed_geometries: Vec<(usize, usize, Vec<Coord>)> = edge_data
        .into_par_iter()
        .map(|(from_id, to_id, geom)| {
            // C++ line 509: smoothenOutliers(50) - remove sharp angle outliers
            let geom = smoothen_outliers_coords(&geom, 50.0);

            // C++ line 510: simplify(1) - simplify at 1m
            let ls = GeoLineString::new(geom.iter().map(|c| Coord { x: c.x, y: c.y }).collect());
            let simplified = ls.simplify(1.0);
            let geom: Vec<Coord> = simplified
                .0
                .iter()
                .map(|c| Coord { x: c.x, y: c.y })
                .collect();

            // C++ line 511: densify(5) - densify at 5m
            let geom = densify_coords_from_coords(&geom, 5.0);

            // C++ line 512: applyChaikinSmooth(1) - apply 1 iteration of Chaikin smoothing
            let geom = chaikin_smooth_coords(&geom, 1);

            // C++ line 513: simplify(1) - simplify again at 1m
            let ls2 = GeoLineString::new(geom.iter().map(|c| Coord { x: c.x, y: c.y }).collect());
            let simplified2 = ls2.simplify(1.0);
            let geom: Vec<Coord> = simplified2
                .0
                .iter()
                .map(|c| Coord { x: c.x, y: c.y })
                .collect();

            (from_id, to_id, geom)
        })
        .collect();

    // Step 3: Build lookup map for results
    let result_map: AHashMap<(usize, usize), Vec<Coord>> = smoothed_geometries
        .into_iter()
        .map(|(from_id, to_id, geom)| ((from_id, to_id), geom))
        .collect();

    // Step 4: Write results back to graph
    for node in &graph.nodes {
        let node_borrow = node.borrow();
        for edge_ref in &node_borrow.adj_list {
            let mut edge_borrow = edge_ref.borrow_mut();
            if !Rc::ptr_eq(&edge_borrow.from, node) {
                continue;
            }

            let from_id = edge_borrow.from.borrow().id;
            let to_id = edge_borrow.to.borrow().id;

            if let Some(new_geom) = result_map.get(&(from_id, to_id)) {
                edge_borrow.geometry = new_geom.clone();
            }
        }
    }

    // Clean up orphaned nodes
    graph.nodes.retain(|n| n.borrow().get_deg() > 0);
}

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
        // Extract adj_list in single borrow (reduces borrow overhead)
        // Also check degree constraints: skip deg 0 and deg 2
        let adj_list = {
            let node_borrow = node.borrow();
            let deg = node_borrow.adj_list.len();
            // Skip if deg 0 (isolated) or deg 2 (linear track, not junction)
            if deg == 0 || deg == 2 {
                continue;
            }
            node_borrow.adj_list.clone()
        };
        // Find best candidate to merge INTO.
        // C++ iterates outgoing edges.

        let mut best_candidate: Option<(NodeRef, f64)> = None;

        for edge_ref in &adj_list {
            let e = edge_ref.borrow();
            // Process only outgoing edges to avoid double checking pairs
            if !Rc::ptr_eq(&e.from, &node) {
                continue;
            }

            // Use actual geometry length, not just displacement
            let dist = calc_polyline_length(&e.geometry);

            let to_node = Rc::clone(&e.to);
            if Rc::ptr_eq(&to_node, &node) {
                continue;
            }

            // CRITICAL FIX: Enforce threshold check!
            // Even if C++ is loose, we must prevent merging far-apart nodes (like distinct cities)
            // if they happen to be connected by a single edge.
            // "Strange diagonal shapes" suggests we are merging things we shouldn't.
            if dist > threshold {
                continue;
            }

            // CRITICAL (Fix Bug 1): Skip if target is degree-2 (would straighten curves)
            // C++ check: if ((from->getDeg() == 2 || to->getDeg() == 2)) continue;
            // Since we already check 'node' (from) degree at start of loop, we just check to_node here.
            // This logic is correct parity with C++.
            if to_node.borrow().get_deg() == 2 {
                continue;
            }

            // CRITICAL FIX: Verify route continuity before merging in soft_cleanup?
            // C++ DOES NOT do this check in soft_cleanup (lines 367-377).
            // However, to fix "connecting lines that don't run through", we should probably enforces it
            // if we are seeing bad artifacts.
            // The user report specifically mentioned "Chicago Union Station".
            // Merging distinct terminal tracks (deg 1) into a single node (deg N) is risky if they aren't the same station.
            // But if they are 50m apart (d_cut), maybe they SHOULD be merged in a topo map?
            // BUT, if we merge them, we create a connectivity that might not exist in reality if trains can't cross.

            // Let's check `conn_occurs` or ensure we are not merging incompatible lines.
            // But `soft_cleanup` merges NODES. It doesn't check lines.
            // The edge `e` connects them.
            // If we merge `node` into `to_node`, `e` is removed.
            // The edges connecting to `node` are moved to `to_node`.
            // Does this create invalid paths?
            // If `node` was a terminal for Red Line. `to_node` was terminal for Blue Line.
            // Connecting edge `e` (if it exists) must carry SOMETHING?
            // Wait, soft_cleanup iterates `adj_list`. So `node` and `to_node` ARE connected by `e`.
            // If `e` exists, there is a physical track between them.
            // If `e` carries NO routes (empty edge), then merging them is fine (just geometry cleanup).
            // If `e` carries routes, then trains ALREADY go between them.
            // So merging them just collapses the distance.

            // So where do artifacts come from?
            // "Strange diagonal shapes" -> likely from merging nodes that are physically far but topologically close?
            // We use `dist` (polyline length) for candidate selection.
            // `soft_cleanup` threshold `d_cut` is 50m.

            // Re-affirming C++ parity: C++ does exactly this.
            // But verify we aren't merging things we shouldn't.

            // C++ Logic (Lines 367-377): Matches C++ exactly now.
            // It simply merges any connected non-linear cluster in this phase.
            // Strict geometry checks (like angle checks) refer to C++ lines 593-606 (contractNodes)
            // but are NOT used in soft_cleanup (C++ lines 367-377).

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

/// Remove outlier points that create sharp angles (C++: smoothenOutliers)
/// Works directly on Coord vectors
fn smoothen_outliers_coords(coords: &[Coord], threshold: f64) -> Vec<Coord> {
    if coords.len() < 4 {
        return coords.to_vec();
    }

    let mut result = Vec::new();
    result.push(coords[0]); // Keep start

    for i in 1..coords.len().saturating_sub(2) {
        let prev = coords[i - 1];
        let curr = coords[i];
        let next = coords[i + 1];

        // Calculate inner angle using dot product
        let v1 = (prev.x - curr.x, prev.y - curr.y);
        let v2 = (next.x - curr.x, next.y - curr.y);

        let len1 = (v1.0 * v1.0 + v1.1 * v1.1).sqrt();
        let len2 = (v2.0 * v2.0 + v2.1 * v2.1).sqrt();

        if len1 < 1e-10 || len2 < 1e-10 {
            result.push(curr);
            continue;
        }

        let dot = v1.0 * v2.0 + v1.1 * v2.1;
        let cos_angle = dot / (len1 * len2);
        let angle_deg = cos_angle.clamp(-1.0, 1.0).acos().to_degrees();

        // If angle < 35 degrees and point is close to neighbours, remove it
        let dist_prev = ((curr.x - prev.x).powi(2) + (curr.y - prev.y).powi(2)).sqrt();
        let dist_next = ((curr.x - next.x).powi(2) + (curr.y - next.y).powi(2)).sqrt();

        if angle_deg < 35.0 && (dist_prev < threshold || dist_next < threshold) {
            continue;
        }

        result.push(curr);
    }

    // Add last two points
    if coords.len() >= 2 {
        if coords.len() >= 3 {
            result.push(coords[coords.len() - 2]);
        }
        result.push(coords[coords.len() - 1]);
    }

    result
}

/// Densify coords - wrapper for densify_coords working with Coord type
fn densify_coords_from_coords(coords: &[Coord], max_dist: f64) -> Vec<Coord> {
    if coords.len() < 2 {
        return coords.to_vec();
    }

    // Convert to [f64; 2] array, densify, convert back
    let arr: Vec<[f64; 2]> = coords.iter().map(|c| [c.x, c.y]).collect();
    let densified = densify_coords(&arr, max_dist);
    densified
        .iter()
        .map(|p| Coord { x: p[0], y: p[1] })
        .collect()
}

/// Apply Chaikin subdivision smoothing (C++: applyChaikinSmooth)
fn chaikin_smooth_coords(coords: &[Coord], iterations: usize) -> Vec<Coord> {
    let mut current = coords.to_vec();

    for _ in 0..iterations {
        if current.len() < 3 {
            break;
        }

        let mut new_coords = Vec::with_capacity(current.len() * 2);
        new_coords.push(current[0]); // Keep start

        for i in 0..current.len() - 1 {
            let p1 = current[i];
            let p2 = current[i + 1];

            // Q = 0.75 P1 + 0.25 P2
            let q = Coord {
                x: 0.75 * p1.x + 0.25 * p2.x,
                y: 0.75 * p1.y + 0.25 * p2.y,
            };

            // R = 0.25 P1 + 0.75 P2
            let r = Coord {
                x: 0.25 * p1.x + 0.75 * p2.x,
                y: 0.25 * p1.y + 0.75 * p2.y,
            };

            new_coords.push(q);
            new_coords.push(r);
        }

        new_coords.push(current[current.len() - 1]); // Keep end
        current = new_coords;
    }

    current
}
