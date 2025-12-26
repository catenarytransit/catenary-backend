use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rstar::{AABB, RTree, RTreeObject};
use std::collections::{HashMap, HashSet, VecDeque};

/// Build the Support Graph from raw Shapes (Thesis Section 3.2).
///
/// This ignores stops initially and focuses on merging the physical geometry of lines.
pub async fn build_support_graph(pool: &CatenaryPostgresPool) -> Result<Vec<GraphEdge>> {
    let mut conn = pool.get().await?;

    // 1. Load ALL Shapes that are used by routes (rail/subway)
    // We filter by route_type to avoid processing bus shapes if not needed,
    // but the thesis implies a global network. We'll stick to rail/tram for now.

    use catenary::schema::gtfs::shapes::dsl::*;

    // We realistically need shapes that are actually used by trips.
    // For now, load strictly defined rail shapes.
    let loaded_shapes = shapes
        .filter(route_type.eq_any(vec![0, 1, 2])) // Tram, Subway, Rail
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    println!(
        "Loaded {} raw shapes for Support Graph.",
        loaded_shapes.len()
    );

    // 2. Convert Shapes to "Long Edges"
    // Each shape is initially one long edge from Start -> End.
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

        // Create a 'dummy' route ID for now, just to track shape source?
        // Actually, we need to know which lines traverse this.
        // For the Support Graph, we just care about geometry merging.
        // We'll attach the ShapeID as a route_id equivalent for now.
        let route_ids = vec![(String::from("shape"), shape.shape_id)];

        raw_edges.push(GraphEdge {
            from: start_node,
            to: end_node,
            geometry: convert_from_geo(&geom),
            route_ids,
            weight: 0.0, // Recalculate
            original_edge_index: None,
        });
    }

    // 3. Collapse Shared Segments
    // This is the core "Map Construction" step from the thesis.
    // We use a small merging threshold (e.g. 5 meters).
    // The thesis mentions "iterative merging".
    let collapsed = collapse_shared_segments(raw_edges, 0.0005, 0.0001, 7, &mut node_id_counter);

    Ok(collapsed)
}

/// The "Map Construction" algorithm (Section 3.2).
/// Merges lines that are geometrically close.
/// NOTE: Includes "Blocking Set" logic to prevent self-intersections.
fn collapse_shared_segments(
    mut edges: Vec<GraphEdge>,
    d_cut: f64,
    seg_len: f64,
    max_iters: usize,
    next_node_id_counter: &mut usize,
) -> Vec<GraphEdge> {
    use itertools::Itertools;
    use std::cmp::Ordering; // Ensure itertools is available or use manual dedup

    #[derive(Debug, Clone, Copy, PartialEq)]
    struct SnappedNode {
        id: usize,
        point: [f64; 2],
    }

    struct SimpleGrid {
        cell_size: f64,
        cells: HashMap<(i32, i32), Vec<usize>>,
        nodes: HashMap<usize, SnappedNode>,
    }

    impl SimpleGrid {
        fn new(cell_size: f64) -> Self {
            Self {
                cell_size,
                cells: HashMap::new(),
                nodes: HashMap::new(),
            }
        }

        fn get_cell_coords(&self, x: f64, y: f64) -> (i32, i32) {
            (
                (x / self.cell_size).floor() as i32,
                (y / self.cell_size).floor() as i32,
            )
        }

        fn insert(&mut self, node: SnappedNode) {
            let coords = self.get_cell_coords(node.point[0], node.point[1]);
            self.cells.entry(coords).or_default().push(node.id);
            self.nodes.insert(node.id, node);
        }

        fn remove(&mut self, id: usize) {
            if let Some(node) = self.nodes.remove(&id) {
                let coords = self.get_cell_coords(node.point[0], node.point[1]);
                if let Some(indices) = self.cells.get_mut(&coords) {
                    if let Some(pos) = indices.iter().position(|&x| x == id) {
                        indices.swap_remove(pos);
                    }
                }
            }
        }

        fn query_callback<F>(&self, x: f64, y: f64, radius: f64, mut callback: F)
        where
            F: FnMut(&SnappedNode),
        {
            let r_sq = radius * radius;
            let min_c = self.get_cell_coords(x - radius, y - radius);
            let max_c = self.get_cell_coords(x + radius, y + radius);

            for cx in min_c.0..=max_c.0 {
                for cy in min_c.1..=max_c.1 {
                    if let Some(indices) = self.cells.get(&(cx, cy)) {
                        for &id in indices {
                            if let Some(node) = self.nodes.get(&id) {
                                let dx = node.point[0] - x;
                                let dy = node.point[1] - y;
                                if dx * dx + dy * dy <= r_sq {
                                    callback(node);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("Building Support Graph via iterative collapse (Topological Mode)...");

    // We only need one major pass of this Topological Construction for the base support graph.
    // Iterating the WHOLE process might be useful if the centroids drift significantly,
    // but typically one pass of Snap -> Construct -> Heal is sufficient for 'Map Construction'.
    // The C++ code iterates 'collapseShrdSegs' which does update positions.
    // We will stick to the loop.

    for iter in 0..max_iters {
        println!("Iteration {}/{}...", iter + 1, max_iters);

        // Sort by weight (length) to prioritize longer, more significant "backbone" edges
        edges.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(Ordering::Equal));

        let mut node_positions: HashMap<usize, (f64, f64, usize)> = HashMap::new();
        let mut node_grid = SimpleGrid::new(d_cut);
        let mut next_node_id = *next_node_id_counter;

        // ---------------------------------------------------------------------
        // PHASE 1: SNAP & DENSIFY
        // Map every input edge to a sequence of Node IDs (centroids).
        // ---------------------------------------------------------------------
        let mut edge_node_sequences: Vec<Vec<usize>> = Vec::with_capacity(edges.len());
        let total_edges = edges.len();
        let log_int = (total_edges / 10).max(1);

        for (i, edge) in edges.iter().enumerate() {
            if i % log_int == 0 {
                // println!("  Snapping edge {}/{}", i, total_edges);
            }

            // Simplify first to remove noise
            let mut geom = convert_to_geo(&edge.geometry);
            use geo::algorithm::simplify::Simplify;
            // 1e-5 ~ 1 meter. Keeps shape but removes jitter.
            let simplified = geom.simplify(1e-5);
            if !simplified.0.is_empty() {
                geom = simplified;
            }

            // Densify to ensure we have points to snap to the grid
            let densified = densify_geometry(&geom, seg_len);

            // Blocking set to prevent an edge from snapping to the same node multiple times locally
            // (prevents collapsing a loop into a single point)
            let blocking_size = (d_cut / seg_len).ceil() as usize + 2;
            let mut blocking_set: VecDeque<usize> = VecDeque::with_capacity(blocking_size);

            let mut path_ids: Vec<usize> = Vec::with_capacity(densified.len());

            for p in densified {
                let pt = [p.x(), p.y()];

                let mut best_match: Option<usize> = None;
                let mut min_dist_sq = d_cut * d_cut;

                node_grid.query_callback(pt[0], pt[1], d_cut, |n| {
                    if blocking_set.contains(&n.id) {
                        return;
                    }
                    let d = (n.point[0] - pt[0]).powi(2) + (n.point[1] - pt[1]).powi(2);
                    if d < min_dist_sq {
                        min_dist_sq = d;
                        best_match = Some(n.id);
                    }
                });

                let final_id = if let Some(bid) = best_match {
                    // Update Centroid
                    let entry = node_positions.entry(bid).or_insert((0.0, 0.0, 0));
                    // If this node came from grid query, it HAS to be in node_positions.
                    // But if it was inserted in THIS loop iteration earlier, it might be.
                    // Recover from grid if count is 0?
                    if entry.2 == 0 {
                        // Should imply it's newor I messed up.
                        // For safety, initialize with current point if 0?
                        // No, if it's in the grid, it has a position.
                        if let Some(n) = node_grid.nodes.get(&bid) {
                            entry.0 = n.point[0];
                            entry.1 = n.point[1];
                            entry.2 = 1;
                        }
                    }

                    entry.0 += pt[0];
                    entry.1 += pt[1];
                    entry.2 += 1;

                    // Update Grid Node Position (Moving Average)
                    let avg_x = entry.0 / entry.2 as f64;
                    let avg_y = entry.1 / entry.2 as f64;

                    // Update grid - optimization: only remove/insert if cell changes or every N times?
                    // Doing strict update:
                    node_grid.remove(bid);
                    node_grid.insert(SnappedNode {
                        id: bid,
                        point: [avg_x, avg_y],
                    });

                    bid
                } else {
                    // Create New Node
                    let id = next_node_id;
                    next_node_id += 1;

                    node_grid.insert(SnappedNode { id, point: pt });
                    node_positions.insert(id, (pt[0], pt[1], 1));
                    id
                };

                path_ids.push(final_id);
                blocking_set.push_back(final_id);
                if blocking_set.len() > blocking_size {
                    blocking_set.pop_front();
                }
            }
            edge_node_sequences.push(path_ids);
        }
        *next_node_id_counter = next_node_id;

        // ---------------------------------------------------------------------
        // PHASE 2: BUILD TOPOLOGICAL GRAPH
        // ---------------------------------------------------------------------
        // Edge key: canonical (min_id, max_id).
        // Value: EdgeData.
        struct EdgeData {
            u: usize,
            v: usize, // u < v is NOT guaranteed here, but constructing from key implies connection
            routes: HashSet<(String, String)>,
            // The geometry of this specific segment.
            // Important: We store it ordered from u to v.
            geometry: Vec<Coord>,
            original_indices: HashSet<usize>,
        }

        let mut graph_edges: HashMap<(usize, usize), EdgeData> = HashMap::new();

        for (idx, seq) in edge_node_sequences.iter().enumerate() {
            // Dedup adjacent identical nodes (e.g. 100, 100, 100 -> 100)
            let mut deduped = Vec::with_capacity(seq.len());
            for &id in seq {
                if deduped.last() != Some(&id) {
                    deduped.push(id);
                }
            }
            if deduped.len() < 2 {
                continue;
            }

            let orig_edge = &edges[idx];

            for window in deduped.windows(2) {
                let u = window[0];
                let v = window[1];

                // Key is canonical
                let key = if u < v { (u, v) } else { (v, u) };

                // Get positions for simple initial geometry
                let pu = node_positions.get(&u).unwrap(); // Should exist
                let pv = node_positions.get(&v).unwrap(); // Should exist
                // Calculate average pos
                let cu = Coord {
                    x: pu.0 / pu.2 as f64,
                    y: pu.1 / pu.2 as f64,
                };
                let cv = Coord {
                    x: pv.0 / pv.2 as f64,
                    y: pv.1 / pv.2 as f64,
                };

                // Create geometry u -> v
                // Currently just straight line segment.
                // In future iterations, we could preserve shape if we weren't just snapping.
                // But since we densified -> snapped, the shape IS the sequence of nodes!
                // So the segment between two adjacent nodes in the sequence is effectively straight.
                let geom_uv = vec![cu, cv];

                let entry = graph_edges.entry(key).or_insert_with(|| {
                    // Initialize with u, v from key context?
                    // Let's store u=key.0, v=key.1 for consistency
                    EdgeData {
                        u: key.0,
                        v: key.1,
                        routes: HashSet::new(),
                        geometry: if key.0 == u {
                            geom_uv.clone()
                        } else {
                            vec![cv, cu]
                        },
                        original_indices: HashSet::new(),
                    }
                });

                // Merge Route IDs
                for r in &orig_edge.route_ids {
                    entry.routes.insert(r.clone());
                }
                if let Some(oid) = orig_edge.original_edge_index {
                    entry.original_indices.insert(oid);
                }
            }
        }

        // ---------------------------------------------------------------------
        // PHASE 3: SIMPLIFY (HEAL DEGREE-2 NODES)
        // ---------------------------------------------------------------------
        println!(
            "  Graph built with {} segments. Simplifying...",
            graph_edges.len()
        );

        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
        for (k, _) in &graph_edges {
            adj.entry(k.0).or_default().push(k.1);
            adj.entry(k.1).or_default().push(k.0);
        }

        let mut changed = true;
        while changed {
            changed = false;
            // Collect nodes to iterate (snapshot)
            let nodes: Vec<usize> = adj.keys().cloned().collect();

            for n in nodes {
                // Check if n is a candidate for removal
                // Must be Degree 2
                // Must have exactly two neighbors p, q
                if let Some(neighbors) = adj.get(&n) {
                    if neighbors.len() == 2 {
                        let p = neighbors[0];
                        let q = neighbors[1];

                        // Get edges (p, n) and (n, q)
                        let key_pn = if p < n { (p, n) } else { (n, p) };
                        let key_nq = if n < q { (n, q) } else { (q, n) };

                        if let (Some(edge_pn), Some(edge_nq)) =
                            (graph_edges.get(&key_pn), graph_edges.get(&key_nq))
                        {
                            // CHECK COMPATIBILITY
                            // C++ checks 'lineEq', effectively matching the set of lines.
                            // We require exact match of routes.
                            if edge_pn.routes == edge_nq.routes {
                                // MERGE -> Create (p, q), Delete n

                                // 1. Construct new geometry p -> q
                                // geom(p -> q) = geom(p -> n) + geom(n -> q)

                                // Helper to get geometry oriented start->end
                                let get_oriented =
                                    |ed: &EdgeData, start: usize, end: usize| -> Vec<Coord> {
                                        if ed.u == start && ed.v == end {
                                            ed.geometry.clone()
                                        } else {
                                            // Reverse
                                            let mut g = ed.geometry.clone();
                                            g.reverse();
                                            g
                                        }
                                    };

                                let g_pn = get_oriented(edge_pn, p, n); // Ends at n
                                let g_nq = get_oriented(edge_nq, n, q); // Starts at n

                                // Combine: g_pn + g_nq (skip the duplicate n point)
                                let mut new_geom = g_pn;
                                if !new_geom.is_empty() && !g_nq.is_empty() {
                                    // new_geom.last() should ~= g_nq.first()
                                    // Remove last of first
                                    new_geom.pop();
                                    new_geom.extend(g_nq);
                                } else if new_geom.is_empty() {
                                    new_geom = g_nq;
                                }

                                // 2. Create new edge data
                                // We take routes from one (they are equal)
                                let routes = edge_pn.routes.clone();
                                let mut orig = edge_pn.original_indices.clone();
                                orig.extend(&edge_nq.original_indices);

                                // 3. Remove old edges
                                graph_edges.remove(&key_pn);
                                graph_edges.remove(&key_nq);

                                // 4. Update Adjacency
                                adj.get_mut(&p).unwrap().retain(|&x| x != n);
                                adj.get_mut(&q).unwrap().retain(|&x| x != n);
                                adj.remove(&n); // Bye bye n

                                // 5. Insert new edge (p, q)
                                let key_pq = if p < q { (p, q) } else { (q, p) };

                                if let Some(existing) = graph_edges.get_mut(&key_pq) {
                                    // Multigraph case: (p, q) already exists?
                                    // This means we formed a loop or there was already a parallel path.
                                    // If we merge into it, we might mix routes.
                                    // For now, let's just Append routes and Geometry?
                                    // No, geometry is physical. If there's an existing edge, it's a physically distinct path?
                                    // Or are we merging parallel lines?
                                    // If we are strictly simplifying degree-2 nodes, (p,n,q) collapsing to (p,q)
                                    // when (p,q) is already there implies a triangle (p,n,q) existed.
                                    // This is rare for transit lines unless track splitting.
                                    // If routes differ, we can't merge safely into one EdgeData unless we support multiple geometries.
                                    // But typically, we just add the new routes and maybe average geometry?
                                    // For simplicity: Add routes. Keep existing geometry? Or Average?
                                    // Let's keep existing geometry to avoid exploding complexity.
                                    existing.routes.extend(routes);
                                    existing.original_indices.extend(orig);
                                } else {
                                    graph_edges.insert(
                                        key_pq,
                                        EdgeData {
                                            u: key_pq.0,
                                            v: key_pq.1,
                                            routes,
                                            geometry: if key_pq.0 == p {
                                                new_geom
                                            } else {
                                                let mut g = new_geom;
                                                g.reverse();
                                                g
                                            },
                                            original_indices: orig,
                                        },
                                    );
                                    // Update adj
                                    adj.entry(p).or_default().push(q);
                                    adj.entry(q).or_default().push(p);
                                }

                                changed = true;
                            }
                        }
                    }
                }
            }
        }

        println!("  Simplification complete.");

        // ---------------------------------------------------------------------
        // PHASE 4: RECONSTRUCT & CHECK CONVERGENCE
        // ---------------------------------------------------------------------
        let mut new_edges = Vec::new();
        let mut len_new = 0.0;

        for data in graph_edges.values() {
            // Convert Coord Vec to LineString
            // Ensure unique points in geom
            if data.geometry.len() < 2 {
                continue;
            }

            let ls = GeoLineString::new(data.geometry.clone());
            let mut weight = 0.0;
            for line in ls.lines() {
                weight += haversine_dist(line.start.into(), line.end.into());
            }

            new_edges.push(GraphEdge {
                from: NodeId::Intersection(data.u),
                to: NodeId::Intersection(data.v),
                geometry: convert_from_geo(&ls),
                route_ids: data.routes.iter().cloned().sorted().collect(), // Sorted for determinism
                weight,
                original_edge_index: None, // Merged
            });
            len_new += weight;
        }

        let len_old: f64 = edges.iter().map(|e| e.weight).sum();
        let diff = (len_old - len_new).abs();
        println!(
            "  Iteration {} complete. Length delta: {:.4}",
            iter + 1,
            diff
        );

        edges = new_edges;
        if diff < 1e-1 {
            // Convergence threshold
            println!("Converged.");
            break;
        }
    }

    edges
}

fn densify_geometry(line: &GeoLineString<f64>, max_seg_len: f64) -> Vec<GeoPoint<f64>> {
    let mut points = Vec::new();
    if line.0.is_empty() {
        return points;
    }

    let coords = &line.0;
    points.push(GeoPoint::from(coords[0]));

    for i in 0..coords.len() - 1 {
        let p1 = GeoPoint::from(coords[i]);
        let p2 = GeoPoint::from(coords[i + 1]);
        let dist = ((p1.x() - p2.x()).powi(2) + (p1.y() - p2.y()).powi(2)).sqrt();

        if dist > max_seg_len {
            let num_segments = (dist / max_seg_len).ceil() as usize;
            for j in 1..num_segments {
                let frac = j as f64 / num_segments as f64;
                let x = p1.x() + (p2.x() - p1.x()) * frac;
                let y = p1.y() + (p2.y() - p1.y()) * frac;
                points.push(GeoPoint::new(x, y));
            }
        }
        points.push(p2);
    }
    points
}

fn haversine_dist(p1: GeoPoint<f64>, p2: GeoPoint<f64>) -> f64 {
    use geo::HaversineDistance;
    p1.haversine_distance(&p2)
}
