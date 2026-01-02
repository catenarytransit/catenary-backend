use crate::coord_conversion::*;
use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use crate::intercity_split::adaptive_densify_spacing;
use crate::partitioning::*;
use crate::validation::*;
use crate::graph_types::*;
use crate::node_id_allocator::{ChunkedNodeIdTracker, NodeIdAllocator};
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
use std::sync::{Arc, Mutex};

// ===========================================================================
// Build Support Graph
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

    // Build map: (chateau, shape_id) -> Vec<(chateau, route_id)>
    // CRITICAL: Key must include chateau, otherwise different agencies with the same
    // shape_id (e.g., both have "1") will have their routes incorrectly combined!
    let mut shape_to_routes: std::collections::HashMap<(String, String), Vec<(String, String)>> =
        std::collections::HashMap::new();
    for m in metas {
        if let (Some(sid), Some(rid)) = (m.gtfs_shape_id, m.route_id) {
            shape_to_routes
                .entry((m.chateau.clone(), sid))
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

        let start_node = NodeId::Intersection(0, node_id_counter);
        node_id_counter += 1;
        let end_node = NodeId::Intersection(0, node_id_counter);
        node_id_counter += 1;

        // Get actual route IDs for this shape (chateau, route_id)
        // Key is now (chateau, shape_id) to prevent cross-agency contamination
        let route_ids = shape_to_routes
            .get(&(shape.chateau.clone(), shape.shape_id.clone()))
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
            routes: route_ids
                .into_iter()
                .map(|(c, r)| (c, r, shape.route_type as i32))
                .collect(),
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
    // VALIDATION: Record original bboxes for corruption detection
    // =========================================================================
    println!("Recording original edge bboxes for validation...");
    let original_bboxes: HashMap<usize, BBox> = raw_edges
        .iter()
        .enumerate()
        .map(|(idx, edge)| (idx, compute_edge_bbox(edge)))
        .collect();
    println!("  Recorded {} bboxes", original_bboxes.len());

    // =========================================================================
    // C++ TopoMain.cpp Pipeline (lines 128-185)
    // =========================================================================
    let tight_d_cut = 10.0; // 10 meters (C++ first pass: dCut=10)
    let normal_d_cut = 50.0; // 50 meters (C++ maxAggrDistance=50)
    let seg_len = 5.0; // 5 meters (C++ segmentLength=5)

    // Snap threshold for building initial graph
    let snap_threshold = 2.0; // 2 meters in Web Mercator

    // =========================================================================
    // SPATIAL PARTITIONING for Country-scale networks
    // Without this, processing as one component causes OOM
    // =========================================================================

    // Step 1: Partition by geography using spatial grid (BEFORE node snapping)
    // Grid size of 50km creates reasonable geographic regions
    let grid_size_meters = 50000.0; // 50km grid cells

    // NOTE: All edges (including long intercity) are processed together.
    // Adaptive densification handles memory by using coarser spacing for long edges.
    // This enables proper parallel track merging for intercity rail corridors.
    let components = partition_by_geography(raw_edges, grid_size_meters, &mut node_id_counter);

    // Step 2: Further split large components using K-means + mode-aware cutting
    let max_edges_per_cluster = 1000; // Target cluster size
    let min_cluster_size = 50; // Minimum cluster size before consolidation
    let mut all_clusters: Vec<Vec<GraphEdge>> = Vec::new();
    for component in components {
        let clusters = partition_large_component(component, max_edges_per_cluster);
        all_clusters.extend(clusters);
    }

    // Step 3: Consolidate small clusters with adjacent ones
    // This is critical because initial edges can be very long (ICE/TGV routes spanning
    // hundreds of km) and K-means may create tiny clusters that explode during densification
    all_clusters = consolidate_small_clusters(all_clusters, min_cluster_size);

    println!(
        "\n=== Processing {} clusters (after consolidation) ===",
        all_clusters.len()
    );

    // Offload clusters to disk to save memory
    println!("Offloading clusters to disk to save memory...");
    let mut partition_store = crate::partition_storage::PartitionStore::new()?;
    let mut partition_handles = Vec::with_capacity(all_clusters.len());
    for cluster in all_clusters {
        partition_handles.push(partition_store.store_partition(&cluster)?);
    }
    // all_clusters is consumed and dropped here, freeing memory

    // Step 4: Process each cluster in PARALLEL
    // Use a shared node ID allocator that dispenses chunks on demand
    // Start with smaller chunks (100K) that can be extended dynamically
    // This is more memory-efficient than allocating 10M IDs upfront
    let allocator = Arc::new(NodeIdAllocator::new(node_id_counter, 100_000));
    let total_clusters = partition_handles.len();

    // Configure thread count via environment variable (default: 2 to limit memory usage)
    let num_threads: usize = std::env::var("GLOBEFLOWER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    println!(
        "Processing {} clusters in parallel with {} threads (set GLOBEFLOWER_THREADS to change)...",
        total_clusters, num_threads
    );

    // Build a custom thread pool with limited threads
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .expect("Failed to create rayon thread pool");

    let results: Vec<Result<Vec<GraphEdge>, anyhow::Error>> = pool.install(|| {
        partition_handles
            .par_iter()
            .enumerate()
            .map(|(cluster_idx, handle)| {
                // Load cluster from disk (thread-safe: each partition is a separate file)
                let cluster: Vec<GraphEdge> = partition_store.load_partition(handle)?;

                println!(
                    "\n=== Processing cluster {}/{}: {} edges ===",
                    cluster_idx + 1,
                    total_clusters,
                    cluster.len()
                );

                // Create a chunked ID tracker for this cluster (dynamic allocation)
                let mut id_tracker = ChunkedNodeIdTracker::new(Arc::clone(&allocator));
                let chunk_start = id_tracker.chunk_start();
                let chunk_end = chunk_start + 100_000 - 1;

                println!(
                    "[Cluster {}] Allocated ID range: {}-{} (chunk size: 100K)",
                    cluster_idx + 1,
                    chunk_start,
                    chunk_end
                );

                // Build initial graph for this cluster
                let mut graph = build_initial_linegraph_webmerc_tracked(
                    &cluster,
                    &mut id_tracker,
                    snap_threshold,
                );

                // Pre-collapse cleanup
                average_node_positions(&mut graph);
                collapse_degree_2_nodes_serial(&mut graph, normal_d_cut);
                clean_up_geoms(&graph);
                contract_short_edges(&mut graph, normal_d_cut);

                // Two passes of collapse (C++ lines 145-146)
                let collapsed = collapse_shared_segments_from_graph_tracked(
                    &graph,
                    tight_d_cut,
                    seg_len,
                    50,
                    &mut id_tracker,
                );
                graph.clear(); // Release memory from initial graph

                let collapsed = collapse_shared_segments_tracked(
                    collapsed,
                    normal_d_cut,
                    seg_len,
                    50,
                    &mut id_tracker,
                );

                let nodes_used = id_tracker.current() - id_tracker.chunk_start();

                // Find min/max node IDs in output to verify uniqueness
                let mut min_id = usize::MAX;
                let mut max_id = usize::MIN;
                for edge in &collapsed {
                    if let NodeId::Intersection(_, id) = edge.from {
                        min_id = min_id.min(id);
                        max_id = max_id.max(id);
                    }
                    if let NodeId::Intersection(_, id) = edge.to {
                        min_id = min_id.min(id);
                        max_id = max_id.max(id);
                    }
                }

                println!(
                    "Cluster {}/{} complete: {} edges, {} nodes used, actual ID range: {}-{}",
                    cluster_idx + 1,
                    total_clusters,
                    collapsed.len(),
                    nodes_used,
                    min_id,
                    max_id
                );

                Ok(collapsed)
            })
            .collect()
    });

    // Collect all results, propagating any errors
    let mut all_collapsed: Vec<GraphEdge> = Vec::new();
    for result in results {
        all_collapsed.extend(result?);
    }

    // Update node_id_counter to reflect all allocations
    node_id_counter = allocator.current();

    println!(
        "\n=== All clusters processed: {} total edges ===",
        all_collapsed.len()
    );
    let mut collapsed = all_collapsed;

    // NOTE: All edges (including long intercity) were processed together.
    // No reattachment needed - adaptive densification handles memory.

    // =========================================================================
    // Post-Collapse Reconstruction (matches C++ TopoMain.cpp lines 164-186)
    // =========================================================================

    // EDGE SANITIZATION: Remove edges with impossible geometry (>500km span)
    // This catches NodeId collisions that create cross-continent edges
    const MAX_REASONABLE_EDGE_SPAN: f64 = 500_000.0; // 500km in meters (Web Mercator)
    let edges_before = collapsed.len();

    let collapsed: Vec<GraphEdge> = collapsed
        .into_iter()
        .filter(|edge| {
            let geom = convert_to_geo(&edge.geometry);
            if geom.0.len() < 2 {
                return true; // Keep edges with minimal geometry (will be fixed later)
            }

            // Check max span of geometry bbox
            let bbox = compute_edge_bbox(edge);
            let width = bbox.2 - bbox.0;
            let height = bbox.3 - bbox.1;

            if width > MAX_REASONABLE_EDGE_SPAN || height > MAX_REASONABLE_EDGE_SPAN {
                // Edge is impossible (e.g., spans France to USA) - remove it
                false
            } else {
                true
            }
        })
        .collect();

    let edges_removed = edges_before - collapsed.len();
    if edges_removed > 0 {
        println!(
            "SANITIZED: Removed {} impossible edges (bbox > 500km)",
            edges_removed
        );
    }

    // First reconstruction pass (C++: line 164)
    println!("First reconstruction pass (post-collapse)...");
    let mut graph = graph_from_edges(collapsed, &mut node_id_counter);

    // VALIDATION: Check for corrupted edges AFTER graph_from_edges
    println!("Checking edges AFTER graph_from_edges for corruption...");
    let mut bad_edges_after_gfe = 0;
    for node in &graph.nodes {
        let node_borrow = node.borrow();
        for edge_ref in &node_borrow.adj_list {
            let edge_borrow = edge_ref.borrow();
            if !Rc::ptr_eq(&edge_borrow.from, node) {
                continue;
            }
            if edge_borrow.geometry.len() >= 2 {
                let first = &edge_borrow.geometry[0];
                let last = &edge_borrow.geometry[edge_borrow.geometry.len() - 1];
                let dx = (last.x - first.x).abs();
                let dy = (last.y - first.y).abs();
                if dx > 50_000.0 || dy > 50_000.0 {
                    bad_edges_after_gfe += 1;
                    if bad_edges_after_gfe <= 3 {
                        eprintln!("AFTER graph_from_edges - Bad edge:");
                        eprintln!("  Edge ID: {}", edge_borrow.id);
                        eprintln!(
                            "  From node: {} pos: ({:.1}, {:.1})",
                            edge_borrow.from.borrow().id,
                            edge_borrow.from.borrow().pos[0],
                            edge_borrow.from.borrow().pos[1]
                        );
                        eprintln!(
                            "  To node: {} pos: ({:.1}, {:.1})",
                            edge_borrow.to.borrow().id,
                            edge_borrow.to.borrow().pos[0],
                            edge_borrow.to.borrow().pos[1]
                        );
                        eprintln!("  Geometry first: ({:.1}, {:.1})", first.x, first.y);
                        eprintln!("  Geometry last: ({:.1}, {:.1})", last.x, last.y);
                    }
                }
            }
        }
    }
    if bad_edges_after_gfe > 0 {
        eprintln!(
            "=== FOUND {} BAD EDGES AFTER graph_from_edges ===",
            bad_edges_after_gfe
        );
    } else {
        println!("  All edges passed validation after graph_from_edges");
    }

    // C++ uses maxAggrDistance here (TopoConfig::maxAggrDistance), not segmentLength.
    reconstruct_intersections(&mut graph, normal_d_cut);

    // Remove orphan lines (C++: line 178)
    println!("Remove orphan lines...");
    remove_orphan_lines(&mut graph);

    // C++: removeNodeArtifacts(true) - contract degree-2 nodes again (line 180)
    // This cleans up new degree-2 nodes formed by reconstruction
    println!("Contract degree-2 nodes (post-collapse)...");
    collapse_degree_2_nodes_serial(&mut graph, normal_d_cut);

    // Second reconstruction pass (C++: line 182)
    println!("Second reconstruction pass (iterative relaxation)...");
    // Run multiple iterations to relax the graph (spring-like smoothing)
    // This further helps reduce "bumpy lines" by allowing nodes to settle
    for i in 0..3 {
        reconstruct_intersections(&mut graph, normal_d_cut);
    }

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

            // CRITICAL: Use original_node_id for boundary connectivity
            let from_node_id = edge_borrow
                .from
                .borrow()
                .original_node_id
                .clone()
                .unwrap_or_else(|| NodeId::Intersection(0, edge_borrow.from.borrow().id));
            let to_node_id = edge_borrow
                .to
                .borrow()
                .original_node_id
                .clone()
                .unwrap_or_else(|| NodeId::Intersection(0, edge_borrow.to.borrow().id));

            collapsed.push(GraphEdge {
                from: from_node_id,
                to: to_node_id,
                geometry: convert_from_geo(&ls),
                routes: edge_borrow
                    .routes
                    .iter()
                    .map(|r| (r.line_id.0.clone(), r.line_id.1.clone(), r.route_type))
                    .collect(),
                original_shape_ids: vec![], // Lost through LineGraph processing
                weight,
                original_edge_index: None,
            });
        }
    }

    // =========================================================================
    // VALIDATION: Check for cross-map jumping (edges with unreasonably large bbox)
    // An edge spanning more than 100km is suspicious (most metro/rail edges are <50km)
    // =========================================================================
    println!("Validating output edges for geometry corruption...");
    let max_reasonable_span = 100_000.0; // 100km in meters (Web Mercator)
    let mut corrupted_edges = 0;
    for (idx, edge) in collapsed.iter().enumerate() {
        let bbox = compute_edge_bbox(edge);
        let width = bbox.2 - bbox.0;
        let height = bbox.3 - bbox.1;

        if width > max_reasonable_span || height > max_reasonable_span {
            corrupted_edges += 1;
            eprintln!("WARNING: Suspicious edge #{} with large bbox:", idx);
            eprintln!(
                "  Bbox: ({:.1}, {:.1}) - ({:.1}, {:.1})",
                bbox.0, bbox.1, bbox.2, bbox.3
            );
            eprintln!("  Size: {:.1}km x {:.1}km", width / 1000.0, height / 1000.0);
            eprintln!("  Routes: {:?}", edge.routes);
            eprintln!("  From: {:?}, To: {:?}", edge.from, edge.to);

            // Print first and last points of geometry for debugging
            let pts = &edge.geometry.points;
            if !pts.is_empty() {
                eprintln!("  First point: ({:.1}, {:.1})", pts[0].x, pts[0].y);
                eprintln!(
                    "  Last point: ({:.1}, {:.1})",
                    pts[pts.len() - 1].x,
                    pts[pts.len() - 1].y
                );
            }
        }
    }

    if corrupted_edges > 0 {
        eprintln!(
            "=== VALIDATION RESULT: {} edges with suspiciously large bboxes out of {} total ===",
            corrupted_edges,
            collapsed.len()
        );
    } else {
        println!("  All {} edges passed bbox validation", collapsed.len());
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
    // CRITICAL: Use new_with_start_id to ensure new nodes created here don't collide
    let mut graph = LineGraph::new_with_start_id(*next_node_id);
    // Use NodeId as key directly to support all variants (Cluster, Intersection, Split) uniquely
    let mut node_map: HashMap<NodeId, NodeRef> = HashMap::new();

    // First pass: Create all nodes
    for edge in &edges {
        // Determine positions (approximate, will be fixed by reconstruction)
        // We use the first/last point of geometry
        let geom = convert_to_geo(&edge.geometry);
        let from_pos = geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
        let to_pos = geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);

        // CRITICAL: Preserve original NodeIds through reconstruction
        node_map.entry(edge.from.clone()).or_insert_with(|| {
            // Create node with original NodeId preserved for boundary connectivity
            graph.add_nd_with_original(from_pos, edge.from.clone())
        });
        node_map
            .entry(edge.to.clone())
            .or_insert_with(|| graph.add_nd_with_original(to_pos, edge.to.clone()));
    }

    graph.next_node_id = graph.next_node_id.max(*next_node_id);
    *next_node_id = graph.next_node_id;

    // Second pass: Create edges
    for edge in edges {
        let from_node = match node_map.get(&edge.from) {
            Some(n) => n,
            None => continue,
        };
        let to_node = match node_map.get(&edge.to) {
            Some(n) => n,
            None => continue,
        };

        // Check if self-loop
        if Rc::ptr_eq(from_node, to_node) {
            continue;
        }

        let new_edge = graph.add_edg(from_node, to_node);
        let geom = convert_to_geo(&edge.geometry);
        new_edge.borrow_mut().geometry = geom.0.iter().map(|c| Coord { x: c.x, y: c.y }).collect();

        // Restore routes
        for (chateau, rid, route_type) in &edge.routes {
            let full_id = (chateau.clone(), rid.clone());
            // Since we lost direction info in GraphEdge export (it's flattened),
            // we have to assume bidirectional here unless we change GraphEdge format.
            // But this function is only used for reconstruction (post-collapse),
            // where direction matters less for topology than during collapse.
            new_edge
                .borrow_mut()
                .routes
                .push(LineOcc::new_bidirectional(full_id, *route_type));
        }
    }

    graph
}

// ===========================================================================
// Build Initial LineGraph from GraphEdges
// ===========================================================================

/// Build initial LineGraph by snapping together geometrically close endpoints.
/// This creates shared nodes where lines meet, matching C++ input format.
fn build_initial_linegraph(edges: &[GraphEdge], next_node_id: &mut usize) -> LineGraph {
    // CRITICAL: Use new_with_start_id to ensure unique node IDs across clusters
    let mut graph = LineGraph::new_with_start_id(*next_node_id);
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
        for (chateau, rid, route_type) in &edge.routes {
            let full_id = (chateau.clone(), rid.clone());
            // Direction points to TO node - shape goes from -> to
            let direction = Some(Rc::downgrade(&to_node));
            new_edge
                .borrow_mut()
                .routes
                .push(LineOcc::new(full_id, *route_type, direction));
        }
    }

    // Propagate counter so subsequent functions get unique IDs
    *next_node_id = graph.next_node_id;

    graph
}

/// Build initial LineGraph with Web Mercator coordinates (meters).
/// snap_threshold is in meters (matching C++ behaviour with projected coordinates).
/// CRITICAL: Uses next_node_id to ensure globally unique node IDs across clusters.
fn build_initial_linegraph_webmerc(
    edges: &[GraphEdge],
    next_node_id: &mut usize,
    snap_threshold: f64,
) -> LineGraph {
    // CRITICAL FIX: Use new_with_start_id to ensure node IDs don't collide across clusters
    let mut graph = LineGraph::new_with_start_id(*next_node_id);

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
        // CRITICAL: Preserve original NodeId for boundary connectivity
        let from_node = {
            if let Some((existing, _)) =
                geo_idx.find_best_in_radius(from_pos, snap_threshold, |_: &NodeRef, _, _, d_sq| {
                    Some(d_sq)
                })
            {
                // When snapping to an existing node, preserve the original NodeId if the existing
                // node doesn't have one yet (first edge wins) or merge if needed
                if existing.borrow().original_node_id.is_none() {
                    existing.borrow_mut().original_node_id = Some(edge.from.clone());
                }
                existing
            } else {
                // Create new node with original NodeId preserved
                let node = graph.add_nd_with_original(from_pos, edge.from.clone());
                geo_idx.add(from_pos, Rc::clone(&node));
                node
            }
        };

        // Find or create TO node
        // CRITICAL: Preserve original NodeId for boundary connectivity
        let to_node = {
            if let Some((existing, _)) =
                geo_idx.find_best_in_radius(to_pos, snap_threshold, |_: &NodeRef, _, _, d_sq| {
                    Some(d_sq)
                })
            {
                // When snapping to an existing node, preserve the original NodeId if the existing
                // node doesn't have one yet (first edge wins)
                if existing.borrow().original_node_id.is_none() {
                    existing.borrow_mut().original_node_id = Some(edge.to.clone());
                }
                existing
            } else {
                // Create new node with original NodeId preserved
                let node = graph.add_nd_with_original(to_pos, edge.to.clone());
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
        for (chateau, rid, route_type) in &edge.routes {
            let full_id = (chateau.clone(), rid.clone());
            // Direction points to TO node - shape goes from -> to
            let direction = Some(Rc::downgrade(&to_node));
            new_edge
                .borrow_mut()
                .routes
                .push(LineOcc::new(full_id, *route_type, direction));
        }
    }

    println!(
        "  Initial graph built with {} nodes and {} edges.",
        graph.nodes.len(),
        graph.num_edges()
    );

    // CRITICAL: Update the counter so subsequent clusters get unique IDs
    *next_node_id = graph.next_node_id;

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

            // CRITICAL FIX: Use original_node_id when available to preserve boundary connectivity.
            // Boundary nodes from cluster splitting have original_node_id set.
            // Internal nodes created during collapse use their internal id.
            let from_node_id = edge_borrow
                .from
                .borrow()
                .original_node_id
                .clone()
                .unwrap_or_else(|| NodeId::Intersection(0, edge_borrow.from.borrow().id));
            let to_node_id = edge_borrow
                .to
                .borrow()
                .original_node_id
                .clone()
                .unwrap_or_else(|| NodeId::Intersection(0, edge_borrow.to.borrow().id));

            edges.push(GraphEdge {
                from: from_node_id,
                to: to_node_id,
                geometry: convert_from_geo(&ls),
                routes: edge_borrow
                    .routes
                    .iter()
                    .map(|r| (r.line_id.0.clone(), r.line_id.1.clone(), r.route_type))
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
// Tracker-based Wrapper Functions
// ===========================================================================

/// Wrapper for build_initial_linegraph_webmerc that uses ChunkedNodeIdTracker.
/// Simply delegates to existing function and syncs the tracker afterward.
fn build_initial_linegraph_webmerc_tracked(
    edges: &[GraphEdge],
    id_tracker: &mut ChunkedNodeIdTracker,
    snap_threshold: f64,
) -> LineGraph {
    let mut next_node_id = id_tracker.current();
    let graph = build_initial_linegraph_webmerc(edges, &mut next_node_id, snap_threshold);

    // Sync tracker to account for IDs used
    while id_tracker.current() < next_node_id {
        id_tracker.next();
    }

    graph
}

/// Wrapper for collapse_shared_segments_from_graph that uses ChunkedNodeIdTracker.
fn collapse_shared_segments_from_graph_tracked(
    graph: &LineGraph,
    d_cut: f64,
    seg_len: f64,
    max_iters: usize,
    id_tracker: &mut ChunkedNodeIdTracker,
) -> Vec<GraphEdge> {
    let mut next_node_id = id_tracker.current();
    let result =
        collapse_shared_segments_from_graph(graph, d_cut, seg_len, max_iters, &mut next_node_id);

    // Sync tracker to account for IDs used
    while id_tracker.current() < next_node_id {
        id_tracker.next();
    }

    result
}

/// Wrapper for collapse_shared_segments that uses ChunkedNodeIdTracker.
fn collapse_shared_segments_tracked(
    edges: Vec<GraphEdge>,
    d_cut: f64,
    seg_len: f64,
    max_iters: usize,
    id_tracker: &mut ChunkedNodeIdTracker,
) -> Vec<GraphEdge> {
    let mut next_node_id = id_tracker.current();
    let result = collapse_shared_segments(edges, d_cut, seg_len, max_iters, &mut next_node_id);

    // Sync tracker to account for IDs used
    while id_tracker.current() < next_node_id {
        id_tracker.next();
    }

    result
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
        // CRITICAL: Initialize with current node ID counter to ensure global uniqueness
        // across partitions. Without this, each partition's graph starts from ID 0,
        // causing edges from completely different geographic regions to connect!
        let mut tg_new = LineGraph::new_with_start_id(*next_node_id_counter);

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
            // CRITICAL FIX: C++ uses e->getFrom()->pl().getGeom() and e->getTo()->pl().getGeom()
            // which are the OLD graph's node positions. These do NOT move during iteration.
            // The Rust code was incorrectly using img_nds (NEW graph positions) which shift
            // as nodes merge, causing incorrect span constraints and triangle artifacts
            // between parallel train lines.
            let geom = convert_to_geo(&edge.geometry);

            // Use ORIGINAL edge geometry endpoints - matches C++ behavior exactly
            // C++ iterates over edges from the OLD graph (_g->getNds()) and reads node
            // positions from the OLD graph nodes (e->getFrom()->pl().getGeom()).
            // The old graph nodes don't move, so we use geometry endpoints directly.
            let from_pos = geom.0.first().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);
            let to_pos = geom.0.last().map(|c| [c.x, c.y]).unwrap_or([0.0, 0.0]);

            // Build polyline from geometry (C++ lines 237-245)
            // CRITICAL FIX: Unlike C++ where e->pl().getGeom() is INNER geometry only (without endpoints),
            // our edge.geometry ALREADY INCLUDES the endpoints. So we just use the geometry directly
            // without adding from_pos/to_pos which would cause duplicate first/last points.
            // Duplicate points can cause issues with simplification and densification.
            let pl: Vec<[f64; 2]> = geom.0.iter().map(|c| [c.x, c.y]).collect();

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

            // ADAPTIVE DENSIFICATION: Use coarser spacing for long edges to bound memory
            // Short edges (<5km): 5m spacing for accurate curves
            // Long edges (>5km): Spacing proportional to length (max ~2000 points)
            let edge_spacing = adaptive_densify_spacing(edge.weight);
            let pl_dense = densify_coords(&coords, edge_spacing);

            // CRITICAL FIX: Ensure we have at least 2 points (start and end) to form a valid edge.
            // If simplification + densification produces fewer than 2 points, fall back to
            // the original FROM and TO positions. This prevents edge gaps when geometry is
            // very short or gets overly simplified (e.g., at loop junctions like CTA Brown Line).
            let pl_dense = if pl_dense.len() < 2 {
                vec![from_pos, to_pos]
            } else {
                pl_dense
            };

            // NOTE: C++ has no point cap - simplify(0.5) should already reduce density enough

            // Debug logging removed for performance

            let num_lines = edge.routes.len();
            let mut i = 0;
            let pl_len = pl_dense.len();
            let mut front: Option<NodeRef> = None; // For FROM coverage handling (matches C++ 'front' pointer)
            let mut img_from_covered = false;
            let mut img_to_covered = false;
            let mut previous_point: Option<[f64; 2]> = None; // Track previous point for direction computation

            // C++ line 232: back = e->getTo() - use original TO endpoint position
            // CRITICAL FIX: Use to_pos directly (original geometry endpoint) rather than
            // pl_dense.last() which may drift during simplification/densification.
            // This ensures the span constraint uses the exact edge endpoint, matching C++.
            let to_endpoint_pos = to_pos;

            for point in &pl_dense {
                // For span constraints: use front_pos (first node of this path)
                // and to_endpoint_pos (original TO endpoint, cleared at last point)
                // This matches C++: front and back are passed to ndCollapseCand
                // IMPORTANT: In C++ 'front' is a node pointer whose position may shift
                // during collapse (ndCollapseCand moves nodes toward centroids). Using
                // a cached position here weakens the span constraint and can create
                // spurious snaps/"knots".
                let span_a_pos = front.as_ref().map(|n| n.borrow().pos);
                let span_b_pos = if i == pl_len - 1 {
                    None
                } else {
                    Some(to_endpoint_pos)
                };

                // Compute incoming direction from previous point to current point
                let incoming_dir =
                    previous_point.map(|prev| [point[0] - prev[0], point[1] - prev[1]]);

                // Collect current edge route modes for compatibility check
                let current_modes: AHashSet<i32> =
                    edge.routes.iter().map(|(_, _, rt)| *rt).collect();

                // Find or create node with span constraints (matches C++: ndCollapseCand)
                let cur = nd_collapse_cand(
                    &my_nds,
                    &current_modes, // Pass current modes
                    num_lines,
                    d_cut,
                    *point,
                    span_a_pos,
                    span_b_pos,
                    incoming_dir,
                    &mut geo_idx,
                    &mut tg_new,
                );

                // Update previous_point for next iteration
                previous_point = Some(*point);

                // Track image nodes for FROM/TO
                // CRITICAL FIX: Also set original_node_id on the collapsed node
                // so that boundary NodeIds are preserved through the collapse algorithm.
                // Without this, internal nodes get new IDs that can collide across clusters.
                if i == 0 {
                    if !img_nds.contains_key(&edge.from) {
                        // Set original_node_id on the collapsed node for boundary preservation
                        if cur.borrow().original_node_id.is_none() {
                            cur.borrow_mut().original_node_id = Some(edge.from.clone());
                        }
                        img_nds.insert(edge.from.clone(), Rc::clone(&cur));
                        img_nds_set.insert(cur.borrow().id);
                        img_from_covered = true;
                    }
                }
                if i == pl_len - 1 {
                    if !img_nds.contains_key(&edge.to) {
                        // Set original_node_id on the collapsed node for boundary preservation
                        if cur.borrow().original_node_id.is_none() {
                            cur.borrow_mut().original_node_id = Some(edge.to.clone());
                        }
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
                    // New edge target is `cur`. Route should point to `cur`.
                    for (chateau, rid, route_type) in &edge.routes {
                        let full_id = (chateau.clone(), rid.clone());

                        // Check if already exists with same direction
                        // We use strict tuple equality now
                        if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                            // Direction points to `cur` (TO node of this segment)
                            let direction = Some(Rc::downgrade(&cur));
                            new_e.borrow_mut().routes.push(LineOcc::new(
                                full_id,
                                *route_type,
                                direction,
                            ));
                        }
                    }
                }

                // Track front node (C++: if (!front) front = cur)
                if front.is_none() {
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
                        for (chateau, rid, route_type) in &edge.routes {
                            let full_id = (chateau.clone(), rid.clone());
                            if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                                // Direction points to `front` (target of this segment)
                                let direction = Some(Rc::downgrade(front_node));
                                new_e.borrow_mut().routes.push(LineOcc::new(
                                    full_id,
                                    *route_type,
                                    direction,
                                ));
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
                        for (chateau, rid, route_type) in &edge.routes {
                            let full_id = (chateau.clone(), rid.clone());
                            if !new_e.borrow().routes.iter().any(|r| r.line_id == full_id) {
                                // Direction points to `to_node` (target of this segment)
                                let direction = Some(Rc::downgrade(to_node));
                                new_e.borrow_mut().routes.push(LineOcc::new(
                                    full_id,
                                    *route_type,
                                    direction,
                                ));
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

                    // FIX BUG 1: Also check edge GEOMETRY length, not just node distance
                    // C++ uses dist() between node positions, but the edge geometry may be
                    // much longer (curved path). Only merge if actual edge is short.
                    let edge_geom_len = calc_polyline_length(&edge_ref.borrow().geometry);

                    if d_cur <= d_min && edge_geom_len <= seg_len {
                        d_min = d_cur;
                        comb = Some(Rc::clone(&b));
                    }
                }

                // Merge affected node into comb (C++ line 362: combineNodes(a, comb, &tgNew))
                if let Some(comb_node) = comb {
                    if !Rc::ptr_eq(affected_node, &comb_node) {
                        // FIX BUG 2: Check for blocking edges with vastly different geometry
                        // before calling combine_nodes. This prevents creating triangular
                        // artifacts when merging creates shortcut edges.
                        let mut can_merge = true;
                        let affected_adj = affected_node.borrow().adj_list.clone();

                        for other_e in &affected_adj {
                            let other = other_e.borrow().get_other_nd(affected_node);
                            // Skip the connecting edge to comb_node
                            if Rc::ptr_eq(&other, &comb_node) {
                                continue;
                            }

                            // Check if comb_node already has an edge to 'other'
                            if let Some(existing) = tg_new.get_edg(&comb_node, &other) {
                                let ex_len = calc_polyline_length(&existing.borrow().geometry);
                                let oe_len = calc_polyline_length(&other_e.borrow().geometry);
                                // If geometry lengths differ by more than 2*d_cut, don't merge
                                // This matches C++ contractNodes check (lines 593-606) using maxAggrDistance * 2
                                if (ex_len - oe_len).abs() > 2.0 * d_cut {
                                    can_merge = false;
                                    break;
                                }
                            }
                        }

                        if can_merge {
                            if combine_nodes(affected_node, &comb_node, &mut tg_new, &mut geo_idx) {
                                // Successfully merged
                            }
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
        soft_cleanup(&mut tg_new, d_cut);

        // =====================================================================
        // Phase 2: Write edge geoms (C++: lines 379-387)
        // SET ALL EDGES TO STRAIGHT LINES between their endpoints
        // This is critical - geometries are simplified to straight lines
        // =====================================================================
        // =====================================================================
        // Phase 2: Write edge geoms (C++: lines 379-387)
        // UPDATE edge geoms to project to node positions
        // FIX 3: Preserve curve geometry for longer edges (>50m)
        // Only reset short edges to straight lines
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

                // Calculate edge length to decide whether to preserve geometry
                let edge_len = calc_polyline_length(&edge_ref.borrow().geometry);

                if edge_len < 50.0 {
                    // Short edge - reset to straight line
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
                } else {
                    // Longer edge - update endpoints but preserve interior curve geometry
                    let mut geom = edge_ref.borrow().geometry.clone();
                    if geom.len() >= 2 {
                        geom[0] = Coord {
                            x: from_pos[0],
                            y: from_pos[1],
                        };
                        let last_idx = geom.len() - 1;
                        geom[last_idx] = Coord {
                            x: to_pos[0],
                            y: to_pos[1],
                        };
                        edge_ref.borrow_mut().geometry = geom;
                    }
                }
            }
        }

        // Phase 3: Re-collapse (C++: lines 389-419)
        // Contract degree-2 nodes
        collapse_degree_2_nodes_serial(&mut tg_new, d_cut);

        // Phase 4: Edge artifact removal (C++: lines 421-481)
        // NOTE: Our worklist-based contract_short_edges already iterates internally
        // until convergence. The C++ do/while pattern is implemented INSIDE the function.
        // We should NOT wrap it in another loop - that causes infinite cycling.
        println!("Contracting short edges...");
        contract_short_edges(&mut tg_new, d_cut);

        // Phase 4.5: Stabilize intercity rail corridors
        // Look ahead 500m to detect and fix flip-flop patterns where parallel
        // intercity rail tracks split and re-merge rapidly
        println!("Stabilizing intercity rail corridors...");
        stabilize_intercity_rail_corridors(&mut tg_new, 500.0);

        // Phase 4.75: Fold duplicate/parallel edges (C++: foldEdges)
        // Critical for rail: merges parallel tracks between the same nodes into a
        // single averaged geometry and combined route set.
        println!("Folding parallel edges...");
        fold_parallel_edges(&mut tg_new);

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
                    from: NodeId::Intersection(0, from_id),
                    to: NodeId::Intersection(0, to_id),
                    geometry: convert_from_geo(&ls),
                    // Map back strictly to tuples.
                    // Note: Direction info is effectively lost in GraphEdge export
                    // if GraphEdge doesn't support it, but it was used for simplification.
                    routes: edge_borrow
                        .routes
                        .iter()
                        .map(|r| (r.line_id.0.clone(), r.line_id.1.clone(), r.route_type))
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

        // original C++ delta 0.002
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
    current_modes: &AHashSet<i32>,
    num_lines: usize,
    d_cut: f64,
    point: [f64; 2],
    span_a_pos: Option<[f64; 2]>,
    span_b_pos: Option<[f64; 2]>,
    incoming_dir: Option<[f64; 2]>, // Direction vector from previous point to current point
    geo_idx: &mut NodeGeoIdx,
    graph: &mut LineGraph,
) -> NodeRef {
    nd_collapse_cand_impl(
        blocking_set,
        current_modes,
        num_lines,
        d_cut,
        point,
        span_a_pos,
        span_b_pos,
        incoming_dir,
        geo_idx,
        graph,
    )
}

fn nd_collapse_cand_impl(
    blocking_set: &AHashSet<usize>,
    current_modes: &AHashSet<i32>,
    num_lines: usize,
    d_cut: f64,
    point: [f64; 2],
    span_a_pos: Option<[f64; 2]>,
    span_b_pos: Option<[f64; 2]>,
    _incoming_dir: Option<[f64; 2]>, // Unused - mode separation moved to line_eq
    geo_idx: &mut NodeGeoIdx,
    graph: &mut LineGraph,
) -> NodeRef {
    // Use line-aware distance cutoff (C++ maxD - currently just returns d_cut)
    let effective_d_cut = max_d(num_lines, d_cut);

    // C++ lines 90-96: Calculate distance constraints from span endpoints
    // A candidate can only be accepted if it's closer to the point than the
    // span endpoints are.
    // C++ uses dist() / sqrt(2.0), which for squared distances equals dist² / 2.0
    // This creates a ~45 degree acceptance cone for merging candidates.
    let d_span_a_sq = if let Some(sa_pos) = span_a_pos {
        let dx = point[0] - sa_pos[0];
        let dy = point[1] - sa_pos[1];
        (dx * dx + dy * dy) / 2.0 // Match C++: sqrt(2)² = 2.0
    } else {
        f64::INFINITY
    };

    let d_span_b_sq = if let Some(sb_pos) = span_b_pos {
        let dx = point[0] - sb_pos[0];
        let dy = point[1] - sb_pos[1];
        (dx * dx + dy * dy) / 2.0 // Match C++: sqrt(2)² = 2.0
    } else {
        f64::INFINITY
    };

    let d_cut_sq = effective_d_cut * effective_d_cut;

    let incoming_dir = _incoming_dir; // Now used for Fix 2

    // Use callback-based search to avoid Vec allocation (major perf win)
    let best = geo_idx.find_best_in_radius(
        point,
        effective_d_cut,
        |node: &NodeRef, nd_id, _nd_pos, dist_sq| {
            // Skip nodes in blocking set (already on this edge's path)
            if blocking_set.contains(&nd_id) {
                return None;
            }

            // C++: if (ndTest->getDeg() == 0) continue;
            // In Rust we keep NodeRefs around until Vec::retain, so we must
            // explicitly avoid collapsing into "deleted" nodes.
            if node.borrow().get_deg() == 0 {
                return None;
            }

            // CROSS-INTERSECTION CHECK (Fix 2)
            // Prevent merging if this would create a plus/X crossing where
            // the candidate node's edges are perpendicular to our incoming direction
            if let Some(inc_dir) = incoming_dir {
                let inc_len = (inc_dir[0] * inc_dir[0] + inc_dir[1] * inc_dir[1]).sqrt();
                if inc_len > 1e-6 {
                    let in_norm = (inc_dir[0] / inc_len, inc_dir[1] / inc_len);

                    // Check angles with all edges at candidate node
                    for edge_ref in &node.borrow().adj_list {
                        let edge_dir = get_edge_direction_from_node(edge_ref, node);

                        // Calculate cross product for perpendicularity
                        let cross = (in_norm.0 * edge_dir.1 - in_norm.1 * edge_dir.0).abs();
                        let dot = (in_norm.0 * edge_dir.0 + in_norm.1 * edge_dir.1).abs();

                        // Check for perpendicular crossing (cross > 0.85, dot < 0.5)
                        if cross > 0.85 && dot < 0.5 {
                            // Check if lines CONVERGE (turn into each other) ahead
                            // by traversing and checking direction changes
                            let lines_converge = check_lines_converge(edge_ref, in_norm, graph);

                            if !lines_converge {
                                // True perpendicular X-crossing - don't merge
                                return None;
                            }
                        }
                    }
                }
            }

            // HYSTERESIS - Check if this candidate node already has routes
            // with matching modes. If so, use RELAXED thresholds to create "sticky"
            // behavior that prevents oscillation between merge states.
            //
            // The idea: once a node starts accumulating routes of a certain mode,
            // it becomes "sticky" for that mode - easier to keep merging similar modes,
            // preventing the zigzag where a node alternates between being merged/not-merged.
            let mut node_modes = AHashSet::new();
            for edge_ref in &node.borrow().adj_list {
                for occ in &edge_ref.borrow().routes {
                    node_modes.insert(occ.route_type);
                }
            }

            let has_shared_mode = !current_modes.is_empty()
                && !node_modes.is_empty()
                && current_modes.intersection(&node_modes).next().is_some();

            // Check if we're dealing with intercity rail (route_type 2)
            // Intercity rail has wider corridors and needs larger merge thresholds
            let has_intercity_rail = current_modes.contains(&2) || node_modes.contains(&2);

            // Base threshold multiplier for intercity rail (corridors are much wider)
            let mode_base_multiplier = if has_intercity_rail { 16.0 } else { 1.0 }; // 4x distance = 16x squared

            // Determine threshold based on hysteresis state
            // With weighted centroid, we can be more relaxed on thresholds
            // The weighted averaging will stabilize positions naturally
            //
            // ANTI-FLIP-FLOP: For parallel tracks, use stronger hysteresis.
            // Check merge_count to see if this node is already established (has absorbed points).
            // Established nodes get even stickier behavior to prevent rapid state changes.
            let merge_count = node.borrow().merge_count;
            let is_established_node = merge_count >= 2;

            let threshold_sq = if has_shared_mode {
                // Node already has matching modes - use RELAXED threshold
                // Intercity rail: 4x base * 8x hysteresis = 32x total (1024x squared)
                // Other modes: 1x base * 2x hysteresis = 2x total (4x squared)
                let hysteresis_mult = if is_established_node {
                    // Strongly established - very sticky
                    // "Make Y shaped merges deeper" -> Increase capture radius for Rail
                    if has_intercity_rail { 64.0 } else { 4.0 } // 8² or 2²
                } else {
                    // Newly merged - moderately sticky
                    if has_intercity_rail { 36.0 } else { 2.25 } // 6² or 1.5²
                };
                d_cut_sq * mode_base_multiplier * hysteresis_mult
            } else if !current_modes.is_empty() && !node_modes.is_empty() {
                // Node has DIFFERENT modes only - use VERY strict threshold (~0.316x)
                // This strongly discourages rail/subway cross-mode merges
                // For intercity rail crossing other modes, still be strict
                d_cut_sq * 0.1 // sqrt(0.1) ≈ 0.316x normal distance
            } else {
                // Empty modes or new node - use base threshold (with mode multiplier)
                d_cut_sq * mode_base_multiplier
            };

            // C++ line 104: d < dSpanA && d < dSpanB && d < dMax && d < dBest
            // Compare squared distances to avoid sqrt
            if dist_sq < d_span_a_sq && dist_sq < d_span_b_sq && dist_sq < threshold_sq {
                Some(dist_sq) // Score by distance (lower = better)
            } else {
                None
            }
        },
    );

    if let Some((nd_min, _score)) = best {
        // Update node position using WEIGHTED average (C++ uses midpoint, but weighted is smoother)
        // User reported "bumpy lines", which suggests the simple iterative midpoint update
        // essentially does an Exponential Moving Average biased to the last points.
        // Weighted average based on number of merged points ensures geometric stability.

        let old_pos = nd_min.borrow().pos;
        let count = nd_min.borrow().merge_count as f64;

        // New centroid = (old * count + new) / (count + 1)
        let new_pos = [
            (old_pos[0] * count + point[0]) / (count + 1.0),
            (old_pos[1] * count + point[1]) / (count + 1.0),
        ];

        // ALWAYS remove and re-add (matching C++ behavior)
        geo_idx.remove(&nd_min);
        nd_min.borrow_mut().pos = new_pos;
        nd_min.borrow_mut().merge_count += 1;
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

    // Update b's position using simple MIDPOINT centroid (Match C++ MapConstructor::combineNodes)
    // C++ line 761: b->pl().setGeom(util::geo::centroid(util::geo::LineSegment<double>(*a->pl().getGeom(), *b->pl().getGeom())));
    let a_pos = a.borrow().pos;
    let b_pos = b.borrow().pos;

    let new_pos = [(a_pos[0] + b_pos[0]) / 2.0, (a_pos[1] + b_pos[1]) / 2.0];

    geo_idx.remove(b);
    b.borrow_mut().pos = new_pos;
    b.borrow_mut().merge_count += a.borrow().merge_count; // Sum counts (though unused for pos now)
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
            // Safety check: existing_edge cannot be the same as old_edge (would cause RefCell panic)
            if Rc::ptr_eq(&existing_edge, old_edge) {
                continue;
            }

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
            // Create new edge from b to other_node
            // FIX: Update geometry endpoint to match new 'from' node position
            let mut geometry = old_edge.borrow().geometry.clone();
            if !geometry.is_empty() {
                let b_pos = b.borrow().pos;
                geometry[0] = Coord {
                    x: b_pos[0],
                    y: b_pos[1],
                };
            }

            let new_edge = Rc::new(RefCell::new(LineEdge {
                id: 0, // ID will be regenerated if needed
                from: Rc::clone(b),
                to: Rc::clone(&other_node),
                routes: old_edge.borrow().routes.clone(),
                geometry,
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
            // Safety check: existing_edge cannot be the same as old_edge (would cause RefCell panic)
            if Rc::ptr_eq(&existing_edge, old_edge) {
                continue;
            }

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
            // Create new edge from other_node to b
            // FIX: Update geometry endpoint to match new 'to' node position
            let mut geometry = old_edge.borrow().geometry.clone();
            if !geometry.is_empty() {
                let b_pos = b.borrow().pos;
                let last_idx = geometry.len() - 1;
                geometry[last_idx] = Coord {
                    x: b_pos[0],
                    y: b_pos[1],
                };
            }

            let new_edge = Rc::new(RefCell::new(LineEdge {
                id: 0, // ID will be regenerated if needed
                from: Rc::clone(&other_node),
                to: Rc::clone(b),
                routes: old_edge.borrow().routes.clone(),
                geometry,
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

    // PROPAGATE CONNECTION EXCEPTIONS from A to B
    // Matches C++ LineGraph::combineNodes lines 1191-1194
    // When we merge node A into B, any connection exceptions at A should be
    // transferred to B so that forbidden connections remain forbidden.
    {
        let a_conn_exc = a.borrow().conn_exc.clone();
        let mut b_mut = b.borrow_mut();
        for (line_id, exc_map) in a_conn_exc {
            for (from_edge_id, forbidden_set) in exc_map {
                for to_edge_id in forbidden_set {
                    b_mut.add_conn_exc(&line_id, from_edge_id, to_edge_id);
                }
            }
        }
    }

    // Remove 'a' from geo_idx and clear its adjacency list (effectively "deleting" it)
    geo_idx.remove(a);
    a.borrow_mut().adj_list.clear();

    true
}

/// Check if a connection occurs at a node for a given line between two edges.
/// Matches C++ LineNodePL::connOccurs (LineNodePL.cpp:128-137)
///
/// Returns true unless there's an explicit connection exception stored at the node.
fn conn_occurs(
    node: &NodeRef,
    line_id: &(String, String),
    edge_a: &EdgeRef,
    edge_b: &EdgeRef,
) -> bool {
    // First check if line exists on both edges
    let has_a = edge_a.borrow().routes.iter().any(|r| r.line_id == *line_id);
    let has_b = edge_b.borrow().routes.iter().any(|r| r.line_id == *line_id);
    if !has_a || !has_b {
        return false;
    }

    // Then check if there's an exception at this node for this line
    let edge_a_id = edge_a.borrow().id;
    let edge_b_id = edge_b.borrow().id;
    node.borrow()
        .conn_occurs_check(line_id, edge_a_id, edge_b_id)
}

/// Get the normalized direction vector from a node along an edge (towards the edge interior).
/// Returns (dx, dy) normalized to unit length.
fn get_edge_direction_from_node(edge: &EdgeRef, node: &NodeRef) -> (f64, f64) {
    let edge_borrow = edge.borrow();
    let node_pos = node.borrow().pos;

    // Determine which geometry point to use based on whether node is at start or end
    let target_pos = if Rc::ptr_eq(&edge_borrow.from, node) {
        // Node is at start of edge, get direction towards edge interior (second point)
        if edge_borrow.geometry.len() >= 2 {
            [edge_borrow.geometry[1].x, edge_borrow.geometry[1].y]
        } else {
            edge_borrow.to.borrow().pos
        }
    } else {
        // Node is at end of edge, get direction towards edge interior (second-to-last point)
        if edge_borrow.geometry.len() >= 2 {
            let idx = edge_borrow.geometry.len() - 2;
            [edge_borrow.geometry[idx].x, edge_borrow.geometry[idx].y]
        } else {
            edge_borrow.from.borrow().pos
        }
    };

    let dx = target_pos[0] - node_pos[0];
    let dy = target_pos[1] - node_pos[1];
    let len = (dx * dx + dy * dy).sqrt();
    if len < 1e-10 {
        return (1.0, 0.0); // Degenerate case
    }
    (dx / len, dy / len)
}

/// Check if two edges have compatible angles for merging.
/// Returns false if the edges cross at a sharp angle (> 60° from parallel/anti-parallel).
/// This prevents X-intersection crossings from being merged.
fn check_edge_angle_for_merge(a: &EdgeRef, b: &EdgeRef) -> bool {
    // Find shared node
    let shr_nd = if Rc::ptr_eq(&a.borrow().from, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().from, &b.borrow().to)
    {
        Rc::clone(&a.borrow().from)
    } else if Rc::ptr_eq(&a.borrow().to, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().to, &b.borrow().to)
    {
        Rc::clone(&a.borrow().to)
    } else {
        // No shared node - can't merge
        return false;
    };

    // Get direction vectors FROM shared node along each edge
    let dir_a = get_edge_direction_from_node(a, &shr_nd);
    let dir_b = get_edge_direction_from_node(b, &shr_nd);

    // Calculate dot product of normalized vectors
    // For merging, we want edges that are nearly anti-parallel (like ---> <--- meeting at a point)
    // or parallel extensions (fork/join patterns where both go same direction)
    // Reject if crossing at sharp angles (perpendicular or X-crossings)
    let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;
    let abs_dot = dot.abs();

    // abs_dot < 0.5 means angle > 60° from parallel/anti-parallel (crossing)
    // This threshold allows merging of:
    // - Anti-parallel lines (dot ≈ -1, abs 1.0) - typical through routes
    // - Forking lines at < 60° angle - Y-junctions
    // Rejects:
    // - 45° crossings (abs_dot ≈ 0.707) - still allowed, but close
    // - 60°+ crossings (abs_dot < 0.5) - rejected as X-intersections
    // Using 0.4 threshold (66° from parallel) for stricter rejection
    abs_dot >= 0.4
}

/// Lenient version of angle check - only rejects true perpendicular X-crossings.
/// Threshold 0.2 = reject angles >78° from parallel (near-perpendicular crossings).
/// This allows Y-junctions (45°) and most fork patterns while preventing X-crossings.
fn check_edge_angle_for_merge_lenient(a: &EdgeRef, b: &EdgeRef) -> bool {
    // Find shared node
    let shr_nd = if Rc::ptr_eq(&a.borrow().from, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().from, &b.borrow().to)
    {
        Rc::clone(&a.borrow().from)
    } else if Rc::ptr_eq(&a.borrow().to, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().to, &b.borrow().to)
    {
        Rc::clone(&a.borrow().to)
    } else {
        // No shared node - can't merge
        return false;
    };

    let dir_a = get_edge_direction_from_node(a, &shr_nd);
    let dir_b = get_edge_direction_from_node(b, &shr_nd);
    let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;
    let abs_dot = dot.abs();

    // Very lenient: 0.2 threshold = reject angles >78° from parallel
    // Allows Y-junctions, T-junctions at normal angles, most forks
    // Only rejects near-perpendicular X-crossings
    abs_dot >= 0.2
}

/// Strict angle check for merging Rail lines.
/// Rejects merges if deviations are > 30 degrees (abs_dot < 0.866).
/// This discourages sharp turns/merges on rail lines which should be smooth.
fn check_edge_angle_strict_merge(a: &EdgeRef, b: &EdgeRef) -> bool {
    // Find shared node
    let shr_nd = if Rc::ptr_eq(&a.borrow().from, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().from, &b.borrow().to)
    {
        Rc::clone(&a.borrow().from)
    } else if Rc::ptr_eq(&a.borrow().to, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().to, &b.borrow().to)
    {
        Rc::clone(&a.borrow().to)
    } else {
        return false;
    };

    let dir_a = get_edge_direction_from_node(a, &shr_nd);
    let dir_b = get_edge_direction_from_node(b, &shr_nd);
    let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;
    let abs_dot = dot.abs();

    // Strict: 0.85 threshold = reject angles > 31° from parallel/anti-parallel
    abs_dot >= 0.85
}

/// Matches C++ MapConstructor::lineEq
///
/// Checks if two edges can be merged based on their routes and directions.
///
///  Mode-based route type checking replaces C++'s connOccurs.
/// The C++ uses connection exceptions (connOccurs) to prevent bad merges.
/// Since we generate connection exceptions dynamically, we use route type
/// (mode) compatibility combined with angle checks to achieve the same effect.
fn line_eq(a: &EdgeRef, b: &EdgeRef) -> bool {
    let a_routes = &a.borrow().routes;
    let b_routes = &b.borrow().routes;

    // 1. Same number of lines (C++ line 48)
    if a_routes.len() != b_routes.len() {
        return false;
    }

    // 2. Mode compatibility check
    // Collect route types from each edge
    let a_modes: AHashSet<i32> = a_routes.iter().map(|r| r.route_type).collect();
    let b_modes: AHashSet<i32> = b_routes.iter().map(|r| r.route_type).collect();

    // Check if modes are completely disjoint (no overlap)
    let modes_disjoint = !a_modes.is_empty()
        && !b_modes.is_empty()
        && a_modes.intersection(&b_modes).next().is_none();

    // 3. Angle-based crossing detection
    // For same-mode edges: use lenient angle check (allow Y-junctions)
    // For different-mode edges: use stricter angle check (reject most crossings)

    // REGION-BASED SUBWAY VS INTERCITY RAIL LOGIC
    // In North America: subway and intercity rail CAN merge (they share infrastructure)
    // Outside North America: they should NEVER merge (underground vs surface/overhead)
    let has_subway = a_modes.contains(&1) || b_modes.contains(&1);
    let has_intercity_rail = a_modes.contains(&2) || b_modes.contains(&2);

    if has_subway && has_intercity_rail {
        // Check if we're in North America using edge geometry
        // Use first point of edge A as representative location
        let geom = &a.borrow().geometry;
        if !geom.is_empty() {
            let is_north_america = is_in_north_america_webmercator(geom[0].x, geom[0].y);
            if !is_north_america {
                // Outside North America - never merge subway and intercity rail
                return false;
            }
            // In North America - allow merge (fall through to other checks)
        }
    }

    if modes_disjoint {
        // Different route types (e.g. Subway vs Commuter Rail)
        // Only allow merging if edges are nearly anti-parallel (through route)
        // This prevents X-crossings and angled merges between different modes
        if !check_edge_angle_strict_antiparallel(a, b) {
            return false;
        }

        // MINIMUM SEGMENT LENGTH CHECK for disjoint modes
        // Coordinates are in Web Mercator (EPSG:3857) where units are meters.
        // Require at least 800m combined length for different route types to merge.
        // This prevents short accidental merges at crossings while allowing
        // long parallel shared right-of-way segments (like subway + commuter rail tunnels).
        // User requested this be increased to make merging harder.
        const MIN_DISJOINT_SEGMENT_LENGTH: f64 = 800.0; // meters

        let len_a = calc_polyline_length(&a.borrow().geometry);
        let len_b = calc_polyline_length(&b.borrow().geometry);
        let combined_length = len_a + len_b;

        if combined_length < MIN_DISJOINT_SEGMENT_LENGTH {
            return false;
        }
    } else {
        // Same or overlapping route types

        // SUBWAY-SPECIFIC CHECK (Route Type 1) - Fix 1
        // Subway lines meeting at non-parallel angles should NOT merge unless they
        // share significant parallel track distance (>100m). This prevents X-shape
        // crossings (like RATP at Rennes) from being merged while allowing
        // parallel curve merges (y=x^8 shape, like RATP lines 4/12 at Montparnasse).
        let has_subway = a_modes.contains(&1) || b_modes.contains(&1);

        if has_subway {
            let angle_deg = calc_edge_angle_degrees(a, b);

            // If angle > 10 degrees from parallel or anti-parallel, check shared distance
            // Parallel/anti-parallel = 0° or 180°, so we check if NOT in (0-10) or (170-180)
            if angle_deg > 10.0 && angle_deg < 170.0 {
                // Lines are meeting at an angle - require significant parallel track
                let shared_dist = calc_shared_route_distance(a, b);

                if shared_dist < 100.0 {
                    // X-shape crossing without enough parallel track - don't merge
                    return false;
                }
            }
        }

        // SPECIAL HANDLING FOR RAIL (Route Type 2)
        // User request: "discourage sharp turns on rail lines compared to subways and trams"
        let has_rail = a_modes.contains(&2) || b_modes.contains(&2);

        if has_rail {
            // Rail lines have large turning radii. Merging should only happen if lines are parallel.
            // Strict check: abs_dot >= 0.85 (approx 30 degrees max deviation)
            if !check_edge_angle_strict_merge(a, b) {
                return false;
            }
        } else if !has_subway {
            // Use lenient angle check for Tram only - only reject true perpendicular X-crossings
            // (Subway already handled above with graph traversal check)
            if !check_edge_angle_for_merge_lenient(a, b) {
                return false;
            }
        }
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

    // 4. Check each line validation
    for ra in a_routes {
        // Find corresponding line in b
        let rb = match b_routes.iter().find(|r| r.line_id == ra.line_id) {
            Some(r) => r,
            None => return false,
        };

        // C++ line 57: if (!shrNd->pl().connOccurs(ra.line, a, b)) return false;
        // Check if connection is allowed at the shared node (no excluded connections).
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

/// Strict angle check for different-mode edges.
/// Only allows merging if edges are nearly anti-parallel (through route pattern).
/// This prevents X-crossings and angled intersections between different transit modes.
/// Returns true if edges form a valid through-route (angle >160° i.e. <20° from straight).
fn check_edge_angle_strict_antiparallel(a: &EdgeRef, b: &EdgeRef) -> bool {
    // Find shared node
    let shr_nd = if Rc::ptr_eq(&a.borrow().from, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().from, &b.borrow().to)
    {
        Rc::clone(&a.borrow().from)
    } else if Rc::ptr_eq(&a.borrow().to, &b.borrow().from)
        || Rc::ptr_eq(&a.borrow().to, &b.borrow().to)
    {
        Rc::clone(&a.borrow().to)
    } else {
        return false;
    };

    let dir_a = get_edge_direction_from_node(a, &shr_nd);
    let dir_b = get_edge_direction_from_node(b, &shr_nd);
    let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;

    // For anti-parallel (through route), dot product should be close to -1
    // dot < -0.94 means angle > 160° (edges form nearly straight through-route)
    // This is much stricter than the lenient check and only allows true through-routes
    dot < -0.94
}

/// Matches C++ MapConstructor::supportEdge
/// Split a long blocking edge 'ex' into two edges with a new support node in the middle.
/// Returns the ID of the new support node, or None if split failed.
fn support_edge(ex: &EdgeRef, graph: &mut LineGraph) -> Option<usize> {
    let ex_borrow = ex.borrow();
    let from = Rc::clone(&ex_borrow.from);
    let to = Rc::clone(&ex_borrow.to);
    let routes = ex_borrow.routes.clone();
    let geom = ex_borrow.geometry.clone();

    // Split geometry in half
    let geom_len = geom.len();
    if geom_len < 2 {
        return None;
    }

    // Simple split index (approximate middle)
    // C++ uses geometric split at 0.5 length fraction.
    // Here we just pick middle vertex index for simplicity, or interpolate if needed.
    // Given the density, vertex split is likely fine.
    let mid_idx = geom_len / 2;

    // Create support node
    let mid_coord = geom[mid_idx];
    let sup_nd = graph.add_nd([mid_coord.x, mid_coord.y]); // IDs are internal
    let sup_id = sup_nd.borrow().id;

    // Create edge A: from -> sup_nd
    let geom_a = geom[0..=mid_idx].to_vec();
    let edge_a = Rc::new(RefCell::new(LineEdge {
        id: 0, // ID will be regenerated if needed
        from: Rc::clone(&from),
        to: Rc::clone(&sup_nd),
        routes: routes.clone(),
        geometry: geom_a,
    }));

    // Create edge B: sup_nd -> to
    let geom_b = geom[mid_idx..].to_vec();
    let edge_b = Rc::new(RefCell::new(LineEdge {
        id: 0, // ID will be regenerated if needed
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

    Some(sup_id)
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
                route_type: ra.route_type,
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
                route_type: ra.route_type,
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
                route_type: rb.route_type,
                direction: new_dir,
            });
        }
    }

    // Simplify the combined geometry to remove zigzag artifacts (matches C++ simplify)
    // FIX 3: Use length-dependent tolerance to preserve curves on longer edges
    // Very short edges (<30m): 2.0m tolerance (aggressive station de-zigzag)
    // Short edges (<100m): 1.0m tolerance (aggressive cleanup)
    // Medium edges (100-500m): 0.5m tolerance (balanced)
    // Long edges (>500m): 0.2m tolerance (preserve curves)
    let edge_length = calc_polyline_length(&new_geom);
    let simplify_tolerance = if edge_length < 30.0 {
        2.0
    } else if edge_length < 100.0 {
        1.0
    } else if edge_length < 500.0 {
        0.5
    } else {
        0.2 // Preserve curves on long rail segments
    };
    let simplified_geom = simplify_coords(&new_geom, simplify_tolerance);

    // Create new edge
    let new_edge = Rc::new(RefCell::new(LineEdge {
        id: 0, // ID will be regenerated if needed
        from: Rc::clone(&other_a),
        to: Rc::clone(&other_b),
        routes: new_routes,
        geometry: simplified_geom,
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

                if graph.get_edg(&other_a, &other_b).is_some() {
                    // Still blocked (maybe another edge?), abort
                    do_contract = false;
                }
            } else {
                // Short blocking edge exists - C++ says DON'T contract (lines 413-416)
                // This preserves the topology at divergence/convergence points
                // and prevents gaps when multiple lines share a segment.
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

        // INTERCITY RAIL ANGLE CHECK (Fix for near-90° collapses)
        // For intercity rail (route type 2), reject collapsing edges that form
        // angles > 45° from parallel/anti-parallel. This prevents unnatural
        // sharp turns in rail geometry.
        {
            let has_intercity_rail = edge_a.borrow().routes.iter().any(|r| r.route_type == 2)
                || edge_b.borrow().routes.iter().any(|r| r.route_type == 2);

            if has_intercity_rail {
                let angle_deg = calc_edge_angle_degrees(edge_a, edge_b);
                // Angle 0° = straight through (anti-parallel), 90° = perpendicular
                // Reject if angle is > 45° from straight (i.e., in range 45° to 135°)
                // This is stricter than the 31° check in line_eq (0.85 threshold)
                if angle_deg > 45.0 && angle_deg < 135.0 {
                    continue;
                }
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

/// Check if Web Mercator coordinates are in North America.
/// North America defined as: USA, Canada, Mexico, Central America, Caribbean
/// Web Mercator bounds (approximate):
/// - Longitude: -170° to -50° (Pacific to Atlantic, including Caribbean)
/// - Latitude: 7° to 85° (Panama to Canadian Arctic)
fn is_in_north_america_webmercator(x: f64, y: f64) -> bool {
    // Web Mercator (EPSG:3857) coordinate bounds for North America
    // Longitude -170° to -50° in Web Mercator: approximately -18924313 to -5565974
    // Latitude 7° to 85° in Web Mercator: approximately 780000 to 19971869

    // Longitude bounds (x)
    const X_MIN: f64 = -18_924_313.0; // -170° longitude
    const X_MAX: f64 = -5_565_974.0; // -50° longitude

    // Latitude bounds (y)
    const Y_MIN: f64 = 780_000.0; // ~7° latitude (Panama)
    const Y_MAX: f64 = 19_971_869.0; // ~85° latitude (Canadian Arctic)

    x >= X_MIN && x <= X_MAX && y >= Y_MIN && y <= Y_MAX
}

/// Check if a crossing edge eventually turns toward the incoming direction.
/// Returns true if the lines converge (merge is appropriate), false if they
/// stay perpendicular/diverge (true X-crossing, no merge).
/// Used for Fix 2: preventing plus-shape intersection bending.
fn check_lines_converge(
    crossing_edge: &EdgeRef,
    incoming_dir: (f64, f64),
    graph: &LineGraph,
) -> bool {
    use std::collections::HashSet;

    let mut visited: HashSet<usize> = HashSet::new();
    visited.insert(crossing_edge.borrow().id);

    let mut current_edge = Rc::clone(crossing_edge);
    let mut check_dist = 0.0;
    const MAX_CHECK_DIST: f64 = 300.0; // Look ahead up to 300m

    while check_dist < MAX_CHECK_DIST {
        let edge_len = calc_polyline_length(&current_edge.borrow().geometry);
        check_dist += edge_len;

        // Get direction at end of current edge
        let to_node = current_edge.borrow().to.clone();

        // Find continuation edge (degree-2 pass-through or closest angle)
        let next_edges: Vec<EdgeRef> = to_node
            .borrow()
            .adj_list
            .iter()
            .filter(|e| !visited.contains(&e.borrow().id))
            .cloned()
            .collect();

        if next_edges.is_empty() {
            break;
        }

        // Pick edge that continues most in same direction
        let current_dir = get_edge_direction_from_node(&current_edge, &to_node);
        let mut best_edge: Option<EdgeRef> = None;
        let mut best_dot = -f64::MAX;

        for e in &next_edges {
            let e_dir = get_edge_direction_from_node(e, &to_node);
            // Continuation means going AWAY from the node - negate direction
            let continuation_dot = -(current_dir.0 * e_dir.0 + current_dir.1 * e_dir.1);
            if continuation_dot > best_dot {
                best_dot = continuation_dot;
                best_edge = Some(Rc::clone(e));
            }
        }

        if let Some(next) = best_edge {
            visited.insert(next.borrow().id);

            // Check if this edge's direction is now more parallel to incoming
            let next_dir = get_edge_direction_from_node(&next, &to_node);
            let dot_with_incoming =
                (next_dir.0 * incoming_dir.0 + next_dir.1 * incoming_dir.1).abs();

            // If angle with incoming is now < 45° (dot > 0.7), lines are converging
            if dot_with_incoming > 0.7 {
                return true; // Lines turn into each other - merge OK
            }

            current_edge = next;
        } else {
            break;
        }
    }

    false // Lines stay perpendicular - true crossing, don't merge
}

/// Get the shared node between two edges, if any.
fn get_shared_node(a: &EdgeRef, b: &EdgeRef) -> Option<NodeRef> {
    let a_from = &a.borrow().from;
    let a_to = &a.borrow().to;
    let b_from = &b.borrow().from;
    let b_to = &b.borrow().to;

    if Rc::ptr_eq(a_from, b_from) || Rc::ptr_eq(a_from, b_to) {
        Some(Rc::clone(a_from))
    } else if Rc::ptr_eq(a_to, b_from) || Rc::ptr_eq(a_to, b_to) {
        Some(Rc::clone(a_to))
    } else {
        None
    }
}

/// Calculate angle between two edges at their shared node, in degrees (0-180).
/// 0° = anti-parallel (through-route), 180° = same direction, 90° = perpendicular
fn calc_edge_angle_degrees(a: &EdgeRef, b: &EdgeRef) -> f64 {
    let shr_nd = match get_shared_node(a, b) {
        Some(n) => n,
        None => return 90.0, // No shared node = treat as perpendicular
    };

    let dir_a = get_edge_direction_from_node(a, &shr_nd);
    let dir_b = get_edge_direction_from_node(b, &shr_nd);

    let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;
    dot.clamp(-1.0, 1.0).acos().to_degrees()
}

/// Calculate minimum distance between a point on geom_a and any point on geom_b
fn closest_point_distance_to_geom(geom_a: &[Coord], geom_b: &[Coord]) -> f64 {
    if geom_a.is_empty() || geom_b.is_empty() {
        return f64::MAX;
    }

    let mut min_dist = f64::MAX;

    // Sample points from geom_a
    for coord_a in geom_a {
        for coord_b in geom_b {
            let dx = coord_a.x - coord_b.x;
            let dy = coord_a.y - coord_b.y;
            let dist = (dx * dx + dy * dy).sqrt();
            if dist < min_dist {
                min_dist = dist;
            }
        }
    }

    min_dist
}

/// Calculate total shared distance by traversing connected edges.
/// For subway angle merging - finds how far two lines share parallel tracks.
/// Continues along connecting edges while checking if distance to other line stays close.
fn calc_shared_route_distance(edge_a: &EdgeRef, edge_b: &EdgeRef) -> f64 {
    use std::collections::HashSet;

    // Get routes from both edges
    let routes_a: HashSet<(String, String)> = edge_a
        .borrow()
        .routes
        .iter()
        .map(|r| r.line_id.clone())
        .collect();
    let routes_b: HashSet<(String, String)> = edge_b
        .borrow()
        .routes
        .iter()
        .map(|r| r.line_id.clone())
        .collect();

    // Find common routes
    let common_routes: HashSet<_> = routes_a.intersection(&routes_b).cloned().collect();
    if common_routes.is_empty() {
        return 0.0;
    }

    // Calculate shared distance by traversing from edge_a while edge_b stays parallel
    let dist_a = traverse_for_shared_distance(edge_a, edge_b, &common_routes);
    // Also traverse from edge_b direction to get total shared segment
    let dist_b = traverse_for_shared_distance(edge_b, edge_a, &common_routes);

    // Return the maximum found (in case of asymmetric traversal)
    dist_a.max(dist_b)
}

/// Traverse from start_edge along connected edges, measuring how far other_edge stays parallel.
/// Checks if distance to other_edge is increasing (diverging) or stable (parallel).
fn traverse_for_shared_distance(
    start_edge: &EdgeRef,
    other_edge: &EdgeRef,
    common_routes: &std::collections::HashSet<(String, String)>,
) -> f64 {
    use std::collections::HashSet;

    let mut total = calc_polyline_length(&start_edge.borrow().geometry);
    let mut current_edge = Rc::clone(start_edge);
    let mut visited: HashSet<usize> = HashSet::new();
    visited.insert(start_edge.borrow().id);

    // Track distance to other line
    let other_geom: Vec<Coord> = other_edge.borrow().geometry.clone();
    let mut prev_dist = closest_point_distance_to_geom(&start_edge.borrow().geometry, &other_geom);

    // Maximum traversal depth (50 edges or ~5km typical)
    for _ in 0..50 {
        let to_node = current_edge.borrow().to.clone();
        let next_edges: Vec<EdgeRef> = to_node
            .borrow()
            .adj_list
            .iter()
            .filter(|e| {
                let eid = e.borrow().id;
                if visited.contains(&eid) {
                    return false;
                }
                // Edge must have a route in common_routes
                e.borrow()
                    .routes
                    .iter()
                    .any(|r| common_routes.contains(&r.line_id))
            })
            .cloned()
            .collect();

        if next_edges.is_empty() {
            break;
        }

        // Pick the edge that continues closest to other_edge
        let mut best_edge: Option<EdgeRef> = None;
        let mut best_dist = f64::MAX;

        for e in &next_edges {
            let dist = closest_point_distance_to_geom(&e.borrow().geometry, &other_geom);
            if dist < best_dist {
                best_dist = dist;
                best_edge = Some(Rc::clone(e));
            }
        }

        if let Some(next) = best_edge {
            // Check if distance is increasing (diverging) or stable (parallel)
            // Allow up to 50m increase or 1.5x multiplier before counting as diverged
            if best_dist > prev_dist * 1.5 + 50.0 {
                break; // Lines are diverging, stop counting shared distance
            }

            total += calc_polyline_length(&next.borrow().geometry);
            visited.insert(next.borrow().id);
            current_edge = next;
            prev_dist = best_dist;
        } else {
            break;
        }
    }

    total
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
    use geo::Distance;
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

    // Resample both geometries to same number of points based on LENGTH (Match C++)
    // C++ PolyLine::average uses AVERAGING_STEP = 20.0
    // It samples at 0, 20, 40... meters.
    // This acts as a LOW-PASS FILTER, ignoring high-frequency zigzags < 20m.

    let len_a = calc_polyline_length(geom_a);
    let len_b = calc_polyline_length(geom_b);
    let max_len = len_a.max(len_b);

    // Calculate number of steps based on 20m interval
    // Ensure at least 2 points (start/end)
    // num_segments = ceil(len / step)
    // num_points = num_segments + 1
    let num_points = (max_len / 20.0).ceil() as usize + 1;
    let num_points = num_points.max(2);

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

    // Apply Chaikin smoothing to eliminate "shakiness"
    // Depth 1 is subtle, Depth 2 is strong.
    let smoothed = chaikin_smooth_coords(&result, 1);

    // Simplify the result
    simplify_coords(&smoothed, 0.5)
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

        // If we've started, add intermediate points (p2) that fall before end_dist
        // The key is: once started, add p2 if the segment ends before or at end_dist
        // We need to check seg_end_dist <= end_dist (not cur_dist >= start_dist, which is always
        // false in the starting segment where cur_dist < start_dist <= seg_end_dist)
        if started && seg_end_dist <= end_dist {
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
/// Returns the number of edges contracted (used for iterative convergence check).
fn contract_short_edges(graph: &mut LineGraph, max_aggr_distance: f64) -> usize {
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
                        let old_e_len = calc_polyline_length(&old_e.borrow().geometry);
                        let support_threshold = 2.0 * max_aggr_distance;

                        // CRITICAL FIX: Check geometry length difference (C++ lines 593-606)
                        // Prevent folding edges with vastly different geometries
                        // This stops Chicago Union Station tracks from merging with CTA Blue Line
                        if (ex_len - old_e_len).abs() > support_threshold {
                            dont_contract = true;
                            continue;
                        }

                        // Y-JUNCTION TRIANGLE PREVENTION (MODE-AWARE):
                        // Only block Y-angle contractions for DIFFERENT route types.
                        // Same-mode Y-junctions should merge smoothly (they share track).
                        let edge_modes: AHashSet<i32> =
                            edge.borrow().routes.iter().map(|r| r.route_type).collect();
                        let old_e_modes: AHashSet<i32> =
                            old_e.borrow().routes.iter().map(|r| r.route_type).collect();
                        let modes_disjoint = !edge_modes.is_empty()
                            && !old_e_modes.is_empty()
                            && edge_modes.intersection(&old_e_modes).next().is_none();

                        if modes_disjoint {
                            // Different route types - check if they form a Y-angle
                            let dir_edge = get_edge_direction_from_node(&edge, &from);
                            let dir_old_e = get_edge_direction_from_node(old_e, &from);
                            let dot = dir_edge.0 * dir_old_e.0 + dir_edge.1 * dir_old_e.1;

                            // If angle is in the 45°-135° range (Y-shape), don't contract
                            // This prevents triangular artifacts at cross-mode junctions
                            if dot.abs() < 0.707 {
                                dont_contract = true;
                                continue;
                            }
                        }
                        // Same-mode edges: allow contraction regardless of angle
                        // The weighted centroid will stabilize the position

                        if ex_len > support_threshold {
                            blocking_edges_to_split.push(Rc::clone(&ex));
                        } else {
                            // Short blocking edge -> assume we CAN squash this triangle
                            // C++ allows this contraction to merge parallel tracks
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
                if let Some(new_node_id) = support_edge(&block_ex, graph) {
                    if !in_worklist.contains(&new_node_id) {
                        worklist.push_back(new_node_id);
                        in_worklist.insert(new_node_id);
                    }
                    // Also add the endpoints of the split edge since their adjacencies changed
                    // (They now connect to the new node instead of each other)
                    let ex_from_id = block_ex.borrow().from.borrow().id;
                    let ex_to_id = block_ex.borrow().to.borrow().id;

                    if !in_worklist.contains(&ex_from_id) {
                        worklist.push_back(ex_from_id);
                        in_worklist.insert(ex_from_id);
                    }
                    if !in_worklist.contains(&ex_to_id) {
                        worklist.push_back(ex_to_id);
                        in_worklist.insert(ex_to_id);
                    }
                }
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
                // C++ CRITICAL: This case always skips (line 466: unconditional continue)
                // This prevents deleting edges left by degree-2 contraction to avoid long edges
                skip_degree = true;
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

    total_contracted
}

/// Stabilize intercity rail corridors by looking ahead to prevent flip-flopping.
///
/// For intercity rail (route_type 2), parallel tracks often split and re-merge rapidly,
/// creating a zigzag pattern. This function analyzes diverging intercity rail edges
/// and looks ahead up to `look_ahead_distance` meters. If the tracks would re-converge
/// within that distance, it forces them to stay merged instead of splitting.
///
/// Returns the number of stabilization merges performed.
fn stabilize_intercity_rail_corridors(graph: &mut LineGraph, look_ahead_distance: f64) -> usize {
    const INTERCITY_RAIL: i32 = 2;
    let mut total_stabilized = 0;

    // Build a node map for fast lookup
    let node_map: AHashMap<usize, NodeRef> = graph
        .nodes
        .iter()
        .map(|n| (n.borrow().id, Rc::clone(n)))
        .collect();

    // Find all junction nodes (degree > 2) that have intercity rail edges diverging
    let junction_nodes: Vec<NodeRef> = graph
        .nodes
        .iter()
        .filter(|n| n.borrow().get_deg() > 2)
        .map(Rc::clone)
        .collect();

    for junction in &junction_nodes {
        // Skip dead nodes
        if junction.borrow().get_deg() == 0 {
            continue;
        }

        // Find all outgoing intercity rail edges from this junction
        let adj_list = junction.borrow().adj_list.clone();
        let intercity_edges: Vec<EdgeRef> = adj_list
            .iter()
            .filter(|e| {
                e.borrow()
                    .routes
                    .iter()
                    .any(|r| r.route_type == INTERCITY_RAIL)
            })
            .map(Rc::clone)
            .collect();

        // Check pairs of diverging intercity rail edges
        for i in 0..intercity_edges.len() {
            for j in i + 1..intercity_edges.len() {
                let edge_a = &intercity_edges[i];
                let edge_b = &intercity_edges[j];

                // Get the "other" nodes (where these edges lead to)
                let other_a = edge_a.borrow().get_other_nd(junction);
                let other_b = edge_b.borrow().get_other_nd(junction);

                // Skip if they go to the same node (already merged)
                if Rc::ptr_eq(&other_a, &other_b) {
                    continue;
                }

                // Check if edges are diverging (not parallel)
                // Get directions of edges leaving the junction
                let dir_a = get_edge_direction_from_node(edge_a, junction);
                let dir_b = get_edge_direction_from_node(edge_b, junction);
                let dot = dir_a.0 * dir_b.0 + dir_a.1 * dir_b.1;

                // If edges are going in nearly opposite directions, skip
                // (they're not a diverging pair, they go different ways)
                if dot < -0.5 {
                    continue;
                }

                // Look ahead from both edges to see if they reconverge
                let reconverge_distance = find_reconvergence_distance(
                    graph,
                    junction,
                    &other_a,
                    &other_b,
                    look_ahead_distance,
                    &node_map,
                );

                if let Some(dist) = reconverge_distance {
                    // Tracks reconverge within look_ahead_distance!
                    // This is a flip-flop pattern - force merge

                    // Merge the two "other" nodes
                    // Create a dummy geo_idx just for this merge
                    let mut dummy_idx = NodeGeoIdx::new(look_ahead_distance);
                    dummy_idx.add(other_a.borrow().pos, Rc::clone(&other_a));
                    dummy_idx.add(other_b.borrow().pos, Rc::clone(&other_b));

                    if combine_nodes(&other_a, &other_b, graph, &mut dummy_idx) {
                        total_stabilized += 1;
                        // println!(
                        //     "  Stabilized intercity rail: merged nodes at distance {:.1}m",
                        //     dist
                        // );
                    }
                }
            }
        }
    }

    if total_stabilized > 0 {
        println!(
            "  Stabilized {} intercity rail flip-flop patterns.",
            total_stabilized
        );
        graph.nodes.retain(|n| n.borrow().get_deg() > 0);
    }

    total_stabilized
}

/// Trace paths from two nodes to see if they reconverge within max_distance.
/// Returns Some(distance) if they reconverge, None otherwise.
fn find_reconvergence_distance(
    _graph: &LineGraph,
    start_junction: &NodeRef,
    node_a: &NodeRef,
    node_b: &NodeRef,
    max_distance: f64,
    node_map: &AHashMap<usize, NodeRef>,
) -> Option<f64> {
    // BFS from both nodes simultaneously, tracking visited nodes and distances
    // If we find a node reachable from both, they reconverge

    let mut visited_a: AHashMap<usize, f64> = AHashMap::new();
    let mut visited_b: AHashMap<usize, f64> = AHashMap::new();

    let start_id = start_junction.borrow().id;
    visited_a.insert(start_id, 0.0);
    visited_b.insert(start_id, 0.0);

    // Worklist entries: (node, accumulated_distance)
    let mut queue_a: VecDeque<(NodeRef, f64)> = VecDeque::new();
    let mut queue_b: VecDeque<(NodeRef, f64)> = VecDeque::new();

    queue_a.push_back((Rc::clone(node_a), 0.0));
    queue_b.push_back((Rc::clone(node_b), 0.0));
    visited_a.insert(node_a.borrow().id, 0.0);
    visited_b.insert(node_b.borrow().id, 0.0);

    // Process both queues alternately
    while !queue_a.is_empty() || !queue_b.is_empty() {
        // Process one node from queue A
        if let Some((current, dist)) = queue_a.pop_front() {
            if dist > max_distance {
                continue;
            }

            let current_id = current.borrow().id;

            // Check if this node was reached by path B
            if let Some(&dist_b) = visited_b.get(&current_id) {
                // Found reconvergence!
                return Some(dist + dist_b);
            }

            // Explore neighbors
            let adj_list = current.borrow().adj_list.clone();
            for edge in &adj_list {
                let neighbor = edge.borrow().get_other_nd(&current);
                let neighbor_id = neighbor.borrow().id;

                // Don't go back to start
                if neighbor_id == start_id {
                    continue;
                }

                let edge_len = calc_polyline_length(&edge.borrow().geometry);
                let new_dist = dist + edge_len;

                if new_dist <= max_distance {
                    if !visited_a.contains_key(&neighbor_id) || visited_a[&neighbor_id] > new_dist {
                        visited_a.insert(neighbor_id, new_dist);
                        queue_a.push_back((neighbor, new_dist));
                    }
                }
            }
        }

        // Process one node from queue B
        if let Some((current, dist)) = queue_b.pop_front() {
            if dist > max_distance {
                continue;
            }

            let current_id = current.borrow().id;

            // Check if this node was reached by path A
            if let Some(&dist_a) = visited_a.get(&current_id) {
                // Found reconvergence!
                return Some(dist + dist_a);
            }

            // Explore neighbors
            let adj_list = current.borrow().adj_list.clone();
            for edge in &adj_list {
                let neighbor = edge.borrow().get_other_nd(&current);
                let neighbor_id = neighbor.borrow().id;

                // Don't go back to start
                if neighbor_id == start_id {
                    continue;
                }

                let edge_len = calc_polyline_length(&edge.borrow().geometry);
                let new_dist = dist + edge_len;

                if new_dist <= max_distance {
                    if !visited_b.contains_key(&neighbor_id) || visited_b[&neighbor_id] > new_dist {
                        visited_b.insert(neighbor_id, new_dist);
                        queue_b.push_back((neighbor, new_dist));
                    }
                }
            }
        }
    }

    None
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
                                route_type: r.route_type,
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

/// Fold parallel edges across the whole graph (C++: foldEdges).
///
/// The project already had `fold_edges_for_node`, but it was never called.
/// Without this pass, parallel rail tracks (and other duplicate edges between the
/// same nodes) remain separate, which prevents intercity rail infrastructure from
/// merging and can create station zig-zag artifacts.
fn fold_parallel_edges(graph: &mut LineGraph) {
    for _ in 0..20 {
        let nodes: Vec<NodeRef> = graph.nodes.iter().map(Rc::clone).collect();
        let mut changed = false;

        for node in nodes {
            if node.borrow().get_deg() < 2 {
                continue;
            }
            if fold_edges_for_node(graph, &node) {
                changed = true;
            }
        }

        graph.nodes.retain(|n| n.borrow().get_deg() > 0);

        if !changed {
            break;
        }
    }
}

/// Apply polish fixes to the graph - INSIDE the iteration loop
/// Based on C++ collapseShrdSegs lines 421-481 (edge artifact removal)
fn apply_polish_fixes(graph: &mut LineGraph, _max_aggr_distance: f64) {
    // C++ lines 504-514: "smoothen a bit" - apply per-iteration edge smoothing.
    // IMPORTANT: C++ does NOT run the short-edge contraction step here; that happens
    // earlier (the do/while artifact removal block). Contracting again here can
    // introduce unintended merges near close-but-not-connected geometry.

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

            // FIX BUG 3: Check for blocking edges with vastly different geometry BEFORE merge
            // If any blocking edge would create a triangle artifact, skip the whole merge
            let mut can_merge = true;
            let pre_check_adj = node.borrow().adj_list.clone();
            for other_edge_ref in &pre_check_adj {
                if Rc::ptr_eq(other_edge_ref, &edge_ref) {
                    continue;
                }
                let other_node = other_edge_ref.borrow().get_other_nd(&node);
                if Rc::ptr_eq(&other_node, &to_node) {
                    continue;
                }

                // Check if to_node already has an edge to other_node
                if let Some(existing) = graph.get_edg(&to_node, &other_node) {
                    let existing_len = calc_polyline_length(&existing.borrow().geometry);
                    let other_len = calc_polyline_length(&other_edge_ref.borrow().geometry);
                    let length_diff_threshold = 2.0 * threshold;

                    if (existing_len - other_len).abs() > length_diff_threshold {
                        can_merge = false;
                        break;
                    }
                }
            }

            if !can_merge {
                continue;
            }

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
                    // Fold edges - we already checked length compatibility above
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
                                    route_type: r.route_type,
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
                    // Create new edge from to_node to other_node
                    // FIX: Update geometry endpoint to match new 'from' node position
                    let mut geometry = other_edge_ref.borrow().geometry.clone();
                    if !geometry.is_empty() {
                        let to_pos = to_node.borrow().pos;
                        geometry[0] = Coord {
                            x: to_pos[0],
                            y: to_pos[1],
                        };
                    }

                    let new_edge = Rc::new(RefCell::new(LineEdge {
                        id: 0, // ID will be regenerated if needed
                        from: Rc::clone(&to_node),
                        to: Rc::clone(&other_node),
                        routes: other_edge_ref.borrow().routes.clone(),
                        geometry,
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
    let mut nodes_with_distant_edges = 0;

    for node in &graph.nodes {
        let mut positions: Vec<[f64; 2]> = Vec::new();

        for edge_ref in &node.borrow().adj_list {
            let edge = edge_ref.borrow();
            let geom = &edge.geometry;
            if geom.is_empty() {
                continue;
            }

            // Check if we are connected to the start or end of the edge geometry
            if Rc::ptr_eq(&edge.to, node) {
                // Connected to the END of this edge
                let last = geom.last().unwrap();
                positions.push([last.x, last.y]);
            } else {
                // Connected to the START of this edge
                let first = geom.first().unwrap();
                positions.push([first.x, first.y]);
            }
        }

        if positions.is_empty() {
            continue;
        }

        // Check if any positions are suspiciously far apart (>100km)
        // This indicates NodeId collision between distant geographic regions
        let mut max_dist = 0.0f64;
        for i in 0..positions.len() {
            for j in (i + 1)..positions.len() {
                let dx = (positions[i][0] - positions[j][0]).abs();
                let dy = (positions[i][1] - positions[j][1]).abs();
                let dist = (dx * dx + dy * dy).sqrt();
                max_dist = max_dist.max(dist);
            }
        }

        if max_dist > 100_000.0 {
            // 100km in meters
            nodes_with_distant_edges += 1;
            if nodes_with_distant_edges <= 3 {
                eprintln!(
                    "WARNING: Node {} has edges with endpoints {:.1}km apart!",
                    node.borrow().id,
                    max_dist / 1000.0
                );
                eprintln!("  Positions connected to this node:");
                for (i, pos) in positions.iter().enumerate() {
                    eprintln!("    {}: ({:.1}, {:.1})", i, pos[0], pos[1]);
                }
                if let Some(orig_id) = &node.borrow().original_node_id {
                    eprintln!("  Original NodeId: {:?}", orig_id);
                }
            }
            // CRITICAL FIX: Skip averaging for nodes with distant endpoints
            // This indicates a NodeId collision - averaging would corrupt the position
            // Keep the node's current position instead of averaging across distant regions
            continue;
        }

        // Calculate average position (only for nodes with nearby endpoints)
        let sum_x: f64 = positions.iter().map(|p| p[0]).sum();
        let sum_y: f64 = positions.iter().map(|p| p[1]).sum();
        let count = positions.len();

        node.borrow_mut().pos = [sum_x / count as f64, sum_y / count as f64];
    }

    if nodes_with_distant_edges > 0 {
        eprintln!(
            "=== FOUND {} NODES WITH DISTANT EDGE ENDPOINTS ===",
            nodes_with_distant_edges
        );
    }
}

/// Reconstruct intersections (C++: reconstructIntersections)
/// 1. Average node positions
/// 2. Trim/Extend edge geometries to meet exactly at the new node positions
fn reconstruct_intersections(graph: &mut LineGraph, max_aggr_distance: f64) {
    // Fold before repositioning nodes so we don't reconstruct intersections for
    // duplicated parallel edges that should have been one.
    fold_parallel_edges(graph);

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

            // Get the actual geometry endpoints for comparison
            let geom_start = edge.geometry.first().map(|c| [c.x, c.y]);
            let geom_end = edge.geometry.last().map(|c| [c.x, c.y]);

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

            // CRITICAL FIX: Check if node position is close to geometry endpoint
            // If node position is too far (>10km), use geometry's own endpoint instead
            // This prevents NodeId collision from corrupting geometry
            const MAX_ENDPOINT_DEVIATION: f64 = 10_000.0; // 10km in meters

            // Determine actual start point to use
            let actual_from = if let Some(gs) = geom_start {
                let dx = (from_pos[0] - gs[0]).abs();
                let dy = (from_pos[1] - gs[1]).abs();
                let dist = (dx * dx + dy * dy).sqrt();
                if dist > MAX_ENDPOINT_DEVIATION {
                    // Node position is wrong - use geometry's own start
                    Coord { x: gs[0], y: gs[1] }
                } else {
                    Coord {
                        x: from_pos[0],
                        y: from_pos[1],
                    }
                }
            } else {
                Coord {
                    x: from_pos[0],
                    y: from_pos[1],
                }
            };

            // Determine actual end point to use
            let actual_to = if let Some(ge) = geom_end {
                let dx = (to_pos[0] - ge[0]).abs();
                let dy = (to_pos[1] - ge[1]).abs();
                let dist = (dx * dx + dy * dy).sqrt();
                if dist > MAX_ENDPOINT_DEVIATION {
                    // Node position is wrong - use geometry's own end
                    Coord { x: ge[0], y: ge[1] }
                } else {
                    Coord {
                        x: to_pos[0],
                        y: to_pos[1],
                    }
                }
            } else {
                Coord {
                    x: to_pos[0],
                    y: to_pos[1],
                }
            };

            // Insert explicit Start point (fromNode)
            edge.geometry.insert(0, actual_from);

            // Insert explicit End point (toNode)
            edge.geometry.push(actual_to);
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
