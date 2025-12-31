use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use crate::support_graph::{LineNode, NodeRef};
use ahash::AHashMap;
use anyhow::Result;
use geo::{Coord, EuclideanLength, LineString as GeoLineString, Point as GeoPoint};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

// ===========================================================================
// Spatial Index for Nodes (matches C++ NodeGeoIdx)
// ===========================================================================

pub struct NodeGeoIdx {
    pub cell_size: f64,
    pub cells: AHashMap<(i32, i32), Vec<NodeRef>>,
}

impl NodeGeoIdx {
    pub fn new(cell_size: f64) -> Self {
        Self {
            cell_size,
            cells: AHashMap::new(),
        }
    }

    pub fn get_cell_coords(&self, x: f64, y: f64) -> (i32, i32) {
        (
            (x / self.cell_size).floor() as i32,
            (y / self.cell_size).floor() as i32,
        )
    }

    pub fn add(&mut self, pos: [f64; 2], node: NodeRef) {
        let coords = self.get_cell_coords(pos[0], pos[1]);
        self.cells.entry(coords).or_default().push(node);
    }

    pub fn remove(&mut self, node: &NodeRef) {
        let pos = node.borrow().pos;
        let coords = self.get_cell_coords(pos[0], pos[1]);
        if let Some(nodes) = self.cells.get_mut(&coords) {
            if let Some(pos) = nodes.iter().position(|n| Rc::ptr_eq(n, node)) {
                nodes.swap_remove(pos);
            }
        }
    }

    /// Get nodes within radius of point
    pub fn get(&self, pos: [f64; 2], radius: f64) -> Vec<NodeRef> {
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
    pub fn find_best_in_radius<F>(
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

                        // Skip degree-0 nodes
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
// Spatial Partitioning (for Germany-scale networks)
// ===========================================================================

/// Union-Find (Disjoint Set Union) for O(n α(n)) connected component detection
pub struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    pub fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    pub fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]); // Path compression
        }
        self.parent[x]
    }

    pub fn union(&mut self, x: usize, y: usize) {
        let px = self.find(x);
        let py = self.find(y);
        if px == py {
            return;
        }
        // Union by rank
        if self.rank[px] < self.rank[py] {
            self.parent[px] = py;
        } else if self.rank[px] > self.rank[py] {
            self.parent[py] = px;
        } else {
            self.parent[py] = px;
            self.rank[px] += 1;
        }
    }
}

/// Helper struct to track boundary node positions and their assigned IDs
struct BoundaryNodeTracker {
    /// Map from quantized position to NodeId
    position_to_node: AHashMap<(i64, i64), NodeId>,
    /// Quantization factor (positions within this threshold are considered the same)
    quantize_factor: f64,
}

impl BoundaryNodeTracker {
    fn new(quantize_factor: f64) -> Self {
        Self {
            position_to_node: AHashMap::new(),
            quantize_factor,
        }
    }

    /// Get or create a NodeId for a boundary position
    /// Positions within quantize_factor of each other get the same NodeId
    fn get_or_create(&mut self, pos: Coord, node_id_counter: &mut usize) -> NodeId {
        // Quantize position to avoid floating point comparison issues
        let qx = (pos.x / self.quantize_factor).round() as i64;
        let qy = (pos.y / self.quantize_factor).round() as i64;
        let key = (qx, qy);

        self.position_to_node
            .entry(key)
            .or_insert_with(|| {
                // Use sequential IDs for boundary nodes (managed by shared counter)
                let id = NodeId::Intersection(*node_id_counter);
                *node_id_counter += 1;
                id
            })
            .clone()
    }
}

/// Split an edge's geometry at cluster boundaries, assigning unique NodeIds to boundary points.
///
/// Returns a Vec of (cell, GraphEdge, Option<boundary_pos>) where:
/// - cell: the grid cell this segment belongs to
/// - GraphEdge: the segment with proper from/to NodeIds
/// - boundary_pos: if this segment ends at a boundary, the boundary position for deduplication
fn split_edge_at_grid_boundaries_internal(
    edge: &GraphEdge,
    grid_size_meters: f64,
    boundary_tracker: &mut BoundaryNodeTracker,
    node_id_counter: &mut usize,
) -> Vec<((i64, i64), GraphEdge)> {
    let geom = convert_to_geo(&edge.geometry);
    if geom.0.is_empty() {
        return vec![];
    }

    // First, compute raw geometry segments with their cells and boundary points
    let mut raw_segments: Vec<((i64, i64), Vec<Coord>, Option<Coord>)> = Vec::new();
    let mut current_segment: Vec<Coord> = Vec::new();
    let mut current_cell: Option<(i64, i64)> = None;

    for coord in &geom.0 {
        let cell_x = (coord.x / grid_size_meters).floor() as i64;
        let cell_y = (coord.y / grid_size_meters).floor() as i64;
        let cell = (cell_x, cell_y);

        match current_cell {
            None => {
                current_cell = Some(cell);
                current_segment.push(*coord);
            }
            Some(prev_cell) if prev_cell == cell => {
                current_segment.push(*coord);
            }
            Some(prev_cell) => {
                if let Some(last_coord) = current_segment.last() {
                    let boundary_point = calculate_grid_boundary_intersection(
                        *last_coord,
                        *coord,
                        prev_cell,
                        cell,
                        grid_size_meters,
                    );

                    current_segment.push(boundary_point);
                    // Store the boundary point for this segment
                    raw_segments.push((prev_cell, current_segment.clone(), Some(boundary_point)));

                    current_segment = vec![boundary_point, *coord];
                    current_cell = Some(cell);
                }
            }
        }
    }

    if let Some(cell) = current_cell {
        if !current_segment.is_empty() {
            // Last segment has no boundary at the end
            raw_segments.push((cell, current_segment, None));
        }
    }

    // If no splits occurred, return the original edge as-is
    if raw_segments.len() <= 1 {
        if raw_segments.is_empty() {
            return vec![];
        }
        // Single segment - keep original from/to NodeIds
        return vec![(raw_segments[0].0, edge.clone())];
    }

    // CRITICAL FIX: Use boundary position to deduplicate NodeIds
    // When multiple edges cross the same grid boundary at the same (or very close) position,
    // they should share the same NodeId so they reconnect properly after parallel processing.
    let mut result: Vec<((i64, i64), GraphEdge)> = Vec::with_capacity(raw_segments.len());
    let mut prev_boundary_node: Option<NodeId> = None;

    for (idx, (cell, coords, boundary_pos)) in raw_segments.into_iter().enumerate() {
        let from_node = if idx == 0 {
            edge.from.clone()
        } else {
            // Use the boundary node from the previous segment
            prev_boundary_node
                .clone()
                .expect("Previous segment should have set boundary node")
        };

        let to_node = if let Some(bpos) = boundary_pos {
            // This segment ends at a boundary - get or create a shared NodeId for this position
            let node = boundary_tracker.get_or_create(bpos, node_id_counter);
            prev_boundary_node = Some(node.clone());
            node
        } else {
            // Last segment - use original edge's 'to' node
            edge.to.clone()
        };

        let ls = GeoLineString::new(coords);
        let weight = ls.euclidean_length();

        result.push((
            cell,
            GraphEdge {
                from: from_node,
                to: to_node,
                geometry: convert_from_geo(&ls),
                routes: edge.routes.clone(),
                original_shape_ids: edge.original_shape_ids.clone(),
                weight,
                original_edge_index: edge.original_edge_index,
            },
        ));
    }

    result
}

/// Split an edge's geometry at cluster boundaries, assigning unique NodeIds to boundary points.
///
/// Returns a Vec of (cell, GraphEdge) where each GraphEdge has:
/// - First segment: from = original edge's from, to = boundary NodeId
/// - Middle segments: from = previous boundary NodeId, to = next boundary NodeId  
/// - Last segment: from = previous boundary NodeId, to = original edge's to
///
/// NOTE: This is a wrapper that creates a new tracker for single-edge processing.
/// For batch processing, use partition_by_geography which handles deduplication across all edges.
fn split_edge_at_grid_boundaries(
    edge: &GraphEdge,
    grid_size_meters: f64,
    node_id_counter: &mut usize,
) -> Vec<((i64, i64), GraphEdge)> {
    // For single-edge processing, create a temporary tracker
    // Quantize to 1 meter (positions within 1m share the same NodeId)
    let mut tracker = BoundaryNodeTracker::new(1.0);
    split_edge_at_grid_boundaries_internal(edge, grid_size_meters, &mut tracker, node_id_counter)
}

/// Calculate intersection point where line crosses grid boundary
fn calculate_grid_boundary_intersection(
    p1: Coord,
    p2: Coord,
    cell1: (i64, i64),
    cell2: (i64, i64),
    grid_size: f64,
) -> Coord {
    let crossed_x = cell1.0 != cell2.0;
    let crossed_y = cell1.1 != cell2.1;

    if crossed_x && !crossed_y {
        let boundary_x = if cell2.0 > cell1.0 {
            (cell1.0 + 1) as f64 * grid_size
        } else {
            cell1.0 as f64 * grid_size
        };
        let t = (boundary_x - p1.x) / (p2.x - p1.x);
        let y = p1.y + t * (p2.y - p1.y);
        Coord { x: boundary_x, y }
    } else if crossed_y && !crossed_x {
        let boundary_y = if cell2.1 > cell1.1 {
            (cell1.1 + 1) as f64 * grid_size
        } else {
            cell1.1 as f64 * grid_size
        };
        let t = (boundary_y - p1.y) / (p2.y - p1.y);
        let x = p1.x + t * (p2.x - p1.x);
        Coord { x, y: boundary_y }
    } else {
        Coord {
            x: (p1.x + p2.x) / 2.0,
            y: (p1.y + p2.y) / 2.0,
        }
    }
}

/// Partition edges by geographic proximity using spatial grid
pub fn partition_by_geography(
    edges: Vec<GraphEdge>,
    grid_size_meters: f64,
    node_id_counter: &mut usize,
) -> Vec<Vec<GraphEdge>> {
    if edges.is_empty() {
        return vec![];
    }

    println!(
        "Splitting {} edges at grid boundaries (grid size: {}m)...",
        edges.len(),
        grid_size_meters
    );

    // CRITICAL FIX: Use a single BoundaryNodeTracker for ALL edges
    // This ensures that when multiple edges cross the same grid boundary at the same position,
    // they get the same NodeId and will reconnect properly after parallel processing.
    // Quantize to 1 meter (positions within 1m share the same NodeId)
    let mut boundary_tracker = BoundaryNodeTracker::new(1.0);

    let mut split_edges: Vec<(GraphEdge, (i64, i64))> = Vec::new();
    let mut total_segments = 0;

    for edge in &edges {
        let segments = split_edge_at_grid_boundaries_internal(
            edge,
            grid_size_meters,
            &mut boundary_tracker,
            node_id_counter,
        );
        total_segments += segments.len();

        for (cell, split_edge) in segments {
            split_edges.push((split_edge, cell));
        }
    }

    println!(
        "  Split into {} edge segments (avg {:.1} segments per original edge)",
        total_segments,
        total_segments as f64 / edges.len() as f64
    );

    println!(
        "  Created {} unique boundary nodes (deduplication working!)",
        boundary_tracker.position_to_node.len()
    );

    let mut grid_cells: HashMap<(i64, i64), Vec<GraphEdge>> = HashMap::new();
    for (edge, cell) in split_edges {
        grid_cells.entry(cell).or_default().push(edge);
    }

    let cell_keys: Vec<(i64, i64)> = grid_cells.keys().copied().collect();
    let mut cell_to_idx: HashMap<(i64, i64), usize> = HashMap::new();
    for (i, key) in cell_keys.iter().enumerate() {
        cell_to_idx.insert(*key, i);
    }

    let mut uf = UnionFind::new(cell_keys.len());

    for i in 0..cell_keys.len() {
        let (x, y) = cell_keys[i];
        for dx in -1..=1 {
            for dy in -1..=1 {
                if dx == 0 && dy == 0 {
                    continue;
                }
                let neighbor = (x + dx, y + dy);
                if let Some(&j) = cell_to_idx.get(&neighbor) {
                    uf.union(i, j);
                }
            }
        }
    }

    let mut components: HashMap<usize, Vec<GraphEdge>> = HashMap::new();
    for (cell_key, edges) in grid_cells {
        let cell_idx = cell_to_idx[&cell_key];
        let root = uf.find(cell_idx);
        components.entry(root).or_default().extend(edges);
    }

    let result: Vec<Vec<GraphEdge>> = components.into_values().collect();
    let sizes: Vec<usize> = result.iter().map(|c| c.len()).collect();
    let median = {
        let mut sorted = sizes.clone();
        sorted.sort();
        *sorted.get(sorted.len() / 2).unwrap_or(&0)
    };
    println!("Partitioned into {} geographic regions", result.len());
    println!(
        "  Region sizes - min: {}, max: {}, median: {}",
        sizes.iter().min().unwrap_or(&0),
        sizes.iter().max().unwrap_or(&0),
        median
    );
    result
}

/// K-means clustering on edge centroids for splitting large components
fn kmeans_cluster_edges(edges: &[GraphEdge], k: usize, max_iters: usize) -> Vec<usize> {
    if edges.is_empty() || k == 0 {
        return vec![];
    }
    if k >= edges.len() {
        return (0..edges.len()).collect();
    }

    let centroids: Vec<[f64; 2]> = edges
        .iter()
        .map(|e| {
            let geom = convert_to_geo(&e.geometry);
            if geom.0.is_empty() {
                return [0.0, 0.0];
            }
            let mid_idx = geom.0.len() / 2;
            [geom.0[mid_idx].x, geom.0[mid_idx].y]
        })
        .collect();

    let mut centers: Vec<[f64; 2]> = Vec::with_capacity(k);
    centers.push(centroids[0]);

    for _ in 1..k {
        let mut best_idx = 0;
        let mut best_dist = 0.0;
        for (i, c) in centroids.iter().enumerate() {
            let min_dist = centers
                .iter()
                .map(|ctr| (c[0] - ctr[0]).powi(2) + (c[1] - ctr[1]).powi(2))
                .fold(f64::INFINITY, f64::min);
            if min_dist > best_dist {
                best_dist = min_dist;
                best_idx = i;
            }
        }
        centers.push(centroids[best_idx]);
    }

    let mut assignments = vec![0usize; edges.len()];

    for _iter in 0..max_iters {
        let mut changed = false;
        for (i, c) in centroids.iter().enumerate() {
            let mut best_cluster = 0;
            let mut best_dist = f64::INFINITY;
            for (j, ctr) in centers.iter().enumerate() {
                let dist = (c[0] - ctr[0]).powi(2) + (c[1] - ctr[1]).powi(2);
                if dist < best_dist {
                    best_dist = dist;
                    best_cluster = j;
                }
            }
            if assignments[i] != best_cluster {
                assignments[i] = best_cluster;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        let mut sums = vec![[0.0, 0.0]; k];
        let mut counts = vec![0usize; k];
        for (i, c) in centroids.iter().enumerate() {
            let cluster = assignments[i];
            sums[cluster][0] += c[0];
            sums[cluster][1] += c[1];
            counts[cluster] += 1;
        }
        for j in 0..k {
            if counts[j] > 0 {
                centers[j] = [sums[j][0] / counts[j] as f64, sums[j][1] / counts[j] as f64];
            }
        }
    }

    assignments
}

/// Partition a large component using K-means with mode-aware cutting.
pub fn partition_large_component(
    edges: Vec<GraphEdge>,
    max_edges_per_cluster: usize,
) -> Vec<Vec<GraphEdge>> {
    let num_edges = edges.len();
    if num_edges <= max_edges_per_cluster {
        return vec![edges];
    }

    let k = ((num_edges as f64 / max_edges_per_cluster as f64).ceil() as usize).max(2);
    println!(
        "Splitting component ({} edges) into {} clusters",
        num_edges, k
    );

    let assignments = kmeans_cluster_edges(&edges, k, 10);

    let mut clusters: Vec<Vec<GraphEdge>> = vec![vec![]; k];
    for (i, edge) in edges.into_iter().enumerate() {
        clusters[assignments[i]].push(edge);
    }

    let mut node_cluster: HashMap<NodeId, usize> = HashMap::new();
    for (cluster_idx, cluster) in clusters.iter().enumerate() {
        for edge in cluster {
            node_cluster.entry(edge.from.clone()).or_insert(cluster_idx);
            node_cluster.entry(edge.to.clone()).or_insert(cluster_idx);
        }
    }

    let mut route_cluster_counts: HashMap<(String, String), HashMap<usize, usize>> = HashMap::new();
    for (cluster_idx, cluster) in clusters.iter().enumerate() {
        for edge in cluster {
            for (chateau, route_id, route_type) in &edge.routes {
                if *route_type != 2 {
                    let route_key = (chateau.clone(), route_id.clone());
                    *route_cluster_counts
                        .entry(route_key)
                        .or_default()
                        .entry(cluster_idx)
                        .or_insert(0) += 1;
                }
            }
        }
    }

    let mut route_primary_cluster: HashMap<(String, String), usize> = HashMap::new();
    for (route_key, cluster_counts) in route_cluster_counts {
        if let Some((primary_cluster, _)) = cluster_counts.iter().max_by_key(|(_, count)| *count) {
            route_primary_cluster.insert(route_key, *primary_cluster);
        }
    }

    let mut final_clusters: Vec<Vec<GraphEdge>> = vec![vec![]; k];
    for (cluster_idx, cluster) in clusters.into_iter().enumerate() {
        for edge in cluster {
            let from_cluster = node_cluster.get(&edge.from).copied().unwrap_or(cluster_idx);
            let to_cluster = node_cluster.get(&edge.to).copied().unwrap_or(cluster_idx);

            let metro_tram_routes: Vec<_> = edge
                .routes
                .iter()
                .filter(|(_, _, rt)| *rt != 2)
                .map(|(c, r, _)| (c.clone(), r.clone()))
                .collect();

            let target_cluster = if from_cluster != to_cluster {
                if !metro_tram_routes.is_empty() {
                    route_primary_cluster
                        .get(&metro_tram_routes[0])
                        .copied()
                        .unwrap_or(from_cluster)
                } else {
                    from_cluster
                }
            } else {
                cluster_idx
            };

            final_clusters[target_cluster].push(edge);
        }
    }

    final_clusters.retain(|c| !c.is_empty());
    println!(
        "After mode-aware adjustment: {} clusters (before consolidation)",
        final_clusters.len()
    );

    final_clusters
}

/// Consolidate small clusters by merging them with adjacent clusters.
pub fn consolidate_small_clusters(
    mut clusters: Vec<Vec<GraphEdge>>,
    min_cluster_size: usize,
) -> Vec<Vec<GraphEdge>> {
    if clusters.is_empty() {
        return clusters;
    }

    let initial_count = clusters.len();

    let mut cluster_nodes: Vec<HashSet<NodeId>> = Vec::new();
    for cluster in &clusters {
        let mut nodes = HashSet::new();
        for edge in cluster {
            nodes.insert(edge.from.clone());
            nodes.insert(edge.to.clone());
        }
        cluster_nodes.push(nodes);
    }

    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); clusters.len()];
    for i in 0..clusters.len() {
        for j in (i + 1)..clusters.len() {
            if cluster_nodes[i].intersection(&cluster_nodes[j]).count() > 0 {
                adjacency[i].push(j);
                adjacency[j].push(i);
            }
        }
    }

    let mut merged_into: Vec<Option<usize>> = vec![None; clusters.len()];

    let mut cluster_indices: Vec<usize> = (0..clusters.len()).collect();
    cluster_indices.sort_by_key(|&i| clusters[i].len());

    for &cluster_idx in &cluster_indices {
        if merged_into[cluster_idx].is_some() {
            continue;
        }

        if clusters[cluster_idx].len() >= min_cluster_size {
            continue;
        }

        let mut best_target: Option<usize> = None;
        let mut best_size = 0;

        for &adj_idx in &adjacency[cluster_idx] {
            if merged_into[adj_idx].is_some() {
                continue;
            }

            let adj_size = clusters[adj_idx].len();
            if adj_size > best_size {
                best_size = adj_size;
                best_target = Some(adj_idx);
            }
        }

        if let Some(target) = best_target {
            merged_into[cluster_idx] = Some(target);

            let adjacencies_to_add: Vec<usize> = adjacency[cluster_idx]
                .iter()
                .filter(|&&adj_idx| adj_idx != target && !adjacency[target].contains(&adj_idx))
                .copied()
                .collect();

            adjacency[target].extend(adjacencies_to_add);
        }
    }

    let mut final_clusters: Vec<Vec<GraphEdge>> = Vec::new();
    let mut old_to_new: Vec<Option<usize>> = vec![None; clusters.len()];

    for (old_idx, cluster) in clusters.into_iter().enumerate() {
        let mut target = old_idx;
        while let Some(next_target) = merged_into[target] {
            target = next_target;
        }

        match old_to_new[target] {
            Some(new_idx) => {
                final_clusters[new_idx].extend(cluster);
            }
            None => {
                let new_idx = final_clusters.len();
                old_to_new[target] = Some(new_idx);
                final_clusters.push(cluster);
            }
        }
    }

    println!(
        "Consolidated {} clusters into {} (merged {} small clusters)",
        initial_count,
        final_clusters.len(),
        initial_count - final_clusters.len()
    );
    println!(
        "Final cluster sizes: min={}, max={}, avg={:.1}",
        final_clusters.iter().map(|c| c.len()).min().unwrap_or(0),
        final_clusters.iter().map(|c| c.len()).max().unwrap_or(0),
        final_clusters.iter().map(|c| c.len()).sum::<usize>() as f64 / final_clusters.len() as f64
    );

    if std::env::var("EXPORT_GEOJSON").unwrap_or_default() == "1" {
        if let Err(e) =
            export_cluster_boundaries_geojson(&final_clusters, "globeflower_clusters.geojson")
        {
            eprintln!("Warning: Failed to export cluster GeoJSON: {}", e);
        }
    }

    final_clusters
}

/// Export cluster boundaries as GeoJSON polygons for visualization
pub fn export_cluster_boundaries_geojson(
    clusters: &[Vec<GraphEdge>],
    filename: &str,
) -> Result<()> {
    use geo::ConvexHull;
    use std::fs::File;
    use std::io::Write;

    let mut features = Vec::new();

    let to_wgs84 = |x: f64, y: f64| -> (f64, f64) {
        let lon = x / 6378137.0 * 57.29577951308232;
        let lat =
            (std::f64::consts::PI / 2.0 - 2.0 * (-y / 6378137.0).exp().atan()) * 57.29577951308232;
        (lon, lat)
    };

    for (idx, cluster) in clusters.iter().enumerate() {
        if cluster.is_empty() {
            continue;
        }

        let mut points: Vec<Coord> = Vec::new();
        for edge in cluster {
            let geom = convert_to_geo(&edge.geometry);
            points.extend(geom.0);
        }

        if points.is_empty() {
            continue;
        }

        let multi_point =
            geo::MultiPoint::new(points.iter().map(|c| GeoPoint::new(c.x, c.y)).collect());
        let hull = multi_point.convex_hull();

        let coords_str: Vec<String> = hull
            .exterior()
            .points()
            .map(|p| {
                let (lon, lat) = to_wgs84(p.x(), p.y());
                format!("[{},{}]", lon, lat)
            })
            .collect();

        let polygon = format!("[[{}]]", coords_str.join(","));

        let feature = format!(
            r#"{{"type":"Feature","properties":{{"cluster_id":{},"edge_count":{}}},"geometry":{{"type":"Polygon","coordinates":{}}}}}"#,
            idx,
            cluster.len(),
            polygon
        );
        features.push(feature);
    }

    let geojson = format!(
        r#"{{"type":"FeatureCollection","features":[{}]}}"#,
        features.join(",")
    );

    let mut file = File::create(filename)?;
    file.write_all(geojson.as_bytes())?;
    println!("Exported cluster boundaries (convex hulls) to {}", filename);

    Ok(())
}
