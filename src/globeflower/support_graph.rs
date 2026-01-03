use crate::corridor::{CorridorCluster, CorridorId, CorridorIndex};
use crate::geometry_utils;
use crate::osm_types::{Line, LineId, LineOcc, ZClass};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, info, trace};
use rstar::{AABB, RTree, primitives::GeomWithData};
use serde::{Deserialize, Serialize};

/// Unique identifier for a support graph node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SupportNodeId(pub u64);

/// Unique identifier for a support graph edge
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SupportEdgeId(pub u64);

/// A node in the support graph (free line graph skeleton)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupportNode {
    pub id: SupportNodeId,
    pub position: (f64, f64),
    pub z_class: ZClass,
    /// Station info if this is a station node
    pub station_id: Option<String>,
    pub station_label: Option<String>,
}

/// An edge in the support graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupportEdge {
    pub id: SupportEdgeId,
    pub from: SupportNodeId,
    pub to: SupportNodeId,
    pub geometry: Vec<(f64, f64)>,
    pub z_class: ZClass,
    pub length_m: f64,
    /// Lines that traverse this edge
    pub lines: Vec<LineOcc>,
    /// Source corridor IDs
    pub source_corridors: Vec<CorridorId>,
}

/// Configuration for support graph construction
#[derive(Debug, Clone)]
pub struct SupportGraphConfig {
    /// Sampling interval along corridors (meters)
    pub sample_interval_m: f64,
    /// Merge threshold - nodes within this distance are merged (meters)
    pub merge_threshold_m: f64,
    /// Maximum angle difference for direction compatibility (degrees)
    pub max_angle_diff_deg: f64,
    /// Convergence threshold for iterative refinement
    pub convergence_threshold: f64,
}

impl Default for SupportGraphConfig {
    fn default() -> Self {
        Self {
            sample_interval_m: 5.0,
            merge_threshold_m: 15.0,
            max_angle_diff_deg: 30.0,
            convergence_threshold: 0.002, // 0.2%
        }
    }
}

/// The support graph (overlap-free line graph skeleton)
pub struct SupportGraph {
    pub nodes: HashMap<SupportNodeId, SupportNode>,
    pub edges: HashMap<SupportEdgeId, SupportEdge>,
    /// Node adjacency: node -> list of edges
    pub node_edges: HashMap<SupportNodeId, Vec<SupportEdgeId>>,
    /// Spatial index per z-class
    node_trees: HashMap<ZClass, RTree<GeomWithData<[f64; 2], SupportNodeId>>>,
    next_node_id: u64,
    next_edge_id: u64,
}

impl SupportGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            node_edges: HashMap::new(),
            node_trees: HashMap::new(),
            next_node_id: 0,
            next_edge_id: 0,
        }
    }

    /// Build support graph from corridor clusters
    pub fn from_corridors(corridors: &CorridorIndex, config: &SupportGraphConfig) -> Self {
        let mut graph = Self::new();

        // Sort corridors by length (longer first for stable merging)
        let mut sorted_corridors: Vec<_> = corridors.all_corridors().collect();
        sorted_corridors.sort_by(|a, b| {
            b.length_m
                .partial_cmp(&a.length_m)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        info!(
            "Building support graph from {} corridors",
            sorted_corridors.len()
        );

        for corridor in sorted_corridors {
            graph.insert_corridor(corridor, config);
        }

        // Iterative refinement
        let mut iteration = 0;
        loop {
            let change = graph.refine_iteration(config);
            iteration += 1;
            debug!("Refinement iteration {}: change = {:.4}", iteration, change);

            if change < config.convergence_threshold || iteration > 10 {
                break;
            }
        }

        info!(
            "Support graph complete: {} nodes, {} edges",
            graph.nodes.len(),
            graph.edges.len()
        );

        graph
    }

    /// Insert a corridor into the support graph
    fn insert_corridor(&mut self, corridor: &CorridorCluster, config: &SupportGraphConfig) {
        if corridor.centerline.len() < 2 {
            return;
        }

        // Sample points along the corridor
        let samples =
            geometry_utils::sample_along_polyline(&corridor.centerline, config.sample_interval_m);

        if samples.len() < 2 {
            return;
        }

        // Map each sample to a support node (new or existing)
        let mut sample_nodes = Vec::with_capacity(samples.len());
        let mut prev_bearing: Option<f64> = None;

        for (i, &(lon, lat)) in samples.iter().enumerate() {
            // Calculate local bearing for direction check
            let bearing = if i < samples.len() - 1 {
                geometry_utils::bearing((lon, lat), samples[i + 1])
            } else if i > 0 {
                geometry_utils::bearing(samples[i - 1], (lon, lat))
            } else {
                0.0
            };

            let node_id = self.find_or_create_node((lon, lat), corridor.z_class, bearing, config);
            sample_nodes.push(node_id);
            prev_bearing = Some(bearing);
        }

        // Create edges between consecutive distinct nodes
        let mut prev_node: Option<SupportNodeId> = None;
        let mut edge_geom: Vec<(f64, f64)> = Vec::new();

        for (i, &node_id) in sample_nodes.iter().enumerate() {
            edge_geom.push(samples[i]);

            if let Some(prev_id) = prev_node {
                if node_id != prev_id {
                    // Create edge from prev_id to node_id
                    self.create_or_update_edge(
                        prev_id,
                        node_id,
                        std::mem::take(&mut edge_geom),
                        corridor.z_class,
                        corridor.id,
                    );
                    edge_geom.push(samples[i]);
                }
            }
            prev_node = Some(node_id);
        }
    }

    /// Find existing node or create new one
    fn find_or_create_node(
        &mut self,
        pos: (f64, f64),
        z_class: ZClass,
        bearing: f64,
        config: &SupportGraphConfig,
    ) -> SupportNodeId {
        // Only search within same z-class
        if let Some(tree) = self.node_trees.get(&z_class) {
            // Convert threshold to approximate degrees
            let threshold_deg = config.merge_threshold_m / 111320.0;
            let envelope = AABB::from_corners(
                [pos.0 - threshold_deg, pos.1 - threshold_deg],
                [pos.0 + threshold_deg, pos.1 + threshold_deg],
            );

            // Find candidates
            let candidates: Vec<_> = tree.locate_in_envelope(&envelope).collect();

            for candidate in candidates {
                let candidate_id = candidate.data;
                if let Some(node) = self.nodes.get(&candidate_id) {
                    let dist = geometry_utils::polyline_length(&[pos, node.position]);

                    if dist <= config.merge_threshold_m {
                        // Check direction compatibility
                        if self.is_direction_compatible(candidate_id, bearing, config) {
                            return candidate_id;
                        }
                    }
                }
            }
        }

        // Create new node
        let id = SupportNodeId(self.next_node_id);
        self.next_node_id += 1;

        let node = SupportNode {
            id,
            position: pos,
            z_class,
            station_id: None,
            station_label: None,
        };

        self.nodes.insert(id, node);
        self.node_edges.insert(id, Vec::new());

        // Add to spatial index
        self.node_trees
            .entry(z_class)
            .or_insert_with(RTree::new)
            .insert(GeomWithData::new([pos.0, pos.1], id));

        id
    }

    /// Check if merging with a node is direction-compatible
    fn is_direction_compatible(
        &self,
        node_id: SupportNodeId,
        new_bearing: f64,
        config: &SupportGraphConfig,
    ) -> bool {
        let edges = match self.node_edges.get(&node_id) {
            Some(e) => e,
            None => return true,
        };

        if edges.is_empty() {
            return true;
        }

        // Check angles of existing edges at this node
        for edge_id in edges {
            if let Some(edge) = self.edges.get(edge_id) {
                if let Some(edge_bearing) = self.edge_bearing_at_node(edge, node_id) {
                    let diff = geometry_utils::bearing_difference(edge_bearing, new_bearing);
                    // Allow merge if roughly parallel or anti-parallel
                    let normalized = diff.min(180.0 - diff);
                    if normalized > config.max_angle_diff_deg {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Get the bearing of an edge at a specific node
    fn edge_bearing_at_node(&self, edge: &SupportEdge, node_id: SupportNodeId) -> Option<f64> {
        if edge.geometry.len() < 2 {
            return None;
        }

        if edge.from == node_id {
            Some(geometry_utils::bearing(edge.geometry[0], edge.geometry[1]))
        } else if edge.to == node_id {
            let n = edge.geometry.len();
            Some(geometry_utils::bearing(
                edge.geometry[n - 1],
                edge.geometry[n - 2],
            ))
        } else {
            None
        }
    }

    /// Create or update an edge between two nodes
    fn create_or_update_edge(
        &mut self,
        from: SupportNodeId,
        to: SupportNodeId,
        geometry: Vec<(f64, f64)>,
        z_class: ZClass,
        corridor_id: CorridorId,
    ) {
        // Check if edge already exists
        if let Some(edges) = self.node_edges.get(&from) {
            for &edge_id in edges {
                if let Some(edge) = self.edges.get_mut(&edge_id) {
                    if (edge.from == from && edge.to == to) || (edge.from == to && edge.to == from)
                    {
                        // Update existing edge
                        if !edge.source_corridors.contains(&corridor_id) {
                            edge.source_corridors.push(corridor_id);
                        }
                        return;
                    }
                }
            }
        }

        // Create new edge
        let id = SupportEdgeId(self.next_edge_id);
        self.next_edge_id += 1;

        let length_m = geometry_utils::polyline_length(&geometry);

        let edge = SupportEdge {
            id,
            from,
            to,
            geometry,
            z_class,
            length_m,
            lines: Vec::new(),
            source_corridors: vec![corridor_id],
        };

        self.edges.insert(id, edge);
        self.node_edges.entry(from).or_default().push(id);
        self.node_edges.entry(to).or_default().push(id);
    }

    /// One iteration of refinement
    fn refine_iteration(&mut self, _config: &SupportGraphConfig) -> f64 {
        // Calculate total length before
        let total_before: f64 = self.edges.values().map(|e| e.length_m).sum();

        // TODO: Implement blocking set and sub-sampling artifact prevention
        // For now, just return 0 to indicate convergence

        let total_after: f64 = self.edges.values().map(|e| e.length_m).sum();

        if total_before > 0.0 {
            (total_before - total_after).abs() / total_before
        } else {
            0.0
        }
    }

    /// Add line occurrence to an edge
    pub fn add_line_to_edge(&mut self, edge_id: SupportEdgeId, line_occ: LineOcc) {
        if let Some(edge) = self.edges.get_mut(&edge_id) {
            // Check if already present
            if !edge.lines.iter().any(|l| l.line == line_occ.line) {
                edge.lines.push(line_occ);
            }
        }
    }

    /// Get edges at a node
    pub fn edges_at_node(&self, node_id: SupportNodeId) -> Vec<&SupportEdge> {
        self.node_edges
            .get(&node_id)
            .map(|ids| ids.iter().filter_map(|id| self.edges.get(id)).collect())
            .unwrap_or_default()
    }

    /// Rebuild spatial indices after modifications
    pub fn rebuild_indices(&mut self) {
        self.node_trees.clear();

        for (id, node) in &self.nodes {
            self.node_trees
                .entry(node.z_class)
                .or_insert_with(RTree::new)
                .insert(GeomWithData::new([node.position.0, node.position.1], *id));
        }
    }
}

impl Default for SupportGraph {
    fn default() -> Self {
        Self::new()
    }
}
