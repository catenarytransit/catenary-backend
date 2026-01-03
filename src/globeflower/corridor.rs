use crate::geometry_utils;
use crate::osm_loader::OsmRailIndex;
use crate::osm_types::{AtomicEdge, AtomicEdgeId, RailMode, ZClass};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, info};
use rayon::prelude::*;

use serde::{Deserialize, Serialize};

/// Identifier for a corridor cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorridorId(pub u64);

/// A cluster of parallel tracks merged into a single corridor
#[derive(Debug, Clone)]
pub struct CorridorCluster {
    pub id: CorridorId,
    /// Member atomic edge IDs
    pub member_edges: Vec<AtomicEdgeId>,
    /// Computed centerline geometry (lon, lat)
    pub centerline: Vec<(f64, f64)>,
    /// Shared z-class of all members
    pub z_class: ZClass,
    /// Shared mode of all members
    pub mode: RailMode,
    /// Total length of centerline in meters
    pub length_m: f64,
}

/// Configuration for corridor detection
#[derive(Debug, Clone)]
pub struct CorridorConfig {
    /// Maximum Hausdorff distance for parallel track detection (meters)
    pub max_hausdorff_m: f64,
    /// Maximum angle difference for parallel tracks (degrees)
    pub max_angle_diff_deg: f64,
    /// Minimum overlap ratio (0.0 to 1.0)
    pub min_overlap_ratio: f64,
    /// Sampling interval for centerline computation (meters)
    pub centerline_sample_interval: f64,
}

impl Default for CorridorConfig {
    fn default() -> Self {
        Self {
            max_hausdorff_m: 12.0,
            max_angle_diff_deg: 15.0,
            min_overlap_ratio: 0.6,
            centerline_sample_interval: 10.0,
        }
    }
}

/// Union-Find data structure for clustering
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]);
        }
        self.parent[x]
    }

    fn union(&mut self, x: usize, y: usize) {
        let px = self.find(x);
        let py = self.find(y);
        if px == py {
            return;
        }
        match self.rank[px].cmp(&self.rank[py]) {
            std::cmp::Ordering::Less => self.parent[px] = py,
            std::cmp::Ordering::Greater => self.parent[py] = px,
            std::cmp::Ordering::Equal => {
                self.parent[py] = px;
                self.rank[px] += 1;
            }
        }
    }
}

/// Corridor builder for detecting and merging parallel tracks
pub struct CorridorBuilder<'a> {
    index: &'a OsmRailIndex,
    config: CorridorConfig,
}

impl<'a> CorridorBuilder<'a> {
    pub fn new(index: &'a OsmRailIndex, config: CorridorConfig) -> Self {
        Self { index, config }
    }

    /// Build corridors from all edges in the index
    pub fn build_corridors(&self) -> Vec<CorridorCluster> {
        info!(
            "Building corridor clusters from {} edges",
            self.index.edges.len()
        );

        // Group edges by (mode, z_class) - only merge within same group
        let mut groups: HashMap<(RailMode, ZClass), Vec<usize>> = HashMap::new();
        for (i, edge) in self.index.edges.iter().enumerate() {
            groups.entry((edge.mode, edge.z_class)).or_default().push(i);
        }

        let mut all_clusters = Vec::new();
        let mut next_id = 0u64;

        for ((mode, z_class), edge_indices) in groups {
            debug!(
                "Processing group ({:?}, {:?}) with {} edges",
                mode,
                z_class,
                edge_indices.len()
            );

            let group_clusters = self.cluster_group(&edge_indices, mode, z_class, &mut next_id);
            all_clusters.extend(group_clusters);
        }

        info!("Built {} corridor clusters", all_clusters.len());
        all_clusters
    }

    /// Cluster edges within a single (mode, z_class) group
    fn cluster_group(
        &self,
        edge_indices: &[usize],
        mode: RailMode,
        z_class: ZClass,
        next_id: &mut u64,
    ) -> Vec<CorridorCluster> {
        if edge_indices.is_empty() {
            return vec![];
        }

        let n = edge_indices.len();
        let mut uf = UnionFind::new(n);

        // Find parallel pairs and union them
        // For efficiency, use spatial locality - only check nearby edges
        let edges: Vec<&AtomicEdge> = edge_indices.iter().map(|&i| &self.index.edges[i]).collect();

        // Check all pairs (O(n²) but filtered by spatial proximity)
        for i in 0..n {
            for j in (i + 1)..n {
                if self.are_parallel(&edges[i], &edges[j]) {
                    uf.union(i, j);
                }
            }
        }

        // Group by cluster root
        let mut cluster_members: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = uf.find(i);
            cluster_members.entry(root).or_default().push(i);
        }

        // Build corridor clusters
        cluster_members
            .into_values()
            .map(|members| {
                let member_edges: Vec<AtomicEdgeId> =
                    members.iter().map(|&i| edges[i].id).collect();

                let geometries: Vec<&[(f64, f64)]> = members
                    .iter()
                    .map(|&i| edges[i].geometry.as_slice())
                    .collect();

                let centerline = self.compute_centerline(&geometries);
                let length_m = geometry_utils::polyline_length(&centerline);

                let id = CorridorId(*next_id);
                *next_id += 1;

                CorridorCluster {
                    id,
                    member_edges,
                    centerline,
                    z_class,
                    mode,
                    length_m,
                }
            })
            .collect()
    }

    /// Check if two edges are parallel (candidates for merging)
    fn are_parallel(&self, a: &AtomicEdge, b: &AtomicEdge) -> bool {
        // Must be same mode and z_class (already filtered, but double-check)
        if a.mode != b.mode || a.z_class != b.z_class {
            return false;
        }

        // Check Hausdorff distance
        let hausdorff = geometry_utils::hausdorff_distance(&a.geometry, &b.geometry);
        if hausdorff > self.config.max_hausdorff_m {
            return false;
        }

        // Check angle compatibility
        if let (Some(bearing_a), Some(bearing_b)) = (
            geometry_utils::polyline_bearing(&a.geometry),
            geometry_utils::polyline_bearing(&b.geometry),
        ) {
            let angle_diff = geometry_utils::bearing_difference(bearing_a, bearing_b);
            // Parallel should be ~0° or ~180° (same or opposite direction)
            let normalized_diff = angle_diff.min(180.0 - angle_diff);
            if normalized_diff > self.config.max_angle_diff_deg {
                return false;
            }
        }

        // Check overlap ratio
        let overlap =
            geometry_utils::overlap_ratio(&a.geometry, &b.geometry, self.config.max_hausdorff_m);
        if overlap < self.config.min_overlap_ratio {
            return false;
        }

        true
    }

    /// Compute weighted-average centerline from multiple geometries
    fn compute_centerline(&self, geometries: &[&[(f64, f64)]]) -> Vec<(f64, f64)> {
        if geometries.is_empty() {
            return vec![];
        }
        if geometries.len() == 1 {
            return geometries[0].to_vec();
        }

        // Use equal weights for now
        let weighted: Vec<(&[(f64, f64)], f64)> = geometries.iter().map(|&g| (g, 1.0)).collect();

        geometry_utils::weighted_average_centerline(
            &weighted,
            self.config.centerline_sample_interval,
        )
    }
}

/// Mapping from original edges to their corridor
pub struct CorridorIndex {
    /// Edge ID -> Corridor ID
    pub edge_to_corridor: HashMap<AtomicEdgeId, CorridorId>,
    /// Corridor ID -> Cluster data
    pub corridors: HashMap<CorridorId, CorridorCluster>,
}

impl CorridorIndex {
    /// Build index from corridor clusters
    pub fn from_clusters(clusters: Vec<CorridorCluster>) -> Self {
        let mut edge_to_corridor = HashMap::new();
        let mut corridors = HashMap::new();

        for cluster in clusters {
            for &edge_id in &cluster.member_edges {
                edge_to_corridor.insert(edge_id, cluster.id);
            }
            corridors.insert(cluster.id, cluster);
        }

        Self {
            edge_to_corridor,
            corridors,
        }
    }

    /// Get the corridor for an edge
    pub fn corridor_for_edge(&self, edge_id: AtomicEdgeId) -> Option<&CorridorCluster> {
        let corridor_id = self.edge_to_corridor.get(&edge_id)?;
        self.corridors.get(corridor_id)
    }

    /// Get all corridors
    pub fn all_corridors(&self) -> impl Iterator<Item = &CorridorCluster> {
        self.corridors.values()
    }
}
