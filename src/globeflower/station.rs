use crate::geometry_utils;
use crate::osm_types::ZClass;
use crate::support_graph::{SupportEdgeId, SupportGraph, SupportNodeId};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, info};

/// A cluster of GTFS stops representing a single station
#[derive(Debug, Clone)]
pub struct StopCluster {
    pub label: String,
    pub stop_ids: Vec<String>,
    pub centroid: (f64, f64),
}

/// Configuration for station insertion
#[derive(Debug, Clone)]
pub struct StationConfig {
    /// Maximum radius for clustering stops (meters)
    pub cluster_radius_m: f64,
    /// Maximum distance from station centroid to insert node (meters)
    pub snap_radius_m: f64,
    /// Minimum bearing difference to split multi-direction stations (degrees)
    pub direction_split_deg: f64,
}

impl Default for StationConfig {
    fn default() -> Self {
        Self {
            cluster_radius_m: 500.0,
            snap_radius_m: 100.0,
            direction_split_deg: 60.0,
        }
    }
}

/// Station insertion result
#[derive(Debug, Clone)]
pub struct InsertedStation {
    pub station_id: String,
    pub station_label: String,
    /// Node IDs that represent this station (multiple for multi-level)
    pub node_ids: Vec<SupportNodeId>,
}

/// Station handler for inserting station nodes into the support graph
pub struct StationHandler<'a> {
    graph: &'a mut SupportGraph,
    config: StationConfig,
    next_station_id: u64,
}

impl<'a> StationHandler<'a> {
    pub fn new(graph: &'a mut SupportGraph, config: StationConfig) -> Self {
        Self {
            graph,
            config,
            next_station_id: 0,
        }
    }

    /// Insert stations from stop clusters
    pub fn insert_stations(&mut self, clusters: &[StopCluster]) -> Vec<InsertedStation> {
        info!("Inserting {} station clusters", clusters.len());

        let mut results = Vec::with_capacity(clusters.len());

        for cluster in clusters {
            if let Some(result) = self.insert_station_cluster(cluster) {
                results.push(result);
            }
        }

        info!(
            "Inserted {} stations with {} total nodes",
            results.len(),
            results.iter().map(|r| r.node_ids.len()).sum::<usize>()
        );

        results
    }

    /// Insert a single station cluster
    fn insert_station_cluster(&mut self, cluster: &StopCluster) -> Option<InsertedStation> {
        let station_id = format!("station_{}", self.next_station_id);
        self.next_station_id += 1;

        // Find nearby edges in the support graph
        let nearby_edges = self.find_nearby_edges(cluster.centroid);
        if nearby_edges.is_empty() {
            debug!("No edges near station '{}'", cluster.label);
            return None;
        }

        // Group edges by z-class
        let mut edges_by_z: HashMap<ZClass, Vec<SupportEdgeId>> = HashMap::new();
        for edge_id in &nearby_edges {
            if let Some(edge) = self.graph.edges.get(edge_id) {
                edges_by_z.entry(edge.z_class).or_default().push(*edge_id);
            }
        }

        // Create station node for each z-class group
        let mut node_ids = Vec::new();

        for (z_class, edge_ids) in edges_by_z {
            // Optionally split by direction for complex hubs
            let direction_groups = self.split_by_direction(&edge_ids, cluster.centroid);

            for group in direction_groups {
                if let Some(node_id) = self.insert_station_node(
                    cluster.centroid,
                    z_class,
                    &station_id,
                    &cluster.label,
                    &group,
                ) {
                    node_ids.push(node_id);
                }
            }
        }

        if node_ids.is_empty() {
            return None;
        }

        Some(InsertedStation {
            station_id,
            station_label: cluster.label.clone(),
            node_ids,
        })
    }

    /// Find edges near a point
    fn find_nearby_edges(&self, center: (f64, f64)) -> Vec<SupportEdgeId> {
        let threshold_deg = self.config.snap_radius_m / 111320.0;

        self.graph
            .edges
            .iter()
            .filter(|(_, edge)| {
                // Check if any point on edge geometry is within radius
                edge.geometry.iter().any(|&(lon, lat)| {
                    let dx = (lon - center.0).abs();
                    let dy = (lat - center.1).abs();
                    dx <= threshold_deg && dy <= threshold_deg
                })
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Split edges into direction groups based on bearing at station
    fn split_by_direction(
        &self,
        edge_ids: &[SupportEdgeId],
        center: (f64, f64),
    ) -> Vec<Vec<SupportEdgeId>> {
        if edge_ids.len() <= 2 {
            return vec![edge_ids.to_vec()];
        }

        // Calculate bearing for each edge relative to center
        let mut edge_bearings: Vec<(SupportEdgeId, f64)> = edge_ids
            .iter()
            .filter_map(|&id| {
                let edge = self.graph.edges.get(&id)?;
                let bearing = self.edge_bearing_from_center(&edge.geometry, center)?;
                Some((id, bearing))
            })
            .collect();

        // Sort by bearing
        edge_bearings.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Group consecutive edges if angle gap is small
        let mut groups: Vec<Vec<SupportEdgeId>> = Vec::new();
        let mut current_group: Vec<SupportEdgeId> = Vec::new();
        let mut prev_bearing: Option<f64> = None;

        for (id, bearing) in edge_bearings {
            if let Some(prev) = prev_bearing {
                let diff = geometry_utils::bearing_difference(prev, bearing);
                if diff > self.config.direction_split_deg {
                    if !current_group.is_empty() {
                        groups.push(std::mem::take(&mut current_group));
                    }
                }
            }
            current_group.push(id);
            prev_bearing = Some(bearing);
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        // If only one group, return as-is
        if groups.len() <= 1 {
            return vec![edge_ids.to_vec()];
        }

        groups
    }

    /// Calculate bearing of edge from a center point
    fn edge_bearing_from_center(&self, geometry: &[(f64, f64)], center: (f64, f64)) -> Option<f64> {
        if geometry.is_empty() {
            return None;
        }

        // Find closest point on edge to center
        let (_, _, closest) = geometry_utils::project_point_to_polyline(center, geometry)?;
        Some(geometry_utils::bearing(center, closest))
    }

    /// Insert a station node for a specific z-class and direction group
    fn insert_station_node(
        &mut self,
        center: (f64, f64),
        z_class: ZClass,
        station_id: &str,
        station_label: &str,
        _edge_group: &[SupportEdgeId],
    ) -> Option<SupportNodeId> {
        // Find best existing node near center with matching z-class
        let threshold_deg = self.config.snap_radius_m / 111320.0;

        let mut best_node: Option<(SupportNodeId, f64)> = None;

        for (id, node) in &self.graph.nodes {
            if node.z_class != z_class {
                continue;
            }

            let dx = (node.position.0 - center.0).abs();
            let dy = (node.position.1 - center.1).abs();

            if dx <= threshold_deg && dy <= threshold_deg {
                let dist = geometry_utils::polyline_length(&[center, node.position]);
                if best_node.map_or(true, |(_, d)| dist < d) {
                    best_node = Some((*id, dist));
                }
            }
        }

        if let Some((node_id, _)) = best_node {
            // Update existing node with station info
            if let Some(node) = self.graph.nodes.get_mut(&node_id) {
                node.station_id = Some(station_id.to_string());
                node.station_label = Some(station_label.to_string());
            }
            return Some(node_id);
        }

        // No suitable node found - could create new one, but for now skip
        debug!(
            "No suitable node for station '{}' at {:?}",
            station_label, center
        );
        None
    }
}

/// Cluster GTFS stops by name and proximity
pub fn cluster_stops(
    stops: &[(String, String, f64, f64)], // (stop_id, name, lon, lat)
    radius_m: f64,
) -> Vec<StopCluster> {
    if stops.is_empty() {
        return vec![];
    }

    // Group by normalized name
    let mut by_name: HashMap<String, Vec<(String, f64, f64)>> = HashMap::new();
    for (id, name, lon, lat) in stops {
        let key = normalize_station_name(name);
        by_name
            .entry(key)
            .or_default()
            .push((id.clone(), *lon, *lat));
    }

    // For each name group, cluster by proximity
    let threshold_deg = radius_m / 111320.0;
    let mut clusters = Vec::new();

    for (name, group_stops) in by_name {
        // Simple single-cluster per name for now
        // Could use DBSCAN for more complex clustering
        let centroid = calculate_centroid(&group_stops);
        let stop_ids: Vec<String> = group_stops.into_iter().map(|(id, _, _)| id).collect();

        clusters.push(StopCluster {
            label: name,
            stop_ids,
            centroid,
        });
    }

    clusters
}

/// Normalize station name for clustering
fn normalize_station_name(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .replace("hauptbahnhof", "hbf")
        .replace("central station", "central")
        .replace("gare de ", "")
        .replace("station", "")
        .trim()
        .to_string()
}

/// Calculate centroid of a group of stops
fn calculate_centroid(stops: &[(String, f64, f64)]) -> (f64, f64) {
    if stops.is_empty() {
        return (0.0, 0.0);
    }

    let sum_lon: f64 = stops.iter().map(|(_, lon, _)| lon).sum();
    let sum_lat: f64 = stops.iter().map(|(_, _, lat)| lat).sum();
    let n = stops.len() as f64;

    (sum_lon / n, sum_lat / n)
}
