use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableStop {
    pub id: String,
    pub code: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub location_type: i16,
    pub parent_station: Option<String>,
    pub zone_id: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub timezone: Option<String>,
    pub platform_code: Option<String>,
    pub level_id: Option<String>,
    pub routes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableStopCluster {
    pub cluster_id: usize,
    pub centroid: [f64; 2], // x, y
    pub stops: Vec<SerializableStop>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LandMass {
    pub id: usize,
    pub clusters: Vec<usize>,
    pub edges: Vec<usize>, // Indicies into the edges vector
    pub stop_count: usize,
    pub route_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NodeId {
    Cluster(usize),
    /// Intersection node with (cluster_id, local_id) tuple to ensure global uniqueness across clusters
    Intersection(usize, usize),
    Split(usize, usize), // (edge_index, split_index)
    /// OSM junction node ID for OSM-accelerated collapse
    OsmJunction(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableGraphEdge {
    pub from: NodeId,
    pub to: NodeId,
    pub geometry: Vec<[f64; 2]>,          // x, y points
    pub route_ids: Vec<(String, String)>, // (chateau, route_id)
    pub weight: f64,
    pub original_edge_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRestriction {
    pub from_edge_index: usize,
    pub to_edge_index: usize,
    pub route_id: (String, String), // (chateau, route_id)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableExportGraph {
    pub land_masses: Vec<LandMass>,
    pub edges: Vec<SerializableGraphEdge>,
    pub clusters: Vec<SerializableStopCluster>,
    pub restrictions: Vec<TurnRestriction>,
}
