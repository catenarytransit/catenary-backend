use anyhow::Result;
use catenary::routing_common::osm_graph::{self, StreetData};
use catenary::routing_common::transit_graph::{
    self, EdgeEntry, GlobalPatternIndex, TransferChunk, TransitPartition,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct GraphManager {
    pub transit_partitions: RwLock<HashMap<u32, Arc<TransitPartition>>>,
    pub street_partitions: RwLock<HashMap<u32, Arc<StreetData>>>,
    pub transfer_partitions: HashMap<u32, TransferChunk>,
    pub edge_partitions: RwLock<HashMap<u32, Arc<Vec<EdgeEntry>>>>,
    pub global_index: Option<GlobalPatternIndex>,
    pub global_timetable: Option<catenary::routing_common::transit_graph::GlobalTimetable>,
    pub direct_connections: Option<catenary::routing_common::transit_graph::DirectConnections>,
    pub manifest: Option<catenary::routing_common::transit_graph::Manifest>,
    pub base_path: Option<std::path::PathBuf>,
}

impl GraphManager {
    pub fn new() -> Self {
        Self {
            transit_partitions: RwLock::new(HashMap::new()),
            street_partitions: RwLock::new(HashMap::new()),
            transfer_partitions: HashMap::new(),
            edge_partitions: RwLock::new(HashMap::new()),
            global_index: None,
            global_timetable: None,
            direct_connections: None,
            manifest: None,
            base_path: None,
        }
    }

    pub fn load_from_directory(&mut self, dir_path: &str) -> Result<()> {
        let path = Path::new(dir_path);
        self.base_path = Some(path.to_path_buf());

        // Load Global Patterns
        let global_path = path.join("global_patterns.bincode");
        if global_path.exists() {
            println!("Loading global patterns from {:?}", global_path);
            let index: GlobalPatternIndex =
                transit_graph::load_bincode(global_path.to_str().unwrap())?;
            self.global_index = Some(index);
        }

        // Load Global Timetable
        let timetable_path = path.join("global_timetable.bincode");
        if timetable_path.exists() {
            println!("Loading global timetable from {:?}", timetable_path);
            let timetable: transit_graph::GlobalTimetable =
                transit_graph::load_bincode(timetable_path.to_str().unwrap())?;
            self.global_timetable = Some(timetable);
        }

        // Load Direct Connections
        let dc_path = path.join("direct_connections.bincode");
        if dc_path.exists() {
            println!("Loading direct connections from {:?}", dc_path);
            let dc: transit_graph::DirectConnections =
                transit_graph::load_bincode(dc_path.to_str().unwrap())?;
            self.direct_connections = Some(dc);
        }

        // Load Manifest
        let manifest_path = path.join("manifest.json");
        if manifest_path.exists() {
            println!("Loading manifest from {:?}", manifest_path);
            let file = std::fs::File::open(&manifest_path)?;
            let reader = std::io::BufReader::new(file);
            let manifest: catenary::routing_common::transit_graph::Manifest =
                serde_json::from_reader(reader)?;
            self.manifest = Some(manifest);
        }

        println!("Graph Manager initialized. Partitions will be loaded on demand.");
        Ok(())
    }

    pub fn get_transit_partition(&self, partition_id: u32) -> Option<Arc<TransitPartition>> {
        // 1. Fast path: Check read lock
        {
            let map = self.transit_partitions.read().unwrap();
            if let Some(partition) = map.get(&partition_id) {
                return Some(partition.clone());
            }
        }

        // 2. Slow path: Load from disk and insert
        if let Some(base_path) = &self.base_path {
            println!("Loading transit partition {} from disk", partition_id);
            // Try loading from new path structure: patterns/{pid}/local_v1.bin
            let path = base_path
                .join("patterns")
                .join(partition_id.to_string())
                .join("local_v1.bin");

            if path.exists() {
                if let Ok(partition) =
                    transit_graph::load_bincode::<TransitPartition>(path.to_str().unwrap())
                {
                    let arc_partition = Arc::new(partition);
                    let mut map = self.transit_partitions.write().unwrap();
                    map.insert(partition_id, arc_partition.clone());
                    return Some(arc_partition);
                }
            } else {
                // Fallback to old path for backward compatibility if needed, or just log missing
                // For now, let's keep it strictly new path as per plan to force migration
                println!("Partition file not found at {:?}", path);
            }
        }
        None
    }

    pub fn get_transfer_partition(&self, partition_id: u32) -> Option<TransferChunk> {
        if let Some(partition) = self.transfer_partitions.get(&partition_id) {
            return Some(partition.clone());
        }

        if let Some(base_path) = &self.base_path {
            println!("Loading transfer partition {} from disk", partition_id);
            let filename = format!("transfers_chunk_{}.bincode", partition_id);
            let path = base_path.join(filename);
            if path.exists() {
                if let Ok(partition) =
                    transit_graph::load_bincode::<TransferChunk>(path.to_str().unwrap())
                {
                    return Some(partition);
                }
            }
        }
        None
    }

    pub fn get_edge_partition(&self, partition_id: u32) -> Option<Arc<Vec<EdgeEntry>>> {
        // 1. Fast path: Check read lock
        {
            let map = self.edge_partitions.read().unwrap();
            if let Some(edges) = map.get(&partition_id) {
                return Some(edges.clone());
            }
        }

        // 2. Slow path: Load from disk and insert
        if let Some(base_path) = &self.base_path {
            println!("Loading edge partition {} from disk", partition_id);
            let filename = format!("edges_chunk_{}.json", partition_id);
            let path = base_path.join(filename);
            if path.exists() {
                if let Ok(file) = std::fs::File::open(&path) {
                    let reader = std::io::BufReader::new(file);
                    if let Ok(edges) = serde_json::from_reader::<_, Vec<EdgeEntry>>(reader) {
                        let arc_edges = Arc::new(edges);
                        let mut map = self.edge_partitions.write().unwrap();
                        map.insert(partition_id, arc_edges.clone());
                        return Some(arc_edges);
                    }
                }
            }
        }
        None
    }

    pub fn get_street_partition(&self, partition_id: u32) -> Option<Arc<StreetData>> {
        // 1. Fast path: Check read lock
        {
            let map = self.street_partitions.read().unwrap();
            if let Some(partition) = map.get(&partition_id) {
                return Some(partition.clone());
            }
        }

        // 2. Slow path: Load from disk and insert
        if let Some(base_path) = &self.base_path {
            println!("Loading street partition {} from disk", partition_id);
            let filename = format!("osm_chunk_{}.pbf", partition_id);
            let path = base_path.join(filename);
            if path.exists() {
                if let Ok(partition) = osm_graph::load_pbf::<StreetData>(path.to_str().unwrap()) {
                    let arc_partition = Arc::new(partition);
                    let mut map = self.street_partitions.write().unwrap();
                    map.insert(partition_id, arc_partition.clone());
                    return Some(arc_partition);
                }
            }
        }
        None
    }

    pub fn find_partitions_for_point(&self, lat: f64, lon: f64, radius_deg: f64) -> Vec<u32> {
        let mut partitions = Vec::new();
        if let Some(manifest) = &self.manifest {
            for (pid, boundary) in &manifest.partition_boundaries {
                // Calculate bbox of the boundary polygon
                let mut b_min_lat = f64::MAX;
                let mut b_max_lat = f64::MIN;
                let mut b_min_lon = f64::MAX;
                let mut b_max_lon = f64::MIN;

                for p in &boundary.points {
                    if p.lat < b_min_lat {
                        b_min_lat = p.lat;
                    }
                    if p.lat > b_max_lat {
                        b_max_lat = p.lat;
                    }
                    if p.lon < b_min_lon {
                        b_min_lon = p.lon;
                    }
                    if p.lon > b_max_lon {
                        b_max_lon = p.lon;
                    }
                }

                // Check intersection with point radius
                let p_min_lat = lat - radius_deg;
                let p_max_lat = lat + radius_deg;
                let p_min_lon = lon - radius_deg;
                let p_max_lon = lon + radius_deg;

                if p_max_lat >= b_min_lat
                    && p_min_lat <= b_max_lat
                    && p_max_lon >= b_min_lon
                    && p_min_lon <= b_max_lon
                {
                    partitions.push(*pid);
                }
            }
        }
        partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_partition_loading_api() {
        let manager = GraphManager::new();
        let edges = manager.get_edge_partition(999);
        assert!(edges.is_none());
    }
}
