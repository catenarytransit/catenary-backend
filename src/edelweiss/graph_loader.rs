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
    pub edge_partitions: HashMap<u32, Vec<EdgeEntry>>,
    pub global_index: Option<GlobalPatternIndex>,
    pub global_timetable: Option<catenary::routing_common::transit_graph::GlobalTimetable>,
    pub manifest: Option<catenary::routing_common::transit_graph::Manifest>,
    pub base_path: Option<std::path::PathBuf>,
}

impl GraphManager {
    pub fn new() -> Self {
        Self {
            transit_partitions: RwLock::new(HashMap::new()),
            street_partitions: RwLock::new(HashMap::new()),
            transfer_partitions: HashMap::new(),
            edge_partitions: HashMap::new(),
            global_index: None,
            global_timetable: None,
            manifest: None,
            base_path: None,
        }
    }

    pub fn load_from_directory(&mut self, dir_path: &str) -> Result<()> {
        let path = Path::new(dir_path);
        self.base_path = Some(path.to_path_buf());

        // Load Global Patterns
        let global_path = path.join("global_patterns.pbf");
        if global_path.exists() {
            println!("Loading global patterns from {:?}", global_path);
            let index: GlobalPatternIndex = transit_graph::load_pbf(global_path.to_str().unwrap())?;
            self.global_index = Some(index);
        }

        // Load Global Timetable
        let timetable_path = path.join("global_timetable.pbf");
        if timetable_path.exists() {
            println!("Loading global timetable from {:?}", timetable_path);
            let timetable: transit_graph::GlobalTimetable =
                transit_graph::load_pbf(timetable_path.to_str().unwrap())?;
            self.global_timetable = Some(timetable);
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
            let filename = format!("transit_chunk_{}.pbf", partition_id);
            let path = base_path.join(filename);
            if path.exists() {
                // println!("Loading transit partition {} from disk", partition_id);
                if let Ok(partition) =
                    transit_graph::load_pbf::<TransitPartition>(path.to_str().unwrap())
                {
                    let arc_partition = Arc::new(partition);
                    let mut map = self.transit_partitions.write().unwrap();
                    map.insert(partition_id, arc_partition.clone());
                    return Some(arc_partition);
                }
            }
        }
        None
    }

    pub fn get_transfer_partition(&self, partition_id: u32) -> Option<TransferChunk> {
        if let Some(partition) = self.transfer_partitions.get(&partition_id) {
            return Some(partition.clone());
        }

        if let Some(base_path) = &self.base_path {
            let filename = format!("transfers_chunk_{}.pbf", partition_id);
            let path = base_path.join(filename);
            if path.exists() {
                if let Ok(partition) =
                    transit_graph::load_pbf::<TransferChunk>(path.to_str().unwrap())
                {
                    return Some(partition);
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
