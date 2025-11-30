use anyhow::Result;
use catenary::routing_common::osm_graph::{self, StreetData};
use catenary::routing_common::transit_graph::{
    self, EdgeEntry, GlobalPatternIndex, TransferChunk, TransitPartition,
};
use std::collections::HashMap;
use std::path::Path;

pub struct GraphManager {
    pub transit_partitions: HashMap<u32, TransitPartition>,
    pub street_partitions: HashMap<u32, StreetData>,
    pub transfer_partitions: HashMap<u32, TransferChunk>,
    pub edge_partitions: HashMap<u32, Vec<EdgeEntry>>,
    pub global_index: Option<GlobalPatternIndex>,
}

impl GraphManager {
    pub fn new() -> Self {
        Self {
            transit_partitions: HashMap::new(),
            street_partitions: HashMap::new(),
            transfer_partitions: HashMap::new(),
            edge_partitions: HashMap::new(),
            global_index: None,
        }
    }

    pub fn load_from_directory(&mut self, dir_path: &str) -> Result<()> {
        let path = Path::new(dir_path);

        // Load Global Patterns
        let global_path = path.join("global_patterns.pbf");
        if global_path.exists() {
            println!("Loading global patterns from {:?}", global_path);
            let index: GlobalPatternIndex = transit_graph::load_pbf(global_path.to_str().unwrap())?;
            self.global_index = Some(index);
        }

        // Iterate over files in directory
        let entries = std::fs::read_dir(path)?;
        println!("Scanning directory: {:?}", path);
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_str().unwrap();

            if file_name.starts_with("transit_chunk_") && file_name.ends_with(".pbf") {
                println!("Loading transit partition from {:?}", path);
                let partition: TransitPartition = transit_graph::load_pbf(path.to_str().unwrap())?;
                self.transit_partitions
                    .insert(partition.partition_id, partition);
            } else if file_name.starts_with("streets_chunk_") && file_name.ends_with(".pbf") {
                println!("Loading street partition from {:?}", path);
                let partition: StreetData = osm_graph::load_pbf(path.to_str().unwrap())?;
                self.street_partitions
                    .insert(partition.partition_id, partition);
            } else if file_name.starts_with("transfers_chunk_") && file_name.ends_with(".pbf") {
                println!("Loading transfer partition from {:?}", path);
                let partition: TransferChunk = transit_graph::load_pbf(path.to_str().unwrap())?;
                self.transfer_partitions
                    .insert(partition.partition_id, partition);
            } else if file_name.starts_with("edges_chunk_") && file_name.ends_with(".json") {
                println!("Loading edge partition from {:?}", path);
                let file = std::fs::File::open(&path)?;
                let reader = std::io::BufReader::new(file);
                let edges: Vec<EdgeEntry> = serde_json::from_reader(reader)?;

                // Extract partition ID from filename: edges_chunk_123.json
                if let Some(pid_str) = file_name
                    .strip_prefix("edges_chunk_")
                    .and_then(|s| s.strip_suffix(".json"))
                {
                    if let Ok(pid) = pid_str.parse::<u32>() {
                        self.edge_partitions.insert(pid, edges);
                    }
                }
            }
        }

        Ok(())
    }
}
