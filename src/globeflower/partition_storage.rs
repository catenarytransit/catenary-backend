use crate::edges::GraphEdge;
use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub struct PartitionStore {
    base_dir: PathBuf,
    stored_files: Vec<PathBuf>,
}

impl PartitionStore {
    pub fn new() -> Result<Self> {
        let temp_dir = std::env::temp_dir().join("globeflower_partitions");
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir)?;
        }
        // Unique subdir for this run to avoid collisions
        let run_dir = temp_dir.join(Uuid::new_v4().to_string());
        fs::create_dir_all(&run_dir)?;

        println!("Partition storage initialized at {:?}", run_dir);

        Ok(Self {
            base_dir: run_dir,
            stored_files: Vec::new(),
        })
    }

    pub fn store_partition(&mut self, partition: &[GraphEdge]) -> Result<PartitionHandle> {
        let id = Uuid::new_v4();
        let path = self.base_dir.join(format!("{}.bin", id));
        let file = File::create(&path).context("Failed to create partition file")?;
        let mut writer = BufWriter::new(file);

        // Use bincode 2.0 legacy configuration (same as lib.rs)
        let config = bincode::config::legacy();
        bincode::serde::encode_into_std_write(partition, &mut writer, config)
            .context("Failed to serialize partition")?;

        self.stored_files.push(path.clone());
        Ok(PartitionHandle {
            path,
            edge_count: partition.len(),
        })
    }

    pub fn load_partition(&self, handle: &PartitionHandle) -> Result<Vec<GraphEdge>> {
        let file = File::open(&handle.path)
            .with_context(|| format!("Failed to open partition file {:?}", handle.path))?;
        let mut reader = BufReader::new(file);
        let config = bincode::config::legacy();
        let partition: Vec<GraphEdge> = bincode::serde::decode_from_std_read(&mut reader, config)
            .context("Failed to deserialize partition")?;
        Ok(partition)
    }
}

impl Drop for PartitionStore {
    fn drop(&mut self) {
        println!("Cleaning up partition storage at {:?}", self.base_dir);
        let _ = fs::remove_dir_all(&self.base_dir);
    }
}

#[derive(Clone, Debug)]
pub struct PartitionHandle {
    path: PathBuf,
    pub edge_count: usize,
}
