use crate::routing_common::lookup::Lookup;
use crate::routing_common::ways::RoutingGraph;
use dashmap::DashMap;
use rstar::{AABB, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionManifest {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

#[derive(Clone)]
pub struct RegionBoundingBox {
    pub id: String,
    pub aabb: AABB<[f64; 2]>,
}

impl RTreeObject for RegionBoundingBox {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}

impl rstar::PointDistance for RegionBoundingBox {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        self.aabb.distance_2(point)
    }
}

pub struct LoadedGraph {
    pub routing: Arc<RoutingGraph>,
    pub lookup: Arc<Lookup>,
}

pub struct GraphManager {
    base_dir: PathBuf,
    pub rtree: RTree<RegionBoundingBox>,
    loaded_regions: DashMap<String, Arc<LoadedGraph>>,
}

impl GraphManager {
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        let base_dir = base_dir.as_ref().to_path_buf();
        let mut rtree = RTree::new();

        if base_dir.exists() {
            if let Ok(entries) = fs::read_dir(&base_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let manifest_path = path.join("manifest.json");
                        if manifest_path.exists() {
                            if let Ok(content) = fs::read_to_string(&manifest_path) {
                                if let Ok(manifest) =
                                    serde_json::from_str::<RegionManifest>(&content)
                                {
                                    let id =
                                        path.file_name().unwrap().to_string_lossy().to_string();
                                    let aabb = AABB::from_corners(
                                        [manifest.min_lon, manifest.min_lat],
                                        [manifest.max_lon, manifest.max_lat],
                                    );
                                    rtree.insert(RegionBoundingBox { id, aabb });
                                }
                            }
                        }
                    }
                }
            }
        } else {
            println!(
                "Warning: Graph directory {} does not exist",
                base_dir.display()
            );
        }

        Self {
            base_dir,
            rtree,
            loaded_regions: DashMap::new(),
        }
    }

    pub fn get_region_for_point(&self, lat: f64, lon: f64) -> Option<String> {
        self.rtree
            .locate_all_at_point(&[lon, lat])
            .next()
            .map(|region| region.id.clone())
    }

    pub fn get_or_load_region(&self, region_id: &str) -> anyhow::Result<Arc<LoadedGraph>> {
        if let Some(loaded) = self.loaded_regions.get(region_id) {
            return Ok(loaded.clone());
        }

        println!("Dynamically loading region: {}", region_id);
        let region_dir = self.base_dir.join(region_id);
        let routing_path = region_dir.join("routing.bin");
        let lookup_path = region_dir.join("lookup.bin");

        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&std::fs::File::open(&routing_path)?)? });
        let routing = RoutingGraph::load(mmap);
        let lookup_bytes = fs::read(&lookup_path)?;

        let (lookup, _): (Lookup, usize) =
            bincode::serde::decode_from_slice(&lookup_bytes, bincode::config::standard())?;

        let loaded = Arc::new(LoadedGraph {
            routing: Arc::new(routing),
            lookup: Arc::new(lookup),
        });

        self.loaded_regions
            .insert(region_id.to_string(), loaded.clone());
        Ok(loaded)
    }
}
