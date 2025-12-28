use ahash::AHashMap as HashMap;
use catenary::routing_common::osm_graph::{StreetData, load_pbf};
use catenary::routing_common::transit_graph::OsmLink;
use rstar::RTree;
use rstar::primitives::GeomWithData;
use std::collections::VecDeque;
use std::path::PathBuf;

use super::utils::{haversine_distance, lon_lat_to_tile};

pub struct ChunkCache {
    capacity: usize,
    map: HashMap<(u32, u32), (StreetData, RTree<GeomWithData<[f64; 2], u32>>)>,
    queue: VecDeque<(u32, u32)>,
}

impl ChunkCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            queue: VecDeque::new(),
        }
    }

    pub fn get_or_load(
        &mut self,
        x: u32,
        y: u32,
        osm_dir: &PathBuf,
    ) -> Option<&(StreetData, RTree<GeomWithData<[f64; 2], u32>>)> {
        if self.map.contains_key(&(x, y)) {
            // Promote to back (recently used)
            if let Some(pos) = self.queue.iter().position(|&k| k == (x, y)) {
                self.queue.remove(pos);
                self.queue.push_back((x, y));
            }
            return self.map.get(&(x, y));
        }

        // Load
        let chunk_filename = format!("chunk_{}_{}_{}.pbf", x, y, 10);
        let chunk_path = osm_dir.join(chunk_filename);

        if !chunk_path.exists() {
            return None;
        }

        let street_data: StreetData = load_pbf(chunk_path.to_str().unwrap()).ok()?;

        // Build RTree
        let points: Vec<GeomWithData<[f64; 2], u32>> = street_data
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| GeomWithData::new([node.lon, node.lat], i as u32))
            .collect();

        let rtree = RTree::bulk_load(points);

        // Insert
        if self.map.len() >= self.capacity {
            if let Some(old_key) = self.queue.pop_front() {
                self.map.remove(&old_key);
            }
        }

        self.map.insert((x, y), (street_data, rtree));
        self.queue.push_back((x, y));

        self.map.get(&(x, y))
    }
}

pub async fn find_osm_link(
    lon: f64,
    lat: f64,
    stop_idx: u32,
    osm_dir: &PathBuf,
    cache: &mut ChunkCache,
) -> Option<OsmLink> {
    let (x, y) = lon_lat_to_tile(lon, lat, 12);

    let (street_data, rtree) = cache.get_or_load(x, y, osm_dir)?;

    // Use RTree to find nearest neighbour
    let nearest = rtree.nearest_neighbor(&[lon, lat])?;
    let node_idx = nearest.data;
    let node = &street_data.nodes[node_idx as usize];

    let dist = haversine_distance(lat, lon, node.lat, node.lon);

    if dist > 200.0 {
        return None;
    }

    let walk_seconds = (dist / 1.4) as u32;
    Some(OsmLink {
        stop_idx,
        osm_node_id: node_idx,
        walk_seconds,
        distance_meters: dist as u32,
        wheelchair_accessible: true, // Default to true for now
    })
}
