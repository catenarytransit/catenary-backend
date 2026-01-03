use crate::osm_types::{AtomicEdge, AtomicEdgeId, OsmNodeId, OsmWayId, RailMode, ZClass};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use geo::HaversineDistance;
use log::{debug, info, warn};
use osmpbfreader::{OsmObj, OsmPbfReader, Tags};
use rstar::{RTree, primitives::GeomWithData, primitives::Rectangle};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Parsed OSM node with coordinates
#[derive(Debug, Clone, Copy)]
pub struct OsmNode {
    pub id: OsmNodeId,
    pub lon: f64,
    pub lat: f64,
}

/// Parsed OSM way representing a rail segment
#[derive(Debug, Clone)]
pub struct OsmRailWay {
    pub id: OsmWayId,
    pub nodes: Vec<OsmNodeId>,
    pub mode: RailMode,
    pub z_class: ZClass,
    pub name: Option<String>,
}

/// The complete OSM rail index built from a PBF file
pub struct OsmRailIndex {
    /// All nodes referenced by rail ways
    pub nodes: HashMap<OsmNodeId, OsmNode>,
    /// All rail ways
    pub ways: HashMap<OsmWayId, OsmRailWay>,
    /// Atomic edges derived from ways
    pub edges: Vec<AtomicEdge>,
    /// Edge index by ID
    pub edge_index: HashMap<AtomicEdgeId, usize>,
    /// Adjacency: node -> edges touching it
    pub node_adjacency: HashMap<OsmNodeId, Vec<AtomicEdgeId>>,
    /// Spatial index for edges (bounding boxes)
    pub edge_rtree: RTree<GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, AtomicEdgeId>>,
    /// Spatial index for nodes
    pub node_rtree: RTree<GeomWithData<[f64; 2], OsmNodeId>>,
}

impl OsmRailIndex {
    /// Load rail infrastructure from a filtered PBF file using two-pass approach.
    pub fn load_from_pbf(path: &Path) -> anyhow::Result<Self> {
        info!("Loading OSM rail data from {:?}", path);

        // Pass 1: Collect way IDs and their referenced nodes
        let (way_ids, node_ids) = Self::pass1_collect_ids(path)?;
        info!(
            "Pass 1 complete: {} ways, {} referenced nodes",
            way_ids.len(),
            node_ids.len()
        );

        // Pass 2: Load node coordinates and way details
        let (nodes, ways) = Self::pass2_load_data(path, &way_ids, &node_ids)?;
        info!(
            "Pass 2 complete: {} nodes loaded, {} ways loaded",
            nodes.len(),
            ways.len()
        );

        // Build atomic edges from ways
        let (edges, node_adjacency) = Self::build_atomic_edges(&nodes, &ways);
        info!("Built {} atomic edges", edges.len());

        // Build edge index
        let edge_index: HashMap<AtomicEdgeId, usize> =
            edges.iter().enumerate().map(|(i, e)| (e.id, i)).collect();

        // Build spatial indices
        let edge_rtree = Self::build_edge_rtree(&edges);
        let node_rtree = Self::build_node_rtree(&nodes);

        Ok(Self {
            nodes,
            ways,
            edges,
            edge_index,
            node_adjacency,
            edge_rtree,
            node_rtree,
        })
    }

    /// Pass 1: Scan PBF for rail ways and collect their IDs and referenced node IDs
    fn pass1_collect_ids(path: &Path) -> anyhow::Result<(HashSet<OsmWayId>, HashSet<OsmNodeId>)> {
        let file = File::open(path)?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));

        let mut way_ids = HashSet::new();
        let mut node_ids = HashSet::new();

        for obj in reader.iter() {
            let obj = obj?;
            if let OsmObj::Way(way) = obj {
                if Self::is_rail_way(&way.tags) {
                    way_ids.insert(OsmWayId(way.id.0));
                    for node_ref in &way.nodes {
                        node_ids.insert(OsmNodeId(node_ref.0));
                    }
                }
            }
        }

        Ok((way_ids, node_ids))
    }

    /// Pass 2: Load node coordinates and way details
    fn pass2_load_data(
        path: &Path,
        way_ids: &HashSet<OsmWayId>,
        node_ids: &HashSet<OsmNodeId>,
    ) -> anyhow::Result<(HashMap<OsmNodeId, OsmNode>, HashMap<OsmWayId, OsmRailWay>)> {
        let file = File::open(path)?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));

        let mut nodes = HashMap::with_capacity(node_ids.len());
        let mut ways = HashMap::with_capacity(way_ids.len());

        for obj in reader.iter() {
            let obj = obj?;
            match obj {
                OsmObj::Node(node) => {
                    let id = OsmNodeId(node.id.0);
                    if node_ids.contains(&id) {
                        nodes.insert(
                            id,
                            OsmNode {
                                id,
                                lon: node.lon(),
                                lat: node.lat(),
                            },
                        );
                    }
                }
                OsmObj::Way(way) => {
                    let id = OsmWayId(way.id.0);
                    if way_ids.contains(&id) {
                        if let Some(rail_way) = Self::parse_rail_way(&way) {
                            ways.insert(id, rail_way);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok((nodes, ways))
    }

    /// Check if a way has a rail-related railway tag
    fn is_rail_way(tags: &Tags) -> bool {
        if let Some(railway) = tags.get("railway") {
            RailMode::from_osm_tag(railway)
                .map(|m| m.is_active_rail())
                .unwrap_or(false)
        } else {
            false
        }
    }

    /// Parse a way into an OsmRailWay
    fn parse_rail_way(way: &osmpbfreader::Way) -> Option<OsmRailWay> {
        let railway = way.tags.get("railway")?;
        let mode = RailMode::from_osm_tag(railway)?;
        if !mode.is_active_rail() {
            return None;
        }

        let layer = way.tags.get("layer").and_then(|v| v.parse::<i8>().ok());
        let bridge = way.tags.get("bridge").map(|s| s.as_str());
        let tunnel = way.tags.get("tunnel").map(|s| s.as_str());
        let z_class = ZClass::from_tags(layer, bridge, tunnel);

        let name = way.tags.get("name").map(|s| s.to_string());

        Some(OsmRailWay {
            id: OsmWayId(way.id.0),
            nodes: way.nodes.iter().map(|n| OsmNodeId(n.0)).collect(),
            mode,
            z_class,
            name,
        })
    }

    /// Build atomic edges from ways, splitting at junctions
    fn build_atomic_edges(
        nodes: &HashMap<OsmNodeId, OsmNode>,
        ways: &HashMap<OsmWayId, OsmRailWay>,
    ) -> (Vec<AtomicEdge>, HashMap<OsmNodeId, Vec<AtomicEdgeId>>) {
        // Count how many ways reference each node (for junction detection)
        let mut node_degree: HashMap<OsmNodeId, u32> = HashMap::new();
        for way in ways.values() {
            for (i, &node_id) in way.nodes.iter().enumerate() {
                let entry = node_degree.entry(node_id).or_insert(0);
                // Endpoints always count, intermediate nodes add 1
                if i == 0 || i == way.nodes.len() - 1 {
                    *entry += 1;
                } else {
                    *entry += 2; // Passing through counts as 2 for degree
                }
            }
        }

        let mut edges = Vec::new();
        let mut node_adjacency: HashMap<OsmNodeId, Vec<AtomicEdgeId>> = HashMap::new();

        for way in ways.values() {
            let mut segment_start = 0;
            let mut segment_idx = 0u32;

            for i in 1..way.nodes.len() {
                let node_id = way.nodes[i];
                let is_endpoint = i == way.nodes.len() - 1;
                let is_junction = node_degree.get(&node_id).copied().unwrap_or(0) > 2;

                if is_endpoint || is_junction {
                    // Create atomic edge from segment_start to i
                    let edge_nodes: Vec<_> = way.nodes[segment_start..=i].to_vec();
                    if edge_nodes.len() >= 2 {
                        let geometry: Vec<(f64, f64)> = edge_nodes
                            .iter()
                            .filter_map(|n| nodes.get(n).map(|node| (node.lon, node.lat)))
                            .collect();

                        if geometry.len() >= 2 {
                            let length = crate::geometry_utils::polyline_length(&geometry);
                            let edge_id = AtomicEdgeId::from_way_segment(way.id, segment_idx);

                            let edge = AtomicEdge {
                                id: edge_id,
                                from: edge_nodes[0],
                                to: *edge_nodes.last().unwrap(),
                                mode: way.mode,
                                z_class: way.z_class,
                                geometry,
                                source_way: way.id,
                                length_m: length,
                            };

                            node_adjacency.entry(edge.from).or_default().push(edge_id);
                            node_adjacency.entry(edge.to).or_default().push(edge_id);

                            edges.push(edge);
                            segment_idx += 1;
                        }
                    }

                    segment_start = i;
                }
            }
        }

        (edges, node_adjacency)
    }

    /// Build R-tree for edges using bounding boxes
    fn build_edge_rtree(
        edges: &[AtomicEdge],
    ) -> RTree<GeomWithData<Rectangle<[f64; 2]>, AtomicEdgeId>> {
        let items: Vec<_> = edges
            .iter()
            .filter_map(|edge| {
                if edge.geometry.is_empty() {
                    return None;
                }
                let mut min = [f64::INFINITY, f64::INFINITY];
                let mut max = [f64::NEG_INFINITY, f64::NEG_INFINITY];
                for &(lon, lat) in &edge.geometry {
                    min[0] = min[0].min(lon);
                    min[1] = min[1].min(lat);
                    max[0] = max[0].max(lon);
                    max[1] = max[1].max(lat);
                }
                Some(GeomWithData::new(
                    Rectangle::from_corners(min, max),
                    edge.id,
                ))
            })
            .collect();

        RTree::bulk_load(items)
    }

    /// Build R-tree for nodes
    fn build_node_rtree(
        nodes: &HashMap<OsmNodeId, OsmNode>,
    ) -> RTree<GeomWithData<[f64; 2], OsmNodeId>> {
        let items: Vec<_> = nodes
            .values()
            .map(|node| GeomWithData::new([node.lon, node.lat], node.id))
            .collect();

        RTree::bulk_load(items)
    }

    /// Find edges near a point within a radius (in meters)
    pub fn edges_near_point(&self, lon: f64, lat: f64, radius_m: f64) -> Vec<&AtomicEdge> {
        // Convert radius to approximate degrees
        let radius_deg = radius_m / 111320.0;
        let envelope = rstar::AABB::from_corners(
            [lon - radius_deg, lat - radius_deg],
            [lon + radius_deg, lat + radius_deg],
        );

        self.edge_rtree
            .locate_in_envelope(&envelope)
            .filter_map(|item| {
                let edge_idx = self.edge_index.get(&item.data)?;
                Some(&self.edges[*edge_idx])
            })
            .collect()
    }

    /// Find the nearest edge to a point, optionally filtering by mode
    pub fn nearest_edge(
        &self,
        lon: f64,
        lat: f64,
        mode_filter: Option<RailMode>,
    ) -> Option<(&AtomicEdge, f64)> {
        let candidates = self.edges_near_point(lon, lat, 100.0);
        let query_point = geo::Point::new(lon, lat);

        candidates
            .into_iter()
            .filter(|e| mode_filter.map_or(true, |m| e.mode == m))
            .filter_map(|edge| {
                let (_, dist, _) =
                    crate::geometry_utils::project_point_to_polyline((lon, lat), &edge.geometry)?;
                Some((edge, dist))
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
    }

    /// Get edges adjacent to a node
    pub fn edges_at_node(&self, node: OsmNodeId) -> Vec<&AtomicEdge> {
        self.node_adjacency
            .get(&node)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| {
                        let idx = self.edge_index.get(id)?;
                        Some(&self.edges[*idx])
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get node coordinates
    pub fn node_position(&self, node: OsmNodeId) -> Option<(f64, f64)> {
        self.nodes.get(&node).map(|n| (n.lon, n.lat))
    }
}
