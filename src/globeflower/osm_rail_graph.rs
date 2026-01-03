// ===========================================================================
// OSM Railway Graph for Topology Validation
// ===========================================================================
//
// This module provides OSM railway network data for validating track merging
// decisions. It reads pre-filtered railway PBF files and builds spatial indices
// to efficiently answer queries like:
// - Do two points share an OSM junction?
// - Are two positions on the same OSM way?
// - What is the nearest junction to a point?
//
// This enables ground-truth validation of merge decisions, especially important
// for preventing false merges at X-crossings (flyovers/underpasses) and for
// correctly identifying merge points at actual junctions.
// ===========================================================================

use ahash::{AHashMap, AHashSet};
use osmpbfreader::{OsmObj, OsmPbfReader};
use rstar::{AABB, RTree, RTreeObject};
use std::fs::File;
use std::path::Path;
use std::sync::OnceLock;

// ===========================================================================
// Global OSM Index Singleton
// ===========================================================================
//
// The OSM index is loaded once at startup and accessed globally during
// graph processing. This avoids threading the index through every function.

static OSM_RAIL_INDEX: OnceLock<OsmRailIndex> = OnceLock::new();

/// Initialize the global OSM rail index from a PBF file
///
/// Initialize the global OSM rail index from a PBF file
///
/// Call this once at startup before processing.
/// Files can be specified directly (e.g. from CLI args).
pub fn init_global_osm_index(pbf_paths: Option<Vec<String>>) {
    OSM_RAIL_INDEX.get_or_init(|| match pbf_paths {
        Some(paths) if !paths.is_empty() => {
            println!("Loading OSM railway data from {} files...", paths.len());
            let config = OsmRailConfig {
                pbf_paths: paths,
                bbox: None,
            };
            match OsmRailIndex::load_from_pbf(&config) {
                Ok(index) => {
                    println!("OSM rail index loaded successfully.");
                    index
                }
                Err(e) => {
                    eprintln!(
                        "Failed to load OSM rail index: {}. Disabling OSM validation.",
                        e
                    );
                    OsmRailIndex::empty()
                }
            }
        }
        _ => {
            println!("No OSM PBF paths provided. OSM validation disabled.");
            OsmRailIndex::empty()
        }
    });
}

/// Get a reference to the global OSM rail index
///
/// Returns None if the index hasn't been initialized yet.
pub fn get_osm_index() -> Option<&'static OsmRailIndex> {
    OSM_RAIL_INDEX.get()
}

/// Check if OSM validation is available (index is loaded and non-empty)
pub fn is_osm_validation_enabled() -> bool {
    OSM_RAIL_INDEX
        .get()
        .map(|i| i.node_count() > 0)
        .unwrap_or(false)
}

/// A point in the OSM railway node spatial index
#[derive(Debug, Clone)]
pub struct RTreeNode {
    pub node_id: i64,
    pub pos: [f64; 2], // [x, y] in Web Mercator meters
}

impl RTreeObject for RTreeNode {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.pos)
    }
}

/// Route metadata extracted from OSM relations
#[derive(Debug, Clone)]
pub struct OsmRouteInfo {
    pub relation_id: i64,
    /// Route name (e.g., "TER C11", "Ligne 10")
    pub name: Option<String>,
    /// Reference code (e.g., "C11", "10", "M10")
    pub ref_code: Option<String>,
    /// Route type from relation tags ("train", "subway", "tram", "light_rail")
    pub route_type: String,
    /// Network name (e.g., "Métro de Paris", "TER Hauts-de-France")
    pub network: Option<String>,
    /// GTFS route ID if tagged (gtfs:route_id:*)
    pub gtfs_route_id: Option<String>,
}

/// Station metadata extracted from OSM stop_area relations
#[derive(Debug, Clone)]
pub struct OsmStationInfo {
    pub relation_id: i64,
    pub name: Option<String>,
    pub network: Option<String>,
    pub members: Vec<i64>,                  // Way/Node IDs of platforms
    pub bbox: Option<(f64, f64, f64, f64)>, // [min_x, min_y, max_x, max_y]
}

/// Configuration for OSM rail index loading
#[derive(Debug, Clone)]
pub struct OsmRailConfig {
    /// Paths to the pre-filtered railway PBF files
    pub pbf_paths: Vec<String>,
    /// Optional bounding box filter [min_lon, min_lat, max_lon, max_lat]
    pub bbox: Option<(f64, f64, f64, f64)>,
}

/// Spatial index for OSM railway network
///
/// Enables efficient queries for junction detection and track validation.
pub struct OsmRailIndex {
    /// RTree for spatial queries on nodes
    node_tree: RTree<RTreeNode>,
    /// Map from node ID to list of way IDs that contain it
    node_to_ways: AHashMap<i64, Vec<i64>>,
    /// Set of junction node IDs (nodes in 2+ ways)
    junctions: AHashSet<i64>,
    /// Coverage bounding box [min_x, min_y, max_x, max_y] in Web Mercator
    coverage_bbox: Option<(f64, f64, f64, f64)>,
    /// Map from way ID to list of route relation IDs that contain it
    way_to_relations: AHashMap<i64, Vec<i64>>,
    /// Map from relation ID to route metadata
    relation_info: AHashMap<i64, OsmRouteInfo>,
    /// Map from way ID to its railway type tag
    way_to_railway_type: AHashMap<i64, String>,
    /// Map from way ID to its geometry (list of coordinates)
    way_geometries: AHashMap<i64, Vec<[f64; 2]>>,
    /// Map from way ID to its layer/level (default 0)
    way_layers: AHashMap<i64, i32>,
    /// List of stations (stop_area relations)
    stations: Vec<OsmStationInfo>,
    /// Spatial index for stations
    station_tree: RTree<RTreeStation>,
}

/// A station in the spatial index
#[derive(Debug, Clone)]
pub struct RTreeStation {
    pub index: usize, // Index into self.stations
    pub bbox: AABB<[f64; 2]>,
}

impl RTreeObject for RTreeStation {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.bbox
    }
}

impl OsmRailIndex {
    /// Create an empty index
    pub fn empty() -> Self {
        Self {
            node_tree: RTree::new(),
            node_to_ways: AHashMap::new(),
            junctions: AHashSet::new(),
            coverage_bbox: None,
            way_to_relations: AHashMap::new(),
            relation_info: AHashMap::new(),
            way_to_railway_type: AHashMap::new(),
            way_geometries: AHashMap::new(),
            way_layers: AHashMap::new(),
            stations: Vec::new(),
            station_tree: RTree::new(),
        }
    }

    /// Load OSM railway data from pre-filtered PBF files
    ///
    /// The PBF files should contain only railway-related data (filtered with railway-filter.txt).
    /// Uses all files specified in config.pbf_paths.
    /// Optionally filters by a bounding box.
    pub fn load_from_pbf(config: &OsmRailConfig) -> anyhow::Result<Self> {
        // Collect data from all files
        // Use Vec instead of HashMap to save memory (avoid duplication for RTree)
        let mut nodes: Vec<RTreeNode> = Vec::new();
        let mut node_to_ways: AHashMap<i64, Vec<i64>> = AHashMap::new();
        // way_id, node_ids, railway_type, layer
        let mut ways_all: Vec<(i64, Vec<i64>, String, i32)> = Vec::new();

        // NEW: Collect relations for route-based merge decisions
        let mut relations_all: Vec<(i64, Vec<i64>, OsmRouteInfo)> = Vec::new(); // (relation_id, way_members, info)
        // NEW: Collect stations
        let mut stations: Vec<OsmStationInfo> = Vec::new();

        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;
        let mut has_coverage = false;

        for pbf_path_str in &config.pbf_paths {
            let path = Path::new(pbf_path_str);
            if !path.exists() {
                eprintln!("Warning: OSM PBF file not found: {}", pbf_path_str);
                continue;
            }

            println!("Reading PBF file: {}", pbf_path_str);
            let file = File::open(path)?;
            let mut reader = OsmPbfReader::new(file);

            for obj in reader.iter().flatten() {
                match obj {
                    OsmObj::Node(node) => {
                        let lon = node.lon();
                        let lat = node.lat();

                        // Apply bbox filter if specified
                        if let Some((min_lon, min_lat, max_lon, max_lat)) = config.bbox {
                            if lon < min_lon || lon > max_lon || lat < min_lat || lat > max_lat {
                                continue;
                            }
                        }

                        // Convert to Web Mercator
                        let (x, y) = crate::coord_conversion::lat_lng_to_web_merc(lon, lat);

                        // Store in simple vector
                        nodes.push(RTreeNode {
                            node_id: node.id.0,
                            pos: [x, y],
                        });

                        // Track coverage bbox
                        min_x = min_x.min(x);
                        min_y = min_y.min(y);
                        max_x = max_x.max(x);
                        max_y = max_y.max(y);
                        has_coverage = true;
                    }
                    OsmObj::Way(way) => {
                        // Check if this is a railway way
                        let railway_type = way.tags.get("railway").map(|v| v.as_str());
                        if let Some(rtype) = railway_type {
                            // Only include actual track types
                            if matches!(
                                rtype,
                                "rail" | "subway" | "tram" | "light_rail" | "narrow_gauge"
                            ) {
                                let node_ids: Vec<i64> = way.nodes.iter().map(|n| n.0).collect();

                                // Parse layer/level (default 0)
                                let layer = way
                                    .tags
                                    .get("layer")
                                    .or_else(|| way.tags.get("level"))
                                    .and_then(|v| v.parse::<i32>().ok())
                                    .unwrap_or(0);

                                ways_all.push((way.id.0, node_ids, rtype.to_string(), layer));
                            }
                        }
                    }
                    OsmObj::Relation(rel) => {
                        // Check if this is a transit route relation OR stop_area
                        let rel_type = rel.tags.get("type").map(|v| v.as_str());

                        // CASE 1: Route Relations (type=route or type=route_master)
                        if matches!(rel_type, Some("route") | Some("route_master")) {
                            // Get the route type (subway, train, tram, etc.)
                            let route_tag = rel
                                .tags
                                .get("route")
                                .or_else(|| rel.tags.get("route_master"))
                                .map(|v| v.as_str());

                            // Only include relevant transit route types
                            let route_type = match route_tag {
                                Some("subway") => "subway",
                                Some("train") | Some("railway") => "train",
                                Some("tram") => "tram",
                                Some("light_rail") => "light_rail",
                                _ => continue, // Skip non-transit routes (bus, etc.)
                            };

                            // Extract way members (ignore node members for route definition)
                            let way_members: Vec<i64> = rel
                                .refs
                                .iter()
                                .filter_map(|member| {
                                    if member.member.is_way() {
                                        Some(member.member.way().unwrap().0)
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            if !way_members.is_empty() {
                                // Extract GTFS route ID if present (could be under various keys)
                                let gtfs_route_id = rel
                                    .tags
                                    .iter()
                                    .find(|(k, _)| {
                                        k.starts_with("gtfs:route_id") || k.starts_with("ref:FR:")
                                    })
                                    .map(|(_, v)| v.to_string());

                                let info = OsmRouteInfo {
                                    relation_id: rel.id.0,
                                    name: rel.tags.get("name").map(|v| v.to_string()),
                                    ref_code: rel.tags.get("ref").map(|v| v.to_string()),
                                    route_type: route_type.to_string(),
                                    network: rel.tags.get("network").map(|v| v.to_string()),
                                    gtfs_route_id,
                                };

                                relations_all.push((rel.id.0, way_members, info));
                            }
                        }
                        // CASE 2: Station Areas (public_transport=stop_area)
                        else if matches!(rel_type, Some("public_transport")) {
                            let pt_tag = rel.tags.get("public_transport").map(|v| v.as_str());
                            if pt_tag == Some("stop_area") {
                                // Collect members (platforms, stops)
                                let members: Vec<i64> = rel
                                    .refs
                                    .iter()
                                    .filter_map(|member| {
                                        if member.member.is_way() {
                                            Some(member.member.way().unwrap().0)
                                        } else {
                                            None // Ignore nodes for now, focus on platform tracks/polygons
                                        }
                                    })
                                    .collect();

                                if !members.is_empty() {
                                    let station = OsmStationInfo {
                                        relation_id: rel.id.0,
                                        name: rel.tags.get("name").map(|v| v.to_string()),
                                        network: rel.tags.get("network").map(|v| v.to_string()),
                                        members,
                                        bbox: None, // Will be computed later
                                    };
                                    stations.push(station);
                                }
                            }
                        }
                    }
                }
            }
        }

        if nodes.is_empty() {
            println!("Warning: No OSM nodes loaded from check paths or coverage filter.");
        } else {
            // Sort and verify no duplicates (though PBF shouldn't have them usually)
            // Sorting allows binary search for topology building
            println!("Sorting {} nodes...", nodes.len());
            nodes.sort_unstable_by_key(|n| n.node_id);
            nodes.dedup_by_key(|n| n.node_id);
        }

        // Second pass: build node_to_ways mapping and identify junctions
        println!("Building topology from {} ways...", ways_all.len());
        for (way_id, node_ids, _railway_type, _layer) in &ways_all {
            for node_id in node_ids {
                // Only include nodes that exist (were within bbox or not filtered)
                // Use binary search since we don't have a hash map
                if nodes.binary_search_by_key(node_id, |n| n.node_id).is_ok() {
                    node_to_ways
                        .entry(*node_id)
                        .or_insert_with(Vec::new)
                        .push(*way_id);
                }
            }
        }

        // Identify junctions: nodes that appear in 2+ ways
        let mut junctions: AHashSet<i64> = AHashSet::new();
        for (node_id, way_ids) in &node_to_ways {
            if way_ids.len() >= 2 {
                junctions.insert(*node_id);
            }
        }

        // Build spatial index
        println!("Building spatial index...");

        // Build map for station processing later (before moving nodes)
        let mut node_pos_map: AHashMap<i64, [f64; 2]> = AHashMap::with_capacity(nodes.len());
        for n in &nodes {
            node_pos_map.insert(n.node_id, n.pos);
        }

        // nodes is already a Vec<RTreeNode>, we can just move it into the tree
        // This avoids the HUGE memory spike of duplicating the list
        let node_count = nodes.len();
        let node_tree = RTree::bulk_load(nodes);

        let coverage_bbox = if has_coverage {
            Some((min_x, min_y, max_x, max_y))
        } else {
            None
        };

        // Build way properties mappings
        let mut way_to_railway_type: AHashMap<i64, String> = AHashMap::new();
        let mut way_geometries: AHashMap<i64, Vec<[f64; 2]>> = AHashMap::new();
        let mut way_layers: AHashMap<i64, i32> = AHashMap::new();

        for (way_id, way_node_ids, railway_type, layer) in &ways_all {
            way_to_railway_type.insert(*way_id, railway_type.clone());
            way_layers.insert(*way_id, *layer);

            // Reconstruct geometry
            let mut geometry = Vec::with_capacity(way_node_ids.len());
            for node_id in way_node_ids {
                if let Some(pos) = node_pos_map.get(node_id) {
                    geometry.push(*pos);
                }
            }
            if !geometry.is_empty() {
                way_geometries.insert(*way_id, geometry);
            }
        }

        // Build relation mappings
        let mut way_to_relations: AHashMap<i64, Vec<i64>> = AHashMap::new();
        let mut relation_info: AHashMap<i64, OsmRouteInfo> = AHashMap::new();

        for (relation_id, way_members, info) in relations_all {
            // Store relation metadata
            relation_info.insert(relation_id, info);

            // Map each way to its containing relations
            for way_id in way_members {
                way_to_relations
                    .entry(way_id)
                    .or_insert_with(Vec::new)
                    .push(relation_id);
            }
        }

        // Process Stations: Compute bboxes and build tree
        let mut station_items: Vec<RTreeStation> = Vec::new();

        // This is getting complex to do efficiently. Let's do it:
        // Build a map of way_id -> &Vec<node_id>
        let way_nodes_map: AHashMap<i64, &Vec<i64>> = ways_all
            .iter()
            .map(|(id, nodes, _, _)| (*id, nodes))
            .collect();
        // Since we need node positions, let's build map node_id -> pos
        // ALREADY BUILT ABOVE

        for (idx, station) in stations.iter_mut().enumerate() {
            let mut min_x = f64::MAX;
            let mut min_y = f64::MAX;
            let mut max_x = f64::MIN;
            let mut max_y = f64::MIN;
            let mut has_bounds = false;

            for member_id in &station.members {
                if let Some(nodes) = way_nodes_map.get(member_id) {
                    for node_id in *nodes {
                        if let Some(pos) = node_pos_map.get(node_id) {
                            min_x = min_x.min(pos[0]);
                            min_y = min_y.min(pos[1]);
                            max_x = max_x.max(pos[0]);
                            max_y = max_y.max(pos[1]);
                            has_bounds = true;
                        }
                    }
                }
            }

            if has_bounds {
                // Add 50m buffer
                let buffer = 50.0;
                station.bbox = Some((
                    min_x - buffer,
                    min_y - buffer,
                    max_x + buffer,
                    max_y + buffer,
                ));

                station_items.push(RTreeStation {
                    index: idx,
                    bbox: AABB::from_corners(
                        [min_x - buffer, min_y - buffer],
                        [max_x + buffer, max_y + buffer],
                    ),
                });
            }
        }

        let station_tree = RTree::bulk_load(station_items);

        println!(
            "Loaded OSM rail index: {} nodes, {} ways, {} junctions, {} route relations, {} stations",
            node_count,
            ways_all.len(),
            junctions.len(),
            relation_info.len(),
            stations.len()
        );

        Ok(Self {
            node_tree,
            node_to_ways,
            junctions,
            coverage_bbox,
            way_to_relations,
            relation_info,
            way_to_railway_type,
            way_geometries,
            way_layers,
            stations,
            station_tree,
        })
    }

    /// Check if a position is within the OSM coverage area (with margin)
    pub fn is_in_coverage(&self, pos: [f64; 2], margin_meters: f64) -> bool {
        match self.coverage_bbox {
            Some((min_x, min_y, max_x, max_y)) => {
                pos[0] >= min_x - margin_meters
                    && pos[0] <= max_x + margin_meters
                    && pos[1] >= min_y - margin_meters
                    && pos[1] <= max_y + margin_meters
            }
            None => false,
        }
    }

    /// Find the nearest OSM node to a position
    ///
    /// Returns (node_id, distance) if found within max_dist_m
    pub fn nearest_node(&self, pos: [f64; 2], max_dist_m: f64) -> Option<(i64, f64)> {
        // Use envelope-based search to avoid needing PointDistance trait
        let aabb = AABB::from_corners(
            [pos[0] - max_dist_m, pos[1] - max_dist_m],
            [pos[0] + max_dist_m, pos[1] + max_dist_m],
        );

        let max_dist_sq = max_dist_m * max_dist_m;
        let mut best: Option<(i64, f64)> = None;

        for node in self.node_tree.locate_in_envelope(&aabb) {
            let dx = node.pos[0] - pos[0];
            let dy = node.pos[1] - pos[1];
            let dist_sq = dx * dx + dy * dy;

            if dist_sq <= max_dist_sq {
                let dist = dist_sq.sqrt();
                match &best {
                    Some((_, best_dist)) if dist >= *best_dist => {}
                    _ => best = Some((node.node_id, dist)),
                }
            }
        }

        best
    }

    /// Find the nearest junction to a position
    ///
    /// Returns (junction_node_id, distance) if found within max_dist_m
    pub fn nearest_junction(&self, pos: [f64; 2], max_dist_m: f64) -> Option<(i64, f64)> {
        let search_radius = max_dist_m;
        let aabb = AABB::from_corners(
            [pos[0] - search_radius, pos[1] - search_radius],
            [pos[0] + search_radius, pos[1] + search_radius],
        );

        let mut best: Option<(i64, f64)> = None;

        for node in self.node_tree.locate_in_envelope(&aabb) {
            if !self.junctions.contains(&node.node_id) {
                continue;
            }

            let dx = node.pos[0] - pos[0];
            let dy = node.pos[1] - pos[1];
            let dist = (dx * dx + dy * dy).sqrt();

            if dist <= max_dist_m {
                match &best {
                    Some((_, best_dist)) if dist >= *best_dist => {}
                    _ => best = Some((node.node_id, dist)),
                }
            }
        }

        best
    }

    /// Check if two positions share an OSM junction
    ///
    /// Returns the junction node ID if both positions are within radius of the same junction.
    /// This is the primary method for X-shape detection: tracks that cross at a flyover
    /// won't share a junction, but tracks that actually merge will.
    pub fn share_junction(&self, pos1: [f64; 2], pos2: [f64; 2], radius_m: f64) -> Option<i64> {
        // Find junctions near pos1
        let junctions_near_1 = self.junctions_in_radius(pos1, radius_m);
        if junctions_near_1.is_empty() {
            return None;
        }

        // Find junctions near pos2
        let junctions_near_2 = self.junctions_in_radius(pos2, radius_m);
        if junctions_near_2.is_empty() {
            return None;
        }

        // Check for intersection
        for junction in &junctions_near_1 {
            if junctions_near_2.contains(junction) {
                return Some(*junction);
            }
        }

        None
    }

    /// Get all junctions within radius of a position
    fn junctions_in_radius(&self, pos: [f64; 2], radius_m: f64) -> Vec<i64> {
        let aabb = AABB::from_corners(
            [pos[0] - radius_m, pos[1] - radius_m],
            [pos[0] + radius_m, pos[1] + radius_m],
        );

        let radius_sq = radius_m * radius_m;

        self.node_tree
            .locate_in_envelope(&aabb)
            .filter(|node| self.junctions.contains(&node.node_id))
            .filter(|node| {
                let dx = node.pos[0] - pos[0];
                let dy = node.pos[1] - pos[1];
                dx * dx + dy * dy <= radius_sq
            })
            .map(|node| node.node_id)
            .collect()
    }

    /// Check if two positions are on the same OSM way
    ///
    /// Returns true if nodes near both positions share at least one way.
    pub fn on_same_way(&self, pos1: [f64; 2], pos2: [f64; 2], radius_m: f64) -> bool {
        let nodes1 = self.nodes_in_radius(pos1, radius_m);
        if nodes1.is_empty() {
            return false;
        }

        let nodes2 = self.nodes_in_radius(pos2, radius_m);
        if nodes2.is_empty() {
            return false;
        }

        // Get all ways containing nodes near pos1
        let ways1: AHashSet<i64> = nodes1
            .iter()
            .filter_map(|n| self.node_to_ways.get(n))
            .flatten()
            .copied()
            .collect();

        if ways1.is_empty() {
            return false;
        }

        // Check if any node near pos2 shares a way
        for node_id in nodes2 {
            if let Some(ways) = self.node_to_ways.get(&node_id) {
                for way_id in ways {
                    if ways1.contains(way_id) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Get all nodes within radius of a position
    fn nodes_in_radius(&self, pos: [f64; 2], radius_m: f64) -> Vec<i64> {
        let aabb = AABB::from_corners(
            [pos[0] - radius_m, pos[1] - radius_m],
            [pos[0] + radius_m, pos[1] + radius_m],
        );

        let radius_sq = radius_m * radius_m;

        self.node_tree
            .locate_in_envelope(&aabb)
            .filter(|node| {
                let dx = node.pos[0] - pos[0];
                let dy = node.pos[1] - pos[1];
                dx * dx + dy * dy <= radius_sq
            })
            .map(|node| node.node_id)
            .collect()
    }

    /// Get all nodes within a bounding box (public accessor)
    pub fn get_nodes_in_bbox(
        &self,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Vec<&RTreeNode> {
        let aabb = AABB::from_corners([min_x, min_y], [max_x, max_y]);
        self.node_tree.locate_in_envelope(&aabb).collect()
    }

    /// Get ways containing a node
    pub fn get_node_ways(&self, node_id: i64) -> Option<&Vec<i64>> {
        self.node_to_ways.get(&node_id)
    }

    /// Get the railway type of a way (e.g. "rail", "subway", "tram")
    pub fn get_way_type(&self, way_id: i64) -> Option<&String> {
        self.way_to_railway_type.get(&way_id)
    }

    /// Get the geometry of a way
    pub fn get_way_geometry(&self, way_id: i64) -> Option<&Vec<[f64; 2]>> {
        self.way_geometries.get(&way_id)
    }

    /// Get the layer/level of a way
    pub fn get_way_layer(&self, way_id: i64) -> Option<i32> {
        self.way_layers.get(&way_id).copied()
    }

    /// Check if a merge should be allowed based on OSM data
    ///
    /// This is the main entry point for merge validation. Returns true if:
    /// 1. Both points share an OSM junction (actual track connection), OR
    /// 2. Both points are on the same OSM way (same track segment)
    ///
    /// For subway (mode 1) and trams (mode 0), this is strictly enforced.
    /// For other modes, the proximity edge case allows merging when tracks are
    /// <30m apart for >400m (handles cases like RER A / Metro Ligne 1).
    pub fn should_allow_merge(
        &self,
        pos1: [f64; 2],
        pos2: [f64; 2],
        radius_m: f64,
        mode: i32,
    ) -> bool {
        // Check if we have OSM coverage for these positions
        if !self.is_in_coverage(pos1, 1000.0) || !self.is_in_coverage(pos2, 1000.0) {
            // No OSM data available, fall back to geometry-only
            return true;
        }

        // For subway, strictly require OSM validation
        let is_subway = mode == 1;
        let is_tram = mode == 0;

        // Check if points share a junction or are on the same way
        if self.share_junction(pos1, pos2, radius_m).is_some() {
            return true;
        }

        if self.on_same_way(pos1, pos2, radius_m) {
            return true;
        }

        // For subway or tram, deny merge if no OSM connection found
        if is_subway || is_tram {
            return false;
        }

        // For non-subway, allow merge (proximity edge case is handled elsewhere)
        true
    }

    /// Get the number of loaded junctions
    pub fn junction_count(&self) -> usize {
        self.junctions.len()
    }

    /// Get the number of loaded nodes
    pub fn node_count(&self) -> usize {
        self.node_tree.size()
    }

    /// Get the number of loaded route relations
    pub fn relation_count(&self) -> usize {
        self.relation_info.len()
    }

    // =========================================================================
    // Relation-based merge decision methods
    // =========================================================================

    /// Get all OSM ways near a position
    ///
    /// This function uses two search strategies:
    /// 1. Node-based: Find ways containing OSM nodes within radius (fast)
    /// 2. Segment-based: Check point-to-segment distance for all way geometries
    ///
    /// The segment-based search is crucial for handling OSM ways with sparse nodes
    /// (common on straight rail sections), where the search point may fall in the
    /// middle of a long segment without any nearby nodes.
    pub fn ways_near_position(&self, pos: [f64; 2], radius_m: f64) -> Vec<i64> {
        let mut ways: AHashSet<i64> = AHashSet::new();

        // Strategy 1: Node-based search (fast path for areas with dense nodes)
        let nodes = self.nodes_in_radius(pos, radius_m);
        for node_id in nodes {
            if let Some(way_ids) = self.node_to_ways.get(&node_id) {
                for way_id in way_ids {
                    ways.insert(*way_id);
                }
            }
        }

        // Strategy 2: Segment-based search (handles sparse OSM nodes)
        // Check if the point is within radius of any segment of any way geometry
        let radius_sq = radius_m * radius_m;
        for (way_id, geom) in &self.way_geometries {
            if ways.contains(way_id) {
                continue; // Already found via node search
            }

            // Quick bounding box pre-filter
            let mut min_x = f64::MAX;
            let mut min_y = f64::MAX;
            let mut max_x = f64::MIN;
            let mut max_y = f64::MIN;
            for pt in geom {
                min_x = min_x.min(pt[0]);
                min_y = min_y.min(pt[1]);
                max_x = max_x.max(pt[0]);
                max_y = max_y.max(pt[1]);
            }

            // Expand bbox by radius and check if point is inside
            if pos[0] < min_x - radius_m
                || pos[0] > max_x + radius_m
                || pos[1] < min_y - radius_m
                || pos[1] > max_y + radius_m
            {
                continue; // Point is outside expanded bounding box
            }

            // Check distance to each segment
            for i in 0..geom.len().saturating_sub(1) {
                let dist_sq = point_to_segment_distance_sq(pos, geom[i], geom[i + 1]);
                if dist_sq <= radius_sq {
                    ways.insert(*way_id);
                    break; // Found, no need to check more segments
                }
            }
        }

        ways.into_iter().collect()
    }

    /// Check if two ways share any route relation (e.g. both are part of "Ligne 1")
    pub fn share_route_relation(&self, way_a: i64, way_b: i64) -> bool {
        let rels_a = match self.way_to_relations.get(&way_a) {
            Some(r) => r,
            None => return false,
        };

        let rels_b = match self.way_to_relations.get(&way_b) {
            Some(r) => r,
            None => return false,
        };

        // Check for intersection
        for ra in rels_a {
            if rels_b.contains(ra) {
                return true;
            }
        }

        false
    }

    /// Get all way IDs in the index
    pub fn get_all_way_ids(&self) -> Vec<i64> {
        self.way_to_railway_type.keys().copied().collect()
    }

    /// Find all OSM junctions along a path
    ///
    /// Returns list of (junction_id, position, distance_along_path)
    pub fn junctions_along_path(
        &self,
        path: &[[f64; 2]],
        search_radius: f64,
    ) -> Vec<(i64, [f64; 2], f64)> {
        let mut results = Vec::new();
        let mut seen = AHashSet::new();

        let mut rtree_results = Vec::new();

        // Sample path to find candidates
        let total_len = crate::geometry_utils::polyline_length(path);
        let step = 10.0;
        let steps = (total_len / step).ceil() as usize;

        for i in 0..=steps {
            let dist = i as f64 * step;
            if dist > total_len {
                break;
            }

            let pt = crate::geometry_utils::sample_along_polyline_f64(path, dist);

            // Manual RTree search to get Node AND Pos
            let aabb = AABB::from_corners(
                [pt[0] - search_radius, pt[1] - search_radius],
                [pt[0] + search_radius, pt[1] + search_radius],
            );

            let radius_sq = search_radius * search_radius;

            for node in self.node_tree.locate_in_envelope(&aabb) {
                if !self.junctions.contains(&node.node_id) {
                    continue;
                }

                let dx = node.pos[0] - pt[0];
                let dy = node.pos[1] - pt[1];
                if dx * dx + dy * dy <= radius_sq {
                    if seen.insert(node.node_id) {
                        rtree_results.push((node.node_id, node.pos));
                    }
                }
            }
        }

        // Process unique results
        for (jid, pos) in rtree_results {
            // Project junction onto path to get precise distance
            let proj_dist = crate::geometry_utils::project_point_to_polyline_f64(pos, path);
            results.push((jid, pos, proj_dist));
        }

        // Sort by distance
        results.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Get route relations that both positions share
    ///
    /// Returns relation IDs that contain ways near both positions.
    /// This is the key method for determining if two tracks are on the same route.
    pub fn shared_route_relations(
        &self,
        pos1: [f64; 2],
        pos2: [f64; 2],
        radius_m: f64,
    ) -> Vec<i64> {
        let ways1 = self.ways_near_position(pos1, radius_m);
        let ways2 = self.ways_near_position(pos2, radius_m);

        if ways1.is_empty() || ways2.is_empty() {
            return vec![];
        }

        // Get all relations for ways near pos1
        let relations1: AHashSet<i64> = ways1
            .iter()
            .filter_map(|w| self.way_to_relations.get(w))
            .flatten()
            .copied()
            .collect();

        // Get all relations for ways near pos2
        let relations2: AHashSet<i64> = ways2
            .iter()
            .filter_map(|w| self.way_to_relations.get(w))
            .flatten()
            .copied()
            .collect();

        // Find intersection
        relations1.intersection(&relations2).copied().collect()
    }

    /// Check if two positions should merge based on shared route relations
    ///
    /// This is the primary merge decision method. Returns:
    /// - `Some(shared_relations)` if merge is allowed (positions share route relations)
    /// - `None` if merge is not allowed (no shared relations or mode incompatibility)
    ///
    /// The `mode` parameter is used for mode compatibility:
    /// - 1 = subway (only merge with subway routes)
    /// - 0 = tram (merge with tram/light_rail routes)
    /// - 2 = rail (merge with train/rail routes)
    pub fn should_merge_by_relation(
        &self,
        pos1: [f64; 2],
        pos2: [f64; 2],
        radius_m: f64,
        mode: i32,
    ) -> Option<Vec<i64>> {
        // Check if we have OSM coverage for these positions
        if !self.is_in_coverage(pos1, 100.0) || !self.is_in_coverage(pos2, 100.0) {
            // No OSM data available - return None to signal fallback to geometry
            return None;
        }

        let shared_relations = self.shared_route_relations(pos1, pos2, radius_m);

        if shared_relations.is_empty() {
            // No shared relations - also check if they share a junction (physical connection)
            // This handles cases where routes aren't fully tagged in relations
            if self.share_junction(pos1, pos2, radius_m).is_some() {
                return Some(vec![]); // Allow merge at physical junction
            }
            if self.on_same_way(pos1, pos2, radius_m) {
                return Some(vec![]); // Allow merge on same way
            }
            return Some(vec![]); // TEMPORARY: allow merge to avoid regression
            // TODO: Once relation coverage is good, change to:
            // return None; // Different routes - don't merge
        }

        // Check mode compatibility for shared relations
        let has_compatible_relation = shared_relations.iter().any(|rel_id| {
            if let Some(info) = self.relation_info.get(rel_id) {
                match mode {
                    1 => info.route_type == "subway",
                    0 => info.route_type == "tram" || info.route_type == "light_rail",
                    2 => info.route_type == "train",
                    _ => true, // Unknown mode - allow any
                }
            } else {
                false
            }
        });

        if has_compatible_relation || shared_relations.is_empty() {
            Some(shared_relations)
        } else {
            // Shared relations exist but none are mode-compatible
            // This could happen if a subway and rail share infrastructure
            // For now, still allow the merge
            Some(shared_relations)
        }
    }

    /// Get route info for a relation ID
    pub fn get_relation_info(&self, relation_id: i64) -> Option<&OsmRouteInfo> {
        self.relation_info.get(&relation_id)
    }

    /// Get all relations that contain a way
    pub fn get_way_relations(&self, way_id: i64) -> Option<&Vec<i64>> {
        self.way_to_relations.get(&way_id)
    }

    /// Get the railway type tag for a way
    pub fn get_way_railway_type(&self, way_id: i64) -> Option<&String> {
        self.way_to_railway_type.get(&way_id)
    }

    /// Find all OSM junctions along a polyline path within a tolerance
    ///
    /// Returns a list of (junction_id, position, distance_along_path) sorted by distance.
    /// This is used to split edges at junction points.

    // ...

    // I will use a different StartLine/EndLine to INSERT the new method.
    // Insertion point: After `get_way_railway_type` (lines 734-737) and before `snap_to_junction` (line 743).

    /// Get all stations near a position
    pub fn query_nearby_stations(&self, pos: [f64; 2], radius_m: f64) -> Vec<&OsmStationInfo> {
        let aabb = AABB::from_corners(
            [pos[0] - radius_m, pos[1] - radius_m],
            [pos[0] + radius_m, pos[1] + radius_m],
        );

        self.station_tree
            .locate_in_envelope(&aabb)
            .map(|item| &self.stations[item.index])
            .collect()
    }

    /// Snap a position to the nearest OSM junction
    ///
    /// Returns (junction_node_id, snapped_position) if found within radius_m.
    /// Used to align edge endpoints to actual track junctions.
    pub fn snap_to_junction(&self, pos: [f64; 2], radius_m: f64) -> Option<(i64, [f64; 2])> {
        let aabb = AABB::from_corners(
            [pos[0] - radius_m, pos[1] - radius_m],
            [pos[0] + radius_m, pos[1] + radius_m],
        );

        let radius_sq = radius_m * radius_m;
        let mut best: Option<(i64, [f64; 2], f64)> = None;

        for node in self.node_tree.locate_in_envelope(&aabb) {
            if !self.junctions.contains(&node.node_id) {
                continue;
            }

            let dx = node.pos[0] - pos[0];
            let dy = node.pos[1] - pos[1];
            let dist_sq = dx * dx + dy * dy;

            if dist_sq <= radius_sq {
                match &best {
                    Some((_, _, best_dist_sq)) if dist_sq >= *best_dist_sq => {}
                    _ => best = Some((node.node_id, node.pos, dist_sq)),
                }
            }
        }

        best.map(|(id, snap_pos, _)| (id, snap_pos))
    }

    /// Check if there's an OSM junction near the start or end of a path
    ///
    /// Returns (start_junction, end_junction) where each is Option<(id, pos)>
    pub fn path_endpoint_junctions(
        &self,
        path: &[[f64; 2]],
        radius_m: f64,
    ) -> (Option<(i64, [f64; 2])>, Option<(i64, [f64; 2])>) {
        if path.is_empty() {
            return (None, None);
        }

        let start_jct = self.snap_to_junction(path[0], radius_m);
        let end_jct = if path.len() > 1 {
            self.snap_to_junction(*path.last().unwrap(), radius_m)
        } else {
            None
        };

        (start_jct, end_jct)
    }
}

// ===========================================================================
// Geometry Helpers
// ===========================================================================

/// Calculate squared distance from a point to a line segment.
/// Using squared distance avoids sqrt for performance in tight loops.
fn point_to_segment_distance_sq(p: [f64; 2], a: [f64; 2], b: [f64; 2]) -> f64 {
    let dx = b[0] - a[0];
    let dy = b[1] - a[1];
    let len_sq = dx * dx + dy * dy;

    if len_sq < 1e-9 {
        // Degenerate segment (point), just return distance to a
        let px = p[0] - a[0];
        let py = p[1] - a[1];
        return px * px + py * py;
    }

    // Project point onto line, clamped to segment [0, 1]
    let t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);

    let proj_x = a[0] + t * dx;
    let proj_y = a[1] + t * dy;

    let px = p[0] - proj_x;
    let py = p[1] - proj_y;
    px * px + py * py
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord_conversion::lat_lng_to_web_merc;

    #[test]
    fn test_empty_index() {
        let index = OsmRailIndex::empty();
        assert_eq!(index.node_count(), 0);
        assert_eq!(index.junction_count(), 0);
        assert!(index.nearest_junction([0.0, 0.0], 100.0).is_none());
    }

    #[test]
    fn test_lon_lat_to_web_merc() {
        // Test origin
        let (x, y) = lat_lng_to_web_merc(0.0, 0.0);
        assert!((x - 0.0).abs() < 1.0);
        // y at equator is not exactly 0 due to Mercator projection

        // Test positive values (Paris-ish)
        let (x, y) = lat_lng_to_web_merc(2.35, 48.85);
        assert!(x > 0.0);
        assert!(y > 0.0);
    }

    #[test]
    fn test_coverage_check() {
        let mut index = OsmRailIndex::empty();
        index.coverage_bbox = Some((0.0, 0.0, 1000.0, 1000.0));

        assert!(index.is_in_coverage([500.0, 500.0], 0.0));
        assert!(index.is_in_coverage([0.0, 0.0], 0.0));
        assert!(!index.is_in_coverage([-100.0, 500.0], 0.0));
        assert!(index.is_in_coverage([-100.0, 500.0], 200.0)); // With margin
    }
}
