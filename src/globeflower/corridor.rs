use crate::geometry_utils;
use crate::osm_loader::OsmRailIndex;
use crate::osm_types::{AtomicEdge, AtomicEdgeId, LineId, RailMode, ZClass};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, info, warn};
use rstar::{AABB, RTree, RTreeObject}; // Added RTree import
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Identifier for a corridor cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorridorId(pub u64);

/// A cluster of parallel tracks merged into a single corridor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorridorCluster {
    pub id: CorridorId,
    /// Member atomic edge IDs
    pub member_edges: Vec<AtomicEdgeId>,
    /// Computed centerline geometry (lon, lat)
    pub centerline: Vec<(f64, f64)>,
    /// Shared z-class (or Primary if mixed)
    pub z_class: ZClass,
    /// Shared mode
    pub mode: RailMode,
    /// Total length of centerline in meters
    pub length_m: f64,
    /// Whether this cluster contains members from different Z-classes
    pub is_mixed_z: bool,
}

/// Configuration for corridor detection
#[derive(Debug, Clone)]
pub struct CorridorConfig {
    pub max_hausdorff_m: f64,
    pub max_angle_diff_deg: f64,
    pub min_overlap_ratio: f64,
    pub centerline_sample_interval: f64,
}

impl Default for CorridorConfig {
    fn default() -> Self {
        Self {
            max_hausdorff_m: 25.0,
            max_angle_diff_deg: 15.0,
            min_overlap_ratio: 0.3,
            centerline_sample_interval: 10.0,
        }
    }
}

/// R-tree entry for edge bounding boxes
struct EdgeBboxEntry {
    idx: usize,
    min: [f64; 2],
    max: [f64; 2],
}

impl RTreeObject for EdgeBboxEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.min, self.max)
    }
}

/// A sub-segment of an original edge
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct FragmentId {
    edge_idx: usize,
    /// Quantized start/end for hash logic if needed, but we use index in fragments vec usually
    frag_idx: usize,
}

#[derive(Debug, Clone)]
struct Fragment {
    edge_idx: usize,
    start_frac: f64,
    end_frac: f64,
    start_m: f64,       // Metric start
    end_m: f64,         // Metric end
    anchor_start: bool, // Snapped to anchor?
    anchor_end: bool,
}

/// Union-Find data structure
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]);
        }
        self.parent[x]
    }

    fn union(&mut self, x: usize, y: usize) {
        let px = self.find(x);
        let py = self.find(y);
        if px != py {
            match self.rank[px].cmp(&self.rank[py]) {
                std::cmp::Ordering::Less => self.parent[px] = py,
                std::cmp::Ordering::Greater => self.parent[py] = px,
                std::cmp::Ordering::Equal => {
                    self.parent[py] = px;
                    self.rank[px] += 1;
                }
            }
        }
    }
}

/// Corridor builder for detecting and merging parallel tracks
pub struct CorridorBuilder<'a> {
    index: &'a OsmRailIndex,
    config: CorridorConfig,
    edge_to_routes: Option<&'a HashMap<AtomicEdgeId, Vec<LineId>>>,
}

impl<'a> CorridorBuilder<'a> {
    pub fn new(
        index: &'a OsmRailIndex,
        config: CorridorConfig,
        edge_to_routes: Option<&'a HashMap<AtomicEdgeId, Vec<LineId>>>,
    ) -> Self {
        Self {
            index,
            config,
            edge_to_routes,
        }
    }

    /// Build corridors and write directly to disk tiles
    pub fn build_corridors_to_disk(
        &self,
        tile_dir: &Path,
        tile_size_deg: f64,
    ) -> std::io::Result<DiskCorridorIndex> {
        // Return only index, main.rs needs update
        info!(
            "Building corridor clusters from {} edges (tiled)",
            self.index.edges.len()
        );

        let mut disk_index = DiskCorridorIndex::new(tile_dir.to_path_buf(), tile_size_deg)?;

        // 1. Assign edges to tiles (with Halo)
        let halo = 0.002; // ~200m halo
        let mut tiles: HashMap<(i32, i32), Vec<usize>> = HashMap::new();

        for (i, edge) in self.index.edges.iter().enumerate() {
            let (min_x, min_y, max_x, max_y) = Self::bbox_geo(&edge.geometry);

            // Grid range
            let min_tx = ((min_x - halo) / tile_size_deg).floor() as i32;
            let max_tx = ((max_x + halo) / tile_size_deg).floor() as i32;
            let min_ty = ((min_y - halo) / tile_size_deg).floor() as i32;
            let max_ty = ((max_y + halo) / tile_size_deg).floor() as i32;

            for tx in min_tx..=max_tx {
                for ty in min_ty..=max_ty {
                    tiles.entry((tx, ty)).or_default().push(i);
                }
            }
        }

        let mut next_id = 0u64;
        let mut written = 0usize;
        let total_tiles = tiles.len();
        let mut processed_tiles = 0;

        // 2. Process tiles
        for ((tx, ty), edge_indices) in tiles {
            processed_tiles += 1;
            if processed_tiles % 10 == 0 {
                info!(
                    "Processing tile {}/{} ({}, {})",
                    processed_tiles, total_tiles, tx, ty
                );
            }

            // Group by Mode (we merge Cross-Z, but never Cross-Mode for now)
            let mut mode_groups: HashMap<RailMode, Vec<usize>> = HashMap::new();
            for &idx in &edge_indices {
                mode_groups
                    .entry(self.index.edges[idx].mode)
                    .or_default()
                    .push(idx);
            }

            // Define tile bounds for ownership check
            let tile_min_x = tx as f64 * tile_size_deg;
            let tile_min_y = ty as f64 * tile_size_deg;
            let tile_max_x = (tx + 1) as f64 * tile_size_deg;
            let tile_max_y = (ty + 1) as f64 * tile_size_deg;

            for (mode, group_indices) in mode_groups {
                // Calculate local origin for this tile group
                let origin_lon = tile_min_x + tile_size_deg / 2.0;
                let origin_lat = tile_min_y + tile_size_deg / 2.0;

                let clusters =
                    self.cluster_group(&group_indices, mode, origin_lon, origin_lat, &mut next_id);

                for cluster in clusters {
                    // Deduplication: Only write if centroid is in this tile
                    let (clon, clat) = Self::centroid(&cluster.centerline);
                    if clon >= tile_min_x
                        && clon < tile_max_x
                        && clat >= tile_min_y
                        && clat < tile_max_y
                    {
                        if let Err(e) = disk_index.write_corridor(&cluster) {
                            warn!("Failed to write corridor: {}", e);
                        }
                        written += 1;
                    }
                }
            }
        }

        info!("Built {} corridors", written);
        Ok(disk_index)
    }

    /// Process a group of edges (same mode) in a local tangent plane
    fn cluster_group(
        &self,
        edge_indices: &[usize],
        mode: RailMode,
        origin_lon: f64,
        origin_lat: f64,
        next_id: &mut u64,
    ) -> Vec<CorridorCluster> {
        if edge_indices.is_empty() {
            return vec![];
        }

        let ltp = geometry_utils::LocalTangentPlane::new(origin_lon, origin_lat);
        let edges: Vec<&AtomicEdge> = edge_indices.iter().map(|&i| &self.index.edges[i]).collect();

        // Project to metric
        let metric_geoms: Vec<Vec<(f64, f64)>> = edges
            .iter()
            .map(|e| e.geometry.iter().map(|&(x, y)| ltp.project(x, y)).collect())
            .collect();

        // Build RTree
        let rtree_entries: Vec<EdgeBboxEntry> = metric_geoms
            .iter()
            .enumerate()
            .map(|(i, g)| {
                let (min_x, min_y, max_x, max_y) = Self::bbox_metric(g);
                EdgeBboxEntry {
                    idx: i,
                    min: [min_x, min_y],
                    max: [max_x, max_y],
                }
            })
            .collect();
        let rtree = RTree::bulk_load(rtree_entries);

        // 1. Interval Discovery & Splitting
        // Map: edge_idx -> list of split points (metric distances)
        // Also track "Anchor" points? For now just splitting is enough if fragments match.
        // Actually, "Anchor Snapping" means if two splits are close, use same coord.
        // Simplest way: just store split fractions.
        let mut splits: HashMap<usize, Vec<f64>> = HashMap::new();
        for i in 0..edges.len() {
            splits.insert(i, vec![0.0, 1.0]);
        }

        let search_dist = self.config.max_hausdorff_m;

        for i in 0..edges.len() {
            let (min_x, min_y, max_x, max_y) = Self::bbox_metric(&metric_geoms[i]);
            let envelope = AABB::from_corners(
                [min_x - search_dist, min_y - search_dist],
                [max_x + search_dist, max_y + search_dist],
            );

            for entry in rtree.locate_in_envelope_intersecting(&envelope) {
                let j = entry.idx;
                if j <= i {
                    continue;
                }

                let edge_a = edges[i];
                let edge_b = edges[j];

                // Route Gating (Subway)
                if !self.can_merge_routes(mode, edge_a, edge_b) {
                    continue;
                }

                // Interval check
                let intervals = geometry_utils::overlap_intervals_metric(
                    &metric_geoms[i],
                    &metric_geoms[j],
                    self.config.max_hausdorff_m,
                    self.config.max_angle_diff_deg,
                );

                for (as_f, ae_f, bs_f, be_f) in intervals {
                    // Cross-Z Safety
                    if edge_a.z_class != edge_b.z_class {
                        // Check overlap length
                        let len_a = (ae_f - as_f).abs()
                            * geometry_utils::polyline_length_metric(&metric_geoms[i]);
                        if len_a < 30.0 {
                            continue;
                        }

                        // Check p90
                        // Extract sub-segments
                        let sub_a =
                            geometry_utils::extract_sub_polyline(&metric_geoms[i], as_f, ae_f);
                        let sub_b =
                            geometry_utils::extract_sub_polyline(&metric_geoms[j], bs_f, be_f);
                        let p90 = geometry_utils::closest_distance_percentile_metric(
                            &sub_a, &sub_b, 90.0,
                        );

                        if p90 > 1.0 {
                            continue;
                        }

                        // Check endpoints proximity (prevent bridge shadow)
                        // Start-Start or Start-End proximity
                        let dist_s = Self::pt_dist(sub_a.first().unwrap(), sub_b.first().unwrap())
                            .min(Self::pt_dist(sub_a.first().unwrap(), sub_b.last().unwrap()));
                        let dist_e = Self::pt_dist(sub_a.last().unwrap(), sub_b.last().unwrap())
                            .min(Self::pt_dist(sub_a.last().unwrap(), sub_b.first().unwrap()));

                        if dist_s > 2.0 && dist_e > 2.0 {
                            continue;
                        }
                    }

                    // Valid interval -> Add splits
                    splits
                        .entry(i)
                        .or_default()
                        .extend_from_slice(&[as_f, ae_f]);
                    splits
                        .entry(j)
                        .or_default()
                        .extend_from_slice(&[bs_f, be_f]);
                }
            }
        }

        // 2. Create Fragments
        let mut fragments: Vec<Fragment> = Vec::new();
        for i in 0..edges.len() {
            let edge_splits = splits.get_mut(&i).unwrap();
            edge_splits.sort_by(|a, b| a.partial_cmp(b).unwrap());
            edge_splits.dedup_by(|a, b| (*a - *b).abs() < 1e-5);

            let total_len = geometry_utils::polyline_length_metric(&metric_geoms[i]);

            for w in edge_splits.windows(2) {
                let s = w[0];
                let e = w[1];
                let len = (e - s).abs() * total_len;
                if len > 5.0 {
                    // Min 5m fragment
                    fragments.push(Fragment {
                        edge_idx: i,
                        start_frac: s,
                        end_frac: e,
                        start_m: s * total_len,
                        end_m: e * total_len,
                        anchor_start: false,
                        anchor_end: false,
                    });
                }
            }
        }

        // 3. Cluster Fragments
        let mut uf = UnionFind::new(fragments.len());
        // Map to quickly find fragments for edge
        let mut frags_by_edge: Vec<Vec<usize>> = vec![Vec::new(); edges.len()];
        for (idx, f) in fragments.iter().enumerate() {
            frags_by_edge[f.edge_idx].push(idx);
        }

        for i in 0..fragments.len() {
            let fi = &fragments[i];
            let edge_i = edges[fi.edge_idx];

            // Query potential overlaps
            // Only check fragments on OTHER edges locally nearby
            // Use RTree again? Or just iterate edges in box.
            // Optimize: iterate edges in bbox of fragment?
            // Simple: iterate nearby edges from RTree
            let frag_geom_i = geometry_utils::extract_sub_polyline(
                &metric_geoms[fi.edge_idx],
                fi.start_frac,
                fi.end_frac,
            );
            let (min_x, min_y, max_x, max_y) = Self::bbox_metric(&frag_geom_i);
            let envelope = AABB::from_corners(
                [min_x - search_dist, min_y - search_dist],
                [max_x + search_dist, max_y + search_dist],
            );

            for entry in rtree.locate_in_envelope_intersecting(&envelope) {
                let edge_j_idx = entry.idx;
                if edge_j_idx <= fi.edge_idx {
                    continue;
                } // canonical order

                let edge_j = edges[edge_j_idx];

                // Route Gating safety net
                if !self.can_merge_routes(mode, edge_i, edge_j) {
                    continue;
                }

                for &j in &frags_by_edge[edge_j_idx] {
                    let fj = &fragments[j];

                    // Check direct METRIC overlap on fragments
                    // Anti-Transitive: must overlap directly
                    if self.fragments_overlap_metric(
                        &metric_geoms[fi.edge_idx],
                        fi,
                        &metric_geoms[edge_j_idx],
                        fj,
                    ) {
                        // Double check Z-class safety for fragments
                        if edge_i.z_class != edge_j.z_class {
                            // Strict again? Yes.
                            let frag_geom_j = geometry_utils::extract_sub_polyline(
                                &metric_geoms[fj.edge_idx],
                                fj.start_frac,
                                fj.end_frac,
                            );
                            let p90 = geometry_utils::closest_distance_percentile_metric(
                                &frag_geom_i,
                                &frag_geom_j,
                                90.0,
                            );
                            if p90 > 1.0 {
                                continue;
                            }
                        }

                        uf.union(i, j);
                    }
                }
            }
        }

        // 4. Build Output
        let mut cluster_map: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..fragments.len() {
            cluster_map.entry(uf.find(i)).or_default().push(i);
        }

        cluster_map
            .into_values()
            .map(|indices| {
                let mut z_classes = HashSet::new();
                let mut member_edges = HashSet::new();
                let mut weighted_geoms = Vec::new();

                for &idx in &indices {
                    let f = &fragments[idx];
                    let e = edges[f.edge_idx];
                    z_classes.insert(e.z_class);
                    member_edges.insert(e.id);

                    let geom = geometry_utils::extract_sub_polyline(
                        &metric_geoms[f.edge_idx],
                        f.start_frac,
                        f.end_frac,
                    );
                    weighted_geoms.push((geom, 1.0));
                }

                // Sort primary Z-class
                // If mixed, prefer ground? Or just take mode?
                let primary_z = if z_classes.contains(&ZClass::GROUND) {
                    ZClass::GROUND
                } else {
                    *z_classes.iter().next().unwrap()
                };
                let is_mixed_z = z_classes.len() > 1;

                // Centerline
                let refs: Vec<_> = weighted_geoms
                    .iter()
                    .map(|(g, w)| (g.as_slice(), *w))
                    .collect();
                let start_m = geometry_utils::weighted_average_centerline_metric(
                    &refs,
                    self.config.centerline_sample_interval,
                );

                // Unproject
                let centerline: Vec<(f64, f64)> =
                    start_m.iter().map(|&(x, y)| ltp.unproject(x, y)).collect();
                let length_m = geometry_utils::polyline_length_metric(&start_m);

                let id = CorridorId(*next_id);
                *next_id += 1;

                CorridorCluster {
                    id,
                    member_edges: member_edges.into_iter().collect(),
                    centerline,
                    z_class: primary_z,
                    mode,
                    length_m,
                    is_mixed_z,
                }
            })
            .collect()
    }

    fn pt_dist(p1: &(f64, f64), p2: &(f64, f64)) -> f64 {
        ((p1.0 - p2.0).powi(2) + (p1.1 - p2.1).powi(2)).sqrt()
    }

    fn can_merge_routes(&self, mode: RailMode, a: &AtomicEdge, b: &AtomicEdge) -> bool {
        if mode != RailMode::Subway {
            return true;
        }
        let map = match self.edge_to_routes {
            Some(m) => m,
            None => return true, // No route info, strict geom check will handle? or allow?
        };

        // If unknown route data, fall back to "allow but rely on geometry"?
        // User recommendation: "Treat empty as unknown... Allow unknown<->known only under strong geometry"
        // Here we are in "Gate".
        // Stronger: "If both non-empty -> require intersection".
        // "If either empty -> allow merge" (geometry check comes later or inside intervals).
        // Let's implement intersection check here.

        let ra = map.get(&a.id);
        let rb = map.get(&b.id);

        match (ra, rb) {
            (Some(routes_a), Some(routes_b)) => {
                if routes_a.is_empty() || routes_b.is_empty() {
                    return true;
                } // Unknown
                // Check intersection
                for r in routes_a {
                    if routes_b.contains(r) {
                        return true;
                    }
                }
                false // Disjoint known routes
            }
            _ => true, // Unknown
        }
    }

    fn fragments_overlap_metric(
        &self,
        geom_a: &[(f64, f64)],
        frag_a: &Fragment,
        geom_b: &[(f64, f64)],
        frag_b: &Fragment,
    ) -> bool {
        let sub_a =
            geometry_utils::extract_sub_polyline(geom_a, frag_a.start_frac, frag_a.end_frac);
        let sub_b =
            geometry_utils::extract_sub_polyline(geom_b, frag_b.start_frac, frag_b.end_frac);

        let ratio_a =
            geometry_utils::overlap_ratio_metric(&sub_a, &sub_b, self.config.max_hausdorff_m);
        let ratio_b =
            geometry_utils::overlap_ratio_metric(&sub_b, &sub_a, self.config.max_hausdorff_m);

        ratio_a.max(ratio_b) >= self.config.min_overlap_ratio
    }

    fn bbox_geo(geom: &[(f64, f64)]) -> (f64, f64, f64, f64) {
        let mut min_x = f64::INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        for &(x, y) in geom {
            min_x = min_x.min(x);
            min_y = min_y.min(y);
            max_x = max_x.max(x);
            max_y = max_y.max(y);
        }
        (min_x, min_y, max_x, max_y)
    }

    fn bbox_metric(geom: &[(f64, f64)]) -> (f64, f64, f64, f64) {
        Self::bbox_geo(geom)
    }

    fn centroid(geom: &[(f64, f64)]) -> (f64, f64) {
        if geom.is_empty() {
            return (0.0, 0.0);
        }
        let (sum_x, sum_y) = geom.iter().fold((0.0, 0.0), |a, b| (a.0 + b.0, a.1 + b.1));
        let n = geom.len() as f64;
        (sum_x / n, sum_y / n)
    }
}

pub struct CorridorIndex {/* Legacy or used? */}
// We use DiskCorridorIndex mostly now.

/// Disk-backed corridor index
pub struct DiskCorridorIndex {
    pub tile_dir: PathBuf,
    pub tile_size_deg: f64,
    pub tiles: HashMap<(i32, i32), (PathBuf, usize)>,
    pub total_corridors: usize,
}

impl DiskCorridorIndex {
    pub fn new(tile_dir: PathBuf, tile_size_deg: f64) -> std::io::Result<Self> {
        if tile_dir.exists() {
            fs::remove_dir_all(&tile_dir)?;
        }
        fs::create_dir_all(&tile_dir)?;
        Ok(Self {
            tile_dir,
            tile_size_deg,
            tiles: HashMap::new(),
            total_corridors: 0,
        })
    }

    // ... Copy existing methods ...
    pub fn write_corridor(&mut self, corridor: &CorridorCluster) -> std::io::Result<()> {
        // ... same impl ...
        // Need to duplicate impl for brevity or rewrite?
        // I will rewrite minimal impl.

        let (sum_lon, sum_lat): (f64, f64) = corridor
            .centerline
            .iter()
            .fold((0.0, 0.0), |(a, b), &(lon, lat)| (a + lon, b + lat));
        let n = corridor.centerline.len() as f64;
        let centroid = (sum_lon / n, sum_lat / n);

        let tx = (centroid.0 / self.tile_size_deg).floor() as i32;
        let ty = (centroid.1 / self.tile_size_deg).floor() as i32;
        let key = (tx, ty);

        let path = self.tile_dir.join(format!("tile_{}_{}.bin", tx, ty));
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);

        let config = bincode::config::standard();
        let encoded = bincode::serde::encode_to_vec(corridor, config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let len = encoded.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&encoded)?;

        self.tiles.entry(key).or_insert((path, 0)).1 += 1;
        self.total_corridors += 1;
        Ok(())
    }

    pub fn cleanup(&self) -> std::io::Result<()> {
        if self.tile_dir.exists() {
            fs::remove_dir_all(&self.tile_dir)?;
        }
        Ok(())
    }

    pub fn read_tile(&self, key: (i32, i32)) -> std::io::Result<Vec<CorridorCluster>> {
        let path = match self.tiles.get(&key) {
            Some((p, _)) => p,
            None => return Ok(vec![]),
        };
        if !path.exists() {
            return Ok(vec![]);
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut corridors = Vec::new();
        let config = bincode::config::standard();
        loop {
            let mut len_buf = [0u8; 4];
            match std::io::Read::read_exact(&mut reader, &mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            std::io::Read::read_exact(&mut reader, &mut data)?;
            let (c, _): (CorridorCluster, _) = bincode::serde::decode_from_slice(&data, config)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            corridors.push(c);
        }
        Ok(corridors)
    }
    pub fn sorted_tile_keys(&self) -> Vec<(i32, i32)> {
        let mut keys: Vec<_> = self.tiles.keys().cloned().collect();
        keys.sort();
        keys
    }
}
