use crate::graph::{LineOnEdge, RenderGraph};
use geo::{Coord, LineString as GeoLineString, Rect, Simplify};
use log::{debug, info};
use mvt::{GeomData, GeomEncoder, Tile};
use pointy::BBox;
use rayon::prelude::*;
use std::collections::HashSet;
use std::f64::consts::PI;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Result of parallel tile generation
pub struct GeneratedTile {
    pub z: u8,
    pub x: u32,
    pub y: u32,
    pub tile: Tile,
}

/// Configuration for parallel tile generation
#[derive(Debug, Clone)]
pub struct ParallelTileConfig {
    /// Number of threads to use (None = use rayon default)
    pub num_threads: Option<usize>,
    /// Buffer percentage for tile bounds (default 0.1 = 10%)
    pub buffer_pct: f64,
}

impl Default for ParallelTileConfig {
    fn default() -> Self {
        Self {
            num_threads: None,
            buffer_pct: 0.1,
        }
    }
}

pub struct TileGenerator;

impl TileGenerator {
    pub fn generate_tile(graph: &RenderGraph, z: u8, x: u32, y: u32) -> Tile {
        let mut tile = Tile::new(4096);
        let mut layer = tile.create_layer("transit");

        let extent = 4096.0;

        // Calculate tile bounds in lat/lon with buffer
        // Buffer factor 0.1 means 10% of tile size extra
        let (min_lon, min_lat, max_lon, max_lat) = Self::tile_bounds(z, x, y, 0.1);

        info!(
            "Generating Tile z={}, x={}, y={}. Bounds: [{}, {}, {}, {}]",
            z, x, y, min_lon, min_lat, max_lon, max_lat
        );

        let envelope = rstar::AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);

        // Query spatial index
        let candidates: Vec<_> = graph
            .tree
            .locate_in_envelope_intersecting(&envelope)
            .collect();

        info!(
            "Found {} candidate edges for tile {}/{}/{}",
            candidates.len(),
            z,
            x,
            y
        );

        for item in candidates {
            let edge_idx = item.data;
            if edge_idx >= graph.edges.len() {
                continue;
            }
            let edge = &graph.edges[edge_idx];
            if edge.geometry.is_empty() {
                continue;
            }

            // Clip the geometry to tile bounds BEFORE projection
            // This produces proper interpolated points at tile boundaries
            // Use a small buffer to avoid edge artifacts
            let clip_buffer = 0.05; // 5% buffer in tile-relative coordinates
            let clip_rect = Rect::new(
                Coord {
                    x: min_lon - (max_lon - min_lon) * clip_buffer,
                    y: min_lat - (max_lat - min_lat) * clip_buffer,
                },
                Coord {
                    x: max_lon + (max_lon - min_lon) * clip_buffer,
                    y: max_lat + (max_lat - min_lat) * clip_buffer,
                },
            );

            // Cutback is applied AFTER projection to tile space, not here.
            // The cutback distance depends on the number of lines and spacing, which varies per node.
            // We compute proper "node front" cutback in tile space during rendering.
            let working_coords: Vec<[f64; 2]> = edge.geometry.clone();

            // Build the source linestring from edge geometry (lon, lat)
            let source_coords: Vec<Coord> = working_coords
                .iter()
                .map(|p| Coord { x: p[0], y: p[1] })
                .collect();

            if source_coords.len() < 2 {
                continue;
            }

            // Clip the line to tile bounds - this produces clipped segments
            let clipped_segments = Self::clip_line_to_rect(&source_coords, &clip_rect);

            if clipped_segments.is_empty() {
                continue;
            }

            // Process each clipped segment
            for clipped_coords in clipped_segments {
                if clipped_coords.len() < 2 {
                    continue;
                }

                // Apply line simplification for lower zoom levels
                // This reduces complexity and creates smoother lines at zoomed-out views
                let smoothed_coords = if let Some(epsilon) = Self::get_simplification_epsilon(z) {
                    Self::simplify_coords(&clipped_coords, epsilon)
                } else {
                    clipped_coords
                };

                if smoothed_coords.len() < 2 {
                    continue;
                }

                // Project smoothed points to tile coordinates
                let tile_points: Vec<(f64, f64)> = smoothed_coords
                    .iter()
                    .map(|c| Self::project_to_tile(c.x, c.y, z, x, y, extent))
                    .collect();

                if tile_points.len() < 2 {
                    continue;
                }

                // Compute normals for each segment
                // normals[i] corresponds to segment i -> i+1
                let mut segment_normals = Vec::with_capacity(tile_points.len().saturating_sub(1));
                for i in 0..tile_points.len().saturating_sub(1) {
                    let (x1, y1) = tile_points[i];
                    let (x2, y2) = tile_points[i + 1];
                    let dx = x2 - x1;
                    let dy = y2 - y1;
                    let len = (dx * dx + dy * dy).sqrt();
                    if len > 0.0 {
                        segment_normals.push((-dy / len, dx / len));
                    } else {
                        segment_normals.push((0.0, 0.0));
                    }
                }
                if segment_normals.is_empty() {
                    continue;
                }

                // Compute vertex normals (averaging adjacent segment normals)
                // vertex_normals[i] corresponds to point i
                let mut vertex_normals = Vec::with_capacity(tile_points.len());

                // Start point normal is same as first segment
                vertex_normals.push(segment_normals[0]);

                for i in 1..tile_points.len() - 1 {
                    let (nx1, ny1) = segment_normals[i - 1];
                    let (nx2, ny2) = segment_normals[i];
                    // Average
                    let mx = (nx1 + nx2) * 0.5;
                    let my = (ny1 + ny2) * 0.5;
                    let len = (mx * mx + my * my).sqrt();
                    if len > 0.0 {
                        vertex_normals.push((mx / len, my / len));
                    } else {
                        vertex_normals.push((nx1, ny1));
                    }
                }

                // End point normal is same as last segment
                vertex_normals.push(*segment_normals.last().unwrap());

                let spacing = Self::get_zoom_spacing(z);

                // Group lines by color - adjacent lines with same color share the same offset slot.
                // This allows interlining where routes with the same color overlap.
                let mut groups: Vec<Vec<&crate::graph::LineOnEdge>> = Vec::new();
                for line in &edge.lines {
                    if let Some(last_group) = groups.last_mut() {
                        let first = last_group[0];
                        if first.color == line.color {
                            last_group.push(line);
                            continue;
                        }
                    }
                    groups.push(vec![line]);
                }

                let n_groups = groups.len();

                for (i, group) in groups.iter().enumerate() {
                    // Calculate shift based on group index, not line index
                    let shift = (i as f64 - (n_groups as f64 - 1.0) / 2.0) * spacing;

                    for line in group {
                        // Create encoder with bounding box for proper line clipping
                        // The bbox tells the MVT encoder the valid tile extent, enabling it to
                        // clip lines that extend outside the tile boundaries. Without this,
                        // points outside the tile cause encoding failures that are silently dropped.
                        let tile_bbox = BBox::new([
                            (-256.0_f64, -256.0_f64),         // Lower-left corner with buffer
                            (extent + 256.0, extent + 256.0), // Upper-right corner with buffer
                        ]);

                        // Compute cutback distances at each end based on node connectivity
                        // Following loom's approach: cutback = spacing * (n_lines - 1) / 2 + margin
                        let from_degree = graph
                            .node_to_edges
                            .get(&edge.from)
                            .map(|e| e.len())
                            .unwrap_or(1);
                        let to_degree = graph
                            .node_to_edges
                            .get(&edge.to)
                            .map(|e| e.len())
                            .unwrap_or(1);

                        // Cutback distance scales with number of lines to create room for bezier
                        let base_cutback = spacing * 2.0; // Base distance for bezier curves
                        let start_cutback = if from_degree >= 2 { base_cutback } else { 0.0 };
                        let end_cutback = if to_degree >= 2 { base_cutback } else { 0.0 };

                        // Build offset polyline with cutback applied
                        let mut offset_points: Vec<(f64, f64)> = tile_points
                            .iter()
                            .enumerate()
                            .map(|(j, (tx, ty))| {
                                let (nx, ny) = vertex_normals[j];
                                (tx + nx * shift, ty + ny * shift)
                            })
                            .collect();

                        // Apply cutback at start (shorten the polyline by moving first point forward)
                        if start_cutback > 0.0 && offset_points.len() >= 2 {
                            let (x0, y0) = offset_points[0];
                            let (x1, y1) = offset_points[1];
                            let dx = x1 - x0;
                            let dy = y1 - y0;
                            let seg_len = (dx * dx + dy * dy).sqrt();
                            if seg_len > start_cutback * 1.5 {
                                let t = start_cutback / seg_len;
                                offset_points[0] = (x0 + dx * t, y0 + dy * t);
                            }
                        }

                        // Apply cutback at end (shorten the polyline by moving last point backward)
                        if end_cutback > 0.0 && offset_points.len() >= 2 {
                            let n = offset_points.len();
                            let (x_last, y_last) = offset_points[n - 1];
                            let (x_prev, y_prev) = offset_points[n - 2];
                            let dx = x_last - x_prev;
                            let dy = y_last - y_prev;
                            let seg_len = (dx * dx + dy * dy).sqrt();
                            if seg_len > end_cutback * 1.5 {
                                let t = end_cutback / seg_len;
                                offset_points[n - 1] = (x_last - dx * t, y_last - dy * t);
                            }
                        }

                        // Encode the cutback polyline
                        let encoder_result = offset_points.iter().try_fold(
                            GeomEncoder::new(mvt::GeomType::Linestring).bbox(tile_bbox),
                            |enc, (sx, sy)| enc.point(*sx, *sy),
                        );

                        let geometry = match encoder_result.and_then(|enc| enc.encode()) {
                            Ok(g) => g,
                            Err(e) => {
                                // Log encoding errors for debugging instead of silently skipping
                                debug!("Failed to encode geometry for edge {}: {:?}", edge.id, e);
                                continue;
                            }
                        };

                        // Add feature
                        let mut feature = layer.into_feature(geometry);
                        // Create a unique ID for this specific route on the edge
                        // edge.id is i64, feature id is u64.
                        // We shift edge.id by 16 bits (allowing 65k routes per edge)
                        // We use the address of the line? No. use index in edge.lines?
                        // We need a stable-ish ID.
                        // Let's find index in edge.lines
                        let original_idx = edge
                            .lines
                            .iter()
                            .position(|l| std::ptr::eq(l, *line))
                            .unwrap_or(0);
                        let unique_id = (edge.id as u64) << 16 | (original_idx as u64);
                        feature.set_id(unique_id);
                        feature.add_tag_string("line_id", &line.line_id);
                        // Ensure single # char
                        feature.add_tag_string(
                            "color",
                            &format!("#{}", line.color.trim_start_matches('#')),
                        );
                        feature.add_tag_string("chateau_id", &line.chateau_id);
                        feature.add_tag_string("route_id", &line.route_id);
                        // Expose offset for client-side rendering if needed
                        feature.add_tag_double("offset", shift);
                        layer = feature.into_layer();
                    }
                }
            } // End of for clipped_coords loop
        } // End of for item in candidates loop

        // Stations Layer
        let mut station_layer = tile.create_layer("stations");

        let station_candidates: Vec<_> = graph
            .node_tree
            .locate_in_envelope_intersecting(&envelope)
            .collect();

        for item in station_candidates {
            let node_id = item.data;
            if let Some(node) = graph.nodes.get(&node_id) {
                let (tx, ty) = Self::project_to_tile(node.x, node.y, z, x, y, extent);

                let mut encoder = GeomEncoder::new(mvt::GeomType::Point);
                let _ = encoder.add_point(tx, ty);

                if let Ok(geometry) = encoder.encode() {
                    let mut feature = station_layer.into_feature(geometry);
                    feature.set_id(node.id as u64); // ID is positive for clusters
                    if let Some(name) = &node.name {
                        feature.add_tag_string("name", name);
                    }
                    station_layer = feature.into_layer();
                }
            }
        }

        // Generate Node Connections (Bezier Curves)
        // Uses graph.restrictions for proper partner matching
        let tile_bbox = BBox::new([(-256.0_f64, -256.0_f64), (extent + 256.0, extent + 256.0)]);
        layer = Self::generate_node_connections(graph, z, x, y, extent, layer, &tile_bbox);

        if let Err(e) = tile.add_layer(station_layer) {
            log::error!("Failed to add station layer: {}", e);
        }
        if let Err(e) = tile.add_layer(layer) {
            log::error!("Failed to add transit layer: {}", e);
        }

        tile
    }

    fn project_to_tile(lon: f64, lat: f64, z: u8, x: u32, y: u32, extent: f64) -> (f64, f64) {
        let n = 2.0f64.powi(z as i32);
        let lon_rad = lon * PI / 180.0;
        let lat_rad = lat * PI / 180.0;

        let world_x = ((lon + 180.0) / 360.0) * n;
        let world_y = (1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n;

        // Tile coordinates
        let tile_x = (world_x - x as f64) * extent;
        let tile_y = (world_y - y as f64) * extent;

        (tile_x, tile_y)
    }

    fn tile_bounds(z: u8, x: u32, y: u32, buffer_pct: f64) -> (f64, f64, f64, f64) {
        let n = 2.0f64.powi(z as i32);

        // World coordinates of tile edges (0..n)
        let wx_min = x as f64 - buffer_pct;
        let wx_max = (x + 1) as f64 + buffer_pct;
        let wy_min = y as f64 - buffer_pct;
        let wy_max = (y + 1) as f64 + buffer_pct;

        let min_lon = wx_min / n * 360.0 - 180.0;
        let max_lon = wx_max / n * 360.0 - 180.0;

        let min_lat = Self::world_y_to_lat(wy_max, n); // wy increases downwards, so max wy is min lat
        let max_lat = Self::world_y_to_lat(wy_min, n);

        // Sorting just in case
        let (real_min_lon, real_max_lon) = if min_lon < max_lon {
            (min_lon, max_lon)
        } else {
            (max_lon, min_lon)
        };
        let (real_min_lat, real_max_lat) = if min_lat < max_lat {
            (min_lat, max_lat)
        } else {
            (max_lat, min_lat)
        };

        (real_min_lon, real_min_lat, real_max_lon, real_max_lat)
    }

    fn world_y_to_lat(wy: f64, n: f64) -> f64 {
        // world_y = (1 - ln(tan + sec)/PI) / 2 * n
        // 2*wy/n = 1 - ln(...)/PI
        // ln(...) = PI * (1 - 2*wy/n)
        // tan + sec = exp(PI * (1 - 2*wy/n))
        // lat = atan(sinh(PI * (1 - 2*wy/n)))
        let val = PI * (1.0 - 2.0 * wy / n);
        val.sinh().atan() * 180.0 / PI
    }

    fn get_zoom_spacing(z: u8) -> f64 {
        match z {
            5 => 6.0,
            6 => 8.0,
            7 => 10.0,
            8..=9 => 17.0,
            10..=12 => 25.0,
            _ => 30.0,
        }
    }

    /// Get the simplification epsilon (in degrees) for a given zoom level.
    /// Returns None for high zoom levels where no simplification is needed.
    /// At lower zoom levels, more aggressive simplification creates smoother lines
    /// by removing unnecessary detail that wouldn't be visible anyway.
    fn get_simplification_epsilon(z: u8) -> Option<f64> {
        // Epsilon values in degrees (approx: 0.00001° ≈ 1.1m at equator)
        // Higher epsilon = more simplification = smoother lines
        match z {
            0..=4 => Some(0.001),  // ~111m - very aggressive for world view
            5..=6 => Some(0.0005), // ~55m - aggressive for continental view
            7..=8 => Some(0.0002), // ~22m - moderate simplification
            9 => Some(0.0001),     // ~11m - gentle simplification
            10 => Some(0.00005),   // ~5.5m - very gentle smoothing
            _ => None,             // No simplification for z >= 11
        }
    }

    /// Simplify a coordinate vector using Douglas-Peucker algorithm.
    /// This reduces the number of points while preserving the overall shape.
    fn simplify_coords(coords: &[Coord], epsilon: f64) -> Vec<Coord> {
        if coords.len() < 3 {
            return coords.to_vec();
        }

        // Convert to geo LineString, simplify, then convert back
        let line: GeoLineString<f64> = coords.iter().cloned().collect();
        let simplified = line.simplify(epsilon);
        simplified.into_inner()
    }

    /// Clip a linestring to a bounding rectangle.
    /// Returns a vector of line segments (each is a Vec<Coord>) that are inside/crossing the rect.
    /// This implements segment-by-segment clipping with interpolation at boundaries.
    fn clip_line_to_rect(coords: &[Coord], rect: &Rect) -> Vec<Vec<Coord>> {
        if coords.len() < 2 {
            return vec![];
        }

        let min_x = rect.min().x;
        let min_y = rect.min().y;
        let max_x = rect.max().x;
        let max_y = rect.max().y;

        let mut result_segments: Vec<Vec<Coord>> = Vec::new();
        let mut current_segment: Vec<Coord> = Vec::new();

        for i in 0..coords.len() - 1 {
            let p1 = coords[i];
            let p2 = coords[i + 1];

            // Clip this segment to the rect using Cohen-Sutherland approach
            if let Some((clipped_p1, clipped_p2)) =
                Self::clip_segment(p1, p2, min_x, min_y, max_x, max_y)
            {
                // Check if this continues the current segment or starts a new one
                if current_segment.is_empty() {
                    current_segment.push(clipped_p1);
                    current_segment.push(clipped_p2);
                } else {
                    // Check if clipped_p1 connects to the last point
                    let last = current_segment.last().unwrap();
                    let dist =
                        ((clipped_p1.x - last.x).powi(2) + (clipped_p1.y - last.y).powi(2)).sqrt();
                    if dist < 1e-9 {
                        // Continuous segment
                        current_segment.push(clipped_p2);
                    } else {
                        // Start new segment
                        if current_segment.len() >= 2 {
                            result_segments.push(std::mem::take(&mut current_segment));
                        } else {
                            current_segment.clear();
                        }
                        current_segment.push(clipped_p1);
                        current_segment.push(clipped_p2);
                    }
                }
            } else {
                // Segment is entirely outside - finalize current segment if any
                if current_segment.len() >= 2 {
                    result_segments.push(std::mem::take(&mut current_segment));
                } else {
                    current_segment.clear();
                }
            }
        }

        // Finalize last segment
        if current_segment.len() >= 2 {
            result_segments.push(current_segment);
        }

        result_segments
    }

    /// Clip a single line segment to a rectangle using Cohen-Sutherland algorithm.
    /// Returns Some((p1, p2)) if the segment intersects the rectangle, None otherwise.
    fn clip_segment(
        p1: Coord,
        p2: Coord,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Option<(Coord, Coord)> {
        // Outcode bits
        const INSIDE: u8 = 0;
        const LEFT: u8 = 1;
        const RIGHT: u8 = 2;
        const BOTTOM: u8 = 4;
        const TOP: u8 = 8;

        let compute_outcode = |p: Coord| -> u8 {
            let mut code = INSIDE;
            if p.x < min_x {
                code |= LEFT;
            } else if p.x > max_x {
                code |= RIGHT;
            }
            if p.y < min_y {
                code |= BOTTOM;
            } else if p.y > max_y {
                code |= TOP;
            }
            code
        };

        let mut x1 = p1.x;
        let mut y1 = p1.y;
        let mut x2 = p2.x;
        let mut y2 = p2.y;

        let mut outcode1 = compute_outcode(Coord { x: x1, y: y1 });
        let mut outcode2 = compute_outcode(Coord { x: x2, y: y2 });

        loop {
            if (outcode1 | outcode2) == 0 {
                // Both inside
                return Some((Coord { x: x1, y: y1 }, Coord { x: x2, y: y2 }));
            } else if (outcode1 & outcode2) != 0 {
                // Both on same side outside
                return None;
            } else {
                // At least one endpoint is outside
                let outcode_out = if outcode1 != 0 { outcode1 } else { outcode2 };
                let (x, y);

                if outcode_out & TOP != 0 {
                    x = x1 + (x2 - x1) * (max_y - y1) / (y2 - y1);
                    y = max_y;
                } else if outcode_out & BOTTOM != 0 {
                    x = x1 + (x2 - x1) * (min_y - y1) / (y2 - y1);
                    y = min_y;
                } else if outcode_out & RIGHT != 0 {
                    y = y1 + (y2 - y1) * (max_x - x1) / (x2 - x1);
                    x = max_x;
                } else {
                    // LEFT
                    y = y1 + (y2 - y1) * (min_x - x1) / (x2 - x1);
                    x = min_x;
                }

                if outcode_out == outcode1 {
                    x1 = x;
                    y1 = y;
                    outcode1 = compute_outcode(Coord { x: x1, y: y1 });
                } else {
                    x2 = x;
                    y2 = y;
                    outcode2 = compute_outcode(Coord { x: x2, y: y2 });
                }
            }
        }
    }

    /// Generates Bezier curve connections for lines passing through nodes
    /// Matches lines by line_id across edge pairs to find continuous routes
    fn generate_node_connections(
        graph: &RenderGraph,
        z: u8,
        x: u32,
        y: u32,
        tile_extent: f64,
        mut layer: mvt::Layer,
        tile_bbox: &BBox<f64>,
    ) -> mvt::Layer {
        let (min_lon, min_lat, max_lon, max_lat) = Self::tile_bounds(z, x, y, 0.1);
        let envelope = rstar::AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);

        // Query ALL nodes, not just clusters (node_tree only has clusters)
        // We need to check intersection nodes too
        let spacing = Self::get_zoom_spacing(z);

        if z < 7 {
            return layer;
        }

        // Iterate over all nodes that have at least 2 edges
        for (&node_id, edge_indices) in &graph.node_to_edges {
            if edge_indices.len() < 2 {
                continue;
            }

            // Check if node is within tile bounds
            let node = match graph.nodes.get(&node_id) {
                Some(n) => n,
                None => continue,
            };

            // Quick bounds check
            if node.x < min_lon || node.x > max_lon || node.y < min_lat || node.y > max_lat {
                continue;
            }

            // For each pair of edges at this node, find lines that continue across
            for (i, &edge1_idx) in edge_indices.iter().enumerate() {
                if edge1_idx >= graph.edges.len() {
                    continue;
                }
                let edge1 = &graph.edges[edge1_idx];
                if edge1.geometry.len() < 2 {
                    continue;
                }

                for &edge2_idx in edge_indices.iter().skip(i + 1) {
                    if edge2_idx >= graph.edges.len() {
                        continue;
                    }
                    let edge2 = &graph.edges[edge2_idx];
                    if edge2.geometry.len() < 2 {
                        continue;
                    }

                    // Build groups for each edge by COLOR
                    // Returns: Vec of (color, line_indices, Set<line_id>, Vec<(chateau_id, route_id)>)
                    // The route metadata is needed to check restrictions
                    let build_groups = |edge: &crate::graph::Edge| -> Vec<(
                        String,
                        Vec<usize>,
                        std::collections::HashSet<String>,
                        Vec<(String, String)>,
                    )> {
                        let mut groups: Vec<(
                            String,
                            Vec<usize>,
                            std::collections::HashSet<String>,
                            Vec<(String, String)>,
                        )> = Vec::new();
                        for (idx, line) in edge.lines.iter().enumerate() {
                            if let Some(last_group) = groups.last_mut() {
                                if last_group.0 == line.color {
                                    last_group.1.push(idx);
                                    last_group.2.insert(line.line_id.clone());
                                    last_group
                                        .3
                                        .push((line.chateau_id.clone(), line.route_id.clone()));
                                    continue;
                                }
                            }
                            let mut set = std::collections::HashSet::new();
                            set.insert(line.line_id.clone());
                            groups.push((
                                line.color.clone(),
                                vec![idx],
                                set,
                                vec![(line.chateau_id.clone(), line.route_id.clone())],
                            ));
                        }
                        groups
                    };

                    let groups1 = build_groups(edge1);
                    let groups2 = build_groups(edge2);
                    let n_groups1 = groups1.len();
                    let n_groups2 = groups2.len();

                    // Track which color-group pairs we've already drawn beziers for
                    let mut drawn_pairs: std::collections::HashSet<(usize, usize)> =
                        std::collections::HashSet::new();

                    // Match groups that have same color AND share at least one UNRESTRICTED route
                    // This fixes the "ladder" bug where routes that split at junctions were
                    // incorrectly connected across all branch combinations.
                    for (group1_idx, (color1, line_indices1, ids1, routes1)) in
                        groups1.iter().enumerate()
                    {
                        // Find matching group on edge2
                        // Must match color AND have at least one shared line_id
                        let group2_match =
                            groups2
                                .iter()
                                .enumerate()
                                .find(|(_, (color2, _, ids2, routes2))| {
                                    if color1 != color2 || ids1.is_disjoint(ids2) {
                                        return false;
                                    }
                                    // Check if at least one route is NOT restricted between these edges
                                    // A route can transition if it's NOT in the restriction map
                                    for route in routes1 {
                                        if !routes2.contains(route) {
                                            continue;
                                        }
                                        // Check if this route is restricted
                                        let is_restricted = graph
                                            .restrictions
                                            .get(&(edge1_idx, edge2_idx))
                                            .map(|r| r.contains(route))
                                            .unwrap_or(false)
                                            || graph
                                                .restrictions
                                                .get(&(edge2_idx, edge1_idx))
                                                .map(|r| r.contains(route))
                                                .unwrap_or(false);
                                        if !is_restricted {
                                            return true; // Found at least one unrestricted shared route
                                        }
                                    }
                                    false // All shared routes are restricted
                                });

                        let (group2_idx, (_, _line_indices2, _, _)) = match group2_match {
                            Some(r) => r,
                            None => continue,
                        };

                        // Skip if we've already drawn this pair
                        if !drawn_pairs.insert((group1_idx, group2_idx)) {
                            continue;
                        }

                        // Get a representative line for metadata (first line in the group)
                        let line1 = &edge1.lines[line_indices1[0]];

                        // Calculate tile-space positions and directions for each edge
                        let calc_point_and_dir =
                            |edge: &crate::graph::Edge,
                             group_idx: usize,
                             n_groups: usize|
                             -> Option<((f64, f64), (f64, f64))> {
                                if n_groups == 0 {
                                    return None;
                                }

                                // Get the geometry endpoint at the node
                                let (node_pt, other_pt) = if edge.from == node_id {
                                    let p0 = edge.geometry[0];
                                    let p1 = if edge.geometry.len() > 2 {
                                        edge.geometry[1]
                                    } else {
                                        edge.geometry[1]
                                    };
                                    (p0, p1)
                                } else {
                                    let len = edge.geometry.len();
                                    let p_end = edge.geometry[len - 1];
                                    let p_prev = if len > 2 {
                                        edge.geometry[len - 2]
                                    } else {
                                        edge.geometry[len - 2]
                                    };
                                    (p_end, p_prev)
                                };

                                // Direction pointing AWAY from node (into the edge)
                                let dx = other_pt[0] - node_pt[0];
                                let dy = other_pt[1] - node_pt[1];

                                let len_v = (dx * dx + dy * dy).sqrt();
                                if len_v < 1e-10 {
                                    return None;
                                }
                                let dir = (dx / len_v, dy / len_v);

                                // Project node position to tile space
                                let (node_tx, node_ty) =
                                    Self::project_to_tile(node.x, node.y, z, x, y, tile_extent);

                                // Project a point along the direction to get tile-space direction
                                let epsilon = 0.0001;
                                let world_along =
                                    (node.x + dir.0 * epsilon, node.y + dir.1 * epsilon);
                                let (along_tx, along_ty) = Self::project_to_tile(
                                    world_along.0,
                                    world_along.1,
                                    z,
                                    x,
                                    y,
                                    tile_extent,
                                );

                                let t_dx = along_tx - node_tx;
                                let t_dy = along_ty - node_ty;
                                let t_len = (t_dx * t_dx + t_dy * t_dy).sqrt();
                                if t_len < 1e-10 {
                                    return None;
                                }
                                // Direction in tile space pointing AWAY from node
                                let tile_dir = (t_dx / t_len, t_dy / t_len);

                                // CRITICAL: Invert slot index if edge points TOWARDS the node (edge.to == node_id)
                                // This matches C++ SvgRenderer.cpp lines 321-330:
                                // aFromInv = edge->getTo() == nd; aSlotFrom = !aFromInv ? slot : (n-1-slot)
                                let effective_idx = if edge.to == node_id {
                                    n_groups - 1 - group_idx
                                } else {
                                    group_idx
                                };

                                // Calculate offset for this GROUP
                                let shift = (effective_idx as f64 - (n_groups as f64 - 1.0) / 2.0)
                                    * spacing;
                                let norm_x = -tile_dir.1;
                                let norm_y = tile_dir.0;

                                // Cutback point
                                let cutback_dist = spacing * 2.0;
                                let p_x = node_tx + norm_x * shift + tile_dir.0 * cutback_dist;
                                let p_y = node_ty + norm_y * shift + tile_dir.1 * cutback_dist;

                                Some(((p_x, p_y), (-tile_dir.0, -tile_dir.1)))
                            };

                        let result1 = calc_point_and_dir(edge1, group1_idx, n_groups1);
                        let result2 = calc_point_and_dir(edge2, group2_idx, n_groups2);

                        let ((p1, dir1), (p2, dir2)) = match (result1, result2) {
                            (Some(r1), Some(r2)) => (r1, r2),
                            _ => continue,
                        };

                        // Validate distance
                        // Reduced max distance to prevent long incorrect connections
                        let dist = ((p1.0 - p2.0).powi(2) + (p1.1 - p2.1).powi(2)).sqrt();
                        if dist > 500.0 || dist < 0.5 {
                            continue;
                        }

                        // Generate Bezier curve
                        // Cap control distance to cutback distance to prevent bezier from
                        // extending beyond the cutback points (matching C++ loom behavior)
                        let cutback_dist = spacing * 2.0;
                        let ctrl_dist = (dist * 0.55).min(cutback_dist);
                        let cp1 = (p1.0 + dir1.0 * ctrl_dist, p1.1 + dir1.1 * ctrl_dist);
                        let cp2 = (p2.0 + dir2.0 * ctrl_dist, p2.1 + dir2.1 * ctrl_dist);

                        let points = bezier_points(p1, cp1, cp2, p2, 16);

                        let mut encoder =
                            GeomEncoder::new(mvt::GeomType::Linestring).bbox(tile_bbox.clone());
                        for pt in &points {
                            let _ = encoder.add_point(pt.0, pt.1);
                        }
                        if let Ok(geom) = encoder.encode() {
                            let mut feature = layer.into_feature(geom);
                            feature.set_id((node_id as u64).wrapping_add(
                                edge1_idx as u64 * 10000
                                    + edge2_idx as u64 * 100
                                    + group1_idx as u64,
                            ));
                            feature.add_tag_string("line_id", &line1.line_id);
                            feature.add_tag_string(
                                "color",
                                &format!("#{}", line1.color.trim_start_matches('#')),
                            );
                            feature.add_tag_string("type", "connection");
                            layer = feature.into_layer();
                        }
                    }
                }
            }
        }
        layer
    }

    // ========== Parallel Tile Generation Methods ==========

    /// Generate multiple tiles in parallel.
    /// Returns a vector of generated tiles with their coordinates.
    ///
    /// # Arguments
    /// * `graph` - The render graph containing all edge and node data
    /// * `tiles` - List of (z, x, y) tile coordinates to generate
    ///
    /// # Example
    /// ```ignore
    /// let tiles = vec![(10, 512, 512), (10, 512, 513), (10, 513, 512)];
    /// let results = TileGenerator::generate_tiles_parallel(graph, &tiles);
    /// ```
    pub fn generate_tiles_parallel(
        graph: &RenderGraph,
        tiles: &[(u8, u32, u32)],
    ) -> Vec<GeneratedTile> {
        tiles
            .par_iter()
            .map(|&(z, x, y)| {
                let tile = Self::generate_tile(graph, z, x, y);
                GeneratedTile { z, x, y, tile }
            })
            .collect()
    }

    /// Generate multiple tiles in parallel with progress tracking.
    /// The progress counter is updated atomically as tiles are completed.
    ///
    /// # Arguments
    /// * `graph` - The render graph containing all edge and node data
    /// * `tiles` - List of (z, x, y) tile coordinates to generate
    /// * `progress` - Atomic counter that tracks the number of completed tiles
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    ///
    /// let tiles = vec![(10, 512, 512), (10, 512, 513), (10, 513, 512)];
    /// let progress = Arc::new(AtomicUsize::new(0));
    /// let total = tiles.len();
    ///
    /// // In another thread, monitor progress
    /// let progress_clone = progress.clone();
    /// std::thread::spawn(move || {
    ///     loop {
    ///         let completed = progress_clone.load(Ordering::Relaxed);
    ///         println!("Progress: {}/{}", completed, total);
    ///         if completed >= total { break; }
    ///         std::thread::sleep(std::time::Duration::from_millis(100));
    ///     }
    /// });
    ///
    /// let results = TileGenerator::generate_tiles_parallel_with_progress(graph, &tiles, &progress);
    /// ```
    pub fn generate_tiles_parallel_with_progress(
        graph: &RenderGraph,
        tiles: &[(u8, u32, u32)],
        progress: &AtomicUsize,
    ) -> Vec<GeneratedTile> {
        tiles
            .par_iter()
            .map(|&(z, x, y)| {
                let tile = Self::generate_tile(graph, z, x, y);
                progress.fetch_add(1, Ordering::Relaxed);
                GeneratedTile { z, x, y, tile }
            })
            .collect()
    }

    /// Generate all tiles for a specific zoom level in parallel.
    /// Automatically determines which tiles need to be generated based on the graph's spatial index.
    ///
    /// # Arguments
    /// * `graph` - The render graph containing all edge and node data
    /// * `z` - Zoom level to generate tiles for
    ///
    /// # Returns
    /// A vector of generated tiles for the specified zoom level
    pub fn generate_tiles_for_zoom_level(graph: &RenderGraph, z: u8) -> Vec<GeneratedTile> {
        let tile_coords = Self::collect_tiles_for_zoom(graph, z);
        info!(
            "Generating {} tiles for zoom level {} in parallel",
            tile_coords.len(),
            z
        );
        Self::generate_tiles_parallel(graph, &tile_coords)
    }

    /// Generate all tiles for a zoom level range in parallel.
    /// Generates tiles for each zoom level from min_z to max_z (inclusive).
    ///
    /// # Arguments
    /// * `graph` - The render graph containing all edge and node data
    /// * `min_z` - Minimum zoom level (inclusive)
    /// * `max_z` - Maximum zoom level (inclusive)
    ///
    /// # Returns
    /// A vector of all generated tiles across all zoom levels
    pub fn generate_tiles_for_zoom_range(
        graph: &RenderGraph,
        min_z: u8,
        max_z: u8,
    ) -> Vec<GeneratedTile> {
        // Collect all tile coordinates for all zoom levels
        let all_tiles: Vec<(u8, u32, u32)> = (min_z..=max_z)
            .flat_map(|z| Self::collect_tiles_for_zoom(graph, z))
            .collect();

        info!(
            "Generating {} total tiles for zoom levels {}-{} in parallel",
            all_tiles.len(),
            min_z,
            max_z
        );

        Self::generate_tiles_parallel(graph, &all_tiles)
    }

    /// Generate all tiles for a zoom level range with progress tracking.
    ///
    /// # Arguments
    /// * `graph` - The render graph containing all edge and node data
    /// * `min_z` - Minimum zoom level (inclusive)
    /// * `max_z` - Maximum zoom level (inclusive)
    /// * `progress` - Atomic counter that tracks the number of completed tiles
    ///
    /// # Returns
    /// A tuple of (total tile count, vector of generated tiles)
    pub fn generate_tiles_for_zoom_range_with_progress(
        graph: &RenderGraph,
        min_z: u8,
        max_z: u8,
        progress: &AtomicUsize,
    ) -> (usize, Vec<GeneratedTile>) {
        let all_tiles: Vec<(u8, u32, u32)> = (min_z..=max_z)
            .flat_map(|z| Self::collect_tiles_for_zoom(graph, z))
            .collect();

        let total = all_tiles.len();
        info!(
            "Generating {} total tiles for zoom levels {}-{} in parallel",
            total, min_z, max_z
        );

        let tiles = Self::generate_tiles_parallel_with_progress(graph, &all_tiles, progress);
        (total, tiles)
    }

    /// Collect all tile coordinates that need to be generated for a zoom level.
    /// Uses the graph's spatial index to determine which tiles contain data.
    ///
    /// # Arguments
    /// * `graph` - The render graph containing all edge and node data
    /// * `z` - Zoom level
    ///
    /// # Returns
    /// A vector of (z, x, y) tile coordinates
    pub fn collect_tiles_for_zoom(graph: &RenderGraph, z: u8) -> Vec<(u8, u32, u32)> {
        let mut tiles: HashSet<(u32, u32)> = HashSet::new();

        // Query the spatial index to find all edges
        for item in graph.tree.iter() {
            let bounds = item.geom();
            let min_pt = bounds.lower();
            let max_pt = bounds.upper();

            // Convert lat/lon to tile coordinates
            let (min_tx, min_ty) = Self::latlon_to_tile(min_pt[1], min_pt[0], z);
            let (max_tx, max_ty) = Self::latlon_to_tile(max_pt[1], max_pt[0], z);

            // Ensure we iterate from smaller to larger tile coordinates
            let (tx_start, tx_end) = (min_tx.min(max_tx), min_tx.max(max_tx));
            let (ty_start, ty_end) = (min_ty.min(max_ty), min_ty.max(max_ty));

            for x in tx_start..=tx_end {
                for y in ty_start..=ty_end {
                    tiles.insert((x, y));
                }
            }
        }

        tiles.into_iter().map(|(x, y)| (z, x, y)).collect()
    }

    /// Convert latitude/longitude to tile coordinates.
    fn latlon_to_tile(lat: f64, lon: f64, z: u8) -> (u32, u32) {
        let n = 2.0f64.powi(z as i32);
        let x = ((lon + 180.0) / 360.0 * n).floor() as u32;
        let lat_rad = lat.to_radians();
        let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n).floor() as u32;
        (x, y)
    }

    /// Set the thread pool size for parallel operations.
    /// This affects all subsequent parallel tile generation calls.
    ///
    /// Note: This function should be called before starting tile generation.
    /// Calling it during generation may have undefined behavior.
    ///
    /// # Arguments
    /// * `num_threads` - Number of threads to use for parallel operations
    ///
    /// # Returns
    /// Result indicating success or failure of thread pool initialization
    pub fn set_thread_pool_size(num_threads: usize) -> Result<(), rayon::ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
    }
}

// Bezier Curve Logic
#[derive(Clone, Copy, Debug)]
struct BezierControl {
    p: (f64, f64),
    q: (f64, f64),
    cp1: (f64, f64),
    cp2: (f64, f64),
}

impl BezierControl {
    // k = 4/3 * (sqrt(2) - 1) approx 0.55228474
    const K: f64 = 0.55228474;

    fn new(p: (f64, f64), q: (f64, f64), dir_p: (f64, f64), dir_q: (f64, f64)) -> Self {
        let (px, py) = p;
        let (qx, qy) = q;
        let (dx_p, dy_p) = dir_p;
        let (dx_q, dy_q) = dir_q;

        let len_p = (dx_p * dx_p + dy_p * dy_p).sqrt();
        let len_q = (dx_q * dx_q + dy_q * dy_q).sqrt();
        let dp = if len_p > 0.0 {
            (dx_p / len_p, dy_p / len_p)
        } else {
            (0.0, 0.0)
        };
        let dq = if len_q > 0.0 {
            (dx_q / len_q, dy_q / len_q)
        } else {
            (0.0, 0.0)
        };

        // Intersection of P + t*dp and Q + s*dq
        let det = dp.1 * dq.0 - dp.0 * dq.1;
        let diff_x = qx - px;
        let diff_y = qy - py;

        let dist = (diff_x * diff_x + diff_y * diff_y).sqrt();

        // If det close to 0, parallel
        if det.abs() < 1e-4 {
            // Just straight line or simple curve
            // The paper says: "if t_euA and t_fuA do not intersect... simple placement"
            let offset = Self::K * dist;
            // But which direction? projected?
            // Fallback: cp1 = p + dp*offset, cp2 = q + dq*offset
            let cp1 = (px + dp.0 * offset * 0.5, py + dp.1 * offset * 0.5);
            let cp2 = (qx + dq.0 * offset * 0.5, qy + dq.1 * offset * 0.5);
            return Self { p, q, cp1, cp2 };
        }

        let t = (diff_x * (-dq.1) - diff_y * (-dq.0)) / det;
        // let s = (dp.0 * diff_y - dp.1 * diff_x) / det;

        let i_x = px + t * dp.0;
        let i_y = py + t * dp.1;

        // If intersection is "behind" both (t < 0?), then we might have parallel-ish anti-aligned?
        // But we trust the geometry for now.

        let dist_pi = ((i_x - px).powi(2) + (i_y - py).powi(2)).sqrt();
        let dist_qi = ((i_x - qx).powi(2) + (i_y - qy).powi(2)).sqrt();

        // Handle "magic number" logic
        // if |p-i| == |q-i| then delta = |p-i|
        let delta = (dist_pi + dist_qi) / 2.0;

        // We use K * delta as distance from P/Q to CP
        let offset = Self::K * delta;

        // CP1 is along dp
        let cp1 = (px + dp.0 * offset, py + dp.1 * offset);
        // CP2 is along dq
        let cp2 = (qx + dq.0 * offset, qy + dq.1 * offset);

        Self { p, q, cp1, cp2 }
    }

    fn points(&self, steps: usize) -> Vec<(f64, f64)> {
        let mut pts = Vec::with_capacity(steps + 1);
        for i in 0..=steps {
            let t = i as f64 / steps as f64;
            let inv_t = 1.0 - t;

            let b0 = inv_t * inv_t * inv_t;
            let b1 = 3.0 * inv_t * inv_t * t;
            let b2 = 3.0 * inv_t * t * t;
            let b3 = t * t * t;

            let x = b0 * self.p.0 + b1 * self.cp1.0 + b2 * self.cp2.0 + b3 * self.q.0;
            let y = b0 * self.p.1 + b1 * self.cp1.1 + b2 * self.cp2.1 + b3 * self.q.1;
            pts.push((x, y));
        }
        pts
    }
}

/// Generate cubic bezier curve points
/// p0: start point, cp1: control point 1, cp2: control point 2, p1: end point
fn bezier_points(
    p0: (f64, f64),
    cp1: (f64, f64),
    cp2: (f64, f64),
    p1: (f64, f64),
    steps: usize,
) -> Vec<(f64, f64)> {
    let mut pts = Vec::with_capacity(steps + 1);
    for i in 0..=steps {
        let t = i as f64 / steps as f64;
        let inv_t = 1.0 - t;

        let b0 = inv_t * inv_t * inv_t;
        let b1 = 3.0 * inv_t * inv_t * t;
        let b2 = 3.0 * inv_t * t * t;
        let b3 = t * t * t;

        let x = b0 * p0.0 + b1 * cp1.0 + b2 * cp2.0 + b3 * p1.0;
        let y = b0 * p0.1 + b1 * cp1.1 + b2 * cp2.1 + b3 * p1.1;
        pts.push((x, y));
    }
    pts
}
