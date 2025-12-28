use crate::graph::{LineOnEdge, RenderGraph};
use geo::{Coord, Rect};
use log::{debug, info};
use mvt::{GeomData, GeomEncoder, Tile};
use pointy::BBox;
use std::f64::consts::PI;

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

            // Shorten geometry at junction nodes (nodes with 2+ edges)
            // This creates gaps that are filled by Bezier curves (Figure 6.1 step 3)
            let cutback_distance = 0.00015; // approx 15 meters in degrees at mid-latitudes

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

            let mut working_coords: Vec<[f64; 2]> = edge.geometry.clone();

            // Cutback at start (from node) if it's a junction
            if from_degree >= 2 && working_coords.len() >= 2 {
                let p0 = working_coords[0];
                let p1 = working_coords[1];
                let dx = p1[0] - p0[0];
                let dy = p1[1] - p0[1];
                let seg_len = (dx * dx + dy * dy).sqrt();
                if seg_len > cutback_distance * 1.5 {
                    // Shorten from start
                    let t = cutback_distance / seg_len;
                    working_coords[0] = [p0[0] + dx * t, p0[1] + dy * t];
                }
            }

            // Cutback at end (to node) if it's a junction
            if to_degree >= 2 && working_coords.len() >= 2 {
                let n = working_coords.len();
                let p_last = working_coords[n - 1];
                let p_prev = working_coords[n - 2];
                let dx = p_last[0] - p_prev[0];
                let dy = p_last[1] - p_prev[1];
                let seg_len = (dx * dx + dy * dy).sqrt();
                if seg_len > cutback_distance * 1.5 {
                    // Shorten from end
                    let t = cutback_distance / seg_len;
                    working_coords[n - 1] = [p_last[0] - dx * t, p_last[1] - dy * t];
                }
            }

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

                // Project clipped points to tile coordinates
                let tile_points: Vec<(f64, f64)> = clipped_coords
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

                // Group lines to support interlining (specifically for nyct)
                // Adjacent lines with same color and chateau='nyct' share the same offset slot.
                let mut groups: Vec<Vec<&crate::graph::LineOnEdge>> = Vec::new();
                for line in &edge.lines {
                    if line.chateau_id == "nyct" {
                        if let Some(last_group) = groups.last_mut() {
                            let first = last_group[0];
                            if first.chateau_id == "nyct" && first.color == line.color {
                                last_group.push(line);
                                continue;
                            }
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

                        // Use the fluent API which properly propagates errors
                        let encoder_result = tile_points.iter().enumerate().try_fold(
                            GeomEncoder::new(mvt::GeomType::Linestring).bbox(tile_bbox),
                            |enc, (j, (tx, ty))| {
                                let (nx, ny) = vertex_normals[j];
                                let sx = tx + nx * shift;
                                let sy = ty + ny * shift;
                                enc.point(sx, sy)
                            },
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
            5..=7 => 15.0,
            8..=9 => 20.0,
            10..=12 => 25.0,
            _ => 30.0,
        }
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
    /// Uses graph.restrictions to find valid partner pairs (edges where routes are allowed to connect)
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

        let nodes_in_tile: Vec<_> = graph
            .node_tree
            .locate_in_envelope_intersecting(&envelope)
            .collect();

        let spacing = Self::get_zoom_spacing(z);

        for item in nodes_in_tile {
            let node_id = item.data;
            if let Some(node) = graph.nodes.get(&node_id) {
                // Get all edges connected to this node
                if let Some(edge_indices) = graph.node_to_edges.get(&node_id) {
                    if edge_indices.len() < 2 {
                        continue;
                    }

                    // For each pair of edges at this node, check if there are allowed connections
                    for &edge1_idx in edge_indices {
                        for &edge2_idx in edge_indices {
                            if edge1_idx >= edge2_idx {
                                continue; // Skip self and avoid duplicate pairs
                            }

                            // Check if there are allowed routes from edge1 to edge2
                            let allowed_routes: Vec<(String, String)> = {
                                let mut routes = Vec::new();
                                if let Some(r) = graph.restrictions.get(&(edge1_idx, edge2_idx)) {
                                    routes.extend(r.iter().cloned());
                                }
                                if let Some(r) = graph.restrictions.get(&(edge2_idx, edge1_idx)) {
                                    routes.extend(r.iter().cloned());
                                }
                                routes
                            };

                            if allowed_routes.is_empty() {
                                continue;
                            }

                            let edge1 = &graph.edges[edge1_idx];
                            let edge2 = &graph.edges[edge2_idx];

                            if edge1.geometry.len() < 2 || edge2.geometry.len() < 2 {
                                continue;
                            }

                            // For each allowed route, find the line on each edge and generate curve
                            for (route_chateau, route_id) in &allowed_routes {
                                // Find line position on edge1
                                let line1_pos = edge1.lines.iter().position(|l| {
                                    &l.chateau_id == route_chateau && &l.route_id == route_id
                                });
                                let line2_pos = edge2.lines.iter().position(|l| {
                                    &l.chateau_id == route_chateau && &l.route_id == route_id
                                });

                                let (line1_idx, line2_idx) = match (line1_pos, line2_pos) {
                                    (Some(i1), Some(i2)) => (i1, i2),
                                    _ => continue, // Route not on both edges
                                };

                                let line1 = &edge1.lines[line1_idx];

                                // Calculate tile-space positions and directions for each edge
                                let calc_point_and_dir = |edge: &crate::graph::Edge, line_idx: usize| -> Option<((f64, f64), (f64, f64))> {
                                    let n_lines = edge.lines.len();
                                    if n_lines == 0 {
                                        return None;
                                    }

                                    // Direction pointing AWAY from node (into edge)
                                    let (dx, dy) = if edge.from == node_id {
                                        let p0 = edge.geometry[0];
                                        let p1 = edge.geometry[1];
                                        (p1[0] - p0[0], p1[1] - p0[1])
                                    } else {
                                        let len = edge.geometry.len();
                                        let p_end = edge.geometry[len - 1];
                                        let p_prev = edge.geometry[len - 2];
                                        (p_prev[0] - p_end[0], p_prev[1] - p_end[1])
                                    };

                                    let len_v = (dx * dx + dy * dy).sqrt();
                                    if len_v < 1e-10 {
                                        return None;
                                    }
                                    let dir = (dx / len_v, dy / len_v);

                                    // Project to tile space
                                    let (node_tx, node_ty) = Self::project_to_tile(node.x, node.y, z, x, y, tile_extent);
                                    let epsilon = 0.0001;
                                    let (h_tx, h_ty) = Self::project_to_tile(
                                        node.x + dir.0 * epsilon,
                                        node.y + dir.1 * epsilon,
                                        z, x, y, tile_extent,
                                    );

                                    let t_dx = h_tx - node_tx;
                                    let t_dy = h_ty - node_ty;
                                    let t_len = (t_dx * t_dx + t_dy * t_dy).sqrt();
                                    if t_len < 1e-10 {
                                        return None;
                                    }
                                    let (tn_x, tn_y) = (t_dx / t_len, t_dy / t_len);

                                    // Calculate offset for this line
                                    let shift = (line_idx as f64 - (n_lines as f64 - 1.0) / 2.0) * spacing;
                                    let norm_x = -tn_y;
                                    let norm_y = tn_x;

                                    // Point with offset
                                    let start_x = node_tx + norm_x * shift;
                                    let start_y = node_ty + norm_y * shift;

                                    // Shorten slightly into edge
                                    let shorten_dist = 15.0;
                                    let p_x = start_x + tn_x * shorten_dist;
                                    let p_y = start_y + tn_y * shorten_dist;

                                    Some(((p_x, p_y), (tn_x, tn_y)))
                                };

                                let result1 = calc_point_and_dir(edge1, line1_idx);
                                let result2 = calc_point_and_dir(edge2, line2_idx);

                                let ((p1, dir1), (p2, dir2)) = match (result1, result2) {
                                    (Some(r1), Some(r2)) => (r1, r2),
                                    _ => continue,
                                };

                                // Validate distance
                                let dist_sq = (p1.0 - p2.0).powi(2) + (p1.1 - p2.1).powi(2);
                                if dist_sq > 100.0 * 100.0 || dist_sq < 1.0 {
                                    continue;
                                }

                                // Generate Bezier curve
                                // Directions point AWAY from node, so negate for curve tangents
                                let d_p = (-dir1.0, -dir1.1);
                                let d_q = (-dir2.0, -dir2.1);

                                let curve = BezierControl::new(p1, p2, d_p, d_q);
                                let points = curve.points(10);

                                // Encode
                                let mut encoder = GeomEncoder::new(mvt::GeomType::Linestring)
                                    .bbox(tile_bbox.clone());
                                for pt in points {
                                    let _ = encoder.add_point(pt.0, pt.1);
                                }
                                if let Ok(geom) = encoder.encode() {
                                    let mut feature = layer.into_feature(geom);
                                    feature.set_id(
                                        (node_id as u64).wrapping_add(
                                            edge1_idx as u64 * 1000 + edge2_idx as u64,
                                        ),
                                    );
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
            }
        }
        layer
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
