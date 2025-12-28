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

            // Build the source linestring from edge geometry (lon, lat)
            let source_coords: Vec<Coord> = edge
                .geometry
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
}
