use crate::graph::{LineOnEdge, RenderGraph};
use geo::{LineString, Point};
use log::{debug, info};
use mvt::{GeomData, GeomEncoder, Tile};
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

            // Project all points to tile coordinates
            let mut tile_points_raw = Vec::with_capacity(edge.geometry.len());
            for p in &edge.geometry {
                let (tx, ty) = Self::project_to_tile(p[0], p[1], z, x, y, extent);
                tile_points_raw.push(geo::Coord { x: tx, y: ty });
            }

            let line_string = geo::LineString::new(tile_points_raw);
            // Simplify with epsilon relative to tile resolution.
            // 4096 extent. 1px is 1 unit? Usually 4096 is high res.
            // 256 or 512 is display size. So 1px ≈ 4096/256 = 16 units?
            // If we assume standard 256px tiles, and 4096 extent, then 1px = 16 units.
            // Let's use 10.0 units as epsilon (approx 0.6px).
            use geo::Simplify;
            let simplified = line_string.simplify(10.0);

            let tile_points: Vec<(f64, f64)> =
                simplified.0.into_iter().map(|c| (c.x, c.y)).collect();

            if tile_points.len() < 2 {
                continue;
            }

            // Log first edge details for debugging (sampling)
            if edge_idx % 1000 == 0 {
                debug!(
                    "Edge {}: {} points. First point projected: {:?}",
                    edge_idx,
                    tile_points.len(),
                    tile_points[0]
                );
            }

            // Simple culling check (bounding box of tile points)
            let min_tx = tile_points
                .iter()
                .map(|p| p.0)
                .fold(f64::INFINITY, f64::min);
            let max_tx = tile_points
                .iter()
                .map(|p| p.0)
                .fold(f64::NEG_INFINITY, f64::max);
            let min_ty = tile_points
                .iter()
                .map(|p| p.1)
                .fold(f64::INFINITY, f64::min);
            let max_ty = tile_points
                .iter()
                .map(|p| p.1)
                .fold(f64::NEG_INFINITY, f64::max);

            let buf = 100.0; // Reasonable buffer in tile coordinates
            if max_tx < -buf || min_tx > extent + buf || max_ty < -buf || min_ty > extent + buf {
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
            let n_lines = edge.lines.len();

            for (i, line) in edge.lines.iter().enumerate() {
                // Calculate shift
                let shift = (i as f64 - (n_lines as f64 - 1.0) / 2.0) * spacing;

                let mut encoder = GeomEncoder::new(mvt::GeomType::Linestring);

                for (j, (tx, ty)) in tile_points.iter().enumerate() {
                    let (nx, ny) = vertex_normals[j];
                    let sx = tx + nx * shift;
                    let sy = ty + ny * shift;
                    let _ = encoder.add_point(sx, sy);
                }

                let geometry = match encoder.encode() {
                    Ok(g) => g,
                    Err(_) => continue,
                };

                // Add feature
                let mut feature = layer.into_feature(geometry);
                // Create a unique ID for this specific route on the edge
                // edge.id is i64, feature id is u64.
                // We shift edge.id by 16 bits (allowing 65k routes per edge)
                let unique_id = (edge.id as u64) << 16 | (i as u64);
                feature.set_id(unique_id);
                feature.add_tag_string("line_id", &line.line_id);
                feature.add_tag_string("color", &format!("#{}", line.color));
                feature.add_tag_string("chateau_id", &line.chateau_id);
                feature.add_tag_string("route_id", &line.route_id);
                layer = feature.into_layer();
            }
        }

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
            0..=9 => 60.0,
            10..=12 => 45.0,
            _ => 30.0,
        }
    }
}
