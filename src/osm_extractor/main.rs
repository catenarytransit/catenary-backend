use anyhow::{Context, Result};
use clap::Parser;
use reqwest::Client;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Geofabrik region string (e.g., "north-america/us/california")
    #[arg(short, long)]
    region: String,

    /// Output directory for chunks
    #[arg(short, long, default_value = "osm_chunks")]
    output_dir: PathBuf,

    /// Temporary directory for downloads
    #[arg(short, long, default_value = "temp_osm")]
    temp_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    println!("Starting OSM Extractor for region: {}", args.region);

    // Create directories
    tokio::fs::create_dir_all(&args.output_dir)
        .await
        .context("Failed to create output dir")?;
    tokio::fs::create_dir_all(&args.temp_dir)
        .await
        .context("Failed to create temp dir")?;

    let client = Client::new();

    // 1. Download KML Boundary
    let kml_url = format!("https://download.geofabrik.de/{}.kml", args.region);
    let kml_path = args.temp_dir.join("boundary.kml");
    download_file(&client, &kml_url, &kml_path).await?;

    // 2. Download OSM PBF
    let pbf_url = format!(
        "https://download.geofabrik.de/{}-latest.osm.pbf",
        args.region
    );
    let pbf_path = args.temp_dir.join("data.osm.pbf");
    download_file(&client, &pbf_url, &pbf_path).await?;

    println!("Downloads complete. Processing...");

    // 3. Parse KML to get polygon
    println!("Parsing KML boundary...");
    let kml_content = std::fs::read_to_string(&kml_path)?;
    let boundary_geom = parse_kml_boundary(&kml_content)?;

    // 4. Determine covering tiles (Z10)
    let zoom_level = 10;
    println!("Calculating covering tiles at Z{}...", zoom_level);
    let tiles = calculate_covering_tiles(&boundary_geom, zoom_level);
    println!("Found {} tiles covering the region.", tiles.len());

    // 5. Stream PBF and assign to tiles
    println!("Processing OSM PBF...");
    let file = File::open(&pbf_path)?;
    let mut reader = osmpbfreader::OsmPbfReader::new(file);

    println!("Building Node Index...");
    // We'll use a simple HashMap for now, assuming sufficient RAM.
    // If OOM, we'll switch to a disk-backed solution.
    let mut node_coords = ahash::AHashMap::new();

    for obj in reader.iter() {
        if let Ok(osmpbfreader::OsmObj::Node(node)) = obj {
            node_coords.insert(node.id.0, (node.lat(), node.lon()));
        }
    }
    println!("Indexed {} nodes.", node_coords.len());

    // Reset reader
    let file = File::open(&pbf_path)?;
    let mut reader = osmpbfreader::OsmPbfReader::new(file);

    let mut chunk_buffers: std::collections::HashMap<(u32, u32), ChunkBuffer> =
        std::collections::HashMap::new();

    println!("Distributing ways to chunks...");
    for obj in reader.iter() {
        match obj {
            Ok(osmpbfreader::OsmObj::Way(way)) => {
                // Find bounding box of the way to determine which tiles it touches
                let mut min_x = u32::MAX;
                let mut max_x = u32::MIN;
                let mut min_y = u32::MAX;
                let mut max_y = u32::MIN;

                let mut found_coords = false;

                for node_id in &way.nodes {
                    if let Some(&(lat, lon)) = node_coords.get(&node_id.0) {
                        let (tx, ty) = lon_lat_to_tile(lon, lat, zoom_level);
                        min_x = min_x.min(tx);
                        max_x = max_x.max(tx);
                        min_y = min_y.min(ty);
                        max_y = max_y.max(ty);
                        found_coords = true;
                    }
                }

                if found_coords {
                    // Add way to all overlapping tiles
                    for x in min_x..=max_x {
                        for y in min_y..=max_y {
                            chunk_buffers
                                .entry((x, y))
                                .or_insert_with(|| ChunkBuffer {
                                    nodes: Vec::new(),
                                    ways: Vec::new(),
                                })
                                .ways
                                .push(way.clone());
                        }
                    }
                }
            }
            Ok(osmpbfreader::OsmObj::Node(_)) => {
                // Skip nodes in this pass
            }
            _ => {}
        }
    }

    println!("Populating nodes for chunks...");
    for ((_x, _y), buffer) in chunk_buffers.iter_mut() {
        let mut nodes_needed = ahash::AHashSet::new();
        for way in &buffer.ways {
            for node_id in &way.nodes {
                nodes_needed.insert(node_id.0);
            }
        }

        for node_id in nodes_needed {
            if let Some(&(lat, lon)) = node_coords.get(&node_id) {
                let node = osmpbfreader::Node {
                    id: osmpbfreader::NodeId(node_id),
                    decimicro_lat: (lat * 1e7) as i32,
                    decimicro_lon: (lon * 1e7) as i32,
                    tags: osmpbfreader::Tags::new(),
                };
                buffer.nodes.push(node);
            }
        }
    }

    // 6. Save Chunks
    println!("Saving {} chunks...", chunk_buffers.len());
    for ((x, y), buffer) in chunk_buffers {
        let chunk_path = args
            .output_dir
            .join(format!("chunk_{}_{}_{}.pbf", x, y, zoom_level));
        let street_data = convert_to_street_data(buffer, x, y, zoom_level)?;
        catenary::routing_common::osm_graph::save_pbf(&street_data, chunk_path.to_str().unwrap())?;
    }

    Ok(())
}

pub struct ChunkBuffer {
    nodes: Vec<osmpbfreader::Node>,
    ways: Vec<osmpbfreader::Way>,
}

fn convert_to_street_data(
    buffer: ChunkBuffer,
    x: u32,
    y: u32,
    z: u8,
) -> Result<catenary::routing_common::osm_graph::StreetData> {
    use catenary::routing_common::osm_graph::{
        Edge, Geometry, Node, StreetData, edge_flags, permissions,
    };
    use geo::HaversineDistance;
    use geo::prelude::*;
    use std::collections::HashMap;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut geometries = Vec::new();

    // We need a local mapping from OSM Node ID to local index (0..N)
    let mut osm_id_to_idx = HashMap::new();

    // 1. Process Nodes
    // StreetData expects nodes to be sorted by NodeID (0 to N) for the local graph.
    // This implies the vector index IS the NodeID in the local graph.
    let mut sorted_nodes = buffer.nodes;
    sorted_nodes.sort_by_key(|n| n.id.0);

    for (i, osm_node) in sorted_nodes.iter().enumerate() {
        osm_id_to_idx.insert(osm_node.id.0, i as u32);

        nodes.push(Node {
            lat: osm_node.lat(),
            lon: osm_node.lon(),
            elevation: 0.0,    // TODO: Fetch elevation
            first_edge_idx: 0, // Filled later
        });
    }

    // 2. Process Ways (Edges)
    let mut edges_per_node: Vec<Vec<Edge>> = vec![Vec::new(); nodes.len()];

    // Tile bounds for boundary check
    let (w, s, e, n) = tile_bbox(x, y, z);

    for way in buffer.ways {
        for i in 0..way.nodes.len() - 1 {
            let u_id = way.nodes[i].0;
            let v_id = way.nodes[i + 1].0;

            if let (Some(&u_idx), Some(&v_idx)) =
                (osm_id_to_idx.get(&u_id), osm_id_to_idx.get(&v_id))
            {
                let u_node = &nodes[u_idx as usize];
                let v_node = &nodes[v_idx as usize];

                // Calculate distance
                let p1 = geo::Point::new(u_node.lon, u_node.lat);
                let p2 = geo::Point::new(v_node.lon, v_node.lat);
                let dist_meters = p1.haversine_distance(&p2);
                let dist_mm = (dist_meters * 1000.0) as u32;

                // Check boundary flags
                // If either node is outside the tile, mark as border edge.
                let mut flags = 0;
                let u_outside =
                    u_node.lon < w || u_node.lon > e || u_node.lat < s || u_node.lat > n;
                let v_outside =
                    v_node.lon < w || v_node.lon > e || v_node.lat < s || v_node.lat > n;

                if u_outside || v_outside {
                    flags |= edge_flags::EDGE_FLAG_BORDER;
                }

                // Store geometry
                let geom_idx = geometries.len() as u32;
                geometries.push(Geometry {
                    coords: vec![
                        u_node.lat as f32,
                        u_node.lon as f32,
                        v_node.lat as f32,
                        v_node.lon as f32,
                    ],
                });

                let edge = Edge {
                    target_node: v_idx,
                    distance_mm: dist_mm,
                    geometry_id: geom_idx,
                    permissions: permissions::WALK_FWD | permissions::CYCL_FWD,
                    surface_type: 0,
                    grade_percent: 0,
                    flags,
                };

                edges_per_node[u_idx as usize].push(edge);

                // Bidirectional
                let edge_rev = Edge {
                    target_node: u_idx,
                    distance_mm: dist_mm,
                    geometry_id: 0, // Placeholder, will be updated
                    permissions: permissions::WALK_FWD | permissions::CYCL_FWD,
                    surface_type: 0,
                    grade_percent: 0,
                    flags,
                };

                let geom_rev_idx = geometries.len() as u32;
                geometries.push(Geometry {
                    coords: vec![
                        v_node.lat as f32,
                        v_node.lon as f32,
                        u_node.lat as f32,
                        u_node.lon as f32,
                    ],
                });

                // Update reverse edge geometry_id
                let mut edge_rev = edge_rev;
                edge_rev.geometry_id = geom_rev_idx;

                edges_per_node[v_idx as usize].push(edge_rev);
            }
        }
    }

    // Flatten edges and update nodes
    let mut current_edge_idx = 0;
    for (i, node_edges) in edges_per_node.iter_mut().enumerate() {
        nodes[i].first_edge_idx = current_edge_idx;
        edges.append(node_edges);
        current_edge_idx += node_edges.len() as u32;
    }

    Ok(StreetData {
        nodes,
        edges,
        geometries,
        partition_id: 0, // TODO: Encode x,y,z into ID?
        boundary_nodes: Vec::new(),
    })
}

fn parse_kml_boundary(content: &str) -> Result<geo::MultiPolygon<f64>> {
    // Simple regex-based extraction for now, or use a proper KML parser if available.
    // The `geofabrik-handler` crate might have something, but let's check `Cargo.toml`.
    // It has `geofabrik-handler`, let's see if we can use it.
    // Actually, let's just use `quick-xml` or similar if we want to be robust,
    // but for Geofabrik KMLs, they are usually simple.
    // Let's try to find coordinates in <coordinates> tags.

    // A quick hacky parser for Geofabrik KMLs which usually contain a MultiGeometry or Polygon.
    // Format: <coordinates>lon,lat ...</coordinates>

    let mut polygons = Vec::new();

    // This is a very naive parser. In a real production system, use a proper XML parser.
    // For this task, I'll assume standard Geofabrik format.
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with("<coordinates>") && line.ends_with("</coordinates>") {
            let coords_str = &line[13..line.len() - 14];
            let mut exterior = Vec::new();
            for coord_pair in coords_str.split(' ') {
                let parts: Vec<&str> = coord_pair.split(',').collect();
                if parts.len() >= 2 {
                    if let (Ok(lon), Ok(lat)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                        exterior.push((lon, lat));
                    }
                }
            }
            if !exterior.is_empty() {
                polygons.push(geo::Polygon::new(geo::LineString::from(exterior), vec![]));
            }
        }
    }

    if polygons.is_empty() {
        anyhow::bail!("No polygons found in KML");
    }

    Ok(geo::MultiPolygon::new(polygons))
}

fn calculate_covering_tiles(boundary: &geo::MultiPolygon<f64>, zoom: u8) -> Vec<(u32, u32)> {
    use geo::prelude::*;

    // Bounding box of the region
    let bbox = boundary
        .bounding_rect()
        .expect("Boundary has no bounding box");

    let (min_x, min_y) = lon_lat_to_tile(bbox.min().x, bbox.max().y, zoom); // Top-Left
    let (max_x, max_y) = lon_lat_to_tile(bbox.max().x, bbox.min().y, zoom); // Bottom-Right

    let mut tiles = Vec::new();

    for x in min_x..=max_x {
        for y in min_y..=max_y {
            // Check if tile intersects the boundary
            // This is an optimization: only keep tiles that actually overlap the polygon.
            // For a simple box, we take all. For complex shapes, we might want to filter.
            // For now, let's just take the bounding box tiles to be safe and simple.
            tiles.push((x, y));
        }
    }

    tiles
}

// Math helpers
fn lon_lat_to_tile(lon: f64, lat: f64, zoom: u8) -> (u32, u32) {
    let n = 2.0_f64.powi(zoom as i32);
    let x = ((lon + 180.0) / 360.0 * n).floor() as u32;
    let lat_rad = lat.to_radians();
    let y = ((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n).floor() as u32;
    (x, y)
}

fn tile_bbox(x: u32, y: u32, zoom: u8) -> (f64, f64, f64, f64) {
    let n = 2.0_f64.powi(zoom as i32);
    let w = x as f64 / n * 360.0 - 180.0;
    let e = (x + 1) as f64 / n * 360.0 - 180.0;

    fn tile_y_to_lat(y: u32, n: f64) -> f64 {
        let lat_rad = (std::f64::consts::PI * (1.0 - 2.0 * y as f64 / n))
            .sinh()
            .atan();
        lat_rad.to_degrees()
    }

    let n_lat = tile_y_to_lat(y, n);
    let s_lat = tile_y_to_lat(y + 1, n);

    (w, s_lat, e, n_lat)
}

async fn download_file(client: &Client, url: &str, path: &PathBuf) -> Result<()> {
    if path.exists() {
        println!("File {:?} already exists, skipping download.", path);
        return Ok(());
    }

    println!("Downloading {} to {:?}", url, path);
    let response = client.get(url).send().await?.error_for_status()?;
    let content = response.bytes().await?;

    let mut file = File::create(path)?;
    file.write_all(&content)?;
    println!("Downloaded {}", url);
    Ok(())
}
