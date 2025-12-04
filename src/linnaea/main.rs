// cargo run --bin linnaea --release -- --input output --output transit_viz.geojson

use catenary::routing_common::transit_graph::{
    EdgeType, GlobalPatternIndex, Manifest, TransitPartition,
};
use clap::Parser;
use geojson::{Feature, FeatureCollection, Geometry, JsonObject, Value};

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input folder containing transit_chunk_*.bincode files
    #[arg(short, long)]
    input: PathBuf,

    /// Output GeoJSON file
    #[arg(short, long)]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut features = Vec::new();
    let mut partitions: Vec<TransitPartition> = Vec::new();
    let mut global_patterns: Option<GlobalPatternIndex> = None;
    let mut manifest: Option<Manifest> = None;

    println!("Reading files from {:?}", args.input);

    let mut hub_count = 0;
    let entries = std::fs::read_dir(&args.input)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
            if filename.starts_with("transit_chunk_") && filename.ends_with(".bincode") {
                println!("Loading partition: {}", filename);
                let partition: TransitPartition =
                    catenary::routing_common::transit_graph::load_bincode(path.to_str().unwrap())?;
                partitions.push(partition);
            } else if filename == "global_patterns.bincode" {
                println!("Loading global patterns: {}", filename);
                global_patterns = Some(catenary::routing_common::transit_graph::load_bincode(
                    path.to_str().unwrap(),
                )?);
            } else if filename == "manifest.json" {
                println!("Loading manifest: {}", filename);
                let mut file = File::open(&path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                manifest = Some(serde_json::from_slice(&buffer[..])?);
            }
        }
    }

    // Index partitions by ID for quick lookup
    let partition_map: HashMap<u32, &TransitPartition> =
        partitions.iter().map(|p| (p.partition_id, p)).collect();

    // 1. Plot Partition Boundaries (Prefer from Partition, fallback to Manifest)
    for partition in &partitions {
        if let Some(boundary) = &partition.boundary {
            let mut properties = JsonObject::new();
            properties.insert("type".to_string(), "partition_boundary".into());
            properties.insert("partition_id".to_string(), partition.partition_id.into());
            properties.insert("source".to_string(), "partition_pbf".into());

            let ring: Vec<Vec<f64>> = boundary.points.iter().map(|p| vec![p.lon, p.lat]).collect();
            // GeoJSON Polygon requires a list of rings (outer + inner holes)
            // We assume single outer ring for now.
            features.push(Feature {
                bbox: None,
                geometry: Some(Geometry::new(Value::Polygon(vec![ring]))),
                id: None,
                properties: Some(properties),
                foreign_members: None,
            });
        }
    }

    if let Some(manifest) = &manifest {
        for (partition_id, boundary) in &manifest.partition_boundaries {
            // Only plot if we didn't already plot it from the partition itself (or maybe plot both for comparison?)
            // For now, let's plot it if the partition wasn't loaded or didn't have a boundary.
            let has_partition_boundary = partition_map
                .get(partition_id)
                .map(|p| p.boundary.is_some())
                .unwrap_or(false);

            if !has_partition_boundary {
                let mut properties = JsonObject::new();
                properties.insert("type".to_string(), "partition_boundary".into());
                properties.insert("partition_id".to_string(), partition_id.clone().into());
                properties.insert("source".to_string(), "manifest".into());
                properties.insert("style".to_string(), "convex_fallback".into());

                let ring: Vec<Vec<f64>> =
                    boundary.points.iter().map(|p| vec![p.lon, p.lat]).collect();

                features.push(Feature {
                    bbox: None,
                    geometry: Some(Geometry::new(Value::Polygon(vec![ring]))),
                    id: None,
                    properties: Some(properties),
                    foreign_members: None,
                });
            }
        }
    }

    // 2. Process Global DAGs
    if let Some(gp) = global_patterns {
        println!("Processing {} global DAGs", gp.partition_dags.len());
        for dag in gp.partition_dags {
            let from_part_id = dag.from_partition;
            let to_part_id = dag.to_partition;

            // Resolve hubs
            let mut hub_coords: Vec<Option<(f64, f64)>> = Vec::new();
            for hub in &dag.hubs {
                let coords = if let Some(p) = partition_map.get(&hub.original_partition_id) {
                    if let Some(stop) = p.stops.get(hub.stop_idx_in_partition as usize) {
                        Some((stop.lon, stop.lat))
                    } else {
                        None
                    }
                } else {
                    None
                };
                hub_coords.push(coords);

                // Plot Hub
                if let Some((lon, lat)) = coords {
                    hub_count += 1;
                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "hub".into());
                    properties.insert(
                        "original_partition_id".to_string(),
                        hub.original_partition_id.into(),
                    );
                    properties.insert("stop_idx".to_string(), hub.stop_idx_in_partition.into());

                    features.push(Feature {
                        bbox: None,
                        geometry: Some(Geometry::new(Value::Point(vec![lon, lat]))),
                        id: None,
                        properties: Some(properties),
                        foreign_members: None,
                    });
                }
            }

            for edge in dag.edges {
                if let (Some(Some(from)), Some(Some(to))) = (
                    hub_coords.get(edge.from_node_idx as usize),
                    hub_coords.get(edge.to_node_idx as usize),
                ) {
                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "global_dag_edge".into());
                    properties.insert("from_partition".to_string(), from_part_id.into());
                    properties.insert("to_partition".to_string(), to_part_id.into());

                    let edge_type_str = match &edge.edge_type {
                        Some(EdgeType::Transit(_)) => "transit",
                        Some(EdgeType::LongDistanceTransit(_)) => "long_distance",
                        Some(EdgeType::Walk(_)) => "walk",
                        None => "unknown",
                    };
                    properties.insert("edge_type".to_string(), edge_type_str.into());

                    features.push(Feature {
                        bbox: None,
                        geometry: Some(Geometry::new(Value::LineString(vec![
                            vec![from.0, from.1],
                            vec![to.0, to.1],
                        ]))),
                        id: None,
                        properties: Some(properties),
                        foreign_members: None,
                    });
                }
            }
        }
    }

    println!("Total Hub Nodes Found: {}", hub_count);

    let geojson = geojson::GeoJson::FeatureCollection(FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    });

    let file = File::create(&args.output)?;
    serde_json::to_writer(file, &geojson)?;
    println!("Done! Wrote visualization to {:?}", args.output);

    Ok(())
}
