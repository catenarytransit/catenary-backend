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

fn resolve_coords(
    partition_id: u32,
    stop_idx: u32,
    partition_map: &HashMap<u32, &TransitPartition>,
) -> Option<(f64, f64)> {
    if let Some(p) = partition_map.get(&partition_id) {
        if let Some(stop) = p.stops.get(stop_idx as usize) {
            return Some((stop.lon, stop.lat));
        }
    }
    None
}

fn edge_type_to_string(et: &Option<EdgeType>) -> &'static str {
    match et {
        Some(EdgeType::Transit(_)) => "transit",
        Some(EdgeType::LongDistanceTransit(_)) => "long_distance",
        Some(EdgeType::Walk(_)) => "walk",
        None => "unknown",
    }
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

        // We iterate both standard and long-distance DAGs
        // We use boolean flag to distinguish them in output properties
        let dag_groups = vec![
            (gp.partition_dags, "standard"),
            (gp.long_distance_dags, "long_distance"),
        ];

        for (dags, dag_type) in dag_groups {
            for dag in dags {
                let from_part_id = dag.from_partition;
                let to_part_id = dag.to_partition;

                // Resolve hubs
                let mut hub_coords: Vec<Option<(f64, f64)>> = Vec::new();
                for hub in &dag.hubs {
                    let coords = resolve_coords(
                        hub.original_partition_id,
                        hub.stop_idx_in_partition,
                        &partition_map,
                    );
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
                        properties.insert("dag_type".to_string(), dag_type.into());

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::Point(vec![lon, lat]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }
                }

                for edge in &dag.edges {
                    if let (Some(Some(from)), Some(Some(to))) = (
                        hub_coords.get(edge.from_node_idx as usize),
                        hub_coords.get(edge.to_node_idx as usize),
                    ) {
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "global_dag_edge".into());
                        properties.insert("from_partition".to_string(), from_part_id.into());
                        properties.insert("to_partition".to_string(), to_part_id.into());
                        properties.insert("dag_type".to_string(), dag_type.into());
                        properties.insert(
                            "edge_type".to_string(),
                            edge_type_to_string(&edge.edge_type).into(),
                        );

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
    }

    // 3. Process Per-Partition Long Distance Transfer Patterns
    println!("Processing Partition Long Distance Transfer Patterns");
    for partition in &partitions {
        for ltp in &partition.long_distance_transfer_patterns {
            let from_coords =
                resolve_coords(partition.partition_id, ltp.from_stop_idx, &partition_map);

            if let Some((from_lon, from_lat)) = from_coords {
                for edge in &ltp.edges {
                    // Resolve To Node (Stop or External Hub)
                    let to_coords = if (edge.to_node_idx as usize) < partition.stops.len() {
                        resolve_coords(partition.partition_id, edge.to_node_idx, &partition_map)
                    } else {
                        let ext_hub_idx = (edge.to_node_idx as usize) - partition.stops.len();
                        if let Some(hub) = partition.external_hubs.get(ext_hub_idx) {
                            resolve_coords(
                                hub.original_partition_id,
                                hub.stop_idx_in_partition,
                                &partition_map,
                            )
                        } else {
                            None
                        }
                    };

                    if let Some((to_lon, to_lat)) = to_coords {
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "local_transfer_pattern_edge".into());
                        properties
                            .insert("partition_id".to_string(), partition.partition_id.into());
                        properties.insert(
                            "edge_type".to_string(),
                            edge_type_to_string(&edge.edge_type).into(),
                        );

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::LineString(vec![
                                vec![from_lon, from_lat],
                                vec![to_lon, to_lat],
                            ]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }
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
