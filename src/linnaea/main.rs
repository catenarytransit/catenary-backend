// cargo run --bin linnaea --release -- --input containers/test_graph_gen/output --output transit_viz.geojson

use catenary::routing_common::transit_graph::TransitPartition;
use clap::Parser;
use geojson::{Feature, FeatureCollection, Geometry, JsonObject, Value};
use prost::Message;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input folder containing transit_chunk_*.pbf files
    #[arg(short, long)]
    input: PathBuf,

    /// Output GeoJSON file
    #[arg(short, long)]
    output: PathBuf,
}

/// Helper to resolve a chateau name from its index.
/// Falls back to a placeholder if the index is out of bounds.
fn resolve_chateau<'a>(partition: &'a TransitPartition, chateau_idx: u32) -> &'a str {
    partition
        .chateau_ids
        .get(chateau_idx as usize)
        .map(String::as_str)
        .unwrap_or("<unknown_chateau>")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut features = Vec::new();
    let mut partitions: Vec<TransitPartition> = Vec::new();

    println!("Reading files from {:?}", args.input);

    let mut hub_count = 0;
    let entries = std::fs::read_dir(&args.input)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
            if filename.starts_with("transit_chunk_") && filename.ends_with(".pbf") {
                println!("Loading partition: {}", filename);
                let mut file = File::open(&path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                let partition = TransitPartition::decode(&buffer[..])?;

                // Index stops
                for (_idx, stop) in partition.stops.iter().enumerate() {
                    let chateau = resolve_chateau(&partition, stop.chateau_idx);

                    if stop.is_border {
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "border_node".into());
                        properties.insert("chateau".to_string(), chateau.into());
                        properties
                            .insert("gtfs_id".to_string(), stop.gtfs_original_id.clone().into());
                        properties
                            .insert("partition_id".to_string(), partition.partition_id.into());

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::Point(vec![stop.lon, stop.lat]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }

                    if stop.is_hub {
                        hub_count += 1;
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "hub_node".into());
                        properties.insert("chateau".to_string(), chateau.into());
                        properties
                            .insert("gtfs_id".to_string(), stop.gtfs_original_id.clone().into());
                        properties
                            .insert("partition_id".to_string(), partition.partition_id.into());

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::Point(vec![stop.lon, stop.lat]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }

                    if stop.is_external_gateway {
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "external_gateway".into());
                        properties.insert("chateau".to_string(), chateau.into());
                        properties
                            .insert("gtfs_id".to_string(), stop.gtfs_original_id.clone().into());
                        properties
                            .insert("partition_id".to_string(), partition.partition_id.into());

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::Point(vec![stop.lon, stop.lat]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }
                }

                // Output Partition Boundary (Convex)
                if let Some(boundary) = &partition.boundary {
                    let mut ring = Vec::new();
                    for point in &boundary.points {
                        ring.push(vec![point.lon, point.lat]);
                    }
                    // Close the ring if not already closed
                    if let (Some(first), Some(last)) = (ring.first(), ring.last()) {
                        if first != last {
                            ring.push(first.clone());
                        }
                    }

                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "partition_boundary".into());
                    properties.insert("partition_id".to_string(), partition.partition_id.into());

                    features.push(Feature {
                        bbox: None,
                        geometry: Some(Geometry::new(Value::Polygon(vec![ring]))),
                        id: None,
                        properties: Some(properties),
                        foreign_members: None,
                    });
                }

                partitions.push(partition);
            } else if filename.starts_with("transfer_chunk_") && filename.ends_with(".pbf") {
                // Skip loading transfer chunks as we don't output them
            } else if filename == "global_patterns.pbf" {
                // Skip loading global patterns as we don't output them
            }
        }
    }
    println!("Total Hub Nodes Found: {}", hub_count);

    // Removed: Processing patterns (Trip Geometry)
    // Removed: Processing internal transfers
    // Removed: Processing local transfer patterns
    // Removed: Processing external transfers
    // Removed: Processing global transfer patterns

    let geojson = geojson::GeoJson::FeatureCollection(FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    });

    let file = File::create(&args.output)?;
    serde_json::to_writer(file, &geojson)?;

    println!("Done! Wrote to {:?}", args.output);

    Ok(())
}
