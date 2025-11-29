//cargo run --bin linnaea --release -- --input containers/test_graph_gen/output --output transit_viz.geojson

use clap::Parser;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;
use catenary::routing_common::transit_graph::{TransitPartition, TransferChunk};
use prost::Message;
use geojson::{Feature, FeatureCollection, Geometry, Value, JsonObject};
use ahash::{AHashMap, AHashSet};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

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

const TAILWIND_COLORS: &[&str] = &[
    "#ef4444", // red-500
    "#f97316", // orange-500
    "#f59e0b", // amber-500
    "#eab308", // yellow-500
    "#84cc16", // lime-500
    "#22c55e", // green-500
    "#10b981", // emerald-500
    "#14b8a6", // teal-500
    "#06b6d4", // cyan-500
    "#0ea5e9", // sky-500
    "#3b82f6", // blue-500
    "#6366f1", // indigo-500
    "#8b5cf6", // violet-500
    "#a855f7", // purple-500
    "#d946ef", // fuchsia-500
    "#ec4899", // pink-500
    "#f43f5e", // rose-500
];

fn get_color(key: &str) -> String {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    TAILWIND_COLORS[hash as usize % TAILWIND_COLORS.len()].to_string()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut features = Vec::new();
    let mut stop_lookup: AHashMap<(String, String), (f64, f64)> = AHashMap::new(); // (chateau, gtfs_id) -> coords
    let mut partition_stops: AHashMap<(u32, u32), (f64, f64)> = AHashMap::new(); // (partition_id, stop_idx) -> coords
    let mut partitions: Vec<TransitPartition> = Vec::new();
    let mut transfer_chunks: Vec<TransferChunk> = Vec::new();

    println!("Reading files from {:?}", args.input);

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
                for (idx, stop) in partition.stops.iter().enumerate() {
                    let coords = (stop.lon, stop.lat);
                    stop_lookup.insert((stop.chateau.clone(), stop.gtfs_original_id.clone()), coords);
                    partition_stops.insert((partition.partition_id, idx as u32), coords);

                    if stop.is_border {
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "border_node".into());
                        properties.insert("chateau".to_string(), stop.chateau.clone().into());
                        properties.insert("gtfs_id".to_string(), stop.gtfs_original_id.clone().into());
                        properties.insert("partition_id".to_string(), partition.partition_id.into());

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::Point(vec![stop.lon, stop.lat]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }

                    if stop.is_hub {
                        let mut properties = JsonObject::new();
                        properties.insert("type".to_string(), "hub_node".into());
                        properties.insert("chateau".to_string(), stop.chateau.clone().into());
                        properties.insert("gtfs_id".to_string(), stop.gtfs_original_id.clone().into());
                        properties.insert("partition_id".to_string(), partition.partition_id.into());

                        features.push(Feature {
                            bbox: None,
                            geometry: Some(Geometry::new(Value::Point(vec![stop.lon, stop.lat]))),
                            id: None,
                            properties: Some(properties),
                            foreign_members: None,
                        });
                    }
                }
                partitions.push(partition);
            } else if filename.starts_with("transfer_chunk_") && filename.ends_with(".pbf") {
                println!("Loading transfer chunk: {}", filename);
                let mut file = File::open(&path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                let chunk = TransferChunk::decode(&buffer[..])?;
                transfer_chunks.push(chunk);
            }
        }
    }

    println!("Processing patterns...");
    for partition in &partitions {
        let mut seen_patterns = AHashSet::new();
        for pattern in &partition.trip_patterns {
            if !seen_patterns.insert(pattern.stop_indices.clone()) {
                continue;
            }
            let mut line_coords = Vec::new();
            for &stop_idx in &pattern.stop_indices {
                if let Some(&coords) = partition_stops.get(&(partition.partition_id, stop_idx)) {
                    line_coords.push(vec![coords.0, coords.1]);
                }
            }

            if line_coords.len() > 1 {
                let color = get_color(&pattern.chateau);
                let mut properties = JsonObject::new();
                properties.insert("type".to_string(), "trip_pattern".into());
                properties.insert("chateau".to_string(), pattern.chateau.clone().into());
                properties.insert("route_id".to_string(), pattern.route_id.clone().into());

                features.push(Feature {
                    bbox: None,
                    geometry: Some(Geometry::new(Value::LineString(line_coords))),
                    id: None,
                    properties: Some(properties),
                    foreign_members: None,
                });
            }
        }

        let mut seen_internal_transfers = AHashSet::new();
        for transfer in &partition.internal_transfers {
             if !seen_internal_transfers.insert((transfer.from_stop_idx, transfer.to_stop_idx)) {
                 continue;
             }
             let from_coords = partition_stops.get(&(partition.partition_id, transfer.from_stop_idx));
             let to_coords = partition_stops.get(&(partition.partition_id, transfer.to_stop_idx));

             if let (Some(from), Some(to)) = (from_coords, to_coords) {
                let mut properties = JsonObject::new();
                properties.insert("type".to_string(), "internal_transfer".into());

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

    println!("Processing local transfer patterns...");
    for partition in &partitions {
        let mut seen_edges = AHashSet::new();
        for pattern in &partition.local_transfer_patterns {
             for edge in &pattern.edges {
                 // Edges in LTP use local stop indices (within the partition)
                 // from_hub_idx and to_hub_idx are local indices
                 if !seen_edges.insert((partition.partition_id, edge.from_hub_idx, edge.to_hub_idx)) {
                     continue;
                 }

                 let from_coords = partition_stops.get(&(partition.partition_id, edge.from_hub_idx));
                 let to_coords = partition_stops.get(&(partition.partition_id, edge.to_hub_idx));

                 if let (Some(from), Some(to)) = (from_coords, to_coords) {
                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "transfer_pattern".into());
                    properties.insert("partition_id".to_string(), partition.partition_id.into());
                    properties.insert("signature".to_string(), edge.pattern_signature.clone().into());

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

    println!("Processing external transfers...");
    for chunk in &transfer_chunks {
        let mut seen_external_transfers = AHashSet::new();
        for transfer in &chunk.external_transfers {
            if !seen_external_transfers.insert((transfer.from_stop_idx, transfer.to_chateau.clone(), transfer.to_stop_gtfs_id.clone())) {
                continue;
            }
            let from_coords = partition_stops.get(&(chunk.partition_id, transfer.from_stop_idx));
            let to_coords = stop_lookup.get(&(transfer.to_chateau.clone(), transfer.to_stop_gtfs_id.clone()));

            if let (Some(from), Some(to)) = (from_coords, to_coords) {
                let mut properties = JsonObject::new();
                properties.insert("type".to_string(), "external_transfer".into());

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
            } else {
                 println!("Warning: Could not resolve endpoints for external transfer from partition {} stop {} to {}/{}", 
                     chunk.partition_id, transfer.from_stop_idx, transfer.to_chateau, transfer.to_stop_gtfs_id);
            }
        }
    }

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
