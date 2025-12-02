// cargo run --bin linnaea --release -- --input containers/test_graph_gen/output --output transit_viz.geojson --output-data transit_data.json

use catenary::routing_common::transit_graph::{
    GlobalPatternIndex, LocalTransferPattern, TransitPartition, TransitStop, TripPattern,
};
use clap::Parser;
use geo::{ConcaveHull, ConvexHull, Point, Polygon};
use geojson::{Feature, FeatureCollection, Geometry, JsonObject, Value};
use prost::Message;
use std::collections::HashMap;
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

    /// Output Data JSON file (optional)
    #[arg(long)]
    output_data: Option<PathBuf>,
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

#[derive(serde::Serialize)]
struct OutputData {
    stops: Vec<TransitStop>,
    trip_patterns: Vec<TripPattern>,
    local_transfer_patterns: Vec<LocalTransferPattern>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut features = Vec::new();
    let mut partitions: Vec<TransitPartition> = Vec::new();
    let mut global_patterns: Option<GlobalPatternIndex> = None;

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
                partitions.push(partition);
            } else if filename == "global_patterns.pbf" {
                println!("Loading global patterns: {}", filename);
                let mut file = File::open(&path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                global_patterns = Some(GlobalPatternIndex::decode(&buffer[..])?);
            }
        }
    }

    // Index partitions by ID for quick lookup
    let partition_map: HashMap<u32, &TransitPartition> =
        partitions.iter().map(|p| (p.partition_id, p)).collect();

    // 1. Process Partitions (Stops and Boundaries)
    for partition in &partitions {
        let mut border_points: Vec<Point<f64>> = Vec::new();

        // Index stops
        for (_idx, stop) in partition.stops.iter().enumerate() {
            let chateau = resolve_chateau(partition, stop.chateau_idx);

            if stop.is_border {
                border_points.push(Point::new(stop.lon, stop.lat));

                let mut properties = JsonObject::new();
                properties.insert("type".to_string(), "border_node".into());
                properties.insert("chateau".to_string(), chateau.into());
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
                hub_count += 1;
                let mut properties = JsonObject::new();
                properties.insert("type".to_string(), "hub_node".into());
                properties.insert("chateau".to_string(), chateau.into());
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

        // Output Partition Boundary (Concave Hull of Border Stops)
        if !border_points.is_empty() {
            // Use a high concavity to make it "tight" but not too crazy.
            // If < 3 points, we can't make a polygon, so we skip or just make a line/point?
            // ConcaveHull requires at least 3 points.
            if border_points.len() >= 3 {
                // Concavity of 2.0 is usually a good starting point. Lower is more convex.
                // Actually, for `geo`, `concave_hull` takes a `concavity` parameter.
                // Let's try 2.0.
                let poly: Option<Polygon<f64>> = try_concave_hull(&border_points, 2.0);

                if let Some(poly) = poly {
                    let ring = poly
                        .exterior()
                        .points()
                        .map(|p| vec![p.x(), p.y()])
                        .collect::<Vec<_>>();

                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "partition_boundary".into());
                    properties.insert("partition_id".to_string(), partition.partition_id.into());
                    properties.insert("style".to_string(), "concave".into());

                    features.push(Feature {
                        bbox: None,
                        geometry: Some(Geometry::new(Value::Polygon(vec![ring]))),
                        id: None,
                        properties: Some(properties),
                        foreign_members: None,
                    });
                } else {
                    // Fallback to Convex Hull
                    let poly = UseConvexHull(&border_points);
                    let ring = poly
                        .exterior()
                        .points()
                        .map(|p| vec![p.x(), p.y()])
                        .collect::<Vec<_>>();

                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "partition_boundary".into());
                    properties.insert("partition_id".to_string(), partition.partition_id.into());
                    properties.insert("style".to_string(), "convex_fallback".into());

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
            }

            for edge in dag.edges {
                if let (Some(Some(from)), Some(Some(to))) = (
                    hub_coords.get(edge.from_hub_idx as usize),
                    hub_coords.get(edge.to_hub_idx as usize),
                ) {
                    let mut properties = JsonObject::new();
                    properties.insert("type".to_string(), "global_dag_edge".into());
                    properties.insert("from_partition".to_string(), from_part_id.into());
                    properties.insert("to_partition".to_string(), to_part_id.into());

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

    // 3. Output Data File
    if let Some(ref data_path) = args.output_data {
        println!("Writing data to {:?}", data_path);
        let mut all_stops = Vec::new();
        let mut all_trip_patterns = Vec::new();
        let mut all_local_transfer_patterns = Vec::new();

        for p in partitions {
            all_stops.extend(p.stops);
            all_trip_patterns.extend(p.trip_patterns);
            all_local_transfer_patterns.extend(p.local_transfer_patterns);
        }

        let data = OutputData {
            stops: all_stops,
            trip_patterns: all_trip_patterns,
            local_transfer_patterns: all_local_transfer_patterns,
        };

        let file = File::create(data_path)?;
        serde_json::to_writer(file, &data)?;
        println!("Done! Wrote data to {:?}", args.output_data);
    }

    Ok(())
}

fn try_concave_hull(points: &[Point<f64>], concavity: f64) -> Option<Polygon<f64>> {
    use geo::algorithm::concave_hull::ConcaveHull;
    // Collect into a collection that implements ConcaveHull
    // geo 0.28+ implements it for [Point<T>]
    // But we need to handle potential failures or panics if points are collinear etc.
    // The trait method is `concave_hull`.

    // Note: ConcaveHull might not be implemented for slice directly in all versions,
    // but usually is for MultiPoint or Vec<Point>.
    let mp = geo::MultiPoint::new(points.to_vec());
    Some(mp.concave_hull(concavity))
}

fn UseConvexHull(points: &[Point<f64>]) -> Polygon<f64> {
    use geo::algorithm::convex_hull::ConvexHull;
    let mp = geo::MultiPoint::new(points.to_vec());
    mp.convex_hull()
}
