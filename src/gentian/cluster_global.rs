use crate::clustering::merge_based_clustering;
use crate::reduce_borders::reduce_borders_by_merging;
use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use anyhow::{Context, Result};
use catenary::routing_common::transit_graph::{
    BoundaryPoint, IntermediateLocalEdge, IntermediateStation, load_bincode, save_bincode,
};
use geo::algorithm::concave_hull::ConcaveHull;
use geo::{MultiPoint, Point, Polygon};
use std::path::Path;

use rand::prelude::*;

/// Run the global clustering phase.
/// Reads all shards, builds a meta-graph of micro-partitions (tiles),
/// clusters them, and outputs the final station-to-cluster mapping.
pub fn run_global_clustering(output_dir: &Path) -> Result<()> {
    let shards_dir = output_dir.join("shards");
    println!("Starting Global Clustering from shards in {:?}", shards_dir);

    // 1. Load all Stations to build Micro-Partitions
    // We need to know the size (number of stations) of each tile.
    println!("Loading stations to build micro-partitions...");

    let mut tile_sizes: HashMap<String, usize> = HashMap::new();
    let mut station_to_tile: HashMap<String, String> = HashMap::new();
    let mut station_to_chateau: HashMap<String, String> = HashMap::new();
    let mut station_locations: HashMap<String, (f64, f64)> = HashMap::new();

    // Iterate over station shards
    let entries = std::fs::read_dir(&shards_dir).context("Failed to read shards dir")?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("stations_") && name.ends_with(".bincode") {
                let stations: Vec<IntermediateStation> = load_bincode(path.to_str().unwrap())?;

                // The tile ID is in the filename or in the stations.
                // Let's trust the stations' tile_id field.
                for s in stations {
                    *tile_sizes.entry(s.tile_id.clone()).or_default() += 1;
                    station_to_tile.insert(s.station_id.clone(), s.tile_id);
                    station_to_chateau.insert(s.station_id.clone(), s.chateau_id);
                    station_locations.insert(s.station_id, (s.lat, s.lon));
                }
            }
        }
    }

    println!("Found {} micro-partitions (tiles).", tile_sizes.len());

    // Map Tile ID -> Integer Index for clustering algorithm
    let mut tile_ids: Vec<String> = tile_sizes.keys().cloned().collect();
    tile_ids.sort(); // Deterministic order

    let tile_to_idx: HashMap<String, usize> = tile_ids
        .iter()
        .enumerate()
        .map(|(i, t)| (t.clone(), i))
        .collect();

    // 2. Build Meta-Graph (Edges between Tiles)
    println!("Building Meta-Graph...");
    let mut meta_adjacency: HashMap<(usize, usize), u32> = HashMap::new();

    let entries = std::fs::read_dir(&shards_dir).context("Failed to read shards dir")?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("edges_") && name.ends_with(".bincode") {
                let edges: Vec<IntermediateLocalEdge> = load_bincode(path.to_str().unwrap())?;

                for e in edges {
                    if let (Some(tile_u), Some(tile_v)) = (
                        station_to_tile.get(&e.u_station_id),
                        station_to_tile.get(&e.v_station_id),
                    ) {
                        if let (Some(&idx_u), Some(&idx_v)) =
                            (tile_to_idx.get(tile_u), tile_to_idx.get(tile_v))
                        {
                            if idx_u != idx_v {
                                let (min, max) = if idx_u < idx_v {
                                    (idx_u, idx_v)
                                } else {
                                    (idx_v, idx_u)
                                };
                                // Aggregate weight (frequency). Cast to u32 for clustering algo.
                                *meta_adjacency.entry((min, max)).or_default() +=
                                    e.total_weight() as u32;
                            }
                        }
                    }
                }
            }
        }
    }

    println!("Meta-Graph built with {} edges.", meta_adjacency.len());

    // 3. Run Clustering
    // We need to adapt the clustering algorithm to handle weighted nodes (tiles have different sizes).
    // The existing `merge_based_clustering` assumes each node has size 1 initially.
    // We might need to modify it or wrap it.

    // Actually, `merge_based_clustering` initializes `clusters` as `vec![vec![i]]`.
    // We can pass the meta-graph to it.
    // But it checks `clusters[c1].len() + clusters[c2].len() <= max_size`.
    // This counts *number of tiles*, not number of stations.
    // This is WRONG. We want to limit by *number of stations*.

    // We need a modified clustering function that accepts node weights.
    // Or we can hack it: The `clusters` vector in `merge_based_clustering` stores the list of original nodes.
    // If we can modify `merge_based_clustering` to take a `node_weights` vector, that would be best.

    // Let's modify `src/gentian/clustering.rs` first to support node weights.
    // But I can't modify it in this step easily without context switching.
    //
    // Alternative: Treat each station as a node? No, that defeats the purpose of micro-partitions.
    //
    // I MUST modify `merge_based_clustering`.
    // Let's assume I will modify it. I'll write the call here assuming the new signature.
    //
    // `merge_based_clustering(num_nodes, adjacency, max_size, node_weights)`

    // Wait, I can't compile if I don't change it.
    // I should probably pause writing this file and modify clustering.rs first.
    // But I'm already writing this file.
    //
    // I will write this file using a hypothetical `merge_based_clustering_weighted` and then implement it.

    let node_weights: Vec<usize> = tile_ids
        .iter()
        .map(|t| *tile_sizes.get(t).unwrap())
        .collect();

    // Target cluster size: 1500 stations (default).
    // We should probably pass this in args, but let's hardcode or pass via function arg.
    // For now, hardcode 1500.
    let target_size = 1500;

    println!("Running Merge-Based Clustering...");
    // We need to implement this function in clustering.rs
    let clusters = crate::clustering::merge_based_clustering_weighted(
        tile_ids.len(),
        &meta_adjacency,
        target_size,
        &node_weights,
    );

    println!("Initial Clusters: {}", clusters.len());

    // 4. Border Reduction
    // Similarly, `reduce_borders_by_merging` needs to know node weights (station counts).
    // It currently takes `clusters` (vec of vec of indices) and `adjacency`.
    // It calculates `cluster_size` by summing `clusters[c].len()`.
    // We need it to sum `node_weights[i]` for i in cluster.

    println!("Running Border Reduction...");
    let clusters = crate::reduce_borders::reduce_borders_by_merging_weighted(
        clusters,
        &meta_adjacency,
        target_size,
        &node_weights,
    );

    println!("Final Clusters: {}", clusters.len());

    // 5. Materialize Output
    // Map: Station ID -> Cluster ID
    let mut station_to_cluster: HashMap<String, u32> = HashMap::new();

    // Efficient way:
    // Build Map: Tile Index -> Cluster ID
    let mut tile_to_cluster: HashMap<usize, u32> = HashMap::new();
    for (c_idx, tiles) in clusters.iter().enumerate() {
        for &t_idx in tiles {
            tile_to_cluster.insert(t_idx, c_idx as u32);
        }
    }

    for (station_id, tile_id) in station_to_tile {
        if let Some(&tile_idx) = tile_to_idx.get(&tile_id) {
            if let Some(&cluster_id) = tile_to_cluster.get(&tile_idx) {
                station_to_cluster.insert(station_id, cluster_id);
            }
        }
    }

    // Save to disk
    let output_path = output_dir.join("station_to_cluster_map.bincode");
    save_bincode(&station_to_cluster, output_path.to_str().unwrap())?;
    println!("Saved station-to-cluster map to {:?}", output_path);

    // 6. Build and Save Manifest
    println!("Building Manifest...");
    use catenary::routing_common::transit_graph::{Manifest, PartitionBoundary};

    let mut chateau_to_partitions: std::collections::HashMap<String, Vec<u32>> =
        std::collections::HashMap::new();
    let mut partition_to_chateaux: std::collections::HashMap<u32, Vec<String>> =
        std::collections::HashMap::new();

    for (station_id, cluster_id) in &station_to_cluster {
        if let Some(chateau_id) = station_to_chateau.get(station_id) {
            chateau_to_partitions
                .entry(chateau_id.clone())
                .or_default()
                .push(*cluster_id);
            partition_to_chateaux
                .entry(*cluster_id)
                .or_default()
                .push(chateau_id.clone());
        }
    }

    // Deduplicate
    for parts in chateau_to_partitions.values_mut() {
        parts.sort();
        parts.dedup();
    }
    for chateaux in partition_to_chateaux.values_mut() {
        chateaux.sort();
        chateaux.dedup();
    }

    // Build Partition Boundaries
    let mut partition_boundaries: std::collections::HashMap<u32, PartitionBoundary> =
        std::collections::HashMap::new();

    // Group stations by partition
    let mut partition_points: HashMap<u32, Vec<(f64, f64)>> = HashMap::new();
    for (station_id, cluster_id) in &station_to_cluster {
        if let Some(&loc) = station_locations.get(station_id) {
            partition_points.entry(*cluster_id).or_default().push(loc);
        }
    }

    for (cluster_id, points) in partition_points {
        if points.is_empty() {
            continue;
        }

        let boundary_points_vec: Vec<BoundaryPoint>;

        if points.len() >= 3 {
            let geo_points: Vec<Point<f64>> = points
                .iter()
                .map(|(lat, lon)| Point::new(*lon, *lat))
                .collect();
            let mp = MultiPoint(geo_points);
            let hull = mp.concave_hull(0.1);

            boundary_points_vec = hull
                .exterior()
                .points()
                .map(|p| BoundaryPoint {
                    lat: p.y(),
                    lon: p.x(),
                })
                .collect();
        } else {
            // Fallback for small clusters: just return the points (maybe as a box if needed, but for now points)
            // To make it a valid polygon for some renderers, we might need to duplicate points or create a small box.
            // Let's create a small box around the point(s).
            let (min_lat, max_lat, min_lon, max_lon) = points.iter().fold(
                (
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                ),
                |(min_lat, max_lat, min_lon, max_lon), (lat, lon)| {
                    (
                        min_lat.min(*lat),
                        max_lat.max(*lat),
                        min_lon.min(*lon),
                        max_lon.max(*lon),
                    )
                },
            );

            // Buffer size in degrees (approx 100m)
            let buffer = 0.001;
            boundary_points_vec = vec![
                BoundaryPoint {
                    lat: min_lat - buffer,
                    lon: min_lon - buffer,
                },
                BoundaryPoint {
                    lat: max_lat + buffer,
                    lon: min_lon - buffer,
                },
                BoundaryPoint {
                    lat: max_lat + buffer,
                    lon: max_lon + buffer,
                },
                BoundaryPoint {
                    lat: min_lat - buffer,
                    lon: max_lon + buffer,
                },
                BoundaryPoint {
                    lat: min_lat - buffer,
                    lon: min_lon - buffer,
                }, // Close the loop
            ];
        }

        partition_boundaries.insert(
            cluster_id,
            PartitionBoundary {
                points: boundary_points_vec,
            },
        );
    }

    let manifest = Manifest {
        chateau_to_partitions,
        partition_to_chateaux,
        partition_boundaries,
    };

    let manifest_path = output_dir.join("manifest.json");
    let file = std::fs::File::create(&manifest_path).context("Failed to create manifest.json")?;
    serde_json::to_writer_pretty(file, &manifest).context("Failed to write manifest.json")?;
    println!("Saved manifest to {:?}", manifest_path);

    Ok(())
}
