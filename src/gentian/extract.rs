use crate::station_matching;
use anyhow::{Context, Result};
use catenary::models::{
    Agency, Calendar, CompressedTrip as DbCompressedTrip, ItineraryPatternRow, Route, Station, Stop,
};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::routing_common::transit_graph::{
    IntermediateLocalEdge, IntermediateStation, load_bincode, save_bincode,
};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geohash::{Coord, encode};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Run the extraction phase for a list of chateaux.
/// This reads data from the DB, matches stations, computes tiles,
/// and updates the global shard files directly.
pub async fn run_extraction(
    pool: Arc<CatenaryPostgresPool>,
    chateaux_list: Vec<String>,
    output_dir: &Path,
) -> Result<()> {
    let mut conn = pool.get().await.context("Failed to get DB connection")?;

    println!(
        "Starting Extraction for {} chateaux: {:?}",
        chateaux_list.len(),
        chateaux_list
    );

    // Create shards directory
    let shards_dir = output_dir.join("shards");
    tokio::fs::create_dir_all(&shards_dir).await?;

    // 1. Fetch Data
    // We can reuse logic from main.rs, but simplified since we don't need full graph structures yet.
    // We primarily need: Stops, Trips, Patterns (to build adjacency).

    let mut db_stops: Vec<Stop> = Vec::new();
    let mut db_trips: Vec<DbCompressedTrip> = Vec::new();
    let mut db_patterns: Vec<ItineraryPatternRow> = Vec::new();

    for chateau_id in &chateaux_list {
        println!("  - Fetching data for {}", chateau_id);

        // Stops
        use catenary::schema::gtfs::stops::dsl::{chateau as stop_chateau, stops};
        let stops_chunk: Vec<Stop> = stops
            .filter(stop_chateau.eq(chateau_id))
            .select(Stop::as_select())
            .load(&mut conn)
            .await?;
        db_stops.extend(stops_chunk);

        // Trips
        use catenary::schema::gtfs::trips_compressed::dsl::{
            chateau as trip_chateau, trips_compressed,
        };
        let trips_chunk: Vec<DbCompressedTrip> = trips_compressed
            .filter(trip_chateau.eq(chateau_id))
            .select(DbCompressedTrip::as_select())
            .load(&mut conn)
            .await?;
        db_trips.extend(trips_chunk);

        // Patterns
        use catenary::schema::gtfs::itinerary_pattern::dsl::{
            chateau as pattern_chateau, itinerary_pattern,
        };
        let patterns_chunk: Vec<ItineraryPatternRow> = itinerary_pattern
            .filter(pattern_chateau.eq(chateau_id))
            .select(ItineraryPatternRow::as_select())
            .load(&mut conn)
            .await?;
        db_patterns.extend(patterns_chunk);
    }

    // 2. Station Matching
    // We need to run station matching to get canonical station IDs.
    // Note: In a real incremental update, we might want to respect existing station IDs.
    // For now, we assume we are processing a set of chateaux that might interact.
    // If we are just adding one chateau, we should probably load existing stations nearby?
    // The user said: "If a new stop is added and the algorithm thinks its part of the new station, it should mark those as clustered together"
    // This implies station matching should happen here.

    println!("Running Station Matching...");
    for chateau_id in &chateaux_list {
        let stops_for_chateau: Vec<Stop> = db_stops
            .iter()
            .filter(|s| s.chateau == *chateau_id)
            .cloned()
            .collect();

        if !stops_for_chateau.is_empty() {
            station_matching::match_stops_to_stations(&pool, chateau_id, &stops_for_chateau)
                .await?;
        }
    }

    // Load Mappings & Stations
    use catenary::models::StopMapping;
    use catenary::schema::gtfs::stations::dsl::{station_id as station_table_id, stations};
    use catenary::schema::gtfs::stop_mappings::dsl::{feed_id as mapping_feed_id, stop_mappings};

    let all_mappings: Vec<StopMapping> = stop_mappings
        .filter(mapping_feed_id.eq_any(&chateaux_list))
        .load(&mut conn)
        .await?;

    let mut stop_to_station_map: HashMap<(String, String), String> = HashMap::new();
    for m in &all_mappings {
        stop_to_station_map.insert((m.feed_id.clone(), m.stop_id.clone()), m.station_id.clone());
    }

    let referenced_station_ids: Vec<String> = stop_to_station_map.values().cloned().collect();
    let referenced_station_ids: HashSet<String> = referenced_station_ids.into_iter().collect();
    let referenced_station_ids: Vec<String> = referenced_station_ids.into_iter().collect();

    let all_stations: Vec<Station> = stations
        .filter(station_table_id.eq_any(&referenced_station_ids))
        .load(&mut conn)
        .await?;

    // 3. Prepare Intermediate Data
    let mut station_to_chateau: HashMap<String, String> = HashMap::new();
    for m in &all_mappings {
        station_to_chateau.insert(m.station_id.clone(), m.feed_id.clone());
    }

    // 3. Prepare Intermediate Data
    let mut intermediate_stations: Vec<IntermediateStation> = Vec::new();
    let mut station_id_to_tile: HashMap<String, String> = HashMap::new();

    for s in all_stations {
        // Compute 3-char Geohash
        let tile_id = match encode(
            Coord {
                x: s.point.x,
                y: s.point.y,
            },
            5,
        ) {
            Ok(g) => g,
            Err(_) => "00000".to_string(), // Fallback
        };

        station_id_to_tile.insert(s.station_id.clone(), tile_id.clone());

        intermediate_stations.push(IntermediateStation {
            station_id: s.station_id.clone(),
            chateau_id: station_to_chateau
                .get(&s.station_id)
                .cloned()
                .unwrap_or_default(),
            lat: s.point.y,
            lon: s.point.x,
            name: s.name.clone(),
            tile_id,
            gtfs_stop_ids: stop_to_station_map
                .iter()
                .filter(|(_, v)| **v == s.station_id)
                .map(|((_, stop_id), _)| stop_id.clone())
                .collect(),
        });
    }

    // 4. Build Adjacency Graph
    // Similar to main.rs logic
    // 4. Build Adjacency Graph
    // Similar to main.rs logic
    let mut adjacency: HashMap<(String, String), HashMap<String, f32>> = HashMap::new();

    // Index patterns
    let mut pattern_rows_map: HashMap<(String, String), Vec<&ItineraryPatternRow>> = HashMap::new();
    for row in &db_patterns {
        pattern_rows_map
            .entry((row.chateau.clone(), row.itinerary_pattern_id.clone()))
            .or_default()
            .push(row);
    }
    for rows in pattern_rows_map.values_mut() {
        rows.sort_by_key(|r| r.stop_sequence);
    }

    let mut trips_by_pattern: HashMap<(String, String), usize> = HashMap::new();
    for trip in &db_trips {
        *trips_by_pattern
            .entry((trip.chateau.clone(), trip.itinerary_pattern_id.clone()))
            .or_default() += 1;
    }

    for ((chateau_id, pattern_id), trip_count) in trips_by_pattern {
        if let Some(rows) = pattern_rows_map.get(&(chateau_id.clone(), pattern_id)) {
            // Map stops to stations
            let station_ids: Vec<Option<String>> = rows
                .iter()
                .map(|r| {
                    stop_to_station_map
                        .get(&(r.chateau.clone(), r.stop_id.to_string()))
                        .cloned()
                })
                .collect();

            // Filter out long-distance? (Simplified check for now, or reuse main.rs logic)
            // For now, just add all edges. The clustering algorithm handles weights.
            // We should probably apply the same distance/speed filters as main.rs if possible.
            // But for simplicity, let's assume all local trips are valid for clustering.

            for i in 0..station_ids.len() - 1 {
                if let (Some(u), Some(v)) = (&station_ids[i], &station_ids[i + 1]) {
                    if u != v {
                        let (min, max) = if u < v { (u, v) } else { (v, u) };
                        *adjacency
                            .entry((min.clone(), max.clone()))
                            .or_default()
                            .entry(chateau_id.clone())
                            .or_default() += trip_count as f32;
                    }
                }
            }
        }
    }

    let intermediate_edges: Vec<IntermediateLocalEdge> = adjacency
        .into_iter()
        .map(|((u, v), weights)| IntermediateLocalEdge {
            u_station_id: u,
            v_station_id: v,
            weights,
        })
        .collect();

    // 5. Update Shards
    println!("Updating Shards...");

    // Group by Tile
    let mut stations_by_tile: HashMap<String, Vec<IntermediateStation>> = HashMap::new();
    for s in intermediate_stations {
        stations_by_tile
            .entry(s.tile_id.clone())
            .or_default()
            .push(s);
    }

    let mut edges_by_tile: HashMap<String, Vec<IntermediateLocalEdge>> = HashMap::new();
    for e in intermediate_edges {
        // Assign edge to the tile of 'u' (source)
        // Since edges are undirected and we sorted (u < v), this is deterministic.
        if let Some(tile) = station_id_to_tile.get(&e.u_station_id) {
            edges_by_tile.entry(tile.clone()).or_default().push(e);
        }
    }

    // Process Station Shards
    for (tile_id, new_stations) in stations_by_tile {
        let shard_path = shards_dir.join(format!("stations_{}.bincode", tile_id));
        let mut existing_stations: Vec<IntermediateStation> = if shard_path.exists() {
            load_bincode(shard_path.to_str().unwrap()).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Merge: Replace existing stations with same ID, append new ones
        let mut station_map: HashMap<String, IntermediateStation> = existing_stations
            .into_iter()
            .map(|s| (s.station_id.clone(), s))
            .collect();

        for s in new_stations {
            station_map.insert(s.station_id.clone(), s);
        }

        let merged_stations: Vec<IntermediateStation> = station_map.into_values().collect();
        save_bincode(&merged_stations, shard_path.to_str().unwrap())?;
    }

    // Process Edge Shards
    for (tile_id, new_edges) in edges_by_tile {
        let shard_path = shards_dir.join(format!("edges_{}.bincode", tile_id));
        let mut existing_edges: Vec<IntermediateLocalEdge> = if shard_path.exists() {
            load_bincode(shard_path.to_str().unwrap()).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Merge edges:
        // We use a map of (u, v) -> {chateau_id -> weight}.
        // This allows us to update the weights for the current chateau(s) while preserving
        // weights from other chateaux that are not part of this extraction run.
        let mut edge_map: HashMap<(String, String), HashMap<String, f32>> = existing_edges
            .into_iter()
            .map(|e| ((e.u_station_id, e.v_station_id), e.weights))
            .collect();

        for e in new_edges {
            let entry = edge_map
                .entry((e.u_station_id, e.v_station_id))
                .or_default();

            // Merge weights: overwrite if chateau exists (update), else insert
            for (chateau, weight) in e.weights {
                entry.insert(chateau, weight);
            }
        }

        let merged_edges: Vec<IntermediateLocalEdge> = edge_map
            .into_iter()
            .map(|((u, v), weights)| IntermediateLocalEdge {
                u_station_id: u,
                v_station_id: v,
                weights,
            })
            .collect();

        save_bincode(&merged_edges, shard_path.to_str().unwrap())?;
    }

    println!("Extraction Complete.");
    Ok(())
}
