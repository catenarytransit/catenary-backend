use anyhow::{Context, Result};
use catenary::routing_common::transit_graph::{
    DagEdge, IntermediateStation, LocalTransferPattern, load_bincode, save_bincode,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Run Local Transfer Pattern generation for a specific cluster.
pub fn run_local_patterns(output_dir: &Path, cluster_id: u32) -> Result<()> {
    println!("Generating Local Patterns for Cluster {}...", cluster_id);

    // 1. Load Station -> Cluster Map
    let map_path = output_dir.join("station_to_cluster_map.bincode");
    let station_to_cluster: HashMap<String, u32> = load_bincode(map_path.to_str().unwrap())?;

    // 2. Identify Stations in this Cluster
    let cluster_stations: HashSet<String> = station_to_cluster
        .iter()
        .filter(|&(_, &c)| c == cluster_id)
        .map(|(s, _)| s.clone())
        .collect();

    if cluster_stations.is_empty() {
        println!("Cluster {} has no stations. Skipping.", cluster_id);
        return Ok(());
    }

    println!(
        "Cluster {} has {} stations.",
        cluster_id,
        cluster_stations.len()
    );

    // 3. Load Timetable Data (Shards)
    // We need to find trips that are fully contained within this cluster.
    // This requires scanning all shards? Or do we have a better way?
    // In "Direct-to-Shard Extraction", we have shards of stations and edges.
    // We DO NOT have shards of trips yet.
    // The extraction phase only saved `IntermediateStation` and `IntermediateLocalEdge`.
    // It did NOT save trips.

    // CRITICAL MISSING PIECE: We need the actual timetable (trips) to run profile searches.
    // The extraction phase should have saved trips too, or we need to fetch them now.
    // Fetching from DB for every cluster is slow if we do it repeatedly.
    // But maybe acceptable for "per cluster job".

    // However, the plan said: "Scan timetable shards, keep only trips where all stops are in C."
    // This implies we have timetable shards.
    // I missed creating timetable shards in Phase 2.

    // I should probably update Phase 2 to also shard trips?
    // Or I can fetch from DB here if I have the `chateau` list for this cluster.
    // But a cluster can span multiple chateaux.

    // Let's assume for now we need to fetch from DB or we need to implement trip sharding.
    // Given the user instructions "Scan timetable shards", I should have implemented trip sharding.

    // I will add a TODO here and maybe implement a basic version that assumes we can access the DB?
    // Or I can go back and add trip sharding to Phase 2.
    // Adding trip sharding to Phase 2 is cleaner.

    // But wait, the user said: "This can be a streaming pass that writes a timetable/C/ directory with the subset."
    // This implies there is a global source of truth (DB or big file) and we extract.

    // Let's assume we can query the DB for trips involving these stations.
    // But that's hard because trips are by chateau.

    // Let's implement a "Trip Sharding" or "Timetable Extraction" step?
    // Or just fetch all trips for the chateaux involved in this cluster.
    // We can find which chateaux are involved by looking at the stations (if they have chateau ID).
    // `IntermediateStation` does NOT have chateau ID.

    // I should add `chateau_id` to `IntermediateStation`.

    // Let's pause `local.rs` and update `IntermediateStation` and `extract.rs`.

    Ok(())
}
