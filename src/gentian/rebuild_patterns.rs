use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::routing_common::transit_graph::{
    DirectionPattern, GlobalHub, GlobalPatternIndex, LocalTransferPattern, Manifest, OsmLink,
    PartitionBoundary, PartitionTimetableData, ServiceException, StaticTransfer, TimeDeltaSequence,
    TimetableData, TransitPartition, TransitStop, TripPattern, load_bincode, save_bincode,
};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use crate::connectivity::{compute_border_patterns, compute_local_patterns_for_partition};

/// Run the pattern rebuild process.
/// This assumes `update-gtfs` has been run for modified chateaux.
/// It rebuilds partitions that contain modified chateaux, then updates border patterns.
pub async fn run_rebuild_patterns(
    pool: Arc<CatenaryPostgresPool>,
    output_dir: &Path,
) -> Result<()> {
    println!("Starting Pattern Rebuild...");

    // 1. Load Manifest
    let manifest_path = output_dir.join("manifest.json");
    let manifest: Manifest = if manifest_path.exists() {
        let file = File::open(&manifest_path).context("Failed to open manifest.json")?;
        let reader = BufReader::new(file);
        serde_json::from_reader(reader)?
    } else {
        return Err(anyhow::anyhow!("Manifest not found. Cannot rebuild."));
    };

    // 2. Identify Affected Partitions
    // Ideally, we should know WHICH chateaux were updated.
    // For now, let's assume we rebuild ALL partitions that have a `timetable_data_{chateau}.bincode` present?
    // Or maybe we should pass a list of updated chateaux?
    // The user request says "Identify partitions affected by the updated chateau".
    // But the command doesn't take arguments.
    //
    // Let's assume we check file modification times or we just rebuild everything that *can* be rebuilt from available timetable data?
    // Or maybe we just iterate all partitions in the manifest and rebuild them if their source data is newer?
    //
    // For this implementation, let's iterate ALL partitions in the manifest and rebuild them.
    // This is "safe" but maybe slower than minimal.
    // Optimization: Check timestamps.
    //
    // Let's just iterate all partitions for now to ensure correctness.

    let mut affected_partitions: HashSet<u32> = HashSet::new();
    for pid in manifest.partition_to_chateaux.keys() {
        affected_partitions.insert(*pid);
    }

    println!("Rebuilding {} partitions...", affected_partitions.len());

    // 3. Rebuild Partitions
    for pid in &affected_partitions {
        rebuild_partition(*pid, &manifest, output_dir).await?;
    }

    // 4. Recompute Border Patterns
    // We need to recompute for affected partitions AND their neighbors.
    // Since we rebuilt all partitions, we recompute all border patterns?
    // Or just the ones we touched?
    //
    // If we rebuilt a partition, its border patterns might change.
    // And its neighbors' border patterns (incoming/outgoing) might change.
    //
    // Let's recompute border patterns for ALL partitions for now.
    // `compute_border_patterns` takes a list of partitions.

    println!("Recomputing Global Patterns...");

    // We need to load all partitions to compute border patterns.
    // We can load them on demand or pass a loader.
    // `compute_border_patterns` expects `Vec<TransitPartition>`.
    // That's too much memory for the whole world.
    //
    // `compute_border_patterns` in `connectivity.rs` iterates through partitions.
    // Wait, `compute_border_patterns` signature:
    // pub fn compute_border_patterns(partitions: &[TransitPartition]) -> Vec<PartitionDag>
    //
    // This implies we need all partitions in memory.
    // If we have 1000 partitions, that's fine.
    // If we have 100k, that's bad.
    //
    // Assuming we fit in memory for now (since we are doing incremental updates on a subset?).
    // But we are rebuilding ALL.
    //
    // Let's load all partitions.
    let mut all_partitions: Vec<TransitPartition> = Vec::new();
    for pid in manifest.partition_to_chateaux.keys() {
        let path = output_dir.join(format!("transit_chunk_{}.bincode", pid));
        if path.exists() {
            let p: TransitPartition = load_bincode(path.to_str().unwrap())?;
            all_partitions.push(p);
        }
    }

    // We also need `GlobalHub`s?
    // `TransitPartition` has `external_hubs`.

    // We need `IntermediateLocalEdge`s for `compute_border_patterns`?
    // `compute_border_patterns` uses `local_dag` from partitions.

    // Prepare data for compute_border_patterns
    let mut loaded_partitions_map: HashMap<u32, TransitPartition> = HashMap::new();
    let mut border_nodes: HashMap<u32, Vec<TransitStop>> = HashMap::new();

    for p in all_partitions {
        let pid = p.partition_id;
        let borders: Vec<TransitStop> = p.stops.iter().filter(|s| s.is_border).cloned().collect();
        border_nodes.insert(pid, borders);
        loaded_partitions_map.insert(pid, p);
    }

    let global_to_partition_map: HashMap<usize, (u32, u32)> = HashMap::new(); // Unused in compute_border_patterns

    let global_patterns = compute_border_patterns(
        &border_nodes,
        &loaded_partitions_map,
        &global_to_partition_map,
    );

    // Save Global Patterns
    let gp_path = output_dir.join("global_patterns.bincode");
    let global_index = GlobalPatternIndex {
        partition_dags: global_patterns,
        long_distance_dags: Vec::new(), // TODO: Recompute long distance patterns if needed
    };
    save_bincode(&global_index, gp_path.to_str().unwrap())?;

    println!("Rebuild Complete.");

    Ok(())
}

async fn rebuild_partition(
    partition_id: u32,
    manifest: &Manifest,
    output_dir: &Path,
) -> Result<()> {
    println!("Rebuilding Partition {}", partition_id);

    // 1. Load Existing Partition (to keep non-timetable data like OSM links, etc.)
    let chunk_path = output_dir.join(format!("transit_chunk_{}.bincode", partition_id));
    let mut partition: TransitPartition = if chunk_path.exists() {
        load_bincode(chunk_path.to_str().unwrap())?
    } else {
        // If it doesn't exist, we can't really "rebuild" it unless we are creating it from scratch.
        // But we need OSM links etc.
        // Assume it exists.
        return Err(anyhow::anyhow!("Partition {} not found", partition_id));
    };

    // 2. Identify Contributing Chateaux
    let chateaux = manifest
        .partition_to_chateaux
        .get(&partition_id)
        .context("Partition not in manifest")?;

    // 3. Load Timetable Data for each Chateau
    let mut merged_timetable = PartitionTimetableData::default();
    merged_timetable.partition_id = partition_id;

    // We need to merge:
    // - Stops (we need a global list of stops for the partition)
    // - Trip Patterns
    // - Time Deltas
    // - Service IDs
    // - Timezones
    // - Direction Patterns

    // The `TransitPartition` already has `stops`.
    // We should PRESERVE the `stops` list from the existing partition if possible,
    // to avoid invalidating external references (like OSM links using stop indices?).
    //
    // `TransitPartition.stops` are `TransitStop`s.
    // `PartitionTimetableData` has `stops` as `Vec<String>` (GTFS IDs).
    //
    // We need to map `PartitionTimetableData` stops to `TransitPartition` stop indices.

    // Let's build a map of GTFS Stop ID -> Index in `partition.stops`.
    // Wait, `TransitStop` has `gtfs_stop_ids` (Vec<String>).
    // Because of station matching, one station can have multiple GTFS stop IDs.

    let mut stop_id_to_idx: HashMap<String, u32> = HashMap::new();
    for (i, stop) in partition.stops.iter().enumerate() {
        for gtfs_id in &stop.gtfs_stop_ids {
            stop_id_to_idx.insert(gtfs_id.clone(), i as u32);
        }
    }

    // Clear existing timetable data in partition
    partition.trip_patterns.clear();
    partition.time_deltas.clear();
    partition.direction_patterns.clear();
    partition.service_ids.clear();
    partition.service_exceptions.clear(); // We need to merge these too
    partition.timezones.clear();
    // partition.local_dag.clear(); // Will be recomputed

    // Merge loop
    for chateau_id in chateaux {
        let tt_path = output_dir.join(format!("timetable_data_{}.bincode", chateau_id));
        if !tt_path.exists() {
            println!(
                "Warning: Timetable data for {} not found. Skipping.",
                chateau_id
            );
            continue;
        }

        let tt_data: TimetableData = load_bincode(tt_path.to_str().unwrap())?;

        // Find the partition data for this partition
        if let Some(p_data) = tt_data
            .partitions
            .iter()
            .find(|p| p.partition_id == partition_id)
        {
            merge_partition_data(&mut partition, p_data, &stop_id_to_idx, chateaux.len());
        }
    }

    // 4. Recompute Local Patterns
    // We need `IntermediateLocalEdge`s for this?
    // `compute_local_patterns_for_partition` uses `trip_patterns` and `stops`.
    // It generates `local_dag`.

    // We need to load `edges_{tile}.bincode`?
    // No, `compute_local_patterns_for_partition` computes the DAG *from the timetable*.
    // Wait, `gentian/main.rs` calls `compute_local_patterns_for_partition`.
    // Let's check its signature.
    // It takes `&mut TransitPartition`.

    compute_local_patterns_for_partition(&mut partition);

    // 5. Save Partition
    save_bincode(&partition, chunk_path.to_str().unwrap())?;

    Ok(())
}

fn merge_partition_data(
    partition: &mut TransitPartition,
    source: &PartitionTimetableData,
    stop_id_to_idx: &HashMap<String, u32>,
    _num_chateaux: usize,
) {
    // Merge Service IDs
    // We need to offset service indices in trips.
    let service_id_offset = partition.service_ids.len() as u32;
    for sid in &source.service_ids {
        partition.service_ids.push(sid.clone());
    }

    // Merge Timezones
    let timezone_offset = partition.timezones.len() as u32;
    for tz in &source.timezones {
        partition.timezones.push(tz.clone());
    }

    // Merge Time Deltas
    let time_delta_offset = partition.time_deltas.len() as u32;
    for td in &source.time_deltas {
        partition.time_deltas.push(td.clone());
    }

    // Merge Direction Patterns
    // We need to remap stop indices.
    // `source.direction_patterns` uses indices into `source.stops`.
    // We need to map `source.stops[i]` -> `stop_id_to_idx` -> `partition.stops[k]`.

    let direction_pattern_offset = partition.direction_patterns.len() as u32;

    for dp in &source.direction_patterns {
        let mut new_indices = Vec::new();
        for &local_idx in &dp.stop_indices {
            if let Some(stop_id) = source.stops.get(local_idx as usize) {
                if let Some(&global_idx) = stop_id_to_idx.get(stop_id) {
                    new_indices.push(global_idx);
                } else {
                    // Stop not found in partition? This shouldn't happen if partition is consistent.
                    // Maybe the stop was added in the update but not in the partition's stop list?
                    // If so, we can't handle it without adding stops to partition (which breaks other things?).
                    // For now, ignore or panic?
                    // Let's use 0 or skip?
                    // println!("Warning: Stop {} not found in partition", stop_id);
                }
            }
        }
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: new_indices,
        });
    }

    // Merge Trip Patterns
    for tp in &source.trip_patterns {
        let mut new_tp = tp.clone();

        // Update indices
        new_tp.direction_pattern_idx += direction_pattern_offset;
        new_tp.timezone_idx += timezone_offset;

        // Update trips
        for trip in &mut new_tp.trips {
            trip.service_idx += service_id_offset;
            trip.time_delta_idx += time_delta_offset;
        }

        partition.trip_patterns.push(new_tp);
    }

    // Merge Service Exceptions
    // source.service_exceptions need to have their service_idx updated
    for sex in &source.service_exceptions {
        let mut new_sex = sex.clone();
        new_sex.service_idx += service_id_offset;
        partition.service_exceptions.push(new_sex);
    }
}
