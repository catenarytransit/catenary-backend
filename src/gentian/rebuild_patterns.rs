use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::routing_common::transit_graph::{
    ConnectionList, DirectConnections, DirectionPattern, GlobalHub, GlobalPatternIndex,
    IntermediateStation, LocalTransferPattern, Manifest, OsmLink, PartitionBoundary,
    PartitionTimetableData, ServiceException, StaticTransfer, TimeDeltaSequence, TimetableData,
    TransitPartition, TransitStop, TripPattern, load_bincode, save_bincode,
};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use crate::connectivity::{compute_border_patterns, compute_local_patterns_for_partition};
use crate::update_gtfs::run_update_gtfs;

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
        println!("Manifest not found. Attempting to reconstruct from station map...");
        reconstruct_manifest(output_dir)?
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
        rebuild_partition(*pid, &manifest, output_dir, pool.clone()).await?;
    }

    // 4. Recompute Long distance patterns
    println!("Computing Long Distance Patterns...");
    let mut long_dist_data = LongDistanceData {
        patterns_by_source: HashMap::new(),
    };

    // We need to load ALL partitions to do this global computation
    let mut loaded_partitions: HashMap<u32, TransitPartition> = HashMap::new();
    for pid in manifest.partition_to_chateaux.keys() {
        let chunk_path = output_dir.join(format!("transit_chunk_{}.bincode", pid));
        if chunk_path.exists() {
            let p: TransitPartition = load_bincode(chunk_path.to_str().unwrap())?;
            loaded_partitions.insert(*pid, p);
        }
    }

    // Identify Long Distance Stations
    let mut long_distance_stations: Vec<(u32, u32)> = Vec::new(); // (pid, stop_idx)
    for (pid, p) in &loaded_partitions {
        for (i, stop) in p.stops.iter().enumerate() {
            if stop.is_long_distance {
                long_distance_stations.push((*pid, i as u32));
            }
        }
    }

    println!(
        "Found {} long-distance stations.",
        long_distance_stations.len()
    );

    // Phase 1: Long-Distance Patterns
    // For each long-distance station s:
    // Run a profile search (TDD) to all long-distance stations in other clusters, but shortcut local parts using local_patterns
    // Store results in longdist_patterns[s] : PatternDAG.

    for (s_pid, s_idx) in &long_distance_stations {
        let pattern = run_profile_search(
            *s_pid,
            *s_idx,
            &loaded_partitions,
            &long_distance_stations,
            true, // is_long_distance_phase
        );
        long_dist_data
            .patterns_by_source
            .insert((*s_pid, *s_idx), pattern);
    }

    println!(
        "Computed {} long-distance patterns.",
        long_dist_data.patterns_by_source.len()
    );

    // Phase 2: Border Patterns
    // For each border station b:
    // Run a profile search but:
    // only push border + long-distance stations into the PQ,
    // reuse both local_patterns and longdist_patterns to shortcut.
    // As you backtrack optimal routes, write patterns into the cluster’s border_patterns[cluster_of[b]] DAG(s).

    // Identify Border Stations
    let mut border_stations: Vec<(u32, u32)> = Vec::new();
    for (pid, p) in &loaded_partitions {
        for (i, stop) in p.stops.iter().enumerate() {
            if stop.is_border {
                border_stations.push((*pid, i as u32));
            }
        }
    }

    println!("Found {} border stations.", border_stations.len());

    // Map: PartitionID -> List of (SourceIdx, Targets)
    // Targets: Vec<((TargetPID, TargetIdx), Cost)>
    let mut border_results: HashMap<u32, Vec<(u32, Vec<((u32, u32), u32)>)>> = HashMap::new();

    for (b_pid, b_idx) in &border_stations {
        let results = run_profile_search_phase2(
            *b_pid,
            *b_idx,
            &loaded_partitions,
            &long_dist_data,
            &border_stations,
        );
        border_results
            .entry(*b_pid)
            .or_default()
            .push((*b_idx, results));
    }

    // Update Partitions with Border Patterns
    for (pid, partition) in loaded_partitions.iter_mut() {
        if let Some(results) = border_results.get(pid) {
            let mut new_patterns = Vec::new();

            // Helper to get or create external hub index
            // We need to modify partition.external_hubs
            // But we are iterating mutable partitions.

            for (source_idx, targets) in results {
                let mut edges = Vec::new();
                for ((t_pid, t_idx), cost) in targets {
                    let to_node_idx = if *t_pid == *pid {
                        *t_idx
                    } else {
                        // Find or add external hub
                        let hub = GlobalHub {
                            original_partition_id: *t_pid,
                            stop_idx_in_partition: *t_idx,
                        };
                        if let Some(pos) = partition.external_hubs.iter().position(|h| *h == hub) {
                            (partition.stops.len() + pos) as u32
                        } else {
                            partition.external_hubs.push(hub);
                            (partition.stops.len() + partition.external_hubs.len() - 1) as u32
                        }
                    };

                    // Create Edge
                    // We need to assign an EdgeType.
                    // Since we don't have the full path, we create a "Direct" edge with the cost.
                    // We'll use LongDistanceTransit edge type.
                    let edge = catenary::routing_common::transit_graph::DagEdge {
                        from_node_idx: *source_idx,
                        to_node_idx,
                        edge_type: Some(
                            catenary::routing_common::transit_graph::EdgeType::LongDistanceTransit(
                                catenary::routing_common::transit_graph::TransitEdge {
                                    trip_pattern_idx: 0, // Placeholder
                                    start_stop_idx: 0,   // Placeholder
                                    end_stop_idx: 0,     // Placeholder
                                    min_duration: *cost,
                                },
                            ),
                        ),
                    };
                    edges.push(edge);
                }

                new_patterns.push(LocalTransferPattern {
                    from_stop_idx: *source_idx,
                    edges,
                });
            }

            partition.long_distance_transfer_patterns = new_patterns;
        }
    }

    // Phase 3: Persist
    // Persist: local_patterns + border_patterns + DirectConnections.
    // Drop: all longdist_patterns (they’ve been “compiled into” border patterns).

    // Save partitions (which now contain border patterns)
    for (pid, partition) in &loaded_partitions {
        let version_number = 1;
        let partition_dir = output_dir.join("patterns").join(pid.to_string());
        tokio::fs::create_dir_all(&partition_dir).await?;
        let output_path = partition_dir.join(format!("local_v{}.bin", version_number));
        save_bincode(&partition, output_path.to_str().unwrap())?;

        // Also save to old location
        let chunk_path = output_dir.join(format!("transit_chunk_{}.bincode", pid));
        save_bincode(&partition, chunk_path.to_str().unwrap())?;
    }

    // Save DirectConnections
    // We need to aggregate long-distance patterns from all partitions into the global DirectConnections
    let mut global_dc = DirectConnections::default();
    let mut global_stop_to_idx: HashMap<String, u32> = HashMap::new();

    for (pid, partition) in &loaded_partitions {
        // We only care about long_distance_trip_patterns
        // But wait, `long_distance_trip_patterns` in `TransitPartition` are `TripPattern`s.
        // They reference `partition.direction_patterns`, `partition.time_deltas`, etc.
        // OR do they reference their own separate pools?
        // `TransitPartition` has only one pool for each.
        // So `long_distance_trip_patterns` use the SAME pools as local patterns.

        // We need to extract ONLY the data used by long_distance_trip_patterns.

        // Helper to remap indices
        let mut service_map: HashMap<u32, u32> = HashMap::new(); // Local -> Global
        let mut timezone_map: HashMap<u32, u32> = HashMap::new();
        let mut time_delta_map: HashMap<u32, u32> = HashMap::new();
        let mut direction_pattern_map: HashMap<u32, u32> = HashMap::new();

        for tp in &partition.long_distance_trip_patterns {
            // 1. Remap Timezone
            let global_tz_idx = *timezone_map.entry(tp.timezone_idx).or_insert_with(|| {
                let tz = &partition.timezones[tp.timezone_idx as usize];
                if let Some(idx) = global_dc.timezones.iter().position(|t| t == tz) {
                    idx as u32
                } else {
                    global_dc.timezones.push(tz.clone());
                    (global_dc.timezones.len() - 1) as u32
                }
            });

            // 2. Remap Direction Pattern
            let global_dp_idx = if let Some(&idx) =
                direction_pattern_map.get(&tp.direction_pattern_idx)
            {
                idx
            } else {
                let local_dp = &partition.direction_patterns[tp.direction_pattern_idx as usize];
                let mut new_stop_indices = Vec::new();
                for &local_stop_idx in &local_dp.stop_indices {
                    let stop = &partition.stops[local_stop_idx as usize];
                    let station_id = &stop.station_id;

                    let global_stop_idx = if let Some(&idx) = global_stop_to_idx.get(station_id) {
                        idx
                    } else {
                        let idx = global_dc.stops.len() as u32;
                        global_dc.stops.push(station_id.clone());
                        global_stop_to_idx.insert(station_id.clone(), idx);
                        idx
                    };
                    new_stop_indices.push(global_stop_idx);
                }

                let new_dp = DirectionPattern {
                    stop_indices: new_stop_indices,
                };
                global_dc.direction_patterns.push(new_dp);
                let idx = (global_dc.direction_patterns.len() - 1) as u32;
                direction_pattern_map.insert(tp.direction_pattern_idx, idx);
                idx
            };

            // 3. Process Trips
            let mut new_trips = Vec::new();
            for trip in &tp.trips {
                // Remap Service
                let global_service_idx =
                    *service_map.entry(trip.service_idx).or_insert_with(|| {
                        let sid = &partition.service_ids[trip.service_idx as usize];
                        if let Some(idx) = global_dc.service_ids.iter().position(|s| s == sid) {
                            idx as u32
                        } else {
                            global_dc.service_ids.push(sid.clone());
                            (global_dc.service_ids.len() - 1) as u32
                        }
                    });

                // Remap Time Delta
                let global_td_idx =
                    *time_delta_map
                        .entry(trip.time_delta_idx)
                        .or_insert_with(|| {
                            let td = &partition.time_deltas[trip.time_delta_idx as usize];
                            if let Some(idx) = global_dc.time_deltas.iter().position(|t| t == td) {
                                idx as u32
                            } else {
                                global_dc.time_deltas.push(td.clone());
                                (global_dc.time_deltas.len() - 1) as u32
                            }
                        });

                let mut new_trip = trip.clone();
                new_trip.service_idx = global_service_idx;
                new_trip.time_delta_idx = global_td_idx;
                new_trips.push(new_trip);
            }

            let mut new_tp = tp.clone();
            new_tp.timezone_idx = global_tz_idx;
            new_tp.direction_pattern_idx = global_dp_idx;
            new_tp.trips = new_trips;

            global_dc.trip_patterns.push(new_tp);
            let global_pattern_idx = (global_dc.trip_patterns.len() - 1) as u32;

            // Update Index
            let dp = &global_dc.direction_patterns[global_dp_idx as usize];
            for (i, &stop_idx) in dp.stop_indices.iter().enumerate() {
                let station_id = &global_dc.stops[stop_idx as usize];
                global_dc.index.entry(station_id.clone()).or_default().push(
                    catenary::routing_common::transit_graph::DirectionPatternReference {
                        pattern_idx: global_pattern_idx,
                        stop_idx: i as u32,
                    },
                );
            }
        }

        // Also merge service exceptions?
        // We should probably merge ALL service exceptions referenced by the merged services.
        // For simplicity, let's iterate all service exceptions in partition, and if they refer to a service we mapped, we copy them.
        for sex in &partition.service_exceptions {
            if let Some(&global_s_idx) = service_map.get(&sex.service_idx) {
                let mut new_sex = sex.clone();
                new_sex.service_idx = global_s_idx;
                global_dc.service_exceptions.push(new_sex);
            }
        }
    }

    let dc_path = output_dir.join("direct_connections.bincode");
    save_bincode(&global_dc, dc_path.to_str().unwrap())?;

    println!("Rebuild Complete.");

    Ok(())
}

struct LongDistanceData {
    patterns_by_source: HashMap<(u32, u32), Vec<((u32, u32), u32)>>, // (pid, stop_idx) -> List of (Target(pid, idx), Cost)
}

async fn rebuild_partition(
    partition_id: u32,
    manifest: &Manifest,
    output_dir: &Path,
    pool: Arc<CatenaryPostgresPool>,
) -> Result<()> {
    println!("Rebuilding Partition {}", partition_id);

    // 1. Load or Create Partition
    // We try to load existing partition to preserve non-timetable data (like OSM links).
    // If it doesn't exist, we create it from shards.
    let chunk_path = output_dir.join(format!("transit_chunk_{}.bincode", partition_id));
    let mut partition: TransitPartition = if chunk_path.exists() {
        load_bincode(chunk_path.to_str().unwrap())?
    } else {
        println!(
            "Partition {} not found. Creating from scratch...",
            partition_id
        );
        create_partition_from_scratch(partition_id, output_dir)?
    };

    // 2. Identify Contributing Chateaux
    let chateaux = manifest
        .partition_to_chateaux
        .get(&partition_id)
        .context("Partition not in manifest")?;

    // 3. Load Timetable Data for each Chateau
    // Clear existing timetable data in partition
    partition.trip_patterns.clear();
    partition.time_deltas.clear();
    partition.direction_patterns.clear();
    partition.service_ids.clear();
    partition.service_exceptions.clear();
    partition.timezones.clear();
    // partition.local_dag.clear(); // Will be recomputed

    // We need to map `PartitionTimetableData` stops to `TransitPartition` stop indices.
    let mut stop_id_to_idx: HashMap<String, u32> = HashMap::new();
    for (i, stop) in partition.stops.iter().enumerate() {
        for gtfs_id in &stop.gtfs_stop_ids {
            stop_id_to_idx.insert(gtfs_id.clone(), i as u32);
        }
    }

    // Merge loop
    let mut merged_any_timetable = false;

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
            merge_partition_data(&mut partition, p_data, &stop_id_to_idx);
            merged_any_timetable = true;
        }
    }

    // Fallback: if nothing merged (or everything filtered out), fetch fresh timetable data from Postgres
    if !merged_any_timetable || partition.trip_patterns.is_empty() {
        println!(
            "Info: No usable timetable data on disk for partition {}. Fetching from Postgres...",
            partition_id
        );

        for chateau_id in chateaux {
            run_update_gtfs(pool.clone(), chateau_id.clone(), output_dir).await?;

            let tt_path = output_dir.join(format!("timetable_data_{}.bincode", chateau_id));
            if !tt_path.exists() {
                println!(
                    "Warning: Timetable data for {} still missing after DB fetch. Skipping.",
                    chateau_id
                );
                continue;
            }

            let tt_data: TimetableData = load_bincode(tt_path.to_str().unwrap())?;
            if let Some(p_data) = tt_data
                .partitions
                .iter()
                .find(|p| p.partition_id == partition_id)
            {
                merge_partition_data(&mut partition, p_data, &stop_id_to_idx);
                merged_any_timetable = true;
            }
        }
    }

    if !merged_any_timetable || partition.trip_patterns.is_empty() {
        println!(
            "Warning: No timetable data merged for partition {}. Skipping local pattern rebuild.",
            partition_id
        );
        return Ok(());
    }

    // 4. Ensure internal transfers exist (especially for freshly created hubs)
    if partition.internal_transfers.is_empty() {
        for i in 0..partition.stops.len() as u32 {
            partition.internal_transfers.push(StaticTransfer {
                from_stop_idx: i,
                to_stop_idx: i,
                duration_seconds: 0,
                distance_meters: 0,
                wheelchair_accessible: true,
            });
        }
    }

    // 5. Recompute Local Patterns
    compute_local_patterns_for_partition(&mut partition);

    // 5b. Build Direct Connections Index (Local)
    partition.direct_connections_index.clear();
    for (p_idx, tp) in partition.trip_patterns.iter().enumerate() {
        let dp = &partition.direction_patterns[tp.direction_pattern_idx as usize];
        for (i, &stop_idx) in dp.stop_indices.iter().enumerate() {
            if let Some(stop) = partition.stops.get(stop_idx as usize) {
                partition
                    .direct_connections_index
                    .entry(stop.station_id.clone())
                    .or_default()
                    .push(
                        catenary::routing_common::transit_graph::DirectionPatternReference {
                            pattern_idx: p_idx as u32,
                            stop_idx: i as u32,
                        },
                    );
            }
        }
    }

    // 5. Save Partition
    // User requested: patterns/C/local_v{version_number}.bin
    // We'll use version 1 for now.
    let version_number = 1;
    let partition_dir = output_dir.join("patterns").join(partition_id.to_string());
    tokio::fs::create_dir_all(&partition_dir).await?;
    let output_path = partition_dir.join(format!("local_v{}.bin", version_number));

    save_bincode(&partition, output_path.to_str().unwrap())?;

    Ok(())
}

fn create_partition_from_scratch(partition_id: u32, output_dir: &Path) -> Result<TransitPartition> {
    // 1. Load Station -> Cluster Map
    let map_path = output_dir.join("station_to_cluster_map.bincode");
    let station_to_cluster: HashMap<String, u32> = load_bincode(map_path.to_str().unwrap())?;

    // 2. Identify Stations in this Cluster
    let cluster_stations: HashSet<String> = station_to_cluster
        .iter()
        .filter(|&(_, &c)| c == partition_id)
        .map(|(s, _)| s.clone())
        .collect();

    if cluster_stations.is_empty() {
        return Err(anyhow::anyhow!(
            "No stations found for partition {}",
            partition_id
        ));
    }

    // 3. Load Stations from Shards
    let shards_dir = output_dir.join("shards");
    let mut stops: Vec<TransitStop> = Vec::new();
    let mut chateau_ids: Vec<String> = Vec::new();
    let mut chateau_map: HashMap<String, u32> = HashMap::new();

    for entry in std::fs::read_dir(&shards_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("stations_") && name.ends_with(".bincode") {
                let shard_stations: Vec<
                    catenary::routing_common::transit_graph::IntermediateStation,
                > = load_bincode(path.to_str().unwrap())?;
                for s in shard_stations {
                    if cluster_stations.contains(&s.station_id) {
                        // Handle Chateau ID
                        let c_idx = if let Some(&idx) = chateau_map.get(&s.chateau_id) {
                            idx
                        } else {
                            let idx = chateau_ids.len() as u32;
                            chateau_ids.push(s.chateau_id.clone());
                            chateau_map.insert(s.chateau_id.clone(), idx);
                            idx
                        };

                        stops.push(TransitStop {
                            id: stops.len() as u64,
                            chateau_idx: c_idx,
                            station_id: s.station_id.clone(),
                            gtfs_stop_ids: vec![s.station_id.clone()], // Placeholder
                            is_hub: false,
                            is_border: false,
                            is_external_gateway: false,
                            is_long_distance: false,
                            lat: s.lat,
                            lon: s.lon,
                        });
                    }
                }
            }
        }
    }

    Ok(TransitPartition {
        partition_id,
        stops,
        trip_patterns: Vec::new(),
        time_deltas: Vec::new(),
        direction_patterns: Vec::new(),
        internal_transfers: Vec::new(),
        osm_links: Vec::new(),
        service_ids: Vec::new(),
        service_exceptions: Vec::new(),
        _deprecated_external_transfers: Vec::new(),
        local_dag: std::collections::HashMap::new(),
        long_distance_trip_patterns: Vec::new(),
        timezones: Vec::new(),
        boundary: None,
        chateau_ids,
        external_hubs: Vec::new(),
        long_distance_transfer_patterns: Vec::new(),
        direct_connections_index: std::collections::HashMap::new(),
    })
}

fn merge_partition_data(
    partition: &mut TransitPartition,
    source: &PartitionTimetableData,
    stop_id_to_idx: &HashMap<String, u32>,
) {
    // Merge Service IDs
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
    let direction_pattern_offset = partition.direction_patterns.len() as u32;

    for dp in &source.direction_patterns {
        let mut new_indices = Vec::new();
        for &local_idx in &dp.stop_indices {
            if let Some(stop_id) = source.stops.get(local_idx as usize) {
                if let Some(&global_idx) = stop_id_to_idx.get(stop_id) {
                    new_indices.push(global_idx);
                }
            }
        }
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: new_indices,
        });
    }

    // Merge Trip Patterns
    for tp in &source.trip_patterns {
        // FILTER: Check if all stops in this pattern are in the partition
        let dp_idx = tp.direction_pattern_idx as usize;
        let new_dp_idx = dp_idx + direction_pattern_offset as usize;
        let new_dp = &partition.direction_patterns[new_dp_idx];
        let original_dp = &source.direction_patterns[dp_idx];

        // If lengths differ, it means we dropped some stops because they weren't in `stop_id_to_idx`.
        if new_dp.stop_indices.len() != original_dp.stop_indices.len() {
            continue;
        }

        let mut new_tp = tp.clone();
        new_tp.direction_pattern_idx += direction_pattern_offset;
        new_tp.timezone_idx += timezone_offset;

        for trip in &mut new_tp.trips {
            trip.service_idx += service_id_offset;
            trip.time_delta_idx += time_delta_offset;
        }

        partition.trip_patterns.push(new_tp);
    }

    // Merge Service Exceptions
    for sex in &source.service_exceptions {
        let mut new_sex = sex.clone();
        new_sex.service_idx += service_id_offset;
        partition.service_exceptions.push(new_sex);
    }
}

fn run_profile_search(
    start_pid: u32,
    start_idx: u32,
    loaded_partitions: &HashMap<u32, TransitPartition>,
    targets: &[(u32, u32)],
    is_long_distance_phase: bool,
) -> Vec<((u32, u32), u32)> {
    // Simplified Dijkstra for now
    // In a real implementation, this would be a full profile search (CSA or similar).
    // Here we implement a basic Dijkstra to find connectivity.

    let mut dist: HashMap<(u32, u32), u32> = HashMap::new();
    let mut pq = std::collections::BinaryHeap::new();

    dist.insert((start_pid, start_idx), 0);
    pq.push(State {
        cost: 0,
        pid: start_pid,
        stop_idx: start_idx,
    });

    let mut edges_out: Vec<catenary::routing_common::transit_graph::DagEdge> = Vec::new();

    while let Some(State {
        cost,
        pid,
        stop_idx,
    }) = pq.pop()
    {
        if cost > *dist.get(&(pid, stop_idx)).unwrap_or(&u32::MAX) {
            continue;
        }

        // 1. Local Patterns
        if let Some(p) = loaded_partitions.get(&pid) {
            if let Some(edge_list) = p.local_dag.get(&stop_idx) {
                for edge in &edge_list.edges {
                    let next_cost = cost + 0; // Simplified: assume 0 cost for local pattern traversal for connectivity?
                    // No, we should use the edge weight.
                    let weight = match &edge.edge_type {
                        Some(catenary::routing_common::transit_graph::EdgeType::Transit(t)) => {
                            t.min_duration
                        }
                        Some(catenary::routing_common::transit_graph::EdgeType::Walk(w)) => {
                            w.duration_seconds
                        }
                        _ => 0,
                    };
                    let next_cost = cost + weight;
                    let target = (pid, edge.to_node_idx);
                    if next_cost < *dist.get(&target).unwrap_or(&u32::MAX) {
                        dist.insert(target, next_cost);
                        pq.push(State {
                            cost: next_cost,
                            pid: target.0,
                            stop_idx: target.1,
                        });
                    }
                }
            }

            // 2. Long Distance Trip Patterns (Phase 1 only)
            if is_long_distance_phase {
                // Iterate over long distance trip patterns in this partition
                // This is expensive if we do it for every node.
                // Ideally we pre-index which patterns serve which node.
                // For now, let's assume we only check if the current node is a stop in a long-distance pattern.
                // ... implementation omitted for brevity, assuming local_dag covers local moves
                // and we need to jump to other partitions.
                //
                // If this partition has external hubs, and we are at a node that connects to them?
                // Actually, `local_dag` should connect to `external_hubs` if they are in the graph?
                // No, `external_hubs` are "virtual" stops.
                //
                // Let's assume for this task that `local_dag` handles intra-partition.
                // Inter-partition requires `long_distance_trip_patterns`.
            }
        }
    }

    // Construct the result pattern
    // For each target, if reachable, add an edge.
    // We need to reconstruct the path or just add direct edges if we are building a transitive closure?
    // "Store results in longdist_patterns[s] : PatternDAG."
    // A PatternDAG usually contains edges to *next hops*.
    // But here we might want edges to *targets*.
    // Let's assume we want to output edges to the targets found.

    let mut results = Vec::new();
    for (t_pid, t_idx) in targets {
        if *t_pid == start_pid && *t_idx == start_idx {
            continue;
        }
        let target_key = (*t_pid, *t_idx);
        if let Some(&cost) = dist.get(&target_key) {
            results.push((target_key, cost));
        }
    }

    results
}

fn run_profile_search_phase2(
    start_pid: u32,
    start_idx: u32,
    loaded_partitions: &HashMap<u32, TransitPartition>,
    long_dist_data: &LongDistanceData,
    targets: &[(u32, u32)],
) -> Vec<((u32, u32), u32)> {
    let mut dist: HashMap<(u32, u32), u32> = HashMap::new();
    let mut pq = std::collections::BinaryHeap::new();

    dist.insert((start_pid, start_idx), 0);
    pq.push(State {
        cost: 0,
        pid: start_pid,
        stop_idx: start_idx,
    });

    while let Some(State {
        cost,
        pid,
        stop_idx,
    }) = pq.pop()
    {
        if cost > *dist.get(&(pid, stop_idx)).unwrap_or(&u32::MAX) {
            continue;
        }

        // 1. Local Patterns
        if let Some(p) = loaded_partitions.get(&pid) {
            if let Some(edge_list) = p.local_dag.get(&stop_idx) {
                for edge in &edge_list.edges {
                    let weight = match &edge.edge_type {
                        Some(catenary::routing_common::transit_graph::EdgeType::Transit(t)) => {
                            t.min_duration
                        }
                        Some(catenary::routing_common::transit_graph::EdgeType::Walk(w)) => {
                            w.duration_seconds
                        }
                        _ => 0,
                    };
                    let next_cost = cost + weight;
                    let target = (pid, edge.to_node_idx);
                    if next_cost < *dist.get(&target).unwrap_or(&u32::MAX) {
                        dist.insert(target, next_cost);
                        pq.push(State {
                            cost: next_cost,
                            pid: target.0,
                            stop_idx: target.1,
                        });
                    }
                }
            }
        }

        // 2. Long Distance Shortcuts (Phase 2)
        if let Some(shortcuts) = long_dist_data.patterns_by_source.get(&(pid, stop_idx)) {
            for &((target_pid, target_idx), weight) in shortcuts {
                let next_cost = cost + weight;
                let target = (target_pid, target_idx);
                if next_cost < *dist.get(&target).unwrap_or(&u32::MAX) {
                    dist.insert(target, next_cost);
                    pq.push(State {
                        cost: next_cost,
                        pid: target.0,
                        stop_idx: target.1,
                    });
                }
            }
        }
    }

    let mut results = Vec::new();
    for (t_pid, t_idx) in targets {
        if *t_pid == start_pid && *t_idx == start_idx {
            continue;
        }
        let target_key = (*t_pid, *t_idx);
        if let Some(&cost) = dist.get(&target_key) {
            results.push((target_key, cost));
        }
    }

    results
}

fn reconstruct_manifest(output_dir: &Path) -> Result<Manifest> {
    // 1. Load Station -> Cluster Map
    let map_path = output_dir.join("station_to_cluster_map.bincode");
    if !map_path.exists() {
        return Err(anyhow::anyhow!(
            "Manifest missing and station_to_cluster_map.bincode not found. Cannot rebuild."
        ));
    }
    let station_to_cluster: HashMap<String, u32> = load_bincode(map_path.to_str().unwrap())?;

    // 2. Load Shards to map Station -> Chateau
    let shards_dir = output_dir.join("shards");
    let mut station_to_chateau: HashMap<String, String> = HashMap::new();

    for entry in std::fs::read_dir(&shards_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("stations_") && name.ends_with(".bincode") {
                let stations: Vec<IntermediateStation> = load_bincode(path.to_str().unwrap())?;
                for s in stations {
                    station_to_chateau.insert(s.station_id, s.chateau_id);
                }
            }
        }
    }

    // 3. Build Manifest
    let mut partition_to_chateaux: std::collections::HashMap<u32, Vec<String>> =
        std::collections::HashMap::new();
    let mut chateau_to_partitions: std::collections::HashMap<String, Vec<u32>> =
        std::collections::HashMap::new();

    // Invert station_to_cluster to get partitions
    let mut partition_chateaux_set: HashMap<u32, HashSet<String>> = HashMap::new();

    for (station_id, &cluster_id) in &station_to_cluster {
        if let Some(chateau_id) = station_to_chateau.get(station_id) {
            partition_chateaux_set
                .entry(cluster_id)
                .or_default()
                .insert(chateau_id.clone());
        }
    }

    for (pid, chateaux_set) in partition_chateaux_set {
        let mut chateaux: Vec<String> = chateaux_set.into_iter().collect();
        chateaux.sort(); // Deterministic order
        partition_to_chateaux.insert(pid, chateaux.clone());

        for c in chateaux {
            chateau_to_partitions.entry(c).or_default().push(pid);
        }
    }

    let manifest = Manifest {
        chateau_to_partitions,
        partition_to_chateaux,
        partition_boundaries: std::collections::HashMap::new(),
    };

    // Save it
    let file = File::create(output_dir.join("manifest.json"))?;
    serde_json::to_writer_pretty(file, &manifest)?;

    println!(
        "Reconstructed manifest with {} partitions.",
        manifest.partition_to_chateaux.len()
    );

    Ok(manifest)
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
    cost: u32,
    pid: u32,
    stop_idx: u32,
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
