use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::{Context, Result};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::routing_common::transit_graph::{
    ConnectionList, DirectConnections, DirectionPattern, GlobalHub, GlobalPatternIndex,
    IntermediateStation, LocalTransferPattern, Manifest, OsmLink, PartitionBoundary,
    PartitionTimetableData, ServiceException, StaticTransfer, TimeDeltaSequence, TimetableData,
    TransitPartition, TransitStop, TripPattern, load_bincode, save_bincode,
};

use rayon::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use crate::connectivity::{compute_border_patterns, compute_local_patterns_for_partition};
use crate::osm::ChunkCache;
use crate::pathfinding::compute_osm_walk;
use crate::trip_based::{
    ProfileScratch, build_stop_to_patterns, compute_initial_transfers, compute_one_to_all_profile,
    compute_profile_query, get_departure_time, refine_transfers, remove_u_turn_transfers,
    run_trip_based_profile,
};
use crate::update_gtfs::run_update_gtfs;
use crate::utils::{haversine_distance, lon_lat_to_tile};
use catenary::routing_common::transit_graph::{DagEdge, EdgeType, TransitEdge, WalkEdge};

/// Run the pattern rebuild process.
/// This assumes `update-gtfs` has been run for modified chateaux.
/// It rebuilds partitions that contain modified chateaux, then updates border patterns.
pub async fn run_rebuild_patterns(
    pool: Arc<CatenaryPostgresPool>,
    output_dir: &Path,
    target_partitions: Option<Vec<u32>>,
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
    let manifest = Arc::new(manifest);

    let mut affected_partitions: HashSet<u32> = HashSet::new();
    if let Some(targets) = target_partitions {
        println!("Filtering to {} requested partitions.", targets.len());
        for pid in targets {
            if manifest.partition_to_chateaux.contains_key(&pid) {
                affected_partitions.insert(pid);
            } else {
                println!(
                    "Warning: Requested partition {} not found in manifest.",
                    pid
                );
                affected_partitions.insert(pid);
            }
        }
    } else {
        for pid in manifest.partition_to_chateaux.keys() {
            affected_partitions.insert(*pid);
        }
    }

    println!("Rebuilding {} partitions...", affected_partitions.len());

    // 3. Rebuild Partitions (Local)
    // This step writes the initial local_v1.bin files.
    // 3. Rebuild Partitions (Local)
    // This step writes the initial local_v1.bin files.
    // Use Rayon for CPU-bound tasks, with dedicated threads.
    println!("Using Rayon for partition rebuild (will autopick thread count).");

    let affected_partitions_vec: Vec<u32> = affected_partitions.into_iter().collect();
    let rt_handle = tokio::runtime::Handle::current();

    affected_partitions_vec.par_iter().try_for_each(|pid| {
        let pool = pool.clone();
        let manifest = manifest.clone();
        let output_dir = output_dir.to_path_buf();
        rebuild_partition(*pid, &manifest, &output_dir, pool, &rt_handle)
    })?;

    // 4. Global Connectivity Phase
    // Memory Optimization:
    // We process DirectConnections (timetable aggregation) incrementally while loading partitions,
    // then STRIP the heavy timetable data from the partition in memory before proceeding to the global graph search.

    println!("Computing Global Connectivity...");

    // Prepare Global DirectConnections structures
    let mut global_dc = DirectConnections::default();
    let mut global_stop_to_idx: HashMap<String, u32> = HashMap::new();
    let mut loaded_partitions: HashMap<u32, TransitPartition> = HashMap::new();

    for pid in manifest.partition_to_chateaux.keys() {
        let chunk_path = output_dir
            .join("patterns")
            .join(pid.to_string())
            .join("local_v1.bin");

        if chunk_path.exists() {
            let mut p: TransitPartition = load_bincode(chunk_path.to_str().unwrap())?;

            // --- A. Process DirectConnections (Merge into Global) ---

            // Helper to remap indices for this partition
            let mut service_map: HashMap<u32, u32> = HashMap::new(); // Local -> Global
            let mut timezone_map: HashMap<u32, u32> = HashMap::new();
            let mut time_delta_map: HashMap<u32, u32> = HashMap::new();
            let mut direction_pattern_map: HashMap<u32, u32> = HashMap::new();

            for tp in &p.long_distance_trip_patterns {
                // 1. Remap Timezone
                let global_tz_idx = *timezone_map.entry(tp.timezone_idx).or_insert_with(|| {
                    let tz = &p.timezones[tp.timezone_idx as usize];
                    if let Some(idx) = global_dc.timezones.iter().position(|t| t == tz) {
                        idx as u32
                    } else {
                        global_dc.timezones.push(tz.clone());
                        (global_dc.timezones.len() - 1) as u32
                    }
                });

                // 2. Remap Direction Patterns
                let global_dp_idx =
                    if let Some(&idx) = direction_pattern_map.get(&tp.direction_pattern_idx) {
                        idx
                    } else {
                        let local_dp = &p.direction_patterns[tp.direction_pattern_idx as usize];
                        let mut new_stop_indices = Vec::new();
                        for &local_stop_idx in &local_dp.stop_indices {
                            let stop = &p.stops[local_stop_idx as usize];
                            let station_id = &stop.station_id;

                            let global_stop_idx =
                                if let Some(&idx) = global_stop_to_idx.get(station_id) {
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
                            let sid = &p.service_ids[trip.service_idx as usize];
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
                                let td = &p.time_deltas[trip.time_delta_idx as usize];
                                if let Some(idx) =
                                    global_dc.time_deltas.iter().position(|t| t == td)
                                {
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

            // Merge Service Exceptions (referenced by mapped services)
            for service_exception in &p.service_exceptions {
                if let Some(&global_s_idx) = service_map.get(&service_exception.service_idx) {
                    let mut new_service_exception = service_exception.clone();
                    new_service_exception.service_idx = global_s_idx;
                    global_dc.service_exceptions.push(new_service_exception);
                }
            }

            // --- B. Strip Partition for Memory Efficiency ---
            // We retain ONLY what is needed for:
            // 1. Identifying long-distance/border stations (p.stops)
            // 2. Running local DAG traversal (p.local_dag)
            // 3. Storing new external hubs (p.external_hubs) - effectively outputs
            // 4. Storing new long-dist transfer patterns (p.long_distance_transfer_patterns) - outputs

            // --- B. Strip Partition for Memory Efficiency ---
            // DISABLED for Trip-Based Routing: We need full timetable data in memory.
            /*
            p.trip_patterns = Vec::new();
            p.time_deltas = Vec::new();
            p.direction_patterns = Vec::new(); // NOTE: local_dag uses indices into stops, not direction patterns.
            p.service_ids = Vec::new();
            p.service_exceptions = Vec::new();
            p.timezones = Vec::new();
            p.long_distance_trip_patterns = Vec::new(); // Already merged
            p.direct_connections_index = std::collections::HashMap::new(); // Not needed for graph search topology

            // We strip stops meta-data that isn't needed, but keeping the vector of stops is required for indexing.
            // We can clear `gtfs_stop_ids` to save some string memory.
            for stop in &mut p.stops {
                stop.gtfs_stop_ids = Vec::new();
            }
            */

            loaded_partitions.insert(*pid, p);
        }
    }

    // Save DirectConnections immediately
    let dc_path = output_dir.join("direct_connections.bincode");
    save_bincode(&global_dc, dc_path.to_str().unwrap())?;
    println!(
        "Saved DirectConnections with {} patterns.",
        global_dc.trip_patterns.len()
    );

    // Free up Global DC memory
    drop(global_dc);
    drop(global_stop_to_idx);

    // Identify Long Distance Stations (using stripped partitions)
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
    println!("Phase 1: Computing Long Distance Patterns...");
    let mut long_dist_data = LongDistanceData {
        patterns_by_source: HashMap::new(),
    };

    let patterns_vec: Vec<((u32, u32), Vec<((u32, u32), Vec<(u32, u32)>)>)> =
        long_distance_stations
            .par_iter()
            .map(|(s_pid, s_idx)| {
                let profile = run_profile_search(
                    *s_pid,
                    *s_idx,
                    &loaded_partitions,
                    &long_distance_stations,
                    None,
                );
                ((*s_pid, *s_idx), profile)
            })
            .collect();

    for (key, val) in patterns_vec {
        long_dist_data.patterns_by_source.insert(key, val);
    }

    println!(
        "Computed {} long-distance patterns.",
        long_dist_data.patterns_by_source.len()
    );

    // Phase 2: Border Patterns
    println!("Phase 2: Computing Border Patterns...");

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
    let mut border_results: HashMap<u32, Vec<(u32, Vec<((u32, u32), Vec<(u32, u32)>)>)>> =
        HashMap::new();

    // REWRITE: Precise Injection
    for ((src_pid, src_idx), targets) in &long_dist_data.patterns_by_source {
        let partition = loaded_partitions.get_mut(src_pid).unwrap();
        for ((tgt_pid, tgt_idx), pareto) in targets {
            if *src_pid != *tgt_pid {
                continue;
            }
            if pareto.is_empty() {
                continue;
            }

            // 1. Create DirectionPattern
            let dp = DirectionPattern {
                stop_indices: vec![*src_idx, *tgt_idx],
            };
            partition.direction_patterns.push(dp);
            let dp_idx = (partition.direction_patterns.len() - 1) as u32;

            // 2. Create TimeDeltas
            // We can create one TimeDelta per trip OR try to share.
            // Sharing is better for memory.
            // But pareto points differ.
            // For simplicity: Create one TimeDelta per trip.

            let mut trips = Vec::new();
            for (dep, arr) in pareto {
                let duration = arr - dep;
                let tds = TimeDeltaSequence {
                    deltas: vec![duration, 0], // Travel, Dwell
                };
                partition.time_deltas.push(tds);
                let td_idx = (partition.time_deltas.len() - 1) as u32;

                trips.push(catenary::routing_common::transit_graph::CompressedTrip {
                    gtfs_trip_id: "synthetic".to_string(),
                    service_mask: u32::MAX,
                    start_time: *dep,
                    time_delta_idx: td_idx,
                    service_idx: 0,
                    bikes_allowed: 0,
                    wheelchair_accessible: 0,
                });
            }

            // 3. Create TripPattern
            let pattern = TripPattern {
                chateau_idx: 0,
                route_id: "synthetic".to_string(),
                direction_pattern_idx: dp_idx,
                trips,
                timezone_idx: 0,
                route_type: 0,
                is_border: false,
            };
            partition.trip_patterns.push(pattern);
        }
    }

    let results_vec: Vec<(u32, u32, Vec<((u32, u32), Vec<(u32, u32)>)>)> = border_stations
        .par_iter()
        .map(|(b_pid, b_idx)| {
            let results =
                run_profile_search_phase2(*b_pid, *b_idx, &loaded_partitions, &border_stations);
            (*b_pid, *b_idx, results)
        })
        .collect();

    for (b_pid, b_idx, results) in results_vec {
        border_results
            .entry(b_pid)
            .or_default()
            .push((b_idx, results));
    }

    // Combine Results
    let mut combined_results: HashMap<u32, Vec<(u32, Vec<((u32, u32), Vec<(u32, u32)>)>)>> =
        HashMap::new();

    // 1. Add Border Results
    for (pid, results_list) in border_results {
        combined_results.insert(pid, results_list);
    }

    // 2. Add Long Distance Results
    // We need to invert the mapping from (pid, idx) -> results to pid -> list of (idx, results)
    for ((pid, idx), patterns) in long_dist_data.patterns_by_source {
        combined_results
            .entry(pid)
            .or_default()
            .push((idx, patterns));
    }

    // Update Partitions with Combined Patterns
    // Note: We are updating the STRIPPED partitions in memory first with the results.
    // However, to save, we need to load the FULL partition again and merge.

    println!("Phase 3: Saving Updated Partitions...");

    // Iterate over IDs we have modified
    // We actually iterate over "combined_results" keys because those are the affected ones.
    for (pid, results_list) in combined_results {
        // Load FULL partition again
        let partition_dir = output_dir.join("patterns").join(pid.to_string());
        let chunk_path = partition_dir.join("local_v1.bin");

        // If partition doesn't exist (e.g. it was skipped?), we can't update it.
        // But it should exist if we computed patterns for it.
        if !chunk_path.exists() {
            println!(
                "Warning: Partition {} to be updated not found on disk.",
                pid
            );
            continue;
        }

        let mut full_partition: TransitPartition = load_bincode(chunk_path.to_str().unwrap())?;

        println!(
            "Updating partition {} with {} sets of patterns...",
            pid,
            results_list.len()
        );

        // Now apply the logic to populate `external_hubs` and `long_distance_transfer_patterns`

        // Consolidate results by source_idx to avoid duplicate LocalTransferPatterns
        let mut results_by_source: HashMap<u32, Vec<((u32, u32), Vec<(u32, u32)>)>> =
            HashMap::new();
        for (source_idx, targets) in results_list {
            results_by_source
                .entry(source_idx)
                .or_default()
                .extend(targets);
        }

        // Apply updates
        let mut new_patterns = Vec::new();

        for (source_idx, targets) in results_by_source {
            let mut edges = Vec::new();

            for ((t_pid, t_idx), pareto) in targets {
                if pareto.is_empty() {
                    continue;
                }

                let to_node_idx = if t_pid == pid {
                    t_idx
                } else {
                    // Find or add external hub
                    let hub = GlobalHub {
                        original_partition_id: t_pid,
                        stop_idx_in_partition: t_idx,
                    };
                    if let Some(pos) = full_partition.external_hubs.iter().position(|h| *h == hub) {
                        (full_partition.stops.len() + pos) as u32
                    } else {
                        full_partition.external_hubs.push(hub);
                        (full_partition.stops.len() + full_partition.external_hubs.len() - 1) as u32
                    }
                };

                // GENERATE SYNTHETIC TRIP PATTERN
                // 1. Create DirectionPattern [src, tgt]
                // Note: 'tgt' index depends on if it is internal or external.
                // Typically DirectionPatterns use internal indices.
                // For external hubs, we use the virtual index (stops.len() + hub_idx).
                let dp = DirectionPattern {
                    stop_indices: vec![source_idx, to_node_idx],
                };

                // We should check if this DP already exists to save space, but for now simple append.
                full_partition.direction_patterns.push(dp);
                let dp_idx = (full_partition.direction_patterns.len() - 1) as u32;

                // 2. Create Trips & TimeDeltas
                let mut trips = Vec::new();
                for (dep, arr) in &pareto {
                    let duration = arr - dep;
                    let tds = TimeDeltaSequence {
                        deltas: vec![*duration, 0], // Travel, Dwell
                    };
                    full_partition.time_deltas.push(tds);
                    let td_idx = (full_partition.time_deltas.len() - 1) as u32;

                    trips.push(catenary::routing_common::transit_graph::CompressedTrip {
                        gtfs_trip_id: "synthetic".to_string(),
                        service_mask: u32::MAX,
                        start_time: *dep,
                        time_delta_idx: td_idx,
                        service_idx: 0,
                        bikes_allowed: 0,
                        wheelchair_accessible: 0,
                    });
                }

                let tp = TripPattern {
                    chateau_idx: 0,
                    route_id: "synthetic".to_string(),
                    direction_pattern_idx: dp_idx,
                    trips,
                    timezone_idx: 0,
                    route_type: 2, // Rail/LongDist
                    is_border: false,
                };

                full_partition.trip_patterns.push(tp);
                let tp_idx = (full_partition.trip_patterns.len() - 1) as u32;

                // Link Edge
                let edge = catenary::routing_common::transit_graph::DagEdge {
                    from_node_idx: source_idx,
                    to_node_idx,
                    edge_type: Some(
                        catenary::routing_common::transit_graph::EdgeType::LongDistanceTransit(
                            catenary::routing_common::transit_graph::TransitEdge {
                                trip_pattern_idx: tp_idx,
                                start_stop_idx: 0,
                                end_stop_idx: 1, // Synthetic patterns always 2 stops
                                min_duration: match pareto.iter().map(|(d, a)| a - d).min() {
                                    Some(m) => m,
                                    None => 0,
                                },
                            },
                        ),
                    ),
                };
                edges.push(edge);
            }

            new_patterns.push(LocalTransferPattern {
                from_stop_idx: source_idx,
                edges,
            });
        }

        // We should merge with existing long_distance_transfer_patterns if any?
        // Currently we just overwrite. Since we combined results, this should be fine
        // as long as we don't have multiple entries for the same source_idx in results_list.
        // The way we constructed combined_results, we might have multiple entries if a node was both border and long dist?
        // Let's check overlap.
        // Border stations are subset of all stops. Long distance are subset.
        // A stop CAN be both.
        // If it is both, we have entries in `border_results` AND `long_dist_data`.
        // We just pushed them both to the vector.
        // So `new_patterns` will have two `LocalTransferPattern` for same `from_stop_idx`.
        // We should consolidate them.

        // Consolidate patterns by source index
        let mut final_patterns_map: HashMap<
            u32,
            Vec<catenary::routing_common::transit_graph::DagEdge>,
        > = HashMap::new();

        for p in new_patterns {
            final_patterns_map
                .entry(p.from_stop_idx)
                .or_default()
                .extend(p.edges);
        }

        let mut final_patterns = Vec::new();
        for (src, edges) in final_patterns_map {
            final_patterns.push(LocalTransferPattern {
                from_stop_idx: src,
                edges,
            });
        }

        full_partition.long_distance_transfer_patterns = final_patterns;

        // Save
        let version_number = 1;
        rt_handle.block_on(tokio::fs::create_dir_all(&partition_dir))?;
        let output_path = partition_dir.join(format!("local_v{}.bin", version_number));
        save_bincode(&full_partition, output_path.to_str().unwrap())?;

        // Also save to old location
        let legacy_path = output_dir.join(format!("transit_chunk_{}.bincode", pid));
        save_bincode(&full_partition, legacy_path.to_str().unwrap())?;
    }

    println!("Rebuild Complete.");

    Ok(())
}

struct LongDistanceData {
    // (pid, stop_idx) -> List of (Target(pid, idx), ParetoSegments)
    // ParetoSegments: List of (DepartureTime, ArrivalTime)
    patterns_by_source: HashMap<(u32, u32), Vec<((u32, u32), Vec<(u32, u32)>)>>,
}

fn rebuild_partition(
    partition_id: u32,
    manifest: &Manifest,
    output_dir: &Path,
    pool: Arc<CatenaryPostgresPool>,
    rt_handle: &tokio::runtime::Handle,
) -> Result<()> {
    println!("Rebuilding Partition {}", partition_id);

    // 1. Load or Create Partition
    // We try to load existing partition to preserve non-timetable data (like OSM links).
    // If it doesn't exist, we create it from shards.
    let chunk_path = output_dir
        .join("patterns")
        .join(partition_id.to_string())
        .join("local_v1.bin");

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
    //    partition.trip_patterns.clear();
    //  partition.time_deltas.clear();
    //  partition.direction_patterns.clear();
    // partition.service_ids.clear();
    //partition.service_exceptions.clear();
    // partition.timezones.clear();
    // partition.local_dag.clear(); // Will be recomputed

    // We need to map `PartitionTimetableData` stops to `TransitPartition` stop indices.
    let mut gtfs_to_idx: HashMap<String, u32> = HashMap::new();
    let mut station_to_idx: HashMap<String, u32> = HashMap::new();

    for (i, stop) in partition.stops.iter().enumerate() {
        for gtfs_id in &stop.gtfs_stop_ids {
            gtfs_to_idx.insert(gtfs_id.clone(), i as u32);
        }
        station_to_idx.insert(stop.station_id.clone(), i as u32);
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
            merge_partition_data(&mut partition, p_data, &station_to_idx, &gtfs_to_idx);
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
            rt_handle.block_on(run_update_gtfs(
                pool.clone(),
                chateau_id.clone(),
                output_dir,
            ))?;

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
                merge_partition_data(&mut partition, p_data, &station_to_idx, &gtfs_to_idx);
                merged_any_timetable = true;
            } else {
                println!(
                    "Could not find partition {} in timetable data for chateau {} after DB fetch. Known partitions are {}",
                    partition_id,
                    chateau_id,
                    tt_data
                        .partitions
                        .iter()
                        .map(|p| p.partition_id.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                );
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

    // 4. Identify Long Distance and Border Stations
    // We iterate over patterns to flag stops.
    // Also, we need to ensure is_hub is set.
    for tp in &partition.trip_patterns {
        let is_long_distance_pattern = tp.route_type == 2 || // Rail
            tp.route_type == 1100 || // Air
            manifest.chateau_to_partitions.iter().any(|(c, _)| c.contains("amtrak") || c.contains("greyhound") || c.contains("flixbus") || c.contains("megabus"));

        // Actually checking chateau for EVERY pattern is inefficient and potentially wrong if partition has mixed chateaux.
        // But `tp` has `chateau_idx`. We can lookup chateau name.
        let chateau_id = &partition.chateau_ids[tp.chateau_idx as usize];
        let is_ld_chateau = chateau_id.contains("amtrak")
            || chateau_id.contains("greyhound")
            || chateau_id.contains("flixbus")
            || chateau_id.contains("megabus");

        let is_ld = tp.route_type == 2 || is_ld_chateau;

        let dp = &partition.direction_patterns[tp.direction_pattern_idx as usize];
        for &stop_idx in &dp.stop_indices {
            if let Some(stop) = partition.stops.get_mut(stop_idx as usize) {
                if is_ld {
                    stop.is_long_distance = true;
                    stop.is_hub = true;
                }
                if tp.is_border {
                    stop.is_border = true;
                    stop.is_hub = true;
                }
            }
        }
    }

    // 5. Ensure internal transfers exist (especially for freshly created hubs)

    // 5. Ensure internal transfers exist (recompute based on distance)
    // We clear existing transfers to ensure we get a clean set based on current stops.
    partition.internal_transfers.clear();

    // Parameters for transfer generation
    const MAX_TRANSFER_DIST: f64 = 500.0; // meters
    // const WALK_SPEED: f64 = 1.1; // m/s (conservative) - used for fallback

    println!(
        "    - Generating internal transfers (max dist {}m). Using OSM A* if available...",
        MAX_TRANSFER_DIST
    );

    let osm_dir = output_dir.to_path_buf(); // Assuming chunks are directly in output_dir for now, or check extract
    let mut chunk_cache = ChunkCache::new(50);

    // Cache stop cache tiles to avoid re-calculating tile coords constantly
    // (Optional, but good for O(N^2))
    let stops_len = partition.stops.len();

    // Naive pairwise (O(N^2))
    for i in 0..stops_len {
        let s_i = &partition.stops[i];
        let (tx_i, ty_i) = lon_lat_to_tile(s_i.lon, s_i.lat, 12);

        // Always add self-loop
        partition.internal_transfers.push(StaticTransfer {
            from_stop_idx: i as u32,
            to_stop_idx: i as u32,
            duration_seconds: 0,
            distance_meters: 0,
            wheelchair_accessible: true,
        });

        // Pre-resolve source node if possible
        let source_node_idx_opt = {
            if let Some((street_data, rtree)) = chunk_cache.get_or_load(tx_i, ty_i, &osm_dir) {
                if let Some(nearest) = rtree.nearest_neighbor(&[s_i.lon, s_i.lat]) {
                    // Check distance to node?
                    let n = &street_data.nodes[nearest.data as usize];
                    if haversine_distance(s_i.lat, s_i.lon, n.lat, n.lon) < 200.0 {
                        Some(nearest.data)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        for j in 0..stops_len {
            if i == j {
                continue;
            }
            let s_j = &partition.stops[j];

            if (s_i.lat - s_j.lat).abs() > 0.01 {
                continue;
            }

            let dist = haversine_distance(s_i.lat, s_i.lon, s_j.lat, s_j.lon);
            if dist <= MAX_TRANSFER_DIST {
                let mut duration_opt = None;

                // Try A* if source node found
                if let Some(src_node_idx) = source_node_idx_opt {
                    let (tx_j, ty_j) = lon_lat_to_tile(s_j.lon, s_j.lat, 12);
                    if tx_i == tx_j && ty_i == ty_j {
                        // Same tile, use cached data
                        // (We just re-get it, cache is fast enough)
                        if let Some((street_data, rtree)) =
                            chunk_cache.get_or_load(tx_i, ty_i, &osm_dir)
                        {
                            if let Some(nearest) = rtree.nearest_neighbor(&[s_j.lon, s_j.lat]) {
                                let n = &street_data.nodes[nearest.data as usize];
                                if haversine_distance(s_j.lat, s_j.lon, n.lat, n.lon) < 200.0 {
                                    let target_node_idx = nearest.data;
                                    if let Some(walk_sec) =
                                        compute_osm_walk(src_node_idx, target_node_idx, street_data)
                                    {
                                        duration_opt = Some(walk_sec);
                                    }
                                }
                            }
                        }
                    }
                }

                // Fallback to Haversine
                let duration = duration_opt.unwrap_or_else(|| {
                    let d = (dist / 1.1).ceil() as u32; // 1.1 m/s fallback
                    std::cmp::max(1, d)
                });

                partition.internal_transfers.push(StaticTransfer {
                    from_stop_idx: i as u32,
                    to_stop_idx: j as u32,
                    duration_seconds: duration,
                    distance_meters: dist as u32,
                    wheelchair_accessible: true,
                });
            }
        }
    }

    // 5. Recompute Local Patterns
    compute_local_patterns_for_partition(&mut partition);

    // 5a. Augment Local DAG with Profile-Based Shortcuts (Pareto Connectivity)
    augment_local_dag_with_shortcuts(&mut partition);

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
    rt_handle.block_on(tokio::fs::create_dir_all(&partition_dir))?;
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
                            gtfs_stop_ids: s.gtfs_stop_ids.clone(),
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
    station_to_idx: &HashMap<String, u32>,
    gtfs_to_idx: &HashMap<String, u32>,
) {
    println!(
        "Merging with a source with {} trip patterns. Partition has {} stops.",
        source.trip_patterns.len(),
        partition.stops.len()
    );

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

    // We used to merge Time Deltas and Direction Patterns blindly here.
    // Now we merge them on demand to support splicing.

    // Map (Source DP Idx) -> (New DP Idx, Kept Original Indices)
    let mut dp_mapping: HashMap<u32, (u32, Vec<usize>)> = HashMap::new();

    // Pre-calculate DP Mappings
    for (i, dp) in source.direction_patterns.iter().enumerate() {
        let mut new_indices = Vec::new(); // Global Partition Indices
        let mut kept_original_indices = Vec::new();

        for (orig_idx, &local_idx) in dp.stop_indices.iter().enumerate() {
            if let Some(stop_id) = source.stops.get(local_idx as usize) {
                // Check Station ID first, then GTFS ID
                let matched_idx = station_to_idx
                    .get(stop_id)
                    .or_else(|| gtfs_to_idx.get(stop_id));

                if let Some(&global_idx) = matched_idx {
                    new_indices.push(global_idx);
                    kept_original_indices.push(orig_idx);
                }
            }
        }

        if !new_indices.is_empty() {
            // Create a new direction pattern in the partition
            let new_dp_idx = partition.direction_patterns.len() as u32;
            partition.direction_patterns.push(DirectionPattern {
                stop_indices: new_indices,
            });
            dp_mapping.insert(i as u32, (new_dp_idx, kept_original_indices));
        }
    }

    // Time Deltas Cache: (SourceTD_Idx, SourceDP_Idx) -> (NewTD_Idx, StartTimeOffset)
    // We key by (SourceTD, SourceDP) because the splicing logic depends on which stops were kept (SourceDP).
    let mut td_cache: HashMap<(u32, u32), (u32, u32)> = HashMap::new();

    // Merge Trip Patterns
    for tp in &source.trip_patterns {
        let source_dp_idx = tp.direction_pattern_idx;

        if let Some((new_dp_idx, kept_indices)) = dp_mapping.get(&source_dp_idx) {
            // Reconstruct Trips
            let mut new_trips = Vec::new();

            for trip in &tp.trips {
                let source_td_idx = trip.time_delta_idx;

                // Get or Create New TimeDelta + Offset
                let (new_td_idx, start_offset) = if let Some(&cached) =
                    td_cache.get(&(source_td_idx, source_dp_idx))
                {
                    cached
                } else {
                    // Compute Spliced Time Delta
                    let source_td = &source.time_deltas[source_td_idx as usize];
                    let (new_deltas, offset) = splice_time_deltas(&source_td.deltas, kept_indices);

                    let idx = partition.time_deltas.len() as u32;
                    partition
                        .time_deltas
                        .push(TimeDeltaSequence { deltas: new_deltas });

                    td_cache.insert((source_td_idx, source_dp_idx), (idx, offset));
                    (idx, offset)
                };

                let mut new_trip = trip.clone();
                new_trip.service_idx += service_id_offset;
                new_trip.time_delta_idx = new_td_idx;
                new_trip.start_time += start_offset;

                new_trips.push(new_trip);
            }

            let mut new_tp = tp.clone();
            new_tp.direction_pattern_idx = *new_dp_idx;
            new_tp.timezone_idx += timezone_offset;
            new_tp.trips = new_trips;

            partition.trip_patterns.push(new_tp);
        }
    }

    // Merge Service Exceptions
    for service_exception in &source.service_exceptions {
        let mut new_service_exception = service_exception.clone();
        new_service_exception.service_idx += service_id_offset;
        partition.service_exceptions.push(new_service_exception);
    }
}

/// Helper to splice time deltas.
/// `original_deltas`: [T0, D0, T1, D1, T2, D2, ...]
/// `kept_indices`: Indices of stops kept from the original sequence. e.g. [0, 2, 3]
/// Returns: (New Deltas, Start Time Offset)
fn splice_time_deltas(original_deltas: &[u32], kept_indices: &[usize]) -> (Vec<u32>, u32) {
    if kept_indices.is_empty() {
        return (Vec::new(), 0);
    }

    // Calculate Start Offset (Time to reach the first kept stop)
    // Arr_k = Arr_0 + Sum(j=0 to k-1) [ D_j + T_{j+1} ]
    let first_kept = kept_indices[0];
    let mut start_offset = 0;

    // Sum D_j + T_{j+1} for j from 0 to first_kept - 1
    // D_j is at 2*j + 1
    // T_{j+1} is at 2*(j+1)
    for j in 0..first_kept {
        let d_j = original_deltas[2 * j + 1];
        let t_next = original_deltas[2 * (j + 1)];
        start_offset += d_j + t_next;
    }

    let mut new_deltas = Vec::new();

    // Process kept stops
    for (new_idx, &orig_idx) in kept_indices.iter().enumerate() {
        // We need T_new and D_new for this stop.

        // D_new is simple: it's the dwell of the original stop.
        let d_orig = original_deltas[2 * orig_idx + 1];

        // T_new is the travel time FROM the previous KEPT stop TO this one.
        // If new_idx == 0, T_new is 0 (relative to NEW start time).
        let t_new = if new_idx == 0 {
            0
        } else {
            let prev_kept_idx = kept_indices[new_idx - 1];

            // Calc time from Dep_prev to Arr_curr
            // Loop from j = prev_kept to orig_idx - 1
            // We want T_{j+1} + D_{j+1} ... except the last D is D_{orig_idx} which is NOT part of travel.
            // Wait.
            // Dep_prev is at time X.
            // Arr_curr is at time Y.
            // Y - X = T_{prev+1} + D_{prev+1} + T_{prev+2} + ... + T_{orig}

            let mut t_acc = 0;
            for j in prev_kept_idx..orig_idx {
                let t_next = original_deltas[2 * (j + 1)];
                t_acc += t_next;
                // If this is intermediate stop (skipped), we add its dwell.
                // j goes from prev_kept_idx to orig_idx - 1
                // We add D_{j+1} if j+1 < orig_idx
                if j + 1 < orig_idx {
                    let d_intermediate = original_deltas[2 * (j + 1) + 1];
                    t_acc += d_intermediate;
                }
            }
            t_acc
        };

        new_deltas.push(t_new);
        new_deltas.push(d_orig);
    }

    (new_deltas, start_offset)
}

fn run_profile_search(
    start_pid: u32,
    start_idx: u32,
    loaded_partitions: &HashMap<u32, TransitPartition>,
    targets: &[(u32, u32)],
    hubs: Option<&HashSet<u32>>,
) -> Vec<((u32, u32), Vec<(u32, u32)>)> {
    // Trip-Based Profile Search (rRAPTOR)

    // 1. Get Partition and Init
    let partition = loaded_partitions.get(&start_pid).unwrap();

    // 2. Build Transfers (Algorithm 1+2+3)
    // Note: In production, these should be precomputed or cached?
    // Calculating per profile search is expensive but safe for rebuild (offline).
    let mut transfers = compute_initial_transfers(partition);
    remove_u_turn_transfers(partition, &mut transfers);
    refine_transfers(partition, &mut transfers);

    // 3. Build Transfer Ranges
    let mut trip_transfer_ranges: HashMap<(usize, usize), (usize, usize)> = HashMap::new();
    // Sort transfers
    transfers.sort_by(|a, b| {
        a.from_pattern_idx
            .cmp(&b.from_pattern_idx)
            .then(a.from_trip_idx.cmp(&b.from_trip_idx))
            .then(a.from_stop_idx_in_pattern.cmp(&b.from_stop_idx_in_pattern))
    });

    let mut start = 0;
    while start < transfers.len() {
        let t = &transfers[start];
        let key = (t.from_pattern_idx, t.from_trip_idx);
        let mut end = start + 1;
        while end < transfers.len()
            && transfers[end].from_pattern_idx == key.0
            && transfers[end].from_trip_idx == key.1
        {
            end += 1;
        }
        trip_transfer_ranges.insert(key, (start, end));
        start = end;
    }

    // 4. Build Stop to Patterns
    let stop_to_patterns = build_stop_to_patterns(partition);

    // 5. Flatten Trips for r_labels
    let mut flat_id_to_pattern_trip = Vec::new();
    let mut pattern_trip_offset = vec![0; partition.trip_patterns.len()];
    let mut current_offset = 0;
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        pattern_trip_offset[p_idx] = current_offset;
        for t_idx in 0..pattern.trips.len() {
            flat_id_to_pattern_trip.push((p_idx, t_idx));
        }
        current_offset += pattern.trips.len();
    }

    // 6. Setup Scratch
    // Assuming reasonable defaults for max_transfers (e.g. 4)
    let max_transfers = 4;
    let mut scratch = ProfileScratch::new(
        partition.stops.len(),
        flat_id_to_pattern_trip.len(),
        max_transfers,
    );

    // 7. Filter Targets (Local Only for Trip-Based)
    let mut local_targets = Vec::new();

    for (t_pid, t_idx) in targets {
        if *t_pid == start_pid {
            local_targets.push(*t_idx);
        }
    }

    // 8. Run Profile Search
    let empty_hubs = HashSet::new();
    let hubs_ref = hubs.unwrap_or(&empty_hubs);

    let raw_results = run_trip_based_profile(
        partition,
        &transfers,
        &trip_transfer_ranges,
        start_idx,
        &local_targets,
        &stop_to_patterns,
        &flat_id_to_pattern_trip,
        &pattern_trip_offset,
        max_transfers,
        &mut scratch,
        hubs_ref,
        false, // is_source_hub (approximation)
    );

    // Construct results
    let mut results = Vec::new();
    for (t_key, pareto) in raw_results {
        // Map local target (u32) to global key (start_pid, t_idx)
        let t_idx = t_key;
        // Filter out unreachable
        if !pareto.is_empty() {
            results.push(((start_pid, t_idx), pareto));
        }
    }

    results
}

fn run_profile_search_phase2(
    start_pid: u32,
    start_idx: u32,
    loaded_partitions: &HashMap<u32, TransitPartition>,
    targets: &[(u32, u32)],
) -> Vec<((u32, u32), Vec<(u32, u32)>)> {
    // Phase 2 just runs the search on the augmented partition.
    // The synthetic trips should have been injected already.
    // Collect hubs for long-distance pruning
    let partition = loaded_partitions.get(&start_pid).unwrap();
    let hubs: HashSet<u32> = partition
        .stops
        .iter()
        .filter(|s| s.is_hub)
        .map(|s| s.id as u32)
        .collect();

    run_profile_search(
        start_pid,
        start_idx,
        loaded_partitions,
        targets,
        Some(&hubs),
    )
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

fn augment_local_dag_with_shortcuts(partition: &mut TransitPartition) {
    println!("    - Augmenting local DAG with profile-based shortcuts...");

    // 1. Identify Hubs
    let hubs: HashSet<u32> = partition
        .stops
        .iter()
        .enumerate()
        .filter(|(_, s)| s.is_border || s.is_long_distance)
        .map(|(i, _)| i as u32)
        .collect();

    if hubs.is_empty() {
        return;
    }
    println!("      Found {} hubs to connect.", hubs.len());

    // 2. Prepare Profile Search Data
    let mut transfers = compute_initial_transfers(partition);
    remove_u_turn_transfers(partition, &mut transfers);
    refine_transfers(partition, &mut transfers);

    // Build Trip Transfer Ranges
    let targets_vec: Vec<u32> = hubs.iter().cloned().collect();

    // 3. Run Profile Search from Each Hub
    let mut shortcuts_added = 0;
    // Map (From, To) -> MinDuration
    let mut best_shortcuts: HashMap<(u32, u32), u32> = HashMap::new();

    for &source_node in &hubs {
        let min_durations =
            compute_one_to_all_profile(partition, &transfers, source_node, &targets_vec, 5);

        for (target, dur) in min_durations {
            if target == source_node {
                continue;
            }
            let entry = best_shortcuts
                .entry((source_node, target))
                .or_insert(u32::MAX);
            if dur < *entry {
                *entry = dur;
            }
        }
    }

    // 4. Inject Shortcuts into Local DAG
    for ((u, v), duration) in best_shortcuts {
        let edge_list = partition.local_dag.entry(u).or_default();

        // Check if existing edge is better
        let existing = edge_list.edges.iter_mut().find(|e| e.to_node_idx == v);

        if let Some(e) = existing {
            // If existing duration is worse, update it?
            // Or add parallel edge?
            // If we update `min_duration` of a TransitEdge, that might be confusing if it doesn't match the pattern.
            // Better to add a separate LongDistanceTransit edge if strictly better.
            // Or update if it's already a LongDistanceTransit edge.
            // If it's a Pattern edge (Transit), and we found a faster way (via transfer),
            // we should probably add a LongDistanceTransit edge.

            let current_min = match &e.edge_type {
                Some(EdgeType::Transit(t)) => t.min_duration,
                Some(EdgeType::Walk(w)) => w.duration_seconds,
                Some(EdgeType::LongDistanceTransit(t)) => t.min_duration,
                None => u32::MAX,
            };

            if duration < current_min {
                // Add new edge
                edge_list.edges.push(DagEdge {
                    from_node_idx: u,
                    to_node_idx: v,
                    edge_type: Some(EdgeType::LongDistanceTransit(TransitEdge {
                        trip_pattern_idx: u32::MAX, // Pseudo-pattern
                        start_stop_idx: u,
                        end_stop_idx: v,
                        min_duration: duration,
                    })),
                });
                shortcuts_added += 1;
            }
        } else {
            edge_list.edges.push(DagEdge {
                from_node_idx: u,
                to_node_idx: v,
                edge_type: Some(EdgeType::LongDistanceTransit(TransitEdge {
                    trip_pattern_idx: u32::MAX,
                    start_stop_idx: u,
                    end_stop_idx: v,
                    min_duration: duration,
                })),
            });
            shortcuts_added += 1;
        }
    }

    println!("      Added {} profile-based shortcuts.", shortcuts_added);
}
