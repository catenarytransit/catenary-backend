use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, EdgeType, TransitEdge, TransitPartition, WalkEdge,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Transfer {
    pub from_pattern_idx: usize,
    pub from_trip_idx: usize,
    pub from_stop_idx_in_pattern: usize,

    pub to_pattern_idx: usize,
    pub to_trip_idx: usize,
    pub to_stop_idx_in_pattern: usize,
}

#[derive(Clone, Copy, Debug)]
struct PatternStopInfo {
    pattern_idx: usize,
    trip_idx: usize,
    stop_idx_in_pattern: usize,
    departure_time: u32,
}

/// Algorithm 1: Initial transfer computation
pub fn compute_initial_transfers(partition: &TransitPartition) -> Vec<Transfer> {
    let mut transfers = Vec::new();

    // 1. Precompute stop -> sorted list of trips departing from it
    let mut stop_to_trips: Vec<Vec<PatternStopInfo>> = vec![Vec::new(); partition.stops.len()];

    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        for (t_idx, trip) in pattern.trips.iter().enumerate() {
            let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
            let mut time = trip.start_time;

            for (i, &stop_idx) in stop_indices.iter().enumerate() {
                // Arrival at stop i
                if i > 0 {
                    if 2 * i < delta_seq.deltas.len() {
                        time += delta_seq.deltas[2 * i];
                    }
                }
                // Departure from stop i
                if 2 * i + 1 < delta_seq.deltas.len() {
                    time += delta_seq.deltas[2 * i + 1];
                }
                let departure_time = time;

                stop_to_trips[stop_idx as usize].push(PatternStopInfo {
                    pattern_idx: p_idx,
                    trip_idx: t_idx,
                    stop_idx_in_pattern: i,
                    departure_time,
                });
            }
        }
    }

    // Sort by departure time
    for trips in &mut stop_to_trips {
        trips.sort_by_key(|t| t.departure_time);
    }

    // 2. Build Footpath Graph
    let mut footpaths: Vec<Vec<(u32, u32)>> = vec![Vec::new(); partition.stops.len()];
    for i in 0..partition.stops.len() {
        footpaths[i].push((i as u32, 0)); // Self-loop
    }
    for transfer in &partition.internal_transfers {
        footpaths[transfer.from_stop_idx as usize]
            .push((transfer.to_stop_idx, transfer.duration_seconds));
    }

    // 3. Main Loop: Find transfers
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        for (t_idx, trip) in pattern.trips.iter().enumerate() {
            let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
            let mut current_time = trip.start_time;

            for (i, &stop_idx) in stop_indices.iter().enumerate() {
                // Arrival at stop i
                if i > 0 {
                    if 2 * i < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * i];
                    }
                }
                let arrival_time = current_time;

                // Departure update for next loop
                if 2 * i + 1 < delta_seq.deltas.len() {
                    current_time += delta_seq.deltas[2 * i + 1];
                }

                // Check transfers
                if let Some(reachable_stops) = footpaths.get(stop_idx as usize) {
                    for &(q, walk_time) in reachable_stops {
                        let min_dep_time = arrival_time + walk_time;

                        if let Some(candidate_trips) = stop_to_trips.get(q as usize) {
                            let mut seen_patterns = HashSet::new();

                            // Find earliest trip for each pattern
                            for cand in candidate_trips {
                                if cand.departure_time >= min_dep_time {
                                    if !seen_patterns.contains(&cand.pattern_idx) {
                                        let is_same_pattern = cand.pattern_idx == p_idx;
                                        let valid = if is_same_pattern {
                                            cand.trip_idx < t_idx || cand.stop_idx_in_pattern < i
                                        } else {
                                            true
                                        };

                                        if valid {
                                            transfers.push(Transfer {
                                                from_pattern_idx: p_idx,
                                                from_trip_idx: t_idx,
                                                from_stop_idx_in_pattern: i,
                                                to_pattern_idx: cand.pattern_idx,
                                                to_trip_idx: cand.trip_idx,
                                                to_stop_idx_in_pattern: cand.stop_idx_in_pattern,
                                            });
                                        }
                                        seen_patterns.insert(cand.pattern_idx);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    transfers
}

/// Algorithm 2: Remove U-turn transfers
pub fn remove_u_turn_transfers(partition: &TransitPartition, transfers: &mut Vec<Transfer>) {
    transfers.retain(|tr| {
        if tr.from_stop_idx_in_pattern == 0 {
            return true;
        }

        let from_pattern = &partition.trip_patterns[tr.from_pattern_idx];
        let from_stops =
            &partition.direction_patterns[from_pattern.direction_pattern_idx as usize].stop_indices;
        let prev_stop_idx = from_stops[tr.from_stop_idx_in_pattern - 1];

        let to_pattern = &partition.trip_patterns[tr.to_pattern_idx];
        let to_stops =
            &partition.direction_patterns[to_pattern.direction_pattern_idx as usize].stop_indices;

        if tr.to_stop_idx_in_pattern + 1 >= to_stops.len() {
            return true;
        }
        let next_stop_idx_on_target = to_stops[tr.to_stop_idx_in_pattern + 1];

        if prev_stop_idx == next_stop_idx_on_target {
            let t_trip = &from_pattern.trips[tr.from_trip_idx];
            let u_trip = &to_pattern.trips[tr.to_trip_idx];

            let arr_t_prev = get_arrival_time(partition, t_trip, tr.from_stop_idx_in_pattern - 1);
            let dep_u_next = get_departure_time(partition, u_trip, tr.to_stop_idx_in_pattern + 1);

            let walk_time = 0;

            if arr_t_prev + walk_time <= dep_u_next {
                return false;
            }
        }
        true
    });
}

/// Algorithm 3: Arrival Time Improvement (Refine Transfers)
pub fn refine_transfers(partition: &TransitPartition, transfers: &mut Vec<Transfer>) {
    // Placeholder for now as discussed in plan
}

/// Profile Query (Earliest Arrival for specific time)
pub fn compute_profile_query(
    partition: &TransitPartition,
    transfers: &[Transfer],
    start_stop: u32,
    start_time: u32,
    targets: &[u32],
) -> Vec<DagEdge> {
    // 1. Index Transfers (should be done once, but doing here for now)
    let mut transfers_from: HashMap<(usize, usize, usize), Vec<&Transfer>> = HashMap::new();
    for tr in transfers {
        transfers_from
            .entry((
                tr.from_pattern_idx,
                tr.from_trip_idx,
                tr.from_stop_idx_in_pattern,
            ))
            .or_default()
            .push(tr);
    }

    // 2. BFS State
    // We want to find paths to targets.
    // We track earliest arrival at stops to prune.
    let mut earliest_arrival = vec![u32::MAX; partition.stops.len()];
    earliest_arrival[start_stop as usize] = start_time;

    // Queue: (pattern_idx, trip_idx, board_stop_idx, arrival_time_at_board)
    // We also need to track how we got here to build the DAG.
    // Predecessors: stop_idx -> Vec<(prev_stop_idx, EdgeType)>
    // We allow multiple parents for DAG.
    let mut predecessors: Vec<Vec<(u32, EdgeType)>> = vec![Vec::new(); partition.stops.len()];

    let mut queue: std::collections::VecDeque<(usize, usize, usize, u32)> =
        std::collections::VecDeque::new();

    // 3. Initial: Footpaths from start_stop
    // (This is effectively "Round 0" - walking to stops)
    let mut initial_stops = Vec::new();
    initial_stops.push((start_stop, start_time));

    // Add internal transfers from start_stop
    for transfer in &partition.internal_transfers {
        if transfer.from_stop_idx == start_stop {
            let arr = start_time + transfer.duration_seconds;
            if arr < earliest_arrival[transfer.to_stop_idx as usize] {
                earliest_arrival[transfer.to_stop_idx as usize] = arr;
                predecessors[transfer.to_stop_idx as usize].push((
                    start_stop,
                    EdgeType::Walk(WalkEdge {
                        duration_seconds: transfer.duration_seconds,
                    }),
                ));
                initial_stops.push((transfer.to_stop_idx, arr));
            }
        }
    }

    // Find trips reachable from initial stops
    for &(stop, time) in &initial_stops {
        // Find trips departing >= time
        // Scan all patterns (slow but correct)
        for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            if let Some(s_idx) = stop_indices.iter().position(|&s| s == stop) {
                for (t_idx, trip) in pattern.trips.iter().enumerate() {
                    let dep_time = get_departure_time(partition, trip, s_idx);
                    if dep_time >= time {
                        // Board this trip
                        queue.push_back((p_idx, t_idx, s_idx, dep_time));
                        break; // Sorted
                    }
                }
            }
        }
    }

    // 4. Process Queue (Trips)
    while let Some((p_idx, t_idx, board_idx, board_time)) = queue.pop_front() {
        let pattern = &partition.trip_patterns[p_idx];
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
        let trip = &pattern.trips[t_idx];

        // Iterate down the trip
        for i in (board_idx + 1)..stop_indices.len() {
            let stop_idx = stop_indices[i];
            let arr_time = get_arrival_time(partition, trip, i);

            // Update arrival at stop
            if arr_time < earliest_arrival[stop_idx as usize] {
                earliest_arrival[stop_idx as usize] = arr_time;

                // Record edge
                // The edge is from board_stop (stop_indices[board_idx]) to current stop (stop_idx)
                let board_stop = stop_indices[board_idx];
                let min_duration = arr_time - board_time; // Approx

                let edge = TransitEdge {
                    trip_pattern_idx: p_idx as u32,
                    start_stop_idx: board_idx as u32,
                    end_stop_idx: i as u32,
                    min_duration,
                };
                predecessors[stop_idx as usize].push((board_stop, EdgeType::Transit(edge)));

                // Check transfers from this stop (using precomputed set T)
                if let Some(out_transfers) = transfers_from.get(&(p_idx, t_idx, i)) {
                    for tr in out_transfers {
                        // Transfer to tr.to_trip_idx at tr.to_stop_idx_in_pattern
                        let target_trip =
                            &partition.trip_patterns[tr.to_pattern_idx].trips[tr.to_trip_idx];
                        let target_dep_time =
                            get_departure_time(partition, target_trip, tr.to_stop_idx_in_pattern);

                        // Check if we improve arrival at the boarding stop of the next trip?
                        // Actually, we just board the next trip.
                        // We don't update earliest_arrival for the transfer stop itself (we just did).
                        // We push the new trip to queue.

                        // Check if this trip is useful?
                        // Simple check: have we boarded this trip (or better) before?
                        // For simplicity, just push. (Infinite loop risk? No, time increases).
                        // But we should check if we already reached this trip at an earlier stop?

                        queue.push_back((
                            tr.to_pattern_idx,
                            tr.to_trip_idx,
                            tr.to_stop_idx_in_pattern,
                            target_dep_time,
                        ));
                    }
                }
            }
        }
    }

    // 5. Reconstruct DAG for targets
    let mut edges = Vec::new();
    let mut visited = HashSet::new();
    let mut q = std::collections::VecDeque::new();

    for &target in targets {
        if earliest_arrival[target as usize] != u32::MAX {
            q.push_back(target);
            visited.insert(target);
        }
    }

    while let Some(curr) = q.pop_front() {
        if curr == start_stop {
            continue;
        }
        for (prev, edge_type) in &predecessors[curr as usize] {
            // Add edge
            edges.push(DagEdge {
                from_hub_idx: *prev,
                to_hub_idx: curr,
                edge_type: Some(edge_type.clone()),
            });

            if !visited.contains(prev) {
                visited.insert(*prev);
                q.push_back(*prev);
            }
        }
    }

    edges
}

// Helpers
fn get_arrival_time(partition: &TransitPartition, trip: &CompressedTrip, stop_idx: usize) -> u32 {
    let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
    let mut time = trip.start_time;
    for k in 0..=stop_idx {
        if k > 0 {
            if 2 * k < delta_seq.deltas.len() {
                time += delta_seq.deltas[2 * k];
            }
        }
        if k < stop_idx {
            if 2 * k + 1 < delta_seq.deltas.len() {
                time += delta_seq.deltas[2 * k + 1];
            }
        }
    }
    time
}

fn get_departure_time(partition: &TransitPartition, trip: &CompressedTrip, stop_idx: usize) -> u32 {
    let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
    let mut time = trip.start_time;
    for k in 0..=stop_idx {
        if k > 0 {
            if 2 * k < delta_seq.deltas.len() {
                time += delta_seq.deltas[2 * k];
            }
        }
        if 2 * k + 1 < delta_seq.deltas.len() {
            time += delta_seq.deltas[2 * k + 1];
        }
    }
    time
}
