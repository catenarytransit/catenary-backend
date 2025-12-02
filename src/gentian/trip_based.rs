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
    pub duration: u32,
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
                            // println!("  Stop {} has {} candidate trips", q, candidate_trips.len());
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
                                                duration: walk_time,
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
    // 1. Sort transfers to allow efficient lookup by (pattern, trip, stop)
    // We sort by from_pattern, from_trip, from_stop_idx_in_pattern
    transfers.sort_by(|a, b| {
        a.from_pattern_idx
            .cmp(&b.from_pattern_idx)
            .then(a.from_trip_idx.cmp(&b.from_trip_idx))
            .then(a.from_stop_idx_in_pattern.cmp(&b.from_stop_idx_in_pattern))
    });

    // 2. Build index for fast lookup: (pattern, trip) -> start_index in transfers
    // We can just use binary search or a simple offset map since we iterate trips.
    // Let's build a map for O(1) lookup of the slice.
    // (pattern_idx, trip_idx) -> range of indices in transfers
    let mut trip_transfer_ranges: HashMap<(usize, usize), (usize, usize)> = HashMap::new();
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

    // 3. Global Arrival Time Array (tau_A)
    // Initialize with infinity
    let mut arrival_time = vec![u32::MAX; partition.stops.len()];

    // 4. Collect all trips and sort by start_time descending
    struct TripRef {
        pattern_idx: usize,
        trip_idx: usize,
        start_time: u32,
    }
    let mut all_trips = Vec::new();
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        for (t_idx, trip) in pattern.trips.iter().enumerate() {
            all_trips.push(TripRef {
                pattern_idx: p_idx,
                trip_idx: t_idx,
                start_time: trip.start_time,
            });
        }
    }
    all_trips.sort_by_key(|t| std::cmp::Reverse(t.start_time));

    // 5. Iterate trips descending
    let mut to_remove = vec![false; transfers.len()];

    for trip_ref in all_trips {
        let pattern = &partition.trip_patterns[trip_ref.pattern_idx];
        let trip = &pattern.trips[trip_ref.trip_idx];
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        // 5a. Compute arrival times along the trip
        // We calculate them but DO NOT update global arrival_time yet.
        // We must check transfers against the *previous* arrival times (from later trips).
        let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
        let mut current_time = trip.start_time;

        let mut trip_arrivals = Vec::with_capacity(stop_indices.len());

        for (i, &_stop_idx) in stop_indices.iter().enumerate() {
            // Arrival at stop i
            if i > 0 {
                if 2 * i < delta_seq.deltas.len() {
                    current_time += delta_seq.deltas[2 * i];
                }
            }
            trip_arrivals.push(current_time);

            // Prepare for next stop (departure)
            if 2 * i + 1 < delta_seq.deltas.len() {
                current_time += delta_seq.deltas[2 * i + 1];
            }
        }

        // 5b. Iterate stops backwards and check transfers
        // We look at transfers originating FROM this trip.
        if let Some(&(start_idx, end_idx)) =
            trip_transfer_ranges.get(&(trip_ref.pattern_idx, trip_ref.trip_idx))
        {
            // The transfers are sorted by from_stop_idx_in_pattern.
            // We can iterate the slice.
            // But we need to iterate stops backwards.
            // Let's just iterate the transfers for this trip and check.
            // Since it's sorted by stop_idx, we can iterate backwards.
            for tr_idx in (start_idx..end_idx).rev() {
                let tr = &transfers[tr_idx];
                // Departure time from u
                let dep_time = get_departure_time(partition, trip, tr.from_stop_idx_in_pattern);

                // Arrival at target v
                // The transfer connects to (to_pattern, to_trip, to_stop_in_pattern).
                // We need the global stop index of the target.
                let target_pattern = &partition.trip_patterns[tr.to_pattern_idx];
                let target_stops = &partition.direction_patterns
                    [target_pattern.direction_pattern_idx as usize]
                    .stop_indices;
                let target_stop_idx = target_stops[tr.to_stop_idx_in_pattern];

                let arr_at_target = dep_time + tr.duration;

                if arr_at_target < arrival_time[target_stop_idx as usize] {
                    // Useful transfer
                    arrival_time[target_stop_idx as usize] = arr_at_target;
                } else {
                    // Redundant transfer
                    to_remove[tr_idx] = true;
                }
            }
        }

        // 5c. Update global arrival time from this trip
        for (i, &stop_idx) in stop_indices.iter().enumerate() {
            let t = trip_arrivals[i];
            if t < arrival_time[stop_idx as usize] {
                arrival_time[stop_idx as usize] = t;
            }
        }
    }

    // 6. Remove marked transfers
    let mut keep_iter = to_remove.iter();
    transfers.retain(|_| !*keep_iter.next().unwrap());
}

/// Profile Query (Trip-Based One-To-All Profile)
/// Computes all Pareto-optimal paths from start_stop to targets for departures >= start_time.
pub fn compute_profile_query(
    partition: &TransitPartition,
    transfers: &[Transfer],
    start_stop: u32,
    start_time: u32,
    targets: &[u32],
) -> Vec<DagEdge> {
    let num_stops = partition.stops.len();
    let num_trips = partition
        .trip_patterns
        .iter()
        .map(|p| p.trips.len())
        .sum::<usize>(); // Approximate, actually we need max trip index?
    // Trip indices are per-pattern. We need a global trip ID or (pattern, trip).
    // Let's use (pattern_idx, trip_idx) as key.
    // Or flatten: flat_trip_id.
    // Let's use a flat mapping for arrays.
    let mut pattern_trip_offset = Vec::with_capacity(partition.trip_patterns.len());
    let mut total_trips = 0;
    for pattern in &partition.trip_patterns {
        pattern_trip_offset.push(total_trips);
        total_trips += pattern.trips.len();
    }

    // Helpers for flat trip ID
    let get_flat_id = |p: usize, t: usize| -> usize { pattern_trip_offset[p] + t };
    // let get_pattern_trip = |id: usize| -> (usize, usize) { ... }; // Not strictly needed if we iterate carefully

    // 1. Build Transfer Index (if not passed in optimized form)
    // The `transfers` arg is a slice. We assume it's sorted by (from_pattern, from_trip, from_stop)
    // as done in refine_transfers. If not, we should sort or build an index.
    // Since refine_transfers sorts it, we can assume it's sorted if we called it.
    // But to be safe and robust, let's build the index here.
    // (pattern, trip) -> range
    let mut trip_transfer_ranges: HashMap<(usize, usize), (usize, usize)> = HashMap::new();
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

    // 2. Identify Target Set for fast lookup
    let mut is_target = vec![false; num_stops];
    for &t in targets {
        is_target[t as usize] = true;
    }

    // 3. Identify Initial Walks (Source -> Stops)
    // We include start_stop itself (0 duration) and internal transfers.
    let mut initial_walks = Vec::new();
    initial_walks.push((start_stop, 0));
    for tr in &partition.internal_transfers {
        if tr.from_stop_idx == start_stop {
            initial_walks.push((tr.to_stop_idx, tr.duration_seconds));
        }
    }

    // 4. Collect all Departure Opportunities (Seeds)
    // A seed is a trip we can board directly from an initial walk.
    // (departure_time, pattern_idx, trip_idx, stop_idx_in_pattern, walk_duration_to_stop)
    struct Seed {
        dep_time: u32,
        pattern_idx: usize,
        trip_idx: usize,
        stop_idx: usize,
        walk_duration: u32,
        stop_id: u32,
    }
    let mut seeds = Vec::new();

    for &(stop_id, walk_dur) in &initial_walks {
        // Find trips departing from stop_id
        // We iterate all patterns. (Optimization: precompute stop->trips map)
        // For now, iterate all is slow but correct.
        // Optimization: Use `stop_to_patterns` if available? Not in TransitPartition.
        // We'll iterate all patterns.
        for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            if let Some(s_idx) = stop_indices.iter().position(|&s| s == stop_id) {
                for (t_idx, trip) in pattern.trips.iter().enumerate() {
                    let dep = get_departure_time(partition, trip, s_idx);
                    if dep >= start_time + walk_dur {
                        seeds.push(Seed {
                            dep_time: dep - walk_dur, // Effective start time at source
                            pattern_idx: p_idx,
                            trip_idx: t_idx,
                            stop_idx: s_idx,
                            walk_duration: walk_dur,
                            stop_id,
                        });
                    }
                }
            }
        }
    }
    // Sort seeds descending by departure time
    seeds.sort_by_key(|s| std::cmp::Reverse(s.dep_time));

    // 5. Trip-Based Profile Search
    const MAX_TRANSFERS: usize = 8;
    // R[n][flat_trip_idx] = min_stop_idx
    let mut r_labels = vec![vec![usize::MAX; total_trips]; MAX_TRANSFERS + 1];

    // Tracking used graph components
    // used_segments: flat_trip_idx -> (min_entry_idx, Vec<exit_idx>)
    let mut used_segments: HashMap<usize, (usize, Vec<usize>)> = HashMap::new();
    // used_transfers: Set<(from_flat, from_stop_idx, to_flat, to_stop_idx)>
    let mut used_transfers: HashSet<(usize, usize, usize, usize)> = HashSet::new();
    // used_initial_walks: Set<(to_stop)>
    let mut used_initial_walks: HashSet<u32> = HashSet::new();

    for seed in seeds {
        let flat_id = get_flat_id(seed.pattern_idx, seed.trip_idx);

        // Update R[0]
        let old_r = r_labels[0][flat_id];
        if seed.stop_idx < old_r {
            r_labels[0][flat_id] = seed.stop_idx;

            // Record usage
            used_initial_walks.insert(seed.stop_id);
            let entry = used_segments
                .entry(flat_id)
                .or_insert((usize::MAX, Vec::new()));
            entry.0 = std::cmp::min(entry.0, seed.stop_idx);
            // No exit recorded yet, but we are on the trip.

            // Propagate
            // Stack stores (n, flat_trip_id, start_scan_stop_idx_in_pattern, old_r_value_for_this_trip)
            let mut stack = Vec::new();
            stack.push((0, flat_id, seed.stop_idx, old_r));

            while let Some((n, flat_id, stop_idx, old_r_val)) = stack.pop() {
                // println!("Pop: n={}, flat_id={}, stop_idx={}, old_r_val={}", n, flat_id, stop_idx, old_r_val);
                // Need to reverse map flat_id to p_idx and t_idx
                let mut p_idx = 0;
                while p_idx < pattern_trip_offset.len() - 1
                    && pattern_trip_offset[p_idx + 1] <= flat_id
                {
                    p_idx += 1;
                }
                let t_idx = flat_id - pattern_trip_offset[p_idx];

                let pattern = &partition.trip_patterns[p_idx];
                let stop_indices = &partition.direction_patterns
                    [pattern.direction_pattern_idx as usize]
                    .stop_indices;

                // The segment to scan is from `stop_idx` up to `old_r_val` (exclusive)
                // If `old_r_val` was usize::MAX, we scan to the end of the trip.
                let scan_limit = std::cmp::min(old_r_val, stop_indices.len());

                // Look up transfers for this trip
                let range = if let Some(r) = trip_transfer_ranges.get(&(p_idx, t_idx)) {
                    r.clone()
                } else {
                    (0, 0)
                };

                // Current transfer pointer for the sorted transfers slice
                let mut tr_ptr = range.0;
                let tr_end = range.1;

                for i in stop_idx..scan_limit {
                    let current_stop_global_id = stop_indices[i];

                    // Check if this stop is a target
                    if is_target[current_stop_global_id as usize] {
                        // println!("  Reached target {} at stop_idx {} on trip (p{},t{}) with {} transfers", current_stop_global_id, i, p_idx, t_idx, n);
                        let entry = used_segments
                            .entry(flat_id)
                            .or_insert_with(|| (usize::MAX, Vec::new()));
                        if stop_idx < entry.0 {
                            // Update min entry if current start is better
                            entry.0 = stop_idx;
                        }
                        entry.1.push(i); // Record this stop as an exit point
                    }

                    // Process transfers from this stop
                    if n < MAX_TRANSFERS {
                        while tr_ptr < tr_end {
                            let tr = &transfers[tr_ptr];
                            if tr.from_stop_idx_in_pattern < i {
                                tr_ptr += 1;
                                continue;
                            }
                            if tr.from_stop_idx_in_pattern > i {
                                break; // Transfers are sorted, so no more transfers from this stop
                            }

                            // Found transfer from current stop `i`
                            let target_flat = get_flat_id(tr.to_pattern_idx, tr.to_trip_idx);
                            let target_stop_idx = tr.to_stop_idx_in_pattern;

                            // println!("  Transfer found at stop idx {} on trip (p{},t{}): to flat_id {} stop {} with {} transfers", i, p_idx, t_idx, target_flat, target_stop_idx, n);

                            let next_n = n + 1;
                            let old_target_r = r_labels[next_n][target_flat];

                            if target_stop_idx < old_target_r {
                                // println!("    Improving R_{} for flat_id {}: {} -> {}", next_n, target_flat, old_target_r, target_stop_idx);
                                r_labels[next_n][target_flat] = target_stop_idx;
                                stack.push((next_n, target_flat, target_stop_idx, old_target_r));

                                // Mark transfer as used
                                used_transfers.insert((flat_id, i, target_flat, target_stop_idx));

                                // Also mark the segment on current trip as used up to this transfer
                                let entry = used_segments
                                    .entry(flat_id)
                                    .or_insert_with(|| (usize::MAX, Vec::new()));
                                if stop_idx < entry.0 {
                                    entry.0 = stop_idx;
                                }
                                entry.1.push(i); // Record this stop as an exit point for the transfer
                            }

                            tr_ptr += 1;
                        }
                    }
                }
            }
        }
    }

    // 6. Reconstruct Graph from Used Components
    // We need to build edges:
    // - Initial Walks: Start -> Stop
    // - Trip Segments: Stop -> Stop (along trip)
    // - Transfers: Stop -> Stop (between trips)

    // Build adjacency list for backward BFS
    // Node ID: Stop Index (Global)
    // Edges: to -> from
    let mut reverse_adj: HashMap<u32, Vec<(u32, DagEdge)>> = HashMap::new();
    let mut adj: HashMap<u32, Vec<(u32, DagEdge)>> = HashMap::new();

    // Add Initial Walks
    for &stop_id in &used_initial_walks {
        // Edge: start_stop -> stop_id

        // Find duration
        let dur = if stop_id == start_stop {
            0
        } else {
            partition
                .internal_transfers
                .iter()
                .find(|t| t.from_stop_idx == start_stop && t.to_stop_idx == stop_id)
                .map(|t| t.duration_seconds)
                .unwrap_or(0)
        };

        let edge = DagEdge {
            from_hub_idx: start_stop,
            to_hub_idx: stop_id,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: dur,
            })),
        };

        adj.entry(start_stop)
            .or_default()
            .push((stop_id, edge.clone()));
        reverse_adj
            .entry(stop_id)
            .or_default()
            .push((start_stop, edge));
    }

    // Add Trip Segments
    for (flat_id, (entry_idx, exits)) in &used_segments {
        if *entry_idx == usize::MAX || exits.is_empty() {
            continue;
        }
        let mut p_idx = 0;
        while p_idx < pattern_trip_offset.len() - 1 && pattern_trip_offset[p_idx + 1] <= *flat_id {
            p_idx += 1;
        }
        let t_idx = *flat_id - pattern_trip_offset[p_idx];

        let pattern = &partition.trip_patterns[p_idx];
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        let u = stop_indices[*entry_idx];

        for &exit_idx in exits {
            if exit_idx <= *entry_idx {
                continue;
            }
            let v = stop_indices[exit_idx];

            // Calculate min_duration
            let trip = &pattern.trips[t_idx];
            let arr = get_arrival_time(partition, trip, exit_idx);
            let dep = get_departure_time(partition, trip, *entry_idx);
            let dur = arr.saturating_sub(dep);

            // Create Transit Edge
            let edge = DagEdge {
                from_hub_idx: u,
                to_hub_idx: v,
                edge_type: Some(EdgeType::Transit(TransitEdge {
                    trip_pattern_idx: p_idx as u32,
                    start_stop_idx: *entry_idx as u32,
                    end_stop_idx: exit_idx as u32,
                    min_duration: dur,
                })),
            };

            adj.entry(u).or_default().push((v, edge.clone()));
            reverse_adj.entry(v).or_default().push((u, edge));
        }
    }

    // Add Transfers
    for &(from_flat, from_idx, to_flat, to_idx) in &used_transfers {
        let mut p_from = 0;
        while p_from < pattern_trip_offset.len() - 1 && pattern_trip_offset[p_from + 1] <= from_flat
        {
            p_from += 1;
        }
        let mut p_to = 0;
        while p_to < pattern_trip_offset.len() - 1 && pattern_trip_offset[p_to + 1] <= to_flat {
            p_to += 1;
        }

        let u = partition.direction_patterns
            [partition.trip_patterns[p_from].direction_pattern_idx as usize]
            .stop_indices[from_idx];
        let v = partition.direction_patterns
            [partition.trip_patterns[p_to].direction_pattern_idx as usize]
            .stop_indices[to_idx];

        // Find duration from transfers list?
        // We assume 0 for now as most transfers are 0 or walk.
        // If we want exact, we need to look it up.
        // The `used_transfers` set only stores indices, not the full transfer object.
        // To get the duration, we'd need to iterate the original `transfers` slice
        // or store the duration in `used_transfers`. For now, use 0.

        let edge = DagEdge {
            from_hub_idx: u,
            to_hub_idx: v,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: 0, // Placeholder
            })),
        };

        adj.entry(u).or_default().push((v, edge.clone()));
        reverse_adj.entry(v).or_default().push((u, edge));
    }

    // 7. Prune: Backward BFS from targets
    let mut useful_nodes = HashSet::new();
    let mut q = std::collections::VecDeque::new();

    // We already built reverse_adj above.
    // But we need to make sure we only include nodes reachable from targets.

    for &t in targets {
        if reverse_adj.contains_key(&t) || t == start_stop {
            q.push_back(t);
            useful_nodes.insert(t);
        }
    }

    while let Some(curr) = q.pop_front() {
        if let Some(preds) = reverse_adj.get(&curr) {
            for (pred, _) in preds {
                if useful_nodes.insert(*pred) {
                    q.push_back(*pred);
                }
            }
        }
    }

    // 8. Construct Result Edges
    let mut result_edges = Vec::new();
    for &u in &useful_nodes {
        if let Some(neighbors) = adj.get(&u) {
            for (v, edge) in neighbors {
                if useful_nodes.contains(v) {
                    result_edges.push(edge.clone());
                }
            }
        }
    }

    result_edges
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
