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

                                        // Fix: Skip transfers from the first stop
                                        if i == 0 {
                                            continue;
                                        }

                                        // Fix: Skip transfers to the last stop
                                        let cand_pattern =
                                            &partition.trip_patterns[cand.pattern_idx];
                                        let cand_stops = &partition.direction_patterns
                                            [cand_pattern.direction_pattern_idx as usize]
                                            .stop_indices;
                                        if cand.stop_idx_in_pattern + 1 == cand_stops.len() {
                                            continue;
                                        }

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

            // Fix: Use actual change time (self-transfer duration)
            let prev_stop_global_idx = prev_stop_idx; // This is already the global index
            let walk_time = partition
                .internal_transfers
                .iter()
                .find(|t| {
                    t.from_stop_idx == prev_stop_global_idx && t.to_stop_idx == prev_stop_global_idx
                })
                .map(|t| t.duration_seconds)
                .unwrap_or(0);

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
    transfers.sort_by(|a, b| {
        a.from_pattern_idx
            .cmp(&b.from_pattern_idx)
            .then(a.from_trip_idx.cmp(&b.from_trip_idx))
            .then(a.from_stop_idx_in_pattern.cmp(&b.from_stop_idx_in_pattern))
    });

    // 2. Build index for fast lookup
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

    // 3. Precompute Footpaths and Change Times
    let mut footpaths: Vec<Vec<(u32, u32)>> = vec![Vec::new(); partition.stops.len()];
    let mut change_times: Vec<u32> = vec![0; partition.stops.len()];

    for tr in &partition.internal_transfers {
        if tr.from_stop_idx == tr.to_stop_idx {
            change_times[tr.from_stop_idx as usize] = tr.duration_seconds;
        } else {
            footpaths[tr.from_stop_idx as usize].push((tr.to_stop_idx, tr.duration_seconds));
        }
    }

    // 4. Per-Trip State Buffers
    let mut tau_a = vec![u32::MAX; partition.stops.len()];
    let mut tau_c = vec![u32::MAX; partition.stops.len()];
    let mut visited_stops = Vec::new();

    // Helper to update tau and track visited
    let mut update_tau = |stop_idx: usize,
                          arr_time: u32,
                          change_time: u32,
                          tau_a: &mut Vec<u32>,
                          tau_c: &mut Vec<u32>,
                          visited: &mut Vec<usize>|
     -> bool {
        let mut improved = false;
        if arr_time < tau_a[stop_idx] {
            if tau_a[stop_idx] == u32::MAX && tau_c[stop_idx] == u32::MAX {
                visited.push(stop_idx);
            }
            tau_a[stop_idx] = arr_time;
            improved = true;
        }
        let c_time = arr_time.saturating_add(change_time);
        if c_time < tau_c[stop_idx] {
            if tau_a[stop_idx] == u32::MAX && tau_c[stop_idx] == u32::MAX {
                visited.push(stop_idx); // Should already be pushed if tau_a improved, but check
            }
            tau_c[stop_idx] = c_time;
            improved = true;
        }
        improved
    };

    let mut to_remove = vec![false; transfers.len()];

    // 5. Iterate all trips
    // Order doesn't strictly matter for correctness since it's per-trip, but we iterate all.
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        for (t_idx, trip) in pattern.trips.iter().enumerate() {
            // Reset state for this trip
            for &s in &visited_stops {
                tau_a[s] = u32::MAX;
                tau_c[s] = u32::MAX;
            }
            visited_stops.clear();

            // Compute arrival times along the trip (for easy lookup)
            let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
            let mut current_time = trip.start_time;
            let mut trip_arrivals = Vec::with_capacity(stop_indices.len());
            for (i, _) in stop_indices.iter().enumerate() {
                if i > 0 {
                    if 2 * i < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * i];
                    }
                }
                trip_arrivals.push(current_time);
                if 2 * i + 1 < delta_seq.deltas.len() {
                    current_time += delta_seq.deltas[2 * i + 1];
                }
            }

            // Scan stops backwards
            for i in (0..stop_indices.len()).rev() {
                let stop_idx = stop_indices[i] as usize;
                let arr_time = trip_arrivals[i];

                // Update tau for current stop (staying on trip)
                let mut changed = update_tau(
                    stop_idx,
                    arr_time,
                    change_times[stop_idx],
                    &mut tau_a,
                    &mut tau_c,
                    &mut visited_stops,
                );

                // Propagate to footpaths
                if changed {
                    for &(neighbor, walk) in &footpaths[stop_idx] {
                        update_tau(
                            neighbor as usize,
                            arr_time + walk,
                            change_times[neighbor as usize],
                            &mut tau_a,
                            &mut tau_c,
                            &mut visited_stops,
                        );
                    }
                }

                // Check transfers originating from this stop
                if let Some(&(start_tr, end_tr)) = trip_transfer_ranges.get(&(p_idx, t_idx)) {
                    // Iterate transfers from this stop
                    // Transfers are sorted by from_stop_idx_in_pattern.
                    // We need to find the range for `i`.
                    // Since we iterate `i` backwards, and transfers are sorted by `i`, we can optimize this.
                    // But binary search or linear scan in the small range is fine.
                    for tr_idx in start_tr..end_tr {
                        let tr = &transfers[tr_idx];
                        if tr.from_stop_idx_in_pattern != i {
                            continue;
                        }

                        // Simulate riding target trip u
                        let target_pattern = &partition.trip_patterns[tr.to_pattern_idx];
                        let target_trip = &target_pattern.trips[tr.to_trip_idx];
                        let target_stops = &partition.direction_patterns
                            [target_pattern.direction_pattern_idx as usize]
                            .stop_indices;

                        // Calculate arrival at target stop (boarding point)
                        // Actually we need to simulate the WHOLE trip from boarding point onwards.
                        let boarding_stop_idx = tr.to_stop_idx_in_pattern;
                        let boarding_time = get_departure_time(partition, trip, i) + tr.duration;

                        // Check if we can actually make the transfer?
                        // The transfer generation (Alg 1) checked this.
                        // But we should verify if we need to? No, assume valid.

                        // We need arrival times at downstream stops of u
                        let target_delta_seq =
                            &partition.time_deltas[target_trip.time_delta_idx as usize];

                        // Reconstruct time for target trip
                        // Optimization: get_arrival_time is slow in loop.
                        // Better: compute relative offsets once?
                        // Or just iterate forward from boarding stop.

                        let mut u_current_time = target_trip.start_time;
                        // Fast forward to boarding stop
                        for k in 0..=boarding_stop_idx {
                            if k > 0 {
                                if 2 * k < target_delta_seq.deltas.len() {
                                    u_current_time += target_delta_seq.deltas[2 * k];
                                }
                            }
                            if k < boarding_stop_idx {
                                if 2 * k + 1 < target_delta_seq.deltas.len() {
                                    u_current_time += target_delta_seq.deltas[2 * k + 1];
                                }
                            }
                        }
                        // u_current_time is now Arrival at boarding stop.
                        // Wait, we board at `boarding_stop_idx`.
                        // We need to depart.
                        // Departure from boarding stop:
                        let mut u_dep_time = u_current_time;
                        if 2 * boarding_stop_idx + 1 < target_delta_seq.deltas.len() {
                            u_dep_time += target_delta_seq.deltas[2 * boarding_stop_idx + 1];
                        }

                        // Iterate downstream stops
                        let mut u_time = u_dep_time;
                        let mut transfer_useful = false;

                        for k in (boarding_stop_idx + 1)..target_stops.len() {
                            // Travel to k
                            if 2 * k < target_delta_seq.deltas.len() {
                                u_time += target_delta_seq.deltas[2 * k];
                            }
                            let u_arr_time = u_time;
                            let u_stop_idx = target_stops[k] as usize;

                            // Check improvement
                            if update_tau(
                                u_stop_idx,
                                u_arr_time,
                                change_times[u_stop_idx],
                                &mut tau_a,
                                &mut tau_c,
                                &mut visited_stops,
                            ) {
                                transfer_useful = true;
                                // Propagate to neighbors
                                for &(neighbor, walk) in &footpaths[u_stop_idx] {
                                    if update_tau(
                                        neighbor as usize,
                                        u_arr_time + walk,
                                        change_times[neighbor as usize],
                                        &mut tau_a,
                                        &mut tau_c,
                                        &mut visited_stops,
                                    ) {
                                        transfer_useful = true;
                                    }
                                }
                            }

                            // Dwell at k (for next iteration)
                            if 2 * k + 1 < target_delta_seq.deltas.len() {
                                u_time += target_delta_seq.deltas[2 * k + 1];
                            }
                        }

                        if !transfer_useful {
                            to_remove[tr_idx] = true;
                        }
                    }
                }
            }
        }
    }

    // 6. Remove marked transfers
    let mut keep_iter = to_remove.iter();
    transfers.retain(|_| !*keep_iter.next().unwrap());
}

/// Profile Query (Trip-Based One-To-All Profile)
/// Computes all Pareto-optimal paths from start_stop to targets for departures >= start_time.
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
        .sum::<usize>();

    // Trip indices are per-pattern. We need a global trip ID or (pattern, trip).
    // Let's use a flat mapping for arrays.
    let mut pattern_trip_offset = Vec::with_capacity(partition.trip_patterns.len());
    let mut total_trips = 0;
    // Precompute flat_id -> (pattern_idx, trip_idx)
    let mut flat_id_to_pattern_trip = Vec::with_capacity(num_trips);

    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        pattern_trip_offset.push(total_trips);
        for t_idx in 0..pattern.trips.len() {
            flat_id_to_pattern_trip.push((p_idx, t_idx));
        }
        total_trips += pattern.trips.len();
    }

    // Helpers for flat trip ID
    let get_flat_id = |p: usize, t: usize| -> usize { pattern_trip_offset[p] + t };

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
    const MAX_TRANSFERS: usize = 16; // Increased from 8
    // R[n][flat_trip_idx] = min_stop_idx
    let mut r_labels = vec![vec![usize::MAX; total_trips]; MAX_TRANSFERS + 1];

    // Tracking used graph components
    // used_segments: flat_trip_idx -> Set<(entry_idx, exit_idx)>
    // We use a Vec and deduplicate later or just append.
    let mut used_segments: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
    // used_transfers: Set<transfer_idx>
    let mut used_transfers: HashSet<usize> = HashSet::new();
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

            // We don't record segment here yet, we record it when we reach a target or transfer.
            // But we need to know the entry point.
            // The stack stores `stop_idx` which IS the entry point for the scan.

            // Propagate
            // Stack stores (n, flat_trip_id, start_scan_stop_idx_in_pattern, old_r_value_for_this_trip)
            let mut stack = Vec::new();
            stack.push((0, flat_id, seed.stop_idx, old_r));

            while let Some((n, flat_id, stop_idx, old_r_val)) = stack.pop() {
                // println!("Pop: n={}, flat_id={}, stop_idx={}, old_r_val={}", n, flat_id, stop_idx, old_r_val);
                let (p_idx, t_idx) = flat_id_to_pattern_trip[flat_id];

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
                        used_segments
                            .entry(flat_id)
                            .or_default()
                            .push((stop_idx, i));
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

                                // Mark transfer as used (store index)
                                used_transfers.insert(tr_ptr);

                                // Also mark the segment on current trip as used up to this transfer
                                used_segments
                                    .entry(flat_id)
                                    .or_default()
                                    .push((stop_idx, i));
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
    for (flat_id, segments) in &used_segments {
        let (p_idx, t_idx) = flat_id_to_pattern_trip[*flat_id];

        let pattern = &partition.trip_patterns[p_idx];
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        for &(entry_idx, exit_idx) in segments {
            if exit_idx <= entry_idx {
                continue;
            }
            let u = stop_indices[entry_idx];
            let v = stop_indices[exit_idx];

            // Calculate min_duration
            let trip = &pattern.trips[t_idx];
            let arr = get_arrival_time(partition, trip, exit_idx);
            let dep = get_departure_time(partition, trip, entry_idx);
            let dur = arr.saturating_sub(dep);

            // Create Transit Edge
            let edge = DagEdge {
                from_hub_idx: u,
                to_hub_idx: v,
                edge_type: Some(EdgeType::Transit(TransitEdge {
                    trip_pattern_idx: p_idx as u32,
                    start_stop_idx: entry_idx as u32,
                    end_stop_idx: exit_idx as u32,
                    min_duration: dur,
                })),
            };

            adj.entry(u).or_default().push((v, edge.clone()));
            reverse_adj.entry(v).or_default().push((u, edge));
        }
    }

    // Add Transfers
    for &tr_idx in &used_transfers {
        let tr = &transfers[tr_idx];
        let (p_from, _) =
            flat_id_to_pattern_trip[get_flat_id(tr.from_pattern_idx, tr.from_trip_idx)];
        let (p_to, _) = flat_id_to_pattern_trip[get_flat_id(tr.to_pattern_idx, tr.to_trip_idx)];

        let u = partition.direction_patterns
            [partition.trip_patterns[p_from].direction_pattern_idx as usize]
            .stop_indices[tr.from_stop_idx_in_pattern];
        let v = partition.direction_patterns
            [partition.trip_patterns[p_to].direction_pattern_idx as usize]
            .stop_indices[tr.to_stop_idx_in_pattern];

        let edge = DagEdge {
            from_hub_idx: u,
            to_hub_idx: v,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: tr.duration,
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
