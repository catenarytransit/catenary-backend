use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, EdgeType, TransitEdge, TransitPartition, WalkEdge,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Transfer {
    pub from_pattern_idx: usize,
    pub from_trip_idx: usize,
    pub from_stop_idx_in_pattern: u16,

    pub to_pattern_idx: usize,
    pub to_trip_idx: usize,
    pub to_stop_idx_in_pattern: u16,
    pub duration: u32,
}

#[derive(Clone, Debug)]
pub struct Seed {
    pub dep_time: u32,
    pub pattern_idx: usize,
    pub trip_idx: usize,
    pub stop_idx: usize,
    pub walk_duration: u32,
    pub stop_id: u32,
}

pub struct ProfileScratch {
    pub r_labels: Vec<Vec<u32>>,
    // Recycled maps/sets
    pub used_segments: HashMap<usize, Vec<(usize, usize)>>,
    pub used_transfers: HashSet<usize>,
    pub used_initial_walks: HashSet<u32>,
    pub is_target: Vec<bool>,
    pub seeds: Vec<Seed>,
    pub initial_walks: Vec<(u32, u32)>,

    // New fields for compute_profile_query recycling
    pub useful_trips: HashMap<(usize, usize), usize>,
    // Tuple: (from, to, type, p_idx, s_idx, e_idx)
    pub dedup_map: HashMap<(u32, u32, u8, u32, u32, u32), DagEdge>,
    pub result_edges: Vec<DagEdge>,
}

impl ProfileScratch {
    pub fn new(num_stops: usize, total_trips: usize, max_transfers: usize) -> Self {
        Self {
            r_labels: vec![vec![u32::MAX; total_trips]; max_transfers + 1],
            used_segments: HashMap::new(),
            used_transfers: HashSet::new(),
            used_initial_walks: HashSet::new(),
            is_target: vec![false; num_stops],
            seeds: Vec::new(),
            initial_walks: Vec::new(),
            useful_trips: HashMap::new(),
            dedup_map: HashMap::new(),
            result_edges: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        for level in &mut self.r_labels {
            for r in level.iter_mut() {
                *r = u32::MAX;
            }
        }
        self.used_segments.clear();
        self.used_transfers.clear();
        self.used_initial_walks.clear();
        // is_target is cleared/filled in query
        self.seeds.clear();
        self.initial_walks.clear();
        self.useful_trips.clear();
        self.dedup_map.clear();
        self.result_edges.clear();
    }
}

#[derive(Clone, Copy, Debug)]
struct PatternStopInfo {
    pattern_idx: usize,
    trip_idx: usize,
    stop_idx_in_pattern: usize,
    departure_time: u32,
}

// Add a small helper for building stop->patterns
pub fn build_stop_to_patterns(partition: &TransitPartition) -> Vec<Vec<(usize, usize)>> {
    let mut stop_to_patterns: Vec<Vec<(usize, usize)>> = vec![Vec::new(); partition.stops.len()];
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stops =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
        for (s_idx, &stop_id) in stops.iter().enumerate() {
            stop_to_patterns[stop_id as usize].push((p_idx, s_idx));
        }
    }
    stop_to_patterns
}

/// Algorithm 1: Initial transfer computation
pub fn compute_initial_transfers(partition: &TransitPartition) -> Vec<Transfer> {
    let mut transfers = Vec::new();

    // Build compact stop -> patterns mapping (pattern idx, stop idx in pattern)
    let stop_to_patterns = build_stop_to_patterns(partition);

    println!("compact stop map built");

    // Build Footpath Graph
    let mut footpaths: Vec<Vec<(u32, u32)>> = vec![Vec::new(); partition.stops.len()];
    let mut change_times = vec![0u32; partition.stops.len()];

    for transfer in &partition.internal_transfers {
        if transfer.from_stop_idx == transfer.to_stop_idx {
            change_times[transfer.from_stop_idx as usize] = transfer.duration_seconds;
        } else {
            footpaths[transfer.from_stop_idx as usize]
                .push((transfer.to_stop_idx, transfer.duration_seconds));
        }
    }

    // Add self-loops with respecting change times
    for i in 0..partition.stops.len() {
        footpaths[i].push((i as u32, change_times[i]));
    }

    println!("footpaths built");

    // Reusable buffers
    let num_patterns = partition.trip_patterns.len();
    let mut seen_patterns = vec![false; num_patterns];
    let mut seen_list: Vec<usize> = Vec::new();

    // Reused set for per-source-stop deduplication to avoid allocation churn
    let mut seen_targets: HashSet<(usize, usize, usize)> = HashSet::default();
    seen_targets.reserve(128);

    // Reserve a bit of space early to reduce reallocation churn (tweak heuristically)
    transfers.reserve(1024);

    // For each pattern/trip/stop (same outer loops as before)
    println!("trip patterns: {}", partition.trip_patterns.len());
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        for (t_idx, trip) in pattern.trips.iter().enumerate() {
            // Compute arrival/departure times along the trip
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

                // Skip transfers from first stop
                if i == 0 {
                    continue;
                }

                seen_targets.clear();

                // Check transfers from stop `stop_idx`
                if let Some(reachable_stops) = footpaths.get(stop_idx as usize) {
                    for &(q, walk_time) in reachable_stops {
                        let min_dep_time = arrival_time + walk_time;

                        // For this reachable stop q: iterate patterns that serve q
                        if let Some(patterns_at_q) = stop_to_patterns.get(q as usize) {
                            for &(cand_pattern_idx, cand_stop_idx_in_pattern) in patterns_at_q {
                                // Find earliest trip in cand_pattern that departs cand_stop_idx_in_pattern >= min_dep_time
                                let cand_pattern = &partition.trip_patterns[cand_pattern_idx];
                                let trips = &cand_pattern.trips;
                                let mut lo = 0usize;
                                let mut hi = trips.len();
                                while lo < hi {
                                    let mid = (lo + hi) / 2;
                                    let dep = get_departure_time(
                                        partition,
                                        &trips[mid],
                                        cand_stop_idx_in_pattern,
                                    );
                                    if dep < min_dep_time {
                                        lo = mid + 1;
                                    } else {
                                        hi = mid;
                                    }
                                }
                                if lo >= trips.len() {
                                    continue; // no candidate in this pattern
                                }
                                let cand_trip_idx = lo;

                                // Skip transfers to last stop
                                let cand_stops = &partition.direction_patterns
                                    [cand_pattern.direction_pattern_idx as usize]
                                    .stop_indices;
                                if cand_stop_idx_in_pattern + 1 == cand_stops.len() {
                                    continue;
                                }

                                // Additional validity checks (skip if same pattern and invalid)
                                let is_same_pattern = cand_pattern_idx == p_idx;
                                let valid = if is_same_pattern {
                                    cand_trip_idx < t_idx || cand_stop_idx_in_pattern < i
                                } else {
                                    true
                                };
                                if !valid {
                                    continue;
                                }

                                // Deduplicate by exact (pattern, trip, stop) triple for this source stop
                                let key =
                                    (cand_pattern_idx, cand_trip_idx, cand_stop_idx_in_pattern);
                                if !seen_targets.insert(key) {
                                    // we already added this exact target for this source stop
                                    continue;
                                }

                                // push transfer
                                transfers.push(Transfer {
                                    from_pattern_idx: p_idx,
                                    from_trip_idx: t_idx,
                                    from_stop_idx_in_pattern: i as u16,
                                    to_pattern_idx: cand_pattern_idx,
                                    to_trip_idx: cand_trip_idx,
                                    to_stop_idx_in_pattern: cand_stop_idx_in_pattern as u16,
                                    duration: walk_time,
                                });
                            } // end for patterns_at_q
                        }
                    } // end reachable_stops
                }
                // seen_targets dropped/cleared at end of this source stop iteration
            }
        }
    }

    println!("{} initial transfers", transfers.len());

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
        let prev_stop_idx = from_stops[tr.from_stop_idx_in_pattern as usize - 1];

        let to_pattern = &partition.trip_patterns[tr.to_pattern_idx];
        let to_stops =
            &partition.direction_patterns[to_pattern.direction_pattern_idx as usize].stop_indices;

        if (tr.to_stop_idx_in_pattern as usize) + 1 >= to_stops.len() {
            return true;
        }
        let next_stop_idx_on_target = to_stops[tr.to_stop_idx_in_pattern as usize + 1];

        if prev_stop_idx == next_stop_idx_on_target {
            let t_trip = &from_pattern.trips[tr.from_trip_idx];
            let u_trip = &to_pattern.trips[tr.to_trip_idx];

            let arr_t_prev = get_arrival_time(partition, t_trip, tr.from_stop_idx_in_pattern as usize - 1);
            let dep_u_next = get_departure_time(partition, u_trip, tr.to_stop_idx_in_pattern as usize + 1);

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
                        if (tr.from_stop_idx_in_pattern as usize) != i {
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
                        let boarding_stop_idx = tr.to_stop_idx_in_pattern as usize;
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
pub fn compute_profile_query(
    partition: &TransitPartition,
    transfers: &[Transfer],
    trip_transfer_ranges: &HashMap<(usize, usize), (usize, usize)>,
    start_stop: u32,
    start_time: u32,
    targets: &[u32],
    stop_to_patterns: &[Vec<(usize, usize)>],
    flat_id_to_pattern_trip: &[(usize, usize)],
    pattern_trip_offset: &[usize],
    max_transfers: usize,
    scratch: &mut ProfileScratch,
    hubs: &HashSet<u32>,
    is_source_hub: bool,
) {
    let num_stops = partition.stops.len();
    let total_trips = flat_id_to_pattern_trip.len();

    scratch.r_labels.clear();
    scratch
        .r_labels
        .resize(max_transfers + 1, vec![u32::MAX; total_trips]);

    // Cheap per-call resets
    scratch.initial_walks.clear();
    scratch.seeds.clear();
    // We don't use used_segments/used_transfers in the forward pass anymore
    scratch.used_segments.clear();
    scratch.used_transfers.clear();

    if scratch.is_target.len() != num_stops {
        scratch.is_target.resize(num_stops, false);
    }
    scratch.is_target.fill(false);

    // Helpers for flat trip ID
    let get_flat_id = |p: usize, t: usize| -> usize { pattern_trip_offset[p] + t };

    // 2. Identify Target Set
    for &t in targets {
        scratch.is_target[t as usize] = true;
    }

    // 3. Identify Initial Walks (Source -> Stops)
    scratch.initial_walks.push((start_stop, 0));
    for tr in &partition.internal_transfers {
        if tr.from_stop_idx == start_stop {
            scratch
                .initial_walks
                .push((tr.to_stop_idx, tr.duration_seconds));
        }
    }

    // 4. Collect all Departure Opportunities (Seeds)
    for &(stop_id, walk_dur) in &scratch.initial_walks {
        if let Some(patterns) = stop_to_patterns.get(stop_id as usize) {
            for &(p_idx, s_idx) in patterns {
                let pattern = &partition.trip_patterns[p_idx];
                let min_dep = start_time + walk_dur;

                // TODO: binary search instead of linear
                for (t_idx, trip) in pattern.trips.iter().enumerate() {
                    let dep = get_departure_time(partition, trip, s_idx);
                    if dep >= min_dep {
                        scratch.seeds.push(Seed {
                            dep_time: dep - walk_dur, // Effective start time at source
                            pattern_idx: p_idx,
                            trip_idx: t_idx,
                            stop_idx: s_idx,
                            walk_duration: walk_dur,
                            stop_id,
                        });
                        break;
                    }
                }
            }
        }
    }
    // Sort seeds descending by departure time
    scratch.seeds.sort_by_key(|s| std::cmp::Reverse(s.dep_time));

    // 5. Trip-Based Profile Search (Forward Pass)
    if scratch.r_labels.len() <= max_transfers {
        scratch
            .r_labels
            .resize(max_transfers + 1, vec![u32::MAX; total_trips]);
    }
    if scratch.r_labels[0].len() < total_trips {
        for level in &mut scratch.r_labels {
            level.resize(total_trips, u32::MAX);
        }
    }

    // Stack for propagation: (n, flat_id, entry_stop_idx, old_r_val)
    let mut stack = Vec::new();
    let mut active_in_stack = 0;

    const INACTIVE_MASK: u32 = 0x80000000;
    const IDX_MASK: u32 = !INACTIVE_MASK;

    for seed in &scratch.seeds {
        let flat_id = get_flat_id(seed.pattern_idx, seed.trip_idx);

        // Update R[0]
        let old_r = scratch.r_labels[0][flat_id];
        let old_r_idx = old_r & IDX_MASK;

        // Seeds are always active initially (unless we start AT a hub? No, even then).
        // If we start at a hub, is_source_hub is true, so we don't prune.
        // If we start at a non-hub, we are active until we hit a hub.

        if (seed.stop_idx as u32) < old_r_idx {
            scratch.r_labels[0][flat_id] = seed.stop_idx as u32;
            stack.push((0, flat_id, seed.stop_idx as u32, old_r));
            active_in_stack += 1;

            while let Some((n, flat_id, stop_idx_raw, old_r_val)) = stack.pop() {
                let stop_idx = (stop_idx_raw & IDX_MASK) as usize;
                let is_inactive = (stop_idx_raw & INACTIVE_MASK) != 0;

                if is_inactive {
                    if active_in_stack == 0 {
                        // Optimization: If we are processing an inactive label, and there are NO active labels
                        // left in the stack, we can stop exploring this seed.
                        // Inactive labels cannot produce active labels (they stay inactive).
                        // They are only needed to dominate non-optimal paths around hubs.
                        // But if we have no active paths to potentially dominate or extend, we are done.
                        stack.clear();
                        break;
                    }
                } else {
                    active_in_stack -= 1;
                }

                let (p_idx, t_idx) = flat_id_to_pattern_trip[flat_id];

                let pattern = &partition.trip_patterns[p_idx];
                let stop_indices = &partition.direction_patterns
                    [pattern.direction_pattern_idx as usize]
                    .stop_indices;

                let old_r_idx = (old_r_val & IDX_MASK) as usize;
                let scan_limit = std::cmp::min(old_r_idx, stop_indices.len());

                // Look up transfers for this trip
                let range = if let Some(r) = trip_transfer_ranges.get(&(p_idx, t_idx)) {
                    r.clone()
                } else {
                    (0, 0)
                };

                let mut tr_ptr = range.0;
                let tr_end = range.1;

                for i in stop_idx..scan_limit {
                    // Check if we become inactive at this stop
                    // "The local search additionally marks labels stemming from labels at transfer nodes of hubs as inactive"
                    // If we are at a hub, and we transfer, the NEW label is inactive.
                    // But wait, if we are ALREADY inactive, we stay inactive.

                    let current_stop_global = stop_indices[i];
                    let becomes_inactive = if !is_source_hub {
                        hubs.contains(&current_stop_global) && current_stop_global != start_stop
                    } else {
                        false
                    };

                    // Process transfers from this stop
                    if n < max_transfers {
                        while tr_ptr < tr_end {
                            let tr = &transfers[tr_ptr];
                            if (tr.from_stop_idx_in_pattern as usize) < i {
                                tr_ptr += 1;
                                continue;
                            }
                            if (tr.from_stop_idx_in_pattern as usize) > i {
                                break;
                            }

                            // Found transfer
                            let target_flat = get_flat_id(tr.to_pattern_idx, tr.to_trip_idx);
                            let target_stop_idx = tr.to_stop_idx_in_pattern as u32;

                            let next_n = n + 1;
                            let old_target_r = scratch.r_labels[next_n][target_flat];
                            let old_target_r_idx = old_target_r & IDX_MASK;

                            if target_stop_idx < old_target_r_idx {
                                // Determine new state
                                let next_inactive = is_inactive || becomes_inactive;
                                let mut next_val = target_stop_idx;
                                if next_inactive {
                                    next_val |= INACTIVE_MASK;
                                }

                                scratch.r_labels[next_n][target_flat] = next_val;
                                stack.push((next_n, target_flat, next_val, old_target_r));
                                if !next_inactive {
                                    active_in_stack += 1;
                                }
                            }

                            tr_ptr += 1;
                        }
                    }
                }
            }
        }
    }

    // 6. Backtracking Phase (Identify Pareto-Optimal Edges)
    // best_arrivals[stop_idx] = Vec<(transfers, arrival_time, is_inactive)>
    let mut best_arrivals: Vec<Vec<(usize, u32, bool)>> = vec![Vec::new(); num_stops];

    // Step 6a: Compute Pareto Frontier for each target stop
    for n in 0..=max_transfers {
        for (flat_id, &entry_val) in scratch.r_labels[n].iter().enumerate() {
            if entry_val == u32::MAX {
                continue;
            }
            let entry_idx = (entry_val & IDX_MASK) as usize;
            let is_inactive = (entry_val & INACTIVE_MASK) != 0;

            let (p_idx, t_idx) = flat_id_to_pattern_trip[flat_id];
            let pattern = &partition.trip_patterns[p_idx];
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            let trip = &pattern.trips[t_idx];

            let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
            let mut current_time = trip.start_time;

            // Fast forward to entry_idx
            for k in 0..=entry_idx {
                if k > 0 {
                    if 2 * k < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * k];
                    }
                }
                if k < entry_idx {
                    if 2 * k + 1 < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * k + 1];
                    }
                }
            }

            let mut passed_hub = false;
            for k in entry_idx..stop_indices.len() {
                if k > entry_idx {
                    if 2 * k < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * k];
                    }
                }
                let arr_time = current_time;
                let stop_id = stop_indices[k] as usize;

                if scratch.is_target[stop_id] {
                    // Update Pareto frontier for stop_id
                    let effective_inactive = is_inactive || passed_hub;
                    let mut dominated = false;
                    for &(existing_n, existing_time, existing_inactive) in &best_arrivals[stop_id] {
                        // If existing is better (less transfers, earlier time)
                        // AND (existing is active OR we are inactive)
                        // If existing is inactive, it can dominate active? No.
                        // Active dominates Inactive? Yes.
                        // Inactive dominates Inactive? Yes.

                        // Wait, Note 8: "Inactive labels are needed to dominate non-optimal paths around hubs."
                        // So Inactive CAN dominate Active if it's strictly better?
                        // "Inactive labels are ignored when the transfer patterns are read off."
                        // So we store them in best_arrivals to prevent worse paths from being added,
                        // but we mark them as inactive.

                        if existing_n <= n && existing_time <= arr_time {
                            if !existing_inactive || effective_inactive {
                                dominated = true;
                                break;
                            }
                        }
                    }
                    if !dominated {
                        best_arrivals[stop_id].retain(
                            |&(existing_n, existing_time, existing_inactive)| {
                                // Remove if we dominate existing
                                // We dominate if we are better AND (we are active OR existing is inactive)
                                let strictly_better = n <= existing_n && arr_time <= existing_time;
                                if strictly_better {
                                    if !effective_inactive || existing_inactive {
                                        return false; // Remove existing
                                    }
                                }
                                true
                            },
                        );
                        best_arrivals[stop_id].push((n, arr_time, effective_inactive));
                    }
                }

                if !is_source_hub || stop_id != start_stop as usize {
                    if hubs.contains(&(stop_id as u32)) {
                        passed_hub = true;
                    }
                }

                if 2 * k + 1 < delta_seq.deltas.len() {
                    current_time += delta_seq.deltas[2 * k + 1];
                }
            }
        }
    }

    // Step 6b: Mark Useful Trips and Backtrack
    // useful_trips: Map<(n, flat_id), max_stop_idx>
    // Step 6b: Mark Useful Trips
    let mut useful_trips: HashMap<(usize, usize), usize> = HashMap::new();

    // Re-scan to mark useful trips based on best_arrivals
    for n in 0..=max_transfers {
        for (flat_id, &entry_val) in scratch.r_labels[n].iter().enumerate() {
            if entry_val == u32::MAX {
                continue;
            }
            let entry_idx = (entry_val & IDX_MASK) as usize;
            let is_inactive = (entry_val & INACTIVE_MASK) != 0;

            let (p_idx, t_idx) = flat_id_to_pattern_trip[flat_id];
            let pattern = &partition.trip_patterns[p_idx];
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            let trip = &pattern.trips[t_idx];

            let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
            let mut current_time = trip.start_time;
            for k in 0..=entry_idx {
                if k > 0 {
                    if 2 * k < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * k];
                    }
                }
                if k < entry_idx {
                    if 2 * k + 1 < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * k + 1];
                    }
                }
            }

            let mut passed_hub = false;
            for k in entry_idx..stop_indices.len() {
                if k > entry_idx {
                    if 2 * k < delta_seq.deltas.len() {
                        current_time += delta_seq.deltas[2 * k];
                    }
                }
                let arr_time = current_time;
                let stop_id = stop_indices[k] as usize;

                if scratch.is_target[stop_id] {
                    // Check if this arrival is in the Pareto frontier
                    // AND if it should be stored (Active, or Hub Access)

                    let mut is_optimal = false;
                    let mut is_target_inactive = false;

                    let effective_inactive = is_inactive || passed_hub;

                    for &(best_n, best_time, best_inactive) in &best_arrivals[stop_id] {
                        if best_n == n
                            && best_time == arr_time
                            && best_inactive == effective_inactive
                        {
                            is_optimal = true;
                            is_target_inactive = best_inactive;
                            break;
                        }
                    }

                    if is_optimal {
                        // "Inactive labels are ignored when the transfer patterns are read off."
                        // UNLESS it's an access station?
                        // "If any of the C_i is a hub, we do not store this pattern anymore. The hub C_i with minimal i is called an access station of A."
                        // "We store transfer patterns A...C_i and C_i...B into and out of the access station."

                        // If we are inactive, it means we passed through a hub already.
                        // If we are active, we store it.
                        // If we are inactive, we DO NOT store it.
                        // BUT, if the *current stop* is the FIRST hub we hit, we are still active arriving at it?
                        // No, `becomes_inactive` happens on TRANSFER at a hub.
                        // If we just arrive at a hub on an active trip, we are active.
                        // So if `is_inactive` is false, we store.

                        // What if we are inactive?
                        // Then we don't store.

                        if !is_target_inactive {
                            let entry = useful_trips.entry((n, flat_id)).or_insert(0);
                            *entry = std::cmp::max(*entry, k);
                        }
                    }
                }

                if !is_source_hub || stop_id != start_stop as usize {
                    if hubs.contains(&(stop_id as u32)) {
                        passed_hub = true;
                    }
                }

                if 2 * k + 1 < delta_seq.deltas.len() {
                    current_time += delta_seq.deltas[2 * k + 1];
                }
            }
        }
    }

    // Step 6c: Recursive Backtracking
    for n in (1..=max_transfers).rev() {
        // Collect useful trips for this level
        // We iterate all transfers to find predecessors
        for (tr_idx, tr) in transfers.iter().enumerate() {
            let target_flat = get_flat_id(tr.to_pattern_idx, tr.to_trip_idx);

            if let Some(&max_idx) = useful_trips.get(&(n, target_flat)) {
                let r_val = scratch.r_labels[n][target_flat];
                let r_idx = (r_val & IDX_MASK) as usize;

                if (tr.to_stop_idx_in_pattern as usize) == r_idx {
                    let source_flat = get_flat_id(tr.from_pattern_idx, tr.from_trip_idx);
                    let source_entry_val = scratch.r_labels[n - 1][source_flat];
                    let source_entry_idx = (source_entry_val & IDX_MASK) as usize;

                    if source_entry_val != u32::MAX
                        && source_entry_idx <= tr.from_stop_idx_in_pattern as usize
                    {
                        let entry = useful_trips.entry((n - 1, source_flat)).or_insert(0);
                        *entry = std::cmp::max(*entry, tr.from_stop_idx_in_pattern as usize);

                        scratch.used_transfers.insert(tr_idx);
                    }
                }
            }
        }
    }

    // Step 6d: Mark Initial Walks
    for ((n, flat_id), _) in &useful_trips {
        if *n == 0 {
            let entry_val = scratch.r_labels[0][*flat_id];
            if entry_val == u32::MAX {
                continue;
            }
            let entry_idx = (entry_val & IDX_MASK) as usize;
            let (p_idx, t_idx) = flat_id_to_pattern_trip[*flat_id];

            // Iterate seeds to find the source
            for seed in &scratch.seeds {
                if seed.pattern_idx == p_idx && seed.trip_idx == t_idx && seed.stop_idx == entry_idx
                {
                    scratch.used_initial_walks.insert(seed.stop_id);
                }
            }
        }
    }

    // 7. Reconstruct Graph from Used Components
    // Reuse scratch.result_edges
    scratch.result_edges.clear();

    // Add Initial Walks
    for &stop_id in &scratch.used_initial_walks {
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

        scratch.result_edges.push(DagEdge {
            from_node_idx: start_stop,
            to_node_idx: stop_id,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: dur,
            })),
        });
    }

    // Add Trip Segments
    for ((n, flat_id), &max_idx) in &useful_trips {
        let entry_val = scratch.r_labels[*n][*flat_id];
        if entry_val == u32::MAX {
            continue;
        }
        let entry_idx = (entry_val & IDX_MASK) as usize;

        // Optional safety assert: this should hold by construction
        debug_assert!(
            max_idx >= entry_idx,
            "max_idx < entry_idx for (n={}, flat_id={})",
            n,
            flat_id
        );

        let (p_idx, t_idx) = flat_id_to_pattern_trip[*flat_id];
        let pattern = &partition.trip_patterns[p_idx];
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        let cap = max_idx - entry_idx + 2;
        let mut relevant_indices: Vec<u16> = Vec::with_capacity(cap);
        relevant_indices.push(entry_idx as u16);

        // Check targets
        for k in (entry_idx)..=max_idx {
            let stop_id = stop_indices[k] as usize;
            if scratch.is_target[stop_id] {
                let trip = &pattern.trips[t_idx];
                let arr = get_arrival_time(partition, trip, k);
                if best_arrivals[stop_id].contains(&(*n, arr, false))
                    || best_arrivals[stop_id].contains(&(*n, arr, true))
                {
                    relevant_indices.push(k as u16);
                }
            }
        }

        // Check transfers (sources)
        for &tr_idx in &scratch.used_transfers {
            let tr = &transfers[tr_idx];
            if tr.from_pattern_idx == p_idx
                && tr.from_trip_idx == t_idx
                && tr.from_stop_idx_in_pattern as usize >= entry_idx
                && tr.from_stop_idx_in_pattern as usize <= max_idx
            {
                relevant_indices.push(tr.from_stop_idx_in_pattern);
            }
        }

        relevant_indices.sort();
        relevant_indices.dedup();

        for w in 0..relevant_indices.len() - 1 {
            let idx1 = relevant_indices[w];
            let idx2 = relevant_indices[w + 1];

            let u = stop_indices[idx1 as usize];
            let v = stop_indices[idx2 as usize];

            let trip = &pattern.trips[t_idx];
            let arr = get_arrival_time(partition, trip, idx2 as usize);
            let dep = get_departure_time(partition, trip, idx1 as usize);
            let dur = arr.saturating_sub(dep);

            scratch.result_edges.push(DagEdge {
                from_node_idx: u,
                to_node_idx: v,
                edge_type: Some(EdgeType::Transit(TransitEdge {
                    trip_pattern_idx: p_idx as u32,
                    start_stop_idx: idx1 as u32,
                    end_stop_idx: idx2 as u32,
                    min_duration: dur,
                })),
            });
        }
    }

    // Add Transfers
    for &tr_idx in &scratch.used_transfers {
        let tr = &transfers[tr_idx];
        let (p_from, _) =
            flat_id_to_pattern_trip[get_flat_id(tr.from_pattern_idx, tr.from_trip_idx)];
        let (p_to, _) = flat_id_to_pattern_trip[get_flat_id(tr.to_pattern_idx, tr.to_trip_idx)];

        let u = partition.direction_patterns
            [partition.trip_patterns[p_from].direction_pattern_idx as usize]
            .stop_indices[tr.from_stop_idx_in_pattern as usize];
        let v = partition.direction_patterns
            [partition.trip_patterns[p_to].direction_pattern_idx as usize]
            .stop_indices[tr.to_stop_idx_in_pattern as usize];

        scratch.result_edges.push(DagEdge {
            from_node_idx: u,
            to_node_idx: v,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: tr.duration,
            })),
        });
    }

    // Deduplicate edges
    // Key: (from, to, type (0=Walk, 1=Transit), p_idx, s_idx, e_idx)
    scratch.dedup_map.clear();

    // Iterate result_edges (we can drain or iterate; iteration is fine since we clear result_edges next time)
    // Actually, we want to leave the FINAL edges in result_edges.
    // So we populate dedup_map, then write back to result_edges?

    for edge in scratch.result_edges.drain(..) {
        let key = match &edge.edge_type {
            Some(EdgeType::Transit(t)) => (
                edge.from_node_idx,
                edge.to_node_idx,
                1,
                t.trip_pattern_idx,
                t.start_stop_idx,
                t.end_stop_idx,
            ),
            Some(EdgeType::LongDistanceTransit(t)) => (
                edge.from_node_idx,
                edge.to_node_idx,
                2,
                t.trip_pattern_idx,
                t.start_stop_idx,
                t.end_stop_idx,
            ),
            Some(EdgeType::Walk(_)) => (edge.from_node_idx, edge.to_node_idx, 0, 0, 0, 0),
            None => continue,
        };

        match scratch.dedup_map.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                match (&mut existing.edge_type, &edge.edge_type) {
                    (Some(EdgeType::Transit(t1)), Some(EdgeType::Transit(t2))) => {
                        if t2.min_duration < t1.min_duration {
                            t1.min_duration = t2.min_duration;
                        }
                    }
                    (
                        Some(EdgeType::LongDistanceTransit(t1)),
                        Some(EdgeType::LongDistanceTransit(t2)),
                    ) => {
                        if t2.min_duration < t1.min_duration {
                            t1.min_duration = t2.min_duration;
                        }
                    }
                    (Some(EdgeType::Walk(w1)), Some(EdgeType::Walk(w2))) => {
                        if w2.duration_seconds < w1.duration_seconds {
                            w1.duration_seconds = w2.duration_seconds;
                        }
                    }
                    _ => {}
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(edge);
            }
        }
    }

    // Move back to result_edges
    scratch
        .result_edges
        .extend(scratch.dedup_map.drain().map(|(_, v)| v));
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
