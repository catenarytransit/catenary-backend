use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use catenary::routing_common::osm_graph::save_pbf;
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, EdgeType, GlobalHub, GlobalPatternIndex, LocalTransferPattern,
    PartitionDag, TransitEdge, TransitPartition, TransitStop, WalkEdge,
};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::PathBuf;

pub fn compute_border_patterns(border_nodes: &HashMap<u32, Vec<TransitStop>>) {
    // Identify connections between border nodes within the same partition.
    // This is handled by `compute_intra_partition_connectivity` and stored in `intra_partition_edges`.
    // We just log the stats here.
    let total_border_nodes: usize = border_nodes.values().map(|v| v.len()).sum();
    println!(
        "  - Border Patterns: Identified {} border nodes across {} partitions (Connectivity computed)",
        total_border_nodes,
        border_nodes.len()
    );
}

pub fn compute_global_patterns(
    border_nodes: &HashMap<u32, Vec<TransitStop>>,
    cross_edges: &[((usize, usize), DagEdge)],
    intra_edges: &[((usize, usize), DagEdge)],
    global_to_partition_map: &HashMap<usize, (u32, u32)>,
    output_dir: &PathBuf,
) {
    println!("  - Building Global Hub Graph...");

    // 1. Build the Global Hub Graph
    // Nodes: Global Stop Indices (usize)
    // Edges: Adjacency List
    let mut graph: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut reverse_graph: HashMap<usize, Vec<usize>> = HashMap::new();

    let mut all_edges = Vec::new();
    all_edges.extend_from_slice(cross_edges);
    all_edges.extend_from_slice(intra_edges);

    let mut edge_types: HashMap<(usize, usize), EdgeType> = HashMap::new();

    for &((u, v), ref edge) in &all_edges {
        graph.entry(u).or_default().push(v);
        reverse_graph.entry(v).or_default().push(u);
        if let Some(et) = &edge.edge_type {
            edge_types.insert((u, v), et.clone());
        }
    }

    // 2. Compute All-Pairs Reachability between Partitions (DAGs)
    let partition_ids: Vec<u32> = border_nodes.keys().cloned().collect();
    let mut partition_dags: Vec<PartitionDag> = Vec::new();

    // Pre-compute hubs per partition (as global indices)
    let mut all_graph_nodes = HashSet::new();
    for &u in graph.keys() {
        all_graph_nodes.insert(u);
    }
    for &u in reverse_graph.keys() {
        all_graph_nodes.insert(u);
    }

    let mut partition_hubs: HashMap<u32, Vec<usize>> = HashMap::new();
    for &u in &all_graph_nodes {
        if let Some(&(pid, _)) = global_to_partition_map.get(&u) {
            partition_hubs.entry(pid).or_default().push(u);
        }
    }

    println!(
        "  - Computing DAGs for {} partitions...",
        partition_ids.len()
    );
    let mut dag_count = 0;

    for &p_start in &partition_ids {
        for &p_end in &partition_ids {
            if p_start == p_end {
                continue;
            }

            let start_hubs = partition_hubs
                .get(&p_start)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let end_hubs = partition_hubs
                .get(&p_end)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            if start_hubs.is_empty() || end_hubs.is_empty() {
                continue;
            }

            // Dijkstra State
            #[derive(Copy, Clone, Eq, PartialEq)]
            struct State {
                cost: u32,
                node: usize,
            }
            impl Ord for State {
                fn cmp(&self, other: &Self) -> Ordering {
                    other.cost.cmp(&self.cost) // Min-heap
                }
            }
            impl PartialOrd for State {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }

            let mut useful_edges: HashSet<(usize, usize)> = HashSet::new();
            let mut useful_nodes: HashSet<usize> = HashSet::new();

            // Run Dijkstra from EACH start hub
            for &start_node in start_hubs {
                let mut dist: HashMap<usize, u32> = HashMap::new();
                let mut predecessors: HashMap<usize, Vec<usize>> = HashMap::new();

                let mut pq = BinaryHeap::new();

                dist.insert(start_node, 0);
                pq.push(State {
                    cost: 0,
                    node: start_node,
                });

                while let Some(State { cost, node: u }) = pq.pop() {
                    if cost > *dist.get(&u).unwrap_or(&u32::MAX) {
                        continue;
                    }

                    if let Some(neighbors) = graph.get(&u) {
                        for &v in neighbors {
                            let weight = if let Some(et) = edge_types.get(&(u, v)) {
                                match et {
                                    EdgeType::Transit(t) => t.min_duration,
                                    EdgeType::Walk(w) => w.duration_seconds,
                                }
                            } else {
                                0
                            };

                            let next_cost = cost + weight;
                            let curr_dist = *dist.get(&v).unwrap_or(&u32::MAX);

                            if next_cost < curr_dist {
                                dist.insert(v, next_cost);
                                predecessors.insert(v, vec![u]);
                                pq.push(State {
                                    cost: next_cost,
                                    node: v,
                                });
                            } else if next_cost == curr_dist {
                                predecessors.entry(v).or_default().push(u);
                            }
                        }
                    }
                }

                // Trace back from ALL end hubs
                for &end_node in end_hubs {
                    if let Some(&d) = dist.get(&end_node) {
                        if d == u32::MAX {
                            continue;
                        }

                        // BFS back from end_node using predecessors
                        let mut q = std::collections::VecDeque::new();
                        q.push_back(end_node);
                        useful_nodes.insert(end_node);

                        let mut visited_back = HashSet::new();
                        visited_back.insert(end_node);

                        while let Some(curr) = q.pop_front() {
                            if curr == start_node {
                                continue;
                            }
                            if let Some(preds) = predecessors.get(&curr) {
                                for &pred in preds {
                                    if useful_edges.insert((pred, curr)) {
                                        useful_nodes.insert(pred);
                                        // Only push if not visited to avoid cycles/redundancy
                                        if visited_back.insert(pred) {
                                            q.push_back(pred);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if useful_nodes.is_empty() {
                continue;
            }

            // Build DAG
            let mut dag_hubs_vec: Vec<usize> = useful_nodes.into_iter().collect();
            dag_hubs_vec.sort();
            let mut global_to_dag_idx: HashMap<usize, u32> = HashMap::new();
            for (i, &g_idx) in dag_hubs_vec.iter().enumerate() {
                global_to_dag_idx.insert(g_idx, i as u32);
            }

            let mut dag_edges = Vec::new();
            for &(u, v) in &useful_edges {
                if global_to_dag_idx.contains_key(&u) && global_to_dag_idx.contains_key(&v) {
                    if let Some(et) = edge_types.get(&(u, v)) {
                        dag_edges.push(DagEdge {
                            from_hub_idx: *global_to_dag_idx.get(&u).unwrap(),
                            to_hub_idx: *global_to_dag_idx.get(&v).unwrap(),
                            edge_type: Some(et.clone()),
                        });
                    }
                }
            }

            // Convert hubs to GlobalHub
            let mut final_hubs = Vec::new();
            for &g_idx in &dag_hubs_vec {
                if let Some(&(pid, l_idx)) = global_to_partition_map.get(&g_idx) {
                    final_hubs.push(GlobalHub {
                        original_partition_id: pid,
                        stop_idx_in_partition: l_idx,
                    });
                }
            }

            partition_dags.push(PartitionDag {
                from_partition: p_start,
                to_partition: p_end,
                hubs: final_hubs,
                edges: dag_edges,
            });
            dag_count += 1;
        }
    }

    println!("  - Generated {} Partition DAGs", dag_count);

    let global_index = GlobalPatternIndex { partition_dags };

    let path = output_dir.join("global_patterns.pbf");
    if let Err(e) = save_pbf(&global_index, path.to_str().unwrap()) {
        eprintln!("Failed to save global patterns: {}", e);
    } else {
        println!("  - Global Patterns: Saved index to {:?}", path);
    }
}

pub fn compute_intra_partition_connectivity(
    partition: &TransitPartition,
    global_indices: &[u32],
) -> Vec<((usize, usize), DagEdge)> {
    let mut edges = Vec::new();

    // Map local_idx -> global_idx
    let local_to_global: Vec<usize> = global_indices.iter().map(|&i| i as usize).collect();

    // Identify Hubs (Local Indices)
    let is_hub_or_border: Vec<bool> = partition
        .stops
        .iter()
        .map(|s| s.is_hub || s.is_border)
        .collect();

    // 1. Transit Edges: Scan all patterns
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;

        // Find all hubs on this pattern
        let mut hubs_on_pattern = Vec::new();
        for (i, &stop_idx) in stop_indices.iter().enumerate() {
            if is_hub_or_border[stop_idx as usize] {
                hubs_on_pattern.push((i, stop_idx));
            }
        }

        // Create edges between adjacent hubs on the pattern
        for k in 0..hubs_on_pattern.len().saturating_sub(1) {
            let (idx1, stop1) = hubs_on_pattern[k];
            let (idx2, stop2) = hubs_on_pattern[k + 1];

            // Calculate min_duration
            let mut min_duration = u32::MAX;
            for trip in &pattern.trips {
                let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
                let mut duration = 0;
                if 2 * idx2 < delta_seq.deltas.len() {
                    for k in (idx1 + 1)..=idx2 {
                        // Add travel time to k
                        duration += delta_seq.deltas[2 * k];
                        // Add dwell time at k-1 (if k > idx1 + 1)
                        if k > idx1 + 1 {
                            duration += delta_seq.deltas[2 * (k - 1) + 1];
                        }
                    }
                }
                if duration < min_duration {
                    min_duration = duration;
                }
            }
            if min_duration == u32::MAX {
                min_duration = 0;
            } // Should not happen

            // Create TransitEdge
            let edge = TransitEdge {
                trip_pattern_idx: p_idx as u32,
                start_stop_idx: idx1 as u32,
                end_stop_idx: idx2 as u32,
                min_duration,
            };

            edges.push((
                (
                    local_to_global[stop1 as usize],
                    local_to_global[stop2 as usize],
                ),
                DagEdge {
                    from_hub_idx: stop1, // Local
                    to_hub_idx: stop2,   // Local
                    edge_type: Some(EdgeType::Transit(edge)),
                },
            ));
        }
    }

    // 2. Walk Edges: Scan internal transfers
    for transfer in &partition.internal_transfers {
        if is_hub_or_border[transfer.from_stop_idx as usize]
            && is_hub_or_border[transfer.to_stop_idx as usize]
        {
            let edge = WalkEdge {
                duration_seconds: transfer.duration_seconds,
            };

            edges.push((
                (
                    local_to_global[transfer.from_stop_idx as usize],
                    local_to_global[transfer.to_stop_idx as usize],
                ),
                DagEdge {
                    from_hub_idx: transfer.from_stop_idx,
                    to_hub_idx: transfer.to_stop_idx,
                    edge_type: Some(EdgeType::Walk(edge)),
                },
            ));
        }
    }

    edges
}

pub fn compute_local_patterns_for_partition(partition: &mut TransitPartition) {
    println!(
        "    - Computing LTPs for partition {} ({} stops)...",
        partition.partition_id,
        partition.stops.len()
    );

    let hubs: Vec<u32> = partition
        .stops
        .iter()
        .enumerate()
        .filter(|(_, s)| s.is_hub || s.is_border) // Compute LTPs to all Global Nodes
        .map(|(i, _)| i as u32)
        .collect();

    if hubs.is_empty() {
        return;
    }

    // Precompute stop -> patterns map
    let mut stop_to_patterns: Vec<Vec<usize>> = vec![Vec::new(); partition.stops.len()];
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
        for &s_idx in stop_indices {
            stop_to_patterns[s_idx as usize].push(p_idx);
        }
    }

    let mut ltps = Vec::new();

    // For each stop, run profile search to hubs
    for start_node in 0..partition.stops.len() {
        let start_node = start_node as u32;

        // Run simplified Raptor at 8:00 AM (28800s)
        let edges = run_raptor(partition, start_node, &hubs, 28800, &stop_to_patterns);

        if !edges.is_empty() {
            ltps.push(LocalTransferPattern {
                from_stop_idx: start_node,
                edges,
            });
        }
    }

    partition.local_transfer_patterns = ltps;
}

pub fn run_raptor(
    partition: &TransitPartition,
    start_node: u32,
    targets: &[u32],
    start_time: u32,
    stop_to_patterns: &[Vec<usize>],
) -> Vec<DagEdge> {
    let num_stops = partition.stops.len();
    let mut earliest_arrival = vec![u32::MAX; num_stops];
    earliest_arrival[start_node as usize] = start_time;

    let mut marked_stops = HashSet::new();
    marked_stops.insert(start_node);

    // Track predecessors: stop_idx -> (prev_stop_idx, EdgeType)
    let mut predecessors: Vec<Option<(u32, EdgeType)>> = vec![None; num_stops];

    for _round in 0..5 {
        // Max 5 transfers
        if marked_stops.is_empty() {
            break;
        }

        let mut next_marked = HashSet::new();

        // 1. Route Scanning
        let mut routes_to_scan: HashMap<usize, u32> = HashMap::new(); // pattern_idx -> min_stop_index_in_pattern

        for &stop in &marked_stops {
            if let Some(patterns) = stop_to_patterns.get(stop as usize) {
                for &p_idx in patterns {
                    let pattern = &partition.trip_patterns[p_idx];
                    let stop_indices = &partition.direction_patterns
                        [pattern.direction_pattern_idx as usize]
                        .stop_indices;
                    // Find index of stop in pattern
                    if let Some(idx_in_pattern) = stop_indices.iter().position(|&s| s == stop) {
                        let current_min =
                            routes_to_scan.entry(p_idx).or_insert(idx_in_pattern as u32);
                        if (idx_in_pattern as u32) < *current_min {
                            *current_min = idx_in_pattern as u32;
                        }
                    }
                }
            }
        }

        for (p_idx, start_idx) in routes_to_scan {
            let pattern = &partition.trip_patterns[p_idx];
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            let mut current_trip: Option<&CompressedTrip> = None;
            let mut boarded_at_stop = 0;
            let mut boarded_at_idx = 0; // Track index in pattern

            for i in (start_idx as usize)..stop_indices.len() {
                let stop_idx = stop_indices[i];

                // Check if we can board a trip
                if let Some(trip) = current_trip {
                    // We are on a trip, check arrival
                    let arrival = calculate_arrival_time(partition, trip, i);

                    if arrival < earliest_arrival[stop_idx as usize] {
                        earliest_arrival[stop_idx as usize] = arrival;

                        // Calculate min_duration
                        let mut min_duration = u32::MAX;
                        for trip in &pattern.trips {
                            let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
                            let mut duration = 0;
                            if 2 * i < delta_seq.deltas.len() {
                                for k in (boarded_at_idx + 1)..=i {
                                    duration += delta_seq.deltas[2 * k];
                                    if k > boarded_at_idx + 1 {
                                        duration += delta_seq.deltas[2 * (k - 1) + 1];
                                    }
                                }
                            }
                            if duration < min_duration {
                                min_duration = duration;
                            }
                        }
                        if min_duration == u32::MAX {
                            min_duration = 0;
                        }

                        let edge = TransitEdge {
                            trip_pattern_idx: p_idx as u32,
                            start_stop_idx: boarded_at_idx as u32,
                            end_stop_idx: i as u32,
                            min_duration,
                        };
                        predecessors[stop_idx as usize] =
                            Some((boarded_at_stop, EdgeType::Transit(edge)));
                        next_marked.insert(stop_idx);
                    }
                }

                // Can we board an earlier trip?
                if earliest_arrival[stop_idx as usize] != u32::MAX {
                    for trip in &pattern.trips {
                        let dep_time = calculate_arrival_time(partition, trip, i); // Arr = Dep
                        if dep_time >= earliest_arrival[stop_idx as usize] {
                            // Found a trip
                            if current_trip.is_none()
                                || dep_time
                                    < calculate_arrival_time(partition, current_trip.unwrap(), i)
                            {
                                current_trip = Some(trip);
                                boarded_at_stop = stop_idx;
                                boarded_at_idx = i;
                            }
                            break; // Trips are sorted
                        }
                    }
                }
            }
        }

        // 2. Transfers (Footpaths)
        let stops_to_transfer: Vec<u32> = next_marked.iter().cloned().collect();
        for stop in stops_to_transfer {
            // Check internal transfers
            for transfer in &partition.internal_transfers {
                if transfer.from_stop_idx == stop {
                    let arrival = earliest_arrival[stop as usize] + transfer.duration_seconds;
                    if arrival < earliest_arrival[transfer.to_stop_idx as usize] {
                        earliest_arrival[transfer.to_stop_idx as usize] = arrival;

                        let edge = WalkEdge {
                            duration_seconds: transfer.duration_seconds,
                        };

                        predecessors[transfer.to_stop_idx as usize] =
                            Some((stop, EdgeType::Walk(edge)));
                        next_marked.insert(transfer.to_stop_idx);
                    }
                }
            }
        }

        marked_stops = next_marked;
    }

    // Reconstruct DAG (Path) for targets
    let mut edges = Vec::new();
    let mut visited_nodes = HashSet::new();

    for &target in targets {
        if earliest_arrival[target as usize] != u32::MAX {
            let mut curr = target;
            while curr != start_node {
                if visited_nodes.contains(&curr) {
                    // Already traced this branch
                    break;
                }
                visited_nodes.insert(curr);

                if let Some((prev, ref edge_type)) = predecessors[curr as usize] {
                    let edge = DagEdge {
                        from_hub_idx: prev, // Using local stop idx
                        to_hub_idx: curr,   // Using local stop idx
                        edge_type: Some(edge_type.clone()),
                    };
                    edges.push(edge);
                    curr = prev;
                } else {
                    break; // Should not happen if reachable
                }
            }
        }
    }

    edges
}

fn calculate_arrival_time(
    partition: &TransitPartition,
    trip: &CompressedTrip,
    stop_idx_in_pattern: usize,
) -> u32 {
    let mut time = trip.start_time;
    let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
    for k in 0..=stop_idx_in_pattern {
        if k < delta_seq.deltas.len() {
            // Add travel time to k (2*k)
            if 2 * k < delta_seq.deltas.len() {
                time += delta_seq.deltas[2 * k];
            }
            // Add dwell time at k (2*k+1) IF we are not at the target stop yet?
            // Wait, arrival time at stop K includes travel TO K.
            // It does NOT include dwell AT K.
            // But the loop goes up to stop_idx_in_pattern.
            // If k == stop_idx_in_pattern, we added travel TO k. We stop.
            // If k < stop_idx_in_pattern, we add travel TO k AND dwell AT k.
            if k < stop_idx_in_pattern {
                if 2 * k + 1 < delta_seq.deltas.len() {
                    time += delta_seq.deltas[2 * k + 1];
                }
            }
        }
    }
    time
}
