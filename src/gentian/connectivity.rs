use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use catenary::routing_common::transit_graph::{
    DagEdge, DagEdgeList, EdgeType, GlobalHub, LocalTransferPattern, PartitionDag, TransitEdge,
    TransitPartition, TransitStop, WalkEdge,
};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::PathBuf;

pub fn compute_border_patterns(
    border_nodes: &HashMap<u32, Vec<TransitStop>>,
    loaded_partitions: &HashMap<u32, TransitPartition>,
    _global_to_partition_map: &HashMap<usize, (u32, u32)>,
) -> Vec<PartitionDag> {
    println!("  - Computing Border Patterns (Step 4)...");

    let mut result_dags = Vec::new();

    // 1. Build Lookups for Patterns
    println!("    - Building pattern lookups...");

    let mut long_dist_pattern_lookup: HashMap<(u32, u32), &LocalTransferPattern> = HashMap::new();

    for (pid, partition) in loaded_partitions {
        // local_dag is now a map, so we don't need to build a lookup for it.
        // We will access it directly via partition.local_dag.
        for pat in &partition.long_distance_transfer_patterns {
            long_dist_pattern_lookup.insert((*pid, pat.from_stop_idx), pat);
        }
    }

    // 2. Iterate over each partition as a Source Cluster
    let partition_ids: Vec<u32> = border_nodes.keys().cloned().collect();

    // Map: (StartPID, EndPID) -> DagBuilder
    struct DagBuilder {
        // (From(PID, Idx), To(PID, Idx), Type)
        edges: HashSet<((u32, u32), (u32, u32), Option<EdgeType>)>,
        // (PID, Idx)
        nodes: HashSet<(u32, u32)>,
    }
    let mut dag_builders: HashMap<(u32, u32), DagBuilder> = HashMap::new();

    for &p_start in &partition_ids {
        let start_borders = &border_nodes[&p_start];
        if start_borders.is_empty() {
            continue;
        }

        println!(
            "    - Processing Partition {} ({} border nodes)...",
            p_start,
            start_borders.len()
        );

        // Run Dijkstra from EACH border node
        for start_node in start_borders {
            let start_key = (p_start, start_node.id as u32);

            let mut dist: HashMap<(u32, u32), u32> = HashMap::new();
            // Predecessors: Target -> List of (Source, EdgeType)
            let mut predecessors: HashMap<(u32, u32), Vec<((u32, u32), Option<EdgeType>)>> =
                HashMap::new();
            let mut pq = BinaryHeap::new();

            #[derive(Copy, Clone, Eq, PartialEq)]
            struct State {
                cost: u32,
                pid: u32,
                stop_idx: u32,
            }
            impl Ord for State {
                fn cmp(&self, other: &Self) -> Ordering {
                    other.cost.cmp(&self.cost)
                }
            }
            impl PartialOrd for State {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }

            dist.insert(start_key, 0);
            pq.push(State {
                cost: 0,
                pid: start_key.0,
                stop_idx: start_key.1,
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

                // A. Local Relaxation (within 'pid')
                if let Some(partition) = loaded_partitions.get(&pid) {
                    if let Some(edge_list) = partition.local_dag.get(&stop_idx) {
                        for edge in &edge_list.edges {
                            let target_idx = edge.to_node_idx;
                            let weight = match &edge.edge_type {
                                Some(EdgeType::Transit(t)) => t.min_duration,
                                Some(EdgeType::Walk(w)) => w.duration_seconds,
                                Some(EdgeType::LongDistanceTransit(t)) => t.min_duration,
                                None => 0,
                            };

                            let next_cost = cost + weight;
                            let target_key = (pid, target_idx);

                            if next_cost < *dist.get(&target_key).unwrap_or(&u32::MAX) {
                                dist.insert(target_key, next_cost);
                                predecessors.insert(
                                    target_key,
                                    vec![((pid, stop_idx), edge.edge_type.clone())],
                                );
                                pq.push(State {
                                    cost: next_cost,
                                    pid,
                                    stop_idx: target_idx,
                                });
                            } else if next_cost == *dist.get(&target_key).unwrap_or(&u32::MAX) {
                                predecessors
                                    .entry(target_key)
                                    .or_default()
                                    .push(((pid, stop_idx), edge.edge_type.clone()));
                            }
                        }
                    }
                }

                // B. Long-Distance Relaxation (Jump to other partitions)
                if let Some(pat) = long_dist_pattern_lookup.get(&(pid, stop_idx)) {
                    let partition = &loaded_partitions[&pid];

                    for edge in &pat.edges {
                        let ext_idx = edge.to_node_idx as usize;
                        if ext_idx >= partition.stops.len() {
                            let hub_idx = ext_idx - partition.stops.len();
                            if hub_idx < partition.external_hubs.len() {
                                let global_hub = &partition.external_hubs[hub_idx];
                                let target_pid = global_hub.original_partition_id;
                                let target_idx = global_hub.stop_idx_in_partition;
                                let target_key = (target_pid, target_idx);

                                let weight = match &edge.edge_type {
                                    Some(EdgeType::Transit(t)) => t.min_duration,
                                    Some(EdgeType::Walk(w)) => w.duration_seconds,
                                    Some(EdgeType::LongDistanceTransit(t)) => t.min_duration,
                                    None => 0,
                                };

                                let next_cost = cost + weight;

                                if next_cost < *dist.get(&target_key).unwrap_or(&u32::MAX) {
                                    dist.insert(target_key, next_cost);
                                    predecessors.insert(
                                        target_key,
                                        vec![((pid, stop_idx), edge.edge_type.clone())],
                                    );
                                    pq.push(State {
                                        cost: next_cost,
                                        pid: target_pid,
                                        stop_idx: target_idx,
                                    });
                                } else if next_cost == *dist.get(&target_key).unwrap_or(&u32::MAX) {
                                    predecessors
                                        .entry(target_key)
                                        .or_default()
                                        .push(((pid, stop_idx), edge.edge_type.clone()));
                                }
                            }
                        }
                    }
                }
            }

            // 3. Extract Paths to Border Nodes of Other Partitions
            for &p_end in &partition_ids {
                if p_end == p_start {
                    continue;
                }

                let end_borders = &border_nodes[&p_end];
                for end_node in end_borders {
                    let end_key = (p_end, end_node.id as u32);

                    if let Some(&d) = dist.get(&end_key) {
                        if d == u32::MAX {
                            continue;
                        }

                        // Backtrack
                        let mut q = std::collections::VecDeque::new();
                        q.push_back(end_key);
                        let mut visited_back = HashSet::new();
                        visited_back.insert(end_key);

                        let builder =
                            dag_builders
                                .entry((p_start, p_end))
                                .or_insert_with(|| DagBuilder {
                                    edges: HashSet::new(),
                                    nodes: HashSet::new(),
                                });

                        builder.nodes.insert(end_key);

                        while let Some(curr) = q.pop_front() {
                            if curr == start_key {
                                builder.nodes.insert(curr);
                                continue;
                            }

                            if let Some(preds) = predecessors.get(&curr) {
                                for (pred_key, edge_type) in preds {
                                    // Add edge and nodes
                                    builder.nodes.insert(*pred_key);
                                    builder.edges.insert((*pred_key, curr, edge_type.clone()));

                                    if visited_back.insert(*pred_key) {
                                        q.push_back(*pred_key);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 4. Construct PartitionDags
    for ((p_start, p_end), builder) in dag_builders {
        // Convert nodes to GlobalHubs and assign indices
        let mut hubs = Vec::new();
        let mut node_to_idx: HashMap<(u32, u32), u32> = HashMap::new();

        for (pid, idx) in builder.nodes {
            let hub_idx = hubs.len() as u32;
            hubs.push(GlobalHub {
                original_partition_id: pid,
                stop_idx_in_partition: idx,
            });
            node_to_idx.insert((pid, idx), hub_idx);
        }

        // Convert edges
        let mut edges = Vec::new();
        for (from_key, to_key, edge_type) in builder.edges {
            if let (Some(&from_idx), Some(&to_idx)) =
                (node_to_idx.get(&from_key), node_to_idx.get(&to_key))
            {
                edges.push(DagEdge {
                    from_node_idx: from_idx,
                    to_node_idx: to_idx,
                    edge_type,
                });
            }
        }

        result_dags.push(PartitionDag {
            from_partition: p_start,
            to_partition: p_end,
            hubs,
            edges,
        });
    }

    println!("  - Generated {} Partition DAGs", result_dags.len());
    result_dags
}

pub fn compute_global_patterns(
    border_nodes: &HashMap<u32, Vec<TransitStop>>,
    cross_edges: &[((usize, usize), DagEdge)],
    global_to_partition_map: &HashMap<usize, (u32, u32)>,
    loaded_partitions: &mut HashMap<u32, TransitPartition>,
    _output_dir: &PathBuf,
) {
    println!("  - Building Global Hub Graph (Profile Search)...");

    // 1. Organize Cross-Partition Edges
    let mut cross_adj: HashMap<usize, Vec<(usize, DagEdge)>> = HashMap::new();
    for &((u, v), ref edge) in cross_edges {
        // Fix 5: Filter out intra-partition edges from cross_edges
        let u_pid = global_to_partition_map.get(&u).map(|x| x.0);
        let v_pid = global_to_partition_map.get(&v).map(|x| x.0);

        let is_cross_partition = match (u_pid, v_pid) {
            (Some(p1), Some(p2)) => p1 != p2,
            _ => true, // Assume cross if we can't tell, though this shouldn't happen for valid nodes
        };

        if is_cross_partition {
            cross_adj.entry(u).or_default().push((v, edge.clone()));
        }
    }

    // 2. Identify Long-Distance Stations (Global Indices)
    // The input `global_to_partition_map` now ONLY contains relevant nodes (long-distance + active border).
    // So we can just use all keys from it.
    let mut long_distance_stations: Vec<usize> = global_to_partition_map.keys().cloned().collect();
    long_distance_stations.sort();

    println!(
        "  - Computing Global Patterns from {} long-distance stations...",
        long_distance_stations.len()
    );

    // Fix 4: Include partitions that have long-distance stations but maybe no borders
    let mut partition_ids_set: HashSet<u32> = border_nodes.keys().cloned().collect();
    for &(_, pid) in global_to_partition_map.values() {
        partition_ids_set.insert(pid);
    }
    let partition_ids: Vec<u32> = partition_ids_set.into_iter().collect();

    // Group long-distance stations by partition
    let mut stations_by_partition: HashMap<u32, Vec<usize>> = HashMap::new();
    for &s in &long_distance_stations {
        if let Some(&(pid, _)) = global_to_partition_map.get(&s) {
            stations_by_partition.entry(pid).or_default().push(s);
        }
    }

    // Precompute map from (pid, local_idx) -> global_idx for fast lookup
    let mut node_to_global: HashMap<(u32, u32), usize> = HashMap::new();
    for (&g, &p) in global_to_partition_map {
        node_to_global.insert(p, g);
    }

    // Store all computed edges grouped by Source Partition
    // Map: Source Partition -> From Global Node -> List of (To Global Node, Edge)
    let mut partition_global_edges: HashMap<u32, HashMap<usize, Vec<(usize, DagEdge)>>> =
        HashMap::new();

    for &p_start in &partition_ids {
        let start_hubs = stations_by_partition
            .get(&p_start)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        if start_hubs.is_empty() {
            continue;
        }

        // Map: p_end -> (u, v) -> cost
        let mut useful_edges_by_pair: HashMap<u32, HashMap<(usize, usize), u32>> = HashMap::new();
        let mut useful_nodes_by_pair: HashMap<u32, HashSet<usize>> = HashMap::new();

        for &start_node in start_hubs {
            let mut dist: HashMap<usize, u32> = HashMap::new();
            let mut predecessors: HashMap<usize, Vec<usize>> = HashMap::new();
            let mut pq = BinaryHeap::new();

            #[derive(Copy, Clone, Eq, PartialEq)]
            struct State {
                cost: u32,
                node: usize,
            }
            impl Ord for State {
                fn cmp(&self, other: &Self) -> Ordering {
                    other.cost.cmp(&self.cost)
                }
            }
            impl PartialOrd for State {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }

            dist.insert(start_node, 0);
            pq.push(State {
                cost: 0,
                node: start_node,
            });

            while let Some(State { cost, node: u }) = pq.pop() {
                if cost > *dist.get(&u).unwrap_or(&u32::MAX) {
                    continue;
                }

                // 1. Cross-Partition Edges
                if let Some(edges) = cross_adj.get(&u) {
                    for (v, edge) in edges {
                        let weight = match &edge.edge_type {
                            Some(EdgeType::Transit(t)) => t.min_duration,
                            Some(EdgeType::Walk(w)) => w.duration_seconds,
                            Some(EdgeType::LongDistanceTransit(t)) => t.min_duration,
                            None => 0,
                        };
                        let next_cost = cost + weight;
                        if next_cost < *dist.get(v).unwrap_or(&u32::MAX) {
                            dist.insert(*v, next_cost);
                            predecessors.insert(*v, vec![u]);
                            pq.push(State {
                                cost: next_cost,
                                node: *v,
                            });
                        } else if next_cost == *dist.get(v).unwrap_or(&u32::MAX) {
                            predecessors.entry(*v).or_default().push(u);
                        }
                    }
                }

                // 2. Intra-Partition (LocalTransferPattern)
                if let Some(&(pid, l_idx)) = global_to_partition_map.get(&u) {
                    if let Some(partition) = loaded_partitions.get(&pid) {
                        // Use local_dag
                        // We need to run a local Dijkstra from l_idx using the Union DAG
                        let mut local_dist: HashMap<u32, u32> = HashMap::new();
                        let mut local_pq = BinaryHeap::new();

                        local_dist.insert(l_idx, 0);
                        local_pq.push(State {
                            cost: 0,
                            node: l_idx as usize,
                        });

                        while let Some(State {
                            cost: local_cost,
                            node: local_u,
                        }) = local_pq.pop()
                        {
                            let local_u = local_u as u32;
                            if local_cost > *local_dist.get(&local_u).unwrap_or(&u32::MAX) {
                                continue;
                            }

                            if local_u != l_idx {
                                if let Some(&global_v) = node_to_global.get(&(pid, local_u)) {
                                    let total_cost = cost + local_cost;
                                    if total_cost < *dist.get(&global_v).unwrap_or(&u32::MAX) {
                                        dist.insert(global_v, total_cost);
                                        predecessors.insert(global_v, vec![u]);
                                        pq.push(State {
                                            cost: total_cost,
                                            node: global_v,
                                        });
                                    } else if total_cost
                                        == *dist.get(&global_v).unwrap_or(&u32::MAX)
                                    {
                                        predecessors.entry(global_v).or_default().push(u);
                                    }
                                }
                            }

                            if let Some(edge_list) = partition.local_dag.get(&local_u) {
                                for edge in &edge_list.edges {
                                    if edge.from_node_idx == local_u {
                                        let weight = match &edge.edge_type {
                                            Some(EdgeType::Transit(t)) => t.min_duration,
                                            Some(EdgeType::Walk(w)) => w.duration_seconds,
                                            _ => 0,
                                        };
                                        let next_local_cost = local_cost + weight;
                                        if next_local_cost
                                            < *local_dist
                                                .get(&edge.to_node_idx)
                                                .unwrap_or(&u32::MAX)
                                        {
                                            local_dist.insert(edge.to_node_idx, next_local_cost);
                                            local_pq.push(State {
                                                cost: next_local_cost,
                                                node: edge.to_node_idx as usize,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Collect results for each target partition
            // Optimization: Only iterate over partitions that were actually reached
            let mut reached_partitions = HashSet::new();
            for &reached_node in dist.keys() {
                if let Some(&(pid, _)) = global_to_partition_map.get(&reached_node) {
                    if pid != p_start {
                        reached_partitions.insert(pid);
                    }
                }
            }

            for &p_end in &reached_partitions {
                let end_hubs = stations_by_partition
                    .get(&p_end)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                for &end_node in end_hubs {
                    if let Some(&d) = dist.get(&end_node) {
                        if d == u32::MAX {
                            continue;
                        }

                        let mut q = std::collections::VecDeque::new();
                        q.push_back(end_node);
                        useful_nodes_by_pair
                            .entry(p_end)
                            .or_default()
                            .insert(end_node);

                        let mut visited_back = HashSet::new();
                        visited_back.insert(end_node);

                        while let Some(curr) = q.pop_front() {
                            if curr == start_node {
                                continue;
                            }
                            if let Some(preds) = predecessors.get(&curr) {
                                for &pred in preds {
                                    let pred_dist = *dist.get(&pred).unwrap_or(&0);
                                    let curr_dist = *dist.get(&curr).unwrap_or(&0);
                                    let edge_cost = if curr_dist >= pred_dist {
                                        curr_dist - pred_dist
                                    } else {
                                        0
                                    };

                                    let entry = useful_edges_by_pair
                                        .entry(p_end)
                                        .or_default()
                                        .entry((pred, curr))
                                        .or_insert(u32::MAX);

                                    if edge_cost < *entry {
                                        *entry = edge_cost;
                                    }

                                    useful_nodes_by_pair.entry(p_end).or_default().insert(pred);
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

        // Process edges for p_start -> *
        for (&p_end, edges_map) in useful_edges_by_pair.iter_mut() {
            let nodes = useful_nodes_by_pair.get(&p_end).unwrap();
            let nodes_vec: Vec<usize> = nodes.iter().cloned().collect();

            // Pruning Logic
            // Optimization: Build adjacency only once
            let mut incoming: HashMap<usize, Vec<usize>> = HashMap::new();
            let mut outgoing: HashMap<usize, Vec<usize>> = HashMap::new();

            for (&(u, v), _) in edges_map.iter() {
                outgoing.entry(u).or_default().push(v);
                incoming.entry(v).or_default().push(u);
            }

            // Pruning Logic
            for &w in &nodes_vec {
                if let (Some(preds), Some(succs)) = (incoming.get(&w), outgoing.get(&w)) {
                    for &u in preds {
                        if u == w {
                            continue;
                        }
                        for &v in succs {
                            if v == w || v == u {
                                continue;
                            }
                            if let Some(&direct_cost) = edges_map.get(&(u, v)) {
                                let cost_u_w = edges_map.get(&(u, w)).cloned().unwrap_or(u32::MAX);
                                let cost_w_v = edges_map.get(&(w, v)).cloned().unwrap_or(u32::MAX);
                                if cost_u_w != u32::MAX && cost_w_v != u32::MAX {
                                    let indirect_cost = cost_u_w + cost_w_v;
                                    if direct_cost > indirect_cost {
                                        edges_map.remove(&(u, v));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Recover Edge Types and Store
            for ((u, v), cost) in edges_map {
                let u = *u;
                let v = *v;

                // Fix 1 & 3: Use the computed cost as the truth
                let edge_type = Some(EdgeType::LongDistanceTransit(TransitEdge {
                    trip_pattern_idx: u32::MAX, // sentinel
                    start_stop_idx: 0,
                    end_stop_idx: 0,
                    min_duration: *cost,
                }));

                let dag_edge = DagEdge {
                    from_node_idx: 0, // Placeholder, will be re-indexed
                    to_node_idx: 0,   // Placeholder
                    edge_type,
                };

                partition_global_edges
                    .entry(p_start)
                    .or_default()
                    .entry(u)
                    .or_default()
                    .push((v, dag_edge));
            }
        }
    }

    // 3. Update Partitions with Long-Distance Patterns
    for (&pid, partition) in loaded_partitions.iter_mut() {
        if let Some(global_edges_map) = partition_global_edges.get(&pid) {
            let mut external_hubs: Vec<GlobalHub> = Vec::new();
            let mut external_hub_map: HashMap<usize, u32> = HashMap::new(); // GlobalIdx -> ExternalIdx

            // Helper to get index (Local < N, External >= N)
            let mut get_node_idx = |global_idx: usize| -> u32 {
                if let Some(&(p, l)) = global_to_partition_map.get(&global_idx) {
                    if p == pid {
                        return l;
                    }
                }
                // External
                if let Some(&idx) = external_hub_map.get(&global_idx) {
                    return partition.stops.len() as u32 + idx;
                }
                let idx = external_hubs.len() as u32;
                if let Some(&(p, l)) = global_to_partition_map.get(&global_idx) {
                    external_hubs.push(GlobalHub {
                        original_partition_id: p,
                        stop_idx_in_partition: l,
                    });
                } else {
                    // Fix 6: Panic if mapping is incomplete
                    panic!(
                        "global_to_partition_map missing entry for global_idx {}",
                        global_idx
                    );
                }
                external_hub_map.insert(global_idx, idx);
                partition.stops.len() as u32 + idx
            };

            let mut new_patterns = Vec::new();

            for (&u_global, edges) in global_edges_map {
                let from_idx = get_node_idx(u_global);
                let mut dag_edges = Vec::new();

                for (v_global, edge_template) in edges {
                    let to_idx = get_node_idx(*v_global);
                    let mut new_edge = edge_template.clone();
                    new_edge.from_node_idx = from_idx;
                    new_edge.to_node_idx = to_idx;
                    dag_edges.push(new_edge);
                }

                new_patterns.push(LocalTransferPattern {
                    from_stop_idx: from_idx,
                    edges: dag_edges,
                });
            }

            partition.external_hubs = external_hubs;
            partition.long_distance_transfer_patterns = new_patterns;

            println!(
                "  - Partition {}: Added {} long-distance patterns referencing {} external hubs",
                pid,
                partition.long_distance_transfer_patterns.len(),
                partition.external_hubs.len()
            );
        }
    }
}

pub fn compute_intra_partition_connectivity(
    partition: &TransitPartition,
    global_indices: &[u32],
) -> Vec<((usize, usize), DagEdge)> {
    let mut edges = Vec::new();

    // Map local_idx -> global_idx
    assert_eq!(
        global_indices.len(),
        partition.stops.len(),
        "global_indices must be a per-stop mapping"
    ); // Fix 7
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
                    from_node_idx: stop1, // Local
                    to_node_idx: stop2,   // Local
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
                    from_node_idx: transfer.from_stop_idx,
                    to_node_idx: transfer.to_stop_idx,
                    edge_type: Some(EdgeType::Walk(edge)),
                },
            ));
        }
    }

    edges
}

pub fn compute_local_patterns_for_partition(partition: &mut TransitPartition) {
    println!(
        "    - Computing LocalTransferPatterns for partition {} ({} stops)...",
        partition.partition_id,
        partition.stops.len()
    );

    if partition.stops.is_empty() {
        return;
    }

    // 1. Compute Transfers (Trip-Based Preprocessing)
    println!("Computing initial transfers");
    let mut transfers = crate::trip_based::compute_initial_transfers(partition);
    println!("removing u turn transfers");
    crate::trip_based::remove_u_turn_transfers(partition, &mut transfers);
    println!("refining transfers");
    crate::trip_based::refine_transfers(partition, &mut transfers);

    println!(
        "      - Computed {} transfers for Trip-Based Routing",
        transfers.len()
    );

    println!("      - Precomputing auxiliary structures for Trip-Based Routing");

    // Precompute auxiliary structures for Trip-Based Routing
    let num_trips = partition
        .trip_patterns
        .iter()
        .map(|p| p.trips.len())
        .sum::<usize>();
    let mut pattern_trip_offset = Vec::with_capacity(partition.trip_patterns.len());
    let mut flat_id_to_pattern_trip = Vec::with_capacity(num_trips);
    let mut total_trips = 0;
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        pattern_trip_offset.push(total_trips);
        for t_idx in 0..pattern.trips.len() {
            flat_id_to_pattern_trip.push((p_idx, t_idx));
        }
        total_trips += pattern.trips.len();
    }

    let mut stop_to_patterns: Vec<Vec<(usize, usize)>> = vec![Vec::new(); partition.stops.len()];
    for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
        let stop_indices =
            &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
        for (i, &s_idx) in stop_indices.iter().enumerate() {
            stop_to_patterns[s_idx as usize].push((p_idx, i));
        }
    }

    // Precompute trip_transfer_ranges
    println!("      - Precomputing trip_transfer_ranges");
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

    // Sharded storage for edges to avoid one massive HashSet
    // Vec index = from_node_idx
    let mut local_dag_shards: Vec<HashSet<DagEdge>> = vec![HashSet::new(); partition.stops.len()];

    let mut scratch = crate::trip_based::ProfileScratch::new(partition.stops.len(), num_trips, 3);

    // Collect hubs
    let mut hubs = HashSet::new();
    for (i, stop) in partition.stops.iter().enumerate() {
        if stop.is_hub {
            hubs.insert(i as u32);
        }
    }

    // For each stop in the partition, run profile search to all other stops
    let all_stops: Vec<u32> = (0..partition.stops.len() as u32).collect();
    println!(
        "      - Running profile search from {} stops (hubs/borders)",
        hubs.len()
    );

    let times = vec![
        0, 3600, 7200, 10800, 14400, 18000, 21600, 25200, 28800, 32400, 36000, 39600, 43200, 46800,
        50400, 54000, 57600, 61200, 64800, 68400, 72000, 75600, 79200, 82800, 86400,
    ];

    // Fix 9: Only run from hubs/borders
    // The `hubs` set populated above uses `stop.is_hub`. We should also ensure it includes borders if not already.
    // In many setups hubs includes borders, but let's be safe or just iterate over `hubs`.
    // The previous code used `all_stops`. We switch to iterating over `hubs`.
    // Note: The `hubs` HashSet contains `u32` indices of stops that are hubs.

    // Convert hubs to sorted vec for deterministic iteration
    let mut sorted_hubs: Vec<u32> = hubs.iter().cloned().collect();
    sorted_hubs.sort();

    for &start_node in &sorted_hubs {
        // Full reset for this source
        scratch.reset();

        let is_source_hub = hubs.contains(&start_node);

        // One profile over [0, end_of_day], like the paper’s 24h profiles
        for start_time in times.iter() {
            crate::trip_based::compute_profile_query(
                partition,
                &transfers,
                &trip_transfer_ranges,
                start_node,
                *start_time as u32,
                &all_stops,
                &stop_to_patterns,
                &flat_id_to_pattern_trip,
                &pattern_trip_offset,
                3,
                &mut scratch,
                &hubs,
                is_source_hub,
            );
        }

        // Collect unique edges from this query into shards
        // scratch.dedup_map contains the edges
        for edge in scratch.dedup_map.values() {
            if (edge.from_node_idx as usize) < local_dag_shards.len() {
                local_dag_shards[edge.from_node_idx as usize].insert(edge.clone());
            }
        }
    }

    // Convert Shards to HashMap<u32, DagEdgeList>
    let mut local_dag: std::collections::HashMap<u32, DagEdgeList> =
        std::collections::HashMap::with_capacity(local_dag_shards.len());

    for (from_idx, unique_edges) in local_dag_shards.into_iter().enumerate() {
        if !unique_edges.is_empty() {
            local_dag.insert(
                from_idx as u32,
                DagEdgeList {
                    edges: unique_edges.into_iter().collect(),
                },
            );
        }
    }

    partition.local_dag = local_dag;
}
