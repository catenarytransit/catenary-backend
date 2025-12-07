use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;

pub fn reduce_borders_by_merging(
    mut clusters: Vec<Vec<usize>>,
    adjacency: &HashMap<(usize, usize), u32>,
    max_cluster_size: usize,
    total_node_count: usize,
) -> Vec<Vec<usize>> {
    println!("Reducing borders by merging clusters (weighted)...");
    //magic number stolen from scalable transfer patterns paper
    const HARD_CAP_CLUSTER_SIZE: usize = 6109;
    let num_stops = total_node_count;
    let mut stop_to_cluster = vec![0; num_stops];
    let mut cluster_active = vec![true; clusters.len()];

    // Track cluster total border weights (perimeter)
    let mut cluster_out_weights = vec![0; clusters.len()];

    // 1. Build Stop -> Cluster Map
    for (c_idx, stops) in clusters.iter().enumerate() {
        for &s_idx in stops {
            if s_idx < num_stops {
                stop_to_cluster[s_idx] = c_idx;
            }
        }
    }

    // 2. Identify Border Nodes and Connectivity
    // node_connectivity[u] = { cluster_id -> weight }
    let mut node_connectivity: Vec<HashMap<usize, u32>> = vec![HashMap::new(); num_stops];
    let mut cluster_borders: Vec<HashSet<usize>> = vec![HashSet::new(); clusters.len()];

    // Also track cluster-to-cluster connectivity for candidate selection
    // (c1, c2) -> weight
    let mut cluster_adj: HashMap<(usize, usize), u32> = HashMap::new();

    for (&(u, v), &w) in adjacency {
        if u >= num_stops || v >= num_stops {
            continue;
        }
        let c1 = stop_to_cluster[u];
        let c2 = stop_to_cluster[v];

        if c1 != c2 {
            // u connects to c2
            *node_connectivity[u].entry(c2).or_default() += w;
            cluster_borders[c1].insert(u);
            cluster_out_weights[c1] += w as usize;

            // v connects to c1
            *node_connectivity[v].entry(c1).or_default() += w;
            cluster_borders[c2].insert(v);
            cluster_out_weights[c2] += w as usize;

            // Cluster adjacency
            let (min, max) = if c1 < c2 { (c1, c2) } else { (c2, c1) };
            *cluster_adj.entry((min, max)).or_default() += w;
        }
    }

    println!(
        "  Initial borders: {}",
        cluster_borders.iter().map(|b| b.len()).sum::<usize>()
    );

    // 3. Merge Loop
    let mut merged_count = 0;
    loop {
        let mut best_merge = None;
        let mut best_score = 0.0;

        // Iterate all adjacent cluster pairs
        for (&(c1, c2), &adj_weight) in &cluster_adj {
            if !cluster_active[c1] || !cluster_active[c2] {
                continue;
            }

            let size1 = clusters[c1].len();
            let size2 = clusters[c2].len();
            let new_size = size1 + size2;

            if new_size > max_cluster_size || new_size > HARD_CAP_CLUSTER_SIZE {
                continue;
            }

            // Calculate Weighted Reduction
            // Sum of weights of edges that cease to be borders
            let mut reduction: u64 = 0;

            // Check c1 borders
            for &u in &cluster_borders[c1] {
                let conns = &node_connectivity[u];
                // If u connects ONLY to c2 (external), then merging removes it from border.
                // Sum the weights of connections to c2.
                let mut connects_to_others = false;
                for &target_c in conns.keys() {
                    if target_c != c2 {
                        connects_to_others = true;
                        break;
                    }
                }
                if !connects_to_others {
                    if let Some(&w) = conns.get(&c2) {
                        reduction += w as u64;
                    }
                }
            }

            // Check c2 borders
            for &v in &cluster_borders[c2] {
                let conns = &node_connectivity[v];
                let mut connects_to_others = false;
                for &target_c in conns.keys() {
                    if target_c != c1 {
                        connects_to_others = true;
                        break;
                    }
                }
                if !connects_to_others {
                    if let Some(&w) = conns.get(&c1) {
                        reduction += w as u64;
                    }
                }
            }

            // Total border weight of the pair (current perimeter)
            // Note: cluster_out_weights includes edges to each other.
            // When merged, the new perimeter is (weight1 + weight2) - 2 * adj_weight
            // But we use the *current* state for thresholds.
            let total_border_weight = (cluster_out_weights[c1] + cluster_out_weights[c2]) as u64;

            if total_border_weight == 0 {
                continue;
            }

            let ratio = reduction as f64 / total_border_weight as f64;

            // Adaptive Thresholds
            let n = new_size as f64;

            // Adaptive absolute threshold
            // Scale by total border weight and inverse log of size?
            // "derived from total_border_weight and cluster sizes"
            // Replaces > 100
            // Let's assume we need a minimum significance proportional to the border size
            // but relaxed for larger clusters?
            // Existing logic: > 100 was hard.
            // Let's use:
            let abs_threshold = (total_border_weight as f64) * 0.05 / (n.ln().max(1.0));

            let is_candidate = (reduction as f64) > abs_threshold;

            if is_candidate {
                // Score: Weighted reduction + fraction of adjacency weight?
                // "use cluster_adj weights in scoring"
                let score = reduction as f64 + (adj_weight as f64 * 0.1);
                if score > best_score {
                    best_score = score;
                    best_merge = Some((c1, c2));
                }
            }
        }

        if let Some((c1, c2)) = best_merge {
            // Execute Merge: Merge c2 into c1
            // println!("    Merging {} (size {}) and {} (size {}) -> Reduction: {}", c1, clusters[c1].len(), c2, clusters[c2].len(), best_score);

            // 1. Move stops
            let stops_c2 = clusters[c2].clone();
            for &s in &stops_c2 {
                stop_to_cluster[s] = c1;
            }
            clusters[c1].extend(stops_c2);
            clusters[c2].clear();
            cluster_active[c2] = false;

            // Update weights (merging c2 into c1)
            // New weight = w1 + w2 - 2 * weight(c1, c2)
            // We need weight(c1, c2)
            let mut adj_weight_c1_c2 = 0;
            // Find (c1, c2) weight from cluster_adj
            let key_self = if c1 < c2 { (c1, c2) } else { (c2, c1) };
            if let Some(&w) = cluster_adj.get(&key_self) {
                adj_weight_c1_c2 = w as usize;
            }

            cluster_out_weights[c1] =
                cluster_out_weights[c1] + cluster_out_weights[c2] - 2 * adj_weight_c1_c2;
            cluster_out_weights[c2] = 0;

            // 2. Update Connectivity Maps (The expensive part)
            // We need to update `node_connectivity` for ALL nodes that pointed to c2 -> point to c1
            // And `node_connectivity` for nodes IN c1/c2 that pointed to each other -> remove

            let mut neighbors_of_c2 = Vec::new();
            for (&(ca, cb), _) in &cluster_adj {
                if ca == c2 && cluster_active[cb] {
                    neighbors_of_c2.push(cb);
                } else if cb == c2 && cluster_active[ca] {
                    neighbors_of_c2.push(ca);
                }
            }

            for &neighbor_c in &neighbors_of_c2 {
                if neighbor_c == c1 {
                    continue;
                } // Handle c1-c2 separately

                for &u in &cluster_borders[neighbor_c] {
                    if let Some(w) = node_connectivity[u].remove(&c2) {
                        *node_connectivity[u].entry(c1).or_default() += w;
                    }
                }

                // Update cluster_adj
                let key_old = if neighbor_c < c2 {
                    (neighbor_c, c2)
                } else {
                    (c2, neighbor_c)
                };
                if let Some(w) = cluster_adj.remove(&key_old) {
                    let key_new = if neighbor_c < c1 {
                        (neighbor_c, c1)
                    } else {
                        (c1, neighbor_c)
                    };
                    *cluster_adj.entry(key_new).or_default() += w;
                }
            }

            // B. Update nodes IN c1 and c2 (The new merged cluster)
            // Their connections to c2 should be removed (internalized).
            // Their connections to c1 should be removed (internalized).
            // Their connections to others remain.

            let mut new_borders_c1 = HashSet::new();

            for &u in &cluster_borders[c1] {
                node_connectivity[u].remove(&c2);
                if !node_connectivity[u].is_empty() {
                    new_borders_c1.insert(u);
                }
            }

            for &v in &cluster_borders[c2] {
                node_connectivity[v].remove(&c1);
                if !node_connectivity[v].is_empty() {
                    new_borders_c1.insert(v);
                }
            }

            cluster_borders[c1] = new_borders_c1;
            cluster_borders[c2].clear();

            cluster_adj.remove(&key_self);

            merged_count += 1;
        } else {
            break;
        }
    }

    println!(
        "  Merged {} times. Final borders: {}",
        merged_count,
        cluster_borders.iter().map(|b| b.len()).sum::<usize>()
    );

    clusters.into_iter().filter(|c| !c.is_empty()).collect()
}

pub fn reduce_borders_by_merging_weighted(
    mut clusters: Vec<Vec<usize>>,
    adjacency: &HashMap<(usize, usize), u32>,
    max_cluster_size: usize,
    node_weights: &[usize],
) -> Vec<Vec<usize>> {
    println!("Reducing borders by merging clusters (weighted)...");
    //magic number stolen from scalable transfer patterns paper
    const HARD_CAP_CLUSTER_SIZE: usize = 6109;
    let num_nodes = node_weights.len();
    let mut node_to_cluster = vec![0; num_nodes];
    let mut cluster_active = vec![true; clusters.len()];

    // Track cluster weights
    let mut cluster_weights = vec![0; clusters.len()];

    // 1. Build Node -> Cluster Map and Weights
    for (c_idx, nodes) in clusters.iter().enumerate() {
        for &n_idx in nodes {
            if n_idx < num_nodes {
                node_to_cluster[n_idx] = c_idx;
                cluster_weights[c_idx] += node_weights[n_idx];
            }
        }
    }

    // 2. Identify Border Nodes and Connectivity
    // node_connectivity[u] = { cluster_id -> weight }
    let mut node_connectivity: Vec<HashMap<usize, u32>> = vec![HashMap::new(); num_nodes];
    let mut cluster_borders: Vec<HashSet<usize>> = vec![HashSet::new(); clusters.len()];

    // Also track cluster-to-cluster connectivity for candidate selection
    // (c1, c2) -> weight
    let mut cluster_adj: HashMap<(usize, usize), u32> = HashMap::new();

    for (&(u, v), &w) in adjacency {
        if u >= num_nodes || v >= num_nodes {
            continue;
        }
        let c1 = node_to_cluster[u];
        let c2 = node_to_cluster[v];

        if c1 != c2 {
            // u connects to c2
            *node_connectivity[u].entry(c2).or_default() += w;
            cluster_borders[c1].insert(u);

            // v connects to c1
            *node_connectivity[v].entry(c1).or_default() += w;
            cluster_borders[c2].insert(v);

            // Cluster adjacency
            let (min, max) = if c1 < c2 { (c1, c2) } else { (c2, c1) };
            *cluster_adj.entry((min, max)).or_default() += w;
        }
    }

    println!(
        "  Initial borders: {}",
        cluster_borders.iter().map(|b| b.len()).sum::<usize>()
    );

    // 3. Merge Loop
    let mut merged_count = 0;
    loop {
        let mut best_merge = None;
        let mut best_score = 0.0;

        // Iterate all adjacent cluster pairs
        for (&(c1, c2), &weight) in &cluster_adj {
            if !cluster_active[c1] || !cluster_active[c2] {
                continue;
            }

            let size1 = cluster_weights[c1];
            let size2 = cluster_weights[c2];
            let new_size = size1 + size2;

            if new_size > max_cluster_size || new_size > HARD_CAP_CLUSTER_SIZE {
                continue;
            }

            // Calculate Reduction
            let mut reduction = 0;

            // Check c1 borders
            for &u in &cluster_borders[c1] {
                let conns = &node_connectivity[u];
                let mut connects_to_others = false;
                for &target_c in conns.keys() {
                    if target_c != c2 {
                        connects_to_others = true;
                        break;
                    }
                }
                if !connects_to_others {
                    reduction += 1;
                }
            }

            // Check c2 borders
            for &v in &cluster_borders[c2] {
                let conns = &node_connectivity[v];
                let mut connects_to_others = false;
                for &target_c in conns.keys() {
                    if target_c != c1 {
                        connects_to_others = true;
                        break;
                    }
                }
                if !connects_to_others {
                    reduction += 1;
                }
            }

            // Score
            let total_borders = cluster_borders[c1].len() + cluster_borders[c2].len();
            if total_borders == 0 {
                continue;
            }

            let ratio = reduction as f64 / total_borders as f64;

            let n = new_size as f64;
            let r = (0.144371 * n.ln() - 0.861038).min(0.15);
            let is_candidate = reduction > 100 && ratio > r;

            if is_candidate {
                let score = reduction as f64;
                if score > best_score {
                    best_score = score;
                    best_merge = Some((c1, c2));
                }
            }
        }

        if let Some((c1, c2)) = best_merge {
            // Execute Merge: Merge c2 into c1

            // 1. Move stops
            let stops_c2 = clusters[c2].clone();
            for &s in &stops_c2 {
                node_to_cluster[s] = c1;
            }
            clusters[c1].extend(stops_c2);
            clusters[c2].clear();
            cluster_active[c2] = false;

            cluster_weights[c1] += cluster_weights[c2];
            cluster_weights[c2] = 0;

            // 2. Update Connectivity Maps
            let mut neighbors_of_c2 = Vec::new();
            for (&(ca, cb), _) in &cluster_adj {
                if ca == c2 && cluster_active[cb] {
                    neighbors_of_c2.push(cb);
                } else if cb == c2 && cluster_active[ca] {
                    neighbors_of_c2.push(ca);
                }
            }

            for &neighbor_c in &neighbors_of_c2 {
                if neighbor_c == c1 {
                    continue;
                }

                for &u in &cluster_borders[neighbor_c] {
                    if let Some(w) = node_connectivity[u].remove(&c2) {
                        *node_connectivity[u].entry(c1).or_default() += w;
                    }
                }

                let key_old = if neighbor_c < c2 {
                    (neighbor_c, c2)
                } else {
                    (c2, neighbor_c)
                };
                if let Some(w) = cluster_adj.remove(&key_old) {
                    let key_new = if neighbor_c < c1 {
                        (neighbor_c, c1)
                    } else {
                        (c1, neighbor_c)
                    };
                    *cluster_adj.entry(key_new).or_default() += w;
                }
            }

            let mut new_borders_c1 = HashSet::new();

            for &u in &cluster_borders[c1] {
                node_connectivity[u].remove(&c2);
                if !node_connectivity[u].is_empty() {
                    new_borders_c1.insert(u);
                }
            }

            for &v in &cluster_borders[c2] {
                node_connectivity[v].remove(&c1);
                if !node_connectivity[v].is_empty() {
                    new_borders_c1.insert(v);
                }
            }

            cluster_borders[c1] = new_borders_c1;
            cluster_borders[c2].clear();

            let key_self = if c1 < c2 { (c1, c2) } else { (c2, c1) };
            cluster_adj.remove(&key_self);

            merged_count += 1;
        } else {
            break;
        }
    }

    println!(
        "  Merged {} times. Final borders: {}",
        merged_count,
        cluster_borders.iter().map(|b| b.len()).sum::<usize>()
    );

    clusters.into_iter().filter(|c| !c.is_empty()).collect()
}
