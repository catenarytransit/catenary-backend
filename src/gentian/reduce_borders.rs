use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;

pub fn reduce_borders_by_merging(
    mut clusters: Vec<Vec<usize>>,
    adjacency: &HashMap<(usize, usize), u32>,
    max_cluster_size: usize,
    total_node_count: usize,
) -> Vec<Vec<usize>> {
    println!("Reducing borders by merging clusters...");
    //magic number stolen from scalable transfer patterns paper
    const HARD_CAP_CLUSTER_SIZE: usize = 6109;
    let num_stops = total_node_count;
    let mut stop_to_cluster = vec![0; num_stops];
    let mut cluster_active = vec![true; clusters.len()];

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

            let size1 = clusters[c1].len();
            let size2 = clusters[c2].len();
            let new_size = size1 + size2;

            if new_size > max_cluster_size || new_size > HARD_CAP_CLUSTER_SIZE {
                continue;
            }

            // Calculate Reduction
            // Reduction = borders(A) + borders(B) - borders(A U B)
            // borders(A U B) = (borders(A) \ internal) U (borders(B) \ internal)
            // "internal" are nodes that ONLY connect to the other cluster (and maybe itself)

            // Heuristic:
            // We can count how many border nodes in A connect ONLY to B (and internal).
            // Actually, we can just check the `node_connectivity`.
            // For a node u in borders[c1]:
            //   If node_connectivity[u] contains ONLY c2 (besides internal connections which are not in map),
            //   then merging c1 and c2 removes u from borders.
            //   If u connects to c2 AND c3, it stays a border (to c3).

            let mut reduction = 0;

            // Check c1 borders
            for &u in &cluster_borders[c1] {
                let conns = &node_connectivity[u];
                // If it connects to c2 and NO other external cluster, it will cease to be a border.
                // The map only contains external clusters.
                // So if map keys are {c2}, or subset of {c2}, it's removed.
                // (It must connect to c2 to be in this check? No, u is in borders[c1], so it connects to SOME external.)
                // Wait, u might connect to c3 but NOT c2. Then it stays border.
                // We only care if it connects to c2.

                // Actually, simpler:
                // New border set = (borders[c1] U borders[c2])
                // Filter: keep u if u connects to any cluster OTHER than c1 or c2.

                // To avoid iterating sets every time, let's estimate or compute exactly for the candidate.
                // Since we need to be greedy, we might need to be fast.
                // But let's do exact count for now.

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
            // We want high reduction.
            // We want to avoid huge clusters if reduction is small.
            // Interconnectedness is roughly `weight` (trip count) or `reduction` (node count).
            // Let's use `reduction`.

            // Thresholds:

            let total_borders = cluster_borders[c1].len() + cluster_borders[c2].len();
            if total_borders == 0 {
                continue;
            }

            let ratio = reduction as f64 / total_borders as f64;

            // Criteria:
            // 1. Reduction > 50 AND Ratio > r

            // crazy notes by kyler
            // i made this so that it doesn't encourage merging of extremely large clusters
            // the ratio rises from a 15% border reduction at 1100 stops to a 30% border reduction at 3109 stops

            let n = new_size as f64;
            let r = (0.144371 * n.ln() - 0.861038).min(0.15);
            //the minimum reduction of 70 is to say that we want to reduce anything with lots of border nodes
            //this number is also taken from section 4.3 of scalable transfer patterns
            let is_candidate = reduction > 100 && ratio > r;

            if is_candidate {
                // Score: prioritise high reduction, penalise size slightly?
                // Score = reduction
                let score = reduction as f64;
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

            // 2. Update Connectivity Maps (The expensive part)
            // We need to update `node_connectivity` for ALL nodes that pointed to c2 -> point to c1
            // And `node_connectivity` for nodes IN c1/c2 that pointed to each other -> remove

            // Optimization: We only need to update nodes that are borders of c1, c2, OR neighbors of c2.
            // Finding neighbors of c2: iterate `cluster_adj`.

            // A. Update neighbors of c2 (nodes outside c1/c2 that pointed to c2)
            // We can't easily find *which nodes* point to c2 without reverse index or iterating all borders.
            // But we have `cluster_adj` which tells us WHICH clusters connect to c2.
            // Let's iterate those clusters.

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

                // For each border node in neighbor_c, if it points to c2, change to c1.
                // We have to iterate neighbor's borders.

                for &u in &cluster_borders[neighbor_c] {
                    if let Some(w) = node_connectivity[u].remove(&c2) {
                        *node_connectivity[u].entry(c1).or_default() += w;
                    }
                    // It still points to c1 (now), so it remains a border.
                }

                // Update cluster_adj
                // Remove (neighbor, c2), Add to (neighbor, c1)
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

            // We need to rebuild `cluster_borders[c1]`.
            let mut new_borders_c1 = HashSet::new();

            // Process old c1 borders
            for &u in &cluster_borders[c1] {
                // Remove connection to c2 if exists (it's now internal)
                node_connectivity[u].remove(&c2);
                // If it still has connections, keep it
                if !node_connectivity[u].is_empty() {
                    new_borders_c1.insert(u);
                }
            }

            // Process old c2 borders
            for &v in &cluster_borders[c2] {
                // Remove connection to c1 if exists
                node_connectivity[v].remove(&c1);
                // If it still has connections, keep it
                if !node_connectivity[v].is_empty() {
                    new_borders_c1.insert(v);
                }
            }

            cluster_borders[c1] = new_borders_c1;
            cluster_borders[c2].clear();

            // Remove (c1, c2) from cluster_adj
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
