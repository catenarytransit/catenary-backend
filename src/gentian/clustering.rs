use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

// Merge-Based Clustering
pub fn merge_based_clustering(
    num_stops: usize,
    adjacency: &HashMap<(usize, usize), u32>,
    max_size: usize,
) -> Vec<Vec<usize>> {
    // Initial clusters: each stop is a cluster
    let mut clusters: Vec<Vec<usize>> = (0..num_stops).map(|i| vec![i]).collect();
    let mut cluster_map: Vec<usize> = (0..num_stops).collect(); // Stop -> Cluster Index
    let mut active_clusters: HashSet<usize> = (0..num_stops).collect();

    // Edge Priority Queue (Weight, Cluster A, Cluster B)
    #[derive(Eq, PartialEq)]
    struct Edge {
        weight: u32,
        c1: usize,
        c2: usize,
    }
    impl Ord for Edge {
        fn cmp(&self, other: &Self) -> Ordering {
            self.weight.cmp(&other.weight)
        }
    }
    impl PartialOrd for Edge {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut pq = BinaryHeap::new();

    // Initialize PQ with stop-stop edges
    for (&(u, v), &w) in adjacency {
        pq.push(Edge {
            weight: w,
            c1: u,
            c2: v,
        });
    }

    // PASS 1: Standard Greedy Merge (Respecting max_size)
    while let Some(edge) = pq.pop() {
        let c1 = cluster_map[edge.c1]; // Get current cluster ID for original node
        let c2 = cluster_map[edge.c2];

        if c1 == c2 {
            continue;
        } // Already merged

        // Check size constraint
        let size1 = clusters[c1].len();
        let size2 = clusters[c2].len();

        if size1 + size2 <= max_size {
            // Merge c2 into c1
            let stops_to_move = clusters[c2].clone();
            for &stop in &stops_to_move {
                cluster_map[stop] = c1;
            }
            clusters[c1].extend(stops_to_move);
            clusters[c2].clear();
            active_clusters.remove(&c2);
        }
    }

    // PASS 2: Cleanup Orphans (Force merge small clusters)
    // Definition of "Small": Less than 1% of max_size, or < 20 stops
    let min_size = (max_size / 100).max(20);

    // We need to iterate until no more merges happen, or just do one pass?
    // One pass is probably enough to catch the worst offenders.
    // We iterate through all active clusters.

    // To do this efficiently, we need to know which clusters are connected.
    // Re-scanning adjacency is expensive but necessary.
    // Let's build a Cluster Adjacency Graph.

    let mut cluster_adj: HashMap<(usize, usize), u32> = HashMap::new();
    for (&(u, v), &w) in adjacency {
        let c1 = cluster_map[u];
        let c2 = cluster_map[v];
        if c1 != c2 {
            let (min, max) = if c1 < c2 { (c1, c2) } else { (c2, c1) };
            *cluster_adj.entry((min, max)).or_default() += w;
        }
    }

    let active_list: Vec<usize> = active_clusters.iter().cloned().collect();
    let mut merged_in_pass2 = HashSet::new();

    for &c_idx in &active_list {
        if merged_in_pass2.contains(&c_idx) {
            continue;
        }

        let size = clusters[c_idx].len();
        if size < min_size {
            // Find best neighbour
            let mut best_neighbor = None;
            let mut max_weight = 0;

            for &other_c in &active_list {
                if c_idx == other_c {
                    continue;
                }
                if merged_in_pass2.contains(&other_c) {
                    continue;
                } // Don't merge into something that's already gone (though we could chain)

                let (min, max) = if c_idx < other_c {
                    (c_idx, other_c)
                } else {
                    (other_c, c_idx)
                };
                if let Some(&w) = cluster_adj.get(&(min, max)) {
                    if w > max_weight {
                        max_weight = w;
                        best_neighbor = Some(other_c);
                    }
                }
            }

            if let Some(target) = best_neighbor {
                // Force Merge c_idx into target
                // Note: This might exceed max_size, but that's acceptable to avoid orphans.
                println!(
                    "    - Merging orphan cluster {} (size {}) into {} (size {})",
                    c_idx,
                    size,
                    target,
                    clusters[target].len()
                );

                let stops_to_move = clusters[c_idx].clone();
                for &stop in &stops_to_move {
                    cluster_map[stop] = target;
                }
                clusters[target].extend(stops_to_move);
                clusters[c_idx].clear();
                active_clusters.remove(&c_idx);
                merged_in_pass2.insert(c_idx);
            }
        }
    }

    clusters.into_iter().filter(|c| !c.is_empty()).collect()
}

// Weighted Merge-Based Clustering
// Nodes have weights (e.g., number of stations in a tile).
pub fn merge_based_clustering_weighted(
    num_nodes: usize,
    adjacency: &HashMap<(usize, usize), u32>,
    max_size: usize,
    node_weights: &[usize],
) -> Vec<Vec<usize>> {
    // Initial clusters: each node is a cluster
    let mut clusters: Vec<Vec<usize>> = (0..num_nodes).map(|i| vec![i]).collect();
    let mut cluster_map: Vec<usize> = (0..num_nodes).collect(); // Node -> Cluster Index
    let mut active_clusters: HashSet<usize> = (0..num_nodes).collect();

    // Track cluster weights
    let mut cluster_weights: Vec<usize> = node_weights.to_vec();

    // Edge Priority Queue (Weight, Cluster A, Cluster B)
    #[derive(Eq, PartialEq)]
    struct Edge {
        weight: u32,
        c1: usize,
        c2: usize,
    }
    impl Ord for Edge {
        fn cmp(&self, other: &Self) -> Ordering {
            self.weight.cmp(&other.weight)
        }
    }
    impl PartialOrd for Edge {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut pq = BinaryHeap::new();

    // Initialize PQ with node-node edges
    for (&(u, v), &w) in adjacency {
        pq.push(Edge {
            weight: w,
            c1: u,
            c2: v,
        });
    }

    // PASS 1: Standard Greedy Merge (Respecting max_size)
    while let Some(edge) = pq.pop() {
        let c1 = cluster_map[edge.c1]; // Get current cluster ID for original node
        let c2 = cluster_map[edge.c2];

        if c1 == c2 {
            continue;
        } // Already merged

        // Check size constraint
        let size1 = cluster_weights[c1];
        let size2 = cluster_weights[c2];

        if size1 + size2 <= max_size {
            // Merge c2 into c1
            let stops_to_move = clusters[c2].clone();
            for &stop in &stops_to_move {
                cluster_map[stop] = c1;
            }
            clusters[c1].extend(stops_to_move);
            clusters[c2].clear();

            cluster_weights[c1] += size2;
            cluster_weights[c2] = 0;

            active_clusters.remove(&c2);
        }
    }

    // PASS 2: Cleanup Orphans (Force merge small clusters)
    // Definition of "Small": Less than 1% of max_size, or < 20 stops
    let min_size = (max_size / 100).max(20);

    let mut cluster_adj: HashMap<(usize, usize), u32> = HashMap::new();
    for (&(u, v), &w) in adjacency {
        let c1 = cluster_map[u];
        let c2 = cluster_map[v];
        if c1 != c2 {
            let (min, max) = if c1 < c2 { (c1, c2) } else { (c2, c1) };
            *cluster_adj.entry((min, max)).or_default() += w;
        }
    }

    let active_list: Vec<usize> = active_clusters.iter().cloned().collect();
    let mut merged_in_pass2 = HashSet::new();

    for &c_idx in &active_list {
        if merged_in_pass2.contains(&c_idx) {
            continue;
        }

        let size = cluster_weights[c_idx];
        if size < min_size {
            // Find best neighbour
            let mut best_neighbor = None;
            let mut max_weight = 0;

            for &other_c in &active_list {
                if c_idx == other_c {
                    continue;
                }
                if merged_in_pass2.contains(&other_c) {
                    continue;
                }

                let (min, max) = if c_idx < other_c {
                    (c_idx, other_c)
                } else {
                    (other_c, c_idx)
                };
                if let Some(&w) = cluster_adj.get(&(min, max)) {
                    if w > max_weight {
                        max_weight = w;
                        best_neighbor = Some(other_c);
                    }
                }
            }

            if let Some(target) = best_neighbor {
                // Force Merge c_idx into target
                println!(
                    "    - Merging orphan cluster {} (size {}) into {} (size {})",
                    c_idx, size, target, cluster_weights[target]
                );

                let stops_to_move = clusters[c_idx].clone();
                for &stop in &stops_to_move {
                    cluster_map[stop] = target;
                }
                clusters[target].extend(stops_to_move);
                clusters[c_idx].clear();

                cluster_weights[target] += cluster_weights[c_idx];
                cluster_weights[c_idx] = 0;

                active_clusters.remove(&c_idx);
                merged_in_pass2.insert(c_idx);
            }
        }
    }

    clusters.into_iter().filter(|c| !c.is_empty()).collect()
}
