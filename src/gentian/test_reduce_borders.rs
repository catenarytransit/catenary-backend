use crate::reduce_borders::reduce_borders_by_merging;
use ahash::AHashMap as HashMap;

#[test]
fn test_reduce_borders_by_merging() {
    // Scenario:
    // Cluster 0: [0..200]
    // Cluster 1: [200..400]
    // Cluster 2: [400..402]
    //
    // Connections:
    // Many connections between C0 and C1 to ensure reduction > 100.
    // Weak connection to C2.

    let mut clusters = Vec::new();
    let c0: Vec<usize> = (0..200).collect();
    let c1: Vec<usize> = (200..400).collect();
    let c2: Vec<usize> = vec![400, 401];

    clusters.push(c0);
    clusters.push(c1);
    clusters.push(c2);

    let mut adjacency = HashMap::new();

    // Create > 100 connections between C0 and C1
    // Connect i in C0 to i+200 in C1 for i in 0..150
    for i in 0..150 {
        adjacency.insert((i, i + 200), 10);
    }

    // Weak connection C1 <-> C2
    adjacency.insert((399, 400), 1);

    // Total nodes = 402
    let new_clusters = reduce_borders_by_merging(clusters, &adjacency, 1000, 402);

    // Expect C0 and C1 to merge. C2 stays separate.
    // Result: 2 clusters.
    assert_eq!(new_clusters.len(), 2, "Should merge C0 and C1");

    // Check sizes
    let sizes: Vec<usize> = new_clusters.iter().map(|c| c.len()).collect();
    assert!(sizes.contains(&400)); // Merged (200 + 200)
    assert!(sizes.contains(&2)); // C2
}

#[test]
fn test_hard_cap_cluster_size() {
    // Scenario:
    // Cluster 0: Size 6000
    // Cluster 1: Size 200
    // Max cluster size: 10000
    // Hard cap: 6109
    //
    // Even though 6000 + 200 < 10000, it is > 6109.
    // So they should NOT merge.

    let mut clusters = Vec::new();
    // Create dummy clusters with just indices
    // C0: 0..6000
    let c0: Vec<usize> = (0..6000).collect();
    // C1: 6000..6200
    let c1: Vec<usize> = (6000..6200).collect();

    clusters.push(c0);
    clusters.push(c1);

    let mut adjacency = HashMap::new();
    // Strong connection between C0 and C1
    // Just pick one node from each to connect
    adjacency.insert((0, 6000), 1000);

    // Total nodes = 6200
    let new_clusters = reduce_borders_by_merging(clusters, &adjacency, 10000, 6200);

    // Should NOT merge
    assert_eq!(new_clusters.len(), 2, "Should NOT merge due to hard cap");
    assert_eq!(new_clusters[0].len(), 6000);
    assert_eq!(new_clusters[1].len(), 200);
}
