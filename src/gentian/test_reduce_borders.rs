
#[test]
fn test_reduce_borders_by_merging() {
    // Scenario:
    // Cluster 0: [0, 1]
    // Cluster 1: [2, 3]
    // Cluster 2: [4, 5]
    //
    // Connections:
    // 0 -> 2 (strong)
    // 1 -> 3 (strong)
    // 2 -> 4 (weak)
    //
    // Merging 0 and 1 should reduce borders significantly (0, 1, 2, 3 are borders).
    // If merged, 0->2 and 1->3 become internal.
    // Border set of {0,1} was {0, 1}. Border set of {2,3} was {2, 3, 2->4}.
    // Merged {0,1,2,3}: 0->2 internal, 1->3 internal.
    // Only 2->4 remains external. So 2 is border.
    // Reduction: 4 borders -> 1 border. High reduction.

    let clusters = vec![vec![0, 1], vec![2, 3], vec![4, 5]];
    let mut adjacency = HashMap::new();

    // C0 <-> C1 (Strong)
    adjacency.insert((0, 2), 10);
    adjacency.insert((1, 3), 10);

    // C1 <-> C2 (Weak)
    adjacency.insert((2, 4), 1);

    let new_clusters = reduce_borders_by_merging(clusters, &adjacency, 10);

    // Expect C0 and C1 to merge. C2 stays separate.
    // Result: 2 clusters.
    assert_eq!(new_clusters.len(), 2, "Should merge C0 and C1");

    // Check sizes
    let sizes: Vec<usize> = new_clusters.iter().map(|c| c.len()).collect();
    assert!(sizes.contains(&4)); // Merged
    assert!(sizes.contains(&2)); // C2
}
