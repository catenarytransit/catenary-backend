#[cfg(test)]
mod tests {
    use crate::connectivity::compute_border_patterns;
    use ahash::AHashMap as HashMap;
    use catenary::routing_common::transit_graph::{
        DagEdge, EdgeType, GlobalHub, LocalTransferPattern, TransitEdge, TransitPartition,
        TransitStop, WalkEdge,
    };

    #[test]
    fn test_border_computation_simple() {
        // Scenario:
        // Partition 0:
        //   Stop 0 (Border)
        //   Stop 1 (Internal)
        //   Local Pattern: 0 -> 1 (Walk, 10s)
        //   Long Dist Pattern: 1 -> ExtHub(P1, Stop 0) (Transit, 100s)
        // Partition 1:
        //   Stop 0 (Border)

        // Expected Result:
        //   DAG for P0 -> P1:
        //     Nodes: P0:0, P0:1, P1:0
        //     Edges: (P0:0 -> P0:1), (P0:1 -> P1:0)

        let mut partitions = HashMap::new();

        // Partition 0
        let mut p0 = TransitPartition::default();
        p0.local_dag = std::collections::HashMap::new();
        p0.partition_id = 0;
        p0.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "s0".to_string(),
            gtfs_stop_ids: vec!["p0_s0".to_string()],
            is_hub: true,
            is_border: true,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.0,
            lon: 0.0,
        });
        p0.stops.push(TransitStop {
            id: 1,
            chateau_idx: 0,
            station_id: "s1".to_string(),
            gtfs_stop_ids: vec!["p0_s1".to_string()],
            is_hub: true,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: true, // Needs to be long distance for long dist pattern? Not strictly, but usually.
            lat: 0.0,
            lon: 0.0,
        });

        // Local Pattern 0 -> 1
        // Local Pattern 0 -> 1
        p0.local_dag
            .entry(0)
            .or_insert_with(|| catenary::routing_common::transit_graph::DagEdgeList {
                edges: vec![],
            })
            .edges
            .push(DagEdge {
                from_node_idx: 0,
                to_node_idx: 1,
                edge_type: Some(EdgeType::Walk(WalkEdge {
                    duration_seconds: 10,
                })),
            });

        // External Hubs
        p0.external_hubs.push(GlobalHub {
            original_partition_id: 1,
            stop_idx_in_partition: 0,
        });

        // Long Distance Pattern 1 -> ExtHub(0) (which is P1:0)
        // Note: to_node_idx for long distance pattern points to `stops.len() + ext_hub_idx`
        let ext_hub_idx = 0;
        let target_idx = p0.stops.len() as u32 + ext_hub_idx;

        p0.long_distance_transfer_patterns
            .push(LocalTransferPattern {
                from_stop_idx: 1,
                edges: vec![DagEdge {
                    from_node_idx: 1,
                    to_node_idx: target_idx,
                    edge_type: Some(EdgeType::Transit(TransitEdge {
                        trip_pattern_idx: 0,
                        start_stop_idx: 0,
                        end_stop_idx: 1,
                        min_duration: 100,
                    })),
                }],
            });

        partitions.insert(0, p0);

        // Partition 1
        let mut p1 = TransitPartition::default();
        p1.local_dag = std::collections::HashMap::new();
        p1.partition_id = 1;
        p1.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "s2".to_string(),
            gtfs_stop_ids: vec!["p1_s0".to_string()],
            is_hub: true,
            is_border: true,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.0,
            lon: 0.0,
        });
        partitions.insert(1, p1);

        // Border Nodes
        let mut border_nodes = HashMap::new();
        border_nodes.insert(0, vec![partitions[&0].stops[0].clone()]);
        border_nodes.insert(1, vec![partitions[&1].stops[0].clone()]);

        // Run Computation
        let dags = compute_border_patterns(&border_nodes, &partitions, &HashMap::new());

        // Assertions
        assert_eq!(dags.len(), 1, "Should produce 1 DAG (P0 -> P1)");
        let dag = &dags[0];
        assert_eq!(dag.from_partition, 0);
        assert_eq!(dag.to_partition, 1);

        // Check Hubs
        // We expect P0:0, P0:1, P1:0
        assert_eq!(dag.hubs.len(), 3);
        let has_p0_0 = dag
            .hubs
            .iter()
            .any(|h| h.original_partition_id == 0 && h.stop_idx_in_partition == 0);
        let has_p0_1 = dag
            .hubs
            .iter()
            .any(|h| h.original_partition_id == 0 && h.stop_idx_in_partition == 1);
        let has_p1_0 = dag
            .hubs
            .iter()
            .any(|h| h.original_partition_id == 1 && h.stop_idx_in_partition == 0);
        assert!(has_p0_0);
        assert!(has_p0_1);
        assert!(has_p1_0);

        // Check Edges
        // 0 -> 1 (Walk)
        // 1 -> 2 (Transit) (assuming indices match hubs order)
        assert_eq!(dag.edges.len(), 2);
    }
}
