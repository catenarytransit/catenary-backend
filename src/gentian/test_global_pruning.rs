#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectivity::{compute_global_patterns, compute_local_patterns_for_partition};
    use crate::trip_based::{self, Transfer};
    use ahash::AHashMap as HashMap;
    use ahash::AHashSet as HashSet;
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DagEdge, DirectionPattern, EdgeType, GlobalHub, GlobalPatternIndex,
        GlobalTimetable, LocalTransferPattern, PartitionDag, PartitionTimetable, PatternTimetable,
        TimeDeltaSequence, TransitEdge, TransitPartition, TransitStop, TripPattern, WalkEdge,
    };
    use std::path::PathBuf;

    fn create_mock_partition(id: u32) -> TransitPartition {
        TransitPartition {
            partition_id: id,
            stops: vec![],
            trip_patterns: vec![],
            time_deltas: vec![],
            direction_patterns: vec![],
            internal_transfers: vec![],
            osm_links: vec![],
            service_ids: vec![],
            service_exceptions: vec![],
            _deprecated_external_transfers: vec![],
            local_dag: std::collections::HashMap::new(),
            long_distance_trip_patterns: vec![],
            timezones: vec![],
            boundary: None,
            chateau_ids: vec![],
            external_hubs: vec![],
            long_distance_transfer_patterns: vec![],
            direct_connections_index: Default::default(),
        }
    }

    #[test]
    fn test_global_pruning() {
        // Scenario:
        // Partition 0: Nodes A(0), H(1), B(2).
        // A -> H (Cost 10)
        // H -> B (Cost 10)
        // A -> B (Cost 25) -> Should be pruned because 10+10 < 25.
        // All nodes are Hubs/Border nodes so they appear in global graph.

        let mut partition = create_mock_partition(0);

        // Stops
        for i in 0..3 {
            partition.stops.push(TransitStop {
                id: i,
                chateau_idx: 0,
                station_id: i.to_string(),
                gtfs_stop_ids: vec![i.to_string()],
                is_hub: true,    // All are hubs
                is_border: true, // All are border/exposed
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            });
        }

        // Local Transfer Patterns (Mocked directly)
        // A -> H
        partition
            .local_dag
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
        // A -> B (Direct, Slow)
        partition
            .local_dag
            .entry(0)
            .or_insert_with(|| catenary::routing_common::transit_graph::DagEdgeList {
                edges: vec![],
            })
            .edges
            .push(DagEdge {
                from_node_idx: 0,
                to_node_idx: 2,
                edge_type: Some(EdgeType::Walk(WalkEdge {
                    duration_seconds: 25,
                })),
            });

        // H -> B
        partition
            .local_dag
            .entry(1)
            .or_insert_with(|| catenary::routing_common::transit_graph::DagEdgeList {
                edges: vec![],
            })
            .edges
            .push(DagEdge {
                from_node_idx: 1,
                to_node_idx: 2,
                edge_type: Some(EdgeType::Walk(WalkEdge {
                    duration_seconds: 10,
                })),
            });

        // B -> (None)
        partition.local_dag.entry(2).or_insert_with(|| {
            catenary::routing_common::transit_graph::DagEdgeList { edges: vec![] }
        });

        let mut loaded_partitions = HashMap::new();
        loaded_partitions.insert(0, partition);

        let mut border_nodes = HashMap::new();
        border_nodes.insert(
            0,
            vec![
                loaded_partitions[&0].stops[0].clone(),
                loaded_partitions[&0].stops[1].clone(),
                loaded_partitions[&0].stops[2].clone(),
            ],
        );

        // We need to mock global_to_partition_map
        // Global IDs: 0->A, 1->H, 2->B
        let mut global_to_partition_map = HashMap::new();
        global_to_partition_map.insert(0, (0, 0));
        global_to_partition_map.insert(1, (0, 1));
        global_to_partition_map.insert(2, (0, 2));

        // We need to mock output_dir
        let output_dir = std::env::temp_dir().join("gentian_test_pruning");
        std::fs::create_dir_all(&output_dir).unwrap();

        // We need to trick `compute_global_patterns` to run on a single partition?
        // `compute_global_patterns` iterates `partition_ids` as `p_start`.
        // And `p_end` in `partition_ids`.
        // If p_start == p_end, it continues.
        // So it ONLY computes Inter-Partition DAGs.
        // But the pruning logic I implemented is inside the loop for `p_end`.
        // So it only prunes edges between partitions.

        // Wait. The user request was: "ensure that the edge results are pruned such that any results that go directly from A-> B that are worse than going through a hub A->H->B are pruned from the result list"
        // If A and B are in the SAME partition, `compute_global_patterns` does NOT compute edges between them in `partition_dags`.
        // `partition_dags` are strictly P_start -> P_end where P_start != P_end.
        // Intra-partition edges are stored in `LocalTransferPattern`.
        // `LocalTransferPattern`s are computed in `compute_local_patterns_for_partition`.

        // IF the user meant pruning *LocalTransferPatterns*, then I modified the WRONG function.
        // IF the user meant pruning *Global Patterns* (which connect partitions), then I modified the RIGHT function.
        // "Global Hub Graph" usually implies the graph connecting hubs.
        // If A, H, B are all hubs, they form a graph.
        // If A, H, B are in the same partition, their connectivity is defined by `LocalTransferPattern`.
        // If A, H, B are in different partitions, their connectivity is defined by `PartitionDag`.

        // Let's assume the user meant the Global Graph (PartitionDAGs).
        // So I need a test case with 3 partitions?
        // P0(A) -> P1(H) -> P2(B).
        // And P0(A) -> P2(B).

        // Scenario 2:
        // P0 has A.
        // P1 has H.
        // P2 has B.
        // Cross edges:
        // A -> H (Cost 10)
        // H -> B (Cost 10)
        // A -> B (Cost 25)

        // Setup:
        // P0: Stop 0 (A)
        // P1: Stop 0 (H)
        // P2: Stop 0 (B)

        let mut p0 = create_mock_partition(0);
        p0.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "A".to_string(),
            gtfs_stop_ids: vec!["A".to_string()],
            is_hub: true,
            is_border: true,
            is_external_gateway: false,
            is_long_distance: true,
            lat: 0.0,
            lon: 0.0,
        });
        // Need long distance stops to be identified?
        // `compute_global_patterns` uses `long_distance_stations`.
        // It checks `is_hub` AND `is_long_distance` (via `long_distance_stops_per_partition`).
        // `long_distance_stops_per_partition` is computed by checking pattern length > 100km.
        // I can bypass this by mocking `long_distance_stops_per_partition`?
        // No, it's computed inside the function.
        // I need to add a fake long pattern to each partition.

        fn add_long_pattern(p: &mut TransitPartition) {
            p.direction_patterns.push(DirectionPattern {
                stop_indices: vec![0, 1],
            });
            p.stops.push(TransitStop {
                id: 1,
                chateau_idx: 0,
                station_id: "Z".to_string(),
                gtfs_stop_ids: vec!["Z".to_string()],
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 10.0,
                lon: 10.0,
            }); // Far away
            p.trip_patterns.push(TripPattern {
                chateau_idx: 0,
                route_id: "L".to_string(),
                direction_pattern_idx: 0,
                trips: vec![],
                timezone_idx: 0,
                route_type: 3,
                is_border: false,
            });
        }

        add_long_pattern(&mut p0);

        let mut p1 = create_mock_partition(1);
        p1.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "H".to_string(),
            gtfs_stop_ids: vec!["H".to_string()],
            is_hub: true,
            is_border: true,
            is_external_gateway: false,
            is_long_distance: true,
            lat: 0.0,
            lon: 0.0,
        });
        add_long_pattern(&mut p1);

        let mut p2 = create_mock_partition(2);
        p2.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "B".to_string(),
            gtfs_stop_ids: vec!["B".to_string()],
            is_hub: true,
            is_border: true,
            is_external_gateway: false,
            is_long_distance: true,
            lat: 0.0,
            lon: 0.0,
        });
        add_long_pattern(&mut p2);

        let mut loaded_partitions = HashMap::new();
        loaded_partitions.insert(0, p0);
        loaded_partitions.insert(1, p1);
        loaded_partitions.insert(2, p2);

        let mut border_nodes = HashMap::new();
        border_nodes.insert(0, vec![loaded_partitions[&0].stops[0].clone()]);
        border_nodes.insert(1, vec![loaded_partitions[&1].stops[0].clone()]);
        border_nodes.insert(2, vec![loaded_partitions[&2].stops[0].clone()]);

        let mut global_to_partition_map = HashMap::new();
        global_to_partition_map.insert(0, (0, 0)); // A
        global_to_partition_map.insert(1, (1, 0)); // H
        global_to_partition_map.insert(2, (2, 0)); // B

        // Cross Edges
        // A(0) -> H(1) : Cost 10
        // H(1) -> B(2) : Cost 10
        // A(0) -> B(2) : Cost 25

        let edge_ah = DagEdge {
            from_node_idx: 0,
            to_node_idx: 0,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: 10,
            })),
        };
        let edge_hb = DagEdge {
            from_node_idx: 0,
            to_node_idx: 0,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: 10,
            })),
        };
        let edge_ab = DagEdge {
            from_node_idx: 0,
            to_node_idx: 0,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: 25,
            })),
        };

        let cross_edges = vec![((0, 1), edge_ah), ((1, 2), edge_hb), ((0, 2), edge_ab)];

        // Run
        // We can't easily capture output because it writes to file.
        // But we can check if it panics.
        // Ideally we'd modify the function to return the index, but it returns void.
        // I'll have to rely on reading the file back?
        // Or I can copy the logic into the test?
        // Copying logic is bad.
        // I can use `load_pbf` to read the result.

        compute_global_patterns(
            &border_nodes,
            &cross_edges,
            &global_to_partition_map,
            &mut loaded_partitions,
            &output_dir,
        );

        // Verify P0
        let p0 = &loaded_partitions[&0];

        // Check External Hubs
        // Should contain H(1) and B(2)
        let h_idx_opt = p0
            .external_hubs
            .iter()
            .position(|h| h.original_partition_id == 1 && h.stop_idx_in_partition == 0);
        let b_idx_opt = p0
            .external_hubs
            .iter()
            .position(|h| h.original_partition_id == 2 && h.stop_idx_in_partition == 0);

        assert!(h_idx_opt.is_some(), "P0 should know about H");
        assert!(b_idx_opt.is_some(), "P0 should know about B");

        let h_idx = p0.stops.len() as u32 + h_idx_opt.unwrap() as u32;
        let b_idx = p0.stops.len() as u32 + b_idx_opt.unwrap() as u32;
        let a_idx = 0; // Local index of A

        // Check Long Distance Patterns
        // Should have pattern starting at A
        let pattern_a = p0
            .long_distance_transfer_patterns
            .iter()
            .find(|p| p.from_stop_idx == a_idx);
        assert!(pattern_a.is_some(), "Should have pattern starting at A");

        let edges = &pattern_a.unwrap().edges;

        // Edges should be:
        // A -> H (Cost 10)
        // H -> B (Cost 10)
        // A -> B (Cost 25) -> Pruned!

        let has_a_h = edges
            .iter()
            .any(|e| e.from_node_idx == a_idx && e.to_node_idx == h_idx);
        let has_a_b = edges
            .iter()
            .any(|e| e.from_node_idx == a_idx && e.to_node_idx == b_idx);

        assert!(has_a_h, "Should contain A -> H");
        assert!(!has_a_b, "Should NOT contain A -> B (pruned)");

        // Verify P1
        let p1 = &loaded_partitions[&1];
        let h_idx_p1 = 0; // Local index of H in P1

        // Check External Hub B in P1
        let b_idx_opt_p1 = p1
            .external_hubs
            .iter()
            .position(|h| h.original_partition_id == 2 && h.stop_idx_in_partition == 0);
        assert!(b_idx_opt_p1.is_some(), "P1 should know about B");
        let b_idx_p1 = p1.stops.len() as u32 + b_idx_opt_p1.unwrap() as u32;

        let pattern_h = p1
            .long_distance_transfer_patterns
            .iter()
            .find(|p| p.from_stop_idx == h_idx_p1);
        assert!(
            pattern_h.is_some(),
            "Should have pattern starting at H in P1"
        );

        let edges_p1 = &pattern_h.unwrap().edges;
        let has_h_b = edges_p1
            .iter()
            .any(|e| e.from_node_idx == h_idx_p1 && e.to_node_idx == b_idx_p1);
        assert!(has_h_b, "Should contain H -> B in P1");
    }
}
