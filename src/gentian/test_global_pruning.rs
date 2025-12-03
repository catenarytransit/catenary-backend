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
            local_transfer_patterns: vec![],
            long_distance_trip_patterns: vec![],
            timezones: vec![],
            boundary: None,
            chateau_ids: vec![],
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
                gtfs_original_id: i.to_string(),
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
            .local_transfer_patterns
            .push(LocalTransferPattern {
                from_stop_idx: 0,
                edges: vec![DagEdge {
                    from_hub_idx: 0,
                    to_hub_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 10,
                    })),
                }],
            });
        // A -> B (Direct, Slow)
        partition
            .local_transfer_patterns
            .last_mut()
            .unwrap()
            .edges
            .push(DagEdge {
                from_hub_idx: 0,
                to_hub_idx: 2,
                edge_type: Some(EdgeType::Walk(WalkEdge {
                    duration_seconds: 25,
                })),
            });

        // H -> B
        partition
            .local_transfer_patterns
            .push(LocalTransferPattern {
                from_stop_idx: 1,
                edges: vec![DagEdge {
                    from_hub_idx: 1,
                    to_hub_idx: 2,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 10,
                    })),
                }],
            });

        // B -> (None)
        partition
            .local_transfer_patterns
            .push(LocalTransferPattern {
                from_stop_idx: 2,
                edges: vec![],
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
            gtfs_original_id: "A".to_string(),
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
                gtfs_original_id: "Z".to_string(),
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
            });
        }

        add_long_pattern(&mut p0);

        let mut p1 = create_mock_partition(1);
        p1.stops.push(TransitStop {
            id: 0,
            chateau_idx: 0,
            gtfs_original_id: "H".to_string(),
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
            gtfs_original_id: "B".to_string(),
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
            from_hub_idx: 0,
            to_hub_idx: 0,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: 10,
            })),
        };
        let edge_hb = DagEdge {
            from_hub_idx: 0,
            to_hub_idx: 0,
            edge_type: Some(EdgeType::Walk(WalkEdge {
                duration_seconds: 10,
            })),
        };
        let edge_ab = DagEdge {
            from_hub_idx: 0,
            to_hub_idx: 0,
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
            &[],
            &global_to_partition_map,
            &loaded_partitions,
            &output_dir,
        );

        // Load result
        let path = output_dir.join("global_patterns.pbf");
        let global_index: GlobalPatternIndex =
            catenary::routing_common::osm_graph::load_pbf(path.to_str().unwrap()).unwrap();

        // Check PartitionDAGs
        // We expect P0 -> P1 (A -> H)
        // We expect P1 -> P2 (H -> B)
        // We expect NO P0 -> P2 (A -> B) because it was pruned.

        let dag_0_1 = global_index
            .partition_dags
            .iter()
            .find(|d| d.from_partition == 0 && d.to_partition == 1);
        assert!(dag_0_1.is_some(), "Should have DAG P0 -> P1");

        let dag_1_2 = global_index
            .partition_dags
            .iter()
            .find(|d| d.from_partition == 1 && d.to_partition == 2);
        assert!(dag_1_2.is_some(), "Should have DAG P1 -> P2");

        let dag_0_2 = global_index
            .partition_dags
            .iter()
            .find(|d| d.from_partition == 0 && d.to_partition == 2);

        // If pruning works, dag_0_2 should be None OR have no edges.
        // Actually, if no edges are found, `partition_dags` might not even contain the entry?
        // My code: `if let Some(edges) = useful_edges_by_pair.get(&p_end)`
        // If edges is empty (after pruning), it might still push?
        // `edges_map.remove(&(u, v))` removes from map.
        // If map becomes empty, `useful_edges_by_pair` still has the key?
        // `useful_edges_by_pair` is populated if we find *any* path.
        // If we prune all edges, the map entry exists but is empty?
        // Wait, `useful_edges_by_pair` stores *edges*.
        // If we prune all edges, `edges` is empty.
        // Then `dag_edges` will be empty.
        // Then we push `PartitionDag` with empty edges?
        // `partition_dags.push(...)`.

        if let Some(dag) = dag_0_2 {
            // Map DAG indices to Global IDs
            let mut dag_idx_to_global = HashMap::new();
            for (i, hub) in dag.hubs.iter().enumerate() {
                // We need to map back to global ID.
                // We know:
                // P0, stop 0 -> Global 0 (A)
                // P1, stop 0 -> Global 1 (H)
                // P2, stop 0 -> Global 2 (B)
                let global_id = match (hub.original_partition_id, hub.stop_idx_in_partition) {
                    (0, 0) => 0,
                    (1, 0) => 1,
                    (2, 0) => 2,
                    _ => panic!("Unknown hub"),
                };
                dag_idx_to_global.insert(i as u32, global_id);
            }

            let mut has_a_h = false;
            let mut has_h_b = false;
            let mut has_a_b = false;

            for edge in &dag.edges {
                let u = dag_idx_to_global[&edge.from_hub_idx];
                let v = dag_idx_to_global[&edge.to_hub_idx];
                match (u, v) {
                    (0, 1) => has_a_h = true,
                    (1, 2) => has_h_b = true,
                    (0, 2) => has_a_b = true,
                    _ => {}
                }
            }

            assert!(has_a_h, "Should contain A -> H");
            assert!(has_h_b, "Should contain H -> B");
            assert!(!has_a_b, "Should NOT contain A -> B (pruned)");
        } else {
            panic!("DAG P0 -> P2 should exist");
        }
    }
}
