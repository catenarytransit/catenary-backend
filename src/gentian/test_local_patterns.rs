#[cfg(test)]
mod tests {
    use crate::trip_based::{self, ProfileScratch, Transfer};
    use ahash::AHashMap as HashMap;
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, EdgeType, TimeDeltaSequence, TransitPartition,
        TransitStop, TripPattern,
    };

    fn create_mock_partition() -> TransitPartition {
        TransitPartition {
            partition_id: 0,
            stops: (0..10)
                .map(|i| TransitStop {
                    id: i,
                    chateau_idx: 0,
                    gtfs_original_id: i.to_string(),
                    is_hub: false,
                    is_border: false,
                    is_external_gateway: false,
                    is_long_distance: false,
                    lat: 0.0,
                    lon: 0.0,
                })
                .collect(),
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
    fn test_dag_construction_simple() {
        let mut partition = create_mock_partition();

        // Network:
        // 0 -> 1 -> 2 (Fast, direct)
        // 0 -> 3 -> 2 (Slow, direct)
        // 0 -> 4 -> 2 (Transfer at 4)

        // Time Deltas
        partition.time_deltas.push(TimeDeltaSequence {
            deltas: vec![0, 0, 600, 0, 600, 0], // 10 min travel
        });

        // P1: 0 -> 1 -> 2 (Fast)
        // Dep 0: 10:00. Arr 2: 10:20.
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1, 2],
        });
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T1".to_string(),
                service_mask: 127,
                start_time: 36000, // 10:00
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
        });

        // P2: 0 -> 3 -> 2 (Slow)
        // Dep 0: 10:05. Arr 2: 10:45. (20 min travel each leg)
        partition.time_deltas.push(TimeDeltaSequence {
            deltas: vec![0, 0, 1200, 0, 1200, 0],
        });
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 3, 2],
        });
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R2".to_string(),
            direction_pattern_idx: 1,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T2".to_string(),
                service_mask: 127,
                start_time: 36300, // 10:05
                time_delta_idx: 1,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
        });

        // Prepare structures
        let transfers = trip_based::compute_initial_transfers(&partition);
        // No transfers expected yet.

        // Precompute aux
        let num_trips = 2;
        let mut pattern_trip_offset = vec![0, 1];
        let mut flat_id_to_pattern_trip: Vec<(usize, usize)> = vec![(0, 0), (1, 0)];
        let mut stop_to_patterns: Vec<Vec<(usize, usize)>> = vec![vec![]; 10];
        stop_to_patterns[0].push((0, 0)); // P1 at 0
        stop_to_patterns[1].push((0, 1)); // P1 at 1
        stop_to_patterns[2].push((0, 2)); // P1 at 2
        stop_to_patterns[0].push((1, 0)); // P2 at 0
        stop_to_patterns[3].push((1, 1)); // P2 at 3
        stop_to_patterns[2].push((1, 2)); // P2 at 2

        let trip_transfer_ranges: HashMap<(usize, usize), (usize, usize)> = HashMap::new();

        let mut scratch = ProfileScratch::new(10, 2, 6);
        let targets: Vec<u32> = (0..10).collect();

        // Run Profile Query from 0
        let edges = trip_based::compute_profile_query(
            &partition,
            &transfers,
            &trip_transfer_ranges,
            0,
            0,
            &targets,
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            6,
            &mut scratch,
        );

        // Verify Edges
        // We expect edges for P1 (Fast): 0->1, 1->2.
        // We expect edges for P2 (Slow)?
        // P1 arrives 2 at 10:20. P2 arrives 2 at 10:45.
        // P1 dominates P2 for stop 2.
        // However, P2 serves stop 3. P1 does not.
        // So P2 should be used to reach stop 3.
        // Edge 0->3 should exist.
        // Edge 3->2?
        // At stop 2, P1 arrives 10:20. P2 arrives 10:45.
        // If we want Pareto optimal (transfers, time).
        // Both are 0 transfers.
        // P1 is strictly better for stop 2.
        // So P2->2 should NOT be in the DAG (unless it's part of a path to somewhere else? No).
        // So we expect: 0->1, 1->2 (from P1).
        // And 0->3 (from P2).
        // 3->2 should NOT be present.

        println!("Edges: {:?}", edges);

        let has_0_1 = edges
            .iter()
            .any(|e| e.from_hub_idx == 0 && e.to_hub_idx == 1);
        let has_1_2 = edges
            .iter()
            .any(|e| e.from_hub_idx == 1 && e.to_hub_idx == 2);
        let has_0_3 = edges
            .iter()
            .any(|e| e.from_hub_idx == 0 && e.to_hub_idx == 3);
        let has_3_2 = edges
            .iter()
            .any(|e| e.from_hub_idx == 3 && e.to_hub_idx == 2);

        assert!(has_0_1, "Should have 0->1");
        assert!(has_1_2, "Should have 1->2");
        assert!(has_0_3, "Should have 0->3");
        assert!(!has_3_2, "Should NOT have 3->2 (dominated)");
    }
}
