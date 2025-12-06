#[cfg(test)]
mod tests {
    use crate::trip_based::{self, Transfer};
    use ahash::AHashMap as HashMap;
    use ahash::AHashSet as HashSet;
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, TimeDeltaSequence, TransitPartition, TransitStop,
        TripPattern,
    };

    fn create_mock_partition() -> TransitPartition {
        TransitPartition {
            partition_id: 0,
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
    fn test_hub_pruning() {
        // Graph: A (0) -> H (1, Hub) -> B (2)
        // A -> H is Pattern 1
        // H -> B is Pattern 2
        // Transfer at H.

        let mut partition = create_mock_partition();

        // Stops
        for i in 0..3 {
            partition.stops.push(TransitStop {
                id: i as u64,
                chateau_idx: 0,
                station_id: i.to_string(),
                gtfs_stop_ids: vec![i.to_string()],
                is_hub: i == 1, // H is hub
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            });
        }

        // Time Deltas (100s travel)
        partition.time_deltas.push(TimeDeltaSequence {
            deltas: vec![0, 0, 100, 0],
        });

        // Patterns
        // P1: 0 -> 1
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1],
        });
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T1".to_string(),
                service_mask: 127,
                start_time: 1000,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        });

        // P2: 1 -> 2
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![1, 2],
        });
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R2".to_string(),
            direction_pattern_idx: 1,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T2".to_string(),
                service_mask: 127,
                start_time: 1200, // Arrive at 1 at 1100. Depart 1200.
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        });

        // Transfers
        let transfers = vec![Transfer {
            from_pattern_idx: 0,
            from_trip_idx: 0,
            from_stop_idx_in_pattern: 1, // Stop 1
            to_pattern_idx: 1,
            to_trip_idx: 0,
            to_stop_idx_in_pattern: 0, // Stop 1
            duration: 60,
        }];

        // Aux structures
        let mut stop_to_patterns = vec![vec![]; 3];
        stop_to_patterns[0].push((0, 0));
        stop_to_patterns[1].push((0, 1));
        stop_to_patterns[1].push((1, 0));
        stop_to_patterns[2].push((1, 1));

        let flat_id_to_pattern_trip = vec![(0, 0), (1, 0)];
        let pattern_trip_offset = vec![0, 1];
        let mut trip_transfer_ranges = HashMap::new();
        trip_transfer_ranges.insert((0, 0), (0, 1));

        let mut scratch = trip_based::ProfileScratch::new(3, 2, 2);
        let mut hubs = HashSet::new();
        hubs.insert(1);

        // Run query from A (0)
        let edges = trip_based::compute_profile_query(
            &partition,
            &transfers,
            &trip_transfer_ranges,
            0,
            0,
            &[0, 1, 2],
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            2,
            &mut scratch,
            &hubs,
            false, // A is not a hub
        );

        // Expect:
        // A -> H (Active) -> Stored
        // H -> B (Inactive, because we transferred at H) -> Pruned?
        // Wait, if we transfer at H, the label becomes inactive.
        // "Inactive labels are ignored when the transfer patterns are read off."
        // So A -> B should NOT be in the result.
        // But A -> H should be.

        let has_a_h = edges.iter().any(|e| e.to_node_idx == 1);
        let has_a_b = edges.iter().any(|e| e.to_node_idx == 2);

        assert!(has_a_h, "Should contain A -> H");
        assert!(!has_a_b, "Should NOT contain A -> B (pruned at hub)");
    }

    #[test]
    fn test_non_hub_path() {
        // Graph: A (0) -> C (1, Non-Hub) -> B (2)
        // Same as above but 1 is NOT a hub.

        let mut partition = create_mock_partition();

        // Stops
        for i in 0..3 {
            partition.stops.push(TransitStop {
                id: i as u64,
                chateau_idx: 0,
                station_id: i.to_string(),
                gtfs_stop_ids: vec![i.to_string()],
                is_hub: false, // NO HUB
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            });
        }

        // Time Deltas (100s travel)
        partition.time_deltas.push(TimeDeltaSequence {
            deltas: vec![0, 0, 100, 0],
        });

        // Patterns
        // P1: 0 -> 1
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1],
        });
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T1".to_string(),
                service_mask: 127,
                start_time: 1000,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        });

        // P2: 1 -> 2
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![1, 2],
        });
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R2".to_string(),
            direction_pattern_idx: 1,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T2".to_string(),
                service_mask: 127,
                start_time: 1200,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        });

        // Transfers
        let transfers = vec![Transfer {
            from_pattern_idx: 0,
            from_trip_idx: 0,
            from_stop_idx_in_pattern: 1,
            to_pattern_idx: 1,
            to_trip_idx: 0,
            to_stop_idx_in_pattern: 0,
            duration: 60,
        }];

        // Aux structures
        let mut stop_to_patterns = vec![vec![]; 3];
        stop_to_patterns[0].push((0, 0));
        stop_to_patterns[1].push((0, 1));
        stop_to_patterns[1].push((1, 0));
        stop_to_patterns[2].push((1, 1));

        let flat_id_to_pattern_trip = vec![(0, 0), (1, 0)];
        let pattern_trip_offset = vec![0, 1];
        let mut trip_transfer_ranges = HashMap::new();
        trip_transfer_ranges.insert((0, 0), (0, 1));

        let mut scratch = trip_based::ProfileScratch::new(3, 2, 2);
        let hubs = HashSet::new(); // Empty

        // Run query from A (0)
        let edges = trip_based::compute_profile_query(
            &partition,
            &transfers,
            &trip_transfer_ranges,
            0,
            0,
            &[0, 1, 2],
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            2,
            &mut scratch,
            &hubs,
            false,
        );

        // Expect:
        // A -> C -> B (Active all the way) -> Stored

        let has_a_b = edges.iter().any(|e| e.to_node_idx == 2);

        assert!(has_a_b, "Should contain A -> B (no hub pruning)");
    }
}
