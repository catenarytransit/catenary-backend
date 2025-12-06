pub mod tests {
    use crate::trip_based;
    use crate::trip_based::Transfer;
    use ahash::AHashMap as HashMap;
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, TimeDeltaSequence, TransitPartition, TransitStop,
        TripPattern,
    };

    #[test]
    pub fn run_repro() {
        // Setup a simple partition with a transfer
        // Pattern 1: 0 -> 1 -> 2 -> 3 -> 4
        // Pattern 2: 5 -> 6 -> 2 -> 7 -> 8
        // Transfer at 2.

        let mut partition = TransitPartition {
            partition_id: 0,
            stops: (0..10)
                .map(|i| TransitStop {
                    id: i,
                    chateau_idx: 0,
                    station_id: i.to_string(),
                    gtfs_stop_ids: vec![i.to_string()],
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
            local_dag: std::collections::HashMap::new(),
            long_distance_trip_patterns: vec![],
            timezones: vec![],
            boundary: None,
            chateau_ids: vec![],
            external_hubs: vec![],
            long_distance_transfer_patterns: vec![],
            direct_connections_index: std::collections::HashMap::new(),
        };

        // Time Deltas (100s travel, 0s dwell)
        partition.time_deltas.push(TimeDeltaSequence {
            deltas: vec![0, 0, 100, 0, 100, 0, 100, 0, 100, 0],
        });

        // Direction Patterns
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1, 2, 3, 4],
        });
        partition.direction_patterns.push(DirectionPattern {
            stop_indices: vec![5, 6, 2, 7, 8],
        });

        // Trip Patterns
        // P1: 0->4. Departs at 1000.
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

        // P2: 5->8. Departs at 1500.
        // Node 2 arrival in P1: 1000 + 100(0->1) + 100(1->2) = 1200.
        // Node 2 departure in P2: 1500 + 100(5->6) + 100(6->2) = 1700.
        // Transfer 1200 -> 1700 is valid.
        partition.trip_patterns.push(TripPattern {
            chateau_idx: 0,
            route_id: "R2".to_string(),
            direction_pattern_idx: 1,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "T2".to_string(),
                service_mask: 127,
                start_time: 1500,
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
        // Explicit transfer at Node 2 (from P1 to P2)
        // P1 is idx 0, Trip 0. Stop 2 is index 2.
        // P2 is idx 1, Trip 0. Stop 2 is index 2.
        let transfers = vec![Transfer {
            from_pattern_idx: 0,
            from_trip_idx: 0,
            from_stop_idx_in_pattern: 2,
            to_pattern_idx: 1,
            to_trip_idx: 0,
            to_stop_idx_in_pattern: 2,
            duration: 60, // 60s walk
        }];

        // Precompute auxiliary
        let num_trips = partition
            .trip_patterns
            .iter()
            .map(|p| p.trips.len())
            .sum::<usize>();
        let mut pattern_trip_offset = Vec::with_capacity(partition.trip_patterns.len());
        let mut flat_id_to_pattern_trip = Vec::with_capacity(num_trips);
        let mut total_trips = 0;
        for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
            pattern_trip_offset.push(total_trips);
            for t_idx in 0..pattern.trips.len() {
                flat_id_to_pattern_trip.push((p_idx, t_idx));
            }
            total_trips += pattern.trips.len();
        }

        let mut stop_to_patterns: Vec<Vec<(usize, usize)>> =
            vec![Vec::new(); partition.stops.len()];
        for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            for (i, &s_idx) in stop_indices.iter().enumerate() {
                stop_to_patterns[s_idx as usize].push((p_idx, i));
            }
        }

        // Precompute trip_transfer_ranges
        let mut trip_transfer_ranges: HashMap<(usize, usize), (usize, usize)> = HashMap::new();
        let mut start = 0;
        while start < transfers.len() {
            let t = &transfers[start];
            let key = (t.from_pattern_idx, t.from_trip_idx);
            let mut end = start + 1;
            while end < transfers.len()
                && transfers[end].from_pattern_idx == key.0
                && transfers[end].from_trip_idx == key.1
            {
                end += 1;
            }
            trip_transfer_ranges.insert(key, (start, end));
            start = end;
        }

        let mut scratch = trip_based::ProfileScratch::new(partition.stops.len(), num_trips, 16);

        println!("Running compute_profile_query for 0 -> 8...");
        let edges = trip_based::compute_profile_query(
            &partition,
            &transfers,
            &trip_transfer_ranges,
            0,    // Start at 0
            900,  // Start time
            &[8], // Target 8
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            16,
            &mut scratch,
            &ahash::AHashSet::new(),
            false,
        );

        println!("Found {} edges:", edges.len());
        for edge in &edges {
            println!(
                "  {} -> {} ({:?})",
                edge.from_node_idx, edge.to_node_idx, edge.edge_type
            );
        }

        if edges.is_empty() {
            println!("FAILURE: No edges found for valid transfer!");
        } else {
            println!("SUCCESS: Edges found.");
        }
    }
}
