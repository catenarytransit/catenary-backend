#[cfg(test)]
mod tests {
    use crate::trip_based::{ProfileScratch, compute_initial_transfers, compute_profile_query};
    use ahash::{AHashMap as HashMap, AHashSet as HashSet};
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, EdgeType, StaticTransfer, TimeDeltaSequence,
        TransitPartition, TransitStop, TripPattern,
    };

    #[test]
    fn test_cut_at_hub() {
        // Stop 0 -> Stop 1 (Hub) -> Stop 2
        let stops = vec![
            TransitStop {
                id: 0,
                chateau_idx: 0,
                station_id: "s0".to_string(),
                gtfs_stop_ids: vec!["s0".to_string()],
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            },
            TransitStop {
                id: 1,
                chateau_idx: 0,
                station_id: "s1".to_string(),
                gtfs_stop_ids: vec!["s1".to_string()],
                is_hub: true,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            },
            TransitStop {
                id: 2,
                chateau_idx: 0,
                station_id: "s2".to_string(),
                gtfs_stop_ids: vec!["s2".to_string()],
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            },
        ];

        let direction_patterns = vec![DirectionPattern {
            stop_indices: vec![0, 1, 2],
        }];

        let time_deltas = vec![TimeDeltaSequence {
            deltas: vec![0, 0, 600, 0, 600, 0], // 10 mins to s1, 10 mins to s2
        }];

        let trips = vec![CompressedTrip {
            gtfs_trip_id: "t0".to_string(),
            service_mask: 1,
            start_time: 3600,
            time_delta_idx: 0,
            service_idx: 0,
            bikes_allowed: 0,
            wheelchair_accessible: 0,
        }];

        let trip_patterns = vec![TripPattern {
            chateau_idx: 0,
            route_id: "r0".to_string(),
            direction_pattern_idx: 0,
            trips,
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        }];

        let partition = TransitPartition {
            partition_id: 0,
            stops,
            trip_patterns,
            time_deltas,
            direction_patterns,
            internal_transfers: vec![],
            osm_links: vec![],
            service_ids: vec!["s1".to_string()],
            service_exceptions: vec![],
            _deprecated_external_transfers: vec![],
            local_dag: std::collections::HashMap::new(),
            long_distance_trip_patterns: vec![],
            timezones: vec!["UTC".to_string()],
            boundary: None,
            chateau_ids: vec!["c1".to_string()],
            external_hubs: vec![],
            long_distance_transfer_patterns: vec![],
            direct_connections_index: Default::default(),
        };

        let transfers = compute_initial_transfers(&partition);
        let trip_transfer_ranges: HashMap<(usize, usize), (usize, usize)> = HashMap::new();

        let mut stop_to_patterns = vec![vec![]; 3];
        stop_to_patterns[0].push((0, 0));
        stop_to_patterns[1].push((0, 1));
        stop_to_patterns[2].push((0, 2));

        let mut flat_id_to_pattern_trip = Vec::new();
        let pattern_trip_offset = vec![0];
        flat_id_to_pattern_trip.push((0, 0));

        let mut scratch = ProfileScratch::new(3, 1, 2);
        let mut hubs: HashSet<u32> = HashSet::new();
        hubs.insert(1);

        let edges = compute_profile_query(
            &partition,
            &transfers,
            &trip_transfer_ranges,
            0u32,          // Start stop
            0u32,          // Start time
            &[1u32, 2u32], // Target stops (try to reach both)
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            2,
            &mut scratch,
            &hubs,
            false,
        );

        println!("Generated {} edges", edges.len());
        for (i, edge) in edges.iter().enumerate() {
            println!("Edge {}: {:?}", i, edge);
        }

        // We expect:
        // 1. Walk 0->0
        // 2. Transit 0->1 (Hub)
        // We expect NO edge to 2, because we cut at 1.

        let edges_to_2 = edges.iter().filter(|e| e.to_node_idx == 2).count();
        assert_eq!(
            edges_to_2, 0,
            "Expected 0 edges to Stop 2 (beyond hub), found {}",
            edges_to_2
        );

        let edges_to_1 = edges.iter().filter(|e| e.to_node_idx == 1).count();
        assert_eq!(
            edges_to_1, 1,
            "Expected 1 edge to Stop 1 (Hub), found {}",
            edges_to_1
        );
    }

    #[test]
    fn test_duplicate_edges_repro() {
        // Stop 0 -> Stop 1 (Pattern A)
        // Stop 1 -> Stop 2 (Pattern B, multiple trips)
        let stops = vec![
            TransitStop {
                id: 0,
                chateau_idx: 0,
                station_id: "s0".to_string(),
                gtfs_stop_ids: vec!["s0".to_string()],
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            },
            TransitStop {
                id: 1,
                chateau_idx: 0,
                station_id: "s1".to_string(),
                gtfs_stop_ids: vec!["s1".to_string()],
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            },
            TransitStop {
                id: 2,
                chateau_idx: 0,
                station_id: "s2".to_string(),
                gtfs_stop_ids: vec!["s2".to_string()],
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                is_long_distance: false,
                lat: 0.0,
                lon: 0.0,
            },
        ];

        let direction_patterns = vec![
            DirectionPattern {
                stop_indices: vec![0, 1],
            }, // Pat 0
            DirectionPattern {
                stop_indices: vec![1, 2],
            }, // Pat 1
        ];

        let time_deltas = vec![TimeDeltaSequence {
            deltas: vec![0, 0, 600, 0], // 10 mins
        }];

        let mut trips = Vec::new();
        // Pattern 0: 1 trip
        trips.push(CompressedTrip {
            gtfs_trip_id: "t0".to_string(),
            service_mask: 1,
            start_time: 3600,
            time_delta_idx: 0,
            service_idx: 0,
            bikes_allowed: 0,
            wheelchair_accessible: 0,
        });

        // Pattern 1: 10 trips
        let mut trips_p1 = Vec::new();
        for i in 0..10 {
            trips_p1.push(CompressedTrip {
                gtfs_trip_id: format!("t1_{}", i),
                service_mask: 1,
                start_time: 3600 + 600 + 60 + i * 1800, // Arrive s1 at 3600+600=4200. Departs 4260 + ...
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            });
        }

        let trip_patterns = vec![
            TripPattern {
                chateau_idx: 0,
                route_id: "r0".to_string(),
                direction_pattern_idx: 0,
                trips,
                timezone_idx: 0,
                route_type: 3,
                is_border: false,
            },
            TripPattern {
                chateau_idx: 0,
                route_id: "r1".to_string(),
                direction_pattern_idx: 1,
                trips: trips_p1,
                timezone_idx: 0,
                route_type: 3,
                is_border: false,
            },
        ];

        let internal_transfers = vec![StaticTransfer {
            from_stop_idx: 1,
            to_stop_idx: 1,
            duration_seconds: 60,
            distance_meters: 0,
            wheelchair_accessible: true,
        }];

        let partition = TransitPartition {
            partition_id: 0,
            stops,
            trip_patterns,
            time_deltas,
            direction_patterns,
            internal_transfers,
            osm_links: vec![],
            service_ids: vec!["s1".to_string()],
            service_exceptions: vec![],
            _deprecated_external_transfers: vec![],
            local_dag: std::collections::HashMap::new(),
            long_distance_trip_patterns: vec![],
            timezones: vec!["UTC".to_string()],
            boundary: None,
            chateau_ids: vec!["c1".to_string()],
            external_hubs: vec![],
            long_distance_transfer_patterns: vec![],
            direct_connections_index: Default::default(),
        };

        let mut transfers = compute_initial_transfers(&partition);
        // Ensure transfers exist
        println!("Computed {} transfers", transfers.len());

        // Build trip_transfer_ranges
        transfers.sort_by(|a, b| {
            a.from_pattern_idx
                .cmp(&b.from_pattern_idx)
                .then(a.from_trip_idx.cmp(&b.from_trip_idx))
        });
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

        let mut stop_to_patterns = vec![vec![]; 3];
        stop_to_patterns[0].push((0, 0));
        stop_to_patterns[1].push((0, 1));
        stop_to_patterns[1].push((1, 0));
        stop_to_patterns[2].push((1, 1));

        let mut flat_id_to_pattern_trip = Vec::new();
        let mut pattern_trip_offset = vec![0, 1];
        flat_id_to_pattern_trip.push((0, 0)); // P0 T0
        for i in 0..10 {
            flat_id_to_pattern_trip.push((1, i)); // P1 Ti
        }

        let mut scratch = ProfileScratch::new(3, 11, 2);
        let hubs: HashSet<u32> = HashSet::new();

        let edges = compute_profile_query(
            &partition,
            &transfers,
            &trip_transfer_ranges,
            0u32,    // Start stop
            0u32,    // Start time
            &[2u32], // Target stop
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            2,
            &mut scratch,
            &hubs,
            false,
        );

        println!("Generated {} edges", edges.len());
        for (i, edge) in edges.iter().enumerate() {
            println!("Edge {}: {:?}", i, edge);
        }

        // We expect:
        // 1. Walk 0->0
        // 2. Transit 0->1 (Pat 0)
        // 3. Walk 1->1 (Transfer)
        // 4. Transit 1->2 (Pat 1) - Should be 1 edge, but if duplicate, multiple.

        // Count edges 1->2
        let p1_edges = edges
            .iter()
            .filter(|e| e.from_node_idx == 1 && e.to_node_idx == 2)
            .count();
        println!("Edges from 1 to 2: {}", p1_edges);

        assert_eq!(
            p1_edges, 1,
            "Expected 1 edge for Pattern 1, found {}",
            p1_edges
        );
    }
}
