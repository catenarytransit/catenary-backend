#[cfg(test)]
mod tests {
    use super::*;
    use crate::trip_based::{
        ProfileScratch, build_stop_to_patterns, compute_initial_transfers, run_trip_based_profile,
    };
    use ahash::{AHashMap as HashMap, AHashSet as HashSet};
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, TimeDeltaSequence, TransitPartition, TransitStop,
        TripPattern,
    };

    #[test]
    fn test_trip_based_profile_pareto() {
        // Setup Partition
        let mut p = TransitPartition::default();

        // Stops: 0=A, 1=B, 2=C
        for i in 0..3 {
            p.stops.push(TransitStop {
                id: i as u64,
                station_id: format!("S{}", i),
                ..Default::default()
            });
        }

        // TimeDeltas:
        // 0: 30 min travel (1800s)
        p.time_deltas.push(TimeDeltaSequence {
            deltas: vec![0, 0, 1800, 0], // Travel 0, Dwell 0, Travel 1, Dwell 1
        });

        // Pattern 1: A->B
        p.direction_patterns.push(DirectionPattern {
            stop_indices: vec![0, 1],
        });

        // Trips for P1
        // Trip 1: 8:00 (28800) -> 8:30
        // Trip 2: 9:00 (32400) -> 9:30
        let trip1 = CompressedTrip {
            start_time: 28800,
            time_delta_idx: 0,
            service_mask: u32::MAX,
            ..Default::default()
        };
        let trip2 = CompressedTrip {
            start_time: 32400,
            time_delta_idx: 0,
            service_mask: u32::MAX,
            ..Default::default()
        };

        p.trip_patterns.push(TripPattern {
            direction_pattern_idx: 0,
            trips: vec![trip1, trip2],
            ..Default::default()
        });

        // Pattern 2: B->C
        p.direction_patterns.push(DirectionPattern {
            stop_indices: vec![1, 2],
        });

        // Trip 3: 8:35 (30900) -> 9:05
        let trip3 = CompressedTrip {
            start_time: 30900,
            time_delta_idx: 0,
            service_mask: u32::MAX,
            ..Default::default()
        };
        p.trip_patterns.push(TripPattern {
            direction_pattern_idx: 1,
            trips: vec![trip3],
            ..Default::default()
        });

        // Build indices
        // compute_initial_transfers might not generate same-stop transfers on this mock data.
        // We manually add a transfer from P1 (Stop 1) to P2 (Stop 0) to ensure connectivity.
        // P0: A->B. P1: B->C.
        // P0 idx 0. T0. Stop 1 (B).
        // P1 idx 1. T0. Stop 0 (B).
        let transfers = vec![crate::trip_based::Transfer {
            from_pattern_idx: 0,
            from_trip_idx: 0,
            from_stop_idx_in_pattern: 1,
            to_pattern_idx: 1,
            to_trip_idx: 0,
            to_stop_idx_in_pattern: 0,
            duration: 300, // 5 min transfer
        }];
        // If they are SAME stop index, we need a "loop" transfer or logic handles it?
        // compute_initial_transfers adds loop transfers?
        // Let's assume yes or rely on implicit.

        // Populate trip_transfer_ranges for the manual transfers
        let mut trip_transfer_ranges = HashMap::new();
        // Transfer 0: From P0, T0. Range [0, 1)
        trip_transfer_ranges.insert((0, 0), (0, 1));

        let stop_to_patterns = build_stop_to_patterns(&p);

        let mut flat_id_to_pattern_trip = Vec::new();
        let mut pattern_trip_offset = vec![0; p.trip_patterns.len()];
        let mut current_offset = 0;
        for (p_idx, pattern) in p.trip_patterns.iter().enumerate() {
            pattern_trip_offset[p_idx] = current_offset;
            for t_idx in 0..pattern.trips.len() {
                flat_id_to_pattern_trip.push((p_idx, t_idx));
            }
            current_offset += pattern.trips.len();
        }

        let max_transfers = 3usize;
        let mut scratch =
            ProfileScratch::new(p.stops.len(), flat_id_to_pattern_trip.len(), max_transfers);
        let hubs = HashSet::new();

        // Run Profile
        let targets = vec![1u32, 2u32];
        let results = run_trip_based_profile(
            &p,
            &transfers,
            &trip_transfer_ranges,
            0u32, // Start A
            &targets,
            &stop_to_patterns,
            &flat_id_to_pattern_trip,
            &pattern_trip_offset,
            max_transfers,
            &mut scratch,
            &hubs,
            false,
        );

        // Verify Results
        let res_map: HashMap<u32, Vec<(u32, u32)>> = results.into_iter().collect();
        println!("Result Map: {:?}", res_map);

        // Target B (1)
        // Should have (28800, 30600) [8:00->8:30]
        // And (32400, 34200) [9:00->9:30]
        let b_res = res_map.get(&1).unwrap();
        assert!(b_res.contains(&(28800, 30600)));
        assert!(b_res.contains(&(32400, 34200)));

        // Target C (2)
        // Should have path A->B->C
        // 8:00 -> 8:30. Transfer. 8:35 -> 9:05 (32700).
        // (28800, 32700).
        // 9:00 -> 9:30. No connection to C (Trip 3 leaves 8:35).
        // So only one pareto point.
        let c_res = res_map.get(&2).unwrap();
        assert!(c_res.contains(&(28800, 32700)));
        assert_eq!(c_res.len(), 1);
    }
}
