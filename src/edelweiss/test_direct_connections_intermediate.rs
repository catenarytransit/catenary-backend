#[cfg(test)]
mod tests {
    use crate::graph_loader::GraphManager;
    use crate::router::Router;
    use catenary::routing_common::api::{RoutingRequest, TravelMode};
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, TimeDeltaSequence, TransitPartition, TransitStop,
        TripPattern,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_intermediate_stop_routing() {
        // Setup:
        // P0: Stop A (start)
        // P1: Stop B (intermediate), Stop C (end)
        // DirectConnection Pattern: A -> B -> C
        // Request: A -> B
        // Bug: build_direct only connects A -> C, so A -> B fails.

        // Stops
        fn create_stop(id: u64, station_id: &str, lat: f64, is_border: bool) -> TransitStop {
            TransitStop {
                id,
                chateau_idx: 0,
                station_id: station_id.to_string(),
                gtfs_stop_ids: vec![station_id.to_string()],
                is_hub: is_border,
                is_border,
                is_external_gateway: false,
                is_long_distance: true, // Mark as LD
                lat,
                lon: 0.0,
            }
        }

        let stop_a = create_stop(0, "A", 0.0, false);
        let stop_b = create_stop(0, "B", 1.0, true); // Border in P1
        let stop_c = create_stop(1, "C", 2.0, false);

        // Partition 0
        let partition0 = TransitPartition {
            partition_id: 0,
            stops: vec![stop_a.clone()],
            trip_patterns: vec![],
            time_deltas: vec![],
            direction_patterns: vec![],
            internal_transfers: vec![],
            osm_links: vec![],
            service_ids: vec![],
            service_exceptions: vec![],
            _deprecated_external_transfers: vec![],
            local_dag: HashMap::new(),
            long_distance_trip_patterns: vec![],
            timezones: vec!["UTC".to_string()],
            boundary: None,
            chateau_ids: vec!["test".to_string()],
            external_hubs: vec![],
            long_distance_transfer_patterns: vec![],
            direct_connections_index: HashMap::new(),
        };

        // Partition 1
        let partition1 = TransitPartition {
            partition_id: 1,
            stops: vec![stop_b.clone(), stop_c.clone()],
            trip_patterns: vec![],
            time_deltas: vec![],
            direction_patterns: vec![],
            internal_transfers: vec![],
            osm_links: vec![],
            service_ids: vec![],
            service_exceptions: vec![],
            _deprecated_external_transfers: vec![],
            local_dag: HashMap::new(),
            long_distance_trip_patterns: vec![],
            timezones: vec!["UTC".to_string()],
            boundary: None,
            chateau_ids: vec!["test".to_string()],
            external_hubs: vec![],
            long_distance_transfer_patterns: vec![],
            direct_connections_index: HashMap::new(),
        };

        let mut graph = GraphManager::new();
        graph
            .transit_partitions
            .write()
            .unwrap()
            .insert(0, Arc::new(partition0));
        graph
            .transit_partitions
            .write()
            .unwrap()
            .insert(1, Arc::new(partition1));

        // Direct Connections: A -> B -> C
        let mut dc = catenary::routing_common::transit_graph::DirectConnections {
            stops: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            trip_patterns: vec![TripPattern {
                chateau_idx: 0,
                route_id: "DirectRoute".to_string(),
                direction_pattern_idx: 0,
                trips: vec![CompressedTrip {
                    gtfs_trip_id: "T1".to_string(),
                    service_mask: 127,
                    start_time: 3600, // 1:00 AM
                    time_delta_idx: 0,
                    service_idx: 0,
                    bikes_allowed: 0,
                    wheelchair_accessible: 0,
                }],
                timezone_idx: 0,
                route_type: 3,
                is_border: false,
            }],
            time_deltas: vec![TimeDeltaSequence {
                // travel A->B (1800), dwell B (60), travel B->C (1800), dwell C (0)
                deltas: vec![0, 0, 1800, 60, 1800, 0],
            }],
            service_ids: vec!["daily".to_string()],
            service_exceptions: vec![],
            timezones: vec!["UTC".to_string()],
            direction_patterns: vec![DirectionPattern {
                stop_indices: vec![0, 1, 2], // A, B, C
            }],
            index: HashMap::new(),
        };

        // Populate index for A
        use catenary::routing_common::transit_graph::DirectionPatternReference;
        dc.index.insert(
            "A".to_string(),
            vec![DirectionPatternReference {
                pattern_idx: 0,
                stop_idx: 0,
            }],
        );

        graph.direct_connections = Some(dc);

        let router = Router::new(&graph);

        // Request A -> B
        let req = RoutingRequest {
            start_lat: 0.0,
            start_lon: 0.0,
            end_lat: 1.0, // Location of B
            end_lon: 0.0,
            mode: TravelMode::Transit,
            time: 3000,
            speed_mps: 1.0,
            is_departure_time: true,
            wheelchair_accessible: false,
        };

        let result = router.route(&req);

        assert!(
            !result.itineraries.is_empty(),
            "Result should not be empty for intermediate stop B"
        );
        let duration = result.itineraries[0].duration_seconds;
        // Start 3000. Trip Start 3600. Arrive B: 3600 + 1800 = 5400.
        // Duration: 5400 - 3000 = 2400.
        assert_eq!(duration, 2400);
    }
}
