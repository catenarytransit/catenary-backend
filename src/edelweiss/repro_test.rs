use crate::graph_loader::GraphManager;
use crate::router::Router;
use catenary::routing_common::api::{RoutingRequest, TravelMode};
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, DirectionPattern, EdgeType, LocalTransferPattern, TimeDeltaSequence,
    TransitEdge, TransitPartition, TransitStop, TripPattern,
};
use std::collections::HashMap;

#[test]
fn test_multi_partition_selection() {
    // Create two partitions:
    // Partition 0: Has a stop near start, but NO path to destination.
    // Partition 1: Has a stop slightly further from start, but HAS path to destination.

    // Partition 0
    let stops_p0 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "S0_P0".to_string(),
        gtfs_stop_ids: vec!["S0_P0".to_string()],
        is_hub: false,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: -0.01,
        lon: 0.0, // Further from start/end
    }];
    let partition0 = TransitPartition {
        partition_id: 0,
        stops: stops_p0,
        trip_patterns: vec![], // No trips
        time_deltas: vec![],
        direction_patterns: vec![],

        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec![],
        service_exceptions: vec![],
        _deprecated_external_transfers: vec![],
        local_dag: HashMap::new(),
        long_distance_trip_patterns: vec![],
        timezones: vec![],
        boundary: None,
        chateau_ids: vec![],
        external_hubs: vec![],
        long_distance_transfer_patterns: vec![],
        direct_connections_index: HashMap::new(),
    };

    // Partition 1
    let stops_p1 = vec![
        TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "S0_P1".to_string(),
            gtfs_stop_ids: vec!["S0_P1".to_string()],
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.001, // Slightly further (approx 111m)
            lon: 0.0,
        },
        TransitStop {
            id: 1,
            chateau_idx: 0,
            station_id: "S1_P1".to_string(),
            gtfs_stop_ids: vec!["S1_P1".to_string()],
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.01, // Destination
            lon: 0.0,
        },
    ];

    // Create a trip in Partition 1 from S0_P1 to S1_P1
    let direction_patterns_p1 = vec![DirectionPattern {
        stop_indices: vec![0, 1],
    }];
    let time_deltas_p1 = vec![0, 600]; // 10 min travel
    let trips_p1 = vec![CompressedTrip {
        gtfs_trip_id: "T1_P1".to_string(),
        service_mask: 127,
        start_time: 28800, // 8:00
        time_delta_idx: 0,
        service_idx: 0,
        bikes_allowed: 0,
        wheelchair_accessible: 0,
    }];
    let trip_patterns_p1 = vec![TripPattern {
        chateau_idx: 0,
        route_id: "R1_P1".to_string(),
        direction_pattern_idx: 0,
        trips: trips_p1,
        timezone_idx: 0,
        route_type: 3,
        is_border: false,
    }];

    let partition1 = TransitPartition {
        partition_id: 1,
        stops: stops_p1,
        trip_patterns: trip_patterns_p1,
        time_deltas: vec![TimeDeltaSequence {
            deltas: vec![0, 0, 600, 0],
        }],
        direction_patterns: direction_patterns_p1,
        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec!["daily".to_string()],
        service_exceptions: vec![],
        _deprecated_external_transfers: vec![],
        local_dag: {
            let mut map = HashMap::new();
            map.insert(
                0,
                catenary::routing_common::transit_graph::DagEdgeList {
                    edges: vec![DagEdge {
                        from_node_idx: 0,
                        to_node_idx: 1,
                        edge_type: Some(EdgeType::Transit(TransitEdge {
                            trip_pattern_idx: 0,
                            start_stop_idx: 0,
                            end_stop_idx: 1,
                            min_duration: 0,
                        })),
                    }],
                },
            );
            map
        },
        long_distance_trip_patterns: vec![],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["p1".to_string()],
        external_hubs: vec![],
        long_distance_transfer_patterns: vec![],
        direct_connections_index: HashMap::new(),
    };

    let mut graph = GraphManager::new();
    graph
        .transit_partitions
        .write()
        .unwrap()
        .insert(0, std::sync::Arc::new(partition0));
    graph
        .transit_partitions
        .write()
        .unwrap()
        .insert(1, std::sync::Arc::new(partition1));
    let router = Router::new(&graph);

    // Request: (0,0) to (0.01, 0)
    // Partition 0 stop is at (0,0) -> dist 0
    // Partition 1 stop is at (0.001, 0) -> dist ~111m
    // Current logic picks first available partition from sorted stops.
    // Since P0 stop is closer, it might pick P0 and fail because P0 has no path.

    let req = RoutingRequest {
        start_lat: 0.0,
        start_lon: 0.0,
        end_lat: 0.01,
        end_lon: 0.0,
        mode: TravelMode::Transit,
        time: 1704095880, // 7:58 (2 min wait)
        speed_mps: 1.0,
        is_departure_time: true,
        wheelchair_accessible: false,
    };

    let result = router.route(&req);
    if !result.itineraries.is_empty() {
        println!("Itinerary legs:");
        for (i, leg) in result.itineraries[0].legs.iter().enumerate() {
            println!(
                "Leg {}: Mode={:?}, Start={:?}, End={:?}",
                i,
                leg.mode(),
                leg.start_stop_id(),
                leg.end_stop_id()
            );
        }
    }

    assert!(
        !result.itineraries.is_empty(),
        "Should find itinerary via Partition 1"
    );
    assert_eq!(
        result.itineraries[0].legs.len(),
        2,
        "Should have Walk + Transit leg"
    );
    assert_eq!(result.itineraries[0].legs[0].mode(), TravelMode::Walk);
    assert_eq!(result.itineraries[0].legs[1].mode(), TravelMode::Transit);
    // Note: Walk legs might be 0 duration if start/end matches stop exactly, but structure remains.
}
