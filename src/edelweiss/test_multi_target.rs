use crate::graph_loader::GraphManager;
use crate::router::Router;
use catenary::routing_common::api::{RoutingRequest, TravelMode};
use catenary::routing_common::transit_graph::{
    DagEdge, DagEdgeList, EdgeType, GlobalHub, GlobalPatternIndex, PartitionDag, TransitPartition,
    TransitStop, WalkEdge,
};
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_multi_partition_target() {
    // Partition P0: Start Stop A (0).
    // Partition P1: Target Stop B (0).
    // Partition P2: Target Stop C (0).
    // Global DAG: A -> B (1000s), A -> C (2000s).
    // Both B and C are at the "target" location (approx).

    let stops_p0 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "A".to_string(),
        gtfs_stop_ids: vec!["A".to_string()],
        is_hub: true, // Hub for global dag
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 0.0,
        lon: 0.0,
    }];

    let stops_p1 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "B".to_string(),
        gtfs_stop_ids: vec!["B".to_string()],
        is_hub: true,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 0.02,
        lon: 0.0,
    }];

    let stops_p2 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "C".to_string(),
        gtfs_stop_ids: vec!["C".to_string()],
        is_hub: true,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 0.0201, // Very close to B
        lon: 0.0,
    }];

    let partition0 = TransitPartition {
        partition_id: 0,
        stops: stops_p0,
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

    let partition1 = TransitPartition {
        partition_id: 1,
        stops: stops_p1,
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

    let partition2 = TransitPartition {
        partition_id: 2,
        stops: stops_p2,
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
    graph
        .transit_partitions
        .write()
        .unwrap()
        .insert(2, Arc::new(partition2));

    // Global Index
    let global_index = GlobalPatternIndex {
        partition_dags: vec![
            PartitionDag {
                from_partition: 0,
                to_partition: 1,
                hubs: vec![
                    GlobalHub {
                        original_partition_id: 0,
                        stop_idx_in_partition: 0,
                    }, // A
                    GlobalHub {
                        original_partition_id: 1,
                        stop_idx_in_partition: 0,
                    }, // B
                ],
                edges: vec![DagEdge {
                    from_node_idx: 0,
                    to_node_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 1000,
                    })),
                }],
            },
            PartitionDag {
                from_partition: 0,
                to_partition: 2,
                hubs: vec![
                    GlobalHub {
                        original_partition_id: 0,
                        stop_idx_in_partition: 0,
                    }, // A
                    GlobalHub {
                        original_partition_id: 2,
                        stop_idx_in_partition: 0,
                    }, // C
                ],
                edges: vec![DagEdge {
                    from_node_idx: 0,
                    to_node_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 2000,
                    })),
                }],
            },
        ],
        long_distance_dags: vec![],
    };
    graph.global_index = Some(global_index);

    let router = Router::new(&graph);

    let req = RoutingRequest {
        start_lat: 0.0,
        start_lon: 0.0,
        end_lat: 0.02,
        end_lon: 0.0,
        mode: TravelMode::Transit,
        time: 0,
        speed_mps: 1.0,
        is_departure_time: true,
        wheelchair_accessible: false,
    };

    // The router should find stops in both P1 and P2 (since they are close to 0.02, 0.0).
    // It should route to both.
    // The path to B (P1) is 1000s.
    // The path to C (P2) is 2000s.
    // It should return itineraries. Ideally sorted by time?
    // Dijkstra returns all reached end nodes.

    let result = router.route(&req);

    println!("Found {} itineraries", result.itineraries.len());
    for itin in &result.itineraries {
        println!("Itinerary duration: {}", itin.duration_seconds);
    }

    assert!(!result.itineraries.is_empty());

    // We expect the BEST itinerary (to B, 1000s)
    let found_b = result
        .itineraries
        .iter()
        .any(|i| i.duration_seconds == 1000);
    assert!(found_b, "Should find best path to B with duration 1000s");

    // We do NOT necessarily expect the suboptimal path to C (2000s) to be returned,
    // as the router now optimizes across all partitions and returns the best result.
    // This is an improvement over the previous behaviour which might have returned multiple
    // potentially redundant or suboptimal paths, or missed the global optimum if partitions were processed sequentially.
}

#[test]
fn test_multi_partition_target_optimization() {
    // Same setup, but make C (P2) closer than B (P1).
    // Global DAG: A -> B (2000s), A -> C (500s).

    let stops_p0 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "A".to_string(),
        gtfs_stop_ids: vec!["A".to_string()],
        is_hub: true,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 0.0,
        lon: 0.0,
    }];

    let stops_p1 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "B".to_string(),
        gtfs_stop_ids: vec!["B".to_string()],
        is_hub: true,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 0.02,
        lon: 0.0,
    }];

    let stops_p2 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "C".to_string(),
        gtfs_stop_ids: vec!["C".to_string()],
        is_hub: true,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 0.0201,
        lon: 0.0,
    }];

    let partition0 = TransitPartition {
        partition_id: 0,
        stops: stops_p0,
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

    let partition1 = TransitPartition {
        partition_id: 1,
        stops: stops_p1,
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

    let partition2 = TransitPartition {
        partition_id: 2,
        stops: stops_p2,
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
    graph
        .transit_partitions
        .write()
        .unwrap()
        .insert(2, Arc::new(partition2));

    // Global Index
    let global_index = GlobalPatternIndex {
        partition_dags: vec![
            PartitionDag {
                from_partition: 0,
                to_partition: 1,
                hubs: vec![
                    GlobalHub {
                        original_partition_id: 0,
                        stop_idx_in_partition: 0,
                    }, // A
                    GlobalHub {
                        original_partition_id: 1,
                        stop_idx_in_partition: 0,
                    }, // B
                ],
                edges: vec![DagEdge {
                    from_node_idx: 0,
                    to_node_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 2000,
                    })),
                }],
            },
            PartitionDag {
                from_partition: 0,
                to_partition: 2,
                hubs: vec![
                    GlobalHub {
                        original_partition_id: 0,
                        stop_idx_in_partition: 0,
                    }, // A
                    GlobalHub {
                        original_partition_id: 2,
                        stop_idx_in_partition: 0,
                    }, // C
                ],
                edges: vec![DagEdge {
                    from_node_idx: 0,
                    to_node_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 500,
                    })),
                }],
            },
        ],
        long_distance_dags: vec![],
    };
    graph.global_index = Some(global_index);

    let router = Router::new(&graph);

    let req = RoutingRequest {
        start_lat: 0.0,
        start_lon: 0.0,
        end_lat: 0.02,
        end_lon: 0.0,
        mode: TravelMode::Transit,
        time: 0,
        speed_mps: 1.0,
        is_departure_time: true,
        wheelchair_accessible: false,
    };

    let result = router.route(&req);

    assert!(!result.itineraries.is_empty());

    // We expect the BEST itinerary (to C, ~500s + walk)
    // Walk from C (0.0201) to Target (0.02) is ~11m = 11s. Total ~511s.
    // Path to B is 2000s.
    let found_c = result.itineraries.iter().any(|i| i.duration_seconds < 1000);
    assert!(
        found_c,
        "Should find best path to C (approx 511s), definitely better than B (2000s)"
    );
}
