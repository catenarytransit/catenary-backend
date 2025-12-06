use crate::graph_loader::GraphManager;
use crate::router::Router;
use catenary::routing_common::api::{RoutingRequest, TravelMode};
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, DagEdgeList, DirectionPattern, EdgeType, GlobalHub,
    GlobalPatternIndex, LocalTransferPattern, PartitionDag, TimeDeltaSequence, TransitEdge,
    TransitPartition, TransitStop, TripPattern, WalkEdge,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

#[test]
fn test_hub_mediated_routing() {
    // Partition P0: A (0) -> H (1) -> B (2). H is Hub.
    // Patterns:
    // A -> H (Local)
    // H -> B (Local)
    // A -> B (Direct) is PRUNED/MISSING.
    // We verify that router can find A -> H -> B.

    let stops_p0 = vec![
        TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "A".to_string(),
            gtfs_stop_ids: vec!["A".to_string()],
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
            station_id: "H".to_string(),
            gtfs_stop_ids: vec!["H".to_string()],
            is_hub: true, // HUB
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.01,
            lon: 0.0,
        },
        TransitStop {
            id: 2,
            chateau_idx: 0,
            station_id: "B".to_string(),
            gtfs_stop_ids: vec!["B".to_string()],
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.02,
            lon: 0.0,
        },
    ];

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
        local_dag: HashMap::from([
            (
                0,
                DagEdgeList {
                    edges: vec![DagEdge {
                        from_node_idx: 0,
                        to_node_idx: 1,
                        edge_type: Some(EdgeType::Walk(WalkEdge {
                            duration_seconds: 600,
                        })),
                    }],
                },
            ),
            (
                1,
                DagEdgeList {
                    edges: vec![DagEdge {
                        from_node_idx: 1,
                        to_node_idx: 2,
                        edge_type: Some(EdgeType::Walk(WalkEdge {
                            duration_seconds: 600,
                        })),
                    }],
                },
            ),
        ]),
        long_distance_trip_patterns: vec![],
        timezones: vec![],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
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
    assert!(
        !result.itineraries.is_empty(),
        "Should find itinerary via Hub H"
    );
    // Path should be Walk(A->H) + Walk(H->B) = 1200s
    // Plus initial/final walks (0s here as coords match)
    // Actually, legs will be: Walk(Start->A), Walk(A->H), Walk(H->B), Walk(B->End)
    // Or merged?
    // Let's just check duration.
    let duration = result.itineraries[0].duration_seconds;
    assert!(duration >= 1200, "Duration should be at least 1200s");
}

#[test]
fn test_long_distance_jump() {
    // Partition P0: A (0). Has Long-Distance Pattern to External Hub B (in P1).
    // Partition P1: B (0).
    // Path: A -> B (via Jump).

    let stops_p0 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "A".to_string(),
        gtfs_stop_ids: vec!["A".to_string()],
        is_hub: false,
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false, // A itself is not marked LD, but has LD pattern? Usually LD patterns are from LD stations.
        // Let's mark A as LD to be safe with our logic (we load patterns for LD stations).
        lat: 0.0,
        lon: 0.0,
    }];

    let stops_p1 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        station_id: "B".to_string(),
        gtfs_stop_ids: vec!["B".to_string()],
        is_hub: true, // B is a hub (external view)
        is_border: false,
        is_external_gateway: false,
        is_long_distance: false,
        lat: 10.0,
        lon: 0.0,
    }];

    let partition0 = TransitPartition {
        partition_id: 0,
        stops: stops_p0,
        trip_patterns: vec![],

        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec![],
        service_exceptions: vec![],
        _deprecated_external_transfers: vec![],
        local_dag: HashMap::new(),
        long_distance_trip_patterns: vec![TripPattern {
            chateau_idx: 0,
            route_id: "LD_R1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "LD_T1".to_string(),
                service_mask: 127,
                start_time: 0,
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
            deltas: vec![0, 0, 3600, 0],
        }],
        direction_patterns: vec![DirectionPattern {
            stop_indices: vec![0, 1], // 0=A, 1=ExternalHub(B)
        }],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
        external_hubs: vec![GlobalHub {
            original_partition_id: 1,
            stop_idx_in_partition: 0,
        }],
        long_distance_transfer_patterns: vec![LocalTransferPattern {
            from_stop_idx: 0,
            edges: vec![DagEdge {
                from_node_idx: 0,
                to_node_idx: 1, // Index into stops + external_hubs. Stops len=1. So 1 = external_hubs[0] = B.
                edge_type: Some(EdgeType::LongDistanceTransit(TransitEdge {
                    trip_pattern_idx: 0, // Dummy
                    start_stop_idx: 0,
                    end_stop_idx: 1,
                    min_duration: 3600,
                })),
            }],
        }],
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
        local_dag: HashMap::new(), // No local patterns needed in P1 for this test
        long_distance_trip_patterns: vec![],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
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

    // Create DirectConnections
    let mut dc = catenary::routing_common::transit_graph::DirectConnections {
        stops: vec!["A".to_string(), "B".to_string()],
        trip_patterns: vec![TripPattern {
            chateau_idx: 0,
            route_id: "LD_R1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "LD_T1".to_string(),
                service_mask: 127,
                start_time: 0,
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
            deltas: vec![0, 0, 3600, 0],
        }],
        service_ids: vec!["daily".to_string()],
        service_exceptions: vec![],
        timezones: vec!["UTC".to_string()],
        direction_patterns: vec![DirectionPattern {
            stop_indices: vec![0, 1], // 0=A, 1=B
        }],
        index: HashMap::new(),
    };

    // Populate index
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

    let req = RoutingRequest {
        start_lat: 0.0,
        start_lon: 0.0,
        end_lat: 10.0,
        end_lon: 0.0,
        mode: TravelMode::Transit,
        time: 0,
        speed_mps: 1.0,
        is_departure_time: true,
        wheelchair_accessible: false,
    };

    let result = router.route(&req);
    assert!(
        !result.itineraries.is_empty(),
        "Should find itinerary via Long Distance Jump"
    );
    let duration = result.itineraries[0].duration_seconds;
    assert!(
        duration >= 3600,
        "Duration should be at least 3600s (jump duration)"
    );
}

#[test]
fn test_cross_cluster_dag() {
    // Partition P0: A (0), Border1 (1).
    // Partition P1: Border2 (0), B (1).
    // Global DAG: Border1 -> Border2.
    // Local P0: A -> Border1.
    // Local P1: Border2 -> B.
    // Route A -> B.

    let stops_p0 = vec![
        TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "A".to_string(),
            gtfs_stop_ids: vec!["A".to_string()],
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
            station_id: "Border1".to_string(),
            gtfs_stop_ids: vec!["Border1".to_string()],
            is_hub: true, // Border/Hub
            is_border: true,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.01,
            lon: 0.0,
        },
    ];

    let stops_p1 = vec![
        TransitStop {
            id: 0,
            chateau_idx: 0,
            station_id: "Border2".to_string(),
            gtfs_stop_ids: vec!["Border2".to_string()],
            is_hub: true, // Border/Hub
            is_border: true,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.02,
            lon: 0.0,
        },
        TransitStop {
            id: 1,
            chateau_idx: 0,
            station_id: "B".to_string(),
            gtfs_stop_ids: vec!["B".to_string()],
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
            lat: 0.03,
            lon: 0.0,
        },
    ];

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
        local_dag: HashMap::from([(
            0,
            DagEdgeList {
                edges: vec![DagEdge {
                    from_node_idx: 0,
                    to_node_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 600,
                    })),
                }],
            },
        )]),
        long_distance_trip_patterns: vec![],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
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
        local_dag: HashMap::from([(
            0,
            DagEdgeList {
                edges: vec![DagEdge {
                    from_node_idx: 0,
                    to_node_idx: 1,
                    edge_type: Some(EdgeType::Walk(WalkEdge {
                        duration_seconds: 600,
                    })),
                }],
            },
        )]),
        long_distance_trip_patterns: vec![],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
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

    // Global Index
    let global_index = GlobalPatternIndex {
        partition_dags: vec![PartitionDag {
            from_partition: 0,
            to_partition: 1,
            hubs: vec![
                GlobalHub {
                    original_partition_id: 0,
                    stop_idx_in_partition: 1, // Border1
                },
                GlobalHub {
                    original_partition_id: 1,
                    stop_idx_in_partition: 0, // Border2
                },
            ],
            edges: vec![DagEdge {
                from_node_idx: 0, // Border1
                to_node_idx: 1,   // Border2
                edge_type: Some(EdgeType::Walk(WalkEdge {
                    duration_seconds: 1800,
                })),
            }],
        }],
        long_distance_dags: vec![],
    };
    graph.global_index = Some(global_index);

    let router = Router::new(&graph);

    let req = RoutingRequest {
        start_lat: 0.0,
        start_lon: 0.0,
        end_lat: 0.03,
        end_lon: 0.0,
        mode: TravelMode::Transit,
        time: 0,
        speed_mps: 1.0,
        is_departure_time: true,
        wheelchair_accessible: false,
    };

    let result = router.route(&req);
    assert!(
        !result.itineraries.is_empty(),
        "Should find itinerary via Global DAG"
    );
    let duration = result.itineraries[0].duration_seconds;
    // 600 (A->Border1) + 1800 (Border1->Border2) + 600 (Border2->B) = 3000
    assert!(duration >= 3000, "Duration should be at least 3000s");
}
