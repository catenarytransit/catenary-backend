use super::osm_graph::*;
use super::transit_graph::*;
use prost::Message;
use std::collections::HashMap;

#[test]
fn test_transit_partition_serialization() {
    let partition = TransitPartition {
        partition_id: 1,
        stops: vec![TransitStop {
            id: 0,
            lat: 34.0,
            lon: -118.0,
            chateau_idx: 0,
            station_id: "stop_1".to_string(),
            gtfs_stop_ids: vec!["stop_1".to_string()],
            is_hub: true,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
        }],
        trip_patterns: vec![TripPattern {
            chateau_idx: 0,
            route_id: "route_1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "trip_1".to_string(),
                service_mask: 1,
                start_time: 3600,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 1,
                wheelchair_accessible: 1,
            }],
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        }],
        time_deltas: vec![],
        direction_patterns: vec![DirectionPattern {
            stop_indices: vec![0],
        }],
        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec!["c_12345".to_string(), "c_67890".to_string()],
        service_exceptions: vec![ServiceException {
            service_idx: 0,
            added_dates: vec![20231225],
            removed_dates: vec![20230101],
        }],
        _deprecated_external_transfers: vec![],
        local_dag: HashMap::new(),
        long_distance_trip_patterns: vec![],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
        external_hubs: vec![],
        long_distance_transfer_patterns: vec![],
        direct_connections_index: HashMap::new(),
    };

    let config = bincode::config::standard();
    let encoded = bincode::serde::encode_to_vec(&partition, config).expect("Failed to encode");
    let (decoded, _): (TransitPartition, usize) =
        bincode::serde::decode_from_slice(&encoded, config).expect("Failed to decode");

    assert_eq!(decoded.partition_id, 1);
    assert_eq!(decoded.service_ids.len(), 2);
    assert_eq!(decoded.service_ids[0], "c_12345");
    assert_eq!(decoded.service_exceptions.len(), 1);
    assert_eq!(decoded.service_exceptions[0].added_dates[0], 20231225);
    assert_eq!(decoded.trip_patterns.len(), 1);
    assert_eq!(decoded.trip_patterns[0].trips[0].bikes_allowed, 1);
    assert_eq!(decoded.trip_patterns[0].trips[0].wheelchair_accessible, 1);
}

#[test]
fn test_osm_partition_serialization() {
    let street_data = StreetData {
        nodes: vec![Node {
            lat: 34.0,
            lon: -118.0,
            elevation: 100.0,
            first_edge_idx: 0,
            flags: 0,
        }],
        edges: vec![],
        geometries: vec![],
        partition_id: 42,
        boundary_nodes: vec![BoundaryNode {
            local_node_idx: 0,
            target_partition_id: 43,
            target_node_idx: 10,
        }],
    };

    let encoded = street_data.encode_to_vec();
    let decoded: StreetData = StreetData::decode(&*encoded).expect("Failed to decode");

    assert_eq!(decoded.partition_id, 42);
    assert_eq!(decoded.boundary_nodes.len(), 1);
    assert_eq!(decoded.boundary_nodes[0].target_partition_id, 43);
}
