use super::osm_graph::*;
use super::transit_graph::*;
use prost::Message;

#[test]
fn test_transit_partition_serialization() {
    let partition = TransitPartition {
        partition_id: 1,
        stops: vec![TransitStop {
            id: 0,
            chateau: "test_chateau".to_string(),
            gtfs_original_id: "stop_1".to_string(),
            is_hub: true,
            lat: 34.0,
            lon: -118.0,
        }],
        trip_patterns: vec![],
        time_deltas: vec![],
        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec!["c_12345".to_string(), "c_67890".to_string()],
        service_exceptions: vec![ServiceException {
            service_idx: 0,
            added_dates: vec![20231225],
            removed_dates: vec![20230101],
        }],
    };

    let encoded = partition.encode_to_vec();
    let decoded = TransitPartition::decode(&encoded[..]).expect("Failed to decode");

    assert_eq!(decoded.partition_id, 1);
    assert_eq!(decoded.service_ids.len(), 2);
    assert_eq!(decoded.service_ids[0], "c_12345");
    assert_eq!(decoded.service_exceptions.len(), 1);
    assert_eq!(decoded.service_exceptions[0].added_dates[0], 20231225);
}

#[test]
fn test_osm_partition_serialization() {
    let street_data = StreetData {
        nodes: vec![Node {
            lat: 34.0,
            lon: -118.0,
            elevation: 100.0,
            first_edge_idx: 0,
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
    let decoded = StreetData::decode(&encoded[..]).expect("Failed to decode");

    assert_eq!(decoded.partition_id, 42);
    assert_eq!(decoded.boundary_nodes.len(), 1);
    assert_eq!(decoded.boundary_nodes[0].target_partition_id, 43);
}
