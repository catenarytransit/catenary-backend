#[cfg(test)]
mod tests {
    use crate::connectivity::compute_global_patterns;
    use ahash::AHashMap as HashMap;
    use catenary::routing_common::transit_graph::{
        GlobalPatternIndex, GlobalTimetable, PartitionDag, PartitionTimetable, TransitPartition,
        load_bincode, save_bincode,
    };
    use std::path::PathBuf;

    fn create_mock_partition(id: u32) -> TransitPartition {
        TransitPartition {
            partition_id: id,
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
    fn test_partition_io() {
        let output_dir = std::env::temp_dir().join("gentian_test_io");
        let _ = std::fs::remove_dir_all(&output_dir);
        std::fs::create_dir_all(&output_dir).unwrap();

        let mut partition = create_mock_partition(0);

        // Add some data to new fields
        partition
            .external_hubs
            .push(catenary::routing_common::transit_graph::GlobalHub {
                original_partition_id: 1,
                stop_idx_in_partition: 5,
            });

        let path = output_dir.join("transit_chunk_0.bincode");
        save_bincode(&partition, path.to_str().unwrap()).unwrap();

        let loaded: TransitPartition = load_bincode(path.to_str().unwrap()).unwrap();

        assert_eq!(loaded.partition_id, 0);
        assert_eq!(loaded.external_hubs.len(), 1);
        assert_eq!(loaded.external_hubs[0].original_partition_id, 1);
        assert_eq!(loaded.external_hubs[0].stop_idx_in_partition, 5);
    }
}
