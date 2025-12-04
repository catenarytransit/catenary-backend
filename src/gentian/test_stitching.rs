#[cfg(test)]
mod tests {
    use crate::connectivity::compute_global_patterns;
    use ahash::AHashMap as HashMap;
    use catenary::routing_common::osm_graph::{load_pbf, save_pbf};
    use catenary::routing_common::transit_graph::{
        GlobalPatternIndex, GlobalTimetable, PartitionDag, PartitionTimetable, TransitPartition,
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
            local_transfer_patterns: vec![],
            long_distance_trip_patterns: vec![],
            timezones: vec![],
            boundary: None,
            chateau_ids: vec![],
        }
    }

    #[test]
    fn test_stitching_logic() {
        let output_dir = std::env::temp_dir().join("gentian_test_stitching");
        let _ = std::fs::remove_dir_all(&output_dir); // Clean start
        std::fs::create_dir_all(&output_dir).unwrap();

        // 1. Create initial GlobalPatternIndex
        // P0 -> P1
        let dag_0_1 = PartitionDag {
            from_partition: 0,
            to_partition: 1,
            hubs: vec![],
            edges: vec![],
        };
        // P2 -> P3
        let dag_2_3 = PartitionDag {
            from_partition: 2,
            to_partition: 3,
            hubs: vec![],
            edges: vec![],
        };

        let initial_index = GlobalPatternIndex {
            partition_dags: vec![dag_0_1, dag_2_3],
            long_distance_dags: vec![],
        };

        let path = output_dir.join("global_patterns.pbf");
        save_pbf(&initial_index, path.to_str().unwrap()).unwrap();

        // 2. Create initial GlobalTimetable
        let tt_0 = PartitionTimetable {
            partition_id: 0,
            pattern_timetables: vec![],
            time_deltas: vec![],
            timezones: vec![],
        };
        let tt_2 = PartitionTimetable {
            partition_id: 2,
            pattern_timetables: vec![],
            time_deltas: vec![],
            timezones: vec![],
        };
        let initial_tt = GlobalTimetable {
            partition_timetables: vec![tt_0, tt_2],
        };
        let path_tt = output_dir.join("global_timetable.pbf");
        save_pbf(&initial_tt, path_tt.to_str().unwrap()).unwrap();

        // 3. Run compute_global_patterns for Partition 0 ONLY
        // This should wipe P0 data and keep P2 data.
        let mut loaded_partitions = HashMap::new();
        loaded_partitions.insert(0, create_mock_partition(0));

        let border_nodes = HashMap::new(); // No border nodes -> No new patterns generated
        let cross_edges = vec![];
        let global_to_partition_map = HashMap::new();

        compute_global_patterns(
            &border_nodes,
            &cross_edges,
            &[],
            &global_to_partition_map,
            &loaded_partitions,
            &output_dir,
        );

        // 4. Verify GlobalPatternIndex
        let new_index: GlobalPatternIndex = load_pbf(path.to_str().unwrap()).unwrap();

        // Should NOT contain P0 -> P1
        assert!(
            !new_index
                .partition_dags
                .iter()
                .any(|d| d.from_partition == 0),
            "P0 data should be removed"
        );
        // Should contain P2 -> P3
        assert!(
            new_index
                .partition_dags
                .iter()
                .any(|d| d.from_partition == 2 && d.to_partition == 3),
            "P2 data should be preserved"
        );

        // 5. Verify GlobalTimetable
        let new_tt: GlobalTimetable = load_pbf(path_tt.to_str().unwrap()).unwrap();

        // Should NOT contain P0
        assert!(
            !new_tt
                .partition_timetables
                .iter()
                .any(|pt| pt.partition_id == 0),
            "P0 timetable should be removed"
        );
        // Should contain P2
        assert!(
            new_tt
                .partition_timetables
                .iter()
                .any(|pt| pt.partition_id == 2),
            "P2 timetable should be preserved"
        );
    }
}
