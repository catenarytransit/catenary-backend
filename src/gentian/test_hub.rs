#[cfg(test)]
mod tests {
    use super::*;
    use crate::identify_hubs_time_dependent;
    use crate::utils::ProcessedPattern;
    use catenary::models::{Calendar, Stop};
    use catenary::routing_common::transit_graph::{CompressedTrip, TimeDeltaSequence};
    use postgis_diesel::types::{Point, PointZ};
    use std::collections::HashSet;

    fn create_mock_stop(i: usize) -> Stop {
        Stop {
            onestop_feed_id: "feed".to_string(),
            attempt_id: "attempt".to_string(),
            gtfs_id: i.to_string(),
            name: Some(format!("Stop {}", i)),
            name_translations: None,
            displayname: None,
            code: None,
            gtfs_desc: None,
            gtfs_desc_translations: None,
            location_type: 0,
            parent_station: None,
            zone_id: None,
            url: None,
            point: Some(Point {
                x: i as f64 * 0.01,
                y: 0.0,
                srid: Some(4326),
            }),
            timezone: None,
            wheelchair_boarding: 0,
            primary_route_type: None,
            level_id: None,
            platform_code: None,
            platform_code_translations: None,
            routes: vec![],
            route_types: vec![],
            children_ids: vec![],
            children_route_types: vec![],
            station_feature: false,
            hidden: false,
            chateau: "test".to_string(),
            location_alias: None,
            tts_name_translations: None,
            tts_name: None,
            allowed_spatial_query: true,
        }
    }

    #[test]
    fn test_hub_identification() {
        // Mock Stops
        let mut stops = Vec::new();
        for i in 0..10 {
            stops.push(create_mock_stop(i));
        }

        // Mock Patterns
        // Pattern 1: 0 -> 1 -> 2 -> 3 -> 4 (Line A)
        // Pattern 2: 5 -> 6 -> 2 -> 7 -> 8 (Line B) - Intersects at 2
        // Pattern 3: 9 -> 2 (Feeder) - Intersects at 2

        // We expect Node 2 to be a hub because it connects multiple lines.

        let mut p1_trips = Vec::new();
        let mut p2_trips = Vec::new();
        for i in 0..20 {
            let start_time = 25200 + i * 900; // Every 15 mins from 7:00
            p1_trips.push(CompressedTrip {
                gtfs_trip_id: format!("t1_{}", i),
                service_mask: 127,
                start_time,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            });
            p2_trips.push(CompressedTrip {
                gtfs_trip_id: format!("t2_{}", i),
                service_mask: 127,
                start_time: start_time + 300, // 5 mins later
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            });
        }

        let p1 = ProcessedPattern {
            route_id: "A".to_string(),
            stop_indices: vec![0, 1, 2, 3, 4],
            trips: p1_trips,
            chateau: "test".to_string(),
            timezone_idx: 0,
        };

        let p2 = ProcessedPattern {
            route_id: "B".to_string(),
            stop_indices: vec![5, 6, 2, 7, 8],
            trips: p2_trips,
            chateau: "test".to_string(),
            timezone_idx: 0,
        };

        let patterns = vec![p1, p2];

        // Time Deltas (All 0 for simplicity, instant travel)
        let time_deltas = vec![TimeDeltaSequence {
            deltas: vec![0; 20],
        }];

        let calendar = vec![];

        // Run with sample_size = 100
        let hubs = identify_hubs_time_dependent(&stops, &patterns, &time_deltas, &calendar, 100, 1);

        // We expect 2 to be the hub
        assert!(hubs.contains(&2), "Node 2 should be identified as a hub");
    }
}
