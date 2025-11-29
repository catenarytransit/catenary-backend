
    #[test]
    fn test_hub_identification() {
        // Mock Stops
        let mut stops = Vec::new();
        for i in 0..10 {
            stops.push(Stop {
                id: i.to_string(),
                gtfs_id: i.to_string(),
                chateau: "test".to_string(),
                name: Some(format!("Stop {}", i)),
                point: Some(postgis::ewkb::Point { x: i as f64 * 0.01, y: 0.0, srid: Some(4326) }),
                ..Default::default()
            });
        }

        // Mock Patterns
        // Pattern 1: 0 -> 1 -> 2 -> 3 -> 4 (Line A)
        // Pattern 2: 5 -> 6 -> 2 -> 7 -> 8 (Line B) - Intersects at 2
        // Pattern 3: 9 -> 2 (Feeder) - Intersects at 2
        
        // We expect Node 2 to be a hub because it connects multiple lines.
        
        let p1 = ProcessedPattern {
            route_id: "A".to_string(),
            stop_indices: vec![0, 1, 2, 3, 4],
            trips: vec![CompressedTrip {
                gtfs_trip_id: "t1".to_string(),
                service_mask: 127,
                start_time: 28800, // 8:00 AM
                delta_pointer: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
        };
        
        let p2 = ProcessedPattern {
            route_id: "B".to_string(),
            stop_indices: vec![5, 6, 2, 7, 8],
            trips: vec![CompressedTrip {
                gtfs_trip_id: "t2".to_string(),
                service_mask: 127,
                start_time: 29100, // 8:05 AM
                delta_pointer: 5, // Offset
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            }],
        };
        
        let patterns = vec![p1, p2];
        
        // Time Deltas (All 0 for simplicity, instant travel)
        let time_deltas = vec![0; 20];
        
        let calendar = vec![];
        
        // Run with sample_size = 100
        let hubs = identify_hubs_time_dependent(&stops, &patterns, &time_deltas, &calendar, 100, 1);
        
        // We expect 2 to be the hub
        assert!(hubs.contains(&2), "Node 2 should be identified as a hub");
    }
