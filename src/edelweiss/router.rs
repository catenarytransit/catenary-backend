use crate::graph_loader::GraphManager;
use crate::osm_router::OsmRouter;
use crate::query_graph::{QueryGraph, ServiceContext};
use catenary::routing_common::api::{Itinerary, RoutingRequest, RoutingResult, TravelMode};
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, DirectionPattern, EdgeType, LocalTransferPattern, TransitEdge,
    TransitPartition, TripPattern,
};
use chrono::{Datelike, TimeZone, Timelike};
use chrono_tz::Tz;
use std::collections::{HashMap, HashSet};

pub struct Router<'a> {
    graph: &'a GraphManager,
}

impl<'a> Router<'a> {
    pub fn new(graph: &'a GraphManager) -> Self {
        Self { graph }
    }

    pub fn route(&self, req: &RoutingRequest) -> RoutingResult {
        // 1. Find access/egress stops
        let start_stops = self.find_nearby_stops(
            req.start_lat,
            req.start_lon,
            req.mode.clone(),
            req.speed_mps,
        );
        let end_stops =
            self.find_nearby_stops(req.end_lat, req.end_lon, req.mode.clone(), req.speed_mps);

        println!(
            "Found {} start stops and {} end stops",
            start_stops.len(),
            end_stops.len()
        );

        // Group start stops by partition
        let mut start_stops_by_partition: HashMap<u32, Vec<(u32, u32, u32, Vec<(f64, f64)>)>> =
            HashMap::new();
        for stop in start_stops {
            start_stops_by_partition
                .entry(stop.0)
                .or_default()
                .push(stop);
        }

        let end_pids: std::collections::HashSet<_> =
            end_stops.iter().map(|(p, _, _, _)| *p).collect();
        let mut itineraries = Vec::new();
        let mut tried_partitions = std::collections::HashSet::new();

        for (start_pid, partition_start_stops) in &start_stops_by_partition {
            if tried_partitions.contains(start_pid) {
                continue;
            }
            tried_partitions.insert(*start_pid);

            if let Some(start_partition) = self.graph.get_transit_partition(*start_pid) {
                // Determine relevant end partitions
                // For now, we try to route to ALL end partitions found.
                for &end_pid in &end_pids {
                    // Step 1: Cluster Analysis
                    // Case A: Same Cluster (and Convex - currently assumed Non-Convex/Global for safety)
                    // Case B: Different Cluster or Non-Convex -> Global Query

                    // We treat everything as Case B (Global/Non-Convex) for now as we lack is_convex flag.
                    // Even for intra-partition (start_pid == end_pid), we build the graph using LTPs.

                    let mut query_graph = QueryGraph::new();
                    let mut involved_partitions = HashSet::new();
                    involved_partitions.insert(*start_pid);
                    involved_partitions.insert(end_pid);

                    // Step 2: Graph Construction

                    // 2a. Source to Border (and Source to Target if same partition)
                    // Add LTPs from Start Stops
                    let start_stops_simple: Vec<(u32, u32)> = partition_start_stops
                        .iter()
                        .map(|(p, s, _, _)| (*p, *s))
                        .collect();
                    query_graph.build_local(&start_partition, &start_stops_simple);

                    // 2b. Target from Border
                    if let Some(end_partition) = self.graph.get_transit_partition(end_pid) {
                        // Identify border stops in end partition
                        let mut border_stops = Vec::new();
                        for (idx, stop) in end_partition.stops.iter().enumerate() {
                            if stop.is_border || stop.is_hub {
                                border_stops.push((end_pid, idx as u32));
                            }
                        }
                        // Add LTPs from Border Stops in End Partition
                        // Note: This adds edges FROM border stops.
                        // If we are routing TO target, we need edges that lead TO target.
                        // LTPs are "From X -> Y".
                        // So adding LTPs from Border Stops allows us to reach Target if Target is reachable from Border.
                        query_graph.build_local(&end_partition, &border_stops);
                    }

                    // 2c. Border to Border (Global DAGs)
                    if *start_pid != end_pid {
                        if let Some(global_index) = &self.graph.global_index {
                            query_graph.build_global(*start_pid, end_pid, global_index);
                        }
                    }

                    // Step 3: Execution (Time-Dependent Dijkstra)
                    // We need service contexts for all involved partitions.
                    // For now, just start and end.
                    let window = 7200; // 2 hours
                    let mut service_contexts = HashMap::new();
                    self.compute_partition_service_contexts(
                        &start_partition,
                        req.time as i64,
                        window,
                        &mut service_contexts,
                    );
                    if let Some(end_part) = self.graph.get_transit_partition(end_pid) {
                        if *start_pid != end_pid {
                            self.compute_partition_service_contexts(
                                &end_part,
                                req.time as i64,
                                window,
                                &mut service_contexts,
                            );
                        }
                    }
                    let end_nodes_map: HashMap<(u32, u32), (u32, Vec<(f64, f64)>)> = end_stops
                        .iter()
                        .filter(|(p, _, _, _)| *p == end_pid)
                        .map(|(p, s, t, g)| ((*p, *s), (*t, g.clone())))
                        .collect();

                    let start_nodes_for_dijkstra: Vec<(u32, u32, u32)> = partition_start_stops
                        .iter()
                        .map(|(p, s, t, _)| (*p, *s, *t))
                        .collect();

                    let mut dijkstra_itineraries = query_graph.dijkstra(
                        &start_nodes_for_dijkstra,
                        &end_nodes_map,
                        req.time as i64,
                        self.graph,
                        &mut service_contexts,
                    );

                    // Prepend Walk Leg
                    for itinerary in &mut dijkstra_itineraries {
                        if let Some(first_leg) = itinerary.legs.first() {
                            if let Some(start_stop_id) = first_leg.start_stop_id() {
                                // Find which start stop was used
                                // We need to match start_stop_id to one of partition_start_stops
                                // This is a bit inefficient, but robust.
                                // Or we can use (pid, stop_idx) if we had it in Itinerary.
                                // But Itinerary only has GTFS ID.
                                // We can look up the stop in the partition to get GTFS ID.

                                for (pid, idx, time, geom) in partition_start_stops {
                                    if let Some(stop) = start_partition.stops.get(*idx as usize) {
                                        if stop.gtfs_original_id == *start_stop_id {
                                            // Found match
                                            let walk_leg = catenary::routing_common::api::Leg::Osm(
                                                catenary::routing_common::api::OsmLeg {
                                                    start_time: req.time,
                                                    end_time: req.time + *time as u64,
                                                    mode: TravelMode::Walk,
                                                    start_stop_id: None, // User Location
                                                    end_stop_id: Some(
                                                        stop.gtfs_original_id.clone(),
                                                    ),
                                                    start_stop_chateau: None,
                                                    end_stop_chateau: Some(
                                                        start_partition.chateau_ids
                                                            [stop.chateau_idx as usize]
                                                            .clone(),
                                                    ),
                                                    start_stop_name: Some("Origin".to_string()),
                                                    end_stop_name: None,
                                                    duration_seconds: *time as u64,
                                                    geometry: geom.clone(),
                                                },
                                            );
                                            itinerary.legs.insert(0, walk_leg);
                                            // Itinerary start time is already set to req.time by Dijkstra?
                                            // Dijkstra sets start_time = start_time_unix (req.time).
                                            // The first transit leg starts at req.time + access_time + wait.
                                            // So adding a walk leg of duration access_time fits perfectly.
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    itineraries.extend(dijkstra_itineraries);
                }
            }
        }

        RoutingResult { itineraries }
    }

    fn find_nearby_stops(
        &self,
        lat: f64,
        lon: f64,
        _mode: TravelMode,
        speed: f64,
    ) -> Vec<(u32, u32, u32, Vec<(f64, f64)>)> {
        // Returns (partition_id, stop_idx, access_time_seconds, geometry)
        let mut stops = Vec::new();
        let max_radius_meters = 2000.0; // 2km
        let max_radius_deg = max_radius_meters / 111000.0;

        let candidate_pids = self
            .graph
            .find_partitions_for_point(lat, lon, max_radius_deg);

        let pids_to_check: Vec<u32> = if self.graph.manifest.is_some() {
            candidate_pids
        } else {
            self.graph
                .transit_partitions
                .read()
                .unwrap()
                .keys()
                .cloned()
                .collect()
        };

        let osm_router = OsmRouter::new(self.graph);

        for pid in pids_to_check {
            if let Some(partition) = self.graph.get_transit_partition(pid) {
                // 1. Filter stops by Euclidean distance first
                let mut candidate_stops = Vec::new();
                for (idx, stop) in partition.stops.iter().enumerate() {
                    let dist = self.haversine_distance(lat, lon, stop.lat, stop.lon);
                    if dist <= max_radius_meters {
                        candidate_stops.push((idx as u32, stop.lat, stop.lon));
                    }
                }

                if candidate_stops.is_empty() {
                    continue;
                }

                // 2. Run A* / Dijkstra on OSM graph
                let max_duration = (max_radius_meters / speed) as u32;
                let reached_opt = osm_router.find_reachable_stops(
                    pid,
                    lat,
                    lon,
                    &candidate_stops,
                    max_duration,
                    speed,
                );

                if let Some(reached) = reached_opt {
                    for (stop_idx, duration, geometry) in reached {
                        stops.push((pid, stop_idx, duration, geometry));
                    }
                } else {
                    // Fallback to Euclidean if OSM data is missing
                    for (idx, lat_s, lon_s) in candidate_stops {
                        let dist = self.haversine_distance(lat, lon, lat_s, lon_s);
                        let time = (dist / speed) as u32;
                        // Straight line geometry
                        let geometry = vec![(lat, lon), (lat_s, lon_s)];
                        stops.push((pid, idx, time, geometry));
                    }
                }
            }
        }
        stops
    }

    fn haversine_distance(&self, lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        let r = 6371000.0; // Earth radius in meters
        let dlat = (lat2 - lat1).to_radians();
        let dlon = (lon2 - lon1).to_radians();
        let a = (dlat / 2.0).sin().powi(2)
            + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        r * c
    }

    fn compute_stop_to_patterns(&self, partition: &TransitPartition) -> Vec<Vec<usize>> {
        let num_stops = partition.stops.len();
        let mut stop_to_patterns = vec![Vec::new(); num_stops];
        for (p_idx, pattern) in partition.trip_patterns.iter().enumerate() {
            let stop_indices =
                &partition.direction_patterns[pattern.direction_pattern_idx as usize].stop_indices;
            for &s_idx in stop_indices {
                stop_to_patterns[s_idx as usize].push(p_idx);
            }
        }
        stop_to_patterns
    }

    fn calculate_relative_arrival_time(
        &self,
        partition: &TransitPartition,
        trip: &CompressedTrip,
        stop_idx_in_pattern: usize,
    ) -> u32 {
        let mut time = trip.start_time;
        let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
        for k in 0..=stop_idx_in_pattern {
            if k < delta_seq.deltas.len() {
                time += delta_seq.deltas[k];
            }
        }
        time
    }

    fn calculate_arrival_time_unix(
        &self,
        partition: &TransitPartition,
        trip: &CompressedTrip,
        stop_idx_in_pattern: usize,
        base_unix_time: i64,
    ) -> i64 {
        let rel_time = self.calculate_relative_arrival_time(partition, trip, stop_idx_in_pattern);
        base_unix_time + rel_time as i64
    }

    fn is_trip_active(
        &self,
        trip: &CompressedTrip,
        context: &ServiceContext,
        exceptions: &[catenary::routing_common::transit_graph::ServiceException],
    ) -> bool {
        // 1. Check Day Mask
        if (trip.service_mask & context.day_mask) == 0 {
            // Check if added in exceptions
            let mut added = false;
            for ex in exceptions {
                if ex.service_idx == trip.service_idx {
                    if ex.added_dates.contains(&context.service_date_int) {
                        added = true;
                    }
                    if ex.removed_dates.contains(&context.service_date_int) {
                        return false; // Explicitly removed
                    }
                }
            }
            if !added {
                return false;
            }
        } else {
            // Active by mask, check if removed
            for ex in exceptions {
                if ex.service_idx == trip.service_idx {
                    if ex.removed_dates.contains(&context.service_date_int) {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn get_hubs_for_partition_pair(&self, start_pid: u32, end_pid: u32) -> Vec<(u32, u32, u32)> {
        let mut hubs = Vec::new();
        if let Some(global_index) = &self.graph.global_index {
            // println!("DEBUG: Global index present. Searching for DAG P{} -> P{}", start_pid, end_pid);
            // Print all DAGs for debugging
            for dag in &global_index.partition_dags {
                println!(
                    "DEBUG: Available DAG: P{} -> P{} ({} hubs)",
                    dag.from_partition,
                    dag.to_partition,
                    dag.hubs.len()
                );
            }

            let mut found_dag = false;
            for dag in &global_index.partition_dags {
                // println!("DEBUG:   Checking DAG: P{} -> P{}", dag.from_partition, dag.to_partition);
                if dag.from_partition == start_pid && dag.to_partition == end_pid {
                    found_dag = true;
                    // println!("DEBUG:   Found matching DAG with {} hubs", dag.hubs.len());
                    for hub in &dag.hubs {
                        if hub.original_partition_id == start_pid {
                            // Access time is 0 for hubs we want to reach
                            hubs.push((start_pid, hub.stop_idx_in_partition, 0));
                        }
                    }
                }
            }
            if !found_dag {
                println!("DEBUG: No DAG found for P{} -> P{}", start_pid, end_pid);
            }
        } else {
            println!("DEBUG: Global index is NONE");
        }
        hubs
    }

    fn find_stop_in_partition(
        &self,
        partition_id: u32,
        chateau: &str,
        gtfs_id: &str,
    ) -> Option<u32> {
        if let Some(partition) = self.graph.get_transit_partition(partition_id) {
            let chateau_idx = partition
                .chateau_ids
                .iter()
                .position(|c| c == chateau)
                .map(|i| i as u32)?;

            for (idx, stop) in partition.stops.iter().enumerate() {
                if stop.gtfs_original_id == gtfs_id && stop.chateau_idx == chateau_idx {
                    return Some(idx as u32);
                }
            }
        }
        None
    }

    pub fn compute_partition_service_contexts(
        &self,
        partition: &TransitPartition,
        req_time: i64,
        window_seconds: u32,
        service_contexts: &mut HashMap<(u32, u32, i64), ServiceContext>,
    ) {
        // Collect all timezones in this partition
        // We iterate over all patterns to find unique timezones?
        // Or just use partition.timezones?
        // partition.timezones is a list of strings.
        // We need to generate contexts for each timezone index used.

        // Actually, we can just iterate over all timezones in the partition
        for (tz_idx, tz_str) in partition.timezones.iter().enumerate() {
            let tz: Tz = tz_str.parse().unwrap_or(chrono_tz::UTC);

            // Search range: req_time - 48h to req_time + window + 48h
            // Similar to run_profile_raptor logic
            let start_search = req_time - 172800;
            let end_search = req_time + window_seconds as i64 + 172800;

            let start_date = tz.timestamp_opt(start_search, 0).unwrap().date_naive();
            let end_date = tz.timestamp_opt(end_search, 0).unwrap().date_naive();

            let mut curr_date = start_date;
            while curr_date <= end_date {
                let noon = tz
                    .from_local_datetime(&curr_date.and_hms_opt(12, 0, 0).unwrap())
                    .unwrap();
                let base_unix = noon.timestamp() - 43200;

                let key = (partition.partition_id, tz_idx as u32, base_unix);
                if !service_contexts.contains_key(&key) {
                    let weekday = curr_date.weekday().num_days_from_monday(); // 0=Mon
                    let day_mask = 1 << weekday;

                    let service_date_int = (curr_date.year() as u32) * 10000
                        + (curr_date.month() as u32) * 100
                        + (curr_date.day() as u32);

                    service_contexts.insert(
                        key,
                        ServiceContext {
                            base_unix_time: base_unix,
                            day_mask,
                            service_date_int,
                        },
                    );
                }
                curr_date = curr_date.succ_opt().unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::routing_common::transit_graph::{
        CompressedTrip, DirectionPattern, TimeDeltaSequence, TransitPartition, TransitStop,
        TripPattern,
    };

    fn create_test_partition() -> TransitPartition {
        // Simple Line: Stop 0 -> Stop 1 -> Stop 2
        // Distances: 0->1 (10 min), 1->2 (10 min)
        // Trip A: Dep 8:00 (28800), Arr 1 8:10, Arr 2 8:20
        // Trip B: Dep 8:30 (30600), Arr 1 8:40, Arr 2 8:50

        let stops = vec![
            TransitStop {
                id: 0,
                chateau_idx: 0,
                gtfs_original_id: "S0".to_string(),
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                lat: 0.0,
                lon: 0.0,
            },
            TransitStop {
                id: 1,
                chateau_idx: 0,
                gtfs_original_id: "S1".to_string(),
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                lat: 0.01,
                lon: 0.0,
            },
            TransitStop {
                id: 2,
                chateau_idx: 0,
                gtfs_original_id: "S2".to_string(),
                is_hub: false,
                is_border: false,
                is_external_gateway: false,
                lat: 0.02,
                lon: 0.0,
            },
        ];

        let direction_patterns = vec![DirectionPattern {
            stop_indices: vec![0, 1, 2],
        }];

        // Time deltas: [0, 600, 600] (0 wait at start, 10 min to next, 10 min to next)
        let time_deltas = vec![TimeDeltaSequence {
            deltas: vec![0, 600, 600],
        }];

        let trips = vec![
            CompressedTrip {
                gtfs_trip_id: "T1".to_string(),
                service_mask: 127, // All days
                start_time: 28800, // 8:00
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            },
            CompressedTrip {
                gtfs_trip_id: "T2".to_string(),
                service_mask: 127,
                start_time: 30600, // 8:30
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 0,
                wheelchair_accessible: 0,
            },
        ];

        let trip_patterns = vec![TripPattern {
            chateau_idx: 0,
            route_id: "R1".to_string(),
            direction_pattern_idx: 0,
            trips,
            timezone_idx: 0,
        }];

        TransitPartition {
            partition_id: 0,
            stops,
            trip_patterns,
            time_deltas,
            direction_patterns,
            internal_transfers: vec![],
            osm_links: vec![],
            service_ids: vec!["daily".to_string()],
            service_exceptions: vec![],
            _deprecated_external_transfers: vec![],
            local_transfer_patterns: vec![
                LocalTransferPattern {
                    from_stop_idx: 0,
                    edges: vec![
                        DagEdge {
                            from_hub_idx: 0, // Not used for LTP
                            to_hub_idx: 1,   // Stop 1
                            edge_type: Some(EdgeType::Transit(TransitEdge {
                                trip_pattern_idx: 0,
                                start_stop_idx: 0,
                                end_stop_idx: 1,
                            })),
                        },
                        DagEdge {
                            from_hub_idx: 0,
                            to_hub_idx: 2, // Stop 2
                            edge_type: Some(EdgeType::Transit(TransitEdge {
                                trip_pattern_idx: 0,
                                start_stop_idx: 0,
                                end_stop_idx: 2,
                            })),
                        },
                    ],
                },
                LocalTransferPattern {
                    from_stop_idx: 1,
                    edges: vec![DagEdge {
                        from_hub_idx: 1,
                        to_hub_idx: 2, // Stop 2
                        edge_type: Some(EdgeType::Transit(TransitEdge {
                            trip_pattern_idx: 0,
                            start_stop_idx: 1,
                            end_stop_idx: 2,
                        })),
                    }],
                },
            ],
            timezones: vec!["UTC".to_string()],
            boundary: None,
            chateau_ids: vec!["test".to_string()],
        }
    }

    #[test]
    fn test_simple_raptor() {
        let partition = create_test_partition();
        let mut graph = GraphManager::new();
        graph
            .transit_partitions
            .write()
            .unwrap()
            .insert(0, std::sync::Arc::new(partition.clone()));
        let router = Router::new(&graph);

        // Request: Stop 0 to Stop 2, departing at 7:50
        let start_time = 1704095400; // 7:50

        let req = RoutingRequest {
            start_lat: 0.0,
            start_lon: 0.0,
            end_lat: 0.02,
            end_lon: 0.0,
            mode: TravelMode::Transit,
            time: start_time as u64,
            speed_mps: 1.0,
            is_departure_time: true,
            wheelchair_accessible: false,
        };

        let result = router.route(&req);

        assert!(
            !result.itineraries.is_empty(),
            "Should find at least one itinerary"
        );
        let itin = &result.itineraries[0];
        // Start time should be trip start (8:00 = 1704096000), not req time (7:50)
        // Wait, Itinerary.start_time usually reflects the trip start?
        // Or the query start?
        // In my Dijkstra implementation:
        // start_time: start_time_unix as u64 (which is req.time)
        // But the first leg (Transit) has a duration.
        // If there is a wait, the transit leg starts later.
        // Let's check Dijkstra reconstruction.
        // Itinerary.start_time = start_time_unix (req.time).
        // Itinerary.end_time = min_end_time.
        // This includes waiting time at start.
        // So start_time is 7:50.
        assert_eq!(itin.start_time, 1704095400);
        // Trip A arrives at Stop 2 at 8:20 = 1704097200
        assert_eq!(itin.end_time, 1704097200);
    }

    #[test]
    fn test_profile_search() {
        let partition = create_test_partition();
        let mut graph = GraphManager::new();
        graph
            .transit_partitions
            .write()
            .unwrap()
            .insert(0, std::sync::Arc::new(partition.clone()));
        let router = Router::new(&graph);

        // Request: 2024-01-01 7:50 UTC
        let req_time = 1704095400; // 7:50

        let req = RoutingRequest {
            start_lat: 0.0,
            start_lon: 0.0,
            end_lat: 0.02,
            end_lon: 0.0,
            mode: TravelMode::Transit,
            time: req_time as u64,
            speed_mps: 1.0,
            is_departure_time: true,
            wheelchair_accessible: false,
        };

        let result = router.route(&req);

        // With standard Dijkstra, we expect the BEST itinerary (Trip A).
        assert!(!result.itineraries.is_empty());
        let itin = &result.itineraries[0];

        // Trip A (8:00) -> 1704096000 (Trip Start)
        // Itinerary Start (Req Time) -> 1704095400
        assert_eq!(itin.start_time, 1704095400);
        assert_eq!(itin.end_time, 1704097200);
    }

    #[test]
    fn test_multi_partition_routing() {
        // Partition 0: Stop 0 -> Stop 1
        let mut partition0 = create_test_partition();
        partition0.partition_id = 0;
        partition0.stops[0].id = 0;
        partition0.stops[0].chateau_idx = 0;
        partition0.stops[0].lat = 0.0;
        partition0.stops[0].lon = 0.0;
        partition0.stops[1].id = 1;
        partition0.stops[1].chateau_idx = 0;
        partition0.stops[1].lat = 0.01;
        partition0.stops[1].lon = 0.0;
        partition0.chateau_ids = vec!["test".to_string()];
        // Remove Stop 2 from P0 for clarity (though create_test_partition has 3 stops)

        // Partition 1: Stop 2 -> Stop 3
        let mut partition1 = create_test_partition();
        partition1.partition_id = 1;
        partition1.stops[0].id = 2; // Remap IDs
        partition1.stops[0].chateau_idx = 0;
        partition1.stops[0].lat = 1.0; // Far away
        partition1.stops[0].lon = 0.0;
        partition1.stops[1].id = 3;
        partition1.stops[1].chateau_idx = 0;
        partition1.stops[1].lat = 1.01;
        partition1.stops[1].lon = 0.0;
        partition1.stops[2].id = 4;
        partition1.stops[2].chateau_idx = 0;
        partition1.chateau_ids = vec!["test".to_string()];

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

        // Setup Global Index to connect P0 -> P1 via Stop 1
        let hub1 = catenary::routing_common::transit_graph::GlobalHub {
            original_partition_id: 0,
            stop_idx_in_partition: 1,
        };
        let hub2 = catenary::routing_common::transit_graph::GlobalHub {
            original_partition_id: 1,
            stop_idx_in_partition: 0, // Stop 2 (index 0 in P1)
        };
        let dag = catenary::routing_common::transit_graph::PartitionDag {
            from_partition: 0,
            to_partition: 1,
            hubs: vec![hub1, hub2],
            edges: vec![DagEdge {
                from_hub_idx: 0,
                to_hub_idx: 1,
                edge_type: Some(EdgeType::Walk(
                    catenary::routing_common::transit_graph::WalkEdge {
                        duration_seconds: 600, // 10 min walk transfer
                    },
                )),
            }],
        };
        graph.global_index = Some(
            catenary::routing_common::transit_graph::GlobalPatternIndex {
                partition_dags: vec![dag],
            },
        );

        let router = Router::new(&graph);

        // Request: Stop 0 (P0) to Stop 2 (P1)
        let req_time = 1704095400;

        let result = router.route(&RoutingRequest {
            start_lat: 0.0,
            start_lon: 0.0,
            end_lat: 1.0,
            end_lon: 0.0,
            mode: TravelMode::Transit,
            time: req_time as u64,
            speed_mps: 1.0,
            is_departure_time: true,
            wheelchair_accessible: false,
        });

        println!("Itineraries found: {}", result.itineraries.len());
        assert!(!result.itineraries.is_empty());

        // Verify we reached the hub (Stop 1)
        let itin = &result.itineraries[0];
        let last_leg = itin.legs.last().unwrap();
        // Stop 0 in P1 has GTFS ID "S0" (inherited from create_test_partition)
        assert_eq!(last_leg.end_stop_id().as_deref(), Some("S0"));
    }
}

#[test]
fn test_multi_partition_selection() {
    use catenary::routing_common::transit_graph::{CompressedTrip, TimeDeltaSequence, TransitStop};

    // Create two partitions:
    // Partition 0: Has a stop near start, but NO path to destination.
    // Partition 1: Has a stop slightly further from start, but HAS path to destination.

    // Partition 0
    let stops_p0 = vec![TransitStop {
        id: 0,
        chateau_idx: 0,
        gtfs_original_id: "S0_P0".to_string(),
        is_hub: false,
        is_border: false,
        is_external_gateway: false,
        lat: 0.0,
        lon: 0.0, // Very close to start (0,0)
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
        local_transfer_patterns: vec![LocalTransferPattern {
            from_stop_idx: 0,
            edges: vec![DagEdge {
                from_hub_idx: 0,
                to_hub_idx: 1,
                edge_type: Some(EdgeType::Transit(TransitEdge {
                    trip_pattern_idx: 0,
                    start_stop_idx: 0,
                    end_stop_idx: 1,
                })),
            }],
        }],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["p0".to_string()],
    };

    // Partition 1
    let stops_p1 = vec![
        TransitStop {
            id: 0,
            chateau_idx: 0,
            gtfs_original_id: "S0_P1".to_string(),
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            lat: 0.001, // Slightly further (approx 111m)
            lon: 0.0,
        },
        TransitStop {
            id: 1,
            chateau_idx: 0,
            gtfs_original_id: "S1_P1".to_string(),
            is_hub: false,
            is_border: false,
            is_external_gateway: false,
            lat: 0.01, // Destination
            lon: 0.0,
        },
    ];

    // Create a trip in Partition 1 from S0_P1 to S1_P1
    let direction_patterns_p1 = vec![DirectionPattern {
        stop_indices: vec![0, 1],
    }];
    let time_deltas_p1 = vec![TimeDeltaSequence {
        deltas: vec![0, 600],
    }]; // 10 min travel
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
    }];

    let partition1 = TransitPartition {
        partition_id: 1,
        stops: stops_p1,
        trip_patterns: trip_patterns_p1,
        time_deltas: time_deltas_p1,
        direction_patterns: direction_patterns_p1,
        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec!["daily".to_string()],
        service_exceptions: vec![],
        _deprecated_external_transfers: vec![],
        local_transfer_patterns: vec![LocalTransferPattern {
            from_stop_idx: 0,
            edges: vec![DagEdge {
                from_hub_idx: 0,
                to_hub_idx: 1,
                edge_type: Some(EdgeType::Transit(TransitEdge {
                    trip_pattern_idx: 0,
                    start_stop_idx: 0,
                    end_stop_idx: 1,
                })),
            }],
        }],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["p1".to_string()],
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
        time: 1704095400, // 7:50
        speed_mps: 1.0,
        is_departure_time: true,
        wheelchair_accessible: false,
    };

    let result = router.route(&req);

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
    // Note: Access/Egress legs are implicit in start/end times but not in legs vec currently.
}
