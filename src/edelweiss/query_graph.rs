use crate::graph_loader::GraphManager;
use catenary::routing_common::api::{Itinerary, Leg, OsmLeg, TransitLeg, TravelMode};
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, EdgeType, GlobalPatternIndex, ServiceException, TransitPartition,
};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueryNode {
    pub partition_id: u32,
    pub stop_idx: u32,
}

#[derive(Clone, Debug)]
pub struct QueryEdge {
    pub from: QueryNode,
    pub to: QueryNode,
    pub edge_type: EdgeType,
}

pub struct QueryGraph {
    pub adjacency: HashMap<QueryNode, Vec<QueryEdge>>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
    cost: u64,
    partition_id: u32,
    stop_idx: u32,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap instead of a max-heap.
impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        // Notice that the we flip the ordering on costs.
        // In case of a tie we compare positions - this step is necessary
        // to make implementations of `PartialEq` and `Ord` consistent.
        other
            .cost
            .cmp(&self.cost)
            .then_with(|| self.partition_id.cmp(&other.partition_id))
            .then_with(|| self.stop_idx.cmp(&other.stop_idx))
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct ServiceContext {
    pub base_unix_time: i64,
    pub day_mask: u32,
    pub service_date_int: u32,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self {
            adjacency: HashMap::new(),
        }
    }

    pub fn add_edge(&mut self, edge: QueryEdge) {
        self.adjacency
            .entry(edge.from.clone())
            .or_default()
            .push(edge);
    }

    pub fn build_local(
        &mut self,
        partition: &TransitPartition,
        start_stops: &[(u32, u32)], // (partition_id, stop_idx)
    ) {
        for &(p_id, s_idx) in start_stops {
            if p_id != partition.partition_id {
                continue;
            }

            // Add LocalTransferPattern edges
            for local_transfer_pattern in &partition.local_transfer_patterns {
                if local_transfer_pattern.from_stop_idx == s_idx {
                    for edge in &local_transfer_pattern.edges {
                        self.add_dag_edge(partition.partition_id, edge);
                    }
                }
            }
        }
    }

    pub fn build_global(
        &mut self,
        start_pid: u32,
        end_pid: u32,
        global_index: &GlobalPatternIndex,
    ) {
        for dag in &global_index.partition_dags {
            if dag.from_partition == start_pid && dag.to_partition == end_pid {
                for edge in &dag.edges {
                    let from_hub = &dag.hubs[edge.from_node_idx as usize];
                    let to_hub = &dag.hubs[edge.to_node_idx as usize];

                    let from_node = QueryNode {
                        partition_id: from_hub.original_partition_id,
                        stop_idx: from_hub.stop_idx_in_partition,
                    };
                    let to_node = QueryNode {
                        partition_id: to_hub.original_partition_id,
                        stop_idx: to_hub.stop_idx_in_partition,
                    };

                    if let Some(edge_type) = &edge.edge_type {
                        self.add_edge(QueryEdge {
                            from: from_node,
                            to: to_node,
                            edge_type: edge_type.clone(),
                        });
                    }
                }
            }
        }

        for dag in &global_index.long_distance_dags {
            if dag.from_partition == start_pid && dag.to_partition == end_pid {
                for edge in &dag.edges {
                    let from_hub = &dag.hubs[edge.from_node_idx as usize];
                    let to_hub = &dag.hubs[edge.to_node_idx as usize];

                    let from_node = QueryNode {
                        partition_id: from_hub.original_partition_id,
                        stop_idx: from_hub.stop_idx_in_partition,
                    };
                    let to_node = QueryNode {
                        partition_id: to_hub.original_partition_id,
                        stop_idx: to_hub.stop_idx_in_partition,
                    };

                    if let Some(edge_type) = &edge.edge_type {
                        self.add_edge(QueryEdge {
                            from: from_node,
                            to: to_node,
                            edge_type: edge_type.clone(),
                        });
                    }
                }
            }
        }
    }

    fn add_dag_edge(&mut self, partition_id: u32, dag_edge: &DagEdge) {
        let from_node = QueryNode {
            partition_id,
            stop_idx: dag_edge.from_node_idx,
        };
        let to_node = QueryNode {
            partition_id,
            stop_idx: dag_edge.to_node_idx,
        };

        if let Some(edge_type) = &dag_edge.edge_type {
            self.add_edge(QueryEdge {
                from: from_node,
                to: to_node,
                edge_type: edge_type.clone(),
            });
        }
    }

    pub fn dijkstra(
        &self,
        start_nodes: &[(u32, u32, u32)], // (partition_id, stop_idx, access_time)
        end_nodes: &HashMap<(u32, u32), (u32, Vec<(f64, f64)>)>, // (partition_id, stop_idx) -> (egress_time, geometry)
        start_time_unix: i64,
        graph_manager: &GraphManager,
        service_contexts: &mut HashMap<(u32, u32, i64), ServiceContext>,
    ) -> Vec<Itinerary> {
        let mut dist: HashMap<QueryNode, u64> = HashMap::new();
        let mut heap = BinaryHeap::new();
        let mut predecessors: HashMap<QueryNode, (QueryNode, Option<Leg>)> = HashMap::new();

        for &(pid, idx, access_time) in start_nodes {
            let node = QueryNode {
                partition_id: pid,
                stop_idx: idx,
            };
            let arrival = start_time_unix as u64 + access_time as u64;
            dist.insert(node.clone(), arrival);
            heap.push(State {
                cost: arrival,
                partition_id: pid,
                stop_idx: idx,
            });
        }

        let mut best_end_node: Option<QueryNode> = None;
        let mut min_end_time = u64::MAX;

        while let Some(State {
            cost,
            partition_id,
            stop_idx,
        }) = heap.pop()
        {
            let u = QueryNode {
                partition_id,
                stop_idx,
            };

            if cost > *dist.get(&u).unwrap_or(&u64::MAX) {
                continue;
            }

            if let Some((egress_time, _)) = end_nodes.get(&(partition_id, stop_idx)) {
                let total_cost = cost + *egress_time as u64;
                if total_cost < min_end_time {
                    min_end_time = total_cost;
                    best_end_node = Some(u.clone());
                }
            }

            if let Some(edges) = self.adjacency.get(&u) {
                for edge in edges {
                    let v = &edge.to;
                    let arrival_time =
                        self.evaluate_edge(edge, cost, graph_manager, service_contexts);

                    if let Some(arr) = arrival_time {
                        if arr < *dist.get(v).unwrap_or(&u64::MAX) {
                            dist.insert(v.clone(), arr);

                            // Create Leg info for reconstruction
                            let leg = self.create_leg_info(edge, cost, arr, graph_manager);
                            predecessors.insert(v.clone(), (u.clone(), leg));

                            heap.push(State {
                                cost: arr,
                                partition_id: v.partition_id,
                                stop_idx: v.stop_idx,
                            });
                        }
                    }
                }
            }
        }

        // Reconstruct path
        if let Some(end_node) = best_end_node {
            let mut legs = Vec::new();
            let mut curr = end_node.clone();

            while let Some((prev, leg_opt)) = predecessors.remove(&curr) {
                if let Some(leg) = leg_opt {
                    legs.push(leg);
                }
                curr = prev;
            }
            legs.reverse();

            // Add Egress Leg
            if let Some((egress_time, geometry)) =
                end_nodes.get(&(end_node.partition_id, end_node.stop_idx))
            {
                if *egress_time > 0 {
                    if let Some(partition) =
                        graph_manager.get_transit_partition(end_node.partition_id)
                    {
                        let stop = &partition.stops[end_node.stop_idx as usize];
                        legs.push(Leg::Osm(OsmLeg {
                            start_time: min_end_time - *egress_time as u64,
                            end_time: min_end_time,
                            mode: TravelMode::Walk,
                            start_stop_id: Some(stop.gtfs_original_id.clone()),
                            end_stop_id: None, // Destination
                            start_stop_chateau: Some(
                                partition.chateau_ids[stop.chateau_idx as usize].clone(),
                            ),
                            end_stop_chateau: None,
                            start_stop_name: None,
                            end_stop_name: None,
                            duration_seconds: *egress_time as u64,
                            geometry: geometry.clone(),
                        }));
                    }
                }
            }

            let itinerary = Itinerary {
                start_time: start_time_unix as u64,
                end_time: min_end_time,
                duration_seconds: min_end_time - start_time_unix as u64,
                transfers: legs
                    .iter()
                    .filter(|l| matches!(l, Leg::Transit(_)))
                    .count()
                    .saturating_sub(1) as u32,
                reliability_score: 1.0,
                legs,
            };
            return vec![itinerary];
        }

        Vec::new()
    }

    fn evaluate_edge(
        &self,
        edge: &QueryEdge,
        current_time: u64,
        graph_manager: &GraphManager,
        service_contexts: &mut HashMap<(u32, u32, i64), ServiceContext>,
    ) -> Option<u64> {
        match &edge.edge_type {
            EdgeType::Walk(w) => Some(current_time + w.duration_seconds as u64),
            EdgeType::Transit(t) => {
                // 1. Try to get loaded partition
                if let Some(partition) = graph_manager.get_transit_partition(edge.from.partition_id)
                {
                    let pattern =
                        if let Some(p) = partition.trip_patterns.get(t.trip_pattern_idx as usize) {
                            p
                        } else {
                            return None;
                        };

                    self.ensure_service_context(
                        edge.from.partition_id,
                        pattern.timezone_idx,
                        &partition.timezones,
                        current_time,
                        service_contexts,
                    );

                    let mut best_arrival = u64::MAX;
                    let mut found = false;

                    for ((pid, t_idx, _), context) in service_contexts.iter() {
                        if *pid != edge.from.partition_id || *t_idx != pattern.timezone_idx {
                            continue;
                        }

                        for trip in &pattern.trips {
                            if !self.is_trip_active(trip, context, &partition.service_exceptions) {
                                continue;
                            }

                            let dep_time = self.calculate_arrival_time_unix(
                                &partition,
                                trip,
                                t.start_stop_idx as usize,
                                context.base_unix_time,
                            );

                            if dep_time >= current_time as i64 {
                                let arr_time = self.calculate_arrival_time_unix(
                                    &partition,
                                    trip,
                                    t.end_stop_idx as usize,
                                    context.base_unix_time,
                                );

                                if (arr_time as u64) < best_arrival {
                                    best_arrival = arr_time as u64;
                                    found = true;
                                }
                                break;
                            }
                        }
                    }
                    if found { Some(best_arrival) } else { None }
                } else if let Some(global_timetable) = &graph_manager.global_timetable {
                    // 2. Try Global Timetable
                    // Find partition timetable
                    if let Some(part_tt) = global_timetable
                        .partition_timetables
                        .iter()
                        .find(|pt| pt.partition_id == edge.from.partition_id)
                    {
                        // Find pattern timetable
                        if let Some(pat_tt) = part_tt
                            .pattern_timetables
                            .iter()
                            .find(|pt| pt.pattern_idx == t.trip_pattern_idx)
                        {
                            self.ensure_service_context(
                                edge.from.partition_id,
                                pat_tt.timezone_idx,
                                &part_tt.timezones,
                                current_time,
                                service_contexts,
                            );

                            let mut best_arrival = u64::MAX;
                            let mut found = false;

                            for ((pid, t_idx, _), context) in service_contexts.iter() {
                                if *pid != edge.from.partition_id || *t_idx != pat_tt.timezone_idx {
                                    continue;
                                }

                                for (i, &start_time) in pat_tt.trip_start_times.iter().enumerate() {
                                    let service_mask = pat_tt.service_masks[i];
                                    // Check service mask (simplified, no exceptions for global yet?)
                                    // GlobalTimetable doesn't store exceptions currently.
                                    if (service_mask & context.day_mask) == 0 {
                                        continue;
                                    }

                                    // Calculate times
                                    // We need time deltas.
                                    let delta_idx = pat_tt.trip_time_delta_indices[i];
                                    let delta_seq = &part_tt.time_deltas[delta_idx as usize];

                                    // Calculate relative departure time
                                    let mut rel_dep_time = start_time;
                                    for k in 0..=t.start_stop_idx as usize {
                                        let travel_idx = 2 * k;
                                        let dwell_idx = 2 * k + 1;
                                        if travel_idx < delta_seq.deltas.len() {
                                            rel_dep_time += delta_seq.deltas[travel_idx];
                                        }
                                        if k < t.start_stop_idx as usize {
                                            if dwell_idx < delta_seq.deltas.len() {
                                                rel_dep_time += delta_seq.deltas[dwell_idx];
                                            }
                                        }
                                    }
                                    let dep_time = context.base_unix_time + rel_dep_time as i64;

                                    if dep_time >= current_time as i64 {
                                        // Calculate relative arrival time
                                        let mut rel_arr_time = start_time;
                                        for k in 0..=t.end_stop_idx as usize {
                                            let travel_idx = 2 * k;
                                            let dwell_idx = 2 * k + 1;
                                            if travel_idx < delta_seq.deltas.len() {
                                                rel_arr_time += delta_seq.deltas[travel_idx];
                                            }
                                            if k < t.end_stop_idx as usize {
                                                if dwell_idx < delta_seq.deltas.len() {
                                                    rel_arr_time += delta_seq.deltas[dwell_idx];
                                                }
                                            }
                                        }
                                        let arr_time = context.base_unix_time + rel_arr_time as i64;

                                        if (arr_time as u64) < best_arrival {
                                            best_arrival = arr_time as u64;
                                            found = true;
                                        }
                                        break;
                                    }
                                }
                            }
                            if found { Some(best_arrival) } else { None }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            EdgeType::LongDistanceTransit(t) => {
                // 1. Try to get loaded partition
                if let Some(partition) = graph_manager.get_transit_partition(edge.from.partition_id)
                {
                    let pattern = if let Some(p) = partition
                        .long_distance_trip_patterns
                        .get(t.trip_pattern_idx as usize)
                    {
                        p
                    } else {
                        return None;
                    };

                    self.ensure_service_context(
                        edge.from.partition_id,
                        pattern.timezone_idx,
                        &partition.timezones,
                        current_time,
                        service_contexts,
                    );

                    let mut best_arrival = u64::MAX;
                    let mut found = false;

                    for ((pid, t_idx, _), context) in service_contexts.iter() {
                        if *pid != edge.from.partition_id || *t_idx != pattern.timezone_idx {
                            continue;
                        }

                        for trip in &pattern.trips {
                            if !self.is_trip_active(trip, context, &partition.service_exceptions) {
                                continue;
                            }

                            let dep_time = self.calculate_arrival_time_unix(
                                &partition,
                                trip,
                                t.start_stop_idx as usize,
                                context.base_unix_time,
                            );

                            if dep_time >= current_time as i64 {
                                let arr_time = self.calculate_arrival_time_unix(
                                    &partition,
                                    trip,
                                    t.end_stop_idx as usize,
                                    context.base_unix_time,
                                );

                                if (arr_time as u64) < best_arrival {
                                    best_arrival = arr_time as u64;
                                    found = true;
                                }
                                break;
                            }
                        }
                    }
                    if found { Some(best_arrival) } else { None }
                } else {
                    // Long distance patterns are NOT in GlobalTimetable currently (based on my read of connectivity.rs).
                    // connectivity.rs only populates used_patterns from partition_dags, not long_distance_dags?
                    // Let's check connectivity.rs.
                    // "for dag in &global_index.partition_dags { ... }"
                    // It does NOT iterate long_distance_dags.
                    // So GlobalTimetable only supports local patterns used in global DAGs.
                    // If we have LongDistanceTransit edges, we MUST load the partition?
                    // Or we should have added them to GlobalTimetable.
                    // For now, return None if not loaded.
                    None
                }
            }
        }
    }

    fn ensure_service_context(
        &self,
        partition_id: u32,
        timezone_idx: u32,
        timezones: &[String],
        current_time: u64,
        service_contexts: &mut HashMap<(u32, u32, i64), ServiceContext>,
    ) {
        // Check if we have context
        let mut has_context = false;
        for ((pid, t_idx, _), _) in service_contexts.iter() {
            if *pid == partition_id && *t_idx == timezone_idx {
                has_context = true;
                break;
            }
        }

        if !has_context {
            use chrono::{Datelike, TimeZone};
            use chrono_tz::Tz;

            if let Some(tz_str) = timezones.get(timezone_idx as usize) {
                let tz: Tz = tz_str.parse().unwrap_or(chrono_tz::UTC);
                let req_time = current_time as i64;
                let window_seconds = 7200; // 2 hours

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

                    let key = (partition_id, timezone_idx, base_unix);
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

    fn create_leg_info(
        &self,
        edge: &QueryEdge,
        start_time: u64,
        end_time: u64,
        graph_manager: &GraphManager,
    ) -> Option<Leg> {
        let partition = graph_manager.get_transit_partition(edge.from.partition_id)?;
        let start_stop = &partition.stops[edge.from.stop_idx as usize];

        match &edge.edge_type {
            EdgeType::Walk(_) => {
                // For walk edges, the destination might be in a different partition (e.g. global DAG edge)
                // or the same partition.
                let (end_stop_id, end_stop_chateau) = if edge.to.partition_id
                    == edge.from.partition_id
                {
                    // Local walk
                    let stop = partition.stops.get(edge.to.stop_idx as usize)?;
                    (
                        stop.gtfs_original_id.clone(),
                        partition.chateau_ids[stop.chateau_idx as usize].clone(),
                    )
                } else {
                    // Cross-partition walk (e.g. at a border)
                    let to_partition = graph_manager.get_transit_partition(edge.to.partition_id)?;
                    let stop = to_partition.stops.get(edge.to.stop_idx as usize)?;
                    (
                        stop.gtfs_original_id.clone(),
                        partition.chateau_ids[stop.chateau_idx as usize].clone(),
                    )
                };

                Some(Leg::Osm(OsmLeg {
                    start_time,
                    end_time,
                    mode: TravelMode::Walk,
                    start_stop_id: Some(start_stop.gtfs_original_id.clone()),
                    end_stop_id: Some(end_stop_id),
                    start_stop_chateau: Some(
                        partition.chateau_ids[start_stop.chateau_idx as usize].clone(),
                    ),
                    end_stop_chateau: Some(end_stop_chateau),
                    start_stop_name: None,
                    end_stop_name: None,
                    duration_seconds: end_time - start_time,
                    geometry: vec![],
                }))
            }
            EdgeType::Transit(t) => {
                let pattern = &partition.trip_patterns[t.trip_pattern_idx as usize];

                // For Transit edges, the `end_stop_idx` in `TransitEdge` refers to the index
                // within the PATTERN's stop sequence, NOT the global partition stop index.
                // We must resolve it to the partition stop index first.
                let dir_pattern =
                    &partition.direction_patterns[pattern.direction_pattern_idx as usize];
                let end_stop_idx_in_partition = dir_pattern.stop_indices[t.end_stop_idx as usize];

                let end_stop = &partition.stops[end_stop_idx_in_partition as usize];

                Some(Leg::Transit(TransitLeg {
                    start_time,
                    end_time,
                    mode: TravelMode::Transit,
                    start_stop_id: start_stop.gtfs_original_id.clone(),
                    end_stop_id: end_stop.gtfs_original_id.clone(),
                    start_stop_chateau: partition.chateau_ids[start_stop.chateau_idx as usize]
                        .clone(),
                    end_stop_chateau: partition.chateau_ids[end_stop.chateau_idx as usize].clone(),
                    route_id: pattern.route_id.clone(),
                    trip_id: None,
                    chateau: partition.chateau_ids[pattern.chateau_idx as usize].clone(),
                    start_stop_name: None,
                    end_stop_name: None,
                    route_name: None,
                    trip_name: None,
                    duration_seconds: end_time - start_time,
                    geometry: {
                        let mut geom = Vec::new();
                        for i in t.start_stop_idx..=t.end_stop_idx {
                            let s_idx = dir_pattern.stop_indices[i as usize];
                            let stop = &partition.stops[s_idx as usize];
                            geom.push((stop.lat, stop.lon));
                        }
                        geom
                    },
                }))
            }
            EdgeType::LongDistanceTransit(t) => {
                let pattern = &partition.long_distance_trip_patterns[t.trip_pattern_idx as usize];

                let dir_pattern =
                    &partition.direction_patterns[pattern.direction_pattern_idx as usize];
                let end_stop_idx_in_partition = dir_pattern.stop_indices[t.end_stop_idx as usize];

                let end_stop = &partition.stops[end_stop_idx_in_partition as usize];

                Some(Leg::Transit(TransitLeg {
                    start_time,
                    end_time,
                    mode: TravelMode::Transit,
                    start_stop_id: start_stop.gtfs_original_id.clone(),
                    end_stop_id: end_stop.gtfs_original_id.clone(),
                    start_stop_chateau: partition.chateau_ids[start_stop.chateau_idx as usize]
                        .clone(),
                    end_stop_chateau: partition.chateau_ids[end_stop.chateau_idx as usize].clone(),
                    route_id: pattern.route_id.clone(),
                    trip_id: None,
                    chateau: partition.chateau_ids[pattern.chateau_idx as usize].clone(),
                    start_stop_name: None,
                    end_stop_name: None,
                    route_name: None,
                    trip_name: None,
                    duration_seconds: end_time - start_time,
                    geometry: {
                        let mut geom = Vec::new();
                        for i in t.start_stop_idx..=t.end_stop_idx {
                            let s_idx = dir_pattern.stop_indices[i as usize];
                            let stop = &partition.stops[s_idx as usize];
                            geom.push((stop.lat, stop.lon));
                        }
                        geom
                    },
                }))
            }
        }
    }

    // Helper methods copied/adapted from Router
    fn calculate_relative_arrival_time(
        &self,
        partition: &TransitPartition,
        trip: &CompressedTrip,
        stop_idx_in_pattern: usize,
    ) -> u32 {
        let mut time = trip.start_time;
        let delta_seq = &partition.time_deltas[trip.time_delta_idx as usize];
        for k in 0..=stop_idx_in_pattern {
            let travel_idx = 2 * k;
            let dwell_idx = 2 * k + 1;

            if travel_idx < delta_seq.deltas.len() {
                time += delta_seq.deltas[travel_idx];
            }

            // Add dwell time if this is not the target stop (we depart from it)
            if k < stop_idx_in_pattern {
                if dwell_idx < delta_seq.deltas.len() {
                    time += delta_seq.deltas[dwell_idx];
                }
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
        exceptions: &[ServiceException],
    ) -> bool {
        if (trip.service_mask & context.day_mask) == 0 {
            let mut added = false;
            for ex in exceptions {
                if ex.service_idx == trip.service_idx {
                    if ex.added_dates.contains(&context.service_date_int) {
                        added = true;
                    }
                    if ex.removed_dates.contains(&context.service_date_int) {
                        return false;
                    }
                }
            }
            if !added {
                return false;
            }
        } else {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::routing_common::transit_graph::{
        GlobalTimetable, PartitionTimetable, PatternTimetable, TimeDeltaSequence, TransitEdge,
    };
    use std::collections::HashMap;

    #[test]
    fn test_evaluate_edge_global_timetable() {
        // Setup GlobalTimetable
        let pattern_tt = PatternTimetable {
            pattern_idx: 10,
            trip_start_times: vec![3600], // 1:00 AM relative to base
            trip_time_delta_indices: vec![0],
            service_masks: vec![127], // Every day (bits 0-6 set)
            timezone_idx: 0,
        };

        let partition_tt = PartitionTimetable {
            partition_id: 1,
            pattern_timetables: vec![pattern_tt],
            time_deltas: vec![TimeDeltaSequence {
                deltas: vec![0, 600, 0],
            }], // 0 wait, 600 travel, 0 dwell
            timezones: vec!["UTC".to_string()],
        };

        let global_tt = GlobalTimetable {
            partition_timetables: vec![partition_tt],
        };

        let mut graph_manager = GraphManager::new();
        graph_manager.global_timetable = Some(global_tt);

        let query_graph = QueryGraph::new();
        let edge = QueryEdge {
            from: QueryNode {
                partition_id: 1,
                stop_idx: 0,
            },
            to: QueryNode {
                partition_id: 1,
                stop_idx: 1,
            },
            edge_type: EdgeType::Transit(TransitEdge {
                trip_pattern_idx: 10,
                start_stop_idx: 0,
                end_stop_idx: 1,
                min_duration: 600,
            }),
        };

        let mut service_contexts = HashMap::new();
        // 2024-01-01 00:50:00 UTC = 1704070200
        // This is a Monday.
        let current_time = 1704070200;

        let arrival =
            query_graph.evaluate_edge(&edge, current_time, &graph_manager, &mut service_contexts);

        assert!(arrival.is_some());

        // Base time for 2024-01-01 is 00:00:00 UTC = 1704067200
        // Trip start: 3600s after base = 01:00:00 UTC = 1704070800
        // Travel time: 600s
        // Arrival: 1704071400 (01:10:00 UTC)

        assert_eq!(arrival.unwrap(), 1704071400);
    }
}
