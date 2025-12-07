use crate::graph_loader::GraphManager;
use catenary::routing_common::api::{Itinerary, Leg, OsmLeg, TransitLeg, TravelMode};
use catenary::routing_common::transit_graph::{
    CompressedTrip, DagEdge, EdgeType, GlobalPatternIndex, ServiceException, TransitPartition,
};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use tracing::{debug, error, info, warn};

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
            // Add LocalTransferPattern edges (from Union DAG)
            if let Some(edge_list) = partition.local_dag.get(&s_idx) {
                for edge in &edge_list.edges {
                    self.add_dag_edge(partition.partition_id, edge);
                }
            }

            // Add LongDistanceTransferPattern edges
            for ld_pattern in &partition.long_distance_transfer_patterns {
                if ld_pattern.from_stop_idx == s_idx {
                    for edge in &ld_pattern.edges {
                        // For LongDistanceTransferPattern, the to_node_idx might be an external hub index
                        // We need to resolve it.
                        // But add_dag_edge expects a DagEdge and assumes to_node_idx is a stop_idx in the SAME partition?
                        // No, add_dag_edge creates a QueryNode with `partition_id` (the current one) and `stop_idx`.
                        // If to_node_idx refers to an external hub, we need to resolve it to (pid, idx).

                        // Check if it's an external hub index
                        let to_node_idx = edge.to_node_idx;
                        if to_node_idx >= partition.stops.len() as u32 {
                            let hub_idx = (to_node_idx as usize) - partition.stops.len();
                            if let Some(hub) = partition.external_hubs.get(hub_idx) {
                                let to_node = QueryNode {
                                    partition_id: hub.original_partition_id,
                                    stop_idx: hub.stop_idx_in_partition,
                                };

                                let from_node = QueryNode {
                                    partition_id: p_id,
                                    stop_idx: s_idx,
                                };

                                if let Some(edge_type) = &edge.edge_type {
                                    self.add_edge(QueryEdge {
                                        from: from_node,
                                        to: to_node,
                                        edge_type: edge_type.clone(),
                                    });
                                }
                            }
                        } else {
                            // Local stop
                            self.add_dag_edge(partition.partition_id, edge);
                        }
                    }
                }
            }
        }
    }

    pub fn build_direct(
        &mut self,
        direct_connections: &catenary::routing_common::transit_graph::DirectConnections,
        relevant_station_ids: &std::collections::HashSet<String>,
        station_id_to_node: &HashMap<String, (u32, u32)>,
    ) {
        // Map station_id string to index in direct_connections.stops
        let dc_stop_id_to_idx: HashMap<&String, u32> = direct_connections
            .stops
            .iter()
            .enumerate()
            .map(|(i, s)| (s, i as u32))
            .collect();

        // Add Access/Egress edges for relevant stations
        for station_id in relevant_station_ids {
            if let Some(&(pid, s_idx)) = station_id_to_node.get(station_id) {
                if let Some(&dc_s_idx) = dc_stop_id_to_idx.get(station_id) {
                    let local_node = QueryNode {
                        partition_id: pid,
                        stop_idx: s_idx,
                    };
                    let global_node = QueryNode {
                        partition_id: u32::MAX,
                        stop_idx: dc_s_idx,
                    };

                    // Access: Local -> Global
                    self.add_edge(QueryEdge {
                        from: local_node.clone(),
                        to: global_node.clone(),
                        edge_type: EdgeType::Walk(
                            catenary::routing_common::transit_graph::WalkEdge {
                                duration_seconds: 0,
                            },
                        ),
                    });

                    // Egress: Global -> Local
                    self.add_edge(QueryEdge {
                        from: global_node,
                        to: local_node,
                        edge_type: EdgeType::Walk(
                            catenary::routing_common::transit_graph::WalkEdge {
                                duration_seconds: 0,
                            },
                        ),
                    });
                }
            }
        }

        for (from_station_id_str, refs) in &direct_connections.index {
            if !relevant_station_ids.contains(from_station_id_str) {
                continue;
            }

            let from_dc_stop_idx = *dc_stop_id_to_idx
                .get(from_station_id_str)
                .expect("Station ID in index not found in stops list");

            for ref_item in refs {
                let trip_pattern_idx = ref_item.pattern_idx;
                let trip_pattern = &direct_connections.trip_patterns[trip_pattern_idx as usize];
                let direction_pattern = &direct_connections.direction_patterns
                    [trip_pattern.direction_pattern_idx as usize];

                let start_stop_idx_in_pattern = ref_item.stop_idx;

                // Iterate over all downstream stops
                for end_stop_idx_in_pattern in
                    (start_stop_idx_in_pattern + 1)..(direction_pattern.stop_indices.len() as u32)
                {
                    let to_dc_stop_idx =
                        direction_pattern.stop_indices[end_stop_idx_in_pattern as usize];
                    let to_station_id_str = &direct_connections.stops[to_dc_stop_idx as usize];

                    if !relevant_station_ids.contains(to_station_id_str) {
                        // println!("Skipping to_station: {} (not relevant)", to_station_id_str);
                        continue;
                    }

                    let from_node = QueryNode {
                        partition_id: u32::MAX,
                        stop_idx: from_dc_stop_idx,
                    };
                    let to_node = QueryNode {
                        partition_id: u32::MAX,
                        stop_idx: to_dc_stop_idx,
                    };

                    self.add_edge(QueryEdge {
                        from: from_node,
                        to: to_node,
                        edge_type: EdgeType::LongDistanceTransit(
                            catenary::routing_common::transit_graph::TransitEdge {
                                trip_pattern_idx: trip_pattern_idx,
                                start_stop_idx: start_stop_idx_in_pattern,
                                end_stop_idx: end_stop_idx_in_pattern,
                                min_duration: 0, // Calculated by evaluate_edge
                            },
                        ),
                    });
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
        info!(
            "Dijkstra start: {} start nodes, {} end nodes, time {}",
            start_nodes.len(),
            end_nodes.len(),
            start_time_unix
        );
        if start_nodes.is_empty() {
            error!("No start nodes provided!");
            return Vec::new();
        }

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
                            start_stop_id: Some(stop.station_id.clone()),
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

            info!("Dijkstra finished: Path found. Cost: {}", min_end_time);
            return vec![itinerary];
        }

        error!("Dijkstra finished: No path found.");

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
                            warn!(
                                "Trip pattern {} not found in partition {}",
                                t.trip_pattern_idx, edge.from.partition_id
                            );
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
                    debug!(
                        "Partition {} not found for edge evaluation",
                        edge.from.partition_id
                    );
                    None
                }
            }
            EdgeType::LongDistanceTransit(t) => {
                // Use DirectConnections
                if let Some(dc) = &graph_manager.direct_connections {
                    let pattern = if let Some(p) = dc.trip_patterns.get(t.trip_pattern_idx as usize)
                    {
                        p
                    } else {
                        return None;
                    };

                    self.ensure_service_context(
                        u32::MAX,
                        pattern.timezone_idx,
                        &dc.timezones,
                        current_time,
                        service_contexts,
                    );

                    let mut best_arrival = u64::MAX;
                    let mut found = false;

                    for ((pid, t_idx, _), context) in service_contexts.iter() {
                        if *pid != u32::MAX || *t_idx != pattern.timezone_idx {
                            continue;
                        }

                        for trip in &pattern.trips {
                            if !self.is_trip_active(trip, context, &dc.service_exceptions) {
                                continue;
                            }

                            // Calculate departure time
                            // We need a version of calculate_arrival_time_unix that works with DirectConnections (or generic traits)
                            // Since TransitPartition and DirectConnections share structure types (TripPattern, TimeDeltaSequence),
                            // we can perhaps duplicate the logic or extract it.
                            // For now, let's duplicate the logic but use `dc` fields.

                            let dep_time = self.calculate_arrival_time_unix_dc(
                                dc,
                                trip,
                                t.start_stop_idx as usize,
                                context.base_unix_time,
                            );

                            if dep_time >= current_time as i64 {
                                let arr_time = self.calculate_arrival_time_unix_dc(
                                    dc,
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
                        stop.station_id.clone(),
                        partition.chateau_ids[stop.chateau_idx as usize].clone(),
                    )
                } else {
                    // Cross-partition walk (e.g. at a border)
                    let to_partition = graph_manager.get_transit_partition(edge.to.partition_id)?;
                    let stop = to_partition.stops.get(edge.to.stop_idx as usize)?;
                    (
                        stop.station_id.clone(),
                        to_partition.chateau_ids[stop.chateau_idx as usize].clone(),
                    )
                };

                Some(Leg::Osm(OsmLeg {
                    start_time,
                    end_time,
                    mode: TravelMode::Walk,
                    start_stop_id: Some(start_stop.station_id.clone()),
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

                let leg = Leg::Transit(TransitLeg {
                    start_time,
                    end_time,
                    mode: TravelMode::Transit,
                    start_stop_id: start_stop.station_id.clone(),
                    end_stop_id: end_stop.station_id.clone(),
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
                });
                debug!("Created Transit Leg: {:?}", leg);
                Some(leg)
            }
            EdgeType::LongDistanceTransit(t) => {
                let pattern = &partition.long_distance_trip_patterns[t.trip_pattern_idx as usize];

                let dir_pattern =
                    &partition.direction_patterns[pattern.direction_pattern_idx as usize];
                let end_stop_idx_in_partition = dir_pattern.stop_indices[t.end_stop_idx as usize];

                let (end_stop_id, end_stop_chateau) = if (end_stop_idx_in_partition as usize)
                    < partition.stops.len()
                {
                    let end_stop = &partition.stops[end_stop_idx_in_partition as usize];
                    (
                        end_stop.station_id.clone(),
                        partition.chateau_ids[end_stop.chateau_idx as usize].clone(),
                    )
                } else {
                    // External Hub
                    let hub_idx = (end_stop_idx_in_partition as usize) - partition.stops.len();
                    if let Some(hub) = partition.external_hubs.get(hub_idx) {
                        if let Some(target_partition) =
                            graph_manager.get_transit_partition(hub.original_partition_id)
                        {
                            if let Some(stop) = target_partition
                                .stops
                                .get(hub.stop_idx_in_partition as usize)
                            {
                                (
                                    stop.station_id.clone(),
                                    target_partition.chateau_ids[stop.chateau_idx as usize].clone(),
                                )
                            } else {
                                ("UNKNOWN".to_string(), "UNKNOWN".to_string())
                            }
                        } else {
                            ("UNKNOWN".to_string(), "UNKNOWN".to_string())
                        }
                    } else {
                        ("UNKNOWN".to_string(), "UNKNOWN".to_string())
                    }
                };

                let leg = Leg::Transit(TransitLeg {
                    start_time,
                    end_time,
                    mode: TravelMode::Transit,
                    start_stop_id: start_stop.station_id.clone(),
                    end_stop_id,
                    start_stop_chateau: partition.chateau_ids[start_stop.chateau_idx as usize]
                        .clone(),
                    end_stop_chateau,
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
                            if (s_idx as usize) < partition.stops.len() {
                                let stop = &partition.stops[s_idx as usize];
                                geom.push((stop.lat, stop.lon));
                            } else {
                                // External Hub
                                let hub_idx = (s_idx as usize) - partition.stops.len();
                                if let Some(hub) = partition.external_hubs.get(hub_idx) {
                                    if let Some(target_partition) = graph_manager
                                        .get_transit_partition(hub.original_partition_id)
                                    {
                                        if let Some(stop) = target_partition
                                            .stops
                                            .get(hub.stop_idx_in_partition as usize)
                                        {
                                            geom.push((stop.lat, stop.lon));
                                        }
                                    }
                                }
                            }
                        }
                        geom
                    },
                });
                debug!("Created LongDistanceTransit Leg: {:?}", leg);
                Some(leg)
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
    fn calculate_arrival_time_unix_dc(
        &self,
        dc: &catenary::routing_common::transit_graph::DirectConnections,
        trip: &CompressedTrip,
        stop_idx_in_pattern: usize,
        base_unix_time: i64,
    ) -> i64 {
        let mut time = trip.start_time;
        let delta_seq = &dc.time_deltas[trip.time_delta_idx as usize];
        for k in 0..=stop_idx_in_pattern {
            let travel_idx = 2 * k;
            let dwell_idx = 2 * k + 1;

            if travel_idx < delta_seq.deltas.len() {
                time += delta_seq.deltas[travel_idx];
            }

            if k < stop_idx_in_pattern {
                if dwell_idx < delta_seq.deltas.len() {
                    time += delta_seq.deltas[dwell_idx];
                }
            }
        }
        base_unix_time + time as i64
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
