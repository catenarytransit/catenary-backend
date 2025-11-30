use crate::graph_loader::GraphManager;
use catenary::routing_common::api::{Itinerary, Leg, TravelMode};
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

            // Add LTP edges
            for ltp in &partition.local_transfer_patterns {
                if ltp.from_stop_idx == s_idx {
                    for edge in &ltp.edges {
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
                    let from_hub = &dag.hubs[edge.from_hub_idx as usize];
                    let to_hub = &dag.hubs[edge.to_hub_idx as usize];

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
            stop_idx: dag_edge.from_hub_idx,
        };
        let to_node = QueryNode {
            partition_id,
            stop_idx: dag_edge.to_hub_idx,
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
        end_nodes: &HashMap<(u32, u32), u32>, // (partition_id, stop_idx) -> egress_time
        start_time_unix: i64,
        graph_manager: &GraphManager,
        service_contexts: &HashMap<(u32, i64), ServiceContext>,
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

            if let Some(&egress_time) = end_nodes.get(&(partition_id, stop_idx)) {
                let total_cost = cost + egress_time as u64;
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
            if let Some(&egress_time) = end_nodes.get(&(end_node.partition_id, end_node.stop_idx)) {
                if egress_time > 0 {
                    if let Some(partition) =
                        graph_manager.transit_partitions.get(&end_node.partition_id)
                    {
                        let stop = &partition.stops[end_node.stop_idx as usize];
                        legs.push(Leg {
                            mode: TravelMode::Walk,
                            start_stop_id: Some(stop.gtfs_original_id.clone()),
                            end_stop_id: None, // Destination
                            start_stop_chateau: Some(stop.chateau.clone()),
                            end_stop_chateau: None,
                            route_id: None,
                            trip_id: None,
                            chateau: None,
                            start_stop_name: None,
                            end_stop_name: None,
                            route_name: None,
                            trip_name: None,
                            duration_seconds: egress_time as u64,
                            geometry: vec![],
                        });
                    }
                }
            }

            let itinerary = Itinerary {
                start_time: start_time_unix as u64,
                end_time: min_end_time,
                duration_seconds: min_end_time - start_time_unix as u64,
                transfers: legs
                    .iter()
                    .filter(|l| l.mode == TravelMode::Transit)
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
        service_contexts: &HashMap<(u32, i64), ServiceContext>,
    ) -> Option<u64> {
        match &edge.edge_type {
            EdgeType::Walk(w) => Some(current_time + w.duration_seconds as u64),
            EdgeType::Transit(t) => {
                let partition = graph_manager
                    .transit_partitions
                    .get(&edge.from.partition_id)?;
                let pattern = if let Some(p) =
                    partition.trip_patterns.get(t.trip_pattern_idx as usize)
                {
                    p
                } else {
                    tracing::warn!(
                        "Invalid trip_pattern_idx {} for partition {} (len: {}). Edge: {:?} -> {:?}",
                        t.trip_pattern_idx,
                        edge.from.partition_id,
                        partition.trip_patterns.len(),
                        edge.from,
                        edge.to
                    );
                    return None;
                };

                // Find next trip
                // We need to check all valid service contexts for this pattern's timezone
                let tz_idx = pattern.timezone_idx;

                let mut best_arrival = u64::MAX;
                let mut found = false;

                for ((t_idx, _), context) in service_contexts {
                    if *t_idx != tz_idx {
                        continue;
                    }

                    for trip in &pattern.trips {
                        if !self.is_trip_active(trip, context, &partition.service_exceptions) {
                            continue;
                        }

                        let dep_time = self.calculate_arrival_time_unix(
                            partition,
                            trip,
                            t.start_stop_idx as usize,
                            context.base_unix_time,
                        );

                        if dep_time >= current_time as i64 {
                            let arr_time = self.calculate_arrival_time_unix(
                                partition,
                                trip,
                                t.end_stop_idx as usize,
                                context.base_unix_time,
                            );

                            if (arr_time as u64) < best_arrival {
                                best_arrival = arr_time as u64;
                                found = true;
                            }
                            // Trips are sorted, so first valid one is best for this day
                            break;
                        }
                    }
                }

                if found { Some(best_arrival) } else { None }
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
        let partition = graph_manager
            .transit_partitions
            .get(&edge.from.partition_id)?;
        let start_stop = &partition.stops[edge.from.stop_idx as usize];

        match &edge.edge_type {
            EdgeType::Walk(_) => {
                // For walk edges, the destination might be in a different partition (e.g. global DAG edge)
                // or the same partition.
                let end_stop = if edge.to.partition_id == edge.from.partition_id {
                    // Local walk
                    partition.stops.get(edge.to.stop_idx as usize)?
                } else {
                    // Cross-partition walk (e.g. at a border)
                    let to_partition = graph_manager
                        .transit_partitions
                        .get(&edge.to.partition_id)?;
                    to_partition.stops.get(edge.to.stop_idx as usize)?
                };

                Some(Leg {
                    mode: TravelMode::Walk,
                    start_stop_id: Some(start_stop.gtfs_original_id.clone()),
                    end_stop_id: Some(end_stop.gtfs_original_id.clone()),
                    start_stop_chateau: Some(start_stop.chateau.clone()),
                    end_stop_chateau: Some(end_stop.chateau.clone()),
                    route_id: None,
                    trip_id: None,
                    chateau: None,
                    start_stop_name: None,
                    end_stop_name: None,
                    route_name: None,
                    trip_name: None,
                    duration_seconds: end_time - start_time,
                    geometry: vec![],
                })
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

                Some(Leg {
                    mode: TravelMode::Transit,
                    start_stop_id: Some(start_stop.gtfs_original_id.clone()),
                    end_stop_id: Some(end_stop.gtfs_original_id.clone()),
                    start_stop_chateau: Some(start_stop.chateau.clone()),
                    end_stop_chateau: Some(end_stop.chateau.clone()),
                    route_id: Some(pattern.route_id.clone()),
                    trip_id: None,
                    chateau: Some(pattern.chateau.clone()),
                    start_stop_name: None,
                    end_stop_name: None,
                    route_name: None,
                    trip_name: None,
                    duration_seconds: end_time - start_time,
                    geometry: vec![],
                })
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
