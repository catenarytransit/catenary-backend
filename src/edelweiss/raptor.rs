use rayon::prelude::*;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};

use catenary::routing_common::api::{Itinerary, Leg, Place, RoutingRequest, RoutingResult};
use catenary::routing_common::timetable::{LocationIdx, RouteIdx, ZeroCopyTimetable};

#[derive(Clone, Copy, Default)]
struct ParentPointer {
    prev_stop: Option<u32>,  // LocationIdx.0 bounds
    route: Option<u32>,      // RouteIdx.0
    trip: Option<u32>,       // trip index inside route
    boarded_at: Option<u32>, // stop sequence bounded
}

pub struct RaptorRouter<'a> {
    timetable: ZeroCopyTimetable<'a>,
}

impl<'a> RaptorRouter<'a> {
    pub fn new(timetable: ZeroCopyTimetable<'a>) -> Self {
        Self { timetable }
    }

    /// Run the RAPTOR algorithm using memory-mapped arrays from Motis-styled structures.
    /// This implementation follows "Algorithm 1: RAPTOR" from the original paper.
    pub fn route(&self, req: &RoutingRequest) -> RoutingResult {
        let mut result = RoutingResult {
            itineraries: vec![],
        };

        let locations_count = self.timetable.coordinates().len();
        if locations_count == 0 {
            return result;
        }

        // We assume Origin and Destination are already mapped to LocationIdx via OSM walking.
        // For demonstration, we'll start with stop 0 as the source (p_s) and 1 as the target (p_t).
        let p_s = LocationIdx(0);
        let p_t = LocationIdx(1);
        let departure_time = req.time as u32;

        let max_rounds = 12; // Motis limits to reasonable transfers.
        let mut tau_k = vec![vec![u32::MAX; locations_count]; max_rounds + 1];
        let tau_star = (0..locations_count)
            .map(|_| AtomicU32::new(u32::MAX))
            .collect::<Vec<_>>();

        let mut marked_stops = vec![false; locations_count];
        let mut parent_pointers =
            vec![vec![ParentPointer::default(); locations_count]; max_rounds + 1];

        // 1. Initialization
        // tau_0(p_s) <- tau
        tau_k[0][p_s.0 as usize] = departure_time;
        tau_star[p_s.0 as usize].store(departure_time, Ordering::Relaxed);
        marked_stops[p_s.0 as usize] = true;

        let routes = self.timetable.routes();
        let route_stops = self.timetable.route_stops();
        let route_stop_times = self.timetable.route_stop_times();
        let stop_footpaths = self.timetable.stop_footpaths();
        let footpaths_out = self.timetable.footpaths_out();

        for k in 1..=max_rounds {
            // Q stores pairs of (RouteIdx, stop sequence offset in route)
            let mut q: HashMap<u32, u32> = HashMap::new();

            // 2a. Accumulate routes serving marked stops
            // In a fully indexed structure, we need a reverse mapping: Stop -> [RouteIdx]
            // We simulate skipping this dictionary step by scanning for now.
            for (p_idx, &is_marked) in marked_stops.iter().enumerate() {
                if !is_marked {
                    continue;
                }

                // Find routes serving p_i
                for (r_idx, route) in routes.iter().enumerate() {
                    let stops_start = route.stops_offset as usize;
                    let stops_end = stops_start + route.stops_count as usize;
                    let r_stops = &route_stops[stops_start..stops_end];

                    if let Some(stop_seq) = r_stops.iter().position(|s| s.0 as usize == p_idx) {
                        let current_best = q.get(&(r_idx as u32)).copied();
                        if let Some(best_seq) = current_best {
                            // Substitute (r, p') by (r, p) if p comes before p' in r
                            if (stop_seq as u32) < best_seq {
                                q.insert(r_idx as u32, stop_seq as u32);
                            }
                        } else {
                            q.insert(r_idx as u32, stop_seq as u32);
                        }
                    }
                }
            }

            // Unmark all stops
            marked_stops.fill(false);

            // 2b. Traverse each route using lock-free parallelization
            struct ParentUpdate {
                location: usize,
                arrival_time: u32,
                route: u32,
                trip: u32,
                boarded: usize,
            }

            // Map each route to a thread-local update log
            let q_list: Vec<(&u32, &u32)> = q.iter().collect();
            let thread_updates: Vec<Vec<ParentUpdate>> = q_list
                .par_iter()
                .map(|&(&r_idx, &p_seq)| {
                    let mut local_log = Vec::new();
                    let route = &routes[r_idx as usize];
                    let stops_start = route.stops_offset as usize;
                    let stops_end = stops_start + route.stops_count as usize;
                    let r_stops = &route_stops[stops_start..stops_end];

                    let mut current_trip: Option<u32> = None;
                    let mut boarded_at: Option<usize> = None;

                    for seq in (p_seq as usize)..route.stops_count as usize {
                        let p_i = r_stops[seq].0 as usize;

                        // Can the label be improved in this round? Includes local and target pruning.
                        if let Some(t_idx) = current_trip {
                            // In RAPTOR: arr(t, p_i) < min(tau*(p_i), tau*(p_t))
                            // We index route_stop_times: [stops][trips * 2 (arr/dep)]
                            let arr_time_idx = route.times_offset as usize
                                + (seq * route.trips_count as usize * 2)
                                + (t_idx as usize * 2);
                            if arr_time_idx < route_stop_times.len() {
                                let arrival_time = route_stop_times[arr_time_idx];

                                let global_star = tau_star[p_i].load(Ordering::Relaxed);
                                let target_star = tau_star[p_t.0 as usize].load(Ordering::Relaxed);

                                if arrival_time < global_star && arrival_time < target_star {
                                    tau_star[p_i].fetch_min(arrival_time, Ordering::Relaxed);
                                    local_log.push(ParentUpdate {
                                        location: p_i,
                                        arrival_time,
                                        route: r_idx,
                                        trip: t_idx,
                                        boarded: boarded_at.unwrap_or(p_seq as usize),
                                    });
                                }
                            }
                        }

                        // Can we catch an earlier trip?
                        if tau_k[k - 1][p_i] != u32::MAX {
                            // Binary search or scan for the earliest trip t where dep(t, p_i) >= tau_{k-1}(p_i)
                            for t_cand in 0..route.trips_count {
                                let dep_time_idx = route.times_offset as usize
                                    + (seq * route.trips_count as usize * 2)
                                    + (t_cand as usize * 2)
                                    + 1;
                                if dep_time_idx < route_stop_times.len() {
                                    let departure_time = route_stop_times[dep_time_idx];
                                    if departure_time >= tau_k[k - 1][p_i] {
                                        // Found a trip!
                                        // If we didn't have a trip, or this trip is earlier (in index usually implies earlier time)
                                        // we hop on.
                                        current_trip = Some(t_cand);
                                        boarded_at = Some(seq);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    local_log
                })
                .collect();

            // Reduce logs serially to restore consistency correctly mapped back to parent pointers
            for update_log in thread_updates {
                for upd in update_log {
                    if upd.arrival_time < tau_k[k][upd.location] {
                        tau_k[k][upd.location] = upd.arrival_time;
                        marked_stops[upd.location] = true;

                        parent_pointers[k][upd.location] = ParentPointer {
                            prev_stop: Some(
                                route_stops[routes[upd.route as usize].stops_offset as usize
                                    + upd.boarded]
                                    .0,
                            ), // Map local sequence onto location
                            route: Some(upd.route),
                            trip: Some(upd.trip),
                            boarded_at: Some(
                                route_stops[routes[upd.route as usize].stops_offset as usize
                                    + upd.boarded]
                                    .0,
                            ),
                        };
                    }
                }
            }

            // 2c. Look at footpaths
            // Clone marked stops since we will mark new ones during iteration
            let newly_marked: Vec<usize> = marked_stops
                .iter()
                .enumerate()
                .filter(|&(_, &m)| m)
                .map(|(i, _)| i)
                .collect();
            for p in newly_marked {
                // Bounds loop for footpath target
                if p < stop_footpaths.len() {
                    let fp_meta = &stop_footpaths[p];
                    let fp_start = fp_meta.offset as usize;
                    let fp_end = fp_start + fp_meta.count as usize;

                    if fp_start < footpaths_out.len() && fp_end <= footpaths_out.len() {
                        let fps = &footpaths_out[fp_start..fp_end];
                        for fp in fps {
                            let p_target = fp.target.0 as usize;
                            let duration = fp.duration_seconds as u32; // In real GTFS we might map to minutes. We will assume u32 minutes.

                            let arrival = tau_k[k][p].saturating_add(duration);
                            if arrival < tau_k[k][p_target] {
                                tau_k[k][p_target] = arrival;
                                marked_stops[p_target] = true;

                                parent_pointers[k][p_target] = ParentPointer {
                                    prev_stop: Some(p as u32),
                                    route: None,
                                    trip: None,
                                    boarded_at: None,
                                };
                            }
                        }
                    }
                }
            }

            // Stopping criterion
            if !marked_stops.iter().any(|&m| m) {
                break;
            }
        }

        // 3. Journey reconstruction
        // Find best arrival time and the corresponding round `k`
        let mut best_k = None;
        let mut best_arrival = u32::MAX;

        for k in 1..=max_rounds {
            if tau_k[k][p_t.0 as usize] < best_arrival {
                best_arrival = tau_k[k][p_t.0 as usize];
                best_k = Some(k);
            }
        }

        if let Some(mut curr_k) = best_k {
            let mut curr_stop = p_t.0 as usize;
            let mut legs = Vec::new();

            while curr_k > 0 && curr_stop != p_s.0 as usize {
                let p = parent_pointers[curr_k][curr_stop];

                if let Some(r_idx) = p.route {
                    // Transit leg
                    let start_stop = p.boarded_at.unwrap();
                    let end_stop = curr_stop as u32;
                    let trip_idx = p.trip.unwrap();

                    // Lookup strings from ZeroCopy bounds
                    let route_id_offsets = self.timetable.route_id_offsets();
                    let route_id = if (r_idx as usize) < route_id_offsets.len() {
                        self.timetable
                            .get_string(&route_id_offsets[r_idx as usize])
                            .unwrap_or_default()
                            .to_string()
                    } else {
                        r_idx.to_string()
                    };

                    let trip_id_offsets = self.timetable.trip_id_offsets();
                    let trip_id = if (trip_idx as usize) < trip_id_offsets.len() {
                        self.timetable
                            .get_string(&trip_id_offsets[trip_idx as usize])
                            .map(|s| s.to_string())
                    } else {
                        None
                    };

                    legs.push(Leg::Transit(catenary::routing_common::api::TransitLeg {
                        start_time: tau_k[curr_k - 1][start_stop as usize] as u64, // Estimate bounds
                        end_time: tau_k[curr_k][curr_stop] as u64,
                        mode: catenary::routing_common::api::TravelMode::Transit,
                        start_stop_id: start_stop.to_string(), // In real app, map to GTFS stop string pool
                        end_stop_id: end_stop.to_string(),
                        start_stop_chateau: String::new(),
                        end_stop_chateau: String::new(),
                        route_id,
                        trip_id,
                        chateau: String::new(),
                        start_stop_name: None,
                        end_stop_name: None,
                        route_name: None,
                        trip_name: None,
                        duration_seconds: (tau_k[curr_k][curr_stop]
                            - tau_k[curr_k - 1][start_stop as usize])
                            as u64,
                        geometry: vec![],
                    }));

                    curr_stop = start_stop as usize;
                } else if let Some(prev) = p.prev_stop {
                    // Footpath leg
                    let start_stop = prev;
                    let end_stop = curr_stop as u32;

                    legs.push(Leg::Osm(catenary::routing_common::api::OsmLeg {
                        start_time: tau_k[curr_k][prev as usize] as u64,
                        end_time: tau_k[curr_k][curr_stop] as u64,
                        mode: catenary::routing_common::api::TravelMode::Walk,
                        start_stop_id: Some(start_stop.to_string()),
                        end_stop_id: Some(end_stop.to_string()),
                        start_stop_chateau: None,
                        end_stop_chateau: None,
                        start_stop_name: None,
                        end_stop_name: None,
                        duration_seconds: (tau_k[curr_k][curr_stop] - tau_k[curr_k][prev as usize])
                            as u64,
                        geometry: vec![],
                    }));

                    curr_stop = prev as usize;
                } else {
                    break;
                }

                curr_k -= 1;
            }

            legs.reverse();

            result.itineraries.push(Itinerary {
                start_time: departure_time as u64,
                end_time: best_arrival as u64,
                duration_seconds: (best_arrival - departure_time) as u64,
                transfers: best_k.unwrap_or(0) as u32 - 1,
                reliability_score: 1.0,
                legs,
            });
        }

        result
    }
}
