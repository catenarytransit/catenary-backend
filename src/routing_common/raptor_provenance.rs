//! Provenance-aware RAPTOR rounds.
//!
//! A stop/round may have several nondominated arrival states: an earlier
//! arrival via a prohibited interchange cannot replace a later feasible one.
//! The preceding vehicle's alighting location/time belongs to the state,
//! rather than to the stop. Search rounds contain exactly k boardings.
//!
//! Inspired by Nigiri's separation of round labels, predecessors and marks;
//! unlike Nigiri's ordinary stop-time labels we retain additional GTFS
//! stop-pair provenance because our boarding predicate depends on it.

use super::{Journey, Leg, Query, RaptorStats, INF};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{ensure, Result};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

type NodeId = usize;

#[derive(Clone, Copy)]
enum Predecessor {
    Access { seconds: u32 },
    Walk { previous: NodeId, seconds: u32 },
    Ride {
        previous: NodeId,
        route: u32,
        transport: u32,
        service_day: u16,
        departure: i64,
        arrival: i64,
    },
}

#[derive(Clone, Copy)]
struct Node {
    stop: usize,
    time: i64,
    // None means initial access: it has no interchange restriction.
    last_alight: Option<(usize, i64)>,
    boardings: usize,
    predecessor: Predecessor,
    // Only used to reject obsolete entries in the current round's heap.
    live: bool,
}

/// Only compare states whose interchange behavior can be ordered for every
/// future stop, including after further walks. Different GTFS rule sources
/// are deliberately incomparable.
///
/// With no explicit rule for either source, a transfer-ready time is
/// max(walk_arrival, alight + max(from_change, target_change)).
fn dominates(tt: &ZeroCopyTimetable<'_>, special: &[bool], a: &Node, b: &Node) -> bool {
    if a.boardings > b.boardings || a.time > b.time { return false; }
    match (a.last_alight, b.last_alight) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some((from_a, alight_a)), Some((from_b, alight_b))) => {
            if from_a == from_b { return alight_a <= alight_b; }
            if special[from_a] || special[from_b] { return false; }
            alight_a <= alight_b &&
                alight_a + i64::from(tt.location_transfer_seconds()[from_a])
                    <= alight_b + i64::from(tt.location_transfer_seconds()[from_b])
        }
    }
}

struct Round {
    // Dense stop addressing but heap-allocated lists only for reached stops.
    states: Vec<Vec<NodeId>>,
    touched: Vec<usize>,
    marked: Vec<u64>,
}

impl Round {
    fn new(stops: usize) -> Self {
        Self { states: vec![Vec::new(); stops], touched: Vec::new(), marked: vec![0; stops.div_ceil(64)] }
    }

    fn clear(&mut self) {
        for stop in self.touched.drain(..) { self.states[stop].clear(); }
        self.marked.fill(0);
    }

    fn has(&self, stop: usize) -> bool {
        self.marked[stop / 64] & (1u64 << (stop % 64)) != 0
    }

    fn insert(
        &mut self,
        tt: &ZeroCopyTimetable<'_>,
        special: &[bool],
        global: &mut [Vec<NodeId>],
        nodes: &mut Vec<Node>,
        candidate: Node,
        stats: &mut RaptorStats,
    ) -> Option<NodeId> {
        // Nigiri-style best_: every new candidate must improve on every
        // previous boarding round, not merely this round's labels.
        let bucket = &mut global[candidate.stop];
        if bucket.iter().any(|&id| dominates(tt, special, &nodes[id], &candidate)) {
            stats.labels_dominated += 1;
            return None;
        }
        bucket.retain(|&id| {
            if dominates(tt, special, &candidate, &nodes[id]) {
                nodes[id].live = false;
                stats.labels_dominated += 1;
                false
            } else {
                true
            }
        });
        if !self.has(candidate.stop) {
            self.marked[candidate.stop / 64] |= 1u64 << (candidate.stop % 64);
            self.touched.push(candidate.stop);
        }
        let id = nodes.len();
        nodes.push(candidate);
        bucket.push(id);
        self.states[candidate.stop].push(id);
        stats.labels_improved += 1;
        Some(id)
    }
}

fn can_board(tt: &ZeroCopyTimetable<'_>, node: Node, stop: usize, departure: i64) -> bool {
    if node.time > departure { return false; }
    let Some((from, alight)) = node.last_alight else { return true; };
    let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(stop as u32));
    if rule.is_some_and(|r| r.prohibited()) { return false; }
    let change = tt.location_transfer_seconds()[from]
        .max(tt.location_transfer_seconds()[stop])
        .max(rule.map_or(0, |r| r.min_seconds));
    alight + i64::from(change) <= departure
}

fn walk_closure(
    tt: &ZeroCopyTimetable<'_>,
    special: &[bool],
    round: &mut Round,
    global: &mut [Vec<NodeId>],
    nodes: &mut Vec<Node>,
    max_time: i64,
    stats: &mut RaptorStats,
) {
    let start = Instant::now();
    let mut heap = BinaryHeap::new();
    for &stop in &round.touched {
        for &id in &round.states[stop] {
            heap.push(Reverse((nodes[id].time, id)));
            stats.walking_heap_pushes += 1;
        }
    }
    while let Some(Reverse((_, id))) = heap.pop() {
        if !nodes[id].live { continue; }
        let from = nodes[id];
        for fp in tt.footpaths_from(LocationIdx(from.stop as u32)) {
            stats.footpath_edges_examined += 1;
            let seconds = fp.seconds();
            let next = from.time + i64::from(seconds);
            if next > max_time { continue; }
            let candidate = Node {
                stop: fp.target().idx(),
                time: next,
                last_alight: from.last_alight,
                boardings: from.boardings,
                predecessor: Predecessor::Walk {previous: id, seconds},
                live: true,
            };
            if let Some(next_id) = round.insert(tt, special, global, nodes, candidate, stats) {
                heap.push(Reverse((next, next_id)));
                stats.walking_heap_pushes += 1;
            }
        }
    }
    stats.walking_ms += start.elapsed().as_secs_f64() * 1000.0;
}

fn marked_routes(tt: &ZeroCopyTimetable<'_>, round: &Round, seen: &mut [u8], epoch: u8) -> Vec<RouteIdx> {
    let mut routes = Vec::new();
    for &loc in &round.touched {
        for &route in tt.routes_at(LocationIdx(loc as u32)) {
            if seen[route.idx()] != epoch {
                seen[route.idx()] = epoch;
                routes.push(route);
            }
        }
    }
    routes.sort_unstable_by_key(|r| r.0);
    routes
}

#[derive(Clone, Copy)]
struct ActiveTrip<'a> {
    transport: u32,
    service_day: u16,
    start: i64,
    events: &'a [crate::routing_common::timetable::Delta],
}

fn active_route_trips<'a>(
    tt: &'a ZeroCopyTimetable<'a>,
    route: RouteIdx,
    departure: i64,
    max_time: i64,
    stats: &mut RaptorStats,
) -> Vec<ActiveTrip<'a>> {
    let base = i64::from(tt.header().first_utc_day);
    let horizon = i64::from(tt.header().day_count);
    let mut result = Vec::new();
    if horizon == 0 { return result; }
    let range = tt.route_transport_ranges()[route.idx()];
    for index in range.from..range.to {
        let transport = TransportIdx(index);
        let days = &tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()];
        let first = i64::from(tt.transport_first_dep_offset()[transport.idx()].seconds());
        let Some(events) = tt.events_for_transport(transport) else { continue };
        if events.is_empty() { continue; }
        let first_event = first + i64::from(events[0].seconds());
        let last_event = first + i64::from(events[events.len() - 1].seconds());
        let earliest = ((departure - last_event).div_euclid(86_400) - base).max(0);
        let latest = ((max_time - first_event).div_euclid(86_400) - base).min(horizon - 1);
        if earliest > latest { continue; }

        for word in (earliest as usize / 64)..=(latest as usize / 64) {
            let lo = if word == earliest as usize / 64 {earliest as usize % 64} else {0};
            let hi = if word == latest as usize / 64 {latest as usize % 64} else {63};
            let mut active = days.words[word] & (u64::MAX << lo) & (u64::MAX >> (63 - hi));
            stats.transport_days_tested += (hi - lo + 1) as u64;
            while active != 0 {
                let bit = active.trailing_zeros() as usize;
                active &= active - 1;
                let day = word * 64 + bit;
                stats.active_transport_days += 1;
                result.push(ActiveTrip {
                    transport: index,
                    service_day: day as u16,
                    start: (base + day as i64) * 86_400 + first,
                    events,
                });
            }
        }
    }
    result.sort_unstable_by_key(|trip| trip.start + i64::from(trip.events[0].seconds()));
    result
}

fn boarding_candidate(
    tt: &ZeroCopyTimetable<'_>,
    prev: &Round,
    nodes: &[Node],
    stop: usize,
) -> Option<(NodeId, i64)> {
    let mut best: Option<(NodeId, i64)> = None;
    for &id in &prev.states[stop] {
        if !nodes[id].live { continue; }
        let label = nodes[id];
        let ready = match label.last_alight {
            None => label.time,
            Some((from, alight)) => {
                let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(stop as u32));
                if rule.is_some_and(|r| r.prohibited()) { continue; }
                let seconds = tt.location_transfer_seconds()[from]
                    .max(tt.location_transfer_seconds()[stop])
                    .max(rule.map_or(0, |r| r.min_seconds));
                label.time.max(alight + i64::from(seconds))
            }
        };
        if best.is_none_or(|(_, time)| ready < time) {
            best = Some((id, ready));
        }
    }
    best
}

fn write_arrival(
    tt: &ZeroCopyTimetable<'_>, special: &[bool],
    global: &mut [Vec<NodeId>], curr: &mut Round, nodes: &mut Vec<Node>,
    route: RouteIdx, trip: ActiveTrip<'_>,
    parent: NodeId, boarded_at: i64, position: usize, stop: usize,
    max_time: i64, stats: &mut RaptorStats,
) {
    let arrival = trip.start + i64::from(trip.events[2 * position - 1].seconds());
    if arrival > max_time { return; }
    let node = Node {
        stop, time: arrival,
        last_alight: Some((stop, arrival)),
        boardings: nodes[parent].boardings + 1,
        predecessor: Predecessor::Ride {
            previous: parent, route: route.0, transport: trip.transport,
            service_day: trip.service_day, departure: boarded_at, arrival,
        },
        live: true,
    };
    curr.insert(tt, special, global, nodes, node, stats);
}

/// Stop-by-stop scan with earliest feasible transport selection, as in
/// Nigiri's update_route()/get_earliest_transport(). A chain is non-overtaking
/// on the same service day. Trips from different days that overlap can violate
/// that property, so conservatively fall back to the exact transport scan.
fn traverse(
    tt: &ZeroCopyTimetable<'_>,
    special: &[bool],
    prev: &Round,
    curr: &mut Round,
    global: &mut [Vec<NodeId>],
    nodes: &mut Vec<Node>,
    marked: &[RouteIdx],
    departure: i64,
    max_time: i64,
    stats: &mut RaptorStats,
) {
    let started = Instant::now();
    for &route in marked {
        stats.routes_scanned += 1;
        let stops = tt.stops_on(route);
        if stops.len() < 2 { continue; }
        let trips = active_route_trips(tt, route, departure, max_time, stats);
        if trips.is_empty() { continue; }
        let overlaps_days = trips.windows(2).any(|two| {
            two[0].service_day != two[1].service_day &&
                two[0].start + i64::from(two[0].events[two[0].events.len()-1].seconds()) >
                two[1].start + i64::from(two[1].events[0].seconds())
        });
        if overlaps_days {
            // Overlapping GTFS service-day instances are not globally ordered.
            for &trip in &trips {
                let mut onboard = None;
                for (position, stop) in stops.iter().enumerate() {
                    stats.stop_events_scanned += 1;
                    let location = stop.location().idx();
                    if let Some((parent, boarded)) = onboard {
                        if stop.inbound() && position > 0 {
                            write_arrival(tt, special, global, curr, nodes,
                                route, trip, parent, boarded, position,
                                location, max_time, stats);
                        }
                    }
                    if onboard.is_none() && stop.outbound() && position+1 < stops.len() {
                        let at = trip.start + i64::from(trip.events[2*position].seconds());
                        if let Some((parent, ready)) = boarding_candidate(tt,prev,nodes,location) {
                            if ready <= at && at <= max_time { onboard = Some((parent,at)); }
                        }
                    }
                }
            }
            continue;
        }

        // Sorting by first departure is sufficient inside a non-overtaking
        // Gentian chain, provided distinct active service days cannot overlap.
        let mut onboard: Option<(usize, NodeId, i64)> = None;
        for (position, stop) in stops.iter().enumerate() {
            stats.stop_events_scanned += 1;
            let location = stop.location().idx();
            if let Some((index, parent, boarded)) = onboard {
                if stop.inbound() && position > 0 {
                    write_arrival(tt,special,global,curr,nodes,route,
                        trips[index],parent,boarded,position,location,max_time,stats);
                }
            }
            if position+1 == stops.len() || !stop.outbound() || !prev.has(location) {
                continue;
            }
            let Some((parent, ready)) = boarding_candidate(tt,prev,nodes,location) else {
                continue;
            };
            if ready > max_time { continue; }
            // Each stop's departure events are monotonic within the
            // non-overtaking chain. Use a lower bound instead of a trip scan.
            let candidate = trips.partition_point(|trip| {
                trip.start + i64::from(trip.events[2*position].seconds()) < ready
            });
            let Some(trip) = trips.get(candidate) else { continue; };
            let board_time = trip.start + i64::from(trip.events[2*position].seconds());
            if board_time > max_time { continue; }
            let replace = match onboard {
                None => true,
                Some((previous, _, _)) => {
                    let current_trip = &trips[previous];
                    let current_departure = current_trip.start +
                        i64::from(current_trip.events[2*position].seconds());
                    board_time < current_departure
                }
            };
            if replace { onboard = Some((candidate,parent,board_time)); }
        }
    }
    stats.route_scan_ms += started.elapsed().as_secs_f64() * 1000.0;
}

fn reconstruct(nodes: &[Node], id: NodeId, egress: u32, departure: i64) -> Option<Journey> {
    let arrival = nodes[id].time + i64::from(egress);
    let mut legs = vec![Leg::Egress {from_stop: nodes[id].stop as u32, seconds: egress}];
    let mut cursor = id;
    let mut traversed = 0;
    let mut boardings = 0;
    loop {
        traversed += 1;
        if traversed > nodes.len() { return None; }
        let node = nodes[cursor];
        match node.predecessor {
            Predecessor::Access {seconds} => {
                legs.push(Leg::Access {to_stop: node.stop as u32, seconds});
                break;
            },
            Predecessor::Walk {previous, seconds} => {
                legs.push(Leg::Transfer {
                    from_stop: nodes[previous].stop as u32,
                    to_stop: node.stop as u32,
                    seconds,
                });
                cursor = previous;
            },
            Predecessor::Ride {previous, route, transport, service_day, departure, arrival} => {
                legs.push(Leg::Ride {
                    from_stop: nodes[previous].stop as u32,
                    to_stop: node.stop as u32,
                    route, transport, service_day,
                    departure_unix_seconds: departure,
                    arrival_unix_seconds: arrival,
                });
                cursor = previous;
                boardings += 1;
            },
        }
    }
    legs.reverse();
    debug_assert_eq!(boardings, nodes[id].boardings);
    Some(Journey {
        boardings,
        departure_unix_seconds: departure,
        arrival_unix_seconds: arrival,
        legs,
    })
}

pub(super) fn route_with_stats(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
) -> Result<(Vec<Journey>, RaptorStats)> {
    ensure!(request.max_boardings <= 16, "max_boardings exceeds supported query limit");
    ensure!((1..=7 * 86_400).contains(&request.max_duration_seconds),
        "max_duration_seconds must be between 1 second and 7 days");
    let n = tt.coordinates().len();
    for ep in request.access.iter().chain(&request.egress) {
        ensure!(ep.stop.idx() < n, "access or egress stop out of range");
    }
    let departure = request.departure_unix_seconds;
    let max_time = departure + i64::from(request.max_duration_seconds);
    let mut stats = RaptorStats::default();
    let mut special = vec![false; n];
    for rule in tt.station_transfer_rules() { special[rule.from.idx()] = true; }
    // Global nondominated state skyline, indexed by stop. Unlike the old
    // implementation this is never reset at the next boarding round.
    let mut global = vec![Vec::<NodeId>::new(); n];

    let mut nodes = Vec::new();
    let mut prev = Round::new(n);
    let mut curr = Round::new(n);
    for ep in &request.access {
        let time = departure + i64::from(ep.seconds);
        if time > max_time { continue; }
        prev.insert(tt, &special, &mut global, &mut nodes, Node {
            stop: ep.stop.idx(), time, last_alight: None,
            boardings: 0,
            predecessor: Predecessor::Access {seconds: ep.seconds},
            live: true,
        }, &mut stats);
    }
    walk_closure(tt, &special, &mut prev, &mut global, &mut nodes, max_time, &mut stats);
    let mut journeys = Vec::new();
    if let Some(seconds) = request.direct_walk_seconds {
        if seconds <= request.max_duration_seconds {
            journeys.push(Journey {
                boardings: 0, departure_unix_seconds: departure,
                arrival_unix_seconds: departure + i64::from(seconds),
                legs: vec![Leg::DirectWalk {seconds}],
            });
        }
    }
    let mut seen = vec![0u8; tt.route_transport_ranges().len()];
    // Round k contains exactly k boardings. No carry labels or copied arrays:
    // predecessor node IDs are stable throughout the entire query.
    for round in 0..=request.max_boardings {
        if let Some((id, seconds)) = request.egress.iter()
            .flat_map(|ep| prev.states[ep.stop.idx()].iter()
                .map(move |&id| (id, ep.seconds)))
            .filter(|(id, seconds)| nodes[*id].live && nodes[*id].time + i64::from(*seconds) <= max_time)
            .min_by_key(|(id, seconds)| nodes[*id].time + i64::from(*seconds))
        {
            if let Some(journey) = reconstruct(&nodes, id, seconds, departure) {
                journeys.push(journey);
            }
        }
        if round == request.max_boardings || prev.touched.is_empty() { break; }
        stats.rounds += 1;
        stats.marked_stops += prev.touched.len() as u64;
        curr.clear();
        let marked = marked_routes(tt, &prev, &mut seen, (round + 1) as u8);
        if marked.is_empty() { break; }
        traverse(tt, &special, &prev, &mut curr, &mut global, &mut nodes, &marked,
            departure, max_time, &mut stats);
        walk_closure(tt, &special, &mut curr, &mut global, &mut nodes, max_time, &mut stats);
        std::mem::swap(&mut prev, &mut curr);
    }
    journeys.sort_by_key(|j| (j.boardings, j.arrival_unix_seconds));
    let mut frontier = Vec::new();
    let mut best_arrival = INF;
    for journey in journeys {
        if journey.arrival_unix_seconds < best_arrival {
            best_arrival = journey.arrival_unix_seconds;
            frontier.push(journey);
        }
    }
    Ok((frontier, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, PackedFootpath, PackedStop, Range, SourceIdx,
        StationTransferRule, TimetableOwned, TimingProfileIdx, TrafficDays,
    };

    fn fixture(prohibit: bool) -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 2);
        tt.add_source("test", "f", "a").unwrap();
        for _ in 0..5 {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
        }
        tt.location_routes_offsets = vec![0, 2, 3, 4, 5, 6];
        tt.location_routes = vec![
            RouteIdx(0), RouteIdx(1), RouteIdx(0), RouteIdx(1),
            RouteIdx(2), RouteIdx(2),
        ];
        tt.footpaths_out_offsets = vec![0, 0, 1, 2, 2, 2];
        tt.footpaths_out = vec![
            PackedFootpath::new(LocationIdx(3), 60).unwrap(),
            PackedFootpath::new(LocationIdx(3), 60).unwrap(),
        ];
        tt.footpaths_in_offsets = vec![0, 0, 0, 0, 2, 2];
        tt.footpaths_in = vec![
            PackedFootpath::new(LocationIdx(1), 60).unwrap(),
            PackedFootpath::new(LocationIdx(2), 60).unwrap(),
        ];
        for &(a,b) in &[(0,1),(0,2),(3,4)] {
            tt.route_stops.extend([
                PackedStop::new(LocationIdx(a), true,true,true,true).unwrap(),
                PackedStop::new(LocationIdx(b), true,true,true,true).unwrap(),
            ]);
        }
        tt.route_stop_offsets.extend([2,4,6]);
        tt.route_transport_ranges.extend([
            Range {from:0,to:1}, Range {from:1,to:2}, Range {from:2,to:3},
        ]);
        tt.route_clasz.extend([10,10,10]);
        tt.timing_profile_offsets.extend([2,4,6]);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(300).unwrap(),
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(360).unwrap(),
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(420).unwrap(),
        ]);
        tt.transport_route.extend([RouteIdx(0), RouteIdx(1), RouteIdx(2)]);
        tt.transport_timing_profile.extend([
            TimingProfileIdx(0), TimingProfileIdx(1), TimingProfileIdx(2),
        ]);
        tt.transport_first_dep_offset.extend([
            Delta::from_seconds(8*3600).unwrap(),
            Delta::from_seconds(8*3600).unwrap(),
            Delta::from_seconds(8*3600+8*60).unwrap(),
        ]);
        tt.transport_initial_day_offset.extend([0,0,0]);
        let mut days=TrafficDays::default();
        days.set(0,true).unwrap();
        tt.bitfields.push(days);
        tt.transport_traffic_days.extend([BitfieldIdx(1);3]);
        tt.route_traffic_days.extend([BitfieldIdx(1);3]);
        tt.route_section_clasz_offsets.extend([0,0,0]);
        tt.section_flag_offsets.extend([0,0,0]);
        tt.transport_to_trip_section_offsets.extend([0,0,0]);
        if prohibit {
            tt.station_transfer_rules.push(StationTransferRule {
                from: LocationIdx(1), to: LocationIdx(3),
                min_seconds: 0, flags: 1,
            });
        }
        tt
    }

    #[test]
    fn later_arrival_with_permitted_transfer_is_not_pruned() {
        let tt = fixture(true);
        let bytes=tt.encode().unwrap();
        let view=ZeroCopyTimetable::new(&bytes).unwrap();
        let departure=20_000_i64*86_400+7*3600+59*60;
        let (journeys, stats) = route_with_stats(&view, &Query {
            departure_unix_seconds: departure,
            access: vec![super::super::WalkingEndpoint {stop: LocationIdx(0),seconds:0}],
            egress: vec![super::super::WalkingEndpoint {stop: LocationIdx(4),seconds:0}],
            direct_walk_seconds: None, max_boardings:2, max_duration_seconds:3600,
        }).unwrap();
        assert_eq!(journeys.len(),1);
        assert_eq!(journeys[0].boardings,2);
        assert_eq!(journeys[0].arrival_unix_seconds,20_000_i64*86_400+8*3600+15*60);
        assert!(stats.labels_improved >= 5);
    }

    #[test]
    fn uniform_transfer_rules_allow_cross_source_dominance() {
        let tt = fixture(false);
        let bytes=tt.encode().unwrap();
        let view=ZeroCopyTimetable::new(&bytes).unwrap();
        let mut nodes=Vec::new();
        let mut global=vec![Vec::<NodeId>::new();5];
        let mut round=Round::new(5);
        let mut stats=RaptorStats::default();
        let make = |time, alight, origin| Node {
            stop: 3, time, last_alight: Some((origin,alight)),
            boardings: 1, predecessor: Predecessor::Access{seconds:0}, live:true,
        };
        assert!(round.insert(&view,&[false;5],&mut global,&mut nodes,make(1020,1000,1),&mut stats).is_some());
        assert!(round.insert(&view,&[false;5],&mut global,&mut nodes,make(1030,1010,2),&mut stats).is_none());
        assert_eq!(round.states[3].len(),1);
    }

    #[test]
    fn same_source_time_vs_alighting_tradeoff_preserves_both() {
        let tt=fixture(false);
        let bytes=tt.encode().unwrap();
        let view=ZeroCopyTimetable::new(&bytes).unwrap();
        let mut nodes=Vec::new();
        let mut global=vec![Vec::<NodeId>::new();5];
        let mut round=Round::new(5);
        let mut stats=RaptorStats::default();
        let make = |time, alight| Node {
            stop:3, time, last_alight:Some((1,alight)),
            boardings:1, predecessor:Predecessor::Access{seconds:0}, live:true,
        };
        round.insert(&view,&[false;5],&mut global,&mut nodes,make(1010,1000),&mut stats);
        round.insert(&view,&[false;5],&mut global,&mut nodes,make(1020,950),&mut stats);
        assert_eq!(round.states[3].len(),2);
    }

    #[test]
    fn global_best_skips_redundant_later_round_state() {
        let tt = fixture(false);
        let binary = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&binary).unwrap();
        let mut nodes = Vec::new();
        let mut global = vec![Vec::<NodeId>::new(); 5];
        let special = vec![false; 5];
        let mut round0 = Round::new(5);
        let mut round1 = Round::new(5);
        let mut stats = RaptorStats::default();
        let early = Node {
            stop: 3, time: 1_000, last_alight: None, boardings: 0,
            predecessor: Predecessor::Access { seconds: 0 }, live: true,
        };
        let late = Node {
            stop: 3, time: 1_100, last_alight: Some((1, 1_080)), boardings: 1,
            predecessor: Predecessor::Access { seconds: 0 }, live: true,
        };
        assert!(round0.insert(&view, &special, &mut global, &mut nodes, early, &mut stats).is_some());
        assert!(round1.insert(&view, &special, &mut global, &mut nodes, late, &mut stats).is_none());
        assert!(round1.touched.is_empty());
        assert_eq!(global[3].len(), 1);
        assert_eq!(stats.labels_dominated, 1);
    }

    #[test]
    fn first_useful_transport_scan_preserves_permitted_later_interchange() {
        let tt = fixture(true);
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let departure = 20_000_i64 * 86_400 + 7 * 3_600 + 59 * 60;
        let (journeys, stats) = route_with_stats(&view, &Query {
            departure_unix_seconds: departure,
            access: vec![super::super::WalkingEndpoint {
                stop: LocationIdx(0), seconds: 0,
            }],
            egress: vec![super::super::WalkingEndpoint {
                stop: LocationIdx(4), seconds: 0,
            }],
            direct_walk_seconds: None, max_boardings: 2, max_duration_seconds: 3600,
        }).unwrap();
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].boardings, 2);
        assert_eq!(journeys[0].arrival_unix_seconds,
            20_000_i64 * 86_400 + 8 * 3_600 + 15 * 60);
        // Repeatedly reopening a dominated walking history is the previous
        // regression; this small fixture needs only a bounded frontier.
        assert!(stats.walking_heap_pushes < 100);
    }

    #[test]
    fn no_rule_fixture_still_returns_fastest_connection() {
        let tt=fixture(false);
        let bytes=tt.encode().unwrap();
        let view=ZeroCopyTimetable::new(&bytes).unwrap();
        let departure=20_000_i64*86_400+7*3600+59*60;
        let (journeys, _) = route_with_stats(&view,&Query {
            departure_unix_seconds:departure,
            access:vec![super::super::WalkingEndpoint{stop:LocationIdx(0),seconds:0}],
            egress:vec![super::super::WalkingEndpoint{stop:LocationIdx(4),seconds:0}],
            direct_walk_seconds:None,max_boardings:2,max_duration_seconds:3600,
        }).unwrap();
        assert_eq!(journeys[0].boardings,2);
        assert_eq!(journeys[0].legs.iter().filter(|l|matches!(l,Leg::Ride{..})).count(),2);
    }
}
