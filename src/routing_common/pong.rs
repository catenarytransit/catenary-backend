//! Static, no-via port of Nigiri's CPU PONG search.
//!
//! Upstream: motis-project/nigiri@82e55cedfc184ea3a33082619ba937d172e9c575,
//! src/routing/raptor/pong.cc. MIT license: third_party/nigiri-LICENSE.
//! The PING/PONG controller follows upstream's sweep order, known-result
//! handling, weighted count and strict cursor validation. CPU wall time is
//! checked between complete sweeps, never inside a point search.
//! Gentian's adapter uses seconds and boardings (upstream uses minutes and
//! transfers). Intermodal start footpaths are disabled in both directions.
//! The static scan uses Nigiri's best_/tmp_/round_times_ label model.
//! Only selected PONG matches are reconstructed, from round times and events.

use crate::routing_common::profile_compact::LowerBoundTopology;
use crate::routing_common::nigiri_raptor::{NigiriRaptor, PointJourney};
use crate::routing_common::profile_compact_reconstruct::valid_journey;
use crate::routing_common::profile_search::{ProfileOptions, sort_journeys_for_response};
use crate::routing_common::raptor_query::{Journey, Leg, Query, RaptorStats, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, ZeroCopyTimetable};
use crate::routing_common::transport_index::TransportIndex;
use anyhow::{Result, ensure};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};

const DAY: u32 = 86_400;
const MAX_TRAVEL_TIME: u32 = 5 * DAY;
// Nigiri pong.cc (4701529b): six days avoids returning the same weekday in
// the following/previous week. This bounds the cursor, not a trip's duration.
const MAX_LOOK_AHEAD: u32 = 6 * DAY;
const MAX_BOARDINGS: usize = 15; // nigiri::routing::kMaxTransfers + 1

/// The key is in *search direction*, separately from reconstructed physical
/// leg times. In particular a ping starts at the query cursor even when the
/// first vehicle leaves later. A pong starts at the ping's destination time.
#[derive(Clone)]
struct Candidate {
    start_time: i64,
    dest_time: i64,
    boardings: usize,
    journey: Option<Journey>,
}

impl Candidate {
    fn point(point: PointJourney) -> Self {
        Self { start_time: point.start_time, dest_time: point.dest_time,
            boardings: point.boardings, journey: None }
    }

    /// nigiri::routing::journey::dominates, including opposite-oriented keys.
    fn dominates(&self, other: &Self) -> bool {
        self.boardings <= other.boardings
            && if self.start_time <= self.dest_time {
                self.start_time >= other.start_time && self.dest_time <= other.dest_time
            } else {
                self.start_time <= other.start_time && self.dest_time >= other.dest_time
            }
    }

    fn travel_time(&self) -> u64 {
        self.dest_time.abs_diff(self.start_time)
    }
}

fn pareto_add(results: &mut Vec<Candidate>, candidate: Candidate) {
    if results.iter().any(|old| old.dominates(&candidate)) {
        return;
    }
    results.retain(|old| !candidate.dominates(old));
    results.push(candidate);
}

fn better(a: i64, b: i64, reverse: bool) -> bool {
    if reverse { a > b } else { a < b }
}

fn step(time: i64, seconds: i64, reverse: bool) -> i64 {
    if reverse { time.saturating_sub(seconds) } else { time.saturating_add(seconds) }
}

/// get_fastest_direct: the supplied OSR duration takes precedence. Otherwise
/// use the same static endpoint/footpath search as Nigiri (only destination
/// locations may be intermediate footpath targets).
fn fastest_direct(tt: &ZeroCopyTimetable<'_>, q: &Query, reverse: bool) -> Option<u32> {
    if q.direct_walk_seconds.is_some() {
        return q.direct_walk_seconds;
    }
    let (starts, ends) = if reverse { (&q.egress, &q.access) } else { (&q.access, &q.egress) };
    let mut targets = HashMap::<u32, u32>::new();
    for ep in ends {
        targets.entry(ep.stop.0).and_modify(|d| *d = (*d).min(ep.seconds)).or_insert(ep.seconds);
    }
    let mut distances = HashMap::<u32, u32>::new();
    let mut heap = BinaryHeap::new();
    for ep in starts {
        let old = distances.entry(ep.stop.0).or_insert(u32::MAX);
        if ep.seconds < *old {
            *old = ep.seconds;
            heap.push(Reverse((ep.seconds, ep.stop.0)));
        }
    }
    let mut best = u32::MAX;
    while let Some(Reverse((distance, stop))) = heap.pop() {
        if distances.get(&stop).is_some_and(|&old| old < distance) {
            continue;
        }
        if let Some(&offset) = targets.get(&stop) {
            best = best.min(distance.saturating_add(offset));
        }
        let paths = if reverse { tt.footpaths_to(LocationIdx(stop)) } else { tt.footpaths_from(LocationIdx(stop)) };
        for path in paths {
            if !targets.contains_key(&path.target().0) {
                continue;
            }
            let distance = distance.saturating_add(path.seconds());
            let old = distances.entry(path.target().0).or_insert(u32::MAX);
            if distance < *old {
                *old = distance;
                heap.push(Reverse((distance, path.target().0)));
            }
        }
    }
    (best != u32::MAX).then_some(best)
}

/// Request-local controls. No search state survives the call. A streaming
/// observer receives replacement snapshots only after cursor validation.
#[derive(Default)]
pub struct PongControl<'a> {
    pub deadline: Option<Instant>,
    pub cancelled: Option<&'a AtomicBool>,
    /// Streaming can keep looking past the initial minimum until the budget
    /// or the timetable/lookahead boundary is reached.
    pub continue_until_budget: bool,
    pub emit_updates: bool,
    /// Count-based HTTP pages must not count unreconstructed side products.
    pub require_reconstructed_count: bool,
}
impl PongControl<'_> {
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
    }
}

pub type PongObserver<'a> = dyn FnMut(&[Journey], &RaptorStats) -> Result<()> + 'a;

fn validated_journeys(
    results: &[Candidate],
    cursor: i64,
    lookahead_end: i64,
    reverse: bool,
    fastest_direct: u64,
    max_travel_time: u32,
) -> Vec<Journey> {
    let mut journeys = results
        .iter()
        .filter(|j| {
            j.journey.is_some()
                && better(j.dest_time, cursor, reverse)
                && better(j.dest_time, lookahead_end, reverse)
                && j.travel_time() < fastest_direct
                && j.travel_time() <= u64::from(max_travel_time)
        })
        .map(|j| {
            let mut journey = j.journey.clone().unwrap();
            journey.departure_unix_seconds = if reverse { j.start_time } else { j.dest_time };
            journey.arrival_unix_seconds = if reverse { j.dest_time } else { j.start_time };
            journey
        })
        .collect::<Vec<_>>();
    sort_journeys_for_response(&mut journeys, reverse);
    journeys
}

pub(crate) fn route_pong_with_stats_precomputed(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    options: ProfileOptions,
    arrive_by: bool,
    topology: Option<&LowerBoundTopology>,
) -> Result<(Vec<Journey>, RaptorStats)> {
    route_pong_observed(tt, request, options, arrive_by, topology,
        &PongControl::default(), &mut |_, _| Ok(()))
}

pub(crate) fn route_pong_observed(
    tt: &ZeroCopyTimetable<'_>, request: &Query, options: ProfileOptions,
    arrive_by: bool, topology: Option<&LowerBoundTopology>,
    control: &PongControl<'_>, observer: &mut PongObserver<'_>,
) -> Result<(Vec<Journey>, RaptorStats)> {
    options.validate()?;
    let processing_start = Instant::now();
    let local_deadline = processing_start
        .checked_add(Duration::from_secs(u64::from(options.timeout_seconds)))
        .unwrap_or(processing_start);
    let deadline = control.deadline.unwrap_or(local_deadline).min(local_deadline);
    let max_travel_time = options.max_journey_duration_seconds.min(MAX_TRAVEL_TIME);
    let lookahead = max_travel_time.saturating_add(DAY).min(MAX_TRAVEL_TIME);
    let fastest_direct = fastest_direct(tt, request, arrive_by).map(u64::from).unwrap_or(u64::MAX);
    let reference = request.departure_unix_seconds;
    let timetable_from = i64::from(tt.header().first_utc_day) * i64::from(DAY);
    let timetable_to = timetable_from.saturating_add(i64::from(tt.header().day_count) * i64::from(DAY));
    ensure!(timetable_from < timetable_to, "PONG requires a nonempty timetable interval");
    // Half-open, as in MOTIS get_start_time. Include the exact requested
    // arrive-by second; forward searches exclude the far endpoint.
    let (interval_from, interval_to) = if arrive_by {
        (reference.checked_sub(i64::from(options.search_window_seconds)), reference.checked_add(1))
    } else {
        (Some(reference), reference.checked_add(i64::from(options.search_window_seconds)))
    };
    let interval_from = interval_from.ok_or_else(|| anyhow::anyhow!("PONG interval timestamp overflow"))?
        .clamp(timetable_from, timetable_to);
    let interval_to = interval_to.ok_or_else(|| anyhow::anyhow!("PONG interval timestamp overflow"))?
        .clamp(timetable_from, timetable_to);
    let lookahead_end = step(
        if arrive_by { interval_to } else { interval_from },
        i64::from(MAX_LOOK_AHEAD), arrive_by,
    );
    let mut cursor = if arrive_by { interval_to.saturating_sub(1) } else { interval_from };
    let end_time = if arrive_by { interval_from.saturating_sub(1) } else { interval_to };

    let mut kernel_query = request.clone();
    kernel_query.max_boardings = request.max_boardings.min(MAX_BOARDINGS);
    kernel_query.max_duration_seconds = MAX_TRAVEL_TIME;
    // PING deliberately ignores fastest_direct and admits the extra day of
    // waiting. Apply duration/direct filtering only to validated pong keys.
    kernel_query.direct_walk_seconds = None;
    let mut state = NigiriRaptor::new(tt, &kernel_query, arrive_by, topology)?;
    let transports = state.transport_index();
    // kPruneWithPingBounds: reverse lower bounds start at zero. Swap vectors
    // when changing direction instead of allocating a second RAPTOR state.
    let mut other_lower_bounds = vec![0; tt.coordinates().len()];
    let mut results = Vec::<Candidate>::new();
    let mut iterations = 0;
    let mut forward_searches = 0;
    let mut backward_searches = 0;
    let mut known_result_hits = 0;
    let mut seeded_destination_bounds = 0;

    loop {
        let result_count = |include_too_slow: bool| {
            results.iter().filter(|j| {
                better(j.dest_time, cursor, arrive_by)
                    && (include_too_slow || (j.travel_time() < fastest_direct
                        && j.travel_time() <= u64::from(max_travel_time)))
            }).count()
        };
        // Preserve the upstream weighted count, including unvalidated-path
        // byproducts only after their destination key is behind the cursor.
        let enough_results = if control.require_reconstructed_count {
            results.iter().filter(|j| j.journey.is_some()
                && better(j.dest_time, cursor, arrive_by)
                && better(j.dest_time, lookahead_end, arrive_by)
                && j.travel_time() < fastest_direct
                && j.travel_time() <= u64::from(max_travel_time)).count() >= options.min_itineraries
        } else { result_count(true) + result_count(false) >= 2 * options.min_itineraries };
        if (!control.continue_until_budget && !better(cursor, end_time, arrive_by)
            && enough_results)
            || !better(cursor, lookahead_end, arrive_by)
            || cursor < timetable_from || cursor >= timetable_to
            || Instant::now() >= deadline || control.is_cancelled()
        {
            break;
        }
        iterations += 1;

        // PING: reset arrivals, start at the cursor, search with one day's
        // lookahead, and collect the strict destination/boarding frontier.
        state.reset_point_arrivals();
        // Nigiri pong.cc:288-292. Results have the opposite orientation:
        // dest_time is the validated origin time, start_time the destination
        // time. A result whose origin is still ahead of the cursor bounds
        // this ping at its boarding count and every larger count.
        for result in &results {
            if !better(result.dest_time, cursor, arrive_by) {
                state.limit_time_at_dest(result.boardings, result.start_time);
                seeded_destination_bounds += 1;
            }
        }
        let mut ping_query = kernel_query.clone();
        ping_query.max_duration_seconds = lookahead;
        let ping = state.execute_point(
            tt, &ping_query, cursor, kernel_query.max_boardings,
            step(cursor, i64::from(lookahead), arrive_by),
        )?;
        if arrive_by { backward_searches += 1; } else { forward_searches += 1; }
        let mut ping_results = Vec::<Candidate>::new();
        for journey in ping {
            let candidate = Candidate::point(journey);
            if !results.iter().any(|old| old.dominates(&candidate)) {
                pareto_add(&mut ping_results, candidate);
            }
        }
        if ping_results.is_empty() {
            break;
        }
        ping_results.sort_by_key(|j| Reverse(j.boardings));

        // PONG: fold and save ping's bounds BEFORE reversing/resetting the
        // shared state. Most transfers first, then retain arrivals between
        // the remaining pong runs exactly as raptor::next_start_time does.
        let ping_bounds = state.fill_ping_bounds(ping_results[0].boardings);
        state.flip_point_direction(&mut other_lower_bounds);
        state.set_ping_bounds(ping_bounds);
        for ping_j in &mut ping_results {
            ensure!(!control.is_cancelled(), "routing request cancelled");
            // Nigiri pong.cc:362-375. An earlier pong may already have
            // established the latest departure for this arrival/boarding
            // pair. Reuse its oriented key instead of searching again.
            if let Some(known) = results.iter().find(|pong_j| {
                pong_j.boardings == ping_j.boardings && pong_j.start_time == ping_j.dest_time
            }) {
                ping_j.start_time = known.dest_time;
                known_result_hits += 1;
                continue;
            }
            let pong = state.execute_point(
                tt, &kernel_query, ping_j.dest_time, ping_j.boardings,
                ping_j.start_time,
            )?;
            if arrive_by { forward_searches += 1; } else { backward_searches += 1; }
            for journey in pong {
                pareto_add(&mut results, Candidate::point(journey));
            }
            let matched = results.iter_mut().find(|pong_j| {
                pong_j.boardings == ping_j.boardings && pong_j.start_time == ping_j.dest_time
            }).ok_or_else(|| anyhow::anyhow!(
                "no pong for boardings={}, start_time={} found", ping_j.boardings, ping_j.dest_time
            ))?;
            if matched.journey.is_none() {
                state.stats_.profile_journeys_reconstructed += 1;
                let point = PointJourney { start_time: matched.start_time,
                    dest_time: matched.dest_time, boardings: matched.boardings };
                let journey = crate::routing_common::nigiri_reconstruct::reconstruct(&state, tt, &kernel_query, point);
                if journey.is_err() { state.stats_.profile_journeys_rejected += 1; }
                matched.journey = Some(journey?);
            }
            ping_j.start_time = matched.dest_time;
        }
        state.flip_point_direction(&mut other_lower_bounds);

        let first = ping_results.iter().map(|j| j.start_time)
            .min_by(|a, b| if arrive_by { b.cmp(a) } else { a.cmp(b) })
            .expect("nonempty ping results");
        let next = step(first, 1, arrive_by);
        ensure!(better(cursor, next, arrive_by), "no pong progress: start_time={cursor}, next={next}");
        cursor = next;
        ensure!(!control.is_cancelled(), "routing request cancelled");
        if control.emit_updates {
            let snapshot = validated_journeys(&results, cursor, lookahead_end, arrive_by,
                fastest_direct, max_travel_time);
            if !snapshot.is_empty() {
                let mut progress = state.stats().clone();
                progress.pong_iterations = iterations;
                progress.pong_forward_searches = forward_searches;
                progress.pong_backward_searches = backward_searches;
                progress.pong_known_result_hits = known_result_hits;
                progress.pong_seeded_destination_bounds = seeded_destination_bounds;
                let searched = if better(lookahead_end, cursor, arrive_by) { lookahead_end } else { cursor };
                progress.pong_interval_from = Some(if arrive_by { searched.saturating_add(1) } else { interval_from });
                progress.pong_interval_to = Some(if arrive_by { interval_to } else { searched });
                observer(&snapshot, &progress)?;
            }
        }
    }

    ensure!(!control.is_cancelled(), "routing request cancelled");
    // Only matched, reconstructed and cursor-validated journeys count as
    // output. No near-Pareto route extras, fallback profile scan, duration
    // ratio heuristic, or initial interval_estimator enlargement is applied.
    let before_filter = results.len();
    let mut journeys = validated_journeys(&results, cursor, lookahead_end, arrive_by,
        fastest_direct, max_travel_time);
    let mut stats = state.stats().clone();
    stats.profile_duration_filtered = (before_filter - journeys.len()) as u64;
    stats.profile_budget_exhausted |= Instant::now() >= deadline;
    stats.pong_iterations = iterations;
    stats.pong_forward_searches = forward_searches;
    stats.pong_backward_searches = backward_searches;
    stats.pong_known_result_hits = known_result_hits;
    stats.pong_seeded_destination_bounds = seeded_destination_bounds;
    let searched_until = if better(lookahead_end, cursor, arrive_by) { lookahead_end } else { cursor };
    stats.pong_interval_from = Some(if arrive_by { searched_until.saturating_add(1) } else { interval_from });
    stats.pong_interval_to = Some(if arrive_by { interval_to } else { searched_until });

    // Nigiri's final forward-only refinement replaces the middle vehicle in
    // ride/walk/ride/walk/ride windows with the earliest feasible direct one.
    // There are no vias or realtime/TD footpaths in Edelweiss's Query type.
    if !arrive_by {
        for journey in &mut journeys {
            improve_middle_rides(tt, &transports, request, journey, &mut stats, deadline);
        }
    }
    if let Some(seconds) = request.direct_walk_seconds.filter(|&d| d <= max_travel_time) {
        let (departure, arrival) = if arrive_by {
            (reference.saturating_sub(i64::from(seconds)), reference)
        } else {
            (reference, reference.saturating_add(i64::from(seconds)))
        };
        journeys.push(Journey {
            boardings: 0, departure_unix_seconds: departure,
            arrival_unix_seconds: arrival, legs: vec![Leg::DirectWalk { seconds }],
        });
    }
    sort_journeys_for_response(&mut journeys, arrive_by);
    Ok((journeys, stats))
}

fn alternative_walks(tt: &ZeroCopyTimetable<'_>, stop: u32, outgoing: bool) -> HashMap<u32, u32> {
    // lookup_footpath with kExact: a reflexive connection includes the
    // station change time; a non-reflexive connection uses its footpath.
    let mut walks = HashMap::from([(stop, tt.location_transfer_seconds()[stop as usize])]);
    let paths = if outgoing { tt.footpaths_from(LocationIdx(stop)) } else { tt.footpaths_to(LocationIdx(stop)) };
    for path in paths {
        walks.entry(path.target().0).and_modify(|d| *d = (*d).min(path.seconds())).or_insert(path.seconds());
    }
    walks
}

fn change_time(tt: &ZeroCopyTimetable<'_>, from: u32, to: u32) -> Option<u32> {
    let rule = tt.station_transfer_rule(LocationIdx(from), LocationIdx(to));
    if rule.is_some_and(|r| r.prohibited()) {
        return None;
    }
    Some(tt.location_transfer_seconds()[from as usize]
        .max(tt.location_transfer_seconds()[to as usize])
        .max(rule.map_or(0, |r| r.min_seconds)))
}

/// Static counterpart of get_earliest_alternative/get_direct_journeys. FIFO
/// compressed chains give each route-pair generator's first feasible trip;
/// the minimum alighting-walk arrival is the upstream merge-heap's head.
fn earliest_alternative(
    tt: &ZeroCopyTimetable<'_>,
    transports: &TransportIndex,
    from: u32,
    to: u32,
    from_arrival: i64,
    to_departure: i64,
    stats: &mut RaptorStats,
    deadline: Instant,
) -> Option<[Leg; 3]> {
    let boarding = alternative_walks(tt, from, true);
    let alighting = alternative_walks(tt, to, false);
    let from_routes = boarding.keys().flat_map(|&s| tt.routes_at(LocationIdx(s)).iter().copied()).collect::<HashSet<_>>();
    let to_routes = alighting.keys().flat_map(|&s| tt.routes_at(LocationIdx(s)).iter().copied()).collect::<HashSet<_>>();
    let mut routes = from_routes.intersection(&to_routes).copied().collect::<Vec<_>>();
    routes.sort_unstable_by_key(|r| r.0);
    let mut best = None;
    let mut best_arrival = i64::MAX;
    for route in routes {
        if Instant::now() >= deadline { break; }
        // direct.cc::for_each_pair: first boarding, next alighting, then
        // continue after that pair. Repeated stop indices remain distinct.
        let mut board_position: Option<usize> = None;
        for (pos, stop) in tt.stops_on(route).iter().enumerate() {
            if let Some(board_pos) = board_position {
                let Some(&egress) = alighting.get(&stop.location().0).filter(|_| stop.inbound()) else { continue; };
                board_position = None;
                let board = tt.stops_on(route)[board_pos];
                let access = boarding[&board.location().0];
                let Some(change_before) = change_time(tt, from, board.location().0) else { continue; };
                let Some(change_after) = change_time(tt, stop.location().0, to) else { continue; };
                let ready = from_arrival.saturating_add(i64::from(access.max(change_before)));
                let limit = to_departure.saturating_sub(i64::from(egress.max(change_after)));
                for chain in transports.chains(route) {
                    let Some(selected) = chain.select(tt, board_pos * 2, ready, false, limit, 0, stats) else { continue; };
                    let midnight = (i64::from(tt.header().first_utc_day) + selected.start_day as i64) * i64::from(DAY);
                    let Some(arrival) = transport_event_second(tt, route, selected.transport, pos, true, midnight) else { continue; };
                    let walk_arrival = arrival.saturating_add(i64::from(egress));
                    if arrival > limit || walk_arrival >= best_arrival { continue; }
                    best_arrival = walk_arrival;
                    best = Some([
                        Leg::Transfer { from_stop: from, to_stop: board.location().0, seconds: access },
                        Leg::Ride {
                            from_stop: board.location().0, to_stop: stop.location().0,
                            route: route.0, transport: selected.transport.0, service_day: selected.start_day as u16,
                            board_stop_index: board_pos, alight_stop_index: pos,
                            departure_unix_seconds: selected.event, arrival_unix_seconds: arrival,
                        },
                        Leg::Transfer { from_stop: stop.location().0, to_stop: to, seconds: egress },
                    ]);
                }
            } else if stop.outbound() && boarding.contains_key(&stop.location().0) {
                board_position = Some(pos);
            }
        }
    }
    best
}

fn improve_middle_rides(
    tt: &ZeroCopyTimetable<'_>,
    transports: &TransportIndex,
    q: &Query,
    journey: &mut Journey,
    stats: &mut RaptorStats,
    deadline: Instant,
) {
    if journey.legs.len() < 5 { return; }
    for i in 0..=journey.legs.len() - 5 {
        let (
            Leg::Ride { to_stop: from, arrival_unix_seconds: arrival, .. },
            Leg::Transfer { .. }, Leg::Ride { .. }, Leg::Transfer { .. },
            Leg::Ride { from_stop: to, departure_unix_seconds: departure, .. },
        ) = (&journey.legs[i], &journey.legs[i + 1], &journey.legs[i + 2], &journey.legs[i + 3], &journey.legs[i + 4]) else { continue; };
        let Some(replacement) = earliest_alternative(tt, transports, *from, *to, *arrival, *departure, stats, deadline) else { continue; };
        let mut improved = journey.clone();
        for (offset, leg) in replacement.into_iter().enumerate() {
            improved.legs[i + 1 + offset] = leg;
        }
        // Pair-specific GTFS restrictions and Catenary's exact footpath
        // representation remain authoritative after the upstream refinement.
        if valid_journey(tt, q, &improved) {
            *journey = improved;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::raptor_query::WalkingEndpoint;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, PackedStop, Range, RouteIdx, SourceIdx, StationTransferRule,
        TimetableOwned, TimingProfileIdx, TrafficDays,
    };

    // A slow direct train (08:00-09:00) competes with a faster two-ride
    // connection (08:10-08:30). The next ping rediscovers a known PONG
    // candidate while another known result can bound its destination time.
    fn timetable(trips: &[(u32, u32, u32, u32)], prohibit_change: bool) -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 2);
        tt.add_source("test", "pong-reuse", "attempt").unwrap();
        let n = trips
            .iter()
            .map(|t| t.0.max(t.1) + 1)
            .max()
            .unwrap_or(0)
            .max(3);
        for _ in 0..n {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
        }
        tt.location_routes_offsets = vec![0];
        for stop in 0..n {
            for (r, &(from, to, _, _)) in trips.iter().enumerate() {
                if from == stop || to == stop {
                    tt.location_routes.push(RouteIdx(r as u32));
                }
            }
            tt.location_routes_offsets
                .push(tt.location_routes.len() as u32);
        }
        tt.footpaths_out_offsets = vec![0; n as usize + 1];
        tt.footpaths_in_offsets = vec![0; n as usize + 1];
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        tt.bitfields.push(days);
        for (r, &(from, to, departure, duration)) in trips.iter().enumerate() {
            tt.route_stops.extend([
                PackedStop::new(LocationIdx(from), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(to), true, true, true, true).unwrap(),
            ]);
            tt.route_stop_offsets.push((2 * (r + 1)) as u32);
            tt.route_transport_ranges.push(Range {
                from: r as u32,
                to: r as u32 + 1,
            });
            tt.timing_profile_offsets.push((2 * (r + 1)) as u32);
            tt.timing_profile_events.extend([
                Delta::from_seconds(0).unwrap(),
                Delta::from_seconds(duration).unwrap(),
            ]);
            tt.route_clasz.push(2);
            tt.transport_route.push(RouteIdx(r as u32));
            tt.transport_timing_profile.push(TimingProfileIdx(r as u32));
            tt.transport_first_dep_offset
                .push(Delta::from_seconds(departure).unwrap());
            tt.transport_initial_day_offset.push(0);
            tt.transport_traffic_days.push(BitfieldIdx(1));
            tt.route_traffic_days.push(BitfieldIdx(1));
            tt.route_section_clasz_offsets.push(0);
            tt.section_flag_offsets.push(0);
            tt.transport_to_trip_section_offsets.push(0);
        }
        if prohibit_change {
            tt.station_transfer_rules.push(StationTransferRule {
                from: LocationIdx(1),
                to: LocationIdx(1),
                min_seconds: 0,
                flags: 1,
            });
        }
        tt
    }

    fn competing_trains(prohibit_change: bool) -> TimetableOwned {
        timetable(
            &[
                (0, 2, 8 * 3_600, 3_600),
                (0, 1, 8 * 3_600 + 10 * 60, 10 * 60),
                (1, 2, 8 * 3_600 + 25 * 60, 5 * 60),
            ],
            prohibit_change,
        )
    }

    #[test]
    fn minute_aligned_pong_fixtures_for_native_comparison() {
        let mut fixtures: Vec<(Vec<(u32, u32, u32, u32)>, u32, u32, u32, usize, u32, u32)> = vec![
            (
                vec![
                    (0, 2, 8 * 3600, 3600),
                    (0, 1, 8 * 3600 + 10 * 60, 10 * 60),
                    (1, 2, 8 * 3600 + 25 * 60, 5 * 60),
                ],
                8 * 3600,
                9 * 3600,
                900,
                1,
                0,
                0,
            ),
            // Nigiri test/routing/pong_gap_truncation_test.cc's fixture.
            (
                vec![
                    (0, 2, 9 * 3600 + 59 * 60, 61 * 60),
                    (0, 1, 10 * 3600 + 5 * 60, 15 * 60),
                    (1, 2, 10 * 3600 + 30 * 60, 30 * 60),
                    (0, 2, 15 * 3600, 30 * 60),
                ],
                10 * 3600,
                16 * 3600,
                180,
                2,
                0,
                0,
            ),
            (
                vec![
                    (0, 1, 23 * 3600 + 55 * 60, 35 * 60),
                    (1, 2, 24 * 3600 + 35 * 60, 20 * 60),
                    (0, 2, 23 * 3600 + 50 * 60, 90 * 60),
                ],
                23 * 3600 + 40 * 60,
                25 * 3600 + 30 * 60,
                900,
                1,
                0,
                0,
            ),
            (
                vec![(0, 2, 8 * 3600, 3600), (0, 2, 8 * 3600 + 10 * 60, 50 * 60)],
                8 * 3600,
                9 * 3600,
                900,
                1,
                0,
                0,
            ),
            (
                vec![
                    (0, 2, 8 * 3600, 3600),
                    (0, 1, 8 * 3600 + 10 * 60, 10 * 60),
                    (1, 2, 8 * 3600 + 25 * 60, 5 * 60),
                ],
                8 * 3600 - 120,
                9 * 3600 + 180,
                900,
                1,
                120,
                180,
            ),
        ];
        let generated = std::env::var_os("CATENARY_NIGIRI_PARITY_INPUT");
        if let Some(path) = &generated {
            let input = std::fs::read_to_string(path).unwrap();
            let mut values = input.split_whitespace().map(|x| x.parse::<u32>().unwrap());
            let count = values.next().unwrap();
            fixtures.clear();
            for _ in 0..count {
                let trips = values.next().unwrap();
                let forward = values.next().unwrap();
                let backward = values.next().unwrap();
                let window = values.next().unwrap();
                let minimum = values.next().unwrap() as usize;
                let access = values.next().unwrap();
                let egress = values.next().unwrap();
                let rows = (0..trips)
                    .map(|_| {
                        (
                            values.next().unwrap(),
                            values.next().unwrap(),
                            values.next().unwrap(),
                            values.next().unwrap(),
                        )
                    })
                    .collect();
                fixtures.push((rows, forward, backward, window, minimum, access, egress));
            }
            assert!(values.next().is_none());
        }
        let base = 20_000_i64 * 86_400;
        let mut output = Vec::new();
        for (case, (trips, forward, backward, window, minimum, access, egress)) in
            fixtures.iter().enumerate()
        {
            let bytes = timetable(trips, false).encode().unwrap();
            let tt = ZeroCopyTimetable::new(&bytes).unwrap();
            for reverse in [false, true] {
                let q = Query {
                    departure_unix_seconds: base
                        + i64::from(if reverse { *backward } else { *forward }),
                    access: vec![WalkingEndpoint {
                        stop: LocationIdx(0),
                        seconds: *access,
                    }],
                    egress: vec![WalkingEndpoint {
                        stop: LocationIdx((tt.coordinates().len() - 1) as u32),
                        seconds: *egress,
                    }],
                    direct_walk_seconds: None,
                    max_boardings: 3,
                    max_duration_seconds: 4 * 86_400,
                };
                let (journeys, _) = route_pong_with_stats_precomputed(
                    &tt,
                    &q,
                    ProfileOptions {
                        search_window_seconds: *window,
                        min_itineraries: *minimum,
                        ..ProfileOptions::default()
                    },
                    reverse,
                    None,
                )
                .unwrap();
                assert!(
                    generated.is_some() || !journeys.is_empty(),
                    "case={case}, reverse={reverse}"
                );
                let mut keys = journeys
                    .iter()
                    .map(|j| {
                        assert!(valid_journey(&tt, &q, j));
                        (
                            j.boardings,
                            j.departure_unix_seconds - base,
                            j.arrival_unix_seconds - base,
                        )
                    })
                    .collect::<Vec<_>>();
                keys.sort_unstable();
                if generated.is_none() && case == 1 && !reverse {
                    assert_eq!(
                        keys,
                        vec![
                            (1, 15 * 3600, 15 * 3600 + 30 * 60),
                            (2, 10 * 3600 + 5 * 60, 11 * 3600)
                        ]
                    );
                }
                for (boardings, departure, arrival) in keys {
                    output.push(format!(
                        "{case} {} {boardings} {departure} {arrival}\n",
                        u8::from(reverse)
                    ));
                }
            }
        }
        if let Some(path) = std::env::var_os("CATENARY_NIGIRI_PARITY_OUTPUT") {
            std::fs::write(path, output.concat()).unwrap();
        }
    }

    #[test]
    fn streaming_emits_validated_snapshots_and_continues_past_initial_minimum() {
        let mut owned = timetable(&[(0, 2, 8 * 3_600, 600)], false);
        owned.day_count = 20;
        for calendar in owned.bitfields.iter_mut().skip(1) {
            for day in 0..20 {
                calendar.set(day, true).unwrap();
            }
        }
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        for reverse in [false, true] {
            let q = Query {
                departure_unix_seconds: 20_010_i64 * 86_400 + 8 * 3_600 + if reverse { 600 } else { 0 },
                access: vec![WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                }],
                egress: vec![WalkingEndpoint {
                    stop: LocationIdx(2),
                    seconds: 0,
                }],
                direct_walk_seconds: None,
                max_boardings: 1,
                max_duration_seconds: DAY,
            };
            let options = ProfileOptions {
                min_itineraries: 1,
                search_window_seconds: 1,
                timeout_seconds: 30,
                ..ProfileOptions::default()
            };
            let (http, _) = route_pong_with_stats_precomputed(&tt, &q, options, reverse, None).unwrap();
            let mut snapshots = Vec::new();
            let (streamed, stats) = route_pong_observed(
                &tt,
                &q,
                options,
                reverse,
                None,
                &PongControl {
                    continue_until_budget: true,
                    emit_updates: true,
                    ..Default::default()
                },
                &mut |journeys, stats| {
                    assert!(journeys.iter().all(|j| valid_journey(&tt, &q, j)));
                    assert!(journeys.iter().all(|j| if reverse {
                        j.arrival_unix_seconds >= stats.pong_interval_from.unwrap()
                    } else {
                        j.departure_unix_seconds < stats.pong_interval_to.unwrap()
                    }));
                    snapshots.push(journeys.len());
                    Ok(())
                },
            )
            .unwrap();
            assert!(http.len() < streamed.len());
            assert_eq!(streamed.len(), 6);
            assert_eq!(snapshots.first(), Some(&1));
            assert_eq!(snapshots.last(), Some(&streamed.len()));
            assert!(stats.pong_iterations > 1);
            let cancelled = AtomicBool::new(false);
            let result = route_pong_observed(
                &tt,
                &q,
                options,
                reverse,
                None,
                &PongControl {
                    cancelled: Some(&cancelled),
                    continue_until_budget: true,
                    emit_updates: true,
                    ..Default::default()
                },
                &mut |_, _| {
                    cancelled.store(true, Ordering::Relaxed);
                    Ok(())
                },
            );
            assert!(result.unwrap_err().to_string().contains("cancelled"));
            let (expired, stats) = route_pong_observed(
                &tt,
                &q,
                options,
                reverse,
                None,
                &PongControl {
                    deadline: Some(Instant::now()),
                    ..Default::default()
                },
                &mut |_, _| panic!("expired request must not publish"),
            )
            .unwrap();
            assert!(expired.is_empty());
            assert!(stats.profile_budget_exhausted);
            assert_eq!(stats.pong_iterations, 0);
        }
    }

    #[test]
    fn six_day_cursor_cap_filters_boundary_and_clamps_interval_in_both_directions() {
        let mut owned = timetable(&[(0, 2, 8 * 3_600, 600)], false);
        owned.day_count = 20;
        for calendar in owned.bitfields.iter_mut().skip(1) {
            for day in 0..20 { calendar.set(day, true).unwrap(); }
        }
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        for reverse in [false, true] {
            // One daily train cannot meet the requested 16 connections in
            // six days, forcing termination by the cap rather than count.
            let reference = base + 10 * i64::from(DAY) + 8 * 3_600 + if reverse { 600 } else { 0 };
            let q = Query {
                departure_unix_seconds: reference,
                access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: 0 }],
                egress: vec![WalkingEndpoint { stop: LocationIdx(2), seconds: 0 }],
                direct_walk_seconds: None, max_boardings: 1, max_duration_seconds: DAY,
            };
            let (journeys, stats) = route_pong_with_stats_precomputed(
                &tt, &q, ProfileOptions { min_itineraries: 16, timeout_seconds: 30, ..ProfileOptions::default() },
                reverse, None,
            ).unwrap();
            let cap = step(reference + i64::from(reverse), i64::from(MAX_LOOK_AHEAD), reverse);
            assert_eq!(journeys.len(), 6);
            assert!(journeys.iter().all(|j| {
                valid_journey(&tt, &q, j) && better(
                    if reverse { j.arrival_unix_seconds } else { j.departure_unix_seconds }, cap, reverse,
                )
            }));
            assert_eq!(if reverse { stats.pong_interval_from } else { stats.pong_interval_to },
                Some(cap + i64::from(reverse)));
            assert!(!stats.profile_budget_exhausted);
        }
    }

    #[test]
    fn scalar_labels_apply_pair_minima_before_boarding_and_reconstruct_once() {
        for minimum in [300, 480] {
            let mut owned = competing_trains(false);
            owned.station_transfer_rules.push(StationTransferRule {
                from: LocationIdx(1),
                to: LocationIdx(1),
                min_seconds: minimum,
                flags: 0,
            });
            let bytes = owned.encode().unwrap();
            let tt = ZeroCopyTimetable::new(&bytes).unwrap();
            let base = 20_000_i64 * 86_400;
            for reverse in [false, true] {
                let q = Query {
                    departure_unix_seconds: base + if reverse { 9 * 3600 } else { 8 * 3600 },
                    access: vec![WalkingEndpoint {
                        stop: LocationIdx(0),
                        seconds: 0,
                    }],
                    egress: vec![WalkingEndpoint {
                        stop: LocationIdx(2),
                        seconds: 0,
                    }],
                    direct_walk_seconds: None,
                    max_boardings: 3,
                    max_duration_seconds: 4 * 86_400,
                };
                let (journeys, stats) = route_pong_with_stats_precomputed(
                    &tt,
                    &q,
                    ProfileOptions {
                        min_itineraries: 1,
                        ..ProfileOptions::default()
                    },
                    reverse,
                    None,
                )
                .unwrap();
                assert_eq!(journeys.len(), if minimum == 300 { 2 } else { 1 });
                assert!(journeys.iter().all(|j| valid_journey(&tt, &q, j)));
                assert_eq!(stats.profile_journeys_reconstructed, journeys.len() as u64);
                assert_eq!(stats.profile_terminal_reconstruction_reused, 0);
            }
        }
    }

    #[test]
    fn scalar_round_time_contains_transfer_readiness_without_losing_physical_walk() {
        use crate::routing_common::timetable::PackedFootpath;
        for minimum in [300, 301] {
            let mut owned = timetable(
                &[
                    (0, 1, 8 * 3600, 10 * 60),
                    (2, 3, 8 * 3600 + 15 * 60, 15 * 60),
                ],
                false,
            );
            owned.location_transfer_seconds[1] = 120;
            owned.location_transfer_seconds[2] = 120;
            owned.footpaths_out = vec![PackedFootpath::new(LocationIdx(2), 149).unwrap()];
            owned.footpaths_out_offsets = vec![0, 0, 1, 1, 1];
            owned.footpaths_in = vec![PackedFootpath::new(LocationIdx(1), 149).unwrap()];
            owned.footpaths_in_offsets = vec![0, 0, 0, 1, 1];
            owned.station_transfer_rules.push(StationTransferRule {
                from: LocationIdx(1),
                to: LocationIdx(2),
                min_seconds: minimum,
                flags: 0,
            });
            let bytes = owned.encode().unwrap();
            let tt = ZeroCopyTimetable::new(&bytes).unwrap();
            let base = 20_000_i64 * 86_400;
            for reverse in [false, true] {
                let q = Query {
                    departure_unix_seconds: base
                        + if reverse {
                            8 * 3600 + 30 * 60
                        } else {
                            8 * 3600
                        },
                    access: vec![WalkingEndpoint {
                        stop: LocationIdx(0),
                        seconds: 0,
                    }],
                    egress: vec![WalkingEndpoint {
                        stop: LocationIdx(3),
                        seconds: 0,
                    }],
                    direct_walk_seconds: None,
                    max_boardings: 3,
                    max_duration_seconds: 4 * 86_400,
                };
                let (journeys, stats) = route_pong_with_stats_precomputed(
                    &tt,
                    &q,
                    ProfileOptions {
                        min_itineraries: 1,
                        ..ProfileOptions::default()
                    },
                    reverse,
                    None,
                )
                .unwrap();
                assert_eq!(journeys.len(), usize::from(minimum == 300));
                for j in journeys {
                    assert!(valid_journey(&tt, &q, &j));
                    assert_eq!(j.boardings, 2);
                    assert!(j.legs.iter().any(|leg| matches!(
                        leg,
                        Leg::Transfer {
                            from_stop: 1,
                            to_stop: 2,
                            seconds: 149
                        }
                    )));
                }
                assert_eq!(stats.profile_journeys_rejected, 0);
            }
        }
    }

    #[test]
    fn repeated_ping_reuses_known_pongs_and_seeds_limits_in_both_directions() {
        let bytes = competing_trains(false).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        for arrive_by in [false, true] {
            let q = Query {
                departure_unix_seconds: base + if arrive_by { 9 * 3_600 } else { 8 * 3_600 },
                access: vec![WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                }],
                egress: vec![WalkingEndpoint {
                    stop: LocationIdx(2),
                    seconds: 0,
                }],
                direct_walk_seconds: None,
                max_boardings: 3,
                max_duration_seconds: 2 * 3_600,
            };
            let (journeys, stats) = route_pong_with_stats_precomputed(
                &tt,
                &q,
                ProfileOptions {
                    min_itineraries: 1,
                    ..ProfileOptions::default()
                },
                arrive_by,
                None,
            )
            .unwrap();
            let mut actual = journeys
                .iter()
                .map(|j| {
                    assert!(valid_journey(&tt, &q, j));
                    (
                        j.boardings,
                        j.departure_unix_seconds - base,
                        j.arrival_unix_seconds - base,
                    )
                })
                .collect::<Vec<_>>();
            actual.sort_unstable();
            assert_eq!(
                actual,
                vec![
                    (1, 8 * 3_600, 9 * 3_600),
                    (2, 8 * 3_600 + 10 * 60, 8 * 3_600 + 30 * 60),
                ]
            );
            assert!(stats.pong_known_result_hits > 0);
            assert!(stats.pong_seeded_destination_bounds > 0);
            assert!(!stats.profile_budget_exhausted);
        }
    }

    #[test]
    fn prohibited_connection_cannot_seed_a_pong_bound() {
        let bytes = competing_trains(true).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        for arrive_by in [false, true] {
            let q = Query {
                departure_unix_seconds: base + if arrive_by { 9 * 3_600 } else { 8 * 3_600 },
                access: vec![WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                }],
                egress: vec![WalkingEndpoint {
                    stop: LocationIdx(2),
                    seconds: 0,
                }],
                direct_walk_seconds: None,
                max_boardings: 3,
                max_duration_seconds: 2 * 3_600,
            };
            let (journeys, stats) = route_pong_with_stats_precomputed(
                &tt,
                &q,
                ProfileOptions {
                    min_itineraries: 1,
                    ..ProfileOptions::default()
                },
                arrive_by,
                None,
            )
            .unwrap();
            assert_eq!(journeys.len(), 1);
            assert_eq!(journeys[0].boardings, 1);
            assert_eq!(journeys[0].departure_unix_seconds, base + 8 * 3_600);
            assert_eq!(journeys[0].arrival_unix_seconds, base + 9 * 3_600);
            assert!(valid_journey(&tt, &q, &journeys[0]));
            assert!(!stats.profile_budget_exhausted);
        }
    }
}
