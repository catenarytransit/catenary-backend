//! Nigiri-style compact round-array RAPTOR over Gentian's mmap timetable.
//!
//! Each (round, stop) has three *scalar* states: ride arrival, physical walk
//! arrival, and transfer-ready time. The third state enforces GTFS stop-pair
//! transfer rules without retaining a Pareto vector of arrival histories.
//! Predecessor slots are overwritten on improvement and only read after a
//! round has finished. Forward profile anchors are evaluated latest-first.
use crate::routing_common::profile_compact_reconstruct::{reconstruct, valid_journey};
use crate::routing_common::raptor_query::{
    Journey, Query, RaptorStats, for_each_route_service_day, transport_event_second,
    get_earliest_transport, get_latest_transport,
    optimize_same_trip_endpoint_walks,
};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, ensure};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::time::{Duration, Instant};

const INF: i64 = i64::MAX / 4;
const NEG_INF: i64 = i64::MIN / 4;
/// Keep a bounded set of near-optimal route alternatives, even after a
/// competitive destination arrival has been found.
const DESTINATION_SLACK_SECONDS: i64 = 20 * 60;

fn better(candidate: i64, incumbent: i64, reverse: bool) -> bool {
    if reverse { candidate > incumbent } else { candidate < incumbent }
}

/// A stop state with no fewer boardings and a no-better time is dominated.
fn dominated_by_lower_round(
    prior: &[Round], stage: Stage, at: usize, time: i64,
    generation: u32, reverse: bool,
) -> bool {
    prior.iter().any(|round| round.get(stage, at, generation)
        .is_some_and(|state| !better(time, state.time, reverse)))
}

/// Nigiri-style persistent profile arrival comparisons. This history only
/// contains anchors solved in monotone profile order and is invalidated when
/// an interval extension breaks that order.
fn dominated_by_profile(
    profile: &[Vec<i64>], k: usize, at: usize, time: i64, reverse: bool,
) -> bool {
    profile.iter().take(k + 1).any(|row|
        !better(time, row[at], reverse))
}


#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Stage { Ride, Walk, Ready }

#[derive(Clone, Copy)]
pub(super) enum Parent {
    Root { seconds: u32 },
    Ride {
        from: usize, to: usize, route: u32, transport: u32, day: u16,
        board_pos: usize, alight_pos: usize, departure: i64, arrival: i64,
    },
    Walk { from: usize, to: usize, seconds: u32 },
    ReadyRide,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Cell {
    pub time: i64,
    pub parent: u32,
    generation: u32,
}

pub(super) struct Round {
    ride: Vec<Cell>,
    walk: Vec<Cell>,
    ready: Vec<Cell>,
}
impl Round {
    fn new(n: usize) -> Self {
        Self { ride: vec![Cell::default(); n],
               walk: vec![Cell::default(); n],
               ready: vec![Cell::default(); n] }
    }
    fn cells(&self, stage: Stage) -> &[Cell] {
        match stage {
            Stage::Ride => &self.ride,
            Stage::Walk => &self.walk,
            Stage::Ready => &self.ready,
        }
    }
    fn cells_mut(&mut self, stage: Stage) -> &mut [Cell] {
        match stage {
            Stage::Ride => &mut self.ride,
            Stage::Walk => &mut self.walk,
            Stage::Ready => &mut self.ready,
        }
    }
    pub(super) fn get(&self, stage: Stage, stop: usize, generation: u32) -> Option<&Cell> {
        self.cells(stage).get(stop).filter(|c| c.generation == generation)
    }
    fn put(
        &mut self, stage: Stage, stop: usize, time: i64, parent: Parent,
        generation: u32, reverse: bool, parents: &mut Vec<Parent>,
        stats: &mut RaptorStats,
    ) -> bool {
        let c = &mut self.cells_mut(stage)[stop];
        if c.generation == generation &&
            if reverse { c.time >= time } else { c.time <= time } {
            stats.labels_dominated += 1;
            return false;
        }
        if c.generation != generation {
            if parents.len() >= u32::MAX as usize { return false; }
            c.parent = parents.len() as u32;
            parents.push(parent);
        } else {
            parents[c.parent as usize] = parent;
        }
        c.time = time;
        c.generation = generation;
        stats.labels_improved += 1;
        true
    }
    fn invalidate(&mut self) {
        self.ride.fill(Cell::default());
        self.walk.fill(Cell::default());
        self.ready.fill(Cell::default());
    }
}

#[derive(Clone, Copy)]
struct Onboard {
    transport: TransportIdx,
    day: usize,
    from: usize,
    pos: usize,
    event: i64,
}
struct ActiveDay {
    day: usize,
    onboard: Option<Onboard>,
}

/// Optimistic section durations are the minimum over all compressed timing
/// patterns of the route, *including* inactive ones. Missing offsets take zero
/// to keep the lower bound admissible. Footpaths use their exact durations.
fn lower_bounds(
    tt: &ZeroCopyTimetable<'_>, request: &Query, reverse: bool,
) -> Vec<u32> {
    let n = tt.coordinates().len();
    // Flat edge storage avoids allocating an individual Vec for every stop
    // (hundreds of thousands of tiny heap objects for regional timetables).
    // The condensed CSR also gives the Dijkstra scan sequential cache access.
    let mut edges: Vec<(u32, u32, u32)> = Vec::new();
    for route_id in 0..tt.route_transport_ranges().len() {
        let route = RouteIdx(route_id as u32);
        let stops = tt.stops_on(route);
        if stops.len() < 2 { continue; }
        let mut minimum = vec![u32::MAX; stops.len() - 1];
        let mut seen_patterns = HashSet::new();
        let range = tt.route_transport_ranges()[route_id];
        for index in range.from..range.to {
            let t = TransportIdx(index);
            let Some(profile) = tt.transport_timing_profile().get(t.idx()) else { continue };
            if !seen_patterns.insert(profile.idx()) { continue; }
            let Some(events) = tt.events_for_transport(t) else { continue };
            for pos in 0..minimum.len() {
                let dep = events.get(pos * 2);
                let arr = events.get(pos * 2 + 1);
                if let (Some(dep), Some(arr)) = (dep, arr) {
                    minimum[pos] = minimum[pos].min(
                        arr.seconds().saturating_sub(dep.seconds()));
                }
            }
        }
        for (pos, pair) in stops.windows(2).enumerate() {
            let from = pair[0].location().idx();
            let to = pair[1].location().idx();
            let cost = if minimum[pos] == u32::MAX { 0 } else { minimum[pos] };
            // Lower-bound graph is deliberately optimistic: even a nonboarding
            // intermediate stop remains reachable while riding through it.
            if reverse {
                edges.push((from as u32, to as u32, cost));
            } else {
                edges.push((to as u32, from as u32, cost));
            }
        }
    }
    let mut offsets = vec![0usize; n + 1];
    for &(from, _, _) in &edges { offsets[from as usize + 1] += 1; }
    for i in 1..offsets.len() { offsets[i] += offsets[i - 1]; }
    let mut next_offset = offsets[..n].to_vec();
    let mut neighbors = vec![(0u32, 0u32); edges.len()];
    for (from, to, cost) in edges {
        let cursor = &mut next_offset[from as usize];
        neighbors[*cursor] = (to, cost);
        *cursor += 1;
    }
    drop(next_offset);
    let mut dist = vec![u32::MAX; n];
    let mut heap = BinaryHeap::<Reverse<(u32, usize)>>::new();
    let terminals = if reverse { &request.access } else { &request.egress };
    for ep in terminals {
        let stop = ep.stop.idx();
        if ep.seconds < dist[stop] {
            dist[stop] = ep.seconds;
            heap.push(Reverse((ep.seconds, stop)));
        }
    }
    while let Some(Reverse((weight, at))) = heap.pop() {
        if weight != dist[at] { continue; }
        for &(next, cost) in &neighbors[offsets[at]..offsets[at + 1]] {
            let next = next as usize;
            let candidate = weight.saturating_add(cost);
            if candidate < dist[next] {
                dist[next] = candidate;
                heap.push(Reverse((candidate, next)));
            }
        }
        let footpaths = if reverse {
            tt.footpaths_from(LocationIdx(at as u32))
        } else {
            tt.footpaths_to(LocationIdx(at as u32))
        };
        for fp in footpaths {
            let next = fp.target().idx();
            let candidate = weight.saturating_add(fp.seconds());
            if candidate < dist[next] {
                dist[next] = candidate;
                heap.push(Reverse((candidate, next)));
            }
        }
    }
    dist
}

fn can_reach(
    bound: &[u32], stop: usize, time: i64,
    anchor: i64, duration: u32, reverse: bool, best_dest: i64,
) -> bool {
    let Some(&lb) = bound.get(stop) else { return false };
    if lb == u32::MAX { return false; }
    let possible_dest = if reverse {
        time.saturating_sub(i64::from(lb))
    } else {
        time.saturating_add(i64::from(lb))
    };
    let within_max_duration = if reverse {
        possible_dest >= anchor.saturating_sub(i64::from(duration))
    } else {
        possible_dest <= anchor.saturating_add(i64::from(duration))
    };
    // Destination-time bounds are tightened only after a timetable-validated
    // journey. Preserve <= (Nigiri's loose comparison) and a small diversity
    // allowance, never pruning an equal-time candidate outright.
    let competitive = if reverse {
        best_dest == NEG_INF ||
            possible_dest >= best_dest.saturating_sub(DESTINATION_SLACK_SECONDS)
    } else {
        best_dest == INF ||
            possible_dest <= best_dest.saturating_add(DESTINATION_SLACK_SECONDS)
    };
    within_max_duration && competitive
}

fn change_seconds(tt: &ZeroCopyTimetable<'_>, from: usize, to: usize)
    -> Option<u32>
{
    let rule = tt.station_transfer_rule(
        LocationIdx(from as u32), LocationIdx(to as u32));
    if rule.is_some_and(|x| x.prohibited()) { return None; }
    Some(tt.location_transfer_seconds()[from]
        .max(tt.location_transfer_seconds()[to])
        .max(rule.map_or(0, |x| x.min_seconds)))
}

fn routes_from_marked(
    tt: &ZeroCopyTimetable<'_>, marked: &[usize],
    stamps: &mut [u32], stamp: &mut u32,
) -> Vec<RouteIdx> {
    *stamp = stamp.wrapping_add(1);
    if *stamp == 0 { stamps.fill(0); *stamp = 1; }
    let mut routes = Vec::new();
    for &stop in marked {
        for &route in tt.routes_at(LocationIdx(stop as u32)) {
            if stamps[route.idx()] != *stamp {
                stamps[route.idx()] = *stamp;
                routes.push(route);
            }
        }
    }
    routes.sort_unstable_by_key(|x| x.0);
    routes
}

pub(crate) struct ProfileSearchState {
    rounds: Vec<Round>,
    parents: Vec<Parent>,
    bounds: Vec<u32>,
    // Persistent scalar profile arrivals; these never own parent pointers.
    // Entries are usable only while anchors are processed monotonically.
    profile_ride: Vec<Vec<i64>>,
    profile_ready: Vec<Vec<i64>>,
    last_anchor: Option<i64>,
    // Best fully validated destination arrival/departure for <= k boardings.
    // This is Nigiri's time_at_dest_ analogue.
    time_at_dest: Vec<i64>,
    stamps: Vec<u32>,
    stamp: u32,
    // Bitset-equivalent O(1) membership in the previous-round frontier;
    // generation tags avoid clearing a large continental-sized array.
    stop_stamps: Vec<u32>,
    stop_stamp: u32,
    generation: u32,
    journeys: Vec<Journey>,
    stats: RaptorStats,
    deadline: Instant,
    reverse: bool,
}
impl ProfileSearchState {
    pub fn new(
        tt: &ZeroCopyTimetable<'_>, q: &Query,
        earliest: i64, latest: i64, reverse: bool, timeout: Duration,
    ) -> Result<Self> {
        ensure!(earliest <= latest, "invalid profile interval");
        ensure!(q.max_boardings <= 16, "max_boardings exceeds 16");
        ensure!((1..=7 * 86_400).contains(&q.max_duration_seconds),
            "max_duration_seconds is outside 1 second to 7 days");
        let n = tt.coordinates().len();
        for ep in q.access.iter().chain(q.egress.iter()) {
            ensure!(ep.stop.idx() < n, "OSR endpoint index out of bounds");
        }
        let start = Instant::now();
        let deadline = start.checked_add(timeout).unwrap_or(start);
        let mut stats = RaptorStats::default();
        stats.profile_searches = 1;
        let bounds_started = Instant::now();
        let bounds = lower_bounds(tt, q, reverse);
        stats.profile_lower_bounds_ms += bounds_started.elapsed().as_secs_f64() * 1_000.0;
        let invalid = if reverse { NEG_INF } else { INF };
        Ok(Self {
            rounds: (0..=q.max_boardings).map(|_| Round::new(n)).collect(),
            parents: Vec::new(),
            bounds,
            profile_ride: vec![vec![invalid; n]; q.max_boardings + 1],
            profile_ready: vec![vec![invalid; n]; q.max_boardings + 1],
            last_anchor: None,
            time_at_dest: vec![invalid; q.max_boardings + 1],
            stamps: vec![0; tt.route_transport_ranges().len()],
            stamp: 0,
            stop_stamps: vec![0; n],
            stop_stamp: 0,
            generation: 0,
            journeys: Vec::new(),
            stats,
            deadline,
            reverse,
        })
    }

    pub fn extend(
        &mut self, tt: &ZeroCopyTimetable<'_>, q: &Query, anchors: &[i64],
    ) -> Result<()> {
        ensure!(anchors.windows(2).all(|w| w[0] <= w[1]),
            "profile anchors must be sorted ascending");
        // Nigiri scans forward profiles from the latest departure backwards.
        // Every event is independent, so expanding the profile interval later
        // never invalidates arrival states from an earlier event.
        let mut ordered = anchors.to_vec();
        if !self.reverse { ordered.reverse(); }
        for anchor in ordered {
            if Instant::now() >= self.deadline {
                self.stats.profile_budget_exhausted = true;
                break;
            }
            // Extending an interval toward later forward departures (or
            // earlier backward arrivals) invalidates profile dominance from
            // previously solved anchors. Nigiri resets persisted round arrivals
            // at this boundary too, while retaining already found journeys.
            if self.last_anchor.is_some_and(|last| {
                if self.reverse { anchor < last } else { anchor > last }
            }) {
                let invalid = if self.reverse { NEG_INF } else { INF };
                for row in &mut self.profile_ride { row.fill(invalid); }
                for row in &mut self.profile_ready { row.fill(invalid); }
                self.time_at_dest.fill(invalid);
            }
            self.last_anchor = Some(anchor);
            self.stats.profile_anchor_events += 1;
            self.solve(tt, q, anchor);
            if self.stats.profile_budget_exhausted { break; }
        }
        Ok(())
    }

    fn solve(&mut self, tt: &ZeroCopyTimetable<'_>, q: &Query, anchor: i64) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            for r in &mut self.rounds { r.invalidate(); }
            self.generation = 1;
        }
        self.parents.clear();
        let generation = self.generation;
        let reverse = self.reverse;
        // Nigiri retains round arrival times independently of whether a
        // journey to the destination was found at this particular anchor.
        // They remain valid dominance bounds for earlier forward starts and
        // later backward starts. Parents are strictly event-local.
        let mut reached_rides = vec![Vec::<usize>::new(); q.max_boardings + 1];
        let mut reached_ready = vec![Vec::<usize>::new(); q.max_boardings + 1];
        let horizon = i64::from(q.max_duration_seconds);
        let (lower, upper) = if reverse {
            (anchor.saturating_sub(horizon), anchor)
        } else {
            (anchor, anchor.saturating_add(horizon))
        };
        let endpoints = if reverse { &q.egress } else { &q.access };
        let mut marked = Vec::new();
        for ep in endpoints {
            let t = if reverse {
                anchor.saturating_sub(i64::from(ep.seconds))
            } else {
                anchor.saturating_add(i64::from(ep.seconds))
            };
            if (lower..=upper).contains(&t)
                && can_reach(&self.bounds, ep.stop.idx(), t, anchor,
                    q.max_duration_seconds, reverse, self.time_at_dest[0])
                && !dominated_by_profile(
                    &self.profile_ready, 0, ep.stop.idx(), t, reverse)
                && self.rounds[0].put(
                    Stage::Ready, ep.stop.idx(), t,
                    Parent::Root { seconds: ep.seconds },
                    generation, reverse, &mut self.parents, &mut self.stats,
                ) {
                marked.push(ep.stop.idx());
            }
        }
        marked.sort_unstable();
        marked.dedup();
        reached_ready[0] = marked.clone();
        for k in 1..=q.max_boardings {
            if marked.is_empty() || Instant::now() >= self.deadline {
                if Instant::now() >= self.deadline {
                    self.stats.profile_budget_exhausted = true;
                }
                break;
            }
            self.stats.rounds += 1;
            self.stats.marked_stops += marked.len() as u64;
            self.stop_stamp = self.stop_stamp.wrapping_add(1);
            if self.stop_stamp == 0 {
                self.stop_stamps.fill(0);
                self.stop_stamp = 1;
            }
            for &stop in &marked { self.stop_stamps[stop] = self.stop_stamp; }
            let routes = routes_from_marked(tt, &marked, &mut self.stamps, &mut self.stamp);
            let (before, after) = self.rounds.split_at_mut(k);
            let previous = &before[k-1];
            let current = &mut after[0];
            let mut improved_rides = Vec::new();
            let started = Instant::now();
            for route in routes {
                if Instant::now() >= self.deadline {
                    self.stats.profile_budget_exhausted = true;
                    break;
                }
                self.stats.routes_scanned += 1;
                let stops = tt.stops_on(route);
                if stops.len() < 2 { continue; }
                let mut active_days = Vec::<ActiveDay>::new();
                // Gentian's route traffic-day bitmap is the union of the
                // transports' calendars. Do not materialize a filtered
                // transport vector for every route, service day and anchor.
                for_each_route_service_day(tt, route, lower, upper, &mut self.stats,
                    |day, _| active_days.push(ActiveDay { day, onboard: None }));
                if active_days.is_empty() { continue; }
                for step in 0..stops.len() {
                    self.stats.stop_events_scanned += 1;
                    if (self.stats.stop_events_scanned & 255) == 0
                        && Instant::now() >= self.deadline
                    {
                        self.stats.profile_budget_exhausted = true;
                        break;
                    }
                    let pos = if reverse { stops.len() - step - 1 } else { step };
                    let stop = stops[pos];
                    let loc = stop.location().idx();
                    let can_finish = if reverse {
                        stop.outbound() && pos + 1 < stops.len()
                    } else {
                        stop.inbound() && pos > 0
                    };
                    let can_start = if reverse {
                        stop.inbound() && pos > 0
                    } else {
                        stop.outbound() && pos + 1 < stops.len()
                    };
                    for day in &mut active_days {
                        let midnight = (i64::from(tt.header().first_utc_day)
                            + day.day as i64) * 86_400;
                        if can_finish {
                            if let Some(onboard) = day.onboard {
                                let Some(event) = transport_event_second(
                                    tt, route, onboard.transport, pos, !reverse, midnight
                                ) else { continue };
                                if (lower..=upper).contains(&event)
                                    && can_reach(&self.bounds, loc, event, anchor,
                                        q.max_duration_seconds, reverse, self.time_at_dest[k])
                                    && !dominated_by_profile(
                                        &self.profile_ride, k, loc, event, reverse)
                                    && !dominated_by_lower_round(
                                        before, Stage::Ride, loc, event, generation, reverse)
                                {
                                    let (from, to, board_pos, alight_pos, dep, arr) =
                                        if reverse {
                                            (loc, onboard.from, pos, onboard.pos,
                                             event, onboard.event)
                                        } else {
                                            (onboard.from, loc, onboard.pos, pos,
                                             onboard.event, event)
                                        };
                                    if current.put(
                                        Stage::Ride, loc, event,
                                        Parent::Ride {
                                            from, to, route: route.0,
                                            transport: onboard.transport.0,
                                            day: onboard.day as u16,
                                            board_pos, alight_pos,
                                            departure: dep, arrival: arr,
                                        },
                                        generation, reverse, &mut self.parents,
                                        &mut self.stats,
                                    ) {
                                        improved_rides.push(loc);
                                    }
                                }
                            }
                        }
                        if !can_start || self.stop_stamps[loc] != self.stop_stamp {
                            continue;
                        }
                        let Some(ready) = previous.get(Stage::Ready, loc, generation) else {
                            continue;
                        };
                        if !(lower..=upper).contains(&ready.time) { continue; }
                        let next_transport = if reverse {
                            get_latest_transport(
                                tt, route, pos, day.day, ready.time, &mut self.stats)
                        } else {
                            get_earliest_transport(
                                tt, route, pos, day.day, ready.time, &mut self.stats)
                        };
                        let Some((transport, event)) = next_transport else { continue };
                        if !(lower..=upper).contains(&event) { continue; }
                        let better = day.onboard.is_none_or(|old| {
                            if reverse { transport.0 > old.transport.0 }
                            else { transport.0 < old.transport.0 }
                        });
                        if better {
                            day.onboard = Some(Onboard {
                                transport, day: day.day, from: loc, pos, event,
                            });
                        }
                    }
                }
                if self.stats.profile_budget_exhausted { break; }
            }
            self.stats.route_scan_ms += started.elapsed().as_secs_f64() * 1_000.0;
            improved_rides.sort_unstable();
            improved_rides.dedup();
            reached_rides[k] = improved_rides.clone();
            let mut changed_ready = Vec::new();
            let walking_started = Instant::now();
            // Only the final best ride state at each stop is expanded; the
            // footprint is at most three cells per stop and RAPTOR round.
            for &from in &improved_rides {
                let Some(ride) = current.get(Stage::Ride, from, generation) else {
                    continue;
                };
                let t = ride.time;
                if let Some(change) = change_seconds(tt, from, from) {
                    let ready = if reverse {
                        t.saturating_sub(i64::from(change))
                    } else {
                        t.saturating_add(i64::from(change))
                    };
                    if (lower..=upper).contains(&ready)
                        && can_reach(&self.bounds, from, ready, anchor,
                            q.max_duration_seconds, reverse, self.time_at_dest[k])
                        && !dominated_by_profile(
                            &self.profile_ready, k, from, ready, reverse)
                        && !dominated_by_lower_round(
                            before, Stage::Ready, from, ready, generation, reverse)
                        && current.put(
                            Stage::Ready, from, ready, Parent::ReadyRide,
                            generation, reverse, &mut self.parents, &mut self.stats
                        ) {
                        changed_ready.push(from);
                    }
                }
                let paths = if reverse {
                    tt.footpaths_to(LocationIdx(from as u32))
                } else {
                    tt.footpaths_from(LocationIdx(from as u32))
                };
                for fp in paths {
                    self.stats.footpath_edges_examined += 1;
                    let to = fp.target().idx();
                    let seconds = fp.seconds();
                    if seconds == 0 { continue; }
                    let at = if reverse {
                        t.saturating_sub(i64::from(seconds))
                    } else {
                        t.saturating_add(i64::from(seconds))
                    };
                    if !(lower..=upper).contains(&at)
                        || !can_reach(&self.bounds, to, at, anchor,
                            q.max_duration_seconds, reverse, self.time_at_dest[k])
                    { continue; }
                    let (walk_from, walk_to) = if reverse { (to, from) } else { (from, to) };
                    // Physical arrival and boarding readiness are different
                    // dimensions: minimum change times do not extend a walk.
                    current.put(
                        Stage::Walk, to, at,
                        Parent::Walk { from: walk_from, to: walk_to, seconds },
                        generation, reverse, &mut self.parents, &mut self.stats
                    );
                    let (rule_from, rule_to) = (walk_from, walk_to);
                    let Some(change) = change_seconds(tt, rule_from, rule_to)
                    else { continue };
                    let ready = if reverse {
                        at.min(t.saturating_sub(i64::from(change)))
                    } else {
                        at.max(t.saturating_add(i64::from(change)))
                    };
                    if (lower..=upper).contains(&ready)
                        && can_reach(&self.bounds, to, ready, anchor,
                            q.max_duration_seconds, reverse, self.time_at_dest[k])
                        && !dominated_by_profile(
                            &self.profile_ready, k, to, ready, reverse)
                        && !dominated_by_lower_round(
                            before, Stage::Ready, to, ready, generation, reverse)
                        && current.put(
                            Stage::Ready, to, ready,
                            // Keep the exact footpath that produced this
                            // transfer-ready time. The best physical-walk
                            // cell can come from a different alighting stop.
                            Parent::Walk { from: walk_from, to: walk_to, seconds },
                            generation, reverse, &mut self.parents, &mut self.stats
                        ) {
                        changed_ready.push(to);
                    }
                }
            }
            self.stats.walking_ms += walking_started.elapsed().as_secs_f64() * 1_000.0;
            // Gather terminal candidates while the current round is mutable.
            // Reconstruction reads immutable snapshots of *all* rounds, and
            // must happen after this scan's mutable borrow has ended.
            let terminals = if reverse { &q.access } else { &q.egress };
            let mut terminal_candidates = Vec::new();
            for ep in terminals {
                for stage in [Stage::Ride, Stage::Walk] {
                    if current.get(stage, ep.stop.idx(), generation).is_some() {
                        terminal_candidates.push((ep.stop.idx(), stage, ep.seconds));
                    }
                }
            }
            for (terminal_stop, stage, terminal_seconds) in terminal_candidates {
                    let Some(mut journey) = reconstruct(
                        &self.rounds, &self.parents, generation, reverse,
                        k, terminal_stop, stage, terminal_seconds,
                    ) else { continue };
                    self.stats.profile_journeys_reconstructed += 1;
                    // Nigiri performs endpoint and transfer optimization
                    // during reconstruction. Preserve Catenary's established
                    // same-vehicle access/egress and transfer optimizer before
                    // validating the final sequence.
                    optimize_same_trip_endpoint_walks(tt, q, &mut journey);
                    if valid_journey(tt, q, &journey) {
                        // Persistent destination-time bounds are indexed by
                        // a maximum boarding count. A result with k rides
                        // also bounds searches permitting more boardings.
                        let metric = if reverse {
                            journey.departure_unix_seconds
                        } else {
                            journey.arrival_unix_seconds
                        };
                        for bound in self.time_at_dest.iter_mut().skip(k) {
                            if better(metric, *bound, reverse) { *bound = metric; }
                        }
                        self.journeys.push(journey);
                    } else {
                        self.stats.profile_journeys_rejected += 1;
                    }
            }
            changed_ready.sort_unstable();
            changed_ready.dedup();
            reached_ready[k] = changed_ready.clone();
            marked = changed_ready;
            if self.stats.profile_budget_exhausted { break; }
        }
        // Preserve the compact arrival skyline across monotone profile
        // anchors exactly as Nigiri's round_times_ does. The current
        // generation-tagged states act like Nigiri's event-local best_/tmp_.
        {
            for k in 0..self.rounds.len() {
                for &stop in &reached_ready[k] {
                    if let Some(value) = self.rounds[k].get(Stage::Ready, stop, generation) {
                        let current = &mut self.profile_ready[k][stop];
                        if better(value.time, *current, reverse) { *current = value.time; }
                    }
                }
                for &stop in &reached_rides[k] {
                    if let Some(value) = self.rounds[k].get(Stage::Ride, stop, generation) {
                        let current = &mut self.profile_ride[k][stop];
                        if better(value.time, *current, reverse) { *current = value.time; }
                    }
                }
            }
        }
    }

    pub fn result(&self) -> Vec<Journey> {
        crate::routing_common::profile_search::route_diverse_pareto(self.journeys.clone())
    }
    pub fn stats(&self) -> &RaptorStats { &self.stats }
}

pub(crate) fn route_profile_incremental(
    tt: &ZeroCopyTimetable<'_>, q: &Query, anchors: &[i64],
    reverse: bool, timeout: Duration,
) -> Result<(Vec<Journey>, RaptorStats)> {
    let earliest = anchors.iter().copied().min().unwrap_or(q.departure_unix_seconds);
    let latest = anchors.iter().copied().max().unwrap_or(q.departure_unix_seconds);
    let mut state = ProfileSearchState::new(tt, q, earliest, latest, reverse, timeout)?;
    let mut sorted = anchors.to_vec();
    sorted.sort_unstable();
    state.extend(tt, q, &sorted)?;
    Ok((state.result(), state.stats().clone()))
}


#[cfg(test)]
mod compact_reconstruction_tests {
    use super::*;
    use crate::routing_common::profile_compact_reconstruct::reconstruct;
    use crate::routing_common::raptor_query::Leg;
    use crate::routing_common::raptor_query::RaptorStats;

    #[test]
    fn boarding_ready_uses_its_own_walk_predecessor() {
        // Two footpaths arrive at stop 2: walking from stop 3 is physically
        // earlier, but the path from stop 1 is the one that is boardable first.
        // Following the Stage::Walk predecessor would create an invalid path.
        let mut rounds = vec![Round::new(4), Round::new(4)];
        let mut parents = Vec::new();
        let mut stats = RaptorStats::default();
        assert!(rounds[0].put(Stage::Ready, 0, 1_000,
            Parent::Root { seconds: 0 }, 1, false, &mut parents, &mut stats));
        assert!(rounds[1].put(Stage::Ride, 1, 1_100,
            Parent::Ride { from: 0, to: 1, route: 0, transport: 0, day: 0,
                board_pos: 0, alight_pos: 1, departure: 1_020, arrival: 1_100 },
            1, false, &mut parents, &mut stats));
        assert!(rounds[1].put(Stage::Ready, 2, 1_140,
            Parent::Walk { from: 1, to: 2, seconds: 40 },
            1, false, &mut parents, &mut stats));
        assert!(rounds[1].put(Stage::Walk, 2, 1_120,
            Parent::Walk { from: 3, to: 2, seconds: 20 },
            1, false, &mut parents, &mut stats));
        let result = reconstruct(&rounds, &parents, 1, false, 1, 2, Stage::Ready, 0)
            .expect("exact ready-state predecessor must reconstruct");
        assert_eq!(result.boardings, 1);
        assert!(matches!(result.legs[2], Leg::Transfer {
            from_stop: 1, to_stop: 2, seconds: 40
        }));
        assert_eq!(result.arrival_unix_seconds, 1_140);
    }

    #[test]
    fn ready_from_ride_needs_no_footpath() {
        let mut rounds = vec![Round::new(2), Round::new(2)];
        let mut parents = Vec::new();
        let mut stats = RaptorStats::default();
        rounds[0].put(Stage::Ready, 0, 0, Parent::Root { seconds: 0 },
            1, false, &mut parents, &mut stats);
        rounds[1].put(Stage::Ride, 1, 100,
            Parent::Ride { from: 0, to: 1, route: 0, transport: 0, day: 0,
                board_pos: 0, alight_pos: 1, departure: 10, arrival: 100 },
            1, false, &mut parents, &mut stats);
        rounds[1].put(Stage::Ready, 1, 130, Parent::ReadyRide,
            1, false, &mut parents, &mut stats);
        let result = reconstruct(&rounds, &parents, 1, false, 1, 1, Stage::Ready, 0)
            .expect("self-transfer readiness should reference the same ride");
        assert_eq!(result.boardings, 1);
        assert_eq!(result.legs.len(), 3);
    }
}


#[cfg(test)]
mod profile_bound_tests {
    use super::*;

    #[test]
    fn destination_bound_respects_slack_and_direction() {
        let lb = [30_u32];
        assert!(can_reach(&lb, 0, 100, 100, 1000, false, INF));
        assert!(can_reach(&lb, 0, 100, 100, 1000, false, 130));
        assert!(can_reach(&lb, 0, 100, 100, 1000, false, 129));
        assert!(!can_reach(&lb, 0, 100, 100, 1000, false, -2000));
        assert!(can_reach(&lb, 0, 900, 900, 1000, true, NEG_INF));
        assert!(can_reach(&lb, 0, 900, 900, 1000, true, 870));
        assert!(!can_reach(&lb, 0, 900, 900, 1000, true, 4000));
    }

    #[test]
    fn profile_dominance_rejects_equal_or_worse_times_at_no_more_rides() {
        let profile = vec![vec![100, INF], vec![80, 100]];
        assert!(dominated_by_profile(&profile, 1, 0, 110, false));
        assert!(dominated_by_profile(&profile, 1, 0, 100, false));
        assert!(!dominated_by_profile(&profile, 1, 0, 79, false));
        assert!(!dominated_by_profile(&profile, 0, 1, 99, false));
        let reverse_profile = vec![vec![100, NEG_INF], vec![120, 200]];
        assert!(dominated_by_profile(&reverse_profile, 1, 0, 119, true));
        assert!(!dominated_by_profile(&reverse_profile, 1, 0, 121, true));
    }
}


#[cfg(test)]
mod compressed_timetable_profile_tests {
    use super::*;
    use crate::routing_common::raptor_query::WalkingEndpoint;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, PackedStop, Range, SourceIdx, TimingProfileIdx,
        TimetableOwned, TrafficDays,
    };

    // A genuine encoded Gentian timetable fixture: one 8-hour daily train
    // crossing two stops, active across four consecutive service days.
    fn daily_train() -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 4);
        tt.add_source("test", "amtrak", "attempt").unwrap();
        for _ in 0..2 {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
            tt.location_routes_offsets.push(tt.location_routes_offsets.len() as u32);
            tt.location_routes.push(RouteIdx(0));
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
        }
        tt.route_stops.extend([
            PackedStop::new(LocationIdx(0), true, true, true, true).unwrap(),
            PackedStop::new(LocationIdx(1), true, true, true, true).unwrap(),
        ]);
        tt.route_stop_offsets.push(2);
        tt.route_transport_ranges.push(Range { from: 0, to: 1 });
        tt.timing_profile_offsets.push(2);
        tt.timing_profile_events = vec![
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(8 * 3_600).unwrap(),
        ];
        tt.route_clasz.push(2);
        tt.transport_route.push(RouteIdx(0));
        tt.transport_timing_profile.push(TimingProfileIdx(0));
        tt.transport_first_dep_offset.push(Delta::from_seconds(9 * 3_600).unwrap());
        tt.transport_initial_day_offset.push(0);
        let mut days = TrafficDays::default();
        for day in 0..4 { days.set(day, true).unwrap(); }
        tt.bitfields.push(days);
        tt.transport_traffic_days.push(BitfieldIdx(1));
        tt.route_traffic_days.push(BitfieldIdx(1));
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.transport_to_trip_section_offsets.push(0);
        tt
    }

    fn query() -> Query {
        Query {
            departure_unix_seconds: 20_000_i64 * 86_400 + 18 * 3_600,
            access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: 0 }],
            egress: vec![WalkingEndpoint { stop: LocationIdx(1), seconds: 0 }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 36 * 3_600,
        }
    }

    #[test]
    fn latest_first_forward_anchors_find_both_daily_trains() {
        let bytes = daily_train().encode().unwrap();
        let timetable = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let departures = [base + 86_400 + 9 * 3_600,
                          base + 2 * 86_400 + 9 * 3_600];
        let (journeys, stats) = route_profile_incremental(
            &timetable, &query(), &departures, false, Duration::from_secs(5)
        ).unwrap();
        assert!(!stats.profile_budget_exhausted, "timed out on tiny timetable: {stats:?}");
        for departure in departures {
            assert!(journeys.iter().any(|j|
                j.departure_unix_seconds == departure
                && j.arrival_unix_seconds == departure + 8 * 3_600
                && j.boardings == 1
            ), "missing train at {departure}: {journeys:?}");
        }
        assert_eq!(stats.profile_anchor_events, 2);
    }

    #[test]
    fn ascending_reverse_anchors_find_both_daily_trains() {
        let bytes = daily_train().encode().unwrap();
        let timetable = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let arrivals = [base + 86_400 + 17 * 3_600,
                        base + 2 * 86_400 + 17 * 3_600];
        let (journeys, stats) = route_profile_incremental(
            &timetable, &query(), &arrivals, true, Duration::from_secs(5)
        ).unwrap();
        assert!(!stats.profile_budget_exhausted, "timed out on tiny timetable: {stats:?}");
        for arrival in arrivals {
            assert!(journeys.iter().any(|j|
                j.arrival_unix_seconds == arrival
                && j.departure_unix_seconds == arrival - 8 * 3_600
                && j.boardings == 1
            ), "missing arrival at {arrival}: {journeys:?}");
        }
        assert_eq!(stats.profile_anchor_events, 2);
    }

    #[test]
    fn extension_in_nonmonotone_direction_resets_persistent_bounds() {
        let bytes = daily_train().encode().unwrap();
        let timetable = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let early = base + 86_400 + 9 * 3_600;
        let late = base + 2 * 86_400 + 9 * 3_600;
        let q = query();
        let mut state = ProfileSearchState::new(
            &timetable, &q, early, late, false, Duration::from_secs(5)
        ).unwrap();
        state.extend(&timetable, &q, &[early]).unwrap();
        assert_eq!(state.last_anchor, Some(early));
        state.extend(&timetable, &q, &[late]).unwrap();
        assert_eq!(state.last_anchor, Some(late));
        assert_eq!(state.stats().profile_anchor_events, 2);
        assert!(state.result().iter().any(|j| j.departure_unix_seconds == late));
    }
}


#[cfg(test)]
mod transfer_round_regressions {
    use super::*;
    use crate::routing_common::raptor_query::WalkingEndpoint;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, PackedStop, Range, SourceIdx, StationTransferRule,
        TimetableOwned, TimingProfileIdx, TrafficDays,
    };

    fn two_leg_timetable(prohibit_change: bool) -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 2);
        tt.add_source("test", "two-leg", "attempt").unwrap();
        for _ in 0..3 {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
        }
        tt.location_routes_offsets = vec![0, 1, 3, 4];
        tt.location_routes = vec![
            RouteIdx(0), RouteIdx(0), RouteIdx(1), RouteIdx(1)
        ];
        tt.footpaths_out_offsets = vec![0; 4];
        tt.footpaths_in_offsets = vec![0; 4];
        for (from, to) in [(0, 1), (1, 2)] {
            tt.route_stops.extend([
                PackedStop::new(LocationIdx(from), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(to), true, true, true, true).unwrap(),
            ]);
        }
        tt.route_stop_offsets.extend([2, 4]);
        tt.route_transport_ranges.extend([
            Range { from: 0, to: 1 }, Range { from: 1, to: 2 }
        ]);
        tt.timing_profile_offsets.extend([2, 4]);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(10 * 60).unwrap(),
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(15 * 60).unwrap(),
        ]);
        tt.route_clasz.extend([2, 2]);
        tt.transport_route.extend([RouteIdx(0), RouteIdx(1)]);
        tt.transport_timing_profile.extend([
            TimingProfileIdx(0), TimingProfileIdx(1)
        ]);
        tt.transport_first_dep_offset.extend([
            Delta::from_seconds(8 * 3600).unwrap(),
            Delta::from_seconds(8 * 3600 + 15 * 60).unwrap(),
        ]);
        tt.transport_initial_day_offset.extend([0, 0]);
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        days.set(1, true).unwrap();
        tt.bitfields.push(days);
        tt.transport_traffic_days.extend([BitfieldIdx(1), BitfieldIdx(1)]);
        tt.route_traffic_days.extend([BitfieldIdx(1), BitfieldIdx(1)]);
        tt.route_section_clasz_offsets.extend([0, 0]);
        tt.section_flag_offsets.extend([0, 0]);
        tt.transport_to_trip_section_offsets.extend([0, 0]);
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

    fn request() -> Query {
        Query {
            departure_unix_seconds: 20_000_i64 * 86400 + 8 * 3600,
            access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: 0 }],
            egress: vec![WalkingEndpoint { stop: LocationIdx(2), seconds: 0 }],
            direct_walk_seconds: None,
            max_boardings: 2,
            max_duration_seconds: 2 * 3600,
        }
    }

    #[test]
    fn two_ride_forward_reconstructs_after_compact_scan() {
        let bytes = two_leg_timetable(false).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = request();
        let (journeys, stats) = route_profile_incremental(
            &tt, &q, &[q.departure_unix_seconds], false, Duration::from_secs(5)
        ).unwrap();
        assert!(!journeys.is_empty(), "two-ride forward missing: {stats:?}");
        assert!(journeys.iter().any(|j| j.boardings == 2
            && j.departure_unix_seconds == q.departure_unix_seconds
            && j.arrival_unix_seconds == q.departure_unix_seconds + 30 * 60));
    }

    #[test]
    fn two_ride_backward_reconstructs_without_reversing_footpaths() {
        let bytes = two_leg_timetable(false).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut q = request();
        q.departure_unix_seconds += 30 * 60;
        let (journeys, stats) = route_profile_incremental(
            &tt, &q, &[q.departure_unix_seconds], true, Duration::from_secs(5)
        ).unwrap();
        assert!(!journeys.is_empty(), "two-ride reverse missing: {stats:?}");
        assert!(journeys.iter().any(|j| j.boardings == 2
            && j.departure_unix_seconds == q.departure_unix_seconds - 30 * 60
            && j.arrival_unix_seconds == q.departure_unix_seconds));
    }

    #[test]
    fn prohibited_same_platform_change_is_not_silently_ignored() {
        let bytes = two_leg_timetable(true).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = request();
        let (forward, _) = route_profile_incremental(
            &tt, &q, &[q.departure_unix_seconds], false, Duration::from_secs(5)
        ).unwrap();
        assert!(forward.is_empty(), "blocked change produced a route: {forward:?}");
        let mut reverse_q = q;
        reverse_q.departure_unix_seconds += 30 * 60;
        let (backward, _) = route_profile_incremental(
            &tt, &reverse_q, &[reverse_q.departure_unix_seconds], true,
            Duration::from_secs(5)
        ).unwrap();
        assert!(backward.is_empty(), "reverse blocked change produced route: {backward:?}");
    }
}
