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
};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, ensure};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::time::{Duration, Instant};

const INF: i64 = i64::MAX / 4;
const NEG_INF: i64 = i64::MIN / 4;

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
    ReadyWalk,
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
    transports: Vec<TransportIdx>,
    onboard: Option<Onboard>,
}

fn latest_or_earliest(
    tt: &ZeroCopyTimetable<'_>, route: RouteIdx, stop_pos: usize,
    day: usize, ready: i64, reverse: bool,
    active: &[TransportIdx], stats: &mut RaptorStats,
) -> Option<(TransportIdx, i64)> {
    let midnight = (i64::from(tt.header().first_utc_day) + day as i64) * 86_400;
    let (mut lo, mut hi) = (0, active.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        stats.transport_lookup_probes += 1;
        let event = transport_event_second(
            tt, route, active[mid], stop_pos, reverse, midnight,
        )?;
        if if reverse { event <= ready } else { event < ready } {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let idx = if reverse { lo.checked_sub(1)? } else { lo };
    let transport = *active.get(idx)?;
    let event = transport_event_second(
        tt, route, transport, stop_pos, reverse, midnight,
    )?;
    Some((transport, event))
}

/// Optimistic section durations are the minimum over all compressed timing
/// patterns of the route, *including* inactive ones. Missing offsets take zero
/// to keep the lower bound admissible. Footpaths use their exact durations.
fn lower_bounds(
    tt: &ZeroCopyTimetable<'_>, request: &Query, reverse: bool,
) -> Vec<u32> {
    let n = tt.coordinates().len();
    let mut graph: Vec<Vec<(usize, u32)>> = vec![Vec::new(); n];
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
            if reverse { graph[from].push((to, cost)); }
            else { graph[to].push((from, cost)); }
        }
    }
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
        for &(next, cost) in &graph[at] {
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
    anchor: i64, duration: u32, reverse: bool,
) -> bool {
    let Some(&lb) = bound.get(stop) else { return false };
    if lb == u32::MAX { return false; }
    if reverse {
        time.saturating_sub(i64::from(lb))
            >= anchor.saturating_sub(i64::from(duration))
    } else {
        time.saturating_add(i64::from(lb))
            <= anchor.saturating_add(i64::from(duration))
    }
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
    stamps: Vec<u32>,
    stamp: u32,
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
        let bounds = lower_bounds(tt, q, reverse);
        Ok(Self {
            rounds: (0..=q.max_boardings).map(|_| Round::new(n)).collect(),
            parents: Vec::new(),
            bounds,
            stamps: vec![0; tt.route_transport_ranges().len()],
            stamp: 0,
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
                    q.max_duration_seconds, reverse)
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
        for k in 1..=q.max_boardings {
            if marked.is_empty() || Instant::now() >= self.deadline {
                if Instant::now() >= self.deadline {
                    self.stats.profile_budget_exhausted = true;
                }
                break;
            }
            self.stats.rounds += 1;
            self.stats.marked_stops += marked.len() as u64;
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
                let range = tt.route_transport_ranges()[route.idx()];
                let mut active_days = Vec::<ActiveDay>::new();
                for_each_route_service_day(tt, route, lower, upper, &mut self.stats,
                    |day, _| {
                        let transports = (range.from..range.to)
                            .map(TransportIdx)
                            .filter(|t| tt.bitfields()[
                                tt.transport_traffic_days()[t.idx()].idx()
                            ].test(day)).collect::<Vec<_>>();
                        if !transports.is_empty() {
                            active_days.push(ActiveDay {
                                day, transports, onboard: None,
                            });
                        }
                    });
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
                                        q.max_duration_seconds, reverse)
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
                        if !can_start || marked.binary_search(&loc).is_err() {
                            continue;
                        }
                        let Some(ready) = previous.get(Stage::Ready, loc, generation) else {
                            continue;
                        };
                        if !(lower..=upper).contains(&ready.time) { continue; }
                        let Some((transport, event)) = latest_or_earliest(
                            tt, route, pos, day.day, ready.time, reverse,
                            &day.transports, &mut self.stats
                        ) else { continue };
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
                            q.max_duration_seconds, reverse)
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
                            q.max_duration_seconds, reverse)
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
                            q.max_duration_seconds, reverse)
                        && current.put(
                            Stage::Ready, to, ready, Parent::ReadyWalk,
                            generation, reverse, &mut self.parents, &mut self.stats
                        ) {
                        changed_ready.push(to);
                    }
                }
            }
            self.stats.walking_ms += walking_started.elapsed().as_secs_f64() * 1_000.0;
            let terminals = if reverse { &q.access } else { &q.egress };
            for ep in terminals {
                for stage in [Stage::Ride, Stage::Walk] {
                    if current.get(stage, ep.stop.idx(), generation).is_none() {
                        continue;
                    }
                    let Some(journey) = reconstruct(
                        &self.rounds, &self.parents, generation, reverse,
                        k, ep.stop.idx(), stage, ep.seconds,
                    ) else { continue };
                    self.stats.profile_journeys_reconstructed += 1;
                    if valid_journey(tt, q, &journey) {
                        self.journeys.push(journey);
                    } else {
                        self.stats.profile_journeys_rejected += 1;
                    }
                }
            }
            changed_ready.sort_unstable();
            changed_ready.dedup();
            marked = changed_ready;
            if self.stats.profile_budget_exhausted { break; }
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
