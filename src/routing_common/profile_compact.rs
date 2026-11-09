//! Legacy event-profile scanner and shared lower-bound preprocessing.
//! Live Edelweiss PONG uses nigiri_raptor.rs and nigiri_reconstruct.rs.
//! This scanner remains for the separate classic profile API and regressions.
//!
//! Each (round, stop) has three *scalar* states: ride arrival, physical walk
//! arrival, and transfer-ready time. The third state enforces GTFS stop-pair
//! transfer rules without retaining a Pareto vector of arrival histories.
//! Accepted predecessor snapshots are immutable, and terminal journey
//! reconstruction follows the captured boarding/footpath provenance.
//! Forward profile anchors are evaluated latest-first.
use crate::routing_common::dial::{Dial, dijkstra};
use crate::routing_common::profile_compact_reconstruct::{reconstruct, valid_journey};
use crate::routing_common::profile_search::StartLabel;
use crate::routing_common::raptor_query::{
    Journey, Query, RaptorStats, optimize_same_trip_endpoint_walks,
};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use crate::routing_common::transport_index::TransportIndex;
use anyhow::{Result, ensure};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

const INF: i64 = i64::MAX / 4;
const NEG_INF: i64 = i64::MIN / 4;
/// Keep a bounded set of near-optimal route alternatives, even after a
/// competitive destination arrival has been found.
// Bound alternatives only at the destination, not at every intermediate
// station. The six route-distinct display options require at least six
// independently reconstructable destination snapshots.
const MAX_TERMINAL_PATHS_PER_STOP: usize = 12;
const MAX_TERMINAL_PATHS_PER_ROUND: usize = 4096;

fn better(candidate: i64, incumbent: i64, reverse: bool) -> bool {
    if reverse {
        candidate > incumbent
    } else {
        candidate < incumbent
    }
}

/// A stop state with no fewer boardings and a no-better time is dominated.
fn dominated_by_lower_round(
    prior: &[Round],
    stage: Stage,
    at: usize,
    time: i64,
    generation: u32,
    reverse: bool,
) -> bool {
    prior.iter().any(|round| {
        round
            .get(stage, at, generation)
            .is_some_and(|state| !better(time, state.time, reverse))
    })
}

/// Nigiri-style persistent profile arrival comparisons. This history only
/// contains anchors solved in monotone profile order and is invalidated when
/// an interval extension breaks that order.
fn dominated_by_profile_with_mode(
    profile: &[Vec<i64>],
    k: usize,
    at: usize,
    time: i64,
    reverse: bool,
    loose: bool,
) -> bool {
    profile
        .iter()
        .take(k + 1)
        .any(|row| !better(time, row[at], reverse) && (!loose || time != row[at]))
}

#[cfg(test)]
fn dominated_by_profile(
    profile: &[Vec<i64>],
    k: usize,
    at: usize,
    time: i64,
    reverse: bool,
) -> bool {
    dominated_by_profile_with_mode(profile, k, at, time, reverse, false)
}

/// Nigiri raptor::fill_bounds/set_bounds, in the mmap timetable's seconds.
/// Rows contain the best ping time with at most k boardings. The opposite
/// search indexes them by its remaining boarding budget.
pub(crate) struct PingBounds {
    rows: Vec<Vec<i64>>,
}

fn within_ping_bounds(
    bounds: Option<&PingBounds>,
    last_round: usize,
    round: usize,
    stop: usize,
    time: i64,
    reverse: bool,
    transfer_seconds: u32,
) -> bool {
    let Some(bounds) = bounds else {
        return true;
    };
    let bound = bounds.rows[last_round - round][stop];
    // raptor::within_bounds compensates for the transfer already present in
    // the ping round time. Comparisons are inclusive on the CPU.
    if reverse {
        time >= bound.saturating_sub(i64::from(transfer_seconds))
    } else {
        time <= bound.saturating_add(i64::from(transfer_seconds))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Stage {
    Ride,
    Walk,
    Ready,
}

#[derive(Clone, Copy)]
pub(super) enum Parent {
    Root {
        seconds: u32,
    },
    Ride {
        from: usize,
        to: usize,
        route: u32,
        transport: u32,
        day: u16,
        board_pos: usize,
        alight_pos: usize,
        departure: i64,
        arrival: i64,
    },
    Walk {
        from: usize,
        to: usize,
        seconds: u32,
    },
    ReadyRide,
}

/// Immutable predecessor snapshot. Compact arrival cells may be improved many
/// times in one search, but paths already derived from them must never change.
#[derive(Clone, Copy)]
pub(super) struct PathLink {
    pub step: Parent,
    pub previous: Option<u32>,
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
        Self {
            ride: vec![Cell::default(); n],
            walk: vec![Cell::default(); n],
            ready: vec![Cell::default(); n],
        }
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
        self.cells(stage)
            .get(stop)
            .filter(|c| c.generation == generation)
    }
    #[cfg(test)]
    fn put(
        &mut self,
        stage: Stage,
        stop: usize,
        time: i64,
        parent: Parent,
        generation: u32,
        reverse: bool,
        parents: &mut Vec<PathLink>,
        stats: &mut RaptorStats,
    ) -> bool {
        self.put_linked(
            stage, stop, time, parent, None, generation, reverse, parents, stats,
        )
    }

    fn put_linked(
        &mut self,
        stage: Stage,
        stop: usize,
        time: i64,
        parent: Parent,
        previous: Option<u32>,
        generation: u32,
        reverse: bool,
        parents: &mut Vec<PathLink>,
        stats: &mut RaptorStats,
    ) -> bool {
        let c = &mut self.cells_mut(stage)[stop];
        if c.generation == generation
            && if reverse {
                c.time >= time
            } else {
                c.time <= time
            }
        {
            stats.labels_dominated += 1;
            return false;
        }
        if parents.len() >= u32::MAX as usize {
            stats.profile_budget_exhausted = true;
            return false;
        }
        // The old implementation rewrote parents[c.parent] here. That
        // silently changed previously selected walking/ride paths and could
        // invalidate journeys already discovered at another stop.
        c.parent = parents.len() as u32;
        parents.push(PathLink {
            step: parent,
            previous,
        });
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
struct Onboard<'a> {
    transport: TransportIdx,
    day: usize,
    from: usize,
    pos: usize,
    event: i64,
    predecessor: u32,
    // Nigiri transport_times: decode the pattern once when boarding changes.
    first_departure: i64,
    events: &'a [crate::routing_common::timetable::Delta],
}
impl Onboard<'_> {
    fn time(&self, pos: usize, arrival: bool) -> i64 {
        let slot = if arrival { pos * 2 - 1 } else { pos * 2 };
        self.first_departure + i64::from(self.events[slot].seconds())
    }
}

/// Immutable optimistic route-duration topology. Build this once per mmap
/// timetable at startup. Each request only runs reverse Dijkstra from its
/// own OSR destination offsets, as with Nigiri's lower-bound preparation.
///
/// Edges are deliberately optimistic: an intermediate stop need not permit
/// boarding when a passenger is already on the same vehicle.
pub(crate) struct LowerBoundTopology {
    transports: Arc<TransportIndex>,
    forward: CsrGraph,
    reverse: CsrGraph,
    stations: Vec<usize>,
    transfer_sensitive: Arc<Vec<bool>>,
    importance: Arc<crate::routing_common::transfer_optimizer::StationImportance>,
}
struct CsrGraph {
    offsets: Vec<usize>,
    edges: Vec<(u32, u32)>,
}
impl CsrGraph {
    fn from_edges(n: usize, edge_iter: impl IntoIterator<Item = (u32, u32, u32)>) -> Self {
        // Nigiri build_lb_graph keeps only the cheapest edge per station pair.
        let mut edges = edge_iter
            .into_iter()
            .filter(|(from, to, _)| from != to)
            .collect::<Vec<_>>();
        edges.sort_unstable();
        edges.dedup_by_key(|edge| (edge.0, edge.1));
        let mut offsets = vec![0usize; n + 1];
        for &(from, _, _) in &edges {
            offsets[from as usize + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut cursors = offsets[..n].to_vec();
        let mut compressed = vec![(0u32, 0u32); edges.len()];
        for (from, to, cost) in edges {
            let cursor = &mut cursors[from as usize];
            compressed[*cursor] = (to, cost);
            *cursor += 1;
        }
        Self {
            offsets,
            edges: compressed,
        }
    }
    fn successors(&self, stop: usize) -> &[(u32, u32)] {
        &self.edges[self.offsets[stop]..self.offsets[stop + 1]]
    }
}
impl LowerBoundTopology {
    pub(crate) fn transport_index(&self) -> Arc<TransportIndex> {
        Arc::clone(&self.transports)
    }

    pub(crate) fn station_importance(
        &self,
    ) -> Arc<crate::routing_common::transfer_optimizer::StationImportance> {
        Arc::clone(&self.importance)
    }

    pub(crate) fn transfer_sensitive(&self) -> Arc<Vec<bool>> {
        Arc::clone(&self.transfer_sensitive)
    }

    pub(crate) fn build(tt: &ZeroCopyTimetable<'_>) -> Self {
        let stations = station_groups(tt.location_parent());
        let n = tt.coordinates().len();
        let mut edges: Vec<(u32, u32, u32)> = Vec::new();
        for route_id in 0..tt.route_transport_ranges().len() {
            let route = RouteIdx(route_id as u32);
            let stops = tt.stops_on(route);
            if stops.len() < 2 {
                continue;
            }
            let mut minimum = vec![u32::MAX; stops.len() - 1];
            let mut seen_patterns = HashSet::new();
            let range = tt.route_transport_ranges()[route_id];
            for index in range.from..range.to {
                let t = TransportIdx(index);
                let Some(profile) = tt.transport_timing_profile().get(t.idx()) else {
                    continue;
                };
                if !seen_patterns.insert(profile.idx()) {
                    continue;
                }
                let Some(events) = tt.events_for_transport(t) else {
                    continue;
                };
                for pos in 0..minimum.len() {
                    if let (Some(dep), Some(arr)) = (events.get(pos * 2), events.get(pos * 2 + 1)) {
                        minimum[pos] =
                            minimum[pos].min(arr.seconds().saturating_sub(dep.seconds()));
                    }
                }
            }
            for (pos, pair) in stops.windows(2).enumerate() {
                let cost = if minimum[pos] == u32::MAX {
                    0
                } else {
                    minimum[pos]
                };
                edges.push((
                    stations[pair[0].location().idx()] as u32,
                    stations[pair[1].location().idx()] as u32,
                    cost,
                ));
            }
        }
        // Like Nigiri loader/build_lb_graph.h, include footpaths in the
        // contracted graph at load time. Never scan full platform transfer
        // lists again in query-local lower-bound Dijkstra.
        for (from, &station) in stations.iter().enumerate() {
            for fp in tt.footpaths_from(LocationIdx(from as u32)) {
                edges.push((
                    station as u32,
                    stations[fp.target().idx()] as u32,
                    fp.seconds(),
                ));
            }
        }
        let forward = CsrGraph::from_edges(n, edges.iter().copied());
        let reverse =
            CsrGraph::from_edges(n, edges.iter().map(|&(from, to, cost)| (to, from, cost)));
        let mut transfer_sensitive = vec![false; stations.len()];
        for rule in tt.station_transfer_rules() {
            transfer_sensitive[rule.from.idx()] = true;
            transfer_sensitive[rule.to.idx()] = true;
        }
        Self {
            forward,
            reverse,
            stations,
            transfer_sensitive: Arc::new(transfer_sensitive),
            transports: Arc::new(TransportIndex::build(tt)),
            importance: Arc::new(
                crate::routing_common::transfer_optimizer::StationImportance::build(tt),
            ),
        }
    }
}

/// Merge every parent/child component, including nested station groups.
/// Union-find also terminates for a malformed parent cycle; merging extra
/// vertices only weakens an optimistic lower bound, never transit routing.
fn station_groups(parents: &[LocationIdx]) -> Vec<usize> {
    fn root(groups: &mut [usize], mut at: usize) -> usize {
        while groups[at] != at {
            groups[at] = groups[groups[at]];
            at = groups[at];
        }
        at
    }
    let mut groups = (0..parents.len()).collect::<Vec<_>>();
    for (at, parent) in parents.iter().enumerate() {
        if *parent != LocationIdx::INVALID {
            let from = root(&mut groups, at);
            let to = root(&mut groups, parent.idx());
            groups[from] = to;
        }
    }
    for at in 0..parents.len() {
        let representative = root(&mut groups, at);
        groups[at] = representative;
    }
    groups
}

// Nigiri routing/dijkstra.{h,cc}: a full Dial traversal, with the global
// five-day bucket horizon. The request duration and wall timeout do not
// truncate preprocessing. Query/PONG bounds are applied by the search.
const LOWER_BOUND_HORIZON: u32 = 5 * 86_400;

pub(crate) fn point_lower_bounds(
    tt: &ZeroCopyTimetable<'_>, q: &Query, reverse: bool,
    topology: &LowerBoundTopology, stats: &mut RaptorStats,
) -> Vec<u32> {
    lower_bounds(tt, q, reverse, topology, stats)
}

fn lower_bounds(
    _tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    reverse: bool,
    topology: &LowerBoundTopology,
    stats: &mut RaptorStats,
) -> Vec<u32> {
    let graph = if reverse {
        &topology.forward
    } else {
        &topology.reverse
    };
    let mut dist = vec![u32::MAX; graph.offsets.len() - 1];
    let mut queue = Dial::new(LOWER_BOUND_HORIZON as usize, |label: &(u32, usize)| {
        label.0 as usize
    });
    // Upstream uses a map of minimum destination offsets, then seeds in
    // station-index order. Do not insert duplicate platform seeds.
    let mut minimum = std::collections::BTreeMap::<usize, u32>::new();
    let terminals = if reverse {
        &request.access
    } else {
        &request.egress
    };
    for ep in terminals {
        let station = topology.stations[ep.stop.idx()];
        minimum
            .entry(station)
            .and_modify(|d| *d = (*d).min(ep.seconds))
            .or_insert(ep.seconds);
    }
    for (station, duration) in minimum {
        // API endpoint offsets are <= 1 hour. Out-of-horizon offsets cannot
        // contribute to a journey within Nigiri's five-day maximum.
        if (duration as usize) < queue.n_buckets() {
            dist[station] = duration;
            queue.push((duration, station));
        }
    }
    let work = dijkstra(
        &graph.offsets,
        &graph.edges,
        &mut queue,
        &mut dist,
        u32::MAX,
    );
    stats.profile_lower_bound_nodes += work.settled;
    stats.profile_lower_bound_edges += work.edges;
    topology
        .stations
        .iter()
        .map(|&station| dist[station])
        .collect()
}

fn initial_destination_bound(anchor: i64, q: &Query, reverse: bool) -> i64 {
    // Nigiri search.h: worst_time_at_dest = start_time +/- min(
    //   fastest_direct, max_travel_time) + one unit for loose comparison.
    // This is a bound, not a preceding point-RAPTOR search.
    let maximum = i64::from(q.max_duration_seconds);
    let direct = q.direct_walk_seconds.map(i64::from).unwrap_or(maximum);
    let budget = maximum.min(direct).saturating_add(1);
    if reverse {
        anchor.saturating_sub(budget)
    } else {
        anchor.saturating_add(budget)
    }
}

fn can_reach(
    bound: &[u32],
    stop: usize,
    time: i64,
    anchor: i64,
    duration: u32,
    reverse: bool,
    best_dest: i64,
) -> bool {
    let Some(&lb) = bound.get(stop) else {
        return false;
    };
    if lb == u32::MAX {
        return false;
    }
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
    // Nigiri uses a loose (inclusive) destination bound. The previous
    // additive slack allowed walking and vehicle states outside the current
    // competitive horizon to propagate, particularly costly for long-distance
    // queries with many service days.
    let competitive = if reverse {
        best_dest == NEG_INF || possible_dest >= best_dest
    } else {
        best_dest == INF || possible_dest <= best_dest
    };
    within_max_duration && competitive
}

fn change_seconds(tt: &ZeroCopyTimetable<'_>, from: usize, to: usize) -> Option<u32> {
    let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(to as u32));
    if rule.is_some_and(|x| x.prohibited()) {
        return None;
    }
    Some(
        tt.location_transfer_seconds()[from]
            .max(tt.location_transfer_seconds()[to])
            .max(rule.map_or(0, |x| x.min_seconds)),
    )
}

fn change_seconds_for_search(
    tt: &ZeroCopyTimetable<'_>,
    from: usize,
    to: usize,
    sensitive: &[bool],
) -> Option<u32> {
    // Most footpaths have no pair-specific GTFS override. Nigiri's ordinary
    // static transfer path is a direct time-array lookup, not a binary search
    // over every country's transfer rules for each relaxed edge.
    if !sensitive[from] || !sensitive[to] {
        Some(tt.location_transfer_seconds()[from].max(tt.location_transfer_seconds()[to]))
    } else {
        change_seconds(tt, from, to)
    }
}


fn routes_from_marked(
    tt: &ZeroCopyTimetable<'_>,
    marked: &[usize],
    stamps: &mut [u32],
    stamp: &mut u32,
) -> Vec<RouteIdx> {
    *stamp = stamp.wrapping_add(1);
    if *stamp == 0 {
        stamps.fill(0);
        *stamp = 1;
    }
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
    transports: Arc<TransportIndex>,
    rounds: Vec<Round>,
    parents: Vec<PathLink>,
    bounds: Vec<u32>,
    // Persistent scalar profile arrivals; these never own parent pointers.
    // Entries are usable only while anchors are processed monotonically.
    profile_ride: Vec<Vec<i64>>,
    profile_ready: Vec<Vec<i64>>,
    // GTFS restricted stop-pair transfers make provenance significant.
    // Suppressing an event-local route solely from a previous anchor's time
    // is unsafe at those stops unless the prior path is also carried.
    transfer_sensitive: Arc<Vec<bool>>,
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
    point_mode: bool,
    point_destination_bound: Option<i64>,
    ping_bounds: Option<PingBounds>,
    bounds_last_round: usize,
    #[cfg(test)]
    footpath_early_pruning: bool,
}
impl ProfileSearchState {
    #[cfg(test)]
    pub fn new(
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        earliest: i64,
        latest: i64,
        reverse: bool,
        timeout: Duration,
    ) -> Result<Self> {
        Self::new_with_topology(tt, q, earliest, latest, reverse, timeout, None)
    }

    pub fn new_with_topology(
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        earliest: i64,
        latest: i64,
        reverse: bool,
        timeout: Duration,
        precomputed: Option<&LowerBoundTopology>,
    ) -> Result<Self> {
        Self::new_internal(tt, q, earliest, latest, reverse, timeout, precomputed)
    }

    fn new_internal(
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        earliest: i64,
        latest: i64,
        reverse: bool,
        timeout: Duration,
        precomputed: Option<&LowerBoundTopology>,
    ) -> Result<Self> {
        ensure!(earliest <= latest, "invalid profile interval");
        ensure!(q.max_boardings <= 16, "max_boardings exceeds 16");
        ensure!(
            (1..=7 * 86_400).contains(&q.max_duration_seconds),
            "max_duration_seconds is outside 1 second to 7 days"
        );
        let n = tt.coordinates().len();
        for ep in q.access.iter().chain(q.egress.iter()) {
            ensure!(ep.stop.idx() < n, "OSR endpoint index out of bounds");
        }
        let start = Instant::now();
        let deadline = start.checked_add(timeout).unwrap_or(start);
        let mut stats = RaptorStats::default();
        stats.profile_searches = 1;
        let bounds_started = Instant::now();
        let owned = precomputed.is_none().then(|| LowerBoundTopology::build(tt));
        let topology = precomputed
            .or(owned.as_ref())
            .expect("topology must be supplied or constructed");
        let bounds = lower_bounds(tt, q, reverse, topology, &mut stats);
        stats.profile_lower_bounds_ms += bounds_started.elapsed().as_secs_f64() * 1_000.0;
        let invalid = if reverse { NEG_INF } else { INF };
        let transfer_sensitive = Arc::clone(&topology.transfer_sensitive);
        Ok(Self {
            transports: Arc::clone(&topology.transports),
            rounds: (0..=q.max_boardings).map(|_| Round::new(n)).collect(),
            parents: Vec::new(),
            bounds,
            profile_ride: vec![vec![invalid; n]; q.max_boardings + 1],
            profile_ready: vec![vec![invalid; n]; q.max_boardings + 1],
            transfer_sensitive,
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
            point_mode: false,
            point_destination_bound: None,
            ping_bounds: None,
            bounds_last_round: 0,
            #[cfg(test)]
            footpath_early_pruning: true,
        })
    }

    /// PONG shares the round buffers between the two search directions, as
    /// Nigiri does. Only ping's immutable lower bounds and the folded ping
    /// round times survive a direction switch.
    pub(crate) fn flip_point_direction(&mut self, other_lower_bounds: &mut Vec<u32>) {
        self.reverse = !self.reverse;
        std::mem::swap(&mut self.bounds, other_lower_bounds);
        self.reset_point_arrivals();
    }

    pub(crate) fn reset_point_arrivals(&mut self) {
        let invalid = if self.reverse { NEG_INF } else { INF };
        for row in &mut self.profile_ride {
            row.fill(invalid);
        }
        for row in &mut self.profile_ready {
            row.fill(invalid);
        }
        self.time_at_dest.fill(invalid);
        self.journeys.clear();
        self.last_anchor = None;
        self.ping_bounds = None;
        self.bounds_last_round = 0;
    }

    pub(crate) fn fill_ping_bounds(&self, n_rows: usize) -> PingBounds {
        let mut rows = Vec::<Vec<i64>>::with_capacity(n_rows);
        for k in 0..n_rows {
            let mut row = self.profile_ready[k].clone();
            for (stop, time) in row.iter_mut().enumerate() {
                // Direct translation of raptor::fill_bounds: row zero is
                // copied; each later row is folded with the preceding row.
                // Ping records round times before destination/LB pruning,
                // as update_transfers/update_footpaths do in Nigiri. Do not
                // try to reconstruct that matrix from surviving ride cells.
                if k != 0 && better(rows[k - 1][stop], *time, self.reverse) {
                    *time = rows[k - 1][stop];
                }
                // A scalar ping bound cannot encode a prohibited or minimum
                // stop-pair transfer. Keep the existing provenance-sensitive
                // GTFS validation authoritative at those locations.
                if self.transfer_sensitive[stop] {
                    *time = if self.reverse { INF } else { NEG_INF };
                }
            }
            rows.push(row);
        }
        PingBounds { rows }
    }

    pub(crate) fn set_ping_bounds(&mut self, bounds: PingBounds) {
        self.ping_bounds = Some(bounds);
    }

    pub(crate) fn transport_index(&self) -> Arc<TransportIndex> {
        Arc::clone(&self.transports)
    }

    /// Nigiri raptor::limit_time_at_dest/update_time_at_dest. Call after
    /// reset_point_arrivals and before execute_point to seed a ping from
    /// already validated PONG results. Catenary indexes by boardings.
    pub(crate) fn limit_time_at_dest(&mut self, boardings: usize, time: i64) {
        for bound in self.time_at_dest.iter_mut().skip(boardings) {
            if better(time, *bound, self.reverse) {
                *bound = time;
            }
        }
    }

    /// Nigiri's point execute(start_time, max_transfers, worst_time_at_dest).
    /// The caller resets arrivals once for ping and once before the sequence
    /// of pongs. Successive pongs retain scalar round arrivals and validated
    /// predecessor snapshots, independently of their new start-time key.
    pub(crate) fn execute_point(
        &mut self,
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        anchor: i64,
        max_boardings: usize,
        worst_time_at_dest: i64,
    ) -> Result<Vec<Journey>> {
        ensure!(max_boardings < self.rounds.len(), "point boarding budget exceeds buffers");
        self.point_mode = true;
        self.point_destination_bound = Some(worst_time_at_dest);
        self.bounds_last_round = max_boardings;
        let mut q = q.clone();
        q.max_boardings = max_boardings;
        let starts = StartLabel::point(&q, anchor, self.reverse);
        self.stats.profile_anchor_events += 1;
        self.stats.profile_start_labels += starts.len() as u64;
        let started = Instant::now();
        self.solve(tt, &q, anchor, &starts);
        self.stats.profile_compact_ms += started.elapsed().as_secs_f64() * 1_000.0;

        // Point results compare destination time and boardings. Optimized
        // physical departure times must not replace the requested start key:
        // PONG obtains that key from its opposite-direction validation pass.
        let mut frontier = Vec::<Journey>::new();
        for journey in &self.journeys {
            let dest = if self.reverse {
                journey.departure_unix_seconds
            } else {
                journey.arrival_unix_seconds
            };
            if journey.boardings > max_boardings
                || better(worst_time_at_dest, dest, self.reverse)
                || better(dest, anchor, self.reverse)
            {
                continue;
            }
            let dominates = |a: &Journey, b: &Journey| {
                let a_time = if self.reverse { a.departure_unix_seconds } else { a.arrival_unix_seconds };
                let b_time = if self.reverse { b.departure_unix_seconds } else { b.arrival_unix_seconds };
                a.boardings <= b.boardings && !better(b_time, a_time, self.reverse)
            };
            if frontier.iter().any(|old| dominates(old, journey)) {
                continue;
            }
            frontier.retain(|old| !dominates(journey, old));
            frontier.push(journey.clone());
        }
        Ok(frontier)
    }

    #[cfg(test)]
    pub fn extend(&mut self, tt: &ZeroCopyTimetable<'_>, q: &Query, anchors: &[i64]) -> Result<()> {
        let mut starts = Vec::new();
        for &anchor in anchors {
            starts.extend(StartLabel::point(q, anchor, self.reverse));
        }
        self.extend_starts(tt, q, &starts)
    }

    pub fn extend_starts(
        &mut self,
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        starts: &[StartLabel],
    ) -> Result<()> {
        ensure!(
            starts.iter().all(|s| s.via_state == 0),
            "via routing is not supported"
        );
        let mut ordered = starts.to_vec();
        ordered.sort_unstable_by_key(|s| (s.time_at_start, s.stop.0, s.time_at_stop));
        ordered.dedup();
        if !self.reverse {
            ordered.reverse();
        }
        let mut begin = 0;
        while begin < ordered.len() {
            let anchor = ordered[begin].time_at_start;
            let mut end = begin + 1;
            while end < ordered.len() && ordered[end].time_at_start == anchor {
                end += 1;
            }
            let group = &ordered[begin..end];
            begin = end;
            if Instant::now() >= self.deadline {
                self.stats.profile_budget_exhausted = true;
                break;
            }
            // Extending an interval toward later forward departures (or
            // earlier backward arrivals) invalidates profile dominance from
            // previously solved anchors. Nigiri resets persisted round arrivals
            // at this boundary too, while retaining already found journeys.
            if self.last_anchor.is_some_and(|last| {
                if self.reverse {
                    anchor < last
                } else {
                    anchor > last
                }
            }) {
                let invalid = if self.reverse { NEG_INF } else { INF };
                for row in &mut self.profile_ride {
                    row.fill(invalid);
                }
                for row in &mut self.profile_ready {
                    row.fill(invalid);
                }
                self.time_at_dest.fill(invalid);
            }
            self.last_anchor = Some(anchor);
            self.stats.profile_anchor_events += 1;
            self.stats.profile_start_labels += group.len() as u64;
            self.solve(tt, q, anchor, group);
            if self.stats.profile_budget_exhausted {
                break;
            }
        }
        Ok(())
    }

    fn solve(&mut self, tt: &ZeroCopyTimetable<'_>, q: &Query, anchor: i64, starts: &[StartLabel]) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            for r in &mut self.rounds {
                r.invalidate();
            }
            self.generation = 1;
        }
        self.parents.clear();
        let generation = self.generation;
        let reverse = self.reverse;
        // Nigiri's CPU PONG completes a whole sweep before checking time.
        // Classic profile search retains its existing inner cancellation.
        let deadline = (!self.point_mode).then_some(self.deadline);
        let is_timeout_reached = || deadline.is_some_and(|end| Instant::now() >= end);
        // Reset only the per-start competitiveness limit, not the arrival
        // matrix. Nigiri executes every start with a worst destination time
        // before updating it from transit and walking relaxations.
        let worst = self.point_destination_bound
            .unwrap_or_else(|| initial_destination_bound(anchor, q, reverse));
        for bound in &mut self.time_at_dest {
            if better(worst, *bound, reverse) {
                *bound = worst;
            }
        }
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
        let mut marked = Vec::new();
        for start in starts {
            let t = start.time_at_stop;
            let ep = crate::routing_common::raptor_query::WalkingEndpoint {
                stop: start.stop,
                seconds: t.abs_diff(anchor) as u32,
            };
            if (lower..=upper).contains(&t)
                && can_reach(
                    &self.bounds,
                    ep.stop.idx(),
                    t,
                    anchor,
                    q.max_duration_seconds,
                    reverse,
                    self.time_at_dest[0],
                )
                && (self.transfer_sensitive[ep.stop.idx()]
                    || !dominated_by_profile_with_mode(&self.profile_ready, 0, ep.stop.idx(), t, reverse, self.point_mode))
                && self.rounds[0].put_linked(
                    Stage::Ready,
                    ep.stop.idx(),
                    t,
                    Parent::Root {
                        seconds: ep.seconds,
                    },
                    None,
                    generation,
                    reverse,
                    &mut self.parents,
                    &mut self.stats,
                )
            {
                marked.push(ep.stop.idx());
            }
        }
        marked.sort_unstable();
        marked.dedup();
        reached_ready[0] = marked.clone();
        let destination_endpoints = if reverse { &q.access } else { &q.egress };
        let mut terminal_seconds = HashMap::<usize, u32>::new();
        for ep in destination_endpoints {
            terminal_seconds
                .entry(ep.stop.idx())
                .and_modify(|current| *current = (*current).min(ep.seconds))
                .or_insert(ep.seconds);
        }
        for k in 1..=q.max_boardings {
            if marked.is_empty() || is_timeout_reached() {
                if is_timeout_reached() {
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
            for &stop in &marked {
                self.stop_stamps[stop] = self.stop_stamp;
            }
            let routes = routes_from_marked(tt, &marked, &mut self.stamps, &mut self.stamp);
            let mut improved_rides = Vec::new();
            // Independent snapshot paths are required for route-distinct
            // terminal journeys even when their arrival cells tie. Nigiri
            // selects destinations using a Pareto frontier before extracting
            // journeys; these bounded snapshots provide the corresponding
            // invariant for the Gentian compact solver.
            let mut ride_terminal_paths = Vec::<(i64, usize, u32, u32)>::new();
            let mut candidate_slots = HashMap::<usize, Vec<usize>>::new();
            // Cache both successful and rejected early reconstructions by
            // immutable snapshot. Round finalization must not extract and
            // validate the same predecessor chain a second time.
            let mut reconstructed_terminals = HashMap::<u32, Option<Journey>>::new();
            // Finish route scanning before taking immutable references to
            // round predecessors for timetable-validated destination updates.
            {
                let (before, after) = self.rounds.split_at_mut(k);
                let previous = &before[k - 1];
                let current = &mut after[0];
                let started = Instant::now();
                for route in routes {
                    if is_timeout_reached() {
                        self.stats.profile_budget_exhausted = true;
                        break;
                    }
                    self.stats.routes_scanned += 1;
                    let stops = tt.stops_on(route);
                    if stops.len() < 2 {
                        continue;
                    }
                    for chain in self.transports.chains(route) {
                        let mut onboard: Option<Onboard<'_>> = None;
                        for step in 0..stops.len() {
                            self.stats.stop_events_scanned += 1;
                            if (self.stats.stop_events_scanned & 255) == 0
                                && is_timeout_reached()
                            {
                                self.stats.profile_budget_exhausted = true;
                                break;
                            }
                            let pos = if reverse {
                                stops.len() - step - 1
                            } else {
                                step
                            };
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
                            if can_finish {
                                if let Some(onboard) = onboard {
                                    let event = onboard.time(pos, !reverse);
                                    if (lower..=upper).contains(&event)
                                        && within_ping_bounds(
                                            self.ping_bounds.as_ref(), self.bounds_last_round,
                                            k, loc, event, reverse,
                                            tt.location_transfer_seconds()[loc],
                                        )
                                        && can_reach(
                                            &self.bounds,
                                            loc,
                                            event,
                                            anchor,
                                            q.max_duration_seconds,
                                            reverse,
                                            self.time_at_dest[k],
                                        )
                                    {
                                        let (from, to, board_pos, alight_pos, dep, arr) = if reverse
                                        {
                                            (
                                                loc,
                                                onboard.from,
                                                pos,
                                                onboard.pos,
                                                event,
                                                onboard.event,
                                            )
                                        } else {
                                            (
                                                onboard.from,
                                                loc,
                                                onboard.pos,
                                                pos,
                                                onboard.event,
                                                event,
                                            )
                                        };
                                        let leg_parent = Parent::Ride {
                                            from,
                                            to,
                                            route: route.0,
                                            transport: onboard.transport.0,
                                            day: onboard.day as u16,
                                            board_pos,
                                            alight_pos,
                                            departure: dep,
                                            arrival: arr,
                                        };
                                        // Capture distinct terminal paths even if a
                                        // faster service has already won this cell.
                                        // Use a bounded top-K per stop to retain
                                        // equivalent-time route alternatives.
                                        if let Some(&seconds) = terminal_seconds.get(&loc) {
                                            let score = if reverse {
                                                event.saturating_sub(i64::from(seconds))
                                            } else {
                                                event.saturating_add(i64::from(seconds))
                                            };
                                            let within_destination_bound = !self.point_mode
                                                || (!better(self.time_at_dest[k], score, reverse)
                                                    && !better(score, anchor, reverse));
                                            let slots = candidate_slots.entry(loc).or_default();
                                            let insertion = if slots.len()
                                                < MAX_TERMINAL_PATHS_PER_STOP
                                                && ride_terminal_paths.len()
                                                    < MAX_TERMINAL_PATHS_PER_ROUND
                                            {
                                                Some(None)
                                            } else {
                                                let worst =
                                                    slots.iter().copied().max_by_key(|&idx| {
                                                        let v = ride_terminal_paths[idx].0;
                                                        if reverse { v.saturating_neg() } else { v }
                                                    });
                                                worst
                                                    .filter(|&idx| {
                                                        let old = ride_terminal_paths[idx].0;
                                                        if reverse {
                                                            score > old
                                                        } else {
                                                            score < old
                                                        }
                                                    })
                                                    .map(Some)
                                            };
                                            if let Some(replace) = insertion.filter(|_| within_destination_bound) {
                                                if self.parents.len() < u32::MAX as usize {
                                                    let snapshot = self.parents.len() as u32;
                                                    self.parents.push(PathLink {
                                                        step: leg_parent,
                                                        previous: Some(onboard.predecessor),
                                                    });
                                                    // Nigiri updates time_at_dest_ as soon as a
                                                    // destination label improves, during route
                                                    // relaxation, not after scanning every route
                                                    // and service day in the round. Validate the
                                                    // predecessor chain before using it as a
                                                    // pruning bound: GTFS pair-specific transfers
                                                    // make unvalidated scalar bounds unsafe here.
                                                    if better(score, self.time_at_dest[k], reverse) {
                                                        let journey = crate::routing_common::profile_compact_reconstruct::reconstruct_validated_snapshot(
                                                            tt, q, &self.parents, snapshot, event,
                                                            loc, reverse, seconds, self.point_mode,
                                                            &mut self.stats,
                                                        );
                                                        if let Some(journey) = &journey {
                                                            let actual = if reverse {
                                                                journey.departure_unix_seconds
                                                            } else {
                                                                journey.arrival_unix_seconds
                                                            };
                                                            for bound in self.time_at_dest.iter_mut().skip(k) {
                                                                if better(actual, *bound, reverse) {
                                                                    *bound = actual;
                                                                }
                                                            }
                                                        }
                                                        reconstructed_terminals.insert(snapshot, journey);
                                                    }
                                                    let item = (score, loc, seconds, snapshot);
                                                    if let Some(idx) = replace {
                                                        reconstructed_terminals.remove(&ride_terminal_paths[idx].3);
                                                        ride_terminal_paths[idx] = item;
                                                    } else {
                                                        slots.push(ride_terminal_paths.len());
                                                        ride_terminal_paths.push(item);
                                                    }
                                                }
                                            }
                                        }
                                        if (self.transfer_sensitive[loc]
                                            || !dominated_by_profile_with_mode(
                                                &self.profile_ride,
                                                k,
                                                loc,
                                                event,
                                                reverse,
                                                self.point_mode,
                                            ))
                                            && !dominated_by_lower_round(
                                                before,
                                                Stage::Ride,
                                                loc,
                                                event,
                                                generation,
                                                reverse,
                                            )
                                            && current.put_linked(
                                                Stage::Ride,
                                                loc,
                                                event,
                                                leg_parent,
                                                Some(onboard.predecessor),
                                                generation,
                                                reverse,
                                                &mut self.parents,
                                                &mut self.stats,
                                            )
                                        {
                                            improved_rides.push(loc);
                                        }
                                    }
                                }
                            }
                            if !can_start || self.stop_stamps[loc] != self.stop_stamp {
                                continue;
                            }
                            // Nigiri raptor::update_route: after the marked
                            // boarding guard, an unreachable lower bound
                            // terminates this route scan before trip lookup.
                            if self.bounds[loc] == u32::MAX {
                                break;
                            }
                            let Some(ready) = previous.get(Stage::Ready, loc, generation) else {
                                continue;
                            };
                            if !(lower..=upper).contains(&ready.time)
                                || !can_reach(
                                    &self.bounds,
                                    loc,
                                    ready.time,
                                    anchor,
                                    q.max_duration_seconds,
                                    reverse,
                                    self.time_at_dest[k],
                                )
                            {
                                continue;
                            }
                            let onboard_time = onboard.map(|old| old.time(pos, reverse));
                            if onboard_time.is_some_and(|time| {
                                if reverse {
                                    ready.time < time
                                } else {
                                    ready.time > time
                                }
                            }) {
                                self.stats.transport_lookup_skipped_onboard += 1;
                                continue;
                            }
                            let slot = if reverse { pos * 2 - 1 } else { pos * 2 };
                            let destination_bound = if reverse {
                                self.time_at_dest[k].max(lower)
                            } else {
                                self.time_at_dest[k].min(upper)
                            };
                            let Some(selected) = chain.select(
                                tt,
                                slot,
                                ready.time,
                                reverse,
                                destination_bound,
                                self.bounds[loc],
                                &mut self.stats,
                            ) else {
                                continue;
                            };
                            if onboard_time.is_none_or(|time| {
                                if reverse {
                                    selected.event >= time
                                } else {
                                    selected.event <= time
                                }
                            }) {
                                let transport = selected.transport;
                                onboard = Some(Onboard {
                                    transport,
                                    day: selected.start_day,
                                    from: loc,
                                    pos,
                                    event: selected.event,
                                    predecessor: ready.parent,
                                    first_departure: (i64::from(tt.header().first_utc_day)
                                        + selected.start_day as i64)
                                        * 86_400
                                        + i64::from(
                                            tt.transport_first_dep_offset()[transport.idx()]
                                                .seconds(),
                                        ),
                                    events: tt
                                        .events_for_transport(transport)
                                        .expect("validated timing profile"),
                                });
                            }
                        }
                    }
                    if self.stats.profile_budget_exhausted {
                        break;
                    }
                }
                self.stats.route_scan_ms += started.elapsed().as_secs_f64() * 1_000.0;
            }
            improved_rides.sort_unstable();
            improved_rides.dedup();
            reached_rides[k] = improved_rides.clone();

            // Nigiri's time_at_dest_ is updated on vehicle arrivals before
            // it relaxes same-station transfers and footpaths. Recover this
            // safe subset of its early bound updates: materialize the complete
            // vehicle path and validate the GTFS service days and predecessor
            // transfer restrictions before tightening the deadline. Unsafe
            // provisional paths never suppress later valid alternatives.
            ride_terminal_paths.sort_unstable_by_key(|(time, stop, seconds, _)| {
                (
                    if reverse {
                        Reverse(*time)
                    } else {
                        Reverse(time.saturating_neg())
                    },
                    *stop,
                    *seconds,
                )
            });
            for (arrival, stop, seconds, snapshot) in ride_terminal_paths {
                let journey = match reconstructed_terminals.remove(&snapshot) {
                    Some(cached) => {
                        self.stats.profile_terminal_reconstruction_reused += 1;
                        cached
                    }
                    None => crate::routing_common::profile_compact_reconstruct::reconstruct_validated_snapshot(
                        tt, q, &self.parents, snapshot,
                        if reverse {
                            arrival.saturating_add(i64::from(seconds))
                        } else {
                            arrival.saturating_sub(i64::from(seconds))
                        },
                        stop, reverse, seconds, self.point_mode, &mut self.stats,
                    ),
                };
                let Some(journey) = journey else {
                    continue;
                };
                if !self.point_mode || !better(self.time_at_dest[k], if reverse {
                    journey.departure_unix_seconds
                } else {
                    journey.arrival_unix_seconds
                }, reverse)
                {
                    let metric = if reverse {
                        journey.departure_unix_seconds
                    } else {
                        journey.arrival_unix_seconds
                    };
                    for bound in self.time_at_dest.iter_mut().skip(k) {
                        if better(metric, *bound, reverse) {
                            *bound = metric;
                        }
                    }
                    self.journeys.push(journey);
                } else {
                    self.stats.profile_journeys_rejected += 1;
                }
            }

            // Point-mode ping retains labels before destination pruning for
            // its opposite-direction bounds. Only classic rRAPTOR may skip
            // those writes here.
            let prune_footpaths = !self.point_mode;
            #[cfg(test)]
            let prune_footpaths = prune_footpaths && self.footpath_early_pruning;
            let (before, after) = self.rounds.split_at_mut(k);
            let current = &mut after[0];
            let mut changed_ready = Vec::new();
            let walking_started = Instant::now();
            // Only the final best ride state at each stop is expanded; the
            // footprint is at most three cells per stop and RAPTOR round.
            for &from in &improved_rides {
                if is_timeout_reached() {
                    self.stats.profile_budget_exhausted = true;
                    break;
                }
                let Some(ride) = current.get(Stage::Ride, from, generation).copied() else {
                    continue;
                };
                let t = ride.time;
                if let Some(change) = change_seconds_for_search(tt, from, from, &self.transfer_sensitive) {
                    let ready = if reverse {
                        t.saturating_sub(i64::from(change))
                    } else {
                        t.saturating_add(i64::from(change))
                    };
                    // Nigiri update_transfers writes round_times_ before
                    // checking time_at_dest_ and destination lower bounds.
                    // These scalar times are needed by the opposite search
                    // even when this stop does not enter ping's frontier.
                    if self.point_mode && self.ping_bounds.is_none() {
                        let saved = &mut self.profile_ready[k][from];
                        if better(ready, *saved, reverse) {
                            *saved = ready;
                        }
                    }
                    if (lower..=upper).contains(&ready)
                        && within_ping_bounds(
                            self.ping_bounds.as_ref(), self.bounds_last_round,
                            k, from, ready, reverse,
                            tt.location_transfer_seconds()[from],
                        )
                        && can_reach(
                            &self.bounds,
                            from,
                            ready,
                            anchor,
                            q.max_duration_seconds,
                            reverse,
                            self.time_at_dest[k],
                        )
                        && (self.transfer_sensitive[from]
                            || !dominated_by_profile_with_mode(&self.profile_ready, k, from, ready, reverse, self.point_mode))
                        && !dominated_by_lower_round(
                            before,
                            Stage::Ready,
                            from,
                            ready,
                            generation,
                            reverse,
                        )
                        && current.put_linked(
                            Stage::Ready,
                            from,
                            ready,
                            Parent::ReadyRide,
                            Some(ride.parent),
                            generation,
                            reverse,
                            &mut self.parents,
                            &mut self.stats,
                        )
                    {
                        changed_ready.push(from);
                    }
                }
                let paths = if reverse {
                    tt.footpaths_to(LocationIdx(from as u32))
                } else {
                    tt.footpaths_from(LocationIdx(from as u32))
                };
                for fp in paths {
                    let seconds = fp.seconds();
                    let at = if reverse {
                        t.saturating_sub(i64::from(seconds))
                    } else {
                        t.saturating_add(i64::from(seconds))
                    };
                    // Incoming and outgoing lists are validated in ascending
                    // raw walking duration. Once the physical walk is strictly
                    // worse than this round's destination bound, every later
                    // walk also fails can_reach(). Transfer minima only make
                    // readiness worse; they need not preserve the raw ordering.
                    // Keep equality, matching can_reach's inclusive bound.
                    if prune_footpaths && better(self.time_at_dest[k], at, reverse) {
                        break;
                    }
                    self.stats.footpath_edges_examined += 1;
                    if self.stats.footpath_edges_examined & 255 == 0
                        && is_timeout_reached()
                    {
                        self.stats.profile_budget_exhausted = true;
                        break;
                    }
                    let to = fp.target().idx();
                    if seconds == 0 {
                        continue;
                    }
                    let (walk_from, walk_to) = if reverse { (to, from) } else { (from, to) };
                    let ready = change_seconds_for_search(tt, walk_from, walk_to, &self.transfer_sensitive).map(|change| {
                        if reverse {
                            at.min(t.saturating_sub(i64::from(change)))
                        } else {
                            at.max(t.saturating_add(i64::from(change)))
                        }
                    });
                    // Nigiri update_footpaths has the same pre-pruning
                    // round_times_ write as update_transfers. Save the legal
                    // transfer-ready time, independently of physical walks.
                    if self.point_mode && self.ping_bounds.is_none()
                        && let Some(ready) = ready
                    {
                        let saved = &mut self.profile_ready[k][to];
                        if better(ready, *saved, reverse) {
                            *saved = ready;
                        }
                    }
                    if !(lower..=upper).contains(&at)
                        || !can_reach(
                            &self.bounds,
                            to,
                            at,
                            anchor,
                            q.max_duration_seconds,
                            reverse,
                            self.time_at_dest[k],
                        )
                    {
                        continue;
                    }
                    // Physical arrival and boarding readiness are different
                    // dimensions: minimum change times do not extend a walk.
                    current.put_linked(
                        Stage::Walk,
                        to,
                        at,
                        Parent::Walk {
                            from: walk_from,
                            to: walk_to,
                            seconds,
                        },
                        Some(ride.parent),
                        generation,
                        reverse,
                        &mut self.parents,
                        &mut self.stats,
                    );
                    let Some(ready) = ready else {
                        continue;
                    };
                    if (lower..=upper).contains(&ready)
                        && within_ping_bounds(
                            self.ping_bounds.as_ref(), self.bounds_last_round,
                            k, to, ready, reverse,
                            tt.location_transfer_seconds()[to],
                        )
                        && can_reach(
                            &self.bounds,
                            to,
                            ready,
                            anchor,
                            q.max_duration_seconds,
                            reverse,
                            self.time_at_dest[k],
                        )
                        && (self.transfer_sensitive[to]
                            || !dominated_by_profile_with_mode(&self.profile_ready, k, to, ready, reverse, self.point_mode))
                        && !dominated_by_lower_round(
                            before,
                            Stage::Ready,
                            to,
                            ready,
                            generation,
                            reverse,
                        )
                        && current.put_linked(
                            Stage::Ready,
                            to,
                            ready,
                            Parent::Walk {
                                from: walk_from,
                                to: walk_to,
                                seconds,
                            },
                            Some(ride.parent),
                            generation,
                            reverse,
                            &mut self.parents,
                            &mut self.stats,
                        )
                    {
                        changed_ready.push(to);
                    }
                }
            }
            self.stats.walking_ms += walking_started.elapsed().as_secs_f64() * 1_000.0;
            // Nigiri collects destination times before reconstructing
            // Pareto-competitive candidates. Sort the scalar destination
            // states by actual endpoint time so the first valid journey
            // tightens time_at_dest for all following candidates.
            //
            // Invalid predecessors NEVER tighten the bound: this is crucial
            // when several transfer-ready states share an arrival timestamp.
            // Nigiri update_intermodal_footpaths uses tmp_ (the vehicle's
            // physical arrival), never a timetable-footpath round time.
            // OSR offsets already cover the intermodal access/egress. A
            // final table walk would become an initial table walk when
            // PONG reverses direction, which intermodal get_starts forbids.
            // Vehicle + OSR terminals were collected above. Table walks
            // still seed the next transit round, but cannot complete a
            // point-mode intermodal journey in either search direction.
            let terminals: &[crate::routing_common::raptor_query::WalkingEndpoint] =
                if self.point_mode {
                    &[]
                } else if reverse {
                    &q.access
                } else {
                    &q.egress
                };
            let mut terminal_candidates = Vec::new();
            for ep in terminals {
                // Ride destinations have already been validated above, so
                // now only new walking-terminal candidates need extraction.
                for stage in [Stage::Walk] {
                    if let Some(cell) = current.get(stage, ep.stop.idx(), generation) {
                        let at_dest = if reverse {
                            cell.time.saturating_sub(i64::from(ep.seconds))
                        } else {
                            cell.time.saturating_add(i64::from(ep.seconds))
                        };
                        terminal_candidates.push((at_dest, ep.stop.idx(), stage, ep.seconds));
                    }
                }
            }
            // Ordering is deterministic, regardless of OSR's endpoint order.
            terminal_candidates.sort_unstable_by_key(|(time, stop, stage, offset)| {
                (
                    if reverse {
                        Reverse(*time)
                    } else {
                        Reverse(time.saturating_neg())
                    },
                    *stop,
                    match stage {
                        Stage::Ride => 0u8,
                        Stage::Walk => 1,
                        Stage::Ready => 2,
                    },
                    *offset,
                )
            });
            for (at_dest, terminal_stop, stage, terminal_seconds) in terminal_candidates {
                let current_best = self.time_at_dest[k];
                let too_slow = if reverse {
                    current_best != NEG_INF && at_dest < current_best
                } else {
                    current_best != INF && at_dest > current_best
                };
                if too_slow {
                    continue;
                }
                let Some(mut journey) = reconstruct(
                    &self.rounds,
                    &self.parents,
                    generation,
                    reverse,
                    k,
                    terminal_stop,
                    stage,
                    terminal_seconds,
                ) else {
                    continue;
                };
                self.stats.profile_journeys_reconstructed += 1;
                // Optimize the same timetable vehicles first, as Nigiri's
                // separate reconstruction phase does. Validate optimized
                // service days and GTFS transfer constraints before recording
                // a destination bound or returning the journey.
                if !self.point_mode {
                    optimize_same_trip_endpoint_walks(tt, q, &mut journey);
                }
                if valid_journey(tt, q, &journey)
                    && (!self.point_mode || (!better(self.time_at_dest[k], if reverse {
                        journey.departure_unix_seconds
                    } else {
                        journey.arrival_unix_seconds
                    }, reverse)))
                {
                    let metric = if reverse {
                        journey.departure_unix_seconds
                    } else {
                        journey.arrival_unix_seconds
                    };
                    for bound in self.time_at_dest.iter_mut().skip(k) {
                        if better(metric, *bound, reverse) {
                            *bound = metric;
                        }
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
            if self.stats.profile_budget_exhausted {
                break;
            }
        }
        // Preserve the compact arrival skyline across monotone profile
        // anchors exactly as Nigiri's round_times_ does. The current
        // generation-tagged states act like Nigiri's event-local best_/tmp_.
        // Nigiri's execute() bounds all round iteration by this execution's
        // max_transfers, not the shared state's capacity. A pong can have a
        // smaller boarding budget than the ping that allocated the buffers.
        // Only the active rows have reached-stop lists; leave higher shared
        // rows intact for subsequent executions.
        {
            for (k, (ready_stops, ride_stops)) in
                reached_ready.iter().zip(&reached_rides).enumerate()
            {
                for &stop in ready_stops {
                    if let Some(value) = self.rounds[k].get(Stage::Ready, stop, generation) {
                        let current = &mut self.profile_ready[k][stop];
                        if better(value.time, *current, reverse) {
                            *current = value.time;
                        }
                    }
                }
                for &stop in ride_stops {
                    if let Some(value) = self.rounds[k].get(Stage::Ride, stop, generation) {
                        let current = &mut self.profile_ride[k][stop];
                        if better(value.time, *current, reverse) {
                            *current = value.time;
                        }
                    }
                }
            }
        }
    }

    pub fn recover_point(
        &mut self,
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        reference: i64,
        deadline: Instant,
    ) -> Result<()> {
        let exhausted = self.stats.profile_budget_exhausted;
        self.stats.profile_budget_exhausted = false;
        self.deadline = deadline;
        self.last_anchor = None;
        let invalid = if self.reverse { NEG_INF } else { INF };
        for row in &mut self.profile_ride {
            row.fill(invalid);
        }
        for row in &mut self.profile_ready {
            row.fill(invalid);
        }
        self.time_at_dest.fill(invalid);
        self.journeys.clear();
        let result = self.extend_starts(tt, q, &StartLabel::point(q, reference, self.reverse));
        self.stats.profile_budget_exhausted |= exhausted;
        result
    }

    pub fn result(&self) -> Vec<Journey> {
        crate::routing_common::profile_search::route_diverse_pareto(self.journeys.clone())
    }
    pub fn stats(&self) -> &RaptorStats {
        &self.stats
    }
}

#[cfg(test)]
pub(crate) fn route_profile_incremental(
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    anchors: &[i64],
    reverse: bool,
    timeout: Duration,
) -> Result<(Vec<Journey>, RaptorStats)> {
    let earliest = anchors
        .iter()
        .copied()
        .min()
        .unwrap_or(q.departure_unix_seconds);
    let latest = anchors
        .iter()
        .copied()
        .max()
        .unwrap_or(q.departure_unix_seconds);
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
        assert!(rounds[0].put(
            Stage::Ready,
            0,
            1_000,
            Parent::Root { seconds: 0 },
            1,
            false,
            &mut parents,
            &mut stats
        ));
        let root = rounds[0].get(Stage::Ready, 0, 1).unwrap().parent;
        assert!(rounds[1].put_linked(
            Stage::Ride,
            1,
            1_100,
            Parent::Ride {
                from: 0,
                to: 1,
                route: 0,
                transport: 0,
                day: 0,
                board_pos: 0,
                alight_pos: 1,
                departure: 1_020,
                arrival: 1_100
            },
            Some(root),
            1,
            false,
            &mut parents,
            &mut stats
        ));
        let ride = rounds[1].get(Stage::Ride, 1, 1).unwrap().parent;
        assert!(rounds[1].put_linked(
            Stage::Ready,
            2,
            1_140,
            Parent::Walk {
                from: 1,
                to: 2,
                seconds: 40
            },
            Some(ride),
            1,
            false,
            &mut parents,
            &mut stats
        ));
        assert!(rounds[1].put_linked(
            Stage::Walk,
            2,
            1_120,
            Parent::Walk {
                from: 3,
                to: 2,
                seconds: 20
            },
            Some(ride),
            1,
            false,
            &mut parents,
            &mut stats
        ));
        let result = reconstruct(&rounds, &parents, 1, false, 1, 2, Stage::Ready, 0)
            .expect("exact ready-state predecessor must reconstruct");
        assert_eq!(result.boardings, 1);
        assert!(matches!(
            result.legs[2],
            Leg::Transfer {
                from_stop: 1,
                to_stop: 2,
                seconds: 40
            }
        ));
        assert_eq!(result.arrival_unix_seconds, 1_140);
    }

    #[test]
    fn ready_from_ride_needs_no_footpath() {
        let mut rounds = vec![Round::new(2), Round::new(2)];
        let mut parents = Vec::new();
        let mut stats = RaptorStats::default();
        rounds[0].put(
            Stage::Ready,
            0,
            0,
            Parent::Root { seconds: 0 },
            1,
            false,
            &mut parents,
            &mut stats,
        );
        let root = rounds[0].get(Stage::Ready, 0, 1).unwrap().parent;
        rounds[1].put_linked(
            Stage::Ride,
            1,
            100,
            Parent::Ride {
                from: 0,
                to: 1,
                route: 0,
                transport: 0,
                day: 0,
                board_pos: 0,
                alight_pos: 1,
                departure: 10,
                arrival: 100,
            },
            Some(root),
            1,
            false,
            &mut parents,
            &mut stats,
        );
        let ride = rounds[1].get(Stage::Ride, 1, 1).unwrap().parent;
        rounds[1].put_linked(
            Stage::Ready,
            1,
            130,
            Parent::ReadyRide,
            Some(ride),
            1,
            false,
            &mut parents,
            &mut stats,
        );
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
    fn destination_bound_is_loose_but_has_no_arbitrary_extra_slack() {
        let lb = [30_u32];
        assert!(can_reach(&lb, 0, 100, 100, 1000, false, INF));
        assert!(can_reach(&lb, 0, 100, 100, 1000, false, 130));
        assert!(!can_reach(&lb, 0, 100, 100, 1000, false, 129));
        assert!(!can_reach(&lb, 0, 100, 100, 1000, false, -2000));
        assert!(can_reach(&lb, 0, 900, 900, 1000, true, NEG_INF));
        assert!(can_reach(&lb, 0, 900, 900, 1000, true, 870));
        assert!(!can_reach(&lb, 0, 900, 900, 1000, true, 871));
        assert!(!can_reach(&lb, 0, 900, 900, 1000, true, 4000));
    }

    #[test]
    fn initial_nigiri_destination_bound_does_not_cap_long_distance_without_direct() {
        use crate::routing_common::raptor_query::WalkingEndpoint;
        let mut q = Query {
            departure_unix_seconds: 1000,
            access: vec![WalkingEndpoint {
                stop: LocationIdx(0),
                seconds: 0,
            }],
            egress: vec![],
            direct_walk_seconds: None,
            max_boardings: 6,
            max_duration_seconds: 4 * 86_400,
        };
        assert_eq!(
            initial_destination_bound(1000, &q, false),
            1000 + 4 * 86_400 + 1
        );
        assert_eq!(
            initial_destination_bound(1000, &q, true),
            1000 - 4 * 86_400 - 1
        );
        q.direct_walk_seconds = Some(900);
        assert_eq!(initial_destination_bound(1000, &q, false), 1901);
        assert_eq!(initial_destination_bound(1000, &q, true), 99);
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
        BitfieldIdx, Delta, PackedStop, Range, SourceIdx, TimetableOwned, TimingProfileIdx,
        TrafficDays,
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
            tt.location_routes_offsets
                .push(tt.location_routes_offsets.len() as u32);
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
        tt.transport_first_dep_offset
            .push(Delta::from_seconds(9 * 3_600).unwrap());
        tt.transport_initial_day_offset.push(0);
        let mut days = TrafficDays::default();
        for day in 0..4 {
            days.set(day, true).unwrap();
        }
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
            access: vec![WalkingEndpoint {
                stop: LocationIdx(0),
                seconds: 0,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(1),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 36 * 3_600,
        }
    }

    fn train_with_endpoint_footpath(reverse: bool) -> TimetableOwned {
        use crate::routing_common::timetable::PackedFootpath;

        let mut tt = daily_train();
        // A third stop has no vehicle service. A table footpath connects it
        // to an end of the train, independently of the OSR endpoint costs.
        tt.coordinates.push(Default::default());
        tt.location_id.push(Default::default());
        tt.location_name.push(Default::default());
        tt.location_source.push(SourceIdx(0));
        tt.location_parent.push(LocationIdx::INVALID);
        tt.location_type.push(1);
        tt.location_transfer_seconds.push(0);
        tt.location_routes_offsets.push(tt.location_routes.len() as u32);
        if reverse {
            // Unavailable initial table walk in the forward validation:
            // stop 2 -> train's boarding stop 0.
            tt.footpaths_out.push(PackedFootpath::new(LocationIdx(0), 60).unwrap());
            tt.footpaths_out_offsets = vec![0, 0, 0, 1];
            tt.footpaths_in.push(PackedFootpath::new(LocationIdx(2), 60).unwrap());
            tt.footpaths_in_offsets = vec![0, 1, 1, 1];
        } else {
            // Unavailable initial table walk in the reverse validation:
            // train's alighting stop 1 -> stop 2.
            tt.footpaths_out.push(PackedFootpath::new(LocationIdx(2), 60).unwrap());
            tt.footpaths_out_offsets = vec![0, 0, 1, 1];
            tt.footpaths_in.push(PackedFootpath::new(LocationIdx(1), 60).unwrap());
            tt.footpaths_in_offsets = vec![0, 0, 0, 1];
        }
        tt
    }

    #[test]
    fn classic_footpath_early_exit_preserves_frontiers_across_anchors() {
        use crate::routing_common::timetable::PackedFootpath;

        let mut owned = daily_train();
        for _ in 2..6 {
            owned.coordinates.push(Default::default());
            owned.location_id.push(Default::default());
            owned.location_name.push(Default::default());
            owned.location_source.push(SourceIdx(0));
            owned.location_parent.push(LocationIdx::INVALID);
            owned.location_type.push(1);
            owned.location_transfer_seconds.push(0);
            owned
                .location_routes_offsets
                .push(owned.location_routes.len() as u32);
        }
        // The first edge has a long effective transfer time. It must not
        // terminate the raw-duration scan before the equal-bound second edge.
        owned.location_transfer_seconds[2] = 600;
        owned.footpaths_out_offsets = vec![0];
        owned.footpaths_in_offsets = vec![0];
        for stop in 0..6 {
            for (other, seconds) in [(2, 60), (3, 120), (4, 180), (5, 240)] {
                if stop == 1 {
                    owned
                        .footpaths_out
                        .push(PackedFootpath::new(LocationIdx(other), seconds).unwrap());
                } else if stop == other {
                    owned
                        .footpaths_out
                        .push(PackedFootpath::new(LocationIdx(0), seconds).unwrap());
                }
                if stop == 0 {
                    owned
                        .footpaths_in
                        .push(PackedFootpath::new(LocationIdx(other), seconds).unwrap());
                } else if stop == other {
                    owned
                        .footpaths_in
                        .push(PackedFootpath::new(LocationIdx(1), seconds).unwrap());
                }
            }
            owned
                .footpaths_out_offsets
                .push(owned.footpaths_out.len() as u32);
            owned
                .footpaths_in_offsets
                .push(owned.footpaths_in.len() as u32);
        }
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let frontier = |state: &ProfileSearchState| {
            let mut keys: Vec<_> = state
                .result()
                .iter()
                .map(|j| {
                    (
                        j.departure_unix_seconds,
                        j.arrival_unix_seconds,
                        j.boardings,
                    )
                })
                .collect();
            keys.sort_unstable();
            keys
        };
        for reverse in [false, true] {
            let mut q = query();
            q.max_boardings = 3;
            let ends = vec![
                WalkingEndpoint {
                    stop: LocationIdx(if reverse { 0 } else { 1 }),
                    seconds: 120,
                },
                WalkingEndpoint {
                    stop: LocationIdx(3),
                    seconds: 0,
                },
            ];
            if reverse {
                q.access = ends;
            } else {
                q.egress = ends;
            }
            let mut pruned = ProfileSearchState::new(
                &tt,
                &q,
                base,
                base + 4 * 86_400,
                reverse,
                Duration::from_secs(5),
            )
            .unwrap();
            let mut full = ProfileSearchState::new(
                &tt,
                &q,
                base,
                base + 4 * 86_400,
                reverse,
                Duration::from_secs(5),
            )
            .unwrap();
            full.footpath_early_pruning = false;
            // First retain bounds in the correct profile order, then reverse
            // that order to exercise the interval-extension reset as well.
            let days = if reverse { [0, 1, 0] } else { [1, 0, 2] };
            for (iteration, day) in days.into_iter().enumerate() {
                let anchor = base + day * 86_400 + if reverse { 17 * 3600 } else { 9 * 3600 };
                // A tighter three-boarding bound must not suppress the
                // one-boarding journey, including its equal-time table walk.
                pruned.time_at_dest[3] = if reverse { anchor + 1 } else { anchor - 1 };
                full.time_at_dest[3] = pruned.time_at_dest[3];
                pruned.extend(&tt, &q, &[anchor]).unwrap();
                full.extend(&tt, &q, &[anchor]).unwrap();
                assert!(!pruned.stats.profile_budget_exhausted);
                assert!(!full.stats.profile_budget_exhausted);
                assert!(!frontier(&full).is_empty());
                assert_eq!(frontier(&pruned), frontier(&full));
                assert_eq!(pruned.time_at_dest, full.time_at_dest);
                assert_eq!(pruned.profile_ride, full.profile_ride);
                assert_eq!(pruned.profile_ready, full.profile_ready);
                if iteration == 0 {
                    let expected = if reverse {
                        anchor - 8 * 3600 - 120
                    } else {
                        anchor + 8 * 3600 + 120
                    };
                    assert_eq!(pruned.time_at_dest[1], expected);
                    assert_eq!(
                        pruned.rounds[1]
                            .get(Stage::Walk, 3, pruned.generation)
                            .unwrap()
                            .time,
                        expected,
                        "equal-time walk was pruned: reverse={reverse}",
                    );
                }
            }
            assert!(pruned.stats.footpath_edges_examined < full.stats.footpath_edges_examined);
        }
    }

    #[test]
    fn intermodal_pong_cannot_finish_by_chaining_a_table_walk_to_an_osr_offset() {
        use crate::routing_common::pong::route_pong_with_stats_precomputed;
        use crate::routing_common::profile_search::ProfileOptions;

        let base = 20_000_i64 * 86_400;
        let departure = base + 9 * 3_600;
        let arrival = base + 17 * 3_600;
        for reverse in [false, true] {
            let bytes = train_with_endpoint_footpath(reverse).encode().unwrap();
            let tt = ZeroCopyTimetable::new(&bytes).unwrap();
            let mut q = query();
            q.max_boardings = 3;
            q.departure_unix_seconds = if reverse { arrival } else { departure };
            let offsets = vec![
                WalkingEndpoint {
                    stop: LocationIdx(if reverse { 0 } else { 1 }),
                    seconds: 600,
                },
                WalkingEndpoint { stop: LocationIdx(2), seconds: 0 },
            ];
            if reverse { q.access = offsets; } else { q.egress = offsets; }
            // The table path costs 60 seconds, but the actual OSR offset
            // from/to the train costs 600. Previously ping chose the table
            // shortcut and pong could not match the one-boarding journey.
            let (journeys, stats) = route_pong_with_stats_precomputed(
                &tt,
                &q,
                ProfileOptions {
                    search_window_seconds: 1,
                    min_itineraries: 1,
                    ..ProfileOptions::default()
                },
                reverse,
                None,
            )
            .unwrap();
            assert_eq!(journeys.len(), 1);
            assert_eq!(journeys[0].boardings, 1);
            assert_eq!(journeys[0].departure_unix_seconds, departure - if reverse { 600 } else { 0 });
            assert_eq!(journeys[0].arrival_unix_seconds, arrival + if reverse { 0 } else { 600 });
            assert!(journeys[0].legs.iter().all(|leg| !matches!(leg,
                crate::routing_common::raptor_query::Leg::Transfer { .. }
            )));
            assert!(valid_journey(&tt, &q, &journeys[0]));
            assert!(!stats.profile_budget_exhausted);
        }
    }

    #[test]
    fn ping_saves_footpath_round_times_before_destination_pruning() {
        let base = 20_000_i64 * 86_400;
        let departure = base + 9 * 3_600;
        let arrival = base + 17 * 3_600;
        for reverse in [false, true] {
            let bytes = train_with_endpoint_footpath(reverse).encode().unwrap();
            let tt = ZeroCopyTimetable::new(&bytes).unwrap();
            let mut q = query();
            q.max_boardings = 3;
            let (anchor, bound, expected) = if reverse {
                (arrival, departure, departure - 60)
            } else {
                (departure, arrival, arrival + 60)
            };
            let mut state = ProfileSearchState::new(
                &tt, &q, anchor, anchor, reverse, Duration::from_secs(5),
            )
            .unwrap();
            state.execute_point(&tt, &q, anchor, 3, bound).unwrap();
            // Stop 2 cannot improve the destination bound. Nigiri still
            // saves its round time before pruning the active frontier.
            assert!(state.rounds[1].get(Stage::Ready, 2, state.generation).is_none());
            let saved = state.fill_ping_bounds(2);
            assert_eq!(saved.rows[1][2], expected);
            assert_eq!(saved.rows[1][if reverse { 1 } else { 0 }], anchor);
        }
    }

    #[test]
    fn point_execution_can_reduce_the_shared_boarding_budget_in_both_directions() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let departure = base + 9 * 3_600;
        let arrival = base + 17 * 3_600;
        let mut q = query();
        q.max_boardings = 3;

        for reverse in [false, true] {
            let (anchor, destination_bound) = if reverse {
                (arrival, departure)
            } else {
                (departure, arrival)
            };
            let mut state = ProfileSearchState::new(
                &tt, &q, anchor, anchor, reverse, Duration::from_secs(5),
            )
            .unwrap();
            // PONG retains shared buffers while executing successively
            // smaller candidate transfer budgets. The last call has two
            // reached-stop rows inside a state with four allocated rows.
            for budget in [3, 2, 1] {
                let inactive_ready = state.profile_ready[budget + 1..].to_vec();
                let inactive_ride = state.profile_ride[budget + 1..].to_vec();
                let journeys = state
                    .execute_point(&tt, &q, anchor, budget, destination_bound)
                    .unwrap();
                assert!(journeys.iter().any(|j| {
                    j.boardings == 1
                        && j.departure_unix_seconds == departure
                        && j.arrival_unix_seconds == arrival
                }));
                assert_eq!(&state.profile_ready[budget + 1..], inactive_ready.as_slice());
                assert_eq!(&state.profile_ride[budget + 1..], inactive_ride.as_slice());
            }
        }
    }

    #[test]
    fn pong_direct_train_uses_a_smaller_budget_than_the_shared_state() {
        use crate::routing_common::pong::route_pong_with_stats_precomputed;
        use crate::routing_common::profile_search::ProfileOptions;

        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let departure = base + 9 * 3_600;
        let arrival = base + 17 * 3_600;
        let mut q = query();
        q.max_boardings = 3;
        for (arrive_by, reference) in [(false, departure), (true, arrival)] {
            q.departure_unix_seconds = reference;
            let (journeys, stats) = route_pong_with_stats_precomputed(
                &tt,
                &q,
                ProfileOptions {
                    search_window_seconds: 1,
                    min_itineraries: 1,
                    ..ProfileOptions::default()
                },
                arrive_by,
                None,
            )
            .unwrap();
            assert_eq!(journeys.len(), 1);
            assert_eq!(journeys[0].boardings, 1);
            assert_eq!(journeys[0].departure_unix_seconds, departure);
            assert_eq!(journeys[0].arrival_unix_seconds, arrival);
            assert!(stats.pong_forward_searches >= 1);
            assert!(stats.pong_backward_searches >= 1);
            assert!(!stats.profile_budget_exhausted);
        }
    }

    #[test]
    fn prior_pong_destination_limits_survive_point_setup_at_the_correct_boarding_count() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let mut q = query();
        q.max_boardings = 3;
        for reverse in [false, true] {
            let (anchor, limit, looser) = if reverse {
                (base + 17 * 3_600, base + 9 * 3_600, base + 8 * 3_600)
            } else {
                (base + 9 * 3_600, base + 17 * 3_600, base + 18 * 3_600)
            };
            let mut state =
                ProfileSearchState::new(&tt, &q, anchor, anchor, reverse, Duration::from_secs(5))
                    .unwrap();
            let invalid = if reverse { NEG_INF } else { INF };
            state.reset_point_arrivals();
            state.limit_time_at_dest(2, limit);
            state.limit_time_at_dest(2, looser);
            assert_eq!(state.time_at_dest, vec![invalid, invalid, limit, limit]);
            // The equal-time direct train must remain reconstructable even
            // when a known two-boarding result supplies the same limit.
            let journeys = state.execute_point(&tt, &q, anchor, 3, looser).unwrap();
            assert!(journeys.iter().any(|j| j.boardings == 1));
            assert_eq!(state.time_at_dest[2], limit);
            assert_eq!(state.time_at_dest[3], limit);
            state.reset_point_arrivals();
            assert_eq!(state.time_at_dest, vec![invalid; 4]);
        }
    }

    #[test]
    fn point_terminal_reconstruction_is_reused_in_both_directions() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let q = query();
        for reverse in [false, true] {
            let (anchor, bound) = if reverse {
                (base + 17 * 3_600, base + 8 * 3_600)
            } else {
                (base + 9 * 3_600, base + 18 * 3_600)
            };
            let mut state =
                ProfileSearchState::new(&tt, &q, anchor, anchor, reverse, Duration::from_secs(5))
                    .unwrap();
            let journeys = state.execute_point(&tt, &q, anchor, 1, bound).unwrap();
            assert_eq!(journeys.len(), 1);
            assert!(valid_journey(&tt, &q, &journeys[0]));
            assert_eq!(state.stats.profile_journeys_reconstructed, 1);
            assert_eq!(state.stats.profile_terminal_reconstruction_reused, 1);
            assert_eq!(state.stats.profile_journeys_rejected, 0);
        }
    }

    #[test]
    fn lower_bound_horizon_is_global_and_query_duration_is_checked_by_search() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let graph = LowerBoundTopology::build(&tt);
        let mut q = query();
        q.max_duration_seconds = 5 * 60;
        let bounds = lower_bounds(
            &tt,
            &q,
            false,
            &graph,
            &mut RaptorStats::default(),
        );
        // Nigiri's Dijkstra retains this eight-hour lower bound even for
        // a five-minute query. The RAPTOR horizon rejects it subsequently.
        assert_eq!(bounds[0], 8 * 3_600);
        assert!(!can_reach(&bounds, 0, 0, 0, q.max_duration_seconds, false, INF));
        assert_eq!(bounds[1], 0);
    }

    #[test]
    fn cached_lower_bound_topology_matches_uncached_in_both_directions() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let topology = LowerBoundTopology::build(&tt);
        let q = query();
        let base = 20_000_i64 * 86_400;
        for (reverse, anchor) in [
            (false, base + 86_400 + 9 * 3600),
            (true, base + 86_400 + 17 * 3600),
        ] {
            let mut uncached =
                ProfileSearchState::new(&tt, &q, anchor, anchor, reverse, Duration::from_secs(5))
                    .unwrap();
            let mut cached = ProfileSearchState::new_with_topology(
                &tt,
                &q,
                anchor,
                anchor,
                reverse,
                Duration::from_secs(5),
                Some(&topology),
            )
            .unwrap();
            assert_eq!(uncached.bounds, cached.bounds);
            uncached.extend(&tt, &q, &[anchor]).unwrap();
            cached.extend(&tt, &q, &[anchor]).unwrap();
            let signature = |s: &ProfileSearchState| {
                s.result()
                    .iter()
                    .map(|j| {
                        (
                            j.departure_unix_seconds,
                            j.arrival_unix_seconds,
                            j.boardings,
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(signature(&uncached), signature(&cached));
            assert!(!cached.result().is_empty());
        }
    }

    #[test]
    fn preprocessing_is_not_truncated_by_the_sweep_deadline() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let graph = LowerBoundTopology::build(&tt);
        let bounds = lower_bounds(&tt, &query(), false, &graph, &mut RaptorStats::default());
        assert_eq!(bounds, vec![8 * 3_600, 0]);
    }

    #[test]
    fn cpu_point_search_finishes_after_the_sweep_deadline() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = query();
        let base = 20_000_i64 * 86_400;
        for reverse in [false, true] {
            let (anchor, bound) = if reverse {
                (base + 17 * 3_600, base + 9 * 3_600)
            } else {
                (base + 9 * 3_600, base + 17 * 3_600)
            };
            let mut state =
                ProfileSearchState::new(&tt, &q, anchor, anchor, reverse, Duration::from_secs(5))
                    .unwrap();
            state.deadline = Instant::now();
            let journeys = state.execute_point(&tt, &q, anchor, 1, bound).unwrap();
            assert_eq!(journeys.len(), 1);
            assert!(valid_journey(&tt, &q, &journeys[0]));
            assert!(!state.stats().profile_budget_exhausted);
        }
    }

    #[test]
    fn four_day_transit_horizon_is_preserved_without_direct_journey() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let mut q = query();
        let requested_departure = base + 18 * 3_600;
        q.departure_unix_seconds = requested_departure;
        q.max_duration_seconds = 4 * 86_400;
        let next_day_departure = base + 86_400 + 9 * 3_600;
        let mut state = ProfileSearchState::new(
            &tt,
            &q,
            requested_departure,
            base + 3 * 86_400,
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        state.extend(&tt, &q, &[next_day_departure]).unwrap();
        assert!(
            state.result().iter().any(|j| j.boardings == 1
                && j.departure_unix_seconds == next_day_departure
                && j.arrival_unix_seconds == next_day_departure + 8 * 3_600),
            "long-distance route must survive without a 12-hour seed"
        );
        assert_eq!(
            state.time_at_dest[1],
            next_day_departure + 8 * 3_600,
            "validated transit arrival should tighten the destination bound"
        );
    }

    #[test]
    fn latest_first_forward_anchors_find_both_daily_trains() {
        let bytes = daily_train().encode().unwrap();
        let timetable = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let departures = [base + 86_400 + 9 * 3_600, base + 2 * 86_400 + 9 * 3_600];
        let (journeys, stats) = route_profile_incremental(
            &timetable,
            &query(),
            &departures,
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            !stats.profile_budget_exhausted,
            "timed out on tiny timetable: {stats:?}"
        );
        for departure in departures {
            assert!(
                journeys
                    .iter()
                    .any(|j| j.departure_unix_seconds == departure
                        && j.arrival_unix_seconds == departure + 8 * 3_600
                        && j.boardings == 1),
                "missing train at {departure}: {journeys:?}"
            );
        }
        assert_eq!(stats.profile_anchor_events, 2);
    }

    #[test]
    fn ascending_reverse_anchors_find_both_daily_trains() {
        let bytes = daily_train().encode().unwrap();
        let timetable = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let arrivals = [base + 86_400 + 17 * 3_600, base + 2 * 86_400 + 17 * 3_600];
        let (journeys, stats) = route_profile_incremental(
            &timetable,
            &query(),
            &arrivals,
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            !stats.profile_budget_exhausted,
            "timed out on tiny timetable: {stats:?}"
        );
        for arrival in arrivals {
            assert!(
                journeys.iter().any(|j| j.arrival_unix_seconds == arrival
                    && j.departure_unix_seconds == arrival - 8 * 3_600
                    && j.boardings == 1),
                "missing arrival at {arrival}: {journeys:?}"
            );
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
        let mut state =
            ProfileSearchState::new(&timetable, &q, early, late, false, Duration::from_secs(5))
                .unwrap();
        state.extend(&timetable, &q, &[early]).unwrap();
        assert_eq!(state.last_anchor, Some(early));
        state.extend(&timetable, &q, &[late]).unwrap();
        assert_eq!(state.last_anchor, Some(late));
        assert_eq!(state.stats().profile_anchor_events, 2);
        assert!(
            state
                .result()
                .iter()
                .any(|j| j.departure_unix_seconds == late)
        );
    }
}

#[cfg(test)]
mod transfer_round_regressions {
    use super::*;
    use crate::routing_common::raptor_query::{Leg, WalkingEndpoint};
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, PackedStop, Range, SourceIdx, StationTransferRule, TimetableOwned,
        TimingProfileIdx, TrafficDays,
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
        tt.location_routes = vec![RouteIdx(0), RouteIdx(0), RouteIdx(1), RouteIdx(1)];
        tt.footpaths_out_offsets = vec![0; 4];
        tt.footpaths_in_offsets = vec![0; 4];
        for (from, to) in [(0, 1), (1, 2)] {
            tt.route_stops.extend([
                PackedStop::new(LocationIdx(from), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(to), true, true, true, true).unwrap(),
            ]);
        }
        tt.route_stop_offsets.extend([2, 4]);
        tt.route_transport_ranges
            .extend([Range { from: 0, to: 1 }, Range { from: 1, to: 2 }]);
        tt.timing_profile_offsets.extend([2, 4]);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(10 * 60).unwrap(),
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(15 * 60).unwrap(),
        ]);
        tt.route_clasz.extend([2, 2]);
        tt.transport_route.extend([RouteIdx(0), RouteIdx(1)]);
        tt.transport_timing_profile
            .extend([TimingProfileIdx(0), TimingProfileIdx(1)]);
        tt.transport_first_dep_offset.extend([
            Delta::from_seconds(8 * 3600).unwrap(),
            Delta::from_seconds(8 * 3600 + 15 * 60).unwrap(),
        ]);
        tt.transport_initial_day_offset.extend([0, 0]);
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        days.set(1, true).unwrap();
        tt.bitfields.push(days);
        tt.transport_traffic_days
            .extend([BitfieldIdx(1), BitfieldIdx(1)]);
        tt.route_traffic_days
            .extend([BitfieldIdx(1), BitfieldIdx(1)]);
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
            access: vec![WalkingEndpoint {
                stop: LocationIdx(0),
                seconds: 0,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 2,
            max_duration_seconds: 2 * 3600,
        }
    }

    /// Competing incoming services reach different platforms. The earlier
    /// physical footpath is prohibited as a GTFS connection; the later one
    /// must remain eligible for the onward train in both search directions.
    fn competing_transfer_timetable(prohibited: bool) -> TimetableOwned {
        use crate::routing_common::timetable::PackedFootpath;
        let mut tt = TimetableOwned::empty(20_000, 2);
        tt.add_source("test", "competing-transfers", "attempt")
            .unwrap();
        for _ in 0..5 {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
        }
        // 0 -> 1, 0 -> 2, 3 -> 4. Both 1 and 2 can walk to 3.
        tt.location_routes_offsets = vec![0, 2, 3, 4, 5, 6];
        tt.location_routes = vec![
            RouteIdx(0),
            RouteIdx(1),
            RouteIdx(0),
            RouteIdx(1),
            RouteIdx(2),
            RouteIdx(2),
        ];
        tt.footpaths_out_offsets = vec![0, 0, 1, 2, 2, 2];
        tt.footpaths_out.extend([
            PackedFootpath::new(LocationIdx(3), 60).unwrap(),
            PackedFootpath::new(LocationIdx(3), 60).unwrap(),
        ]);
        tt.footpaths_in_offsets = vec![0, 0, 0, 0, 2, 2];
        tt.footpaths_in.extend([
            PackedFootpath::new(LocationIdx(1), 60).unwrap(),
            PackedFootpath::new(LocationIdx(2), 60).unwrap(),
        ]);
        for (from, to) in [(0, 1), (0, 2), (3, 4)] {
            tt.route_stops.extend([
                PackedStop::new(LocationIdx(from), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(to), true, true, true, true).unwrap(),
            ]);
        }
        tt.route_stop_offsets.extend([2, 4, 6]);
        tt.route_transport_ranges.extend([
            Range { from: 0, to: 1 },
            Range { from: 1, to: 2 },
            Range { from: 2, to: 3 },
        ]);
        tt.timing_profile_offsets.extend([2, 4, 6]);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(10 * 60).unwrap(),
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(12 * 60).unwrap(),
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(15 * 60).unwrap(),
        ]);
        tt.route_clasz.extend([2, 2, 2]);
        tt.transport_route
            .extend([RouteIdx(0), RouteIdx(1), RouteIdx(2)]);
        tt.transport_timing_profile.extend([
            TimingProfileIdx(0),
            TimingProfileIdx(1),
            TimingProfileIdx(2),
        ]);
        tt.transport_first_dep_offset.extend([
            Delta::from_seconds(8 * 3600).unwrap(),
            Delta::from_seconds(8 * 3600).unwrap(),
            Delta::from_seconds(8 * 3600 + 20 * 60).unwrap(),
        ]);
        tt.transport_initial_day_offset.extend([0, 0, 0]);
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        days.set(1, true).unwrap();
        tt.bitfields.push(days);
        tt.transport_traffic_days.extend([BitfieldIdx(1); 3]);
        tt.route_traffic_days.extend([BitfieldIdx(1); 3]);
        tt.route_section_clasz_offsets.extend([0; 3]);
        tt.section_flag_offsets.extend([0; 3]);
        tt.transport_to_trip_section_offsets.extend([0; 3]);
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(1),
            to: LocationIdx(3),
            min_seconds: if prohibited { 0 } else { 20 * 60 },
            flags: if prohibited { 1 } else { 0 },
        });
        tt
    }

    #[test]
    fn provenance_restricted_transfer_survives_across_profile_anchors() {
        let bytes = competing_transfer_timetable(true).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let start = base + 8 * 3600;
        let next_day = start + 86_400;
        let mut q = request();
        q.departure_unix_seconds = start;
        q.egress = vec![WalkingEndpoint {
            stop: LocationIdx(4),
            seconds: 0,
        }];
        for reverse in [false, true] {
            let offset = if reverse { 35 * 60 } else { 0 };
            let anchors = [start + offset, next_day + offset];
            let (journeys, stats) =
                route_profile_incremental(&tt, &q, &anchors, reverse, Duration::from_secs(5))
                    .unwrap();
            assert_eq!(stats.profile_anchor_events, 2);
            for day in [0, 1] {
                let target_arrival = start + day * 86_400 + 35 * 60;
                assert!(
                    journeys
                        .iter()
                        .any(|j| j.arrival_unix_seconds == target_arrival
                            && j.boardings == 2
                            && j.legs.iter().any(|leg| matches!(
                                leg,
                                Leg::Transfer {
                                    from_stop: 2,
                                    to_stop: 3,
                                    ..
                                }
                            ))),
                    "legal connection on day {day} lost under persistent dominance: {journeys:?}"
                );
            }
        }
    }

    #[test]
    fn prohibited_early_walk_keeps_valid_later_transfer_forward_and_reverse() {
        let bytes = competing_transfer_timetable(true).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        for (reverse, event) in [
            (false, base + 8 * 3_600),
            (true, base + 8 * 3_600 + 35 * 60),
        ] {
            let mut q = request();
            q.departure_unix_seconds = event;
            q.egress = vec![WalkingEndpoint {
                stop: LocationIdx(4),
                seconds: 0,
            }];
            let (journeys, stats) =
                route_profile_incremental(&tt, &q, &[event], reverse, Duration::from_secs(5))
                    .unwrap();
            assert!(
                !journeys.is_empty(),
                "lost legal later-platform connection in reverse={reverse}: {stats:?}"
            );
            let j = journeys
                .iter()
                .find(|j| j.boardings == 2)
                .expect("two-boardings path must be preserved");
            assert!(
                j.legs.iter().any(|leg| matches!(
                    leg,
                    Leg::Transfer {
                        from_stop: 2,
                        to_stop: 3,
                        seconds: 60
                    }
                )),
                "the legal platform 2 -> 3 transfer must win: {:?}",
                j.legs
            );
            assert!(!j.legs.iter().any(|leg| matches!(
                leg,
                Leg::Transfer {
                    from_stop: 1,
                    to_stop: 3,
                    ..
                }
            )));
        }
    }

    #[test]
    fn transfer_ready_uses_later_physical_arrival_with_shorter_change() {
        let bytes = competing_transfer_timetable(false).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000_i64 * 86_400;
        let anchor = base + 8 * 3600;
        let mut q = request();
        q.departure_unix_seconds = anchor;
        q.egress = vec![WalkingEndpoint {
            stop: LocationIdx(4),
            seconds: 0,
        }];
        let (journeys, stats) =
            route_profile_incremental(&tt, &q, &[anchor], false, Duration::from_secs(5)).unwrap();
        assert!(
            !journeys.is_empty(),
            "a slower physical walk with a shorter GTFS change must survive: {stats:?}"
        );
        assert!(journeys.iter().any(|j| {
            j.boardings == 2
                && j.legs.iter().any(|leg| {
                    matches!(
                        leg,
                        Leg::Transfer {
                            from_stop: 2,
                            to_stop: 3,
                            seconds: 60
                        }
                    )
                })
        }));
    }

    #[test]
    fn equal_arrival_routes_remain_distinct_after_best_cell_ties() {
        let mut tt = two_leg_timetable(false);
        // Two independent services with the exact same 0 -> 1 schedule.
        // Neither should overwrite the other's immutable destination path.
        tt.route_stops[2] = PackedStop::new(LocationIdx(0), true, true, true, true).unwrap();
        tt.route_stops[3] = PackedStop::new(LocationIdx(1), true, true, true, true).unwrap();
        tt.location_routes_offsets = vec![0, 2, 4, 4];
        tt.location_routes = vec![RouteIdx(0), RouteIdx(1), RouteIdx(0), RouteIdx(1)];
        tt.timing_profile_events[3] = Delta::from_seconds(10 * 60).unwrap();
        tt.transport_first_dep_offset[1] = Delta::from_seconds(8 * 3600).unwrap();
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut q = request();
        q.egress = vec![WalkingEndpoint {
            stop: LocationIdx(1),
            seconds: 0,
        }];
        q.max_boardings = 1;
        let (journeys, stats) = route_profile_incremental(
            &view,
            &q,
            &[q.departure_unix_seconds],
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        let mut routes: Vec<u32> = journeys
            .iter()
            .flat_map(|j| {
                j.legs.iter().filter_map(|leg| match leg {
                    Leg::Ride { route, .. } => Some(*route),
                    _ => None,
                })
            })
            .collect();
        routes.sort_unstable();
        routes.dedup();
        assert_eq!(
            routes,
            vec![0, 1],
            "equal-time route alternatives were lost: {journeys:?}; {stats:?}"
        );
    }

    #[test]
    fn compact_profile_matches_point_raptor_on_transfer_fixture() {
        let bytes = two_leg_timetable(false).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = request();
        for (arrive_by, instant) in [
            (false, q.departure_unix_seconds),
            (true, q.departure_unix_seconds + 30 * 60),
        ] {
            let mut query = q.clone();
            query.departure_unix_seconds = instant;
            let (profile, stats) = route_profile_incremental(
                &tt,
                &query,
                &[instant],
                arrive_by,
                Duration::from_secs(5),
            )
            .unwrap();
            let point = if arrive_by {
                crate::routing_common::raptor_arrive_by::route_arrive_by_with_stats(&tt, &query)
                    .unwrap()
                    .0
            } else {
                crate::routing_common::raptor_query::route_with_stats(&tt, &query)
                    .unwrap()
                    .0
            };
            let transit = |j: &Vec<Journey>| {
                j.iter()
                    .filter(|x| x.boardings == 2)
                    .map(|x| (x.departure_unix_seconds, x.arrival_unix_seconds))
                    .collect::<Vec<_>>()
            };
            let expected = transit(&point);
            assert!(!expected.is_empty(), "point RAPTOR fixture is invalid");
            let actual = transit(&profile);
            assert!(
                expected.iter().all(|x| actual.contains(x)),
                "compact profile missed point solver journey: reverse={arrive_by} expected={expected:?} actual={actual:?} stats={stats:?}"
            );
        }
    }

    #[test]
    fn two_ride_forward_reconstructs_after_compact_scan() {
        let bytes = two_leg_timetable(false).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = request();
        let (journeys, stats) = route_profile_incremental(
            &tt,
            &q,
            &[q.departure_unix_seconds],
            false,
            Duration::from_secs(5),
        )
        .unwrap();
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
            &tt,
            &q,
            &[q.departure_unix_seconds],
            true,
            Duration::from_secs(5),
        )
        .unwrap();
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
            &tt,
            &q,
            &[q.departure_unix_seconds],
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            forward.is_empty(),
            "blocked change produced a route: {forward:?}"
        );
        let mut reverse_q = q;
        reverse_q.departure_unix_seconds += 30 * 60;
        let (backward, _) = route_profile_incremental(
            &tt,
            &reverse_q,
            &[reverse_q.departure_unix_seconds],
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            backward.is_empty(),
            "reverse blocked change produced route: {backward:?}"
        );
    }
}

#[cfg(test)]
mod immutable_path_tests {
    use super::*;
    use crate::routing_common::profile_compact_reconstruct::reconstruct;

    #[test]
    fn overwriting_best_arrival_never_mutates_an_existing_path() {
        let mut rounds = vec![Round::new(3), Round::new(3)];
        let mut links = Vec::new();
        let mut stats = RaptorStats::default();
        rounds[0].put(
            Stage::Ready,
            0,
            0,
            Parent::Root { seconds: 0 },
            1,
            false,
            &mut links,
            &mut stats,
        );
        let root = rounds[0].get(Stage::Ready, 0, 1).unwrap().parent;
        rounds[1].put_linked(
            Stage::Ride,
            1,
            60,
            Parent::Ride {
                from: 0,
                to: 1,
                route: 11,
                transport: 0,
                day: 0,
                board_pos: 0,
                alight_pos: 1,
                departure: 10,
                arrival: 60,
            },
            Some(root),
            1,
            false,
            &mut links,
            &mut stats,
        );
        let first_ride = rounds[1].get(Stage::Ride, 1, 1).unwrap().parent;
        rounds[1].put_linked(
            Stage::Walk,
            2,
            70,
            Parent::Walk {
                from: 1,
                to: 2,
                seconds: 10,
            },
            Some(first_ride),
            1,
            false,
            &mut links,
            &mut stats,
        );
        rounds[1].put_linked(
            Stage::Ride,
            1,
            50,
            Parent::Ride {
                from: 0,
                to: 1,
                route: 22,
                transport: 1,
                day: 0,
                board_pos: 0,
                alight_pos: 1,
                departure: 10,
                arrival: 50,
            },
            Some(root),
            1,
            false,
            &mut links,
            &mut stats,
        );
        let old_walk = reconstruct(&rounds, &links, 1, false, 1, 2, Stage::Walk, 0)
            .expect("earlier walk must retain the route 11 parent");
        assert!(old_walk.legs.iter().any(|leg| matches!(
            leg,
            crate::routing_common::raptor_query::Leg::Ride { route: 11, .. }
        )));
    }
}

#[cfg(test)]
mod lower_bound_graph_tests {
    use super::*;
    use crate::routing_common::raptor_query::WalkingEndpoint;
    use crate::routing_common::timetable::{PackedFootpath, SourceIdx, TimetableOwned};

    fn walking_graph(parents: &[LocationIdx], edges: &[(usize, usize, u32)]) -> TimetableOwned {
        // Footpaths must be stored by ascending duration (checked by validate()).
        let mut edges = edges.to_vec();
        edges.sort_by_key(|&(_, _, seconds)| seconds);
        let edges = &edges[..];
        let mut tt = TimetableOwned::empty(20_000, 1);
        tt.add_source("test", "lb-graph", "attempt").unwrap();
        for (at, &parent) in parents.iter().enumerate() {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(parent);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
            tt.location_routes_offsets.push(0);
            for &(from, to, seconds) in edges {
                if from == at {
                    tt.footpaths_out
                        .push(PackedFootpath::new(LocationIdx(to as u32), seconds).unwrap());
                }
                if to == at {
                    tt.footpaths_in
                        .push(PackedFootpath::new(LocationIdx(from as u32), seconds).unwrap());
                }
            }
            tt.footpaths_out_offsets.push(tt.footpaths_out.len() as u32);
            tt.footpaths_in_offsets.push(tt.footpaths_in.len() as u32);
        }
        tt
    }

    fn request(from: usize, to: usize) -> Query {
        Query {
            departure_unix_seconds: 20_000_i64 * 86_400,
            access: vec![WalkingEndpoint {
                stop: LocationIdx(from as u32),
                seconds: 7,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(to as u32),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 3,
            max_duration_seconds: 86_400,
        }
    }

    #[test]
    fn station_graph_merges_nested_platforms_and_minimizes_parallel_edges() {
        let owned = walking_graph(
            &[
                LocationIdx::INVALID,
                LocationIdx(0),
                LocationIdx(1),
                LocationIdx::INVALID,
                LocationIdx(3),
                LocationIdx::INVALID,
            ],
            &[(1, 4, 120), (2, 3, 90), (0, 5, 500), (4, 5, 30), (1, 2, 10)],
        );
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let graph = LowerBoundTopology::build(&tt);
        // Upstream keeps root indices; non-root rows remain empty.
        assert_eq!(graph.forward.offsets.len() - 1, 6);
        assert!(graph.forward.successors(1).is_empty());
        assert!(graph.forward.successors(2).is_empty());
        assert_eq!(graph.forward.edges.len(), 3);
        assert_eq!(graph.reverse.edges.len(), 3);
        for reverse in [false, true] {
            let mut stats = RaptorStats::default();
            let bounds = lower_bounds(
                &tt,
                &request(2, 5),
                reverse,
                &graph,
                &mut stats,
            );
            assert_eq!(
                bounds,
                if reverse {
                    vec![7, 7, 7, 97, 97, 127]
                } else {
                    vec![120, 120, 120, 30, 30, 0]
                }
            );
            assert_eq!(stats.profile_lower_bound_edges, 3);
        }
    }

    #[test]
    fn integer_bucket_search_matches_heap_dijkstra_on_directed_graphs() {
        // Deterministic graphs exercise stale labels, unequal parallel costs,
        // disconnected vertices, and exact sub-minute durations.
        let n = 48;
        let mut seed = 17_u32;
        let mut edges = Vec::new();
        for _ in 0..400 {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            let from = seed as usize % n;
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            let to = seed as usize % n;
            let cost = (seed >> 16) % 1000 + 1;
            edges.push((from, to, cost));
        }
        let owned = walking_graph(&vec![LocationIdx::INVALID; n], &edges);
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let graph = LowerBoundTopology::build(&tt);
        for reverse in [false, true] {
            let q = request(0, n - 1);
            let start = if reverse { 0 } else { n - 1 };
            let offset = if reverse { 7 } else { 0 };
            let mut expected = vec![u32::MAX; n];
            expected[start] = offset;
            let mut heap = BinaryHeap::from([Reverse((offset, start))]);
            while let Some(Reverse((weight, stop))) = heap.pop() {
                if weight != expected[stop] {
                    continue;
                }
                for &(from, to, cost) in &edges {
                    let (from, to) = if reverse { (from, to) } else { (to, from) };
                    let candidate = weight + cost;
                    if from == stop && candidate < expected[to] {
                        expected[to] = candidate;
                        heap.push(Reverse((candidate, to)));
                    }
                }
            }
            let actual = lower_bounds(
                &tt,
                &q,
                reverse,
                &graph,
                &mut RaptorStats::default(),
            );
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn cyclic_station_metadata_cannot_hang_preprocessing() {
        let groups = station_groups(&[LocationIdx(1), LocationIdx(0), LocationIdx(1)]);
        assert_eq!(groups[0], groups[1]);
        assert_eq!(groups[1], groups[2]);
    }
}
