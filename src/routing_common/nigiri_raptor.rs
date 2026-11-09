//! Static, no-via CPU RAPTOR translated from Nigiri 82e55ced's
//! include/nigiri/routing/raptor/{raptor.h,raptor_state.h}.
//! MIT license: third_party/nigiri-LICENSE. The wire adapter uses seconds.
//! Search labels are best_, tmp_, and round_times_; no predecessor labels
//! or journey reconstruction participate in route/transfer relaxation.
use crate::routing_common::profile_compact::{LowerBoundTopology, point_lower_bounds};
use crate::routing_common::raptor_query::{Query, RaptorStats, WalkingEndpoint};
use crate::routing_common::timetable::{Delta, LocationIdx, RouteIdx, ZeroCopyTimetable};
use crate::routing_common::transport_index::TransportIndex;
use anyhow::{Context, Result, ensure};
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;

// Nigiri's delta_t is relative minutes in i16. Gentian stores seconds;
// i32 relative seconds preserves that precision and the 512-day calendar.
pub(crate) type DeltaTime = i32;
pub(crate) const INF: DeltaTime = i32::MAX;
pub(crate) const NEG_INF: DeltaTime = i32::MIN;

pub(crate) fn better<T: Ord>(a: T, b: T, reverse: bool) -> bool {
    if reverse { a > b } else { a < b }
}
pub(crate) fn shift(t: i64, d: u32, reverse: bool) -> i64 {
    if reverse {
        t - i64::from(d)
    } else {
        t + i64::from(d)
    }
}
fn shift_delta(t: DeltaTime, d: u32, reverse: bool) -> DeltaTime {
    shift(i64::from(t), d, reverse).clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as DeltaTime
}
fn best(a: DeltaTime, b: DeltaTime, reverse: bool) -> DeltaTime {
    if better(a, b, reverse) { a } else { b }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PointJourney {
    pub start_time: i64,
    pub dest_time: i64,
    pub boardings: usize,
}

struct Marks {
    blocks: Vec<u64>,
}
impl Marks {
    fn new(n: usize) -> Self {
        Self {
            blocks: vec![0; n.div_ceil(64)],
        }
    }
    fn set(&mut self, i: usize) {
        self.blocks[i / 64] |= 1 << (i % 64);
    }
    fn test(&self, i: usize) -> bool {
        self.blocks[i / 64] & (1 << (i % 64)) != 0
    }
    fn clear(&mut self) {
        self.blocks.fill(0);
    }
    fn indices(&self) -> Vec<usize> {
        let mut indices = Vec::new();
        for (block, &value) in self.blocks.iter().enumerate() {
            let mut bits = value;
            while bits != 0 {
                indices.push(block * 64 + bits.trailing_zeros() as usize);
                bits &= bits - 1;
            }
        }
        indices
    }
}

pub(crate) struct PingBounds {
    rows: Vec<Vec<DeltaTime>>,
}

pub(crate) struct NigiriRaptor {
    // Names and lifetimes follow Nigiri: best/tmp are reset for each start;
    // round_times persist between the pongs of a sweep.
    pub(crate) best_: Vec<DeltaTime>,
    pub(crate) tmp_: Vec<DeltaTime>,
    pub(crate) round_times_: Vec<Vec<DeltaTime>>,
    time_at_dest_: Vec<DeltaTime>,
    station_mark_: Marks,
    prev_station_mark_: Marks,
    route_mark_: Marks,
    pub(crate) reverse: bool,
    pub(crate) target: usize,
    base_: i64,
    lb_: Vec<u32>,
    dist_to_end_: Vec<u32>,
    dist_to_start_: Vec<u32>,
    transports: Arc<TransportIndex>,
    transfer_sensitive: Arc<Vec<bool>>,
    bounds_: Option<PingBounds>,
    bounds_last_k_: usize,
    pub(crate) importance: Arc<crate::routing_common::transfer_optimizer::StationImportance>,
    pub(crate) stats_: RaptorStats,
    route_workers: usize,
    route_buffers: Vec<RouteBuffer>,
    /// Footpath early pruning (see `update_footpaths`); only disabled by tests/benchmarks.
    early_prune: bool,
}

impl NigiriRaptor {
    pub(crate) fn new(
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        reverse: bool,
        topology: Option<&LowerBoundTopology>,
    ) -> Result<Self> {
        ensure!(
            q.max_boardings <= 15,
            "max boardings exceeds Nigiri's limit"
        );
        let route_workers = match std::env::var("EDELWEISS_RAPTOR_WORKERS") {
            Ok(value) => value
                .parse::<usize>()
                .context("EDELWEISS_RAPTOR_WORKERS must be a positive integer")?,
            Err(std::env::VarError::NotPresent) => 4,
            Err(error) => return Err(error.into()),
        };
        ensure!(
            route_workers > 0,
            "EDELWEISS_RAPTOR_WORKERS must be positive"
        );
        let target = tt.coordinates().len();
        ensure!(
            q.access
                .iter()
                .chain(&q.egress)
                .all(|ep| ep.stop.idx() < target),
            "OSR endpoint index out of bounds"
        );
        let mut stats = RaptorStats::default();
        stats.profile_searches = 1;
        let start = Instant::now();
        let owned = topology.is_none().then(|| LowerBoundTopology::build(tt));
        let topology = topology.or(owned.as_ref()).unwrap();
        let lb = point_lower_bounds(tt, q, reverse, topology, &mut stats);
        stats.profile_lower_bounds_ms = start.elapsed().as_secs_f64() * 1000.0;
        let n = target + 1; // Nigiri's special intermodal destination.
        let offsets = |endpoints: &[WalkingEndpoint]| {
            let mut values = vec![u32::MAX; n];
            for ep in endpoints {
                values[ep.stop.idx()] = values[ep.stop.idx()].min(ep.seconds);
            }
            values
        };
        let (starts, ends) = if reverse {
            (&q.egress, &q.access)
        } else {
            (&q.access, &q.egress)
        };
        let invalid = if reverse { NEG_INF } else { INF };
        Ok(Self {
            best_: vec![invalid; n],
            tmp_: vec![invalid; n],
            round_times_: vec![vec![invalid; n]; q.max_boardings + 1],
            time_at_dest_: vec![invalid; q.max_boardings + 1],
            station_mark_: Marks::new(n),
            prev_station_mark_: Marks::new(n),
            route_mark_: Marks::new(tt.route_transport_ranges().len()),
            reverse,
            target,
            base_: i64::from(tt.header().first_utc_day) * 86_400,
            lb_: lb,
            dist_to_end_: offsets(ends),
            dist_to_start_: offsets(starts),
            transports: topology.transport_index(),
            transfer_sensitive: topology.transfer_sensitive(),
            importance: topology.station_importance(),
            bounds_: None,
            bounds_last_k_: 0,
            stats_: stats,
            route_workers,
            early_prune: true,
            route_buffers: Vec::new(),
        })
    }
    pub(crate) fn invalid(&self) -> DeltaTime {
        if self.reverse { NEG_INF } else { INF }
    }
    pub(crate) fn stats(&self) -> &RaptorStats {
        &self.stats_
    }
    pub(crate) fn transport_index(&self) -> Arc<TransportIndex> {
        Arc::clone(&self.transports)
    }
    pub(crate) fn reset_point_arrivals(&mut self) {
        let invalid = self.invalid();
        self.time_at_dest_.fill(invalid);
        for row in &mut self.round_times_ {
            row.fill(invalid);
        }
        self.bounds_ = None;
        self.bounds_last_k_ = 0;
    }
    pub(crate) fn flip_point_direction(&mut self, other_lb: &mut Vec<u32>) {
        self.reverse = !self.reverse;
        std::mem::swap(&mut self.lb_, other_lb);
        std::mem::swap(&mut self.dist_to_end_, &mut self.dist_to_start_);
        self.reset_point_arrivals();
    }
    pub(crate) fn fill_ping_bounds(&self, n_rows: usize) -> PingBounds {
        let mut rows: Vec<Vec<DeltaTime>> = Vec::with_capacity(n_rows);
        for k in 0..n_rows {
            let mut row = self.round_times_[k].clone();
            if k != 0 {
                for (at, value) in row.iter_mut().enumerate() {
                    *value = best(*value, rows[k - 1][at], self.reverse);
                }
            }
            // The wire adapter retains pair-specific GTFS transfers. Their
            // extra durations are not encoded by Nigiri's per-station bound
            // compensation; keep bounds permissive at these stops only.
            for (at, &sensitive) in self.transfer_sensitive.iter().enumerate() {
                if sensitive {
                    row[at] = if self.reverse { INF } else { NEG_INF };
                }
            }
            rows.push(row);
        }
        PingBounds { rows }
    }
    pub(crate) fn set_ping_bounds(&mut self, bounds: PingBounds) {
        self.bounds_ = Some(bounds);
    }
    pub(crate) fn to_delta(&self, time: i64) -> DeltaTime {
        (time - self.base_).clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as DeltaTime
    }
    pub(crate) fn to_unix(&self, time: DeltaTime) -> i64 {
        self.base_ + i64::from(time)
    }
    pub(crate) fn limit_time_at_dest(&mut self, k: usize, time: i64) {
        self.update_time_at_dest(k, self.to_delta(time));
    }
    fn update_time_at_dest(&mut self, k: usize, t: DeltaTime) {
        for value in self.time_at_dest_.iter_mut().skip(k) {
            *value = best(*value, t, self.reverse);
        }
    }
    fn next_start_time(&mut self) {
        let invalid = self.invalid();
        self.best_.fill(invalid);
        self.tmp_.fill(invalid);
        self.station_mark_.clear();
        self.prev_station_mark_.clear();
        self.route_mark_.clear();
    }
    fn add_start(&mut self, stop: usize, time: DeltaTime) {
        self.best_[stop] = best(time, self.best_[stop], self.reverse);
        self.round_times_[0][stop] = best(time, self.round_times_[0][stop], self.reverse);
        self.station_mark_.set(stop);
    }
    fn lb_reachable(&self, stop: usize) -> bool {
        self.lb_[stop] != u32::MAX
    }
    fn within_bounds(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        k: usize,
        stop: usize,
        time: DeltaTime,
    ) -> bool {
        let Some(bounds) = &self.bounds_ else {
            return true;
        };
        let bound = bounds.rows[self.bounds_last_k_ - k][stop];
        let compensated = shift_delta(bound, tt.location_transfer_seconds()[stop], self.reverse);
        !better(compensated, time, self.reverse)
    }
    fn reachable(&self, k: usize, stop: usize, time: DeltaTime) -> bool {
        self.lb_reachable(stop)
            && !better(
                self.time_at_dest_[k],
                shift_delta(time, self.lb_[stop], self.reverse),
                self.reverse,
            )
    }

    pub(crate) fn execute_point(
        &mut self,
        tt: &ZeroCopyTimetable<'_>,
        q: &Query,
        anchor: i64,
        max_boardings: usize,
        worst_time_at_dest: i64,
    ) -> Result<Vec<PointJourney>> {
        ensure!(
            max_boardings < self.round_times_.len(),
            "point boarding budget exceeds buffers"
        );
        self.bounds_last_k_ = if self.bounds_.is_some() {
            max_boardings
        } else {
            0
        };
        self.next_start_time();
        let starts = if self.reverse { &q.egress } else { &q.access };
        for ep in starts {
            self.add_start(
                ep.stop.idx(),
                shift_delta(self.to_delta(anchor), ep.seconds, self.reverse),
            );
        }
        self.stats_.profile_anchor_events += 1;
        self.stats_.profile_start_labels += starts.len() as u64;
        let worst = self.to_delta(worst_time_at_dest);
        for time in &mut self.time_at_dest_ {
            *time = best(*time, worst, self.reverse);
        }
        let start = Instant::now();
        for k in 1..=max_boardings {
            // raptor::execute: fold retained round times into global best.
            for (at, value) in self.best_.iter_mut().enumerate() {
                *value = best(self.round_times_[k][at], *value, self.reverse);
            }
            self.update_time_at_dest(k, self.best_[self.target]);
            self.route_mark_.clear();
            let marked = self.station_mark_.indices();
            let mut any_marked = false;
            for &stop in &marked {
                if stop == self.target {
                    continue;
                }
                for &route in tt.routes_at(LocationIdx(stop as u32)) {
                    any_marked = true;
                    self.route_mark_.set(route.idx());
                }
            }
            if !any_marked {
                break;
            }
            self.stats_.rounds += 1;
            self.stats_.marked_stops += marked.len() as u64;
            std::mem::swap(&mut self.prev_station_mark_, &mut self.station_mark_);
            self.station_mark_.clear();
            let scan = Instant::now();
            self.scan_routes(tt, k);
            self.stats_.route_scan_ms += scan.elapsed().as_secs_f64() * 1000.0;
            self.route_mark_.clear();
            std::mem::swap(&mut self.prev_station_mark_, &mut self.station_mark_);
            self.station_mark_.clear();
            let walking = Instant::now();
            self.update_transfers(tt, k);
            self.update_intermodal_footpaths(k);
            self.update_footpaths(tt, k);
            self.stats_.walking_ms += walking.elapsed().as_secs_f64() * 1000.0;
        }
        self.stats_.profile_compact_ms += start.elapsed().as_secs_f64() * 1000.0;
        // Upstream adds scalar destination/boarding candidates after execute.
        let mut result = Vec::<PointJourney>::new();
        for k in 1..=max_boardings {
            let t = self.round_times_[k][self.target];
            if t == self.invalid() {
                continue;
            }
            let destination = self.to_unix(t);
            if result
                .iter()
                .any(|old| !better(destination, old.dest_time, self.reverse))
            {
                continue;
            }
            result.push(PointJourney {
                start_time: anchor,
                dest_time: destination,
                boardings: k,
            });
        }
        Ok(result)
    }

    fn scan_routes(&mut self, tt: &ZeroCopyTimetable<'_>, k: usize) {
        let routes = self.route_mark_.indices();
        // Limit memory and scheduling overhead. Each worker keeps one i32 per
        // stop, reset/merged only at touched stops, and shares all input labels.
        let workers = self
            .route_workers
            .min(rayon::current_num_threads())
            .min(routes.len());
        let work: usize = routes
            .iter()
            .map(|&r| {
                let route = RouteIdx(r as u32);
                tt.stops_on(route)
                    .len()
                    .saturating_mul(self.transports.chains(route).len())
            })
            .sum();
        let parallel = workers > 1 && routes.len() >= workers * 8 && work >= 32_768;
        let scan = RouteScan {
            reverse: self.reverse,
            base: self.base_,
            previous: &self.round_times_[k - 1],
            marked: &self.prev_station_mark_,
            lower_bounds: &self.lb_,
            destination_bound: self.time_at_dest_[k],
            ping_bounds: self
                .bounds_
                .as_ref()
                .map(|b| &b.rows[self.bounds_last_k_ - k][..]),
            transports: &self.transports,
        };
        if !parallel {
            let tmp = &mut self.tmp_;
            let marks = &mut self.station_mark_;
            for route in routes {
                scan.update_route(tt, RouteIdx(route as u32), &mut self.stats_, |loc, time| {
                    tmp[loc] = best(time, tmp[loc], scan.reverse);
                    marks.set(loc);
                });
            }
            return;
        }
        self.stats_.parallel_route_scan_rounds += 1;
        self.stats_.route_scan_workers = self.stats_.route_scan_workers.max(workers as u64);
        while self.route_buffers.len() < workers {
            self.route_buffers.push(RouteBuffer::new(self.tmp_.len()));
        }
        // The immutable snapshot includes the previous round, station marks,
        // calendars and pruning bounds. No label/bound changes until the join.
        self.route_buffers[..workers]
            .par_iter_mut()
            .enumerate()
            .for_each(|(worker, buffer)| {
                buffer.reset(scan.reverse);
                let RouteBuffer {
                    arrivals,
                    touched,
                    stats,
                    ..
                } = buffer;
                for &route in routes.iter().skip(worker).step_by(workers) {
                    scan.update_route(tt, RouteIdx(route as u32), stats, |loc, time| {
                        if arrivals[loc] == if scan.reverse { NEG_INF } else { INF } {
                            touched.push(loc);
                        }
                        arrivals[loc] = best(time, arrivals[loc], scan.reverse);
                    });
                }
            });
        let merge = Instant::now();
        for buffer in &self.route_buffers[..workers] {
            for &loc in &buffer.touched {
                self.tmp_[loc] = best(buffer.arrivals[loc], self.tmp_[loc], self.reverse);
                self.station_mark_.set(loc);
            }
            buffer.add_stats(&mut self.stats_);
        }
        self.stats_.route_scan_merge_ms += merge.elapsed().as_secs_f64() * 1000.0;
    }
    fn transfer_duration(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        from: usize,
        to: usize,
        walk: u32,
    ) -> Option<u32> {
        if !self.transfer_sensitive[from] || !self.transfer_sensitive[to] {
            Some(
                walk.max(tt.location_transfer_seconds()[from])
                    .max(tt.location_transfer_seconds()[to]),
            )
        } else {
            transfer_duration(tt, from, to, walk)
        }
    }
    fn update_transfers(&mut self, tt: &ZeroCopyTimetable<'_>, k: usize) {
        for stop in self.prev_station_mark_.indices() {
            let tmp = self.tmp_[stop];
            if tmp == self.invalid() {
                continue;
            }
            let Some(change) = self.transfer_duration(tt, stop, stop, 0) else {
                continue;
            };
            let t = shift_delta(tmp, change, self.reverse);
            self.update_ready(tt, k, stop, t);
        }
    }
    fn update_ready(
        &mut self,
        tt: &ZeroCopyTimetable<'_>,
        k: usize,
        target: usize,
        time: DeltaTime,
    ) {
        let improves = better(time, self.best_[target], self.reverse);
        // Save ping round labels before destination/LB pruning, as upstream.
        if self.bounds_last_k_ == 0 && improves {
            self.round_times_[k][target] = best(time, self.round_times_[k][target], self.reverse);
        }
        if improves && self.reachable(k, target, time) && self.within_bounds(tt, k, target, time) {
            self.round_times_[k][target] = time;
            self.best_[target] = time;
            self.station_mark_.set(target);
            self.stats_.labels_improved += 1;
        }
    }
    fn update_intermodal_footpaths(&mut self, k: usize) {
        for stop in self.prev_station_mark_.indices() {
            let duration = self.dist_to_end_[stop];
            if duration == u32::MAX || self.tmp_[stop] == self.invalid() {
                continue;
            }
            let time = shift_delta(self.tmp_[stop], duration, self.reverse);
            if !better(self.time_at_dest_[k], time, self.reverse) {
                self.round_times_[k][self.target] = time;
                self.best_[self.target] = time;
                self.update_time_at_dest(k, time);
            }
        }
    }
    fn update_footpaths(&mut self, tt: &ZeroCopyTimetable<'_>, k: usize) {
        let prune = self.early_prune && self.bounds_last_k_ != 0;
        for from in self.prev_station_mark_.indices() {
            let tmp = self.tmp_[from];
            if tmp == self.invalid() {
                continue;
            }
            let paths = if self.reverse {
                tt.footpaths_to(LocationIdx(from as u32))
            } else {
                tt.footpaths_from(LocationIdx(from as u32))
            };
            for fp in paths {
                // Early pruning: footpaths are stored sorted by ascending walk
                // time (validated on load), and every effective transfer
                // duration is >= the raw walk time. Once even the raw walk
                // cannot beat the destination bound for this round, neither
                // this edge nor any later (longer) one can pass `reachable()`
                // in `update_ready`, so the rest of the list is skipped.
                // Not applied while ping round labels are being retained
                // (`bounds_last_k_ == 0`): `update_ready` stores those labels
                // before destination pruning, so skipping would change them.
                if prune
                    && better(
                        self.time_at_dest_[k],
                        shift_delta(tmp, fp.seconds(), self.reverse),
                        self.reverse,
                    )
                {
                    break;
                }
                self.stats_.footpath_edges_examined += 1;
                let to = fp.target().idx();
                let (a, b) = if self.reverse { (to, from) } else { (from, to) };
                let Some(duration) = self.transfer_duration(tt, a, b, fp.seconds()) else {
                    continue;
                };
                self.update_ready(tt, k, to, shift_delta(tmp, duration, self.reverse));
            }
        }
    }
}

/// Wire adapter: put pair-specific GTFS minima/prohibitions into the same
/// single transfer duration consumed by Nigiri's scalar round labels.
pub(crate) fn transfer_duration(
    tt: &ZeroCopyTimetable<'_>,
    from: usize,
    to: usize,
    walk: u32,
) -> Option<u32> {
    let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(to as u32));
    if rule.is_some_and(|r| r.prohibited()) {
        return None;
    }
    Some(
        walk.max(tt.location_transfer_seconds()[from])
            .max(tt.location_transfer_seconds()[to])
            .max(rule.map_or(0, |r| r.min_seconds)),
    )
}

pub(crate) fn sorted_offsets(offsets: &[WalkingEndpoint]) -> Vec<WalkingEndpoint> {
    let mut result = offsets.to_vec();
    result.sort_by_key(|ep| (ep.seconds, ep.stop.0));
    result
}

// Read-only input for one route-scan phase. Routes never consume another
// route's same-round arrivals; the min/max reduction is order independent.
struct RouteScan<'a> {
    reverse: bool,
    base: i64,
    previous: &'a [DeltaTime],
    marked: &'a Marks,
    lower_bounds: &'a [u32],
    destination_bound: DeltaTime,
    ping_bounds: Option<&'a [DeltaTime]>,
    transports: &'a TransportIndex,
}
impl RouteScan<'_> {
    fn update_route(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        route: RouteIdx,
        stats: &mut RaptorStats,
        mut emit: impl FnMut(usize, DeltaTime),
    ) {
        stats.routes_scanned += 1;
        let stops = tt.stops_on(route);
        // Gentian coalesces FIFO chains under a public route. Each chain is
        // an independent Nigiri loader route for transport selection.
        let transports = self.transports;
        for chain in transports.chains(route) {
            let mut onboard: Option<(DeltaTime, &[Delta])> = None;
            for step in 0..stops.len() {
                let pos = if self.reverse {
                    stops.len() - step - 1
                } else {
                    step
                };
                let stop = stops[pos];
                let loc = stop.location().idx();
                stats.stop_events_scanned += 1;
                let finish = if self.reverse {
                    stop.outbound() && pos + 1 < stops.len()
                } else {
                    stop.inbound() && pos > 0
                };
                if finish && let Some((first, events)) = &onboard {
                    let slot = if self.reverse { pos * 2 } else { pos * 2 - 1 };
                    let by_transport = first + events[slot].seconds() as DeltaTime;
                    if self.lower_bounds[loc] != u32::MAX
                        && !better(
                            self.destination_bound,
                            shift_delta(by_transport, self.lower_bounds[loc], self.reverse),
                            self.reverse,
                        )
                        && self.ping_bounds.is_none_or(|row| {
                            !better(
                                shift_delta(
                                    row[loc],
                                    tt.location_transfer_seconds()[loc],
                                    self.reverse,
                                ),
                                by_transport,
                                self.reverse,
                            )
                        })
                    {
                        emit(loc, by_transport);
                        stats.labels_improved += 1;
                    }
                }
                let start = if self.reverse {
                    stop.inbound() && pos > 0
                } else {
                    stop.outbound() && pos + 1 < stops.len()
                };
                if !start || !self.marked.test(loc) {
                    continue;
                }
                if self.lower_bounds[loc] == u32::MAX {
                    break;
                }
                let ready = self.previous[loc];
                let slot = if self.reverse { pos * 2 - 1 } else { pos * 2 };
                let onboard_time = onboard
                    .as_ref()
                    .map(|(first, events)| first + events[slot].seconds() as DeltaTime);
                if ready == if self.reverse { NEG_INF } else { INF }
                    || onboard_time.is_some_and(|t| better(t, ready, self.reverse))
                {
                    stats.transport_lookup_skipped_onboard += 1;
                    continue;
                }
                let Some(selected) = chain.select(
                    tt,
                    slot,
                    self.base + i64::from(ready),
                    self.reverse,
                    self.base + i64::from(self.destination_bound),
                    self.lower_bounds[loc],
                    stats,
                ) else {
                    continue;
                };
                if onboard_time.is_none_or(|old| {
                    !better(
                        old,
                        (selected.event - self.base).clamp(i64::from(i32::MIN), i64::from(i32::MAX))
                            as DeltaTime,
                        self.reverse,
                    )
                }) {
                    let first = selected.start_day as DeltaTime * 86_400
                        + tt.transport_first_dep_offset()[selected.transport.idx()].seconds()
                            as DeltaTime;
                    onboard = Some((first, tt.events_for_transport(selected.transport).unwrap()));
                }
            }
        }
    }
}

struct RouteBuffer {
    reverse: bool,
    arrivals: Vec<DeltaTime>,
    touched: Vec<usize>,
    stats: RaptorStats,
}
impl RouteBuffer {
    fn new(stops: usize) -> Self {
        Self {
            reverse: false,
            arrivals: vec![INF; stops],
            touched: Vec::new(),
            stats: RaptorStats::default(),
        }
    }
    fn reset(&mut self, reverse: bool) {
        // Fill on direction switches too: untouched slots must use the new
        // sentinel. Normal rounds reset only the sparse touched set.
        let invalid = if reverse { NEG_INF } else { INF };
        if self.reverse != reverse {
            self.arrivals.fill(invalid);
        } else {
            for &loc in &self.touched {
                self.arrivals[loc] = invalid;
            }
        }
        self.reverse = reverse;
        self.touched.clear();
        self.stats = RaptorStats::default();
    }
    fn add_stats(&self, to: &mut RaptorStats) {
        let from = &self.stats;
        to.routes_scanned += from.routes_scanned;
        to.stop_events_scanned += from.stop_events_scanned;
        to.labels_improved += from.labels_improved;
        to.transport_lookup_calls += from.transport_lookup_calls;
        to.transport_lookup_probes += from.transport_lookup_probes;
        to.transport_lookup_skipped_onboard += from.transport_lookup_skipped_onboard;
        to.transport_days_tested += from.transport_days_tested;
        to.active_transport_days += from.active_transport_days;
        to.transport_bound_prunes += from.transport_bound_prunes;
    }
}

#[cfg(test)]
mod parallel_tests {
    use super::*;
    use crate::routing_common::timetable::*;

    // Enough intersecting routes to exercise the production parallel threshold.
    // Different running times, service days and departures produce overtaking
    // alternatives, including services whose events cross midnight.
    fn network(routes: usize, stops: usize) -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 4);
        tt.add_source("test", "parallel", "attempt").unwrap();
        for stop in 0..stops {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(30 + (stop % 3) as u32);
            tt.location_routes
                .extend((0..routes).map(|r| RouteIdx(r as u32)));
            tt.location_routes_offsets
                .push(tt.location_routes.len() as u32);
            if stop + 1 < stops {
                tt.footpaths_out
                    .push(PackedFootpath::new(LocationIdx((stop + 1) as u32), 90).unwrap());
            }
            tt.footpaths_out_offsets.push(tt.footpaths_out.len() as u32);
            if stop > 0 {
                tt.footpaths_in
                    .push(PackedFootpath::new(LocationIdx((stop - 1) as u32), 90).unwrap());
            }
            tt.footpaths_in_offsets.push(tt.footpaths_in.len() as u32);
        }
        for day in 0..4 {
            let mut calendar = TrafficDays::default();
            calendar.set(day, true).unwrap();
            tt.bitfields.push(calendar);
        }
        for r in 0..routes {
            for stop in 0..stops {
                // Include pickup/dropoff restrictions at intermediate stops.
                tt.route_stops.push(
                    PackedStop::new(
                        LocationIdx(stop as u32),
                        stop % 11 != 5,
                        stop % 13 != 7,
                        true,
                        true,
                    )
                    .unwrap(),
                );
            }
            tt.route_stop_offsets.push(tt.route_stops.len() as u32);
            tt.route_transport_ranges.push(Range {
                from: (r * 4) as u32,
                to: ((r + 1) * 4) as u32,
            });
            tt.route_clasz.push(2);
            tt.route_section_clasz_offsets.push(0);
            tt.section_flag_offsets.push(0);
            let mut union = TrafficDays::default();
            for day in 0..4 {
                union.set(day, true).unwrap();
            }
            tt.bitfields.push(union);
            tt.route_traffic_days
                .push(BitfieldIdx((tt.bitfields.len() - 1) as u32));
            for trip in 0..4 {
                tt.transport_route.push(RouteIdx(r as u32));
                tt.transport_timing_profile
                    .push(TimingProfileIdx((r * 4 + trip) as u32));
                let departure = 23 * 3600 + (r % 60) as u32 * 30 + trip as u32 * 300;
                tt.transport_first_dep_offset
                    .push(Delta::from_seconds(departure).unwrap());
                tt.transport_initial_day_offset.push(0);
                tt.transport_traffic_days.push(BitfieldIdx(trip as u32 + 1));
                tt.transport_to_trip_section_offsets.push(0);
                tt.timing_profile_events
                    .push(Delta::from_seconds(0).unwrap());
                for stop in 1..stops {
                    let arrival = stop as u32 * (20 + ((r + trip) % 7) as u32);
                    tt.timing_profile_events
                        .push(Delta::from_seconds(arrival).unwrap());
                    if stop + 1 < stops {
                        tt.timing_profile_events
                            .push(Delta::from_seconds(arrival + 5).unwrap());
                    }
                }
                tt.timing_profile_offsets
                    .push(tt.timing_profile_events.len() as u32);
            }
        }
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(40),
            to: LocationIdx(41),
            min_seconds: 180,
            flags: 0,
        });
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(90),
            to: LocationIdx(90),
            min_seconds: 0,
            flags: 1,
        });
        tt
    }

    fn query(stops: usize) -> Query {
        Query {
            departure_unix_seconds: 20_000 * 86_400 + 22 * 3600,
            access: vec![WalkingEndpoint {
                stop: LocationIdx(0),
                seconds: 17,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx((stops - 1) as u32),
                seconds: 23,
            }],
            direct_walk_seconds: None,
            max_boardings: 3,
            max_duration_seconds: 2 * 86_400,
        }
    }

    fn frontier(points: &[PointJourney]) -> Vec<(i64, i64, usize)> {
        points
            .iter()
            .map(|j| (j.start_time, j.dest_time, j.boardings))
            .collect()
    }

    fn assert_labels(a: &NigiriRaptor, b: &NigiriRaptor) {
        assert_eq!(a.best_, b.best_);
        assert_eq!(a.tmp_, b.tmp_);
        assert_eq!(a.round_times_, b.round_times_);
        assert_eq!(a.time_at_dest_, b.time_at_dest_);
        assert_eq!(a.station_mark_.blocks, b.station_mark_.blocks);
        // Algorithmic work must match too; time and worker diagnostics differ.
        let x = &a.stats_;
        let y = &b.stats_;
        assert_eq!(
            (
                x.routes_scanned,
                x.stop_events_scanned,
                x.labels_improved,
                x.transport_lookup_calls,
                x.transport_lookup_probes,
                x.transport_days_tested,
                x.active_transport_days,
                x.transport_bound_prunes,
                x.transport_lookup_skipped_onboard
            ),
            (
                y.routes_scanned,
                y.stop_events_scanned,
                y.labels_improved,
                y.transport_lookup_calls,
                y.transport_lookup_probes,
                y.transport_days_tested,
                y.active_transport_days,
                y.transport_bound_prunes,
                y.transport_lookup_skipped_onboard
            )
        );
    }

    #[test]
    fn parallel_rounds_match_serial_labels_with_reuse_bounds_and_direction_changes() {
        let bytes = network(160, 256).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let topology = LowerBoundTopology::build(&tt);
        let q = query(256);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        pool.install(|| {
            let mut serial = NigiriRaptor::new(&tt, &q, false, Some(&topology)).unwrap();
            serial.route_workers = 1;
            let mut parallel = NigiriRaptor::new(&tt, &q, false, Some(&topology)).unwrap();
            parallel.route_workers = 4;
            let mut serial_lb = vec![0; tt.coordinates().len()];
            let mut parallel_lb = serial_lb.clone();
            for reverse in [false, true, false, true] {
                if serial.reverse != reverse {
                    serial.flip_point_direction(&mut serial_lb);
                    parallel.flip_point_direction(&mut parallel_lb);
                }
                for day in 0..3 {
                    serial.reset_point_arrivals();
                    parallel.reset_point_arrivals();
                    let anchor = q.departure_unix_seconds
                        + day * 86_400
                        + if reverse { 4 * 3600 } else { 0 };
                    // Execute twice without resetting, as retained PONG labels do.
                    for offset in [0, 60] {
                        let anchor = anchor + offset;
                        let limit = step_limit(anchor, reverse);
                        let a = serial
                            .execute_point(&tt, &q, anchor, q.max_boardings, limit)
                            .unwrap();
                        let b = parallel
                            .execute_point(&tt, &q, anchor, q.max_boardings, limit)
                            .unwrap();
                        assert!(!a.is_empty());
                        assert_eq!(frontier(&a), frontier(&b));
                        assert_labels(&serial, &parallel);
                    }
                    let a = serial.fill_ping_bounds(3);
                    let b = parallel.fill_ping_bounds(3);
                    serial.flip_point_direction(&mut serial_lb);
                    parallel.flip_point_direction(&mut parallel_lb);
                    serial.set_ping_bounds(a);
                    parallel.set_ping_bounds(b);
                    let anchor = anchor + if reverse { -1800 } else { 3 * 3600 };
                    let direction = serial.reverse;
                    let a = serial
                        .execute_point(&tt, &q, anchor, 3, step_limit(anchor, direction))
                        .unwrap();
                    let b = parallel
                        .execute_point(&tt, &q, anchor, 3, step_limit(anchor, direction))
                        .unwrap();
                    assert_eq!(frontier(&a), frontier(&b));
                    assert_labels(&serial, &parallel);
                    serial.flip_point_direction(&mut serial_lb);
                    parallel.flip_point_direction(&mut parallel_lb);
                }
            }
            assert!(parallel.stats_.parallel_route_scan_rounds > 0);
            assert_eq!(parallel.stats_.route_scan_workers, 4);
            assert_eq!(serial.stats_.parallel_route_scan_rounds, 0);
        });
    }

    // Several footpaths per stop, stored by ascending duration as the compiler
    // and `validate()` require. Long edges that early pruning skips go to
    // targets that also have shorter alternatives or are simply far away.
    fn with_dense_footpaths(mut tt: TimetableOwned, stops: usize) -> TimetableOwned {
        tt.footpaths_out.clear();
        tt.footpaths_in.clear();
        tt.footpaths_out_offsets = vec![0];
        tt.footpaths_in_offsets = vec![0];
        let mut incoming = vec![Vec::<(u32, u32)>::new(); stops];
        for from in 0..stops {
            let mut edges: Vec<(u32, u32)> = (1..=6u32)
                .map(|d| {
                    (
                        (from as u32 + d * 7) % stops as u32,
                        60 + d * 75 + (from as u32 % 5),
                    )
                })
                .filter(|&(to, _)| to as usize != from)
                .collect();
            edges.sort_by_key(|&(to, secs)| (secs, to));
            for &(to, secs) in &edges {
                tt.footpaths_out
                    .push(PackedFootpath::new(LocationIdx(to), secs).unwrap());
                incoming[to as usize].push((from as u32, secs));
            }
            tt.footpaths_out_offsets.push(tt.footpaths_out.len() as u32);
        }
        for edges in &mut incoming {
            edges.sort_by_key(|&(from, secs)| (secs, from));
            for &(from, secs) in edges.iter() {
                tt.footpaths_in
                    .push(PackedFootpath::new(LocationIdx(from), secs).unwrap());
            }
            tt.footpaths_in_offsets.push(tt.footpaths_in.len() as u32);
        }
        tt
    }

    #[test]
    fn early_pruning_preserves_labels_and_skips_footpaths() {
        let stops = 256;
        let bytes = with_dense_footpaths(network(160, stops), stops)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let topology = LowerBoundTopology::build(&tt);
        let q = query(stops);
        let mut pruned = NigiriRaptor::new(&tt, &q, false, Some(&topology)).unwrap();
        let mut full = NigiriRaptor::new(&tt, &q, false, Some(&topology)).unwrap();
        full.early_prune = false;
        let mut pruned_lb = vec![0; tt.coordinates().len()];
        let mut full_lb = pruned_lb.clone();
        for reverse in [false, true] {
            if pruned.reverse != reverse {
                pruned.flip_point_direction(&mut pruned_lb);
                full.flip_point_direction(&mut full_lb);
            }
            let anchor = q.departure_unix_seconds + if reverse { 4 * 3600 } else { 0 };
            pruned.reset_point_arrivals();
            full.reset_point_arrivals();
            // Ping pass (no bounds): retained round labels must be untouched.
            let limit = step_limit(anchor, reverse);
            let a = pruned
                .execute_point(&tt, &q, anchor, q.max_boardings, limit)
                .unwrap();
            let b = full
                .execute_point(&tt, &q, anchor, q.max_boardings, limit)
                .unwrap();
            assert!(!a.is_empty());
            assert_eq!(frontier(&a), frontier(&b));
            assert_eq!(pruned.round_times_, full.round_times_);
            // Pong pass (with ping bounds): pruning is active.
            let bounds_a = pruned.fill_ping_bounds(3);
            let bounds_b = full.fill_ping_bounds(3);
            pruned.flip_point_direction(&mut pruned_lb);
            full.flip_point_direction(&mut full_lb);
            pruned.set_ping_bounds(bounds_a);
            full.set_ping_bounds(bounds_b);
            let anchor = anchor + if reverse { -1800 } else { 3 * 3600 };
            let direction = pruned.reverse;
            let limit = step_limit(anchor, direction);
            let a = pruned.execute_point(&tt, &q, anchor, 3, limit).unwrap();
            let b = full.execute_point(&tt, &q, anchor, 3, limit).unwrap();
            assert_eq!(frontier(&a), frontier(&b));
            assert_eq!(pruned.best_, full.best_);
            assert_eq!(pruned.round_times_, full.round_times_);
            assert_eq!(pruned.time_at_dest_, full.time_at_dest_);
            pruned.flip_point_direction(&mut pruned_lb);
            full.flip_point_direction(&mut full_lb);
        }
        assert!(pruned.stats_.footpath_edges_examined <= full.stats_.footpath_edges_examined);
    }

    #[test]
    fn unsorted_footpaths_fail_validation() {
        let mut tt = with_dense_footpaths(network(4, 16), 16);
        // Swap the first stop's two shortest edges.
        tt.footpaths_out.swap(0, 1);
        let bytes = tt.encode().unwrap();
        assert!(ZeroCopyTimetable::new(&bytes).is_err());
    }

    fn step_limit(anchor: i64, reverse: bool) -> i64 {
        anchor + if reverse { -2 * 86_400 } else { 2 * 86_400 }
    }

    #[test]
    fn small_rounds_stay_serial() {
        let bytes = network(4, 128).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = query(128);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        pool.install(|| {
            let mut state = NigiriRaptor::new(&tt, &q, false, None).unwrap();
            state.route_workers = 4;
            state
                .execute_point(
                    &tt,
                    &q,
                    q.departure_unix_seconds,
                    3,
                    step_limit(q.departure_unix_seconds, false),
                )
                .unwrap();
            assert_eq!(state.stats_.parallel_route_scan_rounds, 0);
            assert!(state.route_buffers.is_empty());
        });
    }

    #[test]
    fn pong_profiles_match_serial_itineraries_in_both_directions() {
        use crate::routing_common::profile_search::ProfileOptions;
        let mut owned = network(160, 256);
        owned.location_transfer_seconds.fill(0);
        owned.station_transfer_rules.clear();
        owned.footpaths_out.clear();
        owned.footpaths_in.clear();
        owned.footpaths_out_offsets.fill(0);
        owned.footpaths_in_offsets.fill(0);
        for stop in &mut owned.route_stops {
            *stop = PackedStop::new(stop.location(), true, true, true, true).unwrap();
        }
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let topology = LowerBoundTopology::build(&tt);
        let serial = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let parallel = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        for reverse in [false, true] {
            let mut q = query(256);
            if reverse {
                q.departure_unix_seconds += 5 * 3600;
            }
            let run = || {
                crate::routing_common::pong::route_pong_with_stats_precomputed(
                    &tt,
                    &q,
                    ProfileOptions {
                        search_window_seconds: 900,
                        min_itineraries: 1,
                        timeout_seconds: 60,
                        ..ProfileOptions::default()
                    },
                    reverse,
                    Some(&topology),
                )
                .unwrap()
            };
            let (a, stats_a) = serial.install(run);
            let (b, stats_b) = parallel.install(run);
            assert!(!a.is_empty());
            assert_eq!(format!("{a:?}"), format!("{b:?}"));
            assert!(!stats_a.profile_budget_exhausted && !stats_b.profile_budget_exhausted);
            assert_eq!(stats_a.pong_iterations, stats_b.pong_iterations);
            assert_eq!(stats_a.pong_interval_from, stats_b.pong_interval_from);
            assert_eq!(stats_a.pong_interval_to, stats_b.pong_interval_to);
            assert_eq!(stats_a.parallel_route_scan_rounds, 0);
            assert!(stats_b.parallel_route_scan_rounds > 0);
        }
    }

    /// Synthetic microbenchmark, not a prediction for the LA production query.
    /// cargo test --release --manifest-path tools/raptor-check/Cargo.toml \
    ///   benchmark_parallel_route_scans -- --ignored --nocapture
    #[test]
    #[ignore]
    fn benchmark_parallel_route_scans() {
        let bytes = network(1024, 256).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let topology = LowerBoundTopology::build(&tt);
        let q = query(256);
        let mut baseline = None;
        for workers in [1, 2, 4, 8] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            pool.install(|| {
                let mut state = NigiriRaptor::new(&tt, &q, false, Some(&topology)).unwrap();
                state.route_workers = workers;
                let mut samples = Vec::new();
                let mut scans = Vec::new();
                for run in 0..11 {
                    state.reset_point_arrivals();
                    let old = state.stats_.route_scan_ms;
                    let start = Instant::now();
                    let points = state.execute_point(&tt, &q, q.departure_unix_seconds, 3, step_limit(q.departure_unix_seconds, false)).unwrap();
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    let signature = frontier(&points);
                    assert!(!signature.is_empty());
                    if let Some(expected) = &baseline { assert_eq!(&signature, expected); }
                    else { baseline = Some(signature); }
                    if run >= 2 {
                        samples.push(elapsed);
                        scans.push(state.stats_.route_scan_ms - old);
                    }
                }
                samples.sort_by(f64::total_cmp);
                scans.sort_by(f64::total_cmp);
                eprintln!("workers={workers} median_point_ms={:.3} median_route_scan_ms={:.3} parallel_rounds={}", samples[4], scans[4], state.stats_.parallel_route_scan_rounds);
            });
        }
    }
}
