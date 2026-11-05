//! Static, no-via CPU RAPTOR translated from Nigiri 82e55ced's
//! include/nigiri/routing/raptor/{raptor.h,raptor_state.h}.
//! MIT license: third_party/nigiri-LICENSE. The wire adapter uses seconds.
//! Search labels are best_, tmp_, and round_times_; no predecessor labels
//! or journey reconstruction participate in route/transfer relaxation.
use crate::routing_common::profile_compact::{LowerBoundTopology, point_lower_bounds};
use crate::routing_common::raptor_query::{Query, RaptorStats, WalkingEndpoint};
use crate::routing_common::timetable::{Delta, LocationIdx, RouteIdx, ZeroCopyTimetable};
use crate::routing_common::transport_index::TransportIndex;
use anyhow::{Result, ensure};
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
    pub(crate) stats_: RaptorStats,
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
            bounds_: None,
            bounds_last_k_: 0,
            stats_: stats,
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
            for route in self.route_mark_.indices() {
                self.update_route(tt, k, RouteIdx(route as u32));
            }
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

    fn update_route(&mut self, tt: &ZeroCopyTimetable<'_>, k: usize, route: RouteIdx) {
        self.stats_.routes_scanned += 1;
        let stops = tt.stops_on(route);
        // Gentian coalesces FIFO chains under a public route. Each chain is
        // an independent Nigiri loader route for transport selection.
        let transports = Arc::clone(&self.transports);
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
                self.stats_.stop_events_scanned += 1;
                let finish = if self.reverse {
                    stop.outbound() && pos + 1 < stops.len()
                } else {
                    stop.inbound() && pos > 0
                };
                if finish && let Some((first, events)) = &onboard {
                    let slot = if self.reverse { pos * 2 } else { pos * 2 - 1 };
                    let by_transport = first + events[slot].seconds() as DeltaTime;
                    if self.reachable(k, loc, by_transport)
                        && self.within_bounds(tt, k, loc, by_transport)
                    {
                        self.tmp_[loc] = best(by_transport, self.tmp_[loc], self.reverse);
                        self.station_mark_.set(loc);
                        self.stats_.labels_improved += 1;
                    }
                }
                let start = if self.reverse {
                    stop.inbound() && pos > 0
                } else {
                    stop.outbound() && pos + 1 < stops.len()
                };
                if !start || !self.prev_station_mark_.test(loc) {
                    continue;
                }
                if !self.lb_reachable(loc) {
                    break;
                }
                let ready = self.round_times_[k - 1][loc];
                let slot = if self.reverse { pos * 2 - 1 } else { pos * 2 };
                let onboard_time = onboard
                    .as_ref()
                    .map(|(first, events)| first + events[slot].seconds() as DeltaTime);
                if ready == self.invalid()
                    || onboard_time.is_some_and(|t| better(t, ready, self.reverse))
                {
                    self.stats_.transport_lookup_skipped_onboard += 1;
                    continue;
                }
                let Some(selected) = chain.select(
                    tt,
                    slot,
                    self.to_unix(ready),
                    self.reverse,
                    self.to_unix(self.time_at_dest_[k]),
                    self.lb_[loc],
                    &mut self.stats_,
                ) else {
                    continue;
                };
                if onboard_time
                    .is_none_or(|old| !better(old, self.to_delta(selected.event), self.reverse))
                {
                    let first = selected.start_day as DeltaTime * 86_400
                        + tt.transport_first_dep_offset()[selected.transport.idx()].seconds()
                            as DeltaTime;
                    onboard = Some((first, tt.events_for_transport(selected.transport).unwrap()));
                }
            }
        }
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
