//! Departure-event timetable profile search for Edelweiss.
//!
//! A point-time RAPTOR search alone loses later departures when earlier arrival
//! labels win. Enumerate *actual* reachable first-boarding events (or final
//! alighting events for arrive-by) across the requested interval, and merge the
//! resulting Pareto frontiers. The enumeration is complete up to an explicit
//! work/time budget; partial results are marked in RaptorStats.
//!
//! This is an event-complete interval search, not yet Nigiri's incremental
//! shared-state RAPTOR. OSR offsets and the mmap timetable are reused, while
//! each event is still a point solve. The work budget is therefore essential.

use crate::routing_common::raptor_arrive_by;
use crate::routing_common::interval_estimate::{Interval, IntervalEstimator};
use crate::routing_common::profile_raptor::ProfileSearchState;
use crate::routing_common::raptor_query::{
    self, Journey, Leg, Query, RaptorStats, for_each_route_service_day, transport_event_second,
};
use crate::routing_common::timetable::{TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, ensure};
use std::time::{Duration, Instant};

pub const DEFAULT_SEARCH_WINDOW_SECONDS: u32 = 15 * 60;
pub const DEFAULT_MAX_SEARCH_WINDOW_SECONDS: u32 = 48 * 3_600;
pub const DEFAULT_MIN_ITINERARIES: usize = 5;
pub const DEFAULT_MAX_JOURNEY_DURATION_SECONDS: u32 = 4 * 86_400;
pub const DEFAULT_EXTREME_DURATION_RATIO: f64 = 1.75;
pub const DEFAULT_EXTREME_DURATION_SLACK_SECONDS: u32 = 30 * 60;
pub const DEFAULT_COMPARISON_WINDOW_SECONDS: u32 = 90 * 60;

// Process each real origin event only once, with a shared wall budget.
const MAX_PROFILE_BATCHES: usize = 64;
pub const DEFAULT_PROFILE_TIMEOUT_SECONDS: u32 = 5;
const MAX_SOLVER_HORIZON_SECONDS: u32 = 7 * 86_400;

#[derive(Clone, Copy, Debug)]
pub struct ProfileOptions {
    pub search_window_seconds: u32,
    pub max_search_window_seconds: u32,
    pub min_itineraries: usize,
    pub timeout_seconds: u32,
    pub max_journey_duration_seconds: u32,
    pub extreme_duration_ratio: f64,
    pub extreme_duration_slack_seconds: u32,
    pub comparison_window_seconds: u32,
}

impl Default for ProfileOptions {
    fn default() -> Self {
        Self {
            search_window_seconds: DEFAULT_SEARCH_WINDOW_SECONDS,
            max_search_window_seconds: DEFAULT_MAX_SEARCH_WINDOW_SECONDS,
            min_itineraries: DEFAULT_MIN_ITINERARIES,
            timeout_seconds: DEFAULT_PROFILE_TIMEOUT_SECONDS,
            max_journey_duration_seconds: DEFAULT_MAX_JOURNEY_DURATION_SECONDS,
            extreme_duration_ratio: DEFAULT_EXTREME_DURATION_RATIO,
            extreme_duration_slack_seconds: DEFAULT_EXTREME_DURATION_SLACK_SECONDS,
            comparison_window_seconds: DEFAULT_COMPARISON_WINDOW_SECONDS,
        }
    }
}

impl ProfileOptions {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.search_window_seconds > 0
                && self.search_window_seconds <= self.max_search_window_seconds,
            "search_window_seconds must be positive and <= max_search_window_seconds"
        );
        ensure!(
            self.max_search_window_seconds <= 3 * 86_400,
            "max_search_window_seconds must be <= 3 days"
        );
        ensure!(
            (1..=16).contains(&self.min_itineraries),
            "min_itineraries must be between 1 and 16"
        );
        ensure!((1..=120).contains(&self.timeout_seconds), "timeout_seconds must be between 1 and 120");
        ensure!(
            self.max_journey_duration_seconds > 0
                && self
                    .max_journey_duration_seconds
                    .checked_add(self.max_search_window_seconds)
                    .is_some_and(|sum| sum <= MAX_SOLVER_HORIZON_SECONDS),
            "max_journey_duration_seconds plus max_search_window_seconds must be <= 7 days"
        );
        ensure!(
            self.extreme_duration_ratio.is_finite()
                && (1.0..=10.0).contains(&self.extreme_duration_ratio),
            "extreme_duration_ratio must be finite and between 1 and 10"
        );
        ensure!(
            self.extreme_duration_slack_seconds <= 24 * 3_600,
            "extreme_duration_slack_seconds must be <= 24 hours"
        );
        ensure!(
            self.comparison_window_seconds <= 24 * 3_600,
            "comparison_window_seconds must be <= 24 hours"
        );
        Ok(())
    }
}

fn accumulate_stats(total: &mut RaptorStats, one: RaptorStats) {
    total.rounds += one.rounds;
    total.marked_stops += one.marked_stops;
    total.routes_scanned += one.routes_scanned;
    total.transport_days_tested += one.transport_days_tested;
    total.active_transport_days += one.active_transport_days;
    total.stop_events_scanned += one.stop_events_scanned;
    total.transport_lookup_probes += one.transport_lookup_probes;
    total.footpath_edges_examined += one.footpath_edges_examined;
    total.walking_heap_pushes += one.walking_heap_pushes;
    total.labels_improved += one.labels_improved;
    total.labels_dominated += one.labels_dominated;
    total.round_copy_bytes += one.round_copy_bytes;
    total.route_scan_ms += one.route_scan_ms;
    total.walking_ms += one.walking_ms;
    total.profile_searches += one.profile_searches;
    total.profile_journeys_reconstructed += one.profile_journeys_reconstructed;
    total.profile_journeys_rejected += one.profile_journeys_rejected;
    total.profile_fallback_searches += one.profile_fallback_searches;
}

/// Departure/arrival/boardings dominance matches Nigiri's journey criteria.
/// Equal triples are deduplicated.
fn dominates(a: &Journey, b: &Journey) -> bool {
    a.departure_unix_seconds >= b.departure_unix_seconds
        && a.arrival_unix_seconds <= b.arrival_unix_seconds
        && a.boardings <= b.boardings
}

pub fn departure_aware_pareto(journeys: Vec<Journey>) -> Vec<Journey> {
    let mut result: Vec<Journey> = Vec::new();
    for journey in journeys {
        if result.iter().any(|prior| dominates(prior, &journey)) {
            continue;
        }
        result.retain(|prior| !dominates(&journey, prior));
        result.push(journey);
    }
    result.sort_by_key(|j| {
        (
            j.departure_unix_seconds,
            j.arrival_unix_seconds,
            j.boardings,
        )
    });
    result
}

// Preserve the exact departure/arrival/boardings Pareto frontier. However,
// Nigiri-style journey diversity is also a presentation requirement: two
// distinct public-transport route sequences can be useful when their costs
// are nearly identical. Keep a *small* number of such near-Pareto options
// without inflating the RAPTOR label skyline at every intermediate stop.
const MAX_NEAR_PARETO_ROUTE_OPTIONS: usize = 6;
const MAX_ALTERNATIVE_ARRIVAL_SLACK_SECONDS: i64 = 12 * 60;
const MAX_ALTERNATIVE_DEPARTURE_SLACK_SECONDS: i64 = 20 * 60;

fn route_signature(j: &Journey) -> Vec<u32> {
    j.legs
        .iter()
        .filter_map(|leg| match leg {
            Leg::Ride { route, .. } => Some(*route),
            _ => None,
        })
        .collect()
}

fn route_diversity_count(journeys: &[Journey]) -> usize {
    let mut routes: Vec<Vec<u32>> = journeys
        .iter()
        .map(route_signature)
        .filter(|routes| !routes.is_empty())
        .collect();
    routes.sort_unstable();
    routes.dedup();
    routes.len()
}

/// Keep all strict Pareto journeys, plus at most six route-distinct options
/// that are close to a strict Pareto journey on time and number of boardings.
/// The additional choices are deliberately presentation candidates, not
/// pruning labels, so they never multiply route scans or transfers.
pub(crate) fn route_diverse_pareto(journeys: Vec<Journey>) -> Vec<Journey> {
    let pareto = departure_aware_pareto(journeys.clone());
    let mut seen_routes: Vec<Vec<u32>> = pareto.iter().map(route_signature).collect();
    let mut extras: Vec<Journey> = journeys
        .into_iter()
        .filter(|candidate| {
            let key = route_signature(candidate);
            if key.is_empty() || seen_routes.contains(&key) {
                return false;
            }
            pareto.iter().any(|optimal| {
                let arrival_gap =
                    candidate.arrival_unix_seconds - optimal.arrival_unix_seconds;
                let departure_gap =
                    optimal.departure_unix_seconds - candidate.departure_unix_seconds;
                dominates(optimal, candidate)
                    && (0..=MAX_ALTERNATIVE_ARRIVAL_SLACK_SECONDS)
                        .contains(&arrival_gap)
                    && (0..=MAX_ALTERNATIVE_DEPARTURE_SLACK_SECONDS)
                        .contains(&departure_gap)
                    && candidate.boardings <= optimal.boardings.saturating_add(1)
            })
        })
        .collect();
    // Deterministic quality ordering means the per-signature representative
    // is the fastest candidate, not whichever route was scanned first.
    extras.sort_by_key(|j| {
        (
            j.arrival_unix_seconds.saturating_sub(j.departure_unix_seconds),
            j.boardings,
            j.departure_unix_seconds,
            j.arrival_unix_seconds,
        )
    });
    let mut result = pareto;
    let mut added = 0;
    for journey in extras {
        if added >= MAX_NEAR_PARETO_ROUTE_OPTIONS {
            break;
        }
        let key = route_signature(&journey);
        if seen_routes.contains(&key) {
            continue;
        }
        seen_routes.push(key);
        result.push(journey);
        added += 1;
    }
    result.sort_by_key(|j| {
        (j.departure_unix_seconds, j.arrival_unix_seconds, j.boardings)
    });
    result
}

/// Presentation-only policy, deliberately separate from Pareto dominance.
/// An extreme journey is suppressed only when a much faster journey exists
/// *near the same requested-direction time*. Sparse daily trains are retained.
fn filter_extreme_durations(
    journeys: Vec<Journey>,
    options: ProfileOptions,
    arrive_by: bool,
) -> Vec<Journey> {
    journeys
        .iter()
        .filter(|candidate| {
            let duration = candidate
                .arrival_unix_seconds
                .saturating_sub(candidate.departure_unix_seconds);
            if duration < 0 || candidate.boardings == 0 {
                return true;
            }
            let comparable_time = |j: &Journey| {
                if arrive_by {
                    j.arrival_unix_seconds
                } else {
                    j.departure_unix_seconds
                }
            };
            let best_nearby = journeys
                .iter()
                .filter(|other| {
                    comparable_time(other).abs_diff(comparable_time(candidate))
                        <= u64::from(options.comparison_window_seconds)
                })
                .map(|other| {
                    other
                        .arrival_unix_seconds
                        .saturating_sub(other.departure_unix_seconds)
                })
                .filter(|seconds| *seconds >= 0)
                .min()
                .unwrap_or(duration);
            (duration as f64)
                <= (best_nearby as f64) * options.extreme_duration_ratio
                    + f64::from(options.extreme_duration_slack_seconds)
        })
        .cloned()
        .collect()
}

/// Prefer requested-direction time order; boarding count breaks exact ties.
pub fn sort_journeys_for_response(journeys: &mut [Journey], arrive_by: bool) {
    if arrive_by {
        journeys.sort_by_key(|j| {
            (
                j.arrival_unix_seconds,
                j.boardings,
                j.departure_unix_seconds,
            )
        });
    } else {
        journeys.sort_by_key(|j| {
            (
                j.departure_unix_seconds,
                j.boardings,
                j.arrival_unix_seconds,
            )
        });
    }
}

/// Enumerate all feasible initial boarding (or terminal alighting) events at
/// OSR-reachable stops in the *maximum* requested timetable interval. An event
/// timestamp is shifted by its access/egress duration, producing an origin
/// departure or destination arrival anchor, not a stop event timestamp.
///
/// We binary-search FIFO transport columns per stop/service day to avoid
/// scanning historic trips outside the window. An active service bitmap is
/// checked for each candidate transport before accepting its event.
fn timetable_start_events(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    reference: i64,
    boundary: i64,
    arrive_by: bool,
    stats: &mut RaptorStats,
) -> Vec<i64> {
    // Do not enqueue a fake service at the requested timestamp: an empty
    // initial window must advance to the first actual boarding/alighting.
    let mut events = Vec::new();
    let endpoints = if arrive_by {
        &request.egress
    } else {
        &request.access
    };
    for endpoint in endpoints {
        let offset = i64::from(endpoint.seconds);
        let first_event =
            reference
                .min(boundary)
                .saturating_add(if arrive_by { -offset } else { offset });
        let last_event =
            reference
                .max(boundary)
                .saturating_add(if arrive_by { -offset } else { offset });
        for &route in tt.routes_at(endpoint.stop) {
            let stops = tt.stops_on(route);
            for (position, stop) in stops.iter().enumerate() {
                if stop.location() != endpoint.stop
                    || if arrive_by {
                        !stop.inbound()
                    } else {
                        !stop.outbound()
                    }
                {
                    continue;
                }
                // Last departure / first arrival positions cannot be used
                // to start/end a transit leg.
                if (!arrive_by && position + 1 >= stops.len()) || (arrive_by && position == 0) {
                    continue;
                }
                for_each_route_service_day(
                    tt,
                    route,
                    first_event,
                    last_event,
                    stats,
                    |day, stats| {
                        let midnight = (i64::from(tt.header().first_utc_day) + day as i64) * 86_400;
                        let range = tt.route_transport_ranges()[route.idx()];
                        let (mut lo, mut hi) = (range.from, range.to);
                        while lo < hi {
                            let mid = lo + (hi - lo) / 2;
                            stats.transport_lookup_probes += 1;
                            let Some(event) = transport_event_second(
                                tt,
                                route,
                                TransportIdx(mid),
                                position,
                                arrive_by,
                                midnight,
                            ) else {
                                return;
                            };
                            if event < first_event {
                                lo = mid + 1;
                            } else {
                                hi = mid;
                            }
                        }
                        for transport_idx in lo..range.to {
                            let transport = TransportIdx(transport_idx);
                            let Some(event) = transport_event_second(
                                tt, route, transport, position, arrive_by, midnight,
                            ) else {
                                continue;
                            };
                            if event > last_event {
                                break;
                            }
                            stats.stop_events_scanned += 1;
                            if !tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()]
                                .test(day)
                            {
                                continue;
                            }
                            let at_origin = if arrive_by {
                                event.saturating_add(offset)
                            } else {
                                event.saturating_sub(offset)
                            };
                            if at_origin >= reference.min(boundary)
                                && at_origin <= reference.max(boundary)
                            {
                                events.push(at_origin);
                            }
                        }
                    },
                );
            }
        }
    }
    events.sort_unstable();
    events.dedup();
    if arrive_by {
        events.reverse();
    }
    events
}

/// Solve at every reachable timetable event in the currently searched
/// interval, extend when too few *presentable* itineraries were found, and
/// preserve an explicit incomplete-result signal upon resource exhaustion.
/// Each event is exact for point RAPTOR; the complete interval frontier is
/// exact only when the work/time budget is not exhausted.
pub fn route_profile_with_stats(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    options: ProfileOptions,
    arrive_by: bool,
) -> Result<(Vec<Journey>, RaptorStats)> {
    options.validate()?;
    let started = Instant::now();
    let reference = request.departure_unix_seconds;
    let max_window = i64::from(options.max_search_window_seconds);
    let boundary = if arrive_by {
        reference.checked_sub(max_window)
    } else {
        reference.checked_add(max_window)
    }
    .ok_or_else(|| anyhow::anyhow!("profile interval timestamp overflow"))?;
    let mut stats = RaptorStats::default();
    // Generate only the service-active anchors once from the compressed
    // stop/route/transport columns, rather than probing an entire route graph
    // for every timetable departure.
    let events = timetable_start_events(tt, request, reference, boundary, arrive_by, &mut stats);
    let estimator = IntervalEstimator::new(reference, options.max_search_window_seconds,
        options.min_itineraries, arrive_by);
    let initial = if arrive_by {
        Interval { from: reference.saturating_sub(i64::from(options.search_window_seconds)), to: reference }
    } else {
        Interval { from: reference, to: reference.saturating_add(i64::from(options.search_window_seconds)) }
    };
    let mut interval = estimator.initial(initial);
    let mut examined = 0usize;
    let mut batches = 0usize;
    let deadline = started.checked_add(Duration::from_secs(u64::from(options.timeout_seconds)))
        .unwrap_or(started);
    stats.profile_events_enumerated = events.len() as u64;

    let mut q = request.clone();
    q.direct_walk_seconds = None;
    q.max_duration_seconds = options.max_journey_duration_seconds;
    // Set a conservative global time horizon once: every expansion must be
    // evaluated against the same frontier and immutable predecessor paths.
    let first = reference.min(boundary);
    let last = reference.max(boundary);
    let remaining = deadline.saturating_duration_since(Instant::now());
    let mut state = ProfileSearchState::new(tt, &q, first, last, arrive_by, remaining)?;
    let mut transit = Vec::new();
    loop {
        if Instant::now() >= deadline || batches >= MAX_PROFILE_BATCHES {
            stats.profile_budget_exhausted = true;
            break;
        }
        let previous = examined;
        while examined < events.len() {
            let event = events[examined];
            if event < interval.from || event > interval.to {
                break;
            }
            examined += 1;
        }
        if examined > previous {
            let mut batch = events[previous..examined].to_vec();
            batch.sort_unstable();
            state.extend(tt, &q, &batch)?;
        }
        batches += 1;
        transit = route_diverse_pareto(state.result().into_iter().filter(|j| {
            let t = if arrive_by { j.arrival_unix_seconds } else { j.departure_unix_seconds };
            t >= interval.from && t <= interval.to
                && j.arrival_unix_seconds.saturating_sub(j.departure_unix_seconds)
                    <= i64::from(options.max_journey_duration_seconds)
        }).collect());
        let qualifying = filter_extreme_durations(transit.clone(), options, arrive_by);
        // Like Nigiri's n_results_in_interval(), count qualifying Pareto
        // journeys whose start time lies within the actual searched interval.
        let strict = departure_aware_pareto(qualifying);
        if strict.len() >= options.min_itineraries || state.stats().profile_budget_exhausted {
            break;
        }
        if examined == events.len() { break; }
        let next = estimator.extension(interval,
            options.min_itineraries.saturating_sub(strict.len()));
        if next == interval { break; }
        interval = next;
        stats.profile_interval_extensions += 1;
    }
    let accumulated = state.stats().clone();
    accumulate_stats(&mut stats, accumulated.clone());
    stats.profile_anchor_events += accumulated.profile_anchor_events;
    stats.profile_budget_exhausted |= accumulated.profile_budget_exhausted;
    // Recover a usable candidate when the incremental provenance skyline is
    // interrupted before any valid journey. This invokes *one* bounded,
    // well-tested point RAPTOR search rather than the former 128 point solves.
    // The incomplete profile remains explicitly visible in debug statistics.
    // Do not silently reduce multi-day journey eligibility on fallback.
    if transit.is_empty() && !stats.profile_budget_exhausted
        && Instant::now() < deadline
        && !request.access.is_empty() && !request.egress.is_empty() {
        let mut fallback = request.clone();
        fallback.direct_walk_seconds = None;
        fallback.max_duration_seconds = options.max_journey_duration_seconds;
        fallback.departure_unix_seconds = events.first().copied().unwrap_or(reference);
        let (candidates, one) = if arrive_by {
            raptor_arrive_by::route_arrive_by_with_stats(tt, &fallback)?
        } else {
            raptor_query::route_with_stats(tt, &fallback)?
        };
        stats.profile_fallback_searches += 1;
        accumulate_stats(&mut stats, one);
        transit.extend(candidates.into_iter().filter(|j| {
            let t = if arrive_by {
                j.arrival_unix_seconds
            } else {
                j.departure_unix_seconds
            };
            t.abs_diff(reference) <= u64::from(options.max_search_window_seconds)
                && j.arrival_unix_seconds
                    .saturating_sub(j.departure_unix_seconds)
                    <= i64::from(options.max_journey_duration_seconds)
        }));
        transit = route_diverse_pareto(transit);
    }

    let total_before_filter = transit.len();
    transit = filter_extreme_durations(transit, options, arrive_by);
    stats.profile_duration_filtered = (total_before_filter - transit.len()) as u64;

    if let Some(seconds) = request.direct_walk_seconds {
        if seconds <= options.max_journey_duration_seconds {
            let (departure, arrival) = if arrive_by {
                (reference - i64::from(seconds), reference)
            } else {
                (reference, reference + i64::from(seconds))
            };
            transit.push(Journey {
                boardings: 0,
                departure_unix_seconds: departure,
                arrival_unix_seconds: arrival,
                legs: vec![Leg::DirectWalk { seconds }],
            });
        }
    }
    sort_journeys_for_response(&mut transit, arrive_by);
    Ok((transit, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::raptor_query::WalkingEndpoint;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, LocationIdx, PackedStop, Range, RouteIdx, SourceIdx, TimetableOwned,
        TimingProfileIdx, TrafficDays,
    };

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

    fn request() -> Query {
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
            max_duration_seconds: 6 * 3_600,
        }
    }

    #[test]
    fn first_real_event_is_not_a_fake_requested_departure() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut stats = RaptorStats::default();
        let request = request();
        let reference = request.departure_unix_seconds;
        let events = timetable_start_events(
            &tt, &request, reference, reference + 2 * 86_400,
            false, &mut stats,
        );
        assert!(!events.contains(&reference));
        assert!(!events.is_empty());
        assert!(events[0] > reference + 15 * 60);
    }

    #[test]
    fn a_late_first_service_is_not_lost_before_interval_expansion() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = request();
        let (journeys, stats) = route_profile_with_stats(
            &tt, &q,
            ProfileOptions {
                max_journey_duration_seconds: 36 * 3_600,
                ..ProfileOptions::default()
            },
            false,
        ).unwrap();
        assert!(!journeys.is_empty(), "first service was dropped: {stats:?}");
        assert!(stats.profile_interval_extensions > 0);
        assert!(stats.profile_events_enumerated > 0);
    }

    #[test]
    fn tomorrow_daily_train_is_not_lost_to_six_hour_point_horizon() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        assert!(raptor_query::route(&tt, &request()).unwrap().is_empty());

        let (journeys, stats) = route_profile_with_stats(
            &tt,
            &request(),
            ProfileOptions {
                min_itineraries: 2,
                max_journey_duration_seconds: 36 * 3_600,
                ..ProfileOptions::default()
            },
            false,
        )
        .unwrap();
        let base = 20_000_i64 * 86_400;
        assert_eq!(journeys.len(), 2);
        assert_eq!(
            journeys[0].departure_unix_seconds,
            base + 86_400 + 9 * 3_600
        );
        assert_eq!(
            journeys[1].departure_unix_seconds,
            base + 2 * 86_400 + 9 * 3_600
        );
        assert!(stats.profile_interval_extensions > 0);
        assert_eq!(stats.profile_searches, 1); // shared state across all expansions
    }

    #[test]
    fn incremental_profile_matches_daily_compressed_event_timestamps() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = Query {
            max_duration_seconds: 36 * 3_600,
            ..request()
        };
        let base = 20_000_i64 * 86_400;
        let tomorrow = base + 86_400 + 9 * 3_600;
        let later = base + 2 * 86_400 + 9 * 3_600;
        let (journeys, stats) = crate::routing_common::profile_raptor::route_profile_incremental(
            &tt,
            &q,
            &[tomorrow, later],
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        let departure_times: Vec<_> = journeys.iter().map(|j| j.departure_unix_seconds).collect();
        assert!(
            departure_times.contains(&tomorrow),
            "missing first daily service: {departure_times:?}"
        );
        assert!(
            departure_times.contains(&later),
            "missing later daily service: {departure_times:?}"
        );
        assert_eq!(stats.profile_anchor_events, 2);
        for j in journeys {
            assert_eq!(j.arrival_unix_seconds - j.departure_unix_seconds, 8 * 3_600);
            assert_eq!(j.boardings, 1);
        }
    }

    #[test]
    fn reverse_profile_matches_daily_compressed_event_timestamps() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let q = Query {
            max_duration_seconds: 36 * 3_600,
            ..request()
        };
        let base = 20_000_i64 * 86_400;
        let first_arrival = base + 86_400 + 17 * 3_600;
        let second_arrival = base + 2 * 86_400 + 17 * 3_600;
        let (journeys, stats) = crate::routing_common::profile_raptor::route_profile_incremental(
            &tt,
            &q,
            &[first_arrival, second_arrival],
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(stats.profile_anchor_events, 2);
        assert!(
            journeys
                .iter()
                .any(|j| j.arrival_unix_seconds == first_arrival)
        );
        assert!(
            journeys
                .iter()
                .any(|j| j.arrival_unix_seconds == second_arrival)
        );
        assert!(journeys.iter().all(|j| j.boardings == 1));
    }

    #[test]
    fn later_departure_with_later_arrival_is_an_alternative() {
        let make = |departure, arrival, boardings| Journey {
            departure_unix_seconds: departure,
            arrival_unix_seconds: arrival,
            boardings,
            legs: Vec::new(),
        };
        let kept = departure_aware_pareto(vec![
            make(100, 200, 1),
            make(200, 300, 1),
            make(120, 190, 1),
            make(120, 190, 1),
        ]);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].departure_unix_seconds, 120);
        assert_eq!(kept[1].departure_unix_seconds, 200);
    }

    #[test]
    fn preserves_bounded_distinct_route_options_without_showing_bad_detours() {
        fn candidate(route: u32, dep: i64, arr: i64, boardings: usize) -> Journey {
            Journey {
                departure_unix_seconds: dep,
                arrival_unix_seconds: arr,
                boardings,
                legs: vec![Leg::Ride {
                    from_stop: 0,
                    to_stop: 1,
                    route,
                    transport: route,
                    service_day: 0,
                    board_stop_index: 0,
                    alight_stop_index: 1,
                    departure_unix_seconds: dep,
                    arrival_unix_seconds: arr,
                }],
            }
        }
        let options = route_diverse_pareto(vec![
            candidate(1, 100, 1_900, 1), // Strict optimum.
            candidate(2, 100, 2_140, 1), // +4 min, different route.
            candidate(3, 100, 2_140, 1), // Equal time, different route.
            candidate(1, 100, 2_080, 1), // Same route as optimum.
            candidate(4, 100, 4_000, 1), // Unreasonable detour.
        ]);
        assert_eq!(options.len(), 3);
        assert_eq!(route_diversity_count(&options), 3);
        assert!(options.iter().any(|j| route_signature(j) == vec![2]));
        assert!(options.iter().any(|j| route_signature(j) == vec![3]));
        assert!(!options.iter().any(|j| route_signature(j) == vec![4]));
    }

    #[test]
    fn route_diversity_supplement_preserves_daily_departure_pareto() {
        fn candidate(dep: i64, arr: i64) -> Journey {
            Journey {
                departure_unix_seconds: dep,
                arrival_unix_seconds: arr,
                boardings: 1,
                legs: vec![Leg::Ride {
                    from_stop: 0, to_stop: 1, route: 1, transport: 0, service_day: 0,
                    board_stop_index: 0, alight_stop_index: 1,
                    departure_unix_seconds: dep, arrival_unix_seconds: arr,
                }],
            }
        }
        let choices = route_diverse_pareto(vec![
            candidate(100, 1_900),
            candidate(300, 2_100),
        ]);
        assert_eq!(choices.len(), 2);
    }

    #[test]
    fn output_order_tracks_the_search_direction() {
        let make = |dep, arr, boardings| Journey {
            departure_unix_seconds: dep,
            arrival_unix_seconds: arr,
            boardings,
            legs: Vec::new(),
        };
        let original = vec![make(100, 180, 2), make(140, 190, 1), make(120, 180, 1)];
        let mut forward = original.clone();
        sort_journeys_for_response(&mut forward, false);
        assert_eq!(
            forward
                .iter()
                .map(|j| (j.departure_unix_seconds, j.boardings))
                .collect::<Vec<_>>(),
            vec![(100, 2), (120, 1), (140, 1)]
        );
        let mut backward = original;
        sort_journeys_for_response(&mut backward, true);
        assert_eq!(
            backward
                .iter()
                .map(|j| (j.arrival_unix_seconds, j.boardings))
                .collect::<Vec<_>>(),
            vec![(180, 1), (180, 2), (190, 1)]
        );
    }

    #[test]
    fn sparse_profile_searches_remain_work_bounded() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let (journeys, stats) = route_profile_with_stats(
            &tt,
            &request(),
            ProfileOptions {
                min_itineraries: 3,
                max_journey_duration_seconds: 36 * 3_600,
                ..ProfileOptions::default()
            },
            false,
        )
        .unwrap();
        assert!(stats.profile_searches >= 1);
        assert!(stats.profile_searches <= MAX_PROFILE_BATCHES as u64);
        assert!(!journeys.is_empty());
    }

    #[test]
    fn empty_profile_does_not_run_unbounded_point_queries() {
        let bytes = daily_train().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut q = request();
        q.max_boardings = 0;
        let (journeys, stats) = route_profile_with_stats(
            &tt,
            &q,
            ProfileOptions {
                max_journey_duration_seconds: 36 * 3_600,
                ..ProfileOptions::default()
            },
            false,
        )
        .unwrap();
        assert!(journeys.is_empty());
        assert!(stats.profile_searches <= MAX_PROFILE_BATCHES as u64);
    }

    #[test]
    fn rejects_unbounded_profile_horizons() {
        assert!(
            ProfileOptions {
                max_journey_duration_seconds: 7 * 86_400,
                ..ProfileOptions::default()
            }
            .validate()
            .is_err()
        );
    }
    #[test]
    fn duration_filter_removes_extreme_one_transfer_tradeoff() {
        let make = |dep, arr, boardings| Journey {
            departure_unix_seconds: dep,
            arrival_unix_seconds: arr,
            boardings,
            legs: Vec::new(),
        };
        let choices = departure_aware_pareto(vec![
            make(20_000, 20_000 + 10 * 3_600 + 38 * 60, 2),
            make(20_000, 20_000 + 3 * 3_600 + 43 * 60, 3),
        ]);
        assert_eq!(choices.len(), 2);
        let kept = filter_extreme_durations(choices, ProfileOptions::default(), false);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].boardings, 3);
    }

    #[test]
    fn duration_filter_keeps_sparse_departures() {
        let make = |dep, dur| Journey {
            departure_unix_seconds: dep,
            arrival_unix_seconds: dep + dur,
            boardings: 1,
            legs: Vec::new(),
        };
        let kept = filter_extreme_durations(
            vec![
                make(20_000, 10 * 3_600),
                make(20_000 + 8 * 3_600, 3 * 3_600),
            ],
            ProfileOptions::default(),
            false,
        );
        assert_eq!(kept.len(), 2);
    }
}
