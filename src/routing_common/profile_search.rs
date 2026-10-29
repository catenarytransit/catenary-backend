//! MOTIS/Nigiri-inspired, bounded timetable-view routing.
//!
//! Unlike a point-in-time earliest-arrival RAPTOR query, timetable view needs
//! *several* origin/arrival anchors. Running one search with a longer deadline
//! still loses later departures because RAPTOR keeps an earliest-arrival label
//! per stop and boarding round. We therefore run a bounded series of searches
//! and retain a Pareto frontier that includes the actual origin departure.
//!
//! This is an incremental compatibility layer, not a replacement for a native
//! profile RAPTOR implementation. It reuses the existing zero-copy timetable,
//! OSR offsets, transfer rules and journey reconstruction. The number of
//! searches is bounded to prevent pathological work on dense urban networks.

use crate::routing_common::raptor_arrive_by;
use crate::routing_common::raptor_query::{
    self, Journey, Leg, Query, RaptorStats,
};
use crate::routing_common::timetable::ZeroCopyTimetable;
use anyhow::{Result, ensure};
use std::time::{Duration, Instant};

pub const DEFAULT_SEARCH_WINDOW_SECONDS: u32 = 15 * 60;
pub const DEFAULT_MAX_SEARCH_WINDOW_SECONDS: u32 = 48 * 3_600;
pub const DEFAULT_MIN_ITINERARIES: usize = 5;
pub const DEFAULT_MAX_JOURNEY_DURATION_SECONDS: u32 = 4 * 86_400;

// Each point RAPTOR search visits a large transit network. A request must
// never turn a few candidate connections into hundreds of complete searches.
// Nigiri instead reuses a single algorithm state across timetable start events.
// Until we implement that native interval algorithm, bound the approximation.
const MAX_PROFILE_SOLVES: usize = 5;
const PROFILE_WALL_BUDGET: Duration = Duration::from_secs(5);
const INITIAL_POINT_HORIZON_SECONDS: u32 = 12 * 3_600;
const MAX_SOLVER_HORIZON_SECONDS: u32 = 7 * 86_400;

#[derive(Clone, Copy, Debug)]
pub struct ProfileOptions {
    pub search_window_seconds: u32,
    pub max_search_window_seconds: u32,
    pub min_itineraries: usize,
    pub max_journey_duration_seconds: u32,
}

impl Default for ProfileOptions {
    fn default() -> Self {
        Self {
            search_window_seconds: DEFAULT_SEARCH_WINDOW_SECONDS,
            max_search_window_seconds: DEFAULT_MAX_SEARCH_WINDOW_SECONDS,
            min_itineraries: DEFAULT_MIN_ITINERARIES,
            max_journey_duration_seconds: DEFAULT_MAX_JOURNEY_DURATION_SECONDS,
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
        ensure!(
            self.max_journey_duration_seconds > 0
                && self
                    .max_journey_duration_seconds
                    .checked_add(self.max_search_window_seconds)
                    .is_some_and(|sum| sum <= MAX_SOLVER_HORIZON_SECONDS),
            "max_journey_duration_seconds plus max_search_window_seconds must be <= 7 days"
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
    total.footpath_edges_examined += one.footpath_edges_examined;
    total.walking_heap_pushes += one.walking_heap_pushes;
    total.labels_improved += one.labels_improved;
    total.labels_dominated += one.labels_dominated;
    total.round_copy_bytes += one.round_copy_bytes;
    total.route_scan_ms += one.route_scan_ms;
    total.walking_ms += one.walking_ms;
    total.profile_searches += 1;
}

/// Prefer a later origin departure, an earlier destination arrival, and no
/// more boardings. A later train with a later arrival is *not* dominated just
/// because an earlier train arrived first. Identical triples are deduplicated.
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

/// Grow the departure/arrival window geometrically, without rerunning RAPTOR
/// merely to discover that tomorrow's sparse train is outside the first 15m.
/// Always clamp growth to the requested cap (and ultimately the timetable).
fn extend_to_include(current: &mut u32, needed: u32, maximum: u32, stats: &mut RaptorStats) {
    while needed > *current && *current < maximum {
        *current = current.saturating_mul(2).min(maximum);
        stats.profile_interval_extensions += 1;
    }
}

/// Nigiri-compatible presentation order. For backward routing the physical
/// arrival is the search-direction start time, not the physical departure.
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

/// Search timetable breakpoints, not arbitrarily sampled timestamps.
/// OSR offsets and the timetable are reused across the entire profile.
/// The search horizon covers wait until the last permitted starting event
/// *plus* a separate maximum in-vehicle/transfer journey duration.
///
/// The current point-query solver provides just one frontier per anchor. The
/// bounded multi-search approach returns useful later departures for sparse
/// intercity service without pretending to compute a complete profile.
/// Bounded adaptive point-search approximation.
///
/// Nigiri performs an interval/profile search with shared RAPTOR state and
/// progressive start-label insertion. Our point solver does not have that state;
/// therefore it is fundamentally unsafe to run it once per departure event.
/// A small number of progressively extended point queries provides a useful
/// Pareto subset while controlling the number of full-network scans.
///
/// The first search uses a twelve-hour deadline, not the old four-day default.
/// Long-distance or sparse services extend the search horizon geometrically.
/// Query results are Pareto-filtered and sorted separately from direct travel.
/// A resource-budget exit returns the non-dominated partial result and exposes
/// that fact in the debug counters, rather than returning an HTTP 500.
pub fn route_profile_with_stats(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    options: ProfileOptions,
    arrive_by: bool,
) -> Result<(Vec<Journey>, RaptorStats)> {
    options.validate()?;
    let started = Instant::now();
    let reference = request.departure_unix_seconds;
    let limit = i64::from(options.max_search_window_seconds);
    let boundary = if arrive_by {
        reference.checked_sub(limit)
    } else {
        reference.checked_add(limit)
    }
    .ok_or_else(|| anyhow::anyhow!("profile interval timestamp overflow"))?;
    let maximum_horizon = options
        .max_journey_duration_seconds
        .checked_add(options.max_search_window_seconds)
        .ok_or_else(|| anyhow::anyhow!("profile solver horizon overflow"))?;
    let mut horizon = INITIAL_POINT_HORIZON_SECONDS.min(maximum_horizon);
    let mut window = options.search_window_seconds;
    let mut anchor = reference;
    let mut stats = RaptorStats::default();
    let mut transit = Vec::new();

    for solve in 0..MAX_PROFILE_SOLVES {
        if (arrive_by && anchor < boundary) || (!arrive_by && anchor > boundary) {
            break;
        }
        if solve != 0 && started.elapsed() >= PROFILE_WALL_BUDGET {
            stats.profile_budget_exhausted = true;
            break;
        }

        let mut q = request.clone();
        q.departure_unix_seconds = anchor;
        q.direct_walk_seconds = None;
        // Use a short point-query deadline first. The requested maximum
        // *journey* duration remains a separate filter below.
        q.max_duration_seconds = horizon;
        let (mut candidates, one_stats) = if arrive_by {
            raptor_arrive_by::route_arrive_by_with_stats(tt, &q)?
        } else {
            raptor_query::route_with_stats(tt, &q)?
        };
        accumulate_stats(&mut stats, one_stats);

        // No event can be inferred from an empty result. Broaden only the
        // solver time horizon; do not guess at global timetable breakpoints.
        if candidates.is_empty() {
            if horizon == maximum_horizon {
                break;
            }
            horizon = horizon.saturating_mul(2).min(maximum_horizon);
            continue;
        }

        if arrive_by {
            candidates.sort_by_key(|j| std::cmp::Reverse(j.arrival_unix_seconds));
        } else {
            candidates.sort_by_key(|j| j.departure_unix_seconds);
        }
        let next_event = if arrive_by {
            candidates[0].arrival_unix_seconds
        } else {
            candidates[0].departure_unix_seconds
        };
        let distance = if arrive_by {
            reference.saturating_sub(next_event)
        } else {
            next_event.saturating_sub(reference)
        };
        if distance > limit {
            break;
        }
        extend_to_include(
            &mut window,
            distance as u32,
            options.max_search_window_seconds,
            &mut stats,
        );
        for journey in candidates {
            let time = if arrive_by {
                journey.arrival_unix_seconds
            } else {
                journey.departure_unix_seconds
            };
            let in_window = if arrive_by {
                time >= reference - i64::from(window) && time <= reference
            } else {
                time >= reference && time <= reference + i64::from(window)
            };
            if in_window
                && journey.arrival_unix_seconds - journey.departure_unix_seconds
                    <= i64::from(options.max_journey_duration_seconds)
            {
                transit.push(journey);
            }
        }
        transit = departure_aware_pareto(transit);
        if transit.len() >= options.min_itineraries {
            break;
        }

        // Move strictly past the last event used for this anchor. If an
        // alternative has not yet been found, the next point query may retain
        // a larger time horizon but never expands the number of searches.
        let next_anchor = if arrive_by {
            next_event.saturating_sub(1)
        } else {
            next_event.saturating_add(1)
        };
        if (arrive_by && next_anchor >= anchor)
            || (!arrive_by && next_anchor <= anchor)
        {
            break;
        }
        anchor = next_anchor;
    }

    if stats.profile_searches as usize == MAX_PROFILE_SOLVES
        && transit.len() < options.min_itineraries
    {
        stats.profile_budget_exhausted = true;
    }

    // Direct walking is independent of the transit Pareto frontier and must
    // neither suppress transit results nor satisfy the minimum count.
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
        assert!(stats.profile_searches >= 2);
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
    fn sparse_profile_searches_are_bounded_without_event_enumeration() {
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
        assert!(stats.profile_searches <= MAX_PROFILE_SOLVES as u64);
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
        assert!(stats.profile_searches <= MAX_PROFILE_SOLVES as u64);
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
}
