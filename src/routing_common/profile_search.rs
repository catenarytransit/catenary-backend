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
use crate::routing_common::raptor_query::{self, Journey, Leg, Query, RaptorStats};
use crate::routing_common::timetable::ZeroCopyTimetable;
use anyhow::{ensure, Result};

pub const DEFAULT_SEARCH_WINDOW_SECONDS: u32 = 15 * 60;
pub const DEFAULT_MAX_SEARCH_WINDOW_SECONDS: u32 = 48 * 3_600;
pub const DEFAULT_MIN_ITINERARIES: usize = 5;
pub const DEFAULT_MAX_JOURNEY_DURATION_SECONDS: u32 = 4 * 86_400;

const MAX_PROFILE_SEARCHES: usize = 8;
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
                && self.max_journey_duration_seconds
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
        (j.departure_unix_seconds, j.arrival_unix_seconds, j.boardings)
    });
    result
}

/// Grow the departure/arrival window geometrically, without rerunning RAPTOR
/// merely to discover that tomorrow's sparse train is outside the first 15m.
/// Always clamp growth to the requested cap (and ultimately the timetable).
fn extend_to_include(
    current: &mut u32,
    needed: u32,
    maximum: u32,
    stats: &mut RaptorStats,
) {
    while needed > *current && *current < maximum {
        *current = current.saturating_mul(2).min(maximum);
        stats.profile_interval_extensions += 1;
    }
}

/// Search different departure anchors (or arrival deadlines) with the same
/// walking offsets; no OSR queries or database lookups occur inside this loop.
/// The search horizon covers wait until the last permitted starting event
/// *plus* a separate maximum in-vehicle/transfer journey duration.
///
/// The current point-query solver provides just one frontier per anchor. The
/// bounded multi-search approach returns useful later departures for sparse
/// intercity service without pretending to compute a complete profile.
pub fn route_profile_with_stats(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    options: ProfileOptions,
    arrive_by: bool,
) -> Result<(Vec<Journey>, RaptorStats)> {
    options.validate()?;
    let mut stats = RaptorStats::default();
    let reference = request.departure_unix_seconds;
    let max_window = i64::from(options.max_search_window_seconds);
    let boundary = if arrive_by {
        reference.checked_sub(max_window)
    } else {
        reference.checked_add(max_window)
    }
    .ok_or_else(|| anyhow::anyhow!("profile search timestamp overflow"))?;
    let mut window = options.search_window_seconds;
    let mut anchor = reference;
    let mut journeys = Vec::new();

    // Direct walking does not produce infinitely many interchangeable
    // departures: append it only once at the originally requested timestamp.
    if let Some(seconds) = request.direct_walk_seconds {
        if seconds <= options.max_journey_duration_seconds {
            let (departure, arrival) = if arrive_by {
                (reference - i64::from(seconds), reference)
            } else {
                (reference, reference + i64::from(seconds))
            };
            journeys.push(Journey {
                boardings: 0,
                departure_unix_seconds: departure,
                arrival_unix_seconds: arrival,
                legs: vec![Leg::DirectWalk { seconds }],
            });
        }
    }

    for _ in 0..MAX_PROFILE_SEARCHES {
        if (arrive_by && anchor < boundary) || (!arrive_by && anchor > boundary) {
            break;
        }

        let remaining_window = if arrive_by {
            anchor - boundary
        } else {
            boundary - anchor
        };
        // The point-query RAPTOR code bounds arrival/departure *relative to
        // the anchor*. Include the waiting window separately from journey
        // time so an overnight Amtrak service can still be reached.
        let mut q = request.clone();
        q.departure_unix_seconds = anchor;
        q.direct_walk_seconds = None;
        q.max_duration_seconds =
            options.max_journey_duration_seconds + remaining_window as u32;

        let (mut candidates, one_stats) = if arrive_by {
            raptor_arrive_by::route_arrive_by_with_stats(tt, &q)?
        } else {
            raptor_query::route_with_stats(tt, &q)?
        };
        accumulate_stats(&mut stats, one_stats);

        // An empty answer with a horizon that reaches the final allowed
        // boarding/arrival day cannot improve by moving the anchor further.
        if candidates.is_empty() {
            break;
        }

        // For depart-after, the earliest new departure advances the cursor;
        // for arrive-by, the latest new arrival moves the deadline backward.
        // Keep all candidates at that anchor for transfer-count alternatives.
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
        if distance > max_window {
            break;
        }
        extend_to_include(
            &mut window,
            distance as u32,
            options.max_search_window_seconds,
            &mut stats,
        );

        for journey in candidates {
            let t = if arrive_by {
                journey.arrival_unix_seconds
            } else {
                journey.departure_unix_seconds
            };
            if (arrive_by && (t < reference - i64::from(window) || t > reference))
                || (!arrive_by
                    && (t < reference || t > reference + i64::from(window)))
            {
                continue;
            }
            if journey.arrival_unix_seconds - journey.departure_unix_seconds
                > i64::from(options.max_journey_duration_seconds)
            {
                continue;
            }
            journeys.push(journey);
        }
        journeys = departure_aware_pareto(journeys);
        if journeys.len() >= options.min_itineraries {
            break;
        }
        // Make strict progress; do not rescan the same scheduled departure.
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
    Ok((departure_aware_pareto(journeys), stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::raptor_query::WalkingEndpoint;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, LocationIdx, PackedStop, Range, RouteIdx,
        SourceIdx, TimetableOwned, TimingProfileIdx, TrafficDays,
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
            access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: 0 }],
            egress: vec![WalkingEndpoint { stop: LocationIdx(1), seconds: 0 }],
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
        ).unwrap();
        let base = 20_000_i64 * 86_400;
        assert_eq!(journeys.len(), 2);
        assert_eq!(journeys[0].departure_unix_seconds, base + 86_400 + 9 * 3_600);
        assert_eq!(journeys[1].departure_unix_seconds, base + 2 * 86_400 + 9 * 3_600);
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
    fn rejects_unbounded_profile_horizons() {
        assert!(ProfileOptions {
            max_journey_duration_seconds: 7 * 86_400,
            ..ProfileOptions::default()
        }.validate().is_err());
    }
}
