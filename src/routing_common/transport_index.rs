//! Compressed event columns and event-day lookup adapted from Nigiri.
//! See third_party/nigiri-LICENSE and docs/nigiri-profile-parity.md.
//!
//! The v5 mmap stays in seconds. This immutable view is constructed once at
//! timetable load, not once per query/day. Original transport IDs are retained.
use crate::routing_common::raptor_query::RaptorStats;
use crate::routing_common::timetable::{BitfieldIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use std::collections::HashMap;

const DAY: i64 = 86_400;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SelectedTransport {
    pub transport: TransportIdx,
    /// UTC day of the first departure, relative to first_utc_day.
    pub start_day: usize,
    pub event: i64,
}

/// Nigiri's event_times: first departure + event-major pattern offset.
/// No expanded stop-times matrix or query-local calendar index is needed.
pub(crate) struct TransportChain {
    transports: Vec<TransportIdx>,
    first_departures: Vec<u32>,
    patterns: Vec<usize>,
    offsets: Vec<u32>,
    n_patterns: usize,
    calendars: Vec<BitfieldIdx>,
    /// Union of service calendars shifted to all days on which a vehicle runs.
    /// This can extend past the last start day for overnight vehicles.
    event_days: Vec<u64>,
}

impl TransportChain {
    fn event_offset(&self, column: usize, slot: usize) -> i64 {
        i64::from(self.first_departures[column])
            + i64::from(self.offsets[slot * self.n_patterns + self.patterns[column]])
    }

    /// Same guard and day conversion as raptor::get_earliest_transport:
    /// binary search only on the first event day; then visit chronological
    /// event days, pruning on the candidate event + destination lower bound.
    pub(crate) fn select(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        slot: usize,
        ready: i64,
        reverse: bool,
        destination_bound: i64,
        lower_bound: u32,
        stats: &mut RaptorStats,
    ) -> Option<SelectedTransport> {
        stats.transport_lookup_calls += 1;
        let base = i64::from(tt.header().first_utc_day) * DAY;
        let first_day = (ready - base).div_euclid(DAY);
        let second = (ready - base).rem_euclid(DAY);
        let limit = if reverse {
            destination_bound.saturating_add(i64::from(lower_bound))
        } else {
            destination_bound.saturating_sub(i64::from(lower_bound))
        };
        if if reverse {
            ready < limit
        } else {
            ready > limit
        } {
            stats.transport_bound_prunes += 1;
            return None;
        }
        let last_day = (limit - base).div_euclid(DAY);
        let mut day = first_day;
        while if reverse {
            day >= last_day
        } else {
            day <= last_day
        } {
            stats.transport_days_tested += 1;
            let active = day >= 0
                && self
                    .event_days
                    .get(day as usize / 64)
                    .is_some_and(|word| word & (1u64 << (day as usize % 64)) != 0);
            if active {
                stats.active_transport_days += 1;
                let n = self.transports.len();
                let column = |pos: usize| if reverse { n - pos - 1 } else { pos };
                let mut first = 0;
                if day == first_day {
                    let mut hi = n;
                    while first < hi {
                        let mid = first + (hi - first) / 2;
                        stats.transport_lookup_probes += 1;
                        let value = self.event_offset(column(mid), slot) % DAY;
                        if if reverse {
                            value > second
                        } else {
                            value < second
                        } {
                            first = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                }
                for pos in first..n {
                    stats.transport_lookup_probes += 1;
                    let col = column(pos);
                    let offset = self.event_offset(col, slot);
                    let event = base + day * DAY + offset % DAY;
                    if if reverse {
                        event < limit
                    } else {
                        event > limit
                    } {
                        stats.transport_bound_prunes += 1;
                        return None;
                    }
                    let start_day = day - offset / DAY;
                    if start_day < 0 || start_day >= i64::from(tt.header().day_count) {
                        continue;
                    }
                    if !tt.bitfields()[self.calendars[col].idx()].test(start_day as usize) {
                        continue;
                    }
                    return Some(SelectedTransport {
                        transport: self.transports[col],
                        start_day: start_day as usize,
                        event,
                    });
                }
            }
            day += if reverse { -1 } else { 1 };
        }
        None
    }
}

pub(crate) struct TransportIndex {
    routes: Vec<Vec<TransportChain>>,
}

impl TransportIndex {
    pub(crate) fn chains(&self, route: RouteIdx) -> &[TransportChain] {
        &self.routes[route.idx()]
    }

    pub(crate) fn build(tt: &ZeroCopyTimetable<'_>) -> Self {
        let mut routes = Vec::with_capacity(tt.route_transport_ranges().len());
        for range in tt.route_transport_ranges() {
            // Existing files guarantee elapsed-time FIFO, not modulo-day FIFO.
            // Partition as Nigiri's loader/get_index.h does. Also keep the
            // event-day-offset vector equal within a chain: this preserves
            // elapsed ordering even for multi-day trips with differing runtimes.
            let mut groups = Vec::<Vec<TransportIdx>>::new();
            let mut by_day_offsets = HashMap::<Vec<i64>, Vec<usize>>::new();
            for id in range.from..range.to {
                let t = TransportIdx(id);
                let events = tt
                    .events_for_transport(t)
                    .expect("validated timing profile");
                if events.is_empty() {
                    continue;
                }
                let first = i64::from(tt.transport_first_dep_offset()[t.idx()].seconds());
                let days = events
                    .iter()
                    .map(|e| (first + i64::from(e.seconds())) / DAY)
                    .collect::<Vec<_>>();
                let candidates = by_day_offsets.entry(days).or_default();
                let chain = candidates.iter().copied().find(|&index| {
                    let last = *groups[index].last().unwrap();
                    let prev_first =
                        i64::from(tt.transport_first_dep_offset()[last.idx()].seconds());
                    tt.events_for_transport(last)
                        .unwrap()
                        .iter()
                        .zip(events)
                        .all(|(a, b)| {
                            (prev_first + i64::from(a.seconds())) % DAY
                                <= (first + i64::from(b.seconds())) % DAY
                        })
                });
                if let Some(index) = chain {
                    groups[index].push(t);
                } else {
                    candidates.push(groups.len());
                    groups.push(vec![t]);
                }
            }
            let mut chains = Vec::with_capacity(groups.len());
            for transports in groups {
                let slots = tt.events_for_transport(transports[0]).unwrap().len();
                let mut ids = HashMap::new();
                let mut profiles = Vec::new();
                let mut patterns = Vec::with_capacity(transports.len());
                let mut first_departures = Vec::with_capacity(transports.len());
                let mut calendars = Vec::with_capacity(transports.len());
                let max_days = transports
                    .iter()
                    .map(|&t| {
                        (i64::from(tt.transport_first_dep_offset()[t.idx()].seconds())
                            + i64::from(
                                tt.events_for_transport(t)
                                    .unwrap()
                                    .last()
                                    .unwrap()
                                    .seconds(),
                            ))
                            / DAY
                    })
                    .max()
                    .unwrap_or(0) as usize;
                let mut event_days =
                    vec![0u64; (tt.header().day_count as usize + max_days).div_ceil(64)];
                for &t in &transports {
                    let profile = tt.transport_timing_profile()[t.idx()];
                    let index = *ids.entry(profile.idx()).or_insert_with(|| {
                        profiles.push(tt.events_for_transport(t).unwrap());
                        profiles.len() - 1
                    });
                    patterns.push(index);
                    first_departures.push(tt.transport_first_dep_offset()[t.idx()].seconds());
                    let calendar = tt.transport_traffic_days()[t.idx()];
                    calendars.push(calendar);
                    let max_delta = (i64::from(*first_departures.last().unwrap())
                        + i64::from(
                            tt.events_for_transport(t)
                                .unwrap()
                                .last()
                                .unwrap()
                                .seconds(),
                        ))
                        / DAY;
                    // Nigiri rebuild_route_traffic_days: OR shifted calendars.
                    for shift in 0..=max_delta as usize {
                        for (wi, &word) in tt.bitfields()[calendar.idx()].words.iter().enumerate() {
                            let out = wi + shift / 64;
                            let bits = shift % 64;
                            if out < event_days.len() {
                                event_days[out] |= word << bits;
                            }
                            if bits != 0 && out + 1 < event_days.len() {
                                event_days[out + 1] |= word >> (64 - bits);
                            }
                        }
                    }
                }
                let n_patterns = profiles.len();
                let mut offsets = Vec::with_capacity(slots * n_patterns);
                for slot in 0..slots {
                    for profile in &profiles {
                        offsets.push(profile[slot].seconds());
                    }
                }
                chains.push(TransportChain {
                    transports,
                    first_departures,
                    patterns,
                    offsets,
                    n_patterns,
                    calendars,
                    event_days,
                });
            }
            routes.push(chains);
        }
        Self { routes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::profile_compact::ProfileSearchState;
    use crate::routing_common::profile_search::StartLabel;
    use crate::routing_common::raptor_query::{Leg, Query, WalkingEndpoint};
    use crate::routing_common::timetable::*;
    use std::time::Duration;

    /// Profiles list absolute seconds from the vehicle's start-day midnight.
    fn fixture(profiles: &[Vec<u32>], days: &[Vec<usize>], day_count: u32) -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, day_count);
        tt.add_source("test", "calendar", "attempt").unwrap();
        let n = profiles[0].len() / 2 + 1;
        for loc in 0..n {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
            tt.location_routes_offsets.push((loc + 1) as u32);
            tt.location_routes.push(RouteIdx(0));
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
            tt.route_stops
                .push(PackedStop::new(LocationIdx(loc as u32), true, true, true, true).unwrap());
        }
        tt.route_stop_offsets.push(n as u32);
        tt.route_transport_ranges.push(Range {
            from: 0,
            to: profiles.len() as u32,
        });
        tt.route_clasz.push(2);
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        let mut union = TrafficDays::default();
        for (i, (events, active)) in profiles.iter().zip(days).enumerate() {
            tt.timing_profile_events.extend(
                events
                    .iter()
                    .map(|e| Delta::from_seconds(e - events[0]).unwrap()),
            );
            tt.timing_profile_offsets
                .push(tt.timing_profile_events.len() as u32);
            tt.transport_timing_profile.push(TimingProfileIdx(i as u32));
            tt.transport_first_dep_offset
                .push(Delta::from_seconds(events[0]).unwrap());
            tt.transport_route.push(RouteIdx(0));
            tt.transport_initial_day_offset.push(0);
            tt.transport_to_trip_section_offsets.push(0);
            let mut calendar = TrafficDays::default();
            for &d in active {
                calendar.set(d, true).unwrap();
            }
            union.or_assign(&calendar);
            tt.transport_traffic_days
                .push(BitfieldIdx(tt.bitfields.len() as u32));
            tt.bitfields.push(calendar);
        }
        tt.route_traffic_days
            .push(BitfieldIdx(tt.bitfields.len() as u32));
        tt.bitfields.push(union);
        tt
    }

    #[test]
    fn indexed_selection_matches_exhaustive_calendar_search_forward_and_reverse() {
        let profiles = vec![
            vec![60, 180, 200, 500],
            vec![100, 200, 220, 520],
            vec![86_300, 86_390, 86_410, 86_500],
            vec![86_350, 86_420, 86_450, 172_900], // different multi-day runtime
            vec![172_810, 172_830, 172_850, 172_880],
        ];
        let days = vec![
            vec![0, 2, 63, 64],
            vec![1, 3, 64, 65],
            vec![0, 1, 64, 65],
            vec![0, 63],
            vec![1, 65],
        ];
        let bytes = fixture(&profiles, &days, 66).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let index = TransportIndex::build(&tt);
        let base = 20_000 * DAY;
        for reverse in [false, true] {
            for slot in 0..4 {
                for day in [-1, 0, 1, 2, 3, 4, 62, 63, 64, 65, 66, 67, 68] {
                    for second in [
                        0, 59, 60, 99, 100, 199, 200, 499, 500, 86_299, 86_300, 86_399,
                    ] {
                        let ready = base + day * DAY + second;
                        let bound = ready + if reverse { -3 * DAY } else { 3 * DAY };
                        let lb = 31;
                        for chain in index.chains(RouteIdx(0)) {
                            let actual = chain.select(
                                &tt,
                                slot,
                                ready,
                                reverse,
                                bound,
                                lb,
                                &mut RaptorStats::default(),
                            );
                            let mut expected = Vec::new();
                            for &t in &chain.transports {
                                for &d in &days[t.idx()] {
                                    let event =
                                        base + d as i64 * DAY + i64::from(profiles[t.idx()][slot]);
                                    if if reverse {
                                        event <= ready && event - i64::from(lb) >= bound
                                    } else {
                                        event >= ready && event + i64::from(lb) <= bound
                                    } {
                                        expected.push((event, t.0, d));
                                    }
                                }
                            }
                            expected.sort_unstable();
                            let expected = if reverse {
                                expected.last()
                            } else {
                                expected.first()
                            };
                            assert_eq!(
                                actual.map(|x| (x.event, x.transport.0, x.start_day)),
                                expected.copied(),
                                "reverse={reverse}, slot={slot}, day={day}, second={second}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn overnight_boarding_uses_previous_start_day_past_calendar_end() {
        let bytes = fixture(&[vec![86_300, 86_420, 86_430, 86_600]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base + 86_425,
            access: vec![WalkingEndpoint {
                stop: LocationIdx(1),
                seconds: 0,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
        };
        for reverse in [false, true] {
            let anchor = base + if reverse { 86_605 } else { 86_425 };
            let mut state =
                ProfileSearchState::new(&tt, &q, anchor, anchor, reverse, Duration::from_secs(2))
                    .unwrap();
            state
                .extend_starts(&tt, &q, &StartLabel::point(&q, anchor, reverse))
                .unwrap();
            let result = state.result();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].departure_unix_seconds, base + 86_430);
            assert_eq!(result[0].arrival_unix_seconds, base + 86_600);
            assert!(
                result[0]
                    .legs
                    .iter()
                    .any(|leg| matches!(leg, Leg::Ride { service_day: 0, .. }))
            );
        }
    }

    #[test]
    fn modulo_day_and_multiday_overtaking_are_separate_chains() {
        // Both vehicles have elapsed FIFO, but the later-start vehicle arrives
        // after midnight. A next-day earlier-start vehicle can overtake it.
        let bytes = fixture(
            &[vec![0, 100], vec![86_390, 86_600]],
            &[vec![0, 1], vec![0, 1]],
            2,
        )
        .encode()
        .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let index = TransportIndex::build(&tt);
        assert_eq!(index.chains(RouteIdx(0)).len(), 2);
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base + 86_385,
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
            max_duration_seconds: 1000,
        };
        let mut state = ProfileSearchState::new(
            &tt,
            &q,
            q.departure_unix_seconds,
            q.departure_unix_seconds,
            false,
            Duration::from_secs(2),
        )
        .unwrap();
        state
            .extend_starts(
                &tt,
                &q,
                &StartLabel::point(&q, q.departure_unix_seconds, false),
            )
            .unwrap();
        assert_eq!(
            state.result().iter().map(|j| j.arrival_unix_seconds).min(),
            Some(base + 86_500)
        );
    }

    #[test]
    fn bound_prunes_before_skipping_inactive_calendars() {
        let bytes = fixture(&[vec![100, 200], vec![200, 300]], &[vec![1], vec![0]], 2)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let index = TransportIndex::build(&tt);
        let base = 20_000 * DAY;
        let mut stats = RaptorStats::default();
        assert!(
            index.chains(RouteIdx(0))[0]
                .select(&tt, 0, base, false, base + 50, 0, &mut stats)
                .is_none()
        );
        assert_eq!(stats.transport_bound_prunes, 1);
        assert_eq!(stats.active_transport_days, 1); // reject before testing the inactive first transport
    }

    #[test]
    fn no_lookup_when_marked_stop_cannot_improve_onboard_transport() {
        let bytes = fixture(&[vec![100, 200, 200, 300]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base,
            access: vec![
                WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                },
                WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 250,
                },
            ],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
        };
        let mut state =
            ProfileSearchState::new(&tt, &q, base, base, false, Duration::from_secs(2)).unwrap();
        state
            .extend_starts(&tt, &q, &StartLabel::point(&q, base, false))
            .unwrap();
        assert_eq!(state.stats().transport_lookup_calls, 1);
        assert_eq!(state.stats().transport_lookup_skipped_onboard, 1);
        assert_eq!(state.result()[0].arrival_unix_seconds, base + 300);
    }

    #[test]
    fn grouped_start_marks_only_its_own_stop_and_rejects_unimplemented_vias() {
        let bytes = fixture(&[vec![100, 200, 200, 300]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base,
            access: vec![
                WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                },
                WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 0,
                },
            ],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
        };
        let mut state =
            ProfileSearchState::new(&tt, &q, base, base + 200, false, Duration::from_secs(2))
                .unwrap();
        let start = StartLabel {
            time_at_start: base + 100,
            time_at_stop: base + 100,
            stop: LocationIdx(0),
            via_state: 0,
        };
        state.extend_starts(&tt, &q, &[start, start]).unwrap();
        assert_eq!(state.stats().profile_anchor_events, 1);
        assert_eq!(state.stats().profile_start_labels, 1);
        assert_eq!(state.stats().marked_stops, 1);
        assert!(
            state
                .extend_starts(
                    &tt,
                    &q,
                    &[StartLabel {
                        via_state: 1,
                        ..start
                    }]
                )
                .is_err()
        );
    }

    #[test]
    fn boundary_search_dominates_waiting_at_unrelated_access_stop() {
        use crate::routing_common::profile_search::{ProfileOptions, route_profile_with_stats};
        // At start=100 the first platform reaches the destination at 300.
        // Starting outside the interval (>=151) at platform 1 reaches the same
        // vehicle at 200 and arrives at 300, so Nigiri suppresses start=100.
        let bytes = fixture(&[vec![100, 200, 200, 300]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base + 100,
            access: vec![
                WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                },
                WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 0,
                },
            ],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
        };
        let options = ProfileOptions {
            search_window_seconds: 60,
            max_search_window_seconds: 60,
            min_itineraries: 1,
            max_journey_duration_seconds: 1000,
            ..Default::default()
        };
        let (journeys, stats) = route_profile_with_stats(&tt, &q, options, false).unwrap();
        assert_eq!(stats.profile_boundary_events, 1);
        assert_eq!(stats.round_copy_bytes, 0);
        // Recovery must not put a departure outside the allowed window back.
        assert!(journeys.is_empty());
    }

    #[test]
    fn multi_day_profile_matches_exhaustive_direct_trips_in_both_directions() {
        use crate::routing_common::profile_search::{
            ProfileOptions, departure_aware_pareto, route_profile_with_stats,
        };
        use crate::routing_common::raptor_query::Journey;
        let profiles = vec![
            vec![100, 200],
            vec![300, 410],
            vec![86390, 86600],
            vec![86401, 86450],
        ];
        let days = vec![vec![0, 2], vec![0, 1], vec![0, 1], vec![0, 2]];
        let bytes = fixture(&profiles, &days, 3).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        for reverse in [false, true] {
            let reference = if reverse { base + 3 * DAY } else { base };
            let q = Query {
                departure_unix_seconds: reference,
                access: vec![WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 17,
                }],
                egress: vec![WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 23,
                }],
                direct_walk_seconds: None,
                max_boardings: 1,
                max_duration_seconds: 1000,
            };
            let options = ProfileOptions {
                search_window_seconds: 3 * 86400,
                max_search_window_seconds: 3 * 86400,
                min_itineraries: 16,
                max_journey_duration_seconds: 1000,
                ..Default::default()
            };
            let (actual, stats) = route_profile_with_stats(&tt, &q, options, reverse).unwrap();
            let mut expected = Vec::new();
            for (events, active) in profiles.iter().zip(&days) {
                for &day in active {
                    let dep = base + day as i64 * DAY + i64::from(events[0]) - 17;
                    let arr = base + day as i64 * DAY + i64::from(events[1]) + 23;
                    if if reverse {
                        arr >= base && arr <= base + 3 * DAY
                    } else {
                        dep >= base && dep <= base + 3 * DAY
                    } {
                        expected.push(Journey {
                            departure_unix_seconds: dep,
                            arrival_unix_seconds: arr,
                            boardings: 1,
                            legs: vec![],
                        });
                    }
                }
            }
            let signature = |v: Vec<Journey>| {
                let mut result = v
                    .iter()
                    .map(|j| {
                        (
                            j.departure_unix_seconds,
                            j.arrival_unix_seconds,
                            j.boardings,
                        )
                    })
                    .collect::<Vec<_>>();
                result.sort();
                result
            };
            assert!(!stats.profile_budget_exhausted);
            assert_eq!(stats.profile_fallback_searches, 0);
            assert_eq!(
                signature(departure_aware_pareto(actual)),
                signature(departure_aware_pareto(expected)),
                "reverse={reverse}"
            );
        }
    }

    #[test]
    fn route_scan_avoids_repeated_service_days_and_onboard_lookups() {
        use crate::routing_common::raptor_query::route_with_stats;
        let events = (0..38u32)
            .map(|slot| 100 + slot.div_ceil(2) * 10)
            .collect::<Vec<_>>();
        let bytes = fixture(&[events], &[(0..7).collect()], 7).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base,
            access: (0..19u32)
                .map(|stop| WalkingEndpoint {
                    stop: LocationIdx(stop),
                    seconds: if stop == 0 { 0 } else { 101 + stop * 10 },
                })
                .collect(),
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(19),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 7 * 86400,
        };
        let (reference, old_stats) = route_with_stats(&tt, &q).unwrap();
        let mut state =
            ProfileSearchState::new(&tt, &q, base, base, false, Duration::from_secs(2)).unwrap();
        state
            .extend_starts(&tt, &q, &StartLabel::point(&q, base, false))
            .unwrap();
        assert_eq!(
            state.result()[0].arrival_unix_seconds,
            reference[0].arrival_unix_seconds
        );
        assert_eq!(state.stats().transport_lookup_calls, 1);
        assert_eq!(state.stats().transport_lookup_skipped_onboard, 18);
        assert_eq!(state.stats().active_transport_days, 1);
        assert!(old_stats.active_transport_days >= 7);
        assert!(old_stats.transport_lookup_probes > state.stats().transport_lookup_probes * 20);
    }
}
