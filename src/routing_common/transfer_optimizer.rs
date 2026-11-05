//! Optimize inter-vehicle footpaths after RAPTOR reconstruction.
//!
//! The timetable search keeps its Pareto criteria (departure, arrival, boardings).
//! Once the *same two scheduled vehicles* have been chosen, nearby alighting and
//! boarding positions can be re-linked through a shorter legal directed footpath.
//! This is Nigiri's optimize_transfers idea, kept outside the hot route scan.
//!
//! Port of Nigiri 82e55ced src/routing/raptor/optimize_footpaths.cc and
//! src/loader/init_finish.cc::assign_importance. MIT: third_party/nigiri-LICENSE.
//! Candidate iteration and strict penalty comparison follow the C++ order.
//! Catenary retains exact seconds and directed GTFS minimum/prohibited rules.

use crate::routing_common::raptor_query::{Journey, Leg, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};

/// Nigiri's assign_importance, derived once alongside the loaded topology.
/// Keep this outside the wire format so existing Gentian files remain usable.
#[derive(Debug)]
pub(crate) struct StationImportance {
    roots: Vec<usize>,
    values: Vec<u64>,
    maximum: u64,
}

impl StationImportance {
    pub(crate) fn build(tt: &ZeroCopyTimetable<'_>) -> Self {
        // Literal upstream array, including the implicitly zero-initialized
        // final three entries. Use enum indices, not its stale class comments:
        // index 5 is RideSharing, index 12 is ODM in this Nigiri revision.
        const PRIORITY: [u64; 16] = [20, 20, 20, 20, 20, 16, 15, 10, 10, 3, 2, 10, 1, 0, 0, 0];
        let mut route_importance = Vec::with_capacity(tt.route_transport_ranges().len());
        for (r, range) in tt.route_transport_ranges().iter().enumerate() {
            let first_section = tt.route_section_clasz_offsets()[r] as usize;
            let section_end = tt.route_section_clasz_offsets()[r + 1] as usize;
            let clasz = if first_section < section_end {
                tt.route_section_clasz()[first_section]
            } else {
                tt.route_clasz()[r]
            };
            let active_days: u64 = (range.from..range.to)
                .map(|t| {
                    tt.bitfields()[tt.transport_traffic_days()[t as usize].idx()]
                        .words
                        .iter()
                        .map(|w| u64::from(w.count_ones()))
                        .sum::<u64>()
                })
                .sum();
            route_importance.push(active_days * PRIORITY.get(clasz as usize).copied().unwrap_or(0));
        }
        let parents = tt.location_parent();
        let mut values = vec![0u64; parents.len()];
        let mut roots = Vec::with_capacity(parents.len());
        for (i, &parent) in parents.iter().enumerate() {
            // assign_importance aggregates at the immediate parent.
            let at = if parent == LocationIdx::INVALID {
                i
            } else {
                parent.idx()
            };
            for route in tt.routes_at(LocationIdx(i as u32)) {
                values[at] = values[at].saturating_add(route_importance[route.idx()]);
            }
            // timetable::locations::get_root_idx, including its cycle guard.
            let mut root = i;
            let mut steps = 0;
            while parents[root] != LocationIdx::INVALID {
                let p = parents[root].idx();
                if p == i || steps > 20 {
                    root = parent.idx();
                    break;
                }
                root = p;
                steps += 1;
            }
            roots.push(root);
        }
        let maximum = values.iter().copied().max().unwrap_or(0);
        Self {
            roots,
            values,
            maximum,
        }
    }

    /// get_penalty in seconds: duration - station reward - buffer reward.
    fn penalty(&self, duration: u32, buffer: i64, from: u32, to: u32) -> f64 {
        let mut weight = f64::from(duration);
        let root = self.roots[from as usize];
        if root == self.roots[to as usize] && self.maximum != 0 {
            weight -= self.values[root] as f64 / self.maximum as f64 * 180.0;
        }
        weight -= 0.2 * buffer.clamp(0, 900) as f64;
        weight
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub(crate) struct OptimizationStats {
    pub(crate) edges_examined: u64,
    pub(crate) transfers_improved: u64,
}

#[derive(Clone, Copy)]
struct Ride {
    route: RouteIdx,
    transport: TransportIdx,
    service_day: u16,
    board_position: usize,
    alight_position: usize,
    from_stop: u32,
    to_stop: u32,
    departure: i64,
    arrival: i64,
}

fn ride(leg: &Leg) -> Option<Ride> {
    if let Leg::Ride {
        route,
        transport,
        service_day,
        board_stop_index,
        alight_stop_index,
        from_stop,
        to_stop,
        departure_unix_seconds,
        arrival_unix_seconds,
    } = *leg
    {
        Some(Ride {
            route: RouteIdx(route),
            transport: TransportIdx(transport),
            service_day,
            board_position: board_stop_index,
            alight_position: alight_stop_index,
            from_stop,
            to_stop,
            departure: departure_unix_seconds,
            arrival: arrival_unix_seconds,
        })
    } else {
        None
    }
}

/// Vehicle-change time runs in parallel with walking, not in addition to it.
/// A forbidden directed stop pair cannot be used even if OSR can walk it.
fn change_seconds(tt: &ZeroCopyTimetable<'_>, from: u32, to: u32) -> Option<u32> {
    let a = from as usize;
    let b = to as usize;
    let from_change = *tt.location_transfer_seconds().get(a)?;
    let to_change = *tt.location_transfer_seconds().get(b)?;
    let rule = tt.station_transfer_rule(LocationIdx(from), LocationIdx(to));
    if rule.is_some_and(|r| r.prohibited()) {
        return None;
    }
    Some(
        from_change
            .max(to_change)
            .max(rule.map_or(0, |r| r.min_seconds)),
    )
}

#[derive(Clone, Copy)]
struct Choice {
    from_stop: u32,
    to_stop: u32,
    from_position: usize,
    to_position: usize,
    arrival: i64,
    departure: i64,
    walking_seconds: u32,
}

fn best_transfer(
    tt: &ZeroCopyTimetable<'_>,
    importance: &StationImportance,
    previous: Ride,
    from_stop: u32,
    to_stop: u32,
    walking_seconds: u32,
    next: Ride,
    stats: &mut OptimizationStats,
) -> Option<Choice> {
    let first = tt.stops_on(previous.route);
    let second = tt.stops_on(next.route);
    if previous.board_position >= previous.alight_position
        || previous.alight_position >= first.len()
        || next.board_position >= next.alight_position
        || next.alight_position >= second.len()
        || first[previous.board_position].location().0 != previous.from_stop
        || first[previous.alight_position].location().0 != previous.to_stop
        || second[next.board_position].location().0 != next.from_stop
        || second[next.alight_position].location().0 != next.to_stop
        || from_stop != previous.to_stop
        || to_stop != next.from_stop
    {
        return None;
    }
    let prev_range = tt.route_transport_ranges().get(previous.route.idx())?;
    let next_range = tt.route_transport_ranges().get(next.route.idx())?;
    if previous.transport.0 < prev_range.from
        || previous.transport.0 >= prev_range.to
        || next.transport.0 < next_range.from
        || next.transport.0 >= next_range.to
    {
        return None;
    }

    let prev_midnight =
        (i64::from(tt.header().first_utc_day) + i64::from(previous.service_day)) * 86_400;
    let next_midnight =
        (i64::from(tt.header().first_utc_day) + i64::from(next.service_day)) * 86_400;

    // Explicitly validate the original events. A post-processor must never
    // "repair" corrupt or inconsistent reconstruction by changing trips.
    if transport_event_second(
        tt,
        previous.route,
        previous.transport,
        previous.alight_position,
        true,
        prev_midnight,
    ) != Some(previous.arrival)
        || transport_event_second(
            tt,
            next.route,
            next.transport,
            next.board_position,
            false,
            next_midnight,
        ) != Some(next.departure)
    {
        return None;
    }

    let original_buffer = change_seconds(tt, from_stop, to_stop).and_then(|change| {
        let gap = next.departure - previous.arrival;
        (gap >= i64::from(walking_seconds.max(change))).then_some(gap - i64::from(walking_seconds))
    });
    // Score the original transfer with its own endpoints as well. Upstream
    // passes the surrounding rides' outer endpoints for this baseline; using
    // the transfer endpoints makes the station reward consistent/idempotent.
    let mut penalty_best = original_buffer
        .map(|buffer| importance.penalty(walking_seconds, buffer, from_stop, to_stop))
        .unwrap_or(f64::INFINITY);
    let mut best = None;

    // fr_from.stop_range_ = {board + 1, size}: include later alighting
    // positions beyond the originally reconstructed exit, on the same run.
    for (from_position, stp_from) in first.iter().enumerate().skip(previous.board_position + 1) {
        if !stp_from.inbound() {
            // C++ out_allowed(): permission to alight
            continue;
        }
        // fr_to.stop_range_ = {0, alight}: include every possible entry
        // before the fixed exit, including earlier and later boarding stops.
        for (to_position, stp_to) in second.iter().enumerate().take(next.alight_position) {
            if !stp_to.outbound() {
                // C++ in_allowed(): permission to board
                continue;
            }
            for fp in tt.footpaths_from(stp_from.location()) {
                stats.edges_examined += 1;
                if fp.target() != stp_to.location() {
                    continue;
                }
                let from = stp_from.location().0;
                let to = stp_to.location().0;
                let arrival = transport_event_second(
                    tt,
                    previous.route,
                    previous.transport,
                    from_position,
                    true,
                    prev_midnight,
                )?;
                let departure = transport_event_second(
                    tt,
                    next.route,
                    next.transport,
                    to_position,
                    false,
                    next_midnight,
                )?;
                let walking = fp.seconds();
                // Preserve directed GTFS restrictions in addition to the
                // upstream arr + footpath_duration <= dep feasibility check.
                if let Some(change) = change_seconds(tt, from, to) {
                    let gap = departure - arrival;
                    if gap >= i64::from(walking.max(change)) {
                        let penalty =
                            importance.penalty(walking, gap - i64::from(walking), from, to);
                        if penalty < penalty_best {
                            // strict: keep original on a tie
                            penalty_best = penalty;
                            best = Some(Choice {
                                from_stop: from,
                                to_stop: to,
                                from_position,
                                to_position,
                                arrival,
                                departure,
                                walking_seconds: walking,
                            });
                        }
                    }
                }
                break; // first matching footpath, exactly as in C++
            }
        }
    }
    best
}

/// Re-link the walking connection between adjacent rides without changing
/// either scheduled vehicle, any service day, or journey departure/arrival.
/// Caller must run this before Pareto reduction and journey serialization.
pub(crate) fn optimize_internal_transfers(
    tt: &ZeroCopyTimetable<'_>,
    journey: &mut Journey,
) -> OptimizationStats {
    if !journey.legs.windows(3).any(|legs| {
        matches!(
            (&legs[0], &legs[1], &legs[2]),
            (Leg::Ride { .. }, Leg::Transfer { .. }, Leg::Ride { .. })
        )
    }) {
        return OptimizationStats::default();
    }
    let importance = StationImportance::build(tt);
    optimize_internal_transfers_with_importance(tt, &importance, journey)
}

pub(crate) fn optimize_internal_transfers_with_importance(
    tt: &ZeroCopyTimetable<'_>,
    importance: &StationImportance,
    journey: &mut Journey,
) -> OptimizationStats {
    let mut stats = OptimizationStats::default();
    if journey.legs.len() < 3 {
        return stats;
    }
    for i in 1..journey.legs.len() - 1 {
        let (
            Some(previous),
            Leg::Transfer {
                from_stop,
                to_stop,
                seconds,
            },
            Some(next),
        ) = (
            ride(&journey.legs[i - 1]),
            &journey.legs[i],
            ride(&journey.legs[i + 1]),
        )
        else {
            continue;
        };
        let Some(best) = best_transfer(
            tt, importance, previous, *from_stop, *to_stop, *seconds, next, &mut stats,
        ) else {
            continue;
        };
        if let Leg::Ride {
            to_stop,
            alight_stop_index,
            arrival_unix_seconds,
            ..
        } = &mut journey.legs[i - 1]
        {
            *to_stop = best.from_stop;
            *alight_stop_index = best.from_position;
            *arrival_unix_seconds = best.arrival;
        }
        if let Leg::Transfer {
            from_stop,
            to_stop,
            seconds,
        } = &mut journey.legs[i]
        {
            *from_stop = best.from_stop;
            *to_stop = best.to_stop;
            *seconds = best.walking_seconds;
        }
        if let Leg::Ride {
            from_stop,
            board_stop_index,
            departure_unix_seconds,
            ..
        } = &mut journey.legs[i + 1]
        {
            *from_stop = best.to_stop;
            *board_stop_index = best.to_position;
            *departure_unix_seconds = best.departure;
        }
        stats.transfers_improved += 1;
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::{
        BitfieldIdx, Delta, PackedFootpath, PackedStop, Range, StationTransferRule, TimetableOwned,
        TimingProfileIdx, TrafficDays,
    };

    fn fixture() -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 1);
        tt.add_source("test", "feed", "attempt").unwrap();
        for _ in 0..7 {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(Default::default());
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(120);
            tt.location_routes_offsets.push(0);
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
        }
        for i in 0..4 {
            tt.route_stops
                .push(PackedStop::new(LocationIdx(i), true, true, true, true).unwrap());
        }
        for i in 4..7 {
            tt.route_stops
                .push(PackedStop::new(LocationIdx(i), true, true, true, true).unwrap());
        }
        tt.route_stop_offsets.extend([4, 7]);
        tt.route_transport_ranges
            .extend([Range { from: 0, to: 1 }, Range { from: 1, to: 2 }]);
        tt.route_clasz.extend([10, 10]);
        tt.route_section_clasz_offsets.extend([0, 0]);
        tt.section_flag_offsets.extend([0, 0]);
        tt.transport_to_trip_section_offsets.extend([0, 0]);
        tt.timing_profile_events.extend(
            [0, 300, 300, 600, 600, 780, 0, 300, 300, 900]
                .into_iter()
                .map(|n| Delta::from_seconds(n).unwrap()),
        );
        tt.timing_profile_offsets.extend([6, 10]);
        tt.transport_route.extend([RouteIdx(0), RouteIdx(1)]);
        tt.transport_timing_profile
            .extend([TimingProfileIdx(0), TimingProfileIdx(1)]);
        tt.transport_first_dep_offset.extend([
            Delta::from_seconds(8 * 3600).unwrap(),
            Delta::from_seconds(8 * 3600 + 20 * 60).unwrap(),
        ]);
        tt.transport_initial_day_offset.extend([0, 0]);
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        tt.bitfields.push(days);
        tt.transport_traffic_days
            .extend([BitfieldIdx(1), BitfieldIdx(1)]);
        tt.route_traffic_days
            .extend([BitfieldIdx(1), BitfieldIdx(1)]);
        // 1 -> 4 is the long walk; 2 -> 5 is the 149-second shortcut.
        let edges = [
            vec![],
            vec![(4, 524)],
            vec![(5, 149)],
            vec![],
            vec![],
            vec![],
            vec![],
        ];
        let mut inbound = vec![Vec::new(); 7];
        for (from, outgoing) in edges.iter().enumerate() {
            for &(to, duration) in outgoing {
                tt.footpaths_out
                    .push(PackedFootpath::new(LocationIdx(to), duration).unwrap());
                inbound[to as usize].push((from as u32, duration));
            }
            tt.footpaths_out_offsets[from + 1] = tt.footpaths_out.len() as u32;
        }
        for (to, incoming) in inbound.into_iter().enumerate() {
            for (from, duration) in incoming {
                tt.footpaths_in
                    .push(PackedFootpath::new(LocationIdx(from), duration).unwrap());
            }
            tt.footpaths_in_offsets[to + 1] = tt.footpaths_in.len() as u32;
        }
        tt.location_routes_offsets = vec![0, 1, 2, 3, 4, 5, 6, 7];
        tt.location_routes = (0..7).map(|i| RouteIdx(u32::from(i >= 4))).collect();
        tt
    }

    fn journey() -> Journey {
        let midnight = 20_000_i64 * 86_400;
        Journey {
            boardings: 2,
            departure_unix_seconds: midnight + 8 * 3600,
            arrival_unix_seconds: midnight + 8 * 3600 + 35 * 60,
            legs: vec![
                Leg::Access {
                    to_stop: 0,
                    seconds: 0,
                },
                Leg::Ride {
                    from_stop: 0,
                    to_stop: 1,
                    route: 0,
                    transport: 0,
                    service_day: 0,
                    board_stop_index: 0,
                    alight_stop_index: 1,
                    departure_unix_seconds: midnight + 8 * 3600,
                    arrival_unix_seconds: midnight + 8 * 3600 + 300,
                },
                Leg::Transfer {
                    from_stop: 1,
                    to_stop: 4,
                    seconds: 524,
                },
                Leg::Ride {
                    from_stop: 4,
                    to_stop: 6,
                    route: 1,
                    transport: 1,
                    service_day: 0,
                    board_stop_index: 0,
                    alight_stop_index: 2,
                    departure_unix_seconds: midnight + 8 * 3600 + 20 * 60,
                    arrival_unix_seconds: midnight + 8 * 3600 + 35 * 60,
                },
                Leg::Egress {
                    from_stop: 6,
                    seconds: 0,
                },
            ],
        }
    }

    #[test]
    fn replaces_524_second_transfer_with_149_seconds_on_the_same_vehicles() {
        let bytes = fixture().encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        let before = (
            j.departure_unix_seconds,
            j.arrival_unix_seconds,
            j.boardings,
        );
        let stats = optimize_internal_transfers(&tt, &mut j);
        assert_eq!(stats.transfers_improved, 1);
        assert!(stats.edges_examined >= 2);
        assert_eq!(
            before,
            (
                j.departure_unix_seconds,
                j.arrival_unix_seconds,
                j.boardings
            )
        );
        assert!(matches!(
            j.legs[1],
            Leg::Ride {
                to_stop: 2,
                alight_stop_index: 2,
                transport: 0,
                ..
            }
        ));
        assert!(matches!(
            j.legs[2],
            Leg::Transfer {
                from_stop: 2,
                to_stop: 5,
                seconds: 149
            }
        ));
        assert!(matches!(
            j.legs[3],
            Leg::Ride {
                from_stop: 5,
                board_stop_index: 1,
                transport: 1,
                ..
            }
        ));
        // Idempotent; a second optimization must not oscillate the stop pair.
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
    }

    #[test]
    fn does_not_choose_short_walk_if_the_next_vehicle_has_already_left() {
        let mut owned = fixture();
        // Arrival at stop 2 at 08:24:00, short walk finishes after 08:25:00.
        owned.timing_profile_events[3] = Delta::from_seconds(24 * 60).unwrap();
        owned.timing_profile_events[4] = Delta::from_seconds(24 * 60).unwrap();
        owned.timing_profile_events[5] = Delta::from_seconds(25 * 60).unwrap();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
        assert!(matches!(j.legs[2], Leg::Transfer { seconds: 524, .. }));
    }

    #[test]
    fn respects_directional_gtfs_transfer_prohibitions() {
        let mut owned = fixture();
        owned.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(2),
            to: LocationIdx(5),
            min_seconds: 0,
            flags: 1,
        });
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
    }

    #[test]
    fn rejects_non_alighting_and_non_boarding_stops() {
        let mut owned = fixture();
        owned.route_stops[2] = PackedStop::new(LocationIdx(2), false, true, true, true).unwrap();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
        // Reinstate alighting; disallow boarding at the shorter target instead.
        let mut owned = fixture();
        owned.route_stops[5] = PackedStop::new(LocationIdx(5), true, false, true, true).unwrap();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
    }

    #[test]
    fn larger_minimum_change_time_can_rule_out_a_short_transfer() {
        let mut owned = fixture();
        owned.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(2),
            to: LocationIdx(5),
            min_seconds: 1000,
            flags: 0,
        });
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
    }

    #[test]
    fn importance_counts_active_days_and_aggregates_at_station_parent() {
        let mut owned = fixture();
        owned.location_parent[2] = LocationIdx(3);
        owned.location_parent[5] = LocationIdx(3);
        // Bus weight 2: one route at each of 2, 3 and 5, one active day.
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let importance = StationImportance::build(&tt);
        assert_eq!(importance.values[3], 6);
        assert_eq!(importance.maximum, 6);
        assert_eq!(importance.penalty(200, 0, 2, 5), 20.0);
        assert_eq!(importance.penalty(200, 0, 1, 4), 200.0);
        assert_eq!(importance.penalty(200, 900, 1, 4), 20.0);
        assert_eq!(importance.penalty(200, 1800, 1, 4), 20.0);

        // Twice the service days doubles the contribution. First-section
        // class takes precedence over the route's fallback class.
        owned.bitfields[1].set(1, true).unwrap();
        owned.route_section_clasz = vec![1]; // HighSpeed: weight 20
        owned.route_section_clasz_offsets = vec![0, 1, 1];
        owned.section_flag_offsets = vec![0, 1, 1];
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let importance = StationImportance::build(&tt);
        assert_eq!(importance.values[3], 84); // 40 + 40 + 4
        assert_eq!(importance.maximum, 84);
    }

    #[test]
    fn station_reward_can_prefer_a_longer_walk_and_remains_idempotent() {
        let mut owned = fixture();
        owned.location_parent[2] = LocationIdx(3);
        owned.location_parent[5] = LocationIdx(3);
        owned.footpaths_out[1] = PackedFootpath::new(LocationIdx(5), 600).unwrap();
        owned.footpaths_in[1] = PackedFootpath::new(LocationIdx(2), 600).unwrap();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            1
        );
        assert!(matches!(
            j.legs[2],
            Leg::Transfer {
                from_stop: 2,
                to_stop: 5,
                seconds: 600
            }
        ));
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
    }

    #[test]
    fn zero_importance_is_finite_and_negative_buffer_is_not_rewarded() {
        let mut owned = fixture();
        owned.bitfields[1] = TrafficDays::default();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let importance = StationImportance::build(&tt);
        assert_eq!(importance.maximum, 0);
        assert_eq!(importance.penalty(200, 0, 2, 2), 200.0);
        assert_eq!(importance.penalty(200, -10, 2, 2), 200.0);
    }

    #[test]
    fn equal_penalty_keeps_original_and_allows_boarding_at_exact_walk_arrival() {
        let mut owned = fixture();
        // Both transfer pairs have the same walk and buffer: keep original.
        owned.transport_first_dep_offset[1] = Delta::from_seconds(8 * 3600 + 1200).unwrap();
        owned.footpaths_out[1] = PackedFootpath::new(LocationIdx(5), 524).unwrap();
        owned.footpaths_in[1] = PackedFootpath::new(LocationIdx(2), 524).unwrap();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            0
        );
        assert!(matches!(
            j.legs[2],
            Leg::Transfer {
                from_stop: 1,
                to_stop: 4,
                ..
            }
        ));

        // Exact boundary at 08:25:00: arrival 08:22:31 plus 149 seconds.
        let mut owned = fixture();
        owned.timing_profile_events[3] = Delta::from_seconds(1351).unwrap();
        owned.timing_profile_events[4] = Delta::from_seconds(1351).unwrap();
        owned.timing_profile_events[5] = Delta::from_seconds(1400).unwrap();
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut j = journey();
        assert_eq!(
            optimize_internal_transfers(&tt, &mut j).transfers_improved,
            1
        );
        assert!(matches!(j.legs[2], Leg::Transfer { seconds: 149, .. }));
    }

    #[test]
    fn live_pong_uses_later_alighting_shortcut_in_both_query_directions() {
        use crate::routing_common::pong::route_pong_with_stats_precomputed;
        use crate::routing_common::profile_compact_reconstruct::valid_journey;
        use crate::routing_common::profile_search::ProfileOptions;
        use crate::routing_common::raptor_query::{Query, WalkingEndpoint};
        for walk in [149, 600] {
            let mut owned = fixture();
            if walk == 600 {
                owned.location_parent[2] = LocationIdx(3);
                owned.location_parent[5] = LocationIdx(3);
                owned.footpaths_out[1] = PackedFootpath::new(LocationIdx(5), walk).unwrap();
                owned.footpaths_in[1] = PackedFootpath::new(LocationIdx(2), walk).unwrap();
            }
            let bytes = owned.encode().unwrap();
            let tt = ZeroCopyTimetable::new(&bytes).unwrap();
            let base = i64::from(tt.header().first_utc_day) * 86_400;
            for arrive_by in [false, true] {
                let q = Query {
                    departure_unix_seconds: base + if arrive_by { 9 * 3600 } else { 8 * 3600 - 60 },
                    access: vec![WalkingEndpoint {
                        stop: LocationIdx(0),
                        seconds: 0,
                    }],
                    egress: vec![WalkingEndpoint {
                        stop: LocationIdx(6),
                        seconds: 0,
                    }],
                    direct_walk_seconds: None,
                    max_boardings: 2,
                    max_duration_seconds: 3600,
                };
                let (journeys, _) = route_pong_with_stats_precomputed(
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
                assert_eq!(journeys.len(), 1, "arrive_by={arrive_by}");
                let j = &journeys[0];
                assert!(valid_journey(&tt, &q, j));
                assert_eq!(
                    (
                        j.departure_unix_seconds,
                        j.arrival_unix_seconds,
                        j.boardings
                    ),
                    (base + 8 * 3600, base + 8 * 3600 + 35 * 60, 2)
                );
                assert!(
                    matches!(
                        j.legs[1],
                        Leg::Ride {
                            to_stop: 2,
                            alight_stop_index: 2,
                            transport: 0,
                            ..
                        }
                    ),
                    "{j:?}"
                );
                assert!(
                    matches!(
                        j.legs[2],
                        Leg::Transfer {
                            from_stop: 2,
                            to_stop: 5,
                            seconds
                        } if seconds == walk
                    ),
                    "{j:?}"
                );
                assert!(
                    matches!(
                        j.legs[3],
                        Leg::Ride {
                            from_stop: 5,
                            board_stop_index: 1,
                            transport: 1,
                            ..
                        }
                    ),
                    "{j:?}"
                );
            }
        }
    }
}
