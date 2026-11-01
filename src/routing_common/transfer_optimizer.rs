//! Optimize inter-vehicle footpaths after RAPTOR reconstruction.
//!
//! The timetable search keeps its Pareto criteria (departure, arrival, boardings).
//! Once the *same two scheduled vehicles* have been chosen, nearby alighting and
//! boarding positions can be re-linked through a shorter legal directed footpath.
//! This is Nigiri's optimize_transfers idea, kept outside the hot route scan.
//!
//! Complexity per transfer is O(stops on the first vehicle + stops on the second
//! vehicle + outgoing edges at eligible alighting stops + matching target pairs).
//! No stop-pair Cartesian product, additional RAPTOR labels, or OSR query is used.

use crate::routing_common::raptor_query::{Journey, Leg, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use std::collections::HashMap;

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

/// In units of fifths of a second: walking duration minus a bounded 20%
/// reward for remaining connection buffer. Keep original when exactly tied.
/// Avoid floating point, and don't let huge waits overpower a shorter walk.
fn rank(walk: u32, buffer: i64) -> (i64, u32, i64) {
    (5 * i64::from(walk) - buffer.clamp(0, 900), walk, -buffer)
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

    let original_buffer = change_seconds(tt, from_stop, to_stop).map(|change| {
        next.departure
            .saturating_sub(previous.arrival)
            .saturating_sub(i64::from(walking_seconds.max(change)))
    });
    let mut best_rank = original_buffer
        .filter(|&b| b >= 0)
        .map(|b| rank(walking_seconds, b))
        .unwrap_or((i64::MAX, u32::MAX, i64::MAX));
    let mut best = None;

    // Include all earlier boarding positions on the *same next vehicle*.
    // Repeated GTFS locations keep their distinct stop-sequence indices.
    let mut boarding_by_location: HashMap<u32, Vec<(usize, i64)>> = HashMap::new();
    for (pos, stop) in second.iter().enumerate().take(next.alight_position) {
        if !stop.outbound() {
            continue;
        }
        let Some(departure) =
            transport_event_second(tt, next.route, next.transport, pos, false, next_midnight)
        else {
            continue;
        };
        boarding_by_location
            .entry(stop.location().0)
            .or_default()
            .push((pos, departure));
    }

    // A single adjacency lookup per alighting position replaces Nigiri's
    // nested stop-position scan, which matters for long-distance services.
    for (pos, stop) in first.iter().enumerate().skip(previous.board_position + 1) {
        if !stop.inbound() {
            continue;
        }
        let Some(arrival) = transport_event_second(
            tt,
            previous.route,
            previous.transport,
            pos,
            true,
            prev_midnight,
        ) else {
            continue;
        };
        for edge in tt.footpaths_from(stop.location()) {
            stats.edges_examined += 1;
            let Some(boardings) = boarding_by_location.get(&edge.target().0) else {
                continue;
            };
            let Some(change) = change_seconds(tt, stop.location().0, edge.target().0) else {
                continue;
            };
            let walking = edge.seconds();
            if walking == 0 {
                continue;
            }
            for &(to_position, departure) in boardings {
                let buffer = departure
                    .saturating_sub(arrival)
                    .saturating_sub(i64::from(walking.max(change)));
                if buffer < 0 {
                    continue;
                }
                let candidate_rank = rank(walking, buffer);
                if candidate_rank < best_rank {
                    best_rank = candidate_rank;
                    best = Some(Choice {
                        from_stop: stop.location().0,
                        to_stop: edge.target().0,
                        from_position: pos,
                        to_position,
                        arrival,
                        departure,
                        walking_seconds: walking,
                    });
                }
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
            tt, previous, *from_stop, *to_stop, *seconds, next, &mut stats,
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
            [0, 300, 600, 780, 0, 300, 900]
                .into_iter()
                .map(|n| Delta::from_seconds(n).unwrap()),
        );
        tt.timing_profile_offsets.extend([4, 7]);
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
            tt.footpaths_out_offsets[from] = tt.footpaths_out.len() as u32;
        }
        for (to, incoming) in inbound.into_iter().enumerate() {
            for (from, duration) in incoming {
                tt.footpaths_in
                    .push(PackedFootpath::new(LocationIdx(from), duration).unwrap());
            }
            tt.footpaths_in_offsets[to] = tt.footpaths_in.len() as u32;
        }
        tt.location_routes_offsets = vec![1, 2, 3, 4, 5, 6, 7];
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
        owned.timing_profile_events[2] = Delta::from_seconds(24 * 60).unwrap();
        owned.timing_profile_events[3] = Delta::from_seconds(25 * 60).unwrap();
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
            min_seconds: 800,
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
}
