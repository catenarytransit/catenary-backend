//! Deferred PING/PONG reconstruction with a pinned realtime region snapshot.
//! Adapted from Nigiri's src/routing/raptor/reconstruct.cc (MIT; third_party/nigiri-LICENSE).
//!
//! The search keeps scalar best-time labels rather than predecessor handles.
//! Reconstruction therefore enumerates feasible run instances: realtime
//! candidates first, unsuppressed static candidates second. Every successive
//! ride is checked against the *actual* event of the adjacent selected ride,
//! and an infeasible candidate is backtracked instead of failing the journey.

use crate::routing_common::nigiri_raptor::{
    NigiriRaptor, PointJourney, better, shift, sorted_offsets, transfer_duration,
};
use crate::routing_common::profile_compact_reconstruct::valid_journey_with_realtime;
use crate::routing_common::raptor_query::{Journey, Leg, Query, RtRideRef, transport_event_second};
use crate::routing_common::realtime_overlay::RtPatternRef;
use crate::routing_common::timetable::{DayIdx, LocationIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, anyhow, ensure};

/// A bounded DFS is only used when a scalar predecessor choice cannot be
/// made consistent with the other legs. The normal case takes the first run.
const MAX_RECONSTRUCTION_CANDIDATES: usize = 8_192;
const DAY: i64 = 86_400;

/// Nigiri accepts earlier arrivals (or later departures in reverse searches)
/// when the round label came from another candidate. Only the terminal event
/// anchoring PONG's reported destination time must match exactly.
fn finish_matches(actual: i64, limit: i64, reverse: bool, exact: bool) -> bool {
    if exact {
        actual == limit
    } else if reverse {
        actual >= limit
    } else {
        actual <= limit
    }
}

/// Enumerate the realtime transport-days before visiting any static runs.
/// A missing event or unusable run skips just that candidate, never the
/// whole region, and the run identity stays attached to the reconstructed leg.
fn visit_realtime_transports(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    k: usize,
    location: usize,
    limit: i64,
    exact: bool,
    visit: &mut impl FnMut(Leg) -> bool,
) -> bool {
    let Some(snapshot) = state.realtime.as_deref() else {
        return false;
    };
    if !snapshot.is_published() {
        return false;
    }
    let reverse = state.reverse;
    for &pattern in snapshot.patterns_at(LocationIdx(location as u32)) {
        let Some(stops) = snapshot.stops(tt, pattern) else {
            continue;
        };
        for handle in snapshot.run_handles_for(pattern) {
            if !crate::routing_common::mode_filter::allows_realtime_run(tt, snapshot, handle, state.allowed_modes) {
                continue;
            }
            let Some(run) = snapshot.run(handle) else {
                continue;
            };
            if run.pattern != pattern.pattern {
                continue;
            }
            for (finish, stop) in stops.iter().enumerate() {
                if stop.location().idx() != location
                    || !(if reverse {
                        finish + 1 < stops.len() && stop.outbound()
                    } else {
                        finish > 0 && stop.inbound()
                    })
                {
                    continue;
                }
                let finish_slot = if reverse { finish * 2 } else { finish * 2 - 1 };
                let Some(finish_time) = snapshot.run_event_time(handle, finish_slot) else {
                    continue;
                };
                if !finish_matches(finish_time, limit, reverse, exact) {
                    continue;
                }
                let count = if reverse {
                    stops.len() - finish - 1
                } else {
                    finish
                };
                for distance in 1..=count {
                    let start = if reverse {
                        finish + distance
                    } else {
                        finish - distance
                    };
                    let entry = stops[start];
                    if !(if reverse {
                        entry.inbound()
                    } else {
                        entry.outbound()
                    }) {
                        continue;
                    }
                    let start_slot = if reverse { start * 2 - 1 } else { start * 2 };
                    let Some(start_time) = snapshot.run_event_time(handle, start_slot) else {
                        continue;
                    };
                    let ready = state.round_times_[k - 1][entry.location().idx()];
                    if ready == state.invalid() || better(start_time, state.to_unix(ready), reverse)
                    {
                        continue;
                    }
                    let (from, to, board, alight, departure, arrival) = if reverse {
                        (
                            location,
                            entry.location().idx(),
                            finish,
                            start,
                            finish_time,
                            start_time,
                        )
                    } else {
                        (
                            entry.location().idx(),
                            location,
                            start,
                            finish,
                            start_time,
                            finish_time,
                        )
                    };
                    if arrival < departure {
                        continue;
                    }
                    let original = run.original;
                    let route = match run.pattern {
                        RtPatternRef::Static(route) => route,
                        RtPatternRef::Dynamic(_) => original
                            .and_then(|o| {
                                tt.transport_route().get(o.transport as usize).map(|r| r.0)
                            })
                            .unwrap_or(u32::MAX),
                    };
                    if visit(Leg::Ride {
                        from_stop: from as u32,
                        to_stop: to as u32,
                        route,
                        transport: original.map_or(u32::MAX, |o| o.transport),
                        service_day: original.map_or(0, |o| o.day),
                        board_stop_index: board,
                        alight_stop_index: alight,
                        departure_unix_seconds: departure,
                        arrival_unix_seconds: arrival,
                        realtime: Some(RtRideRef {
                            publication_seq: snapshot.publication_seq,
                            run: handle,
                            pattern,
                        }),
                    }) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Static events are checked only after all usable realtime candidates.
/// For nonterminal transfers we cannot derive the traffic day by exact
/// equality with the scalar round label: the actual event may be earlier
/// (later for reverse). Examine only days within the query's travel horizon.
fn visit_static_transports(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    k: usize,
    location: usize,
    limit: i64,
    exact: bool,
    visit: &mut impl FnMut(Leg) -> bool,
) -> bool {
    let reverse = state.reverse;
    let base = i64::from(tt.header().first_utc_day) * DAY;
    for &route in tt.routes_at(LocationIdx(location as u32)) {
        if !crate::routing_common::mode_filter::allows_route(tt, route, state.allowed_modes) {
            continue;
        }
        let stops = tt.stops_on(route);
        for (finish, stop) in stops.iter().enumerate() {
            if stop.location().idx() != location
                || !(if reverse {
                    finish + 1 < stops.len() && stop.outbound()
                } else {
                    finish > 0 && stop.inbound()
                })
            {
                continue;
            }
            let finish_slot = if reverse { finish * 2 } else { finish * 2 - 1 };
            let range = tt.route_transport_ranges()[route.idx()];
            for transport_number in range.from..range.to {
                let transport = TransportIdx(transport_number);
                let Some(events) = tt.events_for_transport(transport) else {
                    continue;
                };
                let Some(finish_event) = events.get(finish_slot) else {
                    continue;
                };
                let offset = i64::from(tt.transport_first_dep_offset()[transport.idx()].seconds())
                    + i64::from(finish_event.seconds());
                let delta = limit - base - offset;
                let (first_day, last_day) = if exact {
                    if delta.rem_euclid(DAY) != 0 {
                        continue;
                    }
                    (delta.div_euclid(DAY), delta.div_euclid(DAY))
                } else if reverse {
                    (
                        delta.div_euclid(DAY),
                        delta
                            .saturating_add(i64::from(q.max_duration_seconds))
                            .div_euclid(DAY),
                    )
                } else {
                    (
                        delta
                            .saturating_sub(i64::from(q.max_duration_seconds))
                            .div_euclid(DAY),
                        delta.div_euclid(DAY),
                    )
                };
                let first_day = first_day.max(0);
                let last_day = last_day.min(i64::from(tt.header().day_count) - 1);
                for day in first_day..=last_day {
                    let day_index = DayIdx(day as u16);
                    if !tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()]
                        .test(day as usize)
                        || state
                            .realtime
                            .as_ref()
                            .is_some_and(|rt| rt.is_static_suppressed(transport, day_index))
                    {
                        continue;
                    }
                    let midnight = base + day * DAY;
                    let finish_time = midnight + offset;
                    if !finish_matches(finish_time, limit, reverse, exact) {
                        continue;
                    }
                    let count = if reverse {
                        stops.len() - finish - 1
                    } else {
                        finish
                    };
                    for distance in 1..=count {
                        let start = if reverse {
                            finish + distance
                        } else {
                            finish - distance
                        };
                        let entry = stops[start];
                        if !(if reverse {
                            entry.inbound()
                        } else {
                            entry.outbound()
                        }) {
                            continue;
                        }
                        let Some(start_time) =
                            transport_event_second(tt, route, transport, start, reverse, midnight)
                        else {
                            continue;
                        };
                        let ready = state.round_times_[k - 1][entry.location().idx()];
                        if ready == state.invalid()
                            || better(start_time, state.to_unix(ready), reverse)
                        {
                            continue;
                        }
                        let (from, to, board, alight, departure, arrival) = if reverse {
                            (
                                location,
                                entry.location().idx(),
                                finish,
                                start,
                                finish_time,
                                start_time,
                            )
                        } else {
                            (
                                entry.location().idx(),
                                location,
                                start,
                                finish,
                                start_time,
                                finish_time,
                            )
                        };
                        if arrival < departure {
                            continue;
                        }
                        if visit(Leg::Ride {
                            from_stop: from as u32,
                            to_stop: to as u32,
                            route: route.0,
                            transport: transport_number,
                            service_day: day as u16,
                            board_stop_index: board,
                            alight_stop_index: alight,
                            departure_unix_seconds: departure,
                            arrival_unix_seconds: arrival,
                            realtime: None,
                        }) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

fn visit_transports(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    k: usize,
    location: usize,
    limit: i64,
    exact: bool,
    visit: &mut impl FnMut(Leg) -> bool,
) -> bool {
    visit_realtime_transports(state, tt, k, location, limit, exact, visit)
        || visit_static_transports(state, tt, q, k, location, limit, exact, visit)
}

/// The search round's label is an optimistic best time, not proof that a
/// specific predecessor connects to the ride selected one step later.
/// During reconstruction use the selected *actual* departure/arrival as the
/// cutoff for the preceding/succeeding ride, including GTFS transfer time.
fn visit_legs(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    point: PointJourney,
    k: usize,
    location: usize,
    adjacent: Option<&Leg>,
    visit: &mut impl FnMut(Leg, Leg) -> bool,
) -> bool {
    let reverse = state.reverse;
    let current_time = state.to_unix(state.round_times_[k][location]);
    if k == point.boardings {
        let ends = sorted_offsets(if reverse { &q.access } else { &q.egress });
        for ep in ends {
            let event = shift(current_time, ep.seconds, !reverse);
            let walk = if reverse {
                Leg::Access {
                    to_stop: ep.stop.0,
                    seconds: ep.seconds,
                }
            } else {
                Leg::Egress {
                    from_stop: ep.stop.0,
                    seconds: ep.seconds,
                }
            };
            if visit_transports(state, tt, q, k, ep.stop.idx(), event, true, &mut |ride| {
                visit(walk.clone(), ride)
            }) {
                return true;
            }
        }
        return false;
    }

    let mut try_transfer =
        |from: usize, to: usize, target: usize, raw_seconds: u32, effective_seconds: u32| -> bool {
            let Some(Leg::Ride {
                from_stop: adjacent_from,
                to_stop: adjacent_to,
                departure_unix_seconds: adjacent_dep,
                arrival_unix_seconds: adjacent_arr,
                ..
            }) = adjacent
            else {
                return false;
            };
            if (!reverse && *adjacent_from != to as u32) || (reverse && *adjacent_to != from as u32)
            {
                return false;
            }
            let cutoff = if reverse {
                adjacent_arr.saturating_add(i64::from(effective_seconds))
            } else {
                adjacent_dep.saturating_sub(i64::from(effective_seconds))
            };
            let walk = Leg::Transfer {
                from_stop: from as u32,
                to_stop: to as u32,
                seconds: raw_seconds,
            };
            visit_transports(state, tt, q, k, target, cutoff, false, &mut |ride| {
                visit(walk.clone(), ride)
            })
        };

    if let Some(duration) = transfer_duration(tt, location, location, 0) {
        if try_transfer(location, location, location, duration, duration) {
            return true;
        }
    }
    let paths = if reverse {
        tt.footpaths_from(LocationIdx(location as u32))
    } else {
        tt.footpaths_to(LocationIdx(location as u32))
    };
    for fp in paths {
        let other = fp.target().idx();
        let (from, to) = if reverse {
            (location, other)
        } else {
            (other, location)
        };
        let Some(duration) = transfer_duration(tt, from, to, fp.seconds()) else {
            continue;
        };
        if try_transfer(from, to, other, fp.seconds(), duration) {
            return true;
        }
    }
    false
}

fn complete_journey(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    point: PointJourney,
    legs_backwards: &[Leg],
) -> Option<Journey> {
    let Leg::Ride {
        from_stop,
        to_stop,
        departure_unix_seconds,
        arrival_unix_seconds,
        ..
    } = legs_backwards.last()?
    else {
        return None;
    };
    let location = if state.reverse { *to_stop } else { *from_stop };
    let first_event = if state.reverse {
        *arrival_unix_seconds
    } else {
        *departure_unix_seconds
    };
    let starts = sorted_offsets(if state.reverse { &q.egress } else { &q.access });
    for ep in starts {
        if ep.stop.0 != location
            || better(
                shift(first_event, ep.seconds, !state.reverse),
                point.start_time,
                state.reverse,
            )
        {
            continue;
        }
        let mut legs = legs_backwards.to_vec();
        legs.push(if state.reverse {
            Leg::Egress {
                from_stop: location,
                seconds: ep.seconds,
            }
        } else {
            Leg::Access {
                to_stop: location,
                seconds: ep.seconds,
            }
        });
        if !state.reverse {
            legs.reverse();
        }
        let mut journey = Journey {
            departure_unix_seconds: if state.reverse {
                point.dest_time
            } else {
                point.start_time
            },
            arrival_unix_seconds: if state.reverse {
                point.start_time
            } else {
                point.dest_time
            },
            boardings: point.boardings,
            legs,
        };
        if !valid_journey_with_realtime(tt, q, &journey, state.realtime.as_deref()) {
            continue;
        }
        // Nigiri finalizes footpaths only after the complete run sequence is
        // feasible. The legacy optimizer is static-only: never retime RT runs.
        if !journey.legs.iter().any(|leg| {
            matches!(
                leg,
                Leg::Ride {
                    realtime: Some(_),
                    ..
                }
            )
        }) {
            let mut optimized = journey.clone();
            crate::routing_common::transfer_optimizer::optimize_internal_transfers_with_importance(
                tt,
                &state.importance,
                &mut optimized,
            );
            if valid_journey_with_realtime(tt, q, &optimized, state.realtime.as_deref()) {
                journey = optimized;
            }
        }
        return Some(journey);
    }
    None
}

/// Enumerate alternative predecessors only on a failed branch. This matters
/// when a delayed first ride no longer meets the departure of the next ride:
/// the earliest/most recent scalar round label is not a valid run identity.
fn search_chain(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    point: PointJourney,
    k: usize,
    location: usize,
    legs_backwards: &mut Vec<Leg>,
    candidates: &mut usize,
    answer: &mut Option<Journey>,
) -> bool {
    if *candidates >= MAX_RECONSTRUCTION_CANDIDATES {
        return true; // Stop traversing; caller reports exhaustion, not an invalid journey.
    }
    let adjacent = legs_backwards.last().cloned();
    visit_legs(
        state,
        tt,
        q,
        point,
        k,
        location,
        adjacent.as_ref(),
        &mut |walk, ride| {
            *candidates += 1;
            if *candidates > MAX_RECONSTRUCTION_CANDIDATES {
                return true;
            }
            let next_location = match &ride {
                Leg::Ride {
                    from_stop, to_stop, ..
                } => {
                    if state.reverse {
                        *to_stop as usize
                    } else {
                        *from_stop as usize
                    }
                }
                _ => unreachable!(),
            };
            legs_backwards.push(walk);
            legs_backwards.push(ride);
            if k == 1 {
                *answer = complete_journey(state, tt, q, point, legs_backwards);
            } else {
                search_chain(
                    state,
                    tt,
                    q,
                    point,
                    k - 1,
                    next_location,
                    legs_backwards,
                    candidates,
                    answer,
                );
            }
            legs_backwards.pop();
            legs_backwards.pop();
            answer.is_some() || *candidates >= MAX_RECONSTRUCTION_CANDIDATES
        },
    )
}

pub(crate) fn reconstruct(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    point: PointJourney,
) -> Result<Journey> {
    ensure!(
        state.to_unix(state.round_times_[point.boardings][state.target]) == point.dest_time,
        "selected Nigiri destination does not match retained round labels"
    );
    let mut legs = Vec::with_capacity(point.boardings * 2);
    let mut candidates = 0;
    let mut journey = None;
    search_chain(
        state,
        tt,
        q,
        point,
        point.boardings,
        state.target,
        &mut legs,
        &mut candidates,
        &mut journey,
    );
    journey.ok_or_else(|| {
        anyhow!(
            "deferred Nigiri reconstruction found no transfer-feasible realtime/static run chain \
         (boardings={}, target={}, reverse={}, examined={}, snapshot={})",
            point.boardings,
            state.target,
            state.reverse,
            candidates,
            state.realtime.as_ref().map_or(0, |s| s.publication_seq),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::finish_matches;

    #[test]
    fn nigiri_event_match_allows_earlier_rt_arrival_and_later_reverse_departure() {
        assert!(finish_matches(10_000, 10_100, false, false));
        assert!(!finish_matches(10_200, 10_100, false, false));
        assert!(finish_matches(10_200, 10_100, true, false));
        assert!(!finish_matches(10_000, 10_100, true, false));
        assert!(finish_matches(10_100, 10_100, false, true));
        assert!(!finish_matches(10_000, 10_100, false, true));
        assert!(!finish_matches(10_200, 10_100, true, true));
    }
}
