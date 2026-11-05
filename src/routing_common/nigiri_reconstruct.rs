//! Deferred static/no-via reconstruction from Nigiri 82e55ced,
//! src/routing/raptor/reconstruct.cc. MIT: third_party/nigiri-LICENSE.
//! Walk round labels backwards through timetable events; no parent pointers.
use crate::routing_common::nigiri_raptor::{
    NigiriRaptor, PointJourney, better, shift, sorted_offsets, transfer_duration,
};
use crate::routing_common::profile_compact_reconstruct::valid_journey;
use crate::routing_common::raptor_query::{Journey, Leg, Query, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, anyhow, ensure};

fn find_transport(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    k: usize,
    location: usize,
    time: i64,
) -> Option<Leg> {
    let reverse = state.reverse;
    let base = i64::from(tt.header().first_utc_day) * 86_400;
    for &route in tt.routes_at(LocationIdx(location as u32)) {
        let stops = tt.stops_on(route);
        for (finish, stop) in stops.iter().enumerate() {
            if stop.location().idx() != location {
                continue;
            }
            let finish_allowed = if reverse {
                finish + 1 < stops.len() && stop.outbound()
            } else {
                finish > 0 && stop.inbound()
            };
            if !finish_allowed {
                continue;
            }
            let slot = if reverse { finish * 2 } else { finish * 2 - 1 };
            let range = tt.route_transport_ranges()[route.idx()];
            for transport in range.from..range.to {
                let transport = TransportIdx(transport);
                let events = tt.events_for_transport(transport)?;
                let offset = i64::from(tt.transport_first_dep_offset()[transport.idx()].seconds())
                    + i64::from(events[slot].seconds());
                let delta = time - base - offset;
                if delta.rem_euclid(86_400) != 0 {
                    continue;
                }
                let day = delta.div_euclid(86_400);
                if day < 0
                    || day >= i64::from(tt.header().day_count)
                    || !tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()]
                        .test(day as usize)
                {
                    continue;
                }
                let midnight = base + day * 86_400;
                // find_start_in_prev_round: nearest feasible earlier/later
                // stop, in the opposite direction to the search.
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
                    let stop = stops[start];
                    if !(if reverse {
                        stop.inbound()
                    } else {
                        stop.outbound()
                    }) {
                        continue;
                    }
                    let event =
                        transport_event_second(tt, route, transport, start, reverse, midnight)?;
                    let ready = state.round_times_[k - 1][stop.location().idx()];
                    if ready == state.invalid() || better(event, state.to_unix(ready), reverse) {
                        continue;
                    }
                    let (from, to, board, alight, departure, arrival) = if reverse {
                        (location, stop.location().idx(), finish, start, time, event)
                    } else {
                        (stop.location().idx(), location, start, finish, event, time)
                    };
                    return Some(Leg::Ride {
                        from_stop: from as u32,
                        to_stop: to as u32,
                        route: route.0,
                        transport: transport.0,
                        service_day: day as u16,
                        board_stop_index: board,
                        alight_stop_index: alight,
                        departure_unix_seconds: departure,
                        arrival_unix_seconds: arrival,
                    });
                }
            }
        }
    }
    None
}

fn get_legs(
    state: &NigiriRaptor,
    tt: &ZeroCopyTimetable<'_>,
    q: &Query,
    point: PointJourney,
    k: usize,
    location: usize,
) -> Option<(Leg, Leg)> {
    let current_time = state.to_unix(state.round_times_[k][location]);
    let reverse = state.reverse;
    if k == point.boardings {
        let ends = sorted_offsets(if reverse { &q.access } else { &q.egress });
        for ep in ends {
            let event = shift(current_time, ep.seconds, !reverse);
            if let Some(ride) = find_transport(state, tt, k, ep.stop.idx(), event) {
                let offset = if reverse {
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
                return Some((offset, ride));
            }
        }
        return None;
    }
    // get_legs checks a reflexive station transfer before ordinary footpaths.
    if let Some(duration) = transfer_duration(tt, location, location, 0) {
        let event = shift(current_time, duration, !reverse);
        if let Some(ride) = find_transport(state, tt, k, location, event) {
            return Some((
                Leg::Transfer {
                    from_stop: location as u32,
                    to_stop: location as u32,
                    seconds: duration,
                },
                ride,
            ));
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
        let event = shift(current_time, duration, !reverse);
        if let Some(ride) = find_transport(state, tt, k, other, event) {
            return Some((
                Leg::Transfer {
                    from_stop: from as u32,
                    to_stop: to as u32,
                    seconds: fp.seconds(),
                },
                ride,
            ));
        }
    }
    None
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
    let mut location = state.target;
    let mut legs = Vec::new();
    for k in (1..=point.boardings).rev() {
        let (walk, ride) = get_legs(state, tt, q, point, k, location).ok_or_else(|| {
            anyhow!("Nigiri reconstruction failed at boardings={k}, stop={location}")
        })?;
        location = match &ride {
            Leg::Ride {
                from_stop, to_stop, ..
            } => (if state.reverse { *to_stop } else { *from_stop }) as usize,
            _ => unreachable!(),
        };
        legs.push(walk);
        legs.push(ride);
    }
    let first_event = match legs.last().unwrap() {
        Leg::Ride {
            departure_unix_seconds,
            arrival_unix_seconds,
            ..
        } => {
            if state.reverse {
                *arrival_unix_seconds
            } else {
                *departure_unix_seconds
            }
        }
        _ => unreachable!(),
    };
    let starts = sorted_offsets(if state.reverse { &q.egress } else { &q.access });
    let start = starts
        .into_iter()
        .find(|ep| {
            ep.stop.idx() == location
                && !better(
                    shift(first_event, ep.seconds, !state.reverse),
                    point.start_time,
                    state.reverse,
                )
        })
        .ok_or_else(|| anyhow!("Nigiri reconstruction cannot match start offset"))?;
    legs.push(if state.reverse {
        Leg::Egress {
            from_stop: location as u32,
            seconds: start.seconds,
        }
    } else {
        Leg::Access {
            to_stop: location as u32,
            seconds: start.seconds,
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
    ensure!(
        valid_journey(tt, q, &journey),
        "deferred Nigiri journey violates timetable or GTFS transfer rules"
    );
    // reconstruct.cc calls optimize_footpaths after legs are in passenger
    // order for either search direction. Internal transfers leave PONG's
    // departure/arrival keys untouched and retain both scheduled vehicles.
    crate::routing_common::transfer_optimizer::optimize_internal_transfers_with_importance(
        tt, &state.importance, &mut journey,
    );
    ensure!(
        valid_journey(tt, q, &journey),
        "optimized Nigiri journey violates timetable or GTFS transfer rules"
    );
    Ok(journey)
}
