//! Deferred reconstruction for Nigiri-style compact RAPTOR rounds.
//!
//! Every cell contains an arrival/boarding-ready time and one predecessor
//! index. Route scanning never allocates a node for a dominated alternative.
use crate::routing_common::profile_compact::{Cell, Parent, Round, Stage};
use crate::routing_common::raptor_query::{Journey, Leg, Query, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};

pub(super) fn reconstruct(
    rounds: &[Round],
    parents: &[Parent],
    generation: u32,
    arrive_by: bool,
    mut k: usize,
    mut at: usize,
    mut stage: Stage,
    last_seconds: u32,
) -> Option<Journey> {
    let terminal = rounds.get(k)?.get(stage, at, generation)?.time;
    let mut legs = vec![if arrive_by {
        Leg::Access { to_stop: at as u32, seconds: last_seconds }
    } else {
        Leg::Egress { from_stop: at as u32, seconds: last_seconds }
    }];
    // One ride and one transfer per round, plus a ready-state transition.
    for _ in 0..(rounds.len() * 4 + 2) {
        let Cell { parent, .. } = *rounds.get(k)?.get(stage, at, generation)?;
        match *parents.get(parent as usize)? {
            Parent::Root { seconds } if stage == Stage::Ready && k == 0 => {
                legs.push(if arrive_by {
                    Leg::Egress { from_stop: at as u32, seconds }
                } else {
                    Leg::Access { to_stop: at as u32, seconds }
                });
                if !arrive_by { legs.reverse(); }
                let departure = if arrive_by {
                    terminal.checked_sub(i64::from(last_seconds))?
                } else {
                    let first_boarding = legs.iter().find_map(|leg| match leg {
                        Leg::Ride { departure_unix_seconds, .. } =>
                            Some(*departure_unix_seconds),
                        _ => None,
                    })?;
                    let access = match legs.first()? {
                        Leg::Access { seconds, .. } => i64::from(*seconds),
                        _ => return None,
                    };
                    first_boarding.checked_sub(access)?
                };
                let mut time = departure;
                let mut stop = None;
                for leg in &legs {
                    match leg {
                        Leg::Access { to_stop, seconds } => {
                            stop = Some(*to_stop);
                            time = time.checked_add(i64::from(*seconds))?;
                        }
                        Leg::Transfer { from_stop, to_stop, seconds } => {
                            if stop != Some(*from_stop) { return None; }
                            stop = Some(*to_stop);
                            time = time.checked_add(i64::from(*seconds))?;
                        }
                        Leg::Ride { from_stop, to_stop, departure_unix_seconds,
                                    arrival_unix_seconds, .. } => {
                            if stop != Some(*from_stop)
                                || time > *departure_unix_seconds
                                || arrival_unix_seconds < departure_unix_seconds
                            { return None; }
                            stop = Some(*to_stop);
                            time = *arrival_unix_seconds;
                        }
                        Leg::Egress { from_stop, seconds } => {
                            if stop != Some(*from_stop) { return None; }
                            time = time.checked_add(i64::from(*seconds))?;
                        }
                        Leg::DirectWalk { .. } => return None,
                    }
                }
                let boardings = legs.iter()
                    .filter(|leg| matches!(leg, Leg::Ride { .. })).count();
                return (boardings > 0).then_some(Journey {
                    departure_unix_seconds: departure,
                    arrival_unix_seconds: time,
                    boardings,
                    legs,
                });
            }
            Parent::Ride { from, to, route, transport, day,
                           board_pos, alight_pos, departure, arrival }
                if stage == Stage::Ride && k > 0 =>
            {
                legs.push(Leg::Ride {
                    from_stop: from as u32,
                    to_stop: to as u32,
                    route,
                    transport,
                    service_day: day,
                    board_stop_index: board_pos,
                    alight_stop_index: alight_pos,
                    departure_unix_seconds: departure,
                    arrival_unix_seconds: arrival,
                });
                at = if arrive_by { to } else { from };
                stage = Stage::Ready;
                k -= 1;
            }
            Parent::Walk { from, to, seconds }
                if (stage == Stage::Walk || stage == Stage::Ready) && k > 0 =>
            {
                legs.push(Leg::Transfer {
                    from_stop: from as u32,
                    to_stop: to as u32,
                    seconds,
                });
                at = if arrive_by { to } else { from };
                stage = Stage::Ride;
            }
            Parent::ReadyRide if stage == Stage::Ready && k > 0 => {
                stage = Stage::Ride;
            }
            _ => return None,
        }
    }
    None
}

pub(super) fn valid_journey(tt: &ZeroCopyTimetable<'_>, q: &Query, j: &Journey) -> bool {
    let elapsed = j.arrival_unix_seconds.saturating_sub(j.departure_unix_seconds);
    if elapsed < 0 || elapsed > i64::from(q.max_duration_seconds) { return false; }
    let mut time = j.departure_unix_seconds;
    let mut stop = None;
    let mut previous_alight: Option<(usize, i64)> = None;
    let mut rides = 0;
    for (i, leg) in j.legs.iter().enumerate() {
        match leg.clone() {
            Leg::Access { to_stop, seconds } if i == 0 => {
                if !q.access.iter().any(|ep| ep.stop.0 == to_stop && ep.seconds == seconds) {
                    return false;
                }
                stop = Some(to_stop);
                time += i64::from(seconds);
            }
            Leg::Transfer { from_stop, to_stop, seconds } => {
                if stop != Some(from_stop)
                    || !tt.footpaths_from(LocationIdx(from_stop)).iter()
                        .any(|f| f.target().0 == to_stop && f.seconds() == seconds)
                { return false; }
                stop = Some(to_stop);
                time += i64::from(seconds);
            }
            Leg::Ride { from_stop, to_stop, route, transport, service_day,
                        board_stop_index, alight_stop_index,
                        departure_unix_seconds, arrival_unix_seconds } => {
                if stop != Some(from_stop) || time > departure_unix_seconds
                    || board_stop_index >= alight_stop_index { return false; }
                let r = RouteIdx(route);
                let t = TransportIdx(transport);
                let Some(range) = tt.route_transport_ranges().get(r.idx()) else { return false };
                let stops = tt.stops_on(r);
                if t.0 < range.from || t.0 >= range.to
                    || stops.get(board_stop_index)
                        .is_none_or(|s| s.location().0 != from_stop || !s.outbound())
                    || stops.get(alight_stop_index)
                        .is_none_or(|s| s.location().0 != to_stop || !s.inbound())
                    || !tt.transport_traffic_days().get(t.idx())
                        .and_then(|b| tt.bitfields().get(b.idx()))
                        .is_some_and(|bits| bits.test(service_day as usize))
                { return false; }
                let midnight = (i64::from(tt.header().first_utc_day)
                    + i64::from(service_day)) * 86_400;
                if transport_event_second(tt, r, t, board_stop_index, false, midnight)
                    != Some(departure_unix_seconds)
                    || transport_event_second(tt, r, t, alight_stop_index, true, midnight)
                        != Some(arrival_unix_seconds)
                { return false; }
                if let Some((last, last_time)) = previous_alight {
                    let to = from_stop as usize;
                    let rule = tt.station_transfer_rule(LocationIdx(last as u32), LocationIdx(from_stop));
                    if rule.is_some_and(|x| x.prohibited()) { return false; }
                    let seconds = tt.location_transfer_seconds()[last]
                        .max(tt.location_transfer_seconds()[to])
                        .max(rule.map_or(0, |x| x.min_seconds));
                    if last_time + i64::from(seconds) > departure_unix_seconds { return false; }
                }
                stop = Some(to_stop);
                time = arrival_unix_seconds;
                previous_alight = Some((to_stop as usize, time));
                rides += 1;
            }
            Leg::Egress { from_stop, seconds } if i + 1 == j.legs.len() => {
                if stop != Some(from_stop)
                    || !q.egress.iter().any(|ep| ep.stop.0 == from_stop && ep.seconds == seconds)
                { return false; }
                time += i64::from(seconds);
            }
            _ => return false,
        }
    }
    rides > 0 && rides == j.boardings && time == j.arrival_unix_seconds
}
