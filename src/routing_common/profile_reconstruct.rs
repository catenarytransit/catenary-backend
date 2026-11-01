//! Journey extraction from compact RAPTOR round predecessor indices.
//!
//! Arrival scans retain one scalar time per stop/stage/round. The leg graph is
//! interpreted only after all rounds for an origin event have completed, so
//! overwriting a current-round arrival cannot corrupt an earlier result.
use crate::routing_common::profile_raptor::{CompactRound, Link, StageKind};
use crate::routing_common::raptor_query::{Journey, Leg, Query, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};

/// Reconstruct one terminal state after the compact scan has stopped mutating
/// its round arrays. Paths reference an earlier round at every transit edge
/// and a ride-arrival cell at every same-round walking edge.
pub(super) fn reconstruct(
    rounds: &[CompactRound],
    links: &[Link],
    generation: u32,
    arrive_by: bool,
    end_round: usize,
    end_stop: usize,
    end_stage: StageKind,
    endpoint_seconds: u32,
) -> Option<Journey> {
    let terminal_time = rounds.get(end_round)?
        .stage(end_stage).time(end_stop, generation, arrive_by)?;
    let mut legs = Vec::new();
    if arrive_by {
        legs.push(Leg::Access {
            to_stop: end_stop as u32,
            seconds: endpoint_seconds,
        });
    } else {
        legs.push(Leg::Egress {
            from_stop: end_stop as u32,
            seconds: endpoint_seconds,
        });
    }

    let mut round = end_round;
    let mut loc = end_stop;
    let mut stage = end_stage;
    // At most one walk per round (Gentian's transfer graph has a closure).
    // This bound protects against corrupt predecessors without path-node caps.
    for _ in 0..(rounds.len() * 3 + 2) {
        let link_idx = rounds.get(round)?.stage(stage).link(loc, generation)?;
        match *links.get(link_idx)? {
            Link::Root { seconds } if round == 0 && stage == StageKind::Ride => {
                if arrive_by {
                    legs.push(Leg::Egress {
                        from_stop: loc as u32,
                        seconds,
                    });
                } else {
                    legs.push(Leg::Access {
                        to_stop: loc as u32,
                        seconds,
                    });
                    legs.reverse();
                }
                let first_ride = legs.iter().find_map(|leg| match leg {
                    Leg::Ride { departure_unix_seconds, .. } => Some(*departure_unix_seconds),
                    _ => None,
                })?;
                let last_ride = legs.iter().rev().find_map(|leg| match leg {
                    Leg::Ride { arrival_unix_seconds, .. } => Some(*arrival_unix_seconds),
                    _ => None,
                })?;
                let access = match legs.first()? {
                    Leg::Access { seconds, .. } => i64::from(*seconds),
                    _ => return None,
                };
                let egress = match legs.last()? {
                    Leg::Egress { seconds, .. } => i64::from(*seconds),
                    _ => return None,
                };
                let departure = first_ride.checked_sub(access)?;
                let arrival = last_ride.checked_add(egress)?;
                // An intermodal endpoint may be reached after an internal
                // transfer rather than directly from the last ride.
                let actual_end = if arrive_by {
                    terminal_time.checked_sub(i64::from(endpoint_seconds))?
                } else {
                    terminal_time.checked_add(i64::from(endpoint_seconds))?
                };
                if (arrive_by && departure > actual_end)
                    || (!arrive_by && arrival < actual_end)
                {
                    return None;
                }
                let boardings = legs.iter().filter(|leg| matches!(leg, Leg::Ride { .. })).count();
                return Some(Journey {
                    departure_unix_seconds: departure,
                    arrival_unix_seconds: if arrive_by { last_ride + egress } else { actual_end },
                    boardings,
                    legs,
                });
            }
            Link::Ride {
                from_stop,
                to_stop,
                route,
                transport,
                service_day,
                board_stop_index,
                alight_stop_index,
                departure,
                arrival,
                previous_stage,
            } if stage == StageKind::Ride && round > 0 => {
                legs.push(Leg::Ride {
                    from_stop: from_stop as u32,
                    to_stop: to_stop as u32,
                    route,
                    transport,
                    service_day,
                    board_stop_index,
                    alight_stop_index,
                    departure_unix_seconds: departure,
                    arrival_unix_seconds: arrival,
                });
                loc = if arrive_by { to_stop } else { from_stop };
                stage = previous_stage;
                round -= 1;
            }
            Link::Footpath { next, seconds } if stage != StageKind::Ride && round > 0 => {
                legs.push(if arrive_by {
                    Leg::Transfer {
                        from_stop: loc as u32,
                        to_stop: next as u32,
                        seconds,
                    }
                } else {
                    Leg::Transfer {
                        from_stop: next as u32,
                        to_stop: loc as u32,
                        seconds,
                    }
                });
                loc = next;
                stage = StageKind::Ride;
            }
            _ => return None,
        }
    }
    None
}

/// Validate the final, optimized itinerary, including exact GTFS timetable
/// seconds, service calendars, physical walking edges and transfer restrictions.
pub(super) fn valid_journey(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    journey: &Journey,
) -> bool {
    let duration = journey.arrival_unix_seconds.saturating_sub(journey.departure_unix_seconds);
    if duration < 0 || duration > i64::from(request.max_duration_seconds) {
        return false;
    }
    let mut loc = None;
    let mut now = journey.departure_unix_seconds;
    let mut previous_alight: Option<(usize, i64)> = None;
    let mut boardings = 0usize;
    for (i, leg) in journey.legs.iter().enumerate() {
        match *leg {
            Leg::Access { to_stop, seconds } if i == 0 => {
                if !request.access.iter().any(|ep| ep.stop.0 == to_stop && ep.seconds == seconds) {
                    return false;
                }
                loc = Some(to_stop);
                now = now.saturating_add(i64::from(seconds));
            }
            Leg::Transfer { from_stop, to_stop, seconds } => {
                if loc != Some(from_stop)
                    || !tt.footpaths_from(LocationIdx(from_stop))
                        .iter().any(|fp| fp.target().0 == to_stop && fp.seconds() == seconds)
                {
                    return false;
                }
                loc = Some(to_stop);
                now = now.saturating_add(i64::from(seconds));
            }
            Leg::Ride {
                from_stop,
                to_stop,
                route,
                transport,
                service_day,
                board_stop_index,
                alight_stop_index,
                departure_unix_seconds,
                arrival_unix_seconds,
            } => {
                if loc != Some(from_stop)
                    || now > departure_unix_seconds
                    || board_stop_index >= alight_stop_index
                {
                    return false;
                }
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
                        .is_some_and(|calendar| calendar.test(service_day as usize))
                {
                    return false;
                }
                let midnight = (i64::from(tt.header().first_utc_day) + i64::from(service_day)) * 86_400;
                if transport_event_second(tt, r, t, board_stop_index, false, midnight)
                    != Some(departure_unix_seconds)
                    || transport_event_second(tt, r, t, alight_stop_index, true, midnight)
                        != Some(arrival_unix_seconds)
                {
                    return false;
                }
                if let Some((from, alight)) = previous_alight {
                    let to = from_stop as usize;
                    let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(from_stop));
                    if rule.is_some_and(|x| x.prohibited()) {
                        return false;
                    }
                    let change = tt.location_transfer_seconds()[from]
                        .max(tt.location_transfer_seconds()[to])
                        .max(rule.map_or(0, |x| x.min_seconds));
                    if alight.saturating_add(i64::from(change)) > departure_unix_seconds {
                        return false;
                    }
                }
                now = arrival_unix_seconds;
                loc = Some(to_stop);
                previous_alight = Some((to_stop as usize, arrival_unix_seconds));
                boardings += 1;
            }
            Leg::Egress { from_stop, seconds } if i + 1 == journey.legs.len() => {
                if loc != Some(from_stop)
                    || !request.egress.iter().any(|ep| ep.stop.0 == from_stop && ep.seconds == seconds)
                {
                    return false;
                }
                now = now.saturating_add(i64::from(seconds));
            }
            _ => return false,
        }
    }
    boardings > 0 && boardings == journey.boardings && now == journey.arrival_unix_seconds
}
