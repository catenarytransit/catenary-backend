//! Validate reconstructed profile journeys separately from compact timetable scans.
use crate::routing_common::raptor_query::{Journey, Leg, Query, transport_event_second};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};

pub(super) fn valid_journey(tt: &ZeroCopyTimetable<'_>, request: &Query, j: &Journey) -> bool {
    let elapsed = j
        .arrival_unix_seconds
        .saturating_sub(j.departure_unix_seconds);
    if elapsed < 0 || elapsed > i64::from(request.max_duration_seconds) {
        return false;
    }
    let mut time = j.departure_unix_seconds;
    let mut stop: Option<u32> = None;
    let mut previous_alight: Option<(usize, i64)> = None;
    let mut rides = 0usize;
    for (i, leg) in j.legs.iter().enumerate() {
        match leg.clone() {
            Leg::Access { to_stop, seconds } if i == 0 => {
                if !request
                    .access
                    .iter()
                    .any(|a| a.stop.0 == to_stop && a.seconds == seconds)
                {
                    return false;
                }
                stop = Some(to_stop);
                time = time.saturating_add(i64::from(seconds));
            }
            Leg::Transfer {
                from_stop,
                to_stop,
                seconds,
            } => {
                if stop != Some(from_stop)
                    || !tt
                        .footpaths_from(LocationIdx(from_stop))
                        .iter()
                        .any(|f| f.target().0 == to_stop && f.seconds() == seconds)
                {
                    return false;
                }
                stop = Some(to_stop);
                time = time.saturating_add(i64::from(seconds));
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
                if stop != Some(from_stop)
                    || time > departure_unix_seconds
                    || board_stop_index >= alight_stop_index
                {
                    return false;
                }
                let route = RouteIdx(route);
                let transport = TransportIdx(transport);
                let Some(r) = tt.route_transport_ranges().get(route.idx()) else {
                    return false;
                };
                let stops = tt.stops_on(route);
                if transport.0 < r.from
                    || transport.0 >= r.to
                    || stops
                        .get(board_stop_index)
                        .is_none_or(|s| s.location().0 != from_stop || !s.outbound())
                    || stops
                        .get(alight_stop_index)
                        .is_none_or(|s| s.location().0 != to_stop || !s.inbound())
                    || !tt
                        .transport_traffic_days()
                        .get(transport.idx())
                        .and_then(|b| tt.bitfields().get(b.idx()))
                        .is_some_and(|bits| bits.test(service_day as usize))
                {
                    return false;
                }
                let midnight =
                    (i64::from(tt.header().first_utc_day) + i64::from(service_day)) * 86_400;
                if transport_event_second(tt, route, transport, board_stop_index, false, midnight)
                    != Some(departure_unix_seconds)
                    || transport_event_second(
                        tt,
                        route,
                        transport,
                        alight_stop_index,
                        true,
                        midnight,
                    ) != Some(arrival_unix_seconds)
                {
                    return false;
                }
                if let Some((from, alight)) = previous_alight {
                    let to = from_stop as usize;
                    let rule =
                        tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(from_stop));
                    if rule.is_some_and(|r| r.prohibited()) {
                        return false;
                    }
                    let change = tt.location_transfer_seconds()[from]
                        .max(tt.location_transfer_seconds()[to])
                        .max(rule.map_or(0, |r| r.min_seconds));
                    if alight.saturating_add(i64::from(change)) > departure_unix_seconds {
                        return false;
                    }
                }
                stop = Some(to_stop);
                time = arrival_unix_seconds;
                previous_alight = Some((to_stop as usize, arrival_unix_seconds));
                rides += 1;
            }
            Leg::Egress { from_stop, seconds } if i + 1 == j.legs.len() => {
                if stop != Some(from_stop)
                    || !request
                        .egress
                        .iter()
                        .any(|e| e.stop.0 == from_stop && e.seconds == seconds)
                {
                    return false;
                }
                time = time.saturating_add(i64::from(seconds));
            }
            _ => return false,
        }
    }
    rides > 0 && rides == j.boardings && time == j.arrival_unix_seconds
}

