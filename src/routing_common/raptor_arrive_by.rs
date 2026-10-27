//! Latest-departure RAPTOR search for a requested arrival deadline.
//!
//! Traverse trips, transfer footpaths, and service days backwards. Forward
//! routing from (deadline - estimated duration) is not equivalent to arrive-by:
//! it can discard a later departure that still arrives on time.
use crate::routing_common::raptor_query::{Journey, Leg, Query, RaptorStats};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{ensure, Result};
use std::collections::BinaryHeap;

const NEG_INF: i64 = i64::MIN / 4;

#[derive(Clone, Copy)]
enum Next {
    Egress { seconds: u32 },
    Carry { previous_round: usize },
    Walk { to: usize, seconds: u32 },
    Ride {
        to: usize,
        route: u32,
        transport: u32,
        service_day: u16,
        board_stop_index: usize,
        alight_stop_index: usize,
        departure: i64,
        arrival: i64,
    },
}

#[derive(Clone, Copy)]
struct Label {
    time: i64,
    next: Option<Next>,
    // First boarding *after* the current position in the itinerary. This
    // preserves directional stop-pair transfer constraints across footpaths.
    next_board: Option<(usize, i64)>,
}

fn improve(labels: &mut [Label], stop: usize, time: i64, next: Next, next_board: Option<(usize, i64)>) -> bool {
    if time > labels[stop].time {
        labels[stop] = Label { time, next: Some(next), next_board };
        true
    } else {
        false
    }
}

fn can_alight(tt: &ZeroCopyTimetable<'_>, label: Label, stop: usize, arrival: i64) -> bool {
    if label.time < arrival {
        return false;
    }
    let Some((next_stop, next_departure)) = label.next_board else {
        // The final walk/egress never requires a station transfer penalty.
        return true;
    };
    let rule = tt.station_transfer_rule(LocationIdx(stop as u32), LocationIdx(next_stop as u32));
    if rule.is_some_and(|r| r.prohibited()) {
        return false;
    }
    let change = tt.location_transfer_seconds()[stop]
        .max(tt.location_transfer_seconds()[next_stop])
        .max(rule.map_or(0, |r| r.min_seconds));
    arrival + i64::from(change) <= next_departure
}

fn relax_reverse_walks(
    tt: &ZeroCopyTimetable<'_>,
    labels: &mut [Label],
    seeds: &[usize],
    min_time: i64,
    stats: &mut RaptorStats,
) {
    let started = std::time::Instant::now();
    let mut heap = BinaryHeap::<(i64, usize)>::new();
    for &stop in seeds {
        if labels[stop].time >= min_time {
            heap.push((labels[stop].time, stop));
            stats.walking_heap_pushes += 1;
        }
    }
    while let Some((time, to)) = heap.pop() {
        if labels[to].time != time {
            continue;
        }
        // Inbound CSR entries describe source -> to. Do not assume walking
        // edges are symmetric; one-way paths matter in arrival searches.
        for edge in tt.footpaths_to(LocationIdx(to as u32)) {
            stats.footpath_edges_examined += 1;
            let from = edge.target().idx();
            let seconds = edge.seconds();
            if seconds == 0 {
                continue;
            }
            let earlier = time - i64::from(seconds);
            let next_board = labels[to].next_board;
            if earlier >= min_time
                && improve(labels, from, earlier, Next::Walk { to, seconds }, next_board)
            {
                heap.push((earlier, from));
                stats.walking_heap_pushes += 1;
                stats.labels_improved += 1;
            }
        }
    }
    stats.walking_ms += started.elapsed().as_secs_f64() * 1000.0;
}

fn routes_from_marked(tt: &ZeroCopyTimetable<'_>, stops: &[usize], seen: &mut [u8], round: u8) -> Vec<RouteIdx> {
    let mut routes = Vec::new();
    for &stop in stops {
        for &route in tt.routes_at(LocationIdx(stop as u32)) {
            if seen[route.idx()] != round {
                seen[route.idx()] = round;
                routes.push(route);
            }
        }
    }
    routes.sort_unstable_by_key(|route| route.0);
    routes
}

fn reverse_scan(
    tt: &ZeroCopyTimetable<'_>,
    previous: &[Label],
    current: &mut [Label],
    routes: &[RouteIdx],
    min_time: i64,
    deadline: i64,
    stats: &mut RaptorStats,
) {
    let started = std::time::Instant::now();
    let base = i64::from(tt.header().first_utc_day);
    let horizon = i64::from(tt.header().day_count);
    if horizon == 0 {
        return;
    }
    for &route in routes {
        stats.routes_scanned += 1;
        let stops = tt.stops_on(route);
        if stops.len() < 2 {
            continue;
        }
        let transport_range = tt.route_transport_ranges()[route.idx()];
        for id in transport_range.from..transport_range.to {
            let transport = TransportIdx(id);
            let traffic = &tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()];
            let first_departure = i64::from(tt.transport_first_dep_offset()[transport.idx()].seconds());
            let Some(events) = tt.events_for_transport(transport) else {
                continue;
            };
            let earliest_event = first_departure + i64::from(events.first().map_or(0, |e| e.seconds()));
            let latest_event = first_departure + i64::from(events.last().map_or(0, |e| e.seconds()));
            let first_day = ((min_time - latest_event).div_euclid(86_400) - base).max(0);
            let last_day = ((deadline - earliest_event).div_euclid(86_400) - base).min(horizon - 1);
            if first_day > last_day {
                continue;
            }
            let first_word = first_day as usize / 64;
            let last_word = last_day as usize / 64;
            for word in first_word..=last_word {
                let low = if word == first_word { first_day as usize % 64 } else { 0 };
                let high = if word == last_word { last_day as usize % 64 } else { 63 };
                let mask = (u64::MAX << low) & (u64::MAX >> (63 - high));
                let mut active = traffic.words[word] & mask;
                stats.transport_days_tested += (high - low + 1) as u64;
                while active != 0 {
                    let bit = active.trailing_zeros() as usize;
                    active &= active - 1;
                    let service_day = (word * 64 + bit) as i64;
                    stats.active_transport_days += 1;
                    let midnight = (base + service_day) * 86_400 + first_departure;
                    // This is the first feasible *later alighting* on this
                    // vehicle while scanning its stops in reverse order.
                    let mut alighting: Option<(usize, i64, usize)> = None;
                    for (position, stop) in stops.iter().enumerate().rev() {
                        stats.stop_events_scanned += 1;
                        let loc = stop.location().idx();
                        if let Some((to, arrival, alight_stop_index)) = alighting {
                            if stop.outbound() && position + 1 < stops.len() {
                                if let Some(offset) = events.get(position * 2) {
                                    let dep = midnight + i64::from(offset.seconds());
                                    if dep >= min_time && dep <= deadline
                                        && improve(current, loc, dep,
                                            Next::Ride {
                                                to, route: route.0, transport: transport.0,
                                                service_day: service_day as u16,
                                                board_stop_index: position, alight_stop_index,
                                                departure: dep, arrival,
                                            }, Some((loc, dep)))
                                    {
                                        stats.labels_improved += 1;
                                    }
                                }
                            }
                        }
                        if alighting.is_none() && stop.inbound() && position > 0 {
                            if let Some(offset) = events.get(position * 2 - 1) {
                                let arrival = midnight + i64::from(offset.seconds());
                                if arrival >= min_time && arrival <= deadline
                                    && can_alight(tt, previous[loc], loc, arrival)
                                {
                                    alighting = Some((loc, arrival, position));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    stats.route_scan_ms += started.elapsed().as_secs_f64() * 1000.0;
}

fn reconstruct(
    all: &[Vec<Label>],
    round: usize,
    origin: usize,
    access_seconds: u32,
    min_time: i64,
    deadline: i64,
) -> Option<Journey> {
    let departure = all[round][origin].time - i64::from(access_seconds);
    if departure < min_time {
        return None;
    }
    let mut legs = vec![Leg::Access { to_stop: origin as u32, seconds: access_seconds }];
    let mut clock = departure + i64::from(access_seconds);
    let mut stop = origin;
    let mut level = round;
    let mut boardings = 0usize;
    let mut guard = 0usize;
    loop {
        guard += 1;
        if guard > all.len() * all[0].len() + 1 {
            return None;
        }
        match all[level][stop].next? {
            Next::Egress { seconds } => {
                legs.push(Leg::Egress { from_stop: stop as u32, seconds });
                clock += i64::from(seconds);
                break;
            }
            Next::Carry { previous_round } => level = previous_round,
            Next::Walk { to, seconds } => {
                legs.push(Leg::Transfer { from_stop: stop as u32, to_stop: to as u32, seconds });
                stop = to;
                clock += i64::from(seconds);
            }
            Next::Ride {
                to, route, transport, service_day, board_stop_index,
                alight_stop_index, departure: ride_departure, arrival,
            } => {
                if clock > ride_departure || level == 0 {
                    return None;
                }
                legs.push(Leg::Ride {
                    from_stop: stop as u32, to_stop: to as u32, route, transport,
                    service_day, board_stop_index, alight_stop_index,
                    departure_unix_seconds: ride_departure, arrival_unix_seconds: arrival,
                });
                clock = arrival;
                stop = to;
                boardings += 1;
                level -= 1;
            }
        }
    }
    if clock > deadline {
        return None;
    }
    Some(Journey {
        boardings, departure_unix_seconds: departure,
        arrival_unix_seconds: clock, legs,
    })
}

/// Search for trips departing as late as feasible and arriving by the deadline.
///
/// The Query timestamp is interpreted as the *arrival deadline* for this
/// function only; the existing forward search retains departure semantics.
pub fn route_arrive_by_with_stats(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
) -> Result<(Vec<Journey>, RaptorStats)> {
    ensure!(request.max_boardings <= 16, "max_boardings exceeds supported query limit");
    ensure!((1..=7 * 86_400).contains(&request.max_duration_seconds),
        "max_duration_seconds must be between 1 second and 7 days");
    let n = tt.coordinates().len();
    for endpoint in request.access.iter().chain(&request.egress) {
        ensure!(endpoint.stop.idx() < n, "access or egress stop out of range");
    }
    let mut stats = RaptorStats::default();
    let deadline = request.departure_unix_seconds;
    let min_time = deadline - i64::from(request.max_duration_seconds);
    let mut first = vec![Label { time: NEG_INF, next: None, next_board: None }; n];
    for endpoint in &request.egress {
        let latest = deadline - i64::from(endpoint.seconds);
        if latest >= min_time {
            improve(&mut first, endpoint.stop.idx(), latest,
                Next::Egress { seconds: endpoint.seconds }, None);
        }
    }
    let seeds: Vec<usize> = request.egress.iter().map(|ep| ep.stop.idx()).collect();
    relax_reverse_walks(tt, &mut first, &seeds, min_time, &mut stats);
    let mut marked: Vec<usize> = first.iter().enumerate()
        .filter_map(|(idx, label)| (label.time >= min_time).then_some(idx)).collect();
    let mut layers = vec![first];
    let mut seen = vec![0u8; tt.route_transport_ranges().len()];
    for round in 1..=request.max_boardings {
        stats.rounds += 1;
        stats.marked_stops += marked.len() as u64;
        let prev = &layers[round - 1];
        let mut curr = prev.iter().map(|label| Label {
            time: label.time,
            next: if label.time == NEG_INF { None } else {
                Some(Next::Carry { previous_round: round - 1 })
            },
            next_board: label.next_board,
        }).collect::<Vec<_>>();
        stats.round_copy_bytes += (n * std::mem::size_of::<Label>()) as u64;
        let routes = routes_from_marked(tt, &marked, &mut seen, round as u8);
        if routes.is_empty() { break; }
        reverse_scan(tt, prev, &mut curr, &routes, min_time, deadline, &mut stats);
        let seeds: Vec<usize> = curr.iter().zip(prev).enumerate()
            .filter_map(|(i, (new, old))| (new.time > old.time).then_some(i)).collect();
        relax_reverse_walks(tt, &mut curr, &seeds, min_time, &mut stats);
        marked = curr.iter().zip(prev).enumerate()
            .filter_map(|(i, (new, old))| (new.time > old.time).then_some(i)).collect();
        layers.push(curr);
        if marked.is_empty() { break; }
    }

    let mut journeys = Vec::new();
    if let Some(seconds) = request.direct_walk_seconds {
        if seconds <= request.max_duration_seconds {
            journeys.push(Journey {
                boardings: 0,
                departure_unix_seconds: deadline - i64::from(seconds),
                arrival_unix_seconds: deadline,
                legs: vec![Leg::DirectWalk { seconds }],
            });
        }
    }
    for (round, layer) in layers.iter().enumerate() {
        let origin = request.access.iter()
            .filter(|ep| layer[ep.stop.idx()].time - i64::from(ep.seconds) >= min_time)
            .max_by_key(|ep| layer[ep.stop.idx()].time - i64::from(ep.seconds));
        if let Some(start) = origin {
            if let Some(journey) = reconstruct(&layers, round, start.stop.idx(), start.seconds, min_time, deadline) {
                journeys.push(journey);
            }
        }
    }
    // Latest feasible origin departure is the primary arrive-by objective.
    // Keep a Pareto frontier by boardings, favoring later departures within
    // each transfer budget. The actual arrival never exceeds the deadline.
    journeys.sort_by_key(|j| (j.boardings, std::cmp::Reverse(j.departure_unix_seconds), j.arrival_unix_seconds));
    let mut best_departure = i64::MIN;
    journeys.retain(|j| {
        if j.departure_unix_seconds > best_departure {
            best_departure = j.departure_unix_seconds;
            true
        } else {
            false
        }
    });
    Ok((journeys, stats))
}
