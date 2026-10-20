use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, ensure};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const INF: i64 = i64::MAX / 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalkingEndpoint {
    pub stop: LocationIdx,
    pub minutes: u32,
}

#[derive(Clone, Debug)]
pub struct Query {
    pub departure_unix_seconds: i64,
    pub access: Vec<WalkingEndpoint>,
    pub egress: Vec<WalkingEndpoint>,
    pub direct_walk_minutes: Option<u32>,
    pub max_boardings: usize,
    pub max_duration_minutes: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Leg {
    Access { to_stop: u32, minutes: u32 },
    Transfer { from_stop: u32, to_stop: u32, minutes: u32 },
    Ride {
        from_stop: u32,
        to_stop: u32,
        route: u32,
        transport: u32,
        service_day: u16,
        departure_unix_seconds: i64,
        arrival_unix_seconds: i64,
    },
    Egress { from_stop: u32, minutes: u32 },
    DirectWalk { minutes: u32 },
}

#[derive(Clone, Debug, Serialize)]
pub struct Journey {
    pub boardings: usize,
    pub departure_unix_seconds: i64,
    pub arrival_unix_seconds: i64,
    pub legs: Vec<Leg>,
}

#[derive(Clone, Copy)]
enum Parent {
    Access { minutes: u32 },
    Carry { previous_round: usize },
    Walk { from: usize, minutes: u32 },
    Ride {
        from: usize,
        route: u32,
        transport: u32,
        service_day: u16,
        departure: i64,
        arrival: i64,
    },
}

#[derive(Clone, Copy)]
struct Label {
    time: i64,
    parent: Option<Parent>,
}

fn improve(labels: &mut [Label], loc: usize, time: i64, parent: Parent) -> bool {
    if time < labels[loc].time {
        labels[loc] = Label { time, parent: Some(parent) };
        true
    } else {
        false
    }
}

fn relax_walks(
    tt: &ZeroCopyTimetable<'_>,
    labels: &mut [Label],
    max_time: i64,
) {
    let mut heap = BinaryHeap::new();
    for (idx, label) in labels.iter().enumerate() {
        if label.time < INF && label.time <= max_time {
            heap.push(Reverse((label.time, idx)));
        }
    }
    while let Some(Reverse((time, from))) = heap.pop() {
        if labels[from].time != time { continue; }
        for edge in tt.footpaths_from(LocationIdx(from as u32)) {
            let to = edge.target().idx();
            let minutes = u32::from(edge.minutes());
            if minutes == 0 { continue; }
            let next = time + i64::from(minutes);
            if next <= max_time && improve(
                labels, to, next, Parent::Walk { from, minutes },
            ) {
                heap.push(Reverse((next, to)));
            }
        }
    }
}

fn traverse_routes(
    tt: &ZeroCopyTimetable<'_>,
    prev: &[Label],
    curr: &mut [Label],
    departure: i64,
    max_time: i64,
) -> Result<()> {
    let base = i64::from(tt.header().first_utc_day);
    let horizon = i64::from(tt.header().day_count);
    let first_day = (departure.div_euclid(1440) - base - 31).max(0);
    let last_day = (max_time.div_euclid(1440) - base).min(horizon - 1);
    if first_day > last_day { return Ok(()); }

    for (r, transport_range) in tt.route_transport_ranges().iter().enumerate() {
        let route = RouteIdx(r as u32);
        let stops = tt.stops_on(route);
        if stops.len() < 2 || !stops.iter().any(|s| prev[s.location().idx()].time < INF) {
            continue;
        }
        for index in transport_range.from..transport_range.to {
            let transport = TransportIdx(index);
            let active_days = tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()];
            let first_departure = i64::from(tt.transport_first_dep_offset()[transport.idx()].minutes());
            for day in first_day..=last_day {
                if !active_days.test(day as usize) { continue; }
                let absolute_first = (base + day) * 1440 + first_departure;
                let mut onboard: Option<(usize, i64)> = None;
                for (position, stop) in stops.iter().enumerate() {
                    let loc = stop.location().idx();
                    if let Some((boarding_loc, boarding_time)) = onboard {
                        if stop.inbound() {
                            if let Some(offset) = tt.event_time(route, transport, position, true) {
                                let arrival = absolute_first + i64::from(offset.minutes());
                                if arrival <= max_time {
                                    improve(curr, loc, arrival, Parent::Ride {
                                        from: boarding_loc,
                                        route: route.0,
                                        transport: transport.0,
                                        service_day: day as u16,
                                        departure: boarding_time,
                                        arrival,
                                    });
                                }
                            }
                        }
                    }
                    if onboard.is_none() && stop.outbound() && position + 1 < stops.len() {
                        if let Some(offset) = tt.event_time(route, transport, position, false) {
                            let board_time = absolute_first + i64::from(offset.minutes());
                            if prev[loc].time <= board_time && board_time <= max_time {
                                onboard = Some((loc, board_time));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn reconstruct(
    all: &[Vec<Label>],
    round: usize,
    destination: usize,
    egress: u32,
    departure: i64,
) -> Option<Journey> {
    let arrival = all[round][destination].time + i64::from(egress);
    let mut steps = vec![Leg::Egress { from_stop: destination as u32, minutes: egress }];
    let mut loc = destination;
    let mut level = round;
    let mut boarded = 0;
    let mut guard = 0;
    loop {
        guard += 1;
        if guard > all.len() * all[0].len() + 1 { return None; }
        let label = all[level][loc];
        match label.parent? {
            Parent::Access { minutes } => {
                steps.push(Leg::Access { to_stop: loc as u32, minutes });
                break;
            }
            Parent::Carry { previous_round } => level = previous_round,
            Parent::Walk { from, minutes } => {
                steps.push(Leg::Transfer {
                    from_stop: from as u32,
                    to_stop: loc as u32,
                    minutes,
                });
                loc = from;
            }
            Parent::Ride { from, route, transport, service_day, departure, arrival } => {
                steps.push(Leg::Ride {
                    from_stop: from as u32,
                    to_stop: loc as u32,
                    route,
                    transport,
                    service_day,
                    departure_unix_seconds: departure * 60,
                    arrival_unix_seconds: arrival * 60,
                });
                loc = from;
                if level == 0 { return None; }
                level -= 1;
                boarded += 1;
            }
        }
    }
    steps.reverse();
    Some(Journey {
        boardings: boarded,
        departure_unix_seconds: departure * 60,
        arrival_unix_seconds: arrival * 60,
        legs: steps,
    })
}

pub fn route(tt: &ZeroCopyTimetable<'_>, request: &Query) -> Result<Vec<Journey>> {
    ensure!(request.max_boardings <= 16, "max_boardings exceeds supported query limit");
    ensure!((1..=7 * 1440).contains(&request.max_duration_minutes),
        "max_duration_minutes must be between 1 minute and 7 days");
    let n = tt.coordinates().len();
    for endpoint in request.access.iter().chain(&request.egress) {
        ensure!(endpoint.stop.idx() < n, "access or egress stop out of range");
    }
    let departure = request.departure_unix_seconds.div_euclid(60)
        + i64::from(request.departure_unix_seconds.rem_euclid(60) != 0);
    let max_time = departure + i64::from(request.max_duration_minutes);
    let mut round0 = vec![Label {time:INF, parent:None};n];
    for endpoint in &request.access {
        let time = departure + i64::from(endpoint.minutes);
        if time <= max_time {
            improve(&mut round0, endpoint.stop.idx(), time, Parent::Access {
                minutes: endpoint.minutes,
            });
        }
    }
    relax_walks(tt, &mut round0, max_time);
    let mut labels = vec![round0];
    for round in 1..=request.max_boardings {
        let prev = &labels[round - 1];
        let mut curr: Vec<Label> = prev.iter().map(|label| Label {
            time: label.time,
            parent: if label.time == INF { None } else {
                Some(Parent::Carry { previous_round: round - 1 })
            },
        }).collect();
        traverse_routes(tt, prev, &mut curr, departure, max_time)?;
        relax_walks(tt, &mut curr, max_time);
        let improved = curr.iter().zip(prev).any(|(a,b)| a.time < b.time);
        labels.push(curr);
        if !improved { break; }
    }

    let mut journeys = Vec::new();
    if let Some(minutes) = request.direct_walk_minutes {
        if minutes <= request.max_duration_minutes {
            journeys.push(Journey {
                boardings: 0,
                departure_unix_seconds: request.departure_unix_seconds,
                arrival_unix_seconds: request.departure_unix_seconds + i64::from(minutes)*60,
                legs: vec![Leg::DirectWalk {minutes}],
            });
        }
    }
    for (round, layer) in labels.iter().enumerate() {
        let best = request.egress.iter()
            .filter(|x| layer[x.stop.idx()].time + i64::from(x.minutes) <= max_time)
            .min_by_key(|x| layer[x.stop.idx()].time + i64::from(x.minutes));
        if let Some(dest) = best {
            if let Some(journey) = reconstruct(&labels, round, dest.stop.idx(),
                                               dest.minutes, departure) {
                journeys.push(journey);
            }
        }
    }
    journeys.sort_by_key(|x| (x.boardings, x.arrival_unix_seconds));
    let mut frontier = Vec::new();
    let mut best_arrival = i64::MAX;
    for journey in journeys {
        if journey.arrival_unix_seconds < best_arrival {
            best_arrival = journey.arrival_unix_seconds;
            frontier.push(journey);
        }
    }
    Ok(frontier)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::{PackedStop, TimetableOwned, Delta, Range, TrafficDays,
        BitfieldIdx};

    fn fixture() -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, 2);
        tt.add_source("test", "feed", "attempt").unwrap();
        for _ in 0..3 {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(crate::routing_common::timetable::SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_minutes.push(0);
            tt.location_routes_offsets.push(0);
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
        }
        for i in 0..3 {
            tt.route_stops.push(PackedStop::new(LocationIdx(i), true,true,true,true).unwrap());
        }
        tt.route_stop_offsets.push(3);
        tt.route_transport_ranges.push(Range {from:0,to:1});
        tt.route_stop_time_ranges.push(Range {from:0,to:4});
        tt.route_stop_times = vec![
            Delta::from_minutes(0).unwrap(), Delta::from_minutes(10).unwrap(),
            Delta::from_minutes(12).unwrap(), Delta::from_minutes(22).unwrap(),
        ];
        tt.route_clasz.push(10);
        tt.transport_route.push(RouteIdx(0));
        tt.transport_first_dep_offset.push(Delta::from_minutes(480).unwrap());
        tt.transport_initial_day_offset.push(0);
        let mut days = TrafficDays::default();
        days.set(0,true).unwrap();
        tt.bitfields.push(days);
        tt.transport_traffic_days.push(BitfieldIdx(1));
        tt.route_traffic_days.push(BitfieldIdx(1));
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.transport_to_trip_section_offsets.push(0);
        tt
    }

    #[test]
    fn route_with_one_vehicle_returns_one_boarding() {
        let tt = fixture();
        let binary = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&binary).unwrap();
        let input = Query {
            departure_unix_seconds: (20_000_i64*1440 + 475)*60,
            access:vec![WalkingEndpoint {stop:LocationIdx(0),minutes:2}],
            egress:vec![WalkingEndpoint {stop:LocationIdx(2),minutes:3}],
            direct_walk_minutes: None,
            max_boardings:3, max_duration_minutes:120,
        };
        let journeys = route(&view,&input).unwrap();
        assert_eq!(journeys.len(),1);
        assert_eq!(journeys[0].boardings,1);
        assert_eq!(journeys[0].arrival_unix_seconds,
            (20_000_i64*1440+505)*60);
    }
}
