use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use anyhow::{Result, ensure};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const INF: i64 = i64::MAX / 4;

/// Work counters describe the search, not allocations in OSR or metadata hydration.
/// Keep the fields additive so production logs remain comparable across revisions.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RaptorStats {
    pub rounds: u64,
    pub marked_stops: u64,
    pub routes_scanned: u64,
    pub transport_days_tested: u64,
    pub active_transport_days: u64,
    pub stop_events_scanned: u64,
    pub footpath_edges_examined: u64,
    pub walking_heap_pushes: u64,
    pub labels_improved: u64,
    pub round_copy_bytes: u64,
    pub route_scan_ms: f64,
    pub walking_ms: f64,
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalkingEndpoint {
    pub stop: LocationIdx,
    pub seconds: u32,
}

#[derive(Clone, Debug)]
pub struct Query {
    pub departure_unix_seconds: i64,
    pub access: Vec<WalkingEndpoint>,
    pub egress: Vec<WalkingEndpoint>,
    pub direct_walk_seconds: Option<u32>,
    pub max_boardings: usize,
    pub max_duration_seconds: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Leg {
    Access { to_stop: u32, seconds: u32 },
    Transfer { from_stop: u32, to_stop: u32, seconds: u32 },
    Ride {
        from_stop: u32,
        to_stop: u32,
        route: u32,
        transport: u32,
        service_day: u16,
        departure_unix_seconds: i64,
        arrival_unix_seconds: i64,
    },
    Egress { from_stop: u32, seconds: u32 },
    DirectWalk { seconds: u32 },
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
    Access { seconds: u32 },
    Carry { previous_round: usize },
    Walk { from: usize, seconds: u32 },
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
    // Last transit alighting event, retained across walking/carry labels.
    // Absent for origin access so first boarding is not treated as a transfer.
    last_alight: Option<(usize, i64)>,
}

fn improve(
    labels: &mut [Label], loc: usize, time: i64,
    parent: Parent, last_alight: Option<(usize, i64)>,
) -> bool {
    if time < labels[loc].time {
        labels[loc] = Label { time, parent: Some(parent), last_alight };
        true
    } else {
        false
    }
}

/// Transfer rules affect boarding after a ride, not initial access or egress.
/// Use max(walk arrival, source/destination change time after alighting),
/// rather than adding a station penalty to the physical walking time.
fn can_board(tt: &ZeroCopyTimetable<'_>, label: Label, stop: usize, departure: i64) -> bool {
    if label.time > departure { return false; }
    let Some((from, alight_time)) = label.last_alight else { return true };
    let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(stop as u32));
    if rule.is_some_and(|r| r.prohibited()) { return false; }
    let change_time = tt.location_transfer_seconds()[from]
        .max(tt.location_transfer_seconds()[stop])
        .max(rule.map_or(0, |r| r.min_seconds));
    alight_time + i64::from(change_time) <= departure
}

fn relax_walks_profiled(
    tt: &ZeroCopyTimetable<'_>,
    labels: &mut [Label],
    max_time: i64,
    stats: &mut RaptorStats,
) {
    let started = std::time::Instant::now();
    let mut heap = BinaryHeap::new();
    for (idx, label) in labels.iter().enumerate() {
        if label.time < INF && label.time <= max_time {
            heap.push(Reverse((label.time, idx)));
            stats.walking_heap_pushes += 1;
        }
    }
    while let Some(Reverse((time, from))) = heap.pop() {
        if labels[from].time != time { continue; }
        for edge in tt.footpaths_from(LocationIdx(from as u32)) {
            stats.footpath_edges_examined += 1;
            let to = edge.target().idx();
            let seconds = u32::from(edge.seconds());
            if seconds == 0 { continue; }
            let next = time + i64::from(seconds);
            let last_alight = labels[from].last_alight;
            if next <= max_time && improve(
                labels, to, next, Parent::Walk { from, seconds }, last_alight,
            ) {
                stats.labels_improved += 1;
                stats.walking_heap_pushes += 1;
                heap.push(Reverse((next, to)));
            }
        }
    }
    stats.walking_ms += started.elapsed().as_secs_f64() * 1000.0;
}

#[cfg(test)]
fn relax_walks(tt: &ZeroCopyTimetable<'_>, labels: &mut [Label], max_time: i64) {
    relax_walks_profiled(tt, labels, max_time, &mut RaptorStats::default());
}

/// RAPTOR's route marking: only routes incident to a newly improved
/// location can produce new labels in the next round. The CSR index was
/// built during Gentian compilation, so no global route scan is necessary.
///
/// A bitmap deduplicates looped stops and routes shared by several marked
/// locations. Sorting the resulting route indices preserves the deterministic
/// tie-breaking order of the previous exhaustive scan.
fn routes_from_marked(
    tt: &ZeroCopyTimetable<'_>,
    marked_stops: &[usize],
    seen_epoch: &mut [u8],
    epoch: u8,
) -> Vec<RouteIdx> {
    debug_assert_eq!(seen_epoch.len(), tt.route_transport_ranges().len());
    let mut routes = Vec::new();
    for &stop in marked_stops {
        for &route in tt.routes_at(LocationIdx(stop as u32)) {
            if seen_epoch[route.idx()] != epoch {
                seen_epoch[route.idx()] = epoch;
                routes.push(route);
            }
        }
    }
    routes.sort_unstable_by_key(|r| r.0);
    routes
}

fn traverse_routes_profiled(
    tt: &ZeroCopyTimetable<'_>,
    prev: &[Label],
    curr: &mut [Label],
    marked_routes: &[RouteIdx],
    departure: i64,
    max_time: i64,
    stats: &mut RaptorStats,
) -> Result<()> {
    let started = std::time::Instant::now();
    let base = i64::from(tt.header().first_utc_day);
    let horizon = i64::from(tt.header().day_count);
    if horizon == 0 { return Ok(()); }

    for &route in marked_routes {
        stats.routes_scanned += 1;
        let transport_range = tt.route_transport_ranges()[route.idx()];
        let stops = tt.stops_on(route);
        if stops.len() < 2 { continue; }

        // A service day contributes only when its actual event interval
        // intersects the request interval. Do not use a fixed GTFS lookback.
        for index in transport_range.from..transport_range.to {
            let transport = TransportIdx(index);
            let active_days = tt.bitfields()[tt.transport_traffic_days()[transport.idx()].idx()];
            let first_departure = i64::from(tt.transport_first_dep_offset()[transport.idx()].seconds());
            let Some(events) = tt.events_for_transport(transport) else { continue };
            let earliest_event = first_departure + i64::from(events.first().map_or(0, |x| x.seconds()));
            let latest_event = first_departure + i64::from(events.last().map_or(0, |x| x.seconds()));
            let first_day = ((departure - latest_event).div_euclid(86_400) - base).max(0);
            let last_day = ((max_time - earliest_event).div_euclid(86_400) - base).min(horizon - 1);
            if first_day > last_day { continue; }

            // Walk only active service days within the exact window, using
            // bit operations rather than visiting each inactive calendar day.
            let first_word = (first_day as usize) / 64;
            let last_word = (last_day as usize) / 64;
            for word_idx in first_word..=last_word {
                let lower = if word_idx == first_word { (first_day as usize) % 64 } else { 0 };
                let upper = if word_idx == last_word { (last_day as usize) % 64 } else { 63 };
                let lower_mask = u64::MAX << lower;
                let upper_mask = u64::MAX >> (63 - upper);
                let mut remaining = active_days.words[word_idx] & lower_mask & upper_mask;
                stats.transport_days_tested += (upper - lower + 1) as u64;
                while remaining != 0 {
                    let bit = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1;
                    let day = (word_idx * 64 + bit) as i64;
                    stats.active_transport_days += 1;
                    let absolute_first = (base + day) * 86_400 + first_departure;
                    let mut onboard: Option<(usize, i64)> = None;
                    for (position, stop) in stops.iter().enumerate() {
                    stats.stop_events_scanned += 1;
                    let loc = stop.location().idx();
                    if let Some((boarding_loc, boarding_time)) = onboard {
                        if stop.inbound() {
                            if let Some(offset) = position.checked_mul(2).and_then(|i| i.checked_sub(1)).and_then(|i| events.get(i)) {
                                let arrival = absolute_first + i64::from(offset.seconds());
                                if arrival <= max_time {
                                    if improve(curr, loc, arrival, Parent::Ride {
                                        from: boarding_loc,
                                        route: route.0,
                                        transport: transport.0,
                                        service_day: day as u16,
                                        departure: boarding_time,
                                        arrival,
                                    }, Some((loc, arrival))) {
                                        stats.labels_improved += 1;
                                    }
                                }
                            }
                        }
                    }
                    if onboard.is_none() && stop.outbound() && position + 1 < stops.len() {
                        if let Some(offset) = position.checked_mul(2).and_then(|i| events.get(i)) {
                            let board_time = absolute_first + i64::from(offset.seconds());
                            if board_time <= max_time &&
                               can_board(tt, prev[loc], loc, board_time)
                            {
                                onboard = Some((loc, board_time));
                            }
                        }
                    }
                }
                }
            }
        }
    }
    stats.route_scan_ms += started.elapsed().as_secs_f64() * 1000.0;
    Ok(())
}

#[cfg(test)]
fn traverse_routes(
    tt: &ZeroCopyTimetable<'_>,
    prev: &[Label],
    curr: &mut [Label],
    marked_routes: &[RouteIdx],
    departure: i64,
    max_time: i64,
) -> Result<()> {
    traverse_routes_profiled(
        tt, prev, curr, marked_routes, departure, max_time,
        &mut RaptorStats::default(),
    )
}

fn reconstruct(
    all: &[Vec<Label>],
    round: usize,
    destination: usize,
    egress: u32,
    departure: i64,
) -> Option<Journey> {
    let arrival = all[round][destination].time + i64::from(egress);
    let mut steps = vec![Leg::Egress { from_stop: destination as u32, seconds: egress }];
    let mut loc = destination;
    let mut level = round;
    let mut boarded = 0;
    let mut guard = 0;
    loop {
        guard += 1;
        if guard > all.len() * all[0].len() + 1 { return None; }
        let label = all[level][loc];
        match label.parent? {
            Parent::Access { seconds } => {
                steps.push(Leg::Access { to_stop: loc as u32, seconds });
                break;
            }
            Parent::Carry { previous_round } => level = previous_round,
            Parent::Walk { from, seconds } => {
                steps.push(Leg::Transfer {
                    from_stop: from as u32,
                    to_stop: loc as u32,
                    seconds,
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
                    departure_unix_seconds: departure,
                    arrival_unix_seconds: arrival,
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
        departure_unix_seconds: departure,
        arrival_unix_seconds: arrival,
        legs: steps,
    })
}

pub fn route(tt: &ZeroCopyTimetable<'_>, request: &Query) -> Result<Vec<Journey>> {
    route_with_stats(tt, request).map(|(journeys, _)| journeys)
}

pub fn route_with_stats(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
) -> Result<(Vec<Journey>, RaptorStats)> {
    let mut stats = RaptorStats::default();
    ensure!(request.max_boardings <= 16, "max_boardings exceeds supported query limit");
    ensure!((1..=7 * 86_400).contains(&request.max_duration_seconds),
        "max_duration_seconds must be between 1 second and 7 days");
    let n = tt.coordinates().len();
    for endpoint in request.access.iter().chain(&request.egress) {
        ensure!(endpoint.stop.idx() < n, "access or egress stop out of range");
    }
    let departure = request.departure_unix_seconds;
    let max_time = departure + i64::from(request.max_duration_seconds);
    let mut round0 = vec![Label {time:INF, parent:None, last_alight:None};n];
    for endpoint in &request.access {
        let time = departure + i64::from(endpoint.seconds);
        if time <= max_time {
            improve(&mut round0, endpoint.stop.idx(), time, Parent::Access {
                seconds: endpoint.seconds,
            }, None);
        }
    }
    relax_walks_profiled(tt, &mut round0, max_time, &mut stats);
    let mut marked_stops = round0.iter().enumerate()
        .filter_map(|(idx, label)| (label.time < INF).then_some(idx))
        .collect::<Vec<_>>();
    let mut labels = vec![round0];
    // A single route-sized bitmap is sufficient for the whole search.
    // Rounds are limited to 16, so a u8 epoch never wraps.
    let mut seen_epoch = vec![0u8; tt.route_transport_ranges().len()];
    for round in 1..=request.max_boardings {
        stats.rounds += 1;
        stats.marked_stops += marked_stops.len() as u64;
        let prev = &labels[round - 1];
        let mut curr: Vec<Label> = prev.iter().map(|label| Label {
            time: label.time,
            parent: if label.time == INF { None } else {
                Some(Parent::Carry { previous_round: round - 1 })
            },
            last_alight: label.last_alight,
        }).collect();
        stats.round_copy_bytes += (n * std::mem::size_of::<Label>()) as u64;
        let routes = routes_from_marked(tt, &marked_stops, &mut seen_epoch, round as u8);
        if routes.is_empty() { break; }
        traverse_routes_profiled(tt, prev, &mut curr, &routes, departure, max_time, &mut stats)?;
        relax_walks_profiled(tt, &mut curr, max_time, &mut stats);
        marked_stops = curr.iter().zip(prev).enumerate()
            .filter_map(|(idx, (new, old))| (new.time < old.time).then_some(idx))
            .collect();
        labels.push(curr);
        if marked_stops.is_empty() { break; }
    }

    let mut journeys = Vec::new();
    if let Some(seconds) = request.direct_walk_seconds {
        if seconds <= request.max_duration_seconds {
            journeys.push(Journey {
                boardings: 0,
                departure_unix_seconds: request.departure_unix_seconds,
                arrival_unix_seconds: request.departure_unix_seconds + i64::from(seconds),
                legs: vec![Leg::DirectWalk {seconds}],
            });
        }
    }
    for (round, layer) in labels.iter().enumerate() {
        let best = request.egress.iter()
            .filter(|x| layer[x.stop.idx()].time + i64::from(x.seconds) <= max_time)
            .min_by_key(|x| layer[x.stop.idx()].time + i64::from(x.seconds));
        if let Some(dest) = best {
            if let Some(journey) = reconstruct(&labels, round, dest.stop.idx(),
                                               dest.seconds, departure) {
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
    Ok((frontier, stats))
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
            tt.location_transfer_seconds.push(0);
            tt.location_routes_offsets.push((tt.location_routes_offsets.len()) as u32);
            tt.location_routes.push(RouteIdx(0));
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
        }
        for i in 0..3 {
            tt.route_stops.push(PackedStop::new(LocationIdx(i), true,true,true,true).unwrap());
        }
        tt.route_stop_offsets.push(3);
        tt.route_transport_ranges.push(Range {from:0,to:1});
        tt.timing_profile_offsets.push(4);
        tt.timing_profile_events = vec![
            Delta::from_seconds(0).unwrap(), Delta::from_seconds(10 * 60 + 30).unwrap(),
            Delta::from_seconds(12 * 60 + 5).unwrap(), Delta::from_seconds(22 * 60 + 45).unwrap(),
        ];
        tt.route_clasz.push(10);
        tt.transport_route.push(RouteIdx(0));
        tt.transport_timing_profile.push(crate::routing_common::timetable::TimingProfileIdx(0));
        tt.transport_first_dep_offset.push(Delta::from_seconds(480 * 60 + 20).unwrap());
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

    fn multi_route_fixture() -> TimetableOwned {
        let mut tt = fixture();
        tt.coordinates.push(Default::default());
        tt.location_id.push(Default::default());
        tt.location_name.push(Default::default());
        tt.location_source.push(crate::routing_common::timetable::SourceIdx(0));
        tt.location_parent.push(LocationIdx::INVALID);
        tt.location_type.push(1);
        tt.location_transfer_seconds.push(0);
        tt.footpaths_out_offsets.push(0);
        tt.footpaths_in_offsets.push(0);

        // First trip 0 -> 1 -> 2 arrives at 08:23:05.
        // Second trip 2 -> 3 departs at 08:30 and arrives 08:35.
        tt.route_stops.extend([
            PackedStop::new(LocationIdx(2), true, true, true, true).unwrap(),
            PackedStop::new(LocationIdx(3), true, true, true, true).unwrap(),
        ]);
        tt.route_stop_offsets.push(5);
        tt.route_transport_ranges.push(Range { from: 1, to: 2 });
        tt.timing_profile_offsets.push(6);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(5 * 60).unwrap(),
        ]);
        tt.route_clasz.push(10);
        tt.route_traffic_days.push(BitfieldIdx(1));
        tt.transport_route.push(RouteIdx(1));
        tt.transport_timing_profile.push(crate::routing_common::timetable::TimingProfileIdx(1));
        tt.transport_first_dep_offset.push(Delta::from_seconds(510 * 60).unwrap());
        tt.transport_initial_day_offset.push(0);
        tt.transport_traffic_days.push(BitfieldIdx(1));
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.transport_to_trip_section_offsets.push(0);
        // CSR rows: 0->[0], 1->[0], 2->[0,1], 3->[1].
        tt.location_routes_offsets = vec![0, 1, 2, 4, 5];
        tt.location_routes = vec![RouteIdx(0), RouteIdx(0),
            RouteIdx(0), RouteIdx(1), RouteIdx(1)];
        tt
    }

    #[test]
    fn active_day_window_skips_outside_service_days() {
        let mut tt = fixture();
        // A three-day calendar with service only on day zero. Starting a
        // search on day two must not traverse stale day-zero transports.
        tt.day_count = 3;
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let departure = 20_002_i64 * 86_400 + 8 * 3_600;
        let (_journeys, stats) = route_with_stats(&view, &Query {
            departure_unix_seconds: departure,
            access: vec![WalkingEndpoint {stop: LocationIdx(0), seconds: 0}],
            egress: vec![WalkingEndpoint {stop: LocationIdx(2), seconds: 0}],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 3_600,
        }).unwrap();
        assert_eq!(stats.active_transport_days, 0);
        assert_eq!(stats.stop_events_scanned, 0);
    }

    #[test]
    fn day_window_keeps_exact_second_departures() {
        let tt = fixture();
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let scheduled = 20_000_i64 * 86_400 + 480 * 60 + 20;
        let (journeys, stats) = route_with_stats(&view, &Query {
            departure_unix_seconds: scheduled,
            access: vec![WalkingEndpoint {stop: LocationIdx(0), seconds: 0}],
            egress: vec![WalkingEndpoint {stop: LocationIdx(2), seconds: 0}],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 3_600,
        }).unwrap();
        assert_eq!(journeys.len(), 1);
        assert_eq!(stats.active_transport_days, 1);
        assert_eq!(stats.stop_events_scanned, 3);
    }

    #[test]
    fn marked_routes_select_only_incident_routes_without_duplicates() {
        let tt = multi_route_fixture();
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut stamps = vec![0; view.route_transport_ranges().len()];
        assert!(routes_from_marked(&view, &[], &mut stamps, 1).is_empty());
        assert_eq!(routes_from_marked(&view, &[0, 1, 0], &mut stamps, 1),
            vec![RouteIdx(0)]);
        assert_eq!(routes_from_marked(&view, &[3], &mut stamps, 2),
            vec![RouteIdx(1)]);
        assert_eq!(routes_from_marked(&view, &[2, 2], &mut stamps, 3),
            vec![RouteIdx(0), RouteIdx(1)]);
    }

    #[test]
    fn marked_scan_matches_exhaustive_rounds_and_reconstructs_connection() {
        let tt = multi_route_fixture();
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let dep = 20_000_i64 * 86_400 + 7 * 3_600 + 59 * 60;
        let max = dep + 2 * 3_600;
        let mut init = vec![Label { time: INF, parent: None, last_alight: None }; 4];
        improve(&mut init, 0, dep, Parent::Access { seconds: 0 }, None);
        relax_walks(&view, &mut init, max);

        let mut marked = vec![0];
        let mut stamps = vec![0; view.route_transport_ranges().len()];
        let mut optimized = init.clone();
        let mut exhaustive = init;
        for round in 1..=3 {
            let mut next_optimized = optimized.iter().map(|l| Label {
                time: l.time, parent: Some(Parent::Carry { previous_round: round - 1 }),
                last_alight: l.last_alight,
            }).collect::<Vec<_>>();
            let mut next_exhaustive = exhaustive.iter().map(|l| Label {
                time: l.time, parent: Some(Parent::Carry { previous_round: round - 1 }),
                last_alight: l.last_alight,
            }).collect::<Vec<_>>();
            let selected = routes_from_marked(&view, &marked, &mut stamps, round as u8);
            traverse_routes(&view, &optimized, &mut next_optimized, &selected, dep, max).unwrap();
            let all = [RouteIdx(0), RouteIdx(1)];
            traverse_routes(&view, &exhaustive, &mut next_exhaustive, &all, dep, max).unwrap();
            relax_walks(&view, &mut next_optimized, max);
            relax_walks(&view, &mut next_exhaustive, max);
            marked = next_optimized.iter().zip(&optimized).enumerate()
                .filter_map(|(i,(new,old))| (new.time < old.time).then_some(i)).collect();
            assert_eq!(
                next_optimized.iter().map(|l| (l.time, l.last_alight)).collect::<Vec<_>>(),
                next_exhaustive.iter().map(|l| (l.time, l.last_alight)).collect::<Vec<_>>(),
                "round {round} differs from exhaustive route scanning"
            );
            optimized = next_optimized;
            exhaustive = next_exhaustive;
        }
        let journeys = route(&view, &Query {
            departure_unix_seconds: dep,
            access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: 0 }],
            egress: vec![WalkingEndpoint { stop: LocationIdx(3), seconds: 0 }],
            direct_walk_seconds: None, max_boardings: 3, max_duration_seconds: 7200,
        }).unwrap();
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].boardings, 2);
        assert_eq!(journeys[0].arrival_unix_seconds,
            20_000_i64 * 86_400 + 8 * 3_600 + 35 * 60);
    }

    #[test]
    fn station_change_time_applies_after_ride_but_not_first_access() {
        let mut tt = fixture();
        tt.location_transfer_seconds[1] = 120;
        tt.location_transfer_seconds[2] = 150;
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let ride_arrival = Label {
            time: 1000, parent: None, last_alight: Some((1, 1000)),
        };
        assert!(!can_board(&view, ride_arrival, 1, 1119));
        assert!(can_board(&view, ride_arrival, 1, 1120));
        assert!(!can_board(&view, ride_arrival, 2, 1149));
        assert!(can_board(&view, ride_arrival, 2, 1150));
        let first_access = Label {
            time: 1000, parent: None, last_alight: None,
        };
        assert!(can_board(&view, first_access, 1, 1000));
    }

    #[test]
    fn gtfs_stop_pair_override_and_prohibition_are_directional() {
        use crate::routing_common::timetable::StationTransferRule;
        let mut tt = fixture();
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(0), to: LocationIdx(1),
            min_seconds: 0, flags: 1,
        });
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(1), to: LocationIdx(2),
            min_seconds: 240, flags: 0,
        });
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let from_zero = Label { time: 1000, parent: None, last_alight: Some((0, 1000)) };
        assert!(!can_board(&view, from_zero, 1, 2000));
        let from_one = Label { time: 1000, parent: None, last_alight: Some((1, 1000)) };
        assert!(!can_board(&view, from_one, 2, 1239));
        assert!(can_board(&view, from_one, 2, 1240));
        let from_two = Label { time: 1000, parent: None, last_alight: Some((2, 1000)) };
        assert!(can_board(&view, from_two, 1, 1120));
    }

    #[test]
    fn walking_transfer_retains_subminute_seconds() {
        use crate::routing_common::timetable::PackedFootpath;
        let mut tt = fixture();
        // Directed 75-second footpath 0 -> 1. No rounding to two minutes.
        tt.footpaths_out_offsets = vec![0, 1, 1, 1];
        tt.footpaths_out = vec![PackedFootpath::new(LocationIdx(1), 75).unwrap()];
        tt.footpaths_in_offsets = vec![0, 0, 1, 1];
        tt.footpaths_in = vec![PackedFootpath::new(LocationIdx(0), 75).unwrap()];
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let departure = 20_000_i64 * 86_400 + 3;
        let journeys = route(&view, &Query {
            departure_unix_seconds: departure,
            access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: 17 }],
            egress: vec![WalkingEndpoint { stop: LocationIdx(1), seconds: 23 }],
            direct_walk_seconds: None,
            max_boardings: 0,
            max_duration_seconds: 180,
        }).unwrap();
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].arrival_unix_seconds, departure + 17 + 75 + 23);
    }

    #[test]
    fn walking_preserves_alighting_provenance_for_pair_rules() {
        use crate::routing_common::timetable::{PackedFootpath, StationTransferRule};
        let mut tt = fixture();
        tt.footpaths_out_offsets = vec![0, 0, 1, 1];
        tt.footpaths_out = vec![PackedFootpath::new(LocationIdx(2), 25).unwrap()];
        tt.footpaths_in_offsets = vec![0, 0, 0, 1];
        tt.footpaths_in = vec![PackedFootpath::new(LocationIdx(1), 25).unwrap()];
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(0), to: LocationIdx(2), min_seconds: 0, flags: 1,
        });
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut labels = vec![Label { time: INF, parent: None, last_alight: None }; 3];
        labels[1] = Label {
            time: 1000, parent: None, last_alight: Some((0, 1000)),
        };
        relax_walks(&view, &mut labels, 2000);
        assert_eq!(labels[2].time, 1025);
        assert_eq!(labels[2].last_alight, Some((0, 1000)));
        assert!(!can_board(&view, labels[2], 2, 2000));
    }

    #[test]
    fn boarding_uses_exact_departure_second() {
        let tt = fixture();
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        let first_departure = 20_000_i64 * 86_400 + 480 * 60 + 20;
        let search = |departure, access| route(&view, &Query {
            departure_unix_seconds: departure,
            access: vec![WalkingEndpoint { stop: LocationIdx(0), seconds: access }],
            egress: vec![WalkingEndpoint { stop: LocationIdx(2), seconds: 0 }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 3_600,
        }).unwrap();
        assert_eq!(search(first_departure - 17, 17).len(), 1);
        assert!(search(first_departure - 17, 18).is_empty());
    }

    #[test]
    fn route_with_one_vehicle_returns_one_boarding() {
        let tt = fixture();
        let binary = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&binary).unwrap();
        let input = Query {
            departure_unix_seconds: 20_000_i64*86_400 + 475*60 + 33,
            access:vec![WalkingEndpoint {stop:LocationIdx(0),seconds:2 * 60 + 10}],
            egress:vec![WalkingEndpoint {stop:LocationIdx(2),seconds:3 * 60 + 15}],
            direct_walk_seconds: None,
            max_boardings:3, max_duration_seconds:120 * 60,
        };
        let journeys = route(&view,&input).unwrap();
        assert_eq!(journeys.len(),1);
        assert_eq!(journeys[0].boardings,1);
        assert_eq!(journeys[0].arrival_unix_seconds,
            20_000_i64*86_400 + 505*60 + 80);
    }
}
