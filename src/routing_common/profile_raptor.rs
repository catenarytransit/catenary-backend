//! Incremental timetable profile search using Gentian's compressed FIFO columns.
//!
//! The route/transport indices, time-offset patterns, and traffic-day bitmaps
//! remain mmap-backed. Unlike repeated point queries, labels are retained
//! across monotonically ordered start events, and only improved stops mark routes.
//!
//! Crucially, parent paths are immutable arena nodes. A later start can never
//! rewrite an earlier journey's predecessor chain. This mirrors Nigiri's split
//! between persistent round arrivals and event-local search state, without
//! depending on compressed timetable materialization.

use crate::routing_common::raptor_query::{
    Journey, Leg, Query, RaptorStats, for_each_route_service_day, get_earliest_transport,
    get_latest_transport, optimize_same_trip_endpoint_walks, transport_event_second,
};
use crate::routing_common::timetable::{LocationIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use ahash::AHashMap;
use anyhow::{Result, ensure};
use std::time::{Duration, Instant};

const INF: i64 = i64::MAX / 4;
const NEG_INF: i64 = i64::MIN / 4;
/// Bound immutable-path allocations; exhausting it produces an explicitly
/// incomplete profile rather than unbounded memory consumption.
const MAX_PATH_NODES: usize = 1_000_000;

/// Sparse stop frontiers avoid allocating an empty Vec for every stop in
/// every round of a continental-scale timetable.
type Round = AHashMap<usize, Vec<Label>>;

#[derive(Clone, Copy, Debug)]
struct Label {
    time: i64,
    // Immutable prefix (forward) or suffix (reverse).
    path: Option<usize>,
    // Last alighting stop and time (forward), or next boarding stop and time
    // (reverse); these are required for directional station transfer rules.
    transit: Option<(usize, i64)>,
    // Actual departure at origin (forward) or arrival at destination (reverse)
    // once the first vehicle has been chosen. For zero-ride root labels this
    // is the requested origin/deadline event and is refined on boarding.
    anchor: i64,
    // Exact number of boarded vehicles, even for labels carried to later rounds.
    rides: u8,
}

#[derive(Clone)]
struct PathNode {
    leg: Leg,
    next: Option<usize>,
}

fn append_path(nodes: &mut Vec<PathNode>, leg: Leg, next: Option<usize>) -> usize {
    let id = nodes.len();
    nodes.push(PathNode { leg, next });
    id
}

/// A sufficient (deliberately conservative) transfer-feasibility dominance
/// relation. Equal arrival times alone cannot dominate a label reached from a
/// different last-alighting stop: stop-pair GTFS transfer constraints vary with
/// the origin of the transfer. The reverse case is symmetric.
///
/// In addition to journey Pareto dimensions, the last alighting/next boarding
/// event must be no worse. An earlier arrival with a *later* last alighting
/// timestamp can have a worse departure-ready time after walking.
fn label_dominates(a: &Label, b: &Label, arrive_by: bool) -> bool {
    if a.rides > b.rides {
        return false;
    }
    let compatible = match (a.transit, b.transit) {
        (None, None) => true,
        (Some((a_stop, a_time)), Some((b_stop, b_time))) if a_stop == b_stop => {
            if arrive_by {
                a_time >= b_time
            } else {
                a_time <= b_time
            }
        }
        _ => false,
    };
    compatible
        && if arrive_by {
            a.time >= b.time && a.anchor <= b.anchor
        } else {
            a.time <= b.time && a.anchor >= b.anchor
        }
}

/// An insertion affects the next RAPTOR round only when it extends the
/// non-dominated skyline. The caller's path remains an immutable arena link.
fn insert_label(round: &mut Round, stop: usize, label: Label, arrive_by: bool) -> bool {
    let frontier = round.entry(stop).or_default();
    if frontier
        .iter()
        .any(|existing| label_dominates(existing, &label, arrive_by))
    {
        return false;
    }
    frontier.retain(|existing| !label_dominates(&label, existing, arrive_by));
    frontier.push(label);
    true
}

/// Materialize an immutable leg only after its candidate is known to survive
/// provenance-sensitive dominance; dominated candidates allocate no nodes.
fn insert_path_label(
    round: &mut Round,
    stop: usize,
    mut candidate: Label,
    leg: Leg,
    predecessor: Option<usize>,
    nodes: &mut Vec<PathNode>,
    arrive_by: bool,
    stats: &mut RaptorStats,
    changed: &mut Vec<usize>,
) -> Option<Label> {
    let frontier = round.entry(stop).or_default();
    if frontier
        .iter()
        .any(|existing| label_dominates(existing, &candidate, arrive_by))
    {
        stats.labels_dominated += 1;
        return None;
    }
    if nodes.len() >= MAX_PATH_NODES {
        stats.profile_budget_exhausted = true;
        return None;
    }
    candidate.path = Some(append_path(nodes, leg, predecessor));
    frontier.retain(|existing| !label_dominates(&candidate, existing, arrive_by));
    frontier.push(candidate);
    changed.push(stop);
    stats.labels_improved += 1;
    Some(candidate)
}

#[derive(Clone, Copy)]
struct Onboard {
    transport: TransportIdx,
    from: usize,
    event_time: i64,
    position: usize,
    predecessor: usize,
    anchor: i64,
    rides: u8,
}

/// FIFO transport columns permit pruning already-boarded alternatives *after*
/// their transfer-feasibility checks. Earlier vehicles are no later downstream;
/// the anchor/boarding dimensions must also be no worse.
fn onboard_dominates(a: &Onboard, b: &Onboard, arrive_by: bool) -> bool {
    a.rides <= b.rides
        && if arrive_by {
            a.transport.0 >= b.transport.0 && a.anchor <= b.anchor
        } else {
            a.transport.0 <= b.transport.0 && a.anchor >= b.anchor
        }
}

fn insert_onboard(candidates: &mut Vec<Onboard>, candidate: Onboard, arrive_by: bool) {
    if candidates
        .iter()
        .any(|existing| onboard_dominates(existing, &candidate, arrive_by))
    {
        return;
    }
    candidates.retain(|existing| !onboard_dominates(&candidate, existing, arrive_by));
    candidates.push(candidate);
}

/// For the first boarding, the requested search anchor is replaced with the
/// *actual* latest departure / earliest arrival implied by the booked train
/// and the OSR access or egress duration. This prevents raw query timestamps
/// from incorrectly dominating otherwise valid journeys.
fn first_vehicle_anchor(
    nodes: &[PathNode],
    predecessor: usize,
    label: Label,
    event_time: i64,
    arrive_by: bool,
) -> Option<i64> {
    if label.rides > 0 {
        return Some(label.anchor);
    }
    match (&nodes.get(predecessor)?.leg, arrive_by) {
        (Leg::Access { seconds, .. }, false) => event_time.checked_sub(i64::from(*seconds)),
        (Leg::Egress { seconds, .. }, true) => event_time.checked_add(i64::from(*seconds)),
        _ => None,
    }
}

fn marked_routes(
    tt: &ZeroCopyTimetable<'_>,
    marked: &[usize],
    stamps: &mut [u32],
    epoch: &mut u32,
) -> Vec<RouteIdx> {
    *epoch = epoch.wrapping_add(1);
    if *epoch == 0 {
        stamps.fill(0);
        *epoch = 1;
    }
    let mut routes = Vec::new();
    for &loc in marked {
        for &route in tt.routes_at(LocationIdx(loc as u32)) {
            if stamps[route.idx()] != *epoch {
                stamps[route.idx()] = *epoch;
                routes.push(route);
            }
        }
    }
    routes.sort_unstable_by_key(|r| r.0);
    routes
}

fn forward_ready(tt: &ZeroCopyTimetable<'_>, label: Label, stop: usize) -> Option<i64> {
    if label.time == INF {
        return None;
    }
    let Some((from, alight)) = label.transit else {
        return Some(label.time);
    };
    let rule = tt.station_transfer_rule(LocationIdx(from as u32), LocationIdx(stop as u32));
    if rule.is_some_and(|r| r.prohibited()) {
        return None;
    }
    let change = tt.location_transfer_seconds()[from]
        .max(tt.location_transfer_seconds()[stop])
        .max(rule.map_or(0, |r| r.min_seconds));
    Some(label.time.max(alight.saturating_add(i64::from(change))))
}

fn reverse_ready(tt: &ZeroCopyTimetable<'_>, label: Label, stop: usize) -> Option<i64> {
    if label.time == NEG_INF {
        return None;
    }
    let Some((next, departure)) = label.transit else {
        return Some(label.time);
    };
    let rule = tt.station_transfer_rule(LocationIdx(stop as u32), LocationIdx(next as u32));
    if rule.is_some_and(|r| r.prohibited()) {
        return None;
    }
    let change = tt.location_transfer_seconds()[stop]
        .max(tt.location_transfer_seconds()[next])
        .max(rule.map_or(0, |r| r.min_seconds));
    Some(label.time.min(departure.saturating_sub(i64::from(change))))
}

fn reconstruct_forward(
    nodes: &[PathNode],
    label: Label,
    stop: usize,
    egress: u32,
) -> Option<Journey> {
    let mut legs = vec![Leg::Egress {
        from_stop: stop as u32,
        seconds: egress,
    }];
    let mut link = label.path?;
    let mut steps = 0usize;
    loop {
        let node = nodes.get(link)?;
        legs.push(node.leg.clone());
        steps += 1;
        if steps > 128 {
            return None;
        }
        match node.next {
            Some(id) => link = id,
            None => break,
        }
    }
    legs.reverse();
    if !matches!(legs.first(), Some(Leg::Access { .. })) {
        return None;
    }
    let boardings = legs
        .iter()
        .filter(|l| matches!(l, Leg::Ride { .. }))
        .count();
    Some(Journey {
        boardings,
        departure_unix_seconds: label.anchor,
        arrival_unix_seconds: label.time.saturating_add(i64::from(egress)),
        legs,
    })
}

fn reconstruct_reverse(
    nodes: &[PathNode],
    label: Label,
    stop: usize,
    access: u32,
) -> Option<Journey> {
    let mut legs = vec![Leg::Access {
        to_stop: stop as u32,
        seconds: access,
    }];
    let mut link = label.path?;
    let mut steps = 0usize;
    loop {
        let node = nodes.get(link)?;
        legs.push(node.leg.clone());
        steps += 1;
        if steps > 128 {
            return None;
        }
        match node.next {
            Some(id) => link = id,
            None => break,
        }
    }
    if !matches!(legs.last(), Some(Leg::Egress { .. })) {
        return None;
    }
    let boardings = legs
        .iter()
        .filter(|l| matches!(l, Leg::Ride { .. }))
        .count();
    Some(Journey {
        boardings,
        departure_unix_seconds: label.time.saturating_sub(i64::from(access)),
        arrival_unix_seconds: label.anchor,
        legs,
    })
}

/// Validate every linked path against actual compressed timetable events,
/// calendar bits, ordered walking legs, and transfer-time constraints.
/// Reconstruction should never silently yield a non-existent trip.
fn valid_journey(tt: &ZeroCopyTimetable<'_>, request: &Query, j: &Journey) -> bool {
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

fn scan_routes(
    tt: &ZeroCopyTimetable<'_>,
    previous: &Round,
    current: &mut Round,
    nodes: &mut Vec<PathNode>,
    routes: &[RouteIdx],
    lower: i64,
    upper: i64,
    arrive_by: bool,
    stats: &mut RaptorStats,
    changed: &mut Vec<usize>,
    ride_seeds: &mut Vec<(usize, Label)>,
) {
    let started = Instant::now();
    for &route in routes {
        if stats.profile_budget_exhausted {
            break;
        }
        stats.routes_scanned += 1;
        let stops = tt.stops_on(route);
        if stops.len() < 2 {
            continue;
        }
        for_each_route_service_day(tt, route, lower, upper, stats, |day, stats| {
            if stats.profile_budget_exhausted {
                return;
            }
            let midnight = (i64::from(tt.header().first_utc_day) + day as i64) * 86_400;
            let mut onboard: Vec<Onboard> = Vec::new();
            let positions: Vec<usize> = if arrive_by {
                (0..stops.len()).rev().collect()
            } else {
                (0..stops.len()).collect()
            };
            for pos in positions {
                if stats.profile_budget_exhausted {
                    break;
                }
                stats.stop_events_scanned += 1;
                let stop = stops[pos];
                let loc = stop.location().idx();
                let can_finish = if arrive_by {
                    stop.outbound() && pos + 1 < stops.len()
                } else {
                    stop.inbound() && pos > 0
                };
                if can_finish {
                    let mut proposals = Vec::new();
                    for riding in &onboard {
                        let event = transport_event_second(
                            tt,
                            route,
                            riding.transport,
                            pos,
                            !arrive_by,
                            midnight,
                        );
                        let Some(event) = event else { continue };
                        if event < lower || event > upper {
                            continue;
                        }
                        let anchor = riding.anchor;
                        let time = event;
                        let transit = Some((loc, event));
                        let leg = if arrive_by {
                            Leg::Ride {
                                from_stop: loc as u32,
                                to_stop: riding.from as u32,
                                route: route.0,
                                transport: riding.transport.0,
                                service_day: day as u16,
                                board_stop_index: pos,
                                alight_stop_index: riding.position,
                                departure_unix_seconds: event,
                                arrival_unix_seconds: riding.event_time,
                            }
                        } else {
                            Leg::Ride {
                                from_stop: riding.from as u32,
                                to_stop: loc as u32,
                                route: route.0,
                                transport: riding.transport.0,
                                service_day: day as u16,
                                board_stop_index: riding.position,
                                alight_stop_index: pos,
                                departure_unix_seconds: riding.event_time,
                                arrival_unix_seconds: event,
                            }
                        };
                        proposals.push((
                            Label {
                                time,
                                path: None,
                                transit,
                                anchor,
                                rides: riding.rides,
                            },
                            leg,
                            riding.predecessor,
                        ));
                    }
                    for (candidate, leg, predecessor) in proposals {
                        if let Some(label) = insert_path_label(
                            current,
                            loc,
                            candidate,
                            leg,
                            Some(predecessor),
                            nodes,
                            arrive_by,
                            stats,
                            changed,
                        ) {
                            ride_seeds.push((loc, label));
                        }
                        if stats.profile_budget_exhausted {
                            break;
                        }
                    }
                }
                let can_start = if arrive_by {
                    stop.inbound() && pos > 0
                } else {
                    stop.outbound() && pos + 1 < stops.len()
                };
                if !can_start {
                    continue;
                }
                if let Some(labels) = previous.get(&loc) {
                    for &label in labels {
                        let ready = if arrive_by {
                            reverse_ready(tt, label, loc)
                        } else {
                            forward_ready(tt, label, loc)
                        };
                        let Some(ready) = ready else {
                            continue;
                        };
                        if ready < lower || ready > upper {
                            continue;
                        }
                        let option = if arrive_by {
                            get_latest_transport(tt, route, pos, day, ready, stats)
                        } else {
                            get_earliest_transport(tt, route, pos, day, ready, stats)
                        };
                        let Some((transport, event_time)) = option else {
                            continue;
                        };
                        if event_time < lower || event_time > upper {
                            continue;
                        }
                        let Some(predecessor) = label.path else {
                            continue;
                        };
                        let Some(anchor) =
                            first_vehicle_anchor(nodes, predecessor, label, event_time, arrive_by)
                        else {
                            continue;
                        };
                        insert_onboard(
                            &mut onboard,
                            Onboard {
                                transport,
                                from: loc,
                                event_time,
                                position: pos,
                                predecessor,
                                anchor,
                                rides: label.rides.saturating_add(1),
                            },
                            arrive_by,
                        );
                    }
                }
            }
        });
    }
    stats.route_scan_ms += started.elapsed().as_secs_f64() * 1000.0;
}

/// Reuse arrival labels across departure events, collecting only those
/// destination labels improved by the current event. The global scan horizon
/// is conservative for the entire interval; per-journey duration is checked
/// after immutable reconstruction, so prior labels never invalidate a query.
pub(crate) fn route_profile_incremental(
    tt: &ZeroCopyTimetable<'_>,
    request: &Query,
    anchors: &[i64],
    arrive_by: bool,
    budget: Duration,
) -> Result<(Vec<Journey>, RaptorStats)> {
    ensure!(
        request.max_boardings <= 16,
        "max_boardings exceeds supported query limit"
    );
    ensure!(
        (1..=7 * 86_400).contains(&request.max_duration_seconds),
        "max_duration_seconds must be between 1 second and 7 days"
    );
    ensure!(
        anchors.windows(2).all(|w| w[0] <= w[1]),
        "incremental anchors must be ascending"
    );
    let n = tt.coordinates().len();
    for endpoint in request.access.iter().chain(&request.egress) {
        ensure!(endpoint.stop.idx() < n, "access/egress stop out of range");
    }
    let mut stats = RaptorStats::default();
    if anchors.is_empty() || request.max_boardings == 0 {
        return Ok((vec![], stats));
    }
    stats.profile_searches = 1;
    let lower = if arrive_by {
        anchors[0].saturating_sub(i64::from(request.max_duration_seconds))
    } else {
        anchors[0]
    };
    let upper = if arrive_by {
        *anchors.last().unwrap()
    } else {
        anchors
            .last()
            .unwrap()
            .saturating_add(i64::from(request.max_duration_seconds))
    };

    let mut rounds = vec![Round::default(); request.max_boardings + 1];
    let mut nodes = Vec::new();
    let mut stamps = vec![0u32; tt.route_transport_ranges().len()];
    let mut epoch = 0u32;
    let started = Instant::now();
    let mut journeys = Vec::new();
    let ordered: Vec<i64> = if arrive_by {
        anchors.to_vec()
    } else {
        anchors.iter().rev().copied().collect()
    };
    for (event_idx, anchor) in ordered.into_iter().enumerate() {
        if event_idx != 0 && started.elapsed() >= budget {
            stats.profile_budget_exhausted = true;
            break;
        }
        stats.profile_anchor_events += 1;
        let mut marked = Vec::new();
        let initial = if arrive_by {
            &request.egress
        } else {
            &request.access
        };
        for ep in initial {
            let stop = ep.stop.idx();
            let time = if arrive_by {
                anchor.saturating_sub(i64::from(ep.seconds))
            } else {
                anchor.saturating_add(i64::from(ep.seconds))
            };
            if time < lower || time > upper {
                continue;
            }
            let candidate = Label {
                time,
                path: None,
                transit: None,
                anchor,
                rides: 0,
            };
            let leg = if arrive_by {
                Leg::Egress {
                    from_stop: ep.stop.0,
                    seconds: ep.seconds,
                }
            } else {
                Leg::Access {
                    to_stop: ep.stop.0,
                    seconds: ep.seconds,
                }
            };
            insert_path_label(
                &mut rounds[0],
                stop,
                candidate,
                leg,
                None,
                &mut nodes,
                arrive_by,
                &mut stats,
                &mut marked,
            );
        }
        marked.sort_unstable();
        marked.dedup();
        let mut changed_ends: Vec<(usize, Label, usize, u32)> = Vec::new();
        for k in 1..=request.max_boardings {
            if marked.is_empty() || stats.profile_budget_exhausted {
                break;
            }
            stats.rounds += 1;
            stats.marked_stops += marked.len() as u64;
            let (earlier, later) = rounds.split_at_mut(k);
            let prev = &earlier[k - 1];
            let curr = &mut later[0];
            let mut changed = Vec::new();
            for &stop in &marked {
                if let Some(labels) = prev.get(&stop) {
                    for &label in labels {
                        if insert_label(curr, stop, label, arrive_by) {
                            changed.push(stop);
                        }
                    }
                }
            }
            let routes = marked_routes(tt, &marked, &mut stamps, &mut epoch);
            let mut ride_seeds = Vec::new();
            scan_routes(
                tt,
                prev,
                curr,
                &mut nodes,
                &routes,
                lower,
                upper,
                arrive_by,
                &mut stats,
                &mut changed,
                &mut ride_seeds,
            );
            // Walk propagation is intentionally one hop: Gentian constructs a
            // transfer closure. Each improved *provenance* remains independent.
            for (stop, source) in ride_seeds {
                if stats.profile_budget_exhausted {
                    break;
                }
                let Some(predecessor) = source.path else {
                    continue;
                };
                let feet = if arrive_by {
                    tt.footpaths_to(LocationIdx(stop as u32))
                } else {
                    tt.footpaths_from(LocationIdx(stop as u32))
                };
                for edge in feet {
                    stats.footpath_edges_examined += 1;
                    let target = edge.target().idx();
                    let secs = edge.seconds();
                    if secs == 0 {
                        continue;
                    }
                    let time = if arrive_by {
                        source.time.saturating_sub(i64::from(secs))
                    } else {
                        source.time.saturating_add(i64::from(secs))
                    };
                    if time < lower || time > upper {
                        continue;
                    }
                    let leg = if arrive_by {
                        Leg::Transfer {
                            from_stop: target as u32,
                            to_stop: stop as u32,
                            seconds: secs,
                        }
                    } else {
                        Leg::Transfer {
                            from_stop: stop as u32,
                            to_stop: target as u32,
                            seconds: secs,
                        }
                    };
                    insert_path_label(
                        curr,
                        target,
                        Label {
                            time,
                            path: None,
                            ..source
                        },
                        leg,
                        Some(predecessor),
                        &mut nodes,
                        arrive_by,
                        &mut stats,
                        &mut changed,
                    );
                    if stats.profile_budget_exhausted {
                        break;
                    }
                }
            }
            changed.sort_unstable();
            changed.dedup();
            let endpoints = if arrive_by {
                &request.access
            } else {
                &request.egress
            };
            for ep in endpoints {
                if changed.binary_search(&ep.stop.idx()).is_err() {
                    continue;
                }
                if let Some(labels) = curr.get(&ep.stop.idx()) {
                    for &label in labels {
                        changed_ends.push((k, label, ep.stop.idx(), ep.seconds));
                    }
                }
            }
            marked = changed;
        }
        for (_round, label, stop, offset) in changed_ends {
            let Some(mut journey) = (if arrive_by {
                reconstruct_reverse(&nodes, label, stop, offset)
            } else {
                reconstruct_forward(&nodes, label, stop, offset)
            }) else {
                continue;
            };
            if !arrive_by {
                optimize_same_trip_endpoint_walks(tt, request, &mut journey);
            }
            if valid_journey(tt, request, &journey) {
                journeys.push(journey);
            }
        }
        if stats.profile_budget_exhausted {
            break;
        }
    }
    Ok((
        crate::routing_common::profile_search::departure_aware_pareto(journeys),
        stats,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn different_last_alighting_stops_cannot_dominate_each_other() {
        let early_restricted = Label {
            time: 100,
            path: None,
            transit: Some((1, 100)),
            anchor: 50,
            rides: 1,
        };
        let later_allowed = Label {
            time: 105,
            path: None,
            transit: Some((2, 105)),
            anchor: 50,
            rides: 1,
        };
        let mut frontier = Round::default();
        assert!(insert_label(&mut frontier, 3, early_restricted, false));
        assert!(insert_label(&mut frontier, 3, later_allowed, false));
        assert_eq!(frontier[&3].len(), 2);
    }

    #[test]
    fn different_next_boarding_stops_survive_reverse_dominance() {
        let later_restricted = Label {
            time: 200,
            path: None,
            transit: Some((7, 200)),
            anchor: 250,
            rides: 1,
        };
        let earlier_allowed = Label {
            time: 195,
            path: None,
            transit: Some((8, 195)),
            anchor: 250,
            rides: 1,
        };
        let mut frontier = Round::default();
        assert!(insert_label(&mut frontier, 3, later_restricted, true));
        assert!(insert_label(&mut frontier, 3, earlier_allowed, true));
        assert_eq!(frontier[&3].len(), 2);
    }

    #[test]
    fn travel_time_and_origin_time_form_a_two_dimensional_frontier() {
        let mut frontier = Round::default();
        let late_slow = Label {
            time: 400,
            path: None,
            transit: None,
            anchor: 350,
            rides: 0,
        };
        let early_fast = Label {
            time: 390,
            path: None,
            transit: None,
            anchor: 300,
            rides: 0,
        };
        assert!(insert_label(&mut frontier, 1, late_slow, false));
        assert!(insert_label(&mut frontier, 1, early_fast, false));
        assert_eq!(frontier[&1].len(), 2);
        let late_fast = Label {
            time: 380,
            path: None,
            transit: None,
            anchor: 350,
            rides: 0,
        };
        assert!(insert_label(&mut frontier, 1, late_fast, false));
        assert_eq!(frontier[&1].len(), 1);
    }

    #[test]
    fn transfer_ready_time_depends_on_last_alighting_timestamp() {
        let earlier_walk = Label {
            time: 120,
            path: None,
            transit: Some((1, 115)),
            anchor: 50,
            rides: 1,
        };
        let later_alight = Label {
            time: 120,
            path: None,
            transit: Some((1, 120)),
            anchor: 50,
            rides: 1,
        };
        assert!(label_dominates(&earlier_walk, &later_alight, false));
        assert!(!label_dominates(&later_alight, &earlier_walk, false));
    }

    #[test]
    fn fewer_boardings_remain_independent_of_profile_round() {
        let mut frontier = Round::default();
        let one_boarding = Label {
            time: 130,
            path: None,
            transit: Some((1, 130)),
            anchor: 50,
            rides: 1,
        };
        let two_boardings = Label {
            time: 120,
            path: None,
            transit: Some((1, 120)),
            anchor: 50,
            rides: 2,
        };
        assert!(insert_label(&mut frontier, 1, one_boarding, false));
        assert!(insert_label(&mut frontier, 1, two_boardings, false));
        assert_eq!(frontier[&1].len(), 2);
    }

    #[test]
    fn immutable_prefix_and_suffix_survive_later_improvements() {
        let mut nodes = Vec::new();
        let first = append_path(
            &mut nodes,
            Leg::Access {
                to_stop: 1,
                seconds: 30,
            },
            None,
        );
        let ride = append_path(
            &mut nodes,
            Leg::Ride {
                from_stop: 1,
                to_stop: 2,
                route: 0,
                transport: 0,
                service_day: 0,
                board_stop_index: 0,
                alight_stop_index: 1,
                departure_unix_seconds: 100,
                arrival_unix_seconds: 150,
            },
            Some(first),
        );
        let preserved = reconstruct_forward(
            &nodes,
            Label {
                time: 150,
                path: Some(ride),
                transit: None,
                anchor: 50,
                rides: 1,
            },
            2,
            0,
        )
        .unwrap();
        let _later = append_path(
            &mut nodes,
            Leg::Access {
                to_stop: 1,
                seconds: 1,
            },
            None,
        );
        assert!(matches!(preserved.legs[0], Leg::Access { seconds: 30, .. }));
        assert!(matches!(
            preserved.legs[1],
            Leg::Ride {
                departure_unix_seconds: 100,
                ..
            }
        ));
    }

    #[test]
    fn reconstruction_rejects_cycles_and_missing_nodes() {
        let nodes = vec![PathNode {
            leg: Leg::Access {
                to_stop: 0,
                seconds: 0,
            },
            next: Some(5),
        }];
        let label = Label {
            time: 100,
            path: Some(0),
            transit: None,
            anchor: 0,
            rides: 0,
        };
        assert!(reconstruct_forward(&nodes, label, 0, 0).is_none());
    }
}
