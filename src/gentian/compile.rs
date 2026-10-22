use super::external_assembly::ScheduleRuns;
use anyhow::{Context, Result, ensure};
use catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto;
use catenary::routing_common::routing_regions::ChateauSource;
use catenary::routing_common::timetable::{
    BitfieldIdx, Clasz, Coordinate, Delta, LocationIdx, PackedFootpath, PackedStop, Range,
    RouteIdx, TimetableOwned, TimingProfileIdx, TrafficDays, TransportIdx, TripIdx,
    TripTransportRange,
};
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use fallible_iterator::FallibleIterator;
use postgres::{Client, Row, types::ToSql};
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

#[derive(Clone)]
struct Stop {
    id: String,
    name: String,
    lat: f64,
    lon: f64,
    parent: Option<String>,
    kind: i16,
}

#[derive(Clone)]
pub(super) struct Event {
    pub(super) stop: LocationIdx,
    pub(super) arrival: i64,
    pub(super) departure: i64,
}

#[derive(Clone)]
struct Pattern {
    route_id: String,
    route_type: i16,
    timezone: Tz,
    stops: Vec<(String, i32, i32)>,
}

/// A negative value in any stored GTFS offset invalidates the entire pattern.
/// Dropping only one stop would silently change the route's stop sequence.
fn append_pattern_stop(
    pattern: &mut Pattern,
    stop_id: String,
    arrival: Option<i32>,
    departure: Option<i32>,
    interpolated: Option<i32>,
) -> Result<bool> {
    if [arrival, departure, interpolated]
        .into_iter()
        .flatten()
        .any(|offset| offset < 0)
    {
        pattern.stops.clear();
        return Ok(false);
    }
    let arrival = arrival
        .or(interpolated)
        .context("itinerary pattern has no arrival/interpolated time")?;
    let departure = departure
        .or(interpolated)
        .context("itinerary pattern has no departure/interpolated time")?;
    pattern.stops.push((stop_id, arrival, departure));
    Ok(true)
}

#[derive(Clone)]
pub(super) struct Schedule {
    pub(super) trip_id: String,
    pub(super) source: u16,
    pub(super) route_id: String,
    pub(super) route_type: i16,
    pub(super) locations: Vec<LocationIdx>,
    pub(super) events: Vec<Event>,
    pub(super) first_second: u32,
    pub(super) days: TrafficDays,
}

#[derive(Default)]
struct Calendar {
    weekdays: [bool; 7],
    range: Option<(NaiveDate, NaiveDate)>,
    exceptions: BTreeMap<NaiveDate, bool>,
}

fn query_source(
    client: &mut Client,
    sql: &str,
    source: &ChateauSource,
    mut on_row: impl FnMut(Row) -> Result<()>,
) -> Result<()> {
    let params: [&(dyn ToSql + Sync); 2] = [&source.onestop_feed_id, &source.attempt_id];
    let mut rows = client.query_raw(sql, params)?;
    while let Some(row) = rows.next()? {
        on_row(row)?;
    }
    Ok(())
}

fn as_u32(value: usize) -> Result<u32> {
    u32::try_from(value).context("compiled timetable exceeds 32-bit column index capacity")
}

fn load_stops(client: &mut Client, source: &ChateauSource) -> Result<Vec<Stop>> {
    let mut stops = Vec::new();
    query_source(
        client,
        "
        SELECT gtfs_id, COALESCE(displayname, name, gtfs_id),
               ST_Y(point)::float8, ST_X(point)::float8,
               parent_station, location_type
        FROM gtfs.stops
        WHERE onestop_feed_id = $1 AND attempt_id = $2
          AND point IS NOT NULL
        ORDER BY gtfs_id
    ",
        source,
        |r| {
            let lat: f64 = r.get(2);
            let lon: f64 = r.get(3);
            ensure!(
                lat.is_finite() && lon.is_finite(),
                "invalid stop coordinates"
            );
            stops.push(Stop {
                id: r.get(0),
                name: r.get(1),
                lat,
                lon,
                parent: r.get(4),
                kind: r.get(5),
            });
            Ok(())
        },
    )?;
    Ok(stops)
}

fn add_locations(
    tt: &mut TimetableOwned,
    source_idx: u16,
    stops: &[Stop],
) -> Result<HashMap<String, LocationIdx>> {
    let mut ids = HashMap::with_capacity(stops.len());
    for stop in stops {
        let idx = LocationIdx(as_u32(tt.coordinates.len())?);
        ids.insert(stop.id.clone(), idx);
        tt.coordinates.push(Coordinate {
            lat: stop.lat as f32,
            lon: stop.lon as f32,
        });
        let id_ref = tt.add_string(&stop.id)?;
        let name_ref = tt.add_string(&stop.name)?;
        tt.location_id.push(id_ref);
        tt.location_name.push(name_ref);
        tt.location_source
            .push(catenary::routing_common::timetable::SourceIdx(source_idx));
        tt.location_type.push(if stop.kind == 1 { 2 } else { 1 });
        tt.location_transfer_seconds
            .push(catenary::routing_common::timetable::DEFAULT_TRANSFER_SECONDS);
        tt.location_parent.push(LocationIdx::INVALID);
    }
    for stop in stops {
        if let Some(parent) = &stop.parent {
            if let (Some(&child), Some(&parent)) = (ids.get(&stop.id), ids.get(parent)) {
                tt.location_parent[child.idx()] = parent;
            }
        }
    }
    Ok(ids)
}

fn load_calendars(
    client: &mut Client,
    source: &ChateauSource,
) -> Result<HashMap<String, Calendar>> {
    let mut calendars = HashMap::<String, Calendar>::new();
    query_source(
        client,
        "
        SELECT service_id, monday, tuesday, wednesday, thursday, friday,
               saturday, sunday, gtfs_start_date::text, gtfs_end_date::text
        FROM gtfs.calendar
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ",
        source,
        |r| {
            calendars.insert(
                r.get(0),
                Calendar {
                    weekdays: [
                        r.get(1),
                        r.get(2),
                        r.get(3),
                        r.get(4),
                        r.get(5),
                        r.get(6),
                        r.get(7),
                    ],
                    range: Some((
                        NaiveDate::parse_from_str(&r.get::<_, String>(8), "%Y-%m-%d")?,
                        NaiveDate::parse_from_str(&r.get::<_, String>(9), "%Y-%m-%d")?,
                    )),
                    ..Default::default()
                },
            );
            Ok(())
        },
    )?;
    query_source(
        client,
        "
        SELECT service_id, gtfs_date::text, exception_type
        FROM gtfs.calendar_dates
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ",
        source,
        |r| {
            let date = NaiveDate::parse_from_str(&r.get::<_, String>(1), "%Y-%m-%d")?;
            let exception: i16 = r.get(2);
            ensure!(
                exception == 1 || exception == 2,
                "unknown GTFS calendar exception type"
            );
            calendars
                .entry(r.get(0))
                .or_default()
                .exceptions
                .insert(date, exception == 1);
            Ok(())
        },
    )?;
    Ok(calendars)
}

fn active(calendar: &Calendar, date: NaiveDate) -> bool {
    if let Some(v) = calendar.exceptions.get(&date) {
        return *v;
    }
    calendar
        .range
        .is_some_and(|(start, end)| start <= date && date <= end)
        && calendar.weekdays[date.weekday().num_days_from_monday() as usize]
}

fn utc_anchor(date: NaiveDate, tz: Tz) -> Result<i64> {
    let noon = date
        .and_hms_opt(12, 0, 0)
        .context("invalid GTFS service day")?;
    let utc = tz
        .from_local_datetime(&noon)
        .single()
        .context("timezone has ambiguous/nonexistent local noon")?
        .with_timezone(&Utc);
    Ok(utc.timestamp() - 12 * 3600)
}

/// GTFS times are elapsed seconds after the service day's "noon minus 12h".
/// Resolve the noon offset once per active service date. A departure at 25:00
/// remains attached to its original service date, even after a DST transition.
fn service_utc_anchors(
    calendar: &Calendar,
    tz: Tz,
    first_utc_day: i32,
    day_count: u32,
) -> Result<Vec<i64>> {
    let first_day = start_date(first_utc_day)?;
    let mut anchors = Vec::new();
    for offset in -5_i64..i64::from(day_count) + 5 {
        let service_date = first_day
            .checked_add_signed(Duration::days(offset))
            .context("service date out of range")?;
        if active(calendar, service_date) {
            anchors.push(utc_anchor(service_date, tz)?);
        }
    }
    Ok(anchors)
}

/// Like Nigiri's expand_frequencies, enumerate the departure times defined by
/// each GTFS frequency window. Keep seconds precision (Nigiri rounds to minutes)
/// and treat the end of the window as exclusive. For exact_times=0 these are
/// discretized headway-based departures, not guaranteed scheduled runs.
fn expand_frequency_departures(
    trip_id: &str,
    blob: &[u8],
    mut emit: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    let data = GtfsFrequenciesProto::decode(blob)
        .with_context(|| format!("decoding GTFS frequencies for trip {trip_id}"))?;
    ensure!(
        !data.frequencies.is_empty(),
        "trip {trip_id} is marked as frequency-based but has no frequency windows"
    );
    for (index, window) in data.frequencies.into_iter().enumerate() {
        ensure!(
            window.headway_secs != 0,
            "trip {trip_id} frequency window {index} has zero headway"
        );
        ensure!(
            window.start_time < window.end_time,
            "trip {trip_id} frequency window {index} has invalid time range"
        );
        let mut departure = window.start_time;
        while departure < window.end_time {
            emit(departure)?;
            departure = departure
                .checked_add(window.headway_secs)
                .with_context(|| format!("trip {trip_id} frequency departure overflow"))?;
        }
    }
    Ok(())
}

fn start_date(first_utc_day: i32) -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .context("invalid Unix epoch")?
        .checked_add_signed(Duration::days(i64::from(first_utc_day)))
        .context("UTC timetable day out of range")
}

fn make_schedules(
    trip_id: &str,
    source_idx: u16,
    pattern: &Pattern,
    stop_lookup: &HashMap<String, LocationIdx>,
    first_departure_secs: u32,
    calendar: &Calendar,
    first_utc_day: i32,
    day_count: u32,
) -> Result<Vec<Schedule>> {
    let anchors = service_utc_anchors(calendar, pattern.timezone, first_utc_day, day_count)?;
    make_schedules_from_anchors(
        trip_id,
        source_idx,
        pattern,
        stop_lookup,
        first_departure_secs,
        &anchors,
        first_utc_day,
        day_count,
    )
}

fn make_schedules_from_anchors(
    trip_id: &str,
    source_idx: u16,
    pattern: &Pattern,
    stop_lookup: &HashMap<String, LocationIdx>,
    first_departure_secs: u32,
    anchors: &[i64],
    first_utc_day: i32,
    day_count: u32,
) -> Result<Vec<Schedule>> {
    // The stored pattern is relative to the first stop's arrival time.
    // Normalize its events to the first departure so a frequency window's
    // start_time always refers to the departure from the first stop.
    ensure!(
        pattern.stops.len() >= 2,
        "trip {trip_id} needs at least two stops"
    );
    ensure!(
        pattern.stops.windows(2).all(|pair| pair[0].2 <= pair[1].1)
            && pattern
                .stops
                .iter()
                .all(|(stop, arr, dep)| { stop_lookup.contains_key(stop) && arr <= dep }),
        "invalid/nonmonotonic pattern for trip {trip_id}"
    );
    let first_departure_offset = i64::from(pattern.stops[0].2);
    let events = pattern
        .stops
        .iter()
        .map(|(id, arrival, departure)| {
            Ok(Event {
                stop: *stop_lookup.get(id).context("missing pattern stop")?,
                arrival: i64::from(*arrival) - first_departure_offset,
                departure: i64::from(*departure) - first_departure_offset,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let locations = events.iter().map(|event| event.stop).collect::<Vec<_>>();
    let mut result = BTreeMap::<u32, TrafficDays>::new();
    for &anchor in anchors {
        let departure = anchor
            .checked_add(i64::from(first_departure_secs))
            .context("GTFS departure timestamp overflow")?;
        let day = departure.div_euclid(86_400) - i64::from(first_utc_day);
        if !(0..i64::from(day_count)).contains(&day) {
            continue;
        }
        let second = u32::try_from(departure.rem_euclid(86_400))?;
        result.entry(second).or_default().set(day as usize, true)?;
    }
    Ok(result
        .into_iter()
        .map(|(first_second, days)| Schedule {
            trip_id: trip_id.to_owned(),
            source: source_idx,
            route_id: pattern.route_id.clone(),
            route_type: pattern.route_type,
            locations: locations.clone(),
            events: events.clone(),
            first_second,
            days,
        })
        .collect())
}

fn clasz(gtfs_route_type: i16) -> u8 {
    let mode = match gtfs_route_type {
        0 => Clasz::Tram,
        1 => Clasz::Subway,
        2 => Clasz::Regional,
        3 => Clasz::Bus,
        4 => Clasz::Ship,
        5 => Clasz::AerialLift,
        6 => Clasz::AerialLift,
        7 => Clasz::Funicular,
        _ => Clasz::Other,
    };
    mode as u8
}

fn intern_days(
    tt: &mut TimetableOwned,
    index: &mut HashMap<TrafficDays, BitfieldIdx>,
    days: TrafficDays,
) -> Result<BitfieldIdx> {
    if let Some(&idx) = index.get(&days) {
        return Ok(idx);
    }
    let idx = BitfieldIdx(as_u32(tt.bitfields.len())?);
    index.insert(days, idx);
    tt.bitfields.push(days);
    Ok(idx)
}

fn non_overtaking(a: &Schedule, b: &Schedule) -> bool {
    a.first_second <= b.first_second
        && a.events.iter().zip(&b.events).all(|(x, y)| {
            x.arrival + i64::from(a.first_second) <= y.arrival + i64::from(b.first_second)
                && x.departure + i64::from(a.first_second)
                    <= y.departure + i64::from(b.first_second)
        })
}

fn assemble_batch(
    tt: &mut TimetableOwned,
    schedules: Vec<Schedule>,
    days_index: &mut HashMap<TrafficDays, BitfieldIdx>,
    route_id_index: &mut HashMap<(u16, String), catenary::routing_common::timetable::RouteIdIdx>,
    profile_index: &mut HashMap<Vec<u32>, TimingProfileIdx>,
) -> Result<()> {
    ensure!(!schedules.is_empty(), "empty schedule batch");
    let source = schedules[0].source;
    let route_id = schedules[0].route_id.clone();
    let locations: Vec<u32> = schedules[0].locations.iter().map(|s| s.0).collect();
    ensure!(
        schedules.iter().all(|s| s.source == source
            && s.route_id == route_id
            && s.locations
                .iter()
                .map(|x| x.0)
                .eq(locations.iter().copied())),
        "schedule batch contains mixed route patterns"
    );
    {
        let mut chains = Vec::<Vec<Schedule>>::new();
        for schedule in schedules {
            if let Some(chain) = chains
                .iter_mut()
                .find(|chain| non_overtaking(chain.last().expect("nonempty chain"), &schedule))
            {
                chain.push(schedule);
            } else {
                chains.push(vec![schedule]);
            }
        }
        let public_route_id = if let Some(&idx) = route_id_index.get(&(source, route_id.clone())) {
            idx
        } else {
            let idx = tt.add_route_id(
                catenary::routing_common::timetable::SourceIdx(source),
                &route_id,
            )?;
            route_id_index.insert((source, route_id), idx);
            idx
        };
        for chain in chains {
            let route = RouteIdx(as_u32(tt.route_transport_ranges.len())?);
            tt.route_stop_offsets
                .push(as_u32(tt.route_stops.len() + locations.len())?);
            for &stop in &locations {
                tt.route_stops
                    .push(PackedStop::new(LocationIdx(stop), true, true, true, true)?);
            }
            let first = tt.transport_route.len();
            // Intern exact offset vectors across batches and RAPTOR chains.
            // The key is the encoded event sequence, never merely the stop list:
            // different running times on the same stops must remain distinct.
            let mut profiles = Vec::with_capacity(chain.len());
            for schedule in &chain {
                let mut encoded = Vec::with_capacity(schedule.events.len() * 2 - 2);
                for (i, event) in schedule.events.iter().enumerate() {
                    if i + 1 < schedule.events.len() {
                        ensure!(
                            event.departure >= 0
                                && event.departure <= i64::from(Delta::MAX_SECONDS),
                            "departure offset outside Delta representation"
                        );
                        encoded.push(Delta::from_seconds(u32::try_from(event.departure)?)?.0);
                    }
                    if i > 0 {
                        ensure!(
                            event.arrival >= 0 && event.arrival <= i64::from(Delta::MAX_SECONDS),
                            "arrival offset outside Delta representation"
                        );
                        encoded.push(Delta::from_seconds(u32::try_from(event.arrival)?)?.0);
                    }
                }
                // Interleaved event order: departure(0), arrival(1),
                // departure(1), ..., arrival(last).
                let profile = if let Some(&existing) = profile_index.get(&encoded) {
                    existing
                } else {
                    let idx = TimingProfileIdx(as_u32(tt.timing_profile_offsets.len() - 1)?);
                    tt.timing_profile_events
                        .extend(encoded.iter().copied().map(Delta));
                    tt.timing_profile_offsets
                        .push(as_u32(tt.timing_profile_events.len())?);
                    profile_index.insert(encoded, idx);
                    idx
                };
                profiles.push(profile);
            }
            let mut union = TrafficDays::default();
            for (schedule, profile) in chain.iter().zip(profiles) {
                tt.transport_timing_profile.push(profile);
                let idx = intern_days(tt, days_index, schedule.days)?;
                union.or_assign(&schedule.days);
                let transport = TransportIdx(as_u32(tt.transport_route.len())?);
                tt.transport_route.push(route);
                tt.transport_traffic_days.push(idx);
                tt.transport_first_dep_offset
                    .push(Delta::from_seconds(schedule.first_second)?);
                tt.transport_initial_day_offset.push(0);
                let trip_idx = TripIdx(as_u32(tt.trip_public_route_ids.len())?);
                tt.trip_public_route_ids.push(public_route_id);
                tt.add_trip_id(
                    catenary::routing_common::timetable::SourceIdx(source),
                    trip_idx,
                    &schedule.trip_id,
                )?;
                tt.trip_transport_ranges.push(TripTransportRange {
                    transport,
                    stop_from: 0,
                    stop_to: u16::try_from(locations.len())?,
                });
                tt.trip_transport_offsets
                    .push(as_u32(tt.trip_transport_ranges.len())?);
                tt.transport_to_trip_section_offsets
                    .push(as_u32(tt.transport_to_trip_section.len())?);
            }
            ensure!(
                tt.transport_route.len() == first + chain.len(),
                "transport indexing error"
            );
            tt.route_transport_ranges.push(Range {
                from: as_u32(first)?,
                to: as_u32(tt.transport_route.len())?,
            });
            tt.route_clasz.push(clasz(chain[0].route_type));
            let route_days = intern_days(tt, days_index, union)?;
            tt.route_traffic_days.push(route_days);
            tt.route_section_clasz_offsets
                .push(as_u32(tt.route_section_clasz.len())?);
            tt.section_flag_offsets
                .push(as_u32(tt.route_section_clasz.len())?);
        }
    }
    Ok(())
}

fn route_unique_stops(tt: &TimetableOwned, route: usize, locations: usize) -> Result<Vec<usize>> {
    let from = tt.route_stop_offsets[route] as usize;
    let to = tt.route_stop_offsets[route + 1] as usize;
    let mut stops = tt.route_stops[from..to]
        .iter()
        .map(|stop| stop.location().idx())
        .collect::<Vec<_>>();
    stops.sort_unstable();
    stops.dedup();
    ensure!(
        stops.iter().all(|&stop| stop < locations),
        "route references unknown stop"
    );
    Ok(stops)
}

/// Build stop-to-route CSR directly in the final V4 columns. A route visits
/// a location at most once in this inverted index, even if its pattern loops.
fn build_stop_routes_index(tt: &mut TimetableOwned) -> Result<()> {
    let locations = tt.coordinates.len();
    let mut counts = vec![0u32; locations];
    let routes = tt.route_transport_ranges.len();
    for route in 0..routes {
        for stop in route_unique_stops(tt, route, locations)? {
            counts[stop] = counts[stop]
                .checked_add(1)
                .context("stop-to-route index exceeds u32 range")?;
        }
    }
    tt.location_routes_offsets.clear();
    tt.location_routes_offsets.push(0);
    let mut total = 0u32;
    for count in counts {
        total = total
            .checked_add(count)
            .context("stop-to-route column exceeds u32 range")?;
        tt.location_routes_offsets.push(total);
    }
    tt.location_routes.clear();
    tt.location_routes.resize(total as usize, RouteIdx(0));
    let mut cursor = tt.location_routes_offsets[..locations]
        .iter()
        .map(|&offset| offset as usize)
        .collect::<Vec<_>>();
    for route in 0..routes {
        for stop in route_unique_stops(tt, route, locations)? {
            tt.location_routes[cursor[stop]] = RouteIdx(as_u32(route)?);
            cursor[stop] += 1;
        }
    }
    ensure!(
        cursor
            .iter()
            .zip(&tt.location_routes_offsets[1..])
            .all(|(&current, &end)| current == end as usize),
        "incomplete stop-to-route CSR"
    );
    Ok(())
}

fn assemble_routes(tt: &mut TimetableOwned, schedules: ScheduleRuns) -> Result<()> {
    let mut days_index = HashMap::new();
    days_index.insert(TrafficDays::default(), BitfieldIdx(0));
    let mut route_id_index = HashMap::new();
    let mut profile_index = HashMap::new();
    schedules.consume(|batch| {
        assemble_batch(
            tt,
            batch,
            &mut days_index,
            &mut route_id_index,
            &mut profile_index,
        )
    })?;
    build_stop_routes_index(tt)
}

fn compile_source(
    client: &mut Client,
    tt: &mut TimetableOwned,
    source_idx: u16,
    source: &ChateauSource,
    schedules: &mut ScheduleRuns,
    transfer_rules: &mut super::transfer_rules::TransferRules,
) -> Result<()> {
    let stops = load_stops(client, source)?;
    let lookup = add_locations(tt, source_idx, &stops)?;
    super::transfer_rules::load(client, source, &lookup, tt, transfer_rules)?;
    let calendars = load_calendars(client, source)?;
    let mut route_types = HashMap::new();
    query_source(
        client,
        "
        SELECT route_id, route_type FROM gtfs.routes
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ",
        source,
        |r| {
            route_types.insert(r.get::<_, String>(0), r.get::<_, i16>(1));
            Ok(())
        },
    )?;
    let mut patterns = HashMap::<String, Pattern>::new();
    query_source(
        client,
        "
        SELECT itinerary_pattern_id, route_id, timezone
        FROM gtfs.itinerary_pattern_meta
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ",
        source,
        |r| {
            let itinerary: String = r.get(0);
            let route_id: String = r.get(1);
            let tz: String = r.get(2);
            patterns.insert(
                itinerary,
                Pattern {
                    route_type: *route_types
                        .get(&route_id)
                        .context("trip route has no GTFS route record")?,
                    route_id,
                    timezone: tz
                        .parse::<Tz>()
                        .with_context(|| format!("invalid GTFS timezone {tz}"))?,
                    stops: Vec::new(),
                },
            );
            Ok(())
        },
    )?;
    let mut negative_patterns = HashSet::<String>::new();
    query_source(
        client,
        "
        SELECT itinerary_pattern_id, stop_id,
            arrival_time_since_start, departure_time_since_start,
            interpolated_time_since_start
        FROM gtfs.itinerary_pattern
        WHERE onestop_feed_id = $1 AND attempt_id = $2
        ORDER BY itinerary_pattern_id, stop_sequence
    ",
        source,
        |r| {
            let id: String = r.get(0);
            if negative_patterns.contains(&id) {
                return Ok(());
            }
            let pattern = patterns
                .get_mut(&id)
                .with_context(|| format!("itinerary_pattern row missing metadata: {id}"))?;
            if !append_pattern_stop(pattern, r.get(1), r.get(2), r.get(3), r.get(4))? {
                negative_patterns.insert(id);
            }
            Ok(())
        },
    )?;
    let mut skipped_trips = 0usize;
    query_source(
        client,
        "
        SELECT trip_id, itinerary_pattern_id, service_id, start_time::bigint,
               has_frequencies, frequencies
        FROM gtfs.trips_compressed
        WHERE onestop_feed_id = $1 AND attempt_id = $2
        ORDER BY itinerary_pattern_id, trip_id
    ",
        source,
        |r| {
            let trip_id: String = r.get(0);
            let itinerary: String = r.get(1);
            let service: String = r.get(2);
            let start: i64 = r.get(3);
            let has_frequencies: bool = r.get(4);
            let frequency_blob: Option<Vec<u8>> = r.get(5);
            if negative_patterns.contains(&itinerary) {
                skipped_trips += 1;
                return Ok(());
            }
            ensure!(
                (0..u32::MAX as i64).contains(&start),
                "trip {trip_id} invalid start_time"
            );
            let pattern = patterns.get(&itinerary).with_context(|| {
                format!("trip {trip_id} has unknown itinerary pattern {itinerary}")
            })?;
            let calendar = calendars
                .get(&service)
                .with_context(|| format!("trip {trip_id} has unknown service {service}"))?;
            let anchors =
                service_utc_anchors(calendar, pattern.timezone, tt.first_utc_day, tt.day_count)?;
            let mut add_departure = |first_departure: u32| -> Result<()> {
                for schedule in make_schedules_from_anchors(
                    &trip_id,
                    source_idx,
                    pattern,
                    &lookup,
                    first_departure,
                    &anchors,
                    tt.first_utc_day,
                    tt.day_count,
                )? {
                    schedules.push(schedule)?;
                }
                Ok(())
            };
            if has_frequencies {
                let blob = frequency_blob.as_deref().with_context(|| {
                    format!("trip {trip_id} is marked as frequency-based without frequency data")
                })?;
                expand_frequency_departures(&trip_id, blob, &mut add_departure)?;
            } else {
                // Maple's compressed start_time is the first stop arrival, whereas
                // frequencies.txt start_time is the first stop *departure*.
                let first_stop_departure = pattern
                    .stops
                    .first()
                    .with_context(|| format!("trip {trip_id} has no stops"))?
                    .2;
                let first_stop_departure =
                    u32::try_from(first_stop_departure).with_context(|| {
                        format!("trip {trip_id} has negative first departure offset")
                    })?;
                let first_departure = (start as u32)
                    .checked_add(first_stop_departure)
                    .with_context(|| format!("trip {trip_id} first departure overflow"))?;
                add_departure(first_departure)?;
            }
            Ok(())
        },
    )?;
    if !negative_patterns.is_empty() {
        let mut examples = negative_patterns
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>();
        examples.sort_unstable();
        eprintln!(
            "Gentian: skipped {} trips from {} itinerary patterns with negative stop-time offsets in feed {} (attempt {}); example patterns: {}",
            skipped_trips,
            negative_patterns.len(),
            source.onestop_feed_id,
            source.attempt_id,
            examples.join(", "),
        );
    }
    Ok(())
}

fn add_footpaths(tt: &mut TimetableOwned, mut transfers: Vec<Vec<(u32, u32)>>) -> Result<()> {
    ensure!(
        transfers.len() == tt.coordinates.len(),
        "footpath location count mismatch"
    );
    let mut incoming = vec![Vec::<(u32, u32)>::new(); transfers.len()];
    for (from, edges) in transfers.iter_mut().enumerate() {
        // Keep the fastest edge for each directed stop pair.
        edges.sort_unstable_by_key(|&(to, seconds)| (to, seconds));
        edges.dedup_by_key(|entry| entry.0);
        for &(to, seconds) in edges.iter() {
            ensure!((to as usize) < incoming.len(), "unknown footpath target");
            ensure!(seconds > 0, "nonpositive OSR transfer duration");
            if from == to as usize {
                continue;
            }
            tt.footpaths_out
                .push(PackedFootpath::new(LocationIdx(to), seconds)?);
            incoming[to as usize].push((u32::try_from(from)?, seconds));
        }
        tt.footpaths_out_offsets
            .push(as_u32(tt.footpaths_out.len())?);
    }
    for edges in &mut incoming {
        edges.sort_unstable();
        for &(from, seconds) in edges.iter() {
            tt.footpaths_in
                .push(PackedFootpath::new(LocationIdx(from), seconds)?);
        }
        tt.footpaths_in_offsets.push(as_u32(tt.footpaths_in.len())?);
    }
    Ok(())
}

pub fn build(
    client: &mut Client,
    sources: &[ChateauSource],
    first_utc_day: i32,
    day_count: u32,
    osr_dir: &Path,
    max_seconds: u32,
    candidate_meters: f64,
    output: &Path,
    scratch_dir: Option<&Path>,
    schedule_memory_mib: usize,
) -> Result<()> {
    ensure!((1..=512).contains(&day_count), "invalid timetable horizon");
    let mut tt = TimetableOwned::empty(first_utc_day, day_count);
    let scratch_root = scratch_dir.unwrap_or_else(|| {
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    });
    let mut schedules = ScheduleRuns::new(scratch_root, schedule_memory_mib)?;
    let mut transfer_rules = super::transfer_rules::TransferRules::default();
    for (source_idx, source) in sources.iter().enumerate() {
        tt.add_source(&source.chateau, &source.onestop_feed_id, &source.attempt_id)?;
        compile_source(
            client,
            &mut tt,
            u16::try_from(source_idx)?,
            source,
            &mut schedules,
            &mut transfer_rules,
        )?;
    }
    ensure!(
        !schedules.is_empty(),
        "no scheduled trips in selected timetable horizon"
    );
    assemble_routes(&mut tt, schedules)?;
    let mut transfers =
        super::footpaths::compute(osr_dir, &tt.coordinates, max_seconds, candidate_meters)?;
    // Nigiri also connects nearby platforms under one station, where the
    // street graph often omits indoor passages. Never replace a routed OSR edge.
    super::footpaths::add_station_links(&tt, &mut transfers)?;
    add_footpaths(&mut tt, transfers)?;
    transfer_rules.finish(&mut tt);
    let bytes = catenary::routing_common::timetable_mmap::publish(&tt, output)?;
    println!(
        "Wrote {} bytes; {} stops, {} RAPTOR routes, {} trips, {} footpaths",
        bytes,
        tt.coordinates.len(),
        tt.route_transport_ranges.len(),
        tt.trip_public_route_ids.len(),
        tt.footpaths_out.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn locations_default_to_two_minutes_and_keep_station_parent() {
        let mut tt = TimetableOwned::empty(0, 1);
        tt.add_source("test", "feed", "attempt").unwrap();
        let stops = vec![
            Stop {
                id: "station".into(),
                name: "Station".into(),
                lat: 33.65,
                lon: -117.82,
                parent: None,
                kind: 1,
            },
            Stop {
                id: "platform".into(),
                name: "Platform".into(),
                lat: 33.6501,
                lon: -117.8201,
                parent: Some("station".into()),
                kind: 0,
            },
        ];
        let lookup = add_locations(&mut tt, 0, &stops).unwrap();
        assert_eq!(tt.location_transfer_seconds, vec![120, 120]);
        assert_eq!(
            tt.location_parent[lookup["platform"].idx()],
            lookup["station"]
        );
    }

    #[test]
    fn negative_stop_offsets_quarantine_entire_pattern() {
        let make_pattern = || Pattern {
            route_id: "R".into(),
            route_type: 3,
            timezone: "America/Toronto".parse().unwrap(),
            stops: vec![("first".into(), 0, 0)],
        };
        for (arrival, departure, interpolated) in [
            (Some(-5520), Some(-5520), None),
            (Some(100), Some(-1), None),
            (None, None, Some(-1)),
            (Some(100), Some(110), Some(-5)),
        ] {
            let mut pattern = make_pattern();
            assert!(
                !append_pattern_stop(&mut pattern, "bad".into(), arrival, departure, interpolated,)
                    .unwrap()
            );
            assert!(pattern.stops.is_empty());
        }
    }

    #[test]
    fn nonnegative_offsets_keep_stop_order_and_interpolation() {
        let mut pattern = Pattern {
            route_id: "R".into(),
            route_type: 3,
            timezone: "America/Toronto".parse().unwrap(),
            stops: Vec::new(),
        };
        assert!(append_pattern_stop(&mut pattern, "a".into(), Some(0), Some(30), None,).unwrap());
        assert!(append_pattern_stop(&mut pattern, "b".into(), None, None, Some(90),).unwrap());
        assert!(
            append_pattern_stop(&mut pattern, "c".into(), Some(120), Some(150), None,).unwrap()
        );
        assert_eq!(
            pattern.stops,
            vec![
                ("a".into(), 0, 30),
                ("b".into(), 90, 90),
                ("c".into(), 120, 150),
            ]
        );
    }

    #[test]
    fn nonzero_gtfs_seconds_are_preserved_after_combining_offsets() {
        let date = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let first_day = i32::try_from(date.signed_duration_since(epoch).num_days()).unwrap();
        let pattern = Pattern {
            route_id: "test".into(),
            route_type: 3,
            timezone: "America/Los_Angeles".parse().unwrap(),
            stops: vec![("a".into(), 0, 0), ("b".into(), 20, 80)],
        };
        let mut stop_lookup = HashMap::new();
        stop_lookup.insert("a".into(), LocationIdx(0));
        stop_lookup.insert("b".into(), LocationIdx(1));
        let calendar = Calendar {
            weekdays: [true; 7],
            range: Some((date, date)),
            ..Default::default()
        };
        // 08:00:50 at the first stop; 08:01:10 arrival and 08:02:10
        // departure at the second. UTC on this date is PDT + 7 hours.
        let schedules = make_schedules(
            "test-trip",
            0,
            &pattern,
            &stop_lookup,
            8 * 3600 + 50,
            &calendar,
            first_day,
            1,
        )
        .unwrap();
        assert_eq!(schedules.len(), 1);
        let trip = &schedules[0];
        assert!(trip.days.test(0));
        assert_eq!(trip.first_second, 15 * 3_600 + 50);
        assert_eq!(trip.events[0].departure, 0);
        assert_eq!(trip.events[1].arrival, 20);
        assert_eq!(trip.events[1].departure, 80);
    }

    #[test]
    fn spring_forward_uses_gtfs_noon_anchor() {
        let tz: Tz = "America/Los_Angeles".parse().unwrap();
        let before = utc_anchor(NaiveDate::from_ymd_opt(2026, 3, 7).unwrap(), tz).unwrap();
        let after = utc_anchor(NaiveDate::from_ymd_opt(2026, 3, 8).unwrap(), tz).unwrap();
        assert_eq!(after - before, 23 * 3600);
    }
    #[test]
    fn fall_back_uses_gtfs_noon_anchor() {
        let tz: Tz = "America/Los_Angeles".parse().unwrap();
        let before = utc_anchor(NaiveDate::from_ymd_opt(2026, 10, 31).unwrap(), tz).unwrap();
        let after = utc_anchor(NaiveDate::from_ymd_opt(2026, 11, 1).unwrap(), tz).unwrap();
        assert_eq!(after - before, 25 * 3600);
    }
    #[test]
    fn externally_assembled_routes_preserve_overtaking_and_stop_index() {
        use catenary::routing_common::timetable::{SourceIdx, ZeroCopyTimetable};
        let root = std::env::temp_dir().join(format!("gentian-assemble-{}", uuid::Uuid::new_v4()));
        let mut tt = TimetableOwned::empty(20_000, 2);
        tt.add_source("chateau", "feed", "attempt").unwrap();
        let stops = ["a", "b", "c"]
            .iter()
            .map(|id| Stop {
                id: (*id).into(),
                name: (*id).into(),
                lat: 33.6,
                lon: -117.8,
                parent: None,
                kind: 0,
            })
            .collect::<Vec<_>>();
        add_locations(&mut tt, 0, &stops).unwrap();
        tt.footpaths_out_offsets.extend([0, 0, 0]);
        tt.footpaths_in_offsets.extend([0, 0, 0]);
        let mut schedules = ScheduleRuns::new(&root, 1).unwrap();
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        let make = |trip: &str, first: u32, middle: i64, last: i64| Schedule {
            trip_id: trip.into(),
            source: 0,
            route_id: "R".into(),
            route_type: 3,
            locations: vec![LocationIdx(0), LocationIdx(1), LocationIdx(0)],
            events: vec![
                Event {
                    stop: LocationIdx(0),
                    arrival: 0,
                    departure: 0,
                },
                Event {
                    stop: LocationIdx(1),
                    arrival: middle,
                    departure: middle,
                },
                Event {
                    stop: LocationIdx(0),
                    arrival: last,
                    departure: last,
                },
            ],
            first_second: first,
            days,
        };
        // b departs later but reaches the intermediate stop earlier: putting
        // both journeys into the same non-overtaking RAPTOR route is unsound.
        for schedule in [
            make("b", 130, 20, 450),
            make("a", 100, 200, 400),
            make("c", 800, 200, 400),
        ] {
            schedules.push(schedule).unwrap();
        }
        assemble_routes(&mut tt, schedules).unwrap();
        assert_eq!(tt.trip_public_route_ids.len(), 3);
        assert!(tt.route_transport_ranges.len() >= 2);
        assert_eq!(tt.timing_profile_offsets.len() - 1, 2);
        assert_eq!(tt.transport_timing_profile.len(), 3);
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        for trip in ["a", "b", "c"] {
            assert!(view.trip_by_source_id(SourceIdx(0), trip).is_some());
        }
        // A circular route visits stop a twice, but must appear only once in
        // stop a's route index, which is sorted by internal route index.
        let indexed = view.routes_at(LocationIdx(0));
        assert_eq!(indexed.len(), view.route_transport_ranges().len());
        assert!(indexed.windows(2).all(|pair| pair[0].0 < pair[1].0));
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn equal_patterns_share_storage_across_external_batches() {
        use catenary::routing_common::timetable::ZeroCopyTimetable;
        let root = std::env::temp_dir().join(format!("gentian-profiles-{}", uuid::Uuid::new_v4()));
        let mut tt = TimetableOwned::empty(20_000, 1);
        tt.add_source("chateau", "feed", "attempt").unwrap();
        let stops = ["a", "b"]
            .iter()
            .map(|id| Stop {
                id: (*id).into(),
                name: (*id).into(),
                lat: 33.6,
                lon: -117.8,
                parent: None,
                kind: 0,
            })
            .collect::<Vec<_>>();
        add_locations(&mut tt, 0, &stops).unwrap();
        tt.footpaths_out_offsets.extend([0, 0]);
        tt.footpaths_in_offsets.extend([0, 0]);
        let mut runs = ScheduleRuns::new(&root, 1).unwrap();
        let mut days = TrafficDays::default();
        days.set(0, true).unwrap();
        for i in 0..2_500u32 {
            runs.push(Schedule {
                trip_id: format!("trip-{i}"),
                source: 0,
                route_id: "R".into(),
                route_type: 3,
                locations: vec![LocationIdx(0), LocationIdx(1)],
                events: vec![
                    Event {
                        stop: LocationIdx(0),
                        arrival: 0,
                        departure: 0,
                    },
                    Event {
                        stop: LocationIdx(1),
                        arrival: 601,
                        departure: 601,
                    },
                ],
                first_second: i * 25,
                days,
            })
            .unwrap();
        }
        assemble_routes(&mut tt, runs).unwrap();
        assert_eq!(tt.transport_timing_profile.len(), 2_500);
        assert_eq!(tt.timing_profile_offsets.len(), 2);
        assert_eq!(tt.timing_profile_events.len(), 2);
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        assert_eq!(
            view.event_time(RouteIdx(0), TransportIdx(0), 1, true)
                .unwrap()
                .seconds(),
            601
        );
        assert!(view.transport_timing_profile().iter().all(|p| p.0 == 0));
        std::fs::remove_dir(root).unwrap();
    }

    fn sample_frequency_pattern(timezone: &str, first_stop_dwell: i32) -> Pattern {
        Pattern {
            route_id: "R".into(),
            route_type: 3,
            timezone: timezone.parse().unwrap(),
            stops: vec![
                ("a".into(), 0, first_stop_dwell),
                ("b".into(), 1800 + first_stop_dwell, 1800 + first_stop_dwell),
            ],
        }
    }

    fn sample_stop_lookup() -> HashMap<String, LocationIdx> {
        HashMap::from([("a".into(), LocationIdx(0)), ("b".into(), LocationIdx(1))])
    }

    fn single_date_calendar(date: NaiveDate) -> Calendar {
        Calendar {
            weekdays: [true; 7],
            range: Some((date, date)),
            ..Default::default()
        }
    }

    fn unix_day(date: NaiveDate) -> i32 {
        date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32
    }

    #[test]
    fn frequency_windows_decode_with_seconds_precision_and_exclusive_end() {
        use catenary::gtfs_schedule_protobuf::GtfsFrequencyProto;
        let windows = GtfsFrequenciesProto {
            frequencies: vec![
                GtfsFrequencyProto {
                    start_time: 8 * 3600 + 7,
                    end_time: 8 * 3600 + 1807,
                    headway_secs: 600,
                    exact_times: Some(1),
                },
                GtfsFrequencyProto {
                    start_time: 25 * 3600 + 17,
                    end_time: 25 * 3600 + 137,
                    headway_secs: 90,
                    exact_times: Some(0),
                },
            ],
        };
        let bytes = windows.encode_to_vec();
        let mut departures = Vec::new();
        expand_frequency_departures("GIHB_IN", &bytes, |time| {
            departures.push(time);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            departures,
            vec![
                8 * 3600 + 7,
                8 * 3600 + 607,
                8 * 3600 + 1207,
                25 * 3600 + 17,
                25 * 3600 + 107,
            ]
        );
    }

    #[test]
    fn frequency_window_rejects_zero_headway_and_missing_data() {
        use catenary::gtfs_schedule_protobuf::GtfsFrequencyProto;
        let invalid = GtfsFrequenciesProto {
            frequencies: vec![GtfsFrequencyProto {
                start_time: 0,
                end_time: 100,
                headway_secs: 0,
                exact_times: None,
            }],
        }
        .encode_to_vec();
        assert!(expand_frequency_departures("bad", &invalid, |_| Ok(())).is_err());
        assert!(expand_frequency_departures("bad", &[], |_| Ok(())).is_err());
    }

    #[test]
    fn frequency_first_departure_is_not_shifted_by_first_stop_dwell() {
        let date = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let first_day = unix_day(date);
        let departures = make_schedules(
            "GIHB_IN",
            0,
            &sample_frequency_pattern("America/Los_Angeles", 120),
            &sample_stop_lookup(),
            8 * 3600 + 7,
            &single_date_calendar(date),
            first_day,
            2,
        )
        .unwrap();
        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].first_second, 15 * 3600 + 7);
        assert_eq!(departures[0].events[0].departure, 0);
        assert_eq!(departures[0].events[1].arrival, 1800);
    }

    #[test]
    fn post_midnight_frequency_keeps_previous_service_day() {
        let saturday = NaiveDate::from_ymd_opt(2026, 10, 31).unwrap();
        let first_day = unix_day(saturday);
        let schedules = make_schedules(
            "late",
            0,
            &sample_frequency_pattern("America/Los_Angeles", 0),
            &sample_stop_lookup(),
            25 * 3600 + 15,
            &single_date_calendar(saturday),
            first_day,
            3,
        )
        .unwrap();
        assert_eq!(schedules.len(), 1);
        // The Saturday GTFS service day operates on Sunday UTC.
        // GTFS uses the Saturday noon offset across the entire service day,
        // even though Los Angeles changes from PDT to PST overnight.
        assert_eq!(schedules[0].first_second, 8 * 3600 + 15);
        assert!(schedules[0].days.test(1));
        assert!(!schedules[0].days.test(0));
    }

    #[test]
    fn frequency_departure_in_tokyo_can_fall_on_previous_utc_day() {
        let date = NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let first_day = unix_day(date) - 1;
        let schedules = make_schedules(
            "tokyo",
            0,
            &sample_frequency_pattern("Asia/Tokyo", 0),
            &sample_stop_lookup(),
            30 * 60 + 45,
            &single_date_calendar(date),
            first_day,
            3,
        )
        .unwrap();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].first_second, 15 * 3600 + 30 * 60 + 45);
        assert!(schedules[0].days.test(0));
    }

    #[test]
    fn frequency_uses_noon_offset_on_both_dst_transition_days() {
        for (date, expected_utc_hour) in [
            (NaiveDate::from_ymd_opt(2026, 3, 8).unwrap(), 8),
            (NaiveDate::from_ymd_opt(2026, 11, 1).unwrap(), 9),
        ] {
            let schedules = make_schedules(
                "dst",
                0,
                &sample_frequency_pattern("America/Los_Angeles", 0),
                &sample_stop_lookup(),
                90 * 60,
                &single_date_calendar(date),
                unix_day(date),
                2,
            )
            .unwrap();
            assert_eq!(schedules.len(), 1);
            assert_eq!(
                schedules[0].first_second,
                expected_utc_hour * 3600 + 30 * 60
            );
            assert!(schedules[0].days.test(0));
        }
    }

    #[test]
    fn no_overtaking_when_middle_stop_is_faster() {
        let event = |t| Event {
            stop: LocationIdx(0),
            arrival: t,
            departure: t,
        };
        let a = Schedule {
            trip_id: "a".into(),
            source: 0,
            route_id: "r".into(),
            route_type: 3,
            locations: vec![LocationIdx(0), LocationIdx(1)],
            events: vec![event(0), event(20)],
            first_second: 60,
            days: TrafficDays::default(),
        };
        let mut b = a.clone();
        b.first_second = 65;
        b.events[1] = event(10);
        assert!(!non_overtaking(&a, &b));
    }
}
