use anyhow::{Context, Result, ensure};
use catenary::routing_common::routing_regions::ChateauSource;
use catenary::routing_common::timetable::{
    BitfieldIdx, Clasz, Coordinate, Delta, LocationIdx, PackedFootpath, PackedStop,
    Range, RouteIdx, TrafficDays, TransportIdx, TripIdx, TripTransportRange,
    TimetableOwned, ZeroCopyTimetable,
};
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use fallible_iterator::FallibleIterator;
use postgres::{types::ToSql, Client, Row};
use std::collections::{BTreeMap, HashMap};
use std::fs;
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
struct Event {
    stop: LocationIdx,
    arrival: i64,
    departure: i64,
}

#[derive(Clone)]
struct Pattern {
    route_id: String,
    route_type: i16,
    timezone: Tz,
    stops: Vec<(String, i32, i32)>,
}

#[derive(Clone)]
struct Schedule {
    trip_id: String,
    source: u16,
    route_id: String,
    route_type: i16,
    locations: Vec<LocationIdx>,
    events: Vec<Event>,
    first_second: u32,
    days: TrafficDays,
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
    query_source(client, "
        SELECT gtfs_id, COALESCE(displayname, name, gtfs_id),
               ST_Y(point)::float8, ST_X(point)::float8,
               parent_station, location_type
        FROM gtfs.stops
        WHERE onestop_feed_id = $1 AND attempt_id = $2
          AND point IS NOT NULL
        ORDER BY gtfs_id
    ", source, |r| {
        let lat: f64 = r.get(2);
        let lon: f64 = r.get(3);
        ensure!(lat.is_finite() && lon.is_finite(), "invalid stop coordinates");
        stops.push(Stop {
            id: r.get(0), name: r.get(1), lat, lon,
            parent: r.get(4), kind: r.get(5),
        });
        Ok(())
    })?;
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
        tt.coordinates.push(Coordinate { lat: stop.lat as f32, lon: stop.lon as f32 });
        let id_ref = tt.add_string(&stop.id)?;
        let name_ref = tt.add_string(&stop.name)?;
        tt.location_id.push(id_ref);
        tt.location_name.push(name_ref);
        tt.location_source.push(catenary::routing_common::timetable::SourceIdx(source_idx));
        tt.location_type.push(if stop.kind == 1 { 2 } else { 1 });
        tt.location_transfer_seconds.push(catenary::routing_common::timetable::DEFAULT_TRANSFER_SECONDS);
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

fn load_calendars(client: &mut Client, source: &ChateauSource) -> Result<HashMap<String, Calendar>> {
    let mut calendars = HashMap::<String, Calendar>::new();
    query_source(client, "
        SELECT service_id, monday, tuesday, wednesday, thursday, friday,
               saturday, sunday, gtfs_start_date::text, gtfs_end_date::text
        FROM gtfs.calendar
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ", source, |r| {
        calendars.insert(r.get(0), Calendar {
            weekdays: [r.get(1), r.get(2), r.get(3), r.get(4),
                       r.get(5), r.get(6), r.get(7)],
            range: Some((NaiveDate::parse_from_str(&r.get::<_, String>(8), "%Y-%m-%d")?,
                         NaiveDate::parse_from_str(&r.get::<_, String>(9), "%Y-%m-%d")?)),
            ..Default::default()
        });
        Ok(())
    })?;
    query_source(client, "
        SELECT service_id, gtfs_date::text, exception_type
        FROM gtfs.calendar_dates
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ", source, |r| {
        let date = NaiveDate::parse_from_str(&r.get::<_, String>(1), "%Y-%m-%d")?;
        let exception: i16 = r.get(2);
        ensure!(exception == 1 || exception == 2, "unknown GTFS calendar exception type");
        calendars.entry(r.get(0)).or_default()
            .exceptions.insert(date, exception == 1);
        Ok(())
    })?;
    Ok(calendars)
}

fn active(calendar: &Calendar, date: NaiveDate) -> bool {
    if let Some(v) = calendar.exceptions.get(&date) {
        return *v;
    }
    calendar.range.is_some_and(|(start, end)| start <= date && date <= end)
        && calendar.weekdays[date.weekday().num_days_from_monday() as usize]
}

fn utc_anchor(date: NaiveDate, tz: Tz) -> Result<i64> {
    let noon = date.and_hms_opt(12, 0, 0).context("invalid GTFS service day")?;
    let utc = tz.from_local_datetime(&noon).single()
        .context("timezone has ambiguous/nonexistent local noon")?
        .with_timezone(&Utc);
    Ok(utc.timestamp() - 12 * 3600)
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
    trip_start_secs: u32,
    calendar: &Calendar,
    first_utc_day: i32,
    day_count: u32,
) -> Result<Vec<Schedule>> {
    let mut result = BTreeMap::<(u32, Vec<i64>), TrafficDays>::new();
    let first_day = start_date(first_utc_day)?;
    for offset in -5_i64..i64::from(day_count) + 5 {
        let service_date = first_day.checked_add_signed(Duration::days(offset))
            .context("service date out of range")?;
        if !active(calendar, service_date) { continue; }
        let anchor = utc_anchor(service_date, pattern.timezone)?;
        let mut stamps = Vec::with_capacity(pattern.stops.len() * 2);
        for (stop_id, arr, dep) in &pattern.stops {
            ensure!(stop_lookup.contains_key(stop_id), "unknown stop {stop_id} on trip {trip_id}");
            let a = anchor + i64::from(trip_start_secs) + i64::from(*arr);
            let d = anchor + i64::from(trip_start_secs) + i64::from(*dep);
            ensure!(a <= d, "negative dwell time in trip {trip_id}");
            // Preserve the UTC second after adding start and pattern offsets.
            stamps.push(a);
            stamps.push(d);
        }
        ensure!(stamps.len() >= 4, "trip {trip_id} needs at least two stops");
        ensure!(stamps.windows(2).all(|w| w[0] <= w[1]),
            "nonmonotonic UTC stop times for trip {trip_id}");
        let departure = stamps[1];
        let day = departure.div_euclid(86_400) - i64::from(first_utc_day);
        if !(0..i64::from(day_count)).contains(&day) { continue; }
        let second = u32::try_from(departure.rem_euclid(86_400))?;
        let relative: Vec<i64> = stamps.into_iter().map(|t| t - departure).collect();
        let key = (second, relative);
        result.entry(key).or_default().set(day as usize, true)?;
    }
    let mut schedules = Vec::with_capacity(result.len());
    for ((first_second, relative), days) in result {
        let events = pattern.stops.iter().enumerate().map(|(i, (id, _, _))| {
            Ok(Event {
                stop: *stop_lookup.get(id).context("missing pattern stop")?,
                arrival: relative[2*i],
                departure: relative[2*i+1],
            })
        }).collect::<Result<Vec<_>>>()?;
        schedules.push(Schedule {
            trip_id: trip_id.to_owned(), source: source_idx,
            route_id: pattern.route_id.clone(), route_type: pattern.route_type,
            locations: events.iter().map(|e| e.stop).collect(), events,
            first_second, days,
        });
    }
    Ok(schedules)
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
    if let Some(&idx) = index.get(&days) { return Ok(idx); }
    let idx = BitfieldIdx(as_u32(tt.bitfields.len())?);
    index.insert(days, idx);
    tt.bitfields.push(days);
    Ok(idx)
}

fn non_overtaking(a: &Schedule, b: &Schedule) -> bool {
    a.first_second <= b.first_second
        && a.events.iter().zip(&b.events).all(|(x, y)| {
            x.arrival + i64::from(a.first_second)
                <= y.arrival + i64::from(b.first_second)
            && x.departure + i64::from(a.first_second)
                <= y.departure + i64::from(b.first_second)
        })
}

fn assemble_routes(tt: &mut TimetableOwned, schedules: Vec<Schedule>) -> Result<()> {
    let mut buckets = BTreeMap::<(u16, String, Vec<u32>), Vec<Schedule>>::new();
    for schedule in schedules {
        let stops = schedule.locations.iter().map(|s| s.0).collect();
        buckets.entry((schedule.source, schedule.route_id.clone(), stops))
            .or_default().push(schedule);
    }
    let mut days_index = HashMap::new();
    days_index.insert(TrafficDays::default(), BitfieldIdx(0));
    let mut stop_routes = vec![Vec::<RouteIdx>::new(); tt.coordinates.len()];
    let mut route_id_index = HashMap::new();
    for ((source, route_id, locations), mut schedules) in buckets {
        schedules.sort_by(|a, b| a.first_second.cmp(&b.first_second)
            .then_with(|| a.trip_id.cmp(&b.trip_id)));
        let mut chains = Vec::<Vec<Schedule>>::new();
        for schedule in schedules {
            if let Some(chain) = chains.iter_mut().find(|chain| {
                non_overtaking(chain.last().expect("nonempty chain"), &schedule)
            }) {
                chain.push(schedule);
            } else {
                chains.push(vec![schedule]);
            }
        }
        let public_route_id = if let Some(&idx) = route_id_index.get(&(source, route_id.clone())) {
            idx
        } else {
            let idx = tt.add_route_id(catenary::routing_common::timetable::SourceIdx(source), &route_id)?;
            route_id_index.insert((source, route_id), idx);
            idx
        };
        for chain in chains {
            let route = RouteIdx(as_u32(tt.route_transport_ranges.len())?);
            tt.route_stop_offsets.push(as_u32(tt.route_stops.len() + locations.len())?);
            for &stop in &locations {
                tt.route_stops.push(PackedStop::new(LocationIdx(stop), true, true, true, true)?);
                stop_routes[stop as usize].push(route);
            }
            let first = tt.transport_route.len();
            let width = chain.len();
            let first_events = &chain[0].events;
            for i in 0..first_events.len() * 2 - 2 {
                let event_stop = if i % 2 == 0 { i/2 } else { i/2+1 };
                let is_arrival = i % 2 == 1;
                for schedule in &chain {
                    let e = &schedule.events[event_stop];
                    let delta = if is_arrival { e.arrival } else { e.departure };
                    ensure!(delta >= 0 && delta <= i64::from(Delta::MAX_SECONDS),
                        "time offset outside Delta representation");
                    tt.route_stop_times.push(Delta::from_seconds(u32::try_from(delta)?)?);
                }
            }
            let mut union = TrafficDays::default();
            for schedule in &chain {
                let idx = intern_days(tt, &mut days_index, schedule.days)?;
                union.or_assign(&schedule.days);
                let transport = TransportIdx(as_u32(tt.transport_route.len())?);
                tt.transport_route.push(route);
                tt.transport_traffic_days.push(idx);
                tt.transport_first_dep_offset.push(Delta::from_seconds(schedule.first_second)?);
                tt.transport_initial_day_offset.push(0);
                let trip_idx = TripIdx(as_u32(tt.trip_public_route_ids.len())?);
                tt.trip_public_route_ids.push(public_route_id);
                tt.add_trip_id(catenary::routing_common::timetable::SourceIdx(source),
                    trip_idx, &schedule.trip_id)?;
                tt.trip_transport_ranges.push(TripTransportRange {
                    transport, stop_from: 0,
                    stop_to: u16::try_from(locations.len())?,
                });
                tt.trip_transport_offsets.push(as_u32(tt.trip_transport_ranges.len())?);
                tt.transport_to_trip_section_offsets.push(as_u32(tt.transport_to_trip_section.len())?);
            }
            ensure!(tt.transport_route.len() == first + width, "transport indexing error");
            tt.route_transport_ranges.push(Range { from: as_u32(first)?,
                to: as_u32(tt.transport_route.len())? });
            tt.route_stop_time_ranges.push(Range {
                from: as_u32(tt.route_stop_times.len() - (first_events.len() * 2 - 2) * width)?,
                to: as_u32(tt.route_stop_times.len())?,
            });
            tt.route_clasz.push(clasz(chain[0].route_type));
            let route_days = intern_days(tt, &mut days_index, union)?;
            tt.route_traffic_days.push(route_days);
            tt.route_section_clasz_offsets.push(as_u32(tt.route_section_clasz.len())?);
            tt.section_flag_offsets.push(as_u32(tt.route_section_clasz.len())?);
        }
    }
    tt.location_routes.clear();
    for routes in &mut stop_routes {
        routes.sort_unstable_by_key(|r| r.0);
        routes.dedup();
        tt.location_routes.extend_from_slice(routes);
        tt.location_routes_offsets.push(as_u32(tt.location_routes.len())?);
    }
    Ok(())
}

fn compile_source(
    client: &mut Client,
    tt: &mut TimetableOwned,
    source_idx: u16,
    source: &ChateauSource,
    schedules: &mut Vec<Schedule>,
    transfer_rules: &mut super::transfer_rules::TransferRules,
) -> Result<()> {
    let stops = load_stops(client, source)?;
    let lookup = add_locations(tt, source_idx, &stops)?;
    super::transfer_rules::load(client, source, &lookup, tt, transfer_rules)?;
    let calendars = load_calendars(client, source)?;
    let mut route_types = HashMap::new();
    query_source(client, "
        SELECT route_id, route_type FROM gtfs.routes
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ", source, |r| { route_types.insert(r.get::<_, String>(0), r.get::<_, i16>(1)); Ok(()) })?;
    let mut patterns = HashMap::<String, Pattern>::new();
    query_source(client, "
        SELECT itinerary_pattern_id, route_id, timezone
        FROM gtfs.itinerary_pattern_meta
        WHERE onestop_feed_id = $1 AND attempt_id = $2
    ", source, |r| {
        let itinerary: String = r.get(0);
        let route_id: String = r.get(1);
        let tz: String = r.get(2);
        patterns.insert(itinerary, Pattern {
            route_type: *route_types.get(&route_id).context("trip route has no GTFS route record")?,
            route_id, timezone: tz.parse::<Tz>()
                .with_context(|| format!("invalid GTFS timezone {tz}"))?,
            stops: Vec::new(),
        });
        Ok(())
    })?;
    query_source(client, "
        SELECT itinerary_pattern_id, stop_id,
            arrival_time_since_start, departure_time_since_start,
            interpolated_time_since_start
        FROM gtfs.itinerary_pattern
        WHERE onestop_feed_id = $1 AND attempt_id = $2
        ORDER BY itinerary_pattern_id, stop_sequence
    ", source, |r| {
        let id: String = r.get(0);
        let pattern = patterns.get_mut(&id)
            .with_context(|| format!("itinerary_pattern row missing metadata: {id}"))?;
        let fallback: Option<i32> = r.get(4);
        let arrival = r.get::<_, Option<i32>>(2).or(fallback)
            .context("itinerary pattern has no arrival/interpolated time")?;
        let departure = r.get::<_, Option<i32>>(3).or(fallback)
            .context("itinerary pattern has no departure/interpolated time")?;
        pattern.stops.push((r.get(1), arrival, departure));
        Ok(())
    })?;
    query_source(client, "
        SELECT trip_id, itinerary_pattern_id, service_id, start_time::bigint,
               has_frequencies
        FROM gtfs.trips_compressed
        WHERE onestop_feed_id = $1 AND attempt_id = $2
        ORDER BY itinerary_pattern_id, trip_id
    ", source, |r| {
        let trip_id: String = r.get(0);
        let itinerary: String = r.get(1);
        let service: String = r.get(2);
        let start: i64 = r.get(3);
        let has_frequencies: bool = r.get(4);
        ensure!(!has_frequencies, "GTFS frequency trip {trip_id} requires frequency expansion");
        ensure!((0..u32::MAX as i64).contains(&start), "trip {trip_id} invalid start_time");
        let pattern = patterns.get(&itinerary)
            .with_context(|| format!("trip {trip_id} has unknown itinerary pattern {itinerary}"))?;
        let calendar = calendars.get(&service)
            .with_context(|| format!("trip {trip_id} has unknown service {service}"))?;
        schedules.extend(make_schedules(&trip_id, source_idx, pattern, &lookup,
            start as u32, calendar, tt.first_utc_day, tt.day_count)?);
        Ok(())
    })?;
    Ok(())
}

fn add_footpaths(
    tt: &mut TimetableOwned,
    mut transfers: Vec<Vec<(u32, u32)>>,
) -> Result<()> {
    ensure!(transfers.len() == tt.coordinates.len(), "footpath location count mismatch");
    let mut incoming = vec![Vec::<(u32, u32)>::new(); transfers.len()];
    for (from, edges) in transfers.iter_mut().enumerate() {
        // Keep the fastest edge for each directed stop pair.
        edges.sort_unstable_by_key(|&(to, seconds)| (to, seconds));
        edges.dedup_by_key(|entry| entry.0);
        for &(to, seconds) in edges.iter() {
            ensure!((to as usize) < incoming.len(), "unknown footpath target");
            ensure!(seconds > 0, "nonpositive OSR transfer duration");
            if from == to as usize { continue; }
            tt.footpaths_out.push(PackedFootpath::new(LocationIdx(to), seconds)?);
            incoming[to as usize].push((u32::try_from(from)?, seconds));
        }
        tt.footpaths_out_offsets.push(as_u32(tt.footpaths_out.len())?);
    }
    for edges in &mut incoming {
        edges.sort_unstable();
        for &(from, seconds) in edges.iter() {
            tt.footpaths_in.push(PackedFootpath::new(LocationIdx(from), seconds)?);
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
) -> Result<()> {
    ensure!((1..=512).contains(&day_count), "invalid timetable horizon");
    let mut tt = TimetableOwned::empty(first_utc_day, day_count);
    let mut schedules = Vec::new();
    let mut transfer_rules = super::transfer_rules::TransferRules::default();
    for (source_idx, source) in sources.iter().enumerate() {
        tt.add_source(&source.chateau, &source.onestop_feed_id, &source.attempt_id)?;
        compile_source(client, &mut tt, u16::try_from(source_idx)?, source, &mut schedules,
                       &mut transfer_rules)?;
    }
    ensure!(!schedules.is_empty(), "no scheduled trips in selected timetable horizon");
    assemble_routes(&mut tt, schedules)?;
    let mut transfers = super::footpaths::compute(
        osr_dir, &tt.coordinates, max_seconds, candidate_meters,
    )?;
    // Nigiri also connects nearby platforms under one station, where the
    // street graph often omits indoor passages. Never replace a routed OSR edge.
    super::footpaths::add_station_links(&tt, &mut transfers)?;
    add_footpaths(&mut tt, transfers)?;
    transfer_rules.finish(&mut tt);
    let bytes = tt.encode()?;
    ZeroCopyTimetable::new(&bytes)?;
    if let Some(dir) = output.parent() { fs::create_dir_all(dir)?; }
    let temp = output.with_extension("building");
    fs::write(&temp, &bytes)?;
    fs::rename(&temp, output)?;
    println!("Wrote {} bytes; {} stops, {} RAPTOR routes, {} trips, {} footpaths",
        bytes.len(), tt.coordinates.len(), tt.route_transport_ranges.len(),
        tt.trip_public_route_ids.len(), tt.footpaths_out.len());
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
                id: "station".into(), name: "Station".into(),
                lat: 33.65, lon: -117.82, parent: None, kind: 1,
            },
            Stop {
                id: "platform".into(), name: "Platform".into(),
                lat: 33.6501, lon: -117.8201, parent: Some("station".into()), kind: 0,
            },
        ];
        let lookup = add_locations(&mut tt, 0, &stops).unwrap();
        assert_eq!(tt.location_transfer_seconds, vec![120, 120]);
        assert_eq!(tt.location_parent[lookup["platform"].idx()], lookup["station"]);
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
            "test-trip", 0, &pattern, &stop_lookup,
            8 * 3600 + 50, &calendar, first_day, 1,
        ).unwrap();
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
        let before = utc_anchor(NaiveDate::from_ymd_opt(2026,3,7).unwrap(),tz).unwrap();
        let after = utc_anchor(NaiveDate::from_ymd_opt(2026,3,8).unwrap(),tz).unwrap();
        assert_eq!(after-before, 23*3600);
    }
    #[test]
    fn fall_back_uses_gtfs_noon_anchor() {
        let tz: Tz = "America/Los_Angeles".parse().unwrap();
        let before = utc_anchor(NaiveDate::from_ymd_opt(2026,10,31).unwrap(),tz).unwrap();
        let after = utc_anchor(NaiveDate::from_ymd_opt(2026,11,1).unwrap(),tz).unwrap();
        assert_eq!(after-before, 25*3600);
    }
    #[test]
    fn no_overtaking_when_middle_stop_is_faster() {
        let event = |t| Event {stop:LocationIdx(0),arrival:t,departure:t};
        let a=Schedule {trip_id:"a".into(),source:0,route_id:"r".into(),route_type:3,
            locations:vec![LocationIdx(0),LocationIdx(1)],
            events:vec![event(0),event(20)],first_second:60,days:TrafficDays::default()};
        let mut b=a.clone(); b.first_second=65; b.events[1]=event(10);
        assert!(!non_overtaking(&a,&b));
    }
}
