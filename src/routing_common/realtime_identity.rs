//! Static GTFS identity sidecar for resolving Aspen realtime updates.
//!
//! GTFS service IDs/timezones were intentionally not in timetable V6. Gentian
//! writes these compact dictionaries only when compiling a static region.
//! No realtime update writes this file. The fingerprint guards stale sidecars.
//!
//! Nigiri's get_transport resolves a GTFS-local service day with a noon-based
//! timezone offset. Gentian compiles the same rule into UTC-day bitfields; the
//! inverse mapping here must use exactly the same noon anchor.

use crate::routing_common::timetable::{SourceIdx, TransportIdx, TripIdx, DayIdx, LocationIdx, ZeroCopyTimetable};
use crate::routing_common::realtime_source::RoutingTripUpdate;
use catenary_time::Tz;
use chrono::{NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

pub const IDENTITY_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportGtfsIdentity {
    pub timezone: u16,
    pub service_id: u32,
    pub first_departure_gtfs_seconds: u32,
    /// Index into shared original GTFS stop_sequence lists.
    pub stop_sequence_pattern: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GtfsIdentityCatalog {
    pub version: u32,
    pub first_utc_day: i32,
    pub day_count: u32,
    pub timetable_fingerprint: [u8; 32],
    pub timezones: Vec<String>,
    pub service_ids: Vec<String>,
    pub transports: Vec<TransportGtfsIdentity>,
    /// Interned, *original* stop_sequence numbers; non-contiguous values
    /// and circular routes are preserved exactly as supplied by GTFS.
    pub stop_sequence_offsets: Vec<u32>,
    pub stop_sequence_values: Vec<u32>,
}

#[derive(Default)]
pub struct GtfsIdentityBuilder {
    timezones: Vec<String>,
    service_ids: Vec<String>,
    timezone_ids: HashMap<String, u16>,
    service_ids_map: HashMap<String, u32>,
    transports: Vec<TransportGtfsIdentity>,
    stop_sequences: Vec<Vec<u32>>,
    sequence_lookup: HashMap<u64, Vec<u32>>,
}

impl GtfsIdentityBuilder {
    pub fn push(&mut self, tz: &str, service: &str, departure: u32, stop_sequences: &[u32]) -> anyhow::Result<()> {
        anyhow::ensure!(stop_sequences.len() >= 2, "missing GTFS stop_sequence pattern");
        let sequence_hash = {
            use std::hash::{Hash, Hasher};
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            stop_sequences.hash(&mut hash);
            hash.finish()
        };
        let mut sequence_pattern = None;
        if let Some(indices) = self.sequence_lookup.get(&sequence_hash) {
            for &id in indices {
                if self.stop_sequences[id as usize] == stop_sequences {
                    sequence_pattern = Some(id);
                    break;
                }
            }
        }
        let sequence_pattern = if let Some(id) = sequence_pattern {
            id
        } else {
            let id = u32::try_from(self.stop_sequences.len())?;
            self.stop_sequences.push(stop_sequences.to_vec());
            self.sequence_lookup.entry(sequence_hash).or_default().push(id);
            id
        };
        let timezone = if let Some(&id) = self.timezone_ids.get(tz) {
            id
        } else {
            // Validate at compile-time, rather than fail on a live RT update.
            let _: Tz = tz.parse().map_err(anyhow::Error::msg)?;
            let id = u16::try_from(self.timezones.len())?;
            self.timezones.push(tz.to_owned());
            self.timezone_ids.insert(tz.to_owned(), id);
            id
        };
        let service_id = if let Some(&id) = self.service_ids_map.get(service) {
            id
        } else {
            let id = u32::try_from(self.service_ids.len())?;
            self.service_ids.push(service.to_owned());
            self.service_ids_map.insert(service.to_owned(), id);
            id
        };
        self.transports.push(TransportGtfsIdentity {
            timezone, service_id,
            first_departure_gtfs_seconds: departure,
            stop_sequence_pattern: sequence_pattern,
        });
        Ok(())
    }

    pub fn finish(self, tt: &ZeroCopyTimetable<'_>) -> anyhow::Result<GtfsIdentityCatalog> {
        anyhow::ensure!(
            self.transports.len() == tt.transport_route().len(),
            "GTFS identity sidecar transport count mismatch"
        );
        let mut sequence_offsets = Vec::with_capacity(self.stop_sequences.len() + 1);
        let mut sequence_values = Vec::new();
        sequence_offsets.push(0);
        for sequence in self.stop_sequences {
            sequence_values.extend(sequence);
            sequence_offsets.push(u32::try_from(sequence_values.len())?);
        }
        Ok(GtfsIdentityCatalog {
            version: IDENTITY_FORMAT_VERSION,
            first_utc_day: tt.header().first_utc_day,
            day_count: tt.header().day_count,
            timetable_fingerprint: timetable_fingerprint(tt),
            timezones: self.timezones,
            service_ids: self.service_ids,
            transports: self.transports,
            stop_sequence_offsets: sequence_offsets,
            stop_sequence_values: sequence_values,
        })
    }
}

/// SHA-256 over identity-bearing static columns without serializing or copying
/// the entire mmap region. A rebuild with reordered trips, stops, calendars or
/// changed GTFS IDs will not silently accept a stale companion catalog.
pub fn timetable_fingerprint(tt: &ZeroCopyTimetable<'_>) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(tt.header().first_utc_day.to_le_bytes());
    h.update(tt.header().day_count.to_le_bytes());
    h.update(bytemuck::cast_slice(tt.sources()));
    h.update(bytemuck::cast_slice(tt.trip_ids()));
    h.update(bytemuck::cast_slice(tt.trip_transport_offsets()));
    h.update(bytemuck::cast_slice(tt.trip_transport_ranges()));
    h.update(bytemuck::cast_slice(tt.trip_public_route_ids()));
    h.update(bytemuck::cast_slice(tt.transport_route()));
    h.update(bytemuck::cast_slice(tt.transport_traffic_days()));
    h.update(bytemuck::cast_slice(tt.bitfields()));
    h.update(bytemuck::cast_slice(tt.timing_profile_events()));
    h.update(bytemuck::cast_slice(tt.transport_first_dep_offset()));
    h.update(bytemuck::cast_slice(tt.transport_initial_day_offset()));
    h.update(bytemuck::cast_slice(tt.route_stop_offsets()));
    h.update(bytemuck::cast_slice(tt.route_stops()));
    h.update(bytemuck::cast_slice(tt.location_source()));
    h.update(bytemuck::cast_slice(tt.location_id()));
    h.update(tt.string_pool());
    h.finalize().into()
}

impl GtfsIdentityCatalog {
    pub fn validate(&self, tt: &ZeroCopyTimetable<'_>) -> anyhow::Result<()> {
        anyhow::ensure!(self.version == IDENTITY_FORMAT_VERSION, "unsupported realtime identity format");
        anyhow::ensure!(self.first_utc_day == tt.header().first_utc_day, "UTC epoch mismatch");
        anyhow::ensure!(self.day_count == tt.header().day_count, "horizon mismatch");
        anyhow::ensure!(self.transports.len() == tt.transport_route().len(), "transport count mismatch");
        anyhow::ensure!(self.timetable_fingerprint == timetable_fingerprint(tt), "stale realtime identity catalog");
        anyhow::ensure!(self.stop_sequence_offsets.first() == Some(&0)
            && self.stop_sequence_offsets.last() == Some(&(self.stop_sequence_values.len() as u32))
            && self.stop_sequence_offsets.windows(2).all(|p| p[0] <= p[1]),
            "invalid GTFS stop_sequence dictionary");
        anyhow::ensure!(self.transports.iter().enumerate().all(|(i, r)| {
            let Some(&start) = self.stop_sequence_offsets.get(r.stop_sequence_pattern as usize) else {
                return false;
            };
            let Some(&end) = self.stop_sequence_offsets.get(r.stop_sequence_pattern as usize + 1) else {
                return false;
            };
            (r.timezone as usize) < self.timezones.len()
                && (r.service_id as usize) < self.service_ids.len()
                && end >= start
                && (end - start) as usize == tt.stops_on(tt.transport_route()[i]).len()
        }), "invalid realtime identity dictionary index");
        for zone in &self.timezones {
            let _: Tz = zone.parse().map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }

    pub fn stop_sequences(&self, transport: TransportIdx) -> Option<&[u32]> {
        let row = self.transports.get(transport.idx())?;
        let start = *self.stop_sequence_offsets.get(row.stop_sequence_pattern as usize)? as usize;
        let end = *self.stop_sequence_offsets.get(row.stop_sequence_pattern as usize + 1)? as usize;
        self.stop_sequence_values.get(start..end)
    }

    pub fn service_id(&self, transport: TransportIdx) -> Option<&str> {
        let row = self.transports.get(transport.idx())?;
        self.service_ids.get(row.service_id as usize).map(String::as_str)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedInstance {
    pub transport: TransportIdx,
    pub service_day_utc: DayIdx,
    pub trip: TripIdx,
    pub source: SourceIdx,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolveIssue {
    MissingTripId,
    MissingServiceDate,
    TripIdNotFound,
    UnknownStopId(String),
    AmbiguousStopId(String),
    StopIdMissing,
    AmbiguousTripInstance,
    NoActiveService,
    InvalidStartTime,
    InvalidTimezone,
    BadCatalog,
}

/// Build a compact index only for the GTFS strings present in one Aspen page
/// or complete chateau snapshot. Keys refer to the static mmap string pool;
/// no dense cloned copy of every static stop/trip ID is allocated.
pub struct RealtimeGtfsResolver<'tt, 'data> {
    tt: &'tt ZeroCopyTimetable<'data>,
    catalog: &'tt GtfsIdentityCatalog,
    stops: HashMap<(u16, &'tt str), Vec<LocationIdx>>,
    trips: HashMap<(u16, &'tt str), Vec<TripIdx>>,
}

impl<'tt, 'data> RealtimeGtfsResolver<'tt, 'data> {
    pub fn new(
        tt: &'tt ZeroCopyTimetable<'data>,
        catalog: &'tt GtfsIdentityCatalog,
        source: SourceIdx,
        updates: &[RoutingTripUpdate],
    ) -> Result<Self, ResolveIssue> {
        // The caller validates the SHA256 fingerprint before constructing this
        // instance; fail closed for any mismatched row count or horizon.
        if catalog.version != IDENTITY_FORMAT_VERSION
            || catalog.first_utc_day != tt.header().first_utc_day
            || catalog.day_count != tt.header().day_count
            || catalog.transports.len() != tt.transport_route().len()
            || source.idx() >= tt.sources().len()
        {
            return Err(ResolveIssue::BadCatalog);
        }

        let desired_trips: HashSet<&str> = updates.iter()
            .filter_map(|u| u.trip_id.as_deref())
            .collect();
        let desired_stops: HashSet<&str> = updates.iter()
            .flat_map(|u| u.stops.iter())
            .flat_map(|s| [s.stop_id.as_deref(), s.assigned_stop_id.as_deref()])
            .flatten().collect();

        let mut stops = HashMap::new();
        for (i, (&src, &id)) in tt.location_source().iter()
            .zip(tt.location_id()).enumerate() {
            if src != source { continue; }
            if let Some(text) = tt.get_string(id) {
                if desired_stops.contains(text) {
                    stops.entry((source.0, text)).or_insert_with(Vec::new)
                        .push(LocationIdx(i as u32));
                }
            }
        }
        let mut trips = HashMap::new();
        for record in tt.trip_ids() {
            if record.source != source { continue; }
            if let Some(text) = tt.get_string(record.id) {
                if desired_trips.contains(text) {
                    let matches: &mut Vec<TripIdx> =
                        trips.entry((source.0, text)).or_insert_with(Vec::new);
                    if !matches.contains(&record.trip) {
                        matches.push(record.trip);
                    }
                }
            }
        }
        Ok(Self { tt, catalog, stops, trips })
    }

    /// Never look up a raw GTFS stop ID across another feed/attempt.
    pub fn stop(&self, source: SourceIdx, raw: &str) -> Result<LocationIdx, ResolveIssue> {
        let stops = self.stops.get(&(source.0, raw))
            .ok_or_else(|| ResolveIssue::UnknownStopId(raw.to_owned()))?;
        match stops.as_slice() {
            [one] => Ok(*one),
            _ => Err(ResolveIssue::AmbiguousStopId(raw.to_owned())),
        }
    }

    pub fn stop_for_update(
        &self, source: SourceIdx,
        update: &crate::routing_common::realtime_source::RoutingStopUpdate,
    ) -> Result<(LocationIdx, Option<LocationIdx>), ResolveIssue> {
        let id = update.stop_id.as_deref().ok_or(ResolveIssue::StopIdMissing)?;
        let original = self.stop(source, id)?;
        let assigned = update.assigned_stop_id.as_deref()
            .map(|assigned| self.stop(source, assigned)).transpose()?;
        Ok((original, assigned))
    }

    /// Resolve the *occurrence*, not only the LocationIdx. GTFS stop_sequence
    /// values are not guaranteed to be contiguous. When a stop is visited
    /// twice (circular routes), the original GTFS number disambiguates it.
    /// A stop_id alone is accepted only if the location occurs exactly once.
    pub fn stop_occurrence(
        &self,
        source: SourceIdx,
        transport: TransportIdx,
        update: &crate::routing_common::realtime_source::RoutingStopUpdate,
    ) -> Result<(usize, LocationIdx, Option<LocationIdx>), ResolveIssue> {
        let route = *self.tt.transport_route().get(transport.idx())
            .ok_or(ResolveIssue::BadCatalog)?;
        let stops = self.tt.stops_on(route);
        let sequences = self.catalog.stop_sequences(transport)
            .ok_or(ResolveIssue::BadCatalog)?;
        if sequences.len() != stops.len() {
            return Err(ResolveIssue::BadCatalog);
        }
        let original = update.stop_id.as_deref()
            .map(|id| self.stop(source, id)).transpose()?;
        let matched: Vec<usize> = stops.iter().enumerate()
            .filter(|(i, stop)| {
                original.is_none_or(|id| stop.location() == id)
                    && update.sequence.is_none_or(|number|
                        sequences[*i] == u32::from(number))
            })
            .map(|(i, _)| i)
            .collect();
        if original.is_none() && update.sequence.is_none() {
            return Err(ResolveIssue::StopIdMissing);
        }
        let index = match matched.as_slice() {
            [only] => *only,
            [] => return Err(ResolveIssue::UnknownStopId(
                update.stop_id.clone().unwrap_or_else(|| format!("sequence {:?}", update.sequence))
            )),
            _ => return Err(ResolveIssue::AmbiguousStopId(
                update.stop_id.clone().unwrap_or_else(|| format!("sequence {:?}", update.sequence))
            )),
        };
        let location = stops[index].location();
        let assigned = update.assigned_stop_id.as_deref()
            .map(|id| self.stop(source, id)).transpose()?;
        Ok((index, location, assigned))
    }

    /// GTFS-RT's service date is LOCAL and does not equal Gentian's UTC
    /// traffic-day index. Use the same noon anchor and exact first departure
    /// seconds Gentian used at compile time. No nearest-day heuristics.
    pub fn instance(
        &self, source: SourceIdx, update: &RoutingTripUpdate,
    ) -> Result<ResolvedInstance, ResolveIssue> {
        let raw_trip = update.trip_id.as_deref().ok_or(ResolveIssue::MissingTripId)?;
        let date = update.service_date.ok_or(ResolveIssue::MissingServiceDate)?;
        let start_time = update.start_time.as_deref()
            .map(parse_gtfs_start_time).transpose()?;
        let trip_candidates = self.trips.get(&(source.0, raw_trip))
            .ok_or(ResolveIssue::TripIdNotFound)?;
        let mut found: Option<ResolvedInstance> = None;
        for &trip in trip_candidates {
            // Some GTFS feeds reuse a trip_id for multiple public routes.
            // Never conflate those runs when GTFS-RT provides route_id.
            if let Some(route_id) = update.route_id.as_deref() {
                let public = *self.tt.trip_public_route_ids().get(trip.idx())
                    .ok_or(ResolveIssue::BadCatalog)?;
                let record = self.tt.route_ids().get(public.idx())
                    .ok_or(ResolveIssue::BadCatalog)?;
                if record.source != source ||
                    self.tt.get_string(record.id) != Some(route_id)
                {
                    continue;
                }
            }
            let from = *self.tt.trip_transport_offsets().get(trip.idx())
                .ok_or(ResolveIssue::BadCatalog)? as usize;
            let to = *self.tt.trip_transport_offsets().get(trip.idx() + 1)
                .ok_or(ResolveIssue::BadCatalog)? as usize;
            for r in self.tt.trip_transport_ranges().get(from..to)
                .ok_or(ResolveIssue::BadCatalog)? {
                let transport = r.transport;
                let row = self.catalog.transports.get(transport.idx())
                    .ok_or(ResolveIssue::BadCatalog)?;
                // The start_time, when present, disambiguates frequency runs.
                if start_time.is_some_and(|s| s != row.first_departure_gtfs_seconds) {
                    continue;
                }
                let zone = self.catalog.timezones.get(row.timezone as usize)
                    .ok_or(ResolveIssue::BadCatalog)?;
                let tz: Tz = zone.parse().map_err(|_| ResolveIssue::InvalidTimezone)?;
                let departure = gtfs_utc_anchor(date, tz)
                    .ok_or(ResolveIssue::InvalidTimezone)?
                    .checked_add(i64::from(row.first_departure_gtfs_seconds))
                    .ok_or(ResolveIssue::BadCatalog)?;
                let day = departure.div_euclid(86_400) - i64::from(self.tt.header().first_utc_day);
                if !(0..i64::from(self.tt.header().day_count)).contains(&day) {
                    continue;
                }
                if departure.rem_euclid(86_400)
                    != i64::from(self.tt.transport_first_dep_offset()[transport.idx()].seconds())
                {
                    continue;
                }
                let day = DayIdx(day as u16);
                if !self.tt.is_transport_active(transport, day) { continue; }

                let instance = ResolvedInstance { transport, trip, source, service_day_utc: day };
                if found.is_some_and(|previous| previous != instance) {
                    return Err(ResolveIssue::AmbiguousTripInstance);
                }
                found = Some(instance);
            }
        }
        found.ok_or(ResolveIssue::NoActiveService)
    }
}

fn gtfs_utc_anchor(date: NaiveDate, tz: Tz) -> Option<i64> {
    let noon = date.and_hms_opt(12, 0, 0)?;
    let local = tz.from_local_datetime(&noon).single()?;
    Some(local.with_timezone(&Utc).timestamp() - 43_200)
}

fn parse_gtfs_start_time(raw: &str) -> Result<u32, ResolveIssue> {
    let mut parts = raw.split(':');
    let hour: u32 = parts.next().ok_or(ResolveIssue::InvalidStartTime)?
        .parse().map_err(|_| ResolveIssue::InvalidStartTime)?;
    let minute: u32 = parts.next().ok_or(ResolveIssue::InvalidStartTime)?
        .parse().map_err(|_| ResolveIssue::InvalidStartTime)?;
    let second: u32 = parts.next().ok_or(ResolveIssue::InvalidStartTime)?
        .parse().map_err(|_| ResolveIssue::InvalidStartTime)?;
    if parts.next().is_some() || minute >= 60 || second >= 60 {
        return Err(ResolveIssue::InvalidStartTime);
    }
    hour.checked_mul(3600).and_then(|h| h.checked_add(minute * 60))
        .and_then(|x| x.checked_add(second))
        .ok_or(ResolveIssue::InvalidStartTime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn gtfs_service_anchor_survives_dst_and_overnight() {
        let la: Tz = "America/Los_Angeles".parse().unwrap();
        let saturday = NaiveDate::from_ymd_opt(2026,10,31).unwrap();
        let sunday = NaiveDate::from_ymd_opt(2026,11,1).unwrap();
        assert_eq!(gtfs_utc_anchor(sunday,la).unwrap()-gtfs_utc_anchor(saturday,la).unwrap(),25*3600);
        let expected = la.with_ymd_and_hms(2026,11,2,1,30,0).single().unwrap().timestamp();
        assert_eq!(gtfs_utc_anchor(sunday,la).unwrap()+25*3600+1800, expected);
    }

    #[test]
    fn gtfs_start_time_supports_25_hour_clocks_but_rejects_invalid() {
        assert_eq!(parse_gtfs_start_time("25:00:31"),Ok(90_031));
        assert_eq!(parse_gtfs_start_time("00:00:01"),Ok(1));
        for time in ["12:60:00","12:12:60","abc","1:2:3:4","4294967295:00:00"] {
            assert_eq!(parse_gtfs_start_time(time),Err(ResolveIssue::InvalidStartTime));
        }
    }
    fn fixture() -> (Vec<u8>, GtfsIdentityBuilder) {
        use crate::routing_common::timetable::{
            BitfieldIdx, Clasz, Coordinate, Delta, PackedStop, Range,
            RouteIdx, TimingProfileIdx, TimetableOwned, TrafficDays, TripTransportRange,
        };
        let date = NaiveDate::from_ymd_opt(2026, 10, 31).unwrap();
        let first_day = date.signed_duration_since(
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
        let mut tt = TimetableOwned::empty(first_day, 5);
        let a = tt.add_source("c", "feed-a", "attempt-a").unwrap();
        let b = tt.add_source("c", "feed-b", "attempt-b").unwrap();
        for (source, id) in [(a, "start"), (a, "end"), (b, "start")] {
            let id_ref = tt.add_string(id).unwrap();
            let name_ref = tt.add_string(id).unwrap();
            tt.coordinates.push(Coordinate::default());
            tt.location_id.push(id_ref);
            tt.location_name.push(name_ref);
            tt.location_source.push(source);
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_transfer_seconds.push(120);
            tt.location_type.push(0);
            tt.location_routes_offsets.push(0);
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
        }
        tt.route_stops.push(PackedStop::new(LocationIdx(0), true, true, true, true).unwrap());
        tt.route_stops.push(PackedStop::new(LocationIdx(1), true, true, true, true).unwrap());
        // Return to the same stop, with a distinct original GTFS number.
        tt.route_stops.push(PackedStop::new(LocationIdx(0), true, true, true, true).unwrap());
        tt.route_stop_offsets.push(3);
        tt.route_transport_ranges.push(Range { from: 0, to: 1 });
        tt.route_clasz.push(Clasz::Bus as u8);
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        let mut active = TrafficDays::default();
        // 2026-11-01 service day at 25:00 LA time = 2026-11-02 09:00Z.
        active.set(2, true).unwrap();
        tt.bitfields.push(active);
        tt.route_traffic_days.push(BitfieldIdx(1));
        tt.transport_traffic_days.push(BitfieldIdx(1));
        tt.transport_route.push(RouteIdx(0));
        tt.transport_timing_profile.push(TimingProfileIdx(0));
        tt.transport_first_dep_offset.push(Delta::from_seconds(9 * 3600).unwrap());
        tt.transport_initial_day_offset.push(0);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(1800).unwrap(),
            Delta::from_seconds(1860).unwrap(),
            Delta::from_seconds(3600).unwrap(),
        ]);
        tt.timing_profile_offsets.push(4);
        let route = tt.add_route_id(a, "route-76").unwrap();
        tt.trip_public_route_ids.push(route);
        tt.add_trip_id(a, TripIdx(0), "trip-1").unwrap();
        tt.trip_transport_ranges.push(TripTransportRange {
            transport: TransportIdx(0), stop_from: 0, stop_to: 3,
        });
        tt.trip_transport_offsets.push(1);
        tt.transport_to_trip_section_offsets.push(0);
        let mut builder = GtfsIdentityBuilder::default();
        builder.push("America/Los_Angeles", "service-2026", 25 * 3600, &[17, 29, 41]).unwrap();
        (tt.encode().unwrap(), builder)
    }

    fn update() -> RoutingTripUpdate {
        RoutingTripUpdate {
            update_id: "feed-entity".into(),
            trip_id: Some("trip-1".into()),
            route_id: Some("route-76".into()),
            service_date: NaiveDate::from_ymd_opt(2026,11,1),
            start_time: Some("25:00:00".into()),
            schedule_relationship: None,
            delay_seconds: Some(0),
            stops: vec![crate::routing_common::realtime_source::RoutingStopUpdate {
                stop_id: Some("start".into()),
                assigned_stop_id: Some("end".into()),
                sequence: Some(17),
                arrival: None,
                departure: None,
                schedule_relationship: None,
            }],
        }
    }

    #[test]
    fn resolves_original_gtfs_service_day_after_midnight_and_dst() {
        let (bytes, identities) = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let catalog = identities.finish(&tt).unwrap();
        catalog.validate(&tt).unwrap();
        let u = update();
        let resolver = RealtimeGtfsResolver::new(&tt, &catalog, SourceIdx(0), &[u.clone()]).unwrap();
        let instance = resolver.instance(SourceIdx(0), &u).unwrap();
        assert_eq!(instance.transport, TransportIdx(0));
        assert_eq!(instance.service_day_utc, DayIdx(2));
        assert_eq!(catalog.service_id(instance.transport), Some("service-2026"));
        assert_eq!(resolver.stop_for_update(SourceIdx(0), &u.stops[0]).unwrap(),
            (LocationIdx(0), Some(LocationIdx(1))));
    }

    #[test]
    fn gtfs_noncontiguous_numbers_resolve_each_circular_stop_occurrence() {
        let (bytes, identities) = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let catalog = identities.finish(&tt).unwrap();
        let u = update();
        let resolver = RealtimeGtfsResolver::new(&tt, &catalog, SourceIdx(0), &[u]).unwrap();
        let first = crate::routing_common::realtime_source::RoutingStopUpdate {
            stop_id: Some("start".into()),
            sequence: Some(17), assigned_stop_id: None,
            arrival: None, departure: None, schedule_relationship: None,
        };
        let last = crate::routing_common::realtime_source::RoutingStopUpdate {
            sequence: Some(41), ..first.clone()
        };
        let ambiguous = crate::routing_common::realtime_source::RoutingStopUpdate {
            sequence: None, ..first.clone()
        };
        let by_number = crate::routing_common::realtime_source::RoutingStopUpdate {
            stop_id: None, ..last.clone()
        };
        assert_eq!(catalog.stop_sequences(TransportIdx(0)), Some(&[17, 29, 41][..]));
        assert_eq!(resolver.stop_occurrence(SourceIdx(0), TransportIdx(0), &first).unwrap().0, 0);
        assert_eq!(resolver.stop_occurrence(SourceIdx(0), TransportIdx(0), &last).unwrap().0, 2);
        assert_eq!(resolver.stop_occurrence(SourceIdx(0), TransportIdx(0), &by_number).unwrap().0, 2);
        assert!(matches!(
            resolver.stop_occurrence(SourceIdx(0), TransportIdx(0), &ambiguous),
            Err(ResolveIssue::AmbiguousStopId(_))
        ));
        let wrong_id = crate::routing_common::realtime_source::RoutingStopUpdate {
            stop_id: Some("end".into()), ..first
        };
        assert!(resolver.stop_occurrence(SourceIdx(0), TransportIdx(0), &wrong_id).is_err());
    }

    #[test]
    fn prepares_added_and_scheduled_runs_without_mutating_static_service() {
        use crate::routing_common::realtime_prepare::{
            added_action, scheduled_action, PreparedAction, PrepareError,
        };
        use crate::routing_common::realtime_overlay::RtPatternInput;
        use crate::routing_common::realtime_source::{RoutingStopUpdate, RoutingTimeUpdate};
        let (bytes, identities) = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let catalog = identities.finish(&tt).unwrap();
        let mut scheduled = update();
        scheduled.delay_seconds = Some(300);
        // First event is an absolute time, later events inherit that offset.
        let base = (i64::from(tt.header().first_utc_day) + 2) * 86_400 + 9 * 3600;
        scheduled.stops[0].departure = Some(RoutingTimeUpdate {
            unix_seconds: Some(base + 360), delay_seconds: Some(100),
        });
        scheduled.stops[0].assigned_stop_id = None;
        let resolver = RealtimeGtfsResolver::new(
            &tt, &catalog, SourceIdx(0), &[scheduled.clone()],
        ).unwrap();
        let original = resolver.instance(SourceIdx(0), &scheduled).unwrap();
        let PreparedAction::Run(run) = scheduled_action(
            &tt, &resolver, SourceIdx(0), original, &scheduled,
        ).unwrap() else { panic!("expected scheduled realtime run"); };
        assert!(matches!(run.pattern, RtPatternInput::Static(_)));
        assert_eq!(run.event_times, vec![
            base + 360, base + 1800 + 360, base + 1860 + 360, base + 3600 + 360,
        ]);
        assert!(tt.is_transport_active(TransportIdx(0), DayIdx(2)));

        let new_event = |ts| Some(RoutingTimeUpdate {
            unix_seconds: Some(ts), delay_seconds: None,
        });
        let mut added = scheduled.clone();
        added.update_id = "added-entity".into();
        added.trip_id = Some("new-service-trip".into());
        added.schedule_relationship = Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Added);
        added.stops = vec![
            RoutingStopUpdate {
                stop_id: Some("start".into()), sequence: Some(10),
                assigned_stop_id: None, arrival: None, departure: new_event(base + 10),
                schedule_relationship: None,
            },
            RoutingStopUpdate {
                stop_id: Some("end".into()), sequence: Some(70),
                assigned_stop_id: None, arrival: new_event(base + 500),
                departure: new_event(base + 600), schedule_relationship: None,
            },
            RoutingStopUpdate {
                stop_id: Some("start".into()), sequence: Some(90),
                assigned_stop_id: None, arrival: new_event(base + 900),
                departure: None, schedule_relationship: None,
            },
        ];
        let added_resolver = RealtimeGtfsResolver::new(
            &tt, &catalog, SourceIdx(0), &[added.clone()],
        ).unwrap();
        let PreparedAction::Run(new_run) = added_action(
            &added_resolver, SourceIdx(0), &added,
        ).unwrap() else { panic!("expected added run"); };
        assert!(new_run.original.is_none());
        assert_eq!(new_run.event_times, vec![base + 10, base + 500, base + 600, base + 900]);
        let RtPatternInput::Stops(pattern) = new_run.pattern else {
            panic!("expected internable dynamic stop pattern");
        };
        assert_eq!(
            pattern.iter().map(|s| s.location()).collect::<Vec<_>>(),
            vec![LocationIdx(0), LocationIdx(1), LocationIdx(0)],
        );
        added.stops[2].arrival = None;
        assert!(matches!(
            added_action(&added_resolver, SourceIdx(0), &added),
            Err(PrepareError::MissingEventTime)
        ));
    }

    #[test]
    fn stop_ids_and_trip_ids_must_match_source_and_route() {
        let (bytes, identities) = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let catalog = identities.finish(&tt).unwrap();
        let mut u = update();
        let resolver = RealtimeGtfsResolver::new(&tt, &catalog, SourceIdx(0), &[u.clone()]).unwrap();
        assert!(matches!(resolver.stop(SourceIdx(1), "start"), Err(ResolveIssue::UnknownStopId(_))));
        u.route_id = Some("wrong-route".into());
        assert_eq!(resolver.instance(SourceIdx(0), &u),Err(ResolveIssue::NoActiveService));
    }

    #[test]
    fn stale_catalog_and_wrong_local_start_time_are_rejected() {
        let (bytes, identities) = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let mut catalog = identities.finish(&tt).unwrap();
        catalog.timetable_fingerprint[0] ^= 1;
        assert!(catalog.validate(&tt).is_err());
        // Recover a valid catalog and confirm that 24:00 cannot map a 25:00 trip.
        catalog.timetable_fingerprint[0] ^= 1;
        let mut u = update();
        u.start_time = Some("24:00:00".into());
        let resolver = RealtimeGtfsResolver::new(&tt, &catalog, SourceIdx(0), &[u.clone()]).unwrap();
        assert_eq!(resolver.instance(SourceIdx(0), &u), Err(ResolveIssue::NoActiveService));
    }

}
