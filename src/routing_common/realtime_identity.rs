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

pub const IDENTITY_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportGtfsIdentity {
    pub timezone: u16,
    pub service_id: u32,
    pub first_departure_gtfs_seconds: u32,
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
}

#[derive(Default)]
pub struct GtfsIdentityBuilder {
    timezones: Vec<String>,
    service_ids: Vec<String>,
    timezone_ids: HashMap<String, u16>,
    service_ids_map: HashMap<String, u32>,
    transports: Vec<TransportGtfsIdentity>,
}

impl GtfsIdentityBuilder {
    pub fn push(&mut self, tz: &str, service: &str, departure: u32) -> anyhow::Result<()> {
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
        });
        Ok(())
    }

    pub fn finish(self, tt: &ZeroCopyTimetable<'_>) -> anyhow::Result<GtfsIdentityCatalog> {
        anyhow::ensure!(
            self.transports.len() == tt.transport_route().len(),
            "GTFS identity sidecar transport count mismatch"
        );
        Ok(GtfsIdentityCatalog {
            version: IDENTITY_FORMAT_VERSION,
            first_utc_day: tt.header().first_utc_day,
            day_count: tt.header().day_count,
            timetable_fingerprint: timetable_fingerprint(tt),
            timezones: self.timezones,
            service_ids: self.service_ids,
            transports: self.transports,
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
        anyhow::ensure!(self.transports.iter().all(|r|
            (r.timezone as usize) < self.timezones.len() &&
            (r.service_id as usize) < self.service_ids.len()
        ), "invalid realtime identity dictionary index");
        for zone in &self.timezones {
            let _: Tz = zone.parse().map_err(anyhow::Error::msg)?;
        }
        Ok(())
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
}
