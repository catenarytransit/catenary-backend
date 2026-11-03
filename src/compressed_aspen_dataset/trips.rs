use super::columns::{Column, Delays, Indices, StringBuilder, Strings, Times, Values};
use crate::aspen_dataset::*;
use crate::formation_v1::{AspenisedPlatformInfo, UnifiedConsist};
use ahash::AHashMap;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ARRIVAL: u16 = 1;
const DEPARTURE: u16 = 2;
const SEQUENCE: u16 = 4;
const OLD_DATA: u16 = 8;
const OCCUPANCY_SHIFT: u16 = 4;
const RELATIONSHIP_SHIFT: u16 = 8;

/// Twelve-byte hot row. Optional enums use zero for None and value + 1 otherwise.
#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct StopRow {
    stop_id: u32,
    first_event: u32,
    stop_sequence: u16,
    flags: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Events {
    delay: Delays,
    time: Times,
    uncertainty: Column<i16>,
}

#[derive(Default)]
struct EventBuilder {
    delay: Vec<Option<i32>>,
    time: Vec<Option<i64>>,
    uncertainty: Vec<Option<i16>>,
}

impl EventBuilder {
    fn push(&mut self, event: AspenStopTimeEvent) {
        self.delay.push(event.delay);
        self.time.push(event.time);
        self.uncertainty.push(event.uncertainty);
    }

    fn finish(self) -> Events {
        Events {
            delay: Delays::new(self.delay),
            time: Times::new(self.time),
            uncertainty: Column::new(self.uncertainty),
        }
    }
}

/// A copied view of three numeric values; constructing it never allocates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopTimeEventRef {
    pub delay: Option<i32>,
    pub time: Option<i64>,
    pub uncertainty: Option<i16>,
}

impl Events {
    #[inline]
    fn get(&self, row: usize) -> StopTimeEventRef {
        StopTimeEventRef {
            delay: self.delay.get(row),
            time: self.time.get(row),
            uncertainty: self.uncertainty.get(row).copied(),
        }
    }
}

impl StopTimeEventRef {
    pub fn to_owned(self) -> AspenStopTimeEvent {
        AspenStopTimeEvent {
            delay: self.delay,
            time: self.time,
            uncertainty: self.uncertainty,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stops {
    rows: Box<[StopRow]>,
    events: Events,
    properties: Column<AspenisedStopTimeProperties>,
    platforms: Column<u32>,
    platform_info: Column<AspenisedPlatformInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Descriptors {
    trip_id: Indices,
    route_id: Indices,
    start_time: Indices,
    start_date: Values<chrono::NaiveDate>,
    direction_id: Values<u32>,
    relationship: Column<AspenisedTripScheduleRelationship>,
    modified: Column<ModifiedTripSelector>,
}

/// Borrows pooled strings and rare modified selectors from one snapshot.
#[derive(Clone, Copy, Debug)]
pub struct RawTripRef<'a> {
    pub trip_id: Option<&'a str>,
    pub route_id: Option<&'a str>,
    pub start_time: Option<&'a str>,
    pub start_date: Option<chrono::NaiveDate>,
    pub direction_id: Option<u32>,
    pub schedule_relationship: Option<&'a AspenisedTripScheduleRelationship>,
    pub modified_trip: Option<&'a ModifiedTripSelector>,
}

impl RawTripRef<'_> {
    pub fn effective_start_date(&self) -> Option<chrono::NaiveDate> {
        self.modified_trip
            .and_then(|m| m.start_date)
            .or(self.start_date)
    }

    pub fn effective_start_time(&self) -> Option<&str> {
        self.modified_trip
            .and_then(|m| m.start_time.as_deref())
            .or(self.start_time)
    }

    pub fn to_owned(self) -> AspenRawTripInfo {
        AspenRawTripInfo {
            trip_id: self.trip_id.map(str::to_owned),
            route_id: self.route_id.map(str::to_owned),
            start_time: self.start_time.map(str::to_owned),
            start_date: self.start_date,
            direction_id: self.direction_id,
            schedule_relationship: self.schedule_relationship.cloned(),
            modified_trip: self.modified_trip.cloned(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TripRow {
    stop_start: u32,
    stop_len: u32,
    timestamp: Option<u64>,
    last_seen: u64,
    found_schedule_trip_id: bool,
}

/// Read-only trip table, with one string dictionary shared by descriptors and stops.
/// No expanded stop vector or TripDescriptor is retained after construction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompressedTripUpdates {
    ids: AHashMap<CompactString, u32>,
    strings: Strings,
    rows: Box<[TripRow]>,
    descriptors: Descriptors,
    stops: Stops,
    delay: Delays,
    vehicle: Column<AspenisedVehicleDescriptor>,
    properties: Column<AspenTripProperties>,
    headsign: Indices,
    consist: Column<Box<UnifiedConsist>>,
}

impl From<AHashMap<CompactString, AspenisedTripUpdate>> for CompressedTripUpdates {
    fn from(trips: AHashMap<CompactString, AspenisedTripUpdate>) -> Self {
        let mut ids = AHashMap::with_capacity(trips.len());
        let mut strings = StringBuilder::default();
        let mut rows = Vec::with_capacity(trips.len());
        let mut trip_ids = Vec::with_capacity(trips.len());
        let mut route_ids = Vec::with_capacity(trips.len());
        let mut start_times = Vec::with_capacity(trips.len());
        let mut start_dates = Vec::with_capacity(trips.len());
        let mut direction_ids = Vec::with_capacity(trips.len());
        let mut relationships = Vec::with_capacity(trips.len());
        let mut modified = Vec::with_capacity(trips.len());
        let mut stop_rows = Vec::new();
        let mut events = EventBuilder::default();
        let mut stop_properties = Vec::new();
        let mut platforms = Vec::new();
        let mut platform_info = Vec::new();
        let mut delays = Vec::with_capacity(trips.len());
        let mut vehicles = Vec::with_capacity(trips.len());
        let mut properties = Vec::with_capacity(trips.len());
        let mut headsigns = Vec::with_capacity(trips.len());
        let mut consists = Vec::with_capacity(trips.len());
        for (id, update) in trips {
            ids.insert(id, u32::try_from(rows.len()).expect("too many Aspen trips"));
            let AspenRawTripInfo {
                trip_id,
                route_id,
                start_time,
                start_date,
                direction_id,
                schedule_relationship,
                modified_trip,
            } = update.trip;
            trip_ids.push(strings.insert(trip_id.as_deref()));
            route_ids.push(strings.insert(route_id.as_deref()));
            start_times.push(strings.insert(start_time.as_deref()));
            start_dates.push(start_date);
            direction_ids.push(direction_id);
            relationships.push(schedule_relationship);
            modified.push(modified_trip);
            let stop_start = u32::try_from(stop_rows.len()).expect("too many Aspen stops");
            let stop_len =
                u32::try_from(update.stop_time_update.len()).expect("too many Aspen stops");
            // The combined range must fit the same index domain, not just each operand.
            stop_start
                .checked_add(stop_len)
                .expect("too many Aspen stops");
            for stop in update.stop_time_update {
                let mut flags = 0;
                if stop.stop_sequence.is_some() {
                    flags |= SEQUENCE;
                }
                if stop.old_rt_data {
                    flags |= OLD_DATA;
                }
                if let Some(occupancy) = &stop.departure_occupancy_status {
                    flags |= u16::from(occupancy_status_to_u8(occupancy) + 1) << OCCUPANCY_SHIFT;
                }
                if let Some(relationship) = &stop.schedule_relationship {
                    let code: u8 = relationship.into();
                    flags |= u16::from(code + 1) << RELATIONSHIP_SHIFT;
                }
                let first_event = u32::try_from(events.delay.len()).expect("too many Aspen events");
                if let Some(event) = stop.arrival {
                    flags |= ARRIVAL;
                    events.push(event);
                }
                if let Some(event) = stop.departure {
                    flags |= DEPARTURE;
                    events.push(event);
                }
                u32::try_from(events.delay.len()).expect("too many Aspen events");
                stop_rows.push(StopRow {
                    stop_id: strings.insert(stop.stop_id.as_deref()),
                    first_event,
                    stop_sequence: stop.stop_sequence.unwrap_or(0),
                    flags,
                });
                stop_properties.push(stop.stop_time_properties);
                platforms.push(
                    stop.platform_string
                        .as_deref()
                        .map(|p| strings.insert(Some(p))),
                );
                platform_info.push(stop.platform_info);
            }
            rows.push(TripRow {
                stop_start,
                stop_len,
                timestamp: update.timestamp,
                last_seen: update.last_seen,
                found_schedule_trip_id: update.found_schedule_trip_id,
            });
            delays.push(update.delay);
            vehicles.push(update.vehicle);
            properties.push(update.trip_properties);
            headsigns.push(strings.insert(update.trip_headsign.as_deref()));
            consists.push(update.consist);
        }
        Self {
            ids,
            strings: strings.finish(),
            rows: rows.into_boxed_slice(),
            descriptors: Descriptors {
                trip_id: Indices::new(trip_ids),
                route_id: Indices::new(route_ids),
                start_time: Indices::new(start_times),
                start_date: Values::new(start_dates),
                direction_id: Values::new(direction_ids),
                relationship: Column::new(relationships),
                modified: Column::new(modified),
            },
            stops: Stops {
                rows: stop_rows.into_boxed_slice(),
                events: events.finish(),
                properties: Column::new(stop_properties),
                platforms: Column::new(platforms),
                platform_info: Column::new(platform_info),
            },
            delay: Delays::new(delays),
            vehicle: Column::new(vehicles),
            properties: Column::new(properties),
            headsign: Indices::new(headsigns),
            consist: Column::new(consists),
        }
    }
}

impl CompressedTripUpdates {
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[inline]
    pub fn get(&self, id: &str) -> Option<TripUpdateRef<'_>> {
        let row = *self.ids.get(id)? as usize;
        self.rows.get(row)?;
        Some(TripUpdateRef { table: self, row })
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&str, TripUpdateRef<'_>)> {
        self.ids.iter().filter_map(|(id, &row)| {
            self.rows.get(row as usize)?;
            Some((
                id.as_str(),
                TripUpdateRef {
                    table: self,
                    row: row as usize,
                },
            ))
        })
    }

    pub fn to_expanded(&self) -> AHashMap<CompactString, AspenisedTripUpdate> {
        // Only used at compatibility boundaries. Share repeated stop IDs in
        // the returned dataset rather than retaining an allocation per row.
        let mut shared = AHashMap::new();
        self.iter()
            .map(|(id, trip)| {
                let mut update = trip.to_owned();
                for stop in &mut update.stop_time_update {
                    if let Some(id) = &mut stop.stop_id {
                        *id = shared
                            .entry(id.to_string())
                            .or_insert_with(|| Arc::clone(id))
                            .clone();
                    }
                }
                (CompactString::from(id), update)
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TripUpdateRef<'a> {
    table: &'a CompressedTripUpdates,
    row: usize,
}

impl<'a> TripUpdateRef<'a> {
    #[inline]
    pub fn trip(self) -> RawTripRef<'a> {
        let d = &self.table.descriptors;
        RawTripRef {
            trip_id: d
                .trip_id
                .get(self.row)
                .and_then(|id| self.table.strings.get(id)),
            route_id: d
                .route_id
                .get(self.row)
                .and_then(|id| self.table.strings.get(id)),
            start_time: d
                .start_time
                .get(self.row)
                .and_then(|id| self.table.strings.get(id)),
            start_date: d.start_date.get(self.row),
            direction_id: d.direction_id.get(self.row),
            schedule_relationship: d.relationship.get(self.row),
            modified_trip: d.modified.get(self.row),
        }
    }

    #[inline]
    pub fn stops(
        self,
    ) -> impl ExactSizeIterator<Item = StopTimeUpdateRef<'a>> + DoubleEndedIterator {
        let row = &self.table.rows[self.row];
        let start = row.stop_start as usize;
        (start..start + row.stop_len as usize).map(move |row| StopTimeUpdateRef {
            table: self.table,
            row,
        })
    }

    #[inline]
    pub fn timestamp(self) -> Option<u64> {
        self.table.rows[self.row].timestamp
    }
    #[inline]
    pub fn delay(self) -> Option<i32> {
        self.table.delay.get(self.row)
    }
    #[inline]
    pub fn vehicle(self) -> Option<&'a AspenisedVehicleDescriptor> {
        self.table.vehicle.get(self.row)
    }
    #[inline]
    pub fn trip_properties(self) -> Option<&'a AspenTripProperties> {
        self.table.properties.get(self.row)
    }
    #[inline]
    pub fn trip_headsign(self) -> Option<&'a str> {
        self.table.strings.get(self.table.headsign.get(self.row)?)
    }
    #[inline]
    pub fn consist(self) -> Option<&'a UnifiedConsist> {
        self.table.consist.get(self.row).map(Box::as_ref)
    }
    #[inline]
    pub fn last_seen(self) -> u64 {
        self.table.rows[self.row].last_seen
    }
    #[inline]
    pub fn found_schedule_trip_id(self) -> bool {
        self.table.rows[self.row].found_schedule_trip_id
    }

    pub fn to_owned(self) -> AspenisedTripUpdate {
        AspenisedTripUpdate {
            trip: self.trip().to_owned(),
            stop_time_update: self.stops().map(StopTimeUpdateRef::to_owned).collect(),
            timestamp: self.timestamp(),
            delay: self.delay(),
            vehicle: self.vehicle().cloned(),
            trip_properties: self.trip_properties().cloned(),
            trip_headsign: self.trip_headsign().map(Arc::from),
            consist: self.consist().cloned().map(Box::new),
            last_seen: self.last_seen(),
            found_schedule_trip_id: self.found_schedule_trip_id(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StopTimeUpdateRef<'a> {
    table: &'a CompressedTripUpdates,
    row: usize,
}

impl<'a> StopTimeUpdateRef<'a> {
    #[inline]
    fn core(self) -> &'a StopRow {
        &self.table.stops.rows[self.row]
    }
    #[inline]
    pub fn stop_id(self) -> Option<&'a str> {
        self.table.strings.get(self.core().stop_id)
    }
    #[inline]
    pub fn stop_sequence(self) -> Option<u16> {
        (self.core().flags & SEQUENCE != 0).then_some(self.core().stop_sequence)
    }
    #[inline]
    pub fn old_rt_data(self) -> bool {
        self.core().flags & OLD_DATA != 0
    }
    #[inline]
    pub fn arrival(self) -> Option<StopTimeEventRef> {
        (self.core().flags & ARRIVAL != 0).then(|| {
            self.table
                .stops
                .events
                .get(self.core().first_event as usize)
        })
    }
    #[inline]
    pub fn departure(self) -> Option<StopTimeEventRef> {
        (self.core().flags & DEPARTURE != 0).then(|| {
            self.table.stops.events.get(
                self.core().first_event as usize + usize::from(self.core().flags & ARRIVAL != 0),
            )
        })
    }
    #[inline]
    pub fn departure_occupancy_status(self) -> Option<AspenisedOccupancyStatus> {
        let code = ((self.core().flags >> OCCUPANCY_SHIFT) & 15) as u8;
        option_u8_to_occupancy_status(&code.checked_sub(1))
    }
    #[inline]
    pub fn schedule_relationship(self) -> Option<AspenisedStopTimeScheduleRelationship> {
        let code = ((self.core().flags >> RELATIONSHIP_SHIFT) & 7) as u8;
        code.checked_sub(1).map(Into::into)
    }
    #[inline]
    pub fn stop_time_properties(self) -> Option<&'a AspenisedStopTimeProperties> {
        self.table.stops.properties.get(self.row)
    }
    #[inline]
    pub fn platform_string(self) -> Option<&'a str> {
        self.table
            .strings
            .get(*self.table.stops.platforms.get(self.row)?)
    }
    #[inline]
    pub fn platform_info(self) -> Option<&'a AspenisedPlatformInfo> {
        self.table.stops.platform_info.get(self.row)
    }

    pub fn to_owned(self) -> AspenisedStopTimeUpdate {
        AspenisedStopTimeUpdate {
            stop_id: self.stop_id().map(Arc::from),
            stop_sequence: self.stop_sequence(),
            old_rt_data: self.old_rt_data(),
            arrival: self.arrival().map(StopTimeEventRef::to_owned),
            departure: self.departure().map(StopTimeEventRef::to_owned),
            departure_occupancy_status: self.departure_occupancy_status(),
            schedule_relationship: self.schedule_relationship(),
            stop_time_properties: self.stop_time_properties().cloned(),
            platform_string: self.platform_string().map(Into::into),
            platform_info: self.platform_info().cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hot_row_is_twelve_bytes() {
        assert_eq!(std::mem::size_of::<StopRow>(), 12);
    }
}
