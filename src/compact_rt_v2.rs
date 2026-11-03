//! Feed-selected, allocation-conscious GTFS-RT snapshot storage.
//!
//! Stop ID and stop sequence are part of every compact stop row (with explicit
//! absence markers for valid feeds that omit either). They are intentionally not
//! part of the feed feature bitmap. Optional event and stop-time properties are
//! encoded once as feed-wide columns. A missing column allocates no per-row data.

mod trip_descriptor;
pub use trip_descriptor::{CompactTripDescriptorStore, TripDescriptorRef};

use crate::compact_formats::{
    CompactFeedEntity, CompactFeedHeader, CompactFeedMessage, CompactStopTimeEvent,
    CompactStopTimeUpdate, CompactTripUpdate,
};
use ahash::AHashMap;
use gtfs_realtime::trip_update::TripProperties;
use gtfs_realtime::trip_update::stop_time_update::StopTimeProperties;
use gtfs_realtime::{FeedMessage, TripDescriptor, VehicleDescriptor};
use i24::I24;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::mem::size_of;
use std::sync::Arc;

/// Feed snapshot feature flags. STOP_ID and STOP_SEQUENCE deliberately do not
/// appear here: both belong to the mandatory core layout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtFeatures(pub u16);

impl RtFeatures {
    pub const DELAY: u16 = 1 << 0;
    pub const TIME: u16 = 1 << 1;
    pub const UNCERTAINTY: u16 = 1 << 2;
    pub const SCHEDULED_TIME: u16 = 1 << 3;
    pub const ARRIVAL: u16 = 1 << 4;
    pub const DEPARTURE: u16 = 1 << 5;
    pub const SCHEDULE_RELATIONSHIP: u16 = 1 << 6;
    pub const DEPARTURE_OCCUPANCY: u16 = 1 << 7;
    pub const STOP_TIME_PROPERTIES: u16 = 1 << 8;

    pub fn contains(self, feature: u16) -> bool {
        self.0 & feature != 0
    }

    fn insert(&mut self, feature: u16) {
        self.0 |= feature;
    }
}

/// A column's physical encoding is decided once per snapshot, not per row.
///
/// Sparse indices are strictly increasing, permitting binary-search lookups.
/// Uniform represents all rows having the same *present* value. None and
/// Some(default) are never conflated.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OptionalColumn<T> {
    Absent,
    Uniform(T),
    AllPresent(Vec<T>),
    Sparse { indices: Vec<u32>, values: Vec<T> },
    Dense(Vec<Option<T>>),
}

impl<T: Clone + PartialEq> OptionalColumn<T> {
    fn from_values(values: Vec<Option<T>>) -> Self {
        let len = values.len();
        let count = values.iter().filter(|v| v.is_some()).count();
        if count == 0 {
            return Self::Absent;
        }
        if count == len {
            let first = values[0].as_ref().expect("nonempty present column");
            if values.iter().all(|v| v.as_ref() == Some(first)) {
                return Self::Uniform(first.clone());
            }
            return Self::AllPresent(values.into_iter().map(Option::unwrap).collect());
        }
        // Only store exceptions sparsely when doing so beats a dense Option<T>
        // array. Real allocator overhead is measured separately by benchmarks.
        let sparse_bytes = count.saturating_mul(size_of::<u32>() + size_of::<T>());
        let dense_bytes = len.saturating_mul(size_of::<Option<T>>());
        if sparse_bytes < dense_bytes {
            let mut indices = Vec::with_capacity(count);
            let mut packed = Vec::with_capacity(count);
            for (index, value) in values.into_iter().enumerate() {
                if let Some(value) = value {
                    indices.push(u32::try_from(index).expect("too many realtime events"));
                    packed.push(value);
                }
            }
            Self::Sparse {
                indices,
                values: packed,
            }
        } else {
            Self::Dense(values)
        }
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        match self {
            Self::Absent => None,
            Self::Uniform(value) => Some(value),
            Self::AllPresent(values) => values.get(index),
            Self::Sparse { indices, values } => {
                let index = u32::try_from(index).ok()?;
                indices
                    .binary_search(&index)
                    .ok()
                    .and_then(|i| values.get(i))
            }
            Self::Dense(values) => values.get(index).and_then(Option::as_ref),
        }
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}

/// A signed 15-bit value only reaches -16_384..=16_383 seconds (<5 hours).
/// Store exact seconds in two-byte i16 for <= 9 hours. Select I24 for any
/// delay beyond nine hours anywhere in the feed, and i32 if I24 cannot fit.
/// The width decision is shared by event-level and trip-level delays.
const NINE_HOURS_SECONDS: i32 = 9 * 60 * 60;
const I24_MIN_SECONDS: i32 = -8_388_608;
const I24_MAX_SECONDS: i32 = 8_388_607;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DelayWidth {
    I16,
    I24,
    I32,
}

impl DelayWidth {
    fn include(&mut self, delay: Option<i32>) {
        let Some(delay) = delay else { return };
        if !(I24_MIN_SECONDS..=I24_MAX_SECONDS).contains(&delay) {
            *self = Self::I32;
        } else if !(-NINE_HOURS_SECONDS..=NINE_HOURS_SECONDS).contains(&delay) && *self == Self::I16
        {
            *self = Self::I24;
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompactDelayColumn {
    Absent,
    I16(OptionalColumn<i16>),
    I24(OptionalColumn<I24>),
    I32(OptionalColumn<i32>),
}

impl CompactDelayColumn {
    fn from_values(values: Vec<Option<i32>>, width: DelayWidth) -> Self {
        if values.iter().all(Option::is_none) {
            return Self::Absent;
        }

        match width {
            DelayWidth::I16 => Self::I16(OptionalColumn::from_values(
                values
                    .into_iter()
                    .map(|value| {
                        value.map(|v| i16::try_from(v).expect("feed delay width was checked"))
                    })
                    .collect(),
            )),
            DelayWidth::I24 => Self::I24(OptionalColumn::from_values(
                values
                    .into_iter()
                    .map(|value| {
                        value.map(|v| {
                            assert!(
                                (I24_MIN_SECONDS..=I24_MAX_SECONDS).contains(&v),
                                "feed delay width was checked"
                            );
                            I24::wrapping_from_i32(v)
                        })
                    })
                    .collect(),
            )),
            DelayWidth::I32 => Self::I32(OptionalColumn::from_values(values)),
        }
    }

    fn get(&self, index: usize) -> Option<i32> {
        match self {
            Self::Absent => None,
            Self::I16(column) => column.get(index).copied().map(i32::from),
            Self::I24(column) => column.get(index).copied().map(i32::from),
            Self::I32(column) => column.get(index).copied(),
        }
    }

    fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactEventColumns {
    pub count: u32,
    pub delay: CompactDelayColumn,
    pub time: OptionalColumn<I24>,
    pub uncertainty: OptionalColumn<i32>,
    pub scheduled_time: OptionalColumn<I24>,
}

impl CompactEventColumns {
    fn from_events(events: Vec<CompactStopTimeEvent>, width: DelayWidth) -> Self {
        let count = u32::try_from(events.len()).expect("too many realtime events");
        let mut delay = Vec::with_capacity(events.len());
        let mut time = Vec::with_capacity(events.len());
        let mut uncertainty = Vec::with_capacity(events.len());
        let mut scheduled_time = Vec::with_capacity(events.len());
        for event in events {
            delay.push(event.delay);
            time.push(event.time);
            uncertainty.push(event.uncertainty);
            scheduled_time.push(event.scheduled_time);
        }
        Self {
            count,
            delay: CompactDelayColumn::from_values(delay, width),
            time: OptionalColumn::from_values(time),
            uncertainty: OptionalColumn::from_values(uncertainty),
            scheduled_time: OptionalColumn::from_values(scheduled_time),
        }
    }

    fn get(&self, index: u32) -> Option<CompactStopTimeEvent> {
        if index >= self.count {
            return None;
        }
        let index = index as usize;
        Some(CompactStopTimeEvent {
            delay: self.delay.get(index),
            time: self.time.get(index).copied(),
            uncertainty: self.uncertainty.get(index).copied(),
            scheduled_time: self.scheduled_time.get(index).copied(),
        })
    }
}

const HAS_STOP_SEQUENCE: u8 = 1 << 0;
const HAS_ARRIVAL: u8 = 1 << 1;
const HAS_DEPARTURE: u8 = 1 << 2;

/// Fixed-width 16-byte hot row; all uncommon fields are in optional columns.
/// stop_id = 0 represents None, otherwise it is a 1-based string dictionary ID.
#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct CompactStopRow {
    pub stop_id: u32,
    pub stop_sequence: u32,
    pub first_event: u32,
    pub flags: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactStopTimeStore {
    pub rows: Vec<CompactStopRow>,
    pub stop_ids: Vec<Arc<str>>,
    pub events: CompactEventColumns,
    pub schedule_relationship: OptionalColumn<u8>,
    pub departure_occupancy_status: OptionalColumn<u8>,
    pub stop_time_properties: OptionalColumn<StopTimeProperties>,
}

impl CompactStopTimeStore {
    fn get_stop(&self, index: usize) -> Option<CompactStopTimeUpdate> {
        let row = self.rows.get(index)?;
        let mut event_index = row.first_event;
        let arrival = if row.flags & HAS_ARRIVAL != 0 {
            let event = self.events.get(event_index)?;
            event_index += 1;
            Some(Box::new(event))
        } else {
            None
        };
        let departure = if row.flags & HAS_DEPARTURE != 0 {
            Some(Box::new(self.events.get(event_index)?))
        } else {
            None
        };
        Some(CompactStopTimeUpdate {
            stop_sequence: if row.flags & HAS_STOP_SEQUENCE != 0 {
                // The old compact representation already uses u16.
                Some(u16::try_from(row.stop_sequence).ok()?)
            } else {
                None
            },
            stop_id: if row.stop_id == 0 {
                None
            } else {
                Some(self.stop_ids.get((row.stop_id - 1) as usize)?.clone())
            },
            arrival,
            departure,
            departure_occupancy_status: self.departure_occupancy_status.get(index).copied(),
            schedule_relationship: self.schedule_relationship.get(index).copied(),
            stop_time_properties: self.stop_time_properties.get(index).cloned().map(Box::new),
        })
    }
}

// Retained for decoding existing V2 serialized snapshots. New snapshots use
// TripDescriptorColumns, appended after the existing enum variants.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripUpdateV2 {
    pub trip: TripDescriptor,
    pub vehicle: Option<VehicleDescriptor>,
    pub stop_start: u32,
    pub stop_len: u32,
    pub timestamp: Option<u64>,
    pub trip_properties: Option<Box<TripProperties>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripEntityV2 {
    pub id: String,
    pub is_deleted: Option<bool>,
    pub trip_update: Option<CompactTripUpdateV2>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripFeedV2 {
    pub entities: Vec<CompactTripEntityV2>,
    pub trip_delay: CompactDelayColumn,
    pub stops: CompactStopTimeStore,
}

impl CompactTripFeedV2 {
    fn expanded_entities(&self) -> Vec<CompactFeedEntity> {
        self.entities
            .iter()
            .enumerate()
            .map(|(entity_index, entity)| {
                let trip_update = entity.trip_update.as_ref().map(|tu| {
                    let begin = tu.stop_start as usize;
                    let end = begin
                        .checked_add(tu.stop_len as usize)
                        .expect("stop range overflow");
                    let stop_time_update = (begin..end)
                        .map(|index| self.stops.get_stop(index).expect("valid compact stop"))
                        .collect();
                    CompactTripUpdate {
                        trip: tu.trip.clone(),
                        vehicle: tu.vehicle.clone(),
                        stop_time_update,
                        timestamp: tu.timestamp,
                        delay: self.trip_delay.get(entity_index),
                        trip_properties: tu.trip_properties.clone(),
                    }
                });
                CompactFeedEntity {
                    id: entity.id.clone(),
                    is_deleted: entity.is_deleted,
                    trip_update,
                    vehicle: None,
                    alert: None,
                    shape: None,
                    stop: None,
                    trip_modifications: None,
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripUpdateV3 {
    /// Index into the feed-local descriptor columns.
    pub trip_index: u32,
    pub vehicle: Option<VehicleDescriptor>,
    pub stop_start: u32,
    pub stop_len: u32,
    pub timestamp: Option<u64>,
    pub trip_properties: Option<Box<TripProperties>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripEntityV3 {
    pub id: String,
    pub is_deleted: Option<bool>,
    pub trip_update: Option<CompactTripUpdateV3>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripFeedV3 {
    pub entities: Vec<CompactTripEntityV3>,
    pub descriptors: CompactTripDescriptorStore,
    pub trip_delay: CompactDelayColumn,
    pub stops: CompactStopTimeStore,
}

impl CompactTripFeedV3 {
    fn from_entities(entities: Vec<CompactFeedEntity>) -> (Self, RtFeatures) {
        let mut packed_entities = Vec::with_capacity(entities.len());
        let mut descriptors = Vec::new();
        let mut rows = Vec::new();
        let mut stop_ids = Vec::new();
        let mut id_indices: AHashMap<Arc<str>, u32> = AHashMap::new();
        let mut events = Vec::new();
        let mut relationships = Vec::new();
        let mut occupancies = Vec::new();
        let mut properties = Vec::new();
        let mut features = RtFeatures::default();
        let mut trip_delays = Vec::with_capacity(entities.len());

        // Determine the physical delay width once, across trip and event
        // delays. A single exceptional value widens both columns together.
        let mut delay_width = DelayWidth::I16;
        for entity in &entities {
            if let Some(trip) = &entity.trip_update {
                delay_width.include(trip.delay);
                for stop in &trip.stop_time_update {
                    for event in [stop.arrival.as_deref(), stop.departure.as_deref()]
                        .into_iter()
                        .flatten()
                    {
                        delay_width.include(event.delay);
                    }
                }
            }
        }

        for mut entity in entities {
            trip_delays.push(entity.trip_update.as_ref().and_then(|trip| trip.delay));
            let trip_update = entity.trip_update.take().map(|update| {
                let CompactTripUpdate {
                    trip,
                    vehicle,
                    stop_time_update,
                    timestamp,
                    delay: _,
                    trip_properties,
                } = update;
                let stop_start = u32::try_from(rows.len()).expect("too many stop updates");
                let stop_len =
                    u32::try_from(stop_time_update.len()).expect("too many stop updates");
                for stop in stop_time_update {
                    let first_event =
                        u32::try_from(events.len()).expect("too many realtime events");
                    let mut flags = 0;
                    let stop_sequence = match stop.stop_sequence {
                        Some(sequence) => {
                            flags |= HAS_STOP_SEQUENCE;
                            u32::from(sequence)
                        }
                        None => 0,
                    };
                    let stop_id = match stop.stop_id {
                        None => 0,
                        Some(id) => {
                            if let Some(existing) = id_indices.get(id.as_ref()) {
                                *existing
                            } else {
                                let index = u32::try_from(stop_ids.len() + 1)
                                    .expect("too many stop identifiers");
                                id_indices.insert(id.clone(), index);
                                stop_ids.push(id);
                                index
                            }
                        }
                    };
                    if let Some(arrival) = stop.arrival {
                        features.insert(RtFeatures::ARRIVAL);
                        flags |= HAS_ARRIVAL;
                        events.push(*arrival);
                    }
                    if let Some(departure) = stop.departure {
                        features.insert(RtFeatures::DEPARTURE);
                        flags |= HAS_DEPARTURE;
                        events.push(*departure);
                    }
                    rows.push(CompactStopRow {
                        stop_id,
                        stop_sequence,
                        first_event,
                        flags,
                    });
                    relationships.push(stop.schedule_relationship);
                    occupancies.push(stop.departure_occupancy_status);
                    properties.push(stop.stop_time_properties.map(|boxed| *boxed));
                }
                let trip_index =
                    u32::try_from(descriptors.len()).expect("too many trip descriptors");
                descriptors.push(trip);
                CompactTripUpdateV3 {
                    trip_index,
                    vehicle,
                    stop_start,
                    stop_len,
                    timestamp,
                    trip_properties,
                }
            });
            packed_entities.push(CompactTripEntityV3 {
                id: entity.id,
                is_deleted: entity.is_deleted,
                trip_update,
            });
        }

        let events = CompactEventColumns::from_events(events, delay_width);
        let trip_delay = CompactDelayColumn::from_values(trip_delays, delay_width);
        if !events.delay.is_absent() || !trip_delay.is_absent() {
            features.insert(RtFeatures::DELAY);
        }
        if !events.uncertainty.is_absent() {
            features.insert(RtFeatures::UNCERTAINTY);
        }
        if !events.time.is_absent() {
            features.insert(RtFeatures::TIME);
        }
        if !events.scheduled_time.is_absent() {
            features.insert(RtFeatures::SCHEDULED_TIME);
        }

        let schedule_relationship = OptionalColumn::from_values(relationships);
        let departure_occupancy_status = OptionalColumn::from_values(occupancies);
        let stop_time_properties = OptionalColumn::from_values(properties);
        if !schedule_relationship.is_absent() {
            features.insert(RtFeatures::SCHEDULE_RELATIONSHIP);
        }
        if !departure_occupancy_status.is_absent() {
            features.insert(RtFeatures::DEPARTURE_OCCUPANCY);
        }
        if !stop_time_properties.is_absent() {
            features.insert(RtFeatures::STOP_TIME_PROPERTIES);
        }
        (
            Self {
                entities: packed_entities,
                descriptors: CompactTripDescriptorStore::new(descriptors),
                trip_delay,
                stops: CompactStopTimeStore {
                    rows,
                    stop_ids,
                    events,
                    schedule_relationship,
                    departure_occupancy_status,
                    stop_time_properties,
                },
            },
            features,
        )
    }

    fn expanded_entities(&self) -> Vec<CompactFeedEntity> {
        self.entities
            .iter()
            .enumerate()
            .map(|(entity_index, entity)| {
                let trip_update = entity.trip_update.as_ref().map(|tu| {
                    let begin = tu.stop_start as usize;
                    let end = begin
                        .checked_add(tu.stop_len as usize)
                        .expect("stop range overflow");
                    let stop_time_update = (begin..end)
                        .map(|index| self.stops.get_stop(index).expect("valid compact stop"))
                        .collect();
                    CompactTripUpdate {
                        trip: self
                            .descriptors
                            .get(tu.trip_index as usize)
                            .expect("valid compact trip descriptor")
                            .to_owned(),
                        vehicle: tu.vehicle.clone(),
                        stop_time_update,
                        timestamp: tu.timestamp,
                        delay: self.trip_delay.get(entity_index),
                        trip_properties: tu.trip_properties.clone(),
                    }
                });
                CompactFeedEntity {
                    id: entity.id.clone(),
                    is_deleted: entity.is_deleted,
                    trip_update,
                    vehicle: None,
                    alert: None,
                    shape: None,
                    stop: None,
                    trip_modifications: None,
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompactFeedDataV2 {
    TripUpdates(CompactTripFeedV2),
    /// Mixed and non-trip feeds retain the existing lossless representation.
    Legacy(Vec<CompactFeedEntity>),
    /// Appended to preserve the discriminants/layouts of existing V2 variants.
    /// Older readers cannot decode this new variant.
    TripDescriptorColumns(CompactTripFeedV3),
}

/// Persistent GTFS-RT snapshot. The resident trip-update feed uses pooled
/// stop-time events and snapshot-selected optional columns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactFeedMessageV2 {
    pub header: CompactFeedHeader,
    pub reference_epoch: u64,
    pub features: RtFeatures,
    pub data: CompactFeedDataV2,
}

impl CompactFeedMessageV2 {
    pub fn from_feed_message(message: FeedMessage) -> Self {
        let compact = CompactFeedMessage::from_feed_message(message);
        Self::from_legacy(compact)
    }

    pub fn from_legacy(compact: CompactFeedMessage) -> Self {
        let CompactFeedMessage {
            header,
            reference_epoch,
            entity,
        } = compact;
        let trip_only = entity.iter().any(|e| e.trip_update.is_some())
            && entity.iter().all(|e| {
                e.vehicle.is_none()
                    && e.alert.is_none()
                    && e.shape.is_none()
                    && e.stop.is_none()
                    && e.trip_modifications.is_none()
            });
        let (data, features) = if trip_only {
            let (trips, features) = CompactTripFeedV3::from_entities(entity);
            (CompactFeedDataV2::TripDescriptorColumns(trips), features)
        } else {
            (CompactFeedDataV2::Legacy(entity), RtFeatures::default())
        };
        Self {
            header,
            reference_epoch,
            features,
            data,
        }
    }

    pub fn entity_len(&self) -> usize {
        match &self.data {
            CompactFeedDataV2::TripUpdates(feed) => feed.entities.len(),
            CompactFeedDataV2::TripDescriptorColumns(feed) => feed.entities.len(),
            CompactFeedDataV2::Legacy(entities) => entities.len(),
        }
    }

    /// Existing Aspen processors can read a borrowed slice for non-trip
    /// feeds, or an on-demand temporary expansion for trip-update feeds.
    /// Do not store the result persistently: it deliberately allocates.
    pub fn entities(&self) -> Cow<'_, [CompactFeedEntity]> {
        match &self.data {
            CompactFeedDataV2::TripUpdates(feed) => Cow::Owned(feed.expanded_entities()),
            CompactFeedDataV2::TripDescriptorColumns(feed) => Cow::Owned(feed.expanded_entities()),
            CompactFeedDataV2::Legacy(entities) => Cow::Borrowed(entities),
        }
    }

    pub fn to_legacy(&self) -> CompactFeedMessage {
        CompactFeedMessage {
            header: self.header.clone(),
            reference_epoch: self.reference_epoch,
            entity: self.entities().into_owned(),
        }
    }

    pub fn to_feed_message(&self) -> FeedMessage {
        self.to_legacy().to_feed_message()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtfs_realtime::trip_update::{StopTimeEvent, StopTimeUpdate};
    use gtfs_realtime::{FeedEntity, FeedHeader, TripUpdate};

    fn test_message() -> FeedMessage {
        FeedMessage {
            header: FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                incrementality: None,
                timestamp: Some(1_700_000_000),
                feed_version: None,
            },
            entity: vec![FeedEntity {
                id: "a".into(),
                is_deleted: None,
                trip_update: Some(TripUpdate {
                    trip: TripDescriptor::default(),
                    vehicle: None,
                    stop_time_update: vec![StopTimeUpdate {
                        stop_sequence: Some(1),
                        stop_id: Some("stop-1".into()),
                        arrival: Some(StopTimeEvent {
                            delay: Some(60),
                            time: Some(1_700_000_100),
                            uncertainty: None,
                            scheduled_time: None,
                        }),
                        departure: Some(StopTimeEvent {
                            delay: None,
                            time: Some(1_700_000_120),
                            uncertainty: None,
                            scheduled_time: None,
                        }),
                        ..Default::default()
                    }],
                    timestamp: None,
                    delay: None,
                    trip_properties: None,
                }),
                vehicle: None,
                alert: None,
                shape: None,
                stop: None,
                trip_modifications: None,
            }],
        }
    }

    #[test]
    fn absent_fields_have_no_columns() {
        let v2 = CompactFeedMessageV2::from_feed_message(test_message());
        assert!(!v2.features.contains(RtFeatures::UNCERTAINTY));
        assert!(!v2.features.contains(RtFeatures::SCHEDULED_TIME));
        assert!(!v2.features.contains(RtFeatures::DEPARTURE_OCCUPANCY));
        assert!(!v2.features.contains(RtFeatures::STOP_TIME_PROPERTIES));
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &v2.data else {
            panic!("expected trip feed");
        };
        assert!(feed.stops.events.uncertainty.is_absent());
        assert!(feed.stops.events.scheduled_time.is_absent());
        assert!(feed.stops.departure_occupancy_status.is_absent());
        assert!(feed.stops.stop_time_properties.is_absent());
        assert_eq!(size_of::<CompactStopRow>(), 16);
    }

    #[test]
    fn round_trip_optional_fields_and_missing_core_values() {
        let mut raw = test_message();
        let trip = raw.entity[0].trip_update.as_mut().unwrap();
        trip.stop_time_update.push(StopTimeUpdate {
            stop_sequence: None,
            stop_id: None,
            arrival: Some(StopTimeEvent {
                delay: None,
                time: None,
                uncertainty: Some(15),
                scheduled_time: Some(1_700_000_200),
            }),
            departure_occupancy_status: Some(0),
            stop_time_properties: Some(StopTimeProperties::default()),
            ..Default::default()
        });
        let v2 = CompactFeedMessageV2::from_feed_message(raw.clone());
        assert!(v2.features.contains(RtFeatures::UNCERTAINTY));
        assert!(v2.features.contains(RtFeatures::DEPARTURE_OCCUPANCY));
        assert!(v2.features.contains(RtFeatures::STOP_TIME_PROPERTIES));
        assert_eq!(v2.to_feed_message(), raw);
    }

    /// Verify that the common signed two-byte representation covers nine
    /// hours in either direction without losing one-second resolution.
    #[test]
    fn nine_hour_delays_use_two_byte_signed_storage() {
        let mut raw = test_message();
        let trip = raw.entity[0].trip_update.as_mut().unwrap();
        trip.delay = Some(-NINE_HOURS_SECONDS);
        let stop = trip.stop_time_update.first_mut().unwrap();
        stop.arrival.as_mut().unwrap().delay = Some(NINE_HOURS_SECONDS);
        stop.departure.as_mut().unwrap().delay = Some(-1);

        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected compact trip feed");
        };
        assert!(compact.features.contains(RtFeatures::DELAY));
        assert!(matches!(
            feed.stops.events.delay,
            CompactDelayColumn::I16(_)
        ));
        assert!(matches!(feed.trip_delay, CompactDelayColumn::I16(_)));
        assert_eq!(size_of::<i16>(), 2);
        assert_eq!(compact.to_feed_message(), raw);
    }

    #[test]
    fn exceptional_event_delay_promotes_all_delay_columns_to_i24() {
        let mut raw = test_message();
        let trip = raw.entity[0].trip_update.as_mut().unwrap();
        trip.delay = Some(15);
        trip.stop_time_update[0].arrival.as_mut().unwrap().delay = Some(NINE_HOURS_SECONDS + 1);

        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected compact trip feed");
        };
        assert!(matches!(
            feed.stops.events.delay,
            CompactDelayColumn::I24(_)
        ));
        assert!(matches!(feed.trip_delay, CompactDelayColumn::I24(_)));
        assert_eq!(compact.to_feed_message(), raw);

        // Negative exceptions must also promote the whole feed.
        let trip = raw.entity[0].trip_update.as_mut().unwrap();
        trip.stop_time_update[0].arrival.as_mut().unwrap().delay = Some(-NINE_HOURS_SECONDS - 1);
        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected compact trip feed");
        };
        assert!(matches!(
            feed.stops.events.delay,
            CompactDelayColumn::I24(_)
        ));
        assert_eq!(compact.to_feed_message(), raw);
    }

    #[test]
    fn exceptional_trip_delay_promotes_event_delays_to_i24() {
        let mut raw = test_message();
        let trip = raw.entity[0].trip_update.as_mut().unwrap();
        trip.delay = Some(NINE_HOURS_SECONDS + 1);
        trip.stop_time_update[0].arrival.as_mut().unwrap().delay = Some(8);

        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected compact trip feed");
        };
        assert!(matches!(
            feed.stops.events.delay,
            CompactDelayColumn::I24(_)
        ));
        assert!(matches!(feed.trip_delay, CompactDelayColumn::I24(_)));
        assert_eq!(compact.to_feed_message(), raw);
    }

    #[test]
    fn trip_only_delay_is_preserved_and_sets_feature() {
        let mut raw = test_message();
        let trip = raw.entity[0].trip_update.as_mut().unwrap();
        trip.delay = Some(0);
        for stop in &mut trip.stop_time_update {
            if let Some(arrival) = &mut stop.arrival {
                arrival.delay = None;
            }
            if let Some(departure) = &mut stop.departure {
                departure.delay = None;
            }
        }
        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        assert!(compact.features.contains(RtFeatures::DELAY));
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected compact trip feed");
        };
        assert!(feed.stops.events.delay.is_absent());
        assert!(matches!(feed.trip_delay, CompactDelayColumn::I16(_)));
        assert_eq!(compact.to_feed_message(), raw);
    }

    #[test]
    fn i24_bounds_and_i32_overflow_fallback_are_lossless() {
        for event_delay in [
            I24_MIN_SECONDS,
            I24_MAX_SECONDS,
            I24_MIN_SECONDS - 1,
            I24_MAX_SECONDS + 1,
            i32::MIN,
            i32::MAX,
        ] {
            let mut raw = test_message();
            let trip = raw.entity[0].trip_update.as_mut().unwrap();
            trip.delay = Some(-12);
            trip.stop_time_update[0].arrival.as_mut().unwrap().delay = Some(event_delay);

            let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
            let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
                panic!("expected compact trip feed");
            };
            if (I24_MIN_SECONDS..=I24_MAX_SECONDS).contains(&event_delay) {
                assert!(matches!(
                    feed.stops.events.delay,
                    CompactDelayColumn::I24(_)
                ));
                assert!(matches!(feed.trip_delay, CompactDelayColumn::I24(_)));
            } else {
                assert!(matches!(
                    feed.stops.events.delay,
                    CompactDelayColumn::I32(_)
                ));
                assert!(matches!(feed.trip_delay, CompactDelayColumn::I32(_)));
            }
            assert_eq!(compact.to_feed_message(), raw);
        }
    }

    #[test]
    fn absent_delays_use_no_column_at_all() {
        let mut raw = test_message();
        raw.entity[0].trip_update.as_mut().unwrap().stop_time_update[0]
            .arrival
            .as_mut()
            .unwrap()
            .delay = None;
        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        assert!(!compact.features.contains(RtFeatures::DELAY));
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected compact trip feed");
        };
        assert!(feed.stops.events.delay.is_absent());
        assert!(feed.trip_delay.is_absent());
        assert_eq!(compact.to_feed_message(), raw);
    }

    #[test]
    fn mixed_feed_falls_back_without_discarding_payloads() {
        let mut raw = test_message();
        raw.entity[0].alert = Some(Default::default());
        let v2 = CompactFeedMessageV2::from_feed_message(raw.clone());
        assert!(matches!(v2.data, CompactFeedDataV2::Legacy(_)));
        assert_eq!(v2.to_feed_message(), raw);
    }

    #[test]
    fn descriptor_rows_skip_deleted_entities_and_round_trip_serialization() {
        let mut raw = test_message();
        let mut deleted = raw.entity[0].clone();
        deleted.id = "deleted".into();
        deleted.is_deleted = Some(true);
        deleted.trip_update = None;
        raw.entity.insert(0, deleted);
        let mut second = raw.entity[1].clone();
        second.id = "second".into();
        second.trip_update.as_mut().unwrap().trip.route_id = Some("route-2".into());
        raw.entity.push(second);
        let compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        let CompactFeedDataV2::TripDescriptorColumns(feed) = &compact.data else {
            panic!("expected descriptor columns");
        };
        assert_eq!(feed.descriptors.count, 2);
        assert_eq!(feed.entities[1].trip_update.as_ref().unwrap().trip_index, 0);
        assert_eq!(feed.entities[2].trip_update.as_ref().unwrap().trip_index, 1);
        let bytes = crate::bincode_serialize(&compact).unwrap();
        let decoded: CompactFeedMessageV2 = crate::bincode_deserialize(&bytes).unwrap();
        assert_eq!(decoded.to_feed_message(), raw);
    }

    #[test]
    fn existing_v2_variant_remains_decodable() {
        let raw = test_message();
        let mut compact = CompactFeedMessageV2::from_feed_message(raw.clone());
        let CompactFeedDataV2::TripDescriptorColumns(feed) = compact.data else {
            panic!("expected descriptor columns");
        };
        let entities = feed
            .entities
            .iter()
            .map(|entity| CompactTripEntityV2 {
                id: entity.id.clone(),
                is_deleted: entity.is_deleted,
                trip_update: entity.trip_update.as_ref().map(|tu| CompactTripUpdateV2 {
                    trip: feed.descriptors.get(tu.trip_index as usize).unwrap().to_owned(),
                    vehicle: tu.vehicle.clone(),
                    stop_start: tu.stop_start,
                    stop_len: tu.stop_len,
                    timestamp: tu.timestamp,
                    trip_properties: tu.trip_properties.clone(),
                }),
            })
            .collect();
        compact.data = CompactFeedDataV2::TripUpdates(CompactTripFeedV2 {
            entities,
            trip_delay: feed.trip_delay,
            stops: feed.stops,
        });
        let bytes = crate::bincode_serialize(&compact).unwrap();
        let decoded: CompactFeedMessageV2 = crate::bincode_deserialize(&bytes).unwrap();
        assert_eq!(decoded.to_feed_message(), raw);
    }
}
