use compact_str::CompactString;
use gtfs_realtime::trip_update::stop_time_update::StopTimeProperties;
use gtfs_realtime::trip_update::{StopTimeEvent, StopTimeUpdate, TripProperties};
use gtfs_realtime::{
    Alert, FeedEntity, FeedHeader, FeedMessage, Shape, Stop, TripModifications, TripUpdate,
    VehiclePosition,
};
use i24::I24;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactFeedMessage {
    pub header: CompactFeedHeader,
    pub entity: Vec<CompactFeedEntity>,
    pub reference_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactFeedHeader {
    pub gtfs_realtime_version: String,
    pub incrementality: Option<i32>,
    pub timestamp: Option<u64>,
    pub feed_version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactFeedEntity {
    pub id: String,
    pub is_deleted: Option<bool>,
    pub trip_update: Option<CompactTripUpdate>,
    pub vehicle: Option<VehiclePosition>,
    pub alert: Option<Alert>,
    pub shape: Option<Shape>,
    pub stop: Option<Stop>,
    pub trip_modifications: Option<TripModifications>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripUpdate {
    pub trip: gtfs_realtime::TripDescriptor,
    pub vehicle: Option<gtfs_realtime::VehicleDescriptor>,
    pub stop_time_update: Vec<CompactStopTimeUpdate>,
    pub timestamp: Option<u64>,
    pub delay: Option<i32>,
    pub trip_properties: Option<TripProperties>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactStopTimeUpdate {
    pub stop_sequence: Option<u16>,
    pub stop_id: Option<String>,
    pub arrival: Option<Box<CompactStopTimeEvent>>,
    pub departure: Option<Box<CompactStopTimeEvent>>,
    pub departure_occupancy_status: Option<i32>,
    pub schedule_relationship: Option<i32>,
    pub stop_time_properties: Option<Box<StopTimeProperties>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactStopTimeEvent {
    pub delay: Option<i32>,
    pub time: Option<I24>,
    pub uncertainty: Option<i32>,
    pub scheduled_time: Option<I24>,
}

impl CompactFeedMessage {
    pub fn from_feed_message(message: FeedMessage) -> Self {
        // Choose reference epoch. Try header timestamp, else 0.
        // Round down to nearest 65536.
        let timestamp = message.header.timestamp.unwrap_or(0);
        let reference_epoch = (timestamp / 65536) * 65536;

        let entity = message
            .entity
            .into_iter()
            .map(|e| CompactFeedEntity::from_feed_entity(e, reference_epoch))
            .collect();

        CompactFeedMessage {
            header: CompactFeedHeader::from_feed_header(message.header),
            entity,
            reference_epoch,
        }
    }

    pub fn to_feed_message(&self) -> FeedMessage {
        let entity = self
            .entity
            .iter()
            .map(|e| e.to_feed_entity(self.reference_epoch))
            .collect();

        FeedMessage {
            header: self.header.to_feed_header(),
            entity,
        }
    }
}

impl CompactFeedHeader {
    fn from_feed_header(header: FeedHeader) -> Self {
        CompactFeedHeader {
            gtfs_realtime_version: header.gtfs_realtime_version,
            incrementality: header.incrementality,
            timestamp: header.timestamp,
            feed_version: header.feed_version,
        }
    }

    fn to_feed_header(&self) -> FeedHeader {
        FeedHeader {
            gtfs_realtime_version: self.gtfs_realtime_version.clone(),
            incrementality: self.incrementality,
            timestamp: self.timestamp,
            feed_version: self.feed_version.clone(),
        }
    }
}

impl CompactFeedEntity {
    fn from_feed_entity(entity: FeedEntity, ref_epoch: u64) -> Self {
        CompactFeedEntity {
            id: entity.id,
            is_deleted: entity.is_deleted,
            trip_update: entity
                .trip_update
                .map(|tu| CompactTripUpdate::from_trip_update(tu, ref_epoch)),
            vehicle: entity.vehicle,
            alert: entity.alert,
            shape: entity.shape,
            stop: entity.stop,
            trip_modifications: entity.trip_modifications,
        }
    }

    fn to_feed_entity(&self, ref_epoch: u64) -> FeedEntity {
        FeedEntity {
            id: self.id.clone(),
            is_deleted: self.is_deleted,
            trip_update: self
                .trip_update
                .as_ref()
                .map(|tu| tu.to_trip_update(ref_epoch)),
            vehicle: self.vehicle.clone(),
            alert: self.alert.clone(),
            shape: self.shape.clone(),
            stop: self.stop.clone(),
            trip_modifications: self.trip_modifications.clone(),
        }
    }
}

impl CompactTripUpdate {
    fn from_trip_update(tu: TripUpdate, ref_epoch: u64) -> Self {
        CompactTripUpdate {
            trip: tu.trip,
            vehicle: tu.vehicle,
            stop_time_update: tu
                .stop_time_update
                .into_iter()
                .map(|stu| CompactStopTimeUpdate::from_stop_time_update(stu, ref_epoch))
                .collect(),
            timestamp: tu.timestamp,
            delay: tu.delay,
            trip_properties: tu.trip_properties,
        }
    }

    fn to_trip_update(&self, ref_epoch: u64) -> TripUpdate {
        TripUpdate {
            trip: self.trip.clone(),
            vehicle: self.vehicle.clone(),
            stop_time_update: self
                .stop_time_update
                .iter()
                .map(|stu| stu.to_stop_time_update(ref_epoch))
                .collect(),
            timestamp: self.timestamp,
            delay: self.delay,
            trip_properties: self.trip_properties.clone(),
        }
    }
}

impl CompactStopTimeUpdate {
    fn from_stop_time_update(stu: StopTimeUpdate, ref_epoch: u64) -> Self {
        CompactStopTimeUpdate {
            stop_sequence: stu.stop_sequence.map(|s| s as u16),
            stop_id: stu.stop_id,
            arrival: stu
                .arrival
                .map(|a| Box::new(CompactStopTimeEvent::from_stop_time_event(a, ref_epoch))),
            departure: stu
                .departure
                .map(|d| Box::new(CompactStopTimeEvent::from_stop_time_event(d, ref_epoch))),
            departure_occupancy_status: stu.departure_occupancy_status,
            schedule_relationship: stu.schedule_relationship,
            stop_time_properties: stu.stop_time_properties.map(Box::new),
        }
    }

    fn to_stop_time_update(&self, ref_epoch: u64) -> StopTimeUpdate {
        StopTimeUpdate {
            stop_sequence: self.stop_sequence.map(|s| s as u32),
            stop_id: self.stop_id.clone(),
            arrival: self
                .arrival
                .as_ref()
                .map(|a| a.to_stop_time_event(ref_epoch)),
            departure: self
                .departure
                .as_ref()
                .map(|d| d.to_stop_time_event(ref_epoch)),
            departure_occupancy_status: self.departure_occupancy_status,
            schedule_relationship: self.schedule_relationship,
            stop_time_properties: self.stop_time_properties.as_deref().cloned(),
        }
    }
}

impl CompactStopTimeEvent {
    fn from_stop_time_event(ste: StopTimeEvent, ref_epoch: u64) -> Self {
        let time = ste.time.map(|t| {
            let diff = t - (ref_epoch as i64);
            I24::wrapping_from_i32(diff as i32)
        });
        let scheduled_time = ste.scheduled_time.map(|t| {
            let diff = t - (ref_epoch as i64);
            I24::wrapping_from_i32(diff as i32)
        });
        CompactStopTimeEvent {
            delay: ste.delay,
            time,
            uncertainty: ste.uncertainty,
            scheduled_time,
        }
    }

    fn to_stop_time_event(&self, ref_epoch: u64) -> StopTimeEvent {
        let time = self.time.map(|t| {
            let diff: i32 = t.into();
            (ref_epoch as i64) + (diff as i64)
        });
        let scheduled_time = self.scheduled_time.map(|t| {
            let diff: i32 = t.into();
            (ref_epoch as i64) + (diff as i64)
        });
        StopTimeEvent {
            delay: self.delay,
            time,
            uncertainty: self.uncertainty,
            scheduled_time,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactItineraryPatternRow {
    pub stop_sequence: i32,
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub interpolated_time_since_start: Option<i32>,
    pub stop_id: CompactString,
    pub gtfs_stop_sequence: u32,
    pub timepoint: Option<bool>,
    pub stop_headsign_idx: Option<i16>,
}
