use crate::ahash_fast_hash;
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughFeedEntity {
    pub is_deleted: Option<bool>,
    pub trip_update: Option<u64>,
    pub vehicle: Option<u64>,
    pub alert: Option<u64>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashAlert {
    pub active_period: Vec<RoughTimeRange>,
    pub informed_entity: Vec<RoughEntitySelector>,
    pub cause: Option<i32>,
    pub effect: Option<i32>,
    pub url: Option<HashTranslatedString>,
    pub header_text: Option<HashTranslatedString>,
    pub description_text: Option<HashTranslatedString>,
    pub tts_header_text: Option<HashTranslatedString>,
    pub tts_description_text: Option<HashTranslatedString>,
    pub severity_level: Option<i32>,
    pub image: Option<HashTranslatedImage>,
    pub image_alternative_text: Option<HashTranslatedString>,
    pub cause_detail: Option<HashTranslatedString>,
    pub effect_detail: Option<HashTranslatedString>,
}

impl From<gtfs_realtime::Alert> for HashAlert {
    fn from(input: gtfs_realtime::Alert) -> Self {
        HashAlert {
            active_period: input.active_period.into_iter().map(|x| x.into()).collect(),
            informed_entity: input
                .informed_entity
                .into_iter()
                .map(|x| x.into())
                .collect(),
            cause: input.cause,
            effect: input.effect,
            url: input.url.map(|x| x.into()),
            header_text: input.header_text.map(|x| x.into()),
            description_text: input.description_text.map(|x| x.into()),
            tts_header_text: input.tts_header_text.map(|x| x.into()),
            tts_description_text: input.tts_description_text.map(|x| x.into()),
            severity_level: input.severity_level,
            image: input.image.map(|x| x.into()),
            image_alternative_text: input.image_alternative_text.map(|x| x.into()),
            cause_detail: input.cause_detail.map(|x| x.into()),
            effect_detail: input.effect_detail.map(|x| x.into()),
        }
    }
}

impl From<gtfs_realtime::TranslatedString> for HashTranslatedString {
    fn from(input: gtfs_realtime::TranslatedString) -> Self {
        HashTranslatedString {
            translation: input.translation.into_iter().map(|x| x.into()).collect(),
        }
    }
}

use gtfs_realtime::translated_string::Translation;

impl From<Translation> for HashTranslation {
    fn from(input: Translation) -> Self {
        HashTranslation {
            language: input.language,
            text: input.text,
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashTranslatedImage {
    pub localized_image: Vec<HashLocalizedImage>,
}

impl From<gtfs_realtime::TranslatedImage> for HashTranslatedImage {
    fn from(input: gtfs_realtime::TranslatedImage) -> Self {
        HashTranslatedImage {
            localized_image: input
                .localized_image
                .into_iter()
                .map(|x| x.into())
                .collect(),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashLocalizedImage {
    pub url: String,
    pub media_type: String,
    pub language: Option<String>,
}

use gtfs_realtime::translated_image::LocalizedImage;

impl From<LocalizedImage> for HashLocalizedImage {
    fn from(input: LocalizedImage) -> Self {
        HashLocalizedImage {
            language: input.language,
            url: input.url,
            media_type: input.media_type,
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashTranslatedString {
    pub translation: Vec<HashTranslation>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashTranslation {
    pub language: Option<String>,
    pub text: String,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughEntitySelector {
    pub agency_id: Option<String>,
    pub route_id: Option<String>,
    pub route_type: Option<i32>,
    pub trip: Option<RoughTripDescriptor>,
    pub stop_id: Option<String>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughTimeRange {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

impl From<gtfs_realtime::TimeRange> for RoughTimeRange {
    fn from(input: gtfs_realtime::TimeRange) -> Self {
        RoughTimeRange {
            start: input.start,
            end: input.end,
        }
    }
}

impl From<gtfs_realtime::EntitySelector> for RoughEntitySelector {
    fn from(input: gtfs_realtime::EntitySelector) -> Self {
        RoughEntitySelector {
            agency_id: input.agency_id,
            route_id: input.route_id,
            route_type: input.route_type,
            trip: input.trip.map(|x| x.into()),
            stop_id: input.stop_id,
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughVehicleEntity {
    pub trip: Option<RoughTripDescriptor>,
    pub vehicle: Option<RoughVehicleDescriptor>,
    pub position: Option<HashPosition>,
    pub current_stop_sequence: Option<u32>,
    pub stop_id: Option<String>,
    pub current_status: Option<i32>,
    pub congestion_level: Option<i32>,
    pub occupancy_status: Option<i32>,
    pub occupancy_percentage: Option<u32>,
}

impl From<gtfs_realtime::Position> for HashPosition {
    fn from(input: gtfs_realtime::Position) -> Self {
        HashPosition {
            latitude: OrderedFloat(input.latitude),
            longitude: OrderedFloat(input.longitude),
            bearing: input.bearing.map(OrderedFloat),
            odometer: input.odometer.map(OrderedFloat),
            speed: input.speed.map(OrderedFloat),
        }
    }
}

impl From<gtfs_realtime::VehiclePosition> for RoughVehicleEntity {
    fn from(input: gtfs_realtime::VehiclePosition) -> Self {
        RoughVehicleEntity {
            trip: input.trip.map(|x| x.into()),
            vehicle: input.vehicle.map(|x| x.into()),
            position: input.position.map(|x| x.into()),
            current_stop_sequence: input.current_stop_sequence,
            stop_id: input.stop_id,
            current_status: input.current_status,
            congestion_level: input.congestion_level,
            occupancy_status: input.occupancy_status,
            occupancy_percentage: input.occupancy_percentage,
        }
    }
}

impl From<gtfs_realtime::VehicleDescriptor> for RoughVehicleDescriptor {
    fn from(input: gtfs_realtime::VehicleDescriptor) -> Self {
        RoughVehicleDescriptor {
            id: input.id,
            label: input.label,
            license_plate: input.license_plate,
            wheelchair_accessible: input.wheelchair_accessible,
        }
    }
}

impl From<gtfs_realtime::trip_descriptor::ModifiedTripSelector> for RoughModifiedTripSelector {
    fn from(input: gtfs_realtime::trip_descriptor::ModifiedTripSelector) -> Self {
        RoughModifiedTripSelector {
            modifications_id: input.modifications_id,
            affected_trip_id: input.affected_trip_id,
        }
    }
}

impl From<gtfs_realtime::TripDescriptor> for RoughTripDescriptor {
    fn from(input: gtfs_realtime::TripDescriptor) -> Self {
        RoughTripDescriptor {
            trip_id: input.trip_id,
            route_id: input.route_id,
            direction_id: input.direction_id,
            start_time: input.start_time,
            start_date: input.start_date,
            schedule_relationship: input.schedule_relationship,
            modified_trip: input.modified_trip.map(|x| x.into()),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashPosition {
    pub latitude: OrderedFloat<f32>,
    pub longitude: OrderedFloat<f32>,
    pub bearing: Option<OrderedFloat<f32>>,
    pub odometer: Option<OrderedFloat<f64>>,
    pub speed: Option<OrderedFloat<f32>>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughTripUpdate {
    pub trip: RoughTripDescriptor,
    pub vehicle: Option<RoughVehicleDescriptor>,
    pub stop_time_update: Vec<HashStopTimeUpdate>,
    pub delay: Option<i32>,
    pub trip_properties: Option<HashTripProperties>,
}

impl From<gtfs_realtime::TripUpdate> for RoughTripUpdate {
    fn from(input: gtfs_realtime::TripUpdate) -> Self {
        RoughTripUpdate {
            trip: input.trip.into(),
            vehicle: input.vehicle.map(|x| x.into()),
            stop_time_update: input
                .stop_time_update
                .into_iter()
                .map(|x| x.into())
                .collect(),
            delay: input.delay,
            trip_properties: input.trip_properties.map(|x| x.into()),
        }
    }
}

impl From<gtfs_realtime::trip_update::TripProperties> for HashTripProperties {
    fn from(input: gtfs_realtime::trip_update::TripProperties) -> Self {
        HashTripProperties {
            trip_id: input.trip_id,
            start_date: input.start_date,
            start_time: input.start_time,
            shape_id: input.shape_id,
        }
    }
}

impl From<gtfs_realtime::trip_update::StopTimeUpdate> for HashStopTimeUpdate {
    fn from(input: gtfs_realtime::trip_update::StopTimeUpdate) -> Self {
        HashStopTimeUpdate {
            stop_sequence: input.stop_sequence,
            stop_id: input.stop_id,
            arrival: input.arrival.map(|x| x.into()),
            departure: input.departure.map(|x| x.into()),
            departure_occupancy_status: input.departure_occupancy_status,
            schedule_relationship: input.schedule_relationship,
            stop_time_properties: input.stop_time_properties.map(|x| x.into()),
        }
    }
}

impl From<gtfs_realtime::trip_update::stop_time_update::StopTimeProperties>
    for HashStopTimeProperties
{
    fn from(input: gtfs_realtime::trip_update::stop_time_update::StopTimeProperties) -> Self {
        HashStopTimeProperties {
            assigned_stop_id: input.assigned_stop_id,
        }
    }
}

impl From<gtfs_realtime::trip_update::StopTimeEvent> for HashStopTimeEvent {
    fn from(input: gtfs_realtime::trip_update::StopTimeEvent) -> Self {
        HashStopTimeEvent {
            delay: input.delay,
            time: input.time,
            uncertainty: input.uncertainty,
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashTripProperties {
    pub trip_id: Option<String>,
    pub start_date: Option<String>,
    pub start_time: Option<String>,
    pub shape_id: Option<String>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashStopTimeUpdate {
    pub stop_sequence: Option<u32>,
    pub stop_id: Option<String>,
    pub arrival: Option<HashStopTimeEvent>,
    pub departure: Option<HashStopTimeEvent>,
    pub departure_occupancy_status: Option<i32>,
    pub schedule_relationship: Option<i32>,
    pub stop_time_properties: Option<HashStopTimeProperties>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashStopTimeEvent {
    pub delay: Option<i32>,
    pub time: Option<i64>,
    pub uncertainty: Option<i32>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HashStopTimeProperties {
    pub assigned_stop_id: Option<String>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughTripDescriptor {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub schedule_relationship: Option<i32>,
    pub modified_trip: Option<RoughModifiedTripSelector>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughVehicleDescriptor {
    pub id: Option<String>,
    pub label: Option<String>,
    pub license_plate: Option<String>,
    pub wheelchair_accessible: Option<i32>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughModifiedTripSelector {
    pub modifications_id: Option<String>,
    pub affected_trip_id: Option<String>,
}

pub fn rough_hash_of_gtfs_rt(input: &gtfs_realtime::FeedMessage) -> u64 {
    let mut hashed_entries: BTreeMap<String, u64> = BTreeMap::new();

    for entity in &input.entity {
        let rough_entity = RoughFeedEntity {
            is_deleted: entity.is_deleted,
            trip_update: match entity.trip_update.clone() {
                Some(trip_update) => {
                    let trip_update_entity: RoughTripUpdate = trip_update.into();
                    let trip_update_hash = ahash_fast_hash(&trip_update_entity);
                    Some(trip_update_hash)
                }
                None => None,
            },
            vehicle: match entity.vehicle.clone() {
                Some(vehicle) => {
                    let vehicle_entity: RoughVehicleEntity = vehicle.into();
                    let vehicle_hash = ahash_fast_hash(&vehicle_entity);
                    Some(vehicle_hash)
                }
                None => None,
            },
            alert: match entity.alert.clone() {
                Some(alert) => {
                    let alert_entity: HashAlert = alert.into();
                    let alert_hash = ahash_fast_hash(&alert_entity);
                    Some(alert_hash)
                }
                None => None,
            },
        };

        let hash_of_entry = ahash_fast_hash(&rough_entity);
        let id = entity.id.clone();
        hashed_entries.insert(id, hash_of_entry);
    }

    ahash_fast_hash(&hashed_entries)
}
