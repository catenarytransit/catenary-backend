use crate::RtCacheEntry;
use crate::RtKey;
use ahash::AHashMap;
use compact_str::CompactString;
use ecow::EcoString;
use gtfs_realtime::FeedEntity;
use std::hash::Hash;
use std::sync::Arc;

use crate::compact_formats::CompactItineraryPatternRow;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ItineraryPatternInternalCache {
    pub itinerary_patterns: AHashMap<
        String,
        (
            crate::models::ItineraryPatternMeta,
            Vec<CompactItineraryPatternRow>,
        ),
    >,
    pub last_time_full_refreshed: chrono::DateTime<chrono::Utc>,
}

impl ItineraryPatternInternalCache {
    pub fn new() -> Self {
        ItineraryPatternInternalCache {
            itinerary_patterns: AHashMap::new(),
            last_time_full_refreshed: chrono::Utc::now(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CompressedTripInternalCache {
    pub compressed_trips: AHashMap<String, crate::models::CompressedTrip>,
    pub last_time_full_refreshed: chrono::DateTime<chrono::Utc>,
}

impl CompressedTripInternalCache {
    pub fn new() -> Self {
        CompressedTripInternalCache {
            compressed_trips: AHashMap::new(),
            last_time_full_refreshed: chrono::Utc::now(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenisedData {
    pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
    pub vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache>,
    pub vehicle_routes_cache_hash: u64,
    pub vehicle_label_to_gtfs_id: AHashMap<String, String>,
    //id to trip update
    pub trip_updates: AHashMap<CompactString, AspenisedTripUpdate>,
    pub trip_updates_lookup_by_trip_id_to_trip_update_ids:
        AHashMap<CompactString, Vec<CompactString>>,
    //  pub raw_alerts: AHashMap<String, gtfs_realtime::Alert>,
    pub trip_updates_lookup_by_route_id_to_trip_update_ids:
        AHashMap<CompactString, Vec<CompactString>>,
    pub aspenised_alerts: AHashMap<String, AspenisedAlert>,
    pub impacted_routes_alerts: AHashMap<String, Vec<String>>,
    pub impacted_stops_alerts: AHashMap<String, Vec<String>>,
    pub impacted_trips_alerts: AHashMap<String, Vec<String>>,
    pub trip_id_to_vehicle_gtfs_rt_id: AHashMap<String, Vec<String>>,
    pub last_updated_time_ms: u64,
    #[serde(skip, default = "ItineraryPatternInternalCache::new")]
    pub itinerary_pattern_internal_cache: ItineraryPatternInternalCache,
    #[serde(skip, default = "CompressedTripInternalCache::new")]
    pub compressed_trip_internal_cache: CompressedTripInternalCache,
    pub stop_id_to_stop: AHashMap<CompactString, AspenisedStop>,
    pub shape_id_to_shape: AHashMap<CompactString, Option<EcoString>>,
    pub trip_modifications: AHashMap<CompactString, AspenisedTripModification>,
    pub trip_id_to_trip_modification_ids: AHashMap<CompactString, Vec<EcoString>>,
    pub stop_id_to_trip_modification_ids: AHashMap<CompactString, Vec<EcoString>>,
    pub stop_id_to_non_scheduled_trip_ids: AHashMap<CompactString, Vec<EcoString>>,
    pub stop_id_to_parent_id: AHashMap<CompactString, CompactString>,
    pub parent_id_to_children_ids: AHashMap<CompactString, Vec<CompactString>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenisedTripModification {
    pub selected_trips: Vec<AspenSelectedTrips>,
    pub start_times: Vec<String>,
    pub service_dates: Vec<chrono::NaiveDate>,
    pub modifications: Vec<AspenModification>,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenSelectedTrips {
    pub trip_ids: Vec<String>,
    pub shape_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenStopSelector {
    pub stop_sequence: Option<u32>,
    pub stop_id: Option<EcoString>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenReplacementStop {
    pub travel_time_to_stop: Option<i32>,
    pub stop_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenModification {
    pub start_stop_selector: Option<AspenStopSelector>,
    pub end_stop_selector: Option<AspenStopSelector>,
    pub propagated_modification_delay: Option<i32>,
    pub replacement_stops: Vec<AspenReplacementStop>,
    pub service_alert_id: Option<String>,
    pub last_modified_time: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedStop {
    pub stop_id: Option<String>,
    pub stop_code: Option<AspenTranslatedString>,
    pub stop_name: Option<AspenTranslatedString>,
    pub tts_stop_name: Option<AspenTranslatedString>,
    pub stop_desc: Option<AspenTranslatedString>,
    pub stop_lat: Option<f32>,
    pub stop_lon: Option<f32>,
    pub zone_id: Option<String>,
    pub stop_url: Option<AspenTranslatedString>,
    pub parent_station: Option<String>,
    pub stop_timezone: Option<String>,
    pub wheelchair_boarding: Option<i32>,
    pub level_id: Option<String>,
    pub platform_code: Option<AspenTranslatedString>,
}

impl From<gtfs_realtime::TripModifications> for AspenisedTripModification {
    fn from(trip_modifications: gtfs_realtime::TripModifications) -> Self {
        AspenisedTripModification {
            selected_trips: trip_modifications
                .selected_trips
                .into_iter()
                .map(|x| x.into())
                .collect(),
            start_times: trip_modifications.start_times,
            service_dates: trip_modifications
                .service_dates
                .into_iter()
                .map(|date| chrono::NaiveDate::parse_from_str(&date, "%Y%m%d"))
                .flatten()
                .collect(),
            modifications: trip_modifications
                .modifications
                .into_iter()
                .map(|x| x.into())
                .collect(),
        }
    }
}

impl From<gtfs_realtime::trip_modifications::Modification> for AspenModification {
    fn from(modification: gtfs_realtime::trip_modifications::Modification) -> Self {
        AspenModification {
            start_stop_selector: modification.start_stop_selector.map(|x| x.into()),
            end_stop_selector: modification.end_stop_selector.map(|x| x.into()),
            propagated_modification_delay: modification.propagated_modification_delay,
            replacement_stops: modification
                .replacement_stops
                .into_iter()
                .map(|x| x.into())
                .collect(),
            service_alert_id: modification.service_alert_id,
            last_modified_time: modification.last_modified_time,
        }
    }
}

impl From<gtfs_realtime::trip_modifications::SelectedTrips> for AspenSelectedTrips {
    fn from(selected_trips: gtfs_realtime::trip_modifications::SelectedTrips) -> Self {
        AspenSelectedTrips {
            trip_ids: selected_trips.trip_ids,
            shape_id: selected_trips.shape_id,
        }
    }
}

impl From<gtfs_realtime::ReplacementStop> for AspenReplacementStop {
    fn from(replacement_stop: gtfs_realtime::ReplacementStop) -> Self {
        AspenReplacementStop {
            travel_time_to_stop: replacement_stop.travel_time_to_stop,
            stop_id: replacement_stop.stop_id,
        }
    }
}

impl From<gtfs_realtime::StopSelector> for AspenStopSelector {
    fn from(stop_selector: gtfs_realtime::StopSelector) -> Self {
        AspenStopSelector {
            stop_sequence: stop_selector.stop_sequence,
            stop_id: stop_selector.stop_id.map(|x| x.into()),
        }
    }
}

impl From<gtfs_realtime::Stop> for AspenisedStop {
    fn from(stop: gtfs_realtime::Stop) -> Self {
        AspenisedStop {
            stop_id: stop.stop_id,
            stop_code: stop.stop_code.map(|x| x.into()),
            stop_name: stop.stop_name.map(|x| x.into()),
            tts_stop_name: stop.tts_stop_name.map(|x| x.into()),
            stop_desc: stop.stop_desc.map(|x| x.into()),
            stop_lat: stop.stop_lat,
            stop_lon: stop.stop_lon,
            zone_id: stop.zone_id,
            stop_url: stop.stop_url.map(|x| x.into()),
            parent_station: stop.parent_station,
            stop_timezone: stop.stop_timezone,
            wheelchair_boarding: stop.wheelchair_boarding,
            level_id: stop.level_id,
            platform_code: stop.platform_code.map(|x| x.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenTimeRange {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenEntitySelector {
    pub agency_id: Option<String>,
    pub route_id: Option<String>,
    pub route_type: Option<i32>,
    pub trip: Option<AspenRawTripInfo>,
    pub stop_id: Option<String>,
    pub direction_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenTranslatedString {
    pub translation: Vec<AspenTranslation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenTranslation {
    pub text: String,
    pub language: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenTranslatedImage {
    pub localised_image: Vec<AspenLocalisedImage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenLocalisedImage {
    pub url: String,
    pub media_type: String,
    pub language: Option<String>,
}

impl From<gtfs_realtime::TranslatedString> for AspenTranslatedString {
    fn from(translated_string: gtfs_realtime::TranslatedString) -> Self {
        AspenTranslatedString {
            translation: translated_string
                .translation
                .into_iter()
                .map(|x| x.into())
                .collect(),
        }
    }
}

impl From<gtfs_realtime::translated_image::LocalizedImage> for AspenLocalisedImage {
    fn from(localised_image: gtfs_realtime::translated_image::LocalizedImage) -> Self {
        AspenLocalisedImage {
            url: localised_image.url,
            media_type: localised_image.media_type,
            language: localised_image.language,
        }
    }
}

impl From<gtfs_realtime::translated_string::Translation> for AspenTranslation {
    fn from(translation: gtfs_realtime::translated_string::Translation) -> Self {
        AspenTranslation {
            text: translation.text,
            language: translation.language,
        }
    }
}

impl From<gtfs_realtime::TranslatedImage> for AspenTranslatedImage {
    fn from(translated_image: gtfs_realtime::TranslatedImage) -> Self {
        AspenTranslatedImage {
            localised_image: translated_image
                .localized_image
                .into_iter()
                .map(|x| x.into())
                .collect(),
        }
    }
}

impl From<gtfs_realtime::TimeRange> for AspenTimeRange {
    fn from(time_range: gtfs_realtime::TimeRange) -> Self {
        AspenTimeRange {
            start: time_range.start,
            end: time_range.end,
        }
    }
}

impl From<gtfs_realtime::EntitySelector> for AspenEntitySelector {
    fn from(entity_selector: gtfs_realtime::EntitySelector) -> Self {
        AspenEntitySelector {
            agency_id: entity_selector.agency_id,
            route_id: entity_selector.route_id,
            route_type: entity_selector.route_type,
            trip: entity_selector.trip.map(|x| x.into()),
            stop_id: entity_selector.stop_id,
            direction_id: entity_selector.direction_id,
        }
    }
}

impl From<gtfs_realtime::Alert> for AspenisedAlert {
    fn from(alert: gtfs_realtime::Alert) -> Self {
        AspenisedAlert {
            active_period: alert.active_period.into_iter().map(|x| x.into()).collect(),
            informed_entity: alert
                .informed_entity
                .into_iter()
                .map(|x| x.into())
                .collect(),
            cause: alert.cause,
            effect: alert.effect,
            url: alert.url.map(|x| x.into()),
            header_text: alert.header_text.map(|x| x.into()),
            description_text: alert.description_text.map(|x| x.into()),
            tts_header_text: alert.tts_header_text.map(|x| x.into()),
            tts_description_text: alert.tts_description_text.map(|x| x.into()),
            severity_level: alert.severity_level,
            image: alert.image.map(|x| x.into()),
            image_alternative_text: alert.image_alternative_text.map(|x| x.into()),
            cause_detail: alert.cause_detail.map(|x| x.into()),
            effect_detail: alert.effect_detail.map(|x| x.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenisedAlert {
    pub active_period: Vec<AspenTimeRange>,
    pub informed_entity: Vec<AspenEntitySelector>,
    pub cause: Option<i32>,
    pub effect: Option<i32>,
    pub url: Option<AspenTranslatedString>,
    pub header_text: Option<AspenTranslatedString>,
    pub description_text: Option<AspenTranslatedString>,
    pub tts_header_text: Option<AspenTranslatedString>,
    pub tts_description_text: Option<AspenTranslatedString>,
    pub severity_level: Option<i32>,
    pub image: Option<AspenTranslatedImage>,
    pub image_alternative_text: Option<AspenTranslatedString>,
    pub cause_detail: Option<AspenTranslatedString>,
    pub effect_detail: Option<AspenTranslatedString>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedTripUpdate {
    pub trip: AspenRawTripInfo,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub timestamp: Option<u64>,
    pub delay: Option<i32>,
    pub stop_time_update: Vec<AspenisedStopTimeUpdate>,
    pub trip_properties: Option<AspenTripProperties>,
    pub trip_headsign: Option<CompactString>,
    pub found_schedule_trip_id: bool,
    #[serde(default)]
    pub last_seen: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenTripProperties {
    pub trip_id: Option<String>,
    pub start_date: Option<chrono::NaiveDate>,
    pub start_time: Option<String>,
    pub shape_id: Option<String>,
    pub trip_headsign: Option<String>,
    pub trip_short_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenRawTripInfo {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time: Option<String>,
    pub start_date: Option<chrono::NaiveDate>,
    pub schedule_relationship: Option<AspenisedTripScheduleRelationship>,
    pub modified_trip: Option<ModifiedTripSelector>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct ModifiedTripSelector {
    pub modifications_id: Option<String>,
    pub affected_trip_id: Option<String>,
}

impl From<gtfs_realtime::trip_descriptor::ModifiedTripSelector> for ModifiedTripSelector {
    fn from(modified_trip_selector: gtfs_realtime::trip_descriptor::ModifiedTripSelector) -> Self {
        ModifiedTripSelector {
            modifications_id: modified_trip_selector.modifications_id,
            affected_trip_id: modified_trip_selector.affected_trip_id,
        }
    }
}

impl From<gtfs_realtime::TripDescriptor> for AspenRawTripInfo {
    fn from(trip_descriptor: gtfs_realtime::TripDescriptor) -> Self {
        AspenRawTripInfo {
            trip_id: trip_descriptor.trip_id,
            route_id: trip_descriptor.route_id,
            direction_id: trip_descriptor.direction_id,
            start_time: trip_descriptor.start_time,
            start_date: match &trip_descriptor.start_date {
                Some(date) => {
                    //chrono parse yyyymmdd

                    match chrono::NaiveDate::parse_from_str(&date, "%Y%m%d") {
                        Ok(date) => Some(date),
                        Err(_) => None,
                    }
                }
                None => None,
            },
            schedule_relationship: option_i32_to_schedule_relationship(
                &trip_descriptor.schedule_relationship,
            ),
            modified_trip: trip_descriptor.modified_trip.map(|x| x.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedStopTimeUpdate {
    pub stop_sequence: Option<u16>,
    pub stop_id: Option<ecow::EcoString>,
    pub arrival: Option<AspenStopTimeEvent>,
    pub departure: Option<AspenStopTimeEvent>,
    pub departure_occupancy_status: Option<AspenisedOccupancyStatus>,
    pub schedule_relationship: Option<AspenisedStopTimeScheduleRelationship>,
    pub stop_time_properties: Option<AspenisedStopTimeProperties>,
    pub platform_string: Option<ecow::EcoString>,
    pub old_rt_data: bool,
}

pub fn option_i32_to_occupancy_status(
    occupancy_status: &Option<i32>,
) -> Option<AspenisedOccupancyStatus> {
    match occupancy_status {
        Some(status) => match status {
            0 => Some(AspenisedOccupancyStatus::Empty),
            1 => Some(AspenisedOccupancyStatus::ManySeatsAvailable),
            2 => Some(AspenisedOccupancyStatus::FewSeatsAvailable),
            3 => Some(AspenisedOccupancyStatus::StandingRoomOnly),
            4 => Some(AspenisedOccupancyStatus::CrushedStandingRoomOnly),
            5 => Some(AspenisedOccupancyStatus::Full),
            6 => Some(AspenisedOccupancyStatus::NotAcceptingPassengers),
            7 => Some(AspenisedOccupancyStatus::NoDataAvailable),
            8 => Some(AspenisedOccupancyStatus::NotBoardable),
            _ => None,
        },
        None => None,
    }
}

pub fn occupancy_status_to_u8(occupancy_status: &AspenisedOccupancyStatus) -> u8 {
    match occupancy_status {
        AspenisedOccupancyStatus::Empty => 0,
        AspenisedOccupancyStatus::ManySeatsAvailable => 1,
        AspenisedOccupancyStatus::FewSeatsAvailable => 2,
        AspenisedOccupancyStatus::StandingRoomOnly => 3,
        AspenisedOccupancyStatus::CrushedStandingRoomOnly => 4,
        AspenisedOccupancyStatus::Full => 5,
        AspenisedOccupancyStatus::NotAcceptingPassengers => 6,
        AspenisedOccupancyStatus::NoDataAvailable => 7,
        AspenisedOccupancyStatus::NotBoardable => 8,
    }
}

pub fn schedule_relationship_to_u8(
    schedule_relationship: &AspenisedTripScheduleRelationship,
) -> u8 {
    match schedule_relationship {
        AspenisedTripScheduleRelationship::Scheduled => 0,
        AspenisedTripScheduleRelationship::Added => 1,
        AspenisedTripScheduleRelationship::Unscheduled => 2,
        AspenisedTripScheduleRelationship::Cancelled => 3,
        AspenisedTripScheduleRelationship::Replacement => 5,
        AspenisedTripScheduleRelationship::Duplicated => 6,
        AspenisedTripScheduleRelationship::Deleted => 7,
    }
}

pub fn option_i32_to_schedule_relationship(
    schedule_relationship: &Option<i32>,
) -> Option<AspenisedTripScheduleRelationship> {
    match schedule_relationship {
        Some(status) => match status {
            0 => Some(AspenisedTripScheduleRelationship::Scheduled),
            1 => Some(AspenisedTripScheduleRelationship::Added),
            2 => Some(AspenisedTripScheduleRelationship::Unscheduled),
            3 => Some(AspenisedTripScheduleRelationship::Cancelled),
            5 => Some(AspenisedTripScheduleRelationship::Replacement),
            6 => Some(AspenisedTripScheduleRelationship::Duplicated),
            7 => Some(AspenisedTripScheduleRelationship::Deleted),
            _ => None,
        },
        None => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AspenisedOccupancyStatus {
    Empty = 0,
    ManySeatsAvailable = 1,
    FewSeatsAvailable = 2,
    StandingRoomOnly = 3,
    CrushedStandingRoomOnly = 4,
    Full = 5,
    NotAcceptingPassengers = 6,
    NoDataAvailable = 7,
    NotBoardable = 8,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum AspenisedTripScheduleRelationship {
    Scheduled = 0,
    Added = 1,
    Unscheduled = 2,
    Cancelled = 3,
    Replacement = 5,
    Duplicated = 6,
    Deleted = 7,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum AspenisedStopTimeScheduleRelationship {
    Scheduled = 0,
    Skipped = 1,
    NoData = 2,
    Unscheduled = 3,
}

pub fn option_i32_to_stop_time_schedule_relationship(
    schedule_relationship: &Option<i32>,
) -> Option<AspenisedStopTimeScheduleRelationship> {
    match schedule_relationship {
        Some(status) => match status {
            0 => Some(AspenisedStopTimeScheduleRelationship::Scheduled),
            1 => Some(AspenisedStopTimeScheduleRelationship::Skipped),
            2 => Some(AspenisedStopTimeScheduleRelationship::NoData),
            3 => Some(AspenisedStopTimeScheduleRelationship::Unscheduled),
            _ => None,
        },
        None => None,
    }
}

impl Into<u8> for AspenisedStopTimeScheduleRelationship {
    fn into(self) -> u8 {
        match self {
            AspenisedStopTimeScheduleRelationship::Scheduled => 0,
            AspenisedStopTimeScheduleRelationship::Skipped => 1,
            AspenisedStopTimeScheduleRelationship::NoData => 2,
            AspenisedStopTimeScheduleRelationship::Unscheduled => 3,
        }
    }
}

impl Into<u8> for &AspenisedStopTimeScheduleRelationship {
    fn into(self) -> u8 {
        match self {
            AspenisedStopTimeScheduleRelationship::Scheduled => 0,
            AspenisedStopTimeScheduleRelationship::Skipped => 1,
            AspenisedStopTimeScheduleRelationship::NoData => 2,
            AspenisedStopTimeScheduleRelationship::Unscheduled => 3,
        }
    }
}

impl Into<u8> for AspenisedTripScheduleRelationship {
    fn into(self) -> u8 {
        match self {
            AspenisedTripScheduleRelationship::Scheduled => 0,
            AspenisedTripScheduleRelationship::Added => 1,
            AspenisedTripScheduleRelationship::Unscheduled => 2,
            AspenisedTripScheduleRelationship::Cancelled => 3,
            AspenisedTripScheduleRelationship::Replacement => 5,
            AspenisedTripScheduleRelationship::Duplicated => 6,
            AspenisedTripScheduleRelationship::Deleted => 7,
        }
    }
}

impl Into<u8> for &AspenisedTripScheduleRelationship {
    fn into(self) -> u8 {
        match self {
            AspenisedTripScheduleRelationship::Scheduled => 0,
            AspenisedTripScheduleRelationship::Added => 1,
            AspenisedTripScheduleRelationship::Unscheduled => 2,
            AspenisedTripScheduleRelationship::Cancelled => 3,
            AspenisedTripScheduleRelationship::Replacement => 5,
            AspenisedTripScheduleRelationship::Duplicated => 6,
            AspenisedTripScheduleRelationship::Deleted => 7,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AspenisedStopTimeProperties {
    pub assigned_stop_id: Option<EcoString>,
}

use gtfs_realtime::trip_update::StopTimeEvent;
use gtfs_realtime::trip_update::stop_time_update::StopTimeProperties;

impl From<StopTimeProperties> for AspenisedStopTimeProperties {
    fn from(stop_time_properties: StopTimeProperties) -> Self {
        AspenisedStopTimeProperties {
            assigned_stop_id: stop_time_properties.assigned_stop_id.map(|x| x.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AspenStopTimeEvent {
    pub delay: Option<i32>,
    pub time: Option<i64>,
    pub uncertainty: Option<i32>,
}

impl From<StopTimeEvent> for AspenStopTimeEvent {
    fn from(stop_time_event: StopTimeEvent) -> Self {
        AspenStopTimeEvent {
            delay: stop_time_event.delay,
            time: stop_time_event.time,
            uncertainty: stop_time_event.uncertainty,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedVehiclePosition {
    pub trip: Option<AspenisedVehicleTripInfo>,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub position: Option<CatenaryRtVehiclePosition>,
    pub timestamp: Option<u64>,
    pub route_type: i16,
    pub current_stop_sequence: Option<u32>,
    pub current_status: Option<i32>,
    pub congestion_level: Option<i32>,
    pub occupancy_status: Option<i32>,
    pub occupancy_percentage: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatenaryRtVehiclePosition {
    pub latitude: f32,
    pub longitude: f32,
    pub bearing: Option<f32>,
    pub odometer: Option<f64>,
    pub speed: Option<f32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedVehicleDescriptor {
    pub id: Option<String>,
    pub label: Option<String>,
    pub license_plate: Option<String>,
    pub wheelchair_accessible: Option<i32>,
}

use gtfs_realtime::VehicleDescriptor;

impl From<VehicleDescriptor> for AspenisedVehicleDescriptor {
    fn from(vehicle_descriptor: VehicleDescriptor) -> Self {
        AspenisedVehicleDescriptor {
            id: vehicle_descriptor.id,
            label: vehicle_descriptor.label,
            license_plate: vehicle_descriptor.license_plate,
            wheelchair_accessible: vehicle_descriptor.wheelchair_accessible,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedVehicleTripInfo {
    pub trip_id: Option<String>,
    pub trip_headsign: Option<String>,
    pub route_id: Option<String>,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time: Option<String>,
    pub start_date: Option<chrono::NaiveDate>,
    pub schedule_relationship: Option<AspenisedTripScheduleRelationship>,
    pub delay: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AspenisedVehicleRouteCache {
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    // pub route_short_name_langs: Option<HashMap<String, String>>,
    // pub route_long_name_langs: Option<HashMap<String, String>>,
    pub route_colour: Option<String>,
    pub route_text_colour: Option<String>,
    pub route_type: i16,
    pub route_desc: Option<String>,
    pub agency_id: Option<String>,
}

#[derive(Copy, Eq, Hash, PartialEq, Clone, Deserialize, Serialize, Debug)]
pub enum GtfsRtType {
    VehiclePositions,
    TripUpdates,
    Alerts,
}

use gtfs_realtime::trip_update::TripProperties;

impl From<TripProperties> for AspenTripProperties {
    fn from(trip_properties: TripProperties) -> Self {
        AspenTripProperties {
            trip_id: trip_properties.trip_id,
            start_date: match &trip_properties.start_date {
                Some(date) => {
                    //chrono parse yyyymmdd

                    match chrono::NaiveDate::parse_from_str(date, "%Y%m%d") {
                        Ok(date) => Some(date),
                        Err(_) => None,
                    }
                }
                None => None,
            },
            start_time: trip_properties.start_time,
            shape_id: trip_properties.shape_id,
            trip_headsign: trip_properties.trip_headsign,
            trip_short_name: trip_properties.trip_short_name,
        }
    }
}

pub trait ReplaceVehicleLabelWithVehicleId {
    fn replace_vehicle_label_with_vehicle_id(self) -> Self;
}

impl ReplaceVehicleLabelWithVehicleId for AspenisedVehicleDescriptor {
    fn replace_vehicle_label_with_vehicle_id(self) -> Self {
        let mut input = self;

        input.label = input.id.clone();

        input
    }
}

impl ReplaceVehicleLabelWithVehicleId for AspenisedVehiclePosition {
    fn replace_vehicle_label_with_vehicle_id(self) -> Self {
        let mut input = self;

        if let Some(vehicle) = input.vehicle {
            input.vehicle =
                Some(AspenisedVehicleDescriptor::replace_vehicle_label_with_vehicle_id(vehicle));
        }

        input
    }
}

impl ReplaceVehicleLabelWithVehicleId for AspenisedTripUpdate {
    fn replace_vehicle_label_with_vehicle_id(self) -> Self {
        let mut input = self;

        if let Some(vehicle) = input.vehicle {
            input.vehicle =
                Some(AspenisedVehicleDescriptor::replace_vehicle_label_with_vehicle_id(vehicle));
        }

        input
    }
}
