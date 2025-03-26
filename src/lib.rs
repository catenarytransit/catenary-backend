// Copyright: Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Removal of the attribution is not allowed, as covered under the AGPL license

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref,
    clippy::useless_vec,
    clippy::module_inception
)]

#[macro_use]
extern crate diesel_derive_newtype;
#[macro_use]
extern crate serde;

pub mod agency_secret;
pub mod aspen;
pub mod cholla;
pub mod custom_pg_types;
pub mod enum_to_int;
pub mod gtfs_rt_handlers;
pub mod gtfs_rt_rough_hash;
pub mod id_cleanup;
pub mod ip_to_location;
pub mod maple_syrup;
pub mod models;
pub mod postgis_to_diesel;
pub mod postgres_tools;
pub mod schema;
pub mod validate_gtfs_rt;
use crate::aspen::lib::RealtimeFeedMetadataEtcd;
pub mod custom_alerts;
use ahash::AHasher;
use chrono::Datelike;
use chrono::NaiveDate;
use fasthash::MetroHasher;
use gtfs_realtime::{FeedEntity, FeedMessage};
use gtfs_structures::RouteType;
use schema::gtfs::trip_frequencies::start_time;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
pub mod metrolink_ptc_to_stop_id;
pub mod rt_recent_history;
use crate::rt_recent_history::*;
pub mod schedule_filtering;
pub mod tile_save_and_get;
pub mod timestamp_extraction;
use csv::ReaderBuilder;
use csv::StringRecord;
use csv::WriterBuilder;
use flate2::Compression;
use serde::Deserialize;
use serde::Serialize;
use std::io::Cursor;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::{fs::File, io::BufReader, io::BufWriter};

pub fn fix_stop_times_headsigns(
    input_path: &str,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let input_file = File::open(input_path)?;
    let output_file = File::create(output_path)?;
    let input_reader = BufReader::new(input_file);
    let mut output_writer = WriterBuilder::new().from_writer(BufWriter::new(output_file));

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(input_reader);

    let headers = csv_reader.headers()?.clone();
    let mut output_headers = Vec::new();
    let mut stop_headsign_index: Option<usize> = None;
    for (i, header) in headers.into_iter().enumerate() {
        if header == "stop_headsign" {
            stop_headsign_index = Some(i);
        } else {
            output_headers.push(header);
        }
    }
    output_writer.write_record(&output_headers)?;

    let mut current_trip_id: Option<String> = None;
    let mut current_trip_rows: Vec<StringRecord> = Vec::new();
    let mut current_trip_headsigns: Vec<String> = Vec::new();

    for result in csv_reader.records() {
        let record = result?;
        if record.len() < 8 {
            continue; // Skip malformed records
        }
        let trip_id = &record[0];
        let stop_headsign = &record[5];

        if current_trip_id.is_none() || current_trip_id.as_ref().unwrap() != trip_id {
            // Process the previous trip
            if let Some(prev_trip_id) = current_trip_id {
                let all_headsigns_same = current_trip_headsigns.windows(2).all(|w| w[0] == w[1]);
                if all_headsigns_same && !current_trip_headsigns.is_empty() {
                    for row in &current_trip_rows {
                        let mut output_record = Vec::new();
                        for (i, field) in row.iter().enumerate() {
                            if Some(i) != stop_headsign_index {
                                output_record.push(field);
                            }
                        }
                        output_writer.write_record(&output_record)?;
                    }
                } else {
                    for row in &current_trip_rows {
                        output_writer.write_record(row)?;
                    }
                }
            }

            // Start a new trip
            current_trip_id = Some(trip_id.to_string());
            current_trip_rows.clear();
            current_trip_headsigns.clear();
        }

        current_trip_rows.push(record.clone()); // Added .clone() here
        current_trip_headsigns.push(stop_headsign.to_string());
    }

    // Process the last trip
    if let Some(prev_trip_id) = current_trip_id {
        let all_headsigns_same = current_trip_headsigns.windows(2).all(|w| w[0] == w[1]);
        if all_headsigns_same && !current_trip_headsigns.is_empty() {
            for row in &current_trip_rows {
                let mut output_record = Vec::new();
                for (i, field) in row.iter().enumerate() {
                    if Some(i) != stop_headsign_index {
                        output_record.push(field);
                    }
                }
                output_writer.write_record(&output_record)?;
            }
        } else {
            for row in &current_trip_rows {
                output_writer.write_record(row)?;
            }
        }
    }

    output_writer.flush()?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ChateauDataNoGeometry {
    pub chateau_id: String,
    pub static_feeds: Vec<String>,
    pub realtime_feeds: Vec<String>,
}

pub const WGS_84_SRID: u32 = 4326;

pub fn compress_zlib(input: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input).unwrap();
    encoder.finish().unwrap()
}

pub fn decompress_zlib(input: &[u8]) -> Vec<u8> {
    let mut decoder = flate2::read::ZlibDecoder::new(Cursor::new(input));
    let mut decompressed_bytes = Vec::new();
    decoder.read_to_end(&mut decompressed_bytes).unwrap();

    decompressed_bytes
}

pub mod gtfs_schedule_protobuf {
    use gtfs_structures::ExactTimes;

    include!(concat!(env!("OUT_DIR"), "/gtfs_schedule_protobuf.rs"));

    fn frequency_to_protobuf(frequency: &gtfs_structures::Frequency) -> GtfsFrequencyProto {
        GtfsFrequencyProto {
            start_time: frequency.start_time,
            end_time: frequency.end_time,
            headway_secs: frequency.headway_secs,
            exact_times: match frequency.exact_times {
                Some(ExactTimes::FrequencyBased) => Some(ExactTimesProto::FrequencyBased.into()),
                Some(ExactTimes::ScheduleBased) => Some(ExactTimesProto::ScheduleBased.into()),
                None => None,
            },
        }
    }

    fn protobuf_to_frequency(frequency: &GtfsFrequencyProto) -> gtfs_structures::Frequency {
        gtfs_structures::Frequency {
            start_time: frequency.start_time,
            end_time: frequency.end_time,
            headway_secs: frequency.headway_secs,
            exact_times: match frequency.exact_times {
                Some(0) => Some(ExactTimes::FrequencyBased),
                Some(1) => Some(ExactTimes::ScheduleBased),
                _ => None,
                None => None,
            },
        }
    }

    pub fn frequencies_to_protobuf(
        frequencies: &Vec<gtfs_structures::Frequency>,
    ) -> GtfsFrequenciesProto {
        let frequencies: Vec<GtfsFrequencyProto> =
            frequencies.iter().map(frequency_to_protobuf).collect();

        GtfsFrequenciesProto { frequencies }
    }

    pub fn protobuf_to_frequencies(
        frequencies: &GtfsFrequenciesProto,
    ) -> Vec<gtfs_structures::Frequency> {
        frequencies
            .frequencies
            .iter()
            .map(protobuf_to_frequency)
            .collect()
    }
}

pub fn fast_hash<T: Hash>(t: &T) -> u64 {
    let mut s: MetroHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

pub fn ahash_fast_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = AHasher::default();
    t.hash(&mut hasher);
    hasher.finish()
}

/*
pub fn gx_fast_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = gxhasher::GxBuildHasher::default();
    t.hash(&mut hasher);
    hasher.finish()
}*/

pub fn duration_since_unix_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub mod aspen_dataset {
    use crate::RtCacheEntry;
    use crate::RtKey;
    use ahash::AHashMap;
    use compact_str::CompactString;
    use gtfs_realtime::FeedEntity;
    use std::hash::Hash;

    #[derive(Clone, Serialize, Deserialize)]
    pub struct ItineraryPatternInternalCache {
        pub itinerary_pattern_meta: AHashMap<String, crate::models::ItineraryPatternMeta>,
        pub itinerary_pattern_rows: AHashMap<String, Vec<crate::models::ItineraryPatternRow>>,
        pub last_time_full_refreshed: chrono::DateTime<chrono::Utc>,
    }

    impl ItineraryPatternInternalCache {
        pub fn new() -> Self {
            ItineraryPatternInternalCache {
                itinerary_pattern_meta: AHashMap::new(),
                itinerary_pattern_rows: AHashMap::new(),
                last_time_full_refreshed: chrono::Utc::now(),
            }
        }
    }

    #[derive(Clone, Serialize, Deserialize)]
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

    #[derive(Clone, Serialize, Deserialize)]
    pub struct AspenisedData {
        pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
        pub vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache>,
        pub vehicle_label_to_gtfs_id: AHashMap<String, String>,
        //id to trip update
        pub trip_updates: AHashMap<CompactString, AspenisedTripUpdate>,
        pub trip_updates_lookup_by_trip_id_to_trip_update_ids:
            AHashMap<CompactString, Vec<CompactString>>,
        //        pub raw_alerts: AHashMap<String, gtfs_realtime::Alert>,
        pub aspenised_alerts: AHashMap<String, AspenisedAlert>,
        pub impacted_routes_alerts: AHashMap<String, Vec<String>>,
        pub impacted_stops_alerts: AHashMap<String, Vec<String>>,
        pub impacted_trips_alerts: AHashMap<String, Vec<String>>,
        pub last_updated_time_ms: u64,
        pub itinerary_pattern_internal_cache: ItineraryPatternInternalCache,
        pub compressed_trip_internal_cache: CompressedTripInternalCache,
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
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenTripProperties {
        pub trip_id: Option<String>,
        pub start_date: Option<chrono::NaiveDate>,
        pub start_time: Option<String>,
        pub shape_id: Option<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
    pub struct AspenRawTripInfo {
        pub trip_id: Option<String>,
        pub route_id: Option<String>,
        pub direction_id: Option<u32>,
        pub start_time: Option<String>,
        pub start_date: Option<chrono::NaiveDate>,
        pub schedule_relationship: Option<i32>,
        pub modified_trip: Option<ModifiedTripSelector>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
    pub struct ModifiedTripSelector {
        pub modifications_id: Option<String>,
        pub affected_trip_id: Option<String>,
    }

    impl From<gtfs_realtime::trip_descriptor::ModifiedTripSelector> for ModifiedTripSelector {
        fn from(
            modified_trip_selector: gtfs_realtime::trip_descriptor::ModifiedTripSelector,
        ) -> Self {
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
                schedule_relationship: trip_descriptor.schedule_relationship,
                modified_trip: trip_descriptor.modified_trip.map(|x| x.into()),
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenisedStopTimeUpdate {
        pub stop_sequence: Option<u16>,
        pub stop_id: Option<compact_str::CompactString>,
        pub arrival: Option<AspenStopTimeEvent>,
        pub departure: Option<AspenStopTimeEvent>,
        pub departure_occupancy_status: Option<AspenisedOccupancyStatus>,
        pub schedule_relationship: Option<AspenisedScheduleRelationship>,
        pub stop_time_properties: Option<AspenisedStopTimeProperties>,
        pub platform_string: Option<String>,
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
        schedule_relationship: &AspenisedScheduleRelationship,
    ) -> u8 {
        match schedule_relationship {
            AspenisedScheduleRelationship::Scheduled => 0,
            AspenisedScheduleRelationship::Added => 1,
            AspenisedScheduleRelationship::Unscheduled => 2,
            AspenisedScheduleRelationship::Canceled => 3,
            AspenisedScheduleRelationship::Replacement => 5,
            AspenisedScheduleRelationship::Duplicated => 6,
            AspenisedScheduleRelationship::Deleted => 7,
        }
    }

    pub fn option_i32_to_schedule_relationship(
        schedule_relationship: &Option<i32>,
    ) -> Option<AspenisedScheduleRelationship> {
        match schedule_relationship {
            Some(status) => match status {
                0 => Some(AspenisedScheduleRelationship::Scheduled),
                1 => Some(AspenisedScheduleRelationship::Added),
                2 => Some(AspenisedScheduleRelationship::Unscheduled),
                3 => Some(AspenisedScheduleRelationship::Canceled),
                5 => Some(AspenisedScheduleRelationship::Replacement),
                6 => Some(AspenisedScheduleRelationship::Duplicated),
                7 => Some(AspenisedScheduleRelationship::Deleted),
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

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum AspenisedScheduleRelationship {
        Scheduled = 0,
        Added = 1,
        Unscheduled = 2,
        Canceled = 3,
        Replacement = 5,
        Duplicated = 6,
        Deleted = 7,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenisedStopTimeProperties {
        pub assigned_stop_id: Option<String>,
    }

    use gtfs_realtime::trip_update::StopTimeEvent;
    use gtfs_realtime::trip_update::stop_time_update::StopTimeProperties;

    impl From<StopTimeProperties> for AspenisedStopTimeProperties {
        fn from(stop_time_properties: StopTimeProperties) -> Self {
            AspenisedStopTimeProperties {
                assigned_stop_id: stop_time_properties.assigned_stop_id,
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
        pub schedule_relationship: Option<i32>,
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
                input.vehicle = Some(
                    AspenisedVehicleDescriptor::replace_vehicle_label_with_vehicle_id(vehicle),
                );
            }

            input
        }
    }

    impl ReplaceVehicleLabelWithVehicleId for AspenisedTripUpdate {
        fn replace_vehicle_label_with_vehicle_id(self) -> Self {
            let mut input = self;

            if let Some(vehicle) = input.vehicle {
                input.vehicle = Some(
                    AspenisedVehicleDescriptor::replace_vehicle_label_with_vehicle_id(vehicle),
                );
            }

            input
        }
    }
}

pub fn parse_gtfs_rt_message(
    bytes: &[u8],
) -> Result<gtfs_realtime::FeedMessage, Box<dyn std::error::Error>> {
    let x = prost::Message::decode(bytes);

    match x {
        Ok(x) => Ok(x),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn route_id_transform(feed_id: &str, route_id: String) -> String {
    match feed_id {
        "f-dp3-pace~rt" => {
            if !route_id.contains("-367") {
                format!("{}-367", route_id)
            } else {
                route_id.to_owned()
            }
        }
        "f-foothilltransit~rt" => {
            //if the route id is 5 digits, use the last 3
            if route_id.len() == 5 {
                route_id.chars().skip(2).collect()
            } else {
                route_id
            }
        }
        _ => route_id,
    }
}

pub async fn get_node_for_realtime_feed_id_kvclient(
    etcd: &mut etcd_client::KvClient,
    realtime_feed_id: &str,
) -> Option<RealtimeFeedMetadataEtcd> {
    let node = etcd
        .get(
            format!("/aspen_assigned_realtime_feed_ids/{}", realtime_feed_id).as_str(),
            None,
        )
        .await;

    match node {
        Ok(resp) => {
            let kvs = resp.kvs();

            match kvs.len() {
                0 => None,
                _ => {
                    let data = bincode::serde::decode_from_slice::<
                        RealtimeFeedMetadataEtcd,
                        bincode::config::Configuration<
                            bincode::config::LittleEndian,
                            bincode::config::Fixint,
                            bincode::config::NoLimit,
                        >,
                    >(kvs[0].value(), bincode::config::legacy());

                    match data {
                        Ok(data) => Some(data.0),
                        Err(e) => {
                            println!("Error deserializing RealtimeFeedMetadataEtcd: {:?}", e);
                            None
                        }
                    }
                }
            }
        }
        _ => None,
    }
}

pub async fn get_node_for_realtime_feed_id(
    etcd: &mut etcd_client::Client,
    realtime_feed_id: &str,
) -> Option<RealtimeFeedMetadataEtcd> {
    let node = etcd
        .get(
            format!("/aspen_assigned_realtime_feed_ids/{}", realtime_feed_id).as_str(),
            None,
        )
        .await;

    match node {
        Ok(resp) => {
            let kvs = resp.kvs();

            match kvs.len() {
                0 => None,
                _ => {
                    let data = bincode::serde::decode_from_slice::<
                        RealtimeFeedMetadataEtcd,
                        bincode::config::Configuration<
                            bincode::config::LittleEndian,
                            bincode::config::Fixint,
                            bincode::config::NoLimit,
                        >,
                    >(kvs[0].value(), bincode::config::legacy());

                    match data {
                        Ok(data) => Some(data.0),
                        Err(e) => {
                            println!("Error deserializing RealtimeFeedMetadataEtcd: {:?}", e);
                            None
                        }
                    }
                }
            }
        }
        _ => None,
    }
}

pub fn make_feed_from_entity_vec(entities: Vec<FeedEntity>) -> FeedMessage {
    FeedMessage {
        header: gtfs_realtime::FeedHeader {
            gtfs_realtime_version: "2.0".to_string(),
            incrementality: Some(gtfs_realtime::feed_header::Incrementality::FullDataset as i32),
            timestamp: Some(duration_since_unix_epoch().as_secs() as u64),
        },
        entity: entities,
    }
}

pub mod unzip_uk {
    use std::io::Read;
    pub async fn get_raw_gtfs_rt(
        client: &reqwest::Client,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let url = "https://data.bus-data.dft.gov.uk/avl/download/gtfsrt";
        let response = client.get(url).send().await?;
        let bytes = response.bytes().await?;

        //unzip and return file gtfsrt.bin
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
        let mut file = zip.by_name("gtfsrt.bin")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod unzip_uk_test {
    use super::*;
    use reqwest::Client;
    #[tokio::test]
    async fn test_get_raw_gtfs_rt() {
        let client = Client::new();
        let x = unzip_uk::get_raw_gtfs_rt(&client).await.unwrap();
        assert!(!x.is_empty());

        //attempt to decode into gtfs-rt

        let x = parse_gtfs_rt_message(&x);

        assert!(x.is_ok());

        let x = x.unwrap();

        // println!("{:#?}", x);
    }
}

pub struct EtcdConnectionIps {
    pub ip_addresses: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SerializableStop {
    pub id: String,
    pub code: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub location_type: i16,
    pub parent_station: Option<String>,
    pub zone_id: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub timezone: Option<String>,
}

pub fn is_null_island(x: f64, y: f64) -> bool {
    x.abs() < 0.1 && y.abs() < 0.1
}

pub fn contains_rail_or_metro_lines(gtfs: &gtfs_structures::Gtfs) -> bool {
    let mut answer = false;

    for (_, route) in gtfs.routes.iter() {
        let is_rail_line = matches!(
            route.route_type,
            RouteType::Tramway
                | RouteType::Subway
                | RouteType::Rail
                | RouteType::CableCar
                | RouteType::Funicular
        );

        if is_rail_line {
            answer = true;
            break;
        }
    }

    answer
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeneralCalendar {
    pub days: Vec<chrono::Weekday>,
    pub start_date: chrono::NaiveDate,
    pub end_date: chrono::NaiveDate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalendarUnified {
    pub id: String,
    pub general_calendar: Option<GeneralCalendar>,
    pub exceptions:
        Option<std::collections::BTreeMap<chrono::NaiveDate, gtfs_structures::Exception>>,
}

// Kyler Chin
// Iterator Optimisation by https://github.com/Priyansh4444 Priyash Sash
pub fn make_weekdays(calendar: &crate::models::Calendar) -> Vec<chrono::Weekday> {
    use chrono::Weekday::*;

    let day_list = [
        (calendar.monday, Mon),
        (calendar.tuesday, Tue),
        (calendar.wednesday, Wed),
        (calendar.thursday, Thu),
        (calendar.friday, Fri),
        (calendar.saturday, Sat),
        (calendar.sunday, Sun),
    ];

    day_list
        .into_iter()
        .filter(|(a, _)| *a)
        .map(|(_, b)| b)
        .collect()
}

pub fn make_calendar_structure_from_pg_single_chateau(
    services_calendar_lookup_queries_to_perform: Vec<crate::models::Calendar>,
    services_calendar_dates_lookup_queries_to_perform: Vec<crate::models::CalendarDate>,
) -> BTreeMap<String, CalendarUnified> {
    let mut calendar_structures: BTreeMap<String, CalendarUnified> = BTreeMap::new();

    for calendar in services_calendar_lookup_queries_to_perform {
        calendar_structures.insert(
            calendar.service_id.clone(),
            CalendarUnified {
                id: calendar.service_id.clone(),
                general_calendar: Some(GeneralCalendar {
                    days: make_weekdays(&calendar),
                    start_date: calendar.gtfs_start_date,
                    end_date: calendar.gtfs_end_date,
                }),
                exceptions: None,
            },
        );
    }

    for calendar_date in services_calendar_dates_lookup_queries_to_perform {
        let exception_number = match calendar_date.exception_type {
            1 => gtfs_structures::Exception::Added,
            2 => gtfs_structures::Exception::Deleted,
            _ => panic!("WHAT IS THIS!!!!!!"),
        };

        match calendar_structures.entry(calendar_date.service_id.clone()) {
            btree_map::Entry::Occupied(mut oe) => {
                let mut calendar_unified = oe.get_mut();

                if let Some(entry) = &mut calendar_unified.exceptions {
                    entry.insert(calendar_date.gtfs_date, exception_number);
                } else {
                    calendar_unified.exceptions = Some(BTreeMap::from_iter([(
                        calendar_date.gtfs_date,
                        exception_number,
                    )]));
                }
            }
            btree_map::Entry::Vacant(mut ve) => {
                ve.insert(CalendarUnified::empty_exception_from_calendar_date(
                    &calendar_date,
                ));
            }
        }
    }

    calendar_structures
}

impl CalendarUnified {
    pub fn empty_exception_from_calendar_date(x: &crate::models::CalendarDate) -> Self {
        CalendarUnified {
            id: x.service_id.clone(),
            general_calendar: None,
            exceptions: Some(std::collections::BTreeMap::from_iter([(
                x.gtfs_date,
                match x.exception_type {
                    1 => gtfs_structures::Exception::Added,
                    2 => gtfs_structures::Exception::Deleted,
                    _ => panic!("WHAT IS THIS!!!!!!"),
                },
            )])),
        }
    }
}

pub struct TripToFindScheduleFor {
    pub trip_id: String,
    pub chateau: String,
    pub timezone: chrono_tz::Tz,
    pub time_since_start_of_service_date: chrono::Duration,
    pub frequency: Option<Vec<gtfs_structures::Frequency>>,
    pub itinerary_id: String,
    pub direction_id: String,
}

pub fn find_service_ranges(
    service: &CalendarUnified,
    trip_instance: &TripToFindScheduleFor,
    input_time: chrono::DateTime<chrono::Utc>,
    back_duration: chrono::Duration,
    forward_duration: chrono::Duration,
) -> Vec<(chrono::NaiveDate, chrono::DateTime<chrono_tz::Tz>)> {
    let start_chrono = input_time - back_duration;

    let additional_lookback = match &trip_instance.frequency {
        Some(freq) => {
            freq.iter()
                .max_by(|a, b| a.end_time.cmp(&b.end_time))
                .unwrap()
                .end_time
        }
        None => 0,
    };

    let start_service_datetime_falls_here = start_chrono
        - trip_instance.time_since_start_of_service_date
        - chrono::TimeDelta::new(additional_lookback.into(), 0).unwrap();

    let end_chrono = input_time + forward_duration - trip_instance.time_since_start_of_service_date;

    let look_at_this_service_start =
        start_service_datetime_falls_here.with_timezone(&trip_instance.timezone);

    let look_at_this_service_end = end_chrono.with_timezone(&trip_instance.timezone);

    let start_service_date_check = look_at_this_service_start.date_naive();
    let end_date_service_check = look_at_this_service_end.date_naive();

    let mut i = start_service_date_check;
    let mut valid_service_days_to_look_at: Vec<NaiveDate> = vec![];

    while i <= end_date_service_check {
        if datetime_in_service(service, i) {
            valid_service_days_to_look_at.push(i);
        }

        i = i.succ_opt().unwrap();
    }

    //println!("checked {:?} from {:?} to {:?}, found {} valid", service, start_service_date_check, end_date_service_check, valid_service_days_to_look_at.len());

    let results = valid_service_days_to_look_at
        .iter()
        .map(|nd| {
            let noon = nd
                .and_hms_opt(12, 0, 0)
                .unwrap()
                .and_local_timezone(trip_instance.timezone)
                .unwrap();

            let starting_time = noon - chrono::TimeDelta::new(43200, 0).unwrap();

            (*nd, starting_time)
        })
        .collect::<Vec<(chrono::NaiveDate, chrono::DateTime<chrono_tz::Tz>)>>();

    results
}

pub fn datetime_in_service(service: &CalendarUnified, input_date: chrono::NaiveDate) -> bool {
    let mut answer = false;

    if let Some(calendar_general) = &service.general_calendar {
        let weekday = input_date.weekday();

        if calendar_general.days.contains(&weekday)
            && calendar_general.start_date <= input_date
            && calendar_general.end_date >= input_date
        {
            answer = true;
        }
    }

    if let Some(exceptions) = &service.exceptions {
        if let Some(exception) = exceptions.get(&input_date) {
            match exception {
                gtfs_structures::Exception::Added => {
                    answer = true;
                }
                gtfs_structures::Exception::Deleted => {
                    answer = false;
                }
            }
        }
    }

    answer
}

#[cfg(test)]
mod test_calendar {
    use super::*;

    #[test]
    fn test_date() {
        let calendar = CalendarUnified {
            id: "a".to_string(),
            general_calendar: Some(GeneralCalendar {
                days: vec![chrono::Weekday::Mon],
                start_date: NaiveDate::from_ymd(2024, 8, 1),
                end_date: NaiveDate::from_ymd(2024, 8, 31),
            }),
            exceptions: None,
        };

        let date = NaiveDate::from_ymd(2024, 8, 26);

        assert!(datetime_in_service(&calendar, date));

        let trip_instance = TripToFindScheduleFor {
            trip_id: "11499201".to_string(),
            chateau: "orangecountytransportationauthority".to_string(),
            timezone: chrono_tz::Tz::UTC,
            time_since_start_of_service_date: chrono::Duration::zero(),
            frequency: None,
            itinerary_id: "9936372064990961207".to_string(),
            direction_id: "0".to_string(),
        };
    }
}

// Metrolink date fix
pub fn metrolink_unix_fix(date: &str) -> u64 {
    ///Date(1729199040000)/
    //extract all the numbers
    let mut numbers = date.chars().filter(|x| x.is_numeric()).collect::<String>();

    //remove the last 3 digits

    numbers.pop();
    numbers.pop();
    numbers.pop();

    //convert to number

    numbers.parse::<u64>().unwrap()
}

pub fn bincode_serialize<T>(value: &T) -> Result<Vec<u8>, bincode::error::EncodeError>
where
    T: serde::Serialize,
{
    use bincode::config;
    use bincode::config::*;

    let config: Configuration<LittleEndian, Fixint> = config::legacy();

    bincode::serde::encode_to_vec(value, config)
}

pub fn bincode_deserialize<T>(value: &[u8]) -> Result<T, bincode::error::DecodeError>
where
    T: serde::de::DeserializeOwned,
{
    use bincode::config;
    use bincode::config::*;

    let config: Configuration<LittleEndian, Fixint> = config::legacy();

    match bincode::serde::decode_from_slice(value, config) {
        Ok(x) => Ok(x.0),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_size_of_stop_update() {
        let x = std::mem::size_of::<gtfs_realtime::trip_update::StopTimeUpdate>();
        println!("Size of StopTimeUpdate: {}", x);

        let x = std::mem::size_of::<crate::aspen_dataset::AspenisedStopTimeUpdate>();

        println!("Size of AspenisedStopTimeUpdate: {}", x);
    }
}
