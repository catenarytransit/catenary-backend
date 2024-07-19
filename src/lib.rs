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
    clippy::op_ref
)]

#[macro_use]
extern crate diesel_derive_newtype;
#[macro_use]
extern crate serde;

pub mod agency_secret;
pub mod aspen;
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
use ahash::AHasher;
use fasthash::MetroHasher;
use gtfs_rt::translated_image::LocalizedImage;
use gtfs_rt::VehicleDescriptor;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Read;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
pub mod metrolink_ptc_to_stop_id;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ChateauDataNoGeometry {
    pub chateau_id: String,
    pub static_feeds: Vec<String>,
    pub realtime_feeds: Vec<String>,
}

pub const WGS_84_SRID: u32 = 4326;

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

        GtfsFrequenciesProto {
            frequencies: frequencies,
        }
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

pub fn duration_since_unix_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub mod tailscale {
    //stolen from tailscale-rs
    //significantly adapted by Kyler Chin to use ipv6 addressing
    extern crate ipnetwork;
    extern crate pnet;

    use ipnetwork::IpNetwork;
    use pnet::datalink;
    use std::net::IpAddr;

    fn maybe_tailscale(s: &str) -> bool {
        s.starts_with("tailscale")
    }

    /// Retrieve the IP address of the current machine's Tailscale interface, if any.
    /// ```
    /// let iface = catenary::tailscale::interface().expect( "no tailscale interface found");
    /// ```
    pub fn interface() -> Option<IpAddr> {
        let ifaces = datalink::interfaces();
        //let netmask: IpNetwork = "100.64.0.0/10".parse().unwrap();
        ifaces
            .iter()
            .filter(|iface| maybe_tailscale(&iface.name))
            .flat_map(|iface| iface.ips.clone())
            .filter(|ipnet| ipnet.is_ipv6())
            .map(|ipnet| ipnet.ip())
            .next()
    }
}

pub mod aspen_dataset {
    use ahash::AHashMap;
    use gtfs_rt::TripUpdate;
    use std::{collections::BTreeMap, collections::HashMap, hash::Hash};

    #[derive(Clone, Serialize, Deserialize)]
    pub struct AspenisedData {
        pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
        pub vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache>,
        //id to trip update
        pub trip_updates: AHashMap<String, AspenisedTripUpdate>,
        pub trip_updates_lookup_by_trip_id_to_trip_update_ids: AHashMap<String, Vec<String>>,
        //        pub raw_alerts: AHashMap<String, gtfs_rt::Alert>,
        pub aspenised_alerts: AHashMap<String, AspenisedAlert>,
        pub impacted_routes_alerts: AHashMap<String, Vec<String>>,
        pub impacted_stops_alerts: AHashMap<String, Vec<String>>,
        pub impacted_trips_alerts: AHashMap<String, Vec<String>>,
        pub last_updated_time_ms: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenTimeRange {
        pub start: Option<u64>,
        pub end: Option<u64>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenEntitySelector {
        pub agency_id: Option<String>,
        pub route_id: Option<String>,
        pub route_type: Option<i32>,
        pub trip: Option<AspenRawTripInfo>,
        pub stop_id: Option<String>,
        pub direction_id: Option<u32>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenTranslatedString {
        pub translation: Vec<AspenTranslation>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenTranslation {
        pub text: String,
        pub language: Option<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenTranslatedImage {
        pub localised_image: Vec<AspenLocalisedImage>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenLocalisedImage {
        pub url: String,
        pub media_type: String,
        pub language: Option<String>,
    }

    impl From<gtfs_rt::TranslatedString> for AspenTranslatedString {
        fn from(translated_string: gtfs_rt::TranslatedString) -> Self {
            AspenTranslatedString {
                translation: translated_string
                    .translation
                    .into_iter()
                    .map(|x| x.into())
                    .collect(),
            }
        }
    }

    impl From<gtfs_rt::translated_image::LocalizedImage> for AspenLocalisedImage {
        fn from(localised_image: gtfs_rt::translated_image::LocalizedImage) -> Self {
            AspenLocalisedImage {
                url: localised_image.url,
                media_type: localised_image.media_type,
                language: localised_image.language,
            }
        }
    }

    impl From<gtfs_rt::translated_string::Translation> for AspenTranslation {
        fn from(translation: gtfs_rt::translated_string::Translation) -> Self {
            AspenTranslation {
                text: translation.text,
                language: translation.language,
            }
        }
    }

    impl From<gtfs_rt::TranslatedImage> for AspenTranslatedImage {
        fn from(translated_image: gtfs_rt::TranslatedImage) -> Self {
            AspenTranslatedImage {
                localised_image: translated_image
                    .localized_image
                    .into_iter()
                    .map(|x| x.into())
                    .collect(),
            }
        }
    }

    impl From<gtfs_rt::TimeRange> for AspenTimeRange {
        fn from(time_range: gtfs_rt::TimeRange) -> Self {
            AspenTimeRange {
                start: time_range.start,
                end: time_range.end,
            }
        }
    }

    impl From<gtfs_rt::EntitySelector> for AspenEntitySelector {
        fn from(entity_selector: gtfs_rt::EntitySelector) -> Self {
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

    impl From<gtfs_rt::Alert> for AspenisedAlert {
        fn from(alert: gtfs_rt::Alert) -> Self {
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

    #[derive(Clone, Debug, Serialize, Deserialize)]
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
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenTripProperties {
        pub trip_id: Option<String>,
        pub start_date: Option<String>,
        pub start_time: Option<String>,
        pub shape_id: Option<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenRawTripInfo {
        pub trip_id: Option<String>,
        pub route_id: Option<String>,
        pub direction_id: Option<u32>,
        pub start_time: Option<String>,
        pub start_date: Option<String>,
        pub schedule_relationship: Option<i32>,
        pub modified_trip: Option<ModifiedTripSelector>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ModifiedTripSelector {
        pub modifications_id: Option<String>,
        pub affected_trip_id: Option<String>,
    }

    impl From<gtfs_rt::trip_descriptor::ModifiedTripSelector> for ModifiedTripSelector {
        fn from(modified_trip_selector: gtfs_rt::trip_descriptor::ModifiedTripSelector) -> Self {
            ModifiedTripSelector {
                modifications_id: modified_trip_selector.modifications_id,
                affected_trip_id: modified_trip_selector.affected_trip_id,
            }
        }
    }

    impl From<gtfs_rt::TripDescriptor> for AspenRawTripInfo {
        fn from(trip_descriptor: gtfs_rt::TripDescriptor) -> Self {
            AspenRawTripInfo {
                trip_id: trip_descriptor.trip_id,
                route_id: trip_descriptor.route_id,
                direction_id: trip_descriptor.direction_id,
                start_time: trip_descriptor.start_time,
                start_date: trip_descriptor.start_date,
                schedule_relationship: trip_descriptor.schedule_relationship,
                modified_trip: trip_descriptor.modified_trip.map(|x| x.into()),
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenisedStopTimeUpdate {
        pub stop_sequence: Option<u32>,
        pub stop_id: Option<String>,
        pub arrival: Option<AspenStopTimeEvent>,
        pub departure: Option<AspenStopTimeEvent>,
        pub departure_occupancy_status: Option<i32>,
        pub schedule_relationship: Option<i32>,
        pub stop_time_properties: Option<AspenisedStopTimeProperties>,
        pub platform_string: Option<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AspenisedStopTimeProperties {
        pub assigned_stop_id: Option<String>,
    }

    use gtfs_rt::trip_update::stop_time_update::StopTimeProperties;
    use gtfs_rt::trip_update::StopTimeEvent;

    impl From<StopTimeProperties> for AspenisedStopTimeProperties {
        fn from(stop_time_properties: StopTimeProperties) -> Self {
            AspenisedStopTimeProperties {
                assigned_stop_id: stop_time_properties.assigned_stop_id,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
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

    use gtfs_rt::VehicleDescriptor;

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
        pub start_date: Option<String>,
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

    #[derive(Copy, Eq, Hash, PartialEq, Clone)]
    pub enum GtfsRtType {
        VehiclePositions,
        TripUpdates,
        Alerts,
    }

    use gtfs_rt::trip_update::TripProperties;

    impl From<TripProperties> for AspenTripProperties {
        fn from(trip_properties: TripProperties) -> Self {
            AspenTripProperties {
                trip_id: trip_properties.trip_id,
                start_date: trip_properties.start_date,
                start_time: trip_properties.start_time,
                shape_id: trip_properties.shape_id,
            }
        }
    }
}

pub fn parse_gtfs_rt_message(
    bytes: &[u8],
) -> Result<gtfs_rt::FeedMessage, Box<dyn std::error::Error>> {
    let x = prost::Message::decode(bytes);

    match x {
        Ok(x) => Ok(x),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn route_id_transform(feed_id: &str, route_id: String) -> String {
    match feed_id {
        "f-mta~nyc~rt~lirr" | "f-dr5-mtanyclirr" => {
            if !route_id.contains("lirr") {
                format!("lirr{}", route_id)
            } else {
                route_id.to_owned() // Return unmodified route_id if it contains "lirr"
            }
        }
        "f-mta~nyc~rt~mnr" | "f-dr7-mtanyc~metro~north" => {
            if !route_id.contains("mnr") {
                format!("mnr{}", route_id)
            } else {
                route_id.to_owned() // Return unmodified route_id if it contains "mnr"
            }
        }
        _ => route_id,
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
                    let data = bincode::deserialize::<RealtimeFeedMetadataEtcd>(&kvs[0].value());

                    match data {
                        Ok(data) => Some(data),
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

pub fn make_feed_from_entity_vec(entities: Vec<gtfs_rt::FeedEntity>) -> gtfs_rt::FeedMessage {
    gtfs_rt::FeedMessage {
        header: gtfs_rt::FeedHeader {
            gtfs_realtime_version: "2.0".to_string(),
            incrementality: Some(gtfs_rt::feed_header::Incrementality::FullDataset as i32),
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
        assert!(x.len() > 0);

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
