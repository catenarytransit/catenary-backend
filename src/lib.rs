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
pub mod maple_syrup;
pub mod models;
pub mod postgis_to_diesel;
pub mod postgres_tools;
pub mod schema;
pub mod validate_gtfs_rt;
use crate::aspen::lib::RealtimeFeedMetadataZookeeper;
use ahash::AHasher;
use fasthash::MetroHasher;
use gtfs_rt::VehicleDescriptor;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// let iface = tailscale::interface().expect( "no tailscale interface found");
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

    pub struct AspenisedData {
        pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
        pub vehicle_routes_cache: AHashMap<String, AspenisedVehicleRouteCache>,
        //id to trip update
        pub trip_updates: AHashMap<String, AspenisedTripUpdate>,
        pub trip_updates_lookup_by_trip_id_to_trip_update_ids: AHashMap<String, Vec<String>>,
        pub raw_alerts: Option<AHashMap<String, gtfs_rt::Alert>>,
        pub impacted_routes_alerts: Option<AHashMap<String, Vec<String>>>,
        pub impacted_stops_alerts: Option<AHashMap<String, Vec<String>>>,
        pub impacted_routes_stops_alerts: Option<AHashMap<String, Vec<String>>>,
        pub last_updated_time_ms: u64,
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
        pub platform: Option<String>,
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

use tokio_zookeeper::Stat;

pub async fn get_node_for_realtime_feed_id(
    zk: &tokio_zookeeper::ZooKeeper,
    realtime_feed_id: &str,
) -> Option<(RealtimeFeedMetadataZookeeper, Stat)> {
    let node = zk
        .get_data(format!("/aspen_assigned_realtime_feed_ids/{}", realtime_feed_id).as_str())
        .await
        .unwrap();

    match node {
        Some((bytes, stat)) => {
            let data = bincode::deserialize::<RealtimeFeedMetadataZookeeper>(&bytes);

            match data {
                Ok(data) => Some((data, stat)),
                Err(e) => {
                    println!("Error deserializing RealtimeFeedMetadataZookeeper: {:?}", e);
                    None
                }
            }
        }
        None => None,
    }
}
