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
    clippy::iter_cloned_collect
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
pub mod maple_syrup;
pub mod models;
pub mod postgis_to_diesel;
pub mod postgres_tools;
pub mod schema;

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
        let netmask: IpNetwork = "100.64.0.0/10".parse().unwrap();
        ifaces
            .iter()
            .filter(|iface| maybe_tailscale(&iface.name))
            .flat_map(|iface| iface.ips.clone())
            .filter(|ipnet| ipnet.is_ipv6() && netmask.contains(ipnet.network()))
            .map(|ipnet| ipnet.ip())
            .next()
    }
}

pub mod AspenDataset {
    use gtfs_rt::TripUpdate;
    use gtfs_rt::VehicleDescriptor;
    use std::{collections::HashMap, hash::Hash};

    pub struct AspenisedData {
        pub vehicle_positions: Vec<AspenisedVehiclePosition>,
        pub vehicle_routes_cache: HashMap<String, AspenisedVehicleRouteCache>,
        //id to trip update
        pub trip_updates: HashMap<String, TripUpdate>,
        pub trip_updates_lookup_by_trip_id_to_trip_update_ids: HashMap<String, Vec<String>>,
        pub raw_alerts: Option<HashMap<String, gtfs_rt::Alert>>,
        pub impacted_routes_alerts: Option<HashMap<String, Vec<String>>>,
        pub impacted_stops_alerts: Option<HashMap<String, Vec<String>>>,
        pub impacted_routes_stops_alerts: Option<HashMap<String, Vec<String>>>,
    }

    pub struct AspenisedVehiclePosition {
        pub trip: Option<AspenisedVehicleTripInfo>,
        pub vehicle: Option<VehicleDescriptor>,
    }

    pub struct AspenisedVehicleTripInfo {
        pub trip_id: Option<String>,
        pub trip_headsign: Option<String>,
        pub route_id: Option<String>,
        pub trip_short_name: Option<String>,
    }

    pub struct AspenisedVehicleRouteCache {
        route_short_name: Option<String>,
        route_long_name: Option<String>,
        //route_short_name_langs: Option<HashMap<String, String>>,
        //route_long_name_langs: Option<HashMap<String, String>>,
        route_colour: Option<String>,
        route_text_colour: Option<String>,
    }
}
