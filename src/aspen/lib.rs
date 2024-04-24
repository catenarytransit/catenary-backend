// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

/// This is the service definition. It looks a lot like a trait definition.
/// It defines one RPC, hello, which takes one arg, name, and returns a String.
use crate::aspen_dataset::*;
use crate::ChateauDataNoGeometry;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::IpAddr;
use ahash::AHashMap;

#[tarpc::service]
pub trait AspenRpc {
    /// Returns a greeting for name.
    async fn hello(name: String) -> String;

    //maybesend gtfs rt?
    async fn from_alpenrose(
        chateau_id: String,
        realtime_feed_id: String,
        vehicles: Option<Vec<u8>>,
        trips: Option<Vec<u8>>,
        alerts: Option<Vec<u8>>,
        has_vehicles: bool,
        has_trips: bool,
        has_alerts: bool,
        vehicles_response_code: Option<u16>,
        trips_response_code: Option<u16>,
        alerts_response_code: Option<u16>,
        time_of_submission_ms: u64,
    ) -> bool;

    async fn get_vehicle_locations(
        chateau_id: String,
        existing_fasthash_of_routes: Option<u64>,
    ) -> (Option<GetVehicleLocationsResponse>);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetVehicleLocationsResponse {
    pub vehicle_route_cache: Option<AHashMap<String, AspenisedVehicleRouteCache>>,
    pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
    pub hash_of_routes: u64,
    pub last_updated_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChateauMetadataZookeeper {
    pub worker_id: String,
    pub tailscale_ip: IpAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RealtimeFeedMetadataZookeeper {
    pub worker_id: String,
    pub tailscale_ip: IpAddr,
    pub chateau_id: String,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct ChateausLeaderHashMap {
    pub chateaus: BTreeMap<String, ChateauDataNoGeometry>,
}

#[derive(Clone)]
pub struct ProcessAlpenroseData {
    pub chateau_id: String,
    pub realtime_feed_id: String,
    pub vehicles: Option<Vec<u8>>,
    pub trips: Option<Vec<u8>>,
    pub alerts: Option<Vec<u8>>,
    pub has_vehicles: bool,
    pub has_trips: bool,
    pub has_alerts: bool,
    pub vehicles_response_code: Option<u16>,
    pub trips_response_code: Option<u16>,
    pub alerts_response_code: Option<u16>,
    pub time_of_submission_ms: u64,
}
