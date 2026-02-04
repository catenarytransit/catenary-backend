// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use crate::ChateauDataNoGeometry;

#[path = "connection_manager.rs"]
pub mod connection_manager;
use connection_manager::AspenClientManager;

/// This is the service definition. It looks a lot like a trait definition.
/// It defines one RPC, hello, which takes one arg, name, and returns a String.
use crate::aspen_dataset::*;
use ahash::AHashMap;
use ahash::AHashSet;
use ecow::EcoString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use tarpc::{client, tokio_serde::formats::Bincode};

#[derive(serde::Deserialize, Clone, serde::Serialize, Debug)]
pub struct TripsSelectionResponse {
    pub trip_updates: AHashMap<String, AspenisedTripUpdate>,
    pub trip_id_to_trip_update_ids: AHashMap<String, Vec<String>>,
    pub stop_id_to_parent_id: AHashMap<String, String>,
}

#[tarpc::service]
pub trait AspenRpc {
    /// Returns a greeting for name.
    async fn hello(name: String) -> String;

    async fn get_all_trips_with_ids(
        chateau_id: String,
        trip_id: Vec<String>,
    ) -> Option<TripsSelectionResponse>;

    async fn get_all_trips_with_route_ids(
        chateau_id: String,
        route_id: Vec<String>,
    ) -> Option<TripsSelectionResponse>;

    async fn get_shape(chateau_id: String, shape_id: String) -> Option<EcoString>;

    async fn get_trip_modification(
        chateau_id: String,
        modification_id: String,
    ) -> Option<AspenisedTripModification>;

    async fn get_trip_modifications(
        chateau_id: String,
        modification_ids: Vec<String>,
    ) -> Option<AHashMap<String, AspenisedTripModification>>;

    async fn get_realtime_stops(
        chateau_id: String,
        stop_ids: Vec<String>,
    ) -> Option<AHashMap<String, AspenisedStop>>;

    async fn trip_mod_lookup_for_trip_id_service_day(
        chateau_id: String,
        trip_id: String,
        service_day: chrono::NaiveDate,
    ) -> Option<AspenisedTripModification>;

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

    async fn from_alpenrose_compressed(
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
        alerts_dupe_trips: bool,
    ) -> bool;

    async fn get_single_vehicle_location_from_gtfsid(
        chateau_id: String,
        gtfs_id: String,
    ) -> Option<AspenisedVehiclePosition>;

    async fn get_single_vehicle_location_from_vehicle_label(
        chateau_id: String,
        vehicle_label: String,
    ) -> Option<AspenisedVehiclePosition>;

    async fn get_vehicle_locations(
        chateau_id: String,
        existing_fasthash_of_routes: Option<u64>,
        route_types_filter: Option<Vec<i16>>,
    ) -> Option<GetVehicleLocationsResponse>;

    async fn get_vehicle_locations_with_route_filtering(
        chateau_id: String,
        existing_fasthash_of_routes: Option<u64>,
        route_ids: Option<Vec<String>>,
    ) -> Option<GetVehicleLocationsResponse>;

    async fn get_gtfs_rt(
        realtime_feed_id: String,
        feed_type: crate::aspen_dataset::GtfsRtType,
    ) -> Option<Vec<u8>>;

    async fn get_gtfs_rt_compressed(
        realtime_feed_id: String,
        feed_type: crate::aspen_dataset::GtfsRtType,
    ) -> Option<Vec<u8>>;

    async fn get_trip_updates_from_trip_id(
        chateau_id: String,
        trip_id: String,
    ) -> Option<Vec<AspenisedTripUpdate>>;

    async fn get_trip_updates_from_route_ids(
        chateau_id: String,
        route_id: Vec<String>,
    ) -> Option<Vec<AspenisedTripUpdate>>;

    async fn get_alerts_from_route_id(
        chateau_id: String,
        route_id: String,
    ) -> Option<Vec<(String, AspenisedAlert)>>;

    async fn get_alerts_from_stop_id(
        chateau_id: String,
        stop_id: String,
    ) -> Option<Vec<(String, AspenisedAlert)>>;

    async fn get_alert_from_trip_id(
        chateau_id: String,
        trip_id: String,
    ) -> Option<Vec<(String, AspenisedAlert)>>;

    async fn get_alert_from_stop_ids(
        chateau_id: String,
        stop_ids: Vec<String>,
    ) -> Option<AlertsforManyStops>;

    async fn get_nonscheduled_trips_updates_from_stop_ids(
        chateau_id: String,
        stop_ids: Vec<String>,
    ) -> Option<Vec<AspenisedTripUpdate>>;

    async fn get_all_alerts(chateau_id: String) -> Option<HashMap<String, AspenisedAlert>>;

    async fn full_aspen_dataset(chateau_id: String) -> Option<AspenisedData>;

    async fn insert_backup_aspen_dataset(chateau_id: String, data: AspenisedData) -> ();

    async fn full_aspen_dataset_backup(chateau_id: String) -> Option<AspenisedData>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlertsforManyStops {
    pub alerts: AHashMap<String, AspenisedAlert>,
    pub stops_to_alert_ids: AHashMap<String, AHashSet<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetVehicleLocationsResponse {
    pub vehicle_route_cache: Option<AHashMap<String, AspenisedVehicleRouteCache>>,
    pub vehicle_positions: AHashMap<String, AspenisedVehiclePosition>,
    pub hash_of_routes: u64,
    pub last_updated_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChateauMetadataEtcd {
    pub worker_id: String,
    pub socket: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RealtimeFeedMetadataEtcd {
    pub worker_id: String,
    pub socket: SocketAddr,
    pub chateau_id: String,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct ChateauxLeaderHashMap {
    pub chateaus: BTreeMap<String, ChateauDataNoGeometry>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct ProcessAlpenroseData {
    pub chateau_id: String,
    pub realtime_feed_id: String,
    pub has_vehicles: bool,
    pub has_trips: bool,
    pub has_alerts: bool,
    pub vehicles_response_code: Option<u16>,
    pub trips_response_code: Option<u16>,
    pub alerts_response_code: Option<u16>,
    pub time_of_submission_ms: u64,
}

pub async fn spawn_aspen_client_from_ip(
    addr: &SocketAddr,
) -> Result<AspenRpcClient, Box<dyn std::error::Error + Sync + Send>> {
    let transport = tarpc::serde_transport::tcp::connect(addr, Bincode::default).await?;

    Ok(AspenRpcClient::new(client::Config::default(), transport).spawn())
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AspenWorkerMetadataEtcd {
    pub etcd_lease_id: i64,
    pub socket: SocketAddr,
    pub worker_id: String,
}
