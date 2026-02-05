// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

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

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use ahash::AHashSet;
use catenary::postgres_tools::make_async_pool;
use catenary::{aspen::lib::*, id_cleanup};
use clap::Parser;
use compact_str::CompactString;
use futures::{future, prelude::*};
use rand::prelude::*;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::Path,
    time::Duration,
};
use tarpc::{
    context,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Bincode,
};
use tokio::sync::Mutex;

use uuid::Uuid;
mod delay_calculation;
mod import_alpenrose;
mod route_type_overrides;
mod stop_time_logic;
use ahash::AHashMap;
use catenary::aspen_dataset::GtfsRtType;
use catenary::aspen_dataset::*;
use catenary::postgres_tools::CatenaryPostgresPool;
use crossbeam::deque::Injector;
use gtfs_realtime::FeedMessage;

use scc::HashMap as SccHashMap;
use std::error::Error;
mod async_threads_alpenrose;
mod metrolink_california_additions;
use crate::id_cleanup::gtfs_rt_correct_route_id_string;
use catenary::compact_formats::CompactFeedMessage;
use catenary::parse_gtfs_rt_message;
use rand::Rng;

use std::collections::{HashMap, HashSet};
mod alerts_responder;
mod aspen_assignment;
use catenary::rt_recent_history::RtCacheEntry;
use catenary::rt_recent_history::RtKey;
use ecow::EcoString;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use prost::Message;
use rand::distr::Uniform;
use rand::rng;
use rayon::prelude::*;
use std::io::Read;
use std::io::Write;
use std::time::Instant;

mod alerts_processing;

mod persistence;
mod track_number;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct GtfsRealtimeHashStore {
    vehicles: Option<u64>,
    trips: Option<u64>,
    alerts: Option<u64>,
}

// This is the type that implements the generated AspenServer trait. It is the business logic
// and is used to start the server.
#[derive(Clone)]
pub struct AspenServer {
    pub addr: SocketAddr,
    pub worker_id: Arc<String>, // Worker Id for this instance of Aspen
    pub authoritative_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    // Backed up in redis as well, program can be shut down and restarted without data loss
    pub authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    pub conn_pool: Arc<CatenaryPostgresPool>,
    pub authoritative_trip_updates_by_gtfs_feed_history:
        Arc<SccHashMap<CompactString, AHashMap<RtKey, RtCacheEntry>>>,
    pub alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    pub alpenrose_to_process_queue_chateaux: Arc<Mutex<HashSet<String>>>,
    pub rough_hash_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>>,
    pub hash_of_raw_gtfs_rt_protobuf: Arc<SccHashMap<String, GtfsRealtimeHashStore>>,
    pub backup_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    pub backup_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    pub backup_trip_updates_by_gtfs_feed_history:
        Arc<SccHashMap<CompactString, AHashMap<RtKey, RtCacheEntry>>>,
    pub etcd_addresses: Arc<Vec<String>>,
    pub etcd_connect_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub worker_etcd_lease_id: i64,
    pub timestamps_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>>,
}

impl AspenRpc for AspenServer {
    async fn get_realtime_stops(
        self,
        _context: tarpc::context::Context,
        chateau_id: String,
        stop_ids: Vec<String>,
    ) -> Option<AHashMap<String, AspenisedStop>> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let mut stop_ids_set = AHashSet::new();
                for stop_id in &stop_ids {
                    stop_ids_set.insert(stop_id.clone());
                }

                let mut stops: AHashMap<String, AspenisedStop> = AHashMap::new();

                for stop_id in stop_ids_set {
                    if let Some(stop) = aspenised_data.stop_id_to_stop.get(stop_id.as_str()) {
                        stops.insert(stop_id, stop.clone());
                    } else {
                        println!("Stop not found for stop id {}", stop_id);
                    }
                }

                Some(stops)
            }
            None => None,
        }
    }

    async fn get_shape(
        self,
        _context: tarpc::context::Context,
        chateau_id: String,
        shape_id: String,
    ) -> Option<EcoString> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let shape = aspenised_data.shape_id_to_shape.get(shape_id.as_str());

                match shape {
                    Some(shape) => shape.clone(),
                    None => {
                        println!("Shape not found for shape id {}", shape_id);
                        None
                    }
                }
            }
            None => None,
        }
    }

    async fn trip_mod_lookup_for_trip_id_service_day(
        self,
        _: context::Context,
        chateau_id: String,
        trip_id: String,
        service_day: chrono::NaiveDate,
    ) -> Option<AspenisedTripModification> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let trip_modification_ids = aspenised_data
                    .trip_id_to_trip_modification_ids
                    .get(trip_id.as_str());

                let mut trip_modifications: Vec<&AspenisedTripModification> = vec![];

                if let Some(trip_modification_ids) = trip_modification_ids {
                    for trip_modification_id in trip_modification_ids {
                        if let Some(trip_modification) = aspenised_data
                            .trip_modifications
                            .get(trip_modification_id.as_str())
                        {
                            trip_modifications.push(trip_modification);
                        } else {
                            println!(
                                "Trip modification not found for modification id {}",
                                trip_modification_id
                            );
                        }
                    }
                }

                let trip_modification = trip_modifications.iter().find(|trip_modification| {
                    trip_modification.service_dates.contains(&service_day)
                });

                match trip_modification {
                    Some(trip_modification) => Some((*trip_modification).to_owned()),
                    None => {
                        println!("Trip modification not found for trip id {}", trip_id);
                        None
                    }
                }
            }
            None => None,
        }
    }

    async fn get_trip_modification(
        self,
        _: context::Context,
        chateau_id: String,
        modification_id: String,
    ) -> Option<AspenisedTripModification> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let trip_modification = aspenised_data
                    .trip_modifications
                    .get(modification_id.as_str());

                match trip_modification {
                    Some(trip_modification) => Some(trip_modification.clone()),
                    None => {
                        println!(
                            "Trip modification not found for modification id {}",
                            modification_id
                        );
                        None
                    }
                }
            }
            None => None,
        }
    }

    async fn get_nonscheduled_trips_updates_from_stop_ids(
        self,
        _: context::Context,
        chateau_id: String,
        stop_ids: Vec<String>,
    ) -> Option<Vec<AspenisedTripUpdate>> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let mut trip_update_ids_to_get: AHashSet<EcoString> = AHashSet::new();

                for stop_id in &stop_ids {
                    if let Some(trip_update_ids) = aspenised_data
                        .stop_id_to_non_scheduled_trip_ids
                        .get(stop_id.as_str())
                    {
                        for trip_update_id in trip_update_ids {
                            trip_update_ids_to_get.insert(trip_update_id.clone());
                        }
                    }
                }

                let mut trip_data_to_send: Vec<AspenisedTripUpdate> = vec![];

                for trip_update_id in trip_update_ids_to_get.iter() {
                    if let Some(trip_update) =
                        aspenised_data.trip_updates.get(trip_update_id.as_str())
                    {
                        trip_data_to_send.push(trip_update.clone())
                    }
                }

                Some(trip_data_to_send)
            }
            None => None,
        }
    }

    async fn get_trip_modifications(
        self,
        _: context::Context,
        chateau_id: String,
        modification_ids: Vec<String>,
    ) -> Option<AHashMap<String, AspenisedTripModification>> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let mut trip_modifications: AHashMap<String, AspenisedTripModification> =
                    AHashMap::new();

                for modification_id in &modification_ids {
                    if let Some(trip_modification) = aspenised_data
                        .trip_modifications
                        .get(modification_id.as_str())
                    {
                        trip_modifications
                            .insert(modification_id.clone(), trip_modification.clone());
                    } else {
                        println!(
                            "Trip modification not found for modification id {}",
                            modification_id
                        );
                    }
                }

                Some(trip_modifications)
            }
            None => None,
        }
    }

    async fn hello(self, _: context::Context, name: String) -> String {
        let sleep_time = Duration::from_millis(
            Uniform::new_inclusive(1, 10)
                .expect("NOT VALID RANGE")
                .sample(&mut rng()),
        );
        tokio::time::sleep(sleep_time).await;
        format!("Hello, {name}! You are connected from {}", self.addr)
    }

    async fn get_all_trips_with_ids(
        self,
        _ctx: context::Context,
        chateau_id: String,
        trip_ids: Vec<String>,
    ) -> Option<TripsSelectionResponse> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(authoritative_data) => {
                let authoritative_data = authoritative_data.get();
                let trip_id_list = trip_ids.iter().cloned().collect::<AHashSet<String>>();

                let mut trip_id_to_trip_update_ids: AHashMap<String, Vec<String>> = AHashMap::new();
                let mut trip_updates: AHashMap<String, AspenisedTripUpdate> = AHashMap::new();

                for trip_id in trip_id_list {
                    if let Some(trip_update_id_list) = authoritative_data
                        .trip_updates_lookup_by_trip_id_to_trip_update_ids
                        .get(trip_id.as_str())
                    {
                        trip_id_to_trip_update_ids.insert(
                            trip_id.clone(),
                            trip_update_id_list
                                .iter()
                                .map(|x| x.to_string())
                                .collect::<Vec<String>>(),
                        );

                        for trip_update_id in trip_update_id_list {
                            if let Some(trip_update) =
                                authoritative_data.trip_updates.get(trip_update_id.as_str())
                            {
                                trip_updates
                                    .insert(trip_update_id.to_string(), trip_update.clone());
                            }
                        }
                    }
                }

                let relevant_stop_ids: AHashSet<&str> = trip_updates
                    .values()
                    .flat_map(|tu| {
                        tu.stop_time_update
                            .iter()
                            .filter_map(|stu| stu.stop_id.as_deref())
                    })
                    .collect();

                let stop_id_to_parent_id: AHashMap<String, String> = relevant_stop_ids
                    .iter()
                    .filter_map(|stop_id| {
                        authoritative_data
                            .stop_id_to_parent_id
                            .get(*stop_id)
                            .map(|parent| (stop_id.to_string(), parent.to_string()))
                    })
                    .collect();

                Some(TripsSelectionResponse {
                    trip_updates,
                    trip_id_to_trip_update_ids,
                    stop_id_to_parent_id,
                })
            }
            None => None,
        }
    }

    async fn get_all_trips_with_route_ids(
        self,
        _ctx: context::Context,
        chateau_id: String,
        route_ids: Vec<String>,
    ) -> Option<TripsSelectionResponse> {
        let start_of_function = std::time::Instant::now();

        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(authoritative_data) => {
                let authoritative_data = authoritative_data.get();
                let time_it_took_to_peek = start_of_function.elapsed();
                println!(
                    "Time it took to peek: {}ms",
                    time_it_took_to_peek.as_millis()
                );

                let route_id_list = route_ids.iter().cloned().collect::<AHashSet<String>>();

                let mut trip_id_to_trip_update_ids: AHashMap<String, Vec<String>> = AHashMap::new();
                let mut trip_updates: AHashMap<String, AspenisedTripUpdate> = AHashMap::new();

                for route_id in route_id_list {
                    if let Some(trip_update_id_list) = authoritative_data
                        .trip_updates_lookup_by_route_id_to_trip_update_ids
                        .get(route_id.as_str())
                    {
                        for trip_update_id in trip_update_id_list {
                            if let Some(trip_update) =
                                authoritative_data.trip_updates.get(trip_update_id.as_str())
                            {
                                trip_updates
                                    .insert(trip_update_id.to_string(), trip_update.clone());

                                if let Some(trip_id) = &trip_update.trip.trip_id {
                                    trip_id_to_trip_update_ids
                                        .entry(trip_id.clone())
                                        .or_default()
                                        .push(trip_update_id.to_string());
                                }
                            }
                        }
                    }
                }

                let relevant_stop_ids: AHashSet<&str> = trip_updates
                    .values()
                    .flat_map(|tu| {
                        tu.stop_time_update
                            .iter()
                            .filter_map(|stu| stu.stop_id.as_deref())
                    })
                    .collect();

                let stop_id_to_parent_id: AHashMap<String, String> = relevant_stop_ids
                    .iter()
                    .filter_map(|stop_id| {
                        authoritative_data
                            .stop_id_to_parent_id
                            .get(*stop_id)
                            .map(|parent| (stop_id.to_string(), parent.to_string()))
                    })
                    .collect();

                let time_it_took_to_process = start_of_function.elapsed();
                println!(
                    "Time it took to process trips_with_route_ids: {}ms, with {} trip updates",
                    time_it_took_to_process.as_millis(),
                    trip_updates.len()
                );

                Some(TripsSelectionResponse {
                    trip_updates,
                    trip_id_to_trip_update_ids,
                    stop_id_to_parent_id,
                })
            }
            None => None,
        }
    }

    async fn get_gtfs_rt(
        self,
        _: context::Context,
        realtime_feed_id: String,
        feed_type: catenary::aspen_dataset::GtfsRtType,
    ) -> Option<Vec<u8>> {
        let pair = self
            .authoritative_gtfs_rt_store
            .get_async(&(realtime_feed_id, feed_type))
            .await;

        match pair {
            Some(pair) => {
                let message: &CompactFeedMessage = pair.get();
                let message = message.to_feed_message();

                Some(message.encode_to_vec())
            }
            None => None,
        }
    }

    async fn get_gtfs_rt_compressed(
        self,
        _: context::Context,
        realtime_feed_id: String,
        feed_type: catenary::aspen_dataset::GtfsRtType,
    ) -> Option<Vec<u8>> {
        let pair = self
            .authoritative_gtfs_rt_store
            .get_async(&(realtime_feed_id, feed_type))
            .await;

        match pair {
            Some(pair) => {
                let message: &CompactFeedMessage = pair.get();
                let message = message.to_feed_message();

                let mut d = flate2::write::ZlibEncoder::new(Vec::new(), Compression::default());

                let bytes = message.encode_to_vec();

                d.write_all(bytes.as_slice()).unwrap();

                let compressed_bytes = d.finish().unwrap();

                Some(compressed_bytes)
            }
            None => None,
        }
    }

    async fn get_alerts_from_route_id(
        self,
        _: context::Context,
        chateau_id: String,
        route_id: String,
    ) -> Option<Vec<(String, AspenisedAlert)>> {
        alerts_responder::get_alerts_from_route_id(
            Arc::clone(&self.authoritative_data_store),
            &chateau_id,
            &route_id,
        )
    }

    async fn get_alerts_from_stop_id(
        self,
        _: context::Context,
        chateau_id: String,
        stop_id: String,
    ) -> Option<Vec<(String, AspenisedAlert)>> {
        alerts_responder::get_alerts_from_stop_id(
            Arc::clone(&self.authoritative_data_store),
            &chateau_id,
            &stop_id,
        )
    }

    async fn get_alert_from_stop_ids(
        self,
        _: context::Context,
        chateau_id: String,
        stop_ids: Vec<String>,
    ) -> Option<AlertsforManyStops> {
        alerts_responder::get_alert_from_stop_ids(
            Arc::clone(&self.authoritative_data_store),
            &chateau_id,
            stop_ids,
        )
    }

    async fn get_alert_from_trip_id(
        self,
        _: context::Context,
        chateau_id: String,
        trip_id: String,
    ) -> Option<Vec<(String, AspenisedAlert)>> {
        alerts_responder::get_alert_from_trip_id(
            Arc::clone(&self.authoritative_data_store),
            &chateau_id,
            &trip_id,
        )
    }

    async fn from_alpenrose_compressed(
        self,
        _: context::Context,
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
    ) -> bool {
        //decompress using flate2

        //   if new_data_status_from_timestamps {
        if true {
            let vehicles_gtfs_rt = match vehicles_response_code {
                Some(200) => match vehicles {
                    Some(v) => {
                        let mut d = ZlibDecoder::new(v.as_slice());
                        let mut decompressed_bytes = Vec::new();
                        d.read_to_end(&mut decompressed_bytes).unwrap();

                        match parse_gtfs_rt_message(decompressed_bytes.as_slice()) {
                            Ok(v) => Some(gtfs_rt_correct_route_id_string(
                                id_cleanup::gtfs_rt_cleanup(v),
                                realtime_feed_id.as_str(),
                            )),
                            Err(e) => {
                                eprintln!("Error decoding vehicles: {}", e);
                                None
                            }
                        }
                    }
                    None => None,
                },
                _ => None,
            };

            let mut vehicles_gtfs_rt =
                vehicles_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                    "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                    _ => gtfs_rt_feed,
                });

            if realtime_feed_id == "f-metro~losangeles~bus~rt"
                || realtime_feed_id == "f-r7h-translink~rt"
            {
                if let Some(v) = &mut vehicles_gtfs_rt {
                    //remove anything from the route id that is the hyphen - or after

                    for vehicle_position in v.entity.iter_mut() {
                        if let Some(vehicle) = &mut vehicle_position.vehicle {
                            if let Some(id) = &mut vehicle.trip {
                                if let Some(route_id) = &mut id.route_id {
                                    let route_id_new = route_id.as_str();
                                    let route_id_new = route_id_new.split('-').next().unwrap();
                                    *route_id = route_id_new.to_string();
                                }
                            }
                        }
                    }
                }
            }

            //delete stop times
            if let Some(v) = &mut vehicles_gtfs_rt {
                for vehicle_position in v.entity.iter_mut() {
                    if let Some(trip_update) = &mut vehicle_position.trip_update {
                        if !trip_update.stop_time_update.is_empty() {
                            trip_update.stop_time_update = vec![];
                        }
                    }
                }
            }

            let trips_gtfs_rt = match trips_response_code {
                Some(200) => match trips {
                    Some(t) => {
                        let mut d = ZlibDecoder::new(t.as_slice());
                        let mut decompressed_bytes = Vec::new();
                        d.read_to_end(&mut decompressed_bytes).unwrap();
                        match parse_gtfs_rt_message(decompressed_bytes.as_slice()) {
                            Ok(t) => Some(gtfs_rt_correct_route_id_string(
                                id_cleanup::gtfs_rt_cleanup(t),
                                realtime_feed_id.as_str(),
                            )),
                            Err(e) => {
                                eprintln!("Error decoding trips: {}", e);
                                None
                            }
                        }
                    }
                    None => None,
                },
                _ => None,
            };

            let mut trips_gtfs_rt =
                trips_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                    "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                    _ => gtfs_rt_feed,
                });

            if realtime_feed_id == "f-metro~losangeles~bus~rt"
                || "f-r7h-translink~rt" == realtime_feed_id
            {
                if let Some(t) = &mut trips_gtfs_rt {
                    //remove anything from the route id that is the hyphen - or after

                    for entity in t.entity.iter_mut() {
                        if let Some(trip_update) = &mut entity.trip_update {
                            if let Some(route_id) = &mut trip_update.trip.route_id {
                                let route_id_new = route_id.as_str();
                                let route_id_new = route_id_new.split('-').next().unwrap();
                                *route_id = route_id_new.to_string();
                            }
                        }
                    }
                }
            }

            let mut alerts_gtfs_rt = match alerts_dupe_trips {
                true => trips_gtfs_rt.clone(),
                false => match alerts_response_code {
                    Some(200) => match alerts {
                        Some(a) => {
                            let mut d = ZlibDecoder::new(a.as_slice());
                            let mut decompressed_bytes = Vec::new();
                            d.read_to_end(&mut decompressed_bytes).unwrap();
                            match parse_gtfs_rt_message(decompressed_bytes.as_slice()) {
                                Ok(a) => Some(id_cleanup::gtfs_rt_cleanup(a)),
                                Err(e) => {
                                    eprintln!("Error decoding alerts: {}", e);
                                    None
                                }
                            }
                        }
                        None => None,
                    },
                    _ => None,
                },
            };

            //delete stop time updates
            if let Some(a) = &mut alerts_gtfs_rt {
                for entity in a.entity.iter_mut() {
                    if let Some(trip_update) = &mut entity.trip_update {
                        if !trip_update.stop_time_update.is_empty() {
                            trip_update.stop_time_update = vec![];
                        }
                    }
                }
            }

            if realtime_feed_id == "f-metro~losangeles~bus~rt"
                || "f-r7h-translink~rt" == realtime_feed_id
            {
                if let Some(a) = &mut alerts_gtfs_rt {
                    //remove anything from the route id that is the hyphen - or after

                    for entity in a.entity.iter_mut() {
                        if let Some(alert) = &mut entity.alert {
                            //iter through each informed entity

                            for informed_entity in alert.informed_entity.iter_mut() {
                                if let Some(route_id) = &mut informed_entity.route_id {
                                    let route_id_new = route_id.as_str();
                                    let route_id_new = route_id_new.split('-').next().unwrap();
                                    *route_id = route_id_new.to_string();
                                }
                            }
                        }
                    }
                }
            }

            //get and update raw gtfs_rt data

            //  println!("Parsed FeedMessages for {}", realtime_feed_id);

            let mut new_data = false;

            let hash_data_start = Instant::now();

            if let Some(vehicles_gtfs_rt) = &vehicles_gtfs_rt {
                if !new_data {
                    let new_data_v = contains_new_data(
                        &self,
                        &realtime_feed_id,
                        GtfsRtType::VehiclePositions,
                        vehicles_gtfs_rt,
                    );

                    if new_data_v {
                        new_data = new_data_v;
                    }
                }

                let _ = save_timestamps(
                    &self,
                    &realtime_feed_id,
                    GtfsRtType::VehiclePositions,
                    vehicles_gtfs_rt,
                );
            }

            if let Some(trips_gtfs_rt) = &trips_gtfs_rt {
                if !new_data {
                    let new_data_t = contains_new_data(
                        &self,
                        &realtime_feed_id,
                        GtfsRtType::TripUpdates,
                        trips_gtfs_rt,
                    );

                    if new_data_t {
                        new_data = new_data_t;
                    }
                }

                let _ = save_timestamps(
                    &self,
                    &realtime_feed_id,
                    GtfsRtType::TripUpdates,
                    trips_gtfs_rt,
                );
            }

            if let Some(alerts_gtfs_rt) = &alerts_gtfs_rt {
                if !new_data {
                    let new_data_a = contains_new_data(
                        &self,
                        &realtime_feed_id,
                        GtfsRtType::Alerts,
                        alerts_gtfs_rt,
                    );

                    if new_data_a {
                        new_data = new_data_a;
                    }
                }

                let _ =
                    save_timestamps(&self, &realtime_feed_id, GtfsRtType::Alerts, alerts_gtfs_rt);
            }

            if realtime_feed_id.contains("unwire") {
                //always treat as new data
                new_data = true;
            }

            if new_data
                || chateau_id.as_str() == "dallasarearapidtransit"
                || chateau_id.as_str() == "fortworthtransportationauthority"
            {
                if let Some(vehicles_gtfs_rt) = vehicles_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry_async((realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
                        .await
                        .and_modify(|gtfs_data| {
                            *gtfs_data =
                                CompactFeedMessage::from_feed_message(vehicles_gtfs_rt.clone())
                        })
                        .or_insert(CompactFeedMessage::from_feed_message(
                            vehicles_gtfs_rt.clone(),
                        ));
                }

                if let Some(trip_gtfs_rt) = trips_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry_async((realtime_feed_id.clone(), GtfsRtType::TripUpdates))
                        .await
                        .and_modify(|gtfs_data| {
                            *gtfs_data = CompactFeedMessage::from_feed_message(trip_gtfs_rt.clone())
                        })
                        .or_insert(CompactFeedMessage::from_feed_message(trip_gtfs_rt.clone()));
                }

                if let Some(alerts_gtfs_rt) = alerts_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry_async((realtime_feed_id.clone(), GtfsRtType::Alerts))
                        .await
                        .and_modify(|gtfs_data| {
                            *gtfs_data =
                                CompactFeedMessage::from_feed_message(alerts_gtfs_rt.clone())
                        })
                        .or_insert(CompactFeedMessage::from_feed_message(
                            alerts_gtfs_rt.clone(),
                        ));
                }
            }

            let hash_data_duration = hash_data_start.elapsed();

            println!(
                "wrote {realtime_feed_id} in chateau {chateau_id}, took {} ms, is new data: {new_data}",
                hash_data_duration.as_millis()
            );

            //   println!("Saved FeedMessages for {}", realtime_feed_id);

            if new_data
                || chateau_id.as_str() == "santacruzmetro"
                || chateau_id.as_str() == "dallasarearapidtransit"
                || chateau_id.as_str() == "fortworthtransportationauthority"
            {
                let mut lock_chateau_queue = self.alpenrose_to_process_queue_chateaux.lock().await;

                if !lock_chateau_queue.contains(&chateau_id) {
                    lock_chateau_queue.insert(chateau_id.clone());
                    self.alpenrose_to_process_queue.push(ProcessAlpenroseData {
                        chateau_id,
                        realtime_feed_id,
                        has_vehicles,
                        has_trips,
                        has_alerts,
                        vehicles_response_code,
                        trips_response_code,
                        alerts_response_code,
                        time_of_submission_ms,
                    });
                }
            }
        }

        true
    }

    async fn from_alpenrose(
        self,
        _: context::Context,
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
    ) -> bool {
        /*
           let new_v_header_timestamp = vehicles
            .as_ref()
            .map(|x| catenary::timestamp_extraction::get_gtfs_header_timestamp_from_bytes(x.as_slice()))
            .flatten();
        let new_t_header_timestamp = trips
            .as_ref()
            .map(|x| catenary::timestamp_extraction::get_gtfs_header_timestamp_from_bytes(x.as_slice()))
            .flatten();
        let new_a_header_timestamp = alerts
            .as_ref()
            .map(|x| catenary::timestamp_extraction::get_gtfs_header_timestamp_from_bytes(x.as_slice()))
            .flatten();*/

        /*
        let existing_timestamp_v = self
            .timestamps_of_gtfs_rt
            .get(&(realtime_feed_id.clone(), GtfsRtType::VehiclePositions));
        let existing_timestamp_t = self
            .timestamps_of_gtfs_rt
            .get(&(realtime_feed_id.clone(), GtfsRtType::TripUpdates));
        let existing_timestamp_a = self
            .timestamps_of_gtfs_rt
            .get(&(realtime_feed_id.clone(), GtfsRtType::Alerts));

        // if any of the timestamps are None, then we need to update the data
        //otherwise, if any of the timestamps are different, we need to update the data

        let new_data_status_from_timestamps = match (
            new_v_header_timestamp,
            new_t_header_timestamp,
            new_a_header_timestamp,
        ) {
            (
                Some(new_v_header_timestamp),
                Some(new_t_header_timestamp),
                Some(new_a_header_timestamp),
            ) => {
                match (
                    existing_timestamp_v,
                    existing_timestamp_t,
                    existing_timestamp_a,
                ) {
                    (
                        Some(existing_timestamp_v),
                        Some(existing_timestamp_t),
                        Some(existing_timestamp_a),
                    ) => {
                        if new_v_header_timestamp == *existing_timestamp_v.get()
                            && new_t_header_timestamp == *existing_timestamp_t.get()
                            && new_a_header_timestamp == *existing_timestamp_a.get()
                        {
                            println!("Same data as before in feed {}, skipping", realtime_feed_id);
                            false
                        } else {
                            true
                        }
                    }
                    _ => true,
                }
            }
            _ => true,
        };*/

        //   if new_data_status_from_timestamps {
        if true {
            let vehicles_gtfs_rt = match vehicles_response_code {
                Some(200) => match vehicles {
                    Some(v) => match parse_gtfs_rt_message(v.as_slice()) {
                        Ok(v) => Some(gtfs_rt_correct_route_id_string(
                            id_cleanup::gtfs_rt_cleanup(v),
                            realtime_feed_id.as_str(),
                        )),
                        Err(e) => {
                            eprintln!("Error decoding vehicles: {}", e);
                            None
                        }
                    },
                    None => None,
                },
                _ => None,
            };

            let vehicles_gtfs_rt =
                vehicles_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                    "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                    _ => gtfs_rt_feed,
                });

            let trips_gtfs_rt = match trips_response_code {
                Some(200) => match trips {
                    Some(t) => match parse_gtfs_rt_message(t.as_slice()) {
                        Ok(t) => Some(gtfs_rt_correct_route_id_string(
                            id_cleanup::gtfs_rt_cleanup(t),
                            realtime_feed_id.as_str(),
                        )),
                        Err(e) => {
                            eprintln!("Error decoding trips: {}", e);
                            None
                        }
                    },
                    None => None,
                },
                _ => None,
            };

            let trips_gtfs_rt = trips_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                _ => gtfs_rt_feed,
            });

            let alerts_gtfs_rt = match alerts_response_code {
                Some(200) => match alerts {
                    Some(a) => match parse_gtfs_rt_message(a.as_slice()) {
                        Ok(a) => Some(id_cleanup::gtfs_rt_cleanup(a)),
                        Err(e) => {
                            eprintln!("Error decoding alerts: {}", e);
                            None
                        }
                    },
                    None => None,
                },
                _ => None,
            };

            //get and update raw gtfs_rt data

            //  println!("Parsed FeedMessages for {}", realtime_feed_id);

            let mut new_data = false;

            let hash_data_start = Instant::now();

            if let Some(vehicles_gtfs_rt) = &vehicles_gtfs_rt {
                if !new_data {
                    let new_data_v = contains_new_data(
                        &self,
                        &realtime_feed_id,
                        GtfsRtType::VehiclePositions,
                        vehicles_gtfs_rt,
                    );

                    if new_data_v {
                        new_data = new_data_v;
                    }
                }

                let _ = save_timestamps(
                    &self,
                    &realtime_feed_id,
                    GtfsRtType::VehiclePositions,
                    vehicles_gtfs_rt,
                );
            }

            if let Some(trips_gtfs_rt) = &trips_gtfs_rt {
                if !new_data {
                    let new_data_t = contains_new_data(
                        &self,
                        &realtime_feed_id,
                        GtfsRtType::TripUpdates,
                        trips_gtfs_rt,
                    );

                    if new_data_t {
                        new_data = new_data_t;
                    }
                }

                let _ = save_timestamps(
                    &self,
                    &realtime_feed_id,
                    GtfsRtType::TripUpdates,
                    trips_gtfs_rt,
                );
            }

            if let Some(alerts_gtfs_rt) = &alerts_gtfs_rt {
                if !new_data {
                    let new_data_a = contains_new_data(
                        &self,
                        &realtime_feed_id,
                        GtfsRtType::Alerts,
                        alerts_gtfs_rt,
                    );

                    if new_data_a {
                        new_data = new_data_a;
                    }
                }

                let _ =
                    save_timestamps(&self, &realtime_feed_id, GtfsRtType::Alerts, alerts_gtfs_rt);
            }

            if new_data || chateau_id == "uc~irvine~anteater~express" {
                if let Some(vehicles_gtfs_rt) = vehicles_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry_async((realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
                        .await
                        .and_modify(|gtfs_data| {
                            *gtfs_data =
                                CompactFeedMessage::from_feed_message(vehicles_gtfs_rt.clone())
                        })
                        .or_insert(CompactFeedMessage::from_feed_message(
                            vehicles_gtfs_rt.clone(),
                        ));
                }

                if let Some(trip_gtfs_rt) = trips_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry_async((realtime_feed_id.clone(), GtfsRtType::TripUpdates))
                        .await
                        .and_modify(|gtfs_data| {
                            *gtfs_data = CompactFeedMessage::from_feed_message(trip_gtfs_rt.clone())
                        })
                        .or_insert(CompactFeedMessage::from_feed_message(trip_gtfs_rt.clone()));
                }

                if let Some(alerts_gtfs_rt) = alerts_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry_async((realtime_feed_id.clone(), GtfsRtType::Alerts))
                        .await
                        .and_modify(|gtfs_data| {
                            *gtfs_data =
                                CompactFeedMessage::from_feed_message(alerts_gtfs_rt.clone())
                        })
                        .or_insert(CompactFeedMessage::from_feed_message(
                            alerts_gtfs_rt.clone(),
                        ));
                }
            }

            let hash_data_duration = hash_data_start.elapsed();

            // println!(
            //     "wrote {realtime_feed_id} in chateau {chateau_id}, took {} ms, is new data: {new_data}",
            //     hash_data_duration.as_millis()
            // );

            //   println!("Saved FeedMessages for {}", realtime_feed_id);

            if new_data {
                let mut lock_chateau_queue = self.alpenrose_to_process_queue_chateaux.lock().await;

                if !lock_chateau_queue.contains(&chateau_id) {
                    lock_chateau_queue.insert(chateau_id.clone());
                    self.alpenrose_to_process_queue.push(ProcessAlpenroseData {
                        chateau_id,
                        realtime_feed_id,
                        has_vehicles,
                        has_trips,
                        has_alerts,
                        vehicles_response_code,
                        trips_response_code,
                        alerts_response_code,
                        time_of_submission_ms,
                    });
                }
            }
        }

        true
    }

    async fn get_vehicle_locations(
        self,
        _: context::Context,
        chateau_id: String,
        existing_fasthash_of_routes: Option<u64>,
        route_types_filter: Option<Vec<i16>>,
    ) -> Option<GetVehicleLocationsResponse> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let fast_hash_of_routes = aspenised_data.vehicle_routes_cache_hash;

                Some(GetVehicleLocationsResponse {
                    vehicle_route_cache: {
                        let send_nothing = match &existing_fasthash_of_routes {
                            Some(existing_fasthash_of_routes) => {
                                existing_fasthash_of_routes == &fast_hash_of_routes
                            }
                            None => false,
                        };

                        if send_nothing {
                            None
                        } else {
                            match &route_types_filter {
                                Some(route_types_filter) => {
                                    let filtered_vehicle_routes_cache = aspenised_data
                                        .vehicle_routes_cache
                                        .iter()
                                        .filter(|(_route_id, route_info)| {
                                            route_types_filter.contains(&route_info.route_type)
                                        })
                                        .map(|(a, b)| (a.clone(), b.clone()))
                                        .collect::<AHashMap<_, _>>();

                                    Some(filtered_vehicle_routes_cache)
                                }
                                None => Some(aspenised_data.vehicle_routes_cache.clone()),
                            }
                        }
                    },
                    vehicle_positions: match &route_types_filter {
                        Some(route_types_filter) => {
                            let filtered_vehicle_positions = aspenised_data
                                .vehicle_positions
                                .iter()
                                .filter(|(_gtfs_id, vehicle_position)| {
                                    match &vehicle_position.route_type {
                                        route_type => route_types_filter.contains(route_type),
                                    }
                                })
                                .map(|(a, b)| (a.clone(), b.clone()))
                                .collect::<AHashMap<_, _>>();

                            filtered_vehicle_positions
                        }
                        None => aspenised_data.vehicle_positions.clone(),
                    },
                    hash_of_routes: fast_hash_of_routes,
                    last_updated_time_ms: aspenised_data.last_updated_time_ms,
                })
            }
            None => None,
        }
    }

    async fn get_vehicle_locations_with_route_filtering(
        self,
        _context: tarpc::context::Context,
        chateau_id: String,
        existing_fasthash_of_routes: Option<u64>,
        route_ids: Option<Vec<String>>,
    ) -> Option<GetVehicleLocationsResponse> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let fast_hash_of_routes = aspenised_data.vehicle_routes_cache_hash;

                let vehicle_pos_new = aspenised_data
                    .vehicle_positions
                    .iter()
                    .filter(|(_key, value)| match &route_ids {
                        Some(route_ids_filter_input) => match &value.trip {
                            None => false,
                            Some(trip) => match &trip.route_id {
                                None => false,
                                Some(route_id) => route_ids_filter_input.contains(&route_id),
                            },
                        },
                        None => false,
                    })
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect::<AHashMap<_, _>>();

                Some(GetVehicleLocationsResponse {
                    vehicle_route_cache: match existing_fasthash_of_routes {
                        Some(existing_fasthash_of_routes) => {
                            match existing_fasthash_of_routes == fast_hash_of_routes {
                                true => None,
                                false => Some(aspenised_data.vehicle_routes_cache.clone()),
                            }
                        }
                        None => Some(aspenised_data.vehicle_routes_cache.clone()),
                    },
                    vehicle_positions: vehicle_pos_new,
                    hash_of_routes: fast_hash_of_routes,
                    last_updated_time_ms: aspenised_data.last_updated_time_ms,
                })
            }
            None => None,
        }
    }

    async fn get_single_vehicle_location_from_gtfsid(
        self,
        _: context::Context,
        chateau_id: String,
        gtfs_id: String,
    ) -> Option<AspenisedVehiclePosition> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let vehicle_position = aspenised_data.vehicle_positions.get(&gtfs_id);

                match vehicle_position {
                    Some(vehicle_position) => Some(vehicle_position.clone()),
                    None => {
                        println!("Vehicle position not found for gtfs id {}", gtfs_id);
                        None
                    }
                }
            }
            None => None,
        }
    }

    async fn get_single_vehicle_location_from_vehicle_label(
        self,
        _: context::Context,
        chateau_id: String,
        vehicle_id: String,
    ) -> Option<AspenisedVehiclePosition> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let gtfs_id = aspenised_data.vehicle_label_to_gtfs_id.get(&vehicle_id);

                match gtfs_id {
                    Some(gtfs_id) => {
                        let vehicle_position = aspenised_data.vehicle_positions.get(gtfs_id);

                        match vehicle_position {
                            Some(vehicle_position) => Some(vehicle_position.clone()),
                            None => {
                                println!("Vehicle position not found for gtfs id {}", gtfs_id);
                                None
                            }
                        }
                    }
                    None => None,
                }
            }
            None => None,
        }
    }

    async fn get_trip_updates_from_route_ids(
        self,
        _: context::Context,
        chateau_id: String,
        route_ids: Vec<String>,
    ) -> Option<Vec<AspenisedTripUpdate>> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let mut data_out: Vec<AspenisedTripUpdate> = Vec::new();

                for route_id in &route_ids {
                    let trip_updates_id_list = aspenised_data
                        .trip_updates_lookup_by_route_id_to_trip_update_ids
                        .get(route_id.as_str());

                    if let Some(trip_update_ids) = trip_updates_id_list {
                        for trip_update_id in trip_update_ids.iter() {
                            let trip_update =
                                aspenised_data.trip_updates.get(trip_update_id.as_str());

                            if let Some(trip_update) = trip_update {
                                data_out.push(trip_update.clone());
                            }
                        }
                    }
                }

                Some(data_out)
            }
            None => None,
        }
    }

    async fn get_trip_updates_from_trip_id(
        self,
        _: context::Context,
        chateau_id: String,
        trip_id: String,
    ) -> Option<Vec<AspenisedTripUpdate>> {
        match self.authoritative_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();
                let trip_updates_id_list = aspenised_data
                    .trip_updates_lookup_by_trip_id_to_trip_update_ids
                    .get(trip_id.as_str());

                match trip_updates_id_list {
                    Some(trip_updates_id_list) => {
                        let mut trip_updates = Vec::new();

                        for trip_update_id in trip_updates_id_list {
                            let trip_update =
                                aspenised_data.trip_updates.get(trip_update_id.as_str());

                            match trip_update {
                                Some(trip_update) => {
                                    trip_updates.push(trip_update.clone());
                                }
                                None => {
                                    println!(
                                        "Trip update not found for trip update id {}",
                                        trip_update_id
                                    );
                                }
                            }
                        }

                        Some(trip_updates)
                    }
                    None => {
                        println!("Trip id not found in trip updates lookup table");
                        None
                    }
                }
            }
            None => None,
        }
    }

    async fn get_all_alerts(
        self,
        _: context::Context,
        chateau_id: String,
    ) -> Option<HashMap<String, AspenisedAlert>> {
        match self
            .authoritative_data_store
            .as_ref()
            .get_async(&chateau_id)
            .await
        {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();

                Some(
                    aspenised_data
                        .aspenised_alerts
                        .clone()
                        .into_iter()
                        .collect(),
                )
            }
            None => None,
        }
    }

    async fn full_aspen_dataset(
        self,
        _context: tarpc::context::Context,
        chateau_id: String,
    ) -> Option<AspenisedData> {
        match self
            .authoritative_data_store
            .as_ref()
            .get_async(&chateau_id)
            .await
        {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();

                Some(aspenised_data.clone())
            }
            None => None,
        }
    }

    async fn full_aspen_dataset_backup(
        self,
        _context: tarpc::context::Context,
        chateau_id: String,
    ) -> Option<AspenisedData> {
        match self.backup_data_store.get_async(&chateau_id).await {
            Some(aspenised_data) => Some(aspenised_data.get().clone()),
            None => None,
        }
    }

    async fn insert_backup_aspen_dataset(
        self,
        _context: tarpc::context::Context,
        chateau_id: String,
        data: AspenisedData,
    ) -> () {
        let _ = self.backup_data_store.insert_async(chateau_id, data).await;

        ()
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;

    if std::env::var("TOKIOCONSOLE").is_ok() {
        console_subscriber::init();
    }

    let subscriber = tracing_subscriber::fmt()
        // Use a more compact, abbreviated log format
        .compact()
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .finish();

    let tracing_result = tracing::subscriber::set_global_default(subscriber);

    // Worker Id for this instance of Aspen
    let this_worker_id = Arc::new(Uuid::new_v4().to_string());

    let etcd_urls_original =
        std::env::var("ETCD_URLS").unwrap_or_else(|_| "localhost:2379".to_string());
    let etcd_urls = etcd_urls_original
        .split(',')
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    let etcd_addresses = Arc::new(etcd_urls);

    let etcd_username = std::env::var("ETCD_USERNAME");

    let etcd_password = std::env::var("ETCD_PASSWORD");

    let etcd_connect_options: Option<etcd_client::ConnectOptions> =
        match (etcd_username, etcd_password) {
            (Ok(username), Ok(password)) => {
                Some(etcd_client::ConnectOptions::new().with_user(username, password))
            }
            _ => None,
        };

    let arc_etcd_connect_options = Arc::new(etcd_connect_options.clone());

    let channel_count = std::env::var("CHANNELS")
        .expect("channels not set")
        .parse::<usize>()
        .expect("channels not a number");
    let alpenrosethreadcount = std::env::var("ALPENROSETHREADCOUNT")
        .expect("alpenrosethreadcount not set")
        .parse::<usize>()
        .expect("alpenrosethreadcount not a number");

    let etcd_lease_id_for_this_worker: i64 = rand::rng().random_range(0..i64::MAX);

    //connect to postgres
    println!("Connecting to postgres");
    let conn_pool: CatenaryPostgresPool = make_async_pool().await.unwrap();
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);
    println!("Connected to postgres");

    let port_number: u16 = match std::env::var("PORT") {
        Ok(port) => port.parse::<u16>().unwrap(),
        Err(e) => {
            println!("No port number found, defaulting to 40427");
            40427u16
        }
    };

    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), port_number);
    let socket = SocketAddr::new(server_addr.0, server_addr.1);

    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Bincode::default).await?;
    tracing::info!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);

    //connect to etcd

    let mut etcd = etcd_client::Client::connect(
        etcd_addresses.as_slice(),
        arc_etcd_connect_options.as_ref().to_owned(),
    )
    .await
    .expect("Failed to connect to etcd");

    //register etcd_lease_id

    let make_lease = etcd
        .lease_grant(
            //30 seconds
            30,
            Some(etcd_client::LeaseGrantOptions::new().with_id(etcd_lease_id_for_this_worker)),
        )
        .await
        .expect("Failed to make lease with etcd");

    println!("Lease granted: {:?}", make_lease);

    //register that the worker exists

    let worker_metadata = AspenWorkerMetadataEtcd {
        etcd_lease_id: etcd_lease_id_for_this_worker,
        socket,
        worker_id: this_worker_id.clone().to_string(),
    };

    let etcd_this_worker_assignment = etcd
        .put(
            format!("/aspen_workers/{}", &this_worker_id).as_str(),
            catenary::bincode_serialize(&worker_metadata).unwrap(),
            Some(etcd_client::PutOptions::new().with_lease(etcd_lease_id_for_this_worker)),
        )
        .await?;

    let workers_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let chateau_list: Arc<Mutex<Option<ChateauxLeaderHashMap>>> = Arc::new(Mutex::new(None));

    let process_from_alpenrose_queue = Arc::new(Injector::<ProcessAlpenroseData>::new());
    let raw_gtfs = Arc::new(SccHashMap::new());
    let authoritative_data_store = Arc::new(SccHashMap::new());
    let backup_data_store = Arc::new(SccHashMap::new());
    let backup_raw_gtfs = Arc::new(SccHashMap::new());
    let alpenrose_to_process_queue_chateaux = Arc::new(Mutex::new(HashSet::new()));
    let rough_hash_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>> =
        Arc::new(SccHashMap::new());
    let hash_of_raw_gtfs_rt_protobuf: Arc<SccHashMap<String, GtfsRealtimeHashStore>> =
        Arc::new(SccHashMap::new());
    let timestamps_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>> =
        Arc::new(SccHashMap::new());

    //run both the leader and the listener simultaniously
    let b_alpenrose_to_process_queue = Arc::clone(&process_from_alpenrose_queue);
    let b_authoritative_gtfs_rt_store = Arc::clone(&raw_gtfs);
    let b_authoritative_data_store = Arc::clone(&authoritative_data_store);
    let b_conn_pool = Arc::clone(&arc_conn_pool);
    let b_thread_count = alpenrosethreadcount;

    // Load persisted data
    println!("Loading persisted data...");
    let data_dir = Path::new("data/aspen_data");
    if data_dir.exists() {
        if let Ok(entries) = fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    if extension == "zlib" {
                        if let Some(file_stem) = path.file_stem() {
                            if let Some(stem_str) = file_stem.to_str() {
                                if let Some(chateau_id) = stem_str.strip_suffix(".bin") {
                                    println!("Loading data for chateau: {}", chateau_id);
                                    match persistence::load_chateau_data(chateau_id) {
                                        Ok(Some(data)) => {
                                            authoritative_data_store
                                                .entry_async(chateau_id.to_string())
                                                .await
                                                .or_insert(data);
                                            println!("Successfully loaded data for {}", chateau_id);
                                        }
                                        Ok(None) => {
                                            eprintln!("No data found for {}", chateau_id);
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Failed to load data for {}: {}",
                                                chateau_id, e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    println!("Finished loading persisted data.");

    let async_from_alpenrose_processor_handler: tokio::task::JoinHandle<
        Result<(), Box<dyn Error + Sync + Send>>,
    > = tokio::task::spawn(async_threads_alpenrose::alpenrose_process_threads(
        b_alpenrose_to_process_queue,
        b_authoritative_gtfs_rt_store,
        b_authoritative_data_store,
        b_conn_pool,
        b_thread_count,
        Arc::clone(&alpenrose_to_process_queue_chateaux),
        etcd_lease_id_for_this_worker,
        redis_client.clone(),
    ));

    let this_worker_id_copy = this_worker_id.clone();

    let etcd_lease_renewer: tokio::task::JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> =
        tokio::task::spawn({
            let etcd_addresses = etcd_addresses.clone();
            let arc_etcd_connect_options = arc_etcd_connect_options.clone();
            let worker_id_for_this_thread = Arc::clone(&this_worker_id);

            let mut etcd = etcd_client::Client::connect(
                etcd_addresses.clone().as_slice(),
                arc_etcd_connect_options.as_ref().to_owned(),
            )
            .await?;

            async move {
                loop {
                    println!("Renewing lease");
                    let x = etcd.lease_keep_alive(etcd_lease_id_for_this_worker).await;

                    match x {
                        Ok(_) => {
                            //  println!("Lease renew successful");

                            /*
                            let etcd_this_worker_assignment = etcd
                                .put(
                                    format!("/aspen_workers/{}", &this_worker_id_copy).as_str(),
                                    catenary::bincode_serialize(&worker_metadata).unwrap(),
                                    Some(
                                        etcd_client::PutOptions::new()
                                            .with_lease(etcd_lease_id_for_this_worker),
                                    ),
                                )
                                .await;*/

                            //if let Err(e) = &etcd_this_worker_assignment {
                            //    eprintln!("Error inserting worker still active {:#?}", e);
                            //}

                            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

                            //let _ = tokio::task::yield_now().await;
                        }
                        Err(e) => {
                            eprintln!("Error renewing lease: {:#?}", e);

                            if let Ok(etcd_new) = etcd_client::Client::connect(
                                etcd_addresses.clone().as_slice(),
                                arc_etcd_connect_options.as_ref().to_owned(),
                            )
                            .await
                            {
                                etcd = etcd_new
                            }

                            let make_lease = etcd
                                .lease_grant(
                                    //10 seconds
                                    10,
                                    Some(
                                        etcd_client::LeaseGrantOptions::new()
                                            .with_id(etcd_lease_id_for_this_worker),
                                    ),
                                )
                                .await;

                            let etcd_this_worker_assignment = etcd
                                .put(
                                    format!("/aspen_workers/{}", &this_worker_id_copy).as_str(),
                                    catenary::bincode_serialize(&worker_metadata).unwrap(),
                                    Some(
                                        etcd_client::PutOptions::new()
                                            .with_lease(etcd_lease_id_for_this_worker),
                                    ),
                                )
                                .await;

                            match make_lease {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("lease regrant failed {e}");
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
        });

    let tarpc_server: tokio::task::JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> =
        tokio::task::spawn({
            println!("Listening on port {}", listener.local_addr().port());

            let worker_id_for_this_thread = Arc::clone(&this_worker_id);

            let conn_pool_arced = Arc::clone(&arc_conn_pool);

            let etcd_addresses = Arc::clone(&Arc::clone(&etcd_addresses));

            move || async move {
                listener
                    // Ignore accept errors.
                    .filter_map(|r| future::ready(r.ok()))
                    .map(server::BaseChannel::with_defaults)
                    .map(|channel| {
                        let server = AspenServer {
                            addr: channel.transport().peer_addr().unwrap(),
                            worker_id: worker_id_for_this_thread.clone(),
                            authoritative_data_store: Arc::clone(&authoritative_data_store),
                            conn_pool: conn_pool_arced.clone(),
                            alpenrose_to_process_queue: Arc::clone(&process_from_alpenrose_queue),
                            authoritative_gtfs_rt_store: Arc::clone(&raw_gtfs),
                            backup_data_store: Arc::clone(&backup_data_store),
                            backup_gtfs_rt_store: Arc::clone(&backup_raw_gtfs),
                            alpenrose_to_process_queue_chateaux: Arc::clone(
                                &alpenrose_to_process_queue_chateaux,
                            ),
                            rough_hash_of_gtfs_rt: Arc::clone(&rough_hash_of_gtfs_rt),
                            hash_of_raw_gtfs_rt_protobuf: Arc::clone(&hash_of_raw_gtfs_rt_protobuf),
                            worker_etcd_lease_id: etcd_lease_id_for_this_worker,
                            etcd_addresses: etcd_addresses.clone(),
                            etcd_connect_options: Arc::clone(&arc_etcd_connect_options),
                            timestamps_of_gtfs_rt: Arc::clone(&timestamps_of_gtfs_rt),
                            authoritative_trip_updates_by_gtfs_feed_history: Arc::new(
                                SccHashMap::new(),
                            ),
                            backup_trip_updates_by_gtfs_feed_history: Arc::new(SccHashMap::new()),
                        };
                        channel.execute(server.serve()).for_each(spawn)
                    })
                    // Max n channels.
                    .buffer_unordered(channel_count)
                    .for_each(|_| async {})
                    .await;

                panic!("Why did the tarpc task end?");

                Ok(())
            }
        }());

    let feeds_list = chateau_list.clone();

    let arc_etcd_connection_options = Arc::new(etcd_connect_options.clone());

    let this_worker_id_copy = this_worker_id.clone();

    let lease_id_for_this_worker = etcd_lease_id_for_this_worker;

    let etcd_addresses_copy = etcd_addresses.clone();

    let result_series = tokio::try_join!(
        //flatten_stopping_is_err(async_from_alpenrose_processor_handler),
        flatten_stopping_is_err(tarpc_server),
        flatten_stopping_is_err(etcd_lease_renewer),
    )
    .unwrap();

    panic!("IT WASNT SUPPOSED TO END");
}

async fn flatten_stopping_is_err<T>(
    handle: tokio::task::JoinHandle<Result<T, Box<dyn Error + Sync + Send>>>,
) -> Result<T, Box<dyn Error + Sync + Send>> {
    match handle.await {
        Ok(Ok(result)) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "stopping wasnt supposed to happen",
        ))),
        Ok(Err(err)) => {
            eprintln!("{:#?}", err);
            Err(err)
        }
        Err(err) => {
            eprintln!("{:#?}", err);
            Err(Box::new(err))
        }
    }
}

async fn flatten<T>(
    handle: tokio::task::JoinHandle<Result<T, Box<dyn Error + Sync + Send>>>,
) -> Result<T, Box<dyn Error + Sync + Send>> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Box::new(err)),
    }
}

enum SaveTimestamp {
    Saved,
}

fn save_timestamps(
    server: &AspenServer,
    realtime_feed_id: &str,
    gtfs_rt_type: GtfsRtType,
    pb_data: &FeedMessage,
) -> SaveTimestamp {
    let key = (realtime_feed_id.to_string(), gtfs_rt_type);

    match pb_data.header.timestamp {
        Some(0) | None => {}
        Some(new_timestamp) => {
            server
                .timestamps_of_gtfs_rt
                .entry_sync(key.clone())
                .and_modify(|mut_timestamp| *mut_timestamp = new_timestamp)
                .or_insert(new_timestamp);
        }
    }

    SaveTimestamp::Saved
}

fn contains_new_data(
    server: &AspenServer,
    realtime_feed_id: &str,
    gtfs_rt_type: GtfsRtType,
    pb_data: &FeedMessage,
) -> bool {
    let mut new_data = false;

    let key = (realtime_feed_id.to_string(), gtfs_rt_type);

    match pb_data.header.timestamp {
        Some(0) | None => {
            new_data = true;
        }
        Some(new_timestamp) => match server.timestamps_of_gtfs_rt.get_sync(&key) {
            Some(existing_timestamp) => {
                if *(existing_timestamp.get()) == new_timestamp {
                } else {
                    new_data = true;
                }
            }
            None => {
                new_data = true;
            }
        },
    }

    new_data
}
