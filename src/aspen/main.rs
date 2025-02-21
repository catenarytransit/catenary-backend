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
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tarpc::{
    context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Bincode,
};
use tokio::sync::Mutex;
use tokio::time;
use uuid::Uuid;
mod leader_thread;
use leader_thread::aspen_leader_thread;
mod import_alpenrose;
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
use catenary::parse_gtfs_rt_message;
use rand::Rng;
use std::collections::HashMap;
use std::collections::HashSet;
mod alerts_responder;
mod aspen_assignment;
use catenary::rt_recent_history::RtCacheEntry;
use catenary::rt_recent_history::RtKey;
use flate2::read::ZlibDecoder;
use flate2::Compression;
use prost::Message;
use rand::distr::Uniform;
use rand::thread_rng;
use std::io::Read;
use std::time::Instant;

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
    pub authoritative_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    pub conn_pool: Arc<CatenaryPostgresPool>,
    pub authoritative_trip_updates_by_gtfs_feed_history:
        Arc<SccHashMap<CompactString, AHashMap<RtKey, RtCacheEntry>>>,
    pub alpenrose_to_process_queue: Arc<Injector<ProcessAlpenroseData>>,
    pub alpenrose_to_process_queue_chateaus: Arc<Mutex<HashSet<String>>>,
    pub rough_hash_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>>,
    pub hash_of_raw_gtfs_rt_protobuf: Arc<SccHashMap<String, GtfsRealtimeHashStore>>,
    pub backup_data_store: Arc<SccHashMap<String, catenary::aspen_dataset::AspenisedData>>,
    pub backup_gtfs_rt_store: Arc<SccHashMap<(String, GtfsRtType), FeedMessage>>,
    pub backup_trip_updates_by_gtfs_feed_history:
        Arc<SccHashMap<CompactString, AHashMap<RtKey, RtCacheEntry>>>,
    pub etcd_addresses: Arc<Vec<String>>,
    pub etcd_connect_options: Arc<Option<etcd_client::ConnectOptions>>,
    pub worker_etcd_lease_id: i64,
    pub timestamps_of_gtfs_rt: Arc<SccHashMap<(String, GtfsRtType), u64>>,
}

impl AspenRpc for AspenServer {
    async fn hello(self, _: context::Context, name: String) -> String {
        let sleep_time = Duration::from_millis(
            Uniform::new_inclusive(1, 10)
                .expect("NOT VALID RANGE")
                .sample(&mut thread_rng()),
        );
        tokio::time::sleep(sleep_time).await;
        format!("Hello, {name}! You are connected from {}", self.addr)
    }

    async fn get_all_trips_with_ids(
        self,
        ctx: context::Context,
        chateau_id: String,
        trip_ids: Vec<String>,
    ) -> Option<TripsSelectionResponse> {
        match self.authoritative_data_store.get(&chateau_id) {
            None => None,
            Some(authoritative_data) => {
                let authoritative_data = authoritative_data.get();

                let trip_id_list = trip_ids.into_iter().collect::<AHashSet<String>>();

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

                Some(TripsSelectionResponse {
                    trip_updates,
                    trip_id_to_trip_update_ids,
                })
            }
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
            .get(&(realtime_feed_id, feed_type));

        match pair {
            Some(pair) => {
                let message: &FeedMessage = pair.get();

                Some(message.encode_to_vec())
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

            let vehicles_gtfs_rt =
                vehicles_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                    "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                    _ => gtfs_rt_feed,
                });

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

            let trips_gtfs_rt = trips_gtfs_rt.map(|gtfs_rt_feed| match realtime_feed_id.as_str() {
                "f-amtrak~rt" => amtrak_gtfs_rt::filter_capital_corridor(gtfs_rt_feed),
                _ => gtfs_rt_feed,
            });

            let alerts_gtfs_rt = match alerts_dupe_trips {
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
                        .entry((realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
                        .and_modify(|gtfs_data| *gtfs_data = vehicles_gtfs_rt.clone())
                        .or_insert(vehicles_gtfs_rt.clone());
                }

                if let Some(trip_gtfs_rt) = trips_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry((realtime_feed_id.clone(), GtfsRtType::TripUpdates))
                        .and_modify(|gtfs_data| *gtfs_data = trip_gtfs_rt.clone())
                        .or_insert(trip_gtfs_rt.clone());
                }

                if let Some(alerts_gtfs_rt) = alerts_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry((realtime_feed_id.clone(), GtfsRtType::Alerts))
                        .and_modify(|gtfs_data| *gtfs_data = alerts_gtfs_rt.clone())
                        .or_insert(alerts_gtfs_rt.clone());
                }
            }

            let hash_data_duration = hash_data_start.elapsed();

            println!(
                "wrote {realtime_feed_id} in chateau {chateau_id}, took {} ms, is new data: {new_data}",
                hash_data_duration.as_millis()
            );

            //   println!("Saved FeedMessages for {}", realtime_feed_id);

            if new_data {
                let mut lock_chateau_queue = self.alpenrose_to_process_queue_chateaus.lock().await;

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
                        .entry((realtime_feed_id.clone(), GtfsRtType::VehiclePositions))
                        .and_modify(|gtfs_data| *gtfs_data = vehicles_gtfs_rt.clone())
                        .or_insert(vehicles_gtfs_rt.clone());
                }

                if let Some(trip_gtfs_rt) = trips_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry((realtime_feed_id.clone(), GtfsRtType::TripUpdates))
                        .and_modify(|gtfs_data| *gtfs_data = trip_gtfs_rt.clone())
                        .or_insert(trip_gtfs_rt.clone());
                }

                if let Some(alerts_gtfs_rt) = alerts_gtfs_rt {
                    self.authoritative_gtfs_rt_store
                        .entry((realtime_feed_id.clone(), GtfsRtType::Alerts))
                        .and_modify(|gtfs_data| *gtfs_data = alerts_gtfs_rt.clone())
                        .or_insert(alerts_gtfs_rt.clone());
                }
            }

            let hash_data_duration = hash_data_start.elapsed();

            println!(
                "wrote {realtime_feed_id} in chateau {chateau_id}, took {} ms, is new data: {new_data}",
                hash_data_duration.as_millis()
            );

            //   println!("Saved FeedMessages for {}", realtime_feed_id);

            if new_data {
                let mut lock_chateau_queue = self.alpenrose_to_process_queue_chateaus.lock().await;

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
    ) -> Option<GetVehicleLocationsResponse> {
        match self.authoritative_data_store.get(&chateau_id) {
            Some(aspenised_data) => {
                let aspenised_data = aspenised_data.get();

                let fast_hash_of_routes = catenary::fast_hash(
                    &aspenised_data
                        .vehicle_routes_cache
                        .values()
                        .collect::<Vec<&AspenisedVehicleRouteCache>>(),
                );

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
                    vehicle_positions: aspenised_data.vehicle_positions.clone(),
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
        match self.authoritative_data_store.get(&chateau_id) {
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
        match self.authoritative_data_store.get(&chateau_id) {
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

    async fn get_trip_updates_from_trip_id(
        self,
        _: context::Context,
        chateau_id: String,
        trip_id: String,
    ) -> Option<Vec<AspenisedTripUpdate>> {
        match self.authoritative_data_store.get(&chateau_id) {
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
        match self.authoritative_data_store.get(&chateau_id) {
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
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    console_subscriber::init();

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

    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), 40427);
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
            //10 seconds
            10,
            Some(etcd_client::LeaseGrantOptions::new().with_id(etcd_lease_id_for_this_worker)),
        )
        .await
        .expect("Failed to make lease with etcd");

    println!("Lease granted: {:?}", make_lease);

    //register that the worker exists

    let worker_metadata = AspenWorkerMetadataEtcd {
        etcd_lease_id: etcd_lease_id_for_this_worker,
        socket,
        worker_id: this_worker_id.to_string(),
    };

    let etcd_this_worker_assignment = etcd
        .put(
            format!("/aspen_workers/{}", this_worker_id).as_str(),
            bincode::serialize(&worker_metadata).unwrap(),
            Some(etcd_client::PutOptions::new().with_lease(etcd_lease_id_for_this_worker)),
        )
        .await?;

    let workers_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let chateau_list: Arc<Mutex<Option<ChateausLeaderHashMap>>> = Arc::new(Mutex::new(None));

    let process_from_alpenrose_queue = Arc::new(Injector::<ProcessAlpenroseData>::new());
    let raw_gtfs = Arc::new(SccHashMap::new());
    let authoritative_data_store = Arc::new(SccHashMap::new());
    let backup_data_store = Arc::new(SccHashMap::new());
    let backup_raw_gtfs = Arc::new(SccHashMap::new());
    let alpenrose_to_process_queue_chateaus = Arc::new(Mutex::new(HashSet::new()));
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

    let async_from_alpenrose_processor_handler: tokio::task::JoinHandle<
        Result<(), Box<dyn Error + Sync + Send>>,
    > = tokio::task::spawn(async_threads_alpenrose::alpenrose_process_threads(
        b_alpenrose_to_process_queue,
        b_authoritative_gtfs_rt_store,
        b_authoritative_data_store,
        b_conn_pool,
        b_thread_count,
        Arc::clone(&alpenrose_to_process_queue_chateaus),
        etcd_lease_id_for_this_worker,
    ));

    let etcd_lease_renewer: tokio::task::JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> =
        tokio::task::spawn({
            let etcd_addresses = etcd_addresses.clone();
            let arc_etcd_connect_options = arc_etcd_connect_options.clone();

            let this_worker_id = this_worker_id.clone();

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
                            println!("Lease renew successful");

                            let etcd_this_worker_assignment = etcd
                                .put(
                                    format!("/aspen_workers/{}", &this_worker_id).as_str(),
                                    bincode::serialize(&worker_metadata).unwrap(),
                                    Some(
                                        etcd_client::PutOptions::new()
                                            .with_lease(etcd_lease_id_for_this_worker),
                                    ),
                                )
                                .await?;
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Err(e) => {
                            println!("Error renewing lease: {:#?}", e);

                            let make_lease = etcd
                                .lease_grant(
                                    //10 seconds
                                    10,
                                    Some(
                                        etcd_client::LeaseGrantOptions::new()
                                            .with_id(etcd_lease_id_for_this_worker),
                                    ),
                                )
                                .await
                                .expect("Failed to make lease with etcd");

                            let etcd_this_worker_assignment = etcd
                                .put(
                                    format!("/aspen_workers/{}", this_worker_id).as_str(),
                                    bincode::serialize(&worker_metadata).unwrap(),
                                    Some(
                                        etcd_client::PutOptions::new()
                                            .with_lease(etcd_lease_id_for_this_worker),
                                    ),
                                )
                                .await?;
                        }
                    }
                }
                Ok(())
            }
        });

    let tarpc_server: tokio::task::JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> =
        tokio::task::spawn({
            println!("Listening on port {}", listener.local_addr().port());

            move || async move {
                listener
                    // Ignore accept errors.
                    .filter_map(|r| future::ready(r.ok()))
                    .map(server::BaseChannel::with_defaults)
                    .map(|channel| {
                        let server = AspenServer {
                            addr: channel.transport().peer_addr().unwrap(),
                            worker_id: Arc::clone(&this_worker_id),
                            authoritative_data_store: Arc::clone(&authoritative_data_store),
                            conn_pool: Arc::clone(&arc_conn_pool),
                            alpenrose_to_process_queue: Arc::clone(&process_from_alpenrose_queue),
                            authoritative_gtfs_rt_store: Arc::clone(&raw_gtfs),
                            backup_data_store: Arc::clone(&backup_data_store),
                            backup_gtfs_rt_store: Arc::clone(&backup_raw_gtfs),
                            alpenrose_to_process_queue_chateaus: Arc::clone(
                                &alpenrose_to_process_queue_chateaus,
                            ),
                            rough_hash_of_gtfs_rt: Arc::clone(&rough_hash_of_gtfs_rt),
                            hash_of_raw_gtfs_rt_protobuf: Arc::clone(&hash_of_raw_gtfs_rt_protobuf),
                            worker_etcd_lease_id: etcd_lease_id_for_this_worker,
                            etcd_addresses: Arc::clone(&etcd_addresses),
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

                Ok(())
            }
        }());

    async fn flatten<T>(
        handle: tokio::task::JoinHandle<Result<T, Box<dyn Error + Sync + Send>>>,
    ) -> Result<T, Box<dyn Error + Sync + Send>> {
        match handle.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn flatten_stopping_is_err<T>(
        handle: tokio::task::JoinHandle<Result<T, Box<dyn Error + Sync + Send>>>,
    ) -> Result<T, Box<dyn Error + Sync + Send>> {
        match handle.await {
            Ok(Ok(result)) => panic!("Stopping wasn't supposed to happen!"),
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

    let result_series = tokio::try_join!(
        //flatten_stopping_is_err(async_from_alpenrose_processor_handler),
        flatten_stopping_is_err(tarpc_server),
        flatten_stopping_is_err(etcd_lease_renewer)
    )
    .unwrap();

    panic!("IT WASNT SUPPOSED TO END");
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
                .entry(key.clone())
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
        Some(new_timestamp) => match server.timestamps_of_gtfs_rt.get(&key) {
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
