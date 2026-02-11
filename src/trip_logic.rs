use crate::EtcdConnectionIps;
use crate::SerializableStop;
use crate::aspen::lib::ChateauMetadataEtcd;
use crate::aspen::lib::connection_manager::AspenClientManager;
use crate::aspen_dataset::AspenisedTripScheduleRelationship;
use crate::aspen_dataset::AspenisedVehicleDescriptor;
use crate::aspen_dataset::AspenisedVehiclePosition;
use crate::aspen_dataset::{AspenStopTimeEvent, AspenisedTripModification};
use crate::aspen_dataset::{AspenisedAlert, AspenisedStop};
use crate::connections_lookup::connections_lookup;
use crate::models;
use crate::postgres_tools::CatenaryPostgresPool;
use crate::schema::gtfs::calendar as calendar_pg_schema;
use crate::schema::gtfs::calendar_dates as calendar_dates_pg_schema;
use crate::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use crate::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use crate::schema::gtfs::routes as routes_pg_schema;
use crate::schema::gtfs::stops as stops_pg_schema;
use crate::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use ahash::AHashMap;
use chrono::Datelike;
use chrono::TimeZone;
use chrono_tz::Tz;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel_async::RunQueryDsl;
use ecow::EcoString;
use futures::future::join_all;
use geo::HaversineDistance;
use geo::Point;
use geo::coord;
use gtfs_realtime::Alert;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tarpc::context;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ResponseForGtfsVehicle {
    pub found_data: bool,
    pub data: Option<Vec<AspenisedVehiclePosition>>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ResponseForGtfsRtRefresh {
    pub found_data: bool,
    pub data: Option<GtfsRtRefreshData>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GtfsRtRefreshData {
    pub stoptimes: Vec<StopTimeRefresh>,
    pub timestamp: Option<u64>,
    pub trip_id: Option<String>,
    pub chateau: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StopTimeRefresh {
    pub stop_id: Option<EcoString>,
    pub rt_arrival: Option<AspenStopTimeEvent>,
    pub rt_departure: Option<AspenStopTimeEvent>,
    pub schedule_relationship: Option<u8>,
    pub gtfs_stop_sequence: Option<u16>,
    pub rt_platform_string: Option<EcoString>,
    pub departure_occupancy_status: Option<u8>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TripIntroductionInformation {
    pub stoptimes: Vec<StopTimeIntroduction>,
    pub tz: Tz,
    pub block_id: Option<String>,
    pub bikes_allowed: i16,
    pub wheelchair_accessible: i16,
    pub has_frequencies: bool,
    pub route_id: String,
    pub trip_headsign: Option<String>,
    pub route_short_name: Option<String>,
    pub trip_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub route_type: i16,
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_ids_for_this_trip: Vec<String>,
    pub shape_polyline: Option<String>,
    pub trip_id_found_in_db: bool,
    pub service_date: Option<chrono::NaiveDate>,
    pub schedule_trip_exists: bool,
    pub rt_shape: bool,
    pub old_shape_polyline: Option<String>,
    pub cancelled_stoptimes: Vec<StopTimeIntroduction>,
    pub is_cancelled: bool,
    pub deleted: bool,
    pub connecting_routes: Option<BTreeMap<String, BTreeMap<String, crate::models::Route>>>, // chateau -> route_id -> Route
    pub connections_per_stop: Option<BTreeMap<String, BTreeMap<String, Vec<String>>>>, // stop_id -> chateau -> route_ids
    pub trip_id: Option<String>,
    pub chateau: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StopTimeIntroduction {
    pub stop_id: CompactString,
    pub name: Option<String>,
    pub translations: Option<BTreeMap<String, String>>,
    pub platform_code: Option<String>,
    pub rt_platform_string: Option<String>,
    pub timezone: Option<Tz>,
    pub code: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub scheduled_arrival_time_unix_seconds: Option<u64>,
    pub scheduled_departure_time_unix_seconds: Option<u64>,
    pub rt_arrival: Option<AspenStopTimeEvent>,
    pub rt_departure: Option<AspenStopTimeEvent>,
    pub schedule_relationship: Option<u8>,
    pub gtfs_stop_sequence: Option<u16>,
    pub interpolated_stoptime_unix_seconds: Option<u64>,
    pub timepoint: Option<bool>,
    pub replaced_stop: bool,
    pub osm_station_id: Option<i64>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Hash, Eq, PartialEq)]
pub struct QueryTripInformationParams {
    pub trip_id: String,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub route_id: Option<String>,
}

pub enum StopPostgresOrAspen {
    Postgres(crate::models::Stop),
    Aspen(AspenisedStop),
}

pub struct AlertOutput {
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_ids_for_this_trip: Vec<String>,
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
}

pub async fn get_alert_single_trip(
    aspen_client: &crate::aspen::lib::AspenRpcClient,
    chateau: String,
    trip_id: String,
    route_id: String,
    stops: Vec<String>,
) -> Result<AlertOutput, Box<dyn std::error::Error + Sync + Send>> {
    let alerts_for_route = aspen_client
        .get_alerts_from_route_id(context::current(), chateau.clone(), route_id.clone())
        .await;

    let alerts_for_trip = aspen_client
        .get_alert_from_trip_id(context::current(), chateau.clone(), trip_id.clone())
        .await;

    let mut alert_id_to_alert: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
    let mut alert_ids_for_this_route: Vec<String> = vec![];
    let mut alert_ids_for_this_trip: Vec<String> = vec![];
    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();

    if let Ok(Some(alerts_for_route)) = alerts_for_route {
        for (alert_id, alert) in alerts_for_route {
            let relevant = alert.informed_entity.iter().any(|entity| {
                let route_id_covered = match &entity.route_id {
                    None => true,
                    Some(route_id_entity) => route_id_entity == &route_id,
                };
                let trip_covered = match &entity.trip {
                    None => true,
                    Some(trip) => match &trip.trip_id {
                        None => true,
                        Some(entity_trip_id) => entity_trip_id == &trip_id,
                    },
                };
                route_id_covered && trip_covered
            });

            if relevant {
                alert_id_to_alert.insert(alert_id.clone(), alert.clone());
                alert_ids_for_this_route.push(alert_id.clone());
            }
        }
    }

    if let Ok(Some(alerts_for_trip)) = alerts_for_trip {
        for (alert_id, alert) in alerts_for_trip {
            let relevant = alert.informed_entity.iter().any(|entity| {
                let route_id_covered = match &entity.route_id {
                    None => true,
                    Some(route_id_entity) => route_id_entity == &route_id,
                };
                let trip_covered = match &entity.trip {
                    None => true,
                    Some(trip) => match &trip.trip_id {
                        None => true,
                        Some(entity_trip_id) => entity_trip_id == &trip_id,
                    },
                };
                route_id_covered && trip_covered
            });

            if relevant {
                alert_id_to_alert.insert(alert_id.clone(), alert.clone());
                alert_ids_for_this_trip.push(alert_id.clone());
            }
        }
    }

    let alerts_for_stops = aspen_client
        .get_alert_from_stop_ids(context::current(), chateau.clone(), stops)
        .await;

    if let Ok(Some(alerts_for_stops)) = alerts_for_stops {
        let relevant_alert_ids = alerts_for_stops
            .alerts
            .iter()
            .filter(|(alert_id, alert)| {
                alert.informed_entity.iter().any(|entity| {
                    let route_id_covered = match &entity.route_id {
                        None => true,
                        Some(route_id_entity) => route_id_entity == &route_id,
                    };
                    let trip_covered = match &entity.trip {
                        None => true,
                        Some(trip) => match &trip.trip_id {
                            None => true,
                            Some(entity_trip_id) => entity_trip_id == &trip_id,
                        },
                    };
                    route_id_covered && trip_covered
                })
            })
            .map(|(alert_id, _)| alert_id.clone())
            .collect::<BTreeSet<_>>();

        for (alert_id, alerts) in alerts_for_stops.alerts {
            if relevant_alert_ids.contains(&alert_id) {
                alert_id_to_alert.insert(alert_id.clone(), alerts.clone());
            }
        }

        for (stop_id, alert_ids) in alerts_for_stops.stops_to_alert_ids {
            stop_id_to_alert_ids.insert(
                stop_id.clone(),
                alert_ids
                    .iter()
                    .filter(|alert_id| relevant_alert_ids.contains(alert_id.as_str()))
                    .cloned()
                    .collect::<Vec<_>>(),
            );
        }
    }

    Ok(AlertOutput {
        alert_id_to_alert,
        alert_ids_for_this_route,
        alert_ids_for_this_trip,
        stop_id_to_alert_ids,
    })
}

pub async fn fetch_trip_rt_update(
    chateau: String,
    query: QueryTripInformationParams,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
) -> Result<ResponseForGtfsRtRefresh, String> {
    if chateau == "irvine~ca~us" {
        println!(
            "DEBUG: fetch_trip_rt_update called for trip {}",
            query.trip_id
        );
    }
    let etcd =
        crate::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser).await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);
        return Err("Could not connect to etcd".to_string());
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau).as_str(),
            None,
        )
        .await;

    match fetch_assigned_node_for_this_chateau {
        Ok(fetch_assigned_node_for_this_chateau) => {
            let fetch_assigned_node_for_this_chateau_kv_first =
                fetch_assigned_node_for_this_chateau.kvs().first();

            if let Some(fetch_assigned_node_for_this_chateau_data) =
                fetch_assigned_node_for_this_chateau_kv_first
            {
                if chateau == "irvine~ca~us" {
                    println!("DEBUG: fetch_trip_rt_update: Found assigned chateau node via etcd");
                }
                let assigned_chateau_data = crate::bincode_deserialize::<ChateauMetadataEtcd>(
                    fetch_assigned_node_for_this_chateau_data.value(),
                )
                .unwrap();

                let socket = assigned_chateau_data.socket;
                if chateau == "irvine~ca~us" {
                    println!(
                        "DEBUG: fetch_trip_rt_update: Connecting to aspen at {}",
                        socket
                    );
                }
                let aspen_client = if let Some(client) =
                    aspen_client_manager.get_client(socket).await
                {
                    Ok(client)
                } else {
                    let client_res = crate::aspen::lib::spawn_aspen_client_from_ip(&socket).await;
                    if let Ok(client) = &client_res {
                        aspen_client_manager
                            .insert_client(socket, client.clone())
                            .await;
                    }
                    client_res
                };

                match aspen_client {
                    Ok(aspen_client) => {
                        if chateau == "irvine~ca~us" {
                            println!(
                                "DEBUG: fetch_trip_rt_update: Got aspen client, fetching trip updates"
                            );
                        }
                        let get_trip = aspen_client
                            .get_trip_updates_from_trip_id(
                                context::current(),
                                chateau.clone(),
                                query.trip_id.clone(),
                            )
                            .await;

                        match get_trip {
                            Ok(Some(get_trip)) => {
                                if chateau == "irvine~ca~us" {
                                    println!(
                                        "DEBUG: fetch_trip_rt_update: received {} trip options from aspen",
                                        get_trip.len()
                                    );
                                }
                                println!("recieved {} trip options from aspen", get_trip.len());
                                if !get_trip.is_empty() {
                                    let rt_trip_update = match get_trip.len() {
                                        1 => &get_trip[0],
                                        _ => {
                                            if chateau == "irvine~ca~us" {
                                                println!(
                                                    "DEBUG: fetch_trip_rt_update: Multiple trip updates found, filtering..."
                                                );
                                            }
                                            println!(
                                                "Multiple trip updates found for trip id {} {}",
                                                chateau, query.trip_id
                                            );
                                            let query_start_date_parsed =
                                                query.start_date.as_ref().and_then(|date_str| {
                                                    chrono::NaiveDate::parse_from_str(
                                                        date_str, "%Y%m%d",
                                                    )
                                                    .or_else(|_| {
                                                        chrono::NaiveDate::parse_from_str(
                                                            date_str, "%Y-%m-%d",
                                                        )
                                                    })
                                                    .ok()
                                                });

                                            let find_trip = get_trip.iter().find(|each_update| {
                                                let start_time_matches = match (
                                                    &query.start_time,
                                                    &each_update.trip.start_time,
                                                ) {
                                                    (Some(q_time), Some(u_time)) => {
                                                        q_time == u_time
                                                    }
                                                    (None, _) => true,
                                                    (Some(_), None) => false,
                                                };

                                                let start_date_matches = match (
                                                    &query_start_date_parsed,
                                                    &each_update.trip.start_date,
                                                ) {
                                                    (Some(q_date), Some(u_date)) => {
                                                        q_date == u_date
                                                    }
                                                    (None, _) => true,
                                                    (Some(_), None) => false,
                                                };

                                                start_time_matches && start_date_matches
                                            });

                                            match find_trip {
                                                Some(find_trip) => find_trip,
                                                None => &get_trip[0],
                                            }
                                        }
                                    };

                                    println!(
                                        "rt data contains {} stop updates",
                                        rt_trip_update.stop_time_update.len()
                                    );

                                    let stop_data: Vec<StopTimeRefresh> = rt_trip_update
                                        .stop_time_update
                                        .iter()
                                        .map(|stop_time_update| StopTimeRefresh {
                                            stop_id: stop_time_update.stop_id.clone(),
                                            rt_arrival: stop_time_update.arrival.clone(),
                                            rt_departure: stop_time_update.departure.clone(),
                                            schedule_relationship: stop_time_update
                                                .schedule_relationship
                                                .as_ref()
                                                .map(|x| x.clone().into()),
                                            gtfs_stop_sequence: stop_time_update
                                                .stop_sequence
                                                .map(|x| x as u16),
                                            rt_platform_string: stop_time_update
                                                .platform_string
                                                .clone(),
                                            departure_occupancy_status: stop_time_update
                                                .departure_occupancy_status
                                                .as_ref()
                                                .map(|x| {
                                                    crate::aspen_dataset::occupancy_status_to_u8(&x)
                                                }),
                                        })
                                        .collect();

                                    Ok(ResponseForGtfsRtRefresh {
                                        found_data: true,
                                        data: Some(GtfsRtRefreshData {
                                            stoptimes: stop_data,
                                            timestamp: rt_trip_update.timestamp,
                                            trip_id: Some(query.trip_id.clone()),
                                            chateau: Some(chateau.clone()),
                                        }),
                                    })
                                } else {
                                    if chateau == "irvine~ca~us" {
                                        println!(
                                            "DEBUG: fetch_trip_rt_update: get_trip response was empty list"
                                        );
                                    }
                                    Ok(ResponseForGtfsRtRefresh {
                                        found_data: false,
                                        data: None,
                                    })
                                }
                            }
                            _ => {
                                if chateau == "irvine~ca~us" {
                                    println!(
                                        "DEBUG: fetch_trip_rt_update: get_trip failed or returned None. Is Error: {}, Is None: {}",
                                        get_trip.is_err(),
                                        get_trip.as_ref().map(|x| x.is_none()).unwrap_or(false)
                                    );
                                }
                                Ok(ResponseForGtfsRtRefresh {
                                    found_data: false,
                                    data: None,
                                })
                            }
                        }
                    }
                    _ => {
                        if chateau == "irvine~ca~us" {
                            println!(
                                "DEBUG: fetch_trip_rt_update: Failed to connect to realtime server (aspen_client error)"
                            );
                        }
                        Err("Could not connect to realtime data server".to_string())
                    }
                }
            } else {
                if chateau == "irvine~ca~us" {
                    println!("DEBUG: fetch_trip_rt_update: No assigned chateau data found in etcd");
                }
                Err("Could not connect to realtime data server".to_string())
            }
        }
        _ => {
            if chateau == "irvine~ca~us" {
                println!("DEBUG: fetch_trip_rt_update: Failed to fetch assigned node from etcd");
            }
            Err("Could not connect to etcd".to_string())
        }
    }
}

pub async fn fetch_trip_information(
    chateau: String,
    query: QueryTripInformationParams,
    pool: Arc<CatenaryPostgresPool>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    mut timer: Option<&mut simple_server_timing_header::Timer>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
) -> Result<TripIntroductionInformation, String> {
    if chateau == "irvine~ca~us" {
        println!(
            "DEBUG: fetch_trip_information called for trip {}",
            query.trip_id
        );
    }
    if let Some(t) = &mut timer {
        t.add("open_pg_connection");
    }
    let mut rt_shape = false;

    // connect to pool
    let pool_ref = pool.as_ref();
    let conn_pre = pool_ref.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return Err("Error connecting to database".to_string());
    }

    let conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    > = &mut conn_pre.unwrap();

    let conn_pre2 = pool_ref.get().await;
    let conn_pre3 = pool_ref.get().await;

    let mut conn2: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    > = &mut conn_pre2.unwrap();
    let mut conn3: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    > = &mut conn_pre3.unwrap();

    if let Some(t) = &mut timer {
        t.add("query_compressed_trip");
    }

    //ask postgres first
    let trip_compressed = trips_compressed_pg_schema::dsl::trips_compressed
        .filter(trips_compressed_pg_schema::dsl::chateau.eq(&chateau))
        .filter(trips_compressed_pg_schema::dsl::trip_id.eq(&query.trip_id))
        .select(crate::models::CompressedTrip::as_select())
        .load(conn)
        .await;

    if let Err(trip_compressed_err) = &trip_compressed {
        eprintln!("{}", trip_compressed_err);
        return Err("Error fetching trip compressed".to_string());
    }

    let trip_compressed: Vec<crate::models::CompressedTrip> = trip_compressed.unwrap();

    if chateau == "irvine~ca~us" {
        println!(
            "DEBUG: fetch_trip_information: Trip compressed found count: {}",
            trip_compressed.len()
        );
    }

    if let Some(t) = &mut timer {
        t.add("connect_to_etcd");
    }

    let etcd =
        crate::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser).await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);
        return Err("Could not connect to etcd".to_string());
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau).as_str(),
            None,
        )
        .await;

    if let Some(t) = &mut timer {
        t.add("fetch_assigned_aspen_chateau_data_from_etcd");
    }

    // RT ONLY TRIP LOGIC
    if trip_compressed.is_empty() {
        if chateau == "irvine~ca~us" {
            println!(
                "DEBUG: fetch_trip_information: Trip compressed empty, attempting RT ONLY TRIP LOGIC"
            );
        }
        let fetch_assigned_node_for_this_chateau_kv_first = fetch_assigned_node_for_this_chateau
            .as_ref()
            .ok()
            .map(|x| x.kvs().first())
            .flatten();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = crate::bincode_deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let socket = assigned_chateau_data.socket;
            let aspen_client = if let Some(client) = aspen_client_manager.get_client(socket).await {
                Ok(client)
            } else {
                let client_res = crate::aspen::lib::spawn_aspen_client_from_ip(&socket).await;
                if let Ok(client) = &client_res {
                    aspen_client_manager
                        .insert_client(socket, client.clone())
                        .await;
                }
                client_res
            };

            if let Err(aspen_client_err) = &aspen_client {
                if chateau == "irvine~ca~us" {
                    println!("DEBUG: fetch_trip_information: RT ONLY - Failed to connect to aspen");
                }
                eprintln!("{:#?}", aspen_client_err);
                return Err("Could not connect to realtime data server".to_string());
            }

            let aspen_client = aspen_client.unwrap();

            if chateau == "irvine~ca~us" {
                println!(
                    "DEBUG: fetch_trip_information: RT ONLY - Connected to aspen, fetching trip updates"
                );
            }

            let get_trips = aspen_client
                .get_trip_updates_from_trip_id(
                    context::current(),
                    chateau.clone(),
                    query.trip_id.clone(),
                )
                .await;

            if let Ok(get_trips) = get_trips {
                if let Some(get_trip) = get_trips {
                    if get_trip.is_empty() {
                        return Err("Trip not found in rt database".to_string());
                    }

                    let trip = &get_trip[0];

                    let stop_ids_to_lookup: Vec<String> = trip
                        .stop_time_update
                        .iter()
                        .map(|y| y.stop_id.clone())
                        .map(|x| x.map(|x| x.as_str().into()))
                        .flatten()
                        .collect::<Vec<_>>();

                    let stops_data = stops_pg_schema::dsl::stops
                        .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
                        .filter(stops_pg_schema::dsl::gtfs_id.eq_any(&stop_ids_to_lookup))
                        .select(crate::models::Stop::as_select())
                        .load(conn)
                        .await;

                    if let Err(stops_data_err) = &stops_data {
                        eprintln!("{}", stops_data_err);
                        return Err("Error fetching stop data from postgres".to_string());
                    }

                    let stops_data: Vec<crate::models::Stop> = stops_data.unwrap();

                    let stops_hashmap = stops_data
                        .into_iter()
                        .map(|x| (x.gtfs_id.clone(), x))
                        .collect::<BTreeMap<_, _>>();

                    let route_id = trip.trip.route_id.clone().unwrap_or_default();

                    let alerts_response = get_alert_single_trip(
                        &aspen_client,
                        chateau.clone(),
                        query.trip_id.clone(),
                        route_id.clone(),
                        stop_ids_to_lookup.clone(),
                    )
                    .await;

                    let mut alert_id_to_alert: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
                    let mut alert_ids_for_this_route: Vec<String> = vec![];
                    let mut alert_ids_for_this_trip: Vec<String> = vec![];
                    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();

                    if let Ok(alerts_response) = alerts_response {
                        alert_id_to_alert = alerts_response.alert_id_to_alert;
                        alert_ids_for_this_route = alerts_response.alert_ids_for_this_route;
                        alert_ids_for_this_trip = alerts_response.alert_ids_for_this_trip;
                        stop_id_to_alert_ids = alerts_response.stop_id_to_alert_ids;
                    } else {
                        eprintln!("Error fetching alerts from aspen");
                    }

                    let stop_times: Vec<StopTimeIntroduction> = trip
                        .stop_time_update
                        .iter()
                        .filter(|x| x.stop_id.is_some())
                        .enumerate()
                        .map(|(i, stu)| {
                            let stop = stops_hashmap.get((&stu.stop_id).as_ref().unwrap().as_str());

                            StopTimeIntroduction {
                                stop_id: stu
                                    .stop_id
                                    .as_ref()
                                    .map(|x| x.to_string().into())
                                    .unwrap(),
                                name: stop.map(|x| x.name.clone()).flatten(),
                                translations: None,
                                rt_platform_string: None,
                                platform_code: None,
                                code: stop.as_ref().map(|x| x.code.clone()).flatten(),
                                gtfs_stop_sequence: stu.stop_sequence.map(|x| x as u16),
                                timezone: stop
                                    .map(|x| {
                                        x.timezone.as_ref().map(|tz_str| {
                                            chrono_tz::Tz::from_str_insensitive(tz_str.as_str())
                                                .unwrap()
                                        })
                                    })
                                    .flatten(),
                                longitude: stop.map(|x| x.point.map(|p| p.x)).flatten(),
                                latitude: stop.map(|x| x.point.map(|p| p.y)).flatten(),
                                scheduled_arrival_time_unix_seconds: None,
                                scheduled_departure_time_unix_seconds: None,
                                rt_arrival: stu.arrival.clone(),
                                rt_departure: stu.departure.clone(),
                                schedule_relationship: stu
                                    .schedule_relationship
                                    .as_ref()
                                    .map(|x| x.clone().into()),
                                interpolated_stoptime_unix_seconds: None,
                                timepoint: Some(false),
                                replaced_stop: false,
                                osm_station_id: stop.map(|x| x.osm_station_id).flatten(),
                            }
                        })
                        .collect::<Vec<_>>();

                    let last_stop_name = stop_times
                        .iter()
                        .filter(|stu| stu.schedule_relationship != Some(1))
                        .last()
                        .map(|x| x.name.clone())
                        .flatten();

                    let route_query = routes_pg_schema::dsl::routes
                        .filter(routes_pg_schema::dsl::chateau.eq(&chateau))
                        .filter(routes_pg_schema::dsl::route_id.eq(route_id.as_str()))
                        .select(crate::models::Route::as_select())
                        .load(conn)
                        .await;

                    if let Err(route_err) = &route_query {
                        eprintln!("{}", route_err);
                        return Err("Error fetching route data".to_string());
                    }
                    let route_vec = route_query.unwrap();
                    if route_vec.is_empty() {
                        return Err("Route not found".to_string());
                    }
                    let route = &route_vec[0];

                    let agency = crate::schema::gtfs::agencies::dsl::agencies
                        .filter(crate::schema::gtfs::agencies::dsl::chateau.eq(&chateau))
                        .select(crate::models::Agency::as_select())
                        .load(conn)
                        .await;

                    let tz = match agency {
                        Ok(agency) => {
                            if !agency.is_empty() {
                                let agency: &crate::models::Agency = &agency[0];
                                chrono_tz::Tz::from_str_insensitive(agency.agency_timezone.as_str())
                                    .unwrap_or(chrono_tz::UTC)
                            } else {
                                chrono_tz::UTC
                            }
                        }
                        _ => chrono_tz::UTC,
                    };

                    let mut shape_polyline = None;

                    if let Some(trip_properties) = &trip.trip_properties {
                        if let Some(shape_id) = &trip_properties.shape_id {
                            let shape_response = aspen_client
                                .get_shape(context::current(), chateau.clone(), shape_id.clone())
                                .await;

                            if let Ok(Some(shape_response)) = shape_response {
                                shape_polyline = Some(shape_response);
                                // rt_shape = true;
                            }
                        }
                    }

                    let response = TripIntroductionInformation {
                        stoptimes: stop_times,
                        tz: tz,
                        block_id: None,
                        bikes_allowed: 0,
                        wheelchair_accessible: 0,
                        has_frequencies: false,
                        route_id: route.route_id.clone(),
                        trip_headsign: last_stop_name,
                        route_short_name: route.short_name.clone(),
                        trip_short_name: None,
                        route_long_name: route.long_name.clone(),
                        color: route.color.clone(),
                        text_color: route.text_color.clone(),
                        vehicle: None,
                        route_type: route.route_type as i16,
                        stop_id_to_alert_ids: stop_id_to_alert_ids,
                        alert_id_to_alert: alert_id_to_alert,
                        alert_ids_for_this_route: alert_ids_for_this_route,
                        alert_ids_for_this_trip: alert_ids_for_this_trip,
                        shape_polyline: None,
                        trip_id_found_in_db: false,
                        service_date: query
                            .start_date
                            .clone()
                            .map(|x| {
                                let date = chrono::NaiveDate::parse_from_str(&x, "%Y%m%d")
                                    .or_else(|_| chrono::NaiveDate::parse_from_str(&x, "%Y-%m-%d"));
                                match date {
                                    Ok(date) => Some(date),
                                    Err(_) => None,
                                }
                            })
                            .flatten(),
                        schedule_trip_exists: false,
                        rt_shape: false,
                        old_shape_polyline: None,
                        cancelled_stoptimes: vec![],
                        deleted: false,
                        is_cancelled: false,
                        connecting_routes: None,
                        connections_per_stop: None,
                        trip_id: Some(query.trip_id.clone()),
                        chateau: Some(chateau.clone()),
                    };

                    return Ok(response);
                }
            }
        }
        if chateau == "irvine~ca~us" {
            println!("DEBUG: fetch_trip_information: RT ONLY - Failed to find trip in RT database");
        }
        return Err("Compressed trip not found and realtime lookup failed".to_string());
    }

    // STATIC DB FETCHING

    if chateau == "irvine~ca~us" {
        println!("DEBUG: fetch_trip_information: Proceeding with STATIC DB FETCHING");
    }

    let trip_compressed = trip_compressed[0].clone();

    let (itin_meta, itin_rows, route, calendar_req, calendar_dates) = futures::join!(
        itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_meta
            .filter(itinerary_pattern_meta_pg_schema::dsl::chateau.eq(&chateau))
            .filter(
                itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_id
                    .eq(&trip_compressed.itinerary_pattern_id),
            )
            .select(crate::models::ItineraryPatternMeta::as_select())
            .load(conn),
        itinerary_pattern_pg_schema::dsl::itinerary_pattern
            .filter(itinerary_pattern_pg_schema::dsl::chateau.eq(&chateau))
            .filter(
                itinerary_pattern_pg_schema::dsl::itinerary_pattern_id
                    .eq(&trip_compressed.itinerary_pattern_id),
            )
            .select(crate::models::ItineraryPatternRow::as_select())
            .load(conn2),
        routes_pg_schema::dsl::routes
            .filter(routes_pg_schema::dsl::chateau.eq(&chateau))
            .filter(routes_pg_schema::dsl::route_id.eq(&trip_compressed.route_id))
            .select(crate::models::Route::as_select())
            .load(conn3),
        calendar_pg_schema::dsl::calendar
            .filter(calendar_pg_schema::dsl::chateau.eq(&chateau))
            .filter(calendar_pg_schema::dsl::service_id.eq(&trip_compressed.service_id))
            .select(crate::models::Calendar::as_select())
            .load(conn),
        calendar_dates_pg_schema::dsl::calendar_dates
            .filter(calendar_dates_pg_schema::dsl::chateau.eq(&chateau))
            .filter(calendar_dates_pg_schema::dsl::service_id.eq(&trip_compressed.service_id))
            .select(crate::models::CalendarDate::as_select())
            .load(conn),
    );

    if let Some(t) = &mut timer {
        t.add("query_itin_route_and_itin_rows");
    }

    if let Err(route_err) = &route {
        eprintln!("{}", route_err);
        return Err("Error fetching route data".to_string());
    }

    let route: Vec<crate::models::Route> = route.unwrap();

    if route.is_empty() {
        return Err("Route not found".to_string());
    }

    let route = route[0].clone();

    if let Err(itin_meta) = &itin_meta {
        eprintln!("{}", itin_meta);
        return Err("Error fetching itinerary pattern metadata".to_string());
    }

    let itin_meta = itin_meta.unwrap();

    if let Err(itin_rows_err) = &itin_rows {
        eprintln!("{}", itin_rows_err);
        return Err("Error fetching itinerary pattern rows".to_string());
    }

    let itin_rows = itin_rows.unwrap();

    if itin_meta.is_empty() {
        return Err("Trip Itin not found".to_string());
    }

    if calendar_req.is_err() {
        eprintln!("{}", calendar_req.unwrap_err());
        return Err("Error fetching calendar data".to_string());
    }

    let calendar_rows = calendar_req.unwrap();

    if calendar_dates.is_err() {
        eprintln!("{}", calendar_dates.unwrap_err());
        return Err("Error fetching calendar dates data".to_string());
    }

    let calendar_dates = calendar_dates.unwrap();

    let itin_meta: crate::models::ItineraryPatternMeta = itin_meta[0].clone();

    let mut itin_rows_to_use: Vec<crate::models::ItineraryPatternRow> = itin_rows
        .into_iter()
        .filter(|row| itin_meta.attempt_id == row.attempt_id)
        .collect::<Vec<_>>();

    itin_rows_to_use.sort_by_key(|x| x.stop_sequence);

    // query both at the same time
    let stop_ids_to_lookup: Vec<String> = itin_rows_to_use
        .iter()
        .map(|x| x.stop_id.clone().into())
        .collect();

    let (stops_data, shape_lookup): (
        Result<Vec<crate::models::Stop>, diesel::result::Error>,
        Option<crate::models::Shape>,
    ) = futures::join!(
        stops_pg_schema::dsl::stops
            .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
            .filter(stops_pg_schema::dsl::gtfs_id.eq_any(&stop_ids_to_lookup))
            .select(crate::models::Stop::as_select())
            .load(conn),
        async {
            match itin_meta.shape_id {
                Some(shape_id_to_lookup) => {
                    let shape_query = crate::schema::gtfs::shapes::dsl::shapes
                        .filter(crate::schema::gtfs::shapes::dsl::shape_id.eq(shape_id_to_lookup))
                        .filter(crate::schema::gtfs::shapes::dsl::chateau.eq(&chateau))
                        .select(crate::models::Shape::as_select())
                        .first(conn)
                        .await;

                    match shape_query {
                        Ok(shape_query) => Some(shape_query.clone()),
                        Err(_) => None,
                    }
                }
                None => None,
            }
        }
    );

    if let Some(t) = &mut timer {
        t.add("query_stops_and_shape");
    }

    let mut shape_polyline = shape_lookup.map(|shape_info| {
        polyline::encode_coordinates(
            geo::LineString::new(
                shape_info
                    .linestring
                    .points
                    .iter()
                    .map(|point| {
                        coord! {
                            x: point.x,
                            y: point.y
                        }
                    })
                    .collect::<Vec<_>>(),
            ),
            5,
        )
        .unwrap()
    });

    let mut old_shape_polyline: Option<String> = None;

    if let Some(t) = &mut timer {
        t.add("convert_polyline");
    }

    let tz = chrono_tz::Tz::from_str_insensitive(itin_meta.timezone.as_str());

    if let Err(_) = &tz {
        eprintln!("Could not parse timezone {}", itin_meta.timezone.as_str());
        return Err(format!(
            "Could not parse timezone {} from itinerary {}",
            itin_meta.timezone, itin_meta.itinerary_pattern_id
        ));
    }

    let timezone = tz.unwrap();

    let mut is_cancelled = false;
    let mut deleted = false;

    if let Err(stops_data_err) = &stops_data {
        eprintln!("{}", stops_data_err);
        return Err("Error fetching stops data".to_string());
    }

    let stops_data = stops_data.unwrap();

    let mut stops_data_map: BTreeMap<String, crate::models::Stop> = BTreeMap::new();

    for stop in stops_data {
        stops_data_map.insert(stop.gtfs_id.clone(), stop);
    }

    let mut stop_times_for_this_trip: Vec<StopTimeIntroduction> = vec![];

    let mut alert_id_to_alert: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
    let mut alert_ids_for_this_route: Vec<String> = vec![];
    let mut alert_ids_for_this_trip: Vec<String> = vec![];

    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let added_seconds_to_ref_midnight = match &query.start_time {
        Some(start_time) => {
            let start_time_seconds = crate::convert_hhmmss_to_seconds(start_time);

            if start_time_seconds.is_none() {
                eprintln!("Invalid start time");
                return Err("Invalid start time".to_string());
            }

            start_time_seconds.unwrap() as i64
        }
        None => trip_compressed.start_time as i64,
    };

    //map start date to a YYYY, MM, DD format
    let start_naive_date = if let Some(start_date) = &query.start_date {
        let start_date = chrono::NaiveDate::parse_from_str(start_date, "%Y%m%d")
            .or_else(|_| chrono::NaiveDate::parse_from_str(start_date, "%Y-%m-%d"));

        if let Err(start_date_err) = start_date {
            eprintln!("{}", start_date_err);
            return Err("Invalid start date".to_string());
        }

        start_date.unwrap()
    } else {
        // make an array from 7 days in the past to 7 days in the future, chrono naive days

        let now = chrono::Utc::now();
        let now = timezone.from_utc_datetime(&now.naive_utc());

        let now_date = now.date_naive();

        let start_date_iter = now_date - chrono::Duration::days(10);

        let vec_service_dates = start_date_iter
            .iter_days()
            .take(20)
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|date| {
                let day_of_week = date.weekday();

                let mut service_active = false;

                for calendar in &calendar_rows {
                    if calendar.gtfs_start_date <= *date && date <= &(calendar.gtfs_end_date) {
                        service_active = match day_of_week {
                            chrono::Weekday::Mon => calendar.monday,
                            chrono::Weekday::Tue => calendar.tuesday,
                            chrono::Weekday::Wed => calendar.wednesday,
                            chrono::Weekday::Thu => calendar.thursday,
                            chrono::Weekday::Fri => calendar.friday,
                            chrono::Weekday::Sat => calendar.saturday,
                            chrono::Weekday::Sun => calendar.sunday,
                        };
                    }
                }

                let find_calendar_date = calendar_dates
                    .iter()
                    .find(|calendar_date| calendar_date.gtfs_date == *date);

                if let Some(find_calendar_date) = find_calendar_date {
                    service_active = match find_calendar_date.exception_type {
                        1 => true,
                        2 => false,
                        _ => service_active,
                    }
                }

                service_active
            })
            .map(|date| {
                let reference_time_noon = chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap();
                let noon_on_start_date = chrono::NaiveDateTime::new(date, reference_time_noon);
                let noon_on_start_date_with_tz =
                    timezone.from_local_datetime(&noon_on_start_date).unwrap();

                //reference time is 12 hours before noon
                let reference_time = noon_on_start_date_with_tz - chrono::Duration::hours(12);

                let start_time =
                    reference_time + chrono::Duration::seconds(added_seconds_to_ref_midnight);

                let time_diff_from_now = start_time.signed_duration_since(now);

                (date, start_time, time_diff_from_now)
            })
            .collect::<Vec<_>>();

        let mut vec_service_dates = vec_service_dates;

        vec_service_dates.sort_by_key(|x| x.2.abs().num_seconds());

        if vec_service_dates.is_empty() {
            if chateau == "irvine~ca~us" {
                println!("DEBUG: fetch_trip_information: No service date found for trip");
            }
            // Fallback or Error? User Logic calls it "Trip not found" or "No service date found"
            // But if we are here we have static data.
            return Err("No service date found for this trip".to_string());
        }

        let (start_naive_date, _, _) = vec_service_dates[0];

        if chateau == "irvine~ca~us" {
            println!(
                "DEBUG: fetch_trip_information: Selected service date: {}",
                start_naive_date
            );
        }

        start_naive_date
    };

    // get reference time as 12 hours before noon of the starting date
    let reference_time_noon = chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap();

    let noon_on_start_date = chrono::NaiveDateTime::new(start_naive_date, reference_time_noon);
    let noon_on_start_date_with_tz = timezone.from_local_datetime(&noon_on_start_date).unwrap();

    //reference time is 12 hours before noon
    let reference_time = noon_on_start_date_with_tz - chrono::Duration::hours(12);

    //calculate start of the trip time

    let start_of_trip_datetime =
        reference_time + chrono::Duration::seconds(added_seconds_to_ref_midnight);

    for row in itin_rows_to_use {
        let stop = stops_data_map.get(row.stop_id.as_str());

        if stop.is_none() {
            eprintln!("Stop {} not found", row.stop_id);
            continue;
        }

        let stop = stop.unwrap();

        let stop_time = StopTimeIntroduction {
            stop_id: (&stop.gtfs_id).into(),
            name: stop.name.clone(),
            translations: None,
            platform_code: stop.platform_code.clone(),
            osm_station_id: stop.osm_station_id,
            timezone: match stop.timezone.as_ref() {
                Some(tz) => match chrono_tz::Tz::from_str_insensitive(tz) {
                    Ok(tz) => Some(tz),
                    Err(_) => None,
                },
                None => None,
            },
            code: stop.code.clone(),
            longitude: stop.point.map(|point| point.x),
            latitude: stop.point.map(|point| point.y),
            scheduled_arrival_time_unix_seconds: row.arrival_time_since_start.map(
                |arrival_time_since_start| {
                    start_of_trip_datetime.timestamp() as u64 + arrival_time_since_start as u64
                },
            ),
            scheduled_departure_time_unix_seconds: row.departure_time_since_start.map(
                |departure_time_since_start| {
                    start_of_trip_datetime.timestamp() as u64 + departure_time_since_start as u64
                },
            ),
            interpolated_stoptime_unix_seconds: row.interpolated_time_since_start.map(
                |interpolated_time_since_start| {
                    start_of_trip_datetime.timestamp() as u64 + interpolated_time_since_start as u64
                },
            ),
            gtfs_stop_sequence: Some(row.gtfs_stop_sequence as u16),
            rt_arrival: None,
            rt_departure: None,
            schedule_relationship: None,
            rt_platform_string: None,
            timepoint: row.timepoint,
            replaced_stop: false,
        };

        stop_times_for_this_trip.push(stop_time);
    }

    if let Some(t) = &mut timer {
        t.add("stop_time_calculation");
    }

    let mut vehicle = None;
    let mut cancelled_stop_times = vec![];

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        if chateau == "irvine~ca~us" {
            println!("DEBUG: fetch_trip_information: Static Path - Found assigned chateau in etcd");
        }
        let fetch_assigned_node_for_this_chateau_kv_first =
            fetch_assigned_node_for_this_chateau.kvs().first();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = crate::bincode_deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let socket = assigned_chateau_data.socket;
            let aspen_client = if let Some(client) = aspen_client_manager.get_client(socket).await {
                Ok(client)
            } else {
                let client_res = crate::aspen::lib::spawn_aspen_client_from_ip(&socket).await;
                if let Ok(client) = &client_res {
                    aspen_client_manager
                        .insert_client(socket, client.clone())
                        .await;
                }
                client_res
            };

            if let Some(t) = &mut timer {
                t.add("open_aspen_connection");
            }

            match aspen_client {
                Ok(aspen_client) => {
                    if chateau == "irvine~ca~us" {
                        println!("DEBUG: fetch_trip_information: Static Path - Connected to aspen");
                    }
                    let get_trip = aspen_client
                        .get_trip_updates_from_trip_id(
                            context::current(),
                            chateau.clone(),
                            query.trip_id.clone(),
                        )
                        .await;

                    if let Some(t) = &mut timer {
                        t.add("get_trip_rt_from_aspen");
                    }

                    if let Ok(get_trip) = get_trip {
                        match get_trip {
                            Some(get_trip) => {
                                if chateau == "irvine~ca~us" {
                                    println!(
                                        "DEBUG: fetch_trip_information: Static Path - Received {} trip updates from aspen",
                                        get_trip.len()
                                    );
                                }
                                println!("recieved {} trip options from aspen", get_trip.len());
                                if !get_trip.is_empty() {
                                    let rt_trip_update = match get_trip.len() {
                                        1 => &get_trip[0],
                                        _ => {
                                            if chateau == "irvine~ca~us" {
                                                println!(
                                                    "DEBUG: fetch_trip_information: Static Path - Multiple updates found, filtering"
                                                );
                                            }
                                            println!(
                                                "Multiple trip updates found for trip id {} {}",
                                                chateau, query.trip_id
                                            );
                                            let query_start_date_parsed = Some(start_naive_date);

                                            let find_trip = get_trip.iter().find(|each_update| {
                                                let start_time_matches = match (
                                                    &query.start_time,
                                                    &each_update.trip.start_time,
                                                ) {
                                                    (Some(q_time), Some(u_time)) => {
                                                        q_time == u_time
                                                    }
                                                    (None, _) => true,
                                                    (Some(_), None) => false,
                                                };

                                                let start_date_matches = match (
                                                    &query_start_date_parsed,
                                                    &each_update.trip.start_date,
                                                ) {
                                                    (Some(q_date), Some(u_date)) => {
                                                        q_date == u_date
                                                    }
                                                    (None, _) => true,
                                                    (Some(_), None) => false,
                                                };

                                                start_time_matches && start_date_matches
                                            });

                                            match find_trip {
                                                Some(find_trip) => find_trip,
                                                None => &get_trip[0],
                                            }
                                        }
                                    };

                                    let mut modifications_id_for_this_trip: Option<String> = None;

                                    let mut modifications_for_this_trip: Option<
                                        AspenisedTripModification,
                                    > = None;

                                    if rt_trip_update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                            is_cancelled = true;
                                        } else if rt_trip_update.trip.schedule_relationship == Some(crate::aspen_dataset::AspenisedTripScheduleRelationship::Deleted)
                                        {
                                            deleted = true;
                                        }

                                    if let Some(modified_trip) = &rt_trip_update.trip.modified_trip
                                    {
                                        if let Some(modifications_id) =
                                            &modified_trip.modifications_id
                                        {
                                            modifications_id_for_this_trip =
                                                Some(modifications_id.clone());
                                        }
                                    }

                                    if modifications_id_for_this_trip.is_none() {
                                        let modifications_response = aspen_client
                                            .trip_mod_lookup_for_trip_id_service_day(
                                                context::current(),
                                                chateau.clone(),
                                                query.trip_id.clone(),
                                                start_naive_date,
                                            )
                                            .await;

                                        if let Ok(Some(modifications_response)) =
                                            modifications_response
                                        {
                                            modifications_for_this_trip =
                                                Some(modifications_response);
                                        }
                                    }

                                    let mut new_rt_shape_id: Option<String> = None;

                                    if modifications_for_this_trip.is_none() {
                                        if let Some(modifications_id_for_this_trip) =
                                            &modifications_id_for_this_trip
                                        {
                                            let modifications_response: Result<
                                                Option<AspenisedTripModification>,
                                                tarpc::client::RpcError,
                                            > = aspen_client
                                                .get_trip_modification(
                                                    context::current(),
                                                    chateau.clone(),
                                                    modifications_id_for_this_trip.clone(),
                                                )
                                                .await;

                                            if let Ok(Some(modifications_response)) =
                                                modifications_response
                                            {
                                                modifications_for_this_trip =
                                                    Some(modifications_response);
                                            }
                                        }
                                    }

                                    if let Some(modifications_for_this_trip) =
                                        &modifications_for_this_trip
                                    {
                                        //trip modification algorithm
                                        let stops_to_fetch = modifications_for_this_trip
                                            .modifications
                                            .iter()
                                            .map(|modification| {
                                                modification
                                                    .replacement_stops
                                                    .iter()
                                                    .map(|replacement_stop| {
                                                        replacement_stop
                                                            .stop_id
                                                            .as_ref()
                                                            .map(|x| x.to_string())
                                                    })
                                                    .flatten()
                                            })
                                            .flatten()
                                            .collect::<BTreeSet<String>>();

                                        let stops_to_fetch = stops_to_fetch
                                            .difference(
                                                &stop_times_for_this_trip
                                                    .iter()
                                                    .map(|x| x.stop_id.to_string())
                                                    .collect::<BTreeSet<String>>(),
                                            )
                                            .cloned()
                                            .collect::<BTreeSet<_>>();

                                        let stops_data_for_modifications_from_postgres =
                                            stops_pg_schema::dsl::stops
                                                .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
                                                .filter(
                                                    stops_pg_schema::dsl::gtfs_id
                                                        .eq_any(&stops_to_fetch),
                                                )
                                                .select(crate::models::Stop::as_select())
                                                .load(conn)
                                                .await;

                                        let stops_still_missing = stops_to_fetch
                                            .difference(
                                                &stops_data_for_modifications_from_postgres
                                                    .as_ref()
                                                    .unwrap()
                                                    .iter()
                                                    .map(|x| x.gtfs_id.clone())
                                                    .collect::<BTreeSet<String>>(),
                                            )
                                            .cloned()
                                            .collect::<BTreeSet<_>>();

                                        let stops_data_for_modifications_from_aspen = aspen_client
                                            .get_realtime_stops(
                                                context::current(),
                                                chateau.clone(),
                                                stops_still_missing
                                                    .iter()
                                                    .map(|x| x.to_string())
                                                    .collect::<Vec<String>>(),
                                            )
                                            .await;

                                        let stops_data_for_modifications_from_aspen =
                                            match stops_data_for_modifications_from_aspen {
                                                Ok(Some(x)) => x,
                                                _ => {
                                                    eprintln!("Error fetching stops from aspen");
                                                    AHashMap::new()
                                                }
                                            };

                                        for modification in
                                            &modifications_for_this_trip.modifications
                                        {
                                            let mut before_start_selector: Vec<
                                                StopTimeIntroduction,
                                            > = vec![];
                                            let mut with_replacement_stops: Vec<
                                                StopTimeIntroduction,
                                            > = vec![];
                                            let mut after_end_selector: Vec<StopTimeIntroduction> =
                                                vec![];
                                            let mut old_stop_group: Vec<StopTimeIntroduction> =
                                                vec![];

                                            let mut start_stop_selector_idx = None;
                                            let mut end_stop_selector_idx = None;

                                            if let Some(start_stop_selector) =
                                                &modification.start_stop_selector
                                            {
                                                if start_stop_selector.stop_id.is_some()
                                                    || start_stop_selector.stop_sequence.is_some()
                                                {
                                                    start_stop_selector_idx = stop_times_for_this_trip
                                                        .iter()
                                                        .position(|x| {
                                                            match start_stop_selector.stop_id.clone() {
                                                                Some(rt_stop_id) => {
                                                                    match start_stop_selector.stop_sequence {
                                                                        Some(rt_stop_sequence) => {
                                                                            *rt_stop_id == *x.stop_id
                                                                                && Some(rt_stop_sequence as u16)
                                                                                    == x.gtfs_stop_sequence
                                                                        }
                                                                        None => *rt_stop_id == *x.stop_id,
                                                                    }
                                                                }
                                                                None => match start_stop_selector.stop_sequence {
                                                                    Some(rt_stop_sequence) => {
                                                                        Some(rt_stop_sequence as u16)
                                                                            == x.gtfs_stop_sequence
                                                                    }
                                                                    None => false,
                                                                },
                                                            }
                                                        });

                                                    if let Some(start_stop_selector_idx) =
                                                        start_stop_selector_idx
                                                    {
                                                        if let Some(end_stop_selector) =
                                                            &modification.end_stop_selector
                                                        {
                                                            end_stop_selector_idx = stop_times_for_this_trip
                                                            .iter()
                                                            .position(|x| {
                                                                match end_stop_selector.stop_id.clone() {
                                                                    Some(rt_stop_id) => {
                                                                        match end_stop_selector.stop_sequence {
                                                                            Some(rt_stop_sequence) => {
                                                                                *rt_stop_id == *x.stop_id
                                                                                    && x.gtfs_stop_sequence.is_some()
                                                                                    && rt_stop_sequence as u16
                                                                                        == x.gtfs_stop_sequence.unwrap()
                                                                            }
                                                                            None => *rt_stop_id == *x.stop_id,
                                                                        }
                                                                    }
                                                                    None => match end_stop_selector.stop_sequence {
                                                                        Some(rt_stop_sequence) => {
                                                                            x.gtfs_stop_sequence.is_some() && rt_stop_sequence as u16
                                                                                == x.gtfs_stop_sequence.unwrap()
                                                                        }
                                                                        None => false,
                                                                    },
                                                                }
                                                            });
                                                        }

                                                        let reference_stop =
                                                            if start_stop_selector_idx == 0 {
                                                                stop_times_for_this_trip[0].clone()
                                                            } else {
                                                                stop_times_for_this_trip
                                                                    [start_stop_selector_idx - 1]
                                                                    .clone()
                                                            };

                                                        if let Some(end_stop_selector_idx) =
                                                            end_stop_selector_idx
                                                        {
                                                            old_stop_group =
                                                                stop_times_for_this_trip
                                                                    [start_stop_selector_idx
                                                                        ..end_stop_selector_idx
                                                                            + 1]
                                                                    .to_vec();
                                                        }

                                                        before_start_selector =
                                                            stop_times_for_this_trip
                                                                [0..start_stop_selector_idx]
                                                                .to_vec();

                                                        if let Some(end_stop_selector_idx) =
                                                            end_stop_selector_idx
                                                        {
                                                            after_end_selector =
                                                                stop_times_for_this_trip
                                                                    [end_stop_selector_idx + 1..]
                                                                    .to_vec();
                                                        } else {
                                                            after_end_selector =
                                                                stop_times_for_this_trip
                                                                    [start_stop_selector_idx + 1..]
                                                                    .to_vec();
                                                        }

                                                        let arrival_time_at_reference_stop = match reference_stop.scheduled_arrival_time_unix_seconds {
                                                                Some(arrival_time) => arrival_time,
                                                                None => match reference_stop.scheduled_departure_time_unix_seconds {
                                                                    Some(departure_time) => departure_time,
                                                                    None => reference_stop.scheduled_arrival_time_unix_seconds.unwrap()
                                                                }
                                                            };

                                                        let response_replacement_stops = modification.replacement_stops.iter()
                                                        .filter(|replacement_stop| replacement_stop.stop_id.is_some())
                                                        .map(|replacement_stop| {
                                                                let stop_id = replacement_stop.stop_id.as_ref().unwrap();

                                                            let stop: Option<StopPostgresOrAspen> = match stops_data_map.get(stop_id) {
                                                                Some(stop) => {
                                                                    Some(StopPostgresOrAspen::Postgres(stop.clone()))
                                                                }
                                                                None => {
                                                                    let mut found_stop: Option<StopPostgresOrAspen> = None;
                                                                    if let Ok(from_pg_again) = stops_data_for_modifications_from_postgres.as_ref() {
                                                                        if let Some(stop) = from_pg_again.iter().find(|x| x.gtfs_id == *stop_id) {
                                                                            found_stop = Some(StopPostgresOrAspen::Postgres(stop.clone()));
                                                                        }
                                                                    }

                                                                    if found_stop.is_none() {
                                                                        if let Some(stop) = stops_data_for_modifications_from_aspen.get(stop_id) {
                                                                            found_stop = Some(StopPostgresOrAspen::Aspen(stop.clone()));
                                                                        }
                                                                    }

                                                                    found_stop

                                                                }
                                                            };

                                                            match stop {
                                                                None => None,
                                                                Some(stop) =>
                                                                {
                                                                    let stop_id = match &stop {
                                                                        StopPostgresOrAspen::Postgres(stop) => stop.gtfs_id.clone(),
                                                                        StopPostgresOrAspen::Aspen(stop) => stop.stop_id.clone().unwrap_or("".to_string())
                                                                    };

                                                                    let stop_name = match &stop {
                                                                        StopPostgresOrAspen::Postgres(stop) => stop.name.clone(),
                                                                        StopPostgresOrAspen::Aspen(stop) => match stop.stop_name.clone() {
                                                                            Some(stop_name) => Some(stop_name.translation.into_iter().map(|x| x.text).collect::<Vec<String>>().join(", ")),
                                                                            None => None
                                                                        }
                                                                    };

                                                                    let stop_latitude = match &stop {
                                                                        StopPostgresOrAspen::Postgres(stop) => stop.point.map(|point| point.y),
                                                                        StopPostgresOrAspen::Aspen(stop) => stop.stop_lat.map(|x| x as f64)
                                                                    };
                                                                    let stop_longitude = match &stop {
                                                                        StopPostgresOrAspen::Postgres(stop) => stop.point.map(|point| point.x),
                                                                        StopPostgresOrAspen::Aspen(stop) => stop.stop_lon.map(|x| x as f64)
                                                                    };

                                                                    let stop_timezone = match &stop {
                                                                        StopPostgresOrAspen::Postgres(stop) =>stop.timezone.as_ref().map(|pg_timezone| chrono_tz::Tz::from_str_insensitive(&pg_timezone).ok()).flatten(),
                                                                        StopPostgresOrAspen::Aspen(_) => {
                                                                            reference_stop.timezone
                                                                        }
                                                                    };

                                                                    let stop_code = match &stop {
                                                                        StopPostgresOrAspen::Postgres(stop) => stop.code.clone(),
                                                                        StopPostgresOrAspen::Aspen(_) => None
                                                                    };

                                                                return Some(StopTimeIntroduction {
                                                                    stop_id: stop_id.into(),
                                                                    name: stop_name,
                                                                    translations: None,
                                                                    platform_code: None,
                                                                    timezone: stop_timezone,
                                                                    code: stop_code,
                                                                    longitude: stop_longitude,
                                                                    latitude: stop_latitude,
                                                                    scheduled_arrival_time_unix_seconds: Some(arrival_time_at_reference_stop as u64 + replacement_stop.travel_time_to_stop.unwrap_or(0) as u64),
                                                                    scheduled_departure_time_unix_seconds: None,
                                                                    interpolated_stoptime_unix_seconds: None,
                                                                    gtfs_stop_sequence: None,
                                                                    rt_arrival: None,
                                                                    rt_departure: None,
                                                                    schedule_relationship: None,
                                                                    rt_platform_string: None,
                                                                    timepoint: Some(false),
                                                                    replaced_stop: true,
                                                                    osm_station_id: None,
                                                                });
                                                                }
                                                               }
                                                        })
                                                        .flatten()
                                                        .collect::<Vec<StopTimeIntroduction>>()
                                                        ;

                                                        with_replacement_stops =
                                                            response_replacement_stops;

                                                        for post_end_stop in &mut after_end_selector
                                                        {
                                                            if let Some(
                                                                propagated_modification_delay,
                                                            ) = modification
                                                                .propagated_modification_delay
                                                            {
                                                                post_end_stop.scheduled_arrival_time_unix_seconds = post_end_stop.scheduled_arrival_time_unix_seconds.map(|x| x + propagated_modification_delay as u64);
                                                                post_end_stop.scheduled_departure_time_unix_seconds = post_end_stop.scheduled_departure_time_unix_seconds.map(|x| x + propagated_modification_delay as u64);
                                                                post_end_stop.interpolated_stoptime_unix_seconds = post_end_stop.interpolated_stoptime_unix_seconds.map(|x| x + propagated_modification_delay as u64);
                                                            }
                                                        }

                                                        let stops_to_cancel_in_old_group = old_stop_group.iter()
                                                            .filter(|x| {
                                                                !with_replacement_stops.iter().any(|y| y.stop_id == x.stop_id)
                                                            })
                                                            .cloned()
                                                            .collect::<Vec<StopTimeIntroduction>>();

                                                        cancelled_stop_times
                                                            .extend(stops_to_cancel_in_old_group);

                                                        stop_times_for_this_trip = [
                                                            before_start_selector,
                                                            with_replacement_stops,
                                                            after_end_selector,
                                                        ]
                                                        .concat();
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if let Some(trip_modification) = modifications_for_this_trip {
                                        let shape_id = trip_modification
                                            .selected_trips
                                            .iter()
                                            .find(|x| x.trip_ids.contains(&query.trip_id))
                                            .map(|x| x.shape_id.clone())
                                            .flatten();

                                        if let Some(shape_id) = shape_id {
                                            new_rt_shape_id = Some(shape_id);
                                        }
                                    }

                                    if let Some(trip_properties) = &rt_trip_update.trip_properties {
                                        if let Some(shape_id) = &trip_properties.shape_id {
                                            new_rt_shape_id = Some(shape_id.clone());
                                        }
                                    }

                                    if let Some(new_rt_shape_id) = new_rt_shape_id {
                                        let shape_response = aspen_client
                                            .get_shape(
                                                context::current(),
                                                chateau.clone(),
                                                new_rt_shape_id.clone(),
                                            )
                                            .await;

                                        if let Ok(Some(shape_response)) = shape_response {
                                            old_shape_polyline = shape_polyline;
                                            shape_polyline = Some(shape_response.to_string());
                                            rt_shape = true;
                                        }
                                    }

                                    vehicle = rt_trip_update.vehicle.clone();

                                    println!(
                                        "rt data contains {} stop updates",
                                        rt_trip_update.stop_time_update.len()
                                    );

                                    for stop_time_update in &rt_trip_update.stop_time_update {
                                        let stop_time =
                                            stop_times_for_this_trip.iter_mut().find(|x| {
                                                match stop_time_update.stop_id.clone() {
                                                    Some(rt_stop_id) => {
                                                        match stop_time_update.stop_sequence {
                                                            Some(rt_stop_sequence) => {
                                                                crate::stop_matching::rt_stop_matches_scheduled_simple(
                                                                    &rt_stop_id,
                                                                    &x.stop_id,
                                                                )
                                                                    && x.gtfs_stop_sequence
                                                                        .is_some()
                                                                    && rt_stop_sequence as u16
                                                                        == x.gtfs_stop_sequence
                                                                            .unwrap()
                                                            }
                                                            None => crate::stop_matching::rt_stop_matches_scheduled_simple(
                                                                &rt_stop_id,
                                                                &x.stop_id,
                                                            ),
                                                        }
                                                    }
                                                    None => match stop_time_update.stop_sequence {
                                                        Some(rt_stop_sequence) => {
                                                            x.gtfs_stop_sequence.is_some()
                                                                && rt_stop_sequence as u16
                                                                    == x.gtfs_stop_sequence.unwrap()
                                                        }
                                                        None => false,
                                                    },
                                                }
                                            });

                                        if chateau == "irvine~ca~us" {
                                            println!(
                                                "DEBUG: fetch_trip_information: Static Path - Processing stop update. Matched stop: {}",
                                                stop_time.is_some()
                                            );
                                        }

                                        if let Some(stop_time) = stop_time {
                                            if let Some(arrival) = &stop_time_update.arrival {
                                                stop_time.rt_arrival = Some(arrival.clone());
                                            }

                                            if let Some(departure) = &stop_time_update.departure {
                                                stop_time.rt_departure = Some(departure.clone());
                                            }

                                            if let Some(schedule_relationship) =
                                                &stop_time_update.schedule_relationship
                                            {
                                                stop_time.schedule_relationship =
                                                    Some(schedule_relationship.clone().into());
                                            }

                                            if let Some(rt_platform_string) =
                                                stop_time_update.platform_string.clone()
                                            {
                                                stop_time.rt_platform_string =
                                                    Some(rt_platform_string.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                if chateau == "irvine~ca~us" {
                                    println!(
                                        "DEBUG: fetch_trip_information: Static Path - Trip id not found in get_trip result: {}",
                                        query.trip_id
                                    );
                                }
                                eprintln!("Trip id not found {} {}", chateau, query.trip_id);
                            }
                        }
                    } else {
                        if chateau == "irvine~ca~us" {
                            println!(
                                "DEBUG: fetch_trip_information: Static Path - get_trip failed: {:?}",
                                get_trip.err()
                            );
                        }
                    }

                    // GET ALERTS

                    let alerts_response = get_alert_single_trip(
                        &aspen_client,
                        chateau.clone(),
                        query.trip_id.clone(),
                        trip_compressed.route_id.clone(),
                        stop_ids_to_lookup,
                    )
                    .await;

                    if let Ok(alerts_response) = alerts_response {
                        alert_id_to_alert = alerts_response.alert_id_to_alert;
                        alert_ids_for_this_route = alerts_response.alert_ids_for_this_route;
                        alert_ids_for_this_trip = alerts_response.alert_ids_for_this_trip;
                        stop_id_to_alert_ids = alerts_response.stop_id_to_alert_ids;

                        for alert in alert_id_to_alert.values() {
                            if is_cancelled {
                                break;
                            }

                            let effect_is_no_service = alert.effect == Some(1); // NO_SERVICE
                            if effect_is_no_service {
                                let applies_to_trip = alert.informed_entity.iter().any(|e| {
                                    let route_match = e
                                        .route_id
                                        .as_ref()
                                        .map_or(false, |r_id| *r_id == itin_meta.route_id);
                                    let trip_match = e.trip.as_ref().map_or(false, |t| {
                                        t.trip_id
                                            .as_ref()
                                            .map_or(false, |t_id| *t_id == query.trip_id)
                                    });
                                    route_match || trip_match
                                });

                                let applies_to_trip_without_a_referenced_stop = alert
                                    .informed_entity
                                    .iter()
                                    .filter(|e| e.stop_id.is_none())
                                    .any(|e| {
                                        let route_match = e
                                            .route_id
                                            .as_ref()
                                            .map_or(false, |r_id| *r_id == itin_meta.route_id);
                                        let trip_match = e.trip.as_ref().map_or(false, |t| {
                                            t.trip_id
                                                .as_ref()
                                                .map_or(false, |t_id| *t_id == query.trip_id)
                                        });

                                        route_match || trip_match
                                    });

                                if applies_to_trip {
                                    let is_active = alert.active_period.iter().any(|ap| {
                                        let start = ap.start.unwrap_or(0);
                                        let end = ap.end.unwrap_or(u64::MAX);

                                        let start_of_trip = stop_times_for_this_trip[0]
                                            .scheduled_arrival_time_unix_seconds
                                            .unwrap_or(
                                                stop_times_for_this_trip[0]
                                                    .scheduled_arrival_time_unix_seconds
                                                    .unwrap_or(u64::MAX),
                                            );
                                        let end_of_trip = stop_times_for_this_trip
                                            .last()
                                            .unwrap()
                                            .scheduled_arrival_time_unix_seconds
                                            .unwrap_or(0);
                                        start_of_trip >= start && end_of_trip <= end
                                    });
                                    if is_active {
                                        if alert.effect == Some(1) {
                                            if applies_to_trip_without_a_referenced_stop {
                                                is_cancelled = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        eprintln!("Error fetching alerts from aspen");
                    }
                }
                _ => {
                    eprintln!("Error connecting to assigned node. Failed to connect to tarpc");
                }
            }
        }
    }

    // --- compute close connections / transfers per stop (copied from /route_info) ---

    // Build SerializableStop map & seed additional_routes_to_lookup from the static stops
    let mut additional_routes_to_lookup: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut stops_hashmap: AHashMap<String, SerializableStop> = AHashMap::new();

    for stop in stops_data_map.values() {
        let lat = stop.point.map(|p| p.y);
        let lon = stop.point.map(|p| p.x);

        additional_routes_to_lookup
            .entry(stop.chateau.clone())
            .or_insert_with(BTreeSet::new)
            .extend(
                stop.routes
                    .iter()
                    .filter_map(|r| r.clone())
                    .collect::<BTreeSet<String>>(),
            );

        stops_hashmap.insert(
            stop.gtfs_id.clone(),
            SerializableStop {
                id: stop.gtfs_id.clone(),
                name: stop.name.clone(),
                code: stop.code.clone(),
                description: stop.gtfs_desc.clone(),
                latitude: lat,
                longitude: lon,
                location_type: stop.location_type,
                parent_station: stop.parent_station.clone(),
                timezone: stop.timezone.clone(),
                zone_id: stop.zone_id.clone(),
                routes: stop.routes.iter().filter_map(|r| r.clone()).collect(),
                platform_code: stop.platform_code.clone(),
                level_id: stop.level_id.clone(),
            },
        );
    }

    // Pull in parent stations the same way /route_info does
    let parent_stops: ahash::AHashSet<String> = stops_hashmap
        .values()
        .filter_map(|x| x.parent_station.clone())
        .collect();

    if !parent_stops.is_empty() {
        let parent_stops_pg: Vec<crate::models::Stop> = stops_pg_schema::dsl::stops
            .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
            .filter(stops_pg_schema::dsl::gtfs_id.eq_any(&parent_stops))
            .select(crate::models::Stop::as_select())
            .load(conn)
            .await
            .unwrap();

        for stop in parent_stops_pg {
            let lat = stop.point.map(|x| x.y);
            let lon = stop.point.map(|x| x.x);

            additional_routes_to_lookup
                .entry(stop.chateau.clone())
                .or_insert_with(BTreeSet::new)
                .extend(
                    stop.routes
                        .iter()
                        .filter_map(|x| x.clone())
                        .collect::<BTreeSet<String>>(),
                );

            stops_hashmap.insert(
                stop.gtfs_id.clone(),
                SerializableStop {
                    id: stop.gtfs_id.clone(),
                    name: stop.name,
                    code: stop.code,
                    description: stop.gtfs_desc,
                    latitude: lat,
                    longitude: lon,
                    location_type: stop.location_type,
                    parent_station: stop.parent_station,
                    timezone: stop.timezone,
                    zone_id: stop.zone_id,
                    routes: stop.routes.iter().filter_map(|x| x.clone()).collect(),
                    platform_code: stop.platform_code,
                    level_id: stop.level_id,
                },
            );
        }
    }

    // base stops (for this trip) with coordinates
    let stop_positions: Vec<(String, f64, f64)> = stops_hashmap
        .iter()
        .filter_map(|(stop_id, stop)| match (stop.latitude, stop.longitude) {
            (Some(lat), Some(lon)) => Some((stop_id.clone(), lat, lon)),
            _ => None,
        })
        .collect();

    let connections_lookup_result = connections_lookup(
        chateau.as_str(),
        route.route_id.as_str(),
        route.route_type,
        stop_positions,
        additional_routes_to_lookup,
        None,
        pool.clone(),
    )
    .await;

    let response = TripIntroductionInformation {
        stoptimes: stop_times_for_this_trip,
        tz: timezone,
        color: route.color,
        text_color: route.text_color,
        route_id: route.route_id,
        block_id: trip_compressed.block_id,
        bikes_allowed: trip_compressed.bikes_allowed,
        wheelchair_accessible: trip_compressed.wheelchair_accessible,
        has_frequencies: trip_compressed.has_frequencies,
        trip_headsign: itin_meta.trip_headsign,
        trip_short_name: trip_compressed.trip_short_name.map(|x| x.into()),
        route_long_name: route.long_name,
        route_short_name: route.short_name,
        vehicle,
        route_type: route.route_type,
        stop_id_to_alert_ids,
        alert_ids_for_this_route,
        alert_ids_for_this_trip,
        alert_id_to_alert,
        shape_polyline,
        trip_id_found_in_db: true,
        service_date: Some(start_naive_date),
        schedule_trip_exists: true,
        rt_shape: rt_shape,
        old_shape_polyline,
        cancelled_stoptimes: cancelled_stop_times,
        is_cancelled: is_cancelled,
        deleted: deleted,
        connecting_routes: if connections_lookup_result.connecting_routes.is_empty() {
            None
        } else {
            Some(connections_lookup_result.connecting_routes)
        },
        connections_per_stop: if connections_lookup_result.connections_per_stop.is_empty() {
            None
        } else {
            Some(connections_lookup_result.connections_per_stop)
        },
        trip_id: Some(query.trip_id.clone()),
        chateau: Some(chateau.clone()),
    };

    Ok(response)
}
