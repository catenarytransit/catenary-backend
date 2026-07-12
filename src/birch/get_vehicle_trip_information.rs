use crate::connections_lookup::connections_lookup;
use actix_web::rt;
use actix_web::{HttpResponse, Responder, web};
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::SerializableStop;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::aspen_dataset::AspenisedTripScheduleRelationship;
use catenary::aspen_dataset::AspenisedVehicleDescriptor;
use catenary::aspen_dataset::AspenisedVehiclePosition;
use catenary::aspen_dataset::{AspenStopTimeEvent, AspenisedTripModification};
use catenary::aspen_dataset::{AspenisedAlert, AspenisedStop};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::calendar as calendar_pg_schema;
use catenary::schema::gtfs::calendar_dates as calendar_dates_pg_schema;
use catenary::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use catenary::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use catenary::schema::gtfs::routes as routes_pg_schema;
use catenary::schema::gtfs::stops as stops_pg_schema;
use catenary::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use catenary::trip_logic::{
    GtfsRtRefreshData, QueryTripInformationParams, ResponseForGtfsRtRefresh,
    ResponseForGtfsVehicle, StopTimeIntroduction, StopTimeRefresh, TripIntroductionInformation,
    fetch_trip_information, fetch_trip_rt_update,
};
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

#[actix_web::get("/get_vehicle_metadata/{chateau}/{vehicle_id}")]
pub async fn get_vehicle_metadata(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[actix_web::get("/get_vehicle_information_from_label/{chateau}/{vehicle_label}")]
pub async fn get_vehicle_information_from_label(
    path: web::Path<(String, String)>,
    aspen_chateau_cache: web::Data<
        Arc<catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>>,
    >,
) -> impl Responder {
    let (chateau, vehicle_label) = path.into_inner();

    if let Some(assigned_chateau_data) = aspen_chateau_cache.get(&chateau) {
        if true {
            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            if let Ok(aspen_client) = aspen_client {
                let get_vehicle = aspen_client
                    .get_single_vehicle_location_from_vehicle_label(
                        context::current(),
                        chateau.clone(),
                        vehicle_label.clone(),
                    )
                    .await;

                if let Ok(get_vehicle) = get_vehicle {
                    if let Some(get_vehicle) = get_vehicle {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: true,
                            data: Some(vec![get_vehicle]),
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    } else {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: false,
                            data: None,
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    }
                }
            }
        }
    }

    HttpResponse::InternalServerError().body("Could not connect to assigned node")
}

#[actix_web::get("/get_vehicle_information/{chateau}/{gtfs_rt_id}")]
pub async fn get_vehicle_information(
    path: web::Path<(String, String)>,
    aspen_chateau_cache: web::Data<
        Arc<catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>>,
    >,
) -> impl Responder {
    let (chateau, gtfs_id) = path.into_inner();

    if let Some(assigned_chateau_data) = aspen_chateau_cache.get(&chateau) {
        if true {
            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            if let Ok(aspen_client) = aspen_client {
                let get_vehicle = aspen_client
                    .get_single_vehicle_location_from_gtfsid(
                        context::current(),
                        chateau.clone(),
                        gtfs_id.clone(),
                    )
                    .await;

                if let Ok(get_vehicle) = get_vehicle {
                    let vehicles = match get_vehicle {
                        Some(get_vehicle) => Some(vec![get_vehicle]),
                        None => None,
                    };

                    if let Some(vehicles) = vehicles {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: true,
                            data: Some(vehicles),
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    } else {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: false,
                            data: None,
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    }
                }
            }
        }
    }

    HttpResponse::InternalServerError().body("Could not connect to assigned node")
}

#[actix_web::get("/get_trip_information_rt_update/{chateau}/")]
pub async fn get_trip_rt_update(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>, // pool: web::Data<Arc<CatenaryPostgresPool>>,
    aspen_chateau_cache: web::Data<
        Arc<catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>>,
    >,
    aspen_client_manager: web::Data<Arc<AspenClientManager>>,
) -> impl Responder {
    let chateau = path.into_inner();
    let query = query.into_inner();

    match fetch_trip_rt_update(
        chateau,
        query,
        aspen_chateau_cache.as_ref().clone(),
        aspen_client_manager.as_ref().clone(),
        None,
    )
    .await
    {
        Ok((response, _)) => HttpResponse::Ok().json(response),
        Err(e) => HttpResponse::InternalServerError().body(e),
    }
}

#[actix_web::get("/get_trip_information/{chateau}/")]
pub async fn get_trip_init(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    aspen_chateau_cache: web::Data<
        Arc<catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>>,
    >,
    aspen_client_manager: web::Data<Arc<AspenClientManager>>,
) -> impl Responder {
    let mut timer = simple_server_timing_header::Timer::new();
    let chateau = path.into_inner();
    let query_params = query.into_inner();

    match fetch_trip_information(
        chateau,
        query_params,
        pool.as_ref().clone(),
        aspen_chateau_cache.as_ref().clone(),
        aspen_client_manager.as_ref().clone(),
        Some(&mut timer),
    )
    .await
    {
        Ok(response) => {
            let text = serde_json::to_string(&response).unwrap();
            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/json"))
                .insert_header(("Cache-Control", "no-cache"))
                .insert_header((
                    simple_server_timing_header::Timer::header_key(),
                    timer.header_value(),
                ))
                .body(text)
        }
        Err(e) => {
            if e.as_str().contains("not found") {
                HttpResponse::NotFound().body(e)
            } else {
                HttpResponse::InternalServerError().body(e)
            }
        }
    }
}
