use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::GetVehicleLocationsResponse;
use catenary::aspen_dataset::*;
use catenary::models::CompressedTrip;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel::{ExpressionMethods, SelectableHelper};
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use tarpc::context;

#[derive(Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum CategoryOfRealtimeVehicleData {
    Metro,
    Bus,
    Rail,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash)]
pub struct AspenisedVehicleTripInfoOutput {
    pub trip_id: Option<String>,
    pub trip_headsign: Option<String>,
    pub route_id: Option<String>,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub schedule_relationship: Option<u8>,
    pub delay: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]

pub struct AspenisedVehiclePositionOutput {
    pub trip: Option<AspenisedVehicleTripInfoOutput>,
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

fn convert_to_output(input: &AspenisedVehiclePosition) -> AspenisedVehiclePositionOutput {
    let trip_new = match &input.trip {
        Some(trip) => Some(AspenisedVehicleTripInfoOutput {
            trip_id: trip.trip_id.clone(),
            trip_headsign: trip.trip_headsign.clone(),
            route_id: trip.route_id.clone(),
            trip_short_name: trip.trip_short_name.clone(),
            direction_id: trip.direction_id,
            start_time: trip.start_time.clone(),
            start_date: trip.start_date.map(|x| x.format("%Y%m%d").to_string()),
            schedule_relationship: trip.schedule_relationship.as_ref().map(|x| x.into()),
            delay: trip.delay,
        }),
        None => None,
    };

    AspenisedVehiclePositionOutput {
        trip: trip_new,
        vehicle: input.vehicle.clone(),
        position: input.position.clone(),
        timestamp: input.timestamp,
        route_type: input.route_type,
        current_stop_sequence: input.current_stop_sequence,
        current_status: input.current_status,
        congestion_level: input.congestion_level,
        occupancy_status: input.occupancy_status,
        occupancy_percentage: input.occupancy_percentage,
    }
}

fn category_to_allowed_route_ids(category: &CategoryOfRealtimeVehicleData) -> Vec<i16> {
    match category {
        CategoryOfRealtimeVehicleData::Metro => vec![0, 1, 5, 7, 12],
        CategoryOfRealtimeVehicleData::Bus => vec![3, 11],
        CategoryOfRealtimeVehicleData::Rail => vec![2],
        CategoryOfRealtimeVehicleData::Other => vec![4, 6],
    }
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchParams {
    chateaus: BTreeMap<String, ChateauAskParams>,
    categories: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ChateauAskParams {
    category_params: CategoryAskParams,
}

#[derive(Serialize, Deserialize)]
pub struct ChateauAskParamsV2 {
    category_params: CategoryAskParamsV2,
}

#[derive(Serialize, Deserialize)]
pub struct CategoryAskParams {
    bus: Option<SubCategoryAskParams>,
    metro: Option<SubCategoryAskParams>,
    rail: Option<SubCategoryAskParams>,
    other: Option<SubCategoryAskParams>,
}

#[derive(Serialize, Deserialize)]
pub struct SubCategoryAskParams {
    last_updated_time_ms: u64,
    hash_of_routes: u64,
}

#[derive(Serialize, Deserialize)]
pub struct SubCategoryAskParamsV2 {
    last_updated_time_ms: u64,
    prev_user_min_x: Option<u32>,
    prev_user_max_x: Option<u32>,
    prev_user_min_y: Option<u32>,
    prev_user_max_y: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct CategoryAskParamsV2 {
    bus: Option<SubCategoryAskParamsV2>,
    metro: Option<SubCategoryAskParamsV2>,
    rail: Option<SubCategoryAskParamsV2>,
    other: Option<SubCategoryAskParamsV2>,
}

#[derive(Serialize, Deserialize)]
pub struct BoundsInput {
    level5: BoundsInputPerLevel,
    level7: BoundsInputPerLevel,
    level8: BoundsInputPerLevel,
    level10: BoundsInputPerLevel,
}

#[derive(Serialize, Deserialize)]
pub struct BoundsInputV3 {
    level5: BoundsInputPerLevel,
    level7: BoundsInputPerLevel,
    level8: BoundsInputPerLevel,
    level12: BoundsInputPerLevel,
}

#[derive(Serialize, Deserialize)]
pub struct BoundsInputPerLevel {
    min_x: u32,
    max_x: u32,
    min_y: u32,
    max_y: u32,
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchParamsV2 {
    chateaus: BTreeMap<String, ChateauAskParamsV2>,
    categories: Vec<String>,
    bounds_input: BoundsInput,
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchParamsV3 {
    chateaus: BTreeMap<String, ChateauAskParamsV2>,
    categories: Vec<String>,
    bounds_input: BoundsInputV3,
}

#[derive(Serialize, Deserialize)]
pub struct EachChateauResponse {
    categories: Option<PositionDataCategory>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct PositionDataCategory {
    metro: Option<EachCategoryPayload>,
    bus: Option<EachCategoryPayload>,
    rail: Option<EachCategoryPayload>,
    other: Option<EachCategoryPayload>,
}

#[derive(Serialize, Deserialize)]
pub struct EachCategoryPayload {
    pub vehicle_route_cache: Option<AHashMap<String, AspenisedVehicleRouteCache>>,
    pub vehicle_positions: Option<AHashMap<String, AspenisedVehiclePositionOutput>>,
    pub last_updated_time_ms: u64,
    pub hash_of_routes: u64,
}

#[derive(Serialize, Deserialize)]
pub struct EachChateauResponseV2 {
    categories: Option<PositionDataCategoryV2>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct PositionDataCategoryV2 {
    metro: Option<EachCategoryPayloadV2>,
    bus: Option<EachCategoryPayloadV2>,
    rail: Option<EachCategoryPayloadV2>,
    other: Option<EachCategoryPayloadV2>,
}

//other should load at z5

// rail should be loaded at z7

// local rail should be loaded at z8

// buses should load at z10

//v2 of the bulk fetch algorithm should return all new tiles if the last updated time changes, otherwise it should selectively insert into tiles

#[derive(Serialize, Deserialize)]
pub struct EachCategoryPayloadV2 {
    // agency id (unwrap to "null") -> route id -> route
    //pub vehicle_route_cache: Option<BTreeMap<String, BTreeMap<String, AspenisedVehicleRouteCache>>>,
    pub vehicle_positions:
        Option<BTreeMap<u32, BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>>>,
    pub last_updated_time_ms: u64,
    pub replaces_all: bool,
    // agency id (unwrap to "null") -> hash
    //pub hash_of_routes: BTreeMap<String, u64>,
    pub z_level: u8,
    pub list_of_agency_ids: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchResponse {
    chateaus: BTreeMap<String, EachChateauResponse>,
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchResponseV2 {
    chateaus: BTreeMap<String, EachChateauResponseV2>,
}

#[derive(Serialize, Deserialize)]
pub struct PerRouteRtInfo {
    pub vehicle_positions: Option<AHashMap<String, AspenisedVehiclePositionOutput>>,
    pub last_updated_time_ms: u64,
    pub trips_to_trips_compressed: Option<AHashMap<String, CompressedTrip>>,
    pub itinerary_to_direction_id: Option<AHashMap<String, String>>,
    pub trip_updates: Vec<AspenisedTripUpdate>,
    pub trip_id_to_direction_id: AHashMap<String, Option<String>>,
    pub trip_id_to_direction_pattern_parent_id: AHashMap<String, Option<String>>,
}

#[actix_web::post("/bulk_realtime_fetch_v3")]
pub async fn bulk_realtime_fetch_v3(
    req: HttpRequest,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    params: web::Json<BulkFetchParamsV3>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let etcd =
        catenary::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser)
            .await;

    if etcd.is_err() {
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let mut bulk_fetch_response = BulkFetchResponseV2 {
        chateaus: BTreeMap::new(),
    };

    let categories_requested = params
        .categories
        .iter()
        .map(|category| match category.as_str() {
            "metro" => Some(CategoryOfRealtimeVehicleData::Metro),
            "bus" => Some(CategoryOfRealtimeVehicleData::Bus),
            "rail" => Some(CategoryOfRealtimeVehicleData::Rail),
            "other" => Some(CategoryOfRealtimeVehicleData::Other),
            _ => None,
        })
        .flatten()
        .collect::<Vec<CategoryOfRealtimeVehicleData>>();

    let etcd_data_list: Vec<(
        &String,
        &ChateauAskParamsV2,
        Result<etcd_client::GetResponse, etcd_client::Error>,
    )> = futures::stream::iter(params.chateaus.iter().map(|(chateau_id, chateau_params)| {
        let mut etcd = etcd.clone();
        async move {
            let fetch_assigned_node_for_this_realtime_feed = etcd
                .get(
                    format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
                    None,
                )
                .await;

            (
                chateau_id,
                chateau_params,
                fetch_assigned_node_for_this_realtime_feed,
            )
        }
    }))
    .buffer_unordered(64)
    .collect::<Vec<_>>()
    .await;

    let route_types_wanted = Arc::new(match categories_requested.len() {
        4 => None,
        _ => Some(
            categories_requested
                .iter()
                .flat_map(|category| category_to_allowed_route_ids(category))
                .collect::<Vec<i16>>(),
        ),
    });

    let parallel_chateau_data_fetch = futures::stream::iter(
        etcd_data_list
            .iter()
            .filter(|x| x.2.is_ok())
            .map(|(chateau_id, chateau_params, etcd_data)| {
                (chateau_id, chateau_params, etcd_data.as_ref().unwrap())
            })
            .map(|(chateau_id, chateau_params, etcd_data_list)| {
                let route_types_wanted = Arc::clone(&route_types_wanted);

                async move {
                    if etcd_data_list.kvs().is_empty() {
                        return (chateau_id, None, chateau_params);
                    }

                    //deserialise into ChateauMetadataZookeeper

                    let assigned_chateau_data =
                        catenary::bincode_deserialize::<ChateauMetadataEtcd>(
                            etcd_data_list.kvs().first().unwrap().value(),
                        )
                        .unwrap();

                    //then connect to the node via tarpc

                    let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(
                        &assigned_chateau_data.socket,
                    )
                    .await;

                    if aspen_client.is_err() {
                        return (chateau_id, None, chateau_params);
                    }

                    let aspen_client = aspen_client.unwrap();

                    //then call the get_vehicle_locations method

                    let response = aspen_client
                        .get_vehicle_locations(
                            context::current(),
                            chateau_id.to_string(),
                            None,
                            route_types_wanted.as_ref().clone(),
                        )
                        .await;

                    (chateau_id, Some(response), chateau_params)
                }
            }),
    )
    .buffer_unordered(32)
    .collect::<Vec<_>>()
    .await;

    for (chateau_id, response, chateau_params) in parallel_chateau_data_fetch {
        if let Some(Ok(Some(response))) = response {
            let mut each_chateau_response = EachChateauResponseV2 {
                categories: Some(PositionDataCategoryV2::default()),
            };

            for category in &categories_requested {
                let chateau_params_for_this_category = match category {
                    CategoryOfRealtimeVehicleData::Metro => &chateau_params.category_params.metro,
                    CategoryOfRealtimeVehicleData::Bus => &chateau_params.category_params.bus,
                    CategoryOfRealtimeVehicleData::Rail => &chateau_params.category_params.rail,
                    CategoryOfRealtimeVehicleData::Other => &chateau_params.category_params.other,
                };

                let mismatched_times = response.last_updated_time_ms
                    != chateau_params_for_this_category
                        .as_ref()
                        .map_or(0, |x| x.last_updated_time_ms);

                let no_previous_tiles = match category {
                    CategoryOfRealtimeVehicleData::Metro => {
                        let bounds = &params.bounds_input.level8;
                        chateau_params_for_this_category.as_ref().map_or(true, |p| {
                            p.prev_user_min_x.is_none()
                                || p.prev_user_max_x.is_none()
                                || p.prev_user_min_y.is_none()
                                || p.prev_user_max_y.is_none()
                        })
                    }
                    CategoryOfRealtimeVehicleData::Rail => {
                        let bounds = &params.bounds_input.level7;
                        chateau_params_for_this_category.as_ref().map_or(true, |p| {
                            p.prev_user_min_x.is_none()
                                || p.prev_user_max_x.is_none()
                                || p.prev_user_min_y.is_none()
                                || p.prev_user_max_y.is_none()
                        })
                    }
                    CategoryOfRealtimeVehicleData::Bus => {
                        let bounds = &params.bounds_input.level12;
                        chateau_params_for_this_category.as_ref().map_or(true, |p| {
                            p.prev_user_min_x.is_none()
                                || p.prev_user_max_x.is_none()
                                || p.prev_user_min_y.is_none()
                                || p.prev_user_max_y.is_none()
                        })
                    }
                    CategoryOfRealtimeVehicleData::Other => {
                        let bounds = &params.bounds_input.level5;
                        chateau_params_for_this_category.as_ref().map_or(true, |p| {
                            p.prev_user_min_x.is_none()
                                || p.prev_user_max_x.is_none()
                                || p.prev_user_min_y.is_none()
                                || p.prev_user_max_y.is_none()
                        })
                    }
                };

                let replace_all_tiles = mismatched_times || no_previous_tiles;

                let route_types_allowed = category_to_allowed_route_ids(category);

                let filtered_vehicle_positions = response
                    .vehicle_positions
                    .iter()
                    .filter(|vehicle_position| {
                        route_types_allowed.contains(&vehicle_position.1.route_type)
                    })
                    //.map(|(a, b)| (a.clone(), b))
                    .map(|(a, b)| (a.clone(), convert_to_output(&b)))
                    .collect::<AHashMap<String, AspenisedVehiclePositionOutput>>();

                // vehicles are sorted into the slippy tile structure
                //BTreeMap<u32, BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>>

                let mut vehicles_by_tile: BTreeMap<
                    u32,
                    BTreeMap<u32, BTreeMap<String, AspenisedVehiclePositionOutput>>,
                > = BTreeMap::new();

                let zoom = match category {
                    CategoryOfRealtimeVehicleData::Metro => 8,
                    CategoryOfRealtimeVehicleData::Rail => 7,
                    CategoryOfRealtimeVehicleData::Bus => 12,
                    CategoryOfRealtimeVehicleData::Other => 5,
                };

                match replace_all_tiles {
                    true => {
                        for (vehicle_id, vehicle_position) in filtered_vehicle_positions {
                            if let Some(pos) = &vehicle_position.position {
                                let bounds = match category {
                                    CategoryOfRealtimeVehicleData::Metro => {
                                        &params.bounds_input.level8
                                    }
                                    CategoryOfRealtimeVehicleData::Rail => {
                                        &params.bounds_input.level7
                                    }
                                    CategoryOfRealtimeVehicleData::Bus => {
                                        &params.bounds_input.level12
                                    }
                                    CategoryOfRealtimeVehicleData::Other => {
                                        &params.bounds_input.level5
                                    }
                                };

                                if pos.latitude != 0.0 && pos.longitude != 0.0 {
                                    let (x, y) = slippy_map_tiles::lat_lon_to_tile(
                                        pos.latitude,
                                        pos.longitude,
                                        zoom,
                                    );

                                    let in_current_bounds = x >= bounds.min_x
                                        && x <= bounds.max_x
                                        && y >= bounds.min_y
                                        && y <= bounds.max_y;

                                    if in_current_bounds {
                                        vehicles_by_tile
                                            .entry(x)
                                            .or_default()
                                            .entry(y)
                                            .or_default()
                                            .insert(vehicle_id, vehicle_position);
                                    }
                                }
                            }
                        }
                    }
                    false => {
                        // We only send back vehicle positions for tiles that are new to the user.
                        let (bounds, prev_bounds_params) = match category {
                            CategoryOfRealtimeVehicleData::Metro => (
                                &params.bounds_input.level8,
                                chateau_params_for_this_category.as_ref(),
                            ),
                            CategoryOfRealtimeVehicleData::Rail => (
                                &params.bounds_input.level7,
                                chateau_params_for_this_category.as_ref(),
                            ),
                            CategoryOfRealtimeVehicleData::Bus => (
                                &params.bounds_input.level12,
                                chateau_params_for_this_category.as_ref(),
                            ),
                            CategoryOfRealtimeVehicleData::Other => (
                                &params.bounds_input.level5,
                                chateau_params_for_this_category.as_ref(),
                            ),
                        };

                        if let Some(prev_bounds) = prev_bounds_params {
                            if let (
                                Some(prev_min_x),
                                Some(prev_max_x),
                                Some(prev_min_y),
                                Some(prev_max_y),
                            ) = (
                                prev_bounds.prev_user_min_x,
                                prev_bounds.prev_user_max_x,
                                prev_bounds.prev_user_min_y,
                                prev_bounds.prev_user_max_y,
                            ) {
                                for (vehicle_id, vehicle_position) in filtered_vehicle_positions {
                                    if let Some(pos) = &vehicle_position.position {
                                        if pos.latitude != 0.0 && pos.longitude != 0.0 {
                                            let (x, y) = slippy_map_tiles::lat_lon_to_tile(
                                                pos.latitude,
                                                pos.longitude,
                                                zoom,
                                            );

                                            let in_current_bounds = x >= bounds.min_x
                                                && x <= bounds.max_x
                                                && y >= bounds.min_y
                                                && y <= bounds.max_y;
                                            let in_prev_bounds = x >= prev_min_x
                                                && x <= prev_max_x
                                                && y >= prev_min_y
                                                && y <= prev_max_y;

                                            if in_current_bounds && !in_prev_bounds {
                                                vehicles_by_tile
                                                    .entry(x)
                                                    .or_default()
                                                    .entry(y)
                                                    .or_default()
                                                    .insert(vehicle_id, vehicle_position);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let list_of_agency_ids = response.vehicle_route_cache.as_ref().map(|cache| {
                    cache
                        .values()
                        .filter_map(|route_cache| {
                            if route_types_allowed.contains(&route_cache.route_type) {
                                route_cache.agency_id.clone()
                            } else {
                                None
                            }
                        })
                        .collect::<BTreeSet<String>>()
                        .into_iter()
                        .collect::<Vec<String>>()
                });

                let payload = EachCategoryPayloadV2 {
                    vehicle_positions: match vehicles_by_tile.is_empty() {
                        false => Some(vehicles_by_tile),
                        true => None,
                    },
                    last_updated_time_ms: response.last_updated_time_ms,
                    replaces_all: replace_all_tiles,
                    z_level: match category {
                        CategoryOfRealtimeVehicleData::Metro => 8,
                        CategoryOfRealtimeVehicleData::Rail => 7,
                        CategoryOfRealtimeVehicleData::Bus => 12,
                        CategoryOfRealtimeVehicleData::Other => 5,
                    },
                    list_of_agency_ids,
                };

                //add to categories hashmap

                match category {
                    CategoryOfRealtimeVehicleData::Metro => {
                        each_chateau_response.categories.as_mut().unwrap().metro = Some(payload);
                    }
                    CategoryOfRealtimeVehicleData::Rail => {
                        each_chateau_response.categories.as_mut().unwrap().rail = Some(payload);
                    }
                    CategoryOfRealtimeVehicleData::Other => {
                        each_chateau_response.categories.as_mut().unwrap().other = Some(payload);
                    }
                    CategoryOfRealtimeVehicleData::Bus => {
                        each_chateau_response.categories.as_mut().unwrap().bus = Some(payload);
                    }
                }
            }

            bulk_fetch_response
                .chateaus
                .insert(chateau_id.to_string(), each_chateau_response);
        }
    }

    return HttpResponse::Ok()
        .append_header(("Cache-Control", "no-cache"))
        .json(bulk_fetch_response);
}

#[actix_web::get(
    "/get_realtime_locations/{chateau_id}/{category}/{last_updated_time_ms}/{existing_fasthash_of_routes}"
)]
pub async fn get_realtime_locations(
    req: HttpRequest,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    path: web::Path<(String, String, u64, u64)>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let (chateau_id, category, client_last_updated_time_ms, existing_fasthash_of_routes) =
        path.into_inner();

    let etcd =
        catenary::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser)
            .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let category_requested = match category.as_str() {
        "metro" => CategoryOfRealtimeVehicleData::Metro,
        "bus" => CategoryOfRealtimeVehicleData::Bus,
        "rail" => CategoryOfRealtimeVehicleData::Rail,
        "other" => CategoryOfRealtimeVehicleData::Other,
        _ => return HttpResponse::NotFound().body("Invalid category"),
    };

    let existing_fasthash_of_routes = match existing_fasthash_of_routes {
        0 => None,
        _ => Some(existing_fasthash_of_routes),
    };

    //first identify which node to connect to

    let fetch_assigned_node_for_this_realtime_feed = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
            None,
        )
        .await;

    if let Err(err_fetch) = &fetch_assigned_node_for_this_realtime_feed {
        eprintln!("{}", err_fetch);
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body(format!(
                "Error fetching assigned node: {}, failed to connect to etcd",
                err_fetch
            ));
    }

    let fetch_assigned_node_for_this_realtime_feed =
        fetch_assigned_node_for_this_realtime_feed.unwrap();

    if fetch_assigned_node_for_this_realtime_feed.kvs().is_empty() {
        return HttpResponse::Ok()
            .append_header(("Cache-Control", "no-cache"))
            .body("No assigned node found for this chateau according to etcd database");
    }

    //deserialise into ChateauMetadataZookeeper

    let assigned_chateau_data = catenary::bincode_deserialize::<ChateauMetadataEtcd>(
        fetch_assigned_node_for_this_realtime_feed
            .kvs()
            .first()
            .unwrap()
            .value(),
    )
    .unwrap();

    //then connect to the node via tarpc

    let aspen_client =
        catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket).await;

    if aspen_client.is_err() {
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Error connecting to assigned node. Failed to connect to tarpc");
    }

    let aspen_client = aspen_client.unwrap();

    //then call the get_vehicle_locations method

    let response = aspen_client
        .get_vehicle_locations(
            context::current(),
            chateau_id,
            existing_fasthash_of_routes,
            None,
        )
        .await
        .unwrap();

    //serde the response into json and send it

    match response {
        Some(response) => match client_last_updated_time_ms == response.last_updated_time_ms {
            true => HttpResponse::NoContent().finish(),
            false => {
                let route_ids_allowed = category_to_allowed_route_ids(&category_requested);
                let filtered_vehicle_positions = response
                    .vehicle_positions
                    .into_iter()
                    .filter(|vehicle_position| {
                        route_ids_allowed.contains(&vehicle_position.1.route_type)
                    })
                    .collect::<AHashMap<String, AspenisedVehiclePosition>>();

                let filtered_routes_cache: Option<AHashMap<String, AspenisedVehicleRouteCache>> =
                    match response.vehicle_route_cache {
                        Some(vehicle_route_cache) => {
                            let filtered_vehicle_route_cache = vehicle_route_cache
                                .into_iter()
                                .filter(|(route_id, vehicle_route_cache)| {
                                    route_ids_allowed.contains(&vehicle_route_cache.route_type)
                                })
                                .collect::<AHashMap<String, AspenisedVehicleRouteCache>>();
                            Some(filtered_vehicle_route_cache)
                        }
                        None => None,
                    };

                let filtered_response = GetVehicleLocationsResponse {
                    vehicle_positions: filtered_vehicle_positions,
                    vehicle_route_cache: filtered_routes_cache,
                    hash_of_routes: response.hash_of_routes,
                    last_updated_time_ms: response.last_updated_time_ms,
                };

                HttpResponse::Ok()
                    .append_header(("Cache-Control", "no-cache"))
                    .json(filtered_response)
            }
        },
        None => HttpResponse::Ok()
            .append_header(("Cache-Control", "no-cache"))
            .body("No realtime data found for this chateau, aspen server returned None"),
    }
}

#[derive(Deserialize)]
pub struct SingleRouteRtInfo {
    pub last_updated_time_ms: Option<u64>,
    pub chateau: String,
    pub route_id: String,
}

#[actix_web::get("/get_rt_of_single_route")]
pub async fn get_rt_of_route(
    req: HttpRequest,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: web::Query<SingleRouteRtInfo>,
) -> impl Responder {
    let query = query.into_inner();

    let chateau_id = query.chateau.clone();

    //connect to postgres
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    let etcd = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned(),
    )
    .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_realtime_feed = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
            None,
        )
        .await;

    if let Err(err_fetch) = &fetch_assigned_node_for_this_realtime_feed {
        eprintln!("{}", err_fetch);
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body(format!(
                "Error fetching assigned node: {}, failed to connect to etcd",
                err_fetch
            ));
    }

    let fetch_assigned_node_for_this_realtime_feed =
        fetch_assigned_node_for_this_realtime_feed.unwrap();

    if fetch_assigned_node_for_this_realtime_feed.kvs().is_empty() {
        return HttpResponse::Ok()
            .append_header(("Cache-Control", "no-cache"))
            .body("No assigned node found for this chateau according to etcd database");
    }

    //deserialise into ChateauMetadataZookeeper

    let assigned_chateau_data = catenary::bincode_deserialize::<ChateauMetadataEtcd>(
        fetch_assigned_node_for_this_realtime_feed
            .kvs()
            .first()
            .unwrap()
            .value(),
    )
    .unwrap();

    //then connect to the node via tarpc

    let aspen_client =
        catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket).await;

    if aspen_client.is_err() {
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Error connecting to assigned node. Failed to connect to tarpc");
    }

    let aspen_client = aspen_client.unwrap();

    let get_vehicles = aspen_client
        .get_vehicle_locations_with_route_filtering(
            context::current(),
            chateau_id.clone(),
            None,
            Some(vec![query.route_id.clone()]),
        )
        .await;

    let get_vehicles = get_vehicles.unwrap().unwrap();

    if let Some(query_last_updated_time_ms) = query.last_updated_time_ms {
        if query_last_updated_time_ms == get_vehicles.last_updated_time_ms {
            return HttpResponse::NoContent().body("");
        }
    }

    let returned_vehicle_struct = get_vehicles
        .vehicle_positions
        .iter()
        .map(|(a, b)| (a.clone(), convert_to_output(b)))
        .collect::<AHashMap<_, _>>();

    let trip_ids_to_lookup = get_vehicles
        .vehicle_positions
        .iter()
        .map(|(_, vehicle_info)| match &vehicle_info.trip {
            Some(trip) => trip.trip_id.clone(),
            None => None,
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect::<ahash::AHashSet<String>>();

    let trip_fetch_rt = aspen_client
        .get_trip_updates_from_route_ids(
            context::current(),
            chateau_id.clone(),
            vec![query.route_id.clone()],
        )
        .await
        .unwrap();

    let current_unix_time = catenary::duration_since_unix_epoch().as_secs();

    let mut trip_update_list: Vec<AspenisedTripUpdate> = vec![];

    if let Some(trip_fetch_rt) = trip_fetch_rt {
        for trip_update in trip_fetch_rt {
            let mut trip_update = trip_update;

            let stop_time_update = match trip_update.stop_time_update.len() {
                0 => trip_update.stop_time_update,
                1 => trip_update.stop_time_update,
                _ => {
                    let mut start_filter = trip_update
                        .stop_time_update
                        .into_iter()
                        .filter(|stu| {
                            if let Some(departure) = &stu.departure {
                                if let Some(d_time) = departure.time {
                                    if ((d_time as u64) < current_unix_time) {
                                        return false;
                                    }
                                }
                            } else {
                                if let Some(arrival) = &stu.arrival {
                                    if let Some(a_time) = arrival.time {
                                        if ((a_time as u64) < current_unix_time) {
                                            return false;
                                        }
                                    }
                                }
                            }

                            return true;
                        })
                        .collect::<Vec<_>>();

                    if start_filter.len() > 8 {
                        start_filter.truncate(8);
                    }

                    start_filter
                }
            };

            trip_update.stop_time_update = stop_time_update;

            trip_update_list.push(trip_update);
        }
    }

    let trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
        .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::trips_compressed::dsl::trip_id.eq_any(&trip_ids_to_lookup))
        .select(catenary::models::CompressedTrip::as_select())
        .load(conn)
        .await
        .unwrap();

    let mut trips_table = AHashMap::new();

    let mut itineraries_to_fetch: Vec<String> = vec![];

    for trip in trips {
        itineraries_to_fetch.push(trip.itinerary_pattern_id.clone());
        trips_table.insert(trip.trip_id.clone(), trip);
    }

    let mut itinerary_to_direction_id: AHashMap<String, String> = AHashMap::new();

    let itineraries_fetch =
        catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::dsl::chateau.eq(&query.chateau))
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_id
                    .eq_any(&itineraries_to_fetch),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load(conn)
            .await
            .unwrap();

    for itinerary in itineraries_fetch {
        itinerary_to_direction_id.insert(
            itinerary.itinerary_pattern_id.clone(),
            itinerary.direction_pattern_id.clone().unwrap(),
        );
    }

    let directions_to_fetch = itinerary_to_direction_id
        .values()
        .cloned()
        .collect::<ahash::AHashSet<String>>();

    let directions_fetch =
        catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
            .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::chateau.eq(&query.chateau))
            .filter(
                catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_id
                    .eq_any(&directions_to_fetch),
            )
            .select(catenary::models::DirectionPatternMeta::as_select())
            .load::<catenary::models::DirectionPatternMeta>(conn)
            .await
            .unwrap();

    let direction_id_to_direction = directions_fetch
        .into_iter()
        .map(|direction| (direction.direction_pattern_id.clone(), direction))
        .collect::<AHashMap<String, catenary::models::DirectionPatternMeta>>();

    let mut trip_id_to_direction_id: AHashMap<String, Option<String>> = trips_table
        .iter()
        .map(|(trip_id, trip)| {
            let direction_id = itinerary_to_direction_id
                .get(&trip.itinerary_pattern_id)
                .cloned();
            (trip_id.clone(), direction_id)
        })
        .collect();

    let trip_id_to_direction_pattern_parent_id: AHashMap<String, Option<String>> =
        trip_id_to_direction_id
            .iter()
            .map(|(trip_id, direction_id)| {
                let direction = match direction_id {
                    Some(direction_id) => direction_id_to_direction.get(direction_id),
                    None => None,
                };

                let direction_id_parent = match direction {
                    Some(direction) => direction.direction_pattern_id_parents.clone(),
                    None => None,
                };

                (trip_id.clone(), direction_id_parent)
            })
            .collect();

    let returned_data = PerRouteRtInfo {
        vehicle_positions: Some(returned_vehicle_struct),
        last_updated_time_ms: get_vehicles.last_updated_time_ms,
        trips_to_trips_compressed: Some(trips_table),
        itinerary_to_direction_id: Some(itinerary_to_direction_id),
        trip_updates: trip_update_list,
        trip_id_to_direction_id: trip_id_to_direction_id,
        trip_id_to_direction_pattern_parent_id: trip_id_to_direction_pattern_parent_id,
    };

    return HttpResponse::Ok().json(returned_data);
}
