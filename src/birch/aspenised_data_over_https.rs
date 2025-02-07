use actix_web::{web, HttpRequest, HttpResponse, Responder};
use ahash::AHashMap;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::GetVehicleLocationsResponse;
use catenary::aspen_dataset::AspenisedVehiclePosition;
use catenary::aspen_dataset::AspenisedVehicleRouteCache;
use catenary::EtcdConnectionIps;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tarpc::context;

#[derive(Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum CategoryOfRealtimeVehicleData {
    Metro,
    Bus,
    Rail,
    Other,
}

fn category_to_allowed_route_ids(category: &CategoryOfRealtimeVehicleData) -> Vec<i16> {
    match category {
        CategoryOfRealtimeVehicleData::Metro => vec![0, 1, 12],
        CategoryOfRealtimeVehicleData::Bus => vec![3, 11],
        CategoryOfRealtimeVehicleData::Rail => vec![2],
        CategoryOfRealtimeVehicleData::Other => vec![4, 5, 6, 7],
    }
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchParams {
    chateaus: BTreeMap<String, ChateauAskParams>,
    categories: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ChateauAskParams {
    last_updated_time_ms: u64,
    existing_fasthash_of_routes: u64,
}

#[derive(Serialize, Deserialize)]
pub struct EachChateauResponse {
    categories: Option<PositionDataCategory>,
    hash_of_routes: u64,
    last_updated_time_ms: u64,
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
    pub vehicle_positions: Option<AHashMap<String, AspenisedVehiclePosition>>,
    pub last_updated_time_ms: u64,
    pub hash_of_routes: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BulkFetchResponse {
    chateaus: BTreeMap<String, EachChateauResponse>,
}

#[actix_web::post("/bulk_realtime_fetch_v1")]
pub async fn bulk_realtime_fetch_v1(
    req: HttpRequest,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    params: web::Json<BulkFetchParams>,
) -> impl Responder {
    let etcd =
        etcd_client::Client::connect(etcd_connection_ips.ip_addresses.as_slice(), None).await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let mut bulk_fetch_response = BulkFetchResponse {
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
        &ChateauAskParams,
        Result<etcd_client::GetResponse, etcd_client::Error>,
    )> = futures::stream::iter(params.chateaus.iter().map(|(chateau_id, chateau_params)| {
        let mut etcd = etcd.clone();
        async move {
            let fetch_assigned_node_for_this_realtime_feed = etcd
                .get(
                    format!("/aspen_assigned_chateaus/{}", chateau_id).as_str(),
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

    let parallel_chateau_data_fetch = futures::stream::iter(
        etcd_data_list
            .iter()
            .filter(|x| x.2.is_ok())
            .map(|(chateau_id, chateau_params, etcd_data)| {
                (chateau_id, chateau_params, etcd_data.as_ref().unwrap())
            })
            .map(|(chateau_id, chateau_params, etcd_data_list)| async move {
                if etcd_data_list.kvs().is_empty() {
                    return (chateau_id, None, chateau_params);
                }

                //deserialise into ChateauMetadataZookeeper

                let assigned_chateau_data = bincode::deserialize::<ChateauMetadataEtcd>(
                    etcd_data_list.kvs().first().unwrap().value(),
                )
                .unwrap();

                //then connect to the node via tarpc

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                        .await;

                if aspen_client.is_err() {
                    return (chateau_id, None, chateau_params);
                }

                let aspen_client = aspen_client.unwrap();

                //then call the get_vehicle_locations method

                let response = aspen_client
                    .get_vehicle_locations(context::current(), chateau_id.to_string(), None)
                    .await;

                (chateau_id, Some(response), chateau_params)
            }),
    )
    .buffer_unordered(32)
    .collect::<Vec<_>>()
    .await;

    for (chateau_id, response, chateau_params) in parallel_chateau_data_fetch {
        if let Some(Ok(Some(response))) = response {
            let mut each_chateau_response = EachChateauResponse {
                categories: None,
                hash_of_routes: response.hash_of_routes,
                last_updated_time_ms: response.last_updated_time_ms,
            };

            if chateau_params.last_updated_time_ms != response.last_updated_time_ms {
                let mut categories: PositionDataCategory = PositionDataCategory::default();

                for category in &categories_requested {
                    let route_ids_allowed = category_to_allowed_route_ids(category);

                    let filtered_vehicle_positions = response
                        .vehicle_positions
                        .iter()
                        .filter(|vehicle_position| {
                            route_ids_allowed.contains(&vehicle_position.1.route_type)
                        })
                        .map(|(a, b)| (a.clone(), b.clone()))
                        .collect::<AHashMap<String, AspenisedVehiclePosition>>();

                    let filtered_routes_cache: Option<
                        AHashMap<String, AspenisedVehicleRouteCache>,
                    > = match &response.vehicle_route_cache {
                        Some(vehicle_route_cache) => {
                            let filtered_vehicle_route_cache = vehicle_route_cache
                                .iter()
                                .filter(|(route_id, vehicle_route_cache)| {
                                    route_ids_allowed.contains(&vehicle_route_cache.route_type)
                                })
                                .map(|(a, b)| (a.clone(), b.clone()))
                                .collect::<AHashMap<String, AspenisedVehicleRouteCache>>();
                            Some(filtered_vehicle_route_cache)
                        }
                        None => None,
                    };

                    let payload = EachCategoryPayload {
                        vehicle_positions: Some(filtered_vehicle_positions),
                        vehicle_route_cache: filtered_routes_cache,
                        hash_of_routes: response.hash_of_routes,
                        last_updated_time_ms: response.last_updated_time_ms,
                    };

                    //add to categories hashmap

                    match category {
                        CategoryOfRealtimeVehicleData::Metro => {
                            categories.metro = Some(payload);
                        }
                        CategoryOfRealtimeVehicleData::Rail => {
                            categories.rail = Some(payload);
                        }
                        CategoryOfRealtimeVehicleData::Other => {
                            categories.other = Some(payload);
                        }
                        CategoryOfRealtimeVehicleData::Bus => {
                            categories.bus = Some(payload);
                        }
                    };
                }

                each_chateau_response.categories = Some(categories);
            }

            bulk_fetch_response
                .chateaus
                .insert(chateau_id.to_string(), each_chateau_response);
        }
    }

    HttpResponse::Ok()
        .append_header(("Cache-Control", "no-cache"))
        .json(bulk_fetch_response)
}

#[actix_web::get("/get_realtime_locations/{chateau_id}/{category}/{last_updated_time_ms}/{existing_fasthash_of_routes}")]
pub async fn get_realtime_locations(
    req: HttpRequest,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    path: web::Path<(String, String, u64, u64)>,
) -> impl Responder {
    let (chateau_id, category, client_last_updated_time_ms, existing_fasthash_of_routes) =
        path.into_inner();

    let etcd =
        etcd_client::Client::connect(etcd_connection_ips.ip_addresses.as_slice(), None).await;

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
            format!("/aspen_assigned_chateaus/{}", chateau_id).as_str(),
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
            .body("No assigned node found for this chateau");
    }

    //deserialise into ChateauMetadataZookeeper

    let assigned_chateau_data = bincode::deserialize::<ChateauMetadataEtcd>(
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
        .get_vehicle_locations(context::current(), chateau_id, existing_fasthash_of_routes)
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
            .body("No realtime data found for this chateau"),
    }
}
