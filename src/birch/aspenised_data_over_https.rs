use crate::BirchGlobalDatastore;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use ahash::{AHashMap, AHashSet};
use catenary::aspen::lib::ChateauMetadataZookeeper;
use catenary::aspen::lib::GetVehicleLocationsResponse;
use catenary::aspen_dataset::AspenisedVehiclePosition;
use catenary::aspen_dataset::AspenisedVehicleRouteCache;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tarpc::{client, context, tokio_serde::formats::Bincode};

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

#[actix_web::get("/get_realtime_locations/{chateau_id}/{category}/{last_updated_time_ms}/{existing_fasthash_of_routes}")]
pub async fn get_realtime_locations(
    req: HttpRequest,
    zk: web::Data<Arc<tokio_zookeeper::ZooKeeper>>,
    path: web::Path<(String, String, u64, u64)>,
    global_cache: web::Data<Arc<BirchGlobalDatastore>>,
) -> impl Responder {
    let (chateau_id, category, client_last_updated_time_ms, existing_fasthash_of_routes) =
        path.into_inner();

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

    let fetch_assigned_node_for_this_realtime_feed = zk
        .get_data(format!("/aspen_assigned_chateaus/{}", chateau_id).as_str())
        .await;

    if let Err(err_fetch) = &fetch_assigned_node_for_this_realtime_feed {
        eprintln!("{}", err_fetch);
        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body(format!(
                "Error fetching assigned node: {}, failed to connect to zookeeper",
                err_fetch
            ));
    }

    let fetch_assigned_node_for_this_realtime_feed =
        fetch_assigned_node_for_this_realtime_feed.unwrap();

    if fetch_assigned_node_for_this_realtime_feed.is_none() {
        return HttpResponse::Ok()
            .append_header(("Cache-Control", "no-cache"))
            .body("No assigned node found for this chateau");
    }

    let (fetch_assigned_node_for_this_realtime_feed, stat) =
        fetch_assigned_node_for_this_realtime_feed.unwrap();

    //deserialise into ChateauMetadataZookeeper

    let assigned_chateau_data = bincode::deserialize::<ChateauMetadataZookeeper>(
        &fetch_assigned_node_for_this_realtime_feed,
    )
    .unwrap();

    //then connect to the node via tarpc

    let socket_addr = std::net::SocketAddr::new(assigned_chateau_data.tailscale_ip, 40427);

    let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr).await;

    if (aspen_client.is_err()) {
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
                    .header("Cache-Control", "no-cache")
                    .json(filtered_response)
            }
        },
        None => HttpResponse::Ok()
            .append_header(("Cache-Control", "no-cache"))
            .body("No realtime data found for this chateau"),
    }
}
