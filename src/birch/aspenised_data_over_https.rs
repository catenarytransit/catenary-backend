use actix_web::{web, HttpRequest, HttpResponse, Responder};
use ahash::AHashMap;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::GetVehicleLocationsResponse;
use catenary::aspen_dataset::AspenisedVehiclePosition;
use catenary::aspen_dataset::AspenisedVehicleRouteCache;
use catenary::EtcdConnectionIps;
use serde::{Deserialize, Serialize};
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
