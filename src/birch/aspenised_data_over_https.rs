use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use catenary::aspen::lib::ChateauMetadataZookeeper;

use tarpc::{client, context, tokio_serde::formats::Bincode};

#[actix_web::get("/get_realtime_locations/{chateau_id}/{category}/{last_updated_time_ms}/{existing_fasthash_of_routes}")]
pub async fn get_realtime_locations(
    req: HttpRequest,
    zk: web::Data<tokio_zookeeper::ZooKeeper>,
    path: web::Path<(String, String, u64, u64)>,
) -> impl Responder {
    let (chateau_id, category, client_last_updated_time_ms, existing_fasthash_of_routes) =
        path.into_inner();

    let existing_fasthash_of_routes = match existing_fasthash_of_routes {
        0 => None,
        _ => Some(existing_fasthash_of_routes),
    };

    //first identify which node to connect to

    let fetch_assigned_node_for_this_realtime_feed = zk
        .get_data(format!("/aspen_assigned_chateaus/{}", chateau_id).as_str())
        .await
        .unwrap();

    if fetch_assigned_node_for_this_realtime_feed.is_none() {
        return HttpResponse::NotFound().body("No assigned node found for this chateau");
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

    let transport = tarpc::serde_transport::tcp::connect(socket_addr, Bincode::default)
        .await
        .unwrap();

    let aspen_client =
        catenary::aspen::lib::AspenRpcClient::new(client::Config::default(), transport).spawn();

    //then call the get_vehicle_locations method

    let response = aspen_client
        .get_vehicle_locations(context::current(), chateau_id, existing_fasthash_of_routes)
        .await
        .unwrap();

    //serde the response into json and send it

    match response {
        Some(response) => match client_last_updated_time_ms == response.last_updated_time_ms {
            true => HttpResponse::NoContent().finish(),
            false => HttpResponse::Ok().json(response),
        },
        None => HttpResponse::NotFound().body("No data found"),
    }
}
