use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use catenary::unzip_uk::get_raw_gtfs_rt;
use prost::Message;

use crate::custom_rt_feeds::uk;

pub async fn fetch_chicago_data(etcd: &mut etcd_client::Client, feed_id: &str, client: &reqwest::Client) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let socket_addr = std::net::SocketAddr::new(data.tailscale_ip, 40427);
        let worker_id = data.worker_id;

        let chicago_rt_data =
            chicago_gtfs_rt::train_feed(&client, &"13f685e4b9054545b19470556103ec73").await;

        if let Ok(chicago_rt_data) = chicago_rt_data {
            let socket_addr = std::net::SocketAddr::new(data.tailscale_ip, 40427);

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr)
                .await
                .unwrap();

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    data.chateau_id.clone(),
                    String::from(feed_id),
                    Some(chicago_rt_data.vehicle_positions.encode_to_vec()),
                    None,
                    None,
                    true,
                    true,
                    false,
                    Some(200),
                    None,
                    None,
                    duration_since_unix_epoch().as_millis() as u64,
                )
                .await;

            match tarpc_send_to_aspen {
                Ok(_) => {
                    println!(
                        "Successfully sent chicago data to {}, feed {} to chateau {}",
                        data.tailscale_ip, feed_id, data.chateau_id
                    );
                }
                Err(e) => {
                    eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                }
            }
        } else {
            eprintln!("Failed to fetch Chicago Transit data");
            eprintln!("{:?}", chicago_rt_data);
        }
    } else {
        println!("No assigned node found for Chicago Transit");
    }
}
