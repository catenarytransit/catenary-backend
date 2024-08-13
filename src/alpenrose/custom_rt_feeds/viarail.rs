use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use prost::Message;

pub async fn fetch_via_data(
    etcd: &mut etcd_client::Client,
    feed_id: &str,
    client: &reqwest::Client,
) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let socket_addr = std::net::SocketAddr::new(data.ip.0, data.ip.1);
        let worker_id = data.worker_id;

        let via_gtfs_rt_data = via_rail_gtfsrt::get_via_rail_gtfs_rt().await;

        if let Ok(via_gtfs_rt) = via_gtfs_rt_data {
            //extract the binary data
            let vehicle_data = via_gtfs_rt.clone().encode_to_vec();
            let trip_data = via_gtfs_rt.clone().encode_to_vec();

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr)
                .await
                .unwrap();

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    data.chateau_id.clone(),
                    String::from(feed_id),
                    Some(vehicle_data),
                    Some(trip_data),
                    None,
                    true,
                    true,
                    false,
                    Some(200),
                    Some(200),
                    None,
                    duration_since_unix_epoch().as_millis() as u64,
                )
                .await;

            match tarpc_send_to_aspen {
                Ok(_) => {
                    println!("Successfully sent Via Rail data sent to {}", feed_id);
                }
                Err(e) => {
                    eprintln!(
                        "{}: Error sending Via Rail data to {}: {}",
                        feed_id, worker_id, e
                    );
                }
            }
        } else {
            eprintln!("Failed to fetch Via Rail data");
            eprintln!("{:?}", via_gtfs_rt_data.unwrap_err());
        }
    } else {
        println!("No assigned node found for Via Rail");
    }
}
