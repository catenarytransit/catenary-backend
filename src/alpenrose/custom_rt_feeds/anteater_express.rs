use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use prost::Message;
use tokio_zookeeper::ZooKeeper;

pub async fn fetch_anteater_express_data(zk: &ZooKeeper, feed_id: &str) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id(&zk, feed_id).await;

    if let Some((data, stat)) = fetch_assigned_node_meta {
        let socket_addr = std::net::SocketAddr::new(data.tailscale_ip, 40427);
        let worker_id = data.worker_id;

        let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr)
            .await
            .unwrap();

        let zotgtfs_realtime_data = zotgtfs::get_gtfs_rt().await;

        if let Ok(zotgtfs_realtime_data) = zotgtfs_realtime_data {
            let socket_addr = std::net::SocketAddr::new(data.tailscale_ip, 40427);

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr)
                .await
                .unwrap();

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    data.chateau_id.clone(),
                    String::from(feed_id),
                    Some(zotgtfs_realtime_data.encode_to_vec()),
                    None,
                    None,
                    true,
                    false,
                    false,
                    Some(200),
                    None,
                    None,
                    duration_since_unix_epoch().as_millis() as u64,
                )
                .await;

            match tarpc_send_to_aspen {
                Ok(_) => {
                    println!("Successfully sent zotgtfs data sent to {}", feed_id);
                }
                Err(e) => {
                    eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                }
            }
        } else {
            eprintln!("Failed to fetch ZotGTFS data");
            eprintln!("{:?}", zotgtfs_realtime_data);
        }
    } else {
        println!("No assigned node found for Anteater Express");
    }
}
