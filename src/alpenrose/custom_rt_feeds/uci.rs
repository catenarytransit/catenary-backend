use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;
use zotgtfs::get_gtfs_rt;

pub async fn fetch_uci_data(etcd: &mut etcd_client::KvClient, feed_id: &str) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let uci_gtfs_rt = zotgtfs::get_gtfs_rt().await;

        if let Ok(uci_gtfs_rt) = uci_gtfs_rt {
            //extract the binary data
            let vehicle_data = uci_gtfs_rt.encode_to_vec();

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket)
                .await
                .unwrap();

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    data.chateau_id.clone(),
                    String::from(feed_id),
                    Some(vehicle_data.clone()),
                    Some(vehicle_data),
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
                    println!("Successfully sent UCI data sent to {}", feed_id);
                }
                Err(e) => {
                    eprintln!(
                        "{}: Error sending UCI data to {}: {}",
                        feed_id, worker_id, e
                    );
                }
            }
        } else {
            eprintln!("Failed to fetch UCI data");
            eprintln!("{:?}", uci_gtfs_rt.unwrap_err());
        }
    } else {
        println!("No assigned node found for UCI");
    }
}
