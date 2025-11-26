use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;

pub async fn fetch_northern_indiana_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let nictd_data = nictd_gtfs_rt::train_feed(client).await;

        match nictd_data {
            Ok(feed) => {
                //extract the binary data
                let alert_data = feed.alerts.encode_to_vec();

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

                let tarpc_send_to_aspen = aspen_client
                    .from_alpenrose(
                        tarpc::context::current(),
                        data.chateau_id.clone(),
                        String::from(feed_id),
                        None,
                        None,
                        Some(alert_data),
                        true,
                        true,
                        false,
                        Some(200),
                        Some(200),
                        Some(200),
                        duration_since_unix_epoch().as_millis() as u64,
                    )
                    .await;

                match tarpc_send_to_aspen {
                    Ok(_) => {
                        println!("Successfully sent NICTD data sent to {}", feed_id);
                    }
                    Err(e) => {
                        eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch NICTD data");
                eprintln!("{:?}", e);
            }
        }
    } else {
        println!("No assigned node found for NICTD");
    }

    Ok(())
}
