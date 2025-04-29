use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;

pub async fn fetch_amtrak_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let amtrak_gtfs_rt = amtrak_gtfs_rt::fetch_amtrak_gtfs_rt(gtfs, client).await;

        match amtrak_gtfs_rt {
            Ok(amtrak_gtfs_rt) => {
                //extract the binary data
                let vehicle_data = amtrak_gtfs_rt.vehicle_positions.encode_to_vec();
                let trip_data = amtrak_gtfs_rt.trip_updates.encode_to_vec();
                let alert_data = amtrak_gtfs_rt.alerts.encode_to_vec();

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

                let tarpc_send_to_aspen = aspen_client
                    .from_alpenrose(
                        tarpc::context::current(),
                        data.chateau_id.clone(),
                        String::from(feed_id),
                        Some(vehicle_data),
                        Some(trip_data),
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
                        println!("Successfully sent Amtrak data sent to {}", feed_id);
                    }
                    Err(e) => {
                        eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch Amtrak data");
                eprintln!("{:?}", e);
            }
        }
    } else {
        println!("No assigned node found for Amtrak");
    }

    Ok(())
}
