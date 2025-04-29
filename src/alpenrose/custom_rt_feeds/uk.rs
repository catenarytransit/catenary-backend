use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use catenary::unzip_uk::get_raw_gtfs_rt;

pub async fn fetch_dft_bus_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let uk_rt_data = get_raw_gtfs_rt(client).await;

        match uk_rt_data {
            Ok(uk_rt_data) => {
                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await;

                if let Ok(aspen_client) = aspen_client {
                    let tarpc_send_to_aspen = aspen_client
                        .from_alpenrose(
                            tarpc::context::current(),
                            data.chateau_id.clone(),
                            String::from(feed_id),
                            Some(uk_rt_data.clone()),
                            Some(uk_rt_data.clone()),
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
                            println!(
                                "Successfully sent UK data to {}, feed {} to chateau {}",
                                data.socket, feed_id, data.chateau_id
                            );
                        }
                        Err(e) => {
                            eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                        }
                    }
                }
            }
            _ => {
                eprintln!("Failed to fetch UK data");
                eprintln!("{:?}", uk_rt_data);
            }
        }
    } else {
        println!("No assigned node found for UK");
    }

    Ok(())
}
