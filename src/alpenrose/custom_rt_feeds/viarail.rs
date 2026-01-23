use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;

pub async fn fetch_via_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    _client: &reqwest::Client,
    gtfs: &gtfs_structures::Gtfs,
) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(assigned_chateau_data) = fetch_assigned_node_meta {
        let worker_id = assigned_chateau_data.worker_id;

        let via_gtfs_rt_data = via_rail_gtfsrt::get_via_rail_gtfs_rt(gtfs).await;

        match via_gtfs_rt_data {
            Ok(via_rail_results) => {
                //extract the binary data
                let vehicle_data = via_rail_results.vehicle_positions.encode_to_vec();
                let trip_data = via_rail_results.trip_updates.encode_to_vec();
                let alert_data = via_rail_results.alerts.encode_to_vec();

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                        .await
                        .unwrap();

                let tarpc_send_to_aspen = aspen_client
                    .from_alpenrose(
                        tarpc::context::current(),
                        assigned_chateau_data.chateau_id.clone(),
                        String::from(feed_id),
                        Some(vehicle_data),
                        Some(trip_data),
                        Some(alert_data),
                        true,
                        true,
                        true,
                        Some(200),
                        Some(200),
                        Some(200),
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
            }
            _ => {
                eprintln!("Failed to fetch Via Rail data");
                eprintln!("{:?}", via_gtfs_rt_data.unwrap_err());
            }
        }
    } else {
        println!("No assigned node found for Via Rail");
    }
}
