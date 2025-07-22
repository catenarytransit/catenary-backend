use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use gtfs_realtime::FeedHeader;
use gtfs_realtime::FeedMessage;
use prost::Message;

pub async fn fetch_data(etcd: &mut etcd_client::KvClient, feed_id: &str, client: &reqwest::Client) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(assigned_chateau_data) = fetch_assigned_node_meta {
        let worker_id = assigned_chateau_data.worker_id;

        let metrolink_extra_gtfs_rt_data =
            catenary::custom_alerts::metrolink_alerts::gtfs_rt_alerts_from_metrolink_website(
                client,
            )
            .await;

        match metrolink_extra_gtfs_rt_data {
            Ok(metrolink_extra_gtfs_rt_data) => {
                //extract the binary data
                let alerts_msg = FeedMessage {
                    header: FeedHeader {
                        gtfs_realtime_version: String::from("2.0"),
                        incrementality: Some(
                            gtfs_realtime::feed_header::Incrementality::FullDataset.into(),
                        ),
                        feed_version: None,
                        timestamp: Some(duration_since_unix_epoch().as_secs() as u64),
                    },
                    entity: metrolink_extra_gtfs_rt_data,
                };

                let alerts_proto = alerts_msg.encode_to_vec();

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                        .await;

                if let Ok(aspen_client) = aspen_client {
                    let tarpc_send_to_aspen = aspen_client
                        .from_alpenrose(
                            tarpc::context::current(),
                            assigned_chateau_data.chateau_id.clone(),
                            String::from(feed_id),
                            None,
                            None,
                            Some(alerts_proto),
                            false,
                            false,
                            true,
                            None,
                            None,
                            Some(200),
                            duration_since_unix_epoch().as_millis() as u64,
                        )
                        .await;

                    match tarpc_send_to_aspen {
                        Ok(_) => {
                            println!(
                                "Successfully sent Metrolink Alerts Extra feed data sent to {}",
                                feed_id
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "{}: Error sending Metrolink Alerts Extra feed data to {}: {}",
                                feed_id, worker_id, e
                            );
                        }
                    }
                } else {
                    eprintln!(
                        "Failed to connect to Aspen at {}, {}, {:?}",
                        assigned_chateau_data.socket,
                        assigned_chateau_data.chateau_id,
                        aspen_client.unwrap_err()
                    );
                    return;
                }
            }
            _ => {
                eprintln!("Failed to fetch Metrolink Alerts Extra data");
                eprintln!("{:?}", metrolink_extra_gtfs_rt_data.unwrap_err());
            }
        }
    } else {
        println!("No assigned node found for Metrolink Alerts Extra");
    }
}
