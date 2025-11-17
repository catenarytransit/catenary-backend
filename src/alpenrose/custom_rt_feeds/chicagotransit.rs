use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;
use rand::Rng;

pub async fn fetch_chicago_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
    trips_content: &str,
    chicago_gtfs: &gtfs_structures::Gtfs,
) {
    let api_keys = [
        "ae8e2a26183d45438e85a8dd5ff4aac7",
        "13f685e4b9054545b19470556103ec73",
        "c08cdb1cc63a40f186bc9f960deeb5fd",
        "33305d8dcece4aa58c651c740f88d1e2",
    ];

    let randomly_picked_api_key = rand::random_range(0..api_keys.len());

    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(worker_metadata) = fetch_assigned_node_meta {
        let worker_id = worker_metadata.worker_id;

        let chicago_rt_data = chicago_gtfs_rt::train_feed(
            client,
            api_keys[randomly_picked_api_key],
            &trips_content,
            chicago_gtfs,
        )
        .await;

        match chicago_rt_data {
            Ok(chicago_rt_data) => {
                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&worker_metadata.socket).await;

                if let Ok(aspen_client) = aspen_client {
                    println!(
                        "Successfully connected to Aspen at {}",
                        worker_metadata.socket
                    );

                    let tarpc_send_to_aspen = aspen_client
                        .from_alpenrose(
                            tarpc::context::current(),
                            worker_metadata.chateau_id.clone(),
                            String::from(feed_id),
                            Some(chicago_rt_data.vehicle_positions.encode_to_vec()),
                            Some(chicago_rt_data.trip_updates.encode_to_vec()),
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
                                "Successfully sent chicago data to {}, feed {} to chateau {}",
                                worker_metadata.socket, feed_id, worker_metadata.chateau_id
                            );
                        }
                        Err(e) => {
                            eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                        }
                    }
                } else {
                    eprintln!("Failed to connect to Aspen at {}", worker_metadata.socket);
                    return;
                }
            }
            _ => {
                eprintln!("Failed to fetch Chicago Transit data");
                eprintln!("{:?}", chicago_rt_data);
            }
        }
    } else {
        println!("No assigned node found for Chicago Transit");
    }
}
