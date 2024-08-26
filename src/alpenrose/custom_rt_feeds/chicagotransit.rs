use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use prost::Message;
use std::io;
use zip::ZipArchive;

pub async fn fetch_chicago_data(
    etcd: &mut etcd_client::Client,
    feed_id: &str,
    client: &reqwest::Client,
) {
    let chicago_gtfs = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip";

    let schedule_response = client.get(chicago_gtfs).send().await;

    if let Ok(schedule_resp) = schedule_response {
        // Create a ZIP archive from the bytes
        let schedule_bytes = schedule_resp.bytes().await.unwrap();
        let mut archive = ZipArchive::new(io::Cursor::new(schedule_bytes)).unwrap();

        // Find and open the desired file
        let mut trips_file = archive
            .by_name("trips.txt")
            .expect("trips.txt doesn't exist");
        let mut buffer = Vec::new();
        io::copy(&mut trips_file, &mut buffer).unwrap();

        // Convert the buffer to a string
        let trips_content = String::from_utf8(buffer).unwrap();

        let fetch_assigned_node_meta = get_node_for_realtime_feed_id(etcd, feed_id).await;

        if let Some(worker_metadata) = fetch_assigned_node_meta {
            let socket_addr = std::net::SocketAddr::new(worker_metadata.ip.0, worker_metadata.ip.1);
            let worker_id = worker_metadata.worker_id;

            let chicago_rt_data = chicago_gtfs_rt::train_feed(
                client,
                "13f685e4b9054545b19470556103ec73",
                &trips_content,
            )
            .await;

            if let Ok(chicago_rt_data) = chicago_rt_data {
                let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr)
                    .await
                    .unwrap();

                let tarpc_send_to_aspen = aspen_client
                    .from_alpenrose(
                        tarpc::context::current(),
                        worker_metadata.chateau_id.clone(),
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
                            worker_metadata.ip.0, feed_id, worker_metadata.chateau_id
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
}
