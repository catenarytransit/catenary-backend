use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use flixbus_gtfs_realtime::aggregator::Aggregator;
use prost::Message;

pub async fn fetch_flixbus_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    aggregator: &Aggregator,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        println!("Starting global fetch for {}...", feed_id);
        let response = aggregator.fetch_all_live_data(64).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Aggregator error: {}", e),
            )
        })?;

        println!(
            "Global fetch complete for {}. VPs: {}, TUs: {}",
            feed_id,
            response.vehicle_positions.entity.len(),
            response.trip_updates.entity.len()
        );

        let vehicle_data = response.vehicle_positions.encode_to_vec();
        let trip_data = response.trip_updates.encode_to_vec();

        let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

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
                Some(200),
                duration_since_unix_epoch().as_millis() as u64,
            )
            .await;

        match tarpc_send_to_aspen {
            Ok(_) => {
                println!("Successfully sent Flixbus data sent to {}", feed_id);
            }
            Err(e) => {
                eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
            }
        }
    } else {
        println!("No assigned node found for Flixbus feed {}", feed_id);
    }

    Ok(())
}
