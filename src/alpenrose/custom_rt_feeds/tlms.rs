use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use gtfs_realtime::FeedMessage;
use prost::Message;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DresdenResults {
    pub vehicle_positions: FeedMessage,
    pub trip_updates: FeedMessage,
}

pub async fn fetch_tlms_data(
    etcd: &mut etcd_client::Client,
    feed_id: &str,
    client: &reqwest::Client,
) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id(etcd, feed_id).await;

    if let Some(worker_metadata) = fetch_assigned_node_meta {
        let worker_id = worker_metadata.worker_id;

        // the 0 meaning region 0 aka dresden
        if let Ok(dresden_rt_data) = match client
            .get("https://lizard.tlm.solutions/v1/gtfs/0")
            .send()
            .await
        {
            Ok(response) => response.json::<DresdenResults>(),
            Err(e) => {
                eprintln!("Failed to fetch Dresden Transit data");
                eprintln!("{:?}", e);
                return;
            }
        }
        .await
        {
            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&worker_metadata.socket)
                    .await
                    .unwrap();

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    worker_metadata.chateau_id.clone(),
                    String::from(feed_id),
                    Some(dresden_rt_data.vehicle_positions.encode_to_vec()),
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
                        "Successfully sent dresden data to {}, feed {} to chateau {}",
                        worker_metadata.socket, feed_id, worker_metadata.chateau_id
                    );
                }
                Err(e) => {
                    eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                }
            }
        } else {
            eprintln!("Failed to decode gtfs data from Dresden!");
            return;
        }
    } else {
        println!("No assigned node found for Chicago Transit");
    }
}
