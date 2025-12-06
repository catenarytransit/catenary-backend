use crate::RealtimeFeedFetch;
use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use etcd_client::KvClient;
use gtfs_structures::Gtfs;
use prost::Message;
use reqwest::Client;
use std::sync::Arc;
use unwire_gtfs_rt::{
    FeedId, fetch_all_dart_trip_updates, fetch_all_feed_trip_updates, fetch_dart_vehicles,
    fetch_feed_vehicles,
};

pub async fn fetch_unwire_dart_data(
    kv_client: &mut KvClient,
    feed_id: &str,
    _client: &Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(kv_client, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        // Fetch DART specific data
        let vehicles_res = fetch_dart_vehicles().await;
        let trips_res = fetch_all_dart_trip_updates().await;

        if let (Ok(vehicles), Ok(trips)) = (vehicles_res, trips_res) {
            let vehicle_data = vehicles.encode_to_vec();
            let trip_data = trips.encode_to_vec();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    data.chateau_id.clone(),
                    String::from(feed_id),
                    Some(vehicle_data),
                    Some(trip_data),
                    None, // No alerts
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
                    println!("Successfully sent DART/Unwire data sent to {}", feed_id);
                }
                Err(e) => {
                    eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                }
            }
        } else {
            eprintln!("Failed to fetch DART/Unwire data");
        }
    } else {
        println!("No assigned node found for DART/Unwire");
    }

    Ok(())
}

pub async fn fetch_unwire_fawa_data(
    kv_client: &mut KvClient,
    feed_id: &str,
    _client: &Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(kv_client, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        // Fetch FAWA (FWTA) specific data
        let vehicles_res = fetch_feed_vehicles(FeedId::Fwta).await;
        let trips_res = fetch_all_feed_trip_updates(FeedId::Fwta).await;

        if let (Ok(vehicles), Ok(trips)) = (vehicles_res, trips_res) {
            let vehicle_data = vehicles.encode_to_vec();
            let trip_data = trips.encode_to_vec();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await?;

            let tarpc_send_to_aspen = aspen_client
                .from_alpenrose(
                    tarpc::context::current(),
                    data.chateau_id.clone(),
                    String::from(feed_id),
                    Some(vehicle_data),
                    Some(trip_data),
                    None, // No alerts
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
                    println!("Successfully sent FAWA/Unwire data sent to {}", feed_id);
                }
                Err(e) => {
                    eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                }
            }
        } else {
            eprintln!("Failed to fetch FAWA/Unwire data");
        }
    } else {
        println!("No assigned node found for FAWA/Unwire");
    }

    Ok(())
}
