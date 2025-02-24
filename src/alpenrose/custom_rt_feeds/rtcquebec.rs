use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use prost::Message;

pub async fn fetch_rtc_data(
    etcd: &mut etcd_client::Client,
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
    client: &reqwest::Client,
) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let rtc_gtfs_rt_res =
            rtc_quebec_gtfs_rt::faire_les_donnees_gtfs_rt(gtfs, client.clone()).await;

        if let Ok(rtc_gtfs_rt_res) = rtc_gtfs_rt_res {
            if rtc_gtfs_rt_res.vehicles.is_none() || rtc_gtfs_rt_res.voyages.is_none() {
                eprintln!("Failed to fetch Rtc Quebec data, everything is empty");
                return;
            }

            let vehicles = rtc_gtfs_rt_res.vehicles.unwrap();
            let voyages = rtc_gtfs_rt_res.voyages.unwrap();

            if (vehicles.entity.len() == 0) || (voyages.entity.len() == 0) {
                eprintln!("Failed to fetch Rtc Quebec data, entities are empty");
                return;
            }

            //extract the binary data
            let vehicle_data = vehicles.encode_to_vec();
            let trip_data = voyages.encode_to_vec();
            //let alert_data = rtc_gtfs_rt_res.alertes.encode_to_vec();

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket)
                .await
                .unwrap();

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
                    None,
                    duration_since_unix_epoch().as_millis() as u64,
                )
                .await;

            match tarpc_send_to_aspen {
                Ok(_) => {
                    println!("Successfully sent Rtc Quebec data sent to {}", feed_id);
                }
                Err(e) => {
                    eprintln!("{}: Error sending data to {}: {}", feed_id, worker_id, e);
                }
            }
        } else {
            eprintln!("Failed to fetch Rtc Quebec data");
            eprintln!("{:?}", rtc_gtfs_rt_res.unwrap_err());
        }
    } else {
        println!("No assigned node found for Rtc Quebec");
    }
}
