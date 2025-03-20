use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use chrono::Timelike;
use prost::Message;
use std::collections::HashSet;

pub async fn fetch_rtc_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
    client: &reqwest::Client,
) {
    let client = reqwest::Client::new();

    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let rtc_gtfs_rt_res =
            rtc_quebec_gtfs_rt::faire_les_donnees_gtfs_rt(gtfs, client.clone()).await;

        match rtc_gtfs_rt_res {
            Ok(rtc_gtfs_rt_res) => {
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

                if (voyages.entity.len() * 3) < vehicles.entity.len() {
                    eprintln!(
                        "Failed to fetch Rtc Quebec data, vehicles are more than 3 times the voyages"
                    );
                    return;
                }

                let list_of_route_ids: HashSet<String> = voyages
                    .entity
                    .iter()
                    .map(|x| x.trip_update.as_ref().unwrap().trip.route_id.clone())
                    .filter_map(|x| x)
                    .collect();

                //current hour quebec time

                let current_hour = chrono::Utc::now()
                    .with_timezone(&chrono_tz::America::Montreal)
                    .time()
                    .hour();

                //if between 08h and 22h

                if current_hour >= 8 && current_hour < 21 {
                    if !list_of_route_ids.contains("1-801") {
                        eprintln!(
                            "Failed to fetch Rtc Quebec data, route 1-801 is missing during busy hours"
                        );
                        return;
                    }

                    if list_of_route_ids.len() < 10 {
                        eprintln!(
                            "Failed to fetch Rtc Quebec data, route count low during busy hours"
                        );
                        return;
                    }
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
            }
            Err(rtc_err) => {
                eprintln!("Failed to fetch Rtc Quebec data {:?}", rtc_err);
            }
        }
    } else {
        println!("No assigned node found for Rtc Quebec");
    }
}
