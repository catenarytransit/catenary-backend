use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use catenary::get_node_for_realtime_feed_id_kvclient;
use gtfs_realtime::FeedMessage;
use prost::Message;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DresdenResults {
    pub vehicle_positions: FeedMessage,
    pub trip_updates: FeedMessage,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteIdTable {
    pub route_id: String,
    pub short_name: String,
}

pub async fn fetch_tlms_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    client: &reqwest::Client,
) {
    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(worker_metadata) = fetch_assigned_node_meta {
        let worker_id = worker_metadata.worker_id;

        let route_ids_short_names_txt = match client.get("https://raw.githubusercontent.com/catenarytransit/betula-celtiberica-cdn/refs/heads/main/dresden_routes.json")
        .send()
        .await {
            Ok(response) => Ok(response.text().await.unwrap()),
            Err(e) => {
                Err(e)
            }
        };

        match route_ids_short_names_txt {
            Ok(route_ids_short_names_txt) => {
                let route_ids_short_names: Vec<RouteIdTable> =
                    match serde_json::from_str(&route_ids_short_names_txt) {
                        Ok(route_ids_short_names) => route_ids_short_names,
                        Err(e) => {
                            eprintln!("Failed to decode route ids and short names from Dresden!");
                            eprintln!("{:?}", e);
                            return;
                        }
                    };

                //make hashmap of short-name to route-id

                let route_id_to_short_name: std::collections::HashMap<String, String> =
                    route_ids_short_names
                        .iter()
                        .map(|route_id_table| {
                            (
                                route_id_table.short_name.clone(),
                                route_id_table.route_id.clone(),
                            )
                        })
                        .collect();

                // the 0 meaning region 0 aka dresden
                match match client
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
                    Ok(dresden_rt_data) => {
                        let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(
                            &worker_metadata.socket,
                        )
                        .await;

                        if let Ok(aspen_client) = aspen_client {
                            println!(
                                "Successfully connected to Aspen at {}",
                                worker_metadata.socket
                            );

                            let mut dresden_rt_data = dresden_rt_data;

                            //replace route ids properly

                            for entity in dresden_rt_data.vehicle_positions.entity.iter_mut() {
                                entity.vehicle.as_mut().map(|vehicle| {
                                    vehicle.trip.as_mut().map(|trip| {
                                        if let Some(existing_route_id) = &trip.route_id {
                                            if let Some(lookup) =
                                                route_id_to_short_name.get(existing_route_id)
                                            {
                                                trip.route_id = Some(lookup.clone());
                                            }
                                        }

                                        trip.trip_id = Some("Unknown".to_string());
                                    });
                                });
                            }

                            for entity in dresden_rt_data.trip_updates.entity.iter_mut() {
                                entity.trip_update.as_mut().map(|trip_update| {
                                    let trip = &mut trip_update.trip;

                                    if let Some(existing_route_id) = &trip.route_id {
                                        if let Some(lookup) =
                                            route_id_to_short_name.get(existing_route_id)
                                        {
                                            trip.route_id = Some(lookup.clone());
                                        }
                                    }

                                    trip.trip_id = Some("Unknown".to_string());
                                });
                            }

                            let dresden_rt_data = dresden_rt_data;

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
                                        "Successfully sent dresden f-tlms~rt data to {}, feed {} to chateau {}",
                                        worker_metadata.socket, feed_id, worker_metadata.chateau_id
                                    );
                                }
                                Err(e) => {
                                    eprintln!(
                                        "{}: Error sending data to {}: {}",
                                        feed_id, worker_id, e
                                    );
                                }
                            }
                        } else {
                            eprintln!("Failed to connect to Aspen at {}", worker_metadata.socket);
                            return;
                        }
                    }
                    _ => {
                        eprintln!("Failed to decode gtfs data from Dresden f-tlms~rt");
                        return;
                    }
                }
            }
            _ => {
                eprintln!("Failed to fetch route ids and short names from Dresden f-tlms~rt");
            }
        }
    } else {
        println!("No assigned node found for f-tlms~rt");
    }
}
