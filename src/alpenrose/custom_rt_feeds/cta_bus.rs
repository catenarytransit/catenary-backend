use crate::RealtimeFeedFetch;
use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use prost::Message;
use rand::Rng;
use rand::seq::IndexedRandom;

pub async fn fetch_cta_bus_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
    assignment: &RealtimeFeedFetch,
) {
    if let Some(passwords) = &assignment.passwords {
        //choose random account to use
        if !passwords.is_empty() {
            let a_chosen_account = passwords.choose(&mut rand::rng());

            if let Some(a_chosen_account) = &a_chosen_account {
                // expecting single key in password list
                if !a_chosen_account.password.is_empty() {
                    let random_key = &a_chosen_account.password[0];

                    let fetch_assigned_node_meta =
                        get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

                    if let Some(worker_metadata) = fetch_assigned_node_meta {
                        let worker_id = worker_metadata.worker_id;

                        // Call external crate
                        let cta_bus_rt_data =
                            cta_bus_gtfs_realtime::get_cta_bus_gtfs_rt(gtfs, random_key).await;

                        match cta_bus_rt_data {
                            Ok(feed_message) => {
                                if feed_message.entity.is_empty() {
                                    eprintln!("{}: Fetched data is empty.", feed_id);
                                    return;
                                }

                                let aspen_client =
                                    catenary::aspen::lib::spawn_aspen_client_from_ip(
                                        &worker_metadata.socket,
                                    )
                                    .await;

                                if let Ok(aspen_client) = aspen_client {
                                    // Split FeedMessage into components if needed, or send as is?
                                    // Alpenrose usually expects separate vectors for VP, TU, Alerts.
                                    // The `feed_message` contains entities which can be mixed.
                                    // We need to segregate them or send them all in one if aspen supports it
                                    // Looking at chicagotransit.rs, it sends:
                                    // vehicle_positions.encode_to_vec(), trip_updates..., alerts...
                                    // cta-bus-gtfs-realtime returns a single FeedMessage with mixed entities.
                                    // We need to split them.

                                    let mut vps = gtfs_realtime::FeedMessage {
                                        header: feed_message.header.clone(),
                                        entity: vec![],
                                    };
                                    let mut tus = gtfs_realtime::FeedMessage {
                                        header: feed_message.header.clone(),
                                        entity: vec![],
                                    };
                                    let mut alerts = gtfs_realtime::FeedMessage {
                                        header: feed_message.header.clone(),
                                        entity: vec![],
                                    };

                                    for entity in feed_message.entity {
                                        if entity.vehicle.is_some() {
                                            vps.entity.push(entity.clone());
                                        }
                                        if entity.trip_update.is_some() {
                                            tus.entity.push(entity.clone());
                                        }
                                        if entity.alert.is_some() {
                                            alerts.entity.push(entity.clone());
                                        }
                                    }

                                    let tarpc_send_to_aspen = aspen_client
                                        .from_alpenrose(
                                            tarpc::context::current(),
                                            worker_metadata.chateau_id.clone(),
                                            String::from(feed_id),
                                            Some(vps.encode_to_vec()),
                                            Some(tus.encode_to_vec()),
                                            Some(alerts.encode_to_vec()),
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
                                            // success
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "{}: Error sending data to {}: {}",
                                                feed_id, worker_id, e
                                            );
                                        }
                                    }
                                } else {
                                    eprintln!(
                                        "Failed to connect to Aspen at {}",
                                        worker_metadata.socket
                                    );
                                    return;
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to fetch CTA Bus data: {}", e);
                            }
                        }
                    } else {
                        println!("No assigned node found for CTA Bus {}", feed_id);
                    }
                }
            } else {
                eprintln!("Could not choose an account for CTA Bus");
            }
        }
    }
}
