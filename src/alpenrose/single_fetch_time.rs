use crate::KeyFormat;
use crate::RealtimeFeedFetch;
use catenary::ahash_fast_hash;
use catenary::aspen::lib::RealtimeFeedMetadataZookeeper;
use catenary::duration_since_unix_epoch;
use catenary::postgres_tools::CatenaryPostgresPool;
use dashmap::DashMap;
use futures::StreamExt;
use rand::seq::SliceRandom;
use reqwest::Response;
use scc::HashMap as SccHashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarpc::{client, context, tokio_serde::formats::Bincode};
use tokio::sync::RwLock;
use tokio_zookeeper::ZooKeeper;

async fn cleanup_response(
    response: Response,
    urltype: UrlType,
    feed_id: &str,
    hashes_of_data: Arc<SccHashMap<(String, UrlType), u64>>,
) -> Option<Vec<u8>> {
    match response.bytes().await {
        Ok(bytes_pre) => {
            let bytes = bytes_pre.as_ref().to_vec();

            let hash = ahash_fast_hash(&bytes);

            match hashes_of_data.get(&(feed_id.to_string(), urltype)) {
                Some(old_hash) => {
                    let old_hash = old_hash.get();

                    //if the data has not changed, don't send it
                    match hash == *old_hash {
                        true => None,
                        false => {
                            hashes_of_data
                                .entry((feed_id.to_string(), urltype))
                                .and_modify(|value| *value = hash)
                                .or_insert(hash);
                            Some(bytes)
                        }
                    }
                }
                None => Some(bytes),
            }
        }
        Err(_) => None,
    }
}

pub async fn single_fetch_time(
    client: reqwest::Client,
    assignments: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>>,
    last_fetch_per_feed: Arc<DashMap<String, Instant>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let start = Instant::now();

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

    let zk = Arc::new(zk);

    let assignments_lock = assignments.read().await;

    let hashes_of_data: Arc<SccHashMap<(String, UrlType), u64>> = Arc::new(SccHashMap::new());

    futures::stream::iter(assignments_lock.iter().map(|(feed_id, assignment)| {
        let client = client.clone();
        let zk = zk.clone();
        let hashes_of_data = Arc::clone(&hashes_of_data);
        let last_fetch_per_feed = last_fetch_per_feed.clone();
        async move {
            let start = Instant::now();

            let fetch_interval_ms = assignment.fetch_interval_ms.unwrap_or(10_000);

            if let Some(last_fetch) = last_fetch_per_feed.get(&feed_id.clone()) {
                let duration_since_last_fetch = last_fetch.elapsed().as_millis();

                if duration_since_last_fetch < fetch_interval_ms as u128 {
                    //skip because timeout has not been reached
                    return;
                }
            }

            let vehicle_positions_request =
                make_reqwest_for_url(UrlType::VehiclePositions, assignment, client.clone());

            let trip_updates_request =
                make_reqwest_for_url(UrlType::TripUpdates, assignment, client.clone());

            let alerts_request = make_reqwest_for_url(UrlType::Alerts, assignment, client.clone());

            //run all requests concurrently
            let vehicle_positions_future =
                run_optional_req(vehicle_positions_request, client.clone());
            let trip_updates_future = run_optional_req(trip_updates_request, client.clone());
            let alerts_future = run_optional_req(alerts_request, client.clone());

            println!("{}: Fetching data", feed_id);
            let (vehicle_positions_data, trip_updates_data, alerts_data) =
                futures::join!(vehicle_positions_future, trip_updates_future, alerts_future,);

            //send the data to aspen via tarpc

            let vehicle_positions_http_status = match &vehicle_positions_data {
                Some(Ok(response)) => Some(response.status().as_u16()),
                _ => None,
            };

            let trip_updates_http_status = match &trip_updates_data {
                Some(Ok(response)) => Some(response.status().as_u16()),
                _ => None,
            };

            let alerts_http_status = match &alerts_data {
                Some(Ok(response)) => Some(response.status().as_u16()),
                _ => None,
            };

            //lookup currently assigned realtime dataset in zookeeper
            let fetch_assigned_node_for_this_realtime_feed = zk
                .get_data(format!("/aspen_assigned_realtime_feed_ids/{}", feed_id).as_str())
                .await
                .unwrap();

            match fetch_assigned_node_for_this_realtime_feed {
                Some((bytes_realtime_id, stat)) => {
                    let data =
                        bincode::deserialize::<RealtimeFeedMetadataZookeeper>(&bytes_realtime_id)
                            .unwrap();

                    let worker_id = data.worker_id;

                    //send the data to the worker
                    println!(
                        "Attempting to send {} data to {} via tarpc",
                        feed_id, data.tailscale_ip
                    );
                    let socket_addr = std::net::SocketAddr::new(data.tailscale_ip, 40427);

                    let transport =
                        tarpc::serde_transport::tcp::connect(socket_addr, Bincode::default)
                            .await
                            .unwrap();

                    let aspen_client = catenary::aspen::lib::AspenRpcClient::new(
                        client::Config::default(),
                        transport,
                    )
                    .spawn();

                    if vehicle_positions_http_status == Some(200)
                        || trip_updates_http_status == Some(200)
                        || alerts_http_status == Some(200)
                    {
                        let tarpc_send_to_aspen = aspen_client
                            .from_alpenrose(
                                tarpc::context::current(),
                                data.chateau_id,
                                feed_id.clone(),
                                match vehicle_positions_data {
                                    Some(Ok(response)) => {
                                        cleanup_response(
                                            response,
                                            UrlType::VehiclePositions,
                                            feed_id,
                                            Arc::clone(&hashes_of_data),
                                        )
                                        .await
                                    }
                                    _ => None,
                                },
                                match trip_updates_data {
                                    Some(Ok(response)) => {
                                        cleanup_response(
                                            response,
                                            UrlType::TripUpdates,
                                            feed_id,
                                            Arc::clone(&hashes_of_data),
                                        )
                                        .await
                                    }
                                    _ => None,
                                },
                                match alerts_data {
                                    Some(Ok(response)) => {
                                        cleanup_response(
                                            response,
                                            UrlType::Alerts,
                                            feed_id,
                                            Arc::clone(&hashes_of_data),
                                        )
                                        .await
                                    }
                                    _ => None,
                                },
                                assignment.realtime_vehicle_positions.is_some(),
                                assignment.realtime_trip_updates.is_some(),
                                assignment.realtime_alerts.is_some(),
                                vehicle_positions_http_status,
                                trip_updates_http_status,
                                alerts_http_status,
                                duration_since_unix_epoch().as_millis() as u64,
                            )
                            .await;

                        match tarpc_send_to_aspen {
                            Ok(_) => {
                                println!(
                                    "{}: Successfully sent data sent to {}",
                                    feed_id, worker_id
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
                        println!("{}: No data to send", feed_id);
                    }
                }
                None => {
                    eprintln!("{} was not assigned to a worker", feed_id);
                }
            }

            let duration = start.elapsed();
            let duration = duration.as_secs_f64();
            println!("{}: {:.2?}", feed_id, duration);
        }
    }))
    .buffer_unordered(20)
    .collect::<Vec<()>>()
    .await;

    Ok(())
}

async fn run_optional_req(
    request: Option<reqwest::Request>,
    client: reqwest::Client,
) -> Option<Result<Response, Box<dyn std::error::Error + Sync + Send>>> {
    match request {
        Some(request) => Some(client.execute(request).await.map_err(|e| e.into())),
        None => None,
    }
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum UrlType {
    VehiclePositions,
    TripUpdates,
    Alerts,
}

pub fn make_reqwest_for_url(
    url_type: UrlType,
    assignment: &RealtimeFeedFetch,
    client: reqwest::Client,
) -> Option<reqwest::Request> {
    let url = match url_type {
        UrlType::VehiclePositions => assignment.realtime_vehicle_positions.as_ref(),
        UrlType::TripUpdates => assignment.realtime_trip_updates.as_ref(),
        UrlType::Alerts => assignment.realtime_alerts.as_ref(),
    };

    match url {
        Some(url) => {
            let mut request = client.get(url);

            if let Some(passwords) = &assignment.passwords {
                //choose random account to use
                if !passwords.is_empty() {
                    let password_info = passwords.choose(&mut rand::thread_rng());

                    if let Some(password_info) = password_info {
                        if password_info.password.len() == assignment.key_formats.len() {
                            let mut url_parameter_seq: Vec<(String, String)> = vec![];
                            for (key_index, key_format) in assignment.key_formats.iter().enumerate()
                            {
                                match key_format {
                                    KeyFormat::Header(header) => {
                                        request = request
                                            .header(header, &password_info.password[key_index]);
                                    }
                                    KeyFormat::UrlQuery(query) => {
                                        url_parameter_seq.push((
                                            query.to_string(),
                                            password_info.password[key_index].to_string(),
                                        ));
                                    }
                                }
                            }

                            request = request.query(&url_parameter_seq);
                        } else {
                            println!(
                                "Password length does not match key format length for feed_id: {}",
                                assignment.feed_id
                            );
                            return None;
                        }
                    }
                }
            }

            Some(request.build().unwrap())
        }
        None => None,
    }
}
