use crate::KeyFormat;
use crate::RealtimeFeedFetch;
use catenary::ahash_fast_hash;
use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use dashmap::DashMap;
use futures::StreamExt;
use lazy_static::lazy_static;
use rand::seq::SliceRandom;
use reqwest::Response;
use scc::HashMap as SccHashMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::custom_rt_feeds;

lazy_static! {
    static ref CUSTOM_FEEDS: HashSet<&'static str> = HashSet::from_iter([
        "f-amtrak~rt",
        "f-mta~nyc~rt~mnr",
        "f-mta~nyc~rt~lirr",
        "f-bus~dft~gov~uk~rt",
        "f-dp3-cta~rt",
        "f-viarail~rt"
    ]);
}

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

            match hashes_of_data.get(&(feed_id.to_string(), urltype.clone())) {
                Some(old_hash) => {
                    let old_hash = old_hash.get();

                    //if the data has not changed, don't send it
                    match hash == *old_hash {
                        true => None,
                        false => {
                            hashes_of_data
                                .entry((feed_id.to_string(), urltype.clone()))
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
    //   etcd_client_addresses: Arc<RwLock<Vec<String>>>
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let start = Instant::now();

    let amtrak_gtfs = gtfs_structures::GtfsReader::default()
        .read_shapes(false)
        .read_from_url_async("https://content.amtrak.com/content/gtfs/GTFS.zip")
        .await
        .unwrap();

    let amtrak_gtfs = Arc::new(amtrak_gtfs);

    let assignments_lock = assignments.read().await;

    let hashes_of_data: Arc<SccHashMap<(String, UrlType), u64>> = Arc::new(SccHashMap::new());

    futures::stream::iter(assignments_lock.iter().map(|(feed_id, assignment)| {
        let client = client.clone();
        let hashes_of_data = Arc::clone(&hashes_of_data);
        let last_fetch_per_feed = last_fetch_per_feed.clone();
        let amtrak_gtfs = Arc::clone(&amtrak_gtfs);

        async move {
            let start = Instant::now();

            let mut etcd = etcd_client::Client::connect(["localhost:2379"], None)
                .await
                .unwrap();

            let fetch_interval_ms = assignment.fetch_interval_ms.unwrap_or(1_000);

            if let Some(last_fetch) = last_fetch_per_feed.get(&feed_id.clone()) {
                let duration_since_last_fetch = last_fetch.elapsed().as_millis();

                if duration_since_last_fetch < fetch_interval_ms as u128 {
                    //skip because timeout has not been reached
                    return;
                }
            }

            last_fetch_per_feed.insert(feed_id.clone(), Instant::now());

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

            if !CUSTOM_FEEDS.contains(feed_id.as_str()) {
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

                if (vehicle_positions_http_status == Some(429))
                    || (trip_updates_http_status == Some(429))
                    || (alerts_http_status == Some(429))
                {
                    println!("{}: 429 Rate limited", feed_id);
                    return;
                }

                //lookup currently assigned realtime dataset in zookeeper
                let fetch_assigned_node_meta =
                    get_node_for_realtime_feed_id(&mut etcd, feed_id).await;

                match fetch_assigned_node_meta {
                    Some(data) => {
                        let worker_id = data.worker_id;

                        //send the data to the worker
                        println!(
                            "Attempting to send {} data to {} : {} via tarpc",
                            feed_id, data.ip.0, data.ip.1
                        );
                        let socket_addr = std::net::SocketAddr::new(data.ip.0, data.ip.1);

                        let aspen_client =
                            catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr)
                                .await
                                .unwrap();

                        if vehicle_positions_http_status == Some(200)
                            || trip_updates_http_status == Some(200)
                            || alerts_http_status == Some(200)
                        {
                            let tarpc_send_to_aspen = aspen_client
                                .from_alpenrose(
                                    tarpc::context::current(),
                                    data.chateau_id.clone(),
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
                                        "feed {}|chateau {}: Successfully sent data sent to {}",
                                        feed_id, data.chateau_id, worker_id
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
            } else {
                match feed_id.as_str() {
                    "f-amtrak~rt" => {
                        custom_rt_feeds::amtrak::fetch_amtrak_data(
                            &mut etcd,
                            feed_id,
                            &amtrak_gtfs,
                            &client,
                        )
                        .await;
                    }
                    "f-viarail~rt" => {
                        custom_rt_feeds::viarail::fetch_via_data(&mut etcd, feed_id, &client).await;
                    }
                    "f-mta~nyc~rt~lirr" => {
                        custom_rt_feeds::mta::fetch_mta_lirr_data(&mut etcd, feed_id, &client)
                            .await;
                    }
                    "f-mta~nyc~rt~mnr" => {
                        custom_rt_feeds::mta::fetch_mta_metronorth_data(
                            &mut etcd, feed_id, &client,
                        )
                        .await;
                    }
                    "f-bus~dft~gov~uk~rt" => {
                        custom_rt_feeds::uk::fetch_dft_bus_data(&mut etcd, feed_id, &client).await;
                    }
                    "f-dp3-cta~rt" => {
                        custom_rt_feeds::chicagotransit::fetch_chicago_data(
                            &mut etcd, feed_id, &client,
                        )
                        .await;
                    }
                    _ => {}
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
