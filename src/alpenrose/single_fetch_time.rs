use crate::KeyFormat;
use crate::RealtimeFeedFetch;
use crate::custom_rt_feeds;
use catenary::ahash_fast_hash;
use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id;
use catenary::get_node_for_realtime_feed_id_kvclient;
use dashmap::DashMap;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use futures::StreamExt;
use gtfs_realtime::alert;
use lazy_static::lazy_static;
use rand::prelude::*;
use rand::seq::SliceRandom;
use reqwest::Response;
use scc::HashMap as SccHashMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

lazy_static! {
    static ref CUSTOM_FEEDS: HashSet<&'static str> = HashSet::from_iter([
        "f-amtrak~rt",
        "f-mta~nyc~rt~mnr",
        "f-mta~nyc~rt~lirr",
        "f-bus~dft~gov~uk~rt",
        "f-dp3-cta~rt",
        "f-viarail~rt",
        "f-tlms~rt",
        //"f-uc~irvine~anteater~express~rt",
        "f-metrolinktrains~extra~rt",
        "f-rtcquebec~rt",
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
    request_limit: usize,
    client: reqwest::Client,
    assignments: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>>,
    last_fetch_per_feed: Arc<DashMap<String, Instant>>,
    amtrak_gtfs: Arc<gtfs_structures::Gtfs>, //   etcd_client_addresses: Arc<RwLock<Vec<String>>>
    chicago_text_str: Arc<Option<String>>,
    rtcquebec_gtfs: Arc<Option<gtfs_structures::Gtfs>>,
    etcd_urls: &Vec<&str>,
    etcd_connection_options: &Option<etcd_client::ConnectOptions>,
    lease_id: &i64,
    hashes_of_data: Arc<SccHashMap<(String, UrlType), u64>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let start = Instant::now();

    let assignments_lock = assignments.read().await;

    let mut etcd = etcd_client::Client::connect(etcd_urls, etcd_connection_options.clone())
        .await
        .unwrap();

    futures::stream::iter(assignments_lock.iter().map(|(feed_id, assignment)| {
        let client = client.clone();
        let hashes_of_data = Arc::clone(&hashes_of_data);
        let last_fetch_per_feed = last_fetch_per_feed.clone();
        let amtrak_gtfs = Arc::clone(&amtrak_gtfs);
        let rtcquebec_gtfs = rtcquebec_gtfs.clone();
        let chicago_text_str = chicago_text_str.clone();

        let mut kv_client = etcd.kv_client();
        let mut lease_client = etcd.lease_client();

        async move {
            let start = Instant::now();

            let fetch_interval_ms = assignment.fetch_interval_ms.unwrap_or(1_000);

            if let Some(last_fetch) = last_fetch_per_feed.get(&feed_id.clone()) {
                let duration_since_last_fetch = last_fetch.elapsed().as_millis();

                if duration_since_last_fetch < fetch_interval_ms as u128 {
                    //skip because timeout has not been reached
                    return;
                }
            }

            if let Some(trip_url) = &assignment.realtime_trip_updates {
                if trip_url.contains("api.goswift.ly") && assignment.passwords.is_none() {
                    println!("Skipping {} because no password provided", feed_id);

                    return;
                }
            }

            if let Some(alert_url) = &assignment.realtime_alerts {
                if alert_url.contains("api.goswift.ly") && assignment.passwords.is_none() {
                    println!("Skipping {} because no password provided", feed_id);

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

            // println!("{}: Fetching data", feed_id);
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
                    get_node_for_realtime_feed_id_kvclient(&mut kv_client, feed_id).await;

                match fetch_assigned_node_meta {
                    Some(data) => {
                        let worker_id = data.worker_id;

                        //send the data to the worker
                        //  println!(
                        //      "Attempting to send {} data to {} via tarpc",
                        //      feed_id, data.socket
                        // );

                        let aspen_client =
                            catenary::aspen::lib::spawn_aspen_client_from_ip(&data.socket).await;

                        match aspen_client {
                            Ok(aspen_client) => {
                                if vehicle_positions_http_status == Some(200)
                                    || trip_updates_http_status == Some(200)
                                    || alerts_http_status == Some(200)
                                {
                                    let vehicle_positions_cleanup = match vehicle_positions_data {
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
                                    };

                                    let trip_updates_cleanup = match trip_updates_data {
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
                                    };

                                    let mut alerts_cleanup = match alerts_data {
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
                                    };

                                    let mut alert_dupe_trips = false;

                                    if alerts_cleanup == trip_updates_cleanup {
                                        alerts_cleanup = None;
                                        alert_dupe_trips = true;
                                    }

                                    //map compression for all data

                                    let vehicle_positions_cleanup =
                                        vehicle_positions_cleanup.map(|v| {
                                            let mut encoder =
                                                ZlibEncoder::new(Vec::new(), Compression::new(2));
                                            encoder.write_all(v.as_slice()).unwrap();
                                            encoder.finish().unwrap()
                                        });

                                    let trip_updates_cleanup = trip_updates_cleanup.map(|v| {
                                        let mut encoder =
                                            ZlibEncoder::new(Vec::new(), Compression::new(2));
                                        encoder.write_all(v.as_slice()).unwrap();
                                        encoder.finish().unwrap()
                                    });

                                    let alerts_cleanup = alerts_cleanup.map(|v| {
                                        let mut encoder =
                                            ZlibEncoder::new(Vec::new(), Compression::new(2));
                                        encoder.write_all(v.as_slice()).unwrap();
                                        encoder.finish().unwrap()
                                    });

                                    let tarpc_send_to_aspen = aspen_client
                                        .from_alpenrose_compressed(
                                            tarpc::context::current(),
                                            data.chateau_id.clone(),
                                            feed_id.clone(),
                                            vehicle_positions_cleanup,
                                            trip_updates_cleanup,
                                            alerts_cleanup,
                                            assignment.realtime_vehicle_positions.is_some(),
                                            assignment.realtime_trip_updates.is_some(),
                                            assignment.realtime_alerts.is_some(),
                                            vehicle_positions_http_status,
                                            trip_updates_http_status,
                                            alerts_http_status,
                                            duration_since_unix_epoch().as_millis() as u64,
                                            alert_dupe_trips,
                                        )
                                        .await;

                                    match tarpc_send_to_aspen {
                                        Ok(_) => {
                                            //      println!(
                                            //  "feed {}|chateau {}: Successfully sent data sent to {}",
                                            //  feed_id, data.chateau_id, worker_id
                                            // );
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "{}: Error sending data to {}: {}",
                                                feed_id, worker_id, e
                                            );
                                        }
                                    }
                                } else {
                                    println!(
                                        "HTTP statuses for feed {}, v {:?}, t{:?}, a{:?}",
                                        feed_id,
                                        vehicle_positions_http_status,
                                        trip_updates_http_status,
                                        alerts_http_status
                                    );
                                }
                            }
                            Err(aspen_connection_error) => {
                                eprintln!("aspen connection error: {:#?}", aspen_connection_error);
                            }
                        };
                    }
                    None => {
                        eprintln!("{} was not assigned to a worker", feed_id);
                    }
                }
            } else {
                match feed_id.as_str() {
                    "f-amtrak~rt" => {
                        custom_rt_feeds::amtrak::fetch_amtrak_data(
                            &mut kv_client,
                            feed_id,
                            &amtrak_gtfs,
                            &client,
                        )
                        .await;
                    }
                    "f-viarail~rt" => {
                        custom_rt_feeds::viarail::fetch_via_data(&mut kv_client, feed_id, &client)
                            .await;
                    }
                    "f-mta~nyc~rt~lirr" => {
                        custom_rt_feeds::mta::fetch_mta_lirr_data(&mut kv_client, feed_id, &client)
                            .await;
                    }
                    "f-mta~nyc~rt~mnr" => {
                        custom_rt_feeds::mta::fetch_mta_metronorth_data(
                            &mut kv_client,
                            feed_id,
                            &client,
                        )
                        .await;
                    }
                    "f-metrolinktrains~extra~rt" => {
                        custom_rt_feeds::metrolink_extra::fetch_data(
                            &mut kv_client,
                            feed_id,
                            &client,
                        )
                        .await
                    }
                    "f-bus~dft~gov~uk~rt" => {
                        custom_rt_feeds::uk::fetch_dft_bus_data(&mut kv_client, feed_id, &client)
                            .await;
                    }
                    "f-dp3-cta~rt" => match chicago_text_str.as_ref() {
                        Some(chicago_text_str) => {
                            custom_rt_feeds::chicagotransit::fetch_chicago_data(
                                &mut kv_client,
                                feed_id,
                                &client,
                                chicago_text_str.as_str(),
                            )
                            .await;
                        }
                        None => {}
                    },
                    "f-tlms~rt" => {
                        custom_rt_feeds::tlms::fetch_tlms_data(&mut kv_client, feed_id, &client)
                            .await;
                    }
                    "f-rtcquebec~rt" => {

                            if let Some(rtcquebec_gtfs) = rtcquebec_gtfs.as_ref() {
                                custom_rt_feeds::rtcquebec::fetch_rtc_data(
                                    &mut kv_client,
                                    feed_id,
                                    rtcquebec_gtfs,
                                    &client,
                                )
                                .await;
                            }
                    }
                    //    "f-uc~irvine~anteater~express~rt" => {
                    //       custom_rt_feeds::uci::fetch_uci_data(&mut etcd, feed_id).await;
                    //   }
                    _ => {}
                }
            }

            let duration = start.elapsed();
            let duration = duration.as_secs_f64();
            //println!("{}: took {:.3?}s", feed_id, duration);

            //renew lease
            if rand::rng().random_bool(0.1) {
                let _ = lease_client.keep_alive(*lease_id).await;
            }
        }
    }))
    .buffer_unordered(request_limit)
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
            if url.contains("raildata.njtransit.com") {
                //i really dont care about this key.... so what?
                let form = reqwest::multipart::Form::new().text("token", "638671989162459331");

                return Some(
                    client
                        .request(reqwest::Method::POST, url)
                        .multipart(form)
                        .build()
                        .unwrap(),
                );
            }

            let mut request = client.get(url);

            request = request.header(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
            );

            request = request.header("Cache-Control", "no-cache");

            if let Some(passwords) = &assignment.passwords {
                //choose random account to use
                if !passwords.is_empty() {
                    let password_info = passwords.choose(&mut rand::rng());

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
