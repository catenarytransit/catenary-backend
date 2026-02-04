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
use flixbus_gtfs_realtime::aggregator::Aggregator;
use futures::StreamExt;
use gtfs_realtime::alert;
use lazy_static::lazy_static;
use leapfrog::hashmap;
use rand::prelude::*;
use rand::seq::IndexedRandom;
use rand::seq::SliceRandom;
use reqwest::Response;
use scc::HashMap as SccHashMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::RwLock;

lazy_static! {
    static ref CUSTOM_FEEDS: HashSet<&'static str> = HashSet::from_iter([
        "f-amtrak~rt",
        "f-mta~nyc~rt~mnr",
        "f-mta~nyc~rt~lirr",
        "f-bus~dft~gov~uk~rt",
        "f-dp3-cta~rt",
        "f-dp3-cta~bus~rt",
        "f-viarail~rt",
        "f-tlms~rt",
        //"f-uc~irvine~anteater~express~rt",
        "f-metrolinktrains~extra~rt",
        "f-rtcquebec~rt",
        "f-f244-sto~rt",
        "f-northern~indiana~commuter~transportation~district~catenary~alerts~rt",
        "f-9vff-fortworthtransportationauthority~rt~catenary~unwire",
        "f-9vg-dart~rt~catenary~unwire",
        "f-dr7f-greaterbridgeporttransit~rt",
        "f-flixbus~eu~rt",
        "f-flixbus~us~rt",
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

            match hashes_of_data
                .get_async(&(feed_id.to_string(), urltype.clone()))
                .await
            {
                Some(old_hash) => {
                    let old_hash = old_hash.get();

                    //if the data has not changed, don't send it
                    match hash == *old_hash {
                        true => None,
                        false => {
                            hashes_of_data
                                .entry_sync((feed_id.to_string(), urltype.clone()))
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

const HTTP_PROXY_ADDRESSES: [&str; 7] = [
    "47.176.240.250:4228",
    "74.50.96.247:8888",
    "107.167.4.130:9998",
    "35.243.0.245:10051",
    "142.171.224.165:8080",
    "142.171.127.232:7890",
    "35.243.0.217:10007",
];

pub async fn single_fetch_time(
    request_limit: usize,
    client: reqwest::Client,
    assignments: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>>,
    last_fetch_per_feed: Arc<DashMap<String, Instant>>,
    amtrak_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>, //   etcd_client_addresses: Arc<RwLock<Vec<String>>>
    chicago_text_str: Arc<RwLock<Option<String>>>,
    chicago_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>,
    rtcquebec_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>,
    bridgeport_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>,
    mnr_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>,
    via_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>,
    cta_bus_gtfs: Arc<RwLock<Option<gtfs_structures::Gtfs>>>,
    flixbus_us_aggregator: Arc<RwLock<Option<Aggregator>>>,
    flixbus_eu_aggregator: Arc<RwLock<Option<Aggregator>>>,
    etcd_urls: Arc<Vec<String>>,
    etcd_connection_options: Option<etcd_client::ConnectOptions>,
    lease_id: i64,
    hashes_of_data: Arc<SccHashMap<(String, UrlType), u64>>,
    too_many_requests_log: Arc<SccHashMap<String, std::time::Instant>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let start = Instant::now();

    let assignments_lock = assignments.read().await;

    let etcd = etcd_client::Client::connect(etcd_urls.as_ref(), etcd_connection_options).await?;

    let hashmap_of_connections: Arc<
        SccHashMap<std::net::SocketAddr, catenary::aspen::lib::AspenRpcClient>,
    > = Arc::new(scc::HashMap::new());

    futures::stream::iter(
        assignments_lock
            .iter()
            .map(|(feed_id, assignment)| {
                let feed_id = feed_id.clone();
                let assignment = assignment.clone();

                (feed_id, assignment)
            })
            .map(|(feed_id, assignment)| {
                let client = client.clone();
                let hashes_of_data = Arc::clone(&hashes_of_data);
                let last_fetch_per_feed = last_fetch_per_feed.clone();
                let amtrak_gtfs = Arc::clone(&amtrak_gtfs);
                let rtcquebec_gtfs = rtcquebec_gtfs.clone();
                let bridgeport_gtfs = bridgeport_gtfs.clone();
                let chicago_text_str = chicago_text_str.clone();
                let chicago_gtfs = chicago_gtfs.clone();
                let mnr_gtfs = mnr_gtfs.clone();
                let via_gtfs = via_gtfs.clone();
                let cta_bus_gtfs = cta_bus_gtfs.clone();
                let flixbus_us_aggregator = flixbus_us_aggregator.clone();
                let flixbus_eu_aggregator = flixbus_eu_aggregator.clone();

                let mut kv_client = etcd.kv_client();
                let mut lease_client = etcd.lease_client();

                let mut last_429_elapsed: Option<Duration> = None;

                let hashmap_of_connections = hashmap_of_connections.clone();

                let too_many_requests_log = Arc::clone(&too_many_requests_log);

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

                    if let Some(fetch_time_of_429) =
                        too_many_requests_log.get_async(feed_id.as_str()).await
                    {
                        let fetch_time_of_429 = fetch_time_of_429.get();

                        let new_now = Instant::now();
                        last_429_elapsed = Some(new_now.duration_since(*fetch_time_of_429));
                        if new_now.duration_since(*fetch_time_of_429) < Duration::from_secs(10) {
                            //dont fetch again if 429 was recent

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

                    let use_proxy = match last_429_elapsed {
                        Some(duration) => {
                            duration <= Duration::from_secs(300) && assignment.passwords.is_none()
                        }
                        None => false,
                    };

                    let client_for_v_req = match use_proxy {
                        true => {
                            let proxy_url = HTTP_PROXY_ADDRESSES.choose(&mut rand::rng()).unwrap();

                            reqwest::ClientBuilder::new()
                                .proxy(reqwest::Proxy::http(*proxy_url).unwrap())
                                //timeout queries
                                .timeout(Duration::from_secs(10))
                                .connect_timeout(Duration::from_secs(10))
                                .deflate(true)
                                .gzip(true)
                                .brotli(true)
                                .cookie_store(true)
                                .danger_accept_invalid_certs(true)
                                .build()
                                .unwrap()
                        }
                        false => client.clone(),
                    };

                    let vehicle_positions_request = make_reqwest_for_url(
                        UrlType::VehiclePositions,
                        &assignment,
                        client_for_v_req.clone(),
                    );

                    let client_for_t_req = match use_proxy {
                        true => {
                            let proxy_url = HTTP_PROXY_ADDRESSES.choose(&mut rand::rng()).unwrap();

                            reqwest::ClientBuilder::new()
                                .proxy(reqwest::Proxy::http(*proxy_url).unwrap())
                                //timeout queries
                                .timeout(Duration::from_secs(10))
                                .connect_timeout(Duration::from_secs(10))
                                .deflate(true)
                                .gzip(true)
                                .brotli(true)
                                .cookie_store(true)
                                .danger_accept_invalid_certs(true)
                                .build()
                                .unwrap()
                        }
                        false => client.clone(),
                    };

                    let trip_updates_request = make_reqwest_for_url(
                        UrlType::TripUpdates,
                        &assignment,
                        client_for_t_req.clone(),
                    );

                    let client_for_a_req = match use_proxy {
                        true => {
                            let proxy_url = HTTP_PROXY_ADDRESSES.choose(&mut rand::rng()).unwrap();

                            reqwest::ClientBuilder::new()
                                .proxy(reqwest::Proxy::http(*proxy_url).unwrap())
                                //timeout queries
                                .timeout(Duration::from_secs(10))
                                .connect_timeout(Duration::from_secs(10))
                                .deflate(true)
                                .gzip(true)
                                .brotli(true)
                                .cookie_store(true)
                                .danger_accept_invalid_certs(true)
                                .build()
                                .unwrap()
                        }
                        false => client.clone(),
                    };

                    let alerts_request = make_reqwest_for_url(
                        UrlType::Alerts,
                        &assignment,
                        client_for_a_req.clone(),
                    );

                    //run all requests concurrently
                    let vehicle_positions_future =
                        run_optional_req(vehicle_positions_request, client_for_v_req.clone());
                    let trip_updates_future =
                        run_optional_req(trip_updates_request, client_for_t_req.clone());
                    let alerts_future = run_optional_req(alerts_request, client_for_a_req.clone());

                    // println!("{}: Fetching data", feed_id);
                    let (vehicle_positions_data, trip_updates_data, alerts_data) = futures::join!(
                        vehicle_positions_future,
                        trip_updates_future,
                        alerts_future,
                    );

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
                            println!("{}: 429 Rate limited", &feed_id);

                            let _ = too_many_requests_log
                                .insert_async(feed_id.clone(), std::time::Instant::now())
                                .await;
                        }

                        //lookup currently assigned realtime dataset in zookeeper
                        let fetch_assigned_node_meta =
                            get_node_for_realtime_feed_id_kvclient(&mut kv_client, &feed_id).await;

                        match fetch_assigned_node_meta {
                            Some(data) => {
                                let worker_id = data.worker_id;

                                //send the data to the worker
                                //  println!(
                                //      "Attempting to send {} data to {} via tarpc",
                                //      feed_id, data.socket
                                // );

                                let mut aspen_client = None;
                                if let Some(aspen_client_get) =
                                    hashmap_of_connections.get_sync(&data.socket)
                                {
                                    aspen_client = Some(aspen_client_get.clone());
                                } else {
                                    let aspen_client_new =
                                        catenary::aspen::lib::spawn_aspen_client_from_ip(
                                            &data.socket,
                                        )
                                        .await;

                                    match aspen_client_new {
                                        Ok(aspen_client_new) => {
                                            let _ = hashmap_of_connections
                                                .upsert_sync(data.socket, aspen_client_new.clone());

                                            aspen_client = Some(aspen_client_new);
                                        }
                                        Err(aspen_connection_error) => {
                                            if let Some(aspen_client_get) =
                                                hashmap_of_connections.get_sync(&data.socket)
                                            {
                                                aspen_client = Some(aspen_client_get.clone());
                                            } else {
                                                eprintln!(
                                                    "aspen connection error: {:#?}",
                                                    aspen_connection_error
                                                );
                                            }
                                        }
                                    }
                                }

                                let aspen_client = aspen_client;

                                if let Some(aspen_client) = aspen_client {
                                    if vehicle_positions_http_status == Some(200)
                                        || trip_updates_http_status == Some(200)
                                        || alerts_http_status == Some(200)
                                    {
                                        let vehicle_positions_cleanup = match vehicle_positions_data
                                        {
                                            Some(Ok(response)) => {
                                                cleanup_response(
                                                    response,
                                                    UrlType::VehiclePositions,
                                                    &feed_id,
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
                                                    &feed_id,
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
                                                    &feed_id,
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

                                        let vehicle_positions_cleanup = vehicle_positions_cleanup
                                            .map(|v| {
                                                let mut encoder = ZlibEncoder::new(
                                                    Vec::new(),
                                                    Compression::new(2),
                                                );
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
                            }
                            None => {
                                eprintln!("{} was not assigned to a worker", feed_id);
                            }
                        }
                    } else {
                        match feed_id.as_str() {
                            "f-amtrak~rt" => {
                                let amtrak_lock = amtrak_gtfs.read().await;
                                if let Some(gtfs) = amtrak_lock.as_ref() {
                                    let _ = custom_rt_feeds::amtrak::fetch_amtrak_data(
                                        &mut kv_client,
                                        &feed_id,
                                        gtfs,
                                        &client,
                                    )
                                    .await;
                                }
                            }
                            "f-9vg-dart~rt~catenary~unwire" => {
                                let _ = custom_rt_feeds::unwire::fetch_unwire_dart_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await;
                            }
                            "f-9vff-fortworthtransportationauthority~rt~catenary~unwire" => {
                                let _ = custom_rt_feeds::unwire::fetch_unwire_fawa_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await;
                            }

                            "f-viarail~rt" => {
                                let via_lock = via_gtfs.read().await;
                                if let Some(gtfs) = via_lock.as_ref() {
                                    custom_rt_feeds::viarail::fetch_via_data(
                                        &mut kv_client,
                                        &feed_id,
                                        &client,
                                        gtfs,
                                    )
                                    .await;
                                }
                            }
                            "f-mta~nyc~rt~lirr" => {
                                let _ = custom_rt_feeds::mta::fetch_mta_lirr_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await;
                            }
                            "f-mta~nyc~rt~mnr" => {
                                let mnr_lock = mnr_gtfs.read().await;
                                if let Some(gtfs) = mnr_lock.as_ref() {
                                    let _ = custom_rt_feeds::mta::fetch_mta_metronorth_data(
                                        &mut kv_client,
                                        &feed_id,
                                        &client,
                                        gtfs,
                                    )
                                    .await;
                                }
                            }
                            "f-metrolinktrains~extra~rt" => {
                                custom_rt_feeds::metrolink_extra::fetch_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await
                            }
                            "f-bus~dft~gov~uk~rt" => {
                                let _ = custom_rt_feeds::uk::fetch_dft_bus_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await;
                            }
                            "f-dp3-cta~rt" => {
                                let chicago_text_lock = chicago_text_str.read().await;
                                let chicago_gtfs_lock = chicago_gtfs.read().await;
                                if let Some(chicago_text_str) = chicago_text_lock.as_ref() {
                                    if let Some(chicago_gtfs) = chicago_gtfs_lock.as_ref() {
                                        custom_rt_feeds::chicagotransit::fetch_chicago_data(
                                            &mut kv_client,
                                            &feed_id,
                                            &client,
                                            chicago_text_str.as_str(),
                                            chicago_gtfs,
                                            &assignment,
                                        )
                                        .await;
                                    }
                                }
                            },
                             "f-rtcquebec~rt" => {
                                let rtc_lock = rtcquebec_gtfs.read().await;
                                if let Some(gtfs) = rtc_lock.as_ref() {
                                    custom_rt_feeds::rtcquebec::fetch_rtc_data(
                                        &mut kv_client,
                                        &feed_id,
                                        gtfs,
                                        &client,
                                    )
                                    .await;
                                }
                            }
                            "f-dr7f-greaterbridgeporttransit~rt" => {
                                let lock = bridgeport_gtfs.read().await;
                                if let Some(gtfs) = lock.as_ref() {
                                    if let Err(e) = custom_rt_feeds::bridgeport::fetch_bridgeport_data(
                                        &mut kv_client,
                                        &feed_id,
                                        &client,
                                        gtfs,
                                    )
                                    .await {
                                        eprintln!("Error fetching bridgeport: {}", e);
                                    }
                                }
                            }
                            "f-tlms~rt" => {
                                custom_rt_feeds::tlms::fetch_tlms_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await;
                            }
                            "f-dp3-cta~bus~rt" | "f-dp3-cta~bus~rt" => {
                                let cta_bus_lock = cta_bus_gtfs.read().await;
                                if let Some(gtfs) = cta_bus_lock.as_ref() {
                                    custom_rt_feeds::cta_bus::fetch_cta_bus_data(
                                        &mut kv_client,
                                        &feed_id,
                                        gtfs,
                                        &assignment,
                                    )
                                    .await;
                                }
                            }
                            "f-f244-sto~rt" => {
                                let _ = custom_rt_feeds::sto_ca::recuperer_les_donnees_sto(
                                    &mut kv_client,
                                    &client,
                                    &feed_id,
                                )
                                .await;
                            }
                            "f-northern~indiana~commuter~transportation~district~catenary~alerts~rt" => {
                                let _ = custom_rt_feeds::northern_indiana::fetch_northern_indiana_data(
                                    &mut kv_client,
                                    &feed_id,
                                    &client,
                                )
                                .await;
                            }
                            "f-flixbus~us~rt" => {
                                let lock = flixbus_us_aggregator.read().await;
                                if let Some(aggregator) = lock.as_ref() {
                                    if let Err(e) = custom_rt_feeds::flixbus::fetch_flixbus_data(
                                        &mut kv_client,
                                        &feed_id,
                                        aggregator,
                                    ).await {
                                        eprintln!("Error fetching flixbus us: {}", e);
                                    }
                                }
                            }
                            "f-flixbus~eu~rt" => {
                                let lock = flixbus_eu_aggregator.read().await;
                                if let Some(aggregator) = lock.as_ref() {
                                    if let Err(e) = custom_rt_feeds::flixbus::fetch_flixbus_data(
                                        &mut kv_client,
                                        &feed_id,
                                        aggregator,
                                    ).await {
                                        eprintln!("Error fetching flixbus eu: {}", e);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    let duration = start.elapsed();
                    let duration = duration.as_secs_f64();
                    //println!("{}: took {:.3?}s", feed_id, duration);

                    //renew lease
                    if rand::rng().random_bool(0.1) {
                        let _ = lease_client.keep_alive(lease_id).await;
                    }
                }
            }),
    )
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

            // many servers do not accept a response without a user agent
            request = request.header(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
            );

            //on some azure api systems and api gateways, a no-cache value is required to return fresh data.
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

            match request.build() {
                Ok(request) => Some(request),
                Err(e) => {
                    eprintln!("Error building request: {}", e);
                    None
                }
            }
        }
        None => None,
    }
}
