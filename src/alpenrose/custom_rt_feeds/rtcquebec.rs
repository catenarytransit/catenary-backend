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
    let proxy_pool = catenary::proxy_pool::global_proxy_pool().await;
    let proxy_urls = proxy_pool.proxy_urls();

    if proxy_urls.is_empty() {
        eprintln!("No proxies available for RTC Quebec");
        return;
    }

    // Strip protocol prefixes for HTTP-only proxies expected by rtc_quebec_gtfs_rt
    let http_proxies: Vec<String> = proxy_urls
        .iter()
        .filter(|u| u.starts_with("http://"))
        .map(|u| u.strip_prefix("http://").unwrap_or(u).to_string())
        .collect();

    if http_proxies.is_empty() {
        eprintln!("No HTTP proxies available for RTC Quebec");
        return;
    }

    let client_proxy = match proxy_pool.random_proxy_client() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to build proxy client for RTC Quebec: {}", e);
            return;
        }
    };

    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let proxy_addresses_vec: Vec<String> = http_proxies.clone();

        let rtc_gtfs_rt_res = rtc_quebec_gtfs_rt::faire_les_donnees_gtfs_rt(
            gtfs,
            client_proxy.clone(),
            Some(&proxy_addresses_vec),
        )
        .await;

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

                    if !list_of_route_ids.contains("1-802") {
                        eprintln!(
                            "Failed to fetch Rtc Quebec data, route 1-802 is missing during busy hours"
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;

    #[tokio::test]
    //  #[ignore] // This test makes network requests and can be slow.
    async fn test_proxies() {
        let mut futures = Vec::new();

        for &proxy_address in HTTP_PROXY_ADDRESSES {
            let future = async move {
                let proxy = match reqwest::Proxy::http(proxy_address) {
                    Ok(p) => p,
                    Err(e) => return (proxy_address, Err(e.to_string())),
                };
                let client = reqwest::Client::builder()
                    .proxy(proxy)
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap();

                match client.get("https://api.ipify.org").send().await {
                    Ok(res) if res.status().is_success() => (proxy_address, Ok(())),
                    Ok(res) => (proxy_address, Err(format!("Status: {}", res.status()))),
                    Err(e) => (proxy_address, Err(e.to_string())),
                }
            };
            futures.push(future);
        }

        let results = join_all(futures).await;

        for (proxy, result) in results {
            if let Err(e) = result {
                println!("Proxy {} failed: {}", proxy, e);
            } else {
                println!("Proxy {} is working.", proxy);
            }
        }
    }
}
