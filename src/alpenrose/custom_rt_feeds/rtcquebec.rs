use catenary::duration_since_unix_epoch;
use catenary::get_node_for_realtime_feed_id_kvclient;
use chrono::Timelike;
use futures::future::join_all;
use prost::Message;
use rand::Rng;
use std::collections::HashSet;

const HTTP_PROXY_ADDRESSES: &[&str] = &[
    "4.239.94.226:3128",
    "149.56.24.51:3128",
    "16.52.47.20:3128",
    "51.79.80.224:3535",
    "167.114.98.246:9595",
    "15.223.57.73:3128",
    "35.183.180.110:3128",
    "158.69.59.135:80",
    "69.70.244.34:80",
    "23.132.28.133:3128",
    "204.83.205.117:3128",
    "74.48.160.189:3128",
    "142.93.202.130:3128",
    "23.227.38.125:80",
    "23.227.39.52:80",
    "23.227.38.128:80",
    "23.227.39.65:80",
];

pub async fn fetch_rtc_data(
    etcd: &mut etcd_client::KvClient,
    feed_id: &str,
    gtfs: &gtfs_structures::Gtfs,
    client: &reqwest::Client,
) {
    //test proxy addresses, filter out the ones that don't work

    let mut working_proxies = Vec::new();
    let mut futures = Vec::new();

    for proxy in HTTP_PROXY_ADDRESSES.iter() {
        let proxy = proxy.to_string();
        let future = async move {
            let proxy_url = proxy.clone();

            let proxy_client = reqwest::Client::builder()
                .proxy(reqwest::Proxy::http(&proxy_url).unwrap())
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap();

            let res = proxy_client
                .get("https://api.ipify.org?format=json")
                .send()
                .await;

            match res {
                Ok(_) => Some(proxy.to_string()),
                Err(_) => {
                    println!("Proxy {} is not working", &proxy);
                    None
                }
            }
        };

        futures.push(future);
    }

    let results = join_all(futures).await;

    for result in results {
        if let Some(proxy) = result {
            working_proxies.push(proxy);
        }
    }

    if working_proxies.len() == 0 {
        eprintln!("No working proxies found");
        return;
    }

    let client_proxy = reqwest::Client::builder()
        .proxy(
            reqwest::Proxy::http(
                working_proxies[rand::rng().random_range(0..working_proxies.len())].clone(),
            )
            .unwrap(),
        )
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();

    let fetch_assigned_node_meta = get_node_for_realtime_feed_id_kvclient(etcd, feed_id).await;

    if let Some(data) = fetch_assigned_node_meta {
        let worker_id = data.worker_id;

        let proxy_addresses_vec: Vec<String> =
            working_proxies.iter().map(|x| x.to_string()).collect();

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
