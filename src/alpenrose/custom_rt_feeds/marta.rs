use crate::RealtimeFeedFetch;
use catenary::duration_since_unix_epoch;
use prost::Message;
use rand::seq::IndexedRandom;

pub async fn fetch_marta_data(
    realtime_feed_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::RealtimeFeedMetadataEtcd>,
    >,
    feed_id: &str,
    client: &reqwest::Client,
    converter: &marta_gtfs_rt::MartaGtfsRt,
    assignment: &RealtimeFeedFetch,
) {
    let Some(passwords) = &assignment.passwords else {
        eprintln!("{}: MARTA realtime feed has no password/API key", feed_id);
        return;
    };

    let Some(account) = passwords.choose(&mut rand::rng()) else {
        eprintln!("{}: MARTA realtime feed has an empty password list", feed_id);
        return;
    };

    // MARTA takes one API key as the `apiKey` query parameter. Keep this deliberately
    // consistent with chicagotransit.rs: choose one configured account and use its first key.
    if account.password.len() != 1 {
        eprintln!(
            "{}: MARTA realtime feed expected exactly one password value, got {}",
            feed_id,
            account.password.len()
        );
        return;
    }
    let api_key = &account.password[0];

    let Some(worker_metadata) = realtime_feed_cache.get(feed_id) else {
        eprintln!("{}: no assigned Aspen node found for MARTA", feed_id);
        return;
    };

    // Fetch with Alpenrose's client so API-key selection and network policy stay centralized,
    // then hand the decoded rows to marta-gtfs-rt's stateful matcher/converter.
    let response = match client
        .get(marta_gtfs_rt::MARTA_RAIL_REALTIME_URL)
        .query(&[("apiKey", api_key.as_str())])
        .send()
        .await
    {
        Ok(response) => response,
        Err(e) => {
            eprintln!("{}: failed to fetch MARTA realtime data: {}", feed_id, e);
            return;
        }
    };

    let http_status = response.status().as_u16();
    if !response.status().is_success() {
        eprintln!(
            "{}: MARTA realtime endpoint returned HTTP {}",
            feed_id, http_status
        );
        return;
    }

    let rows = match response.json::<Vec<marta_gtfs_rt::MartaTrainRow>>().await {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("{}: failed to decode MARTA realtime JSON: {}", feed_id, e);
            return;
        }
    };

    let realtime = converter.process_rows(rows);
    let aspen_client = match catenary::aspen::lib::spawn_aspen_client_from_ip(
        &worker_metadata.socket,
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!(
                "{}: failed to connect to Aspen at {}: {}",
                feed_id, worker_metadata.socket, e
            );
            return;
        }
    };

    let worker_id = worker_metadata.worker_id;
    let send_result = aspen_client
        .from_alpenrose(
            tarpc::context::current(),
            worker_metadata.chateau_id.clone(),
            String::from(feed_id),
            Some(realtime.vehicle_positions.encode_to_vec()),
            Some(realtime.trip_updates.encode_to_vec()),
            Some(realtime.alerts.encode_to_vec()),
            true,
            true,
            true,
            Some(http_status),
            Some(http_status),
            Some(http_status),
            duration_since_unix_epoch().as_millis() as u64,
        )
        .await;

    if let Err(e) = send_result {
        eprintln!("{}: error sending MARTA data to {}: {}", feed_id, worker_id, e);
    }
}
