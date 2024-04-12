use crate::KeyFormat;
use crate::RealtimeFeedFetch;
use catenary::postgres_tools::CatenaryPostgresPool;
use futures::StreamExt;
use rand::seq::SliceRandom;
use reqwest::Response;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use dashmap::DashMap;

pub async fn single_fetch_time(
    client: reqwest::Client,
    assignments: Arc<RwLock<HashMap<String, RealtimeFeedFetch>>>,
    last_fetch_per_feed: Arc<DashMap<String, Instant>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let start = Instant::now();

    let assignments_lock = assignments.read().await;

    futures::stream::iter(assignments_lock.iter().map(|(feed_id, assignment)| {
        let client = client.clone();
        let last_fetch_per_feed = last_fetch_per_feed.clone();
        async move {
            let start = Instant::now();

            let fetch_interval_ms = match assignment.fetch_interval_ms {
                Some(fetch_interval) => fetch_interval,
                None => 10_000,
            };

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

            let (vehicle_positions_data, trip_updates_data, alerts_data) =
                futures::join!(vehicle_positions_future, trip_updates_future, alerts_future,);

            //send the data to aspen via tarpc

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

enum UrlType {
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
                let password_info = passwords.choose(&mut rand::thread_rng()).unwrap();

                if password_info.password.len() == assignment.key_formats.len() {
                    let mut url_parameter_seq: Vec<(String, String)> = vec![];
                    for (key_index, key_format) in assignment.key_formats.iter().enumerate() {
                        match key_format {
                            KeyFormat::Header(header) => {
                                request =
                                    request.header(header, &password_info.password[key_index]);
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

            Some(request.build().unwrap())
        }
        None => None,
    }
}
