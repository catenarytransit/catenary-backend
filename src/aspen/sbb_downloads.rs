use catenary::catenaryconfig;
use catenary::sbb_formation_types::{SbbFormationApiResponse, SbbFormationData};
use reqwest::Client;
use scc::HashMap as SccHashMap;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};

pub type SbbFormationStore = Arc<RwLock<HashMap<String, Option<SbbFormationData>>>>;

const PERSISTENCE_PATH: &str = "sbb_formations_cache.json";
const EVICTION_HOURS: i64 = 72;

pub fn load_store_from_disk() -> HashMap<String, Option<SbbFormationData>> {
    let path = Path::new(PERSISTENCE_PATH);
    if path.exists() {
        if let Ok(contents) = fs::read_to_string(path) {
            if let Ok(store) =
                serde_json::from_str::<HashMap<String, Option<SbbFormationData>>>(&contents)
            {
                println!("Loaded {} SBB formations from disk cache", store.len());
                return store;
            }
        }
    }
    HashMap::new()
}

pub fn save_store_to_disk(store: &HashMap<String, Option<SbbFormationData>>) {
    let path = Path::new(PERSISTENCE_PATH);
    if let Ok(contents) = serde_json::to_string(store) {
        let _ = fs::write(path, contents);
    }
}

pub async fn bg_fetch_sbb_formations(
    store: SbbFormationStore,
    authoritative_data_store: Arc<SccHashMap<String, Arc<catenary::aspen_dataset::AspenisedData>>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let http_client = Client::new();

    loop {
        let mut train_requests: HashSet<(String, u64)> = HashSet::new();

        if let Some(guard) = authoritative_data_store
            .get_async(&"schweiz".to_string())
            .await
        {
            let data = Arc::clone(guard.get());
            drop(guard);

            for vp in data.vehicle_positions.values() {
                if let Some(trip) = &vp.trip {
                    if let Some(short_name) = &trip.trip_short_name {
                        if let Ok(num) = short_name.parse::<u64>() {
                            let op_date = trip
                                .start_date
                                .map(|d| d.format("%Y-%m-%d").to_string())
                                .unwrap_or_else(|| {
                                    chrono::Utc::now().format("%Y-%m-%d").to_string()
                                });
                            train_requests.insert((op_date, num));
                        }
                    }
                }
            }

            for tu in data.trip_updates.values() {
                if let Some(start_date) = &tu.trip.start_date {
                    if let Some(trip_id) = &tu.trip.trip_id {
                        if let Ok(num) = trip_id.parse::<u64>() {
                            let op_date = start_date.format("%Y-%m-%d").to_string();
                            train_requests.insert((op_date, num));
                        }
                    }
                }
            }
        }

        evict_old_entries(&store).await;

        let api_key = catenaryconfig::config()
            .aspen
            .sbb_api_key
            .clone()
            .unwrap_or_else(|| {
                "Bearer eyJvcmciOiI2NDA2NTFhNTIyZmEwNTAwMDEyOWJiZTEiLCJpZCI6IjVhMmRmMGY2NGMxNTRjMzI5NTcyMTNjYTJiYTc0Y2ExIiwiaCI6Im11cm11cjEyOCJ9".to_string()
            });

        for (operation_date, train_number) in &train_requests {
            let key = format!("{}_{}", operation_date, train_number);

            {
                let read_guard = store.read().await;
                if read_guard.contains_key(&key) {
                    continue;
                }
            }

            let url = format!(
                "https://api.opentransportdata.swiss/formation/v2/formations_full?evu=SBBP&operationDate={}&trainNumber={}",
                operation_date, train_number
            );

            let req_builder = http_client.get(&url).header("Authorization", &api_key);

            match req_builder.send().await {
                Ok(res) => {
                    let status = res.status();
                    if status.as_u16() == 429 {
                        eprintln!("SBB API 429 Too Many Requests. Backing off for 30s...");
                        sleep(Duration::from_secs(30)).await;
                        continue;
                    }

                    if status.is_success() {
                        if let Ok(api_resp) = res.json::<SbbFormationApiResponse>().await {
                            let val = match api_resp {
                                SbbFormationApiResponse::Data(data) => Some(data),
                                SbbFormationApiResponse::Error { .. } => None,
                            };

                            let mut write_guard = store.write().await;
                            write_guard.insert(key, val);
                            save_store_to_disk(&write_guard);
                        } else {
                            let mut write_guard = store.write().await;
                            write_guard.insert(key, None);
                            save_store_to_disk(&write_guard);
                        }
                    } else {
                        let mut write_guard = store.write().await;
                        write_guard.insert(key, None);
                        save_store_to_disk(&write_guard);
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Error fetching SBB formation for train {}: {}",
                        train_number, e
                    );
                }
            }

            sleep(Duration::from_secs(2)).await;
        }

        sleep(Duration::from_secs(60)).await;
    }
}

async fn evict_old_entries(store: &SbbFormationStore) {
    let cutoff = chrono::Utc::now().date_naive() - chrono::Duration::hours(EVICTION_HOURS);

    let mut write_guard = store.write().await;
    let before = write_guard.len();

    write_guard.retain(|key, _| {
        if let Some(date_str) = key.split('_').next() {
            if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                return date >= cutoff;
            }
        }
        false
    });

    let evicted = before - write_guard.len();
    if evicted > 0 {
        println!(
            "Evicted {} SBB formation entries older than {}h",
            evicted, EVICTION_HOURS
        );
        save_store_to_disk(&write_guard);
    }
}
