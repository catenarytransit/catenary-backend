use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::AspenisedData;
use catenary::catenaryconfig;
use catenary::models::{CompressedTrip, Route};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::sbb_formation_types::{SbbFormationApiResponse, SbbFormationData};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use reqwest::Client;
use scc::HashMap as SccHashMap;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};

pub type SbbFormationStore = Arc<RwLock<HashMap<String, Option<SbbFormationData>>>>;

type RealtimeTripRequests = HashMap<String, HashSet<chrono::NaiveDate>>;

const PERSISTENCE_PATH: &str = "sbb_formations_cache.json";
const PERSISTENCE_PATH_ENV: &str = "SBB_FORMATIONS_CACHE_PATH";
const SBB_API_KEY_ENV: &str = "SBB_API_KEY";
const SWITZERLAND_CHATEAU_ID: &str = "schweiz";
const ASSIGNMENT_KEY: &str = "/aspen_assigned_chateaux/schweiz";
const TRAIN_ROUTE_TYPE: i16 = 2;
const EVICTION_HOURS: i64 = 72;
const LOOP_INTERVAL: Duration = Duration::from_secs(60);

fn persistence_path() -> PathBuf {
    env::var_os(PERSISTENCE_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(PERSISTENCE_PATH))
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

pub fn load_store_from_disk() -> HashMap<String, Option<SbbFormationData>> {
    let path = persistence_path();
    let displayed_path = absolute_path(&path);

    match fs::read_to_string(&path) {
        Ok(contents) => match serde_json::from_str::<SbbFormationData>(&contents) {
            Ok(store) => {
                tracing::info!(
                    cache_path = %displayed_path.display(),
                    "Loaded SBB formations from disk cache"
                );
                store
            }
            Err(error) => {
                tracing::error!(
                    error = %error,
                    cache_path = %displayed_path.display(),
                    "Failed to parse SBB formation disk cache; starting with an empty cache"
                );
                HashMap::new()
            }
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::info!(
                cache_path = %displayed_path.display(),
                "No existing SBB formation disk cache"
            );
            HashMap::new()
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                cache_path = %displayed_path.display(),
                "Failed to read SBB formation disk cache; starting with an empty cache"
            );
            HashMap::new()
        }
    }
}

pub fn save_store_to_disk(
    store: &HashMap<String, Option<SbbFormationData>>,
) -> Result<(), io::Error> {
    let path = persistence_path();

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let contents = serde_json::to_vec(store)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let temporary_path = path.with_extension("json.tmp");
    fs::write(&temporary_path, contents)?;
    fs::rename(temporary_path, path)
}

fn persist_store(store: &HashMap<String, Option<SbbFormationData>>) {
    if let Err(error) = save_store_to_disk(store) {
        tracing::error!(
            error = %error,
            cache_path = %absolute_path(&persistence_path()).display(),
            "Failed to persist SBB formation cache"
        );
    }
}

fn configured_api_key() -> Option<String> {
    env::var(SBB_API_KEY_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            catenaryconfig::config()
                .aspen
                .sbb_api_key
                .clone()
                .filter(|value| !value.trim().is_empty())
        })
}

fn insert_realtime_trip_request(
    requests: &mut RealtimeTripRequests,
    trip_id: Option<&str>,
    operation_date: Option<chrono::NaiveDate>,
) {
    let (Some(trip_id), Some(operation_date)) = (trip_id, operation_date) else {
        return;
    };

    requests
        .entry(trip_id.to_string())
        .or_default()
        .insert(operation_date);
}

fn collect_realtime_trip_requests(data: &AspenisedData) -> RealtimeTripRequests {
    let mut requests = HashMap::new();

    for vehicle_position in data.vehicle_positions.values() {
        if let Some(trip) = &vehicle_position.trip {
            insert_realtime_trip_request(
                &mut requests,
                trip.trip_id.as_deref(),
                trip.start_date,
            );
        }
    }

    for trip_update in data.trip_updates.values() {
        insert_realtime_trip_request(
            &mut requests,
            trip_update.trip.trip_id.as_deref(),
            trip_update.trip.start_date,
        );
    }

    requests
}

fn add_train_requests_from_trip_short_name(
    trip_id: &str,
    trip_short_name: Option<&str>,
    operation_dates: &HashSet<chrono::NaiveDate>,
    train_requests: &mut HashSet<(String, u64)>,
) {
    let Some(trip_short_name) = trip_short_name.map(str::trim) else {
        return;
    };

    let Ok(train_number) = trip_short_name.parse::<u64>() else {
        tracing::debug!(
            trip_id,
            trip_short_name,
            "Skipping SBB formation lookup because the scheduled trip_short_name is not numeric"
        );
        return;
    };

    for operation_date in operation_dates {
        train_requests.insert((
            operation_date.format("%Y-%m-%d").to_string(),
            train_number,
        ));
    }
}

async fn resolve_train_requests(
    data: &AspenisedData,
    conn_pool: &CatenaryPostgresPool,
    realtime_trip_requests: &RealtimeTripRequests,
) -> Result<HashSet<(String, u64)>, Box<dyn std::error::Error + Sync + Send>> {
    let mut train_requests = HashSet::new();
    let mut unresolved_trip_ids = Vec::new();

    for (trip_id, operation_dates) in realtime_trip_requests {
        let Some(compressed_trip) = data
            .compressed_trip_internal_cache
            .compressed_trips
            .get(trip_id.as_str())
        else {
            unresolved_trip_ids.push(trip_id.clone());
            continue;
        };

        let Some(route) = data
            .vehicle_routes_cache
            .get(compressed_trip.route_id.as_str())
        else {
            unresolved_trip_ids.push(trip_id.clone());
            continue;
        };

        if route.route_type != TRAIN_ROUTE_TYPE {
            continue;
        }

        add_train_requests_from_trip_short_name(
            trip_id,
            compressed_trip.trip_short_name.as_deref(),
            operation_dates,
            &mut train_requests,
        );
    }

    if unresolved_trip_ids.is_empty() {
        return Ok(train_requests);
    }

    let mut conn = conn_pool.get().await?;
    let compressed_trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
        .filter(
            catenary::schema::gtfs::trips_compressed::dsl::chateau
                .eq(SWITZERLAND_CHATEAU_ID),
        )
        .filter(
            catenary::schema::gtfs::trips_compressed::dsl::trip_id
                .eq_any(&unresolved_trip_ids),
        )
        .load::<CompressedTrip>(&mut conn)
        .await?;

    let route_ids = compressed_trips
        .iter()
        .map(|trip| trip.route_id.clone())
        .collect::<HashSet<String>>()
        .into_iter()
        .collect::<Vec<String>>();

    if route_ids.is_empty() {
        return Ok(train_requests);
    }

    let routes = catenary::schema::gtfs::routes::dsl::routes
        .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(SWITZERLAND_CHATEAU_ID))
        .filter(catenary::schema::gtfs::routes::dsl::route_id.eq_any(&route_ids))
        .load::<Route>(&mut conn)
        .await?;

    let route_types = routes
        .into_iter()
        .map(|route| {
            (
                (route.onestop_feed_id, route.attempt_id, route.route_id),
                route.route_type,
            )
        })
        .collect::<HashMap<(String, String, String), i16>>();

    for compressed_trip in compressed_trips {
        let Some(operation_dates) = realtime_trip_requests.get(&compressed_trip.trip_id) else {
            continue;
        };

        let route_key = (
            compressed_trip.onestop_feed_id.clone(),
            compressed_trip.attempt_id.clone(),
            compressed_trip.route_id.clone(),
        );

        if route_types.get(&route_key).copied() != Some(TRAIN_ROUTE_TYPE) {
            continue;
        }

        add_train_requests_from_trip_short_name(
            &compressed_trip.trip_id,
            compressed_trip.trip_short_name.as_deref(),
            operation_dates,
            &mut train_requests,
        );
    }

    Ok(train_requests)
}

async fn connect_to_etcd_with_retry(
    etcd_addresses: &[String],
    etcd_connect_options: &Option<etcd_client::ConnectOptions>,
) -> etcd_client::Client {
    loop {
        match etcd_client::Client::connect(etcd_addresses, etcd_connect_options.clone()).await {
            Ok(client) => return client,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    "Unable to connect the SBB formation downloader to etcd; retrying"
                );
                sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn is_schweiz_assigned_to_worker(
    etcd: &mut etcd_client::Client,
    worker_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Sync + Send>> {
    let response = etcd.get(ASSIGNMENT_KEY, None).await?;

    let Some(assignment_kv) = response.kvs().first() else {
        return Ok(false);
    };

    let assignment: ChateauMetadataEtcd = catenary::bincode_deserialize(assignment_kv.value())?;
    Ok(assignment.worker_id == worker_id)
}

fn response_preview(body: &str) -> String {
    body.chars().take(500).collect()
}

pub async fn bg_fetch_sbb_formations(
    store: SbbFormationStore,
    authoritative_data_store: Arc<SccHashMap<String, Arc<AspenisedData>>>,
    conn_pool: Arc<CatenaryPostgresPool>,
    worker_id: Arc<String>,
    etcd_addresses: Arc<Vec<String>>,
    etcd_connect_options: Arc<Option<etcd_client::ConnectOptions>>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let http_client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;
    let cache_path = absolute_path(&persistence_path());
    let mut previous_assignment = None;
    let mut missing_api_key_logged = false;
    let mut etcd = connect_to_etcd_with_retry(
        etcd_addresses.as_slice(),
        etcd_connect_options.as_ref(),
    )
    .await;

    tracing::info!(
        worker_id = %worker_id.as_str(),
        cache_path = %cache_path.display(),
        "SBB formation downloader task started"
    );

    loop {
        let assigned = match is_schweiz_assigned_to_worker(&mut etcd, worker_id.as_str()).await {
            Ok(assigned) => assigned,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    worker_id = %worker_id.as_str(),
                    "Unable to verify the schweiz assignment; SBB downloading remains disabled"
                );
                previous_assignment = None;
                sleep(Duration::from_secs(10)).await;
                etcd = connect_to_etcd_with_retry(
                    etcd_addresses.as_slice(),
                    etcd_connect_options.as_ref(),
                )
                .await;
                continue;
            }
        };

        if previous_assignment != Some(assigned) {
            if assigned {
                tracing::info!(
                    worker_id = %worker_id.as_str(),
                    "SBB formation downloading enabled because this Aspen worker owns schweiz"
                );
                let read_guard = store.read().await;
                persist_store(&read_guard);
            } else {
                tracing::info!(
                    worker_id = %worker_id.as_str(),
                    "SBB formation downloading disabled because this Aspen worker does not own schweiz"
                );
            }
            previous_assignment = Some(assigned);
        }

        if !assigned {
            sleep(LOOP_INTERVAL).await;
            continue;
        }

        let Some(api_key) = configured_api_key() else {
            if !missing_api_key_logged {
                tracing::error!(
                    config_key = "aspen.sbb_api_key",
                    environment_variable = SBB_API_KEY_ENV,
                    "SBB formation downloading is enabled but no API key is configured"
                );
                missing_api_key_logged = true;
            }
            sleep(LOOP_INTERVAL).await;
            continue;
        };
        missing_api_key_logged = false;

        let train_requests = if let Some(guard) = authoritative_data_store
            .get_async(&SWITZERLAND_CHATEAU_ID.to_string())
            .await
        {
            let data = Arc::clone(guard.get());
            drop(guard);

            let realtime_trip_requests = collect_realtime_trip_requests(&data);
            match resolve_train_requests(&data, conn_pool.as_ref(), &realtime_trip_requests).await {
                Ok(train_requests) => train_requests,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "Failed to resolve Swiss realtime trip IDs to scheduled train numbers"
                    );
                    sleep(LOOP_INTERVAL).await;
                    continue;
                }
            }
        } else {
            tracing::warn!(
                worker_id = %worker_id.as_str(),
                "This worker owns schweiz, but no authoritative schweiz dataset is loaded yet"
            );
            HashSet::new()
        };

        if !train_requests.is_empty() {
            tracing::info!(
                candidate_count = train_requests.len(),
                "Found route_type 2 SBB trains that may need formation downloads"
            );
        }

        evict_old_entries(&store).await;

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

            tracing::info!(
                operation_date = %operation_date,
                train_number,
                "Downloading SBB train formation"
            );

            let response = match http_client
                .get(&url)
                .header("Authorization", &api_key)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        operation_date = %operation_date,
                        train_number,
                        "Failed to download SBB formation"
                    );
                    continue;
                }
            };

            let status = response.status();
            if status.as_u16() == 429 {
                tracing::warn!(
                    operation_date = %operation_date,
                    train_number,
                    "SBB formation API rate limited this worker; backing off"
                );
                sleep(Duration::from_secs(30)).await;
                break;
            }

            let body = match response.text().await {
                Ok(body) => body,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        status = %status,
                        operation_date = %operation_date,
                        train_number,
                        "Failed to read SBB formation response body"
                    );
                    continue;
                }
            };

            if !status.is_success() {
                tracing::error!(
                    status = %status,
                    response = %response_preview(&body),
                    operation_date = %operation_date,
                    train_number,
                    "SBB formation API returned an unsuccessful response"
                );

                if status.as_u16() == 401 || status.as_u16() == 403 {
                    break;
                }
                continue;
            }

            match serde_json::from_str::<SbbFormationApiResponse>(&body) {
                Ok(SbbFormationApiResponse::Data(data)) => {
                    let formation_count = data.formations.len();
                    let scheduled_stop_count = data.formations_at_scheduled_stops.len();
                    let mut write_guard = store.write().await;
                    write_guard.insert(key, Some(data));
                    persist_store(&write_guard);

                    tracing::info!(
                        operation_date = %operation_date,
                        train_number,
                        formation_count,
                        scheduled_stop_count,
                        "Downloaded SBB train formation"
                    );
                }
                Ok(SbbFormationApiResponse::Error { error }) => {
                    let mut write_guard = store.write().await;
                    write_guard.insert(key, None);
                    persist_store(&write_guard);

                    tracing::warn!(
                        error = %error,
                        operation_date = %operation_date,
                        train_number,
                        "SBB formation API reported no formation data"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        response = %response_preview(&body),
                        operation_date = %operation_date,
                        train_number,
                        "Failed to decode SBB formation response; the request will be retried"
                    );
                }
            }

            sleep(Duration::from_secs(2)).await;
        }

        sleep(LOOP_INTERVAL).await;
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
        tracing::info!(
            evicted,
            eviction_hours = EVICTION_HOURS,
            "Evicted old SBB formation cache entries"
        );
        persist_store(&write_guard);
    }
}
