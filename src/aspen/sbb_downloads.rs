use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::{AspenisedData, AspenisedTripUpdate};
use catenary::catenaryconfig;
use catenary::models::{Agency, CompressedTrip, Route};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::sbb_formation_types::{SbbFormationApiResponse, SbbFormationData};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use reqwest::Client;
use scc::HashMap as SccHashMap;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

pub type SbbFormationStore = Arc<RwLock<HashMap<String, Option<SbbFormationData>>>>;

type RealtimeTripRequests = HashMap<String, HashMap<chrono::NaiveDate, RealtimeTripTiming>>;
type TrainRequests = HashMap<(String, u64), TrainRequest>;
type AgencyKey = (String, String, String);

#[derive(Clone, Copy, Debug, Default)]
struct RealtimeTripTiming {
    first_stop_time: Option<i64>,
    last_stop_time: Option<i64>,
}

impl RealtimeTripTiming {
    fn merge(&mut self, other: Self) {
        self.first_stop_time = match (self.first_stop_time, other.first_stop_time) {
            (Some(existing), Some(incoming)) => Some(existing.min(incoming)),
            (None, incoming) => incoming,
            (existing, None) => existing,
        };
        self.last_stop_time = match (self.last_stop_time, other.last_stop_time) {
            (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
            (None, incoming) => incoming,
            (existing, None) => existing,
        };
    }
}

#[derive(Clone, Debug)]
struct ScheduledTrainCandidate {
    trip_id: String,
    operation_dates: HashMap<chrono::NaiveDate, RealtimeTripTiming>,
    trip_short_name: Option<String>,
    onestop_feed_id: String,
    attempt_id: String,
    agency_id: Option<String>,
}

#[derive(Clone, Debug)]
struct TrainRequest {
    operation_date: String,
    train_number: u64,
    evu: String,
    timing: RealtimeTripTiming,
}

impl TrainRequest {
    fn cache_key(&self) -> String {
        format!("{}_{}", self.operation_date, self.train_number)
    }
}

enum FormationFetchOutcome {
    Data(SbbFormationData),
    NoFormation(String),
    Retry,
    RateLimited,
    DisableApiKey,
}

const PERSISTENCE_PATH: &str = "sbb_formations_cache.json";
const PERSISTENCE_PATH_ENV: &str = "SBB_FORMATIONS_CACHE_PATH";
const SBB_API_KEY_ENV: &str = "SBB_API_KEY";
const SBB_API_KEYS_ENV: &str = "SBB_API_KEYS";
const SWITZERLAND_CHATEAU_ID: &str = "schweiz";
const ASSIGNMENT_KEY: &str = "/aspen_assigned_chateaux/schweiz";
const TRAIN_ROUTE_TYPE: i16 = 2;
const EVICTION_HOURS: i64 = 72;
const LOOP_INTERVAL: Duration = Duration::from_secs(60);
const API_KEY_REQUEST_INTERVAL: Duration = Duration::from_secs(2);
const NO_FORMATION_RETRY_DELAY: Duration = Duration::from_secs(60 * 60);
const NO_FORMATION_ERROR: &str = "There were no formation data";
const CUS_FOS_VEHICLE_COUNT_MISMATCH_ERROR: &str =
    "Failed, because CUS and FOS suggest different numbers of vehicles";

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
        Ok(contents) => {
            match serde_json::from_str::<HashMap<String, Option<SbbFormationData>>>(&contents) {
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
            }
        }
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

fn normalize_api_keys(keys: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();

    keys.into_iter()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
        .filter(|key| seen.insert(key.clone()))
        .collect()
}

fn configured_api_keys() -> Vec<String> {
    let environment_keys = env::var(SBB_API_KEYS_ENV)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .map(normalize_api_keys)
        .unwrap_or_default();
    if !environment_keys.is_empty() {
        return environment_keys;
    }

    if let Ok(api_key) = env::var(SBB_API_KEY_ENV) {
        let api_keys = normalize_api_keys([api_key]);
        if !api_keys.is_empty() {
            return api_keys;
        }
    }

    let config = &catenaryconfig::config().aspen;
    let configured_keys = normalize_api_keys(config.sbb_api_keys.clone().unwrap_or_default());
    if !configured_keys.is_empty() {
        return configured_keys;
    }

    normalize_api_keys(config.sbb_api_key.clone())
}

fn insert_realtime_trip_request(
    requests: &mut RealtimeTripRequests,
    trip_id: Option<&str>,
    operation_date: Option<chrono::NaiveDate>,
    timing: RealtimeTripTiming,
) {
    let (Some(trip_id), Some(operation_date)) = (trip_id, operation_date) else {
        return;
    };

    requests
        .entry(trip_id.to_string())
        .or_default()
        .entry(operation_date)
        .and_modify(|existing| existing.merge(timing))
        .or_insert(timing);
}

fn trip_update_timing(trip_update: &AspenisedTripUpdate) -> RealtimeTripTiming {
    let first_stop_time = trip_update
        .stop_time_update
        .as_slice()
        .first()
        .and_then(|stop| {
            stop.departure
                .as_ref()
                .and_then(|event| event.time)
                .or_else(|| stop.arrival.as_ref().and_then(|event| event.time))
        });
    let last_stop_time = trip_update
        .stop_time_update
        .as_slice()
        .last()
        .and_then(|stop| {
            stop.arrival
                .as_ref()
                .and_then(|event| event.time)
                .or_else(|| stop.departure.as_ref().and_then(|event| event.time))
        });

    RealtimeTripTiming {
        first_stop_time,
        last_stop_time,
    }
}

fn collect_realtime_trip_requests(data: &AspenisedData) -> RealtimeTripRequests {
    let mut requests = HashMap::new();


    for trip_update in data.trip_updates.values() {
        insert_realtime_trip_request(
            &mut requests,
            trip_update.trip.trip_id.as_deref(),
            trip_update.trip.start_date,
            trip_update_timing(trip_update),
        );
    }

    requests
}

fn add_train_requests_from_trip_short_name(
    trip_id: &str,
    trip_short_name: Option<&str>,
    operation_dates: &HashMap<chrono::NaiveDate, RealtimeTripTiming>,
    evu: &str,
    train_requests: &mut TrainRequests,
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

    for (operation_date, timing) in operation_dates {
        let operation_date = operation_date.format("%Y-%m-%d").to_string();
        let request = TrainRequest {
            operation_date: operation_date.clone(),
            train_number,
            evu: evu.to_string(),
            timing: *timing,
        };

        train_requests
            .entry((operation_date, train_number))
            .and_modify(|existing| {
                existing.timing.merge(*timing);
                if existing.evu == "SBBP" && evu != "SBBP" {
                    existing.evu = evu.to_string();
                }
            })
            .or_insert(request);
    }
}

fn evu_for_candidate(
    candidate: &ScheduledTrainCandidate,
    agency_names: &HashMap<AgencyKey, String>,
) -> &'static str {
    let Some(agency_id) = candidate.agency_id.as_deref() else {
        return "SBBP";
    };

    if agency_id == "72" {
        return "RhB";
    }

    let agency_key = (
        candidate.onestop_feed_id.clone(),
        candidate.attempt_id.clone(),
        agency_id.to_string(),
    );

    let Some(agency_name) = agency_names.get(&agency_key) else {
        return "SBBP";
    };

    let agency_name = agency_name.to_lowercase();

    if agency_name.contains("südostbahn") {
        "SOB"
    } else if agency_name.contains("thurbo") {
        "THURBO"
    } else if agency_name.contains("zentralbahn") {
        "ZB"
    } else if agency_name.contains("fribourgeois") {
        "TPF"
    } else if agency_name.contains("verein dampfbahn bern") {
        "VDBB"
    } else if agency_name.contains("neuchâtelois") {
        "TRN"
    } else if agency_name.contains("oensingen-balsthal-bahn") {
        "OeBB"
    } else if agency_name.contains("morges-bière-cossonay") {
        "MBC"
    } else if agency_name.contains("bls") {
        "BLS"
    } else {
        "SBBP"
    }
}

async fn resolve_train_requests(
    data: &AspenisedData,
    conn_pool: &CatenaryPostgresPool,
    realtime_trip_requests: &RealtimeTripRequests,
) -> Result<TrainRequests, Box<dyn std::error::Error + Sync + Send>> {
    let mut candidates = Vec::new();
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

        candidates.push(ScheduledTrainCandidate {
            trip_id: trip_id.clone(),
            operation_dates: operation_dates.clone(),
            trip_short_name: compressed_trip
                .trip_short_name
                .as_deref()
                .map(ToString::to_string),
            onestop_feed_id: compressed_trip.onestop_feed_id.clone(),
            attempt_id: compressed_trip.attempt_id.clone(),
            agency_id: route.agency_id.clone(),
        });
    }

    if candidates.is_empty() && unresolved_trip_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut conn = conn_pool.get().await?;

    if !unresolved_trip_ids.is_empty() {
        let compressed_trips = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(
                catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(SWITZERLAND_CHATEAU_ID),
            )
            .filter(
                catenary::schema::gtfs::trips_compressed::dsl::trip_id.eq_any(&unresolved_trip_ids),
            )
            .load::<CompressedTrip>(&mut conn)
            .await?;

        let route_ids = compressed_trips
            .iter()
            .map(|trip| trip.route_id.clone())
            .collect::<HashSet<String>>()
            .into_iter()
            .collect::<Vec<String>>();

        let routes = if route_ids.is_empty() {
            Vec::new()
        } else {
            catenary::schema::gtfs::routes::dsl::routes
                .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(SWITZERLAND_CHATEAU_ID))
                .filter(catenary::schema::gtfs::routes::dsl::route_type.eq(TRAIN_ROUTE_TYPE))
                .filter(catenary::schema::gtfs::routes::dsl::route_id.eq_any(&route_ids))
                .load::<Route>(&mut conn)
                .await?
        };

        let routes_by_key = routes
            .into_iter()
            .map(|route| {
                (
                    (
                        route.onestop_feed_id.clone(),
                        route.attempt_id.clone(),
                        route.route_id.clone(),
                    ),
                    route,
                )
            })
            .collect::<HashMap<(String, String, String), Route>>();

        for compressed_trip in compressed_trips {
            let Some(operation_dates) = realtime_trip_requests.get(&compressed_trip.trip_id) else {
                continue;
            };
            let route_key = (
                compressed_trip.onestop_feed_id.clone(),
                compressed_trip.attempt_id.clone(),
                compressed_trip.route_id.clone(),
            );
            let Some(route) = routes_by_key.get(&route_key) else {
                continue;
            };

            if route.route_type != TRAIN_ROUTE_TYPE {
                continue;
            }

            candidates.push(ScheduledTrainCandidate {
                trip_id: compressed_trip.trip_id,
                operation_dates: operation_dates.clone(),
                trip_short_name: compressed_trip
                    .trip_short_name
                    .as_deref()
                    .map(ToString::to_string),
                onestop_feed_id: compressed_trip.onestop_feed_id,
                attempt_id: compressed_trip.attempt_id,
                agency_id: route.agency_id.clone(),
            });
        }
    }

    let agency_ids = candidates
        .iter()
        .filter_map(|candidate| candidate.agency_id.clone())
        .filter(|agency_id| agency_id != "72")
        .collect::<HashSet<String>>()
        .into_iter()
        .collect::<Vec<String>>();

    let agency_names = if agency_ids.is_empty() {
        HashMap::new()
    } else {
        catenary::schema::gtfs::agencies::dsl::agencies
            .filter(catenary::schema::gtfs::agencies::dsl::chateau.eq(SWITZERLAND_CHATEAU_ID))
            .filter(catenary::schema::gtfs::agencies::dsl::agency_id.eq_any(&agency_ids))
            .load::<Agency>(&mut conn)
            .await?
            .into_iter()
            .map(|agency| {
                (
                    (
                        agency.static_onestop_id,
                        agency.attempt_id,
                        agency.agency_id,
                    ),
                    agency.agency_name,
                )
            })
            .collect::<HashMap<AgencyKey, String>>()
    };

    let mut train_requests = HashMap::new();
    for candidate in candidates {
        let evu = evu_for_candidate(&candidate, &agency_names);
        add_train_requests_from_trip_short_name(
            &candidate.trip_id,
            candidate.trip_short_name.as_deref(),
            &candidate.operation_dates,
            evu,
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

fn api_error_matches(error: &str, expected: &str) -> bool {
    error
        .trim()
        .trim_end_matches('.')
        .eq_ignore_ascii_case(expected)
}

fn is_no_formation_error(error: &str) -> bool {
    api_error_matches(error, NO_FORMATION_ERROR)
}

fn is_cus_fos_vehicle_count_mismatch(error: &str) -> bool {
    api_error_matches(error, CUS_FOS_VEHICLE_COUNT_MISMATCH_ERROR)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormationEndpoint {
    Full,
    StopBased,
}

impl FormationEndpoint {
    fn path(self) -> &'static str {
        match self {
            Self::Full => "formations_full",
            Self::StopBased => "formations_stop_based",
        }
    }

    fn log_name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::StopBased => "stop_based",
        }
    }
}

async fn record_no_formation(
    store: &SbbFormationStore,
    no_formation_retry_after: &mut HashMap<String, Instant>,
    key: &str,
) {
    no_formation_retry_after.insert(key.to_string(), Instant::now() + NO_FORMATION_RETRY_DELAY);

    let mut write_guard = store.write().await;
    write_guard.insert(key.to_string(), None);
    persist_store(&write_guard);
}

fn trip_phase(timing: RealtimeTripTiming, now: i64) -> u8 {
    match (timing.first_stop_time, timing.last_stop_time) {
        (Some(first), Some(last)) if first <= now && now <= last => 0,
        (Some(first), _) if first > now => 1,
        (_, Some(last)) if last < now => 3,
        _ => 2,
    }
}

fn compare_train_request_priority(a: &TrainRequest, b: &TrainRequest, now: i64) -> Ordering {
    let a_phase = trip_phase(a.timing, now);
    let b_phase = trip_phase(b.timing, now);

    a_phase
        .cmp(&b_phase)
        .then_with(|| match a_phase {
            0 => a
                .timing
                .last_stop_time
                .unwrap_or(i64::MAX)
                .cmp(&b.timing.last_stop_time.unwrap_or(i64::MAX)),
            1 => a
                .timing
                .first_stop_time
                .unwrap_or(i64::MAX)
                .cmp(&b.timing.first_stop_time.unwrap_or(i64::MAX)),
            3 => b
                .timing
                .last_stop_time
                .unwrap_or(i64::MIN)
                .cmp(&a.timing.last_stop_time.unwrap_or(i64::MIN)),
            _ => Ordering::Equal,
        })
        .then_with(|| a.operation_date.cmp(&b.operation_date))
        .then_with(|| a.train_number.cmp(&b.train_number))
        .then_with(|| a.evu.cmp(&b.evu))
}

async fn order_train_requests(
    store: &SbbFormationStore,
    train_requests: TrainRequests,
    no_formation_retry_after: &mut HashMap<String, Instant>,
) -> Vec<TrainRequest> {
    let now = Instant::now();
    let now_unix = chrono::Utc::now().timestamp();
    let cached_states = {
        let read_guard = store.read().await;
        read_guard
            .iter()
            .map(|(key, value)| (key.clone(), value.is_some()))
            .collect::<HashMap<String, bool>>()
    };
    let mut active_keys = HashSet::with_capacity(train_requests.len());
    let mut pending_requests = Vec::new();

    for request in train_requests.into_values() {
        let key = request.cache_key();
        active_keys.insert(key.clone());

        if cached_states.get(&key) == Some(&true) {
            continue;
        }

        let retry = match no_formation_retry_after.get(&key) {
            Some(retry_after) if *retry_after > now => continue,
            Some(_) => true,
            None => cached_states.contains_key(&key),
        };
        pending_requests.push((retry, request));
    }

    no_formation_retry_after
        .retain(|key, retry_after| *retry_after > now || active_keys.contains(key));
    pending_requests.sort_unstable_by(|(a_retry, a), (b_retry, b)| {
        compare_train_request_priority(a, b, now_unix).then_with(|| a_retry.cmp(b_retry))
    });
    pending_requests
        .into_iter()
        .map(|(_, request)| request)
        .collect()
}

async fn fetch_formation_endpoint(
    http_client: &Client,
    api_key: &str,
    request: &TrainRequest,
    endpoint: FormationEndpoint,
) -> Result<FormationFetchOutcome, String> {
    let url = format!(
        "https://api.opentransportdata.swiss/formation/v2/{}?evu={}&operationDate={}&trainNumber={}",
        endpoint.path(),
        request.evu,
        request.operation_date,
        request.train_number
    );

    tracing::info!(
        endpoint = endpoint.log_name(),
        operation_date = %request.operation_date,
        train_number = request.train_number,
        evu = %request.evu,
        "Downloading Swiss train formation"
    );

    let response = match http_client
        .get(&url)
        .header("Authorization", api_key)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(
                error = %error,
                endpoint = endpoint.log_name(),
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Failed to download Swiss formation"
            );
            return Ok(FormationFetchOutcome::Retry);
        }
    };

    let status = response.status();
    if status.as_u16() == 429 {
        tracing::warn!(
            endpoint = endpoint.log_name(),
            operation_date = %request.operation_date,
            train_number = request.train_number,
            evu = %request.evu,
            "Swiss formation API rate limited an API key; backing off that key"
        );
        return Ok(FormationFetchOutcome::RateLimited);
    }

    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(
                error = %error,
                status = %status,
                endpoint = endpoint.log_name(),
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Failed to read Swiss formation response body"
            );
            return Ok(FormationFetchOutcome::Retry);
        }
    };

    if status.as_u16() == 401 || status.as_u16() == 403 {
        tracing::error!(
            status = %status,
            response = %response_preview(&body),
            endpoint = endpoint.log_name(),
            operation_date = %request.operation_date,
            train_number = request.train_number,
            evu = %request.evu,
            "Swiss formation API rejected an API key"
        );
        return Ok(FormationFetchOutcome::DisableApiKey);
    }

    let decoded = serde_json::from_str::<SbbFormationApiResponse>(&body);

    if !status.is_success() {
        if let Ok(SbbFormationApiResponse::Error { error }) = decoded {
            if is_no_formation_error(&error) {
                return Ok(FormationFetchOutcome::NoFormation(error));
            }

            if endpoint == FormationEndpoint::Full && is_cus_fos_vehicle_count_mismatch(&error) {
                return Err(error);
            }

            tracing::error!(
                error = %error,
                status = %status,
                endpoint = endpoint.log_name(),
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Swiss formation API returned an error payload"
            );
        } else {
            tracing::error!(
                status = %status,
                response = %response_preview(&body),
                endpoint = endpoint.log_name(),
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Swiss formation API returned an unsuccessful response"
            );
        }

        return Ok(FormationFetchOutcome::Retry);
    }

    match decoded {
        Ok(SbbFormationApiResponse::Data(data)) => Ok(FormationFetchOutcome::Data(data)),
        Ok(SbbFormationApiResponse::Error { error }) if is_no_formation_error(&error) => {
            Ok(FormationFetchOutcome::NoFormation(error))
        }
        Ok(SbbFormationApiResponse::Error { error })
            if endpoint == FormationEndpoint::Full && is_cus_fos_vehicle_count_mismatch(&error) =>
        {
            Err(error)
        }
        Ok(SbbFormationApiResponse::Error { error }) => {
            tracing::error!(
                error = %error,
                endpoint = endpoint.log_name(),
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Swiss formation API returned an error payload; the request will be retried"
            );
            Ok(FormationFetchOutcome::Retry)
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                response = %response_preview(&body),
                endpoint = endpoint.log_name(),
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Failed to decode Swiss formation response; the request will be retried"
            );
            Ok(FormationFetchOutcome::Retry)
        }
    }
}

async fn fetch_formation(
    http_client: &Client,
    api_key: &str,
    request: &TrainRequest,
) -> FormationFetchOutcome {
    match fetch_formation_endpoint(http_client, api_key, request, FormationEndpoint::Full).await {
        Ok(outcome) => outcome,
        Err(full_error) => {
            tracing::warn!(
                error = %full_error,
                operation_date = %request.operation_date,
                train_number = request.train_number,
                evu = %request.evu,
                "Full Swiss formation is unavailable because CUS and FOS disagree; trying the stop-based endpoint"
            );

            match fetch_formation_endpoint(
                http_client,
                api_key,
                request,
                FormationEndpoint::StopBased,
            )
            .await
            {
                Ok(FormationFetchOutcome::Data(data)) => {
                    tracing::info!(
                        operation_date = %request.operation_date,
                        train_number = request.train_number,
                        evu = %request.evu,
                        "Downloaded stop-based Swiss formation fallback"
                    );
                    FormationFetchOutcome::Data(data)
                }
                Ok(outcome) => outcome,
                Err(fallback_error) => {
                    tracing::error!(
                        error = %fallback_error,
                        operation_date = %request.operation_date,
                        train_number = request.train_number,
                        evu = %request.evu,
                        "Stop-based Swiss formation fallback unexpectedly reported a CUS/FOS mismatch"
                    );
                    FormationFetchOutcome::Retry
                }
            }
        }
    }
}

async fn fetch_requests_for_api_key(
    http_client: Client,
    api_key: String,
    requests: Vec<TrainRequest>,
) -> Vec<(TrainRequest, FormationFetchOutcome)> {
    let mut outcomes = Vec::with_capacity(requests.len());

    for (index, request) in requests.into_iter().enumerate() {
        if index > 0 {
            sleep(API_KEY_REQUEST_INTERVAL).await;
        }

        let outcome = fetch_formation(&http_client, &api_key, &request).await;
        let rate_limited = matches!(&outcome, FormationFetchOutcome::RateLimited);
        let stop_key = rate_limited || matches!(&outcome, FormationFetchOutcome::DisableApiKey);
        outcomes.push((request, outcome));

        if rate_limited {
            sleep(Duration::from_secs(30)).await;
        }
        if stop_key {
            break;
        }
    }

    outcomes
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
    let mut no_formation_retry_after = {
        let retry_after = Instant::now() + NO_FORMATION_RETRY_DELAY;
        let read_guard = store.read().await;
        read_guard
            .iter()
            .filter_map(|(key, value)| {
                if value.is_none() {
                    Some((key.clone(), retry_after))
                } else {
                    None
                }
            })
            .collect::<HashMap<String, Instant>>()
    };
    let mut etcd =
        connect_to_etcd_with_retry(etcd_addresses.as_slice(), etcd_connect_options.as_ref()).await;

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

        let api_keys = configured_api_keys();
        if api_keys.is_empty() {
            if !missing_api_key_logged {
                tracing::error!(
                    config_key = "aspen.sbb_api_keys",
                    legacy_config_key = "aspen.sbb_api_key",
                    environment_variable = SBB_API_KEYS_ENV,
                    legacy_environment_variable = SBB_API_KEY_ENV,
                    "SBB formation downloading is enabled but no API keys are configured"
                );
                missing_api_key_logged = true;
            }
            sleep(LOOP_INTERVAL).await;
            continue;
        }
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
            TrainRequests::new()
        };

        if !train_requests.is_empty() {
            tracing::info!(
                candidate_count = train_requests.len(),
                "Found route_type 2 SBB trains that may need formation downloads"
            );
        }

        evict_old_entries(&store).await;

        let train_requests =
            order_train_requests(&store, train_requests, &mut no_formation_retry_after).await;

        let mut queues = vec![Vec::new(); api_keys.len()];
        for (index, request) in train_requests.into_iter().enumerate() {
            let queue_index = index % queues.len();
            queues[queue_index].push(request);
        }

        let mut fetch_tasks = JoinSet::new();
        for (api_key, requests) in api_keys.into_iter().zip(queues) {
            if requests.is_empty() {
                continue;
            }

            fetch_tasks.spawn(fetch_requests_for_api_key(
                http_client.clone(),
                api_key,
                requests,
            ));
        }

        while let Some(task_result) = fetch_tasks.join_next().await {
            let outcomes = match task_result {
                Ok(outcomes) => outcomes,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "An SBB formation API-key worker failed"
                    );
                    continue;
                }
            };

            for (request, outcome) in outcomes {
                let key = request.cache_key();

                match outcome {
                    FormationFetchOutcome::Data(data) => {
                        let formation_count = data.formations.as_ref().map_or(0, Vec::len);
                        let scheduled_stop_count = data
                            .formations_at_scheduled_stops
                            .as_ref()
                            .map_or(0, Vec::len);
                        let vehicle_journey_type = data
                            .vehicle_journey_type
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string());
                        no_formation_retry_after.remove(&key);
                        let mut write_guard = store.write().await;
                        write_guard.insert(key, Some(data));
                        persist_store(&write_guard);

                        tracing::info!(
                            operation_date = %request.operation_date,
                            train_number = request.train_number,
                            evu = %request.evu,
                            formation_count,
                            scheduled_stop_count,
                            vehicle_journey_type = %vehicle_journey_type,
                            "Downloaded Swiss train formation"
                        );
                    }
                    FormationFetchOutcome::NoFormation(error) => {
                        record_no_formation(&store, &mut no_formation_retry_after, &key).await;

                        tracing::warn!(
                            error = %error,
                            operation_date = %request.operation_date,
                            train_number = request.train_number,
                            evu = %request.evu,
                            retry_after_seconds = NO_FORMATION_RETRY_DELAY.as_secs(),
                            "Swiss formation API reported no formation data; deferring retry"
                        );
                    }
                    FormationFetchOutcome::Retry
                    | FormationFetchOutcome::RateLimited
                    | FormationFetchOutcome::DisableApiKey => {}
                }
            }
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
