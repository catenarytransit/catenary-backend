use catenary::postgres_tools::CatenaryPostgresPool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

pub mod lirr_mnr;
pub mod metrolinx_platforms;
pub mod nyct_subway;
pub mod sncf_siri;
pub mod viarail;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub stop_id: String,
    pub sequence: u32,
    pub platform: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonPlatformInfo {
    pub stop_id: String,
    pub platform_name: String,
}

#[derive(Clone, Debug)]
pub enum TrackData {
    //output Option<MetrolinkOutputTrackData> instead
    Metrolink(Option<MetrolinkOutputTrackData>),
    Amtrak(AmtrakTrackDataMultisource),
    NationalRail(HashMap<String, Vec<PlatformInfo>>),
    IleDeFrance(HashMap<String, Vec<CommonPlatformInfo>>),
    Danmark(HashMap<String, Vec<CommonPlatformInfo>>),
    Schweiz(HashMap<String, Vec<CommonPlatformInfo>>),
    ViaRail(Option<viarail::ViaRailTrackData>),
    MetroNorthRailroad(Option<lirr_mnr::LirrMnrTrackData>),
    LongIslandRailroad(Option<lirr_mnr::LirrMnrTrackData>),
    NyctSubway(Option<nyct_subway::NyctSubwayTrackData>),
    // SNCF is a large nested map. Keep it behind Arc so refreshing/falling back
    // never deep-clones the complete parsed SIRI snapshot.
    Sncf(Option<Arc<sncf_siri::SncfTrackData>>),
    None,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetrolinkTrackData {
    #[serde(rename = "TrainDesignation")]
    train_designation: String,
    #[serde(rename = "RouteCode")]
    route_code: String,
    #[serde(rename = "PlatformName")]
    platform_name: String,
    #[serde(rename = "EventType")]
    event_type: String,
    #[serde(rename = "FormattedTrackDesignation")]
    formatted_track_designation: String,
    #[serde(rename = "TrainMovementTime")]
    train_movement_time: String,
}

#[derive(Clone, Debug)]
pub struct AmtrakTrackDataMultisource {
    pub metrolink: Option<MetrolinkOutputTrackData>,
    pub lirr_mnr: Option<lirr_mnr::LirrMnrTrackData>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetrolinkTrackDataCleaned {
    pub track_movement_time_arrival: Option<u64>,
    pub track_movement_time_departure: Option<u64>,
    pub stop_id: String,
    pub formatted_track_designation: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum MetrolinkEventType {
    Arrival,
    Departure,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetrolinkOutputTrackData {
    //cleaned 3 digit trip number -> stop_id -> MetrolinkTrackDataCleaned
    pub track_lookup: HashMap<String, HashMap<String, MetrolinkTrackDataCleaned>>,
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_fetch_track_data() {
        dotenvy::dotenv().ok();
        let pool = catenary::postgres_tools::make_async_pool()
            .await
            .unwrap_or_else(|e| panic!("pool init failed: {}", e));

        let track_data = super::fetch_track_data("metrolinktrains", &pool).await;
        match track_data {
            super::TrackData::Metrolink(m_data) => {
                assert!(m_data.is_some());
            }
            _ => panic!("Expected Metrolink data"),
        }

        let track_data = super::fetch_track_data("amtrak", &pool).await;
        match track_data {
            super::TrackData::Amtrak(a_data) => {
                assert!(a_data.metrolink.is_some());

                println!(
                    "{:#?}",
                    a_data
                        .metrolink
                        .as_ref()
                        .unwrap()
                        .track_lookup
                        .keys()
                        .cloned()
                        .collect::<Vec<String>>()
                );

                for (k, v) in a_data.metrolink.as_ref().unwrap().track_lookup.iter() {
                    if k.contains("A") && k.chars().count() == 3 {
                        println!("{}: {:#?}", k, v);
                    }
                }
            }
            _ => panic!("Expected Amtrak data"),
        }
    }
}

enum MetrolinkOrAmtrakStopCodes {
    Metrolink,
    Amtrak,
}

async fn metrolink_station_schedule_decode(
    response: Vec<MetrolinkTrackData>,
    stop_codes_to_use: MetrolinkOrAmtrakStopCodes,
) -> MetrolinkOutputTrackData {
    let mut track_lookup: HashMap<String, HashMap<String, MetrolinkTrackDataCleaned>> =
        HashMap::new();

    for t in response {
        let mut train_designation = t.train_designation.clone();

        if train_designation.len() > 1 {
            let last_char = train_designation.chars().last().unwrap_or(' ');
            let second_last_char = train_designation.chars().rev().nth(1).unwrap_or(' ');

            if last_char.is_alphabetic() && second_last_char.is_numeric() {
                train_designation.pop();
            }
        }

        if !track_lookup.contains_key(&train_designation) {
            track_lookup.insert(train_designation.clone(), HashMap::new());
        }

        let mut train_lookup_entry = track_lookup.get_mut(&train_designation).unwrap();

        let stop_id_find = match stop_codes_to_use {
            MetrolinkOrAmtrakStopCodes::Metrolink => {
                catenary::metrolink_ptc_to_stop_id::METROLINK_STOP_LIST
                    .iter()
                    .find(|x| x.1 == &t.platform_name)
            }
            MetrolinkOrAmtrakStopCodes::Amtrak => {
                catenary::metrolink_ptc_to_stop_id::AMTRAK_STOP_TO_SCAX_PTC_LIST
                    .iter()
                    .find(|x| x.1 == &t.platform_name)
            }
        };

        if let Some((stop_id, _)) = stop_id_find {
            if !train_lookup_entry.contains_key(*stop_id) {
                train_lookup_entry.insert(
                    stop_id.to_string(),
                    MetrolinkTrackDataCleaned {
                        track_movement_time_arrival: None,
                        track_movement_time_departure: None,
                        stop_id: stop_id.to_string(),
                        formatted_track_designation: t.formatted_track_designation.clone(),
                    },
                );
            }

            let train_and_stop_entry = train_lookup_entry.get_mut(&stop_id.to_string()).unwrap();

            match t.event_type.as_str() {
                "Arrival" => {
                    train_and_stop_entry.track_movement_time_arrival =
                        Some(catenary::metrolink_unix_fix(&t.train_movement_time));
                }
                "Departure" => {
                    train_and_stop_entry.track_movement_time_departure =
                        Some(catenary::metrolink_unix_fix(&t.train_movement_time));
                }
                _ => {}
            }
        }
    }

    MetrolinkOutputTrackData {
        track_lookup: track_lookup,
    }
}

const SNCF_CACHE_TTL: Duration = Duration::from_secs(30);
const SNCF_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const SNCF_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

struct SncfCacheEntry {
    fetched_at: Instant,
    data: Arc<sncf_siri::SncfTrackData>,
}

static SNCF_CACHE: OnceLock<RwLock<Option<SncfCacheEntry>>> = OnceLock::new();
static SNCF_HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn sncf_cache() -> &'static RwLock<Option<SncfCacheEntry>> {
    SNCF_CACHE.get_or_init(|| RwLock::new(None))
}

fn sncf_http_client() -> &'static reqwest::Client {
    SNCF_HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(SNCF_CONNECT_TIMEOUT)
            .timeout(SNCF_REQUEST_TIMEOUT)
            .build()
            .expect("failed to build SNCF HTTP client")
    })
}

fn cached_sncf_data(max_age: Option<Duration>) -> Option<Arc<sncf_siri::SncfTrackData>> {
    let cache = sncf_cache().read().ok()?;
    let entry = cache.as_ref()?;

    if let Some(max_age) = max_age {
        if entry.fetched_at.elapsed() > max_age {
            return None;
        }
    }

    Some(Arc::clone(&entry.data))
}

fn store_sncf_data(data: Arc<sncf_siri::SncfTrackData>) {
    if let Ok(mut cache) = sncf_cache().write() {
        *cache = Some(SncfCacheEntry {
            fetched_at: Instant::now(),
            data,
        });
    }
}

pub async fn fetch_track_data(chateau_id: &str, pool: &CatenaryPostgresPool) -> TrackData {
    match chateau_id {
        "metrolinktrains" => {
            let url = "https://rtt.metrolinktrains.com/StationScheduleList.json";

            match reqwest::get(url).await {
                Ok(r) => {
                    let response = r.json::<Vec<MetrolinkTrackData>>().await;

                    //println!("{:?}", response);

                    match response {
                        Ok(response) => {
                            let track_lookup = metrolink_station_schedule_decode(
                                response,
                                MetrolinkOrAmtrakStopCodes::Metrolink,
                            )
                            .await;

                            TrackData::Metrolink(Some(track_lookup))
                        }
                        Err(e) => {
                            println!("Error decoding Metrolink data: {}", e);
                            TrackData::Metrolink(None)
                        }
                    }
                }
                Err(e) => {
                    println!("Error fetching Metrolink data: {}", e);
                    TrackData::Metrolink(None)
                }
            }
        }
        "amtrak" => {
            let url = "https://rtt.metrolinktrains.com/StationScheduleList.json";

            let mut multisource = AmtrakTrackDataMultisource {
                metrolink: None,
                lirr_mnr: None,
            };

            match reqwest::get(url).await {
                Ok(r) => {
                    let response = r.json::<Vec<MetrolinkTrackData>>().await;

                    //println!("{:?}", response);

                    match response {
                        Ok(response) => {
                            let track_lookup = metrolink_station_schedule_decode(
                                response,
                                MetrolinkOrAmtrakStopCodes::Amtrak,
                            )
                            .await;

                            multisource.metrolink = Some(track_lookup);
                        }
                        Err(e) => {
                            println!("Error decoding Metrolink data: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Error fetching Metrolink data: {}", e);
                }
            }

            multisource.lirr_mnr = lirr_mnr::fetch_lirr_mnr_track_data(chateau_id, pool).await;

            TrackData::Amtrak(multisource)
        }
        "nationalrailuk" => {
            let url = "http://localhost:26993/platforms-v2";
            match reqwest::get(url).await {
                Ok(r) => match r.json::<HashMap<String, Vec<PlatformInfo>>>().await {
                    Ok(response) => TrackData::NationalRail(response),
                    Err(e) => {
                        println!("Error decoding National Rail data: {}", e);
                        TrackData::NationalRail(HashMap::new())
                    }
                },
                Err(e) => {
                    println!("Error fetching National Rail data: {}", e);
                    TrackData::NationalRail(HashMap::new())
                }
            }
        }
        "île~de~france~mobilités" => {
            let url = "http://localhost:46299/platforms";

            match reqwest::get(url).await {
                Ok(r) => match r.json::<HashMap<String, Vec<CommonPlatformInfo>>>().await {
                    Ok(response) => TrackData::IleDeFrance(response),
                    Err(e) => {
                        println!("Error decoding Île-de-France platform data: {}", e);
                        TrackData::IleDeFrance(HashMap::new())
                    }
                },
                Err(e) => {
                    println!("Error fetching Île-de-France platform data: {}", e);
                    TrackData::IleDeFrance(HashMap::new())
                }
            }
        }
        "danmark" => {
            let url = "http://localhost:46372/platforms";

            match reqwest::get(url).await {
                Ok(r) => match r.json::<HashMap<String, Vec<CommonPlatformInfo>>>().await {
                    Ok(response) => TrackData::Danmark(response),
                    Err(e) => {
                        println!("Error decoding Danmark platform data: {}", e);
                        TrackData::Danmark(HashMap::new())
                    }
                },
                Err(e) => {
                    println!("Error fetching Danmark platform data: {}", e);
                    TrackData::Danmark(HashMap::new())
                }
            }
        }
        "schweiz" => {
            let url = "http://localhost:46485/platforms";

            match reqwest::get(url).await {
                Ok(r) => match r.json::<HashMap<String, Vec<CommonPlatformInfo>>>().await {
                    Ok(response) => TrackData::Schweiz(response),
                    Err(e) => {
                        println!("Error decoding Schweiz platform data: {}", e);
                        TrackData::Schweiz(HashMap::new())
                    }
                },
                Err(e) => {
                    println!("Error fetching Schweiz platform data: {}", e);
                    TrackData::Schweiz(HashMap::new())
                }
            }
        }
        "viarail" => match viarail::fetch_via_rail(pool).await {
            Some(data) => TrackData::ViaRail(Some(data)),
            None => TrackData::ViaRail(None),
        },
        "metro~northrailroad" => TrackData::MetroNorthRailroad(
            lirr_mnr::fetch_lirr_mnr_track_data(chateau_id, pool).await,
        ),
        "longislandrailroad" => TrackData::LongIslandRailroad(
            lirr_mnr::fetch_lirr_mnr_track_data(chateau_id, pool).await,
        ),
        "nyct" => TrackData::NyctSubway(nyct_subway::fetch_nyct_subway_track_data().await),
        "sncf" => {
            // Track/platform data changes much more slowly than individual GTFS-RT jobs.
            // Reuse a fresh immutable snapshot instead of downloading and reparsing the
            // same large SIRI document for every chateau rebuild.
            if let Some(cached) = cached_sncf_data(Some(SNCF_CACHE_TTL)) {
                return TrackData::Sncf(Some(cached));
            }

            let url =
                "https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-estimated-timetable";

            let response = sncf_http_client()
                .get(url)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status);

            let body = match response {
                Ok(response) => match response.text().await {
                    Ok(body) => body,
                    Err(error) => {
                        eprintln!(
                            "Error reading SNCF SIRI data: {}. Using stale cache if available.",
                            error
                        );
                        return TrackData::Sncf(cached_sncf_data(None));
                    }
                },
                Err(error) => {
                    eprintln!(
                        "Error fetching SNCF SIRI data: {}. Using stale cache if available.",
                        error
                    );
                    return TrackData::Sncf(cached_sncf_data(None));
                }
            };

            // Parsing a multi-megabyte XML-ish SIRI document is CPU work. Do not run it
            // on an Alpenrose Tokio worker, where it can prevent unrelated jobs from
            // making progress. The returned Arc is shared by the job and the cache.
            match tokio::task::spawn_blocking(move || sncf_siri::parse_sncf_siri(&body)).await {
                Ok(parsed) => {
                    let parsed = Arc::new(parsed);
                    store_sncf_data(Arc::clone(&parsed));
                    TrackData::Sncf(Some(parsed))
                }
                Err(error) => {
                    eprintln!(
                        "SNCF SIRI parser task failed: {}. Using stale cache if available.",
                        error
                    );
                    TrackData::Sncf(cached_sncf_data(None))
                }
            }
        }
        _ => TrackData::None,
    }
}
