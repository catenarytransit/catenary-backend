use catenary::mta_gtfs_rt::nyct::FeedMessage;
use prost::Message;
use std::collections::HashMap;

const NYCT_FEED_URLS: &[&str] = &[
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
];

#[derive(Clone, Debug, Default)]
pub struct TrackPair {
    pub scheduled_track: Option<String>,
    pub actual_track: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NyctSubwayTrackData {
    /// trip_id -> stop_id -> TrackPair
    pub track_lookup: HashMap<String, HashMap<String, TrackPair>>,
}

pub async fn fetch_nyct_subway_track_data() -> Option<NyctSubwayTrackData> {
    let client = reqwest::Client::new();
    let mut track_lookup: HashMap<String, HashMap<String, TrackPair>> = HashMap::new();

    let mut futures = Vec::new();
    for url in NYCT_FEED_URLS {
        futures.push(client.get(*url).send());
    }

    let results = futures::future::join_all(futures).await;

    for (url, res) in NYCT_FEED_URLS.iter().zip(results) {
        if let Ok(response) = res {
            if response.status().is_success() {
                if let Ok(bytes) = response.bytes().await {
                    if let Ok(feed) = FeedMessage::decode(bytes.as_ref()) {
                        for entity in feed.entity {
                            if let Some(trip_update) = entity.trip_update {
                                let trip_id = trip_update.trip.trip_id.unwrap_or_default();

                                for stu in trip_update.stop_time_update {
                                    if let Some(nyct_ext) = stu.nyct_stop_time_update {
                                        if let Some(stop_id) = &stu.stop_id {
                                            let has_actual = nyct_ext.actual_track.is_some()
                                                && !nyct_ext
                                                    .actual_track
                                                    .as_ref()
                                                    .unwrap()
                                                    .is_empty();
                                            let has_sched = nyct_ext.scheduled_track.is_some()
                                                && !nyct_ext
                                                    .scheduled_track
                                                    .as_ref()
                                                    .unwrap()
                                                    .is_empty();

                                            if has_actual || has_sched {
                                                let trip_entry = track_lookup
                                                    .entry(trip_id.clone())
                                                    .or_default();
                                                let track_pair =
                                                    trip_entry.entry(stop_id.clone()).or_default();

                                                if has_actual {
                                                    track_pair.actual_track =
                                                        nyct_ext.actual_track.clone();
                                                }
                                                if has_sched {
                                                    track_pair.scheduled_track =
                                                        nyct_ext.scheduled_track.clone();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        eprintln!("Failed to decode NYCT GTFS-RT feed: {}", url);
                    }
                }
            } else {
                eprintln!(
                    "Failed to fetch NYCT GTFS-RT feed: {} (Status: {})",
                    url,
                    response.status()
                );
            }
        } else {
            eprintln!("Error fetching NYCT GTFS-RT feed: {}", url);
        }
    }

    if track_lookup.is_empty() {
        None
    } else {
        Some(NyctSubwayTrackData { track_lookup })
    }
}
