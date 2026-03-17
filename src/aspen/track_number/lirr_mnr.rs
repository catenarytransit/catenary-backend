use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LirrMnrApiResponse {
    pub arrivals: Vec<LirrMnrArrival>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LirrMnrArrival {
    pub train_num: String,
    pub railroad: String,
    pub branch: String,
    /// Actual track assignment (may be absent if not yet assigned)
    pub track: Option<String>,
    /// Scheduled track
    pub sched_track: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LirrMnrTrackData {
    /// train_num (trip short name) -> gtfs stop_id -> track string
    pub track_lookup: HashMap<String, HashMap<String, String>>,
}

/// Fetch track data for a Metro-North or LIRR chateau from the unified LIRR/MNR arrivals API.
/// The stop `code` field from GTFS is used as the API stop code (e.g. "0NY", "0HL").
pub async fn fetch_lirr_mnr_track_data(
    chateau_id: &str,
    pool: &CatenaryPostgresPool,
) -> Option<LirrMnrTrackData> {
    let mut conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error getting DB connection for {}: {}", chateau_id, e);
            return None;
        }
    };

    use catenary::schema::gtfs::stops::dsl::{
        chateau as chateau_col, code as code_col, gtfs_id, stops,
    };

    let railroad_filter = match chateau_id {
        "metro~northrailroad" => "MNR",
        "longislandrailroad" => "LIRR",
        "amtrak" => "MNR", // only MNR has Amtrak data; LIRR only shares tracks at PSNY and it doesn't have Amtrak data there
        _ => return None,
    };

    let is_amtrak = chateau_id == "amtrak";

    // Fetch (gtfs_id, stop_code) for all stops in this chateau that have a stop code
    let stops_list: Vec<(String, String)> = if is_amtrak {
        vec![
            ("2NR".to_string(), "NRO".to_string()),
            ("2SM".to_string(), "STM".to_string()),
            ("2BP".to_string(), "BRP".to_string()),
            ("2NH".to_string(), "NHV".to_string()),
            ("2SS".to_string(), "STS".to_string()),
            ("0YK".to_string(), "YNY".to_string()),
            ("0PO".to_string(), "POU".to_string()),
        ]
    } else { 
        match stops
            .filter(chateau_col.eq(chateau_id))
            .filter(code_col.is_not_null())
            .select((gtfs_id, code_col))
            .load::<(String, Option<String>)>(&mut conn)
            .await
        {
            Ok(s) => s
                .into_iter()
                .filter_map(|(id, c)| c.map(|c| (id, c)))
                .collect(),
            Err(e) => {
                eprintln!("Error fetching stops for {}: {}", chateau_id, e);
                return None;
            }
        }
    };

    if stops_list.is_empty() {
        return None;
    }

    // Collect unique stop codes and build reverse mapping: stop_code -> list of gtfs_ids
    let mut code_to_ids: HashMap<String, Vec<String>> = HashMap::new();
    for (id, code) in &stops_list {
        code_to_ids
            .entry(code.clone())
            .or_default()
            .push(id.clone());
    }

    let unique_stop_codes: Vec<String> = code_to_ids.keys().cloned().collect();

    let client = reqwest::Client::new();

    let results: Vec<Option<(String, LirrMnrApiResponse)>> = stream::iter(unique_stop_codes)
        .map(|stop_code| {
            let client = client.clone();
            async move {
                let url = format!(
                    "https://backend-unified.mylirr.org/arrivals/{}?amtrak=true",
                    stop_code
                );
                match client
                    .get(&url)
                    .header("accept-version", "3.0")
                    .send()
                    .await
                {
                    Ok(resp) => match resp.json::<LirrMnrApiResponse>().await {
                        Ok(data) => Some((stop_code, data)),
                        Err(e) => {
                            eprintln!(
                                "Error parsing LIRR/MNR arrivals for stop {}: {}",
                                stop_code, e
                            );
                            None
                        }
                    },
                    Err(e) => {
                        eprintln!(
                            "Error fetching LIRR/MNR arrivals for stop {}: {}",
                            stop_code, e
                        );
                        None
                    }
                }
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    let mut track_lookup: HashMap<String, HashMap<String, String>> = HashMap::new();

    for item in results.into_iter().flatten() {
        let (stop_code, data) = item;

        let gtfs_ids = match code_to_ids.get(&stop_code) {
            Some(ids) => ids,
            None => continue,
        };

        for arrival in &data.arrivals {
            if arrival.railroad != railroad_filter { continue }
            if arrival.branch == "AM" && !is_amtrak { continue }
            if arrival.branch != "AM" && is_amtrak { continue }

            // Prefer live track over scheduled track; skip if neither is available
            let track = match arrival.track.as_deref().filter(|t| !t.is_empty()) {
                Some(t) => t.to_string(),
                None => match arrival.sched_track.as_deref().filter(|t| !t.is_empty()) {
                    Some(t) => t.to_string(),
                    None => continue,
                },
            };

            let train_entry = track_lookup.entry(arrival.train_num.clone()).or_default();

            for gid in gtfs_ids {
                train_entry
                    .entry(gid.clone())
                    .or_insert_with(|| track.clone());
            }
        }
    }

    Some(LirrMnrTrackData { track_lookup })
}
