use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Amtrak GTFS stop ID -> MTA stop code used by the unified API
const AMTRAK_TO_MTA: &[(&str, &str)] = &[
    ("STS", "2SS"), // New Haven State Street
    ("NHV", "2NH"), // New Haven
    ("BRP", "2BP"), // Bridgeport
    ("STM", "2SM"), // Stamford
    ("NYP", "NYK"), // NY Moynihan Train Hall at Penn
    ("NRO", "2NR"), // New Rochelle
    ("YNY", "0YK"), // Yonkers
];

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedTrain {
    #[serde(rename = "train_id")]
    pub train_id: String,
    pub railroad: String,
    #[serde(rename = "train_num")]
    pub train_num: String,
    pub details: UnifiedTrainDetails,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedTrainDetails {
    pub branch: Option<String>,
    pub stops: Vec<UnifiedStop>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedStop {
    pub code: String,
    #[serde(rename = "sign_track")]
    pub sign_track: Option<String>,
    #[serde(rename = "t2s_track")]
    pub t2s_track: String,
}

#[derive(Clone, Debug)]
pub struct LirrMnrTrackData {
    /// train_num (trip short name) -> gtfs stop_id -> track string
    pub track_lookup: HashMap<String, HashMap<String, String>>,
}

pub async fn fetch_lirr_mnr_track_data(
    chateau_id: &str,
    pool: &CatenaryPostgresPool,
) -> Option<LirrMnrTrackData> {
    let is_amtrak = chateau_id == "amtrak";
    let is_mnr = chateau_id == "metro~northrailroad";
    let is_lirr = chateau_id == "longislandrailroad";

    if !is_amtrak && !is_mnr && !is_lirr {
        return None;
    }

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

    // Build reverse mapping: MTA stop code -> list of GTFS stop IDs
    let mut code_to_ids: HashMap<String, Vec<String>> = HashMap::new();

    if is_amtrak {
        for &(amtrak_gtfs, mta_code) in AMTRAK_TO_MTA {
            code_to_ids
                .entry(mta_code.to_string())
                .or_default()
                .push(amtrak_gtfs.to_string());
        }
    } else {
        let db_chateau = chateau_id;
        let stops_list: Vec<(String, Option<String>)> = match stops
            .filter(chateau_col.eq(db_chateau))
            .filter(code_col.is_not_null())
            .select((gtfs_id, code_col))
            .load::<(String, Option<String>)>(&mut conn)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error fetching stops for {}: {}", chateau_id, e);
                return None;
            }
        };

        for (id, code) in stops_list {
            if let Some(c) = code {
                code_to_ids.entry(c).or_default().push(id);
            }
        }
    }

    if code_to_ids.is_empty() {
        return None;
    }

    let client = reqwest::Client::new();
    let trains: Vec<UnifiedTrain> = match client
        .get("https://backend-unified.mylirr.org/locations?geometry=TRACK_TURF&railroad=BOTH")
        .header("accept-version", "3.0")
        .header("cache-control", "no-cache")
        .header("dnt", "1")
        .header("origin", "https://radar.mta.info")
        .header("pragma", "no-cache")
        .send()
        .await
    {
        Ok(resp) => match resp.json::<Vec<UnifiedTrain>>().await {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error parsing unified locations response: {}", e);
                return None;
            }
        },
        Err(e) => {
            eprintln!("Error fetching unified locations: {}", e);
            return None;
        }
    };

    let mut track_lookup: HashMap<String, HashMap<String, String>> = HashMap::new();

    for train in &trains {
        let branch = train.details.branch.as_deref().unwrap_or("");

        let dominated_by_chateau = if is_amtrak {
            branch == "Amtrak"
        } else if is_mnr {
            train.railroad == "MNR" && branch != "Amtrak"
        } else {
            train.railroad == "LIRR" && branch != "Amtrak"
        };

        if !dominated_by_chateau {
            continue;
        }

        for stop in &train.details.stops {
            let track = stop
                .sign_track
                .as_deref()
                .filter(|t| !t.is_empty())
                .or_else(|| {
                    let t = stop.t2s_track.as_str();
                    if t.is_empty() { None } else { Some(t) }
                });

            let track = match track {
                Some(t) => t,
                None => continue,
            };

            if let Some(gtfs_ids) = code_to_ids.get(&stop.code) {
                let train_entry = track_lookup.entry(train.train_num.clone()).or_default();
                for gid in gtfs_ids {
                    train_entry
                        .entry(gid.clone())
                        .or_insert_with(|| track.to_string());
                }
            }
        }
    }

    Some(LirrMnrTrackData { track_lookup })
}
