use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ViaRailStationResponse {
    pub display_time_zone: Option<String>,
    pub active_boarding_locations: Option<Vec<String>>,
    pub arrivals: Option<Vec<ViaRailArrivalDeparture>>,
    pub departures: Option<Vec<ViaRailArrivalDeparture>>,
    pub language_priority_code: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ViaRailArrivalDeparture {
    pub train: String,
    pub schedule_date: String,
    pub destination: Option<String>,
    pub origin: Option<String>,
    pub destinations: Option<Vec<String>>,
    pub originations: Option<Vec<String>>,
    pub scheduled: String,
    pub revised: String,
    pub track: Option<String>,
    pub gate: Option<String>,
    pub platform: Option<String>,
    pub door: Option<String>,
    pub letter: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ViaRailTrackData {
    // Train (Trip Short Name) -> Stop Id -> Platform String
    pub track_lookup: HashMap<String, HashMap<String, String>>,
}

pub async fn fetch_via_rail(pool: &CatenaryPostgresPool) -> Option<ViaRailTrackData> {
    let mut conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            println!("Error getting connection for Via Rail: {}", e);
            return None;
        }
    };

    use catenary::schema::gtfs::stops::dsl::{chateau, gtfs_id, name as stop_name_col, stops};

    let stops_list: Vec<(String, Option<String>)> = match stops
        .filter(chateau.eq("viarail"))
        .select((gtfs_id, stop_name_col))
        .load::<(String, Option<String>)>(&mut conn)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            println!("Error fetching viarail stops: {}", e);
            return None;
        }
    };

    let unique_stop_names: std::collections::HashSet<String> =
        stops_list.iter().filter_map(|x| x.1.clone()).collect();

    // Mapping from Stop Name -> List of Stop IDs (for reverse lookup)
    let mut name_to_ids: HashMap<String, Vec<String>> = HashMap::new();
    for (id, name) in stops_list {
        if let Some(n) = name {
            name_to_ids.entry(n).or_default().push(id);
        }
    }

    let client = reqwest::Client::new();

    // List of futures
    let futures = unique_stop_names.into_iter().map(|name| {
        let client = &client;
        async move {
            let url = format!("https://viarailcis.ca/api/station/kiosk/{}", name);
            match client.get(&url).send().await {
                Ok(resp) => match resp.json::<ViaRailStationResponse>().await {
                    Ok(json) => Some((name, json)),
                    Err(_) => None,
                },
                Err(_) => None,
            }
        }
    });

    let bodies: Vec<Option<(String, ViaRailStationResponse)>> =
        stream::iter(futures).buffer_unordered(10).collect().await;

    let mut track_lookup: HashMap<String, HashMap<String, String>> = HashMap::new();

    for item in bodies {
        if let Some((stop_name, data)) = item {
            // Process Arrivals
            if let Some(arrivals) = data.arrivals {
                for train in arrivals {
                    process_train(&mut track_lookup, &train, &stop_name, &name_to_ids);
                }
            }
            // Process Departures
            if let Some(departures) = data.departures {
                for train in departures {
                    process_train(&mut track_lookup, &train, &stop_name, &name_to_ids);
                }
            }
        }
    }

    Some(ViaRailTrackData { track_lookup })
}

fn process_train(
    lookup: &mut HashMap<String, HashMap<String, String>>,
    train: &ViaRailArrivalDeparture,
    stop_name: &str,
    name_to_ids: &HashMap<String, Vec<String>>,
) {
    let train_num = &train.train;
    // Determine platform string from Track, Gate, Platform, Door
    let mut parts = Vec::new();
    if let Some(t) = &train.track {
        if !t.is_empty() {
            parts.push(format!("Track {}", t));
        }
    }
    if let Some(g) = &train.gate {
        if !g.is_empty() {
            parts.push(format!("Gate {}", g));
        }
    }
    if let Some(p) = &train.platform {
        if !p.is_empty() {
            parts.push(format!("Platform {}", p));
        }
    }
    if let Some(d) = &train.door {
        if !d.is_empty() {
            parts.push(format!("Door {}", d));
        }
    }

    if parts.is_empty() {
        return;
    }

    let info = parts.join(" ");

    if let Some(stop_ids) = name_to_ids.get(stop_name) {
        for stop_id in stop_ids {
            lookup
                .entry(train_num.clone())
                .or_default()
                .insert(stop_id.clone(), info.clone());
        }
    }
}
