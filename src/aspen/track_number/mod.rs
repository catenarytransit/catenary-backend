use catenary::postgres_tools::CatenaryPostgresPool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod metrolinx_platforms;
pub mod viarail;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub stop_id: String,
    pub sequence: u32,
    pub platform: String,
}

#[derive(Clone, Debug)]
pub enum TrackData {
    //output Option<MetrolinkOutputTrackData> instead
    Metrolink(Option<MetrolinkOutputTrackData>),
    Amtrak(AmtrakTrackDataMultisource),
    NationalRail(HashMap<String, Vec<PlatformInfo>>),
    ViaRail(Option<viarail::ViaRailTrackData>),
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
        let track_data = super::fetch_track_data("metrolinktrains").await;
        match track_data {
            super::TrackData::Metrolink(m_data) => {
                assert!(m_data.is_some());
            }
            _ => panic!("Expected Metrolink data"),
        }

        let track_data = super::fetch_track_data("amtrak").await;
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

            let mut multisource = AmtrakTrackDataMultisource { metrolink: None };

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
        "viarail" => match viarail::fetch_via_rail(pool).await {
            Some(data) => TrackData::ViaRail(Some(data)),
            None => TrackData::ViaRail(None),
        },
        _ => TrackData::None,
    }
}
