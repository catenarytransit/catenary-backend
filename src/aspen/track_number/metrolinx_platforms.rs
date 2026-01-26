use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetrolinxStopResponse {
    #[serde(rename = "trainDepartures")]
    pub train_departures: MetrolinxTrainDepartures,
    pub stationCode: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetrolinxTrainDepartures {
    pub items: Vec<MetrolinxDepartureItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetrolinxDepartureItem {
    #[serde(rename = "tripNumber")]
    pub trip_number: String,
    pub platform: String,
    pub service: String,
    #[serde(rename = "lineCode")]
    pub line_code: String,
    #[serde(rename = "allDepartureStops")]
    pub all_departure_stops: Option<MetrolinxAllDepartureStops>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetrolinxAllDepartureStops {
    #[serde(rename = "departureDetailsList")]
    pub departure_details_list: Vec<MetrolinxDepartureDetail>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetrolinxDepartureDetail {
    #[serde(rename = "stopCode")]
    pub stop_code: String,
    // We might not need departureTime for now if we just match by trip ID
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpExpressStopResponse {
    pub departures: Vec<UpExpressDeparture>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpExpressDeparture {
    pub platform: String,
    #[serde(rename = "tripNumber")]
    pub trip_number: String,
}

pub const ALL_METROLINX_STATIONS: &[&str] = &[
    "WR", "WH", "WE", "UN", "UI", "ST", "SR", "SCTH", "SC", "RU", "RO", "RI", "PO", "PIN", "PA",
    "OS", "OR", "OL", "OA", "NI", "NE", "MR", "MP", "MO", "ML", "MK", "MJ", "MI", "ME", "MD", "MA",
    "LS", "LO", "LI", "LA", "KP", "KI", "KE", "KC", "HA", "GU", "GO", "GL", "GE", "EX", "ET", "ER",
    "EG", "EA", "DW", "DI", "DA", "CO", "CL", "CF", "CE", "BU", "BR", "BO", "BM", "BL", "BE", "BD",
    "BA", "AU", "AP", "AL", "AJ", "AG", "AD", "AC",
];

pub const ALL_UPEXPRESS_STATIONS: &[&str] = &["WE", "UN", "PA", "MD", "BL"];

/// Fetches platform assignments for a list of stop codes.
/// Returns a HashMap where Key = (TripNumber, StopCode) and Value = Platform.
pub async fn fetch_metrolinx_platforms(
    stops_to_fetch: Vec<String>,
) -> AHashMap<(String, String), String> {
    use futures::{StreamExt, stream};

    let client = reqwest::Client::new();

    let bodies = stream::iter(stops_to_fetch.into_iter())
        .map(|stop_code| {
            let client = client.clone();
            async move {
                let url = format!(
                    "https://api.metrolinx.com/external/go/departures/stops/{}/departures?page=1&transitTypeName=All&pageLimit=100",
                    stop_code
                );
                let resp = client.get(&url).send().await;
                (stop_code, resp)
            }
        })
        .buffer_unordered(20); // Limit concurrency to avoid overloading the server or local limits

    let mut platforms_map = AHashMap::new();

    let results: Vec<(String, Option<MetrolinxStopResponse>)> = bodies
        .map(|(stop_code, resp)| async move {
            match resp {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.json::<MetrolinxStopResponse>().await {
                            Ok(data) => (stop_code.to_string(), Some(data)),
                            Err(e) => {
                                eprintln!("Error parsing Metrolinx data for {}: {}", stop_code, e);
                                (stop_code.to_string(), None)
                            }
                        }
                    } else {
                        eprintln!(
                            "Error fetching Metrolinx data for {}: Status {}",
                            stop_code,
                            resp.status()
                        );
                        (stop_code.to_string(), None)
                    }
                }
                Err(e) => {
                    eprintln!("Error connecting to Metrolinx API for {}: {}", stop_code, e);
                    (stop_code.to_string(), None)
                }
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    for (_, data_opt) in results {
        if let Some(data) = data_opt {
            for item in data.train_departures.items {
                if item.platform == "-" || item.platform.trim().is_empty() {
                    continue;
                }
                platforms_map.insert(
                    (item.trip_number.clone(), data.stationCode.clone()),
                    item.platform.clone(),
                );
            }
        }
    }

    platforms_map
}

/// Fetches platform assignments for a list of UP Express stop codes.
/// Returns a HashMap where Key = (TripNumber, StopCode) and Value = Platform.
pub async fn fetch_upexpress_platforms(
    stops_to_fetch: Vec<String>,
) -> AHashMap<(String, String), String> {
    use futures::{StreamExt, stream};

    let client = reqwest::Client::new();

    let bodies = stream::iter(stops_to_fetch.into_iter())
        .map(|stop_code| {
            let client = client.clone();
            async move {
                let url = format!(
                    "https://api.metrolinx.com/external/upe/tdp/up/departures/{}",
                    stop_code
                );
                let resp = client.get(&url).send().await;
                (stop_code, resp)
            }
        })
        .buffer_unordered(20);

    let mut platforms_map = AHashMap::new();

    let results: Vec<(String, Option<UpExpressStopResponse>)> = bodies
        .map(|(stop_code, resp)| async move {
            match resp {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.json::<UpExpressStopResponse>().await {
                            Ok(data) => (stop_code, Some(data)),
                            Err(e) => {
                                eprintln!("Error parsing UP Express data for {}: {}", stop_code, e);
                                (stop_code, None)
                            }
                        }
                    } else {
                        eprintln!(
                            "Error fetching UP Express data for {}: Status {}",
                            stop_code,
                            resp.status()
                        );
                        (stop_code, None)
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Error connecting to UP Express API for {}: {}",
                        stop_code, e
                    );
                    (stop_code, None)
                }
            }
        })
        .buffer_unordered(20)
        .collect()
        .await;

    for (stop_code, data_opt) in results {
        if let Some(data) = data_opt {
            for item in data.departures {
                if item.platform == "-" || item.platform.trim().is_empty() {
                    continue;
                }
                platforms_map.insert(
                    (item.trip_number.clone(), stop_code.clone()),
                    item.platform.clone(),
                );
            }
        }
    }

    platforms_map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metrolinx_json() {
        let json_data = r##"
{
  "stationCode": "UN",
  "trainDepartures": {
    "items": [
      {
        "lineCode": "KI",
        "tripNumber": "3539",
        "service": "Kitchener",
        "transitType": 1,
        "transitTypeName": "T",
        "scheduledTime": "23:34",
        "scheduledDateTime": "2026-01-23T23:34:00",
        "platform": "4",
        "scheduledPlatform": null,
        "stopsDisplay": "Bloor-Mount Dennis-Weston",
        "info": "Wait / Attendez",
        "lineColour": "#00853e",
        "allDepartureStops": {
          "stayInTrain": false,
          "tripNumbers": [
            "3539"
          ],
          "departureDetailsList": [
            {
              "stopName": "Union Station",
              "departureTime": "23:34",
              "stopCode": "UN",
              "isMajorStop": true
            }
          ]
        },
        "zone": null,
        "gate": null
      },
       {
        "lineCode": "LW",
        "tripNumber": "1439",
        "service": "Lakeshore West",
        "transitType": 1,
        "transitTypeName": "T",
        "scheduledTime": "23:47",
        "scheduledDateTime": "2026-01-23T23:47:00",
        "platform": "-",
        "scheduledPlatform": null,
        "stopsDisplay": "Exhibition-Mimico",
        "info": "Wait / Attendez",
        "lineColour": "#98002e",
        "allDepartureStops": null,
        "zone": null,
        "gate": null
      }
    ]
  }
}
"##;

        let response: MetrolinxStopResponse =
            serde_json::from_str(json_data).expect("Should parse");
        assert_eq!(response.stationCode, "UN");
        assert_eq!(response.train_departures.items.len(), 2);

        let item1 = &response.train_departures.items[0];
        assert_eq!(item1.trip_number, "3539");
        assert_eq!(item1.platform, "4");

        let item2 = &response.train_departures.items[1];
        assert_eq!(item2.trip_number, "1439");
        assert_eq!(item2.platform, "-");
    }
}
