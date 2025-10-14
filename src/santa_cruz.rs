use rand::Rng;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::error::Error;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SantaCruzRoot {
    #[serde(rename = "bustime-response")]
    pub bustime_response: BustimeResponse,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BustimeResponse {
    pub vehicle: Option<Vec<Vehicle>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Vehicle {
    pub vid: String,
    pub tmstmp: String,
    pub lat: String,
    pub lon: String,
    pub hdg: String,
    pub pid: i64,
    pub rt: String,
    pub des: String,
    pub pdist: i64,
    pub dly: bool,
    pub spd: i64,
    pub tatripid: String,
    pub origtatripno: String,
    pub tablockid: String,
    pub zone: String,
    pub mode: i64,
    pub psgld: String,
    pub stst: i64,
    pub stsd: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CleanedOutput {
    pub vid: String,
    pub lat: f32,
    pub lon: f32,
    pub speed_metres_per_second: f32,
    pub heading: f32,
}

pub async fn fetch_santa_cruz_clever_data(
    route_id: &Vec<String>,
) -> Result<Vec<CleanedOutput>, Box<dyn std::error::Error + Sync + Send>> {
    //split by groups of 10

    let route_id_chunks = route_id
        .chunks(10)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    let client = reqwest::Client::new();

    let requests = route_id_chunks.iter().map(|chunk| {
        let route_ids = chunk.join(",");

        let api_keys = ["AYCrBnK6BBrfvScbPm5tSYnQU",
            "hET8c4imznEYJuBqt3kNxhcLc"
        ];

        let random_key = api_keys[rand::rng().random_range(0..api_keys.len())];

        let url = format!("https://rt.scmetro.org/bustime/api/v3/getvehicles?requestType=getvehicles&rt={}&key={}&xtime=1745892284315&format=json", route_ids, random_key);

        let client = client.clone();

        async move {
            let response = client.get(&url).send().await?;
            let status = response.status();
            if status.is_success() {
                let body = response.text().await?;
                let data: SantaCruzRoot = serde_json::from_str(&body)?;
                Ok::<SantaCruzRoot, Box<dyn Error + Sync + Send>>(data)
            } else {
                Err(format!("Santa Cruz Bustime Rt Error: {}", status).into())
            }
        }
    }).collect::<Vec<_>>();

    let results = futures::future::try_join_all(requests).await?;

    let cleaned_output = results
        .into_iter()
        .filter_map(|data| {
            if let Some(vehicles) = data.bustime_response.vehicle {
                let cleaned_vehicles: Vec<CleanedOutput> = vehicles
                    .into_iter()
                    .map(|vehicle| CleanedOutput {
                        vid: vehicle.vid,
                        lat: vehicle.lat.parse::<f32>().unwrap_or(0.0),
                        lon: vehicle.lon.parse::<f32>().unwrap_or(0.0),
                        speed_metres_per_second: vehicle.spd as f32 * 0.44704,
                        heading: vehicle.hdg.parse::<f32>().unwrap_or(0.0),
                    })
                    .collect();
                Some(cleaned_vehicles)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(cleaned_output)
}
