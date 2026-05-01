use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MtaSubwayTrips {
    pub trips: Vec<Trip>,
    pub stale_route_ids: Vec<Value>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trip {
    pub route_id: String,
    pub direction: String,
    #[serde(rename = "tripId")]
    pub nyct_train_id: String,
    pub shape_segment_ids: Vec<String>,
    pub estimated_latitude: Option<f64>,
    pub estimated_longitude: Option<f64>,
    pub is_assigned: bool,
    pub is_delayed: bool,
    pub headsign: String,
    pub consist: Option<Consist>,
    pub consist_cars: Option<Vec<ConsistCar>>,
    pub stops: Vec<Stop>,
    pub source: String,
    pub updated_at: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Consist {
    pub car_count: i64,
    pub car_length_feet: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsistCar {
    pub number: String,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stop {
    pub station_id: i64,
    pub bubble_id: String,
    pub section_id: String,
    pub platform_edges: Vec<String>,
    pub est_arrive_at: i64,
    pub stop_status: String,
    pub transfers: Vec<String>,
    pub rail_transfers: Vec<String>,
    #[serde(default)]
    pub attributes: Vec<String>,
}

impl Trip {
    /// Generates a condensed summary string of the consist car numbers.
    /// Groups consecutive car numbers into ranges (e.g., "6645-6641").
    pub fn get_consist_summary(&self) -> String {
        let cars = match &self.consist_cars {
            Some(c) if !c.is_empty() => c,
            _ => return String::new(),
        };

        // Parse numbers into integers, ignoring any that fail to parse
        let nums: Vec<i32> = cars
            .iter()
            .filter_map(|car| car.number.parse::<i32>().ok())
            .collect();

        if nums.is_empty() {
            return String::new();
        }

        let mut ranges = Vec::new();
        let mut start_idx = 0;

        for i in 1..=nums.len() {
            // Check if we have reached the end or if the sequence is broken
            // A sequence is broken if the absolute difference is not 1
            if i == nums.len() || (nums[i] - nums[i - 1]).abs() != 1 {
                let range_str = if start_idx == i - 1 {
                    // Single car, no range
                    nums[start_idx].to_string()
                } else {
                    // Range found
                    format!("{}-{}", nums[start_idx], nums[i - 1])
                };

                ranges.push(range_str);
                start_idx = i;
            }
        }

        ranges.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    #[tokio::test]
    async fn test_fetch_and_deserialize_helium_subway() {
        let client = Client::new();
        let res = client
            .get("https://helium-prod.mylirr.org/v1/subway/trips")
            .send()
            .await
            .expect("Failed to send request");

        assert!(res.status().is_success());

        let json_text = res.text().await.expect("Failed to get text");

        let data: Result<MtaSubwayTrips, _> = serde_json::from_str(&json_text);
        assert!(
            data.is_ok(),
            "Failed to deserialize Subway Trips: {:?}",
            data.err()
        );
    }
}
