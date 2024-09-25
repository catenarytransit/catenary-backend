use crate::aspen_dataset::AspenStopTimeEvent;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtKey {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time_secs: Option<u32>,
    pub start_date: Option<chrono::NaiveDate>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtCacheEntry {
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub events: Vec<AspenStopTimeEvent>,
}

//Each Rt cache entry can be inserted directly into Postgres as a cache?
