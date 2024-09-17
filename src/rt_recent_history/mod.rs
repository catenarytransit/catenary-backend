use crate::aspen_dataset::AspenStopTimeEvent;

pub struct RtKey {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time_secs: Option<u32>,
    pub start_date: Option<chrono::NaiveDate>,
}

pub struct RtCacheEntry {
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub events: Vec<AspenStopTimeEvent>,
}
