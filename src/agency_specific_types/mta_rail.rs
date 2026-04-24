use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainStatus {
    pub otp: Option<i32>,
    pub otp_location: Option<String>,
    pub held: bool,
    pub canceled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainCar {
    #[serde(rename = "type")]
    pub traintype: String,
    pub number: Option<i32>,
    pub loading: String,
    pub passengers: Option<u32>,
    pub restroom: Option<bool>,
    pub revenue: Option<bool>,
    pub bikes: Option<i32>,
    pub locomotive: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainConsist {
    pub cars: Vec<TrainCar>,
    pub fleet: Option<String>,
    pub actual_len: Option<i32>,
    pub sched_len: Option<i32>,
    pub occupancy: Option<String>,
    pub occupancy_timestamp: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainLocation {
    pub longitude: f32,
    pub latitude: f32,
    pub speed: Option<f32>,
    pub heading: Option<f32>,
    pub source: String,
    pub timestamp: i32,
    pub extra_info: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainTurf {
    pub length: f32,
    pub location_mp: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainStop {
    pub code: String,
    pub sched_time: i32,
    pub sign_track: Option<String>,
    pub avps_track_id: Option<String>,
    pub posted: bool,
    pub t2s_track: String,
    pub stop_status: Option<String>,
    pub stop_type: String,
    pub track_change: Option<bool>,
    pub local_cancel: Option<bool>,
    pub bus: bool,
    pub occupancy: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrainDetails {
    pub headsign: String,
    pub summary: String,
    pub peak_code: String,
    pub branch: Option<String>,
    pub stops: Vec<TrainStop>,
    pub direction: String,
    pub turf: Option<TrainTurf>,
    pub bike_rule: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MtaTrain {
    pub train_id: String,
    pub railroad: String,
    pub run_date: String,
    pub train_num: String,
    pub realtime: bool,
    pub details: TrainDetails,
    pub consist: TrainConsist,
    pub location: TrainLocation,
    pub status: TrainStatus,
}
