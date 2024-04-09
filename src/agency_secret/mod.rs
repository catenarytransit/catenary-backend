// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// AGPL 3.0
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct PasswordFormat {
    key_formats: Vec<KeyFormat>,
    passwords: Vec<PasswordInfo>,
    override_schedule_url: Option<String>,
    override_realtime_vehicle_positions: Option<String>,
    override_realtime_trip_updates: Option<String>,
    override_alerts: Option<String>,
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub enum KeyFormat {
    Header(String),
    UrlQuery(String),
}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct PasswordInfo {
    pub password: Vec<String>,
    pub creator_email: String,
}
