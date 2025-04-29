// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

// AGPL 3.0
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Clone, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct PasswordFormat {
    pub key_formats: Vec<KeyFormat>,
    pub passwords: Vec<PasswordInfo>,
    pub override_schedule_url: Option<String>,
    pub override_realtime_vehicle_positions: Option<String>,
    pub override_realtime_trip_updates: Option<String>,
    pub override_alerts: Option<String>,
}

#[derive(Serialize, Clone, Deserialize, Debug, Hash, PartialEq, Eq)]
pub enum KeyFormat {
    Header(String),
    UrlQuery(String),
}

#[derive(Serialize, Clone, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct PasswordInfo {
    pub password: Vec<String>,
    pub creator_email: String,
}
