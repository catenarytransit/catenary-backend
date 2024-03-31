pub struct PasswordFormat {
    key_formats: Vec<KeyFormat>,
    passwords: Vec<PasswordInfo>,
    override_schedule_url: Option<String>,
    override_realtime_vehicle_positions: Option<String>,
    override_realtime_trip_updates: Option<String>,
    override_alerts: Option<String>
}

pub enum KeyFormat {
    Header(String),
    UrlQuery(String)
}

pub struct PasswordInfo {
    pub password: Vec<String>,
    pub creator_email: String,
}