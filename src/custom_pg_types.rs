#[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
#[diesel(postgres_type(name = "trip_frequency"))]
pub struct TripFrequency {
    pub start_time: i32,
    pub end_time: i32,
    pub headway_secs: i32,
    pub exact_times: bool,
}
