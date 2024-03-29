use diesel::sql_types::*;

#[derive(Clone, Debug, SqlType)]
#[diesel(postgres_type(name = "trip_frequency", schema = "public"))]
pub struct TripFrequency {
    pub start_time: Int4,
    pub end_time: Int4,
    pub headway_secs: Int4,
    pub exact_times: Bool,
}
