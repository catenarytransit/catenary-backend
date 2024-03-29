use crate::custom_pg_types;
use crate::custom_pg_types::TripFrequency;
use diesel::backend::RawValue;
use diesel::deserialize;
use diesel::deserialize::FromSql;
use diesel::pg;
use diesel::pg::sql_types::Jsonb;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use diesel::sql_types::*;
use diesel::AsExpression;
use diesel::FromSqlRow;
use diesel::SqlType;
use serde_json::Value;
use std::io::Write;

#[derive(Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = crate::schema::gtfs::shapes)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Shape {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub shape_id: String,
    pub linestring: postgis_diesel::types::LineString<postgis_diesel::types::Point>,
    pub color: Option<String>,
    pub routes: Option<Vec<Option<String>>>,
    pub route_type: i16,
    pub route_label: Option<String>,
    pub route_label_translations: Option<serde_json::Value>,
    pub text_color: Option<String>,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::static_download_attempts)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StaticDownloadAttempt {
    pub onestop_feed_id: String,
    pub file_hash: Option<String>,
    pub downloaded_unix_time_ms: i64,
    pub ingested: bool,
    pub url: String,
    pub failed: bool,
    pub ingestion_version: i32,
    pub mark_for_redo: bool,
    pub http_response_code: Option<String>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::chateaus)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Chateau {
    pub chateau: String,
    pub static_feeds: Vec<Option<String>>,
    pub realtime_feeds: Vec<Option<String>>,
    pub languages_avaliable: Vec<Option<String>>,
    pub hull: Option<postgis_diesel::types::Polygon<postgis_diesel::types::Point>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::static_feeds)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StaticFeed {
    pub onestop_feed_id: String,
    pub chateau: String,
    pub previous_chateau_name: String,
    pub hull: Option<postgis_diesel::types::Polygon<postgis_diesel::types::Point>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::realtime_feeds)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct RealtimeFeed {
    pub onestop_feed_id: String,
    pub chateau: String,
    pub previous_chateau_name: String,
    pub fetch_interval_ms: Option<i32>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::ingested_static)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct IngestedStatic {
    pub onestop_feed_id: String,
    pub ingest_start_unix_time_ms: i64,
    pub file_hash: String,
    pub attempt_id: String,
    pub ingesting_in_progress: bool,
    pub ingestion_successfully_finished: bool,
    pub ingestion_errored: bool,
    pub production: bool,
    pub deleted: bool,
    pub feed_expiration_date: Option<chrono::NaiveDate>,
    pub feed_start_date: Option<chrono::NaiveDate>,
    pub languages_avaliable: Vec<Option<String>>,
    pub ingestion_version: i32,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::agencies)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Agency {
    pub static_onestop_id: String,
    pub agency_id: String,
    pub attempt_id: String,
    pub agency_name: String,
    pub agency_name_translations: Option<Value>,
    pub agency_url: String,
    pub agency_url_translations: Option<Value>,
    pub agency_timezone: String,
    pub agency_lang: Option<String>,
    pub agency_phone: Option<String>,
    pub agency_fare_url: Option<String>,
    pub agency_fare_url_translations: Option<Value>,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::trips)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Trip {
    pub onestop_feed_id: String,
    pub trip_id: String,
    pub attempt_id: String,
    pub route_id: String,
    pub service_id: String,
    pub trip_headsign: Option<String>,
    pub trip_headsign_translations: Option<Value>,
    pub has_stop_headsigns: bool,
    pub stop_headsigns: Option<Vec<Option<String>>>,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<i16>,
    pub block_id: Option<String>,
    pub shape_id: Option<String>,
    pub wheelchair_accessible: Option<i16>,
    pub bikes_allowed: i16,
    pub chateau: String,
    pub frequencies: Option<Vec<Option<TripFrequencyModel>>>,
}

#[derive(Clone, Debug, PartialEq, AsExpression)]
#[sql_type = "diesel::sql_types::Uuid"]
pub struct TripFrequencyModel {
    pub start_time: i32,
    pub end_time: i32,
    pub headway_secs: i32,
    pub exact_times: bool,
}

use diesel::serialize::Output;
use diesel::serialize::WriteTuple;

// Learned from https://inve.rs/postgres-diesel-composite/
// https://docs.diesel.rs/2.0.x/diesel/deserialize/trait.FromSql.html
// https://docs.diesel.rs/2.0.x/diesel/serialize/trait.ToSql.html

// Imports https://docs.diesel.rs/master/diesel/pg/struct.PgValue.html as backend raw value
impl ToSql<TripFrequency, Pg> for TripFrequencyModel {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        WriteTuple::<(Int4, Int4, Int4, Bool)>::write_tuple(
            &(
                self.start_time.clone(),
                self.end_time.clone(),
                self.headway_secs.clone(),
                self.exact_times.clone(),
            ),
            out,
        )
    }
}

impl FromSql<TripFrequency, Pg> for TripFrequencyModel {
    fn from_sql(bytes: diesel::pg::PgValue) -> diesel::deserialize::Result<Self> {
        let (start_time, end_time, headway_secs, exact_times) =
            FromSql::<Record<(Int4, Int4, Int4, Bool)>, Pg>::from_sql(bytes)?;

        Ok(TripFrequencyModel {
            start_time,
            end_time,
            headway_secs,
            exact_times,
        })
    }
}
