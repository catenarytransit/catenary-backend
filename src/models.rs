// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use crate::custom_pg_types::TripFrequency;
use diesel::deserialize::FromSql;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use diesel::sql_types::*;
use diesel::AsExpression;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;

#[derive(Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = crate::schema::gtfs::itinerary_pattern)]
pub struct ItineraryPatternRow {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub itinerary_pattern_id: String,
    pub stop_sequence: i32,
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub stop_id: String,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = crate::schema::gtfs::itinerary_pattern_meta)]
pub struct ItineraryPatternMeta {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub trip_ids: Vec<Option<String>>,
    pub itinerary_pattern_id: String,
    pub chateau: String,
    pub trip_headsign: Option<String>,
    pub trip_headsign_translations: Option<Value>,
    pub shape_id: Option<String>,
    pub timezone: String,
    pub route_id: String,
}

#[derive(Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = crate::schema::gtfs::trips_compressed)]
pub struct CompressedTrip {
    pub onestop_feed_id: String,
    pub trip_id: String,
    pub attempt_id: String,
    pub service_id: String,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<bool>,
    pub block_id: Option<String>,
    pub wheelchair_accessible: i16,
    pub bikes_allowed: i16,
    pub chateau: String,
    pub frequencies: Option<Vec<u8>>,
    pub has_frequencies: bool,
    pub itinerary_pattern_id: String,
    pub route_id: String,
    pub start_time: u32
}

#[derive(Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = crate::schema::gtfs::shapes)]
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
    //insert with false, then enable after when mark for production
    pub allowed_spatial_query: bool,
}

#[derive(Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = crate::schema::gtfs::shapes_not_bus)]
pub struct ShapeNotBus {
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
    //insert with false, then enable after when mark for production
    pub allowed_spatial_query: bool,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::static_download_attempts)]
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
pub struct Chateau {
    pub chateau: String,
    pub static_feeds: Vec<Option<String>>,
    pub realtime_feeds: Vec<Option<String>>,
    pub languages_avaliable: Vec<Option<String>>,
    pub hull: Option<postgis_diesel::types::MultiPolygon<postgis_diesel::types::Point>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::static_feeds)]
pub struct StaticFeed {
    pub onestop_feed_id: String,
    pub chateau: String,
    pub previous_chateau_name: String,
    pub default_lang: Option<String>,
    pub languages_avaliable: Vec<Option<String>>,
    pub hull: Option<postgis_diesel::types::Polygon<postgis_diesel::types::Point>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::realtime_feeds)]
pub struct RealtimeFeed {
    pub onestop_feed_id: String,
    pub chateau: String,
    pub previous_chateau_name: String,
    pub fetch_interval_ms: Option<i32>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = crate::schema::gtfs::routes)]
pub struct Route {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub route_id: String,
    pub short_name: Option<String>,
    pub short_name_translations: Option<serde_json::Value>,
    pub long_name: Option<String>,
    pub long_name_translations: Option<serde_json::Value>,
    pub gtfs_desc: Option<String>,
    pub gtfs_desc_translations: Option<serde_json::Value>,
    pub route_type: i16,
    pub url: Option<String>,
    pub url_translations: Option<serde_json::Value>,
    pub agency_id: Option<String>,
    pub gtfs_order: Option<u32>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub continuous_pickup: i16,
    pub continuous_drop_off: i16,
    pub shapes_list: Option<Vec<Option<String>>>,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = crate::schema::gtfs::stopsforroute)]
pub struct StopsForRoute {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub route_id: String,
    pub stops: Option<Vec<u8>>,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::ingested_static)]
pub struct IngestedStatic {
    pub onestop_feed_id: String,
    pub ingest_start_unix_time_ms: i64,
    pub ingest_end_unix_time_ms: i64,
    pub ingest_duration_ms: i32,
    pub file_hash: String,
    pub attempt_id: String,
    pub ingesting_in_progress: bool,
    pub ingestion_successfully_finished: bool,
    pub ingestion_errored: bool,
    pub production: bool,
    pub deleted: bool,
    pub feed_expiration_date: Option<chrono::NaiveDate>,
    pub feed_start_date: Option<chrono::NaiveDate>,
    pub ingestion_version: i32,
    pub default_lang: Option<String>,
    pub languages_avaliable: Vec<Option<String>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::in_progress_static_ingests)]
pub struct InProgressStaticIngest {
    pub onestop_feed_id: String,
    pub file_hash: String,
    pub attempt_id: String,
    pub ingest_start_unix_time_ms: i64,
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
#[diesel(table_name = crate::schema::gtfs::stops)]
pub struct Stop {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub gtfs_id: String,
    pub name: Option<String>,
    pub name_translations: Option<Value>,
    pub displayname: Option<String>,
    pub code: Option<String>,
    pub gtfs_desc: Option<String>,
    pub gtfs_desc_translations: Option<Value>,
    pub location_type: i16,
    pub parent_station: Option<String>,
    pub zone_id: Option<String>,
    pub url: Option<String>,
    pub point: Option<postgis_diesel::types::Point>,
    pub timezone: Option<String>,
    pub wheelchair_boarding: i16,
    pub primary_route_type: Option<i16>,
    pub level_id: Option<String>,
    pub platform_code: Option<String>,
    pub platform_code_translations: Option<Value>,
    pub routes: Vec<Option<String>>,
    pub route_types: Vec<Option<i16>>,
    pub children_ids: Vec<Option<String>>,
    pub children_route_types: Vec<Option<i16>>,
    pub station_feature: bool,
    pub hidden: bool,
    pub chateau: String,
    pub location_alias: Option<Vec<Option<String>>>,
    pub tts_name_translations: Option<Value>,
    pub tts_name: Option<String>,
    //insert with false, then enable after when mark for production
    pub allowed_spatial_query: bool,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::calendar)]
pub struct Calendar {
    pub onestop_feed_id: String,
    pub service_id: String,
    pub monday: bool,
    pub tuesday: bool,
    pub wednesday: bool,
    pub thursday: bool,
    pub friday: bool,
    pub saturday: bool,
    pub sunday: bool,
    pub gtfs_start_date: chrono::NaiveDate,
    pub gtfs_end_date: chrono::NaiveDate,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::calendar_dates)]
pub struct CalendarDate {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub service_id: String,
    pub gtfs_date: chrono::NaiveDate,
    pub exception_type: i16,
    pub chateau: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::feed_info)]
pub struct FeedInfo {
    pub onestop_feed_id: String,
    pub feed_publisher_name: String,
    pub feed_publisher_url: String,
    pub feed_lang: String,
    pub feed_start_date: Option<chrono::NaiveDate>,
    pub feed_end_date: Option<chrono::NaiveDate>,
    pub feed_version: Option<String>,
    pub chateau: String,
    pub default_lang: Option<String>,
    pub feed_contact_email: Option<String>,
    pub feed_contact_url: Option<String>,
    pub attempt_id: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::static_passwords)]
pub struct StaticPasswordRow {
    onestop_feed_id: String,
    passwords: Option<Value>,
    last_updated_ms: i64,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::realtime_passwords)]
pub struct RealtimePasswordRow {
    pub onestop_feed_id: String,
    pub passwords: Option<Value>,
    pub last_updated_ms: i64,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::chateau_metadata_last_updated_time)]
pub struct MetadataLastUpdatedTime {
    pub catenary: i16,
    pub last_updated_ms: i64,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::gtfs::admin_credentials)]
pub struct AdminCredentials {
    pub email: String,
    pub hash: String,
    pub salt: String,
    pub last_updated_ms: i64,
}
