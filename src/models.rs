use diesel::prelude::*;
use postgis_diesel::operators::*;
use postgis_diesel::sql_types::Geometry;
use postgis_diesel::types::*;

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
