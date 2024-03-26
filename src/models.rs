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
    pub downloaded_unix_time_ms: i64,
    pub file_hash: Option<String>,
    pub ingested: bool,
    pub failed: bool,
    pub mark_for_redo: bool,
    pub url: String,
    pub ingestion_version: i32,
    pub http_response_code: Option<String>,
}
