mod colour_correction;
mod convex_hull;
mod flatten;

#[derive(Debug, Clone)]
pub struct DownloadAttempt {
    pub onestop_feed_id: String,
    pub file_hash: String,
    pub downloaded_unix_time_ms: i64,
    pub ingested: bool,
    pub failed: bool,
    pub mark_for_redo: bool,
    pub ingestion_version: i32,
}
