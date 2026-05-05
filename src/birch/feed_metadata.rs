use actix_web::{HttpRequest, HttpResponse, Responder, web};
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct FeedMetadataQuery {
    pub feed_id: String,
}

#[actix_web::get("/feed_metadata")]
pub async fn feed_metadata_endpoint(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: web::Query<FeedMetadataQuery>,
    _req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let mut conn = match conn_pool.get().await {
        Ok(c) => c,
        Err(_) => {
            return HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "application/json"))
                .body(json!({ "error": "Database connection error" }).to_string());
        }
    };

    use catenary::models::{IngestedStatic, StaticDownloadAttempt};
    use catenary::schema::gtfs::ingested_static;
    use catenary::schema::gtfs::static_download_attempts;

    let ingested_static_records = ingested_static::table
        .filter(ingested_static::dsl::onestop_feed_id.eq(&query.feed_id))
        .select(IngestedStatic::as_select())
        .load::<IngestedStatic>(&mut conn)
        .await
        .unwrap_or_else(|_| vec![]);

    let download_attempts = static_download_attempts::table
        .filter(static_download_attempts::dsl::onestop_feed_id.eq(&query.feed_id))
        .select(StaticDownloadAttempt::as_select())
        .load::<StaticDownloadAttempt>(&mut conn)
        .await
        .unwrap_or_else(|_| vec![]);

    let response = json!({
        "ingested_static": ingested_static_records,
        "static_download_attempts": download_attempts
    });

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .body(response.to_string())
}
