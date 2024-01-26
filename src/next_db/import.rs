// Initial version 3 of ingest written by Kyler Chin
// This was heavily inspired and copied from Emma Alexia, thank you Emma!
// Removal of the attribution is not allowed, as covered under the AGPL license

use service::quicli::prelude::info;
use sqlx::migrate::MigrateDatabase;
use sqlx::postgres::PgPoolOptions;
use sqlx::query;
use sqlx::{Connection, PgConnection, PgPool, Postgres};
use std::time::Duration;
mod database;
use std::collections::HashSet;
mod dmfr;
use std::sync::Arc;

mod get_feeds_meta;
mod refresh_metadata_tables;
mod transitland_download;

#[tokio::main]
async fn main() {
    info!("Initializing database connection");

    let pool = database::connect()
        .await
        .expect("Database connection failed");
    let mut transaction = pool.begin().await.unwrap();

    //migrate database
    let _ = database::check_for_migrations().await;

    let transitland_metadata: Arc<get_feeds_meta::TransitlandMetadata> =
        Arc::from(get_feeds_meta::generate_transitland_metadata());

    let eligible_feeds =
        transitland_download::download_return_eligible_feeds(transitland_metadata.clone(), &pool)
            .await;

    let feeds_to_discard: HashSet<&str> = HashSet::from_iter(vec![
        "f-9q8y-sfmta",
        "f-9qc-westcat~ca~us",
        "f-9q9-actransit",
        "f-9q9-vta",
        "f-9q8yy-missionbaytma~ca~us",
        "f-9qbb-marintransit",
        "f-9q8-samtrans",
        "f-9q9-bart",
        "f-9q9-caltrain",
        "f-9qc3-riovistadeltabreeze",
    ]);

    let _ = refresh_metadata_tables::refresh_feed_meta(transitland_metadata.clone(), &pool);
}
