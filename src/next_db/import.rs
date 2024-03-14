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
use std::sync::Arc;

mod refresh_metadata_tables;
mod transitland_download;

use chateau::chateau;
use dmfr_folder_reader::ReturnDmfrAnalysis;
use dmfr_folder_reader::read_folders;

#[tokio::main]
async fn main() {
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

    info!("Initializing database connection");

    let pool = database::connect()
        .await
        .expect("Database connection failed");
    let mut transaction = pool.begin().await.unwrap();

    //migrate database
    let _ = database::check_for_migrations().await;

    let dmfr_result = read_folders("./transitland-atlas/");

    if dmfr_result.feed_hashmap.len() > 100 && dmfr_result.operator_hashmap.len() > 100 {
        let eligible_feeds =
        transitland_download::download_return_eligible_feeds(&dmfr_result, &pool)
            .await;
    }

    let chateau_result = chateau(&dmfr_result);

    //let _ = refresh_metadata_tables::refresh_feed_meta(transitland_metadata.clone(), &pool);
}
