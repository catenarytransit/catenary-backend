// Initial version 3 of ingest: Kyler Chin
//This was heavily inspired and copied from Emma Alexia, thank you Emma!

use service::quicli::prelude::info;
use sqlx::migrate::MigrateDatabase;
use sqlx::postgres::PgPoolOptions;
use sqlx::query;
use sqlx::{Connection, PgConnection, PgPool, Postgres};
use std::time::Duration;
mod database;

mod transitland_import;

#[tokio::main]
async fn main() {
    info!("Initializing database connection");

    let pool = database::connect()
        .await
        .expect("Database connection failed");
    let mut transaction = pool.begin().await.unwrap();

    //migrate database
    let _ = database::check_for_migrations().await;

    let eligible_feeds = transitland_import::download_return_eligible_feeds().await;
}
