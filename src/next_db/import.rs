// Initial version 3 of ingest written by Kyler Chin
// This was heavily inspired and copied from Emma Alexia, thank you Emma!
// Removal of the attribution is not allowed, as covered under the AGPL license

use service::quicli::prelude::info;
use sqlx::migrate::MigrateDatabase;
use sqlx::postgres::PgPoolOptions;
use sqlx::query;
use sqlx::{Connection, PgConnection, PgPool, Postgres};
use std::error::Error;
use std::time::Duration;
mod database;
use std::collections::HashSet;
use std::sync::Arc;

mod chateau_postprocess;
mod refresh_metadata_tables;
mod transitland_download;

use chateau::chateau;
use dmfr_folder_reader::read_folders;
use dmfr_folder_reader::ReturnDmfrAnalysis;

use crate::transitland_download::DownloadedFeedsInformation;

async fn run_ingest() -> Result<(), Box<dyn Error>> {
    //These feeds should be discarded because they are duplicated in a larger dataset called `f-sf~bay~area~rg`, which has everything in a single zip file
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

    // reads a transitland directory and returns a hashmap of all the data feeds (urls) associated with their correct operator and vise versa
    // See https://github.com/catenarytransit/dmfr-folder-reader
    let dmfr_result = read_folders("./transitland-atlas/")?;

    // The DMFR result dataset looks genuine, with over 100 pieces of data!
    if dmfr_result.feed_hashmap.len() > 100 && dmfr_result.operator_hashmap.len() > 100 {
        let eligible_feeds =
            transitland_download::download_return_eligible_feeds(&dmfr_result, &pool).await;

        // Performs depth first search to find groups of feed urls associated with each other
        // See https://github.com/catenarytransit/chateau for the source code
        let chateau_result = chateau(&dmfr_result);

        //pivot table chateau table into HashMap<FeedId, ChateauId>
        let feed_id_to_chateau_lookup =
            chateau_postprocess::feed_id_to_chateau_id_pivot_table(&chateau_result);

        // count eligible feeds that are marked ingest == true using a filter and .len()

        let mut counter_of_eligible_feeds: Option<usize> = match &eligible_feeds {
            Ok(eligible_feeds) => {
                Some(eligible_feeds.iter().filter(|download_feed_info| download_feed_info.ingest == true).collect::<Vec<&DownloadedFeedsInformation>>().len())
            }
            Err(_) => None
        };

        // debug print to output
        match counter_of_eligible_feeds {
            Some(counter_of_eligible_feeds) => {
                println!("{} feeds marked ready for schedule ingestion.", counter_of_eligible_feeds);
            },
            None => {
                println!("Unable to get eligible feed list.");
            }
        }

          //refresh the metadata for anything that's changed

        //insert the feeds that are new

        if let Ok(eligible_feeds) = eligible_feeds {
            for eligible_feed in eligible_feeds.iter() {
                if eligible_feed.ingest == true {
                    
                }
            }
        }

        //determine if the old one should be deleted, if so, delete it
    }

    Ok(())

    //let _ = refresh_metadata_tables::refresh_feed_meta(transitland_metadata.clone(), &pool);
}

#[tokio::main]
async fn main() {
    let _ = run_ingest().await;
}
