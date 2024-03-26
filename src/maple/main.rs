use catenary::postgres_tools::CatenaryPostgresConnection;
use catenary::postgres_tools::CatenaryPostgresPool;
// Initial version 3 of ingest written by Kyler Chin
// This was heavily inspired and copied from Emma Alexia, thank you Emma!
// Removal of the attribution is not allowed, as covered under the AGPL license
use diesel::prelude::*;
use dotenvy::dotenv;
use service::quicli::prelude::info;
use std::collections::HashSet;
use std::env;
use catenary::postgres_tools::get_connection_pool;
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use threadpool::ThreadPool;
use tokio::runtime;

mod gtfs_handlers;
mod gtfs_ingestion_sequence;

mod chateau_postprocess;
mod gtfs_process;
mod refresh_metadata_tables;
mod transitland_download;

use gtfs_process::gtfs_process_feed;

use chateau::chateau;
use dmfr_folder_reader::read_folders;
use dmfr_folder_reader::ReturnDmfrAnalysis;
use futures::StreamExt;
use git2::Repository;

use crate::gtfs_handlers::MAPLE_INGESTION_VERSION;
use crate::transitland_download::DownloadedFeedsInformation;

fn update_transitland_submodule() -> Result<(), Box<dyn Error>> {
    //Ensure git submodule transitland-atlas downloads and updates correctly
    match Repository::open("./") {
        Ok(repo) => {
            match repo.find_submodule("transitland-atlas") {
                Ok(transitland_submodule) => {
                    println!("Submodule found.");

                    let mut transitland_submodule = transitland_submodule;

                    match transitland_submodule.update(true, None) {
                        Ok(update) => {
                            println!("Submodule updated.");

                            Ok(())
                        }
                        Err(update_err) => {
                            eprintln!("Unable to update submodule");

                            // don't need to fail if can't reach github servers for now
                            Ok(())
                        }
                    }
                }
                Err(find_submodule) => {
                    eprintln!("Can't find submodule!");
                    return Err(Box::new(find_submodule));
                }
            }
        }
        Err(repo_err) => {
            eprintln!("Can't find own repo!");
            return Err(Box::new(repo_err));
        }
    }
}

async fn run_ingest() -> Result<(), Box<dyn Error>> {
    //Ensure git submodule transitland-atlas downloads and updates correctly, if not, pass the error
    let _ = update_transitland_submodule()?;

    //These feeds should be discarded because they are duplicated in a larger dataset called `f-sf~bay~area~rg`, which has everything in a single zip file
    let feeds_to_discard: HashSet<&'static str> = HashSet::from_iter(vec![
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
        "f-9q8z-goldengateferry",
        "f-9q9j-dumbartonexpress",
        "f-9qc2-trideltatransit",
        "f-9q9p-sanfranciscobayferry",
        "f-9qc-vinenapacounty",
        "f-9qb-smart",
        "f-9qc60-vacavillecitycoach",
        "f-9q9jy-unioncitytransit",
        "f-sfo~airtrain~shuttles",
        "f-9qbdx-santarosacitybus",
        "f-9q9-acealtamontcorridorexpress",
    ]);

    info!("Initializing database connection");

        // get connection pool from database pool
        let conn_pool: CatenaryPostgresPool<'_> = get_connection_pool().await;
        let arc_conn_pool: Arc<CatenaryPostgresPool<'_>> = Arc::new(conn_pool);

        let conn: CatenaryPostgresConnection<'_> = &mut arc_conn_pool.get().await.unwrap();

    // reads a transitland directory and returns a hashmap of all the data feeds (urls) associated with their correct operator and vise versa
    // See https://github.com/catenarytransit/dmfr-folder-reader
    let dmfr_result = read_folders("./transitland-atlas/")?;

    // The DMFR result dataset looks genuine, with over 100 pieces of data!
    if dmfr_result.feed_hashmap.len() > 100 && dmfr_result.operator_hashmap.len() > 100 {
        let eligible_feeds = transitland_download::download_return_eligible_feeds(
            &dmfr_result,
            &arc_conn_pool,
            &feeds_to_discard,
        )
        .await;

        // Performs depth first search to find groups of feed urls associated with each other
        // See https://github.com/catenarytransit/chateau for the source code
        let chateau_result = chateau(&dmfr_result);

        //pivot table chateau table into HashMap<FeedId, ChateauId>
        let feed_id_to_chateau_lookup =
            chateau_postprocess::feed_id_to_chateau_id_pivot_table(&chateau_result);

        // count eligible feeds that are marked ingest == true using a filter and .len()

        let mut counter_of_eligible_feeds: Option<usize> = match &eligible_feeds {
            Ok(eligible_feeds) => Some(
                eligible_feeds
                    .iter()
                    .filter(|download_feed_info| download_feed_info.ingest == true)
                    .collect::<Vec<&DownloadedFeedsInformation>>()
                    .len(),
            ),
            Err(_) => None,
        };

        // debug print to output
        match counter_of_eligible_feeds {
            Some(counter_of_eligible_feeds) => {
                println!(
                    "{} feeds marked ready for schedule ingestion.",
                    counter_of_eligible_feeds
                );
            }
            None => {
                println!("Unable to get eligible feed list.");
            }
        }

        //refresh the metadata for anything that's changed

        //insert the feeds that are new

        if let Ok(eligible_feeds) = eligible_feeds {
            let to_ingest_feeds = eligible_feeds
                .iter()
                .filter(|download_feed_info| download_feed_info.ingest == true)
                .collect::<Vec<&DownloadedFeedsInformation>>();

            // for now, use a thread pool
            // in the future, map reduce this job out to worker servers

            // Process looks like this
            // Unzip the file and handle any folder nesting
            // process into GTFS representation
            // run function to insert GTFS raw data
            // (todo) use k/d tree presentation to calculate line optimisation and transfer patterns (not clear how this works, needs further research)
            // (todo) hand off to routing algorithm preprocessing engine Prarie (needs further research and development)

            // 2. update metadata
            futures::stream::iter(eligible_feeds.iter()
            .map(|eligible_feed|
                {
                    async move {
                        let sql_query = sqlx::query!("INSERT INTO gtfs.static_download_attempts 
                            (onestop_feed_id, url, file_hash, downloaded_unix_time_ms, ingested, failed, http_response_code, mark_for_redo, ingestion_version)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ",
                        eligible_feed.feed_id, 
                        eligible_feed.url,
                        match eligible_feed.hash {
                            Some(hash) => Some(format!("{}", hash)),
                            None => None
                        }, 
                        eligible_feed.download_timestamp_ms as i64, 
                        false,
                        !eligible_feed.operation_success,
                        eligible_feed.http_response_code,
                        false,
                        MAPLE_INGESTION_VERSION
                        );
                    }
                }
            )).buffer_unordered(100).collect::<Vec<_>>().await;

            // 3. Assign Attempt IDs to each feed_id that is ready to ingest

            let _ = refresh_metadata_tables::refresh_metadata_assignments(
                &dmfr_result,
                &chateau_result,
                &Arc::clone(&pool),
            );
            

            // 4. Unzip folders
            let unzip_feeds: Vec<(String, bool)> =
                futures::stream::iter(to_ingest_feeds.into_iter().map(
                    |to_ingest_feed| async move {
                        let flatten_feed_result =
                            gtfs_handlers::flatten::flatten_feed(to_ingest_feed.feed_id.as_str());

                        (to_ingest_feed.feed_id.clone(), flatten_feed_result.is_ok())
                    },
                ))
                .buffer_unordered(8)
                .collect::<Vec<(String, bool)>>()
                .await;

            let successful_unzip_feeds_count = unzip_feeds
                .iter()
                .map(|x| x.1 == true)
                .collect::<Vec<bool>>()
                .len();

            println!(
                "{} of {} unzipped",
                successful_unzip_feeds_count,
                unzip_feeds.len()
            );

            // todo! perform additional checks to ensure feed is not a zip bomb

            // create thread pool
            // process GTFS and insert into system

            // This will spawn a work-stealing runtime with 4 worker threads.
            let rt = runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name_fn(|| {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("catenary-maple-ingest-{}", id)
                })
                .build()
                .unwrap();

            rt.spawn({
                let conn_pool = Arc::clone(&arc_conn_pool);
                async move {
                    for (feed_id, _) in unzip_feeds
                        .iter()
                        .filter(|unzipped_feed| unzipped_feed.1 == true)
                    {
                        let conn:&mut bb8::PooledConnection<'_, diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::pg::AsyncPgConnection>> = &mut arc_conn_pool.get().await.unwrap();
                        let gtfs_process_result = gtfs_process_feed(&feed_id, conn, "", "").await;

                        if gtfs_process_result.is_ok() {
                            // at the end, UPDATE gtfs.static_download_attempts where onstop_feed_id and download_unix_time_ms match as ingested

                            //CREATE entry in gtfs.ingested_static

                            //determine if the old one should be deleted, if so, delete it

                            //algorithm:
                            //call function to clean old gtfs feeds, accepting feed_id, sqlx pool as arguments
                            //greedy algorithm starts from newest feeds and examines date ranges, and works successively towards older feeds, assigning date ranges to feeds not already taken.
                            //data structure can be a Vec of (start_date, end_date, attempt_id or hash)
                            // older feeds cannot claim dates that are after a newer feed's experation date
                            //any feed that does not have a date range any
                            // more or is sufficiently old (over 5 days old) is wiped
                        } else {
                            //UPDATE gtfs.static_download_attempts where onstop_feed_id and download_unix_time_ms match as failure
                        }
                    }
                }
            });
        }
    } else {
        eprintln!("Not enough data in transitland!");
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = run_ingest().await;
}