// Initial version 3 of ingest written by Kyler Chin
// This was heavily inspired and copied from Emma Alexia, thank you Emma!
// Removal of the attribution is not allowed, as covered under the AGPL license

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::assigning_clones,
    clippy::redundant_allocation,
    bool_comparison,
    bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern
)]

use catenary::postgres_tools::make_async_pool;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use git2::Repository;
use service::quicli::prelude::info;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use tokio::runtime;

use crate::cleanup::delete_attempt_objects;

mod assign_production_tables;
mod chateau_postprocess;
mod cleanup;
mod gtfs_handlers;
mod gtfs_ingestion_sequence;
mod gtfs_process;
mod refresh_metadata_tables;
mod transitland_download;
mod update_schedules_with_new_chateau_id;

use gtfs_process::gtfs_process_feed;

use chateau::chateau;
use dmfr_folder_reader::read_folders;

use crate::gtfs_handlers::MAPLE_INGESTION_VERSION;
use crate::transitland_download::DownloadedFeedsInformation;

fn update_transitland_submodule() -> Result<(), Box<dyn Error + std::marker::Send + Sync>> {
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

fn get_threads_gtfs() -> usize {
    let thread_env = std::env::var("THREADS_GTFS");

    match thread_env {
        Ok(thread_env) => match thread_env.parse::<usize>() {
            Ok(thread_count) => thread_count,
            Err(_) => 4,
        },
        Err(_) => 4,
    }
}

async fn run_ingest() -> Result<(), Box<dyn Error + std::marker::Send + Sync>> {
    //Ensure git submodule transitland-atlas downloads and updates correctly, if not, pass the error
    let _ = update_transitland_submodule()?;

    //These feeds should be discarded because they are duplicated in a larger dataset called `f-sf~bay~area~rg`, which has everything in a single zip file
    let feeds_to_discard: HashSet<String> = HashSet::from_iter(
        vec![
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
        ]
        .into_iter()
        .map(|each_feed_id| String::from(each_feed_id)),
    );

    println!("Initializing database connection");

    // get connection pool from database pool
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    // reads a transitland directory and returns a hashmap of all the data feeds (urls) associated with their correct operator and vise versa
    // See https://github.com/catenarytransit/dmfr-folder-reader
    println!("Reading transitland directory");
    let dmfr_result = read_folders("./transitland-atlas/")?;
    println!(
        "Transitland directory read with {} feeds and {} operators",
        dmfr_result.feed_hashmap.len(),
        dmfr_result.operator_hashmap.len()
    );

    // The DMFR result dataset looks genuine, with over 200 pieces of data!
    if dmfr_result.feed_hashmap.len() > 200 && dmfr_result.operator_hashmap.len() > 100 {
        let eligible_feeds = transitland_download::download_return_eligible_feeds(
            &dmfr_result,
            &arc_conn_pool,
            &feeds_to_discard,
        )
        .await;

        // Performs depth first search to find groups of feed urls associated with each other
        // See https://github.com/catenarytransit/chateau for the source code
        println!("Calculating Château");
        let chateau_result = chateau(&dmfr_result);
        println!("Château done calculating");

        //pivot table chateau table into HashMap<FeedId, ChateauId>
        let feed_id_to_chateau_lookup =
            chateau_postprocess::feed_id_to_chateau_id_pivot_table(&chateau_result);

        // count eligible feeds that are marked ingest == true using a filter and .len()

        let counter_of_eligible_feeds: Option<usize> = match &eligible_feeds {
            Ok(eligible_feeds) => Some(
                eligible_feeds
                    .iter()
                    .filter(|download_feed_info| download_feed_info.ingest)
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
                .filter(|download_feed_info| download_feed_info.ingest)
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
            futures::stream::iter(eligible_feeds.iter().map(|eligible_feed| {
                {
                    let arc_conn_pool = Arc::clone(&arc_conn_pool);
                    async move {
                        let conn_pool = arc_conn_pool.as_ref();
                        let conn_pre = conn_pool.get().await;
                        let conn = &mut conn_pre?;

                        use catenary::models::StaticDownloadAttempt;
                        use catenary::schema::gtfs::static_download_attempts::dsl::*;

                        //save the download attempt happened and information about it
                        let this_attempt = StaticDownloadAttempt {
                            onestop_feed_id: eligible_feed.feed_id.clone(),
                            url: eligible_feed.url.clone(),
                            file_hash: match eligible_feed.hash {
                                Some(hash) => Some(format!("{}", hash)),
                                None => None,
                            },
                            downloaded_unix_time_ms: eligible_feed.download_timestamp_ms as i64,
                            ingested: false,
                            failed: !eligible_feed.operation_success,
                            http_response_code: eligible_feed.http_response_code.clone(),
                            mark_for_redo: false,
                            ingestion_version: MAPLE_INGESTION_VERSION,
                        };

                        diesel::insert_into(static_download_attempts)
                            .values(&this_attempt)
                            .execute(conn)
                            .await?;

                        Ok(())
                    }
                }
            }))
            .buffer_unordered(64)
            .collect::<Vec<Result<(), Box<dyn Error + Send + Sync>>>>()
            .await;

            let download_feed_info_hashmap: HashMap<String, DownloadedFeedsInformation> =
                eligible_feeds
                    .iter()
                    .map(|download_feed_info| {
                        (
                            download_feed_info.feed_id.clone(),
                            download_feed_info.clone(),
                        )
                    })
                    .collect::<HashMap<String, DownloadedFeedsInformation>>();

            let download_feed_info_hashmap = Arc::new(download_feed_info_hashmap);

            // 3. Assign Attempt IDs to each feed_id that is ready to ingest

            println!("Refreshing metatables");

            let _ = refresh_metadata_tables::refresh_metadata_assignments(
                &dmfr_result,
                &chateau_result,
                Arc::clone(&arc_conn_pool),
            )
            .await;

            println!("Metadata refresh done");

            // 4. Unzip folders
            println!("Unzipping all gtfs folders");
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

            let successful_unzip_feeds_count =
                unzip_feeds.iter().map(|x| x.1).collect::<Vec<bool>>().len();

            println!(
                "{} of {} unzipped",
                successful_unzip_feeds_count,
                unzip_feeds.len()
            );

            let check_for_stops_ids = unzip_feeds
                .into_iter()
                .filter(|x| x.1)
                .map(|(feed_id, _)| feed_id)
                .map(|feed_id| {
                    let stop_path_str = format!("gtfs_uncompressed/{}/stops.txt", &feed_id);

                    let has_stop_table = std::path::Path::new(&stop_path_str).exists();

                    let trip_path_str = format!("gtfs_uncompressed/{}/trips.txt", &feed_id);

                    let has_trip_table = std::path::Path::new(&trip_path_str).exists();

                    (feed_id, has_stop_table && has_trip_table)
                })
                .collect::<Vec<(String, bool)>>();

            let feeds_with_stop_table_len = check_for_stops_ids
                .iter()
                .map(|x| x.1)
                .collect::<Vec<bool>>()
                .len();

            let feeds_without_stop_table_len = check_for_stops_ids
                .iter()
                .map(|x| !x.1)
                .collect::<Vec<bool>>()
                .len();

            println!(
                "{} deleted because does not contain stops.txt {} remaining",
                feeds_without_stop_table_len, feeds_with_stop_table_len
            );

            // todo! perform additional checks to ensure feed is not a zip bomb

            let attempt_ids: HashMap<String, String> = {
                let mut attempt_ids = HashMap::new();
                for (feed_id, _) in check_for_stops_ids.iter() {
                    let attempt_id =
                        format!("{}-{}", feed_id, chrono::Utc::now().timestamp_millis());
                    attempt_ids.insert(feed_id.clone(), attempt_id);
                }
                attempt_ids
            };

            let attempt_ids = Arc::new(attempt_ids);

            let unzip_feeds_clone = check_for_stops_ids.clone();

            // 5. Process GTFS feeds

            //Stream the feeds into the processing function

            let ingest_progress: Arc<std::sync::Mutex<u16>> = Arc::new(std::sync::Mutex::new(0));

            let feeds_to_process: Vec<(String, String, String)> = check_for_stops_ids
                .into_iter()
                .filter(|unzipped_feed| unzipped_feed.1)
                .map(|(feed_id, _)| (feed_id.clone(), attempt_ids.get(&feed_id).unwrap().clone()))
                .map(
                    |(feed_id, attempt_id)| match feed_id_to_chateau_lookup.get(&feed_id) {
                        Some(chateau_id) => Some((feed_id, attempt_id, chateau_id.clone())),
                        None => None,
                    },
                )
                .flatten()
                .collect();

            let total_feeds_to_process = feeds_to_process.len() as u16;

            futures::stream::iter(
                feeds_to_process
                .into_iter()
                .map(|(feed_id, attempt_id, chateau_id)| {
                        
                            //clone the smart reference to the connection pool
                            let arc_conn_pool = Arc::clone(&arc_conn_pool);
                            let download_feed_info_hashmap = Arc::clone(&download_feed_info_hashmap);
                            let ingest_progress = Arc::clone(&ingest_progress);
                            async move {
                                //connect to postgres
                                let conn_pool = arc_conn_pool.as_ref();
                                let conn_pre = conn_pool.get().await;
                                let conn = &mut conn_pre.unwrap();
        
                                let this_download_data = download_feed_info_hashmap.get(&feed_id).unwrap();
                                
                                    let start_time = chrono::Utc::now().timestamp_millis();
                                
                                    // call function to process GTFS feed, accepting feed_id, diesel pool args, chateau_id, attempt_id
                                    let gtfs_process_result = gtfs_process_feed(
                                        &feed_id,
                                        Arc::clone(&arc_conn_pool),
                                        &chateau_id,
                                        &attempt_id,
                                        &this_download_data,
                                    )
                                    .await;

                                    let mut ingest_progress  = ingest_progress.lock().unwrap();
                                    *ingest_progress += 1;
            
                                    println!(
                                        "Completion progress: {}/{} [{:.2}%]",
                                        ingest_progress,
                                        total_feeds_to_process,
                                        (*ingest_progress as f32/total_feeds_to_process as f32) * 100.0
                                    );

                                    std::mem::drop(ingest_progress);
        
                                    if gtfs_process_result.is_ok() {
                                        // at the end, UPDATE gtfs.static_download_attempts where onstop_feed_id and download_unix_time_ms match as ingested
        
                                        use catenary::schema::gtfs::static_download_attempts::dsl::static_download_attempts;
        
                                        let _ = diesel::update(static_download_attempts)
                                            .filter(catenary::schema::gtfs::static_download_attempts::dsl::onestop_feed_id.eq(&feed_id))
                                            .filter(catenary::schema::gtfs::static_download_attempts::dsl::downloaded_unix_time_ms.eq(this_download_data.download_timestamp_ms as i64))
                                            .set(catenary::schema::gtfs::static_download_attempts::dsl::ingested.eq(true))
                                            .execute(conn)
                                            .await;

                                        use catenary::schema::gtfs::ingested_static::dsl::ingested_static;

                                        let ingested_static_pq = catenary::models::IngestedStatic {
                                            onestop_feed_id: feed_id.clone(),
                                            attempt_id: attempt_id.clone(),
                                            languages_avaliable: vec![],
                                            file_hash: format!("{}",download_feed_info_hashmap.get(&feed_id).unwrap().hash.unwrap()),
                                            ingest_start_unix_time_ms: start_time,
                                            ingest_end_unix_time_ms: chrono::Utc::now().timestamp_millis(),
                                            ingest_duration_ms: (chrono::Utc::now().timestamp_millis() - start_time) as i32,
                                            ingesting_in_progress: false,
                                            ingestion_errored: false,
                                            ingestion_successfully_finished: true,
                                            deleted: false,
                                            default_lang: gtfs_process_result.unwrap().default_lang,
                                            production: false,
                                            feed_expiration_date: None,
                                            feed_start_date: None,
                                            ingestion_version: MAPLE_INGESTION_VERSION,
                                        };

                                        let _ = diesel::insert_into(ingested_static)
                                            .values(&ingested_static_pq)
                                            .on_conflict_do_nothing()
                                            .execute(conn)
                                            .await;

                                        let _ = assign_production_tables::assign_production_tables(
                                            &feed_id,
                                            &attempt_id,
                                            Arc::clone(&arc_conn_pool),
                                        ).await;

                                    } else {
                                        //print output
                                        eprintln!("GTFS process failed for feed {},\n {:?}", feed_id, gtfs_process_result.unwrap_err());
    
                                        //UPDATE gtfs.static_download_attempts where onstop_feed_id and download_unix_time_ms match as failure
                                        use catenary::schema::gtfs::static_download_attempts::dsl::static_download_attempts;
                                        
                                        let _ = diesel::update(static_download_attempts
                                        .filter(catenary::schema::gtfs::static_download_attempts::dsl::onestop_feed_id.eq(&feed_id))
                                        .filter(catenary::schema::gtfs::static_download_attempts::dsl::downloaded_unix_time_ms.eq(this_download_data.download_timestamp_ms as i64))
                                        ).set(catenary::schema::gtfs::static_download_attempts::dsl::failed.eq(true))
                                        .execute(conn)
                                        .await;
    
                                        //Delete objects from the attempt
                                        let _ = delete_attempt_objects(&feed_id, &attempt_id, Arc::clone(&arc_conn_pool)).await;
                                    }
                                    
                                    //delete InProgressStaticIngest regardless of result

                                    use catenary::schema::gtfs::in_progress_static_ingests::dsl::in_progress_static_ingests;

                                    let _ = diesel::delete(in_progress_static_ingests.filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::onestop_feed_id.eq(&feed_id))
                                    .filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::attempt_id.eq(&attempt_id)))
                                .execute(conn).await;
                                
                            }
                        
                    }
                
            ))
            .buffer_unordered(get_threads_gtfs())
            .collect::<Vec<()>>()
            .await;

            // Refresh the metadata tables after the ingestion is done

            let _ = refresh_metadata_tables::refresh_metadata_assignments(
                &dmfr_result,
                &chateau_result,
                Arc::clone(&arc_conn_pool),
            )
            .await?;
        }
    } else {
        eprintln!("Not enough data in transitland!");
    }

    println!("Maple ingest completed");

    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = run_ingest().await;
}
