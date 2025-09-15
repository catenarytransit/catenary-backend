// Initial version 3 of ingest written by Kyler Chin
// This was heavily inspired and copied from Emma Alexia, thank you Emma!
// Removal of the attribution is not allowed, as covered under the AGPL license

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref
)]

use cleanup::delete_attempt_objects_elasticsearch;
use cleanup::delete_feed_elasticsearch;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use ahash::AHashMap;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::postgres_tools::make_async_pool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
mod delete_overlapping_feeds_dmfr;
use std::sync::Arc;
pub mod correction_of_transfers;
use crate::cleanup::delete_attempt_objects;
use crate::cleanup::wipe_whole_feed;
use discord_webhook_rs::{Author, Embed, Field, Footer, Webhook};

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
use dmfr_dataset_reader::read_folders;

use crate::gtfs_handlers::MAPLE_INGESTION_VERSION;
use crate::transitland_download::DownloadedFeedsInformation;

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(long)]
    transitland: String,
}

fn get_threads_gtfs() -> usize {
    let thread_env = std::env::var("THREADS_GTFS");

    match thread_env {
        Ok(thread_env) => thread_env.parse::<usize>().unwrap_or(4),
        Err(_) => 4,
    }
}

async fn run_ingest() -> Result<(), Box<dyn Error + std::marker::Send + Sync>> {
    let discord_log_env = std::env::var("DISCORD_LOG");

    let elastic_url = std::env::var("ELASTICSEARCH_URL").unwrap();

    let elasticclient = catenary::elasticutils::single_elastic_connect(elastic_url.as_str())?;

    //catenary::elasticutils::wipe_db(&elasticclient).await?;

    catenary::elasticutils::make_index_and_mappings(&elasticclient).await?;

    let elasticclient = Arc::new(elasticclient);

    let args = Args::parse();

    let delete_everything_in_feed_before_ingest = match std::env::var("DELETE_BEFORE_INGEST") {
        Ok(val) => match val.as_str().to_lowercase().as_str() {
            "true" => true,
            _ => false,
        },
        Err(_) => false,
    };

    if delete_everything_in_feed_before_ingest {
        println!("Each feed will be wiped before ingestion");
    }

    let feeds_to_discard: HashSet<String> = HashSet::from_iter(
        vec![
            //These feeds should be discarded because they are duplicated in a larger dataset called `f-sf~bay~area~rg`, which has everything in a single zip file
            //these are all good little feeds, and they have the blessing of being merged into MTC's regional feed
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
            //BAN HAMMER SECTION
            //PRIVATE STUPID BUS COMPANY THAT CHARGES $60 TO DROP YOU OFF IN OLD TOWN FROM LAX
            "f-relaxsan~ca~us",
            //low quality feed that teleports you to antartica.
            "f-wests~bus~service",
            //discard duplicate from inside ratp feed
            "f-u0-sncf~transilien~rer",
            "f-eurostar",
            //duplicated in delfi data
            "f-nvbw",
            "f-u0z-vgn",
        ]
        .into_iter()
        .map(String::from),
    );

    let ban_list = ["f-relaxsan~ca~us"];

    println!("Initializing database connection");

    let restrict_to_feed_id = match std::env::var("ONLY_FEED_ID") {
        Ok(feed_id) => Some(feed_id),
        Err(_) => None,
    };

    let gtfs_temp_storage = std::env::var("GTFS_ZIP_TEMP")
        .expect("Missing GTFS_ZIP_TEMP env variable. Please give a path to store gtfs zip files.");
    let gtfs_uncompressed_temp_storage = std::env::var("GTFS_UNCOMPRESSED_TEMP").expect("Missing GTFS_UNCOMPRESSED_TEMP env variable. Please give a path to store gtfs uncompressed files.");

    // get connection pool from database pool
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    // reads a transitland directory and returns a hashmap of all the data feeds (urls) associated with their correct operator and vise versa
    // See https://github.com/catenarytransit/dmfr-folder-reader
    println!("Reading transitland directory");
    let dmfr_result = read_folders(&args.transitland)?;

    //delete overlapping feeds
    let dmfr_result = delete_overlapping_feeds_dmfr::delete_overlapping_feeds(dmfr_result);

    println!("Broken feeds: {:?}", dmfr_result.list_of_bad_files);

    println!(
        "Transitland directory read with {} feeds and {} operators",
        dmfr_result.feed_hashmap.len(),
        dmfr_result.operator_hashmap.len()
    );

    // The DMFR result dataset looks genuine, with over 200 pieces of data!
    if dmfr_result.feed_hashmap.len() > 200 && dmfr_result.operator_hashmap.len() > 100 {
        if let Ok(discord_log_env) = &discord_log_env {
            let hook_result = Webhook::new(discord_log_env.as_str())
                .username("Catenary Maple")
                .avatar_url("https://images.pexels.com/photos/255381/pexels-photo-255381.jpeg")
                .content("")
                .add_embed(
                    Embed::new()
                        .title("Maple Downloads Starting")
                        .description(format!(
                            "Downloading {} feeds, start time at {}",
                            dmfr_result
                                .feed_hashmap
                                .values()
                                .filter(|x| x.spec == dmfr::FeedSpec::Gtfs)
                                .count(),
                            chrono::Utc::now().to_rfc3339()
                        )),
                )
                .send();
        }

        let eligible_feeds = transitland_download::download_return_eligible_feeds(
            &gtfs_temp_storage,
            &dmfr_result,
            &arc_conn_pool,
            &feeds_to_discard,
            &restrict_to_feed_id,
            &args.transitland,
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

        let total_downloaded_bytes = match &eligible_feeds {
            Ok(eligible_feeds) => Some(
                eligible_feeds
                    .iter()
                    .map(|x| x.byte_size)
                    .flat_map(|x| x)
                    .sum::<u64>(),
            ),
            _ => None,
        };

        let total_downloaded_bytes_to_import = match &eligible_feeds {
            Ok(eligible_feeds) => Some(
                eligible_feeds
                    .iter()
                    .filter(|download_feed_info| download_feed_info.ingest)
                    .map(|x| x.byte_size)
                    .flat_map(|x| x)
                    .sum::<u64>(),
            ),
            _ => None,
        };

        if let Ok(discord_log_env) = &discord_log_env {
            let hook_result = Webhook::new(discord_log_env.as_str())
                .username("Catenary Maple")
                .avatar_url("https://images.pexels.com/photos/255381/pexels-photo-255381.jpeg")
                .content("")
                .add_embed(Embed::new().title("Download finished").description(format!(
                    "{:?} eligible feeds, {:?} bytes, {:?} bytes to import, time `{}`",
                    counter_of_eligible_feeds,
                    total_downloaded_bytes.map(|x| bytefmt::format(x)),
                    total_downloaded_bytes_to_import.map(|x| bytefmt::format(x)),
                    chrono::Utc::now().to_rfc3339()
                )))
                .send();
        }

        // debug print to output
        match counter_of_eligible_feeds {
            Some(counter_of_eligible_feeds) => {
                println!(
                    "{} feeds marked ready for schedule ingestion.",
                    counter_of_eligible_feeds
                );

                if counter_of_eligible_feeds == 0 {
                    if let Ok(eligible_feeds) = &eligible_feeds {
                        println!("Eligible feeds: {:?}", eligible_feeds);
                    }
                }
            }
            None => {
                println!("Unable to get eligible feed list.");
            }
        }

        //refresh the metadata for anything that's changed

        for banned_feed_id in ban_list {
            wipe_whole_feed(banned_feed_id, Arc::clone(&arc_conn_pool)).await?;
        }

        for feed_to_discard in feeds_to_discard.iter() {
            wipe_whole_feed(feed_to_discard, Arc::clone(&arc_conn_pool)).await?;
        }

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
                            file_hash: eligible_feed.hash.map(|hash| format!("{}", hash)),
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

            let download_feed_info_hashmap: AHashMap<String, DownloadedFeedsInformation> =
                eligible_feeds
                    .iter()
                    .map(|download_feed_info| {
                        (
                            download_feed_info.feed_id.clone(),
                            download_feed_info.clone(),
                        )
                    })
                    .collect::<AHashMap<String, DownloadedFeedsInformation>>();

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
                futures::stream::iter(to_ingest_feeds.into_iter().map(|to_ingest_feed| {
                    let gtfs_temp_storage = gtfs_temp_storage.clone();
                    let gtfs_uncompressed_temp_storage = gtfs_uncompressed_temp_storage.clone();
                    let feed = &dmfr_result
                        .feed_hashmap
                        .get(&to_ingest_feed.feed_id)
                        .unwrap()
                        .clone();

                    let rc_feed = std::rc::Rc::new(feed.clone());

                    async move {
                        let flatten_feed_result = gtfs_handlers::flatten::flatten_feed(
                            gtfs_temp_storage.as_str(),
                            gtfs_uncompressed_temp_storage.as_str(),
                            to_ingest_feed.feed_id.as_str(),
                            rc_feed,
                        );

                        if (flatten_feed_result.is_err()) {
                            eprintln!(
                                "Failed to flatten feed {}: {:?}",
                                to_ingest_feed.feed_id.as_str(),
                                flatten_feed_result
                            );
                        }

                        let fix_stops_file = gtfs_handlers::fix_files::fix_files_in_gtfs_directory(
                            format!(
                                "{}/{}",
                                gtfs_uncompressed_temp_storage, to_ingest_feed.feed_id
                            )
                            .as_str(),
                        );

                        (to_ingest_feed.feed_id.clone(), flatten_feed_result.is_ok())
                    }
                }))
                .buffer_unordered(16)
                .collect::<Vec<(String, bool)>>()
                .await;

            let successful_unzip_feeds_count = unzip_feeds
                .iter()
                .map(|x| x.1)
                .filter(|x| *x)
                .collect::<Vec<bool>>()
                .len();

            println!(
                "{} of {} unzipped",
                successful_unzip_feeds_count,
                unzip_feeds.len()
            );

            println!("{:?}", unzip_feeds);

            let check_for_stops_ids = unzip_feeds
                .into_iter()
                .filter(|x| x.1)
                .map(|(feed_id, _)| feed_id)
                .map(|feed_id| {
                    let stop_path_str =
                        format!("{}/{}/stops.txt", gtfs_uncompressed_temp_storage, &feed_id);

                    let has_stop_table = std::path::Path::new(&stop_path_str).exists();

                    let trip_path_str =
                        format!("{}/{}/trips.txt", gtfs_uncompressed_temp_storage, &feed_id);

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

            let attempt_ids: AHashMap<String, String> = {
                let mut attempt_ids = AHashMap::new();
                for (feed_id, _) in check_for_stops_ids.iter() {
                    let attempt_id =
                        format!("{}-{}", feed_id, chrono::Utc::now().timestamp_millis());
                    attempt_ids.insert(feed_id.clone(), attempt_id);
                }
                attempt_ids
            };

            let attempt_ids = Arc::new(attempt_ids);

            //let unzip_feeds_clone = check_for_stops_ids.clone();

            // 5. Process GTFS feeds

            //Stream the feeds into the processing function

            let ingest_progress: Arc<std::sync::Mutex<u16>> = Arc::new(std::sync::Mutex::new(0));

            let feeds_to_process: Vec<(String, String, String)> = check_for_stops_ids
                .into_iter()
                .filter(|unzipped_feed| unzipped_feed.1)
                .map(|(feed_id, _)| (feed_id.clone(), attempt_ids.get(&feed_id).unwrap().clone()))
                .filter_map(|(feed_id, attempt_id)| {
                    feed_id_to_chateau_lookup
                        .get(&feed_id)
                        .map(|chateau_id| (feed_id, attempt_id, chateau_id.clone()))
                })
                .collect();

            let total_feeds_to_process = feeds_to_process.len() as u16;

            println!("Processing {} feeds", total_feeds_to_process);

            if let Ok(discord_log_env) = &discord_log_env {
                let hook_result = Webhook::new(discord_log_env.as_str())
                    .username("Catenary Maple")
                    .avatar_url("https://images.pexels.com/photos/255381/pexels-photo-255381.jpeg")
                    .content("")
                    .add_embed(
                        Embed::new()
                            .title("Feed processing starting")
                            .description(format!(
                                "Processing {} feeds, start time at {}",
                                total_feeds_to_process,
                                chrono::Utc::now().to_rfc3339()
                            )),
                    )
                    .send();
            }

            futures::stream::iter(
                feeds_to_process
                .into_iter()
                .map(|(feed_id, attempt_id, chateau_id)| {
                            let gtfs_uncompressed_temp_storage = gtfs_uncompressed_temp_storage.clone();
                            //clone the smart reference to the connection pool
                            let arc_conn_pool = Arc::clone(&arc_conn_pool);
                            let download_feed_info_hashmap = Arc::clone(&download_feed_info_hashmap);
                            let ingest_progress = Arc::clone(&ingest_progress);
                            let elasticclient = Arc::clone(&elasticclient);

                            let discord_log_env = discord_log_env.clone();

                            async move {
                                //connect to postgres
                                let conn_pool = arc_conn_pool.as_ref();
                                let conn_pre = conn_pool.get().await;
                                let conn = &mut conn_pre.unwrap();
        
                                 let this_download_data = download_feed_info_hashmap.get(&feed_id).unwrap();

                                let get_hash_of_file_contents = catenary::hashfolder::hash_folder_sip_zero(
                                    std::path::Path::new(format!("{}/{}", gtfs_uncompressed_temp_storage, &feed_id).as_str())
                                ).await;

                                let get_hash_of_file_contents = match get_hash_of_file_contents {
                                
                                    Ok(hash) => Some(hash),
                                    Err(_) => None,
                                
                                };

                                let hash_of_file_contents_string = get_hash_of_file_contents.map(|hash| hash.to_string());

                                use catenary::schema::gtfs::static_download_attempts::dsl::static_download_attempts;
                                
                                use catenary::schema::gtfs::ingested_static::dsl::ingested_static;
                                //search for matching hash in the database

                                let matching_hash_rows = ingested_static
                                    .filter(catenary::schema::gtfs::ingested_static::dsl::hash_of_file_contents.eq(hash_of_file_contents_string.clone()))
                                    .filter(catenary::schema::gtfs::ingested_static::dsl::deleted.eq(false))
                                    .filter(catenary::schema::gtfs::ingested_static::dsl::ingestion_version.eq(MAPLE_INGESTION_VERSION))
                                    .select(catenary::models::IngestedStatic::as_select())
                                    .load::<catenary::models::IngestedStatic>(conn)
                                    .await
                                    .unwrap();

                                let file_contents_exists = matching_hash_rows.len() > 0;

                                if !file_contents_exists || std::env::var("FORCE_INGEST_ALL").is_ok() {
                                    if delete_everything_in_feed_before_ingest {
                                        let wipe_whole_feed_result = wipe_whole_feed(
                                            &feed_id,
                                            Arc::clone(&arc_conn_pool)
                                        ).await;

                                        if wipe_whole_feed_result.is_ok() {
                                            println!("Wiped whole feed of {} prior to ingestion", feed_id);
                                        } else {
                                            eprintln!("Failed to wipe whole feed of {} prior to ingestion", feed_id);
                                        }
                                    }

                                    let start_time = chrono::Utc::now().timestamp_millis();
                                
                                    // call function to process GTFS feed, accepting feed_id, diesel pool args, chateau_id, attempt_id
                                    let gtfs_process_result = gtfs_process_feed(
                                        &gtfs_uncompressed_temp_storage,
                                        &feed_id,
                                        Arc::clone(&arc_conn_pool),
                                        &chateau_id,
                                        &attempt_id,
                                        this_download_data,
                                        &elasticclient
                                    )
                                    .await;

                                    let mut ingest_progress = ingest_progress.lock().unwrap();
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
        
                                        let _ = diesel::update(static_download_attempts)
                                            .filter(catenary::schema::gtfs::static_download_attempts::dsl::onestop_feed_id.eq(&feed_id))
                                            .filter(catenary::schema::gtfs::static_download_attempts::dsl::downloaded_unix_time_ms.eq(this_download_data.download_timestamp_ms as i64))
                                            .set(catenary::schema::gtfs::static_download_attempts::dsl::ingested.eq(true))
                                            .execute(conn)
                                            .await;


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
                                            default_lang: gtfs_process_result.as_ref().unwrap().default_lang.clone(),
                                            production: false,
                                            feed_expiration_date: None,
                                            feed_start_date: None,
                                            ingestion_version: MAPLE_INGESTION_VERSION,
                                            hash_of_file_contents: hash_of_file_contents_string.clone(),
                                        };

                                        let ingested_static_result = diesel::insert_into(ingested_static)
                                            .values(&ingested_static_pq)
                                            .on_conflict_do_nothing()
                                            .execute(conn)
                                            .await;

                                        if ingested_static_result.is_ok() {
                                            
                                        let assign_prod_tables = assign_production_tables::assign_production_tables(
                                            &feed_id,
                                            &attempt_id,
                                            Arc::clone(&arc_conn_pool),
                                            gtfs_process_result.as_ref().unwrap().bbox
                                        ).await;

                                    use catenary::schema::gtfs::in_progress_static_ingests::dsl::in_progress_static_ingests;

                                    let _ = diesel::delete(in_progress_static_ingests.filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::onestop_feed_id.eq(&feed_id))
                                    .filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::attempt_id.eq(&attempt_id)))
                                .execute(conn).await;

                                        }

                                    } else {
                                        //print output
                                        eprintln!("GTFS process failed for feed {},\n {:?}", feed_id, gtfs_process_result.as_ref().unwrap_err());
    
                                        if let Ok(discord_log_env) = &discord_log_env {
                                            let hook_result = Webhook::new(discord_log_env.as_str())
                                        .username("Catenary Maple")
                                        .avatar_url("https://images.pexels.com/photos/255381/pexels-photo-255381.jpeg")
                                        .content("")
                                        .add_embed(
                                            Embed::new()
                                                .title("GTFS error")
                                                .description(format!("feed import failed for `{}`,\n {:?}", feed_id, gtfs_process_result.as_ref().unwrap_err())),
                                        )
                                        .send();
                                        }
                                        

                                        //UPDATE gtfs.static_download_attempts where onstop_feed_id and download_unix_time_ms match as failure
                                        use catenary::schema::gtfs::static_download_attempts::dsl::static_download_attempts;
                                        
                                        let update_as_failed = diesel::update(static_download_attempts
                                        .filter(catenary::schema::gtfs::static_download_attempts::dsl::onestop_feed_id.eq(&feed_id))
                                        .filter(catenary::schema::gtfs::static_download_attempts::dsl::downloaded_unix_time_ms.eq(this_download_data.download_timestamp_ms as i64))
                                        ).set(catenary::schema::gtfs::static_download_attempts::dsl::failed.eq(true))
                                        .execute(conn)
                                        .await;

                                        if update_as_failed.is_err() {

                                        } else {
                                        //Delete objects from the attempt
                                        let delete_attempt = delete_attempt_objects(&feed_id, &attempt_id, Arc::clone(&arc_conn_pool)).await;

                                        if delete_attempt.is_ok() {
                                            use catenary::schema::gtfs::in_progress_static_ingests::dsl::in_progress_static_ingests;

                                            let _ = diesel::delete(in_progress_static_ingests.filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::onestop_feed_id.eq(&feed_id))
                                            .filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::attempt_id.eq(&attempt_id)))
                                        .execute(conn).await;
                                        }

                                        let delete_from_elastic_search = delete_attempt_objects_elasticsearch(&feed_id, &attempt_id, &elasticclient).await;

                                        if let Err(delete_from_elastic_search) = delete_from_elastic_search.as_ref() {
                                            eprintln!("delete from elastic failed {:?}", delete_from_elastic_search);
                                        }

                                        if delete_attempt.is_ok() && delete_from_elastic_search.is_ok() {
                                            use catenary::schema::gtfs::in_progress_static_ingests::dsl::in_progress_static_ingests;

                                            let _ = diesel::delete(in_progress_static_ingests.filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::onestop_feed_id.eq(&feed_id))
                                            .filter(catenary::schema::gtfs::in_progress_static_ingests::dsl::attempt_id.eq(&attempt_id)))
                                        .execute(conn).await;
                                        }
                                        }
                                    }
                                } else {
                                    println!("Feed {} already exists in database, skipping ingestion", feed_id);

                                    let _ = diesel::update(static_download_attempts)
                                            .filter(catenary::schema::gtfs::static_download_attempts::dsl::onestop_feed_id.eq(&feed_id))
                                            .filter(catenary::schema::gtfs::static_download_attempts::dsl::downloaded_unix_time_ms.eq(this_download_data.download_timestamp_ms as i64))
                                            .set(catenary::schema::gtfs::static_download_attempts::dsl::ingested.eq(true))
                                            .execute(conn)
                                            .await;
                                    
                                    let mut ingest_progress = ingest_progress.lock().unwrap();
                                    *ingest_progress += 1;
            
                                    println!(
                                        "Completion progress: {}/{} [{:.2}%]",
                                        ingest_progress,
                                        total_feeds_to_process,
                                        (*ingest_progress as f32/total_feeds_to_process as f32) * 100.0
                                    );

                                    std::mem::drop(ingest_progress);
                                }
                                
                                   
                            }
                    }
            ))
            .buffer_unordered(get_threads_gtfs())
            .collect::<Vec<()>>()
            .await;

            // delete static feeds that no longer exist

            let inserted_feeds = catenary::schema::gtfs::static_feeds::dsl::static_feeds
                .select(catenary::models::StaticFeed::as_select())
                .load::<catenary::models::StaticFeed>(conn)
                .await?;

            for feed in inserted_feeds {
                if !dmfr_result.feed_hashmap.contains_key(&feed.onestop_feed_id) {
                    println!(
                        "Deleting whole feed {}, no longer exists",
                        feed.onestop_feed_id
                    );
                    let delete_result =
                        wipe_whole_feed(&feed.onestop_feed_id, Arc::clone(&arc_conn_pool)).await;

                    if delete_result.is_ok() {
                        println!("Deleted whole feed {}", feed.onestop_feed_id);

                        let _ = diesel::delete(
                            catenary::schema::gtfs::static_feeds::dsl::static_feeds.filter(
                                catenary::schema::gtfs::static_feeds::dsl::onestop_feed_id
                                    .eq(&feed.onestop_feed_id),
                            ),
                        )
                        .execute(conn)
                        .await?;
                    } else {
                        eprintln!("Failed to delete whole feed {}", feed.onestop_feed_id);
                    }

                    let delete_feed_from_elasticsearch =
                        delete_feed_elasticsearch(&feed.onestop_feed_id, &elasticclient).await;
                }
            }

            // Refresh the metadata tables after the ingestion is done

            let dmfr_result = read_folders(&args.transitland)?;

            //delete overlapping feeds
            let dmfr_result = delete_overlapping_feeds_dmfr::delete_overlapping_feeds(dmfr_result);

            refresh_metadata_tables::refresh_metadata_assignments(
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

    if let Ok(discord_log_env) = &discord_log_env {
        let hook_result = Webhook::new(discord_log_env.as_str())
            .username("Catenary Maple")
            .avatar_url("https://images.pexels.com/photos/255381/pexels-photo-255381.jpeg")
            .content("")
            .add_embed(
                Embed::new()
                    .title("Ingestion completed")
                    .description(format!(
                        "Time of completion: {}",
                        chrono::Utc::now().to_rfc3339()
                    )),
            )
            .send();
    }

    println!("Deleting temp data");

    fs::remove_dir_all(gtfs_temp_storage).unwrap();
    fs::remove_dir_all(gtfs_uncompressed_temp_storage).unwrap();

    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = run_ingest().await;
}
