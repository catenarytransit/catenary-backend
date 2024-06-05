use crate::CatenaryPostgresPool;
use catenary::models::StaticDownloadAttempt;
use catenary::postgres_tools::CatenaryConn;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};
use dmfr_folder_reader::ReturnDmfrAnalysis;
use futures;
use reqwest::RequestBuilder;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[derive(Clone)]
struct StaticFeedToDownload {
    pub feed_id: String,
    pub url: String,
}

//Written by Kyler Chin
//You are required under the APGL license to retain this annotation

//It's giving UC Berkeley lab assignment!!! 🐻💅🐻💅
//context for this joke: https://inst.eecs.berkeley.edu/~cs162/fa22/static/hw/hw-map-reduce-rs/
// UC Berkeley has exercises from their Rust computing courses that pack massive structs as result
#[derive(Clone)]
pub struct DownloadedFeedsInformation {
    pub feed_id: String,
    pub url: String,
    pub hash: Option<u64>,
    pub download_timestamp_ms: u64,
    // did ingestion complete, None for in progress
    pub operation_success: bool,
    //tells the pipeline to ingest this zip file
    pub ingest: bool,
    //store this data as u64 to enable storage and display to the user
    pub byte_size: Option<u64>,
    pub duration_download: Option<u64>,
    pub http_response_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StaticPassword {
    pub onestop_feed_id: String,
    pub passwords: Option<Vec<String>>,
    pub header_auth_key: Option<String>,
    // this would be "Bearer" so the header would insert Authorization: Bearer {key}
    pub header_auth_value_prefix: Option<String>,
    pub url_auth_key: Option<String>,
}

// This is an efficient method to scan all static ingests and only insert what is new.
// The previous system inserted absolutely everything, which was slow and consumed massive amounts of memory

// Go through every single feed url, download the file, get the hash
// if the file is new, ingest it, if it's new, do a comparison to the previous hash of the zip inserted. If the hashes are different, then mark as ingest

// the parent task in import.rs is in charge of assigning it to other threads + task scheduling, this portion is only for downloading and seeing what is eligible for download

pub async fn download_return_eligible_feeds(
    transitland_meta: &ReturnDmfrAnalysis,
    pool: &Arc<CatenaryPostgresPool>,
    feeds_to_discard: &HashSet<String>,
    restrict_to_feed_id: &Option<String>,
) -> Result<Vec<DownloadedFeedsInformation>, ()> {
    let threads: usize = 32;

    let _ = fs::create_dir("gtfs_static_zips");

    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {
        println!("Downloading zip files now");

        let feeds_to_download = transitland_meta
            .feed_hashmap
            .iter()
            .filter(|(_, feed)| {
                !feeds_to_discard.contains(&feed.id)
                    && match feed.spec {
                        dmfr::FeedSpec::Gtfs => true,
                        _ => false,
                    }
                    && feed.urls.static_current.is_some()
            })
            .filter(|(_, feed)| {
                if let Some(restrict_to_feed_id) = restrict_to_feed_id {
                    return feed.id == *restrict_to_feed_id;
                } else {
                    return true;
                }
            })
            .map(|(string, feed)| StaticFeedToDownload {
                feed_id: feed.id.clone(),
                url: feed.urls.static_current.as_ref().unwrap().to_string(),
            })
            .collect::<Vec<StaticFeedToDownload>>();

        let download_progress: Arc<std::sync::Mutex<u16>> = Arc::new(std::sync::Mutex::new(0));
        let total_feeds_to_download = feeds_to_download.len();
        use futures::StreamExt;
        let static_fetches =
        //perform the downloads as a future stream, so only the thread count is allowed
            futures::stream::iter(feeds_to_download.into_iter().map(
                |staticfeed|
                {

                    let download_progress = Arc::clone(&download_progress);
                    let pool = Arc::clone(pool);
                    async move {
                            //allow various compression algorithms to be used during the download process, as enabled in Cargo.toml
                            let client = reqwest::ClientBuilder::new()
                                //timeout queries after 4 minutes
                                .timeout(Duration::from_secs(60 * 4))
                                .connect_timeout(Duration::from_secs(20))
                                .danger_accept_invalid_certs(true)
                                .deflate(true)
                                .gzip(true)
                                .brotli(true)
                                .cookie_store(true)
                                .build()
                                .unwrap();

                            let url = transform_for_bay_area(staticfeed.url.clone());
            
                            let request = client.get(url);

                            let request = add_auth_headers(request, &staticfeed.feed_id);
            
                            //calculate how long the download takes
                            let start = SystemTime::now();
                            let current_unix_ms_time = start
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_millis();
            
                            let response = request.send().await;
            
                            let duration = SystemTime::now()
                                .duration_since(start)
                                .expect("Time went backwards");

                            let duration_ms = duration.as_millis();
            
                            // say that the download state was unsuccessful by default, and insert the duration
                            let mut answer = DownloadedFeedsInformation {
                                feed_id: staticfeed.feed_id.clone(),
                                url: staticfeed.url.clone(),
                                hash: None,
                                download_timestamp_ms: current_unix_ms_time as u64,
                                operation_success: false,
                                ingest: false,
                                byte_size: None,
                                duration_download: Some(duration_ms as u64),
                                http_response_code: None,
                            };
            
                            match response {
                                // The download request did return a response and the connection did not drop
                                Ok(response) => {
                                    answer.http_response_code = Some(response.status().as_str().to_string());
                                    let mut out = File::create(format!(
                                        "gtfs_static_zips/{}.zip",
                                        staticfeed.feed_id.clone()
                                    ))
                                    .expect("failed to create file");

                                    if response.status().is_success() {
                                    // get raw bytes
                                    let bytes_result = response.bytes().await;
            
                                    if let Ok(bytes_result) = bytes_result {
                                        let data = bytes_result.as_ref();
                                        let byte_length = data.len();
                                        // fast hashing algorithm of the bytes
                                        let hash = seahash::hash(data);
            
                                        answer.hash = Some(hash);
                                        answer.byte_size = Some(byte_length as u64);
            
                                        // stringify the hash
                                        let hash_str = hash.to_string();

            
                                        use catenary::schema::gtfs::static_download_attempts::dsl::*;

                                        //query the SQL database for any ingests that have the same zip
                                        
                            let conn  = &mut pool.get().await.unwrap();
                                        let download_attempts_postgres_lookup = static_download_attempts
                                            .filter(file_hash.eq(hash_str))
                                            .load::<StaticDownloadAttempt>(conn)
                                            .await;
            
                                     //if the dataset is brand new, mark as success, save the file
            
                                        // this is accomplished by checking in the sql table `gtfs.static_download_attempts`
                                        //if hash exists in the table AND the ingestion operation did not fail, cancel.
                                        //if hash doesn't exist write the file to disk
            
                                        match download_attempts_postgres_lookup {
                                            Ok(download_attempts_postgres_lookup) => {
                                                answer.operation_success = true;

                                                if std::env::var("ONLY_FEED_ID").is_ok() {
                                                    answer.ingest = true;
                                                } else {
                                                     // this zip file has never been seen before! Insert it!
                                                if download_attempts_postgres_lookup.len() == 0 {
                                                    answer.ingest = true;
                                                } else {
            
                                                    // a previous succcessful ingest has happened
                                                    let check_for_previous_insert_sucesses = download_attempts_postgres_lookup
                                                        .iter()
                                                        .find(|&x| x.ingested && !x.mark_for_redo);
            
                                                        //thus, don't perform the ingest
                                                    if check_for_previous_insert_sucesses.is_some() {
                                                        answer.ingest = false;
                                                    } else {
                                                        //no successes have occured, reattempt this zip file
                                                        //search through zookeeper tree for current pending operations (todo!)
                                                        answer.ingest = true;
                                                    }
                                                }
                                                }
                                                   
                                            }
                                            Err(error) => {
                                                //could not connect to the postgres, or this query failed. Don't ingest without access to postgres
                                                answer.operation_success = false;
                                            }
                                        }
                                       
                                        let _ = out.write(&(bytes_result));
                                        let mut download_progress  = download_progress.lock().unwrap();
                                        *download_progress += 1;
            
                                        println!("Finished writing {}/{} [{:.2}%]: {}, took {:.3}s",download_progress, total_feeds_to_download, (*download_progress as f32/total_feeds_to_download as f32) * 100.0,  &staticfeed.clone().feed_id, duration_ms as f32 / 1000.0);
                                    }
                                    } else {
                                        let mut download_progress  = download_progress.lock().unwrap();
                                        *download_progress += 1;

                                        println!("Failed to download {}/{} [{:.2}%]: {} responding with {}, took {:.3}s",download_progress, total_feeds_to_download, (*download_progress as f32/total_feeds_to_download as f32) * 100.0, &staticfeed.clone().feed_id, response.status().as_str(), duration_ms as f32 / 1000.0);
                                    }
                                }
                                Err(error) => {

                                    let mut download_progress  = download_progress.lock().unwrap();
                                    *download_progress += 1;

                                    println!(
                                        "Error with downloading {}: {}",
                                        &staticfeed.feed_id, &staticfeed.url
                                    );
                                }
                            }
            
                            answer
                        }
                }
                

                 ))
            .buffer_unordered(threads)
            .collect::<Vec<DownloadedFeedsInformation>>();

        Ok(static_fetches.await)
    } else {
        return Err(());
    }
}

fn transform_for_bay_area(x: String) -> String {
    //.replace("https://api.511.org/transit/datafeeds?operator_id=RG", "https://api.511.org/transit/datafeeds?operator_id=RG&api_key=094f6bc5-9d6a-4529-bfb3-6f1bc4d809d9")

    if x.contains("api.511.org") {
        let mut a = x;

        a.push_str("&api_key=094f6bc5-9d6a-4529-bfb3-6f1bc4d809d9");

        return a;
    } else {
        return x;
    }
}

fn add_auth_headers(request: RequestBuilder, feed_id: &str) -> RequestBuilder {
    let mut headers = reqwest::header::HeaderMap::new();

    match feed_id {
        "f-dp3-metra" => {
            headers.insert(
                "username",
                "bb2c71e54d827a4ab47917c426bdb48c".parse().unwrap(),
            );
            headers.insert("Authorization", "Basic YmIyYzcxZTU0ZDgyN2E0YWI0NzkxN2M0MjZiZGI0OGM6ZjhiY2Y4MDBhMjcxNThiZjkwYWVmMTZhZGFhNDRhZDI=".parse().unwrap());
        }
        "f-dqc-wmata~rail" => {
            headers.insert(
                "api_key",
                "3be3d48087754c4998e6b33b65ec9700".parse().unwrap(),
            );
        }
        "f-dqc-wmata~bus" => {
            headers.insert(
                "api_key",
                "3be3d48087754c4998e6b33b65ec9700".parse().unwrap(),
            );
        }
        "f-r6-nswtrainlink~sydneytrains~buswayswesternsydney~interlinebus" => {
            headers.insert("Authorization", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI3YVNPUVJpVUEyYzRKRTljRURBVDFrZ2ZnVERidjBBNVhUWDRDRW1xU3JFIiwiaWF0IjoxNzE3NjA3MDQyfQ._SbACaNINcZaOTSJbh3O142sQ6WYD7SQbdEjysKexUA".parse().unwrap());
        }
        _ => {}
    };

    request.headers(headers)
}
