use futures::StreamExt;
use reqwest::Client as ReqwestClient;
use reqwest::Request;
use reqwest::RequestBuilder;
use serde_json::Error as SerdeError;
use std::collections::HashMap;
use std::fs;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use crate::dmfr;
use futures;
use std::collections::HashSet;
use std::fs::File;
use std::io::copy;
use std::io::Write;
use std::sync::Arc;

#[derive(Clone)]
struct StaticFeedToDownload {
    feed_id: String,
    url: String,
}

use crate::get_feeds_meta;

#[derive(Debug, Clone)]
struct DownloadAttempt {
    onestop_feed_id: String,
    file_hash: String,
    downloaded_unix_time_ms: i64,
    ingested: bool,
    failed: bool,
}
//Written by Kyler Chin

//This particular API key was published intentionally. This is for your convienence.
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
        _ => {}
    };

    request.headers(headers)
}

//It's giving UC Berkeley lab assignment!!! 🐻💅🐻💅
pub struct DownloadedFeedsInformation {
    feed_id: String,
    url: String,
    hash: Option<u64>,
    download_timestamp_ms: u64,
    operation_success: bool,
    //tells the server to ingest this tag
    ingest: bool,
    //left intentionally as u64 to enable storage and display to the user
    byte_size: Option<u64>,
    duration_download: Option<u64>,
    http_response_code: Option<String>,
}

pub async fn download_return_eligible_feeds(
    transitland_meta: Arc<get_feeds_meta::TransitlandMetadata>,
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<Vec<DownloadedFeedsInformation>, ()> {
    let threads: usize = 32;

    let _ = fs::create_dir("gtfs_static_zips");

    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {
        println!("Downloading zip files now");

        let feeds_to_download = transitland_meta.feedhashmap.iter().filter(|(_, feed)| match feed.spec {
            dmfr::FeedSpec::Gtfs => true,
            _ => false,
        } && feed.urls.static_current.is_some()).map(|(string, feed)| StaticFeedToDownload {
            feed_id: feed.id.clone(),
            url: transform_for_bay_area(feed.urls.static_current.as_ref().unwrap().to_string()),
        });

        let static_fetches =
            futures::stream::iter(feeds_to_download.into_iter().map(|staticfeed| async move {
                let client = reqwest::ClientBuilder::new()
                    .deflate(true)
                    .gzip(true)
                    .brotli(true)
                    .build()
                    .unwrap();

                let request = client.get(&staticfeed.url);

                let request = add_auth_headers(request, &staticfeed.feed_id);

                let start = SystemTime::now();
                let current_unix_ms_time = start
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis();

                let response = request.send().await;

                let duration = SystemTime::now()
                    .duration_since(start)
                    .expect("Time went backwards")
                    .as_millis();

                let mut answer = DownloadedFeedsInformation {
                    feed_id: staticfeed.feed_id.clone(),
                    url: staticfeed.url.clone(),
                    hash: None,
                    download_timestamp_ms: current_unix_ms_time as u64,
                    operation_success: false,
                    ingest: false,
                    byte_size: None,
                    duration_download: Some(duration as u64),
                    http_response_code: None,
                };

                match response {
                    Ok(response) => {
                        answer.http_response_code = Some(response.status().as_str().to_string());
                        let mut out = File::create(format!(
                            "gtfs_static_zips/{}.zip",
                            staticfeed.feed_id.clone()
                        ))
                        .expect("failed to create file");

                        let bytes_result = response.bytes().await;

                        if bytes_result.is_ok() {
                            let bytes_result = bytes_result.unwrap();
                            let data = bytes_result.as_ref();
                            let hash = seahash::hash(data);

                            answer.hash = Some(hash);

                            let hash_str = hash.to_string();

                            let download_attempt_db = sqlx::query_as!(
                                DownloadAttempt,
                                "SELECT * FROM gtfs.static_download_attempts WHERE file_hash = $1;",
                                hash_str
                            )
                            .fetch_all(pool)
                            .await;

                            match download_attempt_db {
                                Ok(download_attempt_db) => {
                                    answer.operation_success = true;

                                    if download_attempt_db.len() == 0 {
                                        answer.ingest = true;
                                    } else {
                                        let check_for_sucesses = download_attempt_db
                                            .iter()
                                            .find(|&x| x.ingested == true);

                                        if check_for_sucesses.is_some() {
                                            answer.ingest = false;
                                        } else {
                                            //no successes have occured
                                            //search through zookeeper tree for current pending operations (todo!)
                                            answer.ingest = true;
                                        }
                                    }
                                }
                                Err(error) => {
                                    answer.operation_success = false;
                                }
                            }

                            //if the dataset is brand new, mark as success, save the file

                            // this is accomplished by checking in the sql table `gtfs.static_download_attempts`
                            //if hash exists in the table AND the ingestion operation did not fail, cancel.
                            //if hash doesn't exist write the file to disk
                            let _ = out.write(&(bytes_result));
                            println!("Finished writing {}", &staticfeed.clone().feed_id);
                        }
                    }
                    Err(error) => {
                        println!(
                            "Error with downloading {}: {}",
                            &staticfeed.feed_id, &staticfeed.url
                        );
                    }
                }

                answer
            }))
            .buffer_unordered(threads)
            .collect::<Vec<DownloadedFeedsInformation>>();

        Ok(static_fetches.await)
    } else {
        return Err(());
    }
}
