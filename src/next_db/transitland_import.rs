use futures::StreamExt;
use reqwest::Client as ReqwestClient;
use reqwest::Request;
use reqwest::RequestBuilder;
use serde_json::Error as SerdeError;
use std::collections::HashMap;
use std::fs;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
mod dmfr;
use futures;
use std::fs::File;
use std::io::copy;
use std::io::Write;

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

pub async fn download_return_eligible_feeds(pool: &mut sqlx::Pool<sqlx::Postgres>) -> Result<Vec<DownloadedFeedsInformation>, ()> {
    let threads: usize = 32;

    let _ = fs::create_dir("gtfs_static_zips");

    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {
        let mut feedhashmap: HashMap<String, dmfr::Feed> = HashMap::new();

        let mut operatorhashmap: HashMap<String, dmfr::Operator> = HashMap::new();

        let mut operator_to_feed_hashmap: HashMap<String, Vec<dmfr::OperatorAssociatedFeedsItem>> =
            HashMap::new();

        //for each file in the folder of feeds
        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(file_name) = entry.file_name().to_str() {
                    println!("{}", file_name);

                    //read the feed contents
                    let contents =
                        fs::read_to_string(format!("transitland-atlas/feeds/{}", file_name));
                    if contents.is_err() {
                        eprintln!("Error reading file: {}", contents.unwrap_err());
                        continue;
                    }

                    //parse the feed contents from JSON to struct
                    let dmfrinfo: Result<dmfr::DistributedMobilityFeedRegistry, SerdeError> =
                        serde_json::from_str(&contents.unwrap());

                    //skip the file if the file doesn't make dmfr spec
                    if dmfrinfo.is_err() {
                        eprintln!("Error parsing file: {}", dmfrinfo.unwrap_err());
                        eprintln!("Skipping file: {}", file_name);
                        continue;
                    }

                    let dmfrinfo = dmfrinfo.unwrap();
                    dmfrinfo.feeds.iter().for_each(|feed| {
                        println!("Feed {}: {:#?}", feed.id.clone(), feed);
                        if feedhashmap.contains_key(&feed.id) {
                            feedhashmap.insert(feed.id.clone(), feed.clone());
                        } else {
                            feedhashmap.insert(feed.id.clone(), feed.clone());
                        }

                        feed.operators.iter().for_each(|operator| {
                            operatorhashmap.insert(operator.onestop_id.clone(), operator.clone());
                            if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {
                                //combine the feeds for this operator together
                                let mut existing_associated_feeds = operator_to_feed_hashmap
                                    .get(&operator.onestop_id)
                                    .unwrap()
                                    .clone();
                                let existing_feed_ids = operator_to_feed_hashmap
                                    .get(&operator.onestop_id)
                                    .unwrap()
                                    .iter()
                                    .map(|associated_feed| {
                                        associated_feed.feed_onestop_id.clone().unwrap()
                                    })
                                    .collect::<Vec<String>>();
                                operator
                                    .associated_feeds
                                    .iter()
                                    .for_each(|associated_feed| {
                                        if !existing_feed_ids.contains(
                                            &associated_feed
                                                .feed_onestop_id
                                                .clone()
                                                .unwrap_or_else(|| feed.id.clone()),
                                        ) {
                                            existing_associated_feeds.push(associated_feed.clone());
                                        }
                                    });
                                operator_to_feed_hashmap
                                    .insert(operator.onestop_id.clone(), existing_associated_feeds);
                            } else {
                                operator_to_feed_hashmap.insert(
                                    operator.onestop_id.clone(),
                                    operator.associated_feeds.clone(),
                                );
                            }
                        });
                    });
                    dmfrinfo.operators.iter().for_each(|operator| {
                        operatorhashmap.insert(operator.onestop_id.clone(), operator.clone());
                        println!(
                            "Operator {}: {:?}",
                            operator.onestop_id.clone(),
                            operator.associated_feeds
                        );
                        if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {
                            //combine the feeds for this operator together
                            let mut existing_associated_feeds = operator_to_feed_hashmap
                                .get(&operator.onestop_id)
                                .unwrap()
                                .clone();
                            let existing_feed_ids = operator_to_feed_hashmap
                                .get(&operator.onestop_id)
                                .unwrap()
                                .iter()
                                .filter(|associated_feed| associated_feed.feed_onestop_id.is_some())
                                .map(|associated_feed| {
                                    associated_feed.feed_onestop_id.clone().unwrap()
                                })
                                .collect::<Vec<String>>();
                            operator
                                .associated_feeds
                                .iter()
                                .for_each(|associated_feed| {
                                    if !existing_feed_ids
                                        .contains(&associated_feed.feed_onestop_id.clone().unwrap())
                                    {
                                        existing_associated_feeds.push(associated_feed.clone());
                                    }
                                });
                            operator_to_feed_hashmap
                                .insert(operator.onestop_id.clone(), existing_associated_feeds);
                        } else {
                            operator_to_feed_hashmap.insert(
                                operator.onestop_id.clone(),
                                operator.associated_feeds.clone(),
                            );
                        }
                    });
                }
            }
        }

        #[derive(Clone)]
        struct StaticFeedToDownload {
            feed_id: String,
            url: String,
        }

        #[derive(Debug,Clone)]
        struct DownloadAttempt {
            onestop_feed_id: String,
            file_hash: String,
            downloaded_unix_time_ms: i64,
            ingested: bool,
            failed: bool
        }

        println!("Downloading zip files now");

        let feeds_to_download = feedhashmap.iter().filter(|(_, feed)| match feed.spec {
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
                        http_response_code: None
                    };

                match response {
                    Ok(response) => {
                        let mut out =
                            File::create(format!("gtfs_static_zips/{}.zip", staticfeed.feed_id.clone()))
                                .expect("failed to create file");

                        let bytes_result = response.bytes().await;

                        if bytes_result.is_ok() {
                            let bytes_result = bytes_result.unwrap();
                            let data = bytes_result.as_ref();
                            let hash = seahash::hash(data);

                            answer.hash = Some(hash);

                            let hash_str = hash.to_string();

                            let download_attempt_db = sqlx::query_as!(DownloadAttempt,
                                "select * from gtfs.static_download_attempts WHERE file_hash::TEXT = ?", hash_str).fetch_all(pool).await;

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
                            &staticfeed.feed_id,
                            &staticfeed.url
                        );
                    }
                }

                answer
            }
        ))
            .buffer_unordered(threads)
            .collect::<Vec<DownloadedFeedsInformation>>();

        Ok(static_fetches.await)
    } else {
        return Err(());
    }
}
