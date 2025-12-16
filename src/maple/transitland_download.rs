//Written by Kyler Chin
//You are required under the APGL license to retain this annotation

use crate::CatenaryPostgresPool;
use crate::gtfs_handlers::MAPLE_INGESTION_VERSION;
use catenary::GirolleFeedDownloadResult;
use catenary::models::StaticDownloadAttempt;
use chrono::{DateTime, FixedOffset};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};
use dmfr_dataset_reader::ReturnDmfrAnalysis;
use reqwest::RequestBuilder;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex;
use url::{ParseError, Url};

#[derive(Serialize)]
struct GenentechLogin {
    passphrase: String,
}

async fn authenticate_genentech(client: &reqwest::Client) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
    let login_url = "https://genentech.tripshot.com/v1/publicAppPassphraseLogin";
    let body = GenentechLogin {
        passphrase: "transportation".to_string(),
    };

    println!("Authenticating with Genentech...");

    let response = client.post(login_url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0")
        .header("Origin", "https://genentech.tripshot.com")
        .header("Referer", "https://genentech.tripshot.com/g/tms/Public.html")
        .json(&body)
        .send()
        .await?;

    println!("Genentech auth status: {}", response.status());

    Ok(response)
}

#[derive(Clone)]
struct StaticFeedToDownload {
    pub feed_id: String,
    pub url: String,
}

//decoding auth login token to access Österreich (austria)
#[derive(Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize, Debug, Clone)]
struct LuxembourgResource {
    last_modified: String,
    latest: String,
}

#[derive(Deserialize, Debug)]
struct LuxembourgApiResponse {
    resources: Vec<LuxembourgResource>,
    // Add other top-level fields from the API response if needed
}

//copied luxembourg api parser from https://github.com/public-transport/transitous/blob/main/src/region_helpers.py
async fn data_public_lu_latest_resource(
    api_url: &str,
) -> Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let response = reqwest::get(api_url).await?;
    response.error_for_status_ref()?;
    let api_data = response.json::<LuxembourgApiResponse>().await?; // reqwest::Error automatically boxed by ?
    println!("Successfully parsed JSON response.");

    if api_data.resources.is_empty() {
        println!("No resources found in the API response.");
        return Err(Box::from("No resources found in the API response"));
    }

    let mut resources_with_dates: Vec<(DateTime<FixedOffset>, LuxembourgResource)> = Vec::new();
    for resource in api_data.resources {
        let mut str_to_parse = resource.last_modified.clone();

        if !str_to_parse.contains("Z") {
            //z is required to match rfc3339 as a zulu time aka utc time.
            str_to_parse.push('Z');
        }

        match DateTime::parse_from_rfc3339(str_to_parse.as_str()) {
            Ok(parsed_date) => {
                resources_with_dates.push((parsed_date, resource));
            }
            Err(e) => {
                // If any date fails to parse, return the error immediately, boxed.
                let error_message = format!(
                    "Failed to parse date string '{}': {}",
                    resource.last_modified, e
                );
                return Err(Box::from(error_message));
            }
        }
    }

    resources_with_dates.sort_by(|a, b| {
        // a.0 and b.0 are the DateTime<FixedOffset> values
        b.0.cmp(&a.0) // Reverse comparison for descending order
    });
    println!("Resources sorted by last_modified date (descending).");

    let latest_url = resources_with_dates[0].1.latest.clone();

    Ok(latest_url)
}

//copied again from public-transport/transitous
async fn get_mvo_keycloak_token(
    client: reqwest::Client,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut form_data = HashMap::new();
    form_data.insert("client_id", "dbp-public-ui");
    form_data.insert("username", "5f7xgv6ilp@ro5wy.anonbox.net");
    form_data.insert("password", ")#E8qE'~CqND5b#");
    form_data.insert("grant_type", "password");
    form_data.insert("scope", "openid");

    let response = client
        .post("https://user.mobilitaetsverbuende.at/auth/realms/dbp-public/protocol/openid-connect/token")
        .form(&form_data)
        .send();

    match response.await {
        Ok(resp) => {
            let resp_data = resp.json::<TokenResponse>().await;
            match resp_data {
                Ok(token_data) => {
                    println!("Token: {:?}", token_data);
                    Ok(token_data.access_token)
                }
                Err(e) => {
                    println!("Error parsing token response: {}", e);
                    Err(Box::new(e))
                }
            }
        }
        Err(e) => {
            println!("Error: {}", e);
            Err(Box::new(e))
        }
    }
}

fn make_reqwest_client() -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .use_rustls_tls()
        .user_agent("Catenary Maple")
        //timeout queries after 30 minutes
        .timeout(Duration::from_secs(60 * 30))
        .connect_timeout(Duration::from_secs(20))
        .danger_accept_invalid_certs(true)
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .cookie_store(true)
        .build()
        .unwrap()
}

async fn try_to_download(
    feed_id: &str,
    client: &reqwest::Client,
    url: &str,
    parsed_url: &Url,
) -> Result<reqwest::Response, reqwest::Error> {
    let new_url = transform_for_bay_area(url.to_string());

    if url.contains("api.odpt.org") && !url.contains("acl:consumerKey") {
        let mut url_with_key = new_url.clone();
        if url_with_key.contains("?") {
            url_with_key.push_str(
                "&acl:consumerKey=gwskedh0p97nh8n6null8ehnspe3p4joi127psyd2sjh3mqw500c5o2b8qv1uv1e",
            );
        } else {
            url_with_key.push_str(
                "?acl:consumerKey=gwskedh0p97nh8n6null8ehnspe3p4joi127psyd2sjh3mqw500c5o2b8qv1uv1e",
            );
        }
        return client.get(&url_with_key).send().await;
    }

    let client = match feed_id {
        "f-9q5b-torrancetransit" | "f-west~hollywood" | "f-genentech" => reqwest::ClientBuilder::new()
            .use_rustls_tls()
            .user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0",
            )
            .danger_accept_invalid_certs(true)
            .deflate(true)
            .gzip(true)
            .brotli(true)
            .cookie_store(true)
            .build()
            .unwrap(),
        _ => client.clone(),
    };

    if feed_id == "f-genentech" {
        let _ = authenticate_genentech(&client).await;
    }

    //the wordpress backup thing
    let client = match url.contains("showpublisheddocument")
        || url.contains("showdocument")
        || feed_id == "f-glendora~ca~us"
    {
        true => reqwest::ClientBuilder::new()
            .use_rustls_tls()
            .user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0",
            )
            .danger_accept_invalid_certs(true)
            .deflate(true)
            .gzip(true)
            .brotli(true)
            .cookie_store(true)
            .build()
            .unwrap(),
        _ => client.clone(),
    };

    if url.contains("nap.transportes.gob.es") {
        return client
            .get(url)
            .header("ApiKey", "d6bfc458-3f53-4456-9108-28fa9f97069d")
            .send()
            .await;
    }

    if feed_id == "f-dr5-nj~transit~rail" {
        let form = reqwest::multipart::Form::new()
            //i dont care, whatever, leak it, so what?
            .text("token", "638671989162459331");

        let request = client
            .request(
                reqwest::Method::POST,
                "https://raildata.njtransit.com/api/GTFSRT/getGTFS",
            )
            .multipart(form);

        let request = add_auth_headers(request, feed_id).await;

        return request.send().await;
    }

    let new_url = match feed_id {
        "f-administration~des~transports~publics~du~luxembourg" => {
            data_public_lu_latest_resource(new_url.as_str())
                .await
                .unwrap_or(url.to_string())
        }
        _ => new_url,
    };

    let request = client.get(&new_url).header("Accept", "*/*");

    let request = add_auth_headers(request, feed_id);

    let response = request.await.send().await;

    match response {
        Ok(response) => {
            if response.status() == reqwest::StatusCode::FORBIDDEN {
                println!(
                    "Got 403 for {}, trying again with wordpress bypass user agent",
                    feed_id
                );
                let client = reqwest::ClientBuilder::new()
            .use_rustls_tls()
            .user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0",
            )
            .danger_accept_invalid_certs(true)
            .deflate(true)
            .gzip(true)
            .brotli(true)
            .cookie_store(true)
            .build()
            .unwrap();

                let request = client.get(&new_url).header("Accept", "*/*");
                let request = add_auth_headers(request, feed_id);
                return request.await.send().await;
            } else {
                Ok(response)
            }
        }
        Err(error) => {
            println!(
                "Error with downloading {}: {}, {:?}, trying again",
                feed_id, &new_url, error
            );

            //trying again with a different client

            let client = reqwest::ClientBuilder::new()
                .user_agent("Catenary Maple")
                .timeout(Duration::from_secs(180))
                .connect_timeout(Duration::from_secs(20))
                .build()
                .unwrap();

            client.get(&new_url).send().await
        }
    }
}

//It's giving UC Berkeley lab assignment!!! 🐻💅🐻💅
//context for this joke: https://inst.eecs.berkeley.edu/~cs162/fa22/static/hw/hw-map-reduce-rs/
// UC Berkeley has exercises from their Rust computing courses that pack massive structs as result
#[derive(Clone, Debug)]
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
    gtfs_temp_storage: &str,
    transitland_meta: &ReturnDmfrAnalysis,
    pool: &Arc<CatenaryPostgresPool>,
    feeds_to_discard: &HashSet<String>,
    restrict_to_feed_ids: &Option<HashSet<String>>,
    transitland_path: &str,
    girolle_data: &Option<BTreeMap<String, GirolleFeedDownloadResult>>,
    use_girolle: bool,
) -> Result<Vec<DownloadedFeedsInformation>, ()> {
    let threads: usize = match std::env::var("DOWNLOAD_THREADS") {
        Ok(ok) => ok.parse::<usize>().unwrap_or_else(|_| 64),
        Err(_) => 64,
    };

    let total_bytes_downloaded: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));

    if !std::path::Path::new(gtfs_temp_storage).exists() {
        fs::create_dir(gtfs_temp_storage)
            .expect("zip directory doesn't exist but could not create it");
    }

    match fs::read_dir(format!("{}/feeds", transitland_path)) {
        Ok(entries) => {
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
                        && !feed.id.ends_with("~flex")
                })
                .filter(|(_, feed)| {
                    if let Some(restrict_to_feed_ids) = restrict_to_feed_ids {
                        restrict_to_feed_ids.contains(&feed.id)
                    } else {
                        true
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

            println!(
                "Downloading {} feeds with {} threads",
                total_feeds_to_download, threads
            );

            //allow various compression algorithms to be used during the download process, as enabled in Cargo.toml
            let client = make_reqwest_client();

            let static_fetches =
        //perform the downloads as a future stream, so only the thread count is allowed
            futures::stream::iter(feeds_to_download.into_iter().map(
                |staticfeed|
                {
                    let client = client.clone();
                    let download_progress = Arc::clone(&download_progress);
                    let pool = Arc::clone(pool);
                    let total_bytes_downloaded = Arc::clone(&total_bytes_downloaded);
                    async move {
                            
                            // get hostname
                            let parse_url = Url::parse(&staticfeed.url);

                           match parse_url {
                           Ok(parse_url) => {
                                
                                //calculate how long the download takes
                            let start = Instant::now();
                            let current_unix_ms_time = std::time::SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_millis();

                            let mut out = File::create(format!(
                                    "{}/{}.zip",
                                    gtfs_temp_storage,
                                    staticfeed.feed_id.clone()
                                ))
                                .expect("failed to create file");

                            let girolle_for_this_feed = match girolle_data {
                                Some(girolle_data) => girolle_data.get(&staticfeed.feed_id),
                                None => None,
                            };

                            let mut skip_due_to_girolle = false;

                            
                                        use catenary::schema::gtfs::static_download_attempts::dsl as sda_dsl;
            
                            if use_girolle {
                                if let Some(girolle_for_this_feed) = girolle_for_this_feed {

                                    if let Some(hash_girolle) = &girolle_for_this_feed.seahash {
                                         let conn  = &mut pool.get().await.unwrap();
                                        let download_attempts_postgres_lookup = sda_dsl::static_download_attempts
                                            .filter(sda_dsl::file_hash.eq(hash_girolle.to_string()))
                                            .filter(sda_dsl::ingestion_version.eq(MAPLE_INGESTION_VERSION))
                                            .filter(sda_dsl::onestop_feed_id.eq(&staticfeed.feed_id))
                                            .load::<StaticDownloadAttempt>(conn)
                                            .await;

                                        if let Ok(download_attempts_postgres_lookup) = download_attempts_postgres_lookup {
                                            
                                        let check_for_previous_insert_sucesses = download_attempts_postgres_lookup
                                                        .iter()
                                                        .find(|&x| x.ingested && !x.mark_for_redo);

                                                     if check_for_previous_insert_sucesses.is_some() {
                                                      //println!("{} already inserted from girolle hash", &staticfeed.feed_id);

                                                       if staticfeed.url.as_str() == girolle_for_this_feed.url.as_str() {
                                                    
                                                             skip_due_to_girolle = true;
                                                             }
                                        }
                                        }

                                       
                                    }

                                    
                                }
                            }

                            if (skip_due_to_girolle) {
                                let mut download_progress  = download_progress.lock().unwrap();
                                *download_progress += 1;

                                println!("Skipping download of {}/{} [{:.2}%]: {} due to girolle hash match",download_progress, total_feeds_to_download, (*download_progress as f32/total_feeds_to_download as f32) * 100.0, &staticfeed.clone().feed_id);

                                return DownloadedFeedsInformation {
                                    feed_id: staticfeed.feed_id.clone(),
                                    url: staticfeed.url.clone(),
                                    hash: None,
                                    download_timestamp_ms: current_unix_ms_time as u64,
                                    operation_success: true,
                                    ingest: false,
                                    byte_size: None,
                                    duration_download: None,
                                    http_response_code: None,
                                };
                            }

                            let response = try_to_download(
                                &staticfeed.feed_id,
                                &client,
                                &staticfeed.url,
                                &parse_url,
                            ).await;
            
                            let duration = start.elapsed();

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

                                    if response.status().is_success() {
                                    // get raw bytes
                                    let bytes_result = response.bytes().await;
            
                                    if let Ok(bytes_result) = bytes_result {
                                        let data = bytes_result.as_ref();
                                        let byte_length = data.len();

                                        let mut total_bytes_downloaded_unlock = total_bytes_downloaded.lock().await;

                                        *total_bytes_downloaded_unlock += byte_length;

                                        drop(total_bytes_downloaded_unlock);

                                        // fast hashing algorithm of the bytes
                                        let hash = seahash::hash(data);
            
                                        answer.hash = Some(hash);
                                        answer.byte_size = Some(byte_length as u64);
            
                                        // stringify the hash
                                        let hash_str = hash.to_string();



            
                                       
                                        //query the SQL database for any ingests that have the same zip
                                        
                            let conn  = &mut pool.get().await.unwrap();
                                        let download_attempts_postgres_lookup = sda_dsl::static_download_attempts
                                            .filter(sda_dsl::file_hash.eq(hash_str))
                                            .filter(sda_dsl::ingestion_version.eq(MAPLE_INGESTION_VERSION))
                                            .filter(sda_dsl::onestop_feed_id.eq(&staticfeed.feed_id))
                                            .load::<StaticDownloadAttempt>(conn)
                                            .await;
            
                                        //if the dataset is brand new, mark as success, save the file

                                        // this is accomplished by checking in the sql table `gtfs.static_download_attempts`
                                        //if hash exists in the table AND the ingestion operation did not fail, cancel.
                                        //if hash doesn't exist write the file to disk

                                        match download_attempts_postgres_lookup {
                                            Ok(download_attempts_postgres_lookup) => {
                                                answer.operation_success = true;

                                                if std::env::var("ONLY_FEED_IDS").is_ok() || std::env::var("ONLY_FEED_ID").is_ok() || std::env::var("FORCE_INGEST_ALL").is_ok() {
                                                    answer.ingest = true;
                                                } else {
                                                     // this zip file has never been seen before! Insert it!
                                                if download_attempts_postgres_lookup.is_empty() {
                                                    answer.ingest = true;
                                                    println!("Never seen this zip file + maple version together: {}", &staticfeed.feed_id);
                                                } else {

                                                    // a previous succcessful ingest has happened
                                                    let check_for_previous_insert_sucesses = download_attempts_postgres_lookup
                                                        .iter()
                                                        .find(|&x| x.ingested && !x.mark_for_redo);
            
                                                        //thus, don't perform the ingest
                                                    if check_for_previous_insert_sucesses.is_some() {
                                                        println!("Don't need to insert: {}, already inserted", &staticfeed.feed_id);
                                                        answer.ingest = false;
                                                    } else {
                                                        //no successes have occured, reattempt this zip file
                                                        //search through zookeeper tree for current pending operations (todo!)
                                                        println!("Insert: {}, tried but failed or mark for redo", &staticfeed.feed_id);
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
                                       
                                        if answer.ingest {
                                            let _ = out.write(&(bytes_result));
                                        }
                                        
                                        let mut download_progress  = download_progress.lock().unwrap();
                                        *download_progress += 1;
            
                                        println!("Finished writing {}/{} [{:.2}%]: {}, took {:.3}s",download_progress, total_feeds_to_download, (*download_progress as f32/total_feeds_to_download as f32) * 100.0,  &staticfeed.clone().feed_id, duration_ms as f32 / 1000.0);
                                    }
                                    } else {
                                        let mut download_progress  = download_progress.lock().unwrap();
                                        *download_progress += 1;

                                        println!("Failed to download {}/{} [{:.2}%]: {} responding with {}, took {:.3}s\nURL: {}",download_progress, total_feeds_to_download, (*download_progress as f32/total_feeds_to_download as f32) * 100.0, &staticfeed.clone().feed_id, response.status().as_str(), duration_ms as f32 / 1000.0, &staticfeed.url);
                                    }
                                }
                                Err(error) => {

                                    let mut download_progress  = download_progress.lock().unwrap();
                                    *download_progress += 1;

                                    println!(
                                        "Error with downloading {}: {}, {:?}",
                                        &staticfeed.feed_id, &staticfeed.url, error
                                    );
                                }
                            }

                            
                            answer
                            }
                            Err(e) => {
                                println!("Could not parse URL: {}", &staticfeed.url);

                                let mut download_progress  = download_progress.lock().unwrap();
                                *download_progress += 1;

                                DownloadedFeedsInformation {
                                    feed_id: staticfeed.feed_id.clone(),
                                    url: staticfeed.url.clone(),
                                    hash: None,
                                    download_timestamp_ms: catenary::duration_since_unix_epoch().as_millis() as u64,
                                    operation_success: false,
                                    ingest: false,
                                    byte_size: None,
                                    duration_download: None,
                                    http_response_code: None,
                                }
                            }}
                        }
                }))
            .buffer_unordered(threads)
            .collect::<Vec<DownloadedFeedsInformation>>();

            Ok(static_fetches.await)
        }
        _ => Err(()),
    }
}

fn transform_for_bay_area(x: String) -> String {
    //.replace("https://api.511.org/transit/datafeeds?operator_id=RG", "https://api.511.org/transit/datafeeds?operator_id=RG&api_key=094f6bc5-9d6a-4529-bfb3-6f1bc4d809d9")

    if x.contains("api.511.org") {
        let mut a = x;

        a.push_str("&api_key=094f6bc5-9d6a-4529-bfb3-6f1bc4d809d9");

        a
    } else {
        x
    }
}

async fn add_auth_headers(request: RequestBuilder, feed_id: &str) -> RequestBuilder {
    let mut request = request;

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
        "f-rb6b-metrochristchurch" => {
            headers.insert(
                "Ocp-Apim-Subscription-Key",
                "286e6dff5afe4565a08f3d453f8d28e2".parse().unwrap(),
            );
        }
        "f-u1-delijn" => {
            headers.insert(
                "Ocp-Apim-Subscription-Key",
                "1c069eb9820947ddba69a89103241380".parse().unwrap(),
            );
        }
        "f-u3h-koleje~dolnoslaskie" => {
            headers.insert(
                "Authorization",
                "Basic a2Q6QmFld29uZ29od3VnM0Fv".parse().unwrap(),
            );
            //login: kd
            //password: Baewongohwug3Ao
            //https://kolejedolnoslaskie.pl/rozklady-gtfs/
        }
        "f-mavcsoport" => {
            headers.insert(
                "Authorization",
                "Basic YXJvbmt2aEBnbWFpbC5jb206M09jOHpNRFc3c3RCQ2ljVg=="
                    .parse()
                    .unwrap(),
            );
        }
        "f-Linz~Österreich"
        | "f-Vorarlberg~Österreich"
        | "f-Tyrol~Österreich"
        | "f-Oberösterreich~Österreich"
        | "f-Carinthia~Österreich"
        | "f-Styria~Österreich"
        | "f-Ostösterreich~Österreich" => {
            let client = make_reqwest_client();

            let token = get_mvo_keycloak_token(client).await;

            if let Ok(token) = token {
                headers.insert(
                    "Authorization",
                    format!("Bearer {}", token).parse().unwrap(),
                );
            }
        }
        "f-9qh-omnitrans" => {
            headers.insert(
                "x-umo-iq-api-key",
                "60cf3070-b8fc-4be5-9b7a-adf155c68581".parse().unwrap(),
            );
        }
        _ => {}
    };

    if feed_id == "f-u05-tcl~systral" {
        //ENV vars get GRAND_LYON_USERNAME
        //ENV vars get GRAND_LYON_PASSWORD

        let username = std::env::var("GRAND_LYON_USERNAME");
        let password = std::env::var("GRAND_LYON_PASSWORD");

        if let Ok(username) = username {
            if let Ok(password) = password {
                println!("Password found for grand lyon!");
                request = request.basic_auth(username, Some(password));
            }
        }
    }

    if feed_id == "f-gtfs~de" {
        let username = std::env::var("DE_USERNAME");
        let password = std::env::var("DE_PASSWORD");

        if let Ok(username) = username {
            if let Ok(password) = password {
                println!("Password found for DE!");
                request = request.basic_auth(username, Some(password));
            }
        }
    }

    request.headers(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_genentech_download() {
        let client = make_reqwest_client();
        let url = "https://genentech.tripshot.com/v1/gtfs.zip";
        let parsed_url = Url::parse(url).unwrap();

        let response = try_to_download("f-genentech", &client, url, &parsed_url).await;
        
        match response {
            Ok(resp) => {
                println!("Response status: {}", resp.status());
                assert!(resp.status().is_success());
                let bytes = resp.bytes().await;
                assert!(bytes.is_ok());
                let len = bytes.unwrap().len();
                println!("Downloaded {} bytes", len);
                assert!(len > 1000); 
            }
            Err(e) => {
                panic!("Download failed: {:?}", e);
            }
        }
    }
}
