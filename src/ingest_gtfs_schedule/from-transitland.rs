use futures::StreamExt;
use reqwest::Request;
use serde_json::Error as SerdeError;
use std::collections::HashMap;
use std::fs;
mod dmfr;
use futures;
use std::fs::File;
use std::io::copy;
use std::io::Write;

use reqwest::Client as ReqwestClient;
use reqwest::RequestBuilder;

//yes im intentionally leaking the API key. I don't care, they are free. This is for your convienence.
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

#[tokio::main]
async fn main() {
    let threads = arguments::parse(std::env::args())
        .unwrap()
        .get::<usize>("threads")
        .unwrap_or_else(|| 32);

    let _ = fs::create_dir("gtfs_static_zips");
    let _ = fs::create_dir("gtfs_uncompressed");
    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {
        let mut feedhashmap: HashMap<String, dmfr::Feed> = HashMap::new();

        let mut operatorhashmap: HashMap<String, dmfr::Operator> = HashMap::new();

        let mut operator_to_feed_hashmap: HashMap<String, Vec<dmfr::OperatorAssociatedFeedsItem>> =
            HashMap::new();

        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(file_name) = entry.file_name().to_str() {
                    println!("{}", file_name);

                    let contents =
                        fs::read_to_string(format!("transitland-atlas/feeds/{}", file_name));
                    if contents.is_err() {
                        eprintln!("Error reading file: {}", contents.unwrap_err());
                        continue;
                    }
                    let dmfrinfo: Result<dmfr::DistributedMobilityFeedRegistry, SerdeError> =
                        serde_json::from_str(&contents.unwrap());
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

        struct staticfeedtodownload {
            feed_id: String,
            url: String,
        }

        let mut number_of_static_feeds = 0;

        let mut vecofstaticstrings: Vec<staticfeedtodownload> = vec![];

        vecofstaticstrings.push(staticfeedtodownload {
            feed_id: "f-anteaterexpress".to_string(),
            url: "https://github.com/CatenaryMaps/zotgtfs/raw/main/anteater_gtfs.zip".to_string(),
        });

        for (key, feed) in feedhashmap.clone().into_iter() {
            //   println!("{} / {:#?}", key, value);

            match feed.spec {
                dmfr::FeedSpec::Gtfs => {
                    //static schedule

                    number_of_static_feeds = number_of_static_feeds + 1;

                    match feed.urls.static_current {
                        Some(static_url) => {
                            vecofstaticstrings.push(staticfeedtodownload {
                                feed_id: feed.id.clone(),
                                url: transform_for_bay_area(static_url.to_string()),
                            });
                        }
                        _ => {}
                    }
                }
                _ => {
                    //do nothing
                }
            }
        }

        println!("number of static feeds: {}", number_of_static_feeds);

        println!("Downloading zip files now");

        let static_fetches =
            futures::stream::iter(vecofstaticstrings.into_iter().map(|staticfeed| async move {
                let client = ReqwestClient::new();

                let request = client.get(&staticfeed.url);

                let request = add_auth_headers(request, &staticfeed.feed_id);

                let response = request.send().await;

                match response {
                    Ok(response) => {
                        let mut out =
                            File::create(format!("gtfs_static_zips/{}.zip", &staticfeed.feed_id))
                                .expect("failed to create file");

                        let bytes_result = response.bytes().await;

                        if bytes_result.is_ok() {
                            let _ = out.write(&(bytes_result.unwrap()));
                            println!("Finished writing {}", &staticfeed.feed_id);
                        }
                    }
                    Err(error) => {
                        println!(
                            "Error with downloading {}: {}",
                            &staticfeed.url, &staticfeed.url
                        );
                    }
                }
            }))
            .buffer_unordered(threads)
            .collect::<Vec<()>>();

        static_fetches.await;

        println!("Done fetching all zip files");

        /*for (key, value) in operator_to_feed_hashmap.into_iter() {
            println!("{} / {:#?}", key, value);
        }*/
    } else {
        println!("Failed to read transit feed directory, does the directory exist?")
    }
}
