use std::fs;
use futures::StreamExt;
use serde_json::{Error as SerdeError};
use std::collections::HashMap;
mod dmfr;
use std::io::Write;
use std::io::copy;
use std::fs::File;
use futures;

#[tokio::main]
async fn main() {
    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {

        let mut feedhashmap: HashMap<String,dmfr::Feed> = HashMap::new();

        let mut operatorhashmap: HashMap<String,dmfr::Operator> = HashMap::new();

        let mut operator_to_feed_hashmap: HashMap<String,Vec<dmfr::OperatorAssociatedFeedsItem>> = HashMap::new();

        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(file_name) = entry.file_name().to_str() {
                    println!("{}", file_name);

                    let contents = fs::read_to_string(format!("transitland-atlas/feeds/{}", file_name));

                    match contents {
                        Ok(contents) => {

                            let dmfrinfo: Result<dmfr::DistributedMobilityFeedRegistry, SerdeError> = serde_json::from_str(&contents);

                        match dmfrinfo {
                                Ok(dmfrinfo) => {


                                    dmfrinfo.feeds.iter().for_each(|feed| {
                                    println!("Feed {}: {:#?}", feed.id.clone(), feed);

                                        if feedhashmap.contains_key(&feed.id) {
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        } else {
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        }

                                        feed.operators.iter().for_each(|operator| 
                                            {operatorhashmap.insert(operator.onestop_id.clone(), operator.clone());
                                            
                                            
                                                if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {

                                                    //combine the feeds for this operator together
                                                    let mut existing_associated_feeds = operator_to_feed_hashmap.get(&operator.onestop_id).unwrap().clone();
        
                                                    let existing_feed_ids = operator_to_feed_hashmap.get(&operator.onestop_id).unwrap().iter().map(|associated_feed| {
                                                        associated_feed.feed_onestop_id.clone().unwrap()
                                                    }).collect::<Vec<String>>();
        
                                                    operator.associated_feeds.iter().for_each(|associated_feed| {
                                                        if !existing_feed_ids.contains(&associated_feed.feed_onestop_id.clone().unwrap_or_else(|| {feed.id.clone()})) {
                                                            existing_associated_feeds.push(associated_feed.clone());
                                                        }
                                                    });

                                                    operator_to_feed_hashmap.insert(operator.onestop_id.clone(), existing_associated_feeds);
                                                } else {
                                                    operator_to_feed_hashmap.insert(operator.onestop_id.clone(), operator.associated_feeds.clone());
                                                }
                                            
                                            }


                                        );

                                    });

                                    dmfrinfo.operators.iter().for_each(|operator| {
                                         operatorhashmap.insert(operator.onestop_id.clone(), operator.clone());
                                        
                                        println!("Operator {}: {:?}", operator.onestop_id.clone(), operator.associated_feeds);

                                        if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {

                                            //combine the feeds for this operator together
                                            let mut existing_associated_feeds = operator_to_feed_hashmap.get(&operator.onestop_id).unwrap().clone();

                                            let existing_feed_ids = operator_to_feed_hashmap.get(&operator.onestop_id).unwrap().iter()
                                            .filter(|associated_feed| associated_feed.feed_onestop_id.is_some())
                                            .map(|associated_feed| {
                                                associated_feed.feed_onestop_id.clone().unwrap()
                                            }).collect::<Vec<String>>();

                                            operator.associated_feeds.iter().for_each(|associated_feed| {
                                                if !existing_feed_ids.contains(&associated_feed.feed_onestop_id.clone().unwrap()) {
                                                    existing_associated_feeds.push(associated_feed.clone());
                                                }
                                            });

                                            operator_to_feed_hashmap.insert(operator.onestop_id.clone(), existing_associated_feeds);
                                        } else {
                                            operator_to_feed_hashmap.insert(operator.onestop_id.clone(), operator.associated_feeds.clone());
                                        }

                                    });
                                },
                                Err(e) => {
                                    println!("Error parsing file: {}", e);
                                    println!("Skipping file: {}", file_name);
                                }
                            }

                            
                        }, 
                        Err(e) => {
                            println!("Error reading file: {}", e);
                        }
                    }
                    
                }
            }
        }

        struct staticfeedtodownload {
            feed_id: String,
            url: String,
        }
        
        let mut number_of_static_feeds = 0;

        let mut vecofstaticstrings: Vec<staticfeedtodownload> = vec![];

        for (key, feed) in feedhashmap.clone().into_iter() {
         //   println!("{} / {:#?}", key, value);
            
            match feed.spec {
                dmfr::FeedSpec::Gtfs => {
                    //static schedule

                    number_of_static_feeds = number_of_static_feeds + 1;

                    match feed.urls.static_current {
                        Some(static_url) => {
                            
                    vecofstaticstrings.push(staticfeedtodownload{feed_id: feed.id.clone(), url: static_url.to_string()});
                        },
                        _ => {

                        }
                    }

                },
                _ => {
                    //do nothing
                }
            }
        }

        println!(
        "number of static feeds: {}", 
        number_of_static_feeds
        );

        println!("Downloading zip files now");

        let static_fetches = futures::stream::iter(vecofstaticstrings.into_iter().map(|staticfeed| {
           async move {
            let response = reqwest::get(&staticfeed.url).await;

            match response {
                Ok(response) => {
                    let mut out = File::create(format!("gtfs_static_zips/{}.zip", &staticfeed.feed_id)).expect("failed to create file");
                  
                    let bytes_result = response.bytes().await;

                    if bytes_result.is_ok() {
                        let _ = out.write(&(bytes_result.unwrap()));
                        println!("Finished writing {}", &staticfeed.feed_id);
                    }

                    
                    },
                Err(error) => {
                    println!("Error with downloading {}: {}", &staticfeed.url, &staticfeed.url);
                }
            }
           }
        })).buffer_unordered(10).collect::<Vec<()>>();

        static_fetches.await;

        println!("Done fetching all zip files");

        for (key, value) in operator_to_feed_hashmap.into_iter() {
       //     println!("{} / {:#?}", key, value);
            
        }

        
    } else {
        println!("Failed to read transit feed directory, does the directory exist?")
    }
}