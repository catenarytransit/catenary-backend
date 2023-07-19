use std::fs;
use serde_json::{Error as SerdeError};
use std::collections::HashMap;
mod dmfr;

fn main() {
    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {

        let mut feedhashmap: HashMap<String,dmfr::Feed> = HashMap::new();

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

                                    });

                                    dmfrinfo.operators.iter().for_each(|operator| {
                                        println!("Operator {}: {:?}", operator.onestop_id.clone(), operator.associated_feeds);

                                        if operator_to_feed_hashmap.contains_key(&operator.onestop_id) {

                                            //combine the feeds for this operator together
                                            let mut existing_associated_feeds = operator_to_feed_hashmap.get(&operator.onestop_id).unwrap().clone();

                                            let existing_feed_ids = operator_to_feed_hashmap.get(&operator.onestop_id).unwrap().iter().map(|associated_feed| {
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

        for (key, value) in feedhashmap.into_iter() {
         //   println!("{} / {:#?}", key, value);
            
        }

        for (key, value) in operator_to_feed_hashmap.into_iter() {
       //     println!("{} / {:#?}", key, value);
            
        }
    } else {
        println!("Failed to read transit feed directory, does the directory exist?")
    }
}