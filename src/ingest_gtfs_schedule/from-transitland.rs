use std::fs;
use serde_json::{Error as SerdeError};
use std::collections::HashMap;
mod dmfr;

fn main() {
    if let Ok(entries) = fs::read_dir("transitland-atlas/feeds") {

        let mut feedhashmap: HashMap<String,dmfr::Feed> = HashMap::new();

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
                                        //println!("{}: {:?}", feed.id.clone(), feed.urls);

                                        if feedhashmap.contains_key(&feed.id) {
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        } else {
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        }

                                    });
                                },
                                Err(e) => {
                                    println!("Error parsing file: {}", e);
                                    println!("Skipping file: {}", file_name)
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
    }
}