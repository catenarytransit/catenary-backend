use std::fs;
use futures::StreamExt;
use serde_json::{Error as SerdeError};
use std::collections::HashMap;
mod dmfr;
use std::io::Write;
use std::io::copy;
use std::fs::File;
use futures;
use std::error::Error;
use std::ops::Deref;

pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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

                    match contents {
                        Ok(contents) => {
                            let dmfrinfo: Result<
                                dmfr::DistributedMobilityFeedRegistry,
                                SerdeError,
                            > = serde_json::from_str(&contents);

                            match dmfrinfo {
                                Ok(dmfrinfo) => {
                                    dmfrinfo.feeds.iter().for_each(|feed| {
                                        //println!("Feed {}: {:#?}", feed.id.clone(), feed);

                                        if feedhashmap.contains_key(&feed.id) {
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        } else {
                                            feedhashmap.insert(feed.id.clone(), feed.clone());
                                        }

                                        feed.operators.iter().for_each(|operator| {
                                            operatorhashmap.insert(
                                                operator.onestop_id.clone(),
                                                operator.clone(),
                                            );

                                            if operator_to_feed_hashmap
                                                .contains_key(&operator.onestop_id)
                                            {
                                                //combine the feeds for this operator together
                                                let mut existing_associated_feeds =
                                                    operator_to_feed_hashmap
                                                        .get(&operator.onestop_id)
                                                        .unwrap()
                                                        .clone();

                                                let existing_feed_ids = operator_to_feed_hashmap
                                                    .get(&operator.onestop_id)
                                                    .unwrap()
                                                    .iter()
                                                    .map(|associated_feed| {
                                                        associated_feed
                                                            .feed_onestop_id
                                                            .clone()
                                                            .unwrap()
                                                    })
                                                    .collect::<Vec<String>>();

                                                operator.associated_feeds.iter().for_each(
                                                    |associated_feed| {
                                                        if !existing_feed_ids.contains(
                                                            &associated_feed
                                                                .feed_onestop_id
                                                                .clone()
                                                                .unwrap_or_else(|| feed.id.clone()),
                                                        ) {
                                                            existing_associated_feeds
                                                                .push(associated_feed.clone());
                                                        }
                                                    },
                                                );

                                                operator_to_feed_hashmap.insert(
                                                    operator.onestop_id.clone(),
                                                    existing_associated_feeds,
                                                );
                                            } else {
                                                operator_to_feed_hashmap.insert(
                                                    operator.onestop_id.clone(),
                                                    operator.associated_feeds.clone(),
                                                );
                                            }
                                        });
                                    });

                                    dmfrinfo.operators.iter().for_each(|operator| {
                                        operatorhashmap
                                            .insert(operator.onestop_id.clone(), operator.clone());

                                        println!(
                                            "Operator {}: {:?}",
                                            operator.onestop_id.clone(),
                                            operator.associated_feeds
                                        );

                                        if operator_to_feed_hashmap
                                            .contains_key(&operator.onestop_id)
                                        {
                                            //combine the feeds for this operator together
                                            let mut existing_associated_feeds =
                                                operator_to_feed_hashmap
                                                    .get(&operator.onestop_id)
                                                    .unwrap()
                                                    .clone();

                                            let existing_feed_ids = operator_to_feed_hashmap
                                                .get(&operator.onestop_id)
                                                .unwrap()
                                                .iter()
                                                .filter(|associated_feed| {
                                                    associated_feed.feed_onestop_id.is_some()
                                                })
                                                .map(|associated_feed| {
                                                    associated_feed.feed_onestop_id.clone().unwrap()
                                                })
                                                .collect::<Vec<String>>();

                                            operator.associated_feeds.iter().for_each(
                                                |associated_feed| {
                                                    if !existing_feed_ids.contains(
                                                        &associated_feed
                                                            .feed_onestop_id
                                                            .clone()
                                                            .unwrap(),
                                                    ) {
                                                        existing_associated_feeds
                                                            .push(associated_feed.clone());
                                                    }
                                                },
                                            );

                                            operator_to_feed_hashmap.insert(
                                                operator.onestop_id.clone(),
                                                existing_associated_feeds,
                                            );
                                        } else {
                                            operator_to_feed_hashmap.insert(
                                                operator.onestop_id.clone(),
                                                operator.associated_feeds.clone(),
                                            );
                                        }
                                    });
                                }
                                Err(e) => {}
                            }
                        }
                        Err(e) => {}
                    }
                }

                for (key, feed) in feedhashmap.clone().into_iter() {
                    match feed.spec {
                        dmfr::FeedSpec::Gtfs => {
                            //println!("{:?}", feed.urls);

                            if feed.urls.static_current.is_some() {
                                //check if folder exists in the directory

                                //process and upload routes, stops, headways, and shapes etc into postgres

                                //calculate the bounds of the feed,

                                //upload the feed id metadata

                                if path_exists(&format!("gtfs_uncompressed/{}/", key)) {
                                    //feed exists

                                    println!("Feed {} exists in local store", key);

                                    let gtfs = gtfs_structures::GtfsReader::default()
                                    .read(&format!("gtfs_uncompressed/{}/", key))?;

                                
                                    println!("read_duration: {:?}ms", gtfs.read_duration);

                                    println!("there are {} stops in the gtfs", gtfs.stops.len());

                                    println!("there are {} routes in the gtfs", gtfs.routes.len());

                                    let mut least_lat: Option<f64> = None;
                                    let mut least_lon: Option<f64> = None;
                            
                                    let mut most_lat: Option<f64> = None;
                                    let mut most_lon: Option<f64> = None;
                            
                                    let timestarting = std::time::Instant::now();
                            
                                    let mut shapes_per_route: HashMap<String, Vec<String>> = HashMap::new();
                            
                                    for (stop_id, stop) in &gtfs.stops {
                                        //check if least_lat has a value
                            
                                        if (*stop).deref().longitude.is_some() {
                                            let stop_lon = (*stop).deref().longitude.unwrap();
                            
                                            if least_lon.is_some() {
                                                if stop_lon < least_lon.unwrap() {
                                                    least_lon = Some(stop_lon);
                                                }
                                            } else {
                                                least_lon = Some(stop_lon);
                                            }
                            
                                            if most_lon.is_some() {
                                                if stop_lon > most_lon.unwrap() {
                                                    most_lon = Some(stop_lon);
                                                }
                                            } else {
                                                most_lon = Some(stop_lon);
                                            }
                                        }
                            
                                        if (*stop).deref().latitude.is_some() {
                                            let stop_lat = (*stop).deref().latitude.unwrap();
                            
                                            if least_lat.is_some() {
                                                if stop_lat < least_lat.unwrap() {
                                                    least_lat = Some(stop_lat);
                                                }
                                            } else {
                                                least_lat = Some(stop_lat);
                                            }
                            
                                            if most_lat.is_some() {
                                                if stop_lat > most_lat.unwrap() {
                                                    most_lat = Some(stop_lat);
                                                }
                                            } else {
                                                most_lat = Some(stop_lat);
                                            }
                                        }
                                    }
                                }
                            }

                            //match feed.urls.stat

                            
                        }
                        _ => {
                            //do nothing
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
