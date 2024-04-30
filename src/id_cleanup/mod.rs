use lazy_static::lazy_static;
use regex::{Captures, Regex};

lazy_static! {
    static ref STARTING_DASHES_REGEX: Regex = Regex::new("^(-|_)").unwrap();
    static ref ENDING_DASHES_REGEX: Regex = Regex::new("(-|_)$").unwrap(); 
}

pub fn gtfs_rt_id_cleanup(global_timestamp: Option<u64>, id: String) -> String {
        //delete global_timestamp from id

        let new_id = match global_timestamp {
            Some(global_timestamp) => {
                let timestamp_str = global_timestamp.to_string();

                id.replace(&timestamp_str, "")
            }, 
            None => id
        }; 

        ENDING_DASHES_REGEX.replace(&STARTING_DASHES_REGEX.replace(new_id.as_str(), ""), "").to_string()
} 

pub fn gtfs_rt_cleanup(x: gtfs_rt::FeedMessage) -> gtfs_rt::FeedMessage {
    let global_timestamp = x.header.timestamp;

    let new_entities = x.entity.into_iter().map(|entity| {
        let new_id = gtfs_rt_id_cleanup(global_timestamp, entity.id);
        gtfs_rt::FeedEntity {
            id: new_id,
            ..entity
        }
    }).collect();

    gtfs_rt::FeedMessage {
        entity: new_entities,
        ..x
    }
}