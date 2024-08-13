use crate::route_id_transform;
use gtfs_realtime::{FeedEntity, FeedMessage};
use lazy_static::lazy_static;
use regex::Regex;

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
        }
        None => id,
    };

    ENDING_DASHES_REGEX
        .replace(&STARTING_DASHES_REGEX.replace(new_id.as_str(), ""), "")
        .to_string()
}

pub fn gtfs_rt_cleanup(x: gtfs_realtime::FeedMessage) -> gtfs_realtime::FeedMessage {
    let global_timestamp = x.header.timestamp;

    let new_entities = x
        .entity
        .into_iter()
        .map(|entity| {
            let new_id = gtfs_rt_id_cleanup(global_timestamp, entity.id);
            gtfs_realtime::FeedEntity {
                id: new_id,
                ..entity
            }
        })
        .collect();

    gtfs_realtime::FeedMessage {
        entity: new_entities,
        ..x
    }
}

pub fn gtfs_rt_correct_route_id_string(
    x: gtfs_realtime::FeedMessage,
    realtime_feed_id: &str,
) -> gtfs_realtime::FeedMessage {
    let new_entities: Vec<gtfs_realtime::FeedEntity> = x
        .entity
        .into_iter()
        .map(|entity| {
            let mut entity = entity;

            //apply route_id_transform

            if let Some(trip_update) = entity.trip_update.as_mut() {
                if let Some(route_id) = trip_update.trip.route_id.as_mut() {
                    *route_id = route_id_transform(realtime_feed_id, route_id.clone())
                }
            }

            if let Some(vehicle) = entity.vehicle.as_mut() {
                if let Some(trip) = vehicle.trip.as_mut() {
                    if let Some(route_id) = trip.route_id.as_mut() {
                        *route_id = route_id_transform(realtime_feed_id, route_id.clone())
                    }
                }
            }

            entity
        })
        .collect::<Vec<gtfs_realtime::FeedEntity>>();

    gtfs_realtime::FeedMessage {
        entity: new_entities,
        ..x
    }
}
