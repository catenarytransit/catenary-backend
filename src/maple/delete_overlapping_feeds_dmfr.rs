use dmfr_dataset_reader::ReturnDmfrAnalysis;
use serde_json::Value;
use std::collections::HashMap;

pub fn delete_overlapping_feeds(input: ReturnDmfrAnalysis) -> ReturnDmfrAnalysis {
    let mut input = input;

    let feed_hashmap = input.feed_hashmap;

    let feed_hashmap = feed_hashmap
        .into_iter()
        .filter(
            |(key, feed)| match feed.tags.get("exclude_from_global_query") {
                Some(value) => match value {
                    Value::String(value) => value != "true",
                    _ => true,
                },
                None => true,
            },
        )
        .collect::<HashMap<String, _>>();

    input.feed_hashmap = feed_hashmap;

    input
}
