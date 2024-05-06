// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

use chateau::Chateau;
use std::collections::HashMap;

//reverses the chateau association to get HashMap<FeedId, ChateauId>
pub fn feed_id_to_chateau_id_pivot_table(
    chateaus: &HashMap<String, Chateau>,
) -> HashMap<String, String> {
    let mut result: HashMap<String, String> = HashMap::new();

    for (_, chateau) in chateaus.iter() {
        for realtime_id in chateau.realtime_feeds.iter() {
            result.insert(realtime_id.clone(), chateau.chateau_id.clone());
        }

        for static_id in chateau.static_feeds.iter() {
            result.insert(static_id.clone(), chateau.chateau_id.clone());
        }
    }

    result
}
