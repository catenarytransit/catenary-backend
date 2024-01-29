use std::sync::Arc;
use crate::get_feeds_meta;
use std::collections::{HashSet,HashMap};
use crate::dmfr;

// Written by Kyler Chin at Catenary Transit Initiatives
// https://github.com/CatenaryTransit/catenary-backend
//You are required under the APGL license to retain this annotation as is
pub async fn refresh_feed_meta(
    transitland_meta: Arc<get_feeds_meta::TransitlandMetadata>,
    pool: &sqlx::Pool<sqlx::Postgres>,
) {
    let feeds_to_discard: HashSet<&str> = HashSet::from_iter(vec![
        "f-9q8y-sfmta",
        "f-9qc-westcat~ca~us",
        "f-9q9-actransit",
        "f-9q9-vta",
        "f-9q8yy-missionbaytma~ca~us",
        "f-9qbb-marintransit",
        "f-9q8-samtrans",
        "f-9q9-bart",
        "f-9q9-caltrain",
        "f-9qc3-riovistadeltabreeze",
    ]);

    //for each static feed, update the information except the hull
    //hullless static feeds will be culled out before http response

    //update realtime data relations

    //update operators list

    for (operator_id, operator) in transitland_meta.operatorhashmap.iter() {
        let empty_vec: Vec<dmfr::OperatorAssociatedFeedsItem> = vec![];
        let listoffeeds = transitland_meta.operator_to_feed_hashmap
            .get(operator_id.as_str())
            .unwrap_or_else(|| &empty_vec)
            .to_owned();
        let mut gtfs_static_feeds: HashMap<String, Option<String>> = HashMap::new();
        let mut gtfs_realtime_feeds: HashMap<String, Option<String>> = HashMap::new();
        let mut simplified_array_static: Vec<String> = vec![];
        let mut simplified_array_realtime: Vec<String> = vec![];

        for x in listoffeeds {
            //get type
            if x.feed_onestop_id.is_some() {
                if transitland_meta.feedhashmap.contains_key((&x.feed_onestop_id).as_ref().unwrap()) {
                    let feed = transitland_meta.feedhashmap
                        .get((&x.feed_onestop_id).as_ref().unwrap())
                        .unwrap();
                    match feed.spec {
                        dmfr::FeedSpec::Gtfs => {
                            if !feeds_to_discard
                                .contains(&(&x.feed_onestop_id).as_ref().unwrap().as_str())
                            {
                                gtfs_static_feeds.insert(
                                    x.feed_onestop_id.to_owned().unwrap(),
                                    x.gtfs_agency_id,
                                );
                                simplified_array_static.push(x.feed_onestop_id.to_owned().unwrap());
                            }
                        }
                        dmfr::FeedSpec::GtfsRt => {
                            gtfs_realtime_feeds
                                .insert(x.feed_onestop_id.to_owned().unwrap(), x.gtfs_agency_id);
                            simplified_array_realtime.push(x.feed_onestop_id.to_owned().unwrap());
                        }
                        _ => {
                            //do nothing
                        }
                    }
                }
            }

            let _ = sqlx::query!("UPDATE gtfs.operators SET name = $2, gtfs_static_feeds = $3, gtfs_realtime_feeds = $4, static_onestop_feeds_to_gtfs_ids = $5, realtime_onestop_feeds_to_gtfs_ids = $6 WHERE onestop_operator_id = $1;",
                    &operator.onestop_id,
                    &operator.name,
                    // gtfs_static_feeds text[]
                    &serde_json::json!(simplified_array_static),
                    //gtfs_realtime_feeds text[]
                    &serde_json::json!(simplified_array_realtime),
                    //static_onestop_feeds_to_gtfs_ids JSONB,
                    &gtfs_static_feeds,
                    //realtime_onestop_feeds_to_gtfs_ids JSONB
                    &gtfs_realtime_feeds
        ).execute(pool).await;
            
        }
    }
}
