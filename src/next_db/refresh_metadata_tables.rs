use crate::dmfr;
use crate::get_feeds_meta;
use crate::get_feeds_meta::OperatorPairInfo;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
        let listoffeeds = transitland_meta
            .operator_to_feed_hashmap
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
                if transitland_meta
                    .feedhashmap
                    .contains_key((&x.feed_onestop_id).as_ref().unwrap())
                {
                    let feed = transitland_meta
                        .feedhashmap
                        .get((&x.feed_onestop_id).as_ref().unwrap())
                        .unwrap();
                    let bruhitfailed: Vec<OperatorPairInfo> = vec![];
                    let listofoperatorpairs = transitland_meta
                        .feed_to_operator_pairs_hashmap
                        .get(&feed.id)
                        .unwrap_or_else(|| &bruhitfailed)
                        .to_owned();
                    let mut operator_pairs_hashmap: HashMap<String, Option<String>> =
                        HashMap::new();
                    for operator_pair in listofoperatorpairs {
                        operator_pairs_hashmap
                            .insert(operator_pair.operator_id, operator_pair.gtfs_agency_id);
                    }
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

                            let operators_owned = operator_pairs_hashmap.iter().map(|(a,b)| a.clone()).collect::<Vec<String>>();

                            let _ = sqlx::query!("INSERT INTO gtfs.realtime_feeds (onestop_feed_id, name, operators, operators_to_gtfs_ids)
                            VALUES ($1, $2, $3, $4) ON CONFLICT (onestop_feed_id) DO UPDATE SET name = $2, operators = $3, operators_to_gtfs_ids = $4;",
                            &feed.id,
                            &"",
                            &operators_owned.as_slice(),
                            &serde_json::json!(serde_json::map::Map::from_iter(operator_pairs_hashmap.iter().map(|(key,value)| {
                                (key.clone(), serde_json::json!(value.clone()))
                            })
                        ))
                        ).execute(pool).await;
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
                    &simplified_array_static,
                    //gtfs_realtime_feeds text[]
                    &simplified_array_realtime,
                    //static_onestop_feeds_to_gtfs_ids JSONB,
                    &serde_json::json!(serde_json::map::Map::from_iter(gtfs_static_feeds.iter()
                    .map(|(key,value)| {
                        (key.clone(), serde_json::json!(value.clone()))
                    })
                )),
                    //realtime_onestop_feeds_to_gtfs_ids JSONB
                    &serde_json::json!(serde_json::map::Map::from_iter(gtfs_realtime_feeds.iter().map(|(key,value)| {
                        (key.clone(), serde_json::json!(value.clone()))
                    })))
        ).execute(pool).await;
        }
    }
}
