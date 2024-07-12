use crate::get_feed_metadata::get_feed_metadata;
use crate::RealtimeFeedFetch;
use catenary::fast_hash;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use dmfr_folder_reader::read_folders;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

pub async fn perform_leader_job(
    arc_conn_pool: Arc<CatenaryPostgresPool>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    //Get data from postgres
    let feeds = get_feed_metadata(Arc::clone(&arc_conn_pool)).await?;

    let dmfr_result = read_folders("./transitland-atlas/")?;

    //get everything out of realtime feeds table and realtime password tables

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre?;

    let feeds_map: BTreeMap<String, RealtimeFeedFetch> = {
        let mut feeds_map = BTreeMap::new();
        for feed in feeds {
            feeds_map.insert(feed.feed_id.clone(), feed);
        }
        feeds_map
    };

    let fast_hash_of_feeds = fast_hash(&feeds_map);

    

    Ok(())
}
