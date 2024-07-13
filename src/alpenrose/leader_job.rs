use crate::get_feed_metadata::get_feed_metadata;
use crate::RealtimeFeedFetch;
use catenary::fast_hash;
use catenary::postgres_tools::CatenaryConn;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use dmfr_folder_reader::read_folders;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

pub async fn perform_leader_job(
    etcd_client: &mut etcd_client::Client,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    last_set_of_active_nodes_hash: &mut Option<u64>,
    last_updated_feeds_hash: &mut Option<u64>,
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

    //get list of worker nodes

    let fetch_workers_hashmap = etcd_client
        .get(
            "/alpenrose_workers/",
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await?
        .take_kvs()
        .into_iter()
        .map(|kv| {
            (
                kv.key_str().unwrap().replace("/alpenrose_workers/", ""),
                bincode::deserialize::<u64>(kv.value()).unwrap(),
            )
        })
        .collect::<HashMap<String, u64>>();

    let mut workers_list = fetch_workers_hashmap
        .keys()
        .cloned()
        .collect::<Vec<String>>();

    workers_list.sort();

    let fast_hash_of_worker_nodes = fast_hash(&workers_list);

    if *last_set_of_active_nodes_hash != Some(fast_hash_of_worker_nodes)
        || *last_updated_feeds_hash != Some(fast_hash_of_feeds)
    {
        //The state of the distributed system has changed!

        //Time to reassign!
    }

    Ok(())
}
