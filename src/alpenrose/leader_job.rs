use crate::get_feed_metadata::get_feed_metadata;
use crate::RealtimeFeedFetch;
use catenary::fast_hash;
use catenary::postgres_tools::CatenaryPostgresPool;
use dmfr_dataset_reader::read_folders;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

pub async fn perform_leader_job(
    etcd: &mut etcd_client::Client,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    last_set_of_active_nodes_hash: &mut Option<u64>,
    last_updated_feeds_hash: &mut Option<u64>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    //Get data from postgres
    let feeds = get_feed_metadata(Arc::clone(&arc_conn_pool)).await?;

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

    let fetch_workers_hashmap = etcd
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
                bincode::deserialize::<i64>(kv.value()).unwrap(),
            )
        })
        .collect::<HashMap<String, i64>>();

    let mut workers_list = fetch_workers_hashmap
        .keys()
        .cloned()
        .collect::<Vec<String>>();

    workers_list.sort();

    let fast_hash_of_worker_nodes = fast_hash(&workers_list);

    if *last_set_of_active_nodes_hash != Some(fast_hash_of_worker_nodes)
        || *last_updated_feeds_hash != Some(fast_hash_of_feeds)
    {
        *last_updated_feeds_hash = Some(fast_hash_of_feeds);
        *last_set_of_active_nodes_hash = Some(fast_hash_of_worker_nodes);

        //The state of the distributed system has changed!

        //Time to reassign!

        // divide feeds between worker nodes

        // feed id -> List of realtime fetch instructions
        let mut assignments: BTreeMap<String, HashMap<String, RealtimeFeedFetch>> = BTreeMap::new();

        for (index, (feed_id, realtime_instructions)) in feeds_map.iter().enumerate() {
            let node_to_assign = &workers_list[index % workers_list.len()];

            //append to list
            assignments
                .entry(node_to_assign.to_string())
                .and_modify(|instructions| {
                    instructions.insert(feed_id.clone(), realtime_instructions.clone());
                })
                .or_insert({
                    let mut map = HashMap::new();
                    map.insert(feed_id.clone(), realtime_instructions.clone());
                    map
                });
        }

        //lock it so you can't change it anymore
        let assignments = assignments;

        for (worker_id, instructions_hashmap) in assignments.iter() {
            let lease_option = etcd_client::PutOptions::new()
                .with_lease(*fetch_workers_hashmap.get(worker_id).unwrap());

            for (feed_id, realtime_instruction) in instructions_hashmap {

                //get data from the feed

                let existing_assignment = etcd
                    .get(
                        format!("/alpenrose_assignments/{}/{}", worker_id, feed_id).as_str(),
                        None,
                    )
                    .await;

                if let Ok(existing_assignment) = existing_assignment {
                    if let Some(existing_assignment) = existing_assignment.kvs().get(0) {
                        let existing_assignment =
                            bincode::deserialize::<RealtimeFeedFetch>(existing_assignment.value())
                                .unwrap();

                        if existing_assignment == *realtime_instruction {
                            continue;
                        }
                    }
                }

                let set_assignment = etcd
                    .put(
                        format!("/alpenrose_assignments/{}/{}", worker_id, feed_id).as_str(),
                        bincode::serialize(&realtime_instruction).unwrap(),
                        Some(lease_option.clone()),
                    )
                    .await;

                if let Err(err) = &set_assignment {
                    eprintln!("{:#?}", err)
                }
            }
            //update the last updated time

            let set_metadata_updated_time = etcd
                .put(
                    format!("/alpenrose_assignments_last_updated/{}", worker_id).as_str(),
                    bincode::serialize(&catenary::duration_since_unix_epoch().as_millis()).unwrap(),
                    Some(lease_option),
                )
                .await;

            if let Err(err) = &set_metadata_updated_time {
                eprintln!("{:#?}", err)
            }
        }
    }

    Ok(())
}
