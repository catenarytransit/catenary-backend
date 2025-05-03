use catenary::ChateauDataNoGeometry;
use catenary::aspen::lib::AspenWorkerMetadataEtcd;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::ChateauxLeaderHashMap;
use catenary::aspen::lib::RealtimeFeedMetadataEtcd;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::SelectableHelper;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel_async::RunQueryDsl;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn assign_chateaus(
    etcd: &mut etcd_client::Client,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    workers_nodes: Arc<Mutex<Vec<String>>>,
    feeds_list: Arc<Mutex<Option<ChateauxLeaderHashMap>>>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    if let Err(conn_pre) = &conn_pre {
        println!("Error with leader connecting to postgres");
        eprintln!("{}", conn_pre);
    }

    let conn = &mut conn_pre?;

    // read out from postgres
    let chateaux_pg_query = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await;

    if let Ok(chateaux) = chateaux_pg_query {
        let mut chateau_list_lock = feeds_list.lock().await;
        let mut workers_nodes_lock = workers_nodes.lock().await;

        let chateau_cache_for_aspen_leader = ChateauxLeaderHashMap {
            chateaus: {
                let mut chateaux_btree: BTreeMap<String, ChateauDataNoGeometry> = BTreeMap::new();
                for chateau in chateaux {
                    chateaux_btree.insert(
                        chateau.chateau.clone(),
                        ChateauDataNoGeometry {
                            chateau_id: chateau.chateau.clone(),
                            static_feeds: chateau
                                .static_feeds
                                .clone()
                                .into_iter()
                                .flatten()
                                .collect(),
                            realtime_feeds: chateau
                                .realtime_feeds
                                .clone()
                                .into_iter()
                                .flatten()
                                .collect(),
                        },
                    );
                }
                chateaux_btree
            },
        };

        let mut chateau_data_changed = false;

        if chateau_list_lock.is_none() {
            *chateau_list_lock = Some(chateau_cache_for_aspen_leader);
            chateau_data_changed = true;
        } else {
            let chateau_list = chateau_list_lock.as_mut().unwrap();
            if chateau_list != &chateau_cache_for_aspen_leader {
                *chateau_list_lock = Some(chateau_cache_for_aspen_leader);
                chateau_data_changed = true;
            }
        }

        //make a hashmap of workers and their ips
        let mut workers_map = BTreeMap::new();

        let mut fetch_workers_from_etcd_req = etcd
            .get(
                "/aspen_workers",
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await?;

        let fetch_workers_from_etcd = fetch_workers_from_etcd_req.take_kvs();

        let fetch_workers_revision_number =
            fetch_workers_from_etcd_req.header().unwrap().revision();

        for kv in fetch_workers_from_etcd {
            let decoded_metadata =
                catenary::bincode_deserialize::<AspenWorkerMetadataEtcd>(kv.value());

            if let Ok(decoded_metadata) = decoded_metadata {
                workers_map.insert(decoded_metadata.worker_id.clone(), decoded_metadata.clone());
            }
        }

        //lock mutability
        let workers_map = workers_map;

        //make a vec out of the worker map and throw it back into the mutex
        *workers_nodes_lock = workers_map.keys().cloned().collect::<Vec<String>>();
        workers_nodes_lock.sort();

        // assignment section

        if workers_nodes_lock.len() == 0 {
            println!("No workers available to assign chateaus to");
        } else {
            println!("Assigning tasks to workers....");

            //prefix fetch aspen_assigned_chateaux

            let mut fetch_assigned_chateaus = etcd
                .get(
                    "/aspen_assigned_chateaux",
                    Some(etcd_client::GetOptions::new().with_prefix()),
                )
                .await?;

            let fetch_assigned_chateaus = fetch_assigned_chateaus.take_kvs();

            let mut existing_assigned_chateaus = HashMap::new();

            for kv in fetch_assigned_chateaus {
                let decoded_metadata =
                    catenary::bincode_deserialize::<ChateauMetadataEtcd>(kv.value());

                if let Ok(decoded_metadata) = decoded_metadata {
                    let key = kv.key_str().unwrap();
                    let chateau = key.replace("/aspen_assigned_chateaux/", "");
                    existing_assigned_chateaus.insert(chateau, decoded_metadata);
                }
            }

            let mut existing_assigned_realtime_feeds = HashMap::new();

            let mut fetch_assigned_realtime_feeds = etcd
                .get(
                    "/aspen_assigned_realtime_feed_ids",
                    Some(etcd_client::GetOptions::new().with_prefix()),
                )
                .await?;

            let fetch_assigned_realtime_feeds = fetch_assigned_realtime_feeds.take_kvs();

            for kv in fetch_assigned_realtime_feeds {
                let decoded_metadata =
                    catenary::bincode_deserialize::<RealtimeFeedMetadataEtcd>(kv.value());

                if let Ok(decoded_metadata) = decoded_metadata {
                    let key = kv.key_str().unwrap();
                    let realtime_feed_id = key.replace("/aspen_assigned_realtime_feed_ids/", "");
                    existing_assigned_realtime_feeds.insert(realtime_feed_id, decoded_metadata);
                }
            }

            if let Some(chateau_list_lock) = chateau_list_lock.as_ref() {
                for (index, (chateau_id, chateau)) in chateau_list_lock.chateaus.iter().enumerate()
                {
                    // in the future, this should be rewritten to prevent full reshuffling of the entire assignment list
                    // For example, taking only the orphened nodes and redistributing them
                    // if a new node is added, carefully reassign the data

                    //for now this simply round robins the chateaux around

                    let selected_aspen_worker_to_assign =
                        workers_nodes_lock[index % workers_nodes_lock.len()].clone();

                    let worker_metadata =
                        workers_map.get(&selected_aspen_worker_to_assign).unwrap();

                    let assigned_chateau_data = ChateauMetadataEtcd {
                        worker_id: selected_aspen_worker_to_assign.clone(),
                        socket: worker_metadata.socket,
                    };

                    // get data from etcd

                    let mut assign_chateau_required = true;

                    if let Some(existing_data) = existing_assigned_chateaus.get(chateau_id) {
                        if assigned_chateau_data == *existing_data {
                            assign_chateau_required = false;
                        }
                    }

                    if assign_chateau_required {
                        let save_to_etcd = etcd
                            .put(
                                format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
                                catenary::bincode_serialize(&assigned_chateau_data).unwrap(),
                                Some(
                                    etcd_client::PutOptions::new()
                                        .with_lease(worker_metadata.etcd_lease_id),
                                ),
                            )
                            .await?;
                    }

                    for realtime_feed_id in chateau.realtime_feeds.iter() {
                        let assigned_realtime_feed_data = RealtimeFeedMetadataEtcd {
                            worker_id: selected_aspen_worker_to_assign.clone(),
                            socket: worker_metadata.socket,
                            chateau_id: chateau_id.clone(),
                        };

                        let mut assign_realtime_feed_required = true;

                        if let Some(existing_data) =
                            existing_assigned_realtime_feeds.get(realtime_feed_id)
                        {
                            if assigned_realtime_feed_data == *existing_data {
                                assign_realtime_feed_required = false;
                            }
                        }

                        if assign_realtime_feed_required {
                            let save_to_etcd_realtime_data = etcd
                                .put(
                                    format!(
                                        "/aspen_assigned_realtime_feed_ids/{}",
                                        realtime_feed_id
                                    ),
                                    catenary::bincode_serialize(&assigned_realtime_feed_data)
                                        .unwrap(),
                                    Some(
                                        etcd_client::PutOptions::new()
                                            .with_lease(worker_metadata.etcd_lease_id),
                                    ),
                                )
                                .await?;
                        }
                    }
                }

                println!(
                    "Assigned {} chateaux across {} workers",
                    chateau_list_lock.chateaus.len(),
                    workers_nodes_lock.len()
                );
            }

            //compact history

            etcd.compact(fetch_workers_revision_number, None).await?;
        }
    }

    Ok(())
}
