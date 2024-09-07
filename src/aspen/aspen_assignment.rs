use catenary::aspen::lib::AspenWorkerMetadataEtcd;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::ChateausLeaderHashMap;
use catenary::aspen::lib::RealtimeFeedMetadataEtcd;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::ChateauDataNoGeometry;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn assign_chateaus(
    etcd: &mut etcd_client::Client,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    workers_nodes: Arc<Mutex<Vec<String>>>,
    feeds_list: Arc<Mutex<Option<ChateausLeaderHashMap>>>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    if let Err(conn_pre) = &conn_pre {
        println!("Error with leader connecting to postgres");
        eprintln!("{}", conn_pre);
    }

    let conn = &mut conn_pre?;

    // read out from postgres
    let chateaus_pg_query = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await;

    if let Ok(chateaus) = chateaus_pg_query {
        let mut chateau_list_lock = feeds_list.lock().await;
        let mut workers_nodes_lock = workers_nodes.lock().await;

        let chateau_cache_for_aspen_leader = ChateausLeaderHashMap {
            chateaus: {
                let mut chateaus_btree: BTreeMap<String, ChateauDataNoGeometry> = BTreeMap::new();
                for chateau in chateaus {
                    chateaus_btree.insert(
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
                chateaus_btree
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

        //make a hashmap of workers and their tailscale ips
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
            let decoded_metadata = bincode::deserialize::<AspenWorkerMetadataEtcd>(kv.value());

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

            if let Some(chateau_list_lock) = chateau_list_lock.as_ref() {
                for (index, (chateau_id, chateau)) in chateau_list_lock.chateaus.iter().enumerate()
                {
                    // in the future, this should be rewritten to prevent full reshuffling of the entire assignment list
                    // For example, taking only the orphened nodes and redistributing them
                    // if a new node is added, carefully reassign the data

                    //for now this simply round robins the chateaus around

                    let selected_aspen_worker_to_assign =
                        workers_nodes_lock[index % workers_nodes_lock.len()].clone();

                    let worker_metadata =
                        workers_map.get(&selected_aspen_worker_to_assign).unwrap();

                    let assigned_chateau_data = ChateauMetadataEtcd {
                        worker_id: selected_aspen_worker_to_assign.clone(),
                        socket: worker_metadata.socket,
                    };

                    let save_to_etcd = etcd
                        .put(
                            format!("/aspen_assigned_chateaus/{}", chateau_id).as_str(),
                            bincode::serialize(&assigned_chateau_data).unwrap(),
                            Some(
                                etcd_client::PutOptions::new()
                                    .with_lease(worker_metadata.etcd_lease_id),
                            ),
                        )
                        .await?;

                    for realtime_feed_id in chateau.realtime_feeds.iter() {
                        let assigned_realtime_feed_data = RealtimeFeedMetadataEtcd {
                            worker_id: selected_aspen_worker_to_assign.clone(),
                            socket: worker_metadata.socket,
                            chateau_id: chateau_id.clone(),
                        };

                        let save_to_etcd_realtime_data = etcd
                            .put(
                                format!("/aspen_assigned_realtime_feed_ids/{}", realtime_feed_id),
                                bincode::serialize(&assigned_realtime_feed_data).unwrap(),
                                Some(
                                    etcd_client::PutOptions::new()
                                        .with_lease(worker_metadata.etcd_lease_id),
                                ),
                            )
                            .await?;
                    }
                }

                println!(
                    "Assigned {} chateaus across {} workers",
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
