use catenary::aspen::lib::ChateauMetadataZookeeper;
use catenary::aspen::lib::ChateausLeaderHashMap;
use catenary::aspen::lib::RealtimeFeedMetadataZookeeper;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::ChateauDataNoGeometry;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel::sql_types::{Float, Integer};
use diesel::ExpressionMethods;
use diesel::Selectable;
use diesel::SelectableHelper;
use diesel_async::pooled_connection::bb8::PooledConnection;
use diesel_async::RunQueryDsl;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_threadpool::Worker;
use tokio_zookeeper::ZooKeeper;
use tokio_zookeeper::{Acl, CreateMode};

pub async fn aspen_leader_thread(
    workers_nodes: Arc<Mutex<Vec<String>>>,
    feeds_list: Arc<Mutex<Option<ChateausLeaderHashMap>>>,
    this_worker_id: Arc<String>,
    tailscale_ip: Arc<IpAddr>,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
) {
    println!("starting leader thread");

    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    println!("Leader connected to postgres");

    loop {
        let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
            .await
            .unwrap();

        let _ = zk
            .create(
                "/aspen_workers",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        //register that the worker exists
        let _ = zk
            .create(
                format!("aspen_workers/{}", this_worker_id).as_str(),
                bincode::serialize(&tailscale_ip).unwrap(),
                Acl::open_unsafe(),
                CreateMode::Ephemeral,
            )
            .await
            .unwrap();

        let _ = zk
            .create(
                "/aspen_assigned_chateaus",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        let _ = zk
            .create(
                "/aspen_assigned_realtime_feed_ids",
                vec![],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        //attempt to check if the system is leaderless, if so, become the leader
        if zk.exists("/aspen_leader").await.unwrap().is_none() {
            let _ = zk
                .create(
                    "/aspen_leader",
                    bincode::serialize(&this_worker_id).unwrap(),
                    Acl::open_unsafe(),
                    CreateMode::Ephemeral,
                )
                .await
                .unwrap();
        }

        //if the current is the current worker id, do leader tasks
        // Read the DMFR dataset, divide it into chunks, and assign it to workers

        let current_leader = zk.get_data("/aspen_leader").await.unwrap();

        if let Some((leader_str_bytes, leader_stats)) = current_leader {
            let leader_id: String = bincode::deserialize(&leader_str_bytes).unwrap();

            if leader_id == *this_worker_id {
                println!("This is the leader thread!");
                //leader tasks
                let mut workers_nodes_lock = workers_nodes.lock().await;
                let mut chateau_list_lock = feeds_list.lock().await;

                //make a hashmap of workers and their tailscale ips
                let mut workers_ips = BTreeMap::new();
                let workers_nodes = zk.get_children("/aspen_workers").await.unwrap();
                if let Some(workers_nodes) = workers_nodes {
                    for worker_node in workers_nodes {
                        let worker_ip_data = zk
                            .get_data(format!("/aspen_workers/{}", worker_node).as_str())
                            .await
                            .unwrap();

                        if let Some((worker_ip_bytes, _)) = worker_ip_data {
                            let worker_ip: IpAddr = bincode::deserialize(&worker_ip_bytes).unwrap();
                            workers_ips.insert(worker_node, worker_ip);
                        }
                    }
                }

                // read out from postgres
                let chateaus_pg_query = catenary::schema::gtfs::chateaus::table
                    .select(catenary::models::Chateau::as_select())
                    .load::<catenary::models::Chateau>(conn)
                    .await;

                if let Ok(chateaus) = chateaus_pg_query {
                    //read into a btree

                    let chateau_cache_for_aspen_leader = ChateausLeaderHashMap {
                        chateaus: {
                            let mut chateaus_btree: BTreeMap<String, ChateauDataNoGeometry> =
                                BTreeMap::new();
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

                    let avaliable_aspen_workers = zk.get_children("/aspen_workers").await.unwrap();

                    let mut aspen_workers_list_changed = false;

                    if let Some(avaliable_aspen_workers) = avaliable_aspen_workers {
                        let mut avaliable_aspen_workers = avaliable_aspen_workers;
                        avaliable_aspen_workers.sort();

                        if avaliable_aspen_workers != *workers_nodes_lock {
                            *workers_nodes_lock = avaliable_aspen_workers;
                            aspen_workers_list_changed = true;
                        }
                    }

                    if chateau_data_changed || aspen_workers_list_changed {
                        println!("Assigning tasks to workers....");
                        // divide it into chunks
                        if let Some(chateau_list_lock) = chateau_list_lock.as_ref() {
                            for (index, (chateau_id, chateau)) in
                                chateau_list_lock.chateaus.iter().enumerate()
                            {
                                // in the future, this should be rewritten to prevent full reshuffling of the entire assignment list
                                // For example, taking only the orphened nodes and redistributing them
                                // if a new node is added, carefully reassign the data

                                //for now this simply round robins the chateaus around
                                let selected_aspen_worker_to_assign =
                                    workers_nodes_lock[index % workers_nodes_lock.len()].clone();

                                let assigned_chateau_data = ChateauMetadataZookeeper {
                                    worker_id: selected_aspen_worker_to_assign.clone(),
                                    tailscale_ip: *workers_ips
                                        .get(&selected_aspen_worker_to_assign)
                                        .unwrap(),
                                };

                                let _ = zk
                                    .create(
                                        format!("/aspen_assigned_chateaus/{}", chateau_id).as_str(),
                                        bincode::serialize(&assigned_chateau_data).unwrap(),
                                        Acl::open_unsafe(),
                                        CreateMode::Persistent,
                                    )
                                    .await
                                    .unwrap();

                                for realtime_feed_id in chateau.realtime_feeds.iter() {
                                    let assigned_realtime_feed_data =
                                        RealtimeFeedMetadataZookeeper {
                                            worker_id: selected_aspen_worker_to_assign.clone(),
                                            tailscale_ip: *workers_ips
                                                .get(&selected_aspen_worker_to_assign)
                                                .unwrap(),
                                            chateau_id: chateau_id.clone(),
                                        };

                                    let _ = zk
                                        .create(
                                            format!(
                                                "/aspen_assigned_realtime_feed_ids/{}",
                                                realtime_feed_id
                                            )
                                            .as_str(),
                                            bincode::serialize(&assigned_realtime_feed_data)
                                                .unwrap(),
                                            Acl::open_unsafe(),
                                            CreateMode::Persistent,
                                        )
                                        .await
                                        .unwrap();
                                }
                            }
                            println!("Assigned {} chateaus across {} workers", chateau_list_lock.chateaus.len(), workers_nodes_lock.len());
                        }
                    }

                    std::mem::drop(workers_nodes_lock);
                    std::mem::drop(chateau_list_lock);
                } else {
                    println!("Error reading chateaus from postgres");
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(10));
        } else {
            println!("Leader entry contains no data");
        }
    }
}
