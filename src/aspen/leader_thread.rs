use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::ChateausLeaderHashMap;
use catenary::aspen::lib::RealtimeFeedMetadataEtcd;
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
use std::error::Error;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_threadpool::Worker;

pub async fn aspen_leader_thread(
    workers_nodes: Arc<Mutex<Vec<String>>>,
    feeds_list: Arc<Mutex<Option<ChateausLeaderHashMap>>>,
    this_worker_id: Arc<String>,
    tailscale_ip: Arc<IpAddr>,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    etcd_addresses: Arc<Vec<String>>,
    lease_id_for_this_worker: i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("starting leader thread");

    let mut etcd = etcd_client::Client::connect(etcd_addresses.as_slice(), None).await?;

    println!("Connected to etcd!");

    let worker_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let feeds_list: Arc<Mutex<Option<ChateausLeaderHashMap>>> = Arc::new(Mutex::new(None));

    loop {
        //attempt to become leader

        let mut election_client = etcd.election_client();

        let current_leader_election = election_client.leader("/aspen_leader").await;

        match current_leader_election {
            Ok(current_leader_election) => {
                let leader_kv = current_leader_election.kv();

                match leader_kv {
                    None => {
                        let attempt_to_become_leader = election_client
                            .campaign(
                                "/aspen_leader",
                                bincode::serialize(this_worker_id.as_ref()).unwrap(),
                                lease_id_for_this_worker,
                            )
                            .await;

                        println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);
                    }
                    Some(leader_kv) => {
                        let leader_id: String = bincode::deserialize(leader_kv.value()).unwrap();

                        if &leader_id == this_worker_id.as_ref() {
                            // I AM THE LEADER!!!

                            println!("I AM THE LEADER!!!");

                            //if the current is the current worker id, do leader tasks
                            // Read the DMFR dataset, divide it into chunks, and assign it to workers

                            crate::aspen_assignment::assign_chateaus(
                                &mut etcd,
                                Arc::clone(&arc_conn_pool),
                                Arc::clone(&workers_nodes),
                                Arc::clone(&feeds_list),
                            )
                            .await?;
                        }
                    }
                }
            }
            Err(leader_election_err) => {
                let attempt_to_become_leader = election_client
                    .campaign(
                        "/aspen_leader",
                        bincode::serialize(this_worker_id.as_ref()).unwrap(),
                        lease_id_for_this_worker,
                    )
                    .await;

                println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);

                eprintln!("{:#?}", leader_election_err);
            }
        }

        //renew the etcd lease
        let _ = etcd.lease_keep_alive(lease_id_for_this_worker).await?;

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
