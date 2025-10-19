use catenary::aspen::lib::ChateauxLeaderHashMap;
use catenary::postgres_tools::CatenaryPostgresPool;
use std::error::Error;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
#[path = "aspen_assignment.rs"]
mod aspen_assignment;

pub async fn aspen_leader_thread(
    workers_nodes: Arc<Mutex<Vec<String>>>,
    feeds_list: Arc<Mutex<Option<ChateauxLeaderHashMap>>>,
    this_worker_id: Arc<String>,
    arc_conn_pool: Arc<CatenaryPostgresPool>,
    etcd_addresses: Arc<Vec<String>>,
    arc_etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    lease_id_for_this_worker: i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("starting leader thread");

    let mut etcd = etcd_client::Client::connect(
        etcd_addresses.as_slice(),
        arc_etcd_connection_options.as_ref().to_owned(),
    )
    .await?;

    println!("Connected to etcd!");

    loop {
        //attempt to become leader

        let make_lease = etcd
            .lease_grant(
                //10 seconds
                10,
                Some(etcd_client::LeaseGrantOptions::new().with_id(lease_id_for_this_worker)),
            )
            .await;

        if let Err(make_lease_err) = make_lease {
            eprintln!("Error connecting to etcd: {:#?}", make_lease_err);

            if let etcd_client::Error::GRpcStatus(status) = &make_lease_err {
                eprintln!("A gRPC error occurred: {}", status.message());
            } else {
                return Err(Box::new(make_lease_err));
            }
        }

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
                                catenary::bincode_serialize(this_worker_id.as_ref()).unwrap(),
                                lease_id_for_this_worker,
                            )
                            .await;

                        println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);
                    }
                    Some(leader_kv) => {
                        let leader_id: String =
                            catenary::bincode_deserialize(leader_kv.value()).unwrap();

                        if &leader_id == this_worker_id.as_ref() {
                            // I AM THE LEADER!!!

                            println!("I AM THE LEADER!!!");

                            //if the current is the current worker id, do leader tasks
                            // Read the DMFR dataset, divide it into chunks, and assign it to workers

                            let assign_round = aspen_assignment::assign_chateaus(
                                &mut etcd,
                                Arc::clone(&arc_conn_pool),
                                Arc::clone(&workers_nodes),
                                Arc::clone(&feeds_list),
                            )
                            .await;

                            if let Err(e) = &assign_round {
                                eprintln!("Error in assign_round: {:#?}", e);
                            }

                            //renew the etcd lease
                            let _ = etcd.lease_keep_alive(lease_id_for_this_worker).await?;

                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
            Err(leader_election_err) => {
                let attempt_to_become_leader = election_client
                    .campaign(
                        "/aspen_leader",
                        catenary::bincode_serialize(this_worker_id.as_ref()).unwrap(),
                        lease_id_for_this_worker,
                    )
                    .await;

                println!("attempt_to_become_leader: {:#?}", attempt_to_become_leader);

                eprintln!("{:#?}", leader_election_err);
            }
        }

        //renew the etcd lease
        let lease_renewal = etcd.lease_keep_alive(lease_id_for_this_worker).await;

        if (lease_renewal.is_err()) {
            eprintln!("Error renewing lease: {:#?}", lease_renewal);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
