use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::bincode_deserialize;
use catenary::postgres_tools::CatenaryPostgresPool;
use rayon::prelude::*;
use std::sync::Arc;

pub const ALLOWED_CHATEAUX: &[&str] = &[
    "deutschland",
    "sncf",
    "nationalrailuk",
    "schweiz",
    "île~de~france~mobilités",
    "sncb",
    "tisséo",
];

pub use catenary::pasque::lib::TrajectorySubscriptionParams;
pub use catenary::pasque::lib::TrajectoryWrapper;

#[derive(serde::Deserialize, Clone, Debug)]
pub struct ClientTrajectorySubscriptionParams {
    pub bbox: Vec<f64>,
    pub zoom: u8,
    pub modes: Vec<String>,
    pub precision: Option<u8>,
    pub client_reference: String,
}

async fn get_aspen_socket(
    chateau_id: &str,
    etcd_ips: &EtcdConnectionIps,
    etcd_opts: &Option<etcd_client::ConnectOptions>,
    etcd_reuser: &tokio::sync::RwLock<Option<etcd_client::Client>>,
) -> Option<std::net::SocketAddr> {
    let mut etcd = catenary::get_etcd_client(etcd_ips, etcd_opts, etcd_reuser)
        .await
        .ok()?;
    let resp = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
            None,
        )
        .await
        .ok()?;
    if !resp.kvs().is_empty() {
        if let Ok(assigned_chateau_data) =
            bincode_deserialize::<ChateauMetadataEtcd>(resp.kvs().first().unwrap().value())
        {
            return Some(assigned_chateau_data.socket);
        }
    }
    None
}
use std::io::Write;

pub async fn get_single_chateau_trajectories(
    _pool: Arc<CatenaryPostgresPool>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    params: TrajectorySubscriptionParams,
    chateau: String,
) -> Vec<TrajectoryWrapper> {
    if !ALLOWED_CHATEAUX.contains(&chateau.as_str()) {
        return vec![];
    }

    let fetch_task = async {
        let socket = match get_aspen_socket(
            &chateau,
            &etcd_connection_ips,
            &etcd_connection_options,
            &etcd_reuser,
        )
        .await
        {
            Some(s) => s,
            None => return vec![],
        };

        let client_res = if let Some(client) = aspen_client_manager.get_client(socket.clone()).await
        {
            Some(client)
        } else if let Ok(new_client) =
            catenary::aspen::lib::spawn_aspen_client_from_ip(&socket).await
        {
            aspen_client_manager
                .insert_client(socket.clone(), new_client.clone())
                .await;
            Some(new_client)
        } else {
            None
        };

        if let Some(mut client) = client_res {
            let client_reference = params.client_reference.clone();

            let mut ctx = tarpc::context::current();
            ctx.deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);

            match client.get_trajectories(ctx, chateau.clone(), params).await {
                Ok(Ok(trajectories)) => {
                    let now_ms = catenary::duration_since_unix_epoch().as_millis() as u64;
                    trajectories
                        .into_iter()
                        .map(|traj| TrajectoryWrapper {
                            source: "trajectory".to_string(),
                            timestamp: now_ms,
                            client_reference: client_reference.clone(),
                            content: traj,
                        })
                        .collect()
                }
                Ok(Err(e)) => {
                    eprintln!("Aspen RPC logic error for {}: {:?}", chateau, e);
                    vec![]
                }
                Err(e) => {
                    eprintln!("Aspen RPC transport error for {}: {:?}", chateau, e);
                    vec![]
                }
            }
        } else {
            vec![]
        }
    };

    // Prevent slow or hanging etcd/RPC connections from blocking the entire response.
    // Increased timeout to 60s to allow large chateaus like ile~de~france~mobilités
    match tokio::time::timeout(std::time::Duration::from_secs(60), fetch_task).await {
        Ok(res) => res,
        Err(_) => {
            eprintln!("Timeout fetching trajectories for chateau {}", chateau);
            vec![]
        }
    }
}

pub async fn get_trajectories(
    _pool: Arc<CatenaryPostgresPool>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    params: TrajectorySubscriptionParams,
    chateaus: std::collections::HashSet<String>,
) -> Result<Vec<TrajectoryWrapper>, String> {
    let zoom = params.zoom;
    let mut futures = Vec::new();

    for ch in chateaus {
        if !ALLOWED_CHATEAUX.contains(&ch.as_str()) {
            continue;
        }
        let ips = etcd_connection_ips.clone();
        let opts = etcd_connection_options.clone();
        let reuser = etcd_reuser.clone();
        let manager = aspen_client_manager.clone();
        let params_clone = params.clone();
        let ch_clone = ch.clone();

        futures.push(tokio::spawn(async move {
            let fetch_task = async {
                let socket = match get_aspen_socket(&ch_clone, &ips, &opts, &reuser).await {
                    Some(s) => s,
                    None => return vec![],
                };

                let client_res = if let Some(client) = manager.get_client(socket.clone()).await {
                    Some(client)
                } else if let Ok(new_client) =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&socket).await
                {
                    manager
                        .insert_client(socket.clone(), new_client.clone())
                        .await;
                    Some(new_client)
                } else {
                    None
                };

                if let Some(mut client) = client_res {
                    let client_reference = params_clone.client_reference.clone();

                    let mut ctx = tarpc::context::current();
                    ctx.deadline =
                        std::time::Instant::now() + std::time::Duration::from_secs(60);

                    match client
                        .get_trajectories(ctx, ch_clone.clone(), params_clone)
                        .await
                    {
                        Ok(Ok(trajectories)) => {
                            let now_ms = catenary::duration_since_unix_epoch().as_millis() as u64;
                            trajectories
                                .into_iter()
                                .map(|traj| TrajectoryWrapper {
                                    source: "trajectory".to_string(),
                                    timestamp: now_ms,
                                    client_reference: client_reference.clone(),
                                    content: traj,
                                })
                                .collect()
                        }
                        Ok(Err(e)) => {
                            eprintln!("Aspen RPC logic error for {}: {:?}", ch_clone, e);
                            vec![]
                        }
                        Err(e) => {
                            eprintln!("Aspen RPC transport error for {}: {:?}", ch_clone, e);
                            vec![]
                        }
                    }
                } else {
                    vec![]
                }
            };

            // Prevent slow or hanging etcd/RPC connections from blocking the entire response.
            match tokio::time::timeout(std::time::Duration::from_secs(60), fetch_task).await {
                Ok(res) => res,
                Err(_) => {
                    eprintln!("Timeout fetching trajectories for chateau {}", ch_clone);
                    vec![]
                }
            }
        }));
    }

    let results = futures::future::join_all(futures).await;
    let mut merged = Vec::new();
    for res in results {
        if let Ok(trajectories) = res {
            merged.extend(trajectories);
        }
    }

    Ok(merged)
}
