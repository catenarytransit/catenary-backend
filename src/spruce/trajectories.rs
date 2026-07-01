use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::bincode_deserialize;
use catenary::postgres_tools::CatenaryPostgresPool;
use rayon::prelude::*;
use std::sync::Arc;

const ALLOWED_CHATEAUX: &[&str] = &[
    "deutschland",
    "sncf",
    "nationalrailuk",
    "schweiz",
    "île~de~france~mobilités",
    "sncb",
    "tisséo",
];

fn haversine_distance_meters(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let earth_radius_km = 6371.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    earth_radius_km * c * 1000.0
}

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
            let min_lon = params.bbox.get(0).copied().unwrap_or(0.0);
            let min_lat = params.bbox.get(1).copied().unwrap_or(0.0);
            let max_lon = params.bbox.get(2).copied().unwrap_or(0.0);
            let max_lat = params.bbox.get(3).copied().unwrap_or(0.0);

            let lat_diff_km = (max_lat - min_lat).abs() * 111.32;
            let avg_lat = (min_lat + max_lat) / 2.0;
            let lon_diff_km = (max_lon - min_lon).abs() * 111.32 * avg_lat.to_radians().cos().abs();
            let min_dim_km = lat_diff_km.min(lon_diff_km);

            let simplify_meters = if min_dim_km > 200.0 {
                1000.0
            } else if min_dim_km > 100.0 {
                500.0
            } else if min_dim_km > 50.0 {
                250.0
            } else if min_dim_km > 10.0 {
                100.0
            } else if min_dim_km > 5.0 {
                20.0
            } else {
                5.0
            };

            match client
                .get_trajectories(tarpc::context::current(), chateau.clone(), params)
                .await
            {
                Ok(Ok(trajectories)) => {
                    let now_ms = catenary::duration_since_unix_epoch().as_millis() as u64;
                    tokio::task::spawn_blocking(move || {
                        trajectories
                            .into_par_iter()
                            .map(|mut traj| {
                                for seg in &mut traj.segments {
                                    seg.coordinates.retain(|pt| !(pt[0] == 0.0 && pt[1] == 0.0));
                                }

                                if simplify_meters > 0.0 {
                                    for seg in &mut traj.segments {
                                        if seg.coordinates.len() > 2 {
                                            let mut simplified =
                                                Vec::with_capacity(seg.coordinates.len());
                                            simplified.push(seg.coordinates[0]);
                                            let mut last_kept = seg.coordinates[0];

                                            for i in 1..seg.coordinates.len() - 1 {
                                                let pt = seg.coordinates[i];
                                                let dist = haversine_distance_meters(
                                                    last_kept[0],
                                                    last_kept[1],
                                                    pt[0],
                                                    pt[1],
                                                );
                                                if dist >= simplify_meters {
                                                    simplified.push(pt);
                                                    last_kept = pt;
                                                }
                                            }

                                            simplified
                                                .push(seg.coordinates[seg.coordinates.len() - 1]);
                                            seg.coordinates = simplified;
                                        }
                                    }
                                }

                                TrajectoryWrapper {
                                    source: "trajectory".to_string(),
                                    timestamp: now_ms,
                                    client_reference: client_reference.clone(),
                                    content: serde_json::to_value(traj)
                                        .unwrap_or(serde_json::Value::Null),
                                }
                            })
                            .collect()
                    })
                    .await
                    .unwrap_or_else(|_| vec![])
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
    // Increased timeout to 15s to allow large chateaus like ile~de~france~mobilités
    match tokio::time::timeout(std::time::Duration::from_secs(15), fetch_task).await {
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
                    let min_lon = params_clone.bbox.get(0).copied().unwrap_or(0.0);
                    let min_lat = params_clone.bbox.get(1).copied().unwrap_or(0.0);
                    let max_lon = params_clone.bbox.get(2).copied().unwrap_or(0.0);
                    let max_lat = params_clone.bbox.get(3).copied().unwrap_or(0.0);

                    let lat_diff_km = (max_lat - min_lat).abs() * 111.32;
                    let avg_lat = (min_lat + max_lat) / 2.0;
                    let lon_diff_km =
                        (max_lon - min_lon).abs() * 111.32 * avg_lat.to_radians().cos().abs();
                    let min_dim_km = lat_diff_km.min(lon_diff_km);

                    let simplify_meters = if min_dim_km > 200.0 {
                        1000.0
                    } else if min_dim_km > 100.0 {
                        500.0
                    } else if min_dim_km > 50.0 {
                        250.0
                    } else if min_dim_km > 10.0 {
                        100.0
                    } else if min_dim_km > 5.0 {
                        20.0
                    } else {
                        5.0
                    };

                    match client
                        .get_trajectories(tarpc::context::current(), ch_clone.clone(), params_clone)
                        .await
                    {
                        Ok(Ok(trajectories)) => {
                            let now_ms = catenary::duration_since_unix_epoch().as_millis() as u64;
                            tokio::task::spawn_blocking(move || {
                                trajectories
                                    .into_par_iter()
                                    .map(|mut traj| {
                                        for seg in &mut traj.segments {
                                            seg.coordinates
                                                .retain(|pt| !(pt[0] == 0.0 && pt[1] == 0.0));
                                        }

                                        if simplify_meters > 0.0 {
                                            for seg in &mut traj.segments {
                                                if seg.coordinates.len() > 2 {
                                                    let mut simplified =
                                                        Vec::with_capacity(seg.coordinates.len());
                                                    simplified.push(seg.coordinates[0]);
                                                    let mut last_kept = seg.coordinates[0];

                                                    for i in 1..seg.coordinates.len() - 1 {
                                                        let pt = seg.coordinates[i];
                                                        let dist = haversine_distance_meters(
                                                            last_kept[0],
                                                            last_kept[1],
                                                            pt[0],
                                                            pt[1],
                                                        );
                                                        if dist >= simplify_meters {
                                                            simplified.push(pt);
                                                            last_kept = pt;
                                                        }
                                                    }

                                                    simplified.push(
                                                        seg.coordinates[seg.coordinates.len() - 1],
                                                    );
                                                    seg.coordinates = simplified;
                                                }
                                            }
                                        }

                                        TrajectoryWrapper {
                                            source: "trajectory".to_string(),
                                            timestamp: now_ms,
                                            client_reference: client_reference.clone(),
                                            content: serde_json::to_value(traj)
                                                .unwrap_or(serde_json::Value::Null),
                                        }
                                    })
                                    .collect()
                            })
                            .await
                            .unwrap_or_else(|_| vec![])
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
            match tokio::time::timeout(std::time::Duration::from_secs(5), fetch_task).await {
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
