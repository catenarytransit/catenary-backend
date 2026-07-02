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
                        let now = chrono::Utc::now();
                        let t_start_str = (now - chrono::Duration::minutes(15))
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                        let t_end_str = (now + chrono::Duration::minutes(25))
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                        trajectories
                            .into_par_iter()
                            .filter(|traj| {
                                let mut keep = false;
                                if traj.segments.is_empty() {
                                    for stop in &traj.stops {
                                        let stop_time = if stop.arrival.is_empty() {
                                            &stop.departure
                                        } else {
                                            &stop.arrival
                                        };
                                        let is_active = stop_time.is_empty()
                                            || (stop_time.as_str() >= t_start_str.as_str()
                                                && stop_time.as_str() <= t_end_str.as_str());
                                        if is_active {
                                            if stop.lon >= min_lon
                                                && stop.lon <= max_lon
                                                && stop.lat >= min_lat
                                                && stop.lat <= max_lat
                                            {
                                                keep = true;
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    for seg in &traj.segments {
                                        if seg.from_stop_index >= traj.stops.len()
                                            || seg.to_stop_index >= traj.stops.len()
                                        {
                                            continue;
                                        }
                                        let from_stop = &traj.stops[seg.from_stop_index];
                                        let to_stop = &traj.stops[seg.to_stop_index];

                                        let seg_departure = if from_stop.departure.is_empty() {
                                            &from_stop.arrival
                                        } else {
                                            &from_stop.departure
                                        };
                                        let seg_arrival = if to_stop.arrival.is_empty() {
                                            &to_stop.departure
                                        } else {
                                            &to_stop.arrival
                                        };

                                        let is_active =
                                            if seg_departure.is_empty() || seg_arrival.is_empty() {
                                                true
                                            } else {
                                                seg_departure.as_str() <= t_end_str.as_str()
                                                    && seg_arrival.as_str() >= t_start_str.as_str()
                                            };

                                        if is_active {
                                            let mut seg_min_lon = f64::MAX;
                                            let mut seg_min_lat = f64::MAX;
                                            let mut seg_max_lon = f64::MIN;
                                            let mut seg_max_lat = f64::MIN;
                                            for coord in &seg.coordinates {
                                                seg_min_lon = seg_min_lon.min(coord[0]);
                                                seg_min_lat = seg_min_lat.min(coord[1]);
                                                seg_max_lon = seg_max_lon.max(coord[0]);
                                                seg_max_lat = seg_max_lat.max(coord[1]);
                                            }
                                            if seg_min_lon <= max_lon
                                                && seg_max_lon >= min_lon
                                                && seg_min_lat <= max_lat
                                                && seg_max_lat >= min_lat
                                            {
                                                keep = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                keep
                            })
                            .map(|mut traj| {
                                for seg in &mut traj.segments {
                                    seg.coordinates.retain(|pt| !(pt[0] == 0.0 && pt[1] == 0.0));
                                }

                                if simplify_meters > 0.0 {
                                    let simplify_meters_sq = simplify_meters * simplify_meters;
                                    let earth_radius_m = 6371000.0_f64;
                                    let rad_per_deg = std::f64::consts::PI / 180.0;

                                    for seg in &mut traj.segments {
                                        if seg.coordinates.len() > 2 {
                                            let mut simplified =
                                                Vec::with_capacity(seg.coordinates.len());
                                            simplified.push(seg.coordinates[0]);
                                            let mut last_kept = seg.coordinates[0];
                                            let cos_factor = (last_kept[1] * rad_per_deg).cos();

                                            for i in 1..seg.coordinates.len() - 1 {
                                                let pt = seg.coordinates[i];
                                                let d_lat = (pt[1] - last_kept[1]) * rad_per_deg;
                                                let d_lon = (pt[0] - last_kept[0]) * rad_per_deg;
                                                let x = d_lon * cos_factor;
                                                let dist_sq = earth_radius_m * earth_radius_m * (x * x + d_lat * d_lat);

                                                if dist_sq >= simplify_meters_sq {
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
                                    content: traj,
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
                                let now = chrono::Utc::now();
                                let t_start_str = (now - chrono::Duration::minutes(15))
                                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                                let t_end_str = (now + chrono::Duration::minutes(25))
                                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                                trajectories
                                    .into_par_iter()
                                    .filter(|traj| {
                                        let mut keep = false;
                                        if traj.segments.is_empty() {
                                            for stop in &traj.stops {
                                                let stop_time = if stop.arrival.is_empty() {
                                                    &stop.departure
                                                } else {
                                                    &stop.arrival
                                                };
                                                let is_active = stop_time.is_empty()
                                                    || (stop_time.as_str() >= t_start_str.as_str()
                                                        && stop_time.as_str()
                                                            <= t_end_str.as_str());
                                                if is_active {
                                                    if stop.lon >= min_lon
                                                        && stop.lon <= max_lon
                                                        && stop.lat >= min_lat
                                                        && stop.lat <= max_lat
                                                    {
                                                        keep = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
                                            for seg in &traj.segments {
                                                if seg.from_stop_index >= traj.stops.len()
                                                    || seg.to_stop_index >= traj.stops.len()
                                                {
                                                    continue;
                                                }
                                                let from_stop = &traj.stops[seg.from_stop_index];
                                                let to_stop = &traj.stops[seg.to_stop_index];

                                                let seg_departure =
                                                    if from_stop.departure.is_empty() {
                                                        &from_stop.arrival
                                                    } else {
                                                        &from_stop.departure
                                                    };
                                                let seg_arrival = if to_stop.arrival.is_empty() {
                                                    &to_stop.departure
                                                } else {
                                                    &to_stop.arrival
                                                };

                                                let is_active = if seg_departure.is_empty()
                                                    || seg_arrival.is_empty()
                                                {
                                                    true
                                                } else {
                                                    seg_departure.as_str() <= t_end_str.as_str()
                                                        && seg_arrival.as_str()
                                                            >= t_start_str.as_str()
                                                };

                                                if is_active {
                                                    let mut seg_min_lon = f64::MAX;
                                                    let mut seg_min_lat = f64::MAX;
                                                    let mut seg_max_lon = f64::MIN;
                                                    let mut seg_max_lat = f64::MIN;
                                                    for coord in &seg.coordinates {
                                                        seg_min_lon = seg_min_lon.min(coord[0]);
                                                        seg_min_lat = seg_min_lat.min(coord[1]);
                                                        seg_max_lon = seg_max_lon.max(coord[0]);
                                                        seg_max_lat = seg_max_lat.max(coord[1]);
                                                    }
                                                    if seg_min_lon <= max_lon
                                                        && seg_max_lon >= min_lon
                                                        && seg_min_lat <= max_lat
                                                        && seg_max_lat >= min_lat
                                                    {
                                                        keep = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        keep
                                    })
                                    .map(|mut traj| {
                                        for seg in &mut traj.segments {
                                            seg.coordinates
                                                .retain(|pt| !(pt[0] == 0.0 && pt[1] == 0.0));
                                        }

                                        if simplify_meters > 0.0 {
                                            let simplify_meters_sq = simplify_meters * simplify_meters;
                                            let earth_radius_m = 6371000.0_f64;
                                            let rad_per_deg = std::f64::consts::PI / 180.0;

                                            for seg in &mut traj.segments {
                                                if seg.coordinates.len() > 2 {
                                                    let mut simplified =
                                                        Vec::with_capacity(seg.coordinates.len());
                                                    simplified.push(seg.coordinates[0]);
                                                    let mut last_kept = seg.coordinates[0];
                                                    let cos_factor = (last_kept[1] * rad_per_deg).cos();

                                                    for i in 1..seg.coordinates.len() - 1 {
                                                        let pt = seg.coordinates[i];
                                                        let d_lat = (pt[1] - last_kept[1]) * rad_per_deg;
                                                        let d_lon = (pt[0] - last_kept[0]) * rad_per_deg;
                                                        let x = d_lon * cos_factor;
                                                        let dist_sq = earth_radius_m * earth_radius_m * (x * x + d_lat * d_lat);
                                                        
                                                        if dist_sq >= simplify_meters_sq {
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

                                        for seg in &mut traj.segments {
                                            for pt in &mut seg.coordinates {
                                                pt[0] = (pt[0] * 1_000_000.0).round() / 1_000_000.0;
                                                pt[1] = (pt[1] * 1_000_000.0).round() / 1_000_000.0;
                                            }
                                        }

                                        TrajectoryWrapper {
                                            source: "trajectory".to_string(),
                                            timestamp: now_ms,
                                            client_reference: client_reference.clone(),
                                            content: traj,
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
