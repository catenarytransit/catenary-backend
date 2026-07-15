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
    "vbb",
    "busÉireann",
    "nederland",
    "rejseplanen~dk~gtfs",
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

use std::io::Write;

fn format_coordinate_precise(val: f64, zoom: u8) -> f64 {
    let decimals = if zoom < 9 {
        4
    } else if zoom < 13 {
        5
    } else {
        6
    };

    let factor = match decimals {
        4 => 10_000.0,
        5 => 100_000.0,
        _ => 1_000_000.0,
    };

    // Extract the exact rounded decimal coefficient as an integer
    let rounded = (val * factor).round() as i64;

    // Construct a stack scratchpad for the scientific string representation
    let mut buf = [0u8; 24];

    // Format the integer using the ultra-fast `itoa` crate (bypasses core::fmt entirely)
    let mut itoa_buf = itoa::Buffer::new();
    let int_bytes = itoa_buf.format(rounded).as_bytes();

    let len = int_bytes.len();
    buf[..len].copy_from_slice(int_bytes);

    // Append the scientific exponent to scale the value back down precisely
    let exp_bytes = match decimals {
        4 => b"e-4",
        5 => b"e-5",
        _ => b"e-6",
    };
    buf[len..len + 3].copy_from_slice(exp_bytes);
    let total_len = len + 3;

    // Parse via fast_float, which implements the Eisel-Lemire algorithm.
    // This uses 128-bit integer multiplications against exact powers of 10,
    // providing identical bitwise precision to a standard decimal parser.
    fast_float::parse(&buf[..total_len]).unwrap_or(val)
}

fn perpendicular_distance_sq(pt: [f64; 2], line_start: [f64; 2], line_end: [f64; 2]) -> f64 {
    let dx = line_end[0] - line_start[0];
    let dy = line_end[1] - line_start[1];

    let length_sq = dx * dx + dy * dy;
    if length_sq == 0.0 {
        let dx = pt[0] - line_start[0];
        let dy = pt[1] - line_start[1];
        return dx * dx + dy * dy;
    }

    let t = ((pt[0] - line_start[0]) * dx + (pt[1] - line_start[1]) * dy) / length_sq;
    let t = t.clamp(0.0, 1.0);

    let proj_x = line_start[0] + t * dx;
    let proj_y = line_start[1] + t * dy;

    let dx = pt[0] - proj_x;
    let dy = pt[1] - proj_y;

    dx * dx + dy * dy
}

fn rdp_indices(projected: &[[f64; 2]], epsilon_sq: f64) -> Vec<usize> {
    if projected.len() <= 2 {
        return (0..projected.len()).collect();
    }

    let mut stack = vec![(0, projected.len() - 1)];
    let mut keep = vec![false; projected.len()];
    keep[0] = true;
    keep[projected.len() - 1] = true;

    while let Some((start, end)) = stack.pop() {
        let mut dmax_sq = 0.0;
        let mut index = start;

        let pt_start = projected[start];
        let pt_end = projected[end];

        for i in start + 1..end {
            let d_sq = perpendicular_distance_sq(projected[i], pt_start, pt_end);
            if d_sq > dmax_sq {
                index = i;
                dmax_sq = d_sq;
            }
        }

        if dmax_sq > epsilon_sq {
            keep[index] = true;
            stack.push((start, index));
            stack.push((index, end));
        }
    }

    keep.into_iter()
        .enumerate()
        .filter_map(|(i, k)| if k { Some(i) } else { None })
        .collect()
}

pub async fn get_single_chateau_trajectories(
    _pool: Arc<CatenaryPostgresPool>,
    aspen_chateau_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>,
    >,
    aspen_client_manager: Arc<AspenClientManager>,
    mut params: TrajectorySubscriptionParams,
    chateau: String,
) -> Result<Vec<TrajectoryWrapper>, String> {
    if !ALLOWED_CHATEAUX.contains(&chateau.as_str()) {
        return Ok(vec![]);
    }

    if params.zoom < 9 {
        params.modes.retain(|m| m != "bus");
    }

    if params.modes.is_empty() {
        return Ok(vec![]);
    }

    let mut retries = 0;
    loop {
        let chateau_clone = chateau.clone();
        let aspen_chateau_cache = aspen_chateau_cache.clone();
        let aspen_client_manager = aspen_client_manager.clone();
        let params = params.clone();

        let fetch_task = async move {
            let socket = aspen_chateau_cache
                .get(&chateau_clone)
                .ok_or_else(|| format!("Could not get socket for chateau {}", chateau_clone))?
                .socket;

            let client_res =
                if let Some(client) = aspen_client_manager.get_client(socket.clone()).await {
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
                let zoom = params.zoom;
                let client_reference = params.client_reference.clone();
                let min_lon = params.bbox.get(0).copied().unwrap_or(0.0);
                let min_lat = params.bbox.get(1).copied().unwrap_or(0.0);
                let max_lon = params.bbox.get(2).copied().unwrap_or(0.0);
                let max_lat = params.bbox.get(3).copied().unwrap_or(0.0);

                let lat_diff_km = (max_lat - min_lat).abs() * 111.32;
                let avg_lat = (min_lat + max_lat) / 2.0;
                let lon_diff_km =
                    (max_lon - min_lon).abs() * 111.32 * avg_lat.to_radians().cos().abs();
                let min_dim_km = lat_diff_km.min(lon_diff_km);

                let simplify_meters = if min_dim_km > 300.0 {
                    1500.0
                } else if min_dim_km > 200.0 {
                    1000.0
                } else if min_dim_km > 100.0 {
                    500.0
                } else if min_dim_km > 50.0 {
                    250.0
                } else if min_dim_km > 20.0 {
                    150.0
                } else if min_dim_km > 10.0 {
                    100.0
                } else if min_dim_km > 5.0 {
                    20.0
                } else {
                    5.0
                };

                let call_start = std::time::Instant::now();
                let call_res = client
                    .get_trajectories(tarpc::context::current(), chateau_clone.clone(), params)
                    .await;
                let call_elapsed = call_start.elapsed();
                println!(
                    "Aspen get_trajectories call for chateau {} took {:?}",
                    chateau_clone, call_elapsed
                );

                match call_res {
                    Ok(Ok(trajectories)) => {
                        let now_ms = catenary::duration_since_unix_epoch().as_millis() as u64;
                        let res = tokio::task::spawn_blocking(move || {
                            let now = chrono::Utc::now();
                            let t_start_str = (now - chrono::Duration::minutes(2))
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                            let t_end_str = (now + chrono::Duration::minutes(3))
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                            let mut filtered: Vec<_> = trajectories
                                .into_par_iter()
                                .filter_map(|mut traj| {
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
                                        traj.segments.retain(|seg| {
                                            if seg.from_stop_index >= traj.stops.len()
                                                || seg.to_stop_index >= traj.stops.len()
                                            {
                                                return false;
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

                                            let is_active = if seg_departure.is_empty()
                                                || seg_arrival.is_empty()
                                            {
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
                                                }
                                                true
                                            } else {
                                                false
                                            }
                                        });
                                    }

                                    if keep {
                                        for seg in &mut traj.segments {
                                            seg.coordinates
                                                .retain(|pt| !(pt[0] == 0.0 && pt[1] == 0.0));
                                        }
                                        Some(traj)
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            let simplify_start = std::time::Instant::now();
                            if simplify_meters > 0.0 {
                                // Adjust simplify_meters for Web Mercator scale distortion at avg_lat
                                let web_mercator_scale = (1.0 / avg_lat.to_radians().cos()).abs();
                                let wm_simplify_meters = simplify_meters * web_mercator_scale;
                                let simplify_meters_sq = wm_simplify_meters * wm_simplify_meters;

                                filtered.par_iter_mut().for_each(|traj| {
                                    for seg in &mut traj.segments {
                                        if seg.coordinates.len() > 2 {
                                            let projected: Vec<[f64; 2]> = seg
                                                .coordinates
                                                .iter()
                                                .map(|pt| {
                                                    let r = 6378137.0;
                                                    let x = pt[0].to_radians() * r;
                                                    let y = ((std::f64::consts::PI / 4.0)
                                                        + (pt[1].to_radians() / 2.0))
                                                        .tan()
                                                        .ln()
                                                        * r;
                                                    [x, y]
                                                })
                                                .collect();

                                            let indices =
                                                rdp_indices(&projected, simplify_meters_sq);
                                            let mut simplified = Vec::with_capacity(indices.len());
                                            for idx in indices {
                                                simplified.push(seg.coordinates[idx]);
                                            }
                                            seg.coordinates = simplified;
                                        }
                                    }
                                });
                            }
                            let simplify_elapsed = simplify_start.elapsed();
                            println!(
                                "Shape simplification for chateau {} took {:?}",
                                chateau_clone, simplify_elapsed
                            );

                            let res: Vec<_> = filtered
                                .into_par_iter()
                                .map(|mut traj| {
                                    for seg in &mut traj.segments {
                                        for pt in &mut seg.coordinates {
                                            pt[0] = format_coordinate_precise(pt[0], zoom);
                                            pt[1] = format_coordinate_precise(pt[1], zoom);
                                        }
                                    }

                                    TrajectoryWrapper {
                                        source: "trajectory".to_string(),
                                        timestamp: now_ms,
                                        client_reference: client_reference.clone(),
                                        content: traj,
                                    }
                                })
                                .collect();
                            res
                        })
                        .await
                        .unwrap_or_else(|_| vec![]);
                        Ok(res)
                    }
                    Ok(Err(e)) => {
                        eprintln!("Aspen RPC logic error for {}: {:?}", chateau_clone, e);
                        Err(format!("Logic error: {:?}", e))
                    }
                    Err(e) => {
                        eprintln!("Aspen RPC transport error for {}: {:?}", chateau_clone, e);
                        Err(format!("Transport error: {:?}", e))
                    }
                }
            } else {
                Err(format!("Failed to connect to Aspen for {}", chateau_clone))
            }
        };

        match tokio::time::timeout(std::time::Duration::from_secs(15), fetch_task).await {
            Ok(Ok(res)) => return Ok(res),
            Ok(Err(e)) => {
                eprintln!("Error fetching trajectories for chateau {}: {}", chateau, e);
            }
            Err(_) => {
                eprintln!("Timeout fetching trajectories for chateau {}", chateau);
            }
        }

        retries += 1;
        if retries >= 3 {
            return Err(format!(
                "Failed to fetch trajectories for {} after 3 retries",
                chateau
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}

pub async fn get_trajectories(
    _pool: Arc<CatenaryPostgresPool>,
    aspen_chateau_cache: std::sync::Arc<
        catenary::etcd_cache::EtcdCache<catenary::aspen::lib::ChateauMetadataEtcd>,
    >,
    aspen_client_manager: Arc<AspenClientManager>,
    mut params: TrajectorySubscriptionParams,
    chateaus: std::collections::HashSet<String>,
) -> Result<Vec<TrajectoryWrapper>, String> {
    if params.zoom < 9 {
        params.modes.retain(|m| m != "bus");
    }

    if params.modes.is_empty() {
        return Ok(vec![]);
    }

    let zoom = params.zoom;
    let mut futures = Vec::new();

    for ch in chateaus {
        if !ALLOWED_CHATEAUX.contains(&ch.as_str()) {
            continue;
        }
        let aspen_chateau_cache = aspen_chateau_cache.clone();
        let manager = aspen_client_manager.clone();
        let params_clone = params.clone();
        let ch_clone = ch.clone();

        futures.push(tokio::spawn(async move {
            let mut retries = 0;
            loop {
                let ch_clone2 = ch_clone.clone();
                let aspen_chateau_cache = aspen_chateau_cache.clone();
                let manager = manager.clone();
                let params_clone = params_clone.clone();

                let fetch_task = async move {
                    let socket = aspen_chateau_cache
                        .get(&ch_clone2)
                        .ok_or_else(|| format!("Could not get socket for chateau {}", ch_clone2))?
                        .socket;

                    let client_res = if let Some(client) = manager.get_client(socket.clone()).await
                    {
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

                        let call_start = std::time::Instant::now();
                        let call_res = client
                            .get_trajectories(
                                tarpc::context::current(),
                                ch_clone2.clone(),
                                params_clone,
                            )
                            .await;
                        let call_elapsed = call_start.elapsed();
                        println!(
                            "Aspen get_trajectories call for chateau {} took {:?}",
                            ch_clone2, call_elapsed
                        );

                        match call_res {
                            Ok(Ok(trajectories)) => {
                                let now_ms =
                                    catenary::duration_since_unix_epoch().as_millis() as u64;
                                let res = tokio::task::spawn_blocking(move || {
                                    let now = chrono::Utc::now();
                                    let t_start_str = (now - chrono::Duration::minutes(2))
                                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                                    let t_end_str = (now + chrono::Duration::minutes(3))
                                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                                    let mut filtered: Vec<_> = trajectories
                                        .into_par_iter()
                                        .filter_map(|mut traj| {
                                            let mut keep = false;
                                            if traj.segments.is_empty() {
                                                for stop in &traj.stops {
                                                    let stop_time = if stop.arrival.is_empty() {
                                                        &stop.departure
                                                    } else {
                                                        &stop.arrival
                                                    };
                                                    let is_active = stop_time.is_empty()
                                                        || (stop_time.as_str()
                                                            >= t_start_str.as_str()
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
                                                traj.segments.retain(|seg| {
                                                    if seg.from_stop_index >= traj.stops.len()
                                                        || seg.to_stop_index >= traj.stops.len()
                                                    {
                                                        return false;
                                                    }
                                                    let from_stop =
                                                        &traj.stops[seg.from_stop_index];
                                                    let to_stop = &traj.stops[seg.to_stop_index];

                                                    let seg_departure =
                                                        if from_stop.departure.is_empty() {
                                                            &from_stop.arrival
                                                        } else {
                                                            &from_stop.departure
                                                        };
                                                    let seg_arrival = if to_stop.arrival.is_empty()
                                                    {
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
                                                        }
                                                        true
                                                    } else {
                                                        false
                                                    }
                                                });
                                            }

                                            if keep {
                                                for seg in &mut traj.segments {
                                                    seg.coordinates.retain(|pt| {
                                                        !(pt[0] == 0.0 && pt[1] == 0.0)
                                                    });
                                                }
                                                Some(traj)
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();

                                    let simplify_start = std::time::Instant::now();
                                    if simplify_meters > 0.0 {
                                        // Adjust simplify_meters for Web Mercator scale distortion at avg_lat
                                        let web_mercator_scale =
                                            (1.0 / avg_lat.to_radians().cos()).abs();
                                        let wm_simplify_meters =
                                            simplify_meters * web_mercator_scale;
                                        let simplify_meters_sq =
                                            wm_simplify_meters * wm_simplify_meters;

                                        filtered.par_iter_mut().for_each(|traj| {
                                            for seg in &mut traj.segments {
                                                if seg.coordinates.len() > 2 {
                                                    let projected: Vec<[f64; 2]> = seg
                                                        .coordinates
                                                        .iter()
                                                        .map(|pt| {
                                                            let r = 6378137.0;
                                                            let x = pt[0].to_radians() * r;
                                                            let y = ((std::f64::consts::PI / 4.0)
                                                                + (pt[1].to_radians() / 2.0))
                                                                .tan()
                                                                .ln()
                                                                * r;
                                                            [x, y]
                                                        })
                                                        .collect();

                                                    let indices =
                                                        rdp_indices(&projected, simplify_meters_sq);
                                                    let mut simplified =
                                                        Vec::with_capacity(indices.len());
                                                    for idx in indices {
                                                        simplified.push(seg.coordinates[idx]);
                                                    }
                                                    seg.coordinates = simplified;
                                                }
                                            }
                                        });
                                    }
                                    let simplify_elapsed = simplify_start.elapsed();
                                    println!(
                                        "Shape simplification for chateau {} took {:?}",
                                        ch_clone2, simplify_elapsed
                                    );

                                    let res: Vec<_> = filtered
                                        .into_par_iter()
                                        .map(|mut traj| {
                                            for seg in &mut traj.segments {
                                                for pt in &mut seg.coordinates {
                                                    pt[0] = format_coordinate_precise(pt[0], zoom);
                                                    pt[1] = format_coordinate_precise(pt[1], zoom);
                                                }
                                            }

                                            TrajectoryWrapper {
                                                source: "trajectory".to_string(),
                                                timestamp: now_ms,
                                                client_reference: client_reference.clone(),
                                                content: traj,
                                            }
                                        })
                                        .collect();
                                    res
                                })
                                .await
                                .unwrap_or_else(|_| vec![]);
                                Ok(res)
                            }
                            Ok(Err(e)) => {
                                eprintln!("Aspen RPC logic error for {}: {:?}", ch_clone2, e);
                                Err(format!("Logic error: {:?}", e))
                            }
                            Err(e) => {
                                eprintln!("Aspen RPC transport error for {}: {:?}", ch_clone2, e);
                                Err(format!("Transport error: {:?}", e))
                            }
                        }
                    } else {
                        Err(format!("Failed to connect to Aspen for {}", ch_clone2))
                    }
                };

                match tokio::time::timeout(std::time::Duration::from_secs(5), fetch_task).await {
                    Ok(Ok(res)) => return Ok(res),
                    Ok(Err(e)) => {
                        eprintln!(
                            "Error fetching trajectories for chateau {}: {}",
                            ch_clone, e
                        );
                    }
                    Err(_) => {
                        eprintln!("Timeout fetching trajectories for chateau {}", ch_clone);
                    }
                }

                retries += 1;
                if retries >= 3 {
                    return Err(format!(
                        "Failed to fetch trajectories for {} after 3 retries",
                        ch_clone
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }));
    }

    let results = futures::future::join_all(futures).await;
    let mut merged = Vec::new();
    for res in results {
        if let Ok(Ok(trajectories)) = res {
            merged.extend(trajectories);
        }
    }

    Ok(merged)
}
