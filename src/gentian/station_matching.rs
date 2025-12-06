use anyhow::{Context, Result};
use catenary::models::{Station, Stop, StopMapping};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::stations::dsl::*;
use catenary::schema::gtfs::stop_mappings::dsl::*;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{StreamExt, stream, stream::FuturesUnordered};
use std::collections::VecDeque;

use postgis_diesel::types::Point;

use uuid::Uuid;

/// Matches a batch of GTFS stops to existing Stations or creates new ones.
pub async fn match_stops_to_stations(
    pool: &CatenaryPostgresPool,
    feed_id_str: &str,
    stops_to_process: &[Stop],
) -> Result<()> {
    println!(
        "Matching {} stops for feed {}",
        stops_to_process.len(),
        feed_id_str
    );

    let concurrency_limit = 16; // Adjust based on DB limits

    let mut results = Vec::new();
    let mut pending_stops: VecDeque<&Stop> = stops_to_process.iter().collect();
    let mut active_futures = FuturesUnordered::new();
    let mut active_coords: Vec<(f64, f64)> = Vec::new(); // (lat, lon)

    loop {
        // 1. Fill active_futures with non-conflicting stops
        let initial_pending_len = pending_stops.len();
        let mut checked_count = 0;

        while active_futures.len() < concurrency_limit && checked_count < initial_pending_len {
            if let Some(stop) = pending_stops.pop_front() {
                let mut conflict = false;
                if let Some(pt) = &stop.point {
                    for &(ay, ax) in &active_coords {
                        // Check if closer than 1km (1000m)
                        if haversine_distance(pt.y, pt.x, ay, ax) < 1000.0 {
                            conflict = true;
                            break;
                        }
                    }
                }

                if conflict {
                    // Defer this stop
                    pending_stops.push_back(stop);
                } else {
                    // Schedule it
                    if let Some(pt) = &stop.point {
                        active_coords.push((pt.y, pt.x));
                    }

                    let pool = pool.clone();
                    let feed_id_str = feed_id_str.to_string();
                    let stop_pt = stop.point.clone();

                    active_futures.push(async move {
                        let mut conn = pool.get().await.context("Failed to get DB connection")?;
                        let res = process_single_stop(&mut conn, &feed_id_str, stop).await;
                        Ok((res, stop_pt)) as Result<(Result<()>, Option<Point>)>
                    });
                }
            } else {
                break;
            }
            checked_count += 1;
        }

        // 2. Wait for at least one future to complete if we can't schedule more
        if active_futures.is_empty() {
            if pending_stops.is_empty() {
                break; // All done
            }
            // If we have pending stops but nothing active, it implies all pending stops conflict with... nothing?
            // This state should be unreachable if logic is correct, as an empty active set causes no conflicts.
            // However, to be safe against infinite loops in case of logic bugs:
            break;
        }

        if let Some(result) = active_futures.next().await {
            match result {
                Ok((res, pt_opt)) => {
                    results.push(res);
                    // Remove from active_coords
                    if let Some(pt) = pt_opt {
                        if let Some(idx) = active_coords
                            .iter()
                            .position(|&(y, x)| y == pt.y && x == pt.x)
                        {
                            active_coords.swap_remove(idx);
                        }
                    }
                }
                Err(e) => {
                    // This handles errors from the async block wrapper itself (unlikely with anyhow::Result wrapper)
                    results.push(Err(e));
                }
            }
        }
    }

    for res in results {
        res?;
    }

    Ok(())
}

async fn process_single_stop(
    conn: &mut AsyncPgConnection,
    feed_id_str: &str,
    stop: &Stop,
) -> Result<()> {
    let stop_pt = match &stop.point {
        Some(p) => p,
        None => return Ok(()), // Skip stops without location
    };

    let (search_radius, decay_constant) = get_station_match_params(stop);

    // Candidate Search: Find stations within search_radius using PostGIS
    // We cast to geography to measure distance in meters.
    let candidates: Vec<Station> = diesel::sql_query(
        "SELECT * FROM gtfs.stations 
         WHERE ST_DWithin(point::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)",
    )
    .bind::<diesel::sql_types::Float8, _>(stop_pt.x)
    .bind::<diesel::sql_types::Float8, _>(stop_pt.y)
    .bind::<diesel::sql_types::Float8, _>(search_radius)
    .load::<Station>(conn)
    .await
    .context("Failed to query candidate stations")?;

    // Scoring Logic
    let mut best_score = 0.0;
    let mut best_station_id = String::new();

    for candidate in candidates {
        let score = compute_match_score(stop, &candidate, decay_constant);
        if score > best_score {
            best_score = score;
            best_station_id = candidate.station_id.clone();
        }
    }

    let high_threshold = 0.8;

    if best_score >= high_threshold {
        // Match to existing
        create_mapping(
            conn,
            feed_id_str,
            &stop.gtfs_id,
            &best_station_id,
            best_score,
            "auto",
        )
        .await?;
    } else {
        // Create new station
        let new_station_id = Uuid::new_v4().to_string();
        let new_station = Station {
            station_id: new_station_id.clone(),
            name: stop.name.clone().unwrap_or("Unknown Station".to_string()),
            point: stop_pt.clone(),
            is_manual: false,
        };

        diesel::insert_into(stations)
            .values(&new_station)
            .execute(conn)
            .await?;

        create_mapping(
            conn,
            feed_id_str,
            &stop.gtfs_id,
            &new_station_id,
            1.0,
            "new",
        )
        .await?;
    }

    Ok(())
}

fn compute_match_score(stop: &Stop, station: &Station, decay_constant: f64) -> f64 {
    // 1. Name Similarity (Jaro-Winkler)
    let stop_name = stop.name.clone().unwrap_or_default().to_lowercase();
    let station_name = station.name.to_lowercase();

    let name_sim = strsim::jaro_winkler(&stop_name, &station_name);

    // 2. Distance Score
    // Calculate distance in meters
    let stop_pt = stop.point.as_ref().unwrap();
    let dist = haversine_distance(stop_pt.y, stop_pt.x, station.point.y, station.point.x);

    // Decay function: exp(-d / decay_constant)
    let dist_score = (-dist / decay_constant).exp();

    0.6 * name_sim + 0.4 * dist_score
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371000.0; // Earth radius in meters
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

async fn create_mapping(
    conn: &mut AsyncPgConnection,
    feed_id_val: &str,
    stop_id_val: &str,
    station_id_val: &str,
    score: f64,
    method: &str,
) -> Result<()> {
    let mapping = StopMapping {
        feed_id: feed_id_val.to_string(),
        stop_id: stop_id_val.to_string(),
        station_id: station_id_val.to_string(),
        match_score: score,
        match_method: method.to_string(),
        active: true,
    };

    diesel::insert_into(stop_mappings)
        .values(&mapping)
        .on_conflict((
            catenary::schema::gtfs::stop_mappings::feed_id,
            catenary::schema::gtfs::stop_mappings::stop_id,
        ))
        .do_update()
        .set((
            catenary::schema::gtfs::stop_mappings::station_id.eq(station_id_val),
            catenary::schema::gtfs::stop_mappings::match_score.eq(score),
            catenary::schema::gtfs::stop_mappings::match_method.eq(method),
            catenary::schema::gtfs::stop_mappings::active.eq(true),
        ))
        .execute(conn)
        .await?;

    Ok(())
}

fn get_station_match_params(stop: &Stop) -> (f64, f64) {
    // Determine the effective route type
    // If primary_route_type is missing, try to infer from route_types list
    let route_type = stop
        .primary_route_type
        .or_else(|| stop.route_types.get(0).and_then(|rt| *rt))
        .unwrap_or(3); // Default to Bus (3) if unknown

    match route_type {
        // Bus (3), Trolleybus (11)
        3 | 11 => (40.0, 10.0),
        // Tram (0), Subway (1), Rail (2), Monorail (12)
        0 | 1 | 2 | 12 => (400.0, 100.0),
        // Default for others (Ferry, etc.)
        _ => (100.0, 30.0),
    }
}
