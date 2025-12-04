use anyhow::{Context, Result};
use catenary::models::{Station, Stop, StopMapping};
use catenary::schema::gtfs::stations::dsl::*;
use catenary::schema::gtfs::stop_mappings::dsl::*;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use postgis_diesel::types::Point;

use uuid::Uuid;

/// Matches a batch of GTFS stops to existing Stations or creates new ones.
pub async fn match_stops_to_stations(
    conn: &mut AsyncPgConnection,
    feed_id_str: &str,
    stops_to_process: &[Stop],
) -> Result<()> {
    println!(
        "Matching {} stops for feed {}",
        stops_to_process.len(),
        feed_id_str
    );

    for stop in stops_to_process {
        process_single_stop(conn, feed_id_str, stop).await?;
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

    // Candidate Search: Find stations within 200m using PostGIS
    // We cast to geography to measure distance in meters.
    let candidates: Vec<Station> = diesel::sql_query(
        "SELECT * FROM gtfs.stations 
         WHERE ST_DWithin(point::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 200.0)"
    )
    .bind::<diesel::sql_types::Float8, _>(stop_pt.x)
    .bind::<diesel::sql_types::Float8, _>(stop_pt.y)
    .load::<Station>(conn)
    .await
    .context("Failed to query candidate stations")?;

    // Scoring Logic
    let mut best_score = 0.0;
    let mut best_station_id = String::new();

    for candidate in candidates {
        let score = compute_match_score(stop, &candidate);
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

fn compute_match_score(stop: &Stop, station: &Station) -> f64 {
    // 1. Name Similarity (Jaro-Winkler)
    let stop_name = stop.name.clone().unwrap_or_default().to_lowercase();
    let station_name = station.name.to_lowercase();

    let name_sim = strsim::jaro_winkler(&stop_name, &station_name);

    // 2. Distance Score
    // Calculate distance in meters
    let stop_pt = stop.point.as_ref().unwrap();
    let dist = haversine_distance(stop_pt.y, stop_pt.x, station.point.y, station.point.x);

    // Decay function: exp(-d / 30m)
    let dist_score = (-dist / 30.0).exp();

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
