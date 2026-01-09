// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// OSM Station Matching Module - Matches GTFS stops to OSM stations during import

use catenary::models::OsmStation;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use geo::{point, HaversineDistance};
use strsim::jaro_winkler;
use std::error::Error;

/// Proximity thresholds by mode (in meters)
pub const RAIL_RADIUS_M: f64 = 1000.0;
pub const TRAM_RADIUS_M: f64 = 200.0;
pub const SUBWAY_RADIUS_M: f64 = 250.0;

/// Minimum score threshold for accepting a match
pub const MIN_MATCH_SCORE: f64 = 0.75;

/// Batch size for processing stops to avoid OOM
const STOP_BATCH_SIZE: i64 = 500;

/// Map GTFS route_type to mode string
pub fn route_type_to_mode(route_type: i16) -> Option<&'static str> {
    match route_type {
        0 => Some("tram"),      // Tram, Streetcar, Light rail
        1 => Some("subway"),    // Subway, Metro
        2 => Some("rail"),      // Rail (intercity, long-distance)
        _ => None,
    }
}

/// Get proximity threshold for a given mode
pub fn get_radius_for_mode(mode: &str) -> f64 {
    match mode {
        "rail" => RAIL_RADIUS_M,
        "tram" => TRAM_RADIUS_M,
        "subway" => SUBWAY_RADIUS_M,
        "light_rail" => TRAM_RADIUS_M,
        _ => RAIL_RADIUS_M, // Default to rail
    }
}

/// Haversine distance between two points in meters using geo crate
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let p1 = point!(x: lon1, y: lat1);
    let p2 = point!(x: lon2, y: lat2);
    p1.haversine_distance(&p2)
}

/// Find OSM stations within radius of a point (meters), filtered by mode
pub async fn find_osm_stations_near(
    conn: &mut AsyncPgConnection,
    lat: f64,
    lon: f64,
    radius_m: f64,
    mode_filter: Option<&str>,
) -> Result<Vec<OsmStation>, diesel::result::Error> {
    // Build the raw SQL query with spatial filter
    // Include new columns: level, local_ref, parent_osm_id
    let mode_clause = mode_filter
        .map(|m| format!("AND mode_type = '{}'", m))
        .unwrap_or_default();
    
    let query = format!(
        r#"
        SELECT osm_id, osm_type, import_id, point, name, name_translations,
               station_type, railway_tag, mode_type, uic_ref, ref AS ref_, 
               wikidata, operator, network, level, local_ref, parent_osm_id
        FROM gtfs.osm_stations
        WHERE ST_DWithin(point::geography, ST_MakePoint({}, {})::geography, {})
        {}
        "#,
        lon, lat, radius_m, mode_clause
    );
    
    diesel::sql_query(query)
        .load::<OsmStation>(conn)
        .await
}

/// Match a single GTFS stop to the best OSM station
/// Returns (osm_station_id, osm_platform_id) if found with score >= MIN_MATCH_SCORE
/// osm_station_id is the parent station, osm_platform_id is the platform if matched by local_ref
pub fn match_stop_to_osm_station_sync(
    candidates: &[OsmStation],
    stop_lat: f64,
    stop_lon: f64,
    stop_name: &str,
    platform_code: Option<&str>,
    radius: f64,
) -> Option<(i64, Option<i64>)> {
    if candidates.is_empty() {
        return None;
    }
    
    // Score each candidate
    let mut best_match: Option<(i64, Option<i64>, f64)> = None;
    let stop_name_lower = stop_name.to_lowercase();
    
    for station in candidates {
        // Compute name similarity using Jaro-Winkler
        let name_sim = station
            .name
            .as_ref()
            .map(|n| jaro_winkler(&stop_name_lower, &n.to_lowercase()))
            .unwrap_or(0.0);
        
        // Compute distance
        let distance = haversine_distance(stop_lat, stop_lon, station.point.y, station.point.x);
        
        // Compute proximity score (1.0 at center, 0.0 at radius edge)
        let proximity_score = 1.0 - (distance / radius).min(1.0);
        
        // Check for platform match via local_ref
        let platform_match = platform_code
            .and_then(|pc| {
                station.local_ref.as_ref().map(|lr| lr == pc)
            })
            .unwrap_or(false);
        
        // Bonus for platform match
        let platform_bonus = if platform_match { 0.15 } else { 0.0 };
        
        // Combined score: 70% name similarity, 30% proximity, bonus for platform
        let score = (name_sim * 0.7) + (proximity_score * 0.3) + platform_bonus;
        
        if score >= MIN_MATCH_SCORE {
            if best_match.as_ref().map_or(true, |(_, _, s)| score > *s) {
                // Determine parent station ID
                let parent_id = station.parent_osm_id.unwrap_or(station.osm_id);
                let platform_id = if station.local_ref.is_some() {
                    Some(station.osm_id)
                } else {
                    None
                };
                best_match = Some((parent_id, platform_id, score));
            }
        }
    }
    
    best_match.map(|(parent_id, platform_id, _)| (parent_id, platform_id))
}

/// Stop data for batch processing
struct StopBatch {
    gtfs_id: String,
    lat: f64,
    lon: f64,
    name: String,
    route_type: i16,
    platform_code: Option<String>,
}

/// Batch match stops for a feed, updating stops.osm_station_id directly
/// This is called during GTFS import in gtfs_process.rs
/// Processes stops in chunks to avoid OOM issues
pub async fn match_stops_for_feed(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
    chateau_id: &str,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::stops::dsl as stops_dsl;
    
    let mut matched_count: u64 = 0;
    let mut offset: i64 = 0;
    
    loop {
        // Fetch a batch of stops
        let stops: Vec<(String, Option<postgis_diesel::types::Point>, Option<String>, Vec<Option<i16>>, Option<String>)> = 
            stops_dsl::stops
                .filter(stops_dsl::onestop_feed_id.eq(feed_id))
                .filter(stops_dsl::attempt_id.eq(attempt_id))
                .select((
                    stops_dsl::gtfs_id,
                    stops_dsl::point,
                    stops_dsl::name,
                    stops_dsl::route_types,
                    stops_dsl::platform_code,
                ))
                .order(stops_dsl::gtfs_id)
                .limit(STOP_BATCH_SIZE)
                .offset(offset)
                .load(conn)
                .await?;
        
        if stops.is_empty() {
            break;
        }
        
        let batch_len = stops.len() as i64;
        
        // Pre-filter stops that need matching (rail/tram/subway only, with coordinates)
        let stops_to_match: Vec<StopBatch> = stops
            .into_iter()
            .filter_map(|(stop_id, point, name, route_types, platform_code)| {
                let point = point?;
                
                // Find rail/tram/subway route types for this stop
                let rail_types: Vec<i16> = route_types
                    .into_iter()
                    .filter_map(|rt| rt)
                    .filter(|&rt| rt == 0 || rt == 1 || rt == 2)
                    .collect();
                
                if rail_types.is_empty() {
                    return None;
                }
                
                // Use the most specific route type (prefer subway > tram > rail)
                let route_type = if rail_types.contains(&1) {
                    1 // subway
                } else if rail_types.contains(&0) {
                    0 // tram
                } else {
                    2 // rail
                };
                
                Some(StopBatch {
                    gtfs_id: stop_id,
                    lat: point.y,
                    lon: point.x,
                    name: name.unwrap_or_default(),
                    route_type,
                    platform_code,
                })
            })
            .collect();
        
        // Process each stop in this batch
        for stop in stops_to_match {
            let mode = match route_type_to_mode(stop.route_type) {
                Some(m) => m,
                None => continue,
            };
            
            let radius = get_radius_for_mode(mode);
            
            // Find candidate OSM stations within radius
            let candidates = match find_osm_stations_near(conn, stop.lat, stop.lon, radius, Some(mode)).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Error finding OSM stations for stop {}: {:?}", stop.gtfs_id, e);
                    continue;
                }
            };
            
            // Match stop to best OSM station
            if let Some((osm_station_id, osm_platform_id)) = match_stop_to_osm_station_sync(
                &candidates,
                stop.lat,
                stop.lon,
                &stop.name,
                stop.platform_code.as_deref(),
                radius,
            ) {
                // Update the stop with OSM station and platform IDs
                let update_result = diesel::update(
                    stops_dsl::stops
                        .filter(stops_dsl::onestop_feed_id.eq(feed_id))
                        .filter(stops_dsl::attempt_id.eq(attempt_id))
                        .filter(stops_dsl::gtfs_id.eq(&stop.gtfs_id))
                )
                .set((
                    stops_dsl::osm_station_id.eq(osm_station_id),
                    stops_dsl::osm_platform_id.eq(osm_platform_id),
                ))
                .execute(conn)
                .await;
                
                if update_result.is_ok() {
                    matched_count += 1;
                }
            }
        }
        
        offset += batch_len;
        
        // If we got fewer than the batch size, we're done
        if batch_len < STOP_BATCH_SIZE {
            break;
        }
    }
    
    if matched_count > 0 {
        println!("  OSM station matching: {} stops matched for {}", matched_count, chateau_id);
    }
    
    Ok(matched_count)
}
