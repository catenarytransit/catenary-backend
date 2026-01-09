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

/// Check if an OSM station is a primary station (railway=station or railway=halt)
/// These should be preferred over stop_position/platform for the primary match
fn is_primary_station(station: &OsmStation) -> bool {
    match station.station_type.as_deref() {
        Some("station") | Some("halt") => true,
        _ => {
            // Also check railway_tag directly
            match station.railway_tag.as_deref() {
                Some("station") | Some("halt") => true,
                _ => false,
            }
        }
    }
}

/// Match a single GTFS stop to the best OSM station
/// Returns (osm_station_id, osm_platform_id) if found with score >= MIN_MATCH_SCORE
/// 
/// Matching strategy:
/// 1. First, try to find a matching primary station (railway=station or railway=halt)
/// 2. If found, also look for a matching platform (via local_ref/platform_code)
/// 3. Only fall back to stop_position/platform as primary if no station match exists
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
    
    let stop_name_lower = stop_name.to_lowercase();
    
    // Separate candidates into stations and platforms
    let (stations, platforms): (Vec<_>, Vec<_>) = candidates
        .iter()
        .partition(|s| is_primary_station(s));
    
    /// Score a candidate and return (osm_id, parent_id, score, is_platform_match)
    fn score_candidate(
        station: &OsmStation,
        stop_lat: f64,
        stop_lon: f64,
        stop_name_lower: &str,
        platform_code: Option<&str>,
        radius: f64,
    ) -> (i64, Option<i64>, f64, bool) {
        // Compute name similarity using Jaro-Winkler
        let name_sim = station
            .name
            .as_ref()
            .map(|n| jaro_winkler(stop_name_lower, &n.to_lowercase()))
            .unwrap_or(0.0);
        
        // Compute distance using geo crate
        let p1 = geo::point!(x: stop_lon, y: stop_lat);
        let p2 = geo::point!(x: station.point.x, y: station.point.y);
        let distance = p1.haversine_distance(&p2);
        
        // Compute proximity score (1.0 at center, 0.0 at radius edge)
        let proximity_score = 1.0 - (distance / radius).min(1.0);
        
        // Check for platform match via local_ref
        let platform_match = platform_code
            .and_then(|pc| station.local_ref.as_ref().map(|lr| lr == pc))
            .unwrap_or(false);
        
        // Combined score: 70% name similarity, 30% proximity
        let score = (name_sim * 0.7) + (proximity_score * 0.3);
        
        let parent_id = station.parent_osm_id;
        
        (station.osm_id, parent_id, score, platform_match)
    }
    
    // Step 1: Try to find the best matching primary station
    let mut best_station: Option<(i64, f64)> = None;
    
    for station in &stations {
        let (osm_id, _, score, _) = score_candidate(
            station, stop_lat, stop_lon, &stop_name_lower, platform_code, radius
        );
        
        if score >= MIN_MATCH_SCORE {
            if best_station.as_ref().map_or(true, |(_, s)| score > *s) {
                best_station = Some((osm_id, score));
            }
        }
    }
    
    // Step 2: If we found a station match, look for matching platform
    if let Some((station_id, _)) = best_station {
        // Look for a platform that matches by local_ref/platform_code
        let mut matching_platform: Option<i64> = None;
        
        if platform_code.is_some() {
            for platform in &platforms {
                let (osm_id, parent_id, score, is_platform_match) = score_candidate(
                    platform, stop_lat, stop_lon, &stop_name_lower, platform_code, radius
                );
                
                // Platform must have matching local_ref and be related to our station
                // (either directly via parent_osm_id or by proximity/name match)
                if is_platform_match && score >= MIN_MATCH_SCORE * 0.8 {
                    // Check if this platform belongs to the matched station
                    if parent_id == Some(station_id) {
                        matching_platform = Some(osm_id);
                        break;
                    }
                }
            }
        }
        
        return Some((station_id, matching_platform));
    }
    
    // Step 3: Fallback - no primary station found, try platforms/stop_positions
    // This handles cases where the OSM data only has platforms, not parent stations
    let mut best_platform: Option<(i64, Option<i64>, f64, bool)> = None;
    
    for platform in &platforms {
        let (osm_id, parent_id, score, is_platform_match) = score_candidate(
            platform, stop_lat, stop_lon, &stop_name_lower, platform_code, radius
        );
        
        // Bonus for platform code match in fallback mode
        let adjusted_score = if is_platform_match { score + 0.1 } else { score };
        
        if adjusted_score >= MIN_MATCH_SCORE {
            if best_platform.as_ref().map_or(true, |(_, _, s, _)| adjusted_score > *s) {
                best_platform = Some((osm_id, parent_id, adjusted_score, is_platform_match));
            }
        }
    }
    
    // If we matched a platform, use its parent if available, otherwise use the platform itself
    best_platform.map(|(osm_id, parent_id, _, is_platform_match)| {
        let station_id = parent_id.unwrap_or(osm_id);
        let platform_id = if is_platform_match { Some(osm_id) } else { None };
        (station_id, platform_id)
    })
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
