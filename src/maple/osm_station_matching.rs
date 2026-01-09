// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// OSM Station Matching Module - Matches GTFS stops to OSM stations during import

use catenary::models::OsmStation;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use strsim::jaro_winkler;
use std::error::Error;

/// Proximity thresholds by mode (in meters)
pub const RAIL_RADIUS_M: f64 = 1000.0;
pub const TRAM_RADIUS_M: f64 = 200.0;
pub const SUBWAY_RADIUS_M: f64 = 250.0;

/// Minimum score threshold for accepting a match
pub const MIN_MATCH_SCORE: f64 = 0.75;

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
        _ => RAIL_RADIUS_M, // Default to rail
    }
}

/// Haversine distance between two points in meters
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();
    
    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    
    EARTH_RADIUS_M * c
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
    let mode_clause = mode_filter
        .map(|m| format!("AND mode_type = '{}'", m))
        .unwrap_or_default();
    
    let query = format!(
        r#"
        SELECT osm_id, osm_type, import_id, point, name, name_translations,
               station_type, railway_tag, mode_type, uic_ref, ref AS ref_, wikidata, operator, network
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
/// Returns the best matching osm_station_id if found with score >= MIN_MATCH_SCORE
pub async fn match_stop_to_osm_station(
    conn: &mut AsyncPgConnection,
    stop_lat: f64,
    stop_lon: f64,
    stop_name: &str,
    route_type: i16,
) -> Result<Option<i64>, Box<dyn Error + Send + Sync>> {
    let mode = match route_type_to_mode(route_type) {
        Some(m) => m,
        None => return Ok(None),
    };
    
    let radius = get_radius_for_mode(mode);
    
    // Find candidate OSM stations within radius, filtered by mode
    let candidates = find_osm_stations_near(conn, stop_lat, stop_lon, radius, Some(mode)).await?;
    
    if candidates.is_empty() {
        return Ok(None);
    }
    
    // Score each candidate
    let mut best_match: Option<(i64, f64)> = None;
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
        
        // Combined score: 70% name similarity, 30% proximity
        let score = (name_sim * 0.7) + (proximity_score * 0.3);
        
        if score >= MIN_MATCH_SCORE {
            if best_match.as_ref().map_or(true, |(_, s)| score > *s) {
                best_match = Some((station.osm_id, score));
            }
        }
    }
    
    Ok(best_match.map(|(id, _)| id))
}

/// Batch match stops for a feed, updating stops.osm_station_id directly
/// This is called during GTFS import in gtfs_process.rs
pub async fn match_stops_for_feed(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
    chateau_id: &str,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::stops::dsl as stops_dsl;
    
    // Get all stops for this feed that have rail/tram/subway routes
    let stops: Vec<(String, Option<postgis_diesel::types::Point>, Option<String>, Vec<Option<i16>>)> = 
        stops_dsl::stops
            .filter(stops_dsl::onestop_feed_id.eq(feed_id))
            .filter(stops_dsl::attempt_id.eq(attempt_id))
            .select((
                stops_dsl::gtfs_id,
                stops_dsl::point,
                stops_dsl::name,
                stops_dsl::route_types,
            ))
            .load(conn)
            .await?;
    
    let mut matched_count: u64 = 0;
    
    for (stop_id, point, name, route_types) in stops {
        // Skip stops without coordinates
        let point = match point {
            Some(p) => p,
            None => continue,
        };
        
        // Find rail/tram/subway route types for this stop
        let rail_types: Vec<i16> = route_types
            .into_iter()
            .filter_map(|rt| rt)
            .filter(|&rt| rt == 0 || rt == 1 || rt == 2)
            .collect();
        
        if rail_types.is_empty() {
            continue;
        }
        
        // Use the most specific route type (prefer subway > tram > rail)
        let route_type = if rail_types.contains(&1) {
            1 // subway
        } else if rail_types.contains(&0) {
            0 // tram
        } else {
            2 // rail
        };
        
        let stop_name = name.unwrap_or_default();
        
        // Try to match this stop
        match match_stop_to_osm_station(conn, point.y, point.x, &stop_name, route_type).await {
            Ok(Some(osm_station_id)) => {
                // Update the stop directly with the OSM station ID
                let update_result = diesel::update(
                    stops_dsl::stops
                        .filter(stops_dsl::onestop_feed_id.eq(feed_id))
                        .filter(stops_dsl::attempt_id.eq(attempt_id))
                        .filter(stops_dsl::gtfs_id.eq(&stop_id))
                )
                .set(stops_dsl::osm_station_id.eq(osm_station_id))
                .execute(conn)
                .await;
                
                if update_result.is_ok() {
                    matched_count += 1;
                }
            }
            Ok(None) => {
                // No match found - that's fine
            }
            Err(e) => {
                eprintln!("Error matching stop {} in {}: {:?}", stop_id, chateau_id, e);
            }
        }
    }
    
    if matched_count > 0 {
        println!("  OSM station matching: {} stops matched for {}", matched_count, chateau_id);
    }
    
    Ok(matched_count)
}
