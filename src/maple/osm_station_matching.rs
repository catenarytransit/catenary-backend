// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// OSM Station Matching Module - Matches GTFS stops to OSM stations during import
//
// PERFORMANCE OPTIMIZATIONS:
// - Geographic chunking: Groups stops into ~10km grid cells
// - R-tree spatial index: O(log n) lookups per chunk instead of O(n)
// - Batch DB queries: One query per chunk instead of per-stop
// - Batch updates: One UPDATE per chunk using unnest arrays
// - Rayon parallelization: Parallel scoring within each chunk

use catenary::models::OsmStation;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use geo::{HaversineDistance, point};
use rayon::prelude::*;
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use std::collections::HashMap;
use std::error::Error;
use strsim::jaro_winkler;

/// Proximity thresholds by mode (in meters)
pub const RAIL_RADIUS_M: f64 = 1000.0;
pub const TRAM_RADIUS_M: f64 = 200.0;
pub const SUBWAY_RADIUS_M: f64 = 250.0;

/// Minimum score threshold for accepting a match
/// Lowered to 0.4 to allow proximity-based matching when names differ
pub const MIN_MATCH_SCORE: f64 = 0.4;

/// Batch size for processing stops to avoid OOM
const STOP_BATCH_SIZE: i64 = 5000;

/// Grid cell size in degrees (~10km at mid-latitudes)
const GRID_CELL_SIZE: f64 = 0.1;

/// Maximum radius in degrees (for bounding box expansion)
/// Rail radius is 1000m = ~0.009° at equator, but longitude degrees shrink at higher latitudes
/// Use 0.02° (~2.2km) for safety margin to ensure we capture stations at cell edges
const MAX_RADIUS_DEG: f64 = 0.02;

/// Get modes that are compatible/related to the given mode
/// These modes may share stations in real-world scenarios
pub fn get_compatible_modes(mode: &str) -> Vec<&'static str> {
    match mode {
        // Rail stations often serve subway/metro in major cities
        "rail" => vec!["rail", "subway", "light_rail"],
        // Subway stations may also be classified as rail or light_rail
        "subway" => vec!["subway", "rail", "light_rail"],
        // Tram and light_rail are often interchangeable
        "tram" => vec!["tram", "light_rail", "subway"],
        // Light rail can be tram-like or subway-like
        "light_rail" => vec!["light_rail", "tram", "subway", "rail"],
        // Monorail is usually standalone
        "monorail" => vec!["monorail"],
        // Funicular is usually standalone
        "funicular" => vec!["funicular"],
        // Unknown modes - return empty, will still match via scoring
        _ => vec![],
    }
}

/// Check if two modes are compatible (could share a station)
pub fn modes_are_compatible(mode1: &str, mode2: &str) -> bool {
    if mode1 == mode2 {
        return true;
    }
    get_compatible_modes(mode1).contains(&mode2)
}

/// Get a priority score for mode matching (higher = better match)
/// 0 = no match, 1 = compatible, 2 = exact match
pub fn mode_match_priority(gtfs_mode: &str, osm_mode: &str) -> u8 {
    if gtfs_mode == osm_mode {
        2 // Exact match
    } else if modes_are_compatible(gtfs_mode, osm_mode) {
        1 // Compatible mode
    } else {
        0 // No match
    }
}

/// Map GTFS route_type to mode string
pub fn route_type_to_mode(route_type: i16) -> Option<&'static str> {
    match route_type {
        0 => Some("tram"),   // Tram, Streetcar, Light rail
        1 => Some("subway"), // Subway, Metro
        2 => Some("rail"),   // Rail (intercity, long-distance)
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

// ============================================================================
// R-Tree indexed station wrapper
// ============================================================================

/// Wrapper for OsmStation that implements RTreeObject for spatial indexing
#[derive(Clone)]
struct IndexedStation {
    osm_id: i64,
    x: f64, // lon
    y: f64, // lat
    name_lower: Option<String>,
    station_type: Option<String>,
    railway_tag: Option<String>,
    mode_type: String,
    local_ref: Option<String>,
    parent_osm_id: Option<i64>,
}

impl IndexedStation {
    fn from_osm_station(station: &OsmStation) -> Self {
        Self {
            osm_id: station.osm_id,
            x: station.point.x,
            y: station.point.y,
            name_lower: station.name.as_ref().map(|n| n.to_lowercase()),
            station_type: station.station_type.clone(),
            railway_tag: station.railway_tag.clone(),
            mode_type: station.mode_type.clone(),
            local_ref: station.local_ref.clone(),
            parent_osm_id: station.parent_osm_id,
        }
    }

    fn is_primary_station(&self) -> bool {
        match self.station_type.as_deref() {
            Some("station") | Some("halt") | Some("tram_stop") => true,
            _ => match self.railway_tag.as_deref() {
                Some("station") | Some("halt") | Some("tram_stop") => true,
                _ => false,
            },
        }
    }
}

impl RTreeObject for IndexedStation {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x, self.y])
    }
}

impl PointDistance for IndexedStation {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.x - point[0];
        let dy = self.y - point[1];
        dx * dx + dy * dy
    }
}

// ============================================================================
// Geographic chunking utilities
// ============================================================================

/// Grid cell key for geographic chunking
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
struct GridCell {
    lat_idx: i32,
    lon_idx: i32,
}

impl GridCell {
    fn from_coords(lat: f64, lon: f64) -> Self {
        Self {
            lat_idx: (lat / GRID_CELL_SIZE).floor() as i32,
            lon_idx: (lon / GRID_CELL_SIZE).floor() as i32,
        }
    }

    /// Get bounding box for this cell, expanded by a margin for overlap
    fn bbox_expanded(&self, margin_deg: f64) -> (f64, f64, f64, f64) {
        let min_lat = (self.lat_idx as f64) * GRID_CELL_SIZE - margin_deg;
        let max_lat = ((self.lat_idx + 1) as f64) * GRID_CELL_SIZE + margin_deg;
        let min_lon = (self.lon_idx as f64) * GRID_CELL_SIZE - margin_deg;
        let max_lon = ((self.lon_idx + 1) as f64) * GRID_CELL_SIZE + margin_deg;
        (min_lat, min_lon, max_lat, max_lon)
    }
}

// ============================================================================
// Database queries
// ============================================================================

/// Fetch OSM stations within a bounding box for specified modes
async fn fetch_osm_stations_in_bbox(
    conn: &mut AsyncPgConnection,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    modes: &[&str],
) -> Result<Vec<OsmStation>, diesel::result::Error> {
    if modes.is_empty() {
        return Ok(Vec::new());
    }

    let modes_list = modes
        .iter()
        .map(|m| format!("'{}'", m))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        r#"
        SELECT osm_id, osm_type, import_id, point, name, name_translations,
               station_type, railway_tag, mode_type, uic_ref, ref AS ref_, 
               wikidata, operator, network, level, local_ref, parent_osm_id, is_derivative
        FROM gtfs.osm_stations
        WHERE point && ST_MakeEnvelope({}, {}, {}, {}, 4326)
          AND mode_type IN ({})
        "#,
        min_lon, min_lat, max_lon, max_lat, modes_list
    );

    // Force usage of the spatial index by disabling seqscan for this query
    // This addresses the concern that Postgres might choose a sequential scan
    conn.transaction(|conn| {
        Box::pin(async move {
            diesel::sql_query("SET LOCAL enable_seqscan = OFF")
                .execute(conn)
                .await?;
            diesel::sql_query(query).load::<OsmStation>(conn).await
        })
    })
    .await
}

/// Batch update stops with OSM station and platform IDs using unnest
async fn batch_update_stops(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
    updates: &[(String, Option<i64>, Option<i64>)], // (gtfs_id, station_id, platform_id)
) -> Result<usize, diesel::result::Error> {
    if updates.is_empty() {
        return Ok(0);
    }

    let gtfs_ids: Vec<&str> = updates.iter().map(|(id, _, _)| id.as_str()).collect();
    let station_ids: Vec<Option<i64>> = updates.iter().map(|(_, sid, _)| *sid).collect();
    let platform_ids: Vec<Option<i64>> = updates.iter().map(|(_, _, pid)| *pid).collect();

    // Build the platform_ids array string (handling NULLs)
    let platform_arr: String = platform_ids
        .iter()
        .map(|p| match p {
            Some(id) => id.to_string(),
            None => "NULL".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let gtfs_arr = gtfs_ids
        .iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");

    let station_arr = station_ids
        .iter()
        .map(|p| match p {
            Some(id) => id.to_string(),
            None => "NULL".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        r#"
        UPDATE gtfs.stops AS s SET
            osm_station_id = u.station_id,
            osm_platform_id = u.platform_id
        FROM (
            SELECT * FROM unnest(
                ARRAY[{}]::text[],
                ARRAY[{}]::bigint[],
                ARRAY[{}]::bigint[]
            ) AS t(gtfs_id, station_id, platform_id)
        ) AS u
        WHERE s.gtfs_id = u.gtfs_id
          AND s.onestop_feed_id = '{}'
          AND s.attempt_id = '{}'
        "#,
        gtfs_arr,
        station_arr,
        platform_arr,
        feed_id.replace('\'', "''"),
        attempt_id.replace('\'', "''")
    );

    diesel::sql_query(query).execute(conn).await
}

// ============================================================================
// Matching logic (CPU-bound, parallelizable)
// ============================================================================

/// Result of matching a single stop
struct MatchResult {
    gtfs_id: String,
    osm_station_id: Option<i64>,
    osm_platform_id: Option<i64>,
}

/// Score a candidate station against a stop
fn score_candidate(
    station: &IndexedStation,
    stop_lon: f64,
    stop_lat: f64,
    stop_name_lower: &str,
    platform_code: Option<&str>,
    radius_m: f64,
) -> (f64, bool) {
    // Helper to clean names for comparison (remove common station words)
    let clean_name = |name: &str| -> String {
        name.replace("gare", "")
            .replace("station", "")
            .replace("  ", " ")
            .trim()
            .to_string()
    };

    let stop_name_clean = clean_name(stop_name_lower);

    // Compute name similarity
    let name_sim = station
        .name_lower
        .as_ref()
        .map(|osm_name| {
            let osm_name_clean = clean_name(osm_name);

            // First try Jaro-Winkler
            let jw_score = jaro_winkler(&stop_name_clean, &osm_name_clean);

            // Also check token-based matching:
            // "Paris Est" should match "Paris Gare de l'Est"
            // Check if all significant words from GTFS name appear in OSM name
            let gtfs_tokens: Vec<&str> = stop_name_clean
                .split(|c: char| !c.is_alphanumeric())
                .filter(|t| t.len() >= 2) // Skip very short tokens
                .collect();

            let token_match = if !gtfs_tokens.is_empty() {
                let matches = gtfs_tokens
                    .iter()
                    .filter(|t| osm_name_clean.contains(*t))
                    .count();
                matches as f64 / gtfs_tokens.len() as f64
            } else {
                0.0
            };

            // Take the better of the two approaches
            jw_score.max(token_match)
        })
        .unwrap_or(0.0);

    // Compute distance using geo crate
    let p1 = point!(x: stop_lon, y: stop_lat);
    let p2 = point!(x: station.x, y: station.y);
    let distance = p1.haversine_distance(&p2);

    // Compute proximity score (1.0 at center, 0.0 at radius edge)
    let proximity_score = 1.0 - (distance / radius_m).min(1.0);

    // Check for platform match via local_ref
    let platform_match = platform_code
        .and_then(|pc| station.local_ref.as_ref().map(|lr| lr == pc))
        .unwrap_or(false);

    // Combined score: prioritize proximity (70%) over name similarity (30%)
    // This allows matching nearby stations even when names differ
    let score = (name_sim * 0.3) + (proximity_score * 0.7);

    (score, platform_match)
}

/// Match a single stop to the best OSM station using R-tree lookup
/// Uses compatible mode matching: exact mode matches are preferred, but
/// compatible modes (e.g., rail+subway, tram+light_rail) are used as fallback
fn match_stop_with_rtree(
    rtree: &RTree<IndexedStation>,
    stop_lon: f64,
    stop_lat: f64,
    stop_name: &str,
    platform_code: Option<&str>,
    radius_m: f64,
    mode: &str,
) -> Option<(i64, Option<i64>)> {
    let stop_name_lower = stop_name.to_lowercase();

    // Convert radius from meters to approximate degrees for R-tree query
    // At worst case (equator), 1 degree ≈ 111km, so radius_m / 111000
    let radius_deg = radius_m / 111000.0 * 1.5; // 1.5x for safety margin

    // Query R-tree for nearby candidates
    let search_box = AABB::from_corners(
        [stop_lon - radius_deg, stop_lat - radius_deg],
        [stop_lon + radius_deg, stop_lat + radius_deg],
    );

    // Consider ALL nearby stations regardless of mode - mode matching is used for ranking only
    // This handles cases where OSM data may have different mode classifications
    let candidates: Vec<&IndexedStation> = rtree.locate_in_envelope(&search_box).collect();

    if candidates.is_empty() {
        return None;
    }

    // Separate into primary stations and platforms
    let (stations, platforms): (Vec<_>, Vec<_>) =
        candidates.into_iter().partition(|s| s.is_primary_station());

    // Step 1: Find best matching primary station
    // Prioritize exact mode matches over compatible mode matches
    let mut best_station: Option<(i64, f64, u8)> = None; // (osm_id, score, mode_priority)

    for station in &stations {
        let (score, _) = score_candidate(
            station,
            stop_lon,
            stop_lat,
            &stop_name_lower,
            platform_code,
            radius_m,
        );

        if score >= MIN_MATCH_SCORE {
            let mode_priority = mode_match_priority(mode, &station.mode_type);

            // Compare by (mode_priority, score) - prefer exact mode, then higher score
            let dominated = best_station
                .as_ref()
                .map_or(false, |(_, best_score, best_prio)| {
                    if mode_priority < *best_prio {
                        // Current has lower priority, skip unless much closer
                        score <= *best_score + 0.15
                    } else if mode_priority > *best_prio {
                        // Current has higher priority, always prefer
                        false
                    } else {
                        // Same priority, compare by score
                        score <= *best_score
                    }
                });

            if !dominated {
                best_station = Some((station.osm_id, score, mode_priority));
            }
        }
    }

    // Step 2: If station found, look for matching platform
    if let Some((station_id, _, _)) = best_station {
        let mut matching_platform: Option<i64> = None;

        if platform_code.is_some() {
            for platform in &platforms {
                let (score, is_platform_match) = score_candidate(
                    platform,
                    stop_lon,
                    stop_lat,
                    &stop_name_lower,
                    platform_code,
                    radius_m,
                );

                if is_platform_match && score >= MIN_MATCH_SCORE * 0.8 {
                    if platform.parent_osm_id == Some(station_id) {
                        matching_platform = Some(platform.osm_id);
                        break;
                    }
                }
            }
        }

        return Some((station_id, matching_platform));
    }

    // Step 3: Fallback to platforms if no primary station found
    let mut best_platform: Option<(i64, Option<i64>, f64, bool, u8)> = None;

    for platform in &platforms {
        let (score, is_platform_match) = score_candidate(
            platform,
            stop_lon,
            stop_lat,
            &stop_name_lower,
            platform_code,
            radius_m,
        );

        let adjusted_score = if is_platform_match {
            score + 0.1
        } else {
            score
        };

        if adjusted_score >= MIN_MATCH_SCORE {
            let mode_priority = mode_match_priority(mode, &platform.mode_type);

            let dominated =
                best_platform
                    .as_ref()
                    .map_or(false, |(_, _, best_score, _, best_prio)| {
                        if mode_priority < *best_prio {
                            adjusted_score <= *best_score + 0.15
                        } else if mode_priority > *best_prio {
                            false
                        } else {
                            adjusted_score <= *best_score
                        }
                    });

            if !dominated {
                best_platform = Some((
                    platform.osm_id,
                    platform.parent_osm_id,
                    adjusted_score,
                    is_platform_match,
                    mode_priority,
                ));
            }
        }
    }

    if let Some((osm_id, parent_id, _, is_platform_match, _)) = best_platform {
        let station_id = parent_id.unwrap_or(osm_id);
        let platform_id = if is_platform_match {
            Some(osm_id)
        } else {
            None
        };
        return Some((station_id, platform_id));
    }

    // Step 4: FINAL FALLBACK - pure proximity match within 500m
    // If no text-based matches found, match the closest station regardless of name
    // Still prioritize exact mode matches

    // Check if in Canada (approximate bbox: Lat 41.6-83.1, Lon -141.0 to -52.6)
    let is_canada = stop_lat >= 41.6 && stop_lat <= 83.1 && stop_lon >= -141.0 && stop_lon <= -52.6;
    let proximity_fallback_m = if is_canada { 300.0 } else { 500.0 };

    let stop_point = point!(x: stop_lon, y: stop_lat);

    // Find closest primary station within 500m, preferring exact mode matches
    let closest_station = stations
        .iter()
        .map(|s| {
            let station_point = point!(x: s.x, y: s.y);
            let distance = stop_point.haversine_distance(&station_point);
            let mode_priority = mode_match_priority(mode, &s.mode_type);
            (s, distance, mode_priority)
        })
        .filter(|(_, d, _)| *d <= proximity_fallback_m)
        // Sort by mode_priority DESC, then distance ASC
        .min_by(|(_, d1, p1), (_, d2, p2)| p2.cmp(p1).then_with(|| d1.partial_cmp(d2).unwrap()));

    if let Some((station, _, _)) = closest_station {
        return Some((station.osm_id, None));
    }

    // Try closest platform within 500m
    let closest_platform = platforms
        .iter()
        .map(|p| {
            let platform_point = point!(x: p.x, y: p.y);
            let distance = stop_point.haversine_distance(&platform_point);
            let mode_priority = mode_match_priority(mode, &p.mode_type);
            (p, distance, mode_priority)
        })
        .filter(|(_, d, _)| *d <= proximity_fallback_m)
        .min_by(|(_, d1, p1), (_, d2, p2)| p2.cmp(p1).then_with(|| d1.partial_cmp(d2).unwrap()));

    if let Some((platform, _, _)) = closest_platform {
        let station_id = platform.parent_osm_id.unwrap_or(platform.osm_id);
        return Some((station_id, None));
    }

    None
}

// ============================================================================
// Stop data structures
// ============================================================================

/// Stop data for batch processing
struct StopData {
    gtfs_id: String,
    lat: f64,
    lon: f64,
    name: String,
    mode: &'static str,
    platform_code: Option<String>,
}

// ============================================================================
// Main entry point
// ============================================================================

/// Propagate OSM station matches from children stops to unmatched parent stations
/// If a parent station is unmatched but its children are matched, assign the child's
/// OSM station ID to the parent.
pub async fn propagate_parent_matches(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
) -> Result<usize, diesel::result::Error> {
    use catenary::schema::gtfs::stops::dsl as stops_dsl;

    // 1. Fetch child matches: (parent_station, osm_station_id)
    // We only care about children that have both a parent and a match
    let child_matches: Vec<(String, i64)> = stops_dsl::stops
        .filter(stops_dsl::onestop_feed_id.eq(feed_id))
        .filter(stops_dsl::attempt_id.eq(attempt_id))
        .filter(stops_dsl::parent_station.is_not_null())
        .filter(stops_dsl::osm_station_id.is_not_null())
        .select((stops_dsl::parent_station, stops_dsl::osm_station_id))
        .load::<(Option<String>, Option<i64>)>(conn)
        .await?
        .into_iter()
        .filter_map(|(p, o)| Some((p?, o?)))
        .collect();

    if child_matches.is_empty() {
        return Ok(0);
    }

    // 2. Aggregate votes in memory: parent_id -> osm_id -> count
    let mut votes: HashMap<String, HashMap<i64, usize>> = HashMap::new();
    for (parent_id, osm_id) in child_matches {
        *votes
            .entry(parent_id)
            .or_default()
            .entry(osm_id)
            .or_default() += 1;
    }

    let parent_ids: Vec<&String> = votes.keys().collect();

    // 3. Fetch only unmatched parents that are candidates (have children with matches)
    // We chunk this to avoid hitting query parameter limits if many parents
    let mut unmatched_parents: Vec<String> = Vec::new();
    const FETCH_BATCH_SIZE: usize = 5000;

    for chunk in parent_ids.chunks(FETCH_BATCH_SIZE) {
        let batch: Vec<String> = stops_dsl::stops
            .filter(stops_dsl::onestop_feed_id.eq(feed_id))
            .filter(stops_dsl::attempt_id.eq(attempt_id))
            .filter(stops_dsl::location_type.eq(1)) // station
            .filter(stops_dsl::osm_station_id.is_null()) // only update if currently null
            .filter(stops_dsl::gtfs_id.eq_any(chunk.iter().copied()))
            .select(stops_dsl::gtfs_id)
            .load::<String>(conn)
            .await?;
        unmatched_parents.extend(batch);
    }

    if unmatched_parents.is_empty() {
        return Ok(0);
    }

    // 4. Determine best match for each unmatched parent
    let mut updates: Vec<(String, Option<i64>, Option<i64>)> = Vec::new();

    for parent_id in unmatched_parents {
        if let Some(counts) = votes.get(&parent_id) {
            // Find osm_id with max count (majority vote)
            if let Some((&best_osm_id, _)) = counts.iter().max_by_key(|&(_, count)| count) {
                // For parent stations, we generally don't assign a platform_id
                updates.push((parent_id, Some(best_osm_id), None));
            }
        }
    }

    println!(
        "  OSM matching: propagating matches to {} parents via children",
        updates.len()
    );

    // 5. Batch update
    let mut total_updated = 0;
    const UPDATE_BATCH_SIZE: usize = 1000;

    for chunk in updates.chunks(UPDATE_BATCH_SIZE) {
        total_updated += batch_update_stops(conn, feed_id, attempt_id, chunk).await?;
    }

    Ok(total_updated)
}

/// Batch match stops for a feed, updating stops.osm_station_id directly
/// This is called during GTFS import in gtfs_process.rs
///
/// Algorithm:
/// 1. Load stops in batches, group by grid cell
/// 2. For each grid cell with stops:
///    a. Fetch OSM stations in the cell's bounding box (single query)
///    b. Build R-tree index for fast spatial lookup
///    c. Match all stops in the cell using parallel processing
///    d. Batch update the database
pub async fn match_stops_for_feed(
    conn: &mut AsyncPgConnection,
    feed_id: &str,
    attempt_id: &str,
    chateau_id: &str,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::stops::dsl as stops_dsl;

    let mut matched_count: u64 = 0;
    let mut offset: i64 = 0;

    // Collect all stops that need matching first
    let mut all_stops: Vec<StopData> = Vec::new();

    loop {
        // Fetch a batch of stops - get both route_types and children_route_types
        let stops: Vec<(
            String,
            Option<postgis_diesel::types::Point>,
            Option<String>,
            Vec<Option<i16>>,
            Vec<Option<i16>>,
            Option<String>,
        )> = stops_dsl::stops
            .filter(stops_dsl::onestop_feed_id.eq(feed_id))
            .filter(stops_dsl::attempt_id.eq(attempt_id))
            .filter(stops_dsl::primary_route_type.ne(3))
            .select((
                stops_dsl::gtfs_id,
                stops_dsl::point,
                stops_dsl::name,
                stops_dsl::route_types,
                stops_dsl::children_route_types,
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
        for (stop_id, point, name, route_types, children_route_types, platform_code) in stops {
            let Some(point) = point else { continue };

            // Combine route_types and children_route_types to catch both parent and child stops
            let all_route_types: Vec<i16> = route_types
                .into_iter()
                .chain(children_route_types.into_iter())
                .filter_map(|rt| rt)
                .collect();

            // Find rail/tram/subway route types for this stop
            let rail_types: Vec<i16> = all_route_types
                .into_iter()
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

            let Some(mode) = route_type_to_mode(route_type) else {
                continue;
            };

            all_stops.push(StopData {
                gtfs_id: stop_id,
                lat: point.y,
                lon: point.x,
                name: name.unwrap_or_default(),
                mode,
                platform_code,
            });
        }

        offset += batch_len;

        if batch_len < STOP_BATCH_SIZE {
            break;
        }
    }

    if all_stops.is_empty() {
        println!(
            "  OSM matching: no rail/tram/subway stops found for {}",
            chateau_id
        );
        return Ok(0);
    }

    println!(
        "  OSM matching: {} rail/tram/subway stops to process for {}",
        all_stops.len(),
        chateau_id
    );

    // Group stops by grid cell
    let mut cells: HashMap<GridCell, Vec<&StopData>> = HashMap::new();
    for stop in &all_stops {
        let cell = GridCell::from_coords(stop.lat, stop.lon);
        cells.entry(cell).or_default().push(stop);
    }

    let total_cells = cells.len();
    println!(
        "  OSM matching: {} geographic cells to process",
        total_cells
    );

    // Collect unique modes across all stops
    let all_modes: Vec<&str> = vec!["rail", "tram", "subway", "light_rail"];

    // Process each grid cell
    let mut processed_cells = 0;
    for (cell, cell_stops) in cells {
        // Get expanded bounding box for this cell
        let (min_lat, min_lon, max_lat, max_lon) = cell.bbox_expanded(MAX_RADIUS_DEG);

        // Fetch OSM stations for this cell (single query)
        let osm_stations =
            match fetch_osm_stations_in_bbox(conn, min_lat, min_lon, max_lat, max_lon, &all_modes)
                .await
            {
                Ok(stations) => stations,
                Err(e) => {
                    eprintln!("Error fetching OSM stations for cell {:?}: {:?}", cell, e);
                    continue;
                }
            };

        processed_cells += 1;

        // Progress indicator every 10 cells or for cells with many stops
        if processed_cells % 10 == 0 || cell_stops.len() > 100 {
            println!(
                "  OSM matching: cell {}/{} - {} stops, {} OSM stations",
                processed_cells,
                total_cells,
                cell_stops.len(),
                osm_stations.len()
            );
        }

        if osm_stations.is_empty() {
            if cell_stops.len() > 50 {
                println!(
                    "  OSM matching: WARNING - cell {:?} has {} stops but 0 OSM stations",
                    cell,
                    cell_stops.len()
                );
            }
            // Do not continue here! We must process these stops to clear any existing matches (set to NULL).
            // Passing an empty R-tree will result in None matches, which is what we want.
        }

        // Build R-tree index (pre-compute lowercase names here)
        let indexed_stations: Vec<IndexedStation> = osm_stations
            .iter()
            .map(IndexedStation::from_osm_station)
            .collect();

        let rtree = RTree::bulk_load(indexed_stations);

        // Match all stops in this cell using parallel processing
        let matches: Vec<MatchResult> = cell_stops
            .par_iter()
            .map(|stop| {
                let radius = get_radius_for_mode(stop.mode);

                let match_result = match_stop_with_rtree(
                    &rtree,
                    stop.lon,
                    stop.lat,
                    &stop.name,
                    stop.platform_code.as_deref(),
                    radius,
                    stop.mode,
                );

                match match_result {
                    Some((station_id, platform_id)) => MatchResult {
                        gtfs_id: stop.gtfs_id.clone(),
                        osm_station_id: Some(station_id),
                        osm_platform_id: platform_id,
                    },
                    None => MatchResult {
                        gtfs_id: stop.gtfs_id.clone(),
                        osm_station_id: None,
                        osm_platform_id: None,
                    },
                }
            })
            .collect();

        // Batch update the database
        let updates: Vec<(String, Option<i64>, Option<i64>)> = matches
            .into_iter()
            .map(|m| (m.gtfs_id, m.osm_station_id, m.osm_platform_id))
            .collect();

        let update_count = updates.len();

        if let Err(e) = batch_update_stops(conn, feed_id, attempt_id, &updates).await {
            eprintln!("Error batch updating stops for cell {:?}: {:?}", cell, e);
            continue;
        }

        matched_count += update_count as u64;
    }

    // Propagate matches to parent stations (post-processing)
    match propagate_parent_matches(conn, feed_id, attempt_id).await {
        Ok(count) => {
            if count > 0 {
                println!(
                    "  OSM matching: Propagated matches to {} parent stations for {}",
                    count, chateau_id
                );
            }
        }
        Err(e) => {
            eprintln!(
                "  OSM matching: Error propagating parent matches for {}: {:?}",
                chateau_id, e
            );
        }
    }

    if matched_count > 0 {
        println!(
            "  OSM station matching: {} stops matched for {}",
            matched_count, chateau_id
        );
    }

    Ok(matched_count)
}

// Keep the old function for backwards compatibility if needed elsewhere
/// Find OSM stations within radius of a point (meters), filtered by mode
#[allow(dead_code)]
pub async fn find_osm_stations_near(
    conn: &mut AsyncPgConnection,
    lat: f64,
    lon: f64,
    radius_m: f64,
    mode_filter: Option<&str>,
) -> Result<Vec<OsmStation>, diesel::result::Error> {
    let mode_clause = mode_filter
        .map(|m| format!("AND mode_type = '{}'", m))
        .unwrap_or_default();

    let query = format!(
        r#"
        SELECT osm_id, osm_type, import_id, point, name, name_translations,
               station_type, railway_tag, mode_type, uic_ref, ref AS ref_, 
               wikidata, operator, network, level, local_ref, parent_osm_id, is_derivative
        FROM gtfs.osm_stations
        WHERE ST_DWithin(point::geography, ST_MakePoint({}, {})::geography, {})
        {}
        "#,
        lon, lat, radius_m, mode_clause
    );

    diesel::sql_query(query).load::<OsmStation>(conn).await
}
