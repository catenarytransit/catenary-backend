use anyhow::Result;
use catenary::models::Stop;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use postgis_diesel::types::Point;
use rstar::{AABB, RTree, RTreeObject};
use std::collections::{HashMap, HashSet};
use strsim::normalized_levenshtein;

// Constants
const BASE_DIST_RAIL: f64 = 100.0;
const BASE_DIST_TRAM: f64 = 30.0;
const SIMILARITY_HIGH: f64 = 0.90;
const SIMILARITY_MED: f64 = 0.75;

#[derive(Debug, Clone)]
pub struct StopCluster {
    pub cluster_id: usize,
    pub centroid: Point,
    pub stops: Vec<Stop>,
}

#[derive(Debug, Clone)]
struct SpatialStop(Stop);

// Implementing PartialEq manually for SpatialStop since Stop doesn't implement it
impl PartialEq for SpatialStop {
    fn eq(&self, other: &Self) -> bool {
        self.0.gtfs_id == other.0.gtfs_id 
        // Note: strictly this should compare all fields but for our purpose ID uniqueness is enough
        // or effectively pointer equality if we had that.
    }
}

impl RTreeObject for SpatialStop {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let p = self.0.point.as_ref().unwrap();
        AABB::from_point([p.x, p.y])
    }
}

impl rstar::PointDistance for SpatialStop {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let p = self.0.point.as_ref().unwrap();
        let dx = p.x - point[0];
        let dy = p.y - point[1];
        dx * dx + dy * dy
    }
}

pub async fn cluster_stops(pool: &CatenaryPostgresPool) -> Result<Vec<StopCluster>> {
    let mut conn = pool.get().await?;

    use catenary::schema::gtfs::stops::dsl::*;

    let all_stops = stops
        .filter(location_type.eq_any(vec![0, 1])) // StopPoint or Station
        .filter(point.is_not_null())
        .select(Stop::as_select())
        .load::<Stop>(&mut conn)
        .await?;

    // Filter to only stops that have rail/tram route types
    let valid_stops: Vec<Stop> = all_stops.into_iter().filter(|s| {
         s.point.is_some() &&
         s.route_types.iter().flatten().any(|&t| t == 0 || t == 1 || t == 2 || (900..=1200).contains(&t))
    }).collect();

    Ok(greedy_clustering(valid_stops))
}

fn greedy_clustering(stops: Vec<Stop>) -> Vec<StopCluster> {
    let mut visited = HashSet::new();
    let mut clusters = Vec::new();
    let mut cluster_id_counter = 0;

    let spatial_stops: Vec<SpatialStop> = stops.iter().cloned().map(SpatialStop).collect();
    let tree = RTree::bulk_load(spatial_stops);

    let mut stop_map: HashMap<String, Stop> = HashMap::new();
    for s in &stops {
        stop_map.insert(s.gtfs_id.clone(), s.clone());
    }

    // Since we need to iterate and access neighbors, but RTree doesn't support easy "remove"
    // we use a visited set ensuring each node is processed exactly once.
    
    // Sort stops to ensure deterministic order (optional but good for consistency)
    let mut stop_ids: Vec<String> = stop_map.keys().cloned().collect();
    stop_ids.sort();

    for stop_id in stop_ids {
        if visited.contains(&stop_id) {
            continue;
        }

        let mut cluster_component = Vec::new();
        let mut queue = vec![stop_id.clone()];
        visited.insert(stop_id.clone());

        while let Some(current_id) = queue.pop() {
            if let Some(current_stop) = stop_map.get(&current_id) {
                cluster_component.push(current_stop.clone());

                let p = current_stop.point.as_ref().unwrap();
                
                // Query candidates within conservative max range (approx 0.01 degrees)
                // 0.005 is approx 500m. Let's use 0.01 to be safe and filter later.
                let candidates = tree.locate_within_distance([p.x, p.y], 0.01); 

                for candidate_wrapper in candidates {
                    let candidate = &candidate_wrapper.0;
                    if !visited.contains(&candidate.gtfs_id) {
                        if should_merge(current_stop, candidate) {
                            visited.insert(candidate.gtfs_id.clone());
                            queue.push(candidate.gtfs_id.clone());
                        }
                    }
                }
            }
        }

        if !cluster_component.is_empty() {
             let centroid = calculate_centroid(&cluster_component);
             clusters.push(StopCluster {
                 cluster_id: cluster_id_counter,
                 centroid,
                 stops: cluster_component
             });
             cluster_id_counter += 1;
        }
    }

    clusters
}

fn calculate_centroid(stops: &[Stop]) -> Point {
    let count = stops.len() as f64;
    let (sum_x, sum_y) = stops.iter().fold((0.0, 0.0), |acc, s| {
        let p = s.point.as_ref().unwrap();
        (acc.0 + p.x, acc.1 + p.y)
    });
    
    Point {
        x: sum_x / count,
        y: sum_y / count,
        srid: Some(4326),
    }
}

fn should_merge(a: &Stop, b: &Stop) -> bool {
    if a.point.is_none() || b.point.is_none() {
        return false;
    }
    let p_a = a.point.as_ref().unwrap();
    let p_b = b.point.as_ref().unwrap();

    // Haversine distance
    let distance = haversine_distance(p_a.y, p_a.x, p_b.y, p_b.x);

    // Normalize names
    let name_a = a.name.as_deref().unwrap_or("").to_lowercase();
    let name_b = b.name.as_deref().unwrap_or("").to_lowercase();

    let similarity = normalized_levenshtein(&name_a, &name_b);

    // Determine strictness based on route type (0 = Tram)
    let is_tram = is_tram_stop(a) || is_tram_stop(b);
    let base_dist = if is_tram {
        BASE_DIST_TRAM
    } else {
        BASE_DIST_RAIL
    };

    if similarity > SIMILARITY_HIGH {
        distance < base_dist * 2.5
    } else if similarity > SIMILARITY_MED {
        distance < base_dist
    } else {
        distance < base_dist * 0.5
    }
}

fn is_tram_stop(stop: &Stop) -> bool {
    stop.route_types
        .iter()
        .flatten()
        .any(|&t| t == 0 || t == 900)
}

/// Distance in meters between two lat/lon points
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371000.0; // Earth radius in meters
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_distance() {
        // LA to NYC
        let lat1 = 34.0522;
        let lon1 = -118.2437;
        let lat2 = 40.7128;
        let lon2 = -74.0060;
        let dist = haversine_distance(lat1, lon1, lat2, lon2);
        
        // Expected approx 3935 km
        assert!(dist > 3_930_000.0 && dist < 3_950_000.0);

        // Same point
        assert_eq!(haversine_distance(0.0, 0.0, 0.0, 0.0), 0.0);
    }
}
