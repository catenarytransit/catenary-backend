use crate::clustering::StopCluster;
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{LineString as GeoLineString, Point as GeoPoint, Coord, Length};
use postgis_diesel::types::{LineString, Point};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub from_cluster: usize,
    pub to_cluster: usize,
    pub geometry: LineString<Point>,
    pub route_ids: Vec<String>,
    pub weight: f64, // length in meters
}

pub async fn generate_edges(
    pool: &CatenaryPostgresPool,
    clusters: &[StopCluster],
) -> Result<Vec<GraphEdge>> {
    let mut conn = pool.get().await?;

    let mut stop_to_cluster: HashMap<String, usize> = HashMap::new();
    for c in clusters {
        for s in &c.stops {
            stop_to_cluster.insert(s.gtfs_id.clone(), c.cluster_id);
        }
    }

    use catenary::schema::gtfs::direction_pattern::dsl::*;

    // Optimized: Fetch only necessary columns to reduce memory usage
    // Reverting to full struct load to avoid type mismatch issues with partial tuples
    let patterns = direction_pattern
        .order((direction_pattern_id, stop_sequence))
        .load::<catenary::models::DirectionPatternRow>(&mut conn)
        .await?;

    let mut pattern_map: HashMap<String, Vec<catenary::models::DirectionPatternRow>> =
        HashMap::new();
    for p in patterns {
        pattern_map
            .entry(p.direction_pattern_id.clone())
            .or_default()
            .push(p);
    }

    use catenary::schema::gtfs::direction_pattern_meta::dsl as meta_dsl;
    use catenary::schema::gtfs::shapes::dsl as user_shapes_dsl;

    let metas = meta_dsl::direction_pattern_meta
        .load::<catenary::models::DirectionPatternMeta>(&mut conn)
        .await?;

    let mut pattern_to_shape: HashMap<String, String> = HashMap::new();
    let mut pattern_to_route: HashMap<String, String> = HashMap::new();
    let mut relevant_shape_ids = HashSet::new();

    for m in metas {
        if let Some(sid) = m.gtfs_shape_id {
            pattern_to_shape.insert(m.direction_pattern_id.clone(), sid.clone());
            relevant_shape_ids.insert(sid);
        }
        if let Some(rid) = m.route_id {
            pattern_to_route.insert(m.direction_pattern_id.clone(), rid.to_string());
        }
    }

    let loaded_shapes = user_shapes_dsl::shapes
        .filter(user_shapes_dsl::shape_id.eq_any(&relevant_shape_ids))
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    let mut shape_map: HashMap<String, catenary::models::Shape> = HashMap::new();
    for s in loaded_shapes {
        shape_map.insert(s.shape_id.clone(), s);
    }

    let mut raw_edges: Vec<GraphEdge> = Vec::new();

    for (pat_id, sequence) in pattern_map {
        let shape_id = pattern_to_shape.get(&pat_id);
        if shape_id.is_none() {
            continue;
        }
        let shape = shape_map.get(shape_id.unwrap());
        if shape.is_none() {
            continue;
        }

        let geo_shape = convert_to_geo(&shape.unwrap().linestring);

        for i in 0..sequence.len() - 1 {
            let from_stop = &sequence[i].stop_id;
            let to_stop = &sequence[i + 1].stop_id;

            let from_c = stop_to_cluster.get(from_stop.as_str());
            let to_c = stop_to_cluster.get(to_stop.as_str());

            if let (Some(&fc), Some(&tc)) = (from_c, to_c) {
                if fc == tc {
                    continue;
                }

                let start_pt = convert_point(&clusters[fc].centroid);
                let end_pt = convert_point(&clusters[tc].centroid);

                // Find positions on shape
                let start_frac = geo_shape.line_locate_point(&start_pt).unwrap_or(0.0);
                let end_frac = geo_shape.line_locate_point(&end_pt).unwrap_or(1.0);

                if start_frac >= end_frac {
                    continue; // Skip backtracking/invalid segments
                }

                if let Some(segment) = get_line_substring(&geo_shape, start_frac, end_frac) {
                    // Use deprecated haversine_length which works reliably
                    let weight = segment.haversine_length();
                    
                    raw_edges.push(GraphEdge {
                        from_cluster: fc,
                        to_cluster: tc,
                        geometry: convert_from_geo(&segment),
                        route_ids: vec![pattern_to_route.get(&pat_id).cloned().unwrap_or_default()],
                        weight,
                    });
                }
            }
        }
    }

    Ok(merge_edges(raw_edges))
}

fn merge_edges(raw_edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
    let mut groups: HashMap<(usize, usize), Vec<GraphEdge>> = HashMap::new();
    for edge in raw_edges {
        groups.entry((edge.from_cluster, edge.to_cluster)).or_default().push(edge);
    }
    
    let mut merged = Vec::new();
    for ((from, to), mut group) in groups {
        if group.is_empty() { continue; }
        
        // Collect all route IDs
        let mut all_routes = HashSet::new();
        for e in &group {
            for r in &e.route_ids {
                all_routes.insert(r.clone());
            }
        }
        let merged_routes: Vec<String> = all_routes.into_iter().collect();
        
        // Pick representative geometry: Median length
        group.sort_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap_or(std::cmp::Ordering::Equal));
        let mid = group.len() / 2;
        let best_edge = &group[mid];
        
        merged.push(GraphEdge {
            from_cluster: from,
            to_cluster: to,
            geometry: best_edge.geometry.clone(),
            route_ids: merged_routes,
            weight: best_edge.weight,
        });
    }
    merged
}
// Basic implementation of line substring
fn get_line_substring(line: &GeoLineString<f64>, start_frac: f64, end_frac: f64) -> Option<GeoLineString<f64>> {
    let mut coords = Vec::new();
    let num_points = line.0.len();
    if num_points < 2 { return None; }
    
    // Total Euclidean length for fraction mapping
    let total_euclidean_len = line.lines().map(|l| l.euclidean_length()).sum::<f64>(); 
    
    let start_dist = start_frac * total_euclidean_len;
    let end_dist = end_frac * total_euclidean_len;
    
    let mut current_dist = 0.0;
    let mut started = false;
    
    // Helper to interpolate
    let interpolate = |l: geo::Line<f64>, dist_on_line: f64| -> Coord {
         let len = l.euclidean_length();
         if len == 0.0 { return l.start; }
         let t = dist_on_line / len;
         Coord {
             x: l.start.x + (l.end.x - l.start.x) * t,
             y: l.start.y + (l.end.y - l.start.y) * t,
         }
    };
    
    for segment in line.lines() {
        let seg_len = segment.euclidean_length();
        let next_dist = current_dist + seg_len;
        
        if !started {
            if next_dist >= start_dist {
                // This segment contains start
                let dist_on_seg = start_dist - current_dist;
                coords.push(interpolate(segment, dist_on_seg));
                started = true;
                
                // If it also contains end
                if next_dist >= end_dist {
                    let dist_on_seg_end = end_dist - current_dist;
                    coords.push(interpolate(segment, dist_on_seg_end));
                    break;
                } else {
                     coords.push(segment.end);
                }
            }
        } else {
            // Already started
            if next_dist >= end_dist {
                 // This segment contains end
                 let dist_on_seg = end_dist - current_dist;
                 coords.push(interpolate(segment, dist_on_seg));
                 break;
            } else {
                coords.push(segment.end);
            }
        }
        current_dist = next_dist;
    }
    
    if coords.len() < 2 { return None; }
    Some(GeoLineString::new(coords))
}

fn convert_to_geo(ls: &LineString<Point>) -> GeoLineString<f64> {
    ls.points.iter().map(|p| Coord { x: p.x, y: p.y }).collect()
}

fn convert_from_geo(ls: &GeoLineString<f64>) -> LineString<Point> {
    let points = ls.0.iter().map(|c| Point { x: c.x, y: c.y, srid: Some(4326) }).collect();
    LineString { points, srid: Some(4326) }
}

fn convert_point(p: &Point) -> GeoPoint<f64> {
    GeoPoint::new(p.x, p.y)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{LineString, Coord};

    #[test]
    fn test_get_line_substring_simple() {
        // Horizontal line from 0,0 to 10,0
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
        ];
        let line = LineString::new(coords);

        // Middle 50%: 2.5 to 7.5
        let sub = get_line_substring(&line, 0.25, 0.75).expect("Should return substring");
        
        // Length should be 5.0
        // Euclidean implementation used in function.
        use geo::EuclideanLength; // Ensure trait is usable if needed, or rely on implementation detail using deprecated one or new one.
        // Actually, verifying coordinates is safer.
        let pts = sub.0;
        assert_eq!(pts.len(), 2);
        assert!((pts[0].x - 2.5).abs() < 1e-6);
        assert!((pts[1].x - 7.5).abs() < 1e-6);
    }

    #[test]
    fn test_get_line_substring_multi_segment() {
        // Line 0,0 -> 10,0 -> 10,10. Total len 20.
        let coords = vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
            Coord { x: 10.0, y: 10.0 },
        ];
        let line = LineString::new(coords);

        // From 0.25 (x=5) to 0.75 (y=5).
        // 0.25 * 20 = 5. Point (5,0).
        // 0.75 * 20 = 15. Point (10,5).
        let sub = get_line_substring(&line, 0.25, 0.75).expect("Should return substring");
        
        // Should contain (5,0), (10,0), (10,5)
        let pts = sub.0;
        assert_eq!(pts.len(), 3);
        assert!((pts[0].x - 5.0).abs() < 1e-6);
        assert!((pts[0].y - 0.0).abs() < 1e-6);
        
        assert!((pts[1].x - 10.0).abs() < 1e-6);
        assert!((pts[1].y - 0.0).abs() < 1e-6);
        
        assert!((pts[2].x - 10.0).abs() < 1e-6);
        assert!((pts[2].y - 5.0).abs() < 1e-6);
    }
}
