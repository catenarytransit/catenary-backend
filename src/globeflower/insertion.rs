use crate::clustering::StopCluster;
use crate::edges::{GraphEdge, NodeId, convert_from_geo, convert_to_geo};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rstar::{AABB, RTree, RTreeObject};
use std::collections::{HashMap, HashSet};

struct EdgeSpatial {
    index: usize,
    geom: GeoLineString<f64>,
}

impl RTreeObject for EdgeSpatial {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        let bounding = self.geom.bounding_rect().unwrap();
        AABB::from_corners(
            [bounding.min().x, bounding.min().y],
            [bounding.max().x, bounding.max().y],
        )
    }
}

pub async fn insert_stations(
    mut edges: Vec<GraphEdge>,
    clusters: &[StopCluster],
    pool: &CatenaryPostgresPool,
) -> Result<Vec<GraphEdge>> {
    println!(
        "Inserting {} stations into Support Graph...",
        clusters.len()
    );

    // index edges
    let edge_spatials: Vec<EdgeSpatial> = edges
        .iter()
        .enumerate()
        .map(|(i, e)| EdgeSpatial {
            index: i,
            geom: convert_to_geo(&e.geometry),
        })
        .collect();
    let tree = RTree::bulk_load(edge_spatials);

    // Build mapping: Cluster ID -> Set of Shape IDs that serve that cluster
    // This is derived from GTFS: Stop -> ItineraryPattern -> ItineraryPatternMeta -> Shape
    // Filtered to only include rail routes (route_type 0, 1, 2)
    let cluster_shape_map = build_cluster_shape_map(pool, clusters).await?;

    // We will modify edges in place, but inserting nodes splits edges.
    // Doing this iteratively is tricky if indices change.
    // Easier approach: Calculate all split points first, then apply splits.

    // Map: Edge Index -> List of (Distance along line, ClusterID, GeoPoint)
    let mut edge_splits: HashMap<usize, Vec<(f64, usize, GeoPoint<f64>)>> = HashMap::new();
    let mut next_node_id = edges
        .iter()
        .flat_map(|e| vec![e.from, e.to])
        .filter_map(|n| match n {
            NodeId::Intersection(_, id) => Some(id),
            _ => None,
        })
        .max()
        .unwrap_or(0)
        + 1;

    for cluster in clusters {
        let shapes_serving_cluster = cluster_shape_map
            .get(&cluster.cluster_id)
            .unwrap_or(&HashSet::new())
            .clone();
        if shapes_serving_cluster.is_empty() {
            continue; // Dead station?
        }

        let qa_len = shapes_serving_cluster.len() as f64;
        let p = cluster.centroid;
        let pt = GeoPoint::new(p.x, p.y);

        // Candidates: Edges within radius r (e.g. 100m ~ 0.001 deg)
        let radius = 0.001;
        let envelope = AABB::from_corners(
            [pt.x() - radius, pt.y() - radius],
            [pt.x() + radius, pt.y() + radius],
        );

        let mut best_candidate: Option<(usize, f64, GeoPoint<f64>)> = None; // (EdgeIdx, DistAlong, Point)
        let mut max_score = f64::NEG_INFINITY;

        for candidate in tree.locate_in_envelope_intersecting(&envelope) {
            let edge = &edges[candidate.index];
            let geom = &candidate.geom;

            // Project cluster centroid onto edge geometry
            if let Some(frac) = geom.line_locate_point(&pt) {
                let projected = get_point_on_line(geom, frac);
                let dist_deg = projected.haversine_distance(&pt); // degrees? No, haversine is meters.

                // Oops haversine_distance returns meters.
                let dist_meters = dist_deg;

                // C: how many shapes in Q_A are "served" by this edge?
                // The edge currently has a route_id which IS the shape_id (from support_graph.rs).
                // So check if edge.route_ids intersects Q_A.
                let served_count = edge
                    .routes
                    .iter()
                    .filter(|(chateau, shape_id, _)| shapes_serving_cluster.contains(shape_id))
                    .count() as f64;

                // Score o = (C / |Q_A|) * 100 - d
                let score = (served_count / qa_len) * 100.0 - dist_meters;

                // STRICT FILTER:
                // Only snap if this edge actually serves the cluster (served_count > 0).
                // This prevents bus-only stops (if any remain) from snapping to arbitrary rail lines.
                if served_count > 0.0 && score > max_score {
                    max_score = score;
                    best_candidate = Some((candidate.index, frac, projected));
                }
            }
        }

        if let Some((edge_idx, frac, point)) = best_candidate {
            edge_splits
                .entry(edge_idx)
                .or_default()
                .push((frac, cluster.cluster_id, point));
        }
    }

    // Apply splits
    let mut new_edges = Vec::new();

    for (i, edge) in edges.into_iter().enumerate() {
        if let Some(mut splits) = edge_splits.remove(&i) {
            // Sort by fraction
            splits.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            splits.dedup_by(|a, b| (a.0 - b.0).abs() < 1e-6);

            let geom = convert_to_geo(&edge.geometry);
            let mut current_frac = 0.0;
            let mut current_node = edge.from;
            let mut current_route_ids = edge.routes.clone();

            for (frac, cluster_id, _pt) in splits {
                // Determine new node ID (It's a Cluster Node now!)
                let new_node = NodeId::Cluster(cluster_id);

                if (frac - current_frac).abs() > 1e-6 {
                    let sub_geom = get_sub_poly(&geom, current_frac, frac);
                    let len = sub_geom.haversine_length();
                    new_edges.push(GraphEdge {
                        from: current_node,
                        to: new_node,
                        geometry: convert_from_geo(&sub_geom),
                        routes: current_route_ids.clone(),
                        original_shape_ids: edge.original_shape_ids.clone(),
                        weight: len,
                        original_edge_index: edge.original_edge_index,
                    });
                }
                current_node = new_node;
                current_frac = frac;
            }

            // Final segment
            if (1.0 - current_frac).abs() > 1e-6 {
                let sub_geom = get_sub_poly(&geom, current_frac, 1.0);
                let len = sub_geom.haversine_length();
                new_edges.push(GraphEdge {
                    from: current_node,
                    to: edge.to,
                    geometry: convert_from_geo(&sub_geom),
                    routes: current_route_ids,
                    original_shape_ids: edge.original_shape_ids.clone(),
                    weight: len,
                    original_edge_index: edge.original_edge_index,
                });
            }
        } else {
            new_edges.push(edge);
        }
    }

    Ok(new_edges)
}

fn get_point_on_line(line: &GeoLineString<f64>, frac: f64) -> GeoPoint<f64> {
    // Interpolate a point at fraction `frac` along the line
    let target_len = line.euclidean_length() * frac;
    let mut cur = 0.0;
    for seg in line.lines() {
        let len = seg.euclidean_length();
        if cur + len >= target_len {
            let t = if len > 0.0 {
                (target_len - cur) / len
            } else {
                0.0
            };
            let x = seg.start.x + (seg.end.x - seg.start.x) * t;
            let y = seg.start.y + (seg.end.y - seg.start.y) * t;
            return GeoPoint::new(x, y);
        }
        cur += len;
    }
    line.points().last().unwrap_or(GeoPoint::new(0.0, 0.0))
}

fn get_sub_poly(line: &GeoLineString<f64>, start: f64, end: f64) -> GeoLineString<f64> {
    crate::edges::get_line_substring(line, start, end).unwrap_or(GeoLineString::new(vec![]))
}

// Helper to fetch shapes per cluster using direction_pattern tables
async fn build_cluster_shape_map(
    pool: &CatenaryPostgresPool,
    clusters: &[StopCluster],
) -> Result<HashMap<usize, HashSet<String>>> {
    let mut map: HashMap<usize, HashSet<String>> = HashMap::new();
    let mut conn = pool.get().await?;

    // Use direction_pattern tables
    use catenary::schema::gtfs::direction_pattern::dsl as dp_dsl;
    use catenary::schema::gtfs::direction_pattern_meta::dsl as dpm_dsl;

    // 1. Map: StopID -> ClusterID
    let mut stop_to_cluster: HashMap<String, usize> = HashMap::new();
    let mut all_stop_ids: Vec<String> = Vec::new();

    for cluster in clusters {
        for stop in &cluster.stops {
            stop_to_cluster.insert(stop.gtfs_id.clone(), cluster.cluster_id);
            all_stop_ids.push(stop.gtfs_id.clone());
        }
    }

    // 2. Query direction_pattern to find pattern_ids for these stops
    let patterns: Vec<(String, String)> = dp_dsl::direction_pattern
        .filter(dp_dsl::stop_id.eq_any(&all_stop_ids))
        .select((dp_dsl::stop_id, dp_dsl::direction_pattern_id))
        .load::<(String, String)>(&mut conn)
        .await?;

    let mut pattern_to_cluster: HashMap<String, Vec<usize>> = HashMap::new();
    let mut all_pattern_ids = Vec::new();

    for (s_id, p_id) in patterns {
        if let Some(&c_id) = stop_to_cluster.get(&s_id) {
            pattern_to_cluster
                .entry(p_id.clone())
                .or_default()
                .push(c_id);
            all_pattern_ids.push(p_id);
        }
    }

    all_pattern_ids.sort();
    all_pattern_ids.dedup();

    // 3. Query direction_pattern_meta to get ShapeIDs for rail patterns
    // direction_pattern_meta has route_type directly, so we can filter in query
    let shape_map: Vec<(String, Option<String>)> = dpm_dsl::direction_pattern_meta
        .filter(dpm_dsl::direction_pattern_id.eq_any(&all_pattern_ids))
        .filter(dpm_dsl::route_type.eq_any(vec![Some(0i16), Some(1i16), Some(2i16)])) // Rail only
        .select((dpm_dsl::direction_pattern_id, dpm_dsl::gtfs_shape_id))
        .load::<(String, Option<String>)>(&mut conn)
        .await?;

    // 4. Populate the result map
    for (p_id, shape_opt) in shape_map {
        if let Some(shape_id) = shape_opt {
            if let Some(cluster_ids) = pattern_to_cluster.get(&p_id) {
                for &c_id in cluster_ids {
                    map.entry(c_id).or_default().insert(shape_id.clone());
                }
            }
        }
    }

    Ok(map)
}
