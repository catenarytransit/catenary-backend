use crate::edges::{GraphEdge, NodeId, convert_to_geo};
use anyhow::Result;
use catenary::graph_formats::TurnRestriction;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString};
use postgis_diesel::types::{LineString, Point};
use std::collections::{HashMap, HashSet};

/// Threshold for deciding if two projected intervals are "consecutive" (in meters).
/// If the gap between end of `u` and start of `v` on the shape is larger than this,
/// we assume they do not connect for this route.
const MAX_GAP_METERS: f64 = 50.0;

/// Map from (chateau, route_id) to list of Shape Geometries
pub type RouteShapeMap = HashMap<(String, String), Vec<GeoLineString<f64>>>;

pub async fn infer_from_db(
    pool: &CatenaryPostgresPool,
    edges: &[GraphEdge],
) -> Result<Vec<TurnRestriction>> {
    println!("Loading shapes for restriction inference...");
    let mut conn = pool.get().await?;

    use catenary::schema::gtfs::direction_pattern_meta::dsl as meta_dsl;
    use catenary::schema::gtfs::shapes::dsl as user_shapes_dsl;

    // Load relationships: Pattern -> Shape, Pattern -> Route
    let metas = meta_dsl::direction_pattern_meta
        .load::<catenary::models::DirectionPatternMeta>(&mut conn)
        .await?;

    let mut pattern_to_shape: HashMap<String, String> = HashMap::new();
    let mut pattern_to_route: HashMap<String, (String, String)> = HashMap::new(); // (chateau, route_id)
    let mut relevant_shape_ids = HashSet::new();

    for m in metas {
        if let Some(sid) = m.gtfs_shape_id {
            pattern_to_shape.insert(m.direction_pattern_id.clone(), sid.clone());
            relevant_shape_ids.insert(sid);
        }
        if let Some(rid) = m.route_id {
            pattern_to_route.insert(
                m.direction_pattern_id.clone(),
                (m.chateau.clone(), rid.to_string()),
            );
        }
    }

    // Load Shapes
    let loaded_shapes = user_shapes_dsl::shapes
        //.filter(user_shapes_dsl::shape_id.eq_any(&relevant_shape_ids)) // Too many params?
        // If too many, maybe load all or chunk. For now assume it fits or diesel handles it.
        // Actually relevant_shape_ids might be huge.
        // Let's rely on routes present in EDGES?
        // Only load shapes for routes that are in `edges`.
        .filter(user_shapes_dsl::stop_to_stop_generated.eq(false))
        .filter(user_shapes_dsl::route_type.eq_any(vec![0, 1, 2])) // Rail only?
        .load::<catenary::models::Shape>(&mut conn)
        .await?;

    let mut shape_map: HashMap<String, catenary::models::Shape> = HashMap::new();
    for s in loaded_shapes {
        shape_map.insert(s.shape_id.clone(), s);
    }

    // Build RouteShapeMap
    let mut route_shapes: RouteShapeMap = HashMap::new();

    for (pat_id, shape_id) in &pattern_to_shape {
        if let Some(route_key) = pattern_to_route.get(pat_id) {
            if let Some(shape) = shape_map.get(shape_id) {
                let geo = convert_to_geo(&shape.linestring);
                route_shapes.entry(route_key.clone()).or_default().push(geo);
            }
        }
    }

    Ok(infer_restrictions(edges, &route_shapes))
}

pub fn infer_restrictions(
    edges: &[GraphEdge],
    route_shapes: &RouteShapeMap,
) -> Vec<TurnRestriction> {
    println!("Inferring turn restrictions...");
    let mut restrictions = Vec::new();

    // 1. Build Adjacency
    let mut adj_out: HashMap<NodeId, Vec<usize>> = HashMap::new();
    let mut adj_in: HashMap<NodeId, Vec<usize>> = HashMap::new();

    for (i, edge) in edges.iter().enumerate() {
        adj_out.entry(edge.from).or_default().push(i);
        adj_in.entry(edge.to).or_default().push(i);
    }

    // 2. Iterate over all nodes to find candidate pairs (u -> v)
    // We only care about nodes that have at least 1 in and 1 out.
    let all_nodes: HashSet<NodeId> = adj_in.keys().chain(adj_out.keys()).cloned().collect();

    let mut restrictions_count = 0;

    for node in all_nodes {
        let incoming = adj_in.get(&node);
        let outgoing = adj_out.get(&node);

        if incoming.is_none() || outgoing.is_none() {
            continue;
        }

        let incoming_indices = incoming.unwrap();
        let outgoing_indices = outgoing.unwrap();

        for &u_idx in incoming_indices {
            for &v_idx in outgoing_indices {
                if u_idx == v_idx {
                    continue; // u-turn on same edge? (GraphEdge is directed)
                }

                let u = &edges[u_idx];
                let v = &edges[v_idx];

                // Find shared routes
                let u_routes: HashSet<&(String, String)> = u.route_ids.iter().collect();
                let v_routes: HashSet<&(String, String)> = v.route_ids.iter().collect();

                let shared_routes: Vec<&(String, String)> =
                    u_routes.intersection(&v_routes).cloned().collect();

                if shared_routes.is_empty() {
                    continue;
                }

                let u_geom = convert_to_geo(&u.geometry);
                let v_geom = convert_to_geo(&v.geometry);

                // For each shared route, check validity
                for route_key in shared_routes {
                    if let Some(shapes) = route_shapes.get(route_key) {
                        let mut valid_any = false;

                        // Check if ANY shape creates a valid connection
                        for shape in shapes {
                            if is_consecutive(&u_geom, &v_geom, shape) {
                                valid_any = true;
                                break;
                            }
                        }

                        if !valid_any {
                            // If NO shape allows this transition, it is restricted.
                            // Note: We only restrict if shapes exist. If no shapes found, we assume valid (safe).
                            restrictions.push(TurnRestriction {
                                from_edge_index: u_idx,
                                to_edge_index: v_idx,
                                route_id: route_key.clone(),
                            });
                            restrictions_count += 1;
                        }
                    }
                }
            }
        }
    }

    println!("Inferred {} turn restrictions.", restrictions_count);
    restrictions
}

fn is_consecutive(
    u_geom: &GeoLineString<f64>,
    v_geom: &GeoLineString<f64>,
    shape: &GeoLineString<f64>,
) -> bool {
    // 1. Project u and v onto shape
    // We get a range [min_frac, max_frac] for each
    let range_u = get_projection_range(u_geom, shape);
    let range_v = get_projection_range(v_geom, shape);

    if range_u.is_none() || range_v.is_none() {
        // If one doesn't project well, we can't judge. Assume valid to avoid false positives?
        // Or assume invalid?
        // If an edge belongs to a route, it SHOULD match the shape.
        return true;
    }

    let (min_u, max_u) = range_u.unwrap();
    let (min_v, max_v) = range_v.unwrap();

    // 2. Check overlap or consecutiveness
    // Transition u -> v means we expect shape flow: start -> u -> v -> end
    // So max_u should be close to min_v.

    // Calculate distance on shape in meters
    // Approximate: Fraction difference * Length
    #[allow(deprecated)]
    let shape_len = shape.haversine_length();

    let gap_frac = min_v - max_u;
    let gap_meters = gap_frac * shape_len;

    // We allow a small distinct gap (or overlap)
    // Overlap: min_v < max_u. This is fine (merging/duplicate geometry).
    // Gap: min_v > max_u.

    if gap_meters > MAX_GAP_METERS {
        // Large gap forward -> Invalid
        return false;
    }

    if gap_meters < -MAX_GAP_METERS {
        // Large gap backward (v is BEFORE u) -> Invalid
        return false;
    }

    true
}

fn get_projection_range(
    segment: &GeoLineString<f64>,
    shape: &GeoLineString<f64>,
) -> Option<(f64, f64)> {
    if segment.0.is_empty() || shape.0.is_empty() {
        return None;
    }

    let coords = &segment.0;

    // Use first and last points
    let start_p = &coords[0];
    let end_p = coords.last().unwrap(); // Safe as checked is_empty

    let points = vec![start_p, end_p];

    let mut min_frac = 1.0;
    let mut max_frac = 0.0;
    let mut found = false;

    // Using `line_locate_point` from `geo`?
    // `geo` crate doesn't have `line_locate_point` built-in on LineString directly in all versions?
    // It is available via `LineLocatePoint` trait if enabled?
    // Let's use `closest_point` dist?
    // Ideally we assume `geometry_utils` or similar has projection.
    // Let's implement a simple projection based on nearest point.

    use geo::ClosestPoint;
    #[allow(deprecated)]
    use geo::EuclideanLength; // For fractional distance along lines

    let total_len = shape.lines().map(|l| l.euclidean_length()).sum::<f64>();
    if total_len == 0.0 {
        return None;
    }

    for p in points {
        // Find closest point on shape
        // This is O(N) per point.
        let p_geo = geo::Point::from(*p);

        // Manual projection since `line_locate_point` is not standard geo trait?
        // Actually `edges.rs` used `line_locate_point`.
        // Let's check `edges.rs` imports: `use geo::prelude::*;`
        // `edges.rs` call: `let start_frac = geo_shape.line_locate_point(&start_pt).unwrap_or(0.0);`
        // This implies `line_locate_point` is available on GeoLineString.
        // It's likely from `geo::algorithm::line_locate_point::LineLocatePoint`.

        let frac = shape.line_locate_point(&p_geo).unwrap_or(0.0);
        if frac < min_frac {
            min_frac = frac;
        }
        if frac > max_frac {
            max_frac = frac;
        }
        found = true;
    }

    if found {
        Some((min_frac, max_frac))
    } else {
        None
    }
}
