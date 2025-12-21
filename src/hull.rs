use delaunator::{EMPTY, Point as DPoint, triangulate};
use geo_types::{Coord, LineString, Point, Polygon};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::f64::consts::PI;

/// Represents an edge in the triangulation with its length.
#[derive(Debug, Clone, Copy)]
struct ScoredEdge {
    /// The half-edge index in the Delaunator structure
    index: usize,
    /// The calculated length (Haversine for geo)
    length: f64,
}

// Implement ordering for the max-heap (longest edges first)
impl PartialEq for ScoredEdge {
    fn eq(&self, other: &Self) -> bool {
        self.length == other.length
    }
}
impl Eq for ScoredEdge {}
impl PartialOrd for ScoredEdge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.length.partial_cmp(&other.length)
    }
}
impl Ord for ScoredEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        // Unwrap is safe for non-NaN floats.
        self.length
            .partial_cmp(&other.length)
            .unwrap_or(Ordering::Equal)
    }
}

/// Calculates the Haversine distance between two points (in metres).
fn haversine_distance(p1: Point, p2: Point) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in metres

    let lat1 = p1.y() * PI / 180.0;
    let lat2 = p2.y() * PI / 180.0;
    let d_lat = (p2.y() - p1.y()) * PI / 180.0;
    let d_lon = (p2.x() - p1.x()) * PI / 180.0;

    let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    R * c
}

/// Computes the Chi-shape (concave hull) of a set of Geo points.
///
/// # Arguments
/// * `points` - A vector of geo_types::Point (lon, lat).
/// * `length_threshold` - The absolute length threshold for edge erosion (in metres).
///     Edges longer than this will be eroded if topology allows.
pub fn chi_shape(points: &[Point], length_threshold: f64) -> Option<Polygon<f64>> {
    if points.len() < 3 {
        return None;
    }

    // 1. Convert to Delaunator points and triangulate
    // Note: Triangulation is performed in planar coordinates (lon/lat).
    // For very large areas, this projection distortion may affect topology,
    // but it is standard for characteristic shape algorithms.
    let d_points: Vec<DPoint> = points
        .iter()
        .map(|p| DPoint { x: p.x(), y: p.y() })
        .collect();

    let triangulation = triangulate(&d_points);
    let num_triangles = triangulation.triangles.len() / 3;

    // 2. Identify initial boundary (Convex Hull) and compute lengths
    let mut boundary_edges = HashSet::new(); // Stores half-edge indices
    let mut is_boundary_point = vec![false; points.len()];
    let mut heap = BinaryHeap::new();

    // Helper to get edge length
    let get_length = |start_idx: usize, end_idx: usize| -> f64 {
        haversine_distance(points[start_idx], points[end_idx])
    };

    // Find all edges on the hull (where opposite half-edge is EMPTY)
    // The halfedges array size is 3 * num_triangles
    for e in 0..triangulation.halfedges.len() {
        if triangulation.halfedges[e] == EMPTY {
            let start = triangulation.triangles[e];
            let next = delaunator::next_halfedge(e);
            let end = triangulation.triangles[next];

            let len = get_length(start, end);

            boundary_edges.insert(e);
            is_boundary_point[start] = true;
            is_boundary_point[end] = true;

            heap.push(ScoredEdge {
                index: e,
                length: len,
            });
        }
    }

    // 3. Erosion Process
    // We track removed triangles to avoid processing them.
    let mut removed_triangles = vec![false; num_triangles];

    while let Some(edge) = heap.pop() {
        // If edge is no longer on the boundary (already processed), skip
        if !boundary_edges.contains(&edge.index) {
            continue;
        }

        // Stop if the longest edge is short enough
        if edge.length <= length_threshold {
            break;
        }

        // Get the triangle this edge belongs to.
        // Since 'edge.index' is an exterior edge (halfedge=EMPTY),
        // we need the INTERIOR half-edge associated with this triangle side.
        // However, in Delaunator, 'edge.index' IS the half-edge index.
        // If halfedges[e] == EMPTY, then 'e' is the half-edge on the hull,
        // traversing the hull counter-clockwise?
        // Actually, Delaunator: "halfedges[e] returns the index of the opposite half-edge".
        // If it is EMPTY, there is no opposite, so 'e' is on the hull.
        // The triangle associated with 'e' is e / 3.

        let t_idx = edge.index / 3;

        if removed_triangles[t_idx] {
            continue;
        }

        // Identify the other two edges of this triangle
        let e_next = delaunator::next_halfedge(edge.index);
        let e_prev = delaunator::prev_halfedge(edge.index);

        // Check Regularity / Topology Constraints
        // 1. We must not remove a triangle if it has more than 1 edge on the boundary.
        //    (Removing a "bridge" or an "ear" with 2 boundary edges simplifies geometry
        //    but does not erode "inwards" in the chi-shape sense).
        let is_next_boundary = boundary_edges.contains(&e_next);
        let is_prev_boundary = boundary_edges.contains(&e_prev);

        if is_next_boundary || is_prev_boundary {
            continue;
        }

        // 2. The 3rd vertex (opposite to the current edge) must NOT be on the boundary.
        //    If it is, removing this triangle would pinch the polygon or split it.
        let opposite_vertex = triangulation.triangles[e_next]; // The vertex at start of e_next is end of e (no), it's the 3rd point.
        // Points in triangle t:
        // e starts at p1, goes to p2.
        // e_next starts at p2, goes to p3.
        // e_prev starts at p3, goes to p1.
        // The opposite vertex to e is the start of e_prev (or end of e_next) -> p3.
        let p3 = triangulation.triangles[e_prev];

        if is_boundary_point[p3] {
            continue;
        }

        // If constraints pass, remove the triangle
        removed_triangles[t_idx] = true;

        // Update Boundary
        // Remove the current edge
        boundary_edges.remove(&edge.index);

        // The other two edges (e_next, e_prev) now become boundary edges.
        // However, we must use the *opposite* half-edge indices for the queue/map
        // because those are the ones facing the "void" now?
        // Wait, e_next was internal. Its opposite was halfedges[e_next].
        // Now that T is removed, halfedges[e_next] is the new boundary edge facing OUT.
        // BUT, halfedges[e_next] belongs to the *neighbor* triangle.
        // We want to track the edges of the active polygon.
        // The edge 'e' was on the boundary.
        // We remove T.
        // The new boundary is formed by the neighbors of T.
        // The edge on the boundary is the one belonging to the neighbor triangle,
        // specifically the opposite of e_next and opposite of e_prev.

        let opp_next = triangulation.halfedges[e_next];
        let opp_prev = triangulation.halfedges[e_prev];

        // Handle case where neighbor might not exist (should not happen if logic is correct
        // and we don't remove bridges, but strictly e_next was internal => opp exists).
        if opp_next != EMPTY {
            boundary_edges.insert(opp_next);
            let start = triangulation.triangles[opp_next];
            let end = triangulation.triangles[delaunator::next_halfedge(opp_next)];
            heap.push(ScoredEdge {
                index: opp_next,
                length: get_length(start, end),
            });
        }

        if opp_prev != EMPTY {
            boundary_edges.insert(opp_prev);
            let start = triangulation.triangles[opp_prev];
            let end = triangulation.triangles[delaunator::next_halfedge(opp_prev)];
            heap.push(ScoredEdge {
                index: opp_prev,
                length: get_length(start, end),
            });
        }

        // Mark the new vertex as part of the boundary
        is_boundary_point[p3] = true;
    }

    // 5. Reconstruct the Polygon from boundary edges
    if boundary_edges.is_empty() {
        return None;
    }

    // Build adjacency map for reconstruction: start_node -> end_node
    let mut adj = HashMap::new();
    for &e_idx in &boundary_edges {
        let start = triangulation.triangles[e_idx];
        let next = delaunator::next_halfedge(e_idx);
        let end = triangulation.triangles[next];
        adj.insert(start, end);
    }

    // Trace the loop (Simple polygon guaranteed by logic)
    // Find a starting point
    let start_node = *adj.keys().next().unwrap();
    let mut current_node = start_node;
    let mut poly_points = Vec::new();

    loop {
        let p = points[current_node];
        poly_points.push(Coord { x: p.x(), y: p.y() });

        if let Some(&next_node) = adj.get(&current_node) {
            if next_node == start_node {
                break;
            }
            current_node = next_node;

            // Safety break for infinite loops (malformed graph)
            if poly_points.len() > points.len() {
                break;
            }
        } else {
            // Should not happen in a closed loop
            break;
        }
    }

    // Close the loop
    poly_points.push(poly_points[0]);

    Some(Polygon::new(LineString(poly_points), vec![]))
}
