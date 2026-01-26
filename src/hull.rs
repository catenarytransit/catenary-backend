use delaunator::{EMPTY, Point as DPoint, triangulate};
use geo::{Distance, Haversine};
use geo_types::{Coord, LineString, Point, Polygon};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

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
        Some(self.cmp(other))
    }
}
impl Ord for ScoredEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        self.length
            .total_cmp(&other.length)
            .then_with(|| self.index.cmp(&other.index))
    }
}

// / Calculates the Haversine distance between two points (in metres).
fn haversine_distance(p1: Point, p2: Point) -> f64 {
    Haversine.distance(p1, p2)
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

    // 0. Deduplicate points to prevent triangulation issues
    let mut unique_points = points.to_vec();
    unique_points.sort_by(|a, b| {
        a.x()
            .total_cmp(&b.x())
            .then_with(|| a.y().total_cmp(&b.y()))
    });
    unique_points.dedup_by(|a, b| (a.x() - b.x()).abs() < 1e-6 && (a.y() - b.y()).abs() < 1e-6);

    if unique_points.len() < 3 {
        return None;
    }

    let points = unique_points; // Shadow with unique

    // 1. Convert to Delaunator points and triangulate
    let d_points: Vec<DPoint> = points
        .iter()
        .map(|p| DPoint { x: p.x(), y: p.y() })
        .collect();

    let triangulation = triangulate(&d_points);
    let num_triangles = triangulation.triangles.len() / 3;

    // 2. Identify initial boundary (Convex Hull) and compute lengths
    let mut boundary_edges = HashSet::new(); // Stores half-edge indices
    let mut boundary_degrees = vec![0; points.len()]; // Track boundary incidence
    let mut heap = BinaryHeap::new();

    // Helper to get edge length
    let get_length = |start_idx: usize, end_idx: usize| -> f64 {
        haversine_distance(points[start_idx], points[end_idx])
    };

    // Find all edges on the hull (where opposite half-edge is EMPTY)
    for e in 0..triangulation.halfedges.len() {
        if triangulation.halfedges[e] == EMPTY {
            let start = triangulation.triangles[e];
            let next = delaunator::next_halfedge(e);
            let end = triangulation.triangles[next];

            let len = get_length(start, end);

            boundary_edges.insert(e);
            boundary_degrees[start] += 1;
            boundary_degrees[end] += 1;

            heap.push(ScoredEdge {
                index: e,
                length: len,
            });
        }
    }

    // 3. Erosion Process
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

        let t_idx = edge.index / 3;

        if removed_triangles[t_idx] {
            continue;
        }

        let e_next = delaunator::next_halfedge(edge.index);
        let e_prev = delaunator::prev_halfedge(edge.index);

        // Check Regularity / Topology Constraints
        // 1. "Ear" check: if triangle has other boundary edges, removing it simplifies boundary (makes it more convex usually)
        //    rather than eroding it inward.
        let is_next_boundary = boundary_edges.contains(&e_next);
        let is_prev_boundary = boundary_edges.contains(&e_prev);

        if is_next_boundary || is_prev_boundary {
            continue;
        }

        // 2. The 3rd vertex must NOT be on the boundary to avoid pinching.
        //    We check current boundary degrees.
        let p3 = triangulation.triangles[e_prev]; // Vertex opposite to `edge`

        if boundary_degrees[p3] > 0 {
            continue;
        }

        // If constraints pass, remove the triangle
        removed_triangles[t_idx] = true;

        // Update Boundary
        // Remove the current edge
        boundary_edges.remove(&edge.index);

        let u = triangulation.triangles[edge.index];
        let v = triangulation.triangles[delaunator::next_halfedge(edge.index)];
        boundary_degrees[u] -= 1;
        boundary_degrees[v] -= 1;

        // Add the two inner edges to the boundary
        let opp_next = triangulation.halfedges[e_next];
        let opp_prev = triangulation.halfedges[e_prev];

        // Ensure neighbours exist (they should if p3 wasn't boundary, but checked implicitly by triangulation)
        // If opp is EMPTY, it means we reached the void, but we shouldn't have unless p3 was boundary?
        // Actually, if opp is EMPTY, that edge is already boundary.
        // But we checked is_prev_boundary (which checks if e_prev is in set).
        // If e_prev was boundary, we continued.
        // So e_prev is NOT boundary.
        // Can e_prev be on hull but not in our set? No, we init set from hull.
        // So opp_next and opp_prev should strictly be internal (not EMPTY)
        // OR, they could be EMPTY if we started with a non-convex hull? No, we start with convex.

        if opp_next != EMPTY {
            boundary_edges.insert(opp_next);
            let s = triangulation.triangles[opp_next];
            let e = triangulation.triangles[delaunator::next_halfedge(opp_next)];
            boundary_degrees[s] += 1;
            boundary_degrees[e] += 1;

            heap.push(ScoredEdge {
                index: opp_next,
                length: get_length(s, e),
            });
        }

        if opp_prev != EMPTY {
            boundary_edges.insert(opp_prev);
            let s = triangulation.triangles[opp_prev];
            let e = triangulation.triangles[delaunator::next_halfedge(opp_prev)];
            boundary_degrees[s] += 1;
            boundary_degrees[e] += 1;

            heap.push(ScoredEdge {
                index: opp_prev,
                length: get_length(s, e),
            });
        }
    }

    // 5. Reconstruct the Polygon from boundary edges
    if boundary_edges.is_empty() {
        return None;
    }

    // Build adjacency map: start -> end
    let mut adj = HashMap::new();
    for &e_idx in &boundary_edges {
        let start = triangulation.triangles[e_idx];
        let next = delaunator::next_halfedge(e_idx);
        let end = triangulation.triangles[next];

        if adj.insert(start, end).is_some() {
            // Topology error: multiple edges starting from same vertex (pinched or broken loop)
            return None;
        }
    }

    // Trace the loop
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

            if poly_points.len() > points.len() {
                // Infinite loop guard
                return None;
            }
        } else {
            // Broken loop
            return None;
        }
    }

    // Close the loop
    poly_points.push(poly_points[0]);

    Some(Polygon::new(LineString(poly_points), vec![]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chi_shape_u_shape() {
        // Create a U-shape set of points.
        // (0,10)   (10,10)
        // | \     / |
        // |  (5,8)  |  <-- Helper point close to top to ensure triangulation connects here
        // (0,0)----(10,0)

        let points = vec![
            Point::new(0.0, 10.0),
            Point::new(0.0, 5.0),
            Point::new(0.0, 0.0),
            Point::new(5.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 5.0),
            Point::new(10.0, 10.0),
            // Add a point that allows the top edge to be a single triangle (0,10)-(10,10)-(5,8)
            Point::new(5.0, 8.0),
        ];

        // Theoretical straight line top edge ~1090km.
        // Threshold 800km should break it.
        // The new edges to (5,8) will be much shorter (~200-300km).
        let threshold = 800_000.0;

        let hull = chi_shape(&points, threshold).expect("Should return a hull");

        // Check if the long edge is gone.
        let mut has_long_edge = false;
        let ext_points: Vec<_> = hull.exterior().points().collect();
        for i in 0..ext_points.len() - 1 {
            let d = haversine_distance(ext_points[i], ext_points[i + 1]);
            if d > 1_000_000.0 {
                has_long_edge = true;
            }
        }

        println!("Hull points count: {}", ext_points.len());
        // Hull should now have more points than the basic convex wrapper
        assert!(
            !has_long_edge,
            "Hull should not contain the long top edge > 1000km"
        );
    }

    #[test]
    fn test_chi_shape_convexity() {
        // A simple square with center.
        let points = vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(5.0, 5.0),
        ];

        // High threshold -> Convex Hull
        let hull = chi_shape(&points, 5_000_000.0).unwrap();
        // Convex hull of square+center is just the square (5 points, closed).
        assert_eq!(hull.exterior().points().count(), 5);

        // Low threshold -> Star/Dented shape
        // Edges of square ~1111km. Threshold 800km.
        // Should erode at least one side to reach (5,5).
        let hull_star = chi_shape(&points, 800_000.0).unwrap();
        let points_vec: Vec<_> = hull_star.exterior().points().collect();
        let has_center = points_vec
            .iter()
            .any(|p| (p.x() - 5.0).abs() < 0.1 && (p.y() - 5.0).abs() < 0.1);
        assert!(
            has_center,
            "Should erode to include center point with low threshold"
        );
    }
}
