use geo::prelude::*;
use geo::{CoordsIter, EuclideanDistance, EuclideanLength, LineString, Point};

/// Loom's AVERAGING_STEP constant.
/// For coordinate-agnostic operation, we use a fixed number of sample points
/// rather than a distance-based step. This works for both lat/lng and Web Mercator.
const MIN_AVERAGING_POINTS: usize = 20;
const MAX_AVERAGING_POINTS: usize = 100;

/// Get a point at a normalised distance (0.0 to 1.0) along a LineString.
pub fn get_point_at_ratio(line: &LineString<f64>, ratio: f64) -> Option<Point<f64>> {
    let total_len = line.euclidean_length();
    if total_len == 0.0 {
        return line.points().next();
    }

    let target_dist = total_len * ratio.clamp(0.0, 1.0);
    let mut current_dist = 0.0;

    for segment in line.lines() {
        let (p1, p2) = segment.points();
        let seg_len = p1.euclidean_distance(&p2);
        if current_dist + seg_len >= target_dist {
            let remaining = target_dist - current_dist;
            let segment_ratio = if seg_len > 0.0 {
                remaining / seg_len
            } else {
                0.0
            };

            let x = p1.x() + (p2.x() - p1.x()) * segment_ratio;
            let y = p1.y() + (p2.y() - p1.y()) * segment_ratio;
            return Some(Point::new(x, y));
        }
        current_dist += seg_len;
    }

    line.points().last()
}

/// Check if `inner` is "contained" within `outer` with some tolerance.
/// Matches loom's behaviour: every point of inner within threshold distance of outer.
pub fn is_contained(inner: &LineString<f64>, outer: &LineString<f64>, threshold: f64) -> bool {
    inner
        .points()
        .all(|p| outer.euclidean_distance(&p) <= threshold)
}

/// Average multiple LineStrings into one.
///
/// Implements loom's PolyLine::average algorithm:
/// - Uses distance-based step size (AVERAGING_STEP / longestLength)
pub fn average_polylines(lines: &[LineString<f64>]) -> LineString<f64> {
    average_polylines_weighted(lines, None)
}

/// Weighted averaging of polylines.
///
/// If weights provided, each line's contribution is scaled by its weight.
/// Weights should correspond 1:1 with lines (e.g. route_count² per edge as in loom).
pub fn average_polylines_weighted(
    lines: &[LineString<f64>],
    weights: Option<&[f64]>,
) -> LineString<f64> {
    if lines.is_empty() {
        return LineString::new(vec![]);
    }
    if lines.len() == 1 {
        return lines[0].clone();
    }

    // Fast path for simple two-line averaging (as in loom)
    let weighted = weights.is_some() && weights.unwrap().len() == lines.len();
    if !weighted && lines.len() == 2 && lines[0].0.len() == 2 && lines[1].0.len() == 2 {
        let a = &lines[0].0;
        let b = &lines[1].0;
        return LineString::from(vec![
            ((a[0].x + b[0].x) / 2.0, (a[0].y + b[0].y) / 2.0),
            ((a[1].x + b[1].x) / 2.0, (a[1].y + b[1].y) / 2.0),
        ]);
    }

    // Find longest line length for step size calculation
    let max_len = lines
        .iter()
        .map(|l| l.euclidean_length())
        .fold(0.0_f64, f64::max);

    if max_len == 0.0 {
        return lines[0].clone();
    }

    // Use a fixed number of sample points based on polyline complexity
    // More complex polylines (more points) get more samples
    let max_points = lines.iter().map(|l| l.0.len()).max().unwrap_or(2);
    let num_samples = max_points.max(MIN_AVERAGING_POINTS).min(MAX_AVERAGING_POINTS);
    let step_size = 1.0 / (num_samples as f64 - 1.0).max(1.0);

    // Calculate total weight
    let total_weight: f64 = if let Some(w) = weights {
        w.iter().sum()
    } else {
        lines.len() as f64
    };

    let mut points = Vec::new();
    let mut ratio = 0.0;

    while ratio <= 1.0 {
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;

        for (i, line) in lines.iter().enumerate() {
            if let Some(p) = get_point_at_ratio(line, ratio) {
                let w = if let Some(weights) = weights {
                    weights.get(i).copied().unwrap_or(1.0)
                } else {
                    1.0
                };
                sum_x += p.x() * w;
                sum_y += p.y() * w;
            }
        }

        if total_weight > 0.0 {
            points.push((sum_x / total_weight, sum_y / total_weight));
        }

        ratio += step_size;
    }

    // Ensure we include the final point at ratio=1.0
    if ratio - step_size < 1.0 {
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        for (i, line) in lines.iter().enumerate() {
            if let Some(p) = get_point_at_ratio(line, 1.0) {
                let w = if let Some(weights) = weights {
                    weights.get(i).copied().unwrap_or(1.0)
                } else {
                    1.0
                };
                sum_x += p.x() * w;
                sum_y += p.y() * w;
            }
        }
        if total_weight > 0.0 {
            points.push((sum_x / total_weight, sum_y / total_weight));
        }
    }

    LineString::from(points)
}

/// Get a line segment orthogonal to the polyline at a specific distance from the start.
/// Returns a line of length 2 * `width` centered at the point on the polyline.
pub fn get_ortho_line_at_dist(
    line_string: &LineString<f64>,
    dist: f64,
    width: f64,
) -> Option<geo::Line<f64>> {
    if line_string.0.is_empty() {
        return None;
    }

    let mut traveled = 0.0;
    for window in line_string.0.windows(2) {
        let p1 = Point::from(window[0]);
        let p2 = Point::from(window[1]);
        #[allow(deprecated)]
        let seg_len = p1.haversine_distance(&p2);

        if traveled + seg_len >= dist {
            let remaining = dist - traveled;
            let ratio = if seg_len > 0.0 {
                remaining / seg_len
            } else {
                0.0
            };

            let dx = p2.x() - p1.x();
            let dy = p2.y() - p1.y();
            let p_center = Point::new(p1.x() + dx * ratio, p1.y() + dy * ratio);

            let len = (dx * dx + dy * dy).sqrt();
            let (nx, ny) = if len > 0.0 {
                (-dy / len, dx / len)
            } else {
                (0.0, 1.0)
            };

            let start = Point::new(p_center.x() + nx * width, p_center.y() + ny * width);
            let end = Point::new(p_center.x() - nx * width, p_center.y() - ny * width);

            return Some(geo::Line::new(start, end));
        }
        traveled += seg_len;
    }

    // Fallback to last point
    if let Some(window) = line_string.0.windows(2).last() {
        let p1 = Point::from(window[0]);
        let p2 = Point::from(window[1]);
        let dx = p2.x() - p1.x();
        let dy = p2.y() - p1.y();
        let p_center = p2;

        let len = (dx * dx + dy * dy).sqrt();
        let (nx, ny) = if len > 0.0 {
            (-dy / len, dx / len)
        } else {
            (0.0, 1.0)
        };
        let start = Point::new(p_center.x() + nx * width, p_center.y() + ny * width);
        let end = Point::new(p_center.x() - nx * width, p_center.y() - ny * width);

        return Some(geo::Line::new(start, end));
    }

    None
}

/// Find intersection points between two line strings
pub fn intersection(l1: &LineString<f64>, l2: &LineString<f64>) -> Vec<Point<f64>> {
    let mut points = Vec::new();

    for w1 in l1.0.windows(2) {
        let s1 = geo::Line::new(geo::Coord::from(w1[0]), geo::Coord::from(w1[1]));
        for w2 in l2.0.windows(2) {
            let s2 = geo::Line::new(geo::Coord::from(w2[0]), geo::Coord::from(w2[1]));
            if let Some(p) = line_intersection(s1, s2) {
                points.push(p);
            }
        }
    }
    points
}

fn line_intersection(l1: geo::Line<f64>, l2: geo::Line<f64>) -> Option<Point<f64>> {
    let x1 = l1.start.x;
    let y1 = l1.start.y;
    let x2 = l1.end.x;
    let y2 = l1.end.y;

    let x3 = l2.start.x;
    let y3 = l2.start.y;
    let x4 = l2.end.x;
    let y4 = l2.end.y;

    let denom = (y4 - y3) * (x2 - x1) - (x4 - x3) * (y2 - y1);
    if denom.abs() < 1e-10 {
        return None;
    }

    let ua = ((x4 - x3) * (y1 - y3) - (y4 - y3) * (x1 - x3)) / denom;
    let ub = ((x2 - x1) * (y1 - y3) - (y2 - y1) * (x1 - x3)) / denom;

    if ua >= 0.0 && ua <= 1.0 && ub >= 0.0 && ub <= 1.0 {
        return Some(Point::new(x1 + ua * (x2 - x1), y1 + ua * (y2 - y1)));
    }

    None
}

/// Subtract two 2D vectors
pub fn sub_2d(a: [f64; 2], b: [f64; 2]) -> [f64; 2] {
    [a[0] - b[0], a[1] - b[1]]
}

/// Normalize a 2D vector
pub fn normalize_2d(v: [f64; 2]) -> [f64; 2] {
    let len = (v[0] * v[0] + v[1] * v[1]).sqrt();
    if len > 1e-9 {
        [v[0] / len, v[1] / len]
    } else {
        [0.0, 0.0]
    }
}

/// Dot product of two 2D vectors
pub fn dot_product_2d(a: [f64; 2], b: [f64; 2]) -> f64 {
    a[0] * b[0] + a[1] * b[1]
}

/// Project a point onto a polyline and return the cumulative distance along the line of the closest point.
pub fn project_point_to_polyline(
    point: &geo::Coord, 
    polyline: &[geo::Coord]
) -> f64 {
    if polyline.len() < 2 {
        return 0.0;
    }
    
    let mut min_dist_sq = f64::MAX;
    let mut best_dist_along = 0.0;
    let mut current_dist_along = 0.0;
    
    for i in 0..polyline.len() - 1 {
        let p1 = polyline[i];
        let p2 = polyline[i+1];
        
        let seg_dx = p2.x - p1.x;
        let seg_dy = p2.y - p1.y;
        let seg_len_sq = seg_dx*seg_dx + seg_dy*seg_dy;
        let seg_len = seg_len_sq.sqrt();
        
        if seg_len_sq < 1e-9 {
            // zero length segment, skip
             current_dist_along += seg_len;
             continue;
        }
        
        // Project vector (point - p1) onto (p2 - p1)
        let t = ((point.x - p1.x) * seg_dx + (point.y - p1.y) * seg_dy) / seg_len_sq;
        
        // Clamp t to segment [0, 1]
        let t_clamped = t.max(0.0).min(1.0);
        
        let proj_x = p1.x + t_clamped * seg_dx;
        let proj_y = p1.y + t_clamped * seg_dy; // Fixed calculation reuse
        
        let dx = point.x - proj_x;
        let dy = point.y - proj_y;
        let dist_sq = dx*dx + dy*dy;
        
        if dist_sq < min_dist_sq {
            min_dist_sq = dist_sq;
            best_dist_along = current_dist_along + t_clamped * seg_len;
        }
        
        current_dist_along += seg_len;
    }
    
    best_dist_along
}

/// Calculate the minimum Euclidean distance from a point to a polyline
pub fn distance_point_to_polyline(
    point: &geo::Coord, 
    polyline: &[geo::Coord]
) -> f64 {
    if polyline.len() < 2 {
        return f64::MAX;
    }
    
    let mut min_dist_sq = f64::MAX;
    
    for i in 0..polyline.len() - 1 {
        let p1 = polyline[i];
        let p2 = polyline[i+1];
        
        let seg_dx = p2.x - p1.x;
        let seg_dy = p2.y - p1.y;
        let seg_len_sq = seg_dx*seg_dx + seg_dy*seg_dy;
        
        if seg_len_sq < 1e-9 {
             let dx = point.x - p1.x;
             let dy = point.y - p1.y;
             min_dist_sq = min_dist_sq.min(dx*dx + dy*dy);
             continue;
        }
        
        // Project vector (point - p1) onto (p2 - p1)
        let t = ((point.x - p1.x) * seg_dx + (point.y - p1.y) * seg_dy) / seg_len_sq;
        let t_clamped = t.max(0.0).min(1.0);
        
        let proj_x = p1.x + t_clamped * seg_dx;
        let proj_y = p1.y + t_clamped * seg_dy;
        
        let dx = point.x - proj_x;
        let dy = point.y - proj_y;
        min_dist_sq = min_dist_sq.min(dx*dx + dy*dy);
    }
    
    min_dist_sq.sqrt()
}

/// Calculate the length of a polyline defined by array points
pub fn polyline_length(path: &[[f64; 2]]) -> f64 {
    let mut len = 0.0;
    for i in 0..path.len().saturating_sub(1) {
        let dx = path[i+1][0] - path[i][0];
        let dy = path[i+1][1] - path[i][1];
        len += (dx*dx + dy*dy).sqrt();
    }
    len
}

/// Sample a point along a polyline at a specific distance
pub fn sample_along_polyline_f64(path: &[[f64; 2]], target_dist: f64) -> [f64; 2] {
    if path.is_empty() { return [0.0, 0.0]; }
    
    let mut accum = 0.0;
    for i in 0..path.len().saturating_sub(1) {
        let p1 = path[i];
        let p2 = path[i+1];
        let dx = p2[0] - p1[0];
        let dy = p2[1] - p1[1];
        let seg_len = (dx*dx + dy*dy).sqrt();
        
        if accum + seg_len >= target_dist {
            let remain = target_dist - accum;
            let t = if seg_len > 1e-9 { remain / seg_len } else { 0.0 };
            return [
                p1[0] + dx * t,
                p1[1] + dy * t,
            ];
        }
        accum += seg_len;
    }
    
    *path.last().unwrap()
}

/// Project a point onto a polyline and return distance along path
pub fn project_point_to_polyline_f64(point: [f64; 2], path: &[[f64; 2]]) -> f64 {
    if path.len() < 2 { return 0.0; }
    
    let mut min_dist_sq = f64::MAX;
    let mut best_dist_along = 0.0;
    let mut current_dist_along = 0.0;
    
    for i in 0..path.len() - 1 {
        let p1 = path[i];
        let p2 = path[i+1];
        
        let seg_dx = p2[0] - p1[0];
        let seg_dy = p2[1] - p1[1];
        let seg_len_sq = seg_dx*seg_dx + seg_dy*seg_dy;
        let seg_len = seg_len_sq.sqrt();
        
        if seg_len_sq < 1e-9 {
             current_dist_along += seg_len;
             continue;
        }
        
        let t = ((point[0] - p1[0]) * seg_dx + (point[1] - p1[1]) * seg_dy) / seg_len_sq;
        let t_clamped = t.max(0.0).min(1.0);
        
        let proj_x = p1[0] + t_clamped * seg_dx;
        let proj_y = p1[1] + t_clamped * seg_dy;
        
        let dx = point[0] - proj_x;
        let dy = point[1] - proj_y;
        let dist_sq = dx*dx + dy*dy;
        
        if dist_sq < min_dist_sq {
            min_dist_sq = dist_sq;
            best_dist_along = current_dist_along + t_clamped * seg_len;
        }
        
        current_dist_along += seg_len;
    }
    
    best_dist_along
}
