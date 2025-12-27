use geo::prelude::*;
use geo::{CoordsIter, EuclideanDistance, EuclideanLength, LineString, Point};

/// Loom's AVERAGING_STEP constant (20 metres in WebMercator).
/// For lat/lon, we approximate 1 degree ≈ 111,111 metres at the equator.
const AVERAGING_STEP_DEGREES: f64 = 20.0 / 111_111.0;

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

    // Distance-based step size matching loom's AVERAGING_STEP
    // Ensure we generate between 10 and 100 points to avoid excessive output
    let step_size = (AVERAGING_STEP_DEGREES / max_len).clamp(0.01, 0.1);

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
