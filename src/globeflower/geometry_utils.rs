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
