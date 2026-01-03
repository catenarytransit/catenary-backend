use geo::{
    Coord, EuclideanDistance, EuclideanLength, HaversineDistance, HaversineLength, Line,
    LineInterpolatePoint, LineLocatePoint, LineString, Point,
};

/// Calculate Hausdorff distance between two polylines in meters.
/// Uses haversine distance for geographic coordinates.
pub fn hausdorff_distance(a: &[(f64, f64)], b: &[(f64, f64)]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return f64::INFINITY;
    }

    let dist_a_to_b = a
        .iter()
        .map(|&(lon, lat)| {
            let p = Point::new(lon, lat);
            b.iter()
                .map(|&(lon2, lat2)| p.haversine_distance(&Point::new(lon2, lat2)))
                .fold(f64::INFINITY, f64::min)
        })
        .fold(0.0f64, f64::max);

    let dist_b_to_a = b
        .iter()
        .map(|&(lon, lat)| {
            let p = Point::new(lon, lat);
            a.iter()
                .map(|&(lon2, lat2)| p.haversine_distance(&Point::new(lon2, lat2)))
                .fold(f64::INFINITY, f64::min)
        })
        .fold(0.0f64, f64::max);

    dist_a_to_b.max(dist_b_to_a)
}

/// Calculate the length of a polyline in meters using haversine formula.
pub fn polyline_length(coords: &[(f64, f64)]) -> f64 {
    if coords.len() < 2 {
        return 0.0;
    }
    let ls: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();
    ls.haversine_length()
}

/// Sample points along a polyline at fixed intervals.
/// Returns positions as (lon, lat) tuples.
pub fn sample_along_polyline(coords: &[(f64, f64)], interval_m: f64) -> Vec<(f64, f64)> {
    if coords.is_empty() {
        return vec![];
    }
    if coords.len() == 1 {
        return vec![coords[0]];
    }

    let total_len = polyline_length(coords);
    if total_len <= 0.0 {
        return vec![coords[0]];
    }

    let num_samples = (total_len / interval_m).ceil() as usize;
    let mut samples = Vec::with_capacity(num_samples + 1);

    let ls: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();

    for i in 0..=num_samples {
        let fraction = (i as f64 * interval_m / total_len).min(1.0);
        if let Some(point) = ls.line_interpolate_point(fraction) {
            samples.push((point.x(), point.y()));
        }
    }

    samples
}

/// Project a point onto a polyline, returning (distance_along, distance_to_line).
/// Returns None if the polyline is empty.
pub fn project_point_to_polyline(
    point: (f64, f64),
    coords: &[(f64, f64)],
) -> Option<(f64, f64, (f64, f64))> {
    if coords.len() < 2 {
        if coords.len() == 1 {
            let dist = Point::new(point.0, point.1)
                .haversine_distance(&Point::new(coords[0].0, coords[0].1));
            return Some((0.0, dist, coords[0]));
        }
        return None;
    }

    let ls: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();
    let p = Point::new(point.0, point.1);

    let fraction = ls.line_locate_point(&p)?;
    let projected = ls.line_interpolate_point(fraction)?;
    let distance_along = fraction * polyline_length(coords);
    let distance_to_line = p.haversine_distance(&projected);

    Some((
        distance_along,
        distance_to_line,
        (projected.x(), projected.y()),
    ))
}

/// Calculate the bearing angle (in degrees, 0-360) from point a to point b.
pub fn bearing(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (lon1, lat1) = (a.0.to_radians(), a.1.to_radians());
    let (lon2, lat2) = (b.0.to_radians(), b.1.to_radians());

    let dlon = lon2 - lon1;
    let x = dlon.cos() * lat2.sin();
    let y = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();

    (x.atan2(y).to_degrees() + 360.0) % 360.0
}

/// Calculate the average bearing of a polyline (from start to end).
pub fn polyline_bearing(coords: &[(f64, f64)]) -> Option<f64> {
    if coords.len() < 2 {
        return None;
    }
    Some(bearing(coords[0], *coords.last()?))
}

/// Calculate the absolute angular difference between two bearings (0-180 degrees).
pub fn bearing_difference(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs() % 360.0;
    if diff > 180.0 { 360.0 - diff } else { diff }
}

/// Calculate weighted average centerline from multiple parallel polylines.
/// Weights can represent line counts or importance.
pub fn weighted_average_centerline(
    polylines: &[(&[(f64, f64)], f64)], // (coords, weight)
    sample_interval: f64,
) -> Vec<(f64, f64)> {
    if polylines.is_empty() {
        return vec![];
    }
    if polylines.len() == 1 {
        return polylines[0].0.to_vec();
    }

    // Find the reference polyline (longest one)
    let (ref_idx, _) = polylines
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| {
            polyline_length(a.0)
                .partial_cmp(&polyline_length(b.0))
                .unwrap()
        })
        .unwrap();

    let ref_coords = polylines[ref_idx].0;
    let ref_len = polyline_length(ref_coords);
    let num_samples = (ref_len / sample_interval).ceil() as usize;

    let mut centerline = Vec::with_capacity(num_samples + 1);
    let total_weight: f64 = polylines.iter().map(|(_, w)| w).sum();

    for i in 0..=num_samples {
        let fraction = (i as f64) / (num_samples as f64);
        let ref_ls: LineString = ref_coords.iter().map(|&(x, y)| Coord { x, y }).collect();

        if let Some(ref_point) = ref_ls.line_interpolate_point(fraction) {
            let ref_p = (ref_point.x(), ref_point.y());

            // Find corresponding points on other polylines and average
            let mut sum_lon = ref_p.0 * polylines[ref_idx].1;
            let mut sum_lat = ref_p.1 * polylines[ref_idx].1;
            let mut sum_weight = polylines[ref_idx].1;

            for (idx, (coords, weight)) in polylines.iter().enumerate() {
                if idx == ref_idx {
                    continue;
                }
                if let Some((_, _, proj)) = project_point_to_polyline(ref_p, coords) {
                    sum_lon += proj.0 * weight;
                    sum_lat += proj.1 * weight;
                    sum_weight += weight;
                }
            }

            if sum_weight > 0.0 {
                centerline.push((sum_lon / sum_weight, sum_lat / sum_weight));
            }
        }
    }

    centerline
}

/// Calculate the overlap length between two polylines as a fraction (0.0 to 1.0).
/// This measures how much of the shorter polyline overlaps with the longer one.
pub fn overlap_ratio(a: &[(f64, f64)], b: &[(f64, f64)], threshold_m: f64) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let len_a = polyline_length(a);
    let len_b = polyline_length(b);
    let shorter = if len_a < len_b { a } else { b };
    let longer = if len_a < len_b { b } else { a };

    let samples = sample_along_polyline(shorter, 5.0);
    if samples.is_empty() {
        return 0.0;
    }

    let overlapping = samples
        .iter()
        .filter(|&&p| {
            if let Some((_, dist, _)) = project_point_to_polyline(p, longer) {
                dist <= threshold_m
            } else {
                false
            }
        })
        .count();

    overlapping as f64 / samples.len() as f64
}

/// Simplify a polyline using Ramer-Douglas-Peucker algorithm.
/// Epsilon is the maximum perpendicular distance in meters.
pub fn simplify_polyline(coords: &[(f64, f64)], epsilon_m: f64) -> Vec<(f64, f64)> {
    if coords.len() < 3 {
        return coords.to_vec();
    }

    // Convert epsilon from meters to approximate degrees at mid-latitude
    let mid_lat = coords.iter().map(|c| c.1).sum::<f64>() / coords.len() as f64;
    let meters_per_degree = 111320.0 * mid_lat.to_radians().cos();
    let epsilon_deg = epsilon_m / meters_per_degree;

    let ls: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();
    let simplified = geo::Simplify::simplify(&ls, epsilon_deg);

    simplified.coords().map(|c| (c.x, c.y)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polyline_length() {
        // ~111km at equator for 1 degree
        let coords = [(0.0, 0.0), (1.0, 0.0)];
        let len = polyline_length(&coords);
        assert!((len - 111320.0).abs() < 1000.0); // Within 1km
    }

    #[test]
    fn test_bearing_difference() {
        assert!((bearing_difference(10.0, 20.0) - 10.0).abs() < 0.001);
        assert!((bearing_difference(350.0, 10.0) - 20.0).abs() < 0.001);
        assert!((bearing_difference(0.0, 180.0) - 180.0).abs() < 0.001);
    }

    #[test]
    fn test_sample_along_polyline() {
        let coords = [(0.0, 0.0), (0.01, 0.0)]; // ~1.1km
        let samples = sample_along_polyline(&coords, 500.0);
        assert!(samples.len() >= 2);
    }

    #[test]
    fn test_hausdorff_identical() {
        let a = [(0.0, 0.0), (1.0, 1.0)];
        let b = [(0.0, 0.0), (1.0, 1.0)];
        assert!(hausdorff_distance(&a, &b) < 1.0);
    }
}
