use geo::{
    Coord, EuclideanDistance, EuclideanLength, HaversineDistance, HaversineLength, Line,
    LineInterpolatePoint, LineLocatePoint, LineString, Point,
};

// --- Metric Projection Helper (Local Tangent Plane) ---

/// A Local Tangent Plane projection centered at a specific (lon0, lat0).
/// Projects (lon, lat) to (x, y) meters and back.
/// Using Equirectangular approximation which is fast and preserves local distance well enough.
/// x = R * cos(lat0) * dlon
/// y = R * dlat
#[derive(Debug, Clone, Copy)]
pub struct LocalTangentPlane {
    origin_lon_rad: f64,
    origin_lat_rad: f64,
    cos_lat0: f64,
    radius: f64,
}

impl LocalTangentPlane {
    const EARTH_RADIUS: f64 = 6_371_007.2;

    pub fn new(lon0: f64, lat0: f64) -> Self {
        let origin_lon_rad = lon0.to_radians();
        let origin_lat_rad = lat0.to_radians();
        Self {
            origin_lon_rad,
            origin_lat_rad,
            cos_lat0: origin_lat_rad.cos(),
            radius: Self::EARTH_RADIUS,
        }
    }

    /// Project (lon, lat) to (x, y) meters
    pub fn project(&self, lon: f64, lat: f64) -> (f64, f64) {
        let dlon = lon.to_radians() - self.origin_lon_rad;
        let dlat = lat.to_radians() - self.origin_lat_rad;
        let x = self.radius * self.cos_lat0 * dlon;
        let y = self.radius * dlat;
        (x, y)
    }

    /// Unproject (x, y) meters to (lon, lat)
    pub fn unproject(&self, x: f64, y: f64) -> (f64, f64) {
        let dlon = x / (self.radius * self.cos_lat0);
        let dlat = y / self.radius;
        let lon_rad = self.origin_lon_rad + dlon;
        let lat_rad = self.origin_lat_rad + dlat;
        (lon_rad.to_degrees(), lat_rad.to_degrees())
    }
}

// --- Metric Geometry Functions (XY Space) ---

/// Calculate Euclidean length of a polyline in metric space (x, y).
pub fn polyline_length_metric(coords: &[(f64, f64)]) -> f64 {
    if coords.len() < 2 {
        return 0.0;
    }
    let mut len = 0.0;
    for i in 0..coords.len() - 1 {
        let (x1, y1) = coords[i];
        let (x2, y2) = coords[i + 1];
        len += ((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt();
    }
    len
}

/// Project point to polyline in metric space.
/// Returns (distance_along_polyline, distance_from_polyline, projected_point).
pub fn project_point_to_polyline_metric(
    point: (f64, f64),
    coords: &[(f64, f64)],
) -> Option<(f64, f64, (f64, f64))> {
    if coords.len() < 2 {
        if coords.len() == 1 {
            let dist = ((point.0 - coords[0].0).powi(2) + (point.1 - coords[0].1).powi(2)).sqrt();
            return Some((0.0, dist, coords[0]));
        }
        return None;
    }

    let ls: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();
    let p = Point::new(point.0, point.1);

    // geo's Euclidean operations
    let fraction = ls.line_locate_point(&p)?;
    let projected = ls.line_interpolate_point(fraction)?;

    // Manual lengths for consistency (geo uses Euclidean)
    let total_len = ls.euclidean_length();
    let distance_along = fraction * total_len;
    let distance_to_line = p.euclidean_distance(&projected);

    Some((
        distance_along,
        distance_to_line,
        (projected.x(), projected.y()),
    ))
}

/// Interpolate along polyline in metric space.
pub fn interpolate_along_polyline_metric(coords: &[(f64, f64)], fraction: f64) -> (f64, f64) {
    if coords.is_empty() {
        return (0.0, 0.0);
    }
    let ls: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();
    match ls.line_interpolate_point(fraction) {
        Some(p) => (p.x(), p.y()),
        None => coords[0],
    }
}

/// Densify a polyline in metric space by adding points (max_spacing_m).
pub fn densify_polyline_metric(coords: &[(f64, f64)], max_spacing_m: f64) -> Vec<(f64, f64)> {
    if coords.len() < 2 {
        return coords.to_vec();
    }
    let mut result = Vec::with_capacity(coords.len() * 2);
    result.push(coords[0]);

    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let dist = ((p2.0 - p1.0).powi(2) + (p2.1 - p1.1).powi(2)).sqrt();

        if dist > max_spacing_m {
            let steps = (dist / max_spacing_m).ceil() as usize;
            for j in 1..steps {
                let t = j as f64 / steps as f64;
                let x = p1.0 + (p2.0 - p1.0) * t;
                let y = p1.1 + (p2.1 - p1.1) * t;
                result.push((x, y));
            }
        }
        result.push(p2);
    }
    result
}

/// Calculate Hausdorff distance between two densified polylines in metric space.
pub fn hausdorff_distance_metric(a: &[(f64, f64)], b: &[(f64, f64)]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return f64::INFINITY;
    }

    let directed_dist = |source: &[(f64, f64)], target: &[(f64, f64)]| -> f64 {
        source.iter().fold(0.0f64, |max_d, &p| {
            let min_d = target.iter().fold(f64::INFINITY, |min_d, &q| {
                let d = ((p.0 - q.0).powi(2) + (p.1 - q.1).powi(2)).sqrt();
                min_d.min(d)
            });
            max_d.max(min_d)
        })
    };

    directed_dist(a, b).max(directed_dist(b, a))
}

/// Check overlap ratio of A onto B in metric space using interval intersection.
pub fn overlap_ratio_metric(a: &[(f64, f64)], b: &[(f64, f64)], threshold_m: f64) -> f64 {
    // Project endpoints and samples of A onto B to find coverage range.
    if b.is_empty() || a.is_empty() {
        return 0.0;
    }

    let b_len = polyline_length_metric(b);
    if b_len <= 1e-6 {
        return 0.0;
    }

    let get_projection = |pt: (f64, f64)| -> Option<f64> {
        if let Some((dist_along, dist_line, _)) = project_point_to_polyline_metric(pt, b) {
            if dist_line <= threshold_m {
                return Some(dist_along);
            }
        }
        None
    };

    // Project start, mid, end of A
    let mut projections = Vec::new();
    if let Some(d) = get_projection(a[0]) {
        projections.push(d);
    }
    if let Some(d) = get_projection(*a.last().unwrap()) {
        projections.push(d);
    }
    // Midpoint
    if a.len() >= 2 {
        let mid = interpolate_along_polyline_metric(a, 0.5);
        if let Some(d) = get_projection(mid) {
            projections.push(d);
        }
    }

    if projections.is_empty() {
        return 0.0;
    }

    let min_p = projections.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_p = projections
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);

    // Compute intersection with B's range [0, b_len]
    let start = min_p.max(0.0);
    let end = max_p.min(b_len);

    if start >= end {
        return 0.0;
    }
    // Standard overlap ratio is intersection / min(len_a, len_b).

    let a_len = polyline_length_metric(a);
    let min_len = a_len.min(b_len);
    if min_len <= 1e-6 {
        return 0.0;
    }

    (end - start) / min_len
}

// --- Legacy Haversine Functions (Wrappers or Deprecated) ---
// Kept for I/O compatibility where inputs are Lon/Lat but simple operations are needed without LTP context.

/// Calculate Hausdorff distance between two polylines in meters (Haversine).
/// WARN: Use metric functions with LTP for heavy processing.
pub fn hausdorff_distance(a: &[(f64, f64)], b: &[(f64, f64)]) -> f64 {
    // Simple wrapper or keep original implementation
    if a.is_empty() || b.is_empty() {
        return f64::INFINITY;
    }

    let dist_a_to_b = a
        .iter()
        .map(|&p| {
            b.iter()
                .map(|&q| Point::new(p.0, p.1).haversine_distance(&Point::new(q.0, q.1)))
                .fold(f64::INFINITY, f64::min)
        })
        .fold(0.0f64, f64::max);

    let dist_b_to_a = b
        .iter()
        .map(|&p| {
            a.iter()
                .map(|&q| Point::new(p.0, p.1).haversine_distance(&Point::new(q.0, q.1)))
                .fold(f64::INFINITY, f64::min)
        })
        .fold(0.0f64, f64::max);

    dist_a_to_b.max(dist_b_to_a)
}

pub fn polyline_length(coords: &[(f64, f64)]) -> f64 {
    if coords.len() < 2 {
        return 0.0;
    }
    let line: LineString = coords.iter().map(|&(x, y)| Coord { x, y }).collect();
    line.haversine_length()
}

pub fn project_point_to_polyline(
    point: (f64, f64),
    coords: &[(f64, f64)],
) -> Option<(f64, f64, (f64, f64))> {
    // WARNING: This mixes Euclidean projection with Haversine conversion. Use with caution or switch to LTP.
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
    let distance_along = fraction * polyline_length(coords); // Approx
    let distance_to_line = p.haversine_distance(&projected);
    Some((
        distance_along,
        distance_to_line,
        (projected.x(), projected.y()),
    ))
}

/// Calculate bearing angle (degrees)
pub fn bearing(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (lon1, lat1) = (a.0.to_radians(), a.1.to_radians());
    let (lon2, lat2) = (b.0.to_radians(), b.1.to_radians());
    let dlon = lon2 - lon1;
    let x = dlon.cos() * lat2.sin();
    let y = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    (x.atan2(y).to_degrees() + 360.0) % 360.0
}

pub fn polyline_bearing(coords: &[(f64, f64)]) -> Option<f64> {
    if coords.len() < 2 {
        return None;
    }
    Some(bearing(coords[0], *coords.last()?))
}

pub fn bearing_difference(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs() % 360.0;
    if diff > 180.0 { 360.0 - diff } else { diff }
}

pub fn weighted_average_centerline(
    _polylines: &[(&[(f64, f64)], f64)],
    _sample_interval: f64,
) -> Vec<(f64, f64)> {
    // TODO: Deprecate or Update to use LTP passed in?
    // Ideally the caller should project to LTP, average in Metric, then unproject.
    vec![]
}

/// Calculate weighted average centerline in METRIC space.
pub fn weighted_average_centerline_metric(
    polylines: &[(&[(f64, f64)], f64)],
    sample_interval_m: f64,
) -> Vec<(f64, f64)> {
    if polylines.is_empty() {
        return vec![];
    }
    if polylines.len() == 1 {
        return polylines[0].0.to_vec();
    }

    let (ref_idx, _) = polylines
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| {
            polyline_length_metric(a.0)
                .partial_cmp(&polyline_length_metric(b.0))
                .unwrap()
        })
        .unwrap();

    let ref_coords = polylines[ref_idx].0;
    let ref_len = polyline_length_metric(ref_coords);
    let num_samples = (ref_len / sample_interval_m).ceil() as usize;
    let mut centerline = Vec::new();

    for i in 0..=num_samples {
        let fraction = (i as f64) / (num_samples as f64).max(1.0);
        let ref_pt = interpolate_along_polyline_metric(ref_coords, fraction);

        let mut sum_x = ref_pt.0 * polylines[ref_idx].1;
        let mut sum_y = ref_pt.1 * polylines[ref_idx].1;
        let mut sum_w = polylines[ref_idx].1;

        for (idx, (coords, w)) in polylines.iter().enumerate() {
            if idx == ref_idx {
                continue;
            }
            if let Some((_, dist, proj)) = project_point_to_polyline_metric(ref_pt, coords) {
                // Only include if reasonably close? Caller handles filtering usually.
                sum_x += proj.0 * w;
                sum_y += proj.1 * w;
                sum_w += w;
            }
        }

        if sum_w > 0.0 {
            centerline.push((sum_x / sum_w, sum_y / sum_w));
        }
    }
    centerline
}

/// Extract sub-polyline based on fractional start/end, finding exact metric coordinates.
pub fn extract_sub_polyline(
    coords: &[(f64, f64)],
    start_frac: f64,
    end_frac: f64,
) -> Vec<(f64, f64)> {
    let start = start_frac.max(0.0).min(1.0);
    let end = end_frac.max(0.0).min(1.0);
    if start >= end {
        return vec![interpolate_along_polyline_metric(coords, start)];
    }

    let mut result = Vec::new();
    let p_start = interpolate_along_polyline_metric(coords, start);
    result.push(p_start);

    // Add intermediate vertices
    let total_len = polyline_length_metric(coords);
    let start_dist = total_len * start;
    let end_dist = total_len * end;

    let mut current_dist = 0.0;
    for i in 0..coords.len() - 1 {
        let p1 = coords[i];
        let p2 = coords[i + 1];
        let seg_len = ((p2.0 - p1.0).powi(2) + (p2.1 - p1.1).powi(2)).sqrt();
        let next_dist = current_dist + seg_len;

        // If segment end is strictly inside the range
        // Use epsilon to avoid duplicating start/end points if they land exactly on vertices
        if next_dist > start_dist + 1e-4 && next_dist < end_dist - 1e-4 {
            result.push(p2);
        }
        current_dist = next_dist;
    }

    let p_end = interpolate_along_polyline_metric(coords, end);
    result.push(p_end);
    result
}

/// Calculate the P-th percentile distance from A to B.
/// e.g. p90 means 90% of points on A are within this distance of B.
pub fn closest_distance_percentile_metric(
    a: &[(f64, f64)],
    b: &[(f64, f64)],
    percentile: f64,
) -> f64 {
    if a.is_empty() {
        return 0.0;
    }
    if b.is_empty() {
        return f64::INFINITY;
    }

    // Densify A to ensure coverage
    let a_dense = densify_polyline_metric(a, 5.0);

    let mut dists: Vec<f64> = a_dense
        .iter()
        .map(|&pt| {
            project_point_to_polyline_metric(pt, b)
                .map(|(_, d, _)| d)
                .unwrap_or(f64::INFINITY)
        })
        .collect();

    if dists.is_empty() {
        return 0.0;
    }

    dists.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((dists.len() as f64 * (percentile / 100.0)).ceil() as usize)
        .max(1)
        .min(dists.len())
        - 1;
    dists[idx]
}

/// Find maximal/consistent overlap intervals between two geometries.
/// Returns Vec<(a_start, a_end, b_start, b_end)> fractions.
/// Uses monotonic scanning, hysteresis, and bi-directional consistency check.
pub fn overlap_intervals_metric(
    geom_a: &[(f64, f64)],
    geom_b: &[(f64, f64)],
    dist_threshold_m: f64,
    angle_threshold_deg: f64,
) -> Vec<(f64, f64, f64, f64)> {
    if geom_a.len() < 2 || geom_b.len() < 2 {
        return Vec::new();
    }

    // Helper: One-way scan with monotonicity & hysteresis
    let scan_one_way = |source: &[(f64, f64)],
                        target: &[(f64, f64)],
                        check_angle: bool|
     -> Vec<(f64, f64, f64, f64)> {
        let source_len = polyline_length_metric(source);
        let target_len = polyline_length_metric(target);
        if source_len < 1.0 || target_len < 1.0 {
            return Vec::new();
        }

        let densified_source = densify_polyline_metric(source, 5.0);
        let mut intervals = Vec::new();
        let mut current_start_src: Option<f64> = None;
        let mut current_start_tgt: Option<f64> = None;
        let mut last_match_src = 0.0;
        let mut last_match_tgt = 0.0;
        let mut consecutive_misses = 0;
        // Monotonicity state
        let mut last_tgt_proj = -1.0;

        for (_i, &pt) in densified_source.iter().enumerate() {
            // Find distance along source
            let (src_dist, _, _) = project_point_to_polyline_metric(pt, source).unwrap();
            let src_frac = src_dist / source_len;

            // Project to target
            if let Some((tgt_dist, dist_m, _)) = project_point_to_polyline_metric(pt, target) {
                // Check Monotonicity: projected target distance should roughly increase.
                // Allow small jitter (backtracking) e.g. 5m, but not large jumps.
                let is_monotonic = if last_tgt_proj < 0.0 {
                    true
                } else {
                    tgt_dist >= last_tgt_proj - 5.0 // Allow 5m backtrack jitter
                };

                let mut is_match = false;
                if is_monotonic && dist_m <= dist_threshold_m {
                    if check_angle {
                        let src_bearing = get_local_bearing(source, src_dist);
                        let tgt_bearing = get_local_bearing(target, tgt_dist);
                        let diff = bearing_difference(src_bearing, tgt_bearing);
                        let diff_norm = diff.min(180.0 - diff);
                        if diff_norm <= angle_threshold_deg {
                            is_match = true;
                        }
                    } else {
                        is_match = true;
                    }
                }

                if is_match {
                    consecutive_misses = 0;
                    last_tgt_proj = tgt_dist;

                    if current_start_src.is_none() {
                        current_start_src = Some(src_frac);
                        current_start_tgt = Some(tgt_dist / target_len);
                    }
                    last_match_src = src_frac;
                    last_match_tgt = tgt_dist / target_len;
                } else {
                    consecutive_misses += 1;
                }

                let hard_break = !is_monotonic;

                if consecutive_misses > 3 || hard_break {
                    if let (Some(s_src), Some(s_tgt)) = (current_start_src, current_start_tgt) {
                        // Min length check? e.g. 10m. Caller can filter.
                        intervals.push((s_src, last_match_src, s_tgt, last_match_tgt));
                    }
                    current_start_src = None;
                    current_start_tgt = None;
                    if hard_break {
                        // Reset monotonicity tracking if we strictly broke it
                        last_tgt_proj = tgt_dist;
                    }
                }
            }
        }

        // Close final
        if let (Some(s_src), Some(s_tgt)) = (current_start_src, current_start_tgt) {
            intervals.push((s_src, last_match_src, s_tgt, last_match_tgt));
        }

        intervals
    };

    // Run A -> B
    let intervals_ab = scan_one_way(geom_a, geom_b, true);
    if intervals_ab.is_empty() {
        return Vec::new();
    }

    // Run B -> A
    // Returns (b_s, b_e, a_s, a_e)
    let intervals_ba = scan_one_way(geom_b, geom_a, true);
    if intervals_ba.is_empty() {
        return Vec::new();
    }

    // Filter/Intersect
    // Keep AB intervals that are "supported" by BA intervals.
    let mut valid_intervals = Vec::new();

    for (a_s1, a_e1, b_s1, b_e1) in intervals_ab {
        for &(b_s2, b_e2, a_s2, a_e2) in &intervals_ba {
            // Check overlap in A-space
            let a_overlap_start = a_s1.max(a_s2);
            let a_overlap_end = a_e1.min(a_e2);

            // Check overlap in B-space
            let b_overlap_start = b_s1.max(b_s2);
            let b_overlap_end = b_e1.min(b_e2);

            let a_len = (a_overlap_end - a_overlap_start).max(0.0);
            let b_len = (b_overlap_end - b_overlap_start).max(0.0);

            // Require minimal overlap (e.g. 1% or small epsilon)
            if a_len > 0.001 && b_len > 0.001 {
                valid_intervals.push((
                    a_overlap_start,
                    a_overlap_end,
                    b_overlap_start,
                    b_overlap_end,
                ));
            }
        }
    }

    valid_intervals
}

fn get_local_bearing(line: &[(f64, f64)], dist_m: f64) -> f64 {
    // Sample a small segment around dist_m
    // [dist_m - 2.0, dist_m + 2.0]
    let total = polyline_length_metric(line);
    let p_prev = interpolate_along_polyline_metric_dist(line, (dist_m - 5.0).max(0.0));
    let p_next = interpolate_along_polyline_metric_dist(line, (dist_m + 5.0).min(total));
    bearing(p_prev, p_next)
}

fn interpolate_along_polyline_metric_dist(coords: &[(f64, f64)], dist: f64) -> (f64, f64) {
    let total = polyline_length_metric(coords);
    if total <= 1e-6 {
        return coords[0];
    }
    interpolate_along_polyline_metric(coords, dist / total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ltp_roundtrip() {
        let ltp = LocalTangentPlane::new(13.4, 52.5); // Berlin
        let (x, y) = ltp.project(13.41, 52.51);
        let (lon, lat) = ltp.unproject(x, y);
        assert!((lon - 13.41).abs() < 1e-6);
        assert!((lat - 52.51).abs() < 1e-6);
    }

    #[test]
    fn test_overlap_ratio_metric() {
        // Two 100m parallel lines, offset by 10m
        // Line A: 0,0 -> 100,0
        // Line B: 0,10 -> 100,10
        // Overlap should be 1.0 (100%) if threshold > 10m
        let a = vec![(0.0, 0.0), (100.0, 0.0)];
        let b = vec![(0.0, 10.0), (100.0, 10.0)];

        let ratio = overlap_ratio_metric(&a, &b, 15.0);
        assert!(ratio > 0.99);

        // Disjoint
        let c = vec![(200.0, 0.0), (300.0, 0.0)];
        let ratio_disjoint = overlap_ratio_metric(&a, &c, 15.0);
        assert!(ratio_disjoint < 0.01);
    }
}

// --- Compatibility Wrappers ---

pub fn interpolate_along_polyline(coords: &[(f64, f64)], fraction: f64) -> (f64, f64) {
    interpolate_along_polyline_metric(coords, fraction)
}

pub fn sample_along_polyline(coords: &[(f64, f64)], interval_m: f64) -> Vec<(f64, f64)> {
    let len = polyline_length_metric(coords);
    if len <= 0.0 {
        return vec![coords[0]];
    }

    let num = (len / interval_m).ceil() as usize;
    let mut res = Vec::with_capacity(num + 1);

    for i in 0..=num {
        let d = i as f64 * interval_m;
        let t = (d / len).min(1.0);
        res.push(interpolate_along_polyline_metric(coords, t));
    }
    // Ensure exact endpoint is included if logic implies?
    // Map matcher usually wants equidistant points.
    res
}
