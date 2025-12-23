use geo::{LineString, Point, CoordsIter, EuclideanLength, EuclideanDistance};
use geo::prelude::*;

// Helper to get a point at a normalized distance (0.0 to 1.0) along a LineString
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
            let segment_ratio = if seg_len > 0.0 { remaining / seg_len } else { 0.0 };
            
            let x = p1.x() + (p2.x() - p1.x()) * segment_ratio;
            let y = p1.y() + (p2.y() - p1.y()) * segment_ratio;
            return Some(Point::new(x, y));
        }
        current_dist += seg_len;
    }
    
    // Fallback to last point
    line.points().last()
}

// Check if `inner` is "contained" within `outer` with some tolerance.
// In loom: checking if every point of inner is within distance of outer.
pub fn is_contained(inner: &LineString<f64>, outer: &LineString<f64>, threshold: f64) -> bool {
    inner.points().all(|p| outer.euclidean_distance(&p) <= threshold)
}

// Average multiple LineStrings into one
pub fn average_polylines(lines: &[LineString<f64>]) -> LineString<f64> {
    if lines.is_empty() {
        return LineString::new(vec![]);
    }
    if lines.len() == 1 {
        return lines[0].clone();
    }

    let max_len = lines.iter().map(|l| l.euclidean_length()).fold(0.0, f64::max);
    // Step size from Loom: AVERAGING_STEP / longestLength. 
    // Loom's AVERAGING_STEP not shown, but likely small constant. 
    // Let's pick a reasonable step size for meters. If coords are lat/lon, they are approx meters?
    // Wait, Loom projects to WebMercator (meters). My coords are LatLon (degrees).
    // Using simple Euclidean on LatLon is bad for length, but for averaging ratio it's inconsistent.
    // Ideally we project, average, unproject. 
    // But for simplicity, we'll assume locally flat for ratio calculation.
    
    // Dynamic step count: e.g. 100 steps, or 1 step per "unit".
    // Let's use 100 steps for now for smoothness.
    let steps = 100; 
    
    let mut points = Vec::new();
    for i in 0..=steps {
        let ratio = i as f64 / steps as f64;
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut count = 0;
           
        for line in lines {
             if let Some(p) = get_point_at_ratio(line, ratio) {
                 sum_x += p.x();
                 sum_y += p.y();
                 count += 1;
             }
        }
        
        if count > 0 {
            points.push((sum_x / count as f64, sum_y / count as f64));
        }
    }
    
    LineString::from(points)
}
