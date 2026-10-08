//! Geometry primitives matching ad-freiburg/util geo/PolyLine.tpp for the
//! gtfs2graph stage. C++ gtfs2graph operates in EPSG:3857 METERS, not degrees.
//! In particular, PolyLine::average() samples at AVERAGING_STEP = 20 meters.
use crate::loom_graph::Point;

const R: f64 = 6_378_137.0;
const AVERAGING_STEP: f64 = 20.0;

type XY = (f64, f64);

fn mercator(p: Point) -> XY {
    let lat = p.lat.clamp(-85.05112878, 85.05112878).to_radians();
    (R * p.lon.to_radians(), R * (std::f64::consts::FRAC_PI_4 + lat * 0.5).tan().ln())
}
fn geographic((x, y): XY) -> Point {
    Point { lon: (x / R).to_degrees(), lat: (2.0 * (y / R).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees() }
}
fn hypot(a: XY, b: XY) -> f64 { (a.0 - b.0).hypot(a.1 - b.1) }
fn length(g: &[XY]) -> f64 { g.windows(2).map(|s| hypot(s[0], s[1])).sum() }
fn project(p: XY, a: XY, b: XY) -> XY {
    let dx = b.0 - a.0;
    let dy = b.1 - a.1;
    let den = dx * dx + dy * dy;
    if den < 1e-16 { return a; }
    let t = (((p.0 - a.0) * dx + (p.1 - a.1) * dy) / den).clamp(0.0, 1.0);
    (a.0 + t * dx, a.1 + t * dy)
}
fn dist_to_line(p: XY, g: &[XY]) -> f64 {
    g.windows(2).map(|s| hypot(p, project(p, s[0], s[1])))
        .fold(f64::INFINITY, f64::min)
}
fn coords(g: &[Point]) -> Vec<XY> { g.iter().copied().map(mercator).collect() }
fn at(g: &[XY], fraction: f64) -> XY {
    let total = length(g);
    if total < 1e-12 { return g[0]; }
    let distance = fraction.clamp(0.0, 1.0) * total;
    let mut before = 0.0;
    for s in g.windows(2) {
        let d = hypot(s[0], s[1]);
        if before + d >= distance && d > 0.0 {
            let t = ((distance - before) / d).clamp(0.0, 1.0);
            return (s[0].0 + (s[1].0 - s[0].0) * t,
                    s[0].1 + (s[1].1 - s[0].1) * t);
        }
        before += d;
    }
    *g.last().unwrap()
}

/// C++ PolyLine::contains: every vertex of `other` is within dmax of the
/// polyline. It deliberately does not check intersection topology.
pub fn contains(poly: &[Point], other: &[Point], dmax: f64) -> bool {
    if poly.len() < 2 || other.len() < 2 { return false; }
    let a = coords(poly);
    coords(other).into_iter().all(|p| dist_to_line(p, &a) <= dmax)
}

/// C++ PolyLine::equals(rhs, dmax), direction independent. The special
/// two-segment fast path is important: two straight lines can be nearly equal
/// in reverse orientation without ever projecting onto the same parameters.
pub fn equals(a: &[Point], b: &[Point], dmax: f64) -> bool {
    if a.len() < 2 || b.len() < 2 { return false; }
    if a.len() == 2 && b.len() == 2 {
        let (a0, a1, b0, b1) = (mercator(a[0]), mercator(a[1]), mercator(b[0]), mercator(b[1]));
        return (hypot(a0, b0) < dmax && hypot(a1, b1) < dmax)
            || (hypot(a0, b1) < dmax && hypot(a1, b0) < dmax);
    }
    contains(a, b, dmax) && contains(b, a, dmax)
}

/// Centroid in the C++ WebMercator working coordinate system.
pub fn centroid(points: &[Point]) -> Point {
    if points.is_empty() { return Point { lon: 0.0, lat: 0.0 }; }
    let mut sum = (0.0, 0.0);
    for p in points { let q = mercator(*p); sum.0 += q.0; sum.1 += q.1; }
    geographic((sum.0 / points.len() as f64, sum.1 / points.len() as f64))
}

pub fn simplify(poly: &[Point], tolerance_m: f64) -> Vec<Point> {
    simplify_xy(&coords(poly), tolerance_m).into_iter().map(geographic).collect()
}

/// C++ PolyLine::nearestSegmentAfter / projectOnAfter: B must be projected
/// at or after the segment chosen for A. This is ESSENTIAL for loop shapes.
fn project_after(line: &[XY], p: XY, start: usize) -> (usize, XY) {
    if line.len() < 2 { return (0, line[0]); }
    let mut best_index = start.min(line.len() - 2);
    let mut best_point = line[best_index];
    let mut best_dist = f64::INFINITY;
    for i in best_index..line.len()-1 {
        let q = project(p, line[i], line[i+1]);
        let dist = hypot(p, q);
        if dist < best_dist {
            best_dist = dist;
            best_index = i;
            best_point = q;
        }
    }
    (best_index, best_point)
}

/// Port of util::geo::PolyLine::getSegment(const Point&, const Point&):
/// projectOn(first), projectOnAfter(second, first.lastIndex), then insert
/// the intermediate vertices. Unlike independent nearest-point projection,
/// this honors the ordered shape traversal even when the line doubles back.
pub fn ordered_segment(shape: &[Point], from: Point, to: Point) -> Vec<Point> {
    if shape.len() < 2 { return vec![from, to]; }
    let line = coords(shape);
    let (from_index, first) = project_after(&line, mercator(from), 0);
    let (to_index, last) = project_after(&line, mercator(to), from_index);
    let mut segment = Vec::with_capacity(to_index - from_index + 3);
    segment.push(first);
    if to_index > from_index {
        segment.extend_from_slice(&line[from_index+1..=to_index]);
    }
    segment.push(last);
    let mut simplified = simplify_xy(&segment, 0.0);
    if simplified.len() < 2 { simplified = vec![first, last]; }
    simplified.into_iter().map(geographic).collect()
}

/// C++ MapConstructor::cleanUpGeoms projects both endpoint nodes
/// independently and uses PolyLine::getSegment(totalPosA, totalPosB), which
/// sorts those positions before extracting the interior shape.
pub fn trim_between_projections(shape: &[Point], from: Point, to: Point) -> Vec<Point> {
    if shape.len() < 2 { return shape.to_vec(); }
    let line = coords(shape);
    let (ia, a) = project_after(&line, mercator(from), 0);
    let (ib, b) = project_after(&line, mercator(to), 0);
    let along = |index: usize, p: XY| -> f64 {
        length(&line[..=index]) + hypot(line[index], p)
    };
    let (start_index, start, end_index, end) = if along(ia, a) <= along(ib, b) {
        (ia, a, ib, b)
    } else { (ib, b, ia, a) };
    let mut piece = Vec::with_capacity(end_index - start_index + 3);
    piece.push(start);
    if end_index > start_index {
        piece.extend_from_slice(&line[start_index+1..=end_index]);
    }
    piece.push(end);
    let simplified = simplify_xy(&piece, 0.0);
    if simplified.len() < 2 { return vec![geographic(start), geographic(end)]; }
    simplified.into_iter().map(geographic).collect()
}

/// C++ PolyLine::getSegmentAtDist, with distances in WebMercator meters.
/// The C++ overload orders the two requested distances before clipping.
pub fn segment_at_dist(shape: &[Point], first_m: f64, last_m: f64) -> Vec<Point> {
    if shape.len() < 2 { return shape.to_vec(); }
    let line = coords(shape);
    let total = length(&line);
    if total < 1e-12 { return shape.to_vec(); }
    let a = first_m.min(last_m).clamp(0.0, total);
    let b = first_m.max(last_m).clamp(0.0, total);
    let locate = |desired: f64| -> (usize, XY) {
        let mut walked = 0.0;
        for (i, endpoints) in line.windows(2).enumerate() {
            let len = hypot(endpoints[0], endpoints[1]);
            if walked + len >= desired && len > 0.0 {
                let t = ((desired - walked) / len).clamp(0.0, 1.0);
                return (i, (endpoints[0].0 + (endpoints[1].0 - endpoints[0].0) * t,
                            endpoints[0].1 + (endpoints[1].1 - endpoints[0].1) * t));
            }
            walked += len;
        }
        (line.len()-2, *line.last().unwrap())
    };
    let (ia, start) = locate(a);
    let (ib, end) = locate(b);
    let mut segment = vec![start];
    if ib > ia { segment.extend_from_slice(&line[ia+1..=ib]); }
    segment.push(end);
    let result = simplify_xy(&segment, 0.0);
    if result.len() < 2 { return vec![geographic(start), geographic(end)]; }
    result.into_iter().map(geographic).collect()
}

pub fn metric_length(g: &[Point]) -> f64 { length(&coords(g)) }

// Ramer-Douglas-Peucker simplification is the tolerance-based operation
// invoked from PolyLine::average() (0.0001 meters).
fn simplify_xy(g: &[XY], tolerance: f64) -> Vec<XY> {
    if g.len() <= 2 { return g.to_vec(); }
    let mut keep = vec![false; g.len()];
    keep[0] = true;
    keep[g.len()-1] = true;
    let mut todo = vec![(0, g.len()-1)];
    while let Some((start, end)) = todo.pop() {
        if end <= start + 1 { continue; }
        let mut best = 0.0;
        let mut index = start;
        for i in start+1..end {
            let d = hypot(g[i], project(g[i], g[start], g[end]));
            if d > best { best = d; index = i; }
        }
        if best > tolerance {
            keep[index] = true;
            todo.push((start, index));
            todo.push((index, end));
        }
    }
    g.iter().zip(keep).filter_map(|(&p, yes)| yes.then_some(p)).collect()
}

/// Direct port of util::geo::PolyLine::average (unweighted overload):
/// average corresponding normalized arc-length positions, every 20 meters
/// of the LONGEST line, then simplify by 0.0001 meters.
pub fn average(lines: &[Vec<Point>]) -> Vec<Point> {
    if lines.is_empty() { return Vec::new(); }
    if lines.len() == 1 { return lines[0].clone(); }
    let shapes: Vec<Vec<XY>> = lines.iter().filter(|s| s.len() >= 2).map(|s| coords(s)).collect();
    if shapes.is_empty() { return Vec::new(); }
    if shapes.len() == 1 { return lines.iter().find(|s| s.len() >= 2).unwrap().clone(); }
    if shapes.len() == 2 && shapes[0].len() == 2 && shapes[1].len() == 2 {
        return (0..2).map(|i| geographic(((shapes[0][i].0 + shapes[1][i].0) * 0.5,
                                            (shapes[0][i].1 + shapes[1][i].1) * 0.5))).collect();
    }
    let longest = shapes.iter().map(|s| length(s)).fold(0.0, f64::max);
    if longest < 1e-9 { return lines[0].clone(); }
    let step = AVERAGING_STEP / longest;
    let mut samples = Vec::new();
    let mut t: f64 = 0.0;
    loop {
        let fraction = t.min(1.0);
        let mut sum = (0.0, 0.0);
        for s in &shapes {
            let p = at(s, fraction);
            sum.0 += p.0;
            sum.1 += p.1;
        }
        samples.push((sum.0 / shapes.len() as f64, sum.1 / shapes.len() as f64));
        if fraction >= 1.0 { break; }
        t += step;
    }
    simplify_xy(&samples, 0.0001).into_iter().map(geographic).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(x: f64, y: f64) -> Point { Point { lon: x, lat: y } }
    #[test]
    fn ordered_segment_never_jumps_back_to_previous_loop_branch() {
        // The shape doubles back, placing a late stop close to its first arm.
        let shape = vec![p(0.0, 0.0), p(0.002, 0.0), p(0.002, 0.001),
                         p(0.0001, 0.001), p(0.0001, 0.0001)];
        let piece = ordered_segment(&shape, p(0.002, 0.0001), p(0.0001, 0.0002));
        assert!(piece.iter().any(|p| p.lat > 0.0009));
    }
    #[test]
    fn reverse_equal_and_contained() {
        let a = vec![p(0.0, 0.0), p(0.001, 0.0)];
        let b = vec![p(0.001, 0.0), p(0.0, 0.0)];
        assert!(equals(&a, &b, 10.0));
        assert!(contains(&a, &b, 50.0));
    }
    #[test]
    fn average_two_segments_is_unweighted_midpoint() {
        let a = vec![p(0.0, 0.0), p(0.001, 0.0)];
        let b = vec![p(0.0, 0.00002), p(0.001, 0.00002)];
        let result = average(&[a, b]);
        assert!((result[0].lat - 0.00001).abs() < 1e-7);
    }
}
