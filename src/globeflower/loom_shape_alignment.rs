//! Sequence-wide GTFS stop-to-shape alignment.
//!
//! The original C++ Builder::getSubPolyLine independently projects each stop
//! pair. This is incorrect for complete loops, lasso trips and reversals:
//! one physical point may occur several times along the trip's geometry.
//!
//! Here an occurrence is identified by its arclength on the ordered GTFS
//! shape. A dynamic program chooses an increasing sequence of occurrences.
//! A shape belonging to a complete trip is NOT duplicated just because its
//! first and last coordinates coincide.
use crate::loom_graph::Point;
use std::collections::HashMap;

const EARTH_RADIUS: f64 = 6_378_137.0;
const CELL_M: f64 = 128.0;
const MAX_INDEXED_CELLS: i64 = 4096;
const PROJECTION_RADIUS_M: f64 = 250.0;
const CANDIDATE_SLACK_M: f64 = 150.0;
const ENDPOINT_TOLERANCE_M: f64 = 250.0;
const MAX_STOP_SHAPE_DEVIATION_M: f64 = 500.0;
const PROGRESSION_EPS_M: f64 = 1e-6;

type XY = (f64, f64);

fn project_geo(p: Point) -> XY {
    let lat = p.lat.clamp(-85.05112878, 85.05112878).to_radians();
    (
        EARTH_RADIUS * p.lon.to_radians(),
        EARTH_RADIUS * (std::f64::consts::FRAC_PI_4 + lat / 2.0).tan().ln(),
    )
}

fn geographic(p: XY) -> Point {
    Point {
        lon: (p.0 / EARTH_RADIUS).to_degrees(),
        lat: (2.0 * (p.1 / EARTH_RADIUS).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees(),
    }
}

fn distance(a: XY, b: XY) -> f64 {
    (a.0 - b.0).hypot(a.1 - b.1)
}

fn project(p: XY, a: XY, b: XY) -> (f64, XY) {
    let ab = (b.0 - a.0, b.1 - a.1);
    let len2 = ab.0 * ab.0 + ab.1 * ab.1;
    let t = if len2 <= 1e-12 {
        0.0
    } else {
        (((p.0 - a.0) * ab.0 + (p.1 - a.1) * ab.1) / len2).clamp(0.0, 1.0)
    };
    (t, (a.0 + t * ab.0, a.1 + t * ab.1))
}

fn cell(v: f64) -> i64 {
    (v / CELL_M).floor() as i64
}

#[derive(Clone, Copy, Debug)]
struct Candidate {
    progress: f64,
    error2: f64,
}

/// Precompute Web-Mercator lengths once per unique (feed, attempt, shape_id).
/// The spatial index covers bounding boxes of individual shape segments.
/// Long segments are checked separately to avoid unbounded grid insertion.
#[derive(Debug)]
pub struct AlignedShape {
    original: Vec<Point>,
    xy: Vec<XY>,
    cumulative: Vec<f64>,
    grid: HashMap<(i64, i64), Vec<usize>>,
    long_segments: Vec<usize>,
}

impl AlignedShape {
    pub fn new(original: Vec<Point>) -> Option<Self> {
        if original.len() < 2
            || original
                .iter()
                .any(|p| !p.lon.is_finite() || !p.lat.is_finite())
        {
            return None;
        }
        let xy: Vec<_> = original.iter().copied().map(project_geo).collect();
        let mut cumulative = Vec::with_capacity(xy.len());
        cumulative.push(0.0);
        for ab in xy.windows(2) {
            cumulative.push(cumulative.last().copied().unwrap_or(0.0) + distance(ab[0], ab[1]));
        }
        if *cumulative.last()? <= PROGRESSION_EPS_M {
            return None;
        }
        let mut grid = HashMap::<(i64, i64), Vec<usize>>::new();
        let mut long_segments = Vec::new();
        for (i, ab) in xy.windows(2).enumerate() {
            if distance(ab[0], ab[1]) <= PROGRESSION_EPS_M {
                continue;
            }
            let (xmin, xmax) = (cell(ab[0].0.min(ab[1].0)), cell(ab[0].0.max(ab[1].0)));
            let (ymin, ymax) = (cell(ab[0].1.min(ab[1].1)), cell(ab[0].1.max(ab[1].1)));
            let width = xmax.saturating_sub(xmin).saturating_add(1);
            let height = ymax.saturating_sub(ymin).saturating_add(1);
            if width.saturating_mul(height) > MAX_INDEXED_CELLS {
                long_segments.push(i);
                continue;
            }
            for x in xmin..=xmax {
                for y in ymin..=ymax {
                    grid.entry((x, y)).or_default().push(i);
                }
            }
        }
        Some(Self {
            original,
            xy,
            cumulative,
            grid,
            long_segments,
        })
    }

    pub fn length(&self) -> f64 {
        *self.cumulative.last().unwrap_or(&0.0)
    }

    /// Returns all nearby occurrences, including repeated visits to exactly
    /// the same location at different positions in the original polyline.
    fn candidates(&self, point: Point) -> Vec<Candidate> {
        let p = project_geo(point);
        let radius = PROJECTION_RADIUS_M + CANDIDATE_SLACK_M;
        let (cx, cy) = (cell(p.0), cell(p.1));
        let delta = (radius / CELL_M).ceil() as i64 + 1;
        let mut segment_ids = Vec::new();
        for x in cx - delta..=cx + delta {
            for y in cy - delta..=cy + delta {
                if let Some(ids) = self.grid.get(&(x, y)) {
                    segment_ids.extend(ids);
                }
            }
        }
        segment_ids.extend(&self.long_segments);
        segment_ids.sort_unstable();
        segment_ids.dedup();
        let compute = |i: usize| {
            let (t, nearest) = project(p, self.xy[i], self.xy[i + 1]);
            Candidate {
                progress: self.cumulative[i] + t * (self.cumulative[i + 1] - self.cumulative[i]),
                error2: distance(p, nearest).powi(2),
            }
        };
        let mut all: Vec<_> = segment_ids.into_iter().map(compute).collect();
        // Missing candidates can indicate a distant stop or malformed shape.
        // Search the complete shape rather than fabricate an interpolation.
        if all.is_empty() || all.iter().all(|c| c.error2 > PROJECTION_RADIUS_M.powi(2)) {
            all = (0..self.xy.len() - 1).map(compute).collect();
        }
        let best = all
            .iter()
            .map(|c| c.error2)
            .fold(f64::INFINITY, f64::min)
            .sqrt();
        let threshold = (best + CANDIDATE_SLACK_M).max(PROJECTION_RADIUS_M).powi(2);
        all.retain(|c| c.error2 <= threshold);
        all.sort_by(|a, b| a.progress.total_cmp(&b.progress));
        // Adjacent segments meeting at a vertex create the same arclength.
        // Retain distinct visits, not duplicate descriptions of one visit.
        all.dedup_by(|a, b| (a.progress - b.progress).abs() <= PROGRESSION_EPS_M);
        all
    }

    /// Globally align stops. Errors are geometric squared distances. A tiny
    /// tie-breaker makes exact repeated-track ambiguities deterministic,
    /// preferring approximately evenly advanced progress, but never dominates
    /// a material projection error. If both endpoints match those of the
    /// shape, they are anchored to 0 and L: this preserves complete circles.
    pub fn match_stops(&self, stops: &[Point]) -> Option<Vec<f64>> {
        if stops.len() < 2
            || stops
                .iter()
                .any(|p| !p.lon.is_finite() || !p.lat.is_finite())
        {
            return None;
        }
        let mut candidates: Vec<Vec<Candidate>> =
            stops.iter().copied().map(|p| self.candidates(p)).collect();
        if candidates.iter().any(|cs| {
            cs.is_empty()
                || cs
                    .iter()
                    .all(|c| c.error2 > MAX_STOP_SHAPE_DEVIATION_M.powi(2))
        }) {
            // Reject wrong or corrupt shapes rather than constructing an
            // unsupported geometric shortcut from distant projections.
            return None;
        }
        let first = distance(project_geo(stops[0]), self.xy[0]);
        let last = distance(project_geo(*stops.last()?), *self.xy.last()?);
        if first <= ENDPOINT_TOLERANCE_M && last <= ENDPOINT_TOLERANCE_M {
            candidates[0] = vec![Candidate {
                progress: 0.0,
                error2: first * first,
            }];
            let n = candidates.len() - 1;
            candidates[n] = vec![Candidate {
                progress: self.length(),
                error2: last * last,
            }];
        }

        let mut predecessors: Vec<Vec<Option<usize>>> = Vec::with_capacity(candidates.len());
        let mut previous: Vec<f64> = Vec::new();
        let mut chord_prefix = Vec::with_capacity(stops.len());
        chord_prefix.push(0.0);
        for pair in stops.windows(2) {
            chord_prefix.push(
                chord_prefix.last().copied().unwrap_or(0.0)
                    + distance(project_geo(pair[0]), project_geo(pair[1])),
            );
        }
        let chord_total = *chord_prefix.last().unwrap_or(&0.0);
        for (i, current) in candidates.iter().enumerate() {
            let mut back = vec![None; current.len()];
            let mut costs = vec![f64::INFINITY; current.len()];
            let desired = if chord_total > PROGRESSION_EPS_M {
                chord_prefix[i] / chord_total
            } else {
                i as f64 / (stops.len() - 1) as f64
            };
            let mut cursor = 0usize;
            let mut min_cost = f64::INFINITY;
            let mut min_index = None;
            for (j, candidate) in current.iter().enumerate() {
                if i > 0 {
                    while cursor < candidates[i - 1].len()
                        && candidates[i - 1][cursor].progress
                            <= candidate.progress + PROGRESSION_EPS_M
                    {
                        if previous[cursor] < min_cost {
                            min_cost = previous[cursor];
                            min_index = Some(cursor);
                        }
                        cursor += 1;
                    }
                    if min_index.is_none() {
                        continue;
                    }
                }
                let frac = candidate.progress / self.length();
                let prior = 1e-4 * (frac - desired).powi(2);
                costs[j] = candidate.error2 + prior + if i == 0 { 0.0 } else { min_cost };
                back[j] = min_index;
            }
            if costs.iter().all(|v| !v.is_finite()) {
                return None;
            }
            predecessors.push(back);
            previous = costs;
        }
        let mut choice = previous
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.total_cmp(b.1))?
            .0;
        let mut progress = vec![0.0; stops.len()];
        for i in (0..stops.len()).rev() {
            progress[i] = candidates[i][choice].progress;
            if i > 0 {
                choice = predecessors[i][choice]?;
            }
        }
        Some(progress)
    }

    fn point_at(&self, progress: f64) -> Point {
        let progress = progress.clamp(0.0, self.length());
        let i = self
            .cumulative
            .partition_point(|&s| s <= progress)
            .saturating_sub(1)
            .min(self.xy.len() - 2);
        let len = self.cumulative[i + 1] - self.cumulative[i];
        if len <= PROGRESSION_EPS_M {
            return self.original[i];
        }
        let t = ((progress - self.cumulative[i]) / len).clamp(0.0, 1.0);
        geographic((
            self.xy[i].0 + t * (self.xy[i + 1].0 - self.xy[i].0),
            self.xy[i].1 + t * (self.xy[i + 1].1 - self.xy[i].1),
        ))
    }

    /// Extract an exact ordered subline; shape vertices are never geometrically
    /// averaged and a reversal follows the backtracking shape as supplied.
    pub fn segment(&self, from: f64, to: f64) -> Option<Vec<Point>> {
        if !from.is_finite()
            || !to.is_finite()
            || from < -PROGRESSION_EPS_M
            || to > self.length() + PROGRESSION_EPS_M
            || from > to + PROGRESSION_EPS_M
        {
            return None;
        }
        let (from, to) = (from.clamp(0.0, self.length()), to.clamp(0.0, self.length()));
        let mut out = vec![self.point_at(from)];
        for (i, &d) in self
            .cumulative
            .iter()
            .enumerate()
            .skip(1)
            .take(self.cumulative.len().saturating_sub(2))
        {
            if d > from + PROGRESSION_EPS_M && d < to - PROGRESSION_EPS_M {
                let p = self.original[i];
                if out.last().copied() != Some(p) {
                    out.push(p);
                }
            }
        }
        let end = self.point_at(to);
        if out.last().copied() != Some(end) || out.len() == 1 {
            out.push(end);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(x: f64, y: f64) -> Point {
        Point { lon: x, lat: y }
    }
    fn align(shape: &[Point], stops: &[Point]) -> Vec<f64> {
        AlignedShape::new(shape.to_vec())
            .unwrap()
            .match_stops(stops)
            .unwrap()
    }
    #[test]
    fn complete_circle_retains_entire_perimeter() {
        let p0 = p(0.0, 0.0);
        let shape = [p0, p(0.01, 0.0), p(0.01, 0.01), p(0.0, 0.01), p0];
        let s = AlignedShape::new(shape.to_vec()).unwrap();
        let ds = s.match_stops(&shape).unwrap();
        assert_eq!(ds[0], 0.0);
        assert_eq!(*ds.last().unwrap(), s.length());
        let reconstructed = s.segment(ds[0], *ds.last().unwrap()).unwrap();
        assert!(reconstructed.len() >= shape.len());
        assert!((project_geo(reconstructed[0]).0 - project_geo(p0).0).abs() < 1e-5);
    }
    #[test]
    fn lasso_keeps_first_and_second_occurrence_of_shared_track() {
        let a = p(0.0, 0.0);
        let b = p(0.01, 0.0);
        let c = p(0.02, 0.0);
        let shape = [a, b, c, p(0.02, 0.01), p(0.03, 0.01), p(0.03, 0.0), c, b, a];
        let ds = align(&shape, &[a, b, c, p(0.03, 0.01), c, b, a]);
        assert_eq!(ds[0], 0.0);
        assert!(ds[1] < ds[2]);
        assert!(ds[4] > ds[3]);
        assert!(ds[5] > ds[4]);
        assert!((ds[6] - AlignedShape::new(shape.to_vec()).unwrap().length()).abs() < 1e-6);
    }
    #[test]
    fn globally_matches_outbound_stop_even_when_inbound_rail_is_nearer() {
        let a = p(0.0, 0.0);
        let b = p(0.01, 0.0);
        let j = p(0.02, 0.0);
        let x = p(0.02, 0.01);
        let y = p(0.03, 0.01);
        let z = p(0.03, 0.0);
        // The returning railway is only eight metres north of outbound.
        let jin = p(0.02, 0.000072);
        let bin = p(0.01, 0.000072);
        let shape = [a, b, j, x, y, z, jin, bin, a];
        // This stop is nearer the return track than the outbound track.
        let noisy_b = p(0.01, 0.000054);
        let ds = align(&shape, &[a, noisy_b, j, x, y, z, jin, bin, a]);
        let limit = AlignedShape::new(shape.to_vec()).unwrap().cumulative[2];
        assert!(ds[1] < limit, "outbound stop matched the inbound visit");
        assert!(ds.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn turnback_visits_same_station_twice_in_order() {
        let a = p(0.0, 0.0);
        let b = p(0.01, 0.0);
        let c = p(0.02, 0.0);
        let s = [a, b, c, b, a];
        let ds = align(&s, &s);
        assert!(ds.windows(2).all(|v| v[1] > v[0]));
    }
    #[test]
    fn incorrect_shape_far_from_stops_is_rejected() {
        let s = AlignedShape::new(vec![p(0.0, 0.0), p(0.01, 0.0)]).unwrap();
        assert!(s.match_stops(&[p(0.0, 0.01), p(0.01, 0.01)]).is_none());
    }
    #[test]
    fn zero_length_is_not_confused_with_full_circle() {
        let a = p(0.0, 0.0);
        let b = p(0.01, 0.0);
        let s = AlignedShape::new(vec![a, b, a]).unwrap();
        let ds = s.match_stops(&[a, a]).unwrap();
        assert!(ds[1] > ds[0] + 1000.0);
    }
    #[test]
    fn self_crossing_preserves_monotone_shape_order() {
        let a = p(0.0, 0.0);
        let b = p(0.01, 0.01);
        let c = p(0.0, 0.01);
        let d = p(0.01, 0.0);
        let shape = [a, b, c, d, a];
        let ds = align(&shape, &shape);
        assert!(ds.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
