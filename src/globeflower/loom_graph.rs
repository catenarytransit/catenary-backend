use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub type NodeId = usize;
pub type EdgeId = usize;
pub type LineId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Point {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Debug, Clone)]
pub struct Stop {
    pub chateau: String,
    pub stop_id: String,
    pub name: String,
    pub pos: Point,
}

#[derive(Debug, Clone)]
pub struct Line {
    pub chateau: String,
    pub route_id: String,
    pub label: String,
    pub color: String,
    pub text_color: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LineOcc {
    pub line: LineId,
    /// None means bidirectional/unknown. Some(node) means travel points toward node.
    pub direction: Option<NodeId>,
}

/// C++ LineEdgePL stores one occurrence per line.  Opposite observed
/// directions make that occurrence bidirectional rather than two independent
/// line entries.  This is important for MapConstructor::lineEq and merging.
pub fn add_line_occ(lines: &mut BTreeSet<LineOcc>, occurrence: LineOcc) {
    let existing: Vec<_> = lines
        .iter()
        .copied()
        .filter(|old| old.line == occurrence.line)
        .collect();
    if existing.is_empty() {
        lines.insert(occurrence);
        return;
    }
    let direction = if existing
        .iter()
        .all(|old| old.direction == occurrence.direction)
    {
        occurrence.direction
    } else {
        None
    };
    for old in existing {
        lines.remove(&old);
    }
    lines.insert(LineOcc {
        line: occurrence.line,
        direction,
    });
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub pos: Point,
    pub stops: Vec<Stop>,
    pub not_served: BTreeSet<LineId>,
    pub adj: BTreeSet<EdgeId>,
    /// Explicit consecutive-edge transitions constructed from direction patterns.
    /// Edge IDs are local to this graph, not database or provenance IDs.
    pub allowed_turns: BTreeMap<LineId, BTreeSet<(EdgeId, EdgeId)>>,
    /// line -> from edge -> forbidden to edges
    pub conn_exc: BTreeMap<LineId, BTreeMap<EdgeId, BTreeSet<EdgeId>>>,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub id: EdgeId,
    pub a: NodeId,
    pub b: NodeId,
    pub geom: Vec<Point>,
    pub lines: BTreeSet<LineOcc>,
    /// IDs of preliminary gtfs2graph edges represented by this topo edge.
    pub originals: BTreeSet<usize>,
}

#[derive(Debug, Default, Clone)]
pub struct Graph {
    pub nodes: Vec<Option<Node>>,
    pub edges: Vec<Option<Edge>>,
    pub lines: Vec<Line>,
}

impl Graph {
    /// Development-time verification for the topology transformations.
    /// Turn inferred restrictions cannot be correct if graph IDs or line
    /// directions refer to deleted or non-incident vertices.
    #[cfg(debug_assertions)]
    pub fn assert_consistent(&self) {
        for (id, node) in self.nodes.iter().enumerate() {
            let Some(node) = node.as_ref() else {
                continue;
            };
            assert_eq!(node.id, id);
            assert!(node.pos.lon.is_finite() && node.pos.lat.is_finite());
            for &edge_id in &node.adj {
                let e = self
                    .edges
                    .get(edge_id)
                    .and_then(Option::as_ref)
                    .expect("node refers to deleted edge");
                assert!(e.a == id || e.b == id, "node lists non-incident edge");
            }
        }
        for (id, maybe_edge) in self.edges.iter().enumerate() {
            let Some(edge) = maybe_edge.as_ref() else {
                continue;
            };
            assert_eq!(id, edge.id);
            assert_ne!(edge.a, edge.b, "self-edge introduced by collapse");
            assert!(edge.geom.len() >= 2, "edge with no geometry");
            for endpoint in [edge.a, edge.b] {
                let node = self
                    .nodes
                    .get(endpoint)
                    .and_then(Option::as_ref)
                    .expect("edge endpoint removed");
                assert!(
                    node.adj.contains(&id),
                    "edge absent from endpoint adjacency"
                );
            }
            for occurrence in &edge.lines {
                assert!(
                    occurrence.direction.is_none()
                        || occurrence.direction == Some(edge.a)
                        || occurrence.direction == Some(edge.b),
                    "dangling route direction"
                );
            }
        }
    }

    pub fn add_node(&mut self, pos: Point) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Some(Node {
            id,
            pos,
            stops: vec![],
            not_served: BTreeSet::new(),
            adj: BTreeSet::new(),
            allowed_turns: BTreeMap::new(),
            conn_exc: BTreeMap::new(),
        }));
        id
    }

    pub fn add_edge(&mut self, a: NodeId, b: NodeId, geom: Vec<Point>) -> EdgeId {
        let id = self.edges.len();
        self.edges.push(Some(Edge {
            id,
            a,
            b,
            geom,
            lines: BTreeSet::new(),
            originals: BTreeSet::new(),
        }));
        self.nodes[a].as_mut().unwrap().adj.insert(id);
        self.nodes[b].as_mut().unwrap().adj.insert(id);
        id
    }

    pub fn other(&self, e: EdgeId, n: NodeId) -> NodeId {
        let e = self.edges[e].as_ref().unwrap();
        if e.a == n { e.b } else { e.a }
    }

    pub fn remove_edge(&mut self, eid: EdgeId) {
        if let Some(e) = self.edges[eid].take() {
            if let Some(n) = self.nodes[e.a].as_mut() {
                n.adj.remove(&eid);
            }
            if let Some(n) = self.nodes[e.b].as_mut() {
                n.adj.remove(&eid);
            }
        }
    }

    /// Append an already-topologized disconnected component while remapping all
    /// graph-local IDs. This lets Globeflower release each raw GTFS component
    /// before loading the next one instead of running topo on a continental graph.
    pub fn append(&mut self, other: Graph) {
        let node_offset = self.nodes.len();
        let edge_offset = self.edges.len();
        let line_offset = self.lines.len();
        let original_offset = self
            .edges
            .iter()
            .flatten()
            .flat_map(|edge| edge.originals.iter().copied())
            .max()
            .map_or(0, |id| id + 1);

        self.lines.extend(other.lines);

        for node in other.nodes {
            self.nodes.push(node.map(|mut node| {
                node.id += node_offset;
                node.not_served = node
                    .not_served
                    .into_iter()
                    .map(|line| line + line_offset)
                    .collect();
                node.adj = node
                    .adj
                    .into_iter()
                    .map(|edge| edge + edge_offset)
                    .collect();
                node.allowed_turns = node
                    .allowed_turns
                    .into_iter()
                    .map(|(line, turns)| {
                        (
                            line + line_offset,
                            turns
                                .into_iter()
                                .map(|(a, b)| (a + edge_offset, b + edge_offset))
                                .collect(),
                        )
                    })
                    .collect();
                node.conn_exc = node
                    .conn_exc
                    .into_iter()
                    .map(|(line, from_map)| {
                        (
                            line + line_offset,
                            from_map
                                .into_iter()
                                .map(|(from, tos)| {
                                    (
                                        from + edge_offset,
                                        tos.into_iter().map(|to| to + edge_offset).collect(),
                                    )
                                })
                                .collect(),
                        )
                    })
                    .collect();
                node
            }));
        }

        for edge in other.edges {
            self.edges.push(edge.map(|mut edge| {
                edge.id += edge_offset;
                edge.a += node_offset;
                edge.b += node_offset;
                edge.lines = edge
                    .lines
                    .into_iter()
                    .map(|occ| LineOcc {
                        line: occ.line + line_offset,
                        direction: occ.direction.map(|node| node + node_offset),
                    })
                    .collect();
                edge.originals = edge
                    .originals
                    .into_iter()
                    .map(|id| id + original_offset)
                    .collect();
                edge
            }));
        }
    }
}

// LOOM performs its entire gtfs2graph/topo pipeline in EPSG:3857.
// NEVER use geodesic meters for Chapter 3's 5/10/50/500 meter thresholds.
const WEB_MERCATOR_R: f64 = 6_378_137.0;

pub fn web_mercator(p: Point) -> (f64, f64) {
    let lat = p.lat.clamp(-85.05112878, 85.05112878).to_radians();
    (
        WEB_MERCATOR_R * p.lon.to_radians(),
        WEB_MERCATOR_R * (std::f64::consts::FRAC_PI_4 + lat * 0.5).tan().ln(),
    )
}

pub fn from_web_mercator((x, y): (f64, f64)) -> Point {
    Point {
        lon: (x / WEB_MERCATOR_R).to_degrees(),
        lat: (2.0 * (y / WEB_MERCATOR_R).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees(),
    }
}

pub fn metric_distance_m(a: Point, b: Point) -> f64 {
    let (ax, ay) = web_mercator(a);
    let (bx, by) = web_mercator(b);
    (ax - bx).hypot(ay - by)
}

// Retained for non-topological callers that need actual surface distances.
pub fn haversine_m(a: Point, b: Point) -> f64 {
    let r = 6_371_008.8_f64;
    let p1 = a.lat.to_radians();
    let p2 = b.lat.to_radians();
    let dp = (b.lat - a.lat).to_radians();
    let dl = (b.lon - a.lon).to_radians();
    let h = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * r * h.sqrt().asin()
}

pub fn polyline_len(g: &[Point]) -> f64 {
    g.windows(2).map(|w| metric_distance_m(w[0], w[1])).sum()
}

pub fn lerp(a: Point, b: Point, t: f64) -> Point {
    let (ax, ay) = web_mercator(a);
    let (bx, by) = web_mercator(b);
    from_web_mercator((ax + (bx - ax) * t, ay + (by - ay) * t))
}

pub fn densify(g: &[Point], step_m: f64) -> Vec<Point> {
    if g.len() < 2 {
        return g.to_vec();
    }
    let mut out = vec![g[0]];
    for w in g.windows(2) {
        let d = metric_distance_m(w[0], w[1]);
        let n = (d / step_m).ceil().max(1.0) as usize;
        for i in 1..=n {
            out.push(lerp(w[0], w[1], i as f64 / n as f64));
        }
    }
    out
}

pub fn project_on_segment(p: Point, a: Point, b: Point) -> (Point, f64) {
    let (px, py) = web_mercator(p);
    let (ax, ay) = web_mercator(a);
    let (bx, by) = web_mercator(b);
    let (vx, vy) = (bx - ax, by - ay);
    let den = vx * vx + vy * vy;
    let t = if den <= 1e-16 {
        0.0
    } else {
        (((px - ax) * vx + (py - ay) * vy) / den).clamp(0.0, 1.0)
    };
    (from_web_mercator((ax + t * vx, ay + t * vy)), t)
}

pub fn project_on_polyline(p: Point, g: &[Point]) -> (Point, f64, f64) {
    let total = polyline_len(g).max(1e-9);
    let mut before = 0.0;
    let mut best = (g[0], 0.0, f64::INFINITY);
    for w in g.windows(2) {
        let seg = metric_distance_m(w[0], w[1]);
        let (q, t) = project_on_segment(p, w[0], w[1]);
        let d = metric_distance_m(p, q);
        if d < best.2 {
            best = (q, (before + t * seg) / total, d);
        }
        before += seg;
    }
    best
}

pub fn subline(g: &[Point], from: f64, to: f64) -> Vec<Point> {
    if g.len() < 2 {
        return g.to_vec();
    }
    // LOOM uses geometric distance along the polyline. Sampling at 2.5m
    // and then indexing by vertex count silently changes the meaning of
    // fractional positions and multiplies allocations on long GTFS shapes.
    let total = polyline_len(g);
    if total <= 1e-9 {
        return vec![g[0], *g.last().unwrap()];
    }
    let start = from.clamp(0.0, 1.0) * total;
    let end = to.clamp(0.0, 1.0) * total;
    if end < start {
        return vec![g[0], g[0]];
    }
    let mut out = Vec::new();
    let mut travelled = 0.0;
    for segment in g.windows(2) {
        let len = metric_distance_m(segment[0], segment[1]);
        let next = travelled + len;
        if next >= start && travelled <= end && len > 0.0 {
            let a = ((start - travelled) / len).clamp(0.0, 1.0);
            let b = ((end - travelled) / len).clamp(0.0, 1.0);
            if out.is_empty() {
                out.push(lerp(segment[0], segment[1], a));
            }
            if next < end {
                out.push(segment[1]);
            } else {
                out.push(lerp(segment[0], segment[1], b));
                break;
            }
        }
        travelled = next;
    }
    if out.len() == 1 {
        out.push(out[0]);
    }
    if out.is_empty() {
        return vec![g[0], g[0]];
    }
    out
}

#[cfg(test)]
mod projected_metric_tests {
    use super::*;

    #[test]
    fn paris_aggregation_uses_web_mercator_meters() {
        let a = Point {
            lon: 2.363,
            lat: 48.867,
        };
        let b = Point {
            lon: 2.3635,
            lat: 48.867,
        };
        // At Paris latitude, half a millidegree longitude is ~36.6m on
        // the ground, but ~55.66m in LOOM's EPSG:3857 coordinates.
        assert!(haversine_m(a, b) < 50.0);
        assert!(metric_distance_m(a, b) > 50.0);
        assert!((polyline_len(&[a, b]) - metric_distance_m(a, b)).abs() < 1e-6);
    }

    #[test]
    fn projected_interpolation_preserves_projected_half_length() {
        let a = Point {
            lon: 2.363,
            lat: 48.867,
        };
        let b = Point {
            lon: 2.370,
            lat: 48.872,
        };
        let mid = lerp(a, b, 0.5);
        assert!((metric_distance_m(a, mid) - metric_distance_m(mid, b)).abs() < 1e-5);
    }
}

#[cfg(test)]
mod occurrence_tests {
    use super::*;
    #[test]
    fn reverse_trip_makes_bidirectional_occurrence() {
        let mut lines = BTreeSet::new();
        add_line_occ(
            &mut lines,
            LineOcc {
                line: 5,
                direction: Some(0),
            },
        );
        add_line_occ(
            &mut lines,
            LineOcc {
                line: 5,
                direction: Some(1),
            },
        );
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.iter().next().unwrap().direction, None);
    }
}
