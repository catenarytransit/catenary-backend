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

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub pos: Point,
    pub stops: Vec<Stop>,
    pub not_served: BTreeSet<LineId>,
    pub adj: BTreeSet<EdgeId>,
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

#[derive(Debug, Default)]
pub struct Graph {
    pub nodes: Vec<Option<Node>>,
    pub edges: Vec<Option<Edge>>,
    pub lines: Vec<Line>,
}

impl Graph {
    pub fn add_node(&mut self, pos: Point) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Some(Node {
            id,
            pos,
            stops: vec![],
            not_served: BTreeSet::new(),
            adj: BTreeSet::new(),
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
    g.windows(2).map(|w| haversine_m(w[0], w[1])).sum()
}

pub fn lerp(a: Point, b: Point, t: f64) -> Point {
    Point {
        lon: a.lon + (b.lon - a.lon) * t,
        lat: a.lat + (b.lat - a.lat) * t,
    }
}

pub fn densify(g: &[Point], step_m: f64) -> Vec<Point> {
    if g.len() < 2 {
        return g.to_vec();
    }
    let mut out = vec![g[0]];
    for w in g.windows(2) {
        let d = haversine_m(w[0], w[1]);
        let n = (d / step_m).ceil().max(1.0) as usize;
        for i in 1..=n {
            out.push(lerp(w[0], w[1], i as f64 / n as f64));
        }
    }
    out
}

pub fn project_on_segment(p: Point, a: Point, b: Point) -> (Point, f64) {
    let lat0 = p.lat.to_radians();
    let sx = 111_320.0 * lat0.cos();
    let sy = 110_540.0;
    let ax = (a.lon - p.lon) * sx;
    let ay = (a.lat - p.lat) * sy;
    let bx = (b.lon - p.lon) * sx;
    let by = (b.lat - p.lat) * sy;
    let vx = bx - ax;
    let vy = by - ay;
    let den = vx * vx + vy * vy;
    let t = if den == 0.0 {
        0.0
    } else {
        (-(ax * vx + ay * vy) / den).clamp(0.0, 1.0)
    };
    (lerp(a, b, t), t)
}

pub fn project_on_polyline(p: Point, g: &[Point]) -> (Point, f64, f64) {
    let total = polyline_len(g).max(1e-9);
    let mut before = 0.0;
    let mut best = (g[0], 0.0, f64::INFINITY);
    for w in g.windows(2) {
        let seg = haversine_m(w[0], w[1]);
        let (q, t) = project_on_segment(p, w[0], w[1]);
        let d = haversine_m(p, q);
        if d < best.2 {
            best = (q, (before + t * seg) / total, d);
        }
        before += seg;
    }
    best
}

pub fn subline(g: &[Point], from: f64, to: f64) -> Vec<Point> {
    let dense = densify(g, 2.5);
    if dense.len() < 2 {
        return dense;
    }
    let a = ((dense.len() - 1) as f64 * from).floor() as usize;
    let b = ((dense.len() - 1) as f64 * to).ceil() as usize;
    dense[a.min(dense.len() - 1)..=b.min(dense.len() - 1)].to_vec()
}
