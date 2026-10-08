//! Edge-by-edge, indexed shared-segment construction following LOOM's
//! MapConstructor::collapseShrdSegs traversal, without global atom union-find.
use crate::loom_graph::{Graph, LineOcc, Point, haversine_m, lerp, polyline_len};
use log::info;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

const SAMPLE_METERS: f64 = 5.0;
const MAX_PASSES: usize = 50;
const MAX_CONTRACTION_METERS: f64 = 500.0;

type Cell = (i64, i64);

fn cell(p: Point, scale: f64) -> Cell {
    // Fixed Mercator-free latitude/longitude bins. Variable longitude width
    // affects only false positives, never acceptance (which uses haversine).
    (
        (p.lon / scale).floor() as i64,
        (p.lat / scale).floor() as i64,
    )
}

struct NodeIndex {
    bins: HashMap<Cell, Vec<usize>>,
    scale: f64,
    radius: f64,
}
impl NodeIndex {
    fn new(radius: f64) -> Self {
        Self {
            bins: HashMap::new(),
            scale: (radius / 111_320.0).max(1e-9),
            radius,
        }
    }
    fn add(&mut self, p: Point, id: usize) {
        self.bins.entry(cell(p, self.scale)).or_default().push(id);
    }
    fn nearest(
        &self,
        p: Point,
        out: &Graph,
        forbidden: &HashSet<usize>,
        span_a: Option<Point>,
        span_b: Option<Point>,
    ) -> Option<usize> {
        let key = cell(p, self.scale);
        // C++ MapConstructor::ndCollapseCand: a candidate must be nearer
        // than both protected ends of the current source-edge span.
        let span_limit = [span_a, span_b]
            .into_iter()
            .flatten()
            .map(|q| haversine_m(p, q) / std::f64::consts::SQRT_2)
            .fold(self.radius, f64::min);
        let mut best = (span_limit, None);
        // Longitude's physical width shrinks with latitude; expand the
        // longitude search window instead of losing candidates at high latitudes.
        let cos = p.lat.to_radians().cos().abs().max(0.1);
        let extent_x = (1.0 / cos).ceil() as i64 + 1;
        for dx in -extent_x..=extent_x {
            for dy in -2..=2 {
                if let Some(ids) = self.bins.get(&(key.0 + dx, key.1 + dy)) {
                    for &id in ids {
                        if forbidden.contains(&id) {
                            continue;
                        }
                        let Some(node) = out.nodes[id].as_ref() else {
                            continue;
                        };
                        // C++ ndCollapseCand excludes isolated nodes. These can
                        // otherwise attract an unrelated edge and create a spur.
                        if node.adj.is_empty() {
                            continue;
                        }
                        let distance = haversine_m(p, node.pos);
                        if distance < best.0 {
                            best = (distance, Some(id));
                        }
                    }
                }
            }
        }
        best.1
    }
}

fn sample(geometry: &[Point], distance: f64) -> Vec<Point> {
    if geometry.len() < 2 {
        return geometry.to_vec();
    }
    let mut sampled = vec![geometry[0]];
    for pair in geometry.windows(2) {
        let meters = haversine_m(pair[0], pair[1]);
        let steps = (meters / distance).ceil().max(1.0) as usize;
        for i in 1..=steps {
            sampled.push(lerp(pair[0], pair[1], i as f64 / steps as f64));
        }
    }
    sampled
}

fn collect_edges(g: &Graph) -> Vec<usize> {
    let mut edges: Vec<_> = g
        .edges
        .iter()
        .enumerate()
        .filter_map(|(id, edge)| edge.as_ref().map(|_| id))
        .collect();
    edges.sort_unstable_by(|&a, &b| {
        let ea = g.edges[a].as_ref().unwrap();
        let eb = g.edges[b].as_ref().unwrap();
        polyline_len(&eb.geom).total_cmp(&polyline_len(&ea.geom))
    });
    edges
}

// Insert segments as we go: C++ collapseShrdSegs sees non-isolated candidates.
fn insert_constructed_segment(
    out: &mut Graph,
    output_edges: &mut HashMap<(usize, usize), usize>,
    a: usize,
    b: usize,
    source: &crate::loom_graph::Edge,
) {
    if a == b {
        return;
    }
    let key = (a.min(b), a.max(b));
    let id = *output_edges.entry(key).or_insert_with(|| {
        let geometry = vec![
            out.nodes[a].as_ref().unwrap().pos,
            out.nodes[b].as_ref().unwrap().pos,
        ];
        out.add_edge(a, b, geometry)
    });
    let target = out.edges[id].as_mut().unwrap();
    target.originals.extend(source.originals.iter().copied());
    for occ in &source.lines {
        let direction = occ.direction.map(|old| if old == source.a { a } else { b });
        target.lines.insert(LineOcc {
            line: occ.line,
            direction,
        });
    }
}

fn construct_once(input: &Graph, radius: f64) -> Graph {
    let mut out = Graph::default();
    out.lines = input.lines.clone();
    let mut index = NodeIndex::new(radius);
    let mut mapped_endpoints = HashMap::<usize, usize>::new();
    let mut output_edges = HashMap::<(usize, usize), usize>::new();

    for source_id in collect_edges(input) {
        let edge = input.edges[source_id].as_ref().unwrap();
        if edge.geom.len() < 2 {
            continue;
        }
        let points = sample(&edge.geom, SAMPLE_METERS);
        let mut path = Vec::with_capacity(points.len());
        // LOOM excludes nodes visited on the current input edge from collapse
        // candidates. This is vital: adjacent 20m samples must not all merge
        // simply because maxAggrDistance is 50m.
        let mut forbidden = HashSet::new();
        let mut front: Option<usize> = None;
        // Endpoints must be mapped before processing their neighboring samples.
        // An already mapped endpoint is a protected topology anchor.
        let back = mapped_endpoints.get(&edge.b).copied();
        for (i, &point) in points.iter().enumerate() {
            let endpoint = if i == 0 {
                Some(edge.a)
            } else if i == points.len() - 1 {
                Some(edge.b)
            } else {
                None
            };
            let mapped = endpoint.and_then(|old| mapped_endpoints.get(&old).copied());
            let id = if let Some(node_id) = mapped {
                node_id
            } else {
                let span_a = front.and_then(|id| out.nodes[id].as_ref().map(|n| n.pos));
                let span_b = if i + 1 == points.len() {
                    None
                } else {
                    back.and_then(|id| out.nodes[id].as_ref().map(|n| n.pos))
                };
                let candidate = index.nearest(point, &out, &forbidden, span_a, span_b);
                let id = candidate.unwrap_or_else(|| {
                    let id = out.add_node(point);
                    index.add(point, id);
                    id
                });
                if let Some(old) = endpoint {
                    mapped_endpoints.insert(old, id);
                }
                id
            };
            // Preserve a self-loop's actual input topology, but prevent an
            // intermediate short-cycle from degenerating into a self-edge.
            if path.last().copied() != Some(id) {
                path.push(id);
            }
            forbidden.insert(id);
            if front.is_none() {
                front = Some(id);
            }
            // Materialize each edge now so subsequent candidates have degree.
            if path.len() >= 2 {
                let a = path[path.len() - 2];
                let b = path[path.len() - 1];
                if a != b {
                    insert_constructed_segment(&mut out, &mut output_edges, a, b, edge);
                }
            }
        }
    }
    // Empty or otherwise unmapped stations are handled by the existing
    // station-insertion stage, which retains source station positions.
    out
}

fn compatible(a: &BTreeSet<LineOcc>, b: &BTreeSet<LineOcc>) -> bool {
    a == b
}

fn contract(out: &mut Graph) {
    let mut todo: VecDeque<usize> = (0..out.nodes.len()).collect();
    while let Some(mid) = todo.pop_front() {
        let Some(node) = out.nodes[mid].as_ref() else {
            continue;
        };
        if node.adj.len() != 2 || !node.stops.is_empty() {
            continue;
        }
        let ids: Vec<_> = node.adj.iter().copied().collect();
        let (Some(a), Some(b)) = (out.edges[ids[0]].as_ref(), out.edges[ids[1]].as_ref()) else {
            continue;
        };
        if !compatible(&a.lines, &b.lines) {
            continue;
        }
        let u = if a.a == mid { a.b } else { a.a };
        let v = if b.a == mid { b.b } else { b.a };
        if u == v
            || out.nodes[u].as_ref().unwrap().adj.iter().any(|&eid| {
                eid != ids[0]
                    && eid != ids[1]
                    && out.edges[eid]
                        .as_ref()
                        .is_some_and(|e| (e.a == u && e.b == v) || (e.a == v && e.b == u))
            })
            || polyline_len(&a.geom) + polyline_len(&b.geom) > MAX_CONTRACTION_METERS
        {
            continue;
        }
        let left = a.clone();
        let right = b.clone();
        let mut geom = left.geom;
        if left.b != mid {
            geom.reverse();
        }
        let mut tail = right.geom;
        if right.a != mid {
            tail.reverse();
        }
        geom.extend(tail.into_iter().skip(1));
        out.remove_edge(ids[0]);
        out.remove_edge(ids[1]);
        out.nodes[mid] = None;
        let new_edge_id = out.add_edge(u, v, geom);
        let edge = out.edges[new_edge_id].as_mut().unwrap();
        edge.lines = left.lines;
        edge.originals = left.originals.union(&right.originals).copied().collect();
        todo.push_back(u);
        todo.push_back(v);
    }
}

pub fn construct(input: &Graph, max_distance: f64) -> Graph {
    // LOOM repeats collapse until the total network length converges.
    // An iteration cap bounds runtime on continent-scale GTFS datasets.
    // Use the configured aggregation radius, not a hidden 20m ceiling.
    // LOOM's topology builder densifies its segments at approximately 5m.
    assert!(max_distance.is_finite() && max_distance > 0.0);
    let mut graph = construct_once(input, max_distance);
    contract(&mut graph);
    let mut old_len: f64 = graph
        .edges
        .iter()
        .flatten()
        .map(|e| polyline_len(&e.geom))
        .sum();
    for iter in 1..MAX_PASSES {
        let mut next = construct_once(&graph, max_distance);
        contract(&mut next);
        let new_len: f64 = next
            .edges
            .iter()
            .flatten()
            .map(|e| polyline_len(&e.geom))
            .sum();
        let gap = (new_len - old_len).abs() / old_len.max(1.0);
        info!(
            "[topo/mapconstructor] pass {}: {} nodes {} edges, length gap {:.5}",
            iter + 1,
            next.nodes.iter().flatten().count(),
            next.edges.iter().flatten().count(),
            gap
        );
        // Never accept a collapsing iteration that obliterates connectivity.
        if next.edges.iter().flatten().next().is_none() {
            break;
        }
        graph = next;
        old_len = new_len;
        if gap < 0.002 {
            break;
        }
    }
    graph
}
