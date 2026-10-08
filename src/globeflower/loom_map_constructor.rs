//! Edge-by-edge, indexed shared-segment construction following LOOM's
//! MapConstructor::collapseShrdSegs traversal, without global atom union-find.
use crate::loom_graph::{
    Graph, LineOcc, Point, add_line_occ, haversine_m, lerp, polyline_len,
};
use log::info;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

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
    bins: HashMap<Cell, HashSet<usize>>,
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
        self.bins.entry(cell(p, self.scale)).or_default().insert(id);
    }
    // Moving a node must remove its OLD spatial entry. Otherwise a stale
    // cell may return a point now outside the search envelope.
    fn relocate(&mut self, previous: Point, next: Point, id: usize) {
        let old_cell = cell(previous, self.scale);
        let new_cell = cell(next, self.scale);
        if old_cell != new_cell {
            if let Some(ids) = self.bins.get_mut(&old_cell) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.bins.remove(&old_cell);
                }
            }
            self.bins.entry(new_cell).or_default().insert(id);
        }
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
                        if distance < best.0
                            || ((distance - best.0).abs() < 1e-9
                                && best.1.is_some_and(|current| id < current))
                        {
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
        polyline_len(&eb.geom)
            .total_cmp(&polyline_len(&ea.geom))
            .then_with(|| a.cmp(&b))
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
        add_line_occ(
            &mut target.lines,
            LineOcc {
                line: occ.line,
                direction,
            },
        );
    }
}

fn construct_once(input: &Graph, radius: f64, segment_length: f64) -> Graph {
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
        // LOOM prepends and appends the actual graph-node positions before
        // sampling the geometry. The shape endpoints can be offset from stops.
        let mut source_geometry = Vec::with_capacity(edge.geom.len() + 2);
        source_geometry.push(input.nodes[edge.a].as_ref().unwrap().pos);
        source_geometry.extend(edge.geom.iter().copied());
        source_geometry.push(input.nodes[edge.b].as_ref().unwrap().pos);
        let points = sample(&source_geometry, segment_length);
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
                // C++ ndCollapseCand: move the accepted candidate toward the sample.
                if candidate.is_some() {
                    let old = out.nodes[id].as_ref().unwrap().pos;
                    let middle = lerp(old, point, 0.5);
                    out.nodes[id].as_mut().unwrap().pos = middle;
                    index.relocate(old, middle, id);
                }
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
            // C++ collapseShrdSegs stops when the image of the destination
            // has been reached, avoiding connections beyond that anchor.
            if back.is_some() && Some(id) == back {
                break;
            }
        }
    }
    // Match LOOM's post-collapse geometry rewrite: every edge endpoint
    // must coincide with the final position of its incident node.
    for edge in out.edges.iter_mut().flatten() {
        if let Some(first) = edge.geom.first_mut() {
            *first = out.nodes[edge.a].as_ref().unwrap().pos;
        }
        if let Some(last) = edge.geom.last_mut() {
            *last = out.nodes[edge.b].as_ref().unwrap().pos;
        }
    }
    // Empty or otherwise unmapped stations are handled by the existing
    // station-insertion stage, which retains source station positions.
    out
}

/// The C++ `lineEq` checks line identity AND whether directions continue
/// across the common node.  Comparing LineOcc sets directly is incorrect:
/// the two edges normally name DIFFERENT destination node IDs.
fn compatible(a: &BTreeSet<LineOcc>, b: &BTreeSet<LineOcc>, mid: usize) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().all(|left| {
        let Some(right) = b.iter().find(|r| r.line == left.line) else {
            return false;
        };
        match (left.direction, right.direction) {
            (None, None) => true,
            (Some(x), Some(y)) => (x == mid) != (y == mid),
            _ => false,
        }
    })
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
        // C++ LineNodePL::connOccurs is TRUE unless a connection exception
        // explicitly forbids this turn. It does NOT demand that a GTFS trip
        // explicitly recorded the transition: that evidence is used later by
        // RestrInferrer. Requiring it here creates spurious degree-two nodes.
        if !compatible(&a.lines, &b.lines, mid) || !a.lines.iter().all(|occ| {
            !node.conn_exc.get(&occ.line)
                .and_then(|forbidden| forbidden.get(&ids[0]))
                .is_some_and(|targets| targets.contains(&ids[1]))
        }) {
            continue;
        }
        let u = if a.a == mid { a.b } else { a.a };
        let v = if b.a == mid { b.b } else { b.a };
        if u == v || polyline_len(&a.geom) + polyline_len(&b.geom) > MAX_CONTRACTION_METERS {
            continue;
        }
        // C++ contractEdges refuses contraction when the endpoint pair is
        // already directly connected (protects parallel and triangular tracks).
        if out.nodes[u].as_ref().is_some_and(|n| {
            n.adj.iter().any(|&eid| {
                eid != ids[0]
                    && eid != ids[1]
                    && out.edges[eid]
                        .as_ref()
                        .is_some_and(|e| (e.a == u && e.b == v) || (e.a == v && e.b == u))
            })
        }) {
            continue;
        }
        let left = a.clone();
        let right = b.clone();
        let mut geom = left.geom.clone();
        if left.b != mid {
            geom.reverse();
        }
        let mut tail = right.geom.clone();
        if right.a != mid {
            tail.reverse();
        }
        geom.extend(tail.into_iter().skip(1));
        let mut occurrences = BTreeSet::new();
        for occ in &left.lines {
            let destination = match occ.direction {
                None => None,
                Some(d) if d == mid => Some(v),
                Some(d) if d == u => Some(u),
                _ => continue,
            };
            add_line_occ(
                &mut occurrences,
                LineOcc {
                    line: occ.line,
                    direction: destination,
                },
            );
        }
        // Direction must continue across the other half, not reverse at mid.
        for occ in &right.lines {
            if let Some(l) = occurrences.iter().find(|l| l.line == occ.line) {
                let consistent = match (l.direction, occ.direction) {
                    (None, None) => true,
                    (Some(d), Some(x)) if d == v => x == v,
                    (Some(d), Some(x)) if d == u => x == mid,
                    _ => false,
                };
                if !consistent {
                    occurrences.clear();
                    break;
                }
            }
        }
        if occurrences.is_empty() {
            continue;
        }
        let originals = left.originals.union(&right.originals).copied().collect();
        out.remove_edge(ids[0]);
        out.remove_edge(ids[1]);
        out.nodes[mid] = None;
        let eid = out.add_edge(u, v, geom);
        let edge = out.edges[eid].as_mut().unwrap();
        edge.lines = occurrences;
        edge.originals = originals;
        // C++ LineGraph::nodeRpl updates the turn graph when edges are
        // replaced. Remap both endpoints before another contraction pass.
        for endpoint in [u, v] {
            if let Some(node) = out.nodes[endpoint].as_mut() {
                for turns in node.allowed_turns.values_mut() {
                    *turns = turns.iter().map(|&(x, y)| (
                        if x == ids[0] || x == ids[1] { eid } else { x },
                        if y == ids[0] || y == ids[1] { eid } else { y },
                    )).filter(|(x, y)| x != y).collect();
                }
            }
        }
        todo.push_back(u);
        todo.push_back(v);
    }
}

/// C++ MapConstructor::combineNodes: redirect all edges of `remove` to
/// `keep`, folding duplicate endpoint pairs and preserving line directions
/// and source provenance. Used only on actual short connecting edges.
fn combine_nodes(graph: &mut Graph, remove: usize, keep: usize, connecting: usize) -> bool {
    if remove == keep {
        return false;
    }
    let (Some(left), Some(right), Some(connector)) = (
        graph.nodes.get(remove).and_then(Option::as_ref),
        graph.nodes.get(keep).and_then(Option::as_ref),
        graph.edges.get(connecting).and_then(Option::as_ref),
    ) else {
        return false;
    };
    if left.stops.len() > 0 || right.stops.len() > 0 {
        return false;
    }
    if !((connector.a == remove && connector.b == keep)
        || (connector.b == remove && connector.a == keep))
    {
        return false;
    }
    let connecting_originals = connector.originals.clone();
    let midpoint = lerp(left.pos, right.pos, 0.5);
    let incident: Vec<usize> = left
        .adj
        .iter()
        .copied()
        .filter(|&id| id != connecting)
        .collect();
    for id in incident {
        let Some(old) = graph.edges[id].as_ref().cloned() else {
            continue;
        };
        let other = if old.a == remove { old.b } else { old.a };
        if other == keep {
            continue;
        }
        let duplicate = graph.nodes[keep]
            .as_ref()
            .unwrap()
            .adj
            .iter()
            .copied()
            .find(|&candidate| {
                candidate != connecting
                    && candidate != id
                    && graph.edges[candidate].as_ref().is_some_and(|e| {
                        (e.a == keep && e.b == other) || (e.b == keep && e.a == other)
                    })
            });
        if let Some(target_id) = duplicate {
            let target = graph.edges[target_id].as_ref().unwrap().clone();
            let mut old_geometry = old.geom.clone();
            if old.a != target.a && old.b != target.b {
                old_geometry.reverse();
            }
            // C++ MapConstructor::foldEdges -> geomAvg uses the UNWEIGHTED
            // PolyLine::average and subsequently simplifies by 0.5 meters.
            // Do not introduce an extra 10m midpoint acceptance predicate:
            // the C++ routine does not have one.
            let averaged = crate::loom_polyline::average(&[target.geom.clone(), old_geometry]);
            let into = graph.edges[target_id].as_mut().unwrap();
            into.geom = crate::loom_polyline::simplify(&averaged, 0.5);
            into.originals.extend(old.originals.iter().copied());
            for occ in &old.lines {
                let direction = occ.direction.map(|d| if d == remove { keep } else { d });
                add_line_occ(
                    &mut into.lines,
                    LineOcc {
                        line: occ.line,
                        direction,
                    },
                );
            }
            graph.remove_edge(id);
        } else {
            // Redirect existing edge without changing its stable edge ID.
            let edge = graph.edges[id].as_mut().unwrap();
            if edge.a == remove {
                edge.a = keep;
            }
            if edge.b == remove {
                edge.b = keep;
            }
            let previous: Vec<_> = edge.lines.iter().copied().collect();
            edge.lines.clear();
            for occ in previous {
                let direction = occ.direction.map(|d| if d == remove { keep } else { d });
                add_line_occ(
                    &mut edge.lines,
                    LineOcc {
                        line: occ.line,
                        direction,
                    },
                );
            }
            graph.nodes[keep].as_mut().unwrap().adj.insert(id);
        }
    }
    graph.remove_edge(connecting);
    graph.nodes[keep].as_mut().unwrap().pos = midpoint;
    for &id in &graph.nodes[keep].as_ref().unwrap().adj.clone() {
        if let Some(e) = graph.edges[id].as_mut() {
            e.originals.extend(connecting_originals.iter().copied());
        }
    }
    graph.nodes[remove] = None;
    true
}

/// C++ `collapseShrdSegs` combines sub-segment-length artifacts if at least
/// one endpoint is a branching vertex. This is a local cleanup; contraction
/// over the full aggregation radius is more disruptive and intentionally
/// remains separate from the 5m short-artifact pass.
fn collapse_short_artifacts(graph: &mut Graph, segment_length: f64) {
    let mut queue: VecDeque<usize> = graph
        .edges
        .iter()
        .flatten()
        .filter_map(|e| {
            let (Some(a), Some(b)) = (graph.nodes[e.a].as_ref(), graph.nodes[e.b].as_ref()) else {
                return None;
            };
            if haversine_m(a.pos, b.pos) <= segment_length && (a.adj.len() >= 3 || b.adj.len() >= 3)
            {
                Some(e.id)
            } else {
                None
            }
        })
        .collect();
    let mut changes = 0usize;
    while let Some(id) = queue.pop_front() {
        let Some(edge) = graph.edges.get(id).and_then(Option::as_ref) else {
            continue;
        };
        let (a, b) = (edge.a, edge.b);
        let (Some(left), Some(right)) = (graph.nodes[a].as_ref(), graph.nodes[b].as_ref()) else {
            continue;
        };
        if haversine_m(left.pos, right.pos) > segment_length
            || (left.adj.len() < 3 && right.adj.len() < 3)
        {
            continue;
        }
        let adjacent: Vec<_> = left.adj.iter().chain(right.adj.iter()).copied().collect();
        if combine_nodes(graph, a, b, id) {
            changes += 1;
            for candidate in adjacent {
                queue.push_back(candidate);
            }
        }
    }
    if changes > 0 {
        info!(
            "[topo/mapconstructor] combined {} sub-5m junction artifacts",
            changes
        );
    }
}

/// C++ MapConstructor::averageNodePositions, also called BEFORE collapse.
pub fn average_node_positions(graph: &mut Graph) {
    // C++ MapConstructor::averageNodePositions: take the mean of incident
    // polyline start/end positions BEFORE trimming the intersection arms.
    // This mean is in WebMercator meters, not geographic degrees.
    let mut averaged = Vec::with_capacity(graph.nodes.len());
    for node in graph.nodes.iter().map(Option::as_ref) {
        let Some(node) = node else { averaged.push(None); continue; };
        let incident: Vec<Point> = node.adj.iter().filter_map(|&eid| {
            let edge = graph.edges.get(eid).and_then(Option::as_ref)?;
            if edge.a == node.id { edge.geom.first().copied() }
            else { edge.geom.last().copied() }
        }).collect();
        averaged.push(if incident.is_empty() { None }
            else { Some(crate::loom_polyline::centroid(&incident)) });
    }
    for (node, average) in graph.nodes.iter_mut().zip(averaged) {
        if let (Some(node), Some(pos)) = (node.as_mut(), average) { node.pos = pos; }
    }
}

/// C++ MapConstructor::cleanUpGeoms, after initial node-artifact removal.
pub fn clean_up_geoms(graph: &mut Graph) {
    for edge in graph.edges.iter_mut().flatten() {
        let from = graph.nodes[edge.a].as_ref().unwrap().pos;
        let to = graph.nodes[edge.b].as_ref().unwrap().pos;
        edge.geom = crate::loom_polyline::trim_between_projections(&edge.geom, from, to);
    }
}

/// C++ MapConstructor::removeNodeArtifacts(false) contracts valid degree-two
/// nodes before and after shared-segment collapse. Station candidates have
/// already been collected by StatInserter::init at this point.
pub fn remove_node_artifacts(graph: &mut Graph) {
    contract(graph);
}

pub fn reconstruct_intersections(graph: &mut Graph, radius: f64) {
    average_node_positions(graph);
    for edge in graph.edges.iter_mut().flatten() {
        let (Some(from), Some(to)) = (graph.nodes[edge.a].as_ref(), graph.nodes[edge.b].as_ref())
        else {
            continue;
        };
        let len = crate::loom_polyline::metric_length(&edge.geom);
        let mut inner = crate::loom_polyline::segment_at_dist(&edge.geom, radius, len - radius);
        inner.retain(|p| p.lon.is_finite() && p.lat.is_finite());
        let mut geom = Vec::with_capacity(inner.len() + 2);
        geom.push(from.pos);
        geom.extend(inner);
        geom.push(to.pos);
        edge.geom = geom;
    }
}

pub fn construct(input: &Graph, max_distance: f64, segment_length: f64) -> Graph {
    // LOOM repeats collapse until the total network length converges.
    // An iteration cap bounds runtime on continent-scale GTFS datasets.
    // Use the configured aggregation radius, not a hidden 20m ceiling.
    // LOOM's topology builder densifies its segments at approximately 5m.
    assert!(max_distance.is_finite() && max_distance > 0.0);
    assert!(segment_length.is_finite() && segment_length > 0.0);
    let segment_length = segment_length.max(0.5);
    let mut graph = construct_once(input, max_distance, segment_length);
    collapse_short_artifacts(&mut graph, segment_length);
    contract(&mut graph);
    let mut old_len: f64 = graph
        .edges
        .iter()
        .flatten()
        .map(|e| polyline_len(&e.geom))
        .sum();
    for iter in 1..MAX_PASSES {
        let mut next = construct_once(&graph, max_distance, segment_length);
        collapse_short_artifacts(&mut next, segment_length);
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

#[cfg(test)]
mod parity_tests {
    use super::*;

    fn point(x: f64) -> Point {
        Point { lon: x, lat: 0.0 }
    }

    #[test]
    fn line_eq_is_oriented_at_shared_node_not_equal_destinations() {
        let mut left = BTreeSet::new();
        left.insert(LineOcc {
            line: 0,
            direction: Some(1),
        });
        let mut right = BTreeSet::new();
        right.insert(LineOcc {
            line: 0,
            direction: Some(2),
        });
        assert!(compatible(&left, &right, 1));
        assert!(!compatible(&left, &right, 2));
    }

    #[test]
    fn degree_two_contraction_keeps_actual_destination() {
        let mut graph = Graph::default();
        let a = graph.add_node(point(0.0));
        let mid = graph.add_node(point(0.0001));
        let b = graph.add_node(point(0.0002));
        let first = graph.add_edge(a, mid, vec![point(0.0), point(0.0001)]);
        let second = graph.add_edge(mid, b, vec![point(0.0001), point(0.0002)]);
        graph.edges[first].as_mut().unwrap().originals.insert(9);
        graph.edges[second].as_mut().unwrap().originals.insert(9);
        graph.edges[first].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(mid),
        });
        graph.edges[second].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(b),
        });
        contract(&mut graph);
        let edges: Vec<_> = graph.edges.iter().flatten().collect();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].lines.iter().next().unwrap().direction, Some(b));
        assert!(graph.nodes[mid].is_none());
    }

    #[test]
    fn contraction_respects_explicit_cpp_connection_exception() {
        let mut graph = Graph::default();
        let a = graph.add_node(point(0.0));
        let middle = graph.add_node(point(0.0001));
        let b = graph.add_node(point(0.0002));
        let e1 = graph.add_edge(a, middle, vec![point(0.0), point(0.0001)]);
        let e2 = graph.add_edge(middle, b, vec![point(0.0001), point(0.0002)]);
        graph.edges[e1].as_mut().unwrap().lines.insert(LineOcc { line: 0, direction: Some(middle) });
        graph.edges[e2].as_mut().unwrap().lines.insert(LineOcc { line: 0, direction: Some(b) });
        graph.nodes[middle].as_mut().unwrap().conn_exc.entry(0).or_default()
            .entry(e1).or_default().insert(e2);
        contract(&mut graph);
        assert_eq!(graph.edges.iter().flatten().count(), 2);
        assert!(graph.nodes[middle].is_some());
    }

    #[test]
    fn reconstruction_keeps_node_endpoints() {
        let mut graph = Graph::default();
        let a = graph.add_node(point(0.0));
        let b = graph.add_node(point(0.002));
        let edge = graph.add_edge(a, b, vec![point(0.0), point(0.001), point(0.002)]);
        reconstruct_intersections(&mut graph, 10.0);
        assert_eq!(graph.edges[edge].as_ref().unwrap().geom[0], point(0.0));
        assert_eq!(
            *graph.edges[edge].as_ref().unwrap().geom.last().unwrap(),
            point(0.002)
        );
    }

    #[test]
    fn combining_nodes_preserves_adjacency_and_directions() {
        let mut graph = Graph::default();
        let a = graph.add_node(point(0.0));
        let b = graph.add_node(point(0.00001));
        let c = graph.add_node(point(0.001));
        let connector = graph.add_edge(a, b, vec![point(0.0), point(0.00001)]);
        let outgoing = graph.add_edge(a, c, vec![point(0.0), point(0.001)]);
        graph.edges[outgoing]
            .as_mut()
            .unwrap()
            .lines
            .insert(LineOcc {
                line: 0,
                direction: Some(a),
            });
        assert!(combine_nodes(&mut graph, a, b, connector));
        assert!(graph.nodes[a].is_none());
        let edge = graph.edges[outgoing].as_ref().unwrap();
        assert_eq!(edge.a, b);
        assert_eq!(edge.lines.iter().next().unwrap().direction, Some(b));
        assert!(graph.nodes[b].as_ref().unwrap().adj.contains(&outgoing));
        assert!(graph.nodes[c].as_ref().unwrap().adj.contains(&outgoing));
    }
}
