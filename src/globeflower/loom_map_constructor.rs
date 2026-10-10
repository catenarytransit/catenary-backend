//! Edge-by-edge, indexed shared-segment construction following LOOM's
//! MapConstructor::collapseShrdSegs traversal, without global atom union-find.
use crate::loom_graph::{
    Graph, LineOcc, Point, add_line_occ, lerp, metric_distance_m, polyline_len, web_mercator,
};
use log::info;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

const MAX_PASSES: usize = 50;
const MAX_CONTRACTION_METERS: f64 = 500.0;

type Cell = (i64, i64);

fn cell(p: Point, scale: f64) -> Cell {
    let (x, y) = web_mercator(p);
    ((x / scale).floor() as i64, (y / scale).floor() as i64)
}

// LOOM's distance-only ndCollapseCand can weld orthogonal tunnels together.
// Angles are unoriented: reverse-direction service on the same alignment is
// compatible, but a geometric X crossing is not a railway junction.
const SAME_LINE_COS: f64 = 0.866_025_403_784_438_6; // cos(30 degrees)
const OTHER_LINE_COS: f64 = 0.939_692_620_785_908_4; // cos(20 degrees)
const OTHER_LINE_MAX_SNAP_M: f64 = 12.0; // projected metres, intentionally conservative
const SAME_LINE_MAX_SNAP_M: f64 = 25.0; // different shape instances may be noisier

fn track_tangent(points: &[Point], at: usize) -> Option<(f64, f64)> {
    if points.len() < 2 || at >= points.len() {
        return None;
    }
    // Use a 4-sample window rather than the immediately adjacent 5m atom.
    // This suppresses heading noise without treating an entire station-to-
    // station segment as straight (important for circular services).
    let a = web_mercator(points[at.saturating_sub(2)]);
    let b = web_mercator(points[(at + 2).min(points.len() - 1)]);
    let (dx, dy) = (b.0 - a.0, b.1 - a.1);
    let norm = dx.hypot(dy);
    (norm > 1e-6).then_some((dx / norm, dy / norm))
}

fn node_track_aligned(
    graph: &Graph,
    id: usize,
    tangent: (f64, f64),
    cos_limit: f64,
    required_lines: Option<&BTreeSet<usize>>,
) -> bool {
    let Some(node) = graph.nodes.get(id).and_then(Option::as_ref) else {
        return false;
    };
    let from = web_mercator(node.pos);
    node.adj.iter().any(|&eid| {
        let Some(edge) = graph.edges[eid].as_ref() else {
            return false;
        };
        if required_lines.is_some_and(|lines| !edge.lines.iter().any(|o| lines.contains(&o.line))) {
            return false;
        }
        let other = if edge.a == id { edge.b } else { edge.a };
        let Some(to) = graph.nodes.get(other).and_then(Option::as_ref) else {
            return false;
        };
        let to = web_mercator(to.pos);
        let (dx, dy) = (to.0 - from.0, to.1 - from.1);
        let norm = dx.hypot(dy);
        norm > 1e-6 && (dx * tangent.0 + dy * tangent.1).abs() >= norm * cos_limit
    })
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
            scale: radius.max(1e-9),
            radius,
        }
    }
    fn add(&mut self, p: Point, id: usize) {
        self.bins.entry(cell(p, self.scale)).or_default().insert(id);
    }
    fn remove(&mut self, p: Point, id: usize) {
        let key = cell(p, self.scale);
        if let Some(ids) = self.bins.get_mut(&key) {
            ids.remove(&id);
            if ids.is_empty() {
                self.bins.remove(&key);
            }
        }
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
    // Cross-route merging needs evidence of a *corridor*, not one accidental
    // coincidence. Sample only six bounded lookahead positions; querying the
    // existing node index preserves O(K) local work, never O(V^2).
    fn sustained_alignment(
        &self,
        points: &[Point],
        at: usize,
        out: &Graph,
        candidate_lines: &BTreeSet<usize>,
        radius: f64,
    ) -> bool {
        let mut supported = 0;
        let mut first = at;
        let mut last = at;
        let mut min_lateral = f64::INFINITY;
        let mut max_lateral = f64::NEG_INFINITY;
        for offset in [-8isize, -4, -2, 2, 4, 8] {
            let Some(j) = at.checked_add_signed(offset).filter(|&j| j < points.len()) else {
                continue;
            };
            let Some(heading) = track_tangent(points, j) else {
                continue;
            };
            let p = points[j];
            let key = cell(p, self.scale);
            let mut witness: Option<(f64, f64)> = None;
            for dx in -1..=1 {
                for dy in -1..=1 {
                    if let Some(ids) = self.bins.get(&(key.0 + dx, key.1 + dy)) {
                        for &id in ids {
                            let Some(node) = out.nodes[id].as_ref() else {
                                continue;
                            };
                            let dist = metric_distance_m(p, node.pos);
                            if dist >= radius || witness.is_some_and(|(best, _)| dist >= best) {
                                continue;
                            }
                            if !node_track_aligned(
                                out,
                                id,
                                heading,
                                OTHER_LINE_COS,
                                Some(candidate_lines),
                            ) {
                                continue;
                            }
                            let (px, py) = web_mercator(p);
                            let (nx, ny) = web_mercator(node.pos);
                            let lateral = heading.0 * (ny - py) - heading.1 * (nx - px);
                            witness = Some((dist, lateral));
                        }
                    }
                }
            }
            if let Some((_, lateral)) = witness {
                supported += 1;
                first = first.min(j);
                last = last.max(j);
                // A changing signed offset reveals a shallow-angle crossing.
                // Even same-route tracklets can intersect on loops or lassos.
                min_lateral = min_lateral.min(lateral);
                max_lateral = max_lateral.max(lateral);
            }
        }
        // Densified samples are at most segment_length apart. Measured
        // arclength avoids treating two indices at a sharp kink as 30m apart.
        let observed_length: f64 = points[first..=last]
            .windows(2)
            .map(|w| metric_distance_m(w[0], w[1]))
            .sum();
        supported >= 2 && observed_length >= 25.0 && max_lateral - min_lateral <= 5.0
    }

    fn nearest(
        &self,
        points: &[Point],
        at: usize,
        source: &crate::loom_graph::Edge,
        out: &Graph,
        forbidden: &HashSet<usize>,
        span_a: Option<Point>,
        span_b: Option<Point>,
    ) -> Option<usize> {
        let p = *points.get(at)?;
        let heading = track_tangent(points, at)?;
        let key = cell(p, self.scale);
        // Retain LOOM's protection against snapping beyond the original edge
        // span, but do not let geometric proximity manufacture a rail turnout.
        let span_limit = [span_a, span_b]
            .into_iter()
            .flatten()
            .map(|q| metric_distance_m(p, q) / std::f64::consts::SQRT_2)
            .fold(self.radius, f64::min);
        let mut best = (span_limit, None);
        for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(ids) = self.bins.get(&(key.0 + dx, key.1 + dy)) {
                    for &id in ids {
                        if forbidden.contains(&id) {
                            continue;
                        }
                        let Some(node) = out.nodes[id].as_ref() else {
                            continue;
                        };
                        if node.adj.is_empty() {
                            continue;
                        }
                        let shared_route = node.adj.iter().any(|&eid| {
                            out.edges[eid].as_ref().is_some_and(|e| {
                                e.lines
                                    .iter()
                                    .any(|a| source.lines.iter().any(|b| a.line == b.line))
                            })
                        });
                        let radius = if shared_route {
                            self.radius.min(SAME_LINE_MAX_SNAP_M)
                        } else {
                            self.radius.min(OTHER_LINE_MAX_SNAP_M)
                        };
                        let distance = metric_distance_m(p, node.pos);
                        if distance >= radius
                            || distance > best.0
                            || ((distance - best.0).abs() < 1e-9
                                && best.1.is_none_or(|current| id >= current))
                        {
                            continue;
                        }
                        let cos_limit = if shared_route {
                            SAME_LINE_COS
                        } else {
                            OTHER_LINE_COS
                        };
                        if !node_track_aligned(out, id, heading, cos_limit, None) {
                            continue;
                        }
                        // Apply sustained-track evidence even when the routes
                        // agree: circles, turnbacks and lassos can cross *their
                        // own* alignment without forming a new junction.
                        let candidate_lines: BTreeSet<_> = node
                            .adj
                            .iter()
                            .filter_map(|&eid| out.edges[eid].as_ref())
                            .flat_map(|e| e.lines.iter().map(|o| o.line))
                            .collect();
                        if !self.sustained_alignment(points, at, out, &candidate_lines, radius) {
                            continue;
                        }
                        best = (distance, Some(id));
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
        let meters = metric_distance_m(pair[0], pair[1]);
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
    let live = |id: usize| {
        out.edges
            .get(id)
            .and_then(Option::as_ref)
            .is_some_and(|e| (e.a.min(e.b), e.a.max(e.b)) == key)
    };
    let existing = output_edges
        .get(&key)
        .copied()
        .filter(|&id| live(id))
        .or_else(|| {
            out.nodes[a]
                .as_ref()
                .unwrap()
                .adj
                .iter()
                .copied()
                .find(|&id| live(id))
        });
    let id = if let Some(id) = existing {
        id
    } else {
        let geometry = vec![
            out.nodes[a].as_ref().unwrap().pos,
            out.nodes[b].as_ref().unwrap().pos,
        ];
        out.add_edge(a, b, geometry)
    };
    output_edges.insert(key, id);
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
        // C++ collapseShrdSegs uses simplify(0.5) BEFORE densifying at SEGL.
        let simplified = crate::loom_polyline::simplify(&source_geometry, 0.5);
        let points = sample(&simplified, segment_length);
        let mut forbidden = HashSet::new();
        let mut affected = Vec::new();
        let mut front: Option<usize> = None;
        let mut last: Option<usize> = None;
        let mut from_covered = false;
        let mut to_covered = false;
        for (i, &point) in points.iter().enumerate() {
            // In C++ EVERY sample calls ndCollapseCand. A previously mapped
            // endpoint is not forcibly substituted for this sample: missing
            // endpoint coverage is repaired by explicit connecting edges.
            let span_a = front.and_then(|id| out.nodes[id].as_ref().map(|n| n.pos));
            let span_b = if i + 1 == points.len() {
                None
            } else {
                Some(input.nodes[edge.b].as_ref().unwrap().pos)
            };
            let candidate = index.nearest(&points, i, edge, &out, &forbidden, span_a, span_b);
            let id = candidate.unwrap_or_else(|| {
                let new_id = out.add_node(point);
                index.add(point, new_id);
                new_id
            });
            if candidate.is_some() {
                let old = out.nodes[id].as_ref().unwrap().pos;
                let middle = lerp(old, point, 0.5);
                out.nodes[id].as_mut().unwrap().pos = middle;
                index.relocate(old, middle, id);
            }
            if i == 0 && !mapped_endpoints.contains_key(&edge.a) {
                mapped_endpoints.insert(edge.a, id);
                from_covered = true;
            }
            if i + 1 == points.len() && !mapped_endpoints.contains_key(&edge.b) {
                mapped_endpoints.insert(edge.b, id);
                to_covered = true;
            }
            forbidden.insert(id);
            if last == Some(id) {
                continue;
            }
            from_covered |= mapped_endpoints.get(&edge.a) == Some(&id);
            to_covered |= mapped_endpoints.get(&edge.b) == Some(&id);
            if let Some(previous) = last {
                insert_constructed_segment(&mut out, &mut output_edges, previous, id, edge);
            }
            affected.push(id);
            if front.is_none() {
                front = Some(id);
            }
            last = Some(id);
            if mapped_endpoints.get(&edge.b) == Some(&id) {
                break;
            }
        }
        // C++ imgFromCovered/imgToCovered reconciliation. Without these,
        // a sampled edge can skip its real shared topology endpoints.
        if !from_covered {
            if let (Some(&from), Some(first)) = (mapped_endpoints.get(&edge.a), front) {
                insert_constructed_segment(&mut out, &mut output_edges, from, first, edge);
            }
        }
        if !to_covered {
            if let (Some(last), Some(&to)) = (last, mapped_endpoints.get(&edge.b)) {
                insert_constructed_segment(&mut out, &mut output_edges, last, to, edge);
            }
        }
        // The C++ short-artifact pass is LOCAL to the affected input edge and
        // must never remove an image of an original graph node.
        let protected: HashSet<_> = mapped_endpoints.values().copied().collect();
        for a in affected {
            if protected.contains(&a) {
                continue;
            }
            let Some(node) = out.nodes.get(a).and_then(Option::as_ref) else {
                continue;
            };
            let mut best: Option<(usize, usize, f64)> = None;
            for &eid in &node.adj {
                let Some(e) = out.edges[eid].as_ref() else {
                    continue;
                };
                let b = if e.a == a { e.b } else { e.a };
                let Some(other) = out.nodes[b].as_ref() else {
                    continue;
                };
                if node.adj.len() < 3 && other.adj.len() < 3 {
                    continue;
                }
                let distance = metric_distance_m(node.pos, other.pos);
                if distance <= segment_length && best.is_none_or(|(_, _, d)| distance < d) {
                    best = Some((b, eid, distance));
                }
            }
            if let Some((b, eid, _)) = best {
                let old_a = out.nodes[a].as_ref().unwrap().pos;
                let old_b = out.nodes[b].as_ref().unwrap().pos;
                if combine_nodes(&mut out, a, b, eid) {
                    index.remove(old_a, a);
                    index.relocate(old_b, out.nodes[b].as_ref().unwrap().pos, b);
                }
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

/// C++ MapConstructor::lineEq: matching route occurrences must also have
/// an allowed continuation across the shared node.
pub(crate) fn line_eq_at(graph: &Graph, first: usize, second: usize, at: usize) -> bool {
    let (Some(a), Some(b), Some(node)) = (
        graph.edges.get(first).and_then(Option::as_ref),
        graph.edges.get(second).and_then(Option::as_ref),
        graph.nodes.get(at).and_then(Option::as_ref),
    ) else {
        return false;
    };
    compatible(&a.lines, &b.lines, at)
        && a.lines.iter().all(|occ| {
            !node
                .conn_exc
                .get(&occ.line)
                .and_then(|map| map.get(&first))
                .is_some_and(|targets| targets.contains(&second))
                && !node
                    .conn_exc
                    .get(&occ.line)
                    .and_then(|map| map.get(&second))
                    .is_some_and(|targets| targets.contains(&first))
        })
}

fn contract(out: &mut Graph) {
    contract_with_cutoff(out, 0.0);
}

/// A nonzero radius additionally enables C++ supportEdge splitting if a
/// long parallel edge would otherwise block a valid degree-two contraction.
fn contract_with_cutoff(out: &mut Graph, cutoff: f64) {
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
        if !compatible(&a.lines, &b.lines, mid)
            || !a.lines.iter().all(|occ| {
                !node
                    .conn_exc
                    .get(&occ.line)
                    .and_then(|forbidden| forbidden.get(&ids[0]))
                    .is_some_and(|targets| targets.contains(&ids[1]))
            })
        {
            continue;
        }
        let u = if a.a == mid { a.b } else { a.a };
        let v = if b.a == mid { b.b } else { b.a };
        if u == v || polyline_len(&a.geom) + polyline_len(&b.geom) > MAX_CONTRACTION_METERS {
            continue;
        }
        // C++ collapseShrdSegs: when a direct edge would block a contraction,
        // split that edge in half if it is longer than twice the cutoff.
        // The new support vertex prevents a multiedge while preserving geometry.
        let blocker = out.nodes[u].as_ref().and_then(|n| {
            n.adj.iter().copied().find(|&eid| {
                eid != ids[0]
                    && eid != ids[1]
                    && out.edges[eid]
                        .as_ref()
                        .is_some_and(|e| (e.a == u && e.b == v) || (e.a == v && e.b == u))
            })
        });
        if let Some(blocker) = blocker {
            if cutoff > 0.0
                && out.edges[blocker]
                    .as_ref()
                    .is_some_and(|e| polyline_len(&e.geom) > 2.0 * cutoff)
                && crate::loom_cpp_topo::support_edge(out, blocker).is_some()
            {
                todo.push_back(mid);
            }
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
                    *turns = turns
                        .iter()
                        .map(|&(x, y)| {
                            (
                                if x == ids[0] || x == ids[1] { eid } else { x },
                                if y == ids[0] || y == ids[1] { eid } else { y },
                            )
                        })
                        .filter(|(x, y)| x != y)
                        .collect();
                }
                // C++ edgeRpl/nodeRpl also repair direction-specific connection
                // exceptions when their incident edge handles are replaced.
                for restrictions in node.conn_exc.values_mut() {
                    let old = std::mem::take(restrictions);
                    for (from, targets) in old {
                        let from = if from == ids[0] || from == ids[1] {
                            eid
                        } else {
                            from
                        };
                        for to in targets {
                            let to = if to == ids[0] || to == ids[1] {
                                eid
                            } else {
                                to
                            };
                            if from != to {
                                restrictions.entry(from).or_default().insert(to);
                            }
                        }
                    }
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
pub(crate) fn combine_nodes(
    graph: &mut Graph,
    remove: usize,
    keep: usize,
    connecting: usize,
) -> bool {
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
    // A short artificial connector between distinct line families is not
    // evidence that their adjacent tracks form a junction. This is a guard
    // for the subsequent soft-cleanup / short-edge contraction passes.
    if left.adj.len() > 1 && right.adj.len() > 1 {
        let arm_lines = |node: &crate::loom_graph::Node| -> BTreeSet<usize> {
            node.adj
                .iter()
                .filter(|&&eid| eid != connecting)
                .filter_map(|&eid| graph.edges[eid].as_ref())
                .flat_map(|e| e.lines.iter().map(|o| o.line))
                .collect()
        };
        let a = arm_lines(left);
        let b = arm_lines(right);
        if !a.is_empty() && !b.is_empty() && a.is_disjoint(&b) {
            return false;
        }
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
// Short-segment contraction is performed inside construct_once, as in the
// C++ per-source-edge affectedNodes walk, with protected imgNdsSet anchors.

/// C++ MapConstructor::averageNodePositions, also called BEFORE collapse.
pub fn average_node_positions(graph: &mut Graph) {
    // C++ MapConstructor::averageNodePositions: take the mean of incident
    // polyline start/end positions BEFORE trimming the intersection arms.
    // This mean is in WebMercator meters, not geographic degrees.
    let mut averaged = Vec::with_capacity(graph.nodes.len());
    for node in graph.nodes.iter().map(Option::as_ref) {
        let Some(node) = node else {
            averaged.push(None);
            continue;
        };
        let incident: Vec<Point> = node
            .adj
            .iter()
            .filter_map(|&eid| {
                let edge = graph.edges.get(eid).and_then(Option::as_ref)?;
                if edge.a == node.id {
                    edge.geom.first().copied()
                } else {
                    edge.geom.last().copied()
                }
            })
            .collect();
        averaged.push(if incident.is_empty() {
            None
        } else {
            Some(crate::loom_polyline::centroid(&incident))
        });
    }
    for (node, average) in graph.nodes.iter_mut().zip(averaged) {
        if let (Some(node), Some(pos)) = (node.as_mut(), average) {
            node.pos = pos;
        }
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

fn smooth_edges(graph: &mut Graph, segment_length: f64) {
    for edge in graph.edges.iter_mut().flatten() {
        edge.geom = crate::loom_polyline::smooth_topology_edge(&edge.geom, segment_length);
        // Every edge must begin/end exactly at its incident graph vertices.
        if let Some(a) = edge.geom.first_mut() {
            *a = graph.nodes[edge.a].as_ref().unwrap().pos;
        }
        if let Some(b) = edge.geom.last_mut() {
            *b = graph.nodes[edge.b].as_ref().unwrap().pos;
        }
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
    crate::loom_cpp_topo::soft_cleanup(&mut graph);
    contract_with_cutoff(&mut graph, max_distance);
    // C++ collapseShrdSegs removes newly exposed short edge artifacts,
    // then contracts degree-two vertices again before smoothing.
    crate::loom_cpp_topo::remove_edge_artifacts(&mut graph, max_distance, true);
    contract_with_cutoff(&mut graph, max_distance);
    smooth_edges(&mut graph, segment_length);
    let mut old_len: f64 = graph
        .edges
        .iter()
        .flatten()
        .map(|e| polyline_len(&e.geom))
        .sum();
    for iter in 1..MAX_PASSES {
        let mut next = construct_once(&graph, max_distance, segment_length);
        crate::loom_cpp_topo::soft_cleanup(&mut next);
        contract_with_cutoff(&mut next, max_distance);
        crate::loom_cpp_topo::remove_edge_artifacts(&mut next, max_distance, true);
        contract_with_cutoff(&mut next, max_distance);
        smooth_edges(&mut next, segment_length);
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

    fn metres(x: f64, y: f64) -> Point {
        Point {
            lon: x / 111_319.490_793_273_6,
            lat: y / 111_319.490_793_273_6,
        }
    }

    fn track(graph: &mut Graph, start: Point, end: Point, line: usize, original: usize) {
        let a = graph.add_node(start);
        let b = graph.add_node(end);
        let id = graph.add_edge(a, b, vec![start, end]);
        graph.edges[id].as_mut().unwrap().lines.insert(LineOcc {
            line,
            direction: Some(b),
        });
        graph.edges[id].as_mut().unwrap().originals.insert(original);
    }

    fn mixed_line_nodes(graph: &Graph) -> usize {
        graph
            .nodes
            .iter()
            .flatten()
            .filter(|n| {
                n.adj
                    .iter()
                    .filter_map(|&eid| graph.edges[eid].as_ref())
                    .flat_map(|e| e.lines.iter().map(|o| o.line))
                    .collect::<BTreeSet<_>>()
                    .len()
                    > 1
            })
            .count()
    }

    #[test]
    fn orthogonal_lines_do_not_acquire_artificial_interchange() {
        let mut graph = Graph::default();
        track(&mut graph, metres(-120.0, 0.0), metres(120.0, 0.0), 3, 100);
        track(&mut graph, metres(0.0, -120.0), metres(0.0, 120.0), 9, 200);
        let result = construct_once(&graph, 50.0, 5.0);
        assert_eq!(mixed_line_nodes(&result), 0, "independent X crossing fused");
        result.assert_consistent();
    }

    #[test]
    fn distinct_parallel_routes_do_not_merge_at_twenty_metres() {
        let mut graph = Graph::default();
        track(&mut graph, metres(0.0, 0.0), metres(180.0, 0.0), 3, 100);
        track(&mut graph, metres(0.0, 20.0), metres(180.0, 20.0), 9, 200);
        let result = construct_once(&graph, 50.0, 5.0);
        assert_eq!(mixed_line_nodes(&result), 0);
        result.assert_consistent();
    }

    #[test]
    fn coincident_distinct_routes_can_share_corridor() {
        let mut graph = Graph::default();
        track(&mut graph, metres(0.0, 0.0), metres(180.0, 0.0), 3, 100);
        track(&mut graph, metres(0.0, 0.0), metres(180.0, 0.0), 9, 200);
        let result = construct_once(&graph, 50.0, 5.0);
        assert!(
            mixed_line_nodes(&result) > 0,
            "coincident track was not merged"
        );
        result.assert_consistent();
    }

    #[test]
    fn shallow_crossing_does_not_become_shared_track() {
        let mut graph = Graph::default();
        track(&mut graph, metres(-120.0, 0.0), metres(120.0, 0.0), 3, 100);
        track(
            &mut graph,
            metres(-120.0, -25.5),
            metres(120.0, 25.5),
            9,
            200,
        );
        let result = construct_once(&graph, 50.0, 5.0);
        assert_eq!(mixed_line_nodes(&result), 0, "shallow X crossing fused");
        result.assert_consistent();
    }

    #[test]
    fn shallow_self_crossing_of_one_route_remains_independent() {
        let mut graph = Graph::default();
        track(&mut graph, metres(-120.0, 0.0), metres(120.0, 0.0), 3, 100);
        track(
            &mut graph,
            metres(-120.0, -25.5),
            metres(120.0, 25.5),
            3,
            200,
        );
        let result = construct_once(&graph, 50.0, 5.0);
        assert!(
            !result.edges.iter().flatten().any(|e| e.originals.len() > 1),
            "self-crossing tracklets collapsed into a shared segment"
        );
        result.assert_consistent();
    }

    #[test]
    fn explicit_source_turn_is_not_disconnected() {
        let mut graph = Graph::default();
        let a = graph.add_node(metres(-100.0, 0.0));
        let junction = graph.add_node(metres(0.0, 0.0));
        let b = graph.add_node(metres(85.0, 65.0));
        let first = graph.add_edge(a, junction, vec![metres(-100.0, 0.0), metres(0.0, 0.0)]);
        let second = graph.add_edge(junction, b, vec![metres(0.0, 0.0), metres(85.0, 65.0)]);
        for id in [first, second] {
            graph.edges[id].as_mut().unwrap().lines.insert(LineOcc {
                line: 3,
                direction: None,
            });
            graph.edges[id].as_mut().unwrap().originals.insert(id);
        }
        let result = construct_once(&graph, 50.0, 5.0);
        let closest = |p: Point| {
            result
                .nodes
                .iter()
                .flatten()
                .min_by(|a, b| metric_distance_m(a.pos, p).total_cmp(&metric_distance_m(b.pos, p)))
                .unwrap()
                .id
        };
        let from = closest(metres(-100.0, 0.0));
        let to = closest(metres(85.0, 65.0));
        let mut queue = VecDeque::from([from]);
        let mut seen = HashSet::from([from]);
        while let Some(n) = queue.pop_front() {
            for &eid in &result.nodes[n].as_ref().unwrap().adj {
                let next = result.other(eid, n);
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
        assert!(seen.contains(&to), "a verified input turn was disconnected");
        result.assert_consistent();
    }

    #[test]
    fn unrelated_routes_on_a_short_connector_are_not_welded() {
        let mut graph = Graph::default();
        let left = graph.add_node(metres(0.0, 0.0));
        let right = graph.add_node(metres(1.0, 0.0));
        let connector = graph.add_edge(left, right, vec![metres(0.0, 0.0), metres(1.0, 0.0)]);
        for (center, a, b, line) in [
            (left, metres(-30.0, 0.0), metres(0.0, 30.0), 3),
            (right, metres(30.0, 0.0), metres(1.0, -30.0), 9),
        ] {
            for endpoint in [a, b] {
                let tip = graph.add_node(endpoint);
                let geom = vec![graph.nodes[center].as_ref().unwrap().pos, endpoint];
                let eid = graph.add_edge(center, tip, geom);
                graph.edges[eid].as_mut().unwrap().lines.insert(LineOcc {
                    line,
                    direction: None,
                });
            }
        }
        assert!(!combine_nodes(&mut graph, left, right, connector));
        assert!(graph.nodes[left].is_some() && graph.nodes[right].is_some());
        graph.assert_consistent();
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
        graph.edges[e1].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(middle),
        });
        graph.edges[e2].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(b),
        });
        graph.nodes[middle]
            .as_mut()
            .unwrap()
            .conn_exc
            .entry(0)
            .or_default()
            .entry(e1)
            .or_default()
            .insert(e2);
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
