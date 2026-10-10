//! Geometry-aware port of LOOM's topo/restr/RestrInferrer.
//! Source: https://github.com/ad-freiburg/loom/tree/master/src/topo/restr
//!
//! Build directed, line-labelled subdivisions of the frozen pre-collapse graph.
//! Per-output-edge orthogonal handles are projected onto those subdivisions;
//! bounded edge-state Dijkstra checks whether the two handles are connected.
use crate::loom_graph::{Graph, LineId, Point, polyline_len, project_on_polyline, subline, web_mercator};
use log::info;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};

#[derive(Clone)]
struct Arc {
    from: usize,
    to: usize,
    original: usize,
    length: f64,
    lines: BTreeSet<LineId>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Cost(f64);
impl Eq for Cost {}
impl Ord for Cost {
    fn cmp(&self, other: &Self) -> Ordering { self.0.total_cmp(&other.0) }
}
impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

fn cross(a: (f64, f64), b: (f64, f64)) -> f64 { a.0 * b.1 - a.1 * b.0 }

/// Return source-polyline fractions intersected by a perpendicular finite line.
fn intersections(geom: &[Point], center: (f64, f64), tangent: (f64, f64), half: f64) -> Vec<f64> {
    let norm = tangent.0.hypot(tangent.1);
    if norm < 1e-9 || geom.len() < 2 { return Vec::new(); }
    let half = half.max(1.0);
    let normal = (-tangent.1 / norm * half, tangent.0 / norm * half);
    let start = (center.0 - normal.0, center.1 - normal.1);
    let direction = (normal.0 * 2.0, normal.1 * 2.0);
    let total = polyline_len(geom).max(1e-9);
    let mut cumulative = 0.0;
    let mut hits = Vec::new();
    for pair in geom.windows(2) {
        let (a, b) = (web_mercator(pair[0]), web_mercator(pair[1]));
        let delta = (b.0 - a.0, b.1 - a.1);
        let denom = cross(direction, delta);
        if denom.abs() > 1e-10 {
            let from = (a.0 - start.0, a.1 - start.1);
            let u = cross(from, delta) / denom;
            let t = cross(from, direction) / denom;
            if (-1e-8..=1.0 + 1e-8).contains(&u) && (-1e-8..=1.0 + 1e-8).contains(&t) {
                hits.push(((cumulative + t.clamp(0.0, 1.0) * delta.0.hypot(delta.1)) / total).clamp(0.0, 1.0));
            }
        }
        cumulative += delta.0.hypot(delta.1);
    }
    hits.sort_by(f64::total_cmp);
    hits.dedup_by(|a, b| (*a - *b).abs() < 1e-7);
    hits
}

/// C++ getOrthoLineAt / getOrthoLineAtDist in projected metres.
fn ortholine(geom: &[Point], fraction: f64) -> Option<((f64, f64), (f64, f64))> {
    if geom.len() < 2 { return None; }
    let mut at = subline(geom, 0.0, fraction.clamp(0.0, 1.0));
    let center = web_mercator(*at.last()?);
    let eps = (1.0 / polyline_len(geom).max(1.0)).min(0.01);
    let prev = web_mercator(*subline(geom, 0.0, (fraction - eps).max(0.0)).last()?);
    at = subline(geom, 0.0, (fraction + eps).min(1.0));
    let next = web_mercator(*at.last()?);
    Some((center, (next.0 - prev.0, next.1 - prev.1)))
}

fn handles_for(aggregated: &[Point], source: &[Point], max_aggregate: f64, check_distance: f64) -> (Vec<f64>, Vec<f64>) {
    let len = polyline_len(aggregated);
    if len < 1e-8 { return (vec![], vec![]); }
    let check = (len * 0.5).min(2.0 * max_aggregate);
    let a = (1.0 / 3.0, check / len);
    let b = (2.0 / 3.0, 1.0 - check / len);
    let lookup = |(main, guard): (f64, f64)| -> Vec<f64> {
        let Some((guard_center, guard_tangent)) = ortholine(aggregated, guard) else { return vec![]; };
        if intersections(source, guard_center, guard_tangent, check_distance * 2.0).is_empty() {
            return vec![];
        }
        if let Some((center, tangent)) = ortholine(aggregated, main) {
            let at = intersections(source, center, tangent, check_distance * 2.0);
            if !at.is_empty() { return at; }
        }
        intersections(source, guard_center, guard_tangent, check_distance * 2.0)
    };
    (lookup(a), lookup(b))
}

struct Restrictions {
    arcs: Vec<Arc>,
    incoming: Vec<Vec<usize>>,
    outgoing: Vec<Vec<usize>>,
    node_origin: Vec<Option<usize>>,
    forbidden: HashMap<(usize, LineId, usize), BTreeSet<usize>>,
    /// Actual ordered source-trip transitions at original graph nodes.
    /// Unlike inferred line membership, these can authorize a true reversal.
    allowed: HashMap<(usize, LineId, usize), BTreeSet<usize>>,
}

impl Restrictions {
    fn from_graph(original: &Graph, handles: &HashMap<usize, Vec<f64>>) -> (Self, HashMap<(usize, u64), usize>) {
        let mut this = Self {
            arcs: vec![], incoming: vec![vec![]; original.nodes.len()],
            outgoing: vec![vec![]; original.nodes.len()],
            node_origin: (0..original.nodes.len()).map(Some).collect(),
            forbidden: HashMap::new(),
            allowed: HashMap::new(),
        };
        let mut handle_nodes = HashMap::new();
        for edge in original.edges.iter().flatten() {
            let mut cuts = vec![(0.0, edge.a), (1.0, edge.b)];
            if let Some(fractions) = handles.get(&edge.id) {
                for &fraction in fractions {
                    if fraction <= 1e-7 || fraction >= 1.0 - 1e-7 { continue; }
                    if cuts.iter().any(|(f, _)| (f - fraction).abs() < 1e-7) { continue; }
                    let id = this.incoming.len();
                    this.incoming.push(vec![]);
                    this.outgoing.push(vec![]);
                    this.node_origin.push(None);
                    cuts.push((fraction, id));
                }
            }
            cuts.sort_by(|a, b| a.0.total_cmp(&b.0));
            for &(fraction, node) in &cuts {
                handle_nodes.insert((edge.id, fraction.to_bits()), node);
            }
            for pair in cuts.windows(2) {
                let (a, b) = (pair[0], pair[1]);
                let length = polyline_len(&subline(&edge.geom, a.0, b.0));
                let forward: BTreeSet<LineId> = edge.lines.iter().filter(|o| o.direction.is_none() || o.direction == Some(edge.b)).map(|o| o.line).collect();
                let backward: BTreeSet<LineId> = edge.lines.iter().filter(|o| o.direction.is_none() || o.direction == Some(edge.a)).map(|o| o.line).collect();
                for (from, to, lines) in [(a.1, b.1, forward), (b.1, a.1, backward)] {
                    if lines.is_empty() { continue; }
                    let id = this.arcs.len();
                    this.arcs.push(Arc { from, to, original: edge.id, length, lines });
                    this.outgoing[from].push(id);
                    this.incoming[to].push(id);
                }
            }
        }
        for node in original.nodes.iter().flatten() {
            for (&line, turns) in &node.allowed_turns {
                for &(from, to) in turns {
                    this.allowed.entry((node.id, line, from)).or_default().insert(to);
                }
            }
            for (&line, entries) in &node.conn_exc {
                for (&a, bs) in entries {
                    this.forbidden.entry((node.id, line, a)).or_default().extend(bs.iter().copied());
                }
            }
        }
        (this, handle_nodes)
    }

    fn reachable(&self, line: LineId, start_nodes: &HashSet<usize>, end_nodes: &HashSet<usize>, bound: f64) -> bool {
        if start_nodes.is_empty() || end_nodes.is_empty() { return false; }
        let mut best = vec![f64::INFINITY; self.arcs.len()];
        let mut heap = BinaryHeap::<Reverse<(Cost, usize)>>::new();
        // LOOM uses incoming arcs at its first handle; start-edge costs are zero.
        for &node in start_nodes {
            for &arc in &self.incoming[node] {
                if self.arcs[arc].lines.contains(&line) {
                    best[arc] = 0.0;
                    heap.push(Reverse((Cost(0.0), arc)));
                }
            }
        }
        while let Some(Reverse((Cost(cost), id))) = heap.pop() {
            if cost > bound + 0.1 { break; }
            if cost > best[id] { continue; }
            let incoming = &self.arcs[id];
            if end_nodes.contains(&incoming.to) { return true; }
            for &next_id in &self.outgoing[incoming.to] {
                let next = &self.arcs[next_id];
                if !next.lines.contains(&line) { continue; }
                // Explicit original traversals are authoritative: a reverse
                // arc is legal only when a source trip actually reversed here.
                let observed = self.node_origin[incoming.to].is_some_and(|node| {
                    self.allowed.get(&(node, line, incoming.original))
                        .is_some_and(|targets| targets.contains(&next.original))
                });
                if next.to == incoming.from && !observed { continue; }
                if let Some(node) = self.node_origin[incoming.to] {
                    if !observed && self.forbidden.get(&(node, line, incoming.original))
                        .is_some_and(|targets| targets.contains(&next.original)) { continue; }
                }
                let candidate = cost + next.length;
                if candidate < best[next_id] && candidate <= bound + 0.1 {
                    best[next_id] = candidate;
                    heap.push(Reverse((Cost(candidate), next_id)));
                }
            }
        }
        false
    }
}

/// Rebuild C++ RestrInferrer's geometric handle graph from the graph frozen
/// before shared-segment collapsing; infer restrictions on the output graph.
pub fn infer(original: &Graph, output: &mut Graph, max_deviation: f64, max_aggregate: f64, max_check_dist: f64) {
    let mut originals_by_source = HashMap::<usize, Vec<usize>>::new();
    for edge in original.edges.iter().flatten() {
        for source in &edge.originals { originals_by_source.entry(*source).or_default().push(edge.id); }
    }
    let mut cuts = HashMap::<usize, Vec<f64>>::new();
    let mut occurrences = Vec::<(usize, bool, usize, f64)>::new();
    for edge in output.edges.iter().flatten() {
        let mut original_ids = HashSet::new();
        for source in &edge.originals {
            if let Some(ids) = originals_by_source.get(source) { original_ids.extend(ids.iter().copied()); }
        }
        for original_id in original_ids {
            let Some(oe) = original.edges.get(original_id).and_then(Option::as_ref) else { continue; };
            // C++ addHndls temporarily attaches the two original graph
            // vertices to the original polyline for geometric intersection,
            // then projects each intersection back onto the edge polyline.
            let mut extended = Vec::with_capacity(oe.geom.len() + 2);
            extended.push(original.nodes[oe.a].as_ref().unwrap().pos);
            extended.extend_from_slice(&oe.geom);
            extended.push(original.nodes[oe.b].as_ref().unwrap().pos);
            let (start, end) = handles_for(&edge.geom, &extended, max_aggregate, max_check_dist);
            for (at_start, fractions) in [(true, start), (false, end)] {
                for extended_fraction in fractions {
                    let hit = *subline(&extended, 0.0, extended_fraction).last().unwrap();
                    let (_, fraction, _) = project_on_polyline(hit, &oe.geom);
                    cuts.entry(original_id).or_default().push(fraction);
                    occurrences.push((edge.id, at_start, original_id, fraction));
                }
            }
        }
    }
    for values in cuts.values_mut() {
        values.sort_by(f64::total_cmp);
        values.dedup_by(|a, b| (*a - *b).abs() < 1e-7);
    }
    let (restriction_graph, handle_nodes) = Restrictions::from_graph(original, &cuts);
    let mut endpoint_handles = HashMap::<(usize, bool), HashSet<usize>>::new();
    for (output_id, at_start, original_id, fraction) in occurrences {
        // Normalize a handle fraction to the deduplicated cut location.
        let Some(&cut) = cuts.get(&original_id).and_then(|a| a.iter().find(|v| (**v - fraction).abs() < 1e-7)) else { continue; };
        let node = if cut <= 1e-7 { original.edges[original_id].as_ref().unwrap().a }
            else if cut >= 1.0 - 1e-7 { original.edges[original_id].as_ref().unwrap().b }
            else if let Some(&n) = handle_nodes.get(&(original_id, cut.to_bits())) { n }
            else { continue };
        endpoint_handles.entry((output_id, at_start)).or_default().insert(node);
    }
    let mut inferred = 0;
    for node_id in 0..output.nodes.len() {
        let Some(node) = output.nodes[node_id].as_ref() else { continue; };
        let adjacent: Vec<_> = node.adj.iter().copied().collect();
        let mut forbids = BTreeMap::<LineId, BTreeMap<usize, BTreeSet<usize>>>::new();
        for &ea in &adjacent {
            let Some(a) = output.edges[ea].as_ref() else { continue; };
            for &eb in &adjacent {
                if ea == eb { continue; }
                let Some(b) = output.edges[eb].as_ref() else { continue; };
                let a_lines: BTreeSet<_> = a.lines.iter().map(|o| o.line).collect();
                for line in a_lines {
                    let Some(ro1) = a.lines.iter().find(|o| o.line == line) else { continue; };
                    let Some(ro2) = b.lines.iter().find(|o| o.line == line) else { continue; };
                    // C++ RestrInferrer::infer explicitly excludes these
                    // directed edge pairs before computing any paths.
                    if let (Some(dest1), Some(dest2)) = (ro1.direction, ro2.direction) {
                        if dest1 == dest2 { continue; }
                        let source1 = if dest1 == a.a { a.b } else { a.a };
                        let source2 = if dest2 == b.a { b.b } else { b.a };
                        if source1 == source2 { continue; }
                    }
                    let ha = endpoint_handles.get(&(ea, a.a == node_id)).cloned().unwrap_or_default();
                    let hb = endpoint_handles.get(&(eb, b.a == node_id)).cloned().unwrap_or_default();
                    let bound = max_deviation + (polyline_len(&a.geom) + polyline_len(&b.geom)) * 0.33;
                    if restriction_graph.reachable(line, &ha, &hb, bound)
                        || restriction_graph.reachable(line, &hb, &ha, bound) { continue; }
                    forbids.entry(line).or_default().entry(ea).or_default().insert(eb);
                    inferred += 1;
                }
            }
        }
        // C++ RestrInferrer::infer: remove a row when every other same-line
        // edge is forbidden; it would otherwise create an artificial terminus.
        let mut unblock = Vec::new();
        for (&line, map) in &forbids {
            for &a in &adjacent {
                let total = adjacent.iter().filter(|&&b| b != a && output.edges[b].as_ref().is_some_and(|e| e.lines.iter().any(|o| o.line == line))).count();
                if total > 0 && map.get(&a).is_some_and(|blocked| blocked.len() >= total) {
                    unblock.push((line, a));
                }
            }
        }
        for (line, a) in unblock {
            if let Some(map) = forbids.get_mut(&line) {
                map.remove(&a);
                // C++ delConnExc is symmetric. Do not leave stale inverse
                // restrictions when dropping a dead-end exception row.
                for targets in map.values_mut() { targets.remove(&a); }
                map.retain(|_, targets| !targets.is_empty());
            }
        }
        forbids.retain(|_, map| !map.is_empty());
        if let Some(node) = output.nodes[node_id].as_mut() { node.conn_exc = forbids; }
    }
    info!("[topo/restr] geometric handles={} directed arcs={} inferred prohibitions={}",
        endpoint_handles.values().map(HashSet::len).sum::<usize>(), restriction_graph.arcs.len(), inferred);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn orthogonal_handle_intersects_original_track_at_half_length() {
        let a = Point { lon: 0.0, lat: 0.0 };
        let b = Point { lon: 0.01, lat: 0.0 };
        let (center, tangent) = ortholine(&[a, b], 0.5).unwrap();
        let crossed = intersections(&[a, b], center, tangent, 100.0);
        assert_eq!(crossed.len(), 1);
        assert!((crossed[0] - 0.5).abs() < 1e-5);
    }

    #[test]
    fn directed_handle_paths_respect_original_connection_exceptions() {
        use crate::loom_graph::LineOcc;
        let mut g = Graph::default();
        let a = g.add_node(Point { lon: 0.0, lat: 0.0 });
        let b = g.add_node(Point { lon: 0.001, lat: 0.0 });
        let c = g.add_node(Point { lon: 0.002, lat: 0.0 });
        let e0 = g.add_edge(a, b, vec![g.nodes[a].as_ref().unwrap().pos, g.nodes[b].as_ref().unwrap().pos]);
        let e1 = g.add_edge(b, c, vec![g.nodes[b].as_ref().unwrap().pos, g.nodes[c].as_ref().unwrap().pos]);
        for id in [e0, e1] {
            g.edges[id].as_mut().unwrap().lines.insert(LineOcc { line: 0, direction: None });
        }
        let handles = HashMap::from([(e0, vec![0.5]), (e1, vec![0.5])]);
        let (open, nodes) = Restrictions::from_graph(&g, &handles);
        let source = HashSet::from([nodes[&(e0, 0.5_f64.to_bits())]]);
        let target = HashSet::from([nodes[&(e1, 0.5_f64.to_bits())]]);
        assert!(open.reachable(0, &source, &target, 200.0));
        g.nodes[b].as_mut().unwrap().conn_exc.entry(0).or_default()
            .entry(e0).or_default().insert(e1);
        let (blocked, _) = Restrictions::from_graph(&g, &handles);
        assert!(!blocked.reachable(0, &source, &target, 200.0));
    }
}
