//! Topological transition validation and provenance-indexed station insertion.
//!
//! Port of the essential semantics in LOOM RestrInferrer and StatInserter:
//! explicit original transitions, line-specific restrictions, candidate coverage,
//! and multiple placements for a stop served by disjoint tracks.
use crate::loom_graph::{
    Graph, LineId, LineOcc, Point, Stop, metric_distance_m, project_on_polyline, subline,
};
use log::{info, warn};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::time::Instant;

pub struct StationOccurrence {
    pub stops: Vec<Stop>,
    pub originals: BTreeSet<usize>,
    pub lines: BTreeSet<LineId>,
}

/// Build an inverted index of *legal* original edge transitions. Source
/// identifiers, rather than transient constructed edge IDs, survive collapse.
fn original_transitions(input: &Graph) -> HashSet<(LineId, usize, usize)> {
    let mut valid = HashSet::new();
    for node in input.nodes.iter().flatten() {
        for (&line, turns) in &node.allowed_turns {
            for &(from, to) in turns {
                let (Some(a), Some(b)) = (
                    input.edges.get(from).and_then(Option::as_ref),
                    input.edges.get(to).and_then(Option::as_ref),
                ) else {
                    continue;
                };
                for &oa in &a.originals {
                    for &ob in &b.originals {
                        valid.insert((line, oa, ob));
                    }
                }
            }
        }
    }
    valid
}

fn can_turn(
    line: LineId,
    a: &BTreeSet<usize>,
    b: &BTreeSet<usize>,
    transitions: &HashSet<(LineId, usize, usize)>,
) -> bool {
    // Both halves of a subdivided original edge must remain traversable.
    if !a.is_disjoint(b) {
        return true;
    }
    // A transition is legal only if a source direction pattern made it.
    a.iter()
        .any(|&x| b.iter().any(|&y| transitions.contains(&(line, x, y))))
}

/// C++ RestrInferrer uses a separate restriction graph and a bounded
/// shortest-path check before forbidding a turn. This compact edge-state graph
/// represents permitted (line, original-edge) transitions from the original
/// direction patterns. Each intermediate edge contributes its length.
struct RestrictionPaths {
    transitions: HashMap<(LineId, usize), Vec<usize>>,
    lengths_cm: HashMap<usize, u64>,
}

impl RestrictionPaths {
    fn new(input: &Graph, turns: &HashSet<(LineId, usize, usize)>) -> Self {
        let mut transitions: HashMap<(LineId, usize), Vec<usize>> = HashMap::new();
        for &(line, from, to) in turns {
            transitions.entry((line, from)).or_default().push(to);
        }
        let mut lengths_cm = HashMap::new();
        for e in input.edges.iter().flatten() {
            let len_cm = (crate::loom_graph::polyline_len(&e.geom).max(0.0) * 100.0) as u64;
            for &id in &e.originals {
                lengths_cm.entry(id).or_insert(len_cm);
            }
        }
        Self {
            transitions,
            lengths_cm,
        }
    }

    fn bounded_reachable(
        &self,
        line: LineId,
        from: &BTreeSet<usize>,
        to: &BTreeSet<usize>,
        max_deviation_m: f64,
    ) -> bool {
        let bound = (max_deviation_m.max(0.0) * 100.0) as u64;
        let mut best: HashMap<usize, u64> = HashMap::new();
        let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
        for &start in from {
            best.insert(start, 0);
            heap.push(Reverse((0, start)));
        }
        while let Some(Reverse((cost, current))) = heap.pop() {
            if cost > bound {
                break;
            }
            if cost > *best.get(&current).unwrap_or(&u64::MAX) {
                continue;
            }
            if to.contains(&current) {
                return true;
            }
            if let Some(next) = self.transitions.get(&(line, current)) {
                for &candidate in next {
                    // We only count the geometry BETWEEN source and target
                    // edge handles, as in C++ RestrInferrer::check.
                    let addition = if to.contains(&candidate) {
                        0
                    } else {
                        *self.lengths_cm.get(&candidate).unwrap_or(&u64::MAX)
                    };
                    let new_cost = cost.saturating_add(addition);
                    if new_cost <= bound && new_cost < *best.get(&candidate).unwrap_or(&u64::MAX) {
                        best.insert(candidate, new_cost);
                        heap.push(Reverse((new_cost, candidate)));
                    }
                }
            }
        }
        false
    }
}

/// An occurrence can enter a vertex only when its destination is that
/// vertex (or its direction is unrestricted), and can leave only toward the
/// other endpoint.  C++ RestrInferrer skips pairs invalid by direction.
fn enterable(edge: &crate::loom_graph::Edge, line: LineId, node: usize) -> bool {
    edge.lines
        .iter()
        .any(|o| o.line == line && (o.direction.is_none() || o.direction == Some(node)))
}
fn leaveable(edge: &crate::loom_graph::Edge, line: LineId, node: usize) -> bool {
    let other = if edge.a == node { edge.b } else { edge.a };
    edge.lines
        .iter()
        .any(|o| o.line == line && (o.direction.is_none() || o.direction == Some(other)))
}

pub fn infer_restrictions(original: &Graph, output: &mut Graph) {
    infer_restrictions_with_deviation(original, output, 500.0);
}

pub fn infer_restrictions_with_deviation(
    original: &Graph,
    output: &mut Graph,
    max_deviation_m: f64,
) {
    let start = Instant::now();
    let transitions = original_transitions(original);
    let paths = RestrictionPaths::new(original, &transitions);
    let mut inferred = 0usize;
    for node_id in 0..output.nodes.len() {
        let Some(node) = output.nodes[node_id].as_ref() else {
            continue;
        };
        let adjacent: Vec<_> = node.adj.iter().copied().collect();
        let mut forbidden = BTreeMap::<LineId, BTreeMap<usize, BTreeSet<usize>>>::new();
        for &from_id in &adjacent {
            let Some(from) = output.edges[from_id].as_ref() else {
                continue;
            };
            for &to_id in &adjacent {
                if from_id == to_id {
                    continue;
                }
                let Some(to) = output.edges[to_id].as_ref() else {
                    continue;
                };
                for occ in &from.lines {
                    if !enterable(from, occ.line, node_id) || !leaveable(to, occ.line, node_id) {
                        continue;
                    }
                    if can_turn(occ.line, &from.originals, &to.originals, &transitions)
                        || paths.bounded_reachable(
                            occ.line,
                            &from.originals,
                            &to.originals,
                            max_deviation_m,
                        )
                    {
                        continue;
                    }
                    forbidden
                        .entry(occ.line)
                        .or_default()
                        .entry(from_id)
                        .or_default()
                        .insert(to_id);
                    inferred += 1;
                }
            }
        }
        // C++ RestrInferrer removes a restriction when it would leave a
        // line occurrence without ANY possible continuation despite having
        // other same-line edges. This also prevents false disconnections where
        // pattern compression omitted an otherwise valid original transition.
        for (&line, map) in &mut forbidden {
            for &from in &adjacent {
                let Some(in_edge) = output.edges[from].as_ref() else {
                    continue;
                };
                if !enterable(in_edge, line, node_id) {
                    continue;
                }
                let choices = adjacent
                    .iter()
                    .copied()
                    .filter(|&to| {
                        to != from
                            && output.edges[to]
                                .as_ref()
                                .is_some_and(|e| leaveable(e, line, node_id))
                    })
                    .collect::<Vec<_>>();
                if !choices.is_empty()
                    && choices
                        .iter()
                        .all(|to| map.get(&from).is_some_and(|blocked| blocked.contains(to)))
                {
                    map.remove(&from);
                }
            }
        }
        forbidden.retain(|_, m| !m.is_empty());
        if let Some(n) = output.nodes[node_id].as_mut() {
            n.conn_exc = forbidden;
        }
    }
    info!(
        "[topo/restr] inferred up to {} direction-aware prohibitions; {} original turns in {:.2?}",
        inferred,
        transitions.len(),
        start.elapsed()
    );
}

/// Station candidates are indexed by edge provenance rather than considering
/// every geometry in the component. The index is incrementally updated on
/// edge splitting, so each insertion sees valid edge IDs.
fn provenance_index(graph: &Graph) -> HashMap<usize, Vec<usize>> {
    let mut index = HashMap::<usize, Vec<usize>>::new();
    for edge in graph.edges.iter().flatten() {
        for &source in &edge.originals {
            index.entry(source).or_default().push(edge.id);
        }
    }
    index
}

fn split_at(graph: &mut Graph, edge_id: usize, fraction: f64) -> Option<(usize, usize, usize)> {
    let edge = graph.edges.get(edge_id)?.as_ref()?.clone();
    // Inserting exactly at an endpoint creates zero-length edges and can
    // invalidate line direction; attach to the existing node instead.
    if fraction <= 1e-6 {
        return Some((edge.a, edge_id, edge_id));
    }
    if fraction >= 1.0 - 1e-6 {
        return Some((edge.b, edge_id, edge_id));
    }
    let left = subline(&edge.geom, 0.0, fraction);
    let right = subline(&edge.geom, fraction, 1.0);
    if left.len() < 2 || right.len() < 2 {
        return None;
    }
    let split_point = *left.last()?;
    graph.remove_edge(edge_id);
    let mid = graph.add_node(split_point);
    let mut ids = [0usize; 2];
    for (slot, (a, b, geom)) in [(edge.a, mid, left), (mid, edge.b, right)]
        .into_iter()
        .enumerate()
    {
        let id = graph.add_edge(a, b, geom);
        ids[slot] = id;
        let e = graph.edges[id].as_mut().unwrap();
        e.originals = edge.originals.clone();
        e.lines = edge
            .lines
            .iter()
            .map(|occ| {
                let direction = match occ.direction {
                    Some(d) if d == edge.a => Some(a),
                    Some(d) if d == edge.b => Some(b),
                    other => other,
                };
                LineOcc {
                    line: occ.line,
                    direction,
                }
            })
            .collect();
    }
    // StatInserter: retain directed through-transitions on split edges.
    for occ in &edge.lines {
        let turns = graph.nodes[mid]
            .as_mut()
            .unwrap()
            .allowed_turns
            .entry(occ.line)
            .or_default();
        match occ.direction {
            Some(dest) if dest == edge.a => {
                turns.insert((ids[1], ids[0]));
            }
            Some(dest) if dest == edge.b => {
                turns.insert((ids[0], ids[1]));
            }
            None => {
                turns.insert((ids[0], ids[1]));
                turns.insert((ids[1], ids[0]));
            }
            _ => {}
        }
    }
    // The outside endpoints' restrictions must point to the replacement edges.
    for (endpoint, replacement) in [(edge.a, ids[0]), (edge.b, ids[1])] {
        if let Some(n) = graph.nodes[endpoint].as_mut() {
            for turns in n.allowed_turns.values_mut() {
                *turns = turns
                    .iter()
                    .map(|&(a, b)| {
                        (
                            if a == edge_id { replacement } else { a },
                            if b == edge_id { replacement } else { b },
                        )
                    })
                    .collect();
            }
            for from_map in n.conn_exc.values_mut() {
                let mut remapped = BTreeMap::new();
                for (&from, tos) in from_map.iter() {
                    let key = if from == edge_id { replacement } else { from };
                    let replacements = tos
                        .iter()
                        .map(|&to| if to == edge_id { replacement } else { to });
                    remapped
                        .entry(key)
                        .or_insert_with(BTreeSet::new)
                        .extend(replacements);
                }
                *from_map = remapped;
            }
        }
    }
    Some((mid, ids[0], ids[1]))
}

/// C++ StatInserter::candScore: distance + provenance shortfall + line
/// shortfall, with an additional 200m penalty for inserting an edge split
/// within maxAggrDistance of an existing node. Both endpoint nodes are
/// independent candidates, not just the closest projection on an edge.
fn station_score(
    distance: f64,
    served_orig: usize,
    needed_orig: usize,
    served_lines: usize,
    needed_lines: usize,
) -> f64 {
    distance
        + 100.0 * needed_orig.saturating_sub(served_orig) as f64 / needed_orig.max(1) as f64
        + 500.0 * needed_lines.saturating_sub(served_lines) as f64 / needed_lines.max(1) as f64
}

fn node_coverage(
    graph: &Graph,
    node: usize,
    originals: &BTreeSet<usize>,
    lines: &BTreeSet<LineId>,
) -> (BTreeSet<usize>, BTreeSet<LineId>) {
    let mut found_orig = BTreeSet::new();
    let mut found_lines = BTreeSet::new();
    if let Some(n) = graph.nodes[node].as_ref() {
        for &eid in &n.adj {
            if let Some(e) = graph.edges[eid].as_ref() {
                found_orig.extend(e.originals.intersection(originals).copied());
                found_lines.extend(e.lines.iter().map(|o| o.line).filter(|l| lines.contains(l)));
            }
        }
    }
    (found_orig, found_lines)
}

pub fn insert_stations(occurrences: &[StationOccurrence], graph: &mut Graph, radius: f64) {
    let start = Instant::now();
    let mut index = provenance_index(graph);
    let mut inserted = 0usize;
    let mut missing = 0usize;
    for occurrence in occurrences {
        if occurrence.stops.is_empty() {
            continue;
        }
        let stop_position = crate::loom_polyline::centroid(
            &occurrence
                .stops
                .iter()
                .map(|stop| stop.pos)
                .collect::<Vec<_>>(),
        );
        let mut remaining_orig = occurrence.originals.clone();
        let mut remaining_lines = occurrence.lines.clone();
        // Original StatInserter uses MAX_INSERTS=3: a station with separate
        // tracks can be represented by multiple topological placements.
        for _ in 0..3 {
            let mut candidates = HashSet::new();
            for orig in &remaining_orig {
                if let Some(ids) = index.get(orig) {
                    candidates.extend(ids.iter().copied());
                }
            }
            // (score, edge id, split fraction, optional existing node,
            //  covered originals, covered lines)
            let mut best: Option<(
                f64,
                usize,
                f64,
                Option<usize>,
                BTreeSet<usize>,
                BTreeSet<LineId>,
            )> = None;
            let mut endpoint_seen = HashSet::new();
            for id in candidates {
                let Some(e) = graph.edges.get(id).and_then(Option::as_ref) else {
                    continue;
                };
                let (a, b) = (e.a, e.b);
                if e.geom.len() < 2 {
                    continue;
                }
                let (_, pos, dist) = project_on_polyline(stop_position, &e.geom);
                if dist <= 4.0 * radius {
                    let covered_orig: BTreeSet<_> =
                        e.originals.intersection(&remaining_orig).copied().collect();
                    let covered_lines: BTreeSet<_> = e
                        .lines
                        .iter()
                        .map(|o| o.line)
                        .filter(|line| remaining_lines.contains(line))
                        .collect();
                    if !covered_orig.is_empty() || !covered_lines.is_empty() {
                        let len = crate::loom_graph::polyline_len(&e.geom);
                        let penalty = if pos * len < radius || (1.0 - pos) * len < radius {
                            200.0
                        } else {
                            0.0
                        };
                        let score = station_score(
                            dist,
                            covered_orig.len(),
                            remaining_orig.len(),
                            covered_lines.len(),
                            remaining_lines.len(),
                        ) + penalty;
                        if best.as_ref().is_none_or(|(old, ..)| score < *old) {
                            best = Some((score, id, pos, None, covered_orig, covered_lines));
                        }
                    }
                }
                for node in [a, b] {
                    if !endpoint_seen.insert(node) {
                        continue;
                    }
                    let Some(n) = graph.nodes[node].as_ref() else {
                        continue;
                    };
                    let distance = metric_distance_m(stop_position, n.pos);
                    if distance > 4.0 * radius {
                        continue;
                    }
                    let (covered_orig, covered_lines) =
                        node_coverage(graph, node, &remaining_orig, &remaining_lines);
                    if covered_orig.is_empty() && covered_lines.is_empty() {
                        continue;
                    }
                    let score = station_score(
                        distance,
                        covered_orig.len(),
                        remaining_orig.len(),
                        covered_lines.len(),
                        remaining_lines.len(),
                    );
                    if best.as_ref().is_none_or(|(old, ..)| score < *old) {
                        best = Some((score, id, 0.0, Some(node), covered_orig, covered_lines));
                    }
                }
            }
            let Some((_, id, pos, endpoint, covered_orig, covered_lines)) = best else {
                break;
            };
            let node = if let Some(n) = endpoint {
                n
            } else {
                let old_orig = graph.edges[id].as_ref().unwrap().originals.clone();
                let Some((n, left, right)) = split_at(graph, id, pos) else {
                    break;
                };
                if left != right {
                    for orig in old_orig {
                        let entry = index.entry(orig).or_default();
                        entry.retain(|&e| e != id);
                        entry.extend([left, right]);
                    }
                }
                n
            };
            // Do not mark previously served lines unserved when another station
            // is inserted at the same node; C++ StatInserter keeps that state.
            let previously_served: BTreeSet<_> = {
                let n = graph.nodes[node].as_ref().unwrap();
                if n.stops.is_empty() {
                    BTreeSet::new()
                } else {
                    n.adj
                        .iter()
                        .filter_map(|id| graph.edges[*id].as_ref())
                        .flat_map(|e| e.lines.iter().map(|o| o.line))
                        .filter(|l| !n.not_served.contains(l))
                        .collect()
                }
            };
            let all_adj_lines: BTreeSet<_> = {
                let n = graph.nodes[node].as_ref().unwrap();
                n.adj
                    .iter()
                    .filter_map(|id| graph.edges[*id].as_ref())
                    .flat_map(|e| e.lines.iter().map(|o| o.line))
                    .collect()
            };
            let target = graph.nodes[node].as_mut().unwrap();
            for station in &occurrence.stops {
                if !target
                    .stops
                    .iter()
                    .any(|s| s.chateau == station.chateau && s.stop_id == station.stop_id)
                {
                    target.stops.push(station.clone());
                }
            }
            for line in all_adj_lines {
                if remaining_lines.contains(&line)
                    || covered_lines.contains(&line)
                    || previously_served.contains(&line)
                {
                    target.not_served.remove(&line);
                } else {
                    target.not_served.insert(line);
                }
            }
            remaining_orig.retain(|o| !covered_orig.contains(o));
            remaining_lines.retain(|l| !covered_lines.contains(l));
            inserted += 1;
            if remaining_orig.is_empty() && remaining_lines.is_empty() {
                break;
            }
        }
        if !remaining_orig.is_empty() || !remaining_lines.is_empty() {
            missing += 1;
        }
    }
    if missing > 0 {
        warn!(
            "[topo/stations] {} station clusters lack full source-edge coverage",
            missing
        );
    }
    info!(
        "[topo/stations] {} placements, {} incomplete in {:.2?}",
        inserted,
        missing,
        start.elapsed()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provenance_transitions_distinguish_unconnected_branches() {
        let mut g = Graph::default();
        let n: Vec<_> = (0..4)
            .map(|i| {
                g.add_node(Point {
                    lon: i as f64 * 0.001,
                    lat: 0.0,
                })
            })
            .collect();
        let e0 = g.add_edge(
            n[0],
            n[1],
            vec![
                g.nodes[n[0]].as_ref().unwrap().pos,
                g.nodes[n[1]].as_ref().unwrap().pos,
            ],
        );
        let e1 = g.add_edge(
            n[1],
            n[2],
            vec![
                g.nodes[n[1]].as_ref().unwrap().pos,
                g.nodes[n[2]].as_ref().unwrap().pos,
            ],
        );
        let e2 = g.add_edge(
            n[1],
            n[3],
            vec![
                g.nodes[n[1]].as_ref().unwrap().pos,
                g.nodes[n[3]].as_ref().unwrap().pos,
            ],
        );
        for (i, e) in [e0, e1, e2].iter().enumerate() {
            g.edges[*e].as_mut().unwrap().originals.insert(i);
            g.edges[*e].as_mut().unwrap().lines.insert(LineOcc {
                line: 0,
                direction: None,
            });
        }
        g.nodes[n[1]]
            .as_mut()
            .unwrap()
            .allowed_turns
            .entry(0)
            .or_default()
            .insert((e0, e1));
        let mut out = Graph::default();
        out.lines = g.lines.clone();
        for node in g.nodes.iter().flatten() {
            out.add_node(node.pos);
        }
        for e in g.edges.iter().flatten() {
            let id = out.add_edge(e.a, e.b, e.geom.clone());
            out.edges[id].as_mut().unwrap().originals = e.originals.clone();
            out.edges[id].as_mut().unwrap().lines = e.lines.clone();
        }
        infer_restrictions(&g, &mut out);
        let forb = &out.nodes[n[1]].as_ref().unwrap().conn_exc[&0];
        assert!(!forb.get(&e0).is_some_and(|s| s.contains(&e1)));
        assert!(forb.get(&e0).is_some_and(|s| s.contains(&e2)));
    }
}

#[cfg(test)]
mod splitting_tests {
    use super::*;

    #[test]
    fn station_split_preserves_forward_direction_and_originals() {
        let mut graph = Graph::default();
        let a = graph.add_node(Point { lon: 0.0, lat: 0.0 });
        let b = graph.add_node(Point {
            lon: 0.002,
            lat: 0.0,
        });
        let edge = graph.add_edge(
            a,
            b,
            vec![
                Point { lon: 0.0, lat: 0.0 },
                Point {
                    lon: 0.002,
                    lat: 0.0,
                },
            ],
        );
        graph.edges[edge].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(b),
        });
        graph.edges[edge].as_mut().unwrap().originals.insert(7);
        let (middle, left, right) = split_at(&mut graph, edge, 0.5).unwrap();
        assert_eq!(
            graph.edges[left]
                .as_ref()
                .unwrap()
                .lines
                .iter()
                .next()
                .unwrap()
                .direction,
            Some(middle)
        );
        assert_eq!(
            graph.edges[right]
                .as_ref()
                .unwrap()
                .lines
                .iter()
                .next()
                .unwrap()
                .direction,
            Some(b)
        );
        assert!(graph.edges[left].as_ref().unwrap().originals.contains(&7));
        assert!(graph.edges[right].as_ref().unwrap().originals.contains(&7));
        assert!(graph.edges[edge].is_none());
    }
}
