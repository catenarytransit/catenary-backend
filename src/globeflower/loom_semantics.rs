//! Topological transition validation and provenance-indexed station insertion.
//!
//! Port of the essential semantics in LOOM RestrInferrer and StatInserter:
//! explicit original transitions, line-specific restrictions, candidate coverage,
//! and multiple placements for a stop served by disjoint tracks.
use crate::loom_graph::{
    Graph, LineId, LineOcc, Point, Stop, haversine_m, project_on_polyline, subline,
};
use log::{info, warn};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    a.iter().any(|&x| b.iter().any(|&y| transitions.contains(&(line, x, y))))
}

pub fn infer_restrictions(original: &Graph, output: &mut Graph) {
    let start = Instant::now();
    let transitions = original_transitions(original);
    let mut inferred = 0usize;
    // Construct restrictions without holding mutable borrows across edge reads.
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
                    if !to.lines.iter().any(|o| o.line == occ.line) {
                        continue;
                    }
                    if can_turn(occ.line, &from.originals, &to.originals, &transitions) {
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
        if let Some(n) = output.nodes[node_id].as_mut() {
            n.conn_exc = forbidden;
        }
    }
    info!(
        "[topo/restr] {} prohibited edge transitions; {} source transitions in {:.2?}",
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
    // A split is transparent to through travel on the affected line.
    for occ in &edge.lines {
        graph.nodes[mid]
            .as_mut()
            .unwrap()
            .allowed_turns
            .entry(occ.line)
            .or_default()
            .insert((ids[0], ids[1]));
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

pub fn insert_stations(occurrences: &[StationOccurrence], graph: &mut Graph, radius: f64) {
    let start = Instant::now();
    let mut index = provenance_index(graph);
    let mut inserted = 0usize;
    let mut missing = 0usize;
    for occurrence in occurrences {
        let Some(stop) = occurrence.stops.first() else {
            continue;
        };
        let mut remaining_orig = occurrence.originals.clone();
        let mut remaining_lines = occurrence.lines.clone();
        for _ in 0..3 {
            let mut candidates = HashSet::new();
            for orig in &remaining_orig {
                if let Some(ids) = index.get(orig) {
                    candidates.extend(ids.iter().copied());
                }
            }
            // Avoid an unrestricted global scan: if there is no provenance,
            // an insertion must be deferred rather than attached to a random line.
            let mut best: Option<(f64, usize, f64, BTreeSet<usize>, BTreeSet<LineId>)> = None;
            for id in candidates {
                let Some(e) = graph.edges.get(id).and_then(Option::as_ref) else {
                    continue;
                };
                if e.geom.len() < 2 {
                    continue;
                }
                let (_, position, dist) = project_on_polyline(stop.pos, &e.geom);
                if dist > 4.0 * radius {
                    continue;
                }
                let covered_orig: BTreeSet<_> =
                    e.originals.intersection(&remaining_orig).copied().collect();
                let covered_lines: BTreeSet<_> = e
                    .lines
                    .iter()
                    .map(|o| o.line)
                    .filter(|line| remaining_lines.contains(line))
                    .collect();
                if covered_orig.is_empty() && covered_lines.is_empty() {
                    continue;
                }
                let missing_orig = remaining_orig.len().saturating_sub(covered_orig.len());
                let missing_lines = remaining_lines.len().saturating_sub(covered_lines.len());
                let score = dist
                    + 100.0 * missing_orig as f64 / remaining_orig.len().max(1) as f64
                    + 500.0 * missing_lines as f64 / remaining_lines.len().max(1) as f64;
                if best.as_ref().is_none_or(|(old, ..)| score < *old) {
                    best = Some((score, id, position, covered_orig, covered_lines));
                }
            }
            let Some((_, id, pos, covered_orig, covered_lines)) = best else {
                break;
            };
            let old_orig = graph.edges[id].as_ref().unwrap().originals.clone();
            let Some((node, left, right)) = split_at(graph, id, pos) else {
                break;
            };
            if left != right {
                for orig in old_orig {
                    let entry = index.entry(orig).or_default();
                    entry.retain(|&e| e != id);
                    entry.extend([left, right]);
                }
            }
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
            for line in &covered_lines {
                target.not_served.remove(line);
            }
            remaining_orig.retain(|orig| !covered_orig.contains(orig));
            remaining_lines.retain(|line| !covered_lines.contains(line));
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
        "[topo/stations] {} station placements, {} incomplete in {:.2?}",
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
