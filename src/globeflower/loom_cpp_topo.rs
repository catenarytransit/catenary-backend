//! Chapter-3 MapConstructor cleanup operations ported from LOOM C++.
//!
//! Reference: src/topo/mapconstructor/MapConstructor.cpp:
//! removeEdgeArtifacts/contractNodes, supportEdge and removeOrphanLines.
use crate::loom_graph::{Graph, LineId, LineOcc, metric_distance_m, polyline_len, subline};
use log::info;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

fn between(graph: &Graph, a: usize, b: usize) -> Option<usize> {
    graph
        .nodes
        .get(a)?
        .as_ref()?
        .adj
        .iter()
        .copied()
        .find(|&id| {
            graph
                .edges
                .get(id)
                .and_then(Option::as_ref)
                .is_some_and(|e| (e.a == a && e.b == b) || (e.a == b && e.b == a))
        })
}

/// C++ collapseShrdSegs "soft cleanup": after rebuilding the sampled graph,
/// combine an edge if NEITHER endpoint has degree two. The degree-two chains
/// are handled separately by combineEdges, which preserves their geometry.
/// Snapshot node IDs because successful contractions delete source nodes.
pub(crate) fn soft_cleanup(graph: &mut Graph) -> usize {
    let mut merged = 0;
    for from in 0..graph.nodes.len() {
        let Some(node) = graph.nodes[from].as_ref() else {
            continue;
        };
        if node.adj.len() == 2 {
            continue;
        }
        let candidates: Vec<_> = node.adj.iter().copied().collect();
        for eid in candidates {
            let Some(edge) = graph.edges.get(eid).and_then(Option::as_ref) else {
                continue;
            };
            if edge.a != from {
                continue;
            }
            let to = edge.b;
            if graph
                .nodes
                .get(to)
                .and_then(Option::as_ref)
                .is_none_or(|n| n.adj.len() == 2)
            {
                continue;
            }
            if crate::loom_map_constructor::combine_nodes(graph, from, to, eid) {
                merged += 1;
                break;
            }
        }
    }
    if merged != 0 {
        info!("[topo/mapconstructor] combined {merged} soft-cleanup nodes");
    }
    merged
}

/// C++ MapConstructor::contractNodes: collapse an edge only when folding
/// neighboring edges does not join polylines with vastly different lengths.
/// A work queue is equivalent to the C++ repeat-until-stable scan but avoids
/// rescanning a continent-sized component after each successful contraction.
pub(crate) fn remove_edge_artifacts(
    graph: &mut Graph,
    distance: f64,
    preserve_chains: bool,
) -> usize {
    let mut pending: VecDeque<usize> = graph.edges.iter().flatten().map(|e| e.id).collect();
    let mut merged = 0;
    while let Some(id) = pending.pop_front() {
        let Some(edge) = graph.edges.get(id).and_then(Option::as_ref) else {
            continue;
        };
        let (from, to) = (edge.a, edge.b);
        let (Some(a), Some(b)) = (graph.nodes[from].as_ref(), graph.nodes[to].as_ref()) else {
            continue;
        };
        if !a.stops.is_empty() || !b.stops.is_empty() || polyline_len(&edge.geom) >= distance {
            continue;
        }
        if preserve_chains {
            let linear = |node: usize| -> bool {
                let n = graph.nodes[node].as_ref().unwrap();
                if n.adj.len() != 2 {
                    return false;
                }
                let mut ids = n.adj.iter().copied();
                let x = ids.next().unwrap();
                let y = ids.next().unwrap();
                crate::loom_map_constructor::line_eq_at(graph, x, y, node)
            };
            if (a.adj.len() == 1 && b.adj.len() == 2 && linear(to))
                || (a.adj.len() == 2 && b.adj.len() == 1)
                || (a.adj.len() == 2 && b.adj.len() == 2 && linear(from) && linear(to))
            {
                continue;
            }
        }
        let mut cannot_contract = false;
        for &other_id in &a.adj {
            if other_id == id {
                continue;
            }
            let old = graph.edges[other_id].as_ref().unwrap();
            let far = if old.a == from { old.b } else { old.a };
            if let Some(existing_id) = between(graph, to, far) {
                let existing = graph.edges[existing_id].as_ref().unwrap();
                if (polyline_len(&existing.geom) - polyline_len(&old.geom)).abs() > 2.0 * distance {
                    cannot_contract = true;
                    break;
                }
            }
        }
        if cannot_contract {
            continue;
        }
        let neighbors: Vec<_> = a.adj.iter().chain(&b.adj).copied().collect();
        if crate::loom_map_constructor::combine_nodes(graph, from, to, id) {
            merged += 1;
            for nid in neighbors {
                pending.push_back(nid);
            }
            if let Some(kept) = graph.nodes[to].as_ref() {
                pending.extend(kept.adj.iter().copied());
            }
        }
    }
    if merged != 0 {
        info!("[topo/mapconstructor] removed {merged} edge artifacts (LOOM contractNodes)");
    }
    merged
}

/// C++ supportEdge: split a long blocking edge at half its arclength, so a
/// nearby degree-two node can contract without forming a parallel multiedge.
/// Return the two replacement edge IDs.
pub(crate) fn support_edge(graph: &mut Graph, edge_id: usize) -> Option<(usize, usize)> {
    let edge = graph.edges.get(edge_id)?.as_ref()?.clone();
    if edge.geom.len() < 2 || polyline_len(&edge.geom) <= 0.0 {
        return None;
    }
    let left = subline(&edge.geom, 0.0, 0.5);
    let right = subline(&edge.geom, 0.5, 1.0);
    if left.len() < 2 || right.len() < 2 {
        return None;
    }
    let point = *left.last()?;
    if metric_distance_m(point, graph.nodes[edge.a].as_ref()?.pos) < 1e-6
        || metric_distance_m(point, graph.nodes[edge.b].as_ref()?.pos) < 1e-6
    {
        return None;
    }
    graph.remove_edge(edge_id);
    let mid = graph.add_node(point);
    let ea = graph.add_edge(edge.a, mid, left);
    let eb = graph.add_edge(mid, edge.b, right);
    for (id, a, b) in [(ea, edge.a, mid), (eb, mid, edge.b)] {
        let target = graph.edges[id].as_mut().unwrap();
        target.originals = edge.originals.clone();
        for occ in &edge.lines {
            let destination = match occ.direction {
                None => None,
                Some(d) if d == edge.a => Some(a),
                Some(d) if d == edge.b => Some(b),
                _ => continue,
            };
            target.lines.insert(LineOcc {
                line: occ.line,
                direction: destination,
            });
        }
    }
    // C++ LineGraph::nodeRpl preserves restrictions at the surviving ends.
    // Remap the old edge handle independently for its two incident nodes.
    for (node_id, replacement) in [(edge.a, ea), (edge.b, eb)] {
        let node = graph.nodes[node_id].as_mut().unwrap();
        for turns in node.allowed_turns.values_mut() {
            *turns = turns
                .iter()
                .map(|&(x, y)| {
                    (
                        if x == edge_id { replacement } else { x },
                        if y == edge_id { replacement } else { y },
                    )
                })
                .collect();
        }
        for map in node.conn_exc.values_mut() {
            let mut updated = BTreeMap::<usize, BTreeSet<usize>>::new();
            for (&from, targets) in map.iter() {
                updated
                    .entry(if from == edge_id { replacement } else { from })
                    .or_default()
                    .extend(
                        targets
                            .iter()
                            .map(|&to| if to == edge_id { replacement } else { to }),
                    );
            }
            *map = updated;
        }
    }
    let turns = &mut graph.nodes[mid].as_mut().unwrap().allowed_turns;
    for occ in &edge.lines {
        let t = turns.entry(occ.line).or_default();
        match occ.direction {
            Some(d) if d == edge.a => {
                t.insert((eb, ea));
            }
            Some(d) if d == edge.b => {
                t.insert((ea, eb));
            }
            None => {
                t.insert((ea, eb));
                t.insert((eb, ea));
            }
            _ => {}
        }
    }
    Some((ea, eb))
}

fn line_continues(
    graph: &Graph,
    node_id: usize,
    first: usize,
    second: usize,
    line: LineId,
) -> bool {
    let Some(a) = graph.edges[first].as_ref() else {
        return false;
    };
    let Some(b) = graph.edges[second].as_ref() else {
        return false;
    };
    let Some(x) = a.lines.iter().find(|x| x.line == line) else {
        return false;
    };
    let Some(y) = b.lines.iter().find(|x| x.line == line) else {
        return false;
    };
    let node = graph.nodes[node_id].as_ref().unwrap();
    if node.adj.len() == 1 {
        return false;
    }
    if node
        .conn_exc
        .get(&line)
        .and_then(|m| m.get(&first))
        .is_some_and(|to| to.contains(&second))
    {
        return false;
    }
    x.direction.is_none()
        || y.direction.is_none()
        || ((x.direction == Some(node_id)) != (y.direction == Some(node_id)))
}

fn terminates_at(graph: &Graph, node_id: usize, edge_id: usize, line: LineId) -> bool {
    graph.nodes[node_id].as_ref().is_none_or(|node| {
        node.adj
            .iter()
            .copied()
            .filter(|&other| other != edge_id)
            .all(|other| !line_continues(graph, node_id, edge_id, other, line))
    })
}

/// C++ MapConstructor::removeOrphanLines, applied after StatInserter.
/// In particular, do not delete the entire line membership from an internal
/// edge where BOTH endpoints branch: the C++ guard protects that connection.
pub(crate) fn remove_orphan_lines(graph: &mut Graph) -> usize {
    let mut removed = 0;
    loop {
        let mut changes = Vec::<(usize, LineId)>::new();
        for edge in graph.edges.iter().flatten() {
            let a = graph.nodes[edge.a].as_ref().unwrap();
            let b = graph.nodes[edge.b].as_ref().unwrap();
            for occ in &edge.lines {
                let unserved_a = a.stops.is_empty() || a.not_served.contains(&occ.line);
                let unserved_b = b.stops.is_empty() || b.not_served.contains(&occ.line);
                if (unserved_a && terminates_at(graph, edge.a, edge.id, occ.line))
                    || (unserved_b && terminates_at(graph, edge.b, edge.id, occ.line))
                {
                    changes.push((edge.id, occ.line));
                }
            }
            if a.adj.len() != 1
                && b.adj.len() != 1
                && changes.iter().filter(|(id, _)| *id == edge.id).count() == edge.lines.len()
            {
                changes.retain(|(id, _)| *id != edge.id);
            }
        }
        if changes.is_empty() {
            break;
        }
        for (eid, line) in changes {
            let Some(edge) = graph.edges.get_mut(eid).and_then(Option::as_mut) else {
                continue;
            };
            let before = edge.lines.len();
            edge.lines.retain(|o| o.line != line);
            removed += before - edge.lines.len();
        }
        let empty: Vec<_> = graph
            .edges
            .iter()
            .flatten()
            .filter(|e| e.lines.is_empty())
            .map(|e| e.id)
            .collect();
        for eid in empty {
            graph.remove_edge(eid);
        }
        // The number of live (edge,line) occurrences strictly decreases each
        // round, so this is bounded even in highly connected intersections.
    }
    // Clear orphaned not_served flags and zero-degree nodes as in C++.
    let edges = &graph.edges;
    for node in graph.nodes.iter_mut().flatten() {
        let adjacent_lines: BTreeSet<LineId> = node
            .adj
            .iter()
            .filter_map(|&id| edges[id].as_ref())
            .flat_map(|e| e.lines.iter().map(|o| o.line))
            .collect();
        node.not_served.retain(|l| adjacent_lines.contains(l));
    }
    // Match LineGraph::delNd on isolated vertices after orphan edge removal.
    // An isolated station is not a useful map vertex either.
    for node in &mut graph.nodes {
        if node.as_ref().is_some_and(|n| n.adj.is_empty()) {
            *node = None;
        }
    }
    info!("[topo/mapconstructor] removed {removed} orphan line occurrences");
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loom_graph::Point;
    fn p(x: f64) -> Point {
        Point { lon: x, lat: 0.0 }
    }
    #[test]
    fn short_edge_artifact_is_collapsed() {
        let mut g = Graph::default();
        let a = g.add_node(p(0.0));
        let b = g.add_node(p(0.00001));
        let c = g.add_node(p(0.001));
        let e = g.add_edge(a, b, vec![p(0.0), p(0.00001)]);
        g.edges[e].as_mut().unwrap().originals.insert(0);
        g.add_edge(a, c, vec![p(0.0), p(0.001)]);
        assert_eq!(remove_edge_artifacts(&mut g, 10.0, false), 1);
        g.assert_consistent();
    }
    #[test]
    fn four_way_junction_bead_removal_keeps_arms() {
        let mut g = Graph::default();
        let center = g.add_node(p(0.0));
        let bead = g.add_node(p(0.00001));
        let west = g.add_node(p(-0.001));
        let east = g.add_node(p(0.001));
        let north = g.add_node(Point {
            lon: 0.0,
            lat: 0.001,
        });
        let south = g.add_node(Point {
            lon: 0.0,
            lat: -0.001,
        });
        g.add_edge(center, bead, vec![p(0.0), p(0.00001)]);
        for arm in [west, north] {
            g.add_edge(
                center,
                arm,
                vec![
                    g.nodes[center].as_ref().unwrap().pos,
                    g.nodes[arm].as_ref().unwrap().pos,
                ],
            );
        }
        for arm in [east, south] {
            g.add_edge(
                bead,
                arm,
                vec![
                    g.nodes[bead].as_ref().unwrap().pos,
                    g.nodes[arm].as_ref().unwrap().pos,
                ],
            );
        }
        assert_eq!(remove_edge_artifacts(&mut g, 10.0, false), 1);
        assert_eq!(g.edges.iter().flatten().count(), 4);
        let hub = g.nodes.iter().flatten().find(|n| n.adj.len() == 4).unwrap();
        assert_eq!(hub.adj.len(), 4);
        g.assert_consistent();
    }

    #[test]
    fn support_edge_preserves_provenance_and_direction() {
        let mut g = Graph::default();
        let a = g.add_node(p(0.0));
        let b = g.add_node(p(0.002));
        let id = g.add_edge(a, b, vec![p(0.0), p(0.002)]);
        g.edges[id].as_mut().unwrap().originals.insert(7);
        g.edges[id].as_mut().unwrap().lines.insert(LineOcc {
            line: 4,
            direction: Some(b),
        });
        let (left, right) = support_edge(&mut g, id).unwrap();
        assert_eq!(
            g.edges[left].as_ref().unwrap().originals,
            g.edges[right].as_ref().unwrap().originals
        );
        assert_eq!(
            g.edges[right]
                .as_ref()
                .unwrap()
                .lines
                .iter()
                .next()
                .unwrap()
                .direction,
            Some(b)
        );
        g.assert_consistent();
    }
}
