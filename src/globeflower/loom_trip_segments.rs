//! Convert globally aligned stop-to-stop trips into graph edges without
//! discarding nonzero station-to-itself cycles or conflating distinct visits.
use crate::loom_graph::{Graph, LineId, LineOcc, Point, add_line_occ, polyline_len, subline};
use std::collections::{HashMap, HashSet};

/// A shape occurrence, not simply a pair of endpoints. Two traversals can
/// share stations and shape_id yet use entirely different shape intervals.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentKey {
    pub from: usize,
    pub to: usize,
    pub shape_id: Option<String>,
    pub start_mm: i64,
    pub end_mm: i64,
}

pub struct EdgeRegistry {
    exact: HashMap<SegmentKey, Vec<usize>>,
    seen_pairs: HashSet<(usize, usize)>,
    next_original: usize,
    pub weights: HashMap<usize, usize>,
    pub line_weights: HashMap<(usize, LineId), usize>,
}

impl EdgeRegistry {
    pub fn new() -> Self {
        Self {
            exact: HashMap::new(),
            seen_pairs: HashSet::new(),
            next_original: 0,
            weights: HashMap::new(),
            line_weights: HashMap::new(),
        }
    }

    /// Add one source passage. The output is the last directed occurrence,
    /// which allows the next stop-to-stop segment to connect correctly.
    ///
    /// A nontrivial A->A journey requires at least three edges: a two-edge
    /// cycle would have parallel endpoints and could be silently merged by
    /// the simple-graph map constructor. Alternative A->B corridors get
    /// internal vertices for the same reason.
    pub fn append(
        &mut self,
        graph: &mut Graph,
        key: SegmentKey,
        geometry: Vec<Point>,
        line: LineId,
        weight: usize,
        previous: Option<(usize, usize)>,
    ) -> Option<(usize, usize)> {
        if geometry.len() < 2 || polyline_len(&geometry) <= 0.01 {
            return previous;
        }
        let from = key.from;
        let to = key.to;
        let endpoint_pair = (from.min(to), from.max(to));
        let edge_ids = if let Some(edge_ids) = self.exact.get(&key) {
            edge_ids.clone()
        } else {
            let nontrivial_cycle = from == to;
            let alternative = self.seen_pairs.contains(&endpoint_pair);
            let split = nontrivial_cycle || alternative;
            let mut segments = Vec::<(usize, usize, Vec<Point>)>::new();
            if split {
                let first = subline(&geometry, 0.0, 1.0 / 3.0);
                let second = subline(&geometry, 1.0 / 3.0, 2.0 / 3.0);
                let third = subline(&geometry, 2.0 / 3.0, 1.0);
                if first.len() < 2 || second.len() < 2 || third.len() < 2 {
                    return previous;
                }
                let p = graph.add_node(*first.last()?);
                let q = graph.add_node(*second.last()?);
                segments.extend([(from, p, first), (p, q, second), (q, to, third)]);
            } else {
                segments.push((from, to, geometry));
            }
            let mut ids = Vec::with_capacity(segments.len());
            for (a, b, geom) in segments {
                let id = graph.add_edge(a, b, geom);
                graph.edges[id]
                    .as_mut()?
                    .originals
                    .insert(self.next_original);
                self.next_original += 1;
                ids.push(id);
            }
            self.seen_pairs.insert(endpoint_pair);
            self.exact.insert(key, ids.clone());
            ids
        };
        let mut predecessor = previous;
        let mut current = from;
        for id in edge_ids {
            let edge = graph.edges.get(id)?.as_ref()?;
            if edge.a != current && edge.b != current {
                return None;
            }
            let source = current;
            let target = if edge.a == current { edge.b } else { edge.a };
            if let Some((prev_node, prev_edge)) = predecessor {
                if prev_node == source {
                    graph.nodes[source]
                        .as_mut()?
                        .allowed_turns
                        .entry(line)
                        .or_default()
                        .insert((prev_edge, id));
                }
            }
            let edge = graph.edges[id].as_mut()?;
            add_line_occ(
                &mut edge.lines,
                LineOcc {
                    line,
                    direction: Some(target),
                },
            );
            *self.weights.entry(id).or_default() += weight;
            *self.line_weights.entry((id, line)).or_default() += weight;
            predecessor = Some((target, id));
            current = target;
        }
        predecessor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(lon: f64, lat: f64) -> Point {
        Point { lon, lat }
    }
    #[test]
    fn real_cycle_survives_as_three_distinct_edges() {
        let mut graph = Graph::default();
        graph.lines.push(crate::loom_graph::Line {
            chateau: "a".into(),
            route_id: "circle".into(),
            label: "C".into(),
            color: "000000".into(),
            text_color: "FFFFFF".into(),
        });
        let a = p(0.0, 0.0);
        let v = graph.add_node(a);
        let mut registry = EdgeRegistry::new();
        let key = SegmentKey {
            from: v,
            to: v,
            shape_id: Some("c".into()),
            start_mm: 0,
            end_mm: 100000,
        };
        let end = registry
            .append(
                &mut graph,
                key,
                vec![a, p(0.01, 0.0), p(0.01, 0.01), p(0.0, 0.01), a],
                0,
                1,
                None,
            )
            .unwrap();
        assert_eq!(end.0, v);
        assert_eq!(graph.edges.iter().flatten().count(), 3);
        assert_eq!(graph.nodes.iter().flatten().count(), 3);
        assert!(
            graph
                .nodes
                .iter()
                .flatten()
                .any(|n| n.allowed_turns.get(&0).is_some())
        );
    }
    #[test]
    fn divergent_same_terminal_pair_is_not_collapsed() {
        let mut graph = Graph::default();
        let a = graph.add_node(p(0.0, 0.0));
        let b = graph.add_node(p(0.02, 0.0));
        let mut r = EdgeRegistry::new();
        for (name, dy) in [("a", 0.002), ("b", -0.002)] {
            r.append(
                &mut graph,
                SegmentKey {
                    from: a,
                    to: b,
                    shape_id: Some(name.into()),
                    start_mm: 0,
                    end_mm: 1000,
                },
                vec![p(0.0, 0.0), p(0.01, dy), p(0.02, 0.0)],
                0,
                1,
                None,
            );
        }
        assert_eq!(graph.edges.iter().flatten().count(), 4);
    }
}
