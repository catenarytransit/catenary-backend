//! Conservative cartographic normalization of genuine four-arm railway junctions.
//!
//! This pass runs AFTER LOOM-style topology construction. It changes only node
//! positions and the first section of incident edge polylines; no connectivity,
//! line membership, turn restriction, or original-edge provenance is changed.
//!
//! An apparent geometric X is NOT enough evidence. We require at least two
//! distinct routes whose *same source GTFS edge* appears on two non-opposite
//! approaches (evidence of turning inside a stop-to-stop shape). At least one
//! approach must be shared by those turning routes. The two fitted axes must
//! also be near-orthogonal, and the fitted center must be near the existing
//! graph junction. Otherwise this pass is deliberately a no-op.
use crate::loom_graph::{Graph, LineId, Point, lerp, metric_distance_m, web_mercator, from_web_mercator};
use log::info;
use std::collections::{BTreeSet, HashMap};

// All distances below are *projected* metres, matching the existing topo
// geometry predicates. These are conservative first-pass tuning parameters.
const MIN_ARM_LENGTH_M: f64 = 75.0;
const MAX_ANCHOR_SHIFT_M: f64 = 65.0;
const MAX_AXIS_DOT: f64 = 0.40; // axes differ by at least ~66 degrees
const MAX_OPPOSITE_DOT: f64 = -0.50; // approach arms differ by >= 120 degrees
const MAX_JOIN_RESIDUAL_M: f64 = 16.0;
const MIN_TURNING_ROUTES: usize = 2;

#[derive(Clone, Copy, Debug)]
struct XY {
    x: f64,
    y: f64,
}

impl XY {
    fn of(point: Point) -> Self {
        let (x, y) = web_mercator(point);
        Self { x, y }
    }
    fn point(self) -> Point {
        from_web_mercator((self.x, self.y))
    }
    fn minus(self, b: Self) -> Self {
        Self { x: self.x - b.x, y: self.y - b.y }
    }
    fn plus(self, b: Self) -> Self {
        Self { x: self.x + b.x, y: self.y + b.y }
    }
    fn times(self, scale: f64) -> Self {
        Self { x: self.x * scale, y: self.y * scale }
    }
    fn dot(self, b: Self) -> f64 {
        self.x * b.x + self.y * b.y
    }
    fn cross(self, b: Self) -> f64 {
        self.x * b.y - self.y * b.x
    }
    fn length(self) -> f64 {
        self.x.hypot(self.y)
    }
    fn normalized(self) -> Option<Self> {
        let length = self.length();
        (length > 1e-9 && length.is_finite()).then(|| self.times(1.0 / length))
    }
}

struct Arm {
    edge_id: usize,
    // Edge geometry in node -> other endpoint order.
    outward: Vec<Point>,
    far: XY,
    join: XY,
    direction: XY,
    length: f64,
    lines: BTreeSet<LineId>,
    originals: BTreeSet<usize>,
}

// Arc-length sampling, rather than vertex sampling: GTFS polylines can have
// wildly different vertex densities, including dense turnout corners.
fn sample_at(geometry: &[Point], distance: f64) -> Option<Point> {
    let mut remaining = distance.max(0.0);
    let first = *geometry.first()?;
    for pair in geometry.windows(2) {
        let d = metric_distance_m(pair[0], pair[1]);
        if d <= 1e-9 {
            continue;
        }
        if remaining <= d {
            return Some(lerp(pair[0], pair[1], remaining / d));
        }
        remaining -= d;
    }
    Some(*geometry.last().unwrap_or(&first))
}

fn arm_at(graph: &Graph, node_id: usize, edge_id: usize) -> Option<Arm> {
    let edge = graph.edges.get(edge_id)?.as_ref()?;
    let mut outward = edge.geom.clone();
    if edge.b == node_id {
        outward.reverse();
    } else if edge.a != node_id {
        return None;
    }
    if outward.len() < 2 {
        return None;
    }
    let length: f64 = outward.windows(2).map(|p| metric_distance_m(p[0], p[1])).sum();
    if length < MIN_ARM_LENGTH_M {
        return None;
    }
    let far = XY::of(sample_at(&outward, length * 0.92)?);
    let join = XY::of(sample_at(&outward, (length * 0.85).min(105.0))?);
    let origin = XY::of(graph.nodes.get(node_id)?.as_ref()?.pos);
    let direction = far.minus(origin).normalized()?;
    Some(Arm {
        edge_id,
        outward,
        far,
        join,
        direction,
        length,
        lines: edge.lines.iter().map(|o| o.line).collect(),
        originals: edge.originals.clone(),
    })
}

#[derive(Clone, Copy)]
struct Axes {
    // Indices of opposite arms, ordered as two intersecting axes.
    pairs: [(usize, usize); 2],
    center: XY,
}

fn fit_axes(arms: &[Arm; 4], old_center: XY) -> Option<Axes> {
    let matchings = [
        [(0, 1), (2, 3)],
        [(0, 2), (1, 3)],
        [(0, 3), (1, 2)],
    ];
    let mut best: Option<(f64, Axes)> = None;
    for pairs in matchings {
        let (a, b) = pairs[0];
        let (c, d) = pairs[1];
        let opposite_1 = arms[a].direction.dot(arms[b].direction);
        let opposite_2 = arms[c].direction.dot(arms[d].direction);
        if opposite_1 > MAX_OPPOSITE_DOT || opposite_2 > MAX_OPPOSITE_DOT {
            continue;
        }
        // Fit the long straight alignments from distant approaches, not the
        // turn curves immediately around the junction. These two small line
        // equations determine the junction center without iterative drift.
        let v1 = arms[b].far.minus(arms[a].far);
        let v2 = arms[d].far.minus(arms[c].far);
        let (Some(u1), Some(u2)) = (v1.normalized(), v2.normalized()) else {
            continue;
        };
        let dot = u1.dot(u2).abs();
        if dot > MAX_AXIS_DOT {
            continue;
        }
        let denominator = v1.cross(v2);
        if denominator.abs() < 1e-8 * v1.length() * v2.length() {
            continue;
        }
        let t = arms[c].far.minus(arms[a].far).cross(v2) / denominator;
        let center = arms[a].far.plus(v1.times(t));
        if !center.x.is_finite() || !center.y.is_finite()
            || center.minus(old_center).length() > MAX_ANCHOR_SHIFT_M
        {
            continue;
        }
        let score = (opposite_1 + 1.0).abs()
            + (opposite_2 + 1.0).abs()
            + dot
            + center.minus(old_center).length() / MAX_ANCHOR_SHIFT_M;
        let solution = Axes { pairs, center };
        if best.as_ref().is_none_or(|(previous, _)| score < *previous) {
            best = Some((score, solution));
        }
    }
    best.map(|(_, axes)| axes)
}

fn turning_witnesses(
    arms: &[Arm; 4],
    axes: &Axes,
    original_routes: &HashMap<usize, BTreeSet<LineId>>,
) -> bool {
    let opposite = |a: usize, b: usize| {
        axes.pairs.iter().any(|&(i, j)| (i == a && j == b) || (i == b && j == a))
    };
    let mut witnessed_routes = BTreeSet::new();
    for a in 0..4 {
        for b in (a + 1)..4 {
            if opposite(a, b) {
                continue; // continuing straight is not a turning witness
            }
            // An original GTFS edge representing a stop-to-stop trip shape
            // must survive on BOTH incident arms, for the SAME route. This
            // is stronger than merely finding equal colors or route names.
            let (short, long) = if arms[a].originals.len() <= arms[b].originals.len() {
                (&arms[a].originals, &arms[b].originals)
            } else {
                (&arms[b].originals, &arms[a].originals)
            };
            for original in short {
                if !long.contains(original) {
                    continue;
                }
                if let Some(routes) = original_routes.get(original) {
                    for route in routes {
                        if arms[a].lines.contains(route) && arms[b].lines.contains(route) {
                            witnessed_routes.insert(*route);
                        }
                    }
                }
            }
        }
    }
    // Independent redundant turning routes must also share an approach arm.
    // This excludes an incidental X and unrelated single-service curves.
    witnessed_routes.len() >= MIN_TURNING_ROUTES
        && arms.iter().any(|arm| arm.lines.intersection(&witnessed_routes).take(2).count() >= 2)
}

fn valid_join_geometry(arms: &[Arm; 4], axes: &Axes) -> bool {
    axes.pairs.iter().all(|&(a, b)| {
        let (Some(axis), true) = (
            arms[b].far.minus(arms[a].far).normalized(),
            arms[a].length >= MIN_ARM_LENGTH_M && arms[b].length >= MIN_ARM_LENGTH_M,
        ) else {
            return false;
        };
        [a, b].into_iter().all(|i| {
            // The preserved outer geometry must reconnect near the fitted
            // straight axis; otherwise we would introduce a visible kink.
            let residual = arms[i].join.minus(axes.center).cross(axis).abs();
            let outward = arms[i].far.minus(axes.center);
            residual <= MAX_JOIN_RESIDUAL_M
                && outward.length() >= 30.0
                && outward.dot(arms[i].direction) > 0.0
        })
    })
}

fn straighten_arm(graph: &mut Graph, node_id: usize, arm: &Arm, center: Point) {
    // Preserve all geometry beyond the cut, and keep the same edge ID and
    // endpoint directions. The local segment is straight; Harebell draws
    // service-specific Bézier turns across the canonical plus.
    let cut = (arm.length * 0.85).min(105.0);
    let Some(join) = sample_at(&arm.outward, cut) else {
        return;
    };
    let mut geometry = vec![center, join];
    let mut traveled = 0.0;
    for pair in arm.outward.windows(2) {
        traveled += metric_distance_m(pair[0], pair[1]);
        if traveled > cut + 1e-6 && metric_distance_m(*geometry.last().unwrap(), pair[1]) > 1e-6 {
            geometry.push(pair[1]);
        }
    }
    let Some(edge) = graph.edges[arm.edge_id].as_mut() else {
        return;
    };
    if edge.b == node_id {
        geometry.reverse();
    }
    edge.geom = geometry;
}

/// Normalize only four-arm junctions with redundant source-shape turning
/// witnesses and robust, near-orthogonal long approach axes.
///
/// Does not invent a junction, connect disconnected nodes, change stop
/// positions, or conflate physical parallel tracks. This belongs after the
/// final topo reconstruct_intersections()/orphan cleanup, not inside its
/// convergence loop, so another averaging pass cannot undo the result.
pub fn normalize(graph: &mut Graph, reference: &Graph) -> usize {
    let mut original_routes: HashMap<usize, BTreeSet<LineId>> = HashMap::new();
    for edge in reference.edges.iter().flatten() {
        for &original in &edge.originals {
            original_routes.entry(original).or_default()
                .extend(edge.lines.iter().map(|occ| occ.line));
        }
    }

    let mut fixed = 0;
    // Do not modify both ends of the same short block independently. Nearby
    // four-way junctions can be less than two straightening radii apart;
    // overlapping edits could produce a new bend or an unstable result.
    let mut modified_edges = BTreeSet::new();
    // Snapshot node IDs; no graph nodes/edges are added or deleted by this pass.
    let candidates: Vec<_> = graph.nodes.iter().flatten()
        .filter(|n| n.adj.len() == 4 && n.stops.is_empty())
        .map(|n| n.id).collect();
    for node_id in candidates {
        let Some(node) = graph.nodes[node_id].as_ref() else {
            continue;
        };
        let old_center = XY::of(node.pos);
        let ids: Vec<_> = node.adj.iter().copied().collect();
        let arms: Option<Vec<_>> = ids.iter().map(|&id| arm_at(graph, node_id, id)).collect();
        let Some(arms) = arms.and_then(|items| items.try_into().ok()) else {
            continue;
        };
        let arms: [Arm; 4] = arms;
        if arms.iter().any(|arm| modified_edges.contains(&arm.edge_id)) {
            continue;
        }
        let Some(axes) = fit_axes(&arms, old_center) else {
            continue;
        };
        if !turning_witnesses(&arms, &axes, &original_routes)
            || !valid_join_geometry(&arms, &axes)
        {
            continue;
        }
        let anchor = axes.center.point();
        graph.nodes[node_id].as_mut().unwrap().pos = anchor;
        for arm in &arms {
            straighten_arm(graph, node_id, arm, anchor);
            modified_edges.insert(arm.edge_id);
        }
        fixed += 1;
        info!(
            "[topo/junction] normalized four-arm junction {} by {:.1} projected metres",
            node_id, old_center.minus(axes.center).length()
        );
    }
    fixed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loom_graph::LineOcc;

    fn p(x: f64, y: f64) -> Point {
        from_web_mercator((x, y))
    }

    // Two independent turn movements (0: north-east, 1: north-west)
    // overlap on the northern arm. A third route runs north-south.
    fn fixture() -> (Graph, Graph, usize) {
        let mut result = Graph::default();
        let hub = result.add_node(p(-35.0, -9.0));
        let directions = [
            (p(0.0, 120.0), p(-9.0, 53.0), &[0usize, 1, 2][..], &[101usize, 102, 103][..]),
            (p(120.0, 0.0), p(52.0, -4.0), &[0][..], &[101][..]),
            (p(0.0, -120.0), p(-7.0, -54.0), &[2][..], &[103][..]),
            (p(-120.0, 0.0), p(-68.0, -5.0), &[1][..], &[102][..]),
        ];
        for (far, middle, routes, originals) in directions {
            let endpoint = result.add_node(far);
            let id = result.add_edge(hub, endpoint, vec![result.nodes[hub].as_ref().unwrap().pos, middle, far]);
            let edge = result.edges[id].as_mut().unwrap();
            edge.originals.extend(originals.iter().copied());
            edge.lines.extend(routes.iter().map(|&line| LineOcc { line, direction: None }));
        }
        let mut reference = Graph::default();
        for (original, route) in [(101usize, 0usize), (102, 1), (103, 2)] {
            let a = reference.add_node(p(-140.0, original as f64));
            let b = reference.add_node(p(140.0, original as f64));
            let id = reference.add_edge(a, b, vec![reference.nodes[a].as_ref().unwrap().pos, reference.nodes[b].as_ref().unwrap().pos]);
            reference.edges[id].as_mut().unwrap().originals.insert(original);
            reference.edges[id].as_mut().unwrap().lines.insert(LineOcc { line: route, direction: None });
        }
        (result, reference, hub)
    }

    #[test]
    fn redundant_turning_routes_recover_displaced_plus() {
        let (mut output, reference, hub) = fixture();
        assert_eq!(normalize(&mut output, &reference), 1);
        let pos = output.nodes[hub].as_ref().unwrap().pos;
        assert!(metric_distance_m(pos, p(0.0, 0.0)) < 12.0, "fitted junction did not recover axes");
        for &edge_id in &output.nodes[hub].as_ref().unwrap().adj {
            let edge = output.edges[edge_id].as_ref().unwrap();
            assert_eq!(edge.geom.first().copied(), Some(pos));
            assert!(!edge.lines.is_empty());
            assert!(!edge.originals.is_empty());
        }
        output.assert_consistent();
    }

    #[test]
    fn apparent_crossing_without_turn_provenance_is_unchanged() {
        let (mut output, mut reference, hub) = fixture();
        reference.edges.iter_mut().flatten().for_each(|e| e.originals.clear());
        let original = output.nodes[hub].as_ref().unwrap().pos;
        assert_eq!(normalize(&mut output, &reference), 0);
        assert_eq!(output.nodes[hub].as_ref().unwrap().pos, original);
    }

    #[test]
    fn one_turning_route_is_insufficient() {
        let (mut output, reference, hub) = fixture();
        for edge in output.edges.iter_mut().flatten() {
            edge.lines.retain(|o| o.line != 1);
        }
        let original = output.nodes[hub].as_ref().unwrap().pos;
        assert_eq!(normalize(&mut output, &reference), 0);
        assert_eq!(output.nodes[hub].as_ref().unwrap().pos, original);
    }

    #[test]
    fn a_long_distance_shift_is_rejected() {
        let (mut output, reference, hub) = fixture();
        output.nodes[hub].as_mut().unwrap().pos = p(-130.0, -9.0);
        assert_eq!(normalize(&mut output, &reference), 0);
    }
}
