#[path = "loom_semantics.rs"]
mod loom_semantics;

use crate::loom_graph::{
    Graph, LineId, LineOcc, Point, Stop, haversine_m, lerp, polyline_len, project_on_polyline,
    subline,
};
use log::info;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct TopoConfig {
    pub max_aggr_distance: f64,
    pub max_length_dev: f64,
    pub max_turn_restr_check_dist: f64,
    pub segment_length: f64,
    pub infer_restrictions: bool,
}

impl Default for TopoConfig {
    fn default() -> Self {
        Self {
            max_aggr_distance: 50.0,
            max_length_dev: 500.0,
            max_turn_restr_check_dist: 50.0,
            segment_length: 5.0,
            infer_restrictions: true,
        }
    }
}

#[derive(Clone, Copy)]
struct Atom {
    // Keep only the source edge here.  Line/original metadata stays on the
    // source edge and is collected once a geometric atom cluster is known.
    // The old representation materialized |lines| * |originals| copies of
    // every 5 m segment, which is catastrophic on shared metro corridors.
    edge: usize,
    a: Point,
    b: Point,
}

#[derive(Clone)]
struct StationOcc {
    stops: Vec<Stop>,
    originals: BTreeSet<usize>,
    lines: BTreeSet<LineId>,
    geom: Vec<Point>,
}

/// Run the combined Chapter-3 topology stage over the preliminary GTFS graph.
pub fn run(mut input: Graph, cfg: &TopoConfig) -> Graph {
    let stage = Instant::now();
    let station_occurrences = collect_stations(&mut input);
    info!(
        "[topo] collected {} station clusters in {:.2?}",
        station_occurrences.len(),
        stage.elapsed()
    );

    let stage = Instant::now();
    // LOOM-style long-edge-first construction with a geographic node index.
    // Do not build a global union-find over every sampled 5m atom.
    let mut output = crate::loom_map_constructor::construct(&input, cfg.max_aggr_distance);
    info!(
        "[topo] aggregation produced {} nodes / {} edges in {:.2?}",
        output.nodes.iter().flatten().count(),
        output.edges.iter().flatten().count(),
        stage.elapsed()
    );

    if cfg.infer_restrictions {
        let stage = Instant::now();
        loom_semantics::infer_restrictions(&input, &mut output);
        info!("[topo] inferred restrictions in {:.2?}", stage.elapsed());
    }

    let stage = Instant::now();
    let station_inputs: Vec<_> = station_occurrences
        .into_iter()
        .map(|o| loom_semantics::StationOccurrence {
            stops: o.stops,
            originals: o.originals,
            lines: o.lines,
        })
        .collect();
    loom_semantics::insert_stations(&station_inputs, &mut output, cfg.max_aggr_distance);
    info!("[topo] inserted stations in {:.2?}", stage.elapsed());
    output
}

fn collect_stations(graph: &mut Graph) -> Vec<StationOcc> {
    let mut by_name = BTreeMap::<(String, String), StationOcc>::new();

    for node in graph.nodes.iter_mut().filter_map(Option::as_mut) {
        for stop in std::mem::take(&mut node.stops) {
            let entry = by_name
                .entry((stop.chateau.clone(), stop.stop_id.clone()))
                .or_insert_with(|| StationOcc {
                    stops: Vec::new(),
                    originals: BTreeSet::new(),
                    lines: BTreeSet::new(),
                    geom: Vec::new(),
                });

            entry.geom.push(stop.pos);
            entry.stops.push(stop);

            for &edge_id in &node.adj {
                if let Some(edge) = &graph.edges[edge_id] {
                    entry.originals.extend(edge.originals.iter().copied());
                    entry.lines.extend(edge.lines.iter().map(|occ| occ.line));
                }
            }
        }
    }

    by_name.into_values().collect()
}

fn atomize(graph: &Graph, step_m: f64) -> Vec<Atom> {
    let mut atoms = Vec::new();
    let step_m = step_m.max(0.5);

    // LOOM samples geometry, not the Cartesian product of geometry x lines x
    // provenance edges.  Stream the samples directly so we also avoid building
    // a second dense polyline for every input edge.
    for edge in graph.edges.iter().filter_map(Option::as_ref) {
        for segment in edge.geom.windows(2) {
            let distance = haversine_m(segment[0], segment[1]);
            let pieces = (distance / step_m).ceil().max(1.0) as usize;
            let mut a = segment[0];

            for i in 1..=pieces {
                let b = lerp(segment[0], segment[1], i as f64 / pieces as f64);
                atoms.push(Atom {
                    edge: edge.id,
                    a,
                    b,
                });
                a = b;
            }
        }
    }

    atoms
}

fn midpoint(a: Point, b: Point) -> Point {
    lerp(a, b, 0.5)
}

fn bearing(a: Point, b: Point) -> f64 {
    let mean_lat = ((a.lat + b.lat) * 0.5).to_radians();
    let y = (b.lon - a.lon).to_radians() * mean_lat.cos();
    let x = (b.lat - a.lat).to_radians();
    y.atan2(x)
}

fn angle_diff(a: f64, b: f64) -> f64 {
    let mut d = (a - b).abs() % std::f64::consts::PI;
    if d > std::f64::consts::FRAC_PI_2 {
        d = std::f64::consts::PI - d;
    }
    d
}

struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let mut a = self.find(a);
        let mut b = self.find(b);

        if a == b {
            return;
        }

        if self.rank[a] < self.rank[b] {
            std::mem::swap(&mut a, &mut b);
        }

        self.parent[b] = a;

        if self.rank[a] == self.rank[b] {
            self.rank[a] += 1;
        }
    }
}

fn aggregate(input: &Graph, atoms: &[Atom], max_distance_m: f64) -> Graph {
    let mut output = Graph::default();
    output.lines = input.lines.clone();

    if atoms.is_empty() {
        return output;
    }

    let mut uf = UnionFind::new(atoms.len());

    // Geographic hash grid. The latitude-dependent error in longitude width is
    // harmless here because exact haversine distance is checked before union.
    let cell_degrees = max_distance_m / 111_320.0;
    let mut buckets = HashMap::<(i64, i64), Vec<usize>>::new();

    for (index, atom) in atoms.iter().enumerate() {
        let m = midpoint(atom.a, atom.b);
        let key = (
            (m.lon / cell_degrees).floor() as i64,
            (m.lat / cell_degrees).floor() as i64,
        );
        buckets.entry(key).or_default().push(index);
    }

    for (&key, ids) in &buckets {
        for dx in -1..=1 {
            for dy in -1..=1 {
                let Some(other_ids) = buckets.get(&(key.0 + dx, key.1 + dy)) else {
                    continue;
                };

                for &i in ids {
                    for &j in other_ids {
                        if j <= i {
                            continue;
                        }

                        let a = &atoms[i];
                        let b = &atoms[j];

                        if haversine_m(midpoint(a.a, a.b), midpoint(b.a, b.b)) > max_distance_m {
                            continue;
                        }

                        if angle_diff(bearing(a.a, a.b), bearing(b.a, b.b)) > 35.0_f64.to_radians()
                        {
                            continue;
                        }

                        uf.union(i, j);
                    }
                }
            }
        }
    }

    let mut groups = BTreeMap::<usize, Vec<usize>>::new();
    for i in 0..atoms.len() {
        let root = uf.find(i);
        groups.entry(root).or_default().push(i);
    }

    let mut shared_segments = Vec::new();

    for ids in groups.into_values() {
        let reference_bearing = bearing(atoms[ids[0]].a, atoms[ids[0]].b);

        let mut start_lon = 0.0;
        let mut start_lat = 0.0;
        let mut end_lon = 0.0;
        let mut end_lat = 0.0;
        let mut line_occurrences = BTreeSet::new();
        let mut originals = BTreeSet::new();

        for &index in &ids {
            let mut a = atoms[index].a;
            let mut b = atoms[index].b;

            if (bearing(a, b) - reference_bearing).cos() < 0.0 {
                std::mem::swap(&mut a, &mut b);
            }

            start_lon += a.lon;
            start_lat += a.lat;
            end_lon += b.lon;
            end_lat += b.lat;
            let source = input.edges[atoms[index].edge]
                .as_ref()
                .expect("atom source edge exists");
            line_occurrences.extend(source.lines.iter().copied());
            originals.extend(source.originals.iter().copied());
        }

        let count = ids.len() as f64;

        shared_segments.push((
            Point {
                lon: start_lon / count,
                lat: start_lat / count,
            },
            Point {
                lon: end_lon / count,
                lat: end_lat / count,
            },
            line_occurrences,
            originals,
        ));
    }

    let mut endpoints = Vec::with_capacity(shared_segments.len() * 2);
    for segment in &shared_segments {
        endpoints.push(segment.0);
        endpoints.push(segment.1);
    }

    let mut endpoint_uf = UnionFind::new(endpoints.len());

    // Do not compare every endpoint with every other endpoint.  On a large
    // component this was O(S^2) after sampling and is the main reason topo
    // appeared to freeze.  The same fixed-radius grid used above gives
    // expected O(S + K), where K is the number of genuinely local candidates.
    let endpoint_cell_degrees = (max_distance_m / 111_320.0).max(1e-9);
    let mut endpoint_buckets = HashMap::<(i64, i64), Vec<usize>>::new();
    for (index, point) in endpoints.iter().enumerate() {
        let key = (
            (point.lon / endpoint_cell_degrees).floor() as i64,
            (point.lat / endpoint_cell_degrees).floor() as i64,
        );
        endpoint_buckets.entry(key).or_default().push(index);
    }

    for (&key, ids) in &endpoint_buckets {
        for dx in -1..=1 {
            for dy in -1..=1 {
                let Some(other_ids) = endpoint_buckets.get(&(key.0 + dx, key.1 + dy)) else {
                    continue;
                };
                for &i in ids {
                    for &j in other_ids {
                        if j <= i {
                            continue;
                        }
                        if haversine_m(endpoints[i], endpoints[j]) <= max_distance_m {
                            endpoint_uf.union(i, j);
                        }
                    }
                }
            }
        }
    }

    let mut centroids = HashMap::<usize, (f64, f64, usize)>::new();

    for (index, point) in endpoints.iter().enumerate() {
        let root = endpoint_uf.find(index);
        let entry = centroids.entry(root).or_insert((0.0, 0.0, 0));
        entry.0 += point.lon;
        entry.1 += point.lat;
        entry.2 += 1;
    }

    let mut node_for_root = HashMap::new();

    for (root, (lon_sum, lat_sum, count)) in centroids {
        let node_id = output.add_node(Point {
            lon: lon_sum / count as f64,
            lat: lat_sum / count as f64,
        });
        node_for_root.insert(root, node_id);
    }

    for (segment_index, segment) in shared_segments.into_iter().enumerate() {
        let start_root = endpoint_uf.find(segment_index * 2);
        let end_root = endpoint_uf.find(segment_index * 2 + 1);

        let a = node_for_root[&start_root];
        let b = node_for_root[&end_root];

        if a == b {
            continue;
        }

        let geometry = vec![
            output.nodes[a].as_ref().expect("node exists").pos,
            output.nodes[b].as_ref().expect("node exists").pos,
        ];

        let edge_id = output.add_edge(a, b, geometry);
        let edge = output.edges[edge_id].as_mut().expect("newly inserted edge");
        edge.lines = segment.2;
        edge.originals = segment.3;
    }

    contract_degree_two(&mut output);
    output
}

fn degree_two_candidate(graph: &Graph, node_id: usize) -> Option<(usize, usize)> {
    let node = graph.nodes.get(node_id)?.as_ref()?;
    if node.adj.len() != 2 || !node.stops.is_empty() {
        return None;
    }

    let mut adjacent = node.adj.iter();
    let e1 = *adjacent.next()?;
    let e2 = *adjacent.next()?;
    let edge1 = graph.edges.get(e1)?.as_ref()?;
    let edge2 = graph.edges.get(e2)?.as_ref()?;

    // A change in line membership is a real topological event and must not be
    // contracted away.
    (edge1.lines == edge2.lines).then_some((e1, e2))
}

fn contract_degree_two(graph: &mut Graph) {
    // The old loop rescanned the complete node vector after every contraction:
    // O(V^2) on a long sampled line.  Only the two neighbours of a contracted
    // node can become new degree-two candidates, so use a local work queue.
    let mut queue: VecDeque<usize> = graph
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(id, node)| node.as_ref().map(|_| id))
        .collect();

    while let Some(node_id) = queue.pop_front() {
        let Some((e1_id, e2_id)) = degree_two_candidate(graph, node_id) else {
            continue;
        };

        let edge1 = graph.edges[e1_id].clone().expect("edge exists");
        let edge2 = graph.edges[e2_id].clone().expect("edge exists");

        let u = if edge1.a == node_id { edge1.b } else { edge1.a };
        let v = if edge2.a == node_id { edge2.b } else { edge2.a };

        // A two-edge cycle is not contractible, but it must not abort
        // contraction of unrelated nodes.
        if u == v {
            continue;
        }

        let node_pos = graph.nodes[node_id].as_ref().expect("node exists").pos;

        let mut geometry = edge1.geom.clone();
        if geometry.last().copied() != Some(node_pos) {
            geometry.reverse();
        }

        let mut right = edge2.geom.clone();
        if right.first().copied() != Some(node_pos) {
            right.reverse();
        }
        geometry.extend(right.into_iter().skip(1));

        graph.remove_edge(e1_id);
        graph.remove_edge(e2_id);
        graph.nodes[node_id] = None;

        let new_edge_id = graph.add_edge(u, v, geometry);
        let new_edge = graph.edges[new_edge_id]
            .as_mut()
            .expect("newly inserted edge");
        new_edge.lines = edge1.lines;
        new_edge.originals = edge1.originals.union(&edge2.originals).copied().collect();

        queue.push_back(u);
        queue.push_back(v);
    }
}

fn infer_restrictions(original: &Graph, graph: &mut Graph, cfg: &TopoConfig) {
    // original_edges_connected used to linearly scan the entire original edge
    // vector twice for every candidate turn.  Build the provenance lookup once.
    let mut original_index = HashMap::<usize, usize>::new();
    for (edge_id, edge) in original.edges.iter().enumerate() {
        let Some(edge) = edge.as_ref() else {
            continue;
        };
        for &original_id in &edge.originals {
            original_index.entry(original_id).or_insert(edge_id);
        }
    }

    let node_ids: Vec<usize> = graph
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(id, node)| node.as_ref().map(|_| id))
        .collect();

    for node_id in node_ids {
        let adjacent: Vec<usize> = graph.nodes[node_id]
            .as_ref()
            .expect("node exists")
            .adj
            .iter()
            .copied()
            .collect();

        for &incoming in &adjacent {
            for &outgoing in &adjacent {
                if incoming == outgoing {
                    continue;
                }

                let Some(a) = &graph.edges[incoming] else {
                    continue;
                };
                let Some(b) = &graph.edges[outgoing] else {
                    continue;
                };

                let lines: BTreeSet<_> = a.lines.iter().map(|occ| occ.line).collect();

                for line in lines {
                    if !b.lines.iter().any(|occ| occ.line == line) {
                        continue;
                    }

                    let directly_supported = a.originals.iter().any(|from_original| {
                        b.originals.iter().any(|to_original| {
                            original_edges_connected(
                                original,
                                &original_index,
                                *from_original,
                                *to_original,
                                line,
                            )
                        })
                    });

                    if directly_supported {
                        continue;
                    }

                    if short_line_specific_explanation(
                        graph,
                        incoming,
                        outgoing,
                        node_id,
                        line,
                        cfg.max_length_dev,
                    ) {
                        continue;
                    }

                    graph.nodes[node_id]
                        .as_mut()
                        .expect("node exists")
                        .conn_exc
                        .entry(line)
                        .or_default()
                        .entry(incoming)
                        .or_default()
                        .insert(outgoing);
                }
            }
        }
    }
}

fn original_edges_connected(
    graph: &Graph,
    original_index: &HashMap<usize, usize>,
    a_original: usize,
    b_original: usize,
    line: LineId,
) -> bool {
    let a = original_index
        .get(&a_original)
        .and_then(|&edge_id| graph.edges.get(edge_id))
        .and_then(Option::as_ref);
    let b = original_index
        .get(&b_original)
        .and_then(|&edge_id| graph.edges.get(edge_id))
        .and_then(Option::as_ref);

    match (a, b) {
        (Some(a), Some(b)) => {
            let a_has_line = a.lines.iter().any(|occ| occ.line == line);
            let b_has_line = b.lines.iter().any(|occ| occ.line == line);
            a_has_line
                && b_has_line
                && [a.a, a.b].iter().any(|&shared| {
                    (b.a == shared || b.b == shared)
                        && graph.nodes[shared]
                            .as_ref()
                            .and_then(|n| n.allowed_turns.get(&line))
                            .is_some_and(|turns| {
                                turns.contains(&(a.id, b.id)) || turns.contains(&(b.id, a.id))
                            })
                })
        }
        _ => false,
    }
}

#[derive(Copy, Clone, PartialEq)]
struct QueueEntry(f64, usize);

impl Eq for QueueEntry {}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.total_cmp(&self.0)
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Search from the *far endpoint* of the incoming edge to the far endpoint of
/// the outgoing edge.  Starting both searches at the shared node would make
/// every candidate trivially reachable with zero cost.
fn short_line_specific_explanation(
    graph: &Graph,
    incoming: usize,
    outgoing: usize,
    shared_node: usize,
    line: LineId,
    max_distance: f64,
) -> bool {
    let Some(in_edge) = &graph.edges[incoming] else {
        return false;
    };
    let Some(out_edge) = &graph.edges[outgoing] else {
        return false;
    };

    let start = if in_edge.a == shared_node {
        in_edge.b
    } else {
        in_edge.a
    };
    let goal = if out_edge.a == shared_node {
        out_edge.b
    } else {
        out_edge.a
    };

    // This search is bounded to max_distance.  Clearing a |V|-sized vector for
    // every candidate turn makes restriction inference O(T * V) even when each
    // search visits only a handful of nearby nodes.
    let mut distances = HashMap::<usize, f64>::new();
    let mut queue = BinaryHeap::new();

    distances.insert(start, 0.0);
    queue.push(QueueEntry(0.0, start));

    while let Some(QueueEntry(distance, node_id)) = queue.pop() {
        if distance > *distances.get(&node_id).unwrap_or(&f64::INFINITY) || distance > max_distance
        {
            continue;
        }

        if node_id == goal {
            return true;
        }

        let Some(node) = &graph.nodes[node_id] else {
            continue;
        };

        for &edge_id in &node.adj {
            // Do not "explain" the turn by traversing the two candidate edges
            // themselves.
            if edge_id == incoming || edge_id == outgoing {
                continue;
            }

            let Some(edge) = &graph.edges[edge_id] else {
                continue;
            };

            if !edge.lines.iter().any(|occ| occ.line == line) {
                continue;
            }

            let next = if edge.a == node_id { edge.b } else { edge.a };
            let next_distance = distance + polyline_len(&edge.geom);

            if next_distance < *distances.get(&next).unwrap_or(&f64::INFINITY) {
                distances.insert(next, next_distance);
                queue.push(QueueEntry(next_distance, next));
            }
        }
    }

    false
}

fn insert_stations(occurrences: &[StationOcc], graph: &mut Graph, cfg: &TopoConfig) {
    for occurrence in occurrences {
        if occurrence.stops.is_empty() {
            continue;
        }

        let mut remaining = occurrence.clone();

        // LOOM's StatInserter permits multiple insertions for a station cluster
        // when one candidate cannot serve all original edges/lines.
        for _ in 0..3 {
            let mut best: Option<(f64, usize, f64, BTreeSet<usize>, BTreeSet<LineId>)> = None;

            for edge in graph.edges.iter().filter_map(Option::as_ref) {
                let (_, position, distance) =
                    project_on_polyline(remaining.stops[0].pos, &edge.geom);

                if distance > 4.0 * cfg.max_aggr_distance {
                    continue;
                }

                let served_originals: BTreeSet<_> = edge
                    .originals
                    .intersection(&remaining.originals)
                    .copied()
                    .collect();

                let edge_lines: BTreeSet<_> = edge.lines.iter().map(|occ| occ.line).collect();

                let served_lines: BTreeSet<_> =
                    edge_lines.intersection(&remaining.lines).copied().collect();

                let mut score = distance;

                if !remaining.originals.is_empty() {
                    score += (remaining.originals.len() - served_originals.len()) as f64
                        / remaining.originals.len() as f64
                        * 100.0;
                }

                if !remaining.lines.is_empty() {
                    score += (remaining.lines.len() - served_lines.len()) as f64
                        / remaining.lines.len() as f64
                        * 500.0;
                }

                let edge_length = polyline_len(&edge.geom);
                if position * edge_length < cfg.max_aggr_distance
                    || (1.0 - position) * edge_length < cfg.max_aggr_distance
                {
                    score += 200.0;
                }

                if best.as_ref().is_none_or(|candidate| score < candidate.0) {
                    best = Some((score, edge.id, position, served_originals, served_lines));
                }
            }

            let Some((_score, edge_id, position, served_originals, served_lines)) = best else {
                break;
            };

            if served_originals.is_empty() && served_lines.is_empty() {
                break;
            }

            let node_id = split_edge(graph, edge_id, position);
            let node = graph.nodes[node_id].as_mut().expect("split node exists");

            node.stops.push(remaining.stops[0].clone());

            for line in &remaining.lines {
                node.not_served.remove(line);
            }

            remaining.originals = remaining
                .originals
                .difference(&served_originals)
                .copied()
                .collect();

            remaining.lines = remaining.lines.difference(&served_lines).copied().collect();

            if remaining.originals.is_empty() && remaining.lines.is_empty() {
                break;
            }
        }
    }
}

fn split_edge(graph: &mut Graph, edge_id: usize, position: f64) -> usize {
    let edge = graph.edges[edge_id].clone().expect("edge exists");

    let left = subline(&edge.geom, 0.0, position);
    let right = subline(&edge.geom, position, 1.0);

    let fallback = lerp(
        graph.nodes[edge.a].as_ref().expect("node exists").pos,
        graph.nodes[edge.b].as_ref().expect("node exists").pos,
        position,
    );
    let split_point = left.last().copied().unwrap_or(fallback);

    graph.remove_edge(edge_id);
    let split_node = graph.add_node(split_point);

    for (a, b, geometry) in [(edge.a, split_node, left), (split_node, edge.b, right)] {
        let new_edge_id = graph.add_edge(a, b, geometry);
        let new_edge = graph.edges[new_edge_id]
            .as_mut()
            .expect("newly inserted edge");

        // StatInserter::split replaces the directional endpoint for each
        // half-edge. Retaining a direction to the old, non-adjacent node
        // corrupts directional connectivity after a station insertion.
        new_edge.lines = edge
            .lines
            .iter()
            .map(|occ| {
                let direction = match occ.direction {
                    None => None,
                    Some(n) if n == edge.b => Some(b),
                    Some(n) if n == edge.a => Some(a),
                    Some(n) => Some(n),
                };
                LineOcc {
                    line: occ.line,
                    direction,
                }
            })
            .collect();
        new_edge.originals = edge.originals.clone();
    }

    split_node
}
