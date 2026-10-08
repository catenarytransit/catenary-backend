//! Transliteration of gtfs2graph/Builder::simplify, EdgePL::simplify,
//! EdgePL::combineIncludedGeoms and EdgePL::averageCombineGeom.
//!
//! Catenary stores compressed trip membership by direction pattern. This
//! adapter uses the corresponding cardinalities, but otherwise follows the
//! C++ operation order: geometry equality (10m), prune per route occurrence,
//! merge included shapes (50m), then UNWEIGHTED PolyLine::average (20m).
//! A canonical topological edge is keyed by unordered GTFS stop-node pair.
use crate::loom_graph::{add_line_occ, Graph, LineOcc, Point};
use crate::loom_polyline::{average, contains, equals, metric_length};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Clone)]
struct EdgeTripGeom {
    geometry: Vec<Point>,
    /// Equivalent to EdgeTripGeom::getTripsUnordered() RouteOccurance counts.
    /// These counts are derived from Catenary compressed pattern membership.
    counts: BTreeMap<usize, usize>,
    lines: BTreeSet<LineOcc>,
    originals: BTreeSet<usize>,
}

fn join(left: &mut EdgeTripGeom, right: EdgeTripGeom) {
    left.originals.extend(right.originals);
    for (line, n) in right.counts {
        *left.counts.entry(line).or_default() += n;
    }
    for occ in right.lines { add_line_occ(&mut left.lines, occ); }
}

fn grouped_edges(
    graph: &Graph,
    weights: &HashMap<usize, usize>,
    line_weights: &HashMap<(usize, usize), usize>,
) -> BTreeMap<(usize, usize), Vec<EdgeTripGeom>> {
    let mut candidates: BTreeMap<(usize, usize), Vec<EdgeTripGeom>> = BTreeMap::new();
    for edge in graph.edges.iter().flatten() {
        let key = (edge.a.min(edge.b), edge.a.max(edge.b));
        let mut shape = edge.geom.clone();
        if edge.a != key.0 { shape.reverse(); }
        let mut counts = BTreeMap::new();
        for occ in &edge.lines {
            counts.insert(occ.line,
                line_weights.get(&(edge.id, occ.line)).copied().unwrap_or_else(||
                    weights.get(&edge.id).copied().unwrap_or(1)));
        }
        candidates.entry(key).or_default().push(EdgeTripGeom {
            geometry: shape,
            counts,
            lines: edge.lines.clone(),
            originals: edge.originals.clone(),
        });
    }
    // C++ EdgePL::addTrip: group geometries if PolyLine::equals(10),
    // accumulating routes and averaging the incoming geometry into the ETG.
    for list in candidates.values_mut() {
        let source = std::mem::take(list);
        for item in source {
            if let Some(existing) = list.iter_mut().find(|other| equals(&other.geometry, &item.geometry, 10.0)) {
                existing.geometry = average(&[existing.geometry.clone(), item.geometry.clone()]);
                join(existing, item);
            } else {
                list.push(item);
            }
        }
    }
    candidates
}

/// Applies the C++ builder's simplification pipeline to the compact patterns.
/// The first edge ID for each stop pair survives; all old IDs are remapped in
/// allowed_turns before they are removed.
pub fn simplify(
    graph: &mut Graph,
    weights: &HashMap<usize, usize>,
    line_weights: &mut HashMap<(usize, usize), usize>,
    prune_threshold: f64,
) {
    let mut groups = grouped_edges(graph, weights, line_weights);
    // Builder::simplify() calculates the average before it calls EdgePL::simplify.
    let (total, number) = groups.values().flat_map(|xs| xs.iter()).flat_map(|e| e.counts.values())
        .fold((0usize, 0usize), |(sum, n), &v| (sum.saturating_add(v), n+1));
    let mean = if number == 0 { 0.0 } else { total as f64 / number as f64 };
    let cutoff = mean * prune_threshold.max(0.0);
    let mut remap: HashMap<usize, usize> = HashMap::new();
    let mut delete = Vec::new();
    for (pair, etgs) in groups.iter_mut() {
        // Remove rare route occurrences *inside each geometry* as in C++.
        for etg in etgs.iter_mut() {
            etg.counts.retain(|_, count| (*count as f64) >= cutoff);
            etg.lines.retain(|o| etg.counts.contains_key(&o.line));
        }
        etgs.retain(|e| !e.counts.is_empty());
        let mut ids: Vec<usize> = graph.edges.iter().flatten()
            .filter(|e| (e.a.min(e.b), e.a.max(e.b)) == *pair)
            .map(|e| e.id).collect();
        ids.sort_unstable();
        if ids.is_empty() { continue; }
        let reference = ids[0];
        for &id in ids.iter().skip(1) { remap.insert(id, reference); delete.push(id); }
        if etgs.is_empty() { delete.push(reference); continue; }

        // EdgePL::combineIncludedGeoms: a longer shape absorbs a shorter
        // contained shape only if the containment is NOT bidirectional.
        let mut idx = 0;
        while idx < etgs.len() {
            let mut absorber = None;
            for j in 0..etgs.len() {
                if j == idx { continue; }
                if metric_length(&etgs[j].geometry) > metric_length(&etgs[idx].geometry)
                    && contains(&etgs[j].geometry, &etgs[idx].geometry, 50.0)
                    && !contains(&etgs[idx].geometry, &etgs[j].geometry, 50.0)
                {
                    absorber = Some(j);
                    break;
                }
            }
            if let Some(j) = absorber {
                let item = etgs.remove(idx);
                let target = if j > idx { j-1 } else { j };
                join(&mut etgs[target], item);
            } else { idx += 1; }
        }
        // EdgePL::averageCombineGeom: surviving ETGs get EQUAL weights,
        // regardless of trip cardinality, exactly as in the C++ overload.
        let geometry = average(&etgs.iter().map(|e| e.geometry.clone()).collect::<Vec<_>>());
        let mut lines = BTreeSet::new();
        let mut originals = BTreeSet::new();
        for e in etgs.iter() {
            originals.extend(e.originals.iter().copied());
            for &occ in &e.lines { add_line_occ(&mut lines, occ); }
        }
        let edge = graph.edges[reference].as_mut().unwrap();
        if edge.a == pair.0 { edge.geom = geometry; }
        else { edge.geom = geometry.into_iter().rev().collect(); }
        edge.originals = originals;
        edge.lines = lines;
    }
    for id in delete { graph.remove_edge(id); }
    let alive: HashSet<usize> = graph.edges.iter().flatten().map(|e| e.id).collect();
    for node in graph.nodes.iter_mut().flatten() {
        for turns in node.allowed_turns.values_mut() {
            *turns = turns.iter().map(|&(a, b)| {
                (*remap.get(&a).unwrap_or(&a), *remap.get(&b).unwrap_or(&b))
            }).filter(|(a,b)| alive.contains(a) && alive.contains(b))
                .collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(x: f64, y: f64) -> Point { Point { lon: x, lat: y } }
    #[test]
    fn contained_geom_does_not_survive_as_parallel_edge() {
        let mut g = Graph::default();
        let a = g.add_node(p(0.0, 0.0));
        let b = g.add_node(p(0.002, 0.0));
        let short = g.add_edge(a,b,vec![p(0.0005,0.0),p(0.0015,0.0)]);
        let long = g.add_edge(a,b,vec![p(0.0,0.0),p(0.002,0.0)]);
        for id in [short,long] {
            g.edges[id].as_mut().unwrap().lines.insert(LineOcc { line:0, direction:Some(b) });
            g.edges[id].as_mut().unwrap().originals.insert(id);
        }
        let weights = HashMap::from([(short,1),(long,5)]);
        let mut lw = HashMap::from([((short,0),1),((long,0),5)]);
        simplify(&mut g,&weights,&mut lw,0.0);
        assert_eq!(g.edges.iter().flatten().count(),1);
        let e = g.edges.iter().flatten().next().unwrap();
        assert!(metric_length(&e.geom)>180.0);
        assert!(e.originals.contains(&short) && e.originals.contains(&long));
    }
}
