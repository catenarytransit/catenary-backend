use std::collections::HashMap;

use crate::routing_common::profiles::*;
use crate::routing_common::types::*;
use crate::routing_common::ways::*;

/// Bucket-based priority queue for integer costs, mirroring OSR's dial.
///
/// O(1) insertion and amortized O(1) extraction minimum, ideal for
/// Dijkstra with bounded integer edge weights.
pub struct Dial {
    buckets: Vec<Vec<(NodeIdx, Level)>>,
    current: usize,
    size: usize,
}

impl Dial {
    pub fn new(n_buckets: usize) -> Self {
        Self {
            buckets: vec![Vec::new(); n_buckets],
            current: 0,
            size: 0,
        }
    }

    pub fn push(&mut self, cost: CostT, node: NodeIdx, level: Level) {
        let bucket = cost as usize;
        if bucket < self.buckets.len() {
            self.buckets[bucket].push((node, level));
            self.size += 1;
        }
    }

    pub fn pop(&mut self) -> Option<(CostT, NodeIdx, Level)> {
        while self.current < self.buckets.len() {
            if let Some((node, level)) = self.buckets[self.current].pop() {
                self.size -= 1;
                return Some((self.current as CostT, node, level));
            }
            self.current += 1;
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
        self.current = 0;
        self.size = 0;
    }
}

/// Entry in the cost map, tracking cost and predecessor.
#[derive(Clone)]
pub struct Entry {
    pub cost: CostT,
    pub pred_node: NodeIdx,
    pub pred_level: Level,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            cost: INFEASIBLE,
            pred_node: NodeIdx::INVALID,
            pred_level: Level::NO_LEVEL,
        }
    }
}

/// Result of a routing query.
#[derive(Debug, Clone)]
pub struct RoutePath {
    pub nodes: Vec<NodeIdx>,
    pub total_cost: CostT,
    pub total_distance: DistanceT,
}

/// Dijkstra router mirroring OSR's dijkstra struct.
pub struct DijkstraRouter {
    dial: Dial,
    cost: HashMap<(NodeIdx, Level), Entry>,
    neighbors_buf: Vec<Neighbor>,
    max_reached: bool,
}

impl DijkstraRouter {
    pub fn new(max_cost: CostT) -> Self {
        Self {
            dial: Dial::new(max_cost as usize + 1),
            cost: HashMap::new(),
            neighbors_buf: Vec::new(),
            max_reached: false,
        }
    }

    pub fn reset(&mut self) {
        self.dial.clear();
        self.cost.clear();
        self.max_reached = false;
    }

    pub fn add_start(&mut self, node: NodeIdx, level: Level) {
        let key = (node, level);
        self.cost.insert(
            key,
            Entry {
                cost: 0,
                pred_node: NodeIdx::INVALID,
                pred_level: Level::NO_LEVEL,
            },
        );
        self.dial.push(0, node, level);
    }

    pub fn get_cost(&self, node: NodeIdx, level: Level) -> CostT {
        self.cost.get(&(node, level)).map_or(INFEASIBLE, |e| e.cost)
    }

    pub fn run<P: RoutingProfile>(
        &mut self,
        params: &P::Params,
        graph: &RoutingGraph,
        max_cost: CostT,
        search_dir: Direction,
        destinations: Option<&[NodeIdx]>,
    ) -> bool {
        let mut remaining_dests: Option<Vec<NodeIdx>> = destinations.map(|d| d.to_vec());

        while let Some((cost, node, level)) = self.dial.pop() {
            if cost > self.get_cost(node, level) {
                continue;
            }

            // Early termination check
            if let Some(ref mut dests) = remaining_dests {
                dests.retain(|d| *d != node);
                if dests.is_empty() {
                    break;
                }
            }

            self.neighbors_buf.clear();
            P::adjacent(
                params,
                graph,
                node,
                level,
                search_dir,
                &mut self.neighbors_buf,
            );

            for neighbor in &self.neighbors_buf {
                let total = (cost as u64) + (neighbor.cost as u64);
                if total >= max_cost as u64 {
                    self.max_reached = true;
                    continue;
                }
                let total_cost = total as CostT;
                let key = (neighbor.node, neighbor.level);
                let entry = self.cost.entry(key).or_default();
                if total_cost < entry.cost {
                    entry.cost = total_cost;
                    entry.pred_node = node;
                    entry.pred_level = level;
                    self.dial.push(total_cost, neighbor.node, neighbor.level);
                }
            }
        }

        !self.max_reached
    }

    pub fn reconstruct_path(&self, dest: NodeIdx, dest_level: Level) -> Option<RoutePath> {
        let cost = self.get_cost(dest, dest_level);
        if cost == INFEASIBLE {
            return None;
        }

        let mut nodes = Vec::new();
        let mut current = dest;
        let mut current_level = dest_level;

        loop {
            nodes.push(current);
            let key = (current, current_level);
            match self.cost.get(&key) {
                Some(entry) if entry.pred_node.is_valid() => {
                    current = entry.pred_node;
                    current_level = entry.pred_level;
                }
                _ => break,
            }
        }

        nodes.reverse();

        Some(RoutePath {
            nodes,
            total_cost: cost,
            total_distance: 0,
        })
    }
}

/// High-level routing function.
///
/// Finds the shortest path between two nodes using the given profile.
pub fn route<P: RoutingProfile>(
    params: &P::Params,
    graph: &RoutingGraph,
    from: NodeIdx,
    to: NodeIdx,
    max_cost: CostT,
) -> Option<RoutePath> {
    let mut router = DijkstraRouter::new(max_cost);
    router.add_start(from, Level::NO_LEVEL);
    router.run::<P>(params, graph, max_cost, Direction::Forward, Some(&[to]));
    router.reconstruct_path(to, Level::NO_LEVEL)
}
