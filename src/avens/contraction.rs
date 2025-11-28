use anyhow::Result;
use catenary::routing_common::osm_graph::{
    self, ContractionHierarchy, Edge, Node, Shortcut, StreetData,
};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use ahash::AHashMap as HashMap;

/// Builder for Contraction Hierarchies.
///
/// This struct manages the state required to contract the graph,
/// including tracking edge differences, shortcuts, and node ordering.
pub struct ContractedGraphBuilder<'a> {
    graph: &'a StreetData,
    /// Maps Node Index (0..N) to its Rank (0..N).
    /// Rank 0 = First node contracted (lowest importance).
    /// Rank N-1 = Last node contracted (highest importance).
    node_ranks: Vec<u32>,
    /// Adjacency list for the *augmented* graph (original edges + shortcuts).
    /// stored as: node_idx -> vec![(target_idx, weight, is_shortcut, original_edge_idx/shortcut_idx)]
    augmented_edges: Vec<Vec<AugmentedEdge>>,
    /// Current "contracted" state of nodes. True if node is already contracted.
    is_contracted: Vec<bool>,
}

struct ContractionWorkspace {
    dists: HashMap<u32, u32>,
    pq: BinaryHeap<Reverse<(u32, u32)>>,
    shortcuts: Vec<(u32, AugmentedEdge)>,
}

#[derive(Clone, Debug)]
struct AugmentedEdge {
    target: u32,
    weight: u32,
    /// If true, this is a shortcut. If false, it's an original edge.
    is_shortcut: bool,
    /// If is_shortcut is true, this is the index in `self.shortcuts`.
    /// If is_shortcut is false, this is the index in `self.graph.edges`.
    original_index: u32,
    /// For shortcuts: the middle node.
    skipped_node: u32,
    /// For shortcuts: original first edge (from source)
    original_first_edge: u32,
    /// For shortcuts: original second edge (into target)
    original_second_edge: u32,
}

impl<'a> ContractedGraphBuilder<'a> {
    pub fn new(graph: &'a StreetData) -> Self {
        let num_nodes = graph.nodes.len();
        let mut augmented_edges = vec![Vec::new(); num_nodes];

        // Initialize augmented edges with original graph edges
        // We need to iterate all nodes to populate the adjacency list
        for (u_idx, node) in graph.nodes.iter().enumerate() {
            let start_edge = node.first_edge_idx as usize;
            let end_edge = if u_idx + 1 < num_nodes {
                graph.nodes[u_idx + 1].first_edge_idx as usize
            } else {
                graph.edges.len()
            };

            for (i, edge) in graph.edges[start_edge..end_edge].iter().enumerate() {
                // Calculate weight based on mode (TODO: Pass mode/profile)
                // For now, assume walking: weight = distance / speed

                let flags = edge.flags;
                let road_class =
                    (flags & catenary::routing_common::osm_graph::edge_flags::ROAD_CLASS_MASK);
                let has_sidewalk = (flags
                    & catenary::routing_common::osm_graph::edge_flags::EDGE_FLAG_HAS_SIDEWALK)
                    != 0;
                let is_crossing = (flags
                    & catenary::routing_common::osm_graph::edge_flags::EDGE_FLAG_CROSSING)
                    != 0;

                // Base Speed: 5 km/h = 1.4 m/s
                // Penalty: Reduce speed for hostile roads
                let mut speed_kmh = 5.0;

                use catenary::routing_common::osm_graph::edge_flags::*;

                if !has_sidewalk {
                    match road_class {
                        CLASS_MOTORWAY | CLASS_TRUNK => speed_kmh = 1.0, // Very hostile
                        CLASS_PRIMARY => speed_kmh = 2.0,                // Hostile
                        CLASS_SECONDARY => speed_kmh = 3.0,              // Unpleasant
                        CLASS_TERTIARY => speed_kmh = 4.0,               // Minor penalty
                        _ => {}                                          // Residential/Path is fine
                    }
                }

                // Speed (m/s) = speed_kmh / 3.6
                // Time (s) = (dist_mm / 1000) / (speed_kmh / 3.6)
                //          = dist_mm * 3.6 / (1000 * speed_kmh)
                //          = dist_mm * 0.0036 / speed_kmh
                //          = dist_mm * 36 / (10000 * speed_kmh)

                // Let's use integer math to avoid float issues if possible, but float is fine for precalc.
                let mut time_seconds = (edge.distance_mm as f64 * 0.0036 / speed_kmh) as u32;

                // Apply Crossing Penalty (Edge)
                if is_crossing {
                    // Only apply penalty if it crosses a ROAD (Residential or higher).
                    // If it connects only PATHs (Footway, Cycleway), no penalty.
                    // We check the edges connected to u and v.
                    let u_edges = Self::get_edges_from_node(graph, u_idx);
                    let v_edges = Self::get_edges_from_node(graph, edge.target_node as usize);

                    let connects_to_road = u_edges.iter().chain(v_edges.iter()).any(|e| {
                        let cls = e.flags & ROAD_CLASS_MASK;
                        cls >= CLASS_RESIDENTIAL
                    });

                    if connects_to_road {
                        time_seconds += 30; // 30 seconds penalty for crossing road
                    }
                }

                let weight = time_seconds;

                augmented_edges[u_idx].push(AugmentedEdge {
                    target: edge.target_node,
                    weight,
                    is_shortcut: false,
                    original_index: (start_edge + i) as u32,
                    skipped_node: u32::MAX,
                    original_first_edge: u32::MAX,
                    original_second_edge: u32::MAX,
                });
            }
        }

        Self {
            graph,
            node_ranks: vec![u32::MAX; num_nodes], // u32::MAX indicates not yet ranked
            augmented_edges,
            is_contracted: vec![false; num_nodes],
        }
    }

    pub fn contract(&mut self) -> ContractionHierarchy {
        let num_nodes = self.graph.nodes.len();
        println!("Starting contraction for {} nodes...", num_nodes);

        // 1. Calculate initial importance for all nodes
        // Priority Queue stores (Importance, NodeID). Min-heap (lowest importance first).
        // Importance = Edge Difference + Contracted Neighbors + ...
        let mut pq = BinaryHeap::new();

        // Workspace for reuse
        let mut workspace = ContractionWorkspace {
            dists: HashMap::new(),
            pq: BinaryHeap::new(),
            shortcuts: Vec::new(),
        };

        for i in 0..num_nodes {
            let importance = self.calculate_importance(i as u32, &mut workspace);
            pq.push(Reverse((importance, i as u32)));
        }

        let mut rank = 0;

        // 2. Contract nodes in order
        while let Some(Reverse((expected_importance, u))) = pq.pop() {
            if self.is_contracted[u as usize] {
                continue;
            }

            // Lazy update: re-calculate importance. If it's higher, push back and skip.
            let current_importance = self.calculate_importance(u, &mut workspace);
            if current_importance > expected_importance {
                pq.push(Reverse((current_importance, u)));
                continue;
            }

            // Contract the node!
            self.contract_node(u, rank, &mut workspace);
            rank += 1;

            if rank % 1000 == 0 {
                println!("Contracted {}/{} nodes", rank, num_nodes);
            }

            // Update neighbors?
            // In a full implementation, we might want to trigger updates for neighbors here,
            // but the lazy update strategy in the loop handles it eventually.
        }

        // 3. Build result
        self.build_result()
    }

    fn calculate_importance(&self, u: u32, workspace: &mut ContractionWorkspace) -> i32 {
        // Simple heuristic: Edge Difference
        // ED = (Number of shortcuts introduced) - (Number of incoming edges + Number of outgoing edges)
        // We simulate contraction to count shortcuts.

        self.simulate_contraction(u, workspace);
        let shortcuts_len = workspace.shortcuts.len();

        // Count incident edges (degree)
        // Since graph is undirected for walking usually, in/out degree is roughly same.
        // But let's be precise.
        // We only care about neighbors that are NOT contracted yet.
        let mut degree = 0;
        for edge in &self.augmented_edges[u as usize] {
            if !self.is_contracted[edge.target as usize] {
                degree += 1;
            }
        }

        // Note: For directed graphs, we'd need incoming edges too.
        // Our StreetData is technically directed storage, but represents bidirectional streets usually.
        // We'll assume symmetric for now or just use out-degree as proxy.

        let edge_difference = (shortcuts_len as i32) - (degree as i32);

        // Add other heuristics if needed (e.g. depth, contracted neighbors)
        edge_difference
    }

    /// Simulates contracting node `u` and populates `workspace.shortcuts` with the shortcuts that would be created.
    fn simulate_contraction(&self, u: u32, workspace: &mut ContractionWorkspace) {
        workspace.shortcuts.clear();

        // Identify neighbors
        // Incoming: v -> u
        // Outgoing: u -> w
        // Since we are using an adjacency list, finding incoming is expensive O(E).
        // However, for walking/cycling, edges are usually bidirectional (u->v AND v->u exist).
        // So we can approximate incoming neighbors as outgoing neighbors.
        // TODO: Handle one-way streets correctly.

        let neighbors: Vec<&AugmentedEdge> = self.augmented_edges[u as usize]
            .iter()
            .filter(|e| !self.is_contracted[e.target as usize])
            .collect();

        // For every pair of neighbors (v, w), check if path v->u->w is a shortest path.
        // If it is, we need a shortcut v->w.

        // Limit the search space for performance (Local Dijkstra)
        let max_settled_nodes = 20;

        for in_edge in &neighbors {
            let v = in_edge.target; // Actually this is u->v. If bidirectional, v->u exists.
            // Let's assume bidirectional for now and verify later.
            // v -> u cost is in_edge.weight

            for out_edge in &neighbors {
                let w = out_edge.target;
                if v == w {
                    continue;
                }

                let cost_vu = in_edge.weight; // Assuming symmetric weight
                let cost_uw = out_edge.weight;
                let mut cost_via_u = cost_vu + cost_uw;

                // Apply Node Penalty (Crossing)
                let u_flags = self.graph.nodes[u as usize].flags;
                if (u_flags & catenary::routing_common::osm_graph::node_flags::NODE_FLAG_CROSSING)
                    != 0
                {
                    // Only apply penalty if the node connects to a ROAD.
                    let u_edges = Self::get_edges_from_node(self.graph, u as usize);
                    let connects_to_road = u_edges.iter().any(|e| {
                        let cls = e.flags
                            & catenary::routing_common::osm_graph::edge_flags::ROAD_CLASS_MASK;
                        cls >= catenary::routing_common::osm_graph::edge_flags::CLASS_RESIDENTIAL
                    });

                    if connects_to_road {
                        cost_via_u += 30; // 30 seconds penalty for crossing road
                    }
                }

                // Check if there is a path v->...->w that is shorter or equal to cost_via_u
                // WITHOUT going through u.
                if self.is_witness_path_shorter(v, w, u, cost_via_u, max_settled_nodes, workspace) {
                    // Witness exists, no shortcut needed
                } else {
                    // No witness, need shortcut
                    workspace.shortcuts.push((
                        v,
                        AugmentedEdge {
                            target: w,
                            weight: cost_via_u,
                            is_shortcut: true,
                            original_index: 0, // Placeholder
                            skipped_node: u,
                            original_first_edge: in_edge.original_index, // This is u->v index. We need v->u index ideally.
                            original_second_edge: out_edge.original_index,
                        },
                    ));
                }
            }
        }
    }

    /// Returns true if there is a path from `source` to `target` with length <= `limit_cost`
    /// that does NOT visit `avoid_node`.
    fn is_witness_path_shorter(
        &self,
        source: u32,
        target: u32,
        avoid_node: u32,
        limit_cost: u32,
        max_settled: usize,
        workspace: &mut ContractionWorkspace,
    ) -> bool {
        workspace.dists.clear();
        workspace.pq.clear();

        workspace.dists.insert(source, 0);
        workspace.pq.push(Reverse((0, source)));

        let mut settled_count = 0;

        while let Some(Reverse((d, u))) = workspace.pq.pop() {
            if d > limit_cost {
                return false;
            }
            if u == target {
                return true; // Found a path <= limit_cost
            }
            if settled_count >= max_settled {
                return false; // Search limit reached, assume shortcut needed (conservative)
            }

            // Lazy check
            if let Some(&best) = workspace.dists.get(&u) {
                if d > best {
                    continue;
                }
            }

            settled_count += 1;

            for edge in &self.augmented_edges[u as usize] {
                if self.is_contracted[edge.target as usize] || edge.target == avoid_node {
                    continue;
                }

                let new_dist = d + edge.weight;
                if new_dist <= limit_cost {
                    if new_dist < *workspace.dists.get(&edge.target).unwrap_or(&u32::MAX) {
                        workspace.dists.insert(edge.target, new_dist);
                        workspace.pq.push(Reverse((new_dist, edge.target)));
                    }
                }
            }
        }

        false
    }

    fn contract_node(&mut self, u: u32, rank: u32, workspace: &mut ContractionWorkspace) {
        self.is_contracted[u as usize] = true;
        self.node_ranks[u as usize] = rank;

        // Real contraction: generate shortcuts and add them to the graph
        self.simulate_contraction(u, workspace);

        for (source, sc) in workspace.shortcuts.drain(..) {
            // Add to augmented graph so neighbors can use it
            // `AugmentedEdge` is what we use for graph traversal.
            // `Shortcut` is what we save to disk.
            // Let's add to augmented edges first.
            self.augmented_edges[source as usize].push(sc);
        }
    }

    fn build_result(&self) -> ContractionHierarchy {
        // We need to gather all shortcuts from `augmented_edges` that are marked as `is_shortcut`.
        // And sort them by source node to build the index.

        let mut all_shortcuts: Vec<(u32, Shortcut)> = Vec::new();

        for (u, edges) in self.augmented_edges.iter().enumerate() {
            for edge in edges {
                if edge.is_shortcut {
                    all_shortcuts.push((
                        u as u32,
                        Shortcut {
                            target_node: edge.target,
                            weight: edge.weight,
                            skipped_node: edge.skipped_node,
                            original_first_edge: edge.original_first_edge,
                            original_second_edge: edge.original_second_edge,
                        },
                    ));
                }
            }
        }

        // Sort by source node (should be already sorted by virtue of iteration order, but let's be safe)
        // Actually, iteration order `for (u, ...)` guarantees source order.
        // So `all_shortcuts` is already sorted by source!

        let mut final_shortcuts = Vec::new();
        let mut shortcut_index = vec![0; self.graph.nodes.len() + 1];

        let mut current_idx = 0;
        for (i, (source, sc)) in all_shortcuts.iter().enumerate() {
            // Fill index gaps
            while current_idx <= *source {
                shortcut_index[current_idx as usize] = i as u32;
                current_idx += 1;
            }
            final_shortcuts.push(sc.clone());
        }
        // Fill remaining
        while current_idx < shortcut_index.len() as u32 {
            shortcut_index[current_idx as usize] = final_shortcuts.len() as u32;
            current_idx += 1;
        }

        ContractionHierarchy {
            node_ranks: self.node_ranks.clone(),
            shortcuts: final_shortcuts,
            shortcut_index,
        }
    }

    fn get_edges_from_node(graph: &StreetData, node_idx: usize) -> &[Edge] {
        let start = graph.nodes[node_idx].first_edge_idx as usize;
        let end = if node_idx + 1 < graph.nodes.len() {
            graph.nodes[node_idx + 1].first_edge_idx as usize
        } else {
            graph.edges.len()
        };
        &graph.edges[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::routing_common::osm_graph::{Edge, Geometry, Node, StreetData};

    #[test]
    fn test_simple_contraction() {
        // Create a graph: 3-0-1-2-4
        // Node 1 is the bridge.
        // We want 1 to be contracted before 0 and 2.
        // To ensure this, we make 0 and 2 have higher degree.
        // Add 5 connected to 0.
        // Add 6 connected to 2.
        // Graph:
        // 5-0-1-2-6
        //   |   |
        //   3   4

        // Nodes: 0,1,2,3,4,5,6
        // Edges:
        // 0-1, 1-0
        // 1-2, 2-1
        // 0-3, 3-0
        // 0-5, 5-0
        // 2-4, 4-2
        // 2-6, 6-2

        let mut nodes = Vec::new();
        for _ in 0..7 {
            nodes.push(Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 0,
                flags: 0,
            });
        }

        let mut edges = Vec::new();

        // Helper to add bidir edge
        let mut add_edge = |u: usize, v: usize| {
            edges.push(Edge {
                target_node: v as u32,
                distance_mm: 14000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: 0,
            });
        };

        // 0 connected to 1, 3, 5
        add_edge(0, 1);
        add_edge(0, 3);
        add_edge(0, 5);
        // 1 connected to 0, 2
        add_edge(1, 0);
        add_edge(1, 2);
        // 2 connected to 1, 4, 6
        add_edge(2, 1);
        add_edge(2, 4);
        add_edge(2, 6);
        // 3 connected to 0
        add_edge(3, 0);
        // 4 connected to 2
        add_edge(4, 2);
        // 5 connected to 0
        add_edge(5, 0);
        // 6 connected to 2
        add_edge(6, 2); // Corrected: 6 connected to 2

        // Fix edges list construction to match `first_edge_idx` logic
        // We need to group edges by source node.

        let adj = vec![
            vec![1, 3, 5], // 0
            vec![0, 2],    // 1
            vec![1, 4, 6], // 2
            vec![0],       // 3
            vec![2],       // 4
            vec![0],       // 5
            vec![2],       // 6
        ];

        let mut flat_edges = Vec::new();
        for (u, targets) in adj.iter().enumerate() {
            nodes[u].first_edge_idx = flat_edges.len() as u32;
            for &v in targets {
                flat_edges.push(Edge {
                    target_node: v as u32,
                    distance_mm: 14000,
                    geometry_id: 0,
                    permissions: 0,
                    surface_type: 0,
                    grade_percent: 0,
                    flags: 0,
                });
            }
        }

        let street_data = StreetData {
            nodes,
            edges: flat_edges,
            geometries: vec![],
            partition_id: 0,
            boundary_nodes: vec![],
        };

        let mut builder = ContractedGraphBuilder::new(&street_data);
        let ch = builder.contract();

        println!("Shortcuts: {:?}", ch.shortcuts);

        // Now 1 should be contracted early.
        // Neighbors of 1 are 0 and 2.
        // Shortcut 0->2 should be created.

        assert!(!ch.shortcuts.is_empty(), "Should have generated shortcuts");

        let has_shortcut = ch
            .shortcuts
            .iter()
            .any(|s| s.target_node == 2 && s.weight == 20);
        assert!(has_shortcut, "Should have shortcut 0->2");
    }

    #[test]
    fn test_penalty_logic() {
        // Test that penalties are applied correctly.
        // Node 0 -> 1: Motorway (Class 5), No Sidewalk -> Speed 1.0 km/h
        // Node 1 -> 2: Residential (Class 0), No Sidewalk -> Speed 5.0 km/h
        // Distance 100m = 100,000mm

        // Time 0->1: 100000 * 0.0036 / 1.0 = 360 seconds
        // Time 1->2: 100000 * 0.0036 / 5.0 = 72 seconds

        use catenary::routing_common::osm_graph::edge_flags::*;

        let nodes = vec![
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 0,
                flags: 0,
            },
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 1,
                flags: 0,
            },
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 2,
                flags: 0,
            },
        ];

        let edges = vec![
            // 0 -> 1 (Motorway)
            Edge {
                target_node: 1,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: CLASS_MOTORWAY,
            },
            // 1 -> 2 (Residential)
            Edge {
                target_node: 2,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: CLASS_RESIDENTIAL,
            },
        ];

        let street_data = StreetData {
            nodes,
            edges,
            geometries: vec![],
            partition_id: 0,
            boundary_nodes: vec![],
        };

        let builder = ContractedGraphBuilder::new(&street_data);

        // Check augmented edges weights
        let edges_0 = &builder.augmented_edges[0];
        assert_eq!(edges_0.len(), 1);
        assert_eq!(
            edges_0[0].weight, 360,
            "Motorway should have high penalty (360s)"
        );

        let edges_1 = &builder.augmented_edges[1];
        assert_eq!(edges_1.len(), 1);
        assert_eq!(
            edges_1[0].weight, 72,
            "Residential should have normal speed (72s)"
        );
    }

    #[test]
    fn test_crossing_penalties() {
        // Test penalties for crossings.
        // Case 1: Path crossing Path (e.g. Footway crossing Cycleway) -> No Penalty.
        // Case 2: Path crossing Road (e.g. Footway crossing Residential) -> Penalty.

        use catenary::routing_common::osm_graph::{edge_flags::*, node_flags::*};

        // Nodes:
        // 0 -> 1 (Crossing Edge) -> 2 (Path)
        // 3 -> 4 (Crossing Edge) -> 5 (Road)

        let nodes = vec![
            // Case 1: Path-Path
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 0,
                flags: 0,
            },
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 1,
                flags: NODE_FLAG_CROSSING,
            },
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 3,
                flags: 0,
            },
            // Case 2: Path-Road
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 4,
                flags: 0,
            },
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 5,
                flags: NODE_FLAG_CROSSING,
            },
            Node {
                lat: 0.0,
                lon: 0.0,
                elevation: 0.0,
                first_edge_idx: 7,
                flags: 0,
            },
        ];

        let edges = vec![
            // Case 1: Path-Path
            // 0 -> 1 (Edge Crossing, CLASS_PATH)
            Edge {
                target_node: 1,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: EDGE_FLAG_CROSSING | CLASS_PATH,
            },
            // 1 -> 0
            Edge {
                target_node: 0,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: EDGE_FLAG_CROSSING | CLASS_PATH,
            },
            // 1 -> 2 (Path)
            Edge {
                target_node: 2,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: CLASS_PATH,
            },
            // 2 -> 1
            Edge {
                target_node: 1,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: CLASS_PATH,
            },
            // Case 2: Path-Road
            // 3 -> 4 (Edge Crossing, CLASS_PATH)
            Edge {
                target_node: 4,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: EDGE_FLAG_CROSSING | CLASS_PATH,
            },
            // 4 -> 3
            Edge {
                target_node: 3,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: EDGE_FLAG_CROSSING | CLASS_PATH,
            },
            // 4 -> 5 (Road)
            Edge {
                target_node: 5,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: CLASS_RESIDENTIAL,
            },
            // 5 -> 4
            Edge {
                target_node: 4,
                distance_mm: 100_000,
                geometry_id: 0,
                permissions: 0,
                surface_type: 0,
                grade_percent: 0,
                flags: CLASS_RESIDENTIAL,
            },
        ];

        let street_data = StreetData {
            nodes,
            edges,
            geometries: vec![],
            partition_id: 0,
            boundary_nodes: vec![],
        };

        let mut builder = ContractedGraphBuilder::new(&street_data);

        // Case 1: Path-Path
        // Edge 0->1 should NOT have penalty (connects to Path at 1)
        let edges_0 = &builder.augmented_edges[0];
        assert_eq!(
            edges_0[0].weight, 72,
            "Path-Path crossing edge should have NO penalty"
        );

        // Create workspace for tests
        let mut workspace = ContractionWorkspace {
            dists: HashMap::new(),
            pq: BinaryHeap::new(),
            shortcuts: Vec::new(),
        };

        // Node 1 contraction (Path-Path)
        builder.simulate_contraction(1, &mut workspace);
        // Should have shortcut 0->2 with weight 72 + 72 = 144 (No node penalty)
        let sc_0_2 = workspace.shortcuts
            .iter()
            .find(|s| s.1.target == 2)
            .expect("Shortcut 0->2");
        assert_eq!(
            sc_0_2.1.weight,
            72 + 72,
            "Path-Path crossing node should have NO penalty"
        );

        // Case 2: Path-Road
        // Edge 3->4 should have penalty (connects to Road at 4)
        let edges_3 = &builder.augmented_edges[3];
        assert_eq!(
            edges_3[0].weight,
            72 + 30,
            "Path-Road crossing edge should have penalty"
        );

        // Node 4 contraction (Path-Road)
        builder.simulate_contraction(4, &mut workspace);
        // Should have shortcut 3->5 with weight (72+30) + 72 + 30 (node penalty) = 204
        let sc_3_5 = workspace.shortcuts
            .iter()
            .find(|s| s.1.target == 5)
            .expect("Shortcut 3->5");
        assert_eq!(
            sc_3_5.1.weight,
            (72 + 30) + 72 + 30,
            "Path-Road crossing node should have penalty"
        );
    }
}
