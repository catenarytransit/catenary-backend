use crate::graph::{Edge, LineOnEdge, Node, RenderGraph};
use good_lp::{
    Expression, IntoAffineExpression, ProblemVariables, Solution, SolverModel, Variable, variable,
};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::f64::consts::PI;

pub struct Optimizer {
    // Weights for the objective function
    weight_consistency: f64,
    weight_split: f64,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            weight_consistency: 40.0,
            weight_split: 10.0,
        }
    }

    pub fn optimize(&self, graph: &mut RenderGraph) {
        println!("Starting Decomposed ILP Line Ordering Optimization...");

        // 1. Identify "Active" edges (those with >= 2 lines)
        // Only these edges need sorting variables.
        let active_edges: Vec<usize> = graph
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| e.lines.len() >= 2)
            .map(|(i, _)| i)
            .collect();

        if active_edges.is_empty() {
            println!("No edges with multiple lines to optimize.");
            return;
        }

        // Simplify Graph Iteratively
        println!("Starting Iterative Simplification...");

        // Pruning Rule 2: Line Partner Collapse
        // According to Algorithm 4.3, this is run ONCE before the loop.
        self.prune_rule_2_line_partner_collapse(graph);

        let mut round = 0;
        loop {
            round += 1;
            if round > 50 {
                // Max rounds safety (Paper says M rounds sufficient)
                break;
            }
            let mut changed = false;

            // 1. Complex Untangling Rules (matches C++ order: DoubleStump → OuterStump → FullX → Y → PartialY → DogBone → PartialDogBone)
            // Full X is now integrated here matching C++ loom's approach
            if self.untangle_complex_repeatedly(graph) {
                changed = true;
            }

            // 3. Cutting Rule 1: Single Line Cut
            if self.cutting_rule_1_single_line_cut(graph) {
                changed = true;
            }

            // 4. Cutting Rule 2: Terminus Detachment
            // Loop to exhaust since implementation breaks early
            while self.cutting_rule_2_terminus_detachment(graph) {
                changed = true;
            }

            // 5. Pruning Rule 1: Node Contraction
            if self.prune_rule_1_node_contraction(graph) {
                changed = true;
            }

            if !changed {
                break;
            }
        }
        println!("Simplification Complete after {} rounds.", round);

        // Re-calculate active edges after simplification
        let active_edges: Vec<usize> = graph
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| e.lines.len() >= 2)
            .map(|(i, _)| i)
            .collect();

        // 2. Build Dependency Graph between active edges
        // Two active edges are coupled if they share a node AND share >= 2 lines (Consistency Constraint).
        // (Split constraints are local to the splitting edge, they depend on neighbors but don't couple variables of neighbors unless they are also active and sharing lines).
        // Actually, if E1 splits into E2 and E3. E1 is active. E2, E3 might be active or not.
        // If E2 is active (e.g. has lines A, C), and E1 has (A, B).
        // If E1 and E2 share >= 2 lines, they are coupled.
        // If they share only 1 line, their ordering variables are independent (A vs B on E1, A vs C on E2).
        // So strict coupling condition: |lines(e1) intersect lines(e2)| >= 2.

        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
        // Initialize adjacency for all active edges
        for &idx in &active_edges {
            adj.entry(idx).or_default();
        }

        // Map Node -> List of Active Edges connected to it
        let mut node_to_active: HashMap<i64, Vec<usize>> = HashMap::new();
        for &idx in &active_edges {
            node_to_active
                .entry(graph.edges[idx].from)
                .or_default()
                .push(idx);
            node_to_active
                .entry(graph.edges[idx].to)
                .or_default()
                .push(idx);
        }

        for edges_at_node in node_to_active.values() {
            for i in 0..edges_at_node.len() {
                for j in (i + 1)..edges_at_node.len() {
                    let u = edges_at_node[i];
                    let v = edges_at_node[j];

                    // Check intersection of lines
                    // Optimization: Use Sets if lines are many, but usually small.
                    let u_lines = &graph.edges[u].lines;
                    let v_lines = &graph.edges[v].lines;

                    let shared_count = u_lines
                        .iter()
                        .filter(|l1| {
                            // Restriction Check
                            let key1 = (l1.chateau_id.clone(), l1.route_id.clone());
                            if let Some(r) = graph.restrictions.get(&(u, v)) {
                                if r.contains(&key1) {
                                    return false;
                                }
                            }
                            if let Some(r) = graph.restrictions.get(&(v, u)) {
                                if r.contains(&key1) {
                                    return false;
                                }
                            }
                            v_lines.iter().any(|l2| l2.line_id == l1.line_id)
                        })
                        .count();

                    if shared_count >= 2 {
                        adj.entry(u).or_default().push(v);
                        adj.entry(v).or_default().push(u);
                    }
                }
            }
        }

        // 3. Find Connected Components
        let mut visited: HashSet<usize> = HashSet::new();
        let mut components: Vec<Vec<usize>> = Vec::new();

        for &start_node in &active_edges {
            if visited.contains(&start_node) {
                continue;
            }

            let mut component = Vec::new();
            let mut queue = VecDeque::new();
            queue.push_back(start_node);
            visited.insert(start_node);

            while let Some(u) = queue.pop_front() {
                component.push(u);
                if let Some(neighbors) = adj.get(&u) {
                    for &v in neighbors {
                        if !visited.contains(&v) {
                            visited.insert(v);
                            queue.push_back(v);
                        }
                    }
                }
            }

            // Sub-divide component using Articulation Points to "break problems up more"
            // Lowered threshold from 100 to 40 for better subdivisions
            if component.len() > 40 {
                let sub_components = self.split_by_articulation_points(&component, &adj);
                components.extend(sub_components);
            } else {
                components.push(component);
            }
        }

        println!(
            "Identified {} independent components for optimization.",
            components.len()
        );

        // 4. Solve each component in PARALLEL
        // Pre-compute global node map for geometry lookups (read-only)
        let mut node_to_all_edges: HashMap<i64, Vec<usize>> = HashMap::new();
        for (idx, edge) in graph.edges.iter().enumerate() {
            node_to_all_edges.entry(edge.from).or_default().push(idx);
            node_to_all_edges.entry(edge.to).or_default().push(idx);
        }

        // 4. Solve each component SEQUENTIALLY
        // We iterate sequentially to avoid OOM by running too many huge ILP problems at once.
        let mut global_results: HashMap<usize, Vec<LineOnEdge>> = HashMap::new();
        let total_components = components.len();

        println!(
            "Starting optimization of {} components...",
            total_components
        );

        for (i, component) in components.iter().enumerate() {
            let size = component.len();

            // Pruning Rule 3: Single Edge Prune (Trivial Component)
            // If component is just 1 edge, we can sort arbitrarily (e.g. by ID) as there are no crossings.
            if size == 1 {
                let edge_idx = component[0];
                let mut lines = graph.edges[edge_idx].lines.clone();
                lines.sort_by(|a, b| a.line_id.cmp(&b.line_id));
                global_results.insert(edge_idx, lines);
                continue;
            }

            // Calculate solution space size like C++ loom does:
            // solutionSpaceSize = Π factorial(lines_per_edge)
            // C++ uses ExhaustiveOptimizer when solutionSpaceSize < 500
            // We use greedy ordering for large solution spaces to avoid ILP OOM
            let solution_space: f64 = component
                .iter()
                .map(|&idx| {
                    let n = graph.edges[idx].lines.len();
                    (1..=n).product::<usize>() as f64 // factorial
                })
                .product();

            // Match C++ ILPOptimizer.cpp line 33: if (solutionSpaceSize(g) < 500)
            // For very small solution spaces, use greedy + hill climbing (like C++ ExhaustiveOptimizer)
            if solution_space < 500.0 {
                let results = self.greedy_hillclimb_component(graph, component, &node_to_all_edges);
                global_results.extend(results);
                continue;
            }

            // For very large solution spaces, use greedy + hill climbing instead of ILP to avoid OOM
            // Thresholds: size > 30 or solution_space > 1e10
            if solution_space > 1e10 || size > 30 {
                println!(
                    "Component {}/{} (Size: {} edges, SolSpace: {:.0}) too large for ILP, using greedy+hillclimb.",
                    i + 1,
                    total_components,
                    size,
                    solution_space
                );
                let results = self.greedy_hillclimb_component(graph, component, &node_to_all_edges);
                global_results.extend(results);
                continue;
            }

            println!(
                "Component {}/{} (Size: {} edges, SolSpace: {:.0}). Building ILP model...",
                i + 1,
                total_components,
                size,
                solution_space
            );
            if let Some(res) = self.solve_component(graph, component, &node_to_all_edges) {
                global_results.extend(res);
            } else {
                // ILP failed, use greedy + hill climbing fallback
                println!("  - ILP failed, using greedy+hillclimb fallback.");
                let results = self.greedy_hillclimb_component(graph, component, &node_to_all_edges);
                global_results.extend(results);
            }
            println!("Component {}/{} solved.", i + 1, total_components);
        }

        // 5. Apply Results
        for (edge_idx, sorted_lines) in global_results {
            // UN-BUNDLE Rule 2 Lines
            let mut final_lines = Vec::new();
            for l in sorted_lines {
                // Check if it's a super-line
                if let Some(original_group) = graph.collapsed_lines.get(&l.line_id) {
                    // Since they were all identical in path, any order internally is valid (they don't cross each other).
                    // Just emit them.
                    final_lines.extend(original_group.clone());
                } else {
                    final_lines.push(l);
                }
            }
            graph.edges[edge_idx].lines = final_lines;
        }

        println!("Decomposed ILP Optimization complete.");
    }

    /// Solve a component using ILP formulation matching C++ loom's ILPEdgeOrderOptimizer.
    ///
    /// Key changes from previous implementation:
    /// 1. Position vars use cumulative semantics: x_(l, p<=k) means "line l has position <= k"
    /// 2. Oracle vars linked via linear constraint instead of Big-M:
    ///    m * x_{A<B} + sum_p(x_A_p<=p - x_B_p<=p) >= 0
    /// 3. Crossing detection uses: decVar >= |x_{A<B}^e1 - x_{A<B}^e2|
    fn solve_component(
        &self,
        graph: &RenderGraph,
        edge_indices: &[usize],
        node_to_all_edges: &HashMap<i64, Vec<usize>>,
    ) -> Option<HashMap<usize, Vec<LineOnEdge>>> {
        let mut vars = ProblemVariables::new();
        // Position vars: (edge, line_idx, p) -> binary, meaning "line's position <= p"
        // Note: p is 0-indexed here to match C++ (0..n-1)
        let mut pos_vars: HashMap<(usize, usize, usize), Variable> = HashMap::new();
        // Oracle vars: (edge, line_a, line_b) -> binary, meaning "line_a < line_b"
        let mut oracle_vars: HashMap<(usize, usize, usize), Variable> = HashMap::new();
        let mut constraints = Vec::new();
        let mut objective: Expression = 0.into();

        // Find max cardinality across all edges in component (needed for oracle linking)
        let max_cardinality = edge_indices
            .iter()
            .map(|&idx| graph.edges[idx].lines.len())
            .max()
            .unwrap_or(0);

        // --- A. Setup Position Variables (matching C++ createProblem) ---
        for &edge_idx in edge_indices {
            let edge = &graph.edges[edge_idx];
            let n = edge.lines.len(); // Known >= 2

            // For each line l and position p (0-indexed), create x_(edge, l, p<=p)
            for l_idx in 0..n {
                for p in 0..n {
                    let v = vars.add(variable().binary());
                    pos_vars.insert((edge_idx, l_idx, p), v);

                    // Monotonicity constraint: x_(l, p<=k) >= x_(l, p<=k-1)
                    // In C++: lp->addRow(..., 0, shared::optim::LO); x_p - x_{p-1} >= 0
                    if p > 0 {
                        let v_prev = pos_vars.get(&(edge_idx, l_idx, p - 1)).unwrap();
                        constraints.push((v.into_expression() - *v_prev).geq(0));
                    }
                }
            }

            // Uniqueness constraint: sum over all lines of x_(l, p<=k) = k+1
            // This ensures exactly k+1 lines have position <= k
            for p in 0..n {
                let mut sum_expr: Expression = 0.into();
                for l_idx in 0..n {
                    sum_expr += pos_vars.get(&(edge_idx, l_idx, p)).unwrap();
                }
                constraints.push(sum_expr.eq((p + 1) as f64));
            }
        }

        // --- B. Setup Oracle Variables (matching C++ writeCrossingOracle) ---
        for &edge_idx in edge_indices {
            let edge = &graph.edges[edge_idx];
            let n = edge.lines.len();

            // Create oracle variables for all line pairs
            for i in 0..n {
                for j in (i + 1)..n {
                    // x_(edge, i < j) - "line i is before line j"
                    let x_ij = vars.add(variable().binary());
                    oracle_vars.insert((edge_idx, i, j), x_ij);

                    // x_(edge, j < i) - "line j is before line i"
                    let x_ji = vars.add(variable().binary());
                    oracle_vars.insert((edge_idx, j, i), x_ji);

                    // Antisymmetry: x_ij + x_ji = 1 (exactly one must be true)
                    constraints.push((x_ij.into_expression() + x_ji).eq(1.0));
                }
            }

            // Link oracle vars to position vars (matching C++ "sum constraint")
            // Constraint: m * x_{A<B} + sum_p(x_A_p<=p - x_B_p<=p) >= 0
            // This ensures: if A < B (x_{A<B}=1), constraint satisfied trivially
            //               if A >= B (x_{A<B}=0), then sum(x_A - x_B) >= 0 means pos(A) <= pos(B)
            for i in 0..n {
                for j in 0..n {
                    if i == j {
                        continue;
                    }

                    let x_ij = oracle_vars.get(&(edge_idx, i, j)).unwrap();

                    // Build: m * x_ij + sum_p(x_i_p - x_j_p) >= 0
                    let mut expr: Expression = x_ij.into_expression() * (max_cardinality as f64);
                    for p in 0..n {
                        let x_i_p = pos_vars.get(&(edge_idx, i, p)).unwrap();
                        let x_j_p = pos_vars.get(&(edge_idx, j, p)).unwrap();
                        expr = expr + *x_i_p - *x_j_p;
                    }
                    constraints.push(expr.geq(0));
                }
            }

            // --- B2. Group Convexity Constraints ---
            // Enforce that lines within the same group are contiguous.
            if n >= 3 {
                let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
                for (i, line) in edge.lines.iter().enumerate() {
                    let key = if let Some(gid) = &line.group_id {
                        gid.clone()
                    } else {
                        format!("color:{}", line.color)
                    };
                    groups.entry(key).or_default().push(i);
                }

                for (_, members) in groups {
                    if members.len() < 2 {
                        continue;
                    }

                    // For every pair in group
                    for idx_a in 0..members.len() {
                        for idx_b in (idx_a + 1)..members.len() {
                            let i = members[idx_a];
                            let j = members[idx_b];

                            // For every line k NOT in group
                            for k in 0..n {
                                if members.contains(&k) {
                                    continue;
                                }

                                // Forbid i < k < j => x_ik + x_kj <= 1
                                // (if both i<k and k<j, then k is between i and j)
                                if let (Some(x_ik), Some(x_kj)) = (
                                    oracle_vars.get(&(edge_idx, i, k)),
                                    oracle_vars.get(&(edge_idx, k, j)),
                                ) {
                                    constraints.push((x_ik.into_expression() + *x_kj).leq(1.0));
                                }

                                // Forbid j < k < i => x_jk + x_ki <= 1
                                if let (Some(x_jk), Some(x_ki)) = (
                                    oracle_vars.get(&(edge_idx, j, k)),
                                    oracle_vars.get(&(edge_idx, k, i)),
                                ) {
                                    constraints.push((x_jk.into_expression() + *x_ki).leq(1.0));
                                }
                            }
                        }
                    }
                }
            }
        }

        // --- B. Constraints (Consistency & Split) ---
        // Helper to get line local index
        let get_line_idx = |e_idx: usize, line_id: &str| -> Option<usize> {
            graph.edges[e_idx]
                .lines
                .iter()
                .position(|l| l.line_id == line_id)
        };

        // We iterate component edges. For each end node, we look at connected edges.
        // If connected edge is ALSO in component -> Consistency check.
        // If connected edge is NOT in component (or is) -> Split check geometry.

        let edge_set: HashSet<usize> = edge_indices.iter().cloned().collect();

        for &e1_idx in edge_indices {
            let edge1 = &graph.edges[e1_idx];

            // Check both ends
            for &node_id in &[edge1.from, edge1.to] {
                // Get all connected edges to this node (from global map)
                let connected = if let Some(c) = node_to_all_edges.get(&node_id) {
                    c
                } else {
                    continue;
                };

                // We need geometry for all connected edges for angle sorting
                let mut edge_angles: Vec<(usize, f64)> = Vec::new();

                for &idx in connected {
                    let edge = &graph.edges[idx];
                    let (dx, dy) = if edge.from == node_id {
                        let p1 = edge.geometry.get(0).unwrap_or(&[0.0, 0.0]);
                        let p2 = edge.geometry.get(1).unwrap_or(&[0.0, 0.0]);
                        (p2[0] - p1[0], p2[1] - p1[1])
                    } else {
                        let p_last = edge.geometry.last().unwrap_or(&[0.0, 0.0]);
                        let p_prev = edge
                            .geometry
                            .get(edge.geometry.len().saturating_sub(2))
                            .unwrap_or(&[0.0, 0.0]);
                        (p_prev[0] - p_last[0], p_prev[1] - p_last[1])
                    };
                    let angle: f64 = dy.atan2(dx);
                    let angle = if angle < 0.0 { angle + 2.0 * PI } else { angle };
                    edge_angles.push((idx, angle));
                }
                // Sort
                edge_angles
                    .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

                // Rank map
                let edge_rank: HashMap<usize, usize> = edge_angles
                    .iter()
                    .enumerate()
                    .map(|(i, (id, _))| (*id, i))
                    .collect();

                // 1. Consistency Check with other Component Edges
                for &e2_idx in connected {
                    if e2_idx <= e1_idx {
                        continue;
                    } // Avoid duplicates, and only if e2 > e1
                    if !edge_set.contains(&e2_idx) {
                        continue;
                    } // Only if e2 is in component

                    // e1 and e2 share a node and are both in component.
                    // Check for shared lines >= 2 (unique line_ids only)
                    let edge2 = &graph.edges[e2_idx];
                    let mut shared_lines_set = HashSet::new();
                    for l1 in &edge1.lines {
                        // Include ALL shared lines in consistency constraints
                        // (ILP needs to order all lines, not just those in restrictions)
                        if edge2.lines.iter().any(|l2| l2.line_id == l1.line_id) {
                            // Only add unique line_ids (bundled routes share same line_id)
                            shared_lines_set.insert(l1.line_id.clone());
                        }
                    }
                    let shared_lines: Vec<String> = shared_lines_set.into_iter().collect();

                    if shared_lines.len() >= 2 {
                        // Add consistency constraints
                        // Direction-aware constraint logic (matching C++ loom):
                        // otherWayX = (edge.from != node) ^ edge.dir
                        // If otherWayA != otherWayB, we need to adjust variable selection
                        let other_way_e1 = (edge1.from != node_id) ^ edge1.dir;
                        let other_way_e2 = (edge2.from != node_id) ^ edge2.dir;
                        let same_direction = !(other_way_e1 ^ other_way_e2);

                        for a_idx in 0..shared_lines.len() {
                            for b_idx in (a_idx + 1)..shared_lines.len() {
                                let la = &shared_lines[a_idx];
                                let lb = &shared_lines[b_idx];

                                let a1 = get_line_idx(e1_idx, la).unwrap();
                                let b1 = get_line_idx(e1_idx, lb).unwrap();
                                let a2 = get_line_idx(e2_idx, la).unwrap();
                                let b2 = get_line_idx(e2_idx, lb).unwrap();

                                let c_var = vars.add(variable().binary());
                                // Weighted crossing cost (Consistency / Same Segment)
                                let w_a = graph.edges[e1_idx].lines[a1].weight as f64;
                                let w_b = graph.edges[e1_idx].lines[b1].weight as f64;
                                objective += c_var * self.weight_consistency * w_a * w_b;

                                // C++ Logic: If edges have same effective direction relative to node,
                                // a crossing occurs when A<B on e1 AND B<A on e2 (or vice versa).
                                // If edges have different directions, a crossing occurs when
                                // A<B on e1 AND A<B on e2 (or B<A on both).
                                if same_direction {
                                    // Same direction: "inverted" logic from C++ perspective (relative to node).
                                    // If edges have same direction relative to node, A<B on one implies B<A on the other to be "consistent"
                                    // (because one enters, one leaves basically, or both enter/leave but indices are reversed geometric ally?
                                    // Actually, just trusting the Swap is sufficient based on analysis).
                                    // Logic taken from previous "else" block:
                                    let x_e1_ab = oracle_vars.get(&(e1_idx, a1, b1)).unwrap();
                                    let x_e2_ab = oracle_vars.get(&(e2_idx, a2, b2)).unwrap();
                                    constraints.push(
                                        (x_e1_ab.into_expression() + *x_e2_ab - 1.0).leq(c_var),
                                    );

                                    let x_e1_ba = oracle_vars.get(&(e1_idx, b1, a1)).unwrap();
                                    let x_e2_ba = oracle_vars.get(&(e2_idx, b2, a2)).unwrap();
                                    constraints.push(
                                        (x_e1_ba.into_expression() + *x_e2_ba - 1.0).leq(c_var),
                                    );
                                } else {
                                    // Different direction: "direct" logic.
                                    // Logic taken from previous "if" block:
                                    let x_e1_ab = oracle_vars.get(&(e1_idx, a1, b1)).unwrap();
                                    let x_e2_ba = oracle_vars.get(&(e2_idx, b2, a2)).unwrap();
                                    constraints.push(
                                        (x_e1_ab.into_expression() + *x_e2_ba - 1.0).leq(c_var),
                                    );

                                    let x_e1_ba = oracle_vars.get(&(e1_idx, b1, a1)).unwrap();
                                    let x_e2_ab = oracle_vars.get(&(e2_idx, a2, b2)).unwrap();
                                    constraints.push(
                                        (x_e1_ba.into_expression() + *x_e2_ab - 1.0).leq(c_var),
                                    );
                                }
                            }
                        }
                    }
                }

                // 2. Split Check (Geometric Target Order)
                // e1 is the "shared" edge. We consist check lines that go to DIFFERENT neighbors.
                let n_lines = edge1.lines.len();
                for i in 0..n_lines {
                    for j in (i + 1)..n_lines {
                        let la = &edge1.lines[i];
                        let lb = &edge1.lines[j];

                        // Find edges they go to (excluding e1)
                        let mut ea_idx = None;
                        let mut eb_idx = None;

                        for &cand in connected {
                            if cand == e1_idx {
                                continue;
                            }
                            if graph.edges[cand].lines.iter().any(|l| {
                                if l.line_id != la.line_id {
                                    return false;
                                }
                                let key = (l.chateau_id.clone(), l.route_id.clone());
                                if let Some(r) = graph.restrictions.get(&(e1_idx, cand)) {
                                    if r.contains(&key) {
                                        return false;
                                    }
                                }
                                if let Some(r) = graph.restrictions.get(&(cand, e1_idx)) {
                                    if r.contains(&key) {
                                        return false;
                                    }
                                }
                                true
                            }) {
                                ea_idx = Some(cand);
                                break;
                            }
                        }
                        for &cand in connected {
                            if cand == e1_idx {
                                continue;
                            }
                            if graph.edges[cand].lines.iter().any(|l| {
                                if l.line_id != lb.line_id {
                                    return false;
                                }
                                let key = (l.chateau_id.clone(), l.route_id.clone());
                                if let Some(r) = graph.restrictions.get(&(e1_idx, cand)) {
                                    if r.contains(&key) {
                                        return false;
                                    }
                                }
                                if let Some(r) = graph.restrictions.get(&(cand, e1_idx)) {
                                    if r.contains(&key) {
                                        return false;
                                    }
                                }
                                true
                            }) {
                                eb_idx = Some(cand);
                                break;
                            }
                        }

                        if let (Some(ea), Some(eb)) = (ea_idx, eb_idx) {
                            if ea == eb {
                                continue;
                            } // Not a split

                            // Split! Check geometry.
                            let rank_a = edge_rank[&ea];
                            let rank_b = edge_rank[&eb];

                            let angle_s =
                                edge_angles.iter().find(|(id, _)| *id == e1_idx).unwrap().1;
                            let angle_a = edge_angles.iter().find(|(id, _)| *id == ea).unwrap().1;
                            let angle_b = edge_angles.iter().find(|(id, _)| *id == eb).unwrap().1;

                            let diff_a = (angle_a - angle_s + 2.0 * PI) % (2.0 * PI);
                            let diff_b = (angle_b - angle_s + 2.0 * PI) % (2.0 * PI);

                            // Determine if we are leaving the node relative to edge1's direction
                            // edge1.dir signals "Forward" direction.
                            // If edge1.from == node_id, we are at the Start. If dir is true (Fwd), we are Leaving. OK.
                            // If edge1.from != node_id, we are at End. If dir is true (Fwd), we are Entering. OK.
                            let is_leaving = (edge1.from == node_id) == edge1.dir;

                            let mut prefer_a_left = diff_a < diff_b;
                            if !is_leaving {
                                prefer_a_left = !prefer_a_left;
                            }

                            let split_penalty = vars.add(variable().binary());
                            // Split Penalty (Diff Segment / Geometry)
                            objective += split_penalty * self.weight_split;

                            if prefer_a_left {
                                // Want x_{e1, A<B} == 1 (A is Left/Low Index)
                                // So we PENALIZE the opposite: x_{e1, B<A} == 1
                                let x_ba = oracle_vars.get(&(e1_idx, j, i)).unwrap();
                                constraints.push(x_ba.into_expression().leq(split_penalty));
                            } else {
                                // Want x_{e1, B<A} == 1 (B is Left/Low Index)
                                // So we PENALIZE the opposite: x_{e1, A<B} == 1
                                let x_ab = oracle_vars.get(&(e1_idx, i, j)).unwrap();
                                constraints.push(x_ab.into_expression().leq(split_penalty));
                            }
                        }
                    }
                }
            } // end loop nodes
        } // end loop edges

        // Solve
        let mut model = vars
            .minimise(objective)
            .using(good_lp::solvers::coin_cbc::coin_cbc);

        model.set_parameter("seconds", "60");
        model.set_parameter("threads", "4"); // Reduced from 16 to match C++ loom (less memory per thread)
        //model.set_parameter("ratio", "0.05");

        for c in constraints {
            model.add_constraint(c);
        }

        println!("  - Invoking solver...");
        let solution = match model.solve() {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "ILP Component Solve failed: {:?}. Skipping optimization for this component.",
                    e
                );
                return None;
            }
        };

        println!("  - Solver completed.");

        // Collect results - extract positions from cumulative pos_vars
        // For each line, position = sum over p of x_(l, p<=p)
        // (Higher sum means higher/later position, lower sum means earlier position)
        let mut component_results = HashMap::new();
        for &edge_idx in edge_indices {
            let edge = &graph.edges[edge_idx];
            let n = edge.lines.len();
            let mut ranks: Vec<(usize, f64)> = Vec::new();
            for i in 0..n {
                let mut rank_sum = 0.0;
                for p in 0..n {
                    if let Some(v) = pos_vars.get(&(edge_idx, i, p)) {
                        rank_sum += solution.value(*v);
                    }
                }
                ranks.push((i, rank_sum));
            }

            // Sort by rank_sum descending (higher sum = later position = appears later in array)
            let mut line_indices: Vec<usize> = (0..n).collect();
            line_indices.sort_by(|&a, &b| {
                let sum_a = ranks.iter().find(|(idx, _)| *idx == a).unwrap().1;
                let sum_b = ranks.iter().find(|(idx, _)| *idx == b).unwrap().1;
                sum_b
                    .partial_cmp(&sum_a)
                    .unwrap_or(std::cmp::Ordering::Equal) // Descending
            });

            let new_lines: Vec<LineOnEdge> = line_indices
                .into_iter()
                .map(|idx| edge.lines[idx].clone())
                .collect();
            component_results.insert(edge_idx, new_lines);
        }

        Some(component_results)
    }

    fn untangle_complex_repeatedly(&self, graph: &mut RenderGraph) -> bool {
        // Order matches C++ loom's OptGraph::untangle():
        // DoubleStump → OuterStump → FullX (loop) → Y → PartialY →
        // DogBone → PartialDogBone → InnerStump
        let mut any_changed = false;
        loop {
            let mut changed = false;
            let node_adj = self.build_node_adjacency(graph);

            // 1. Rule 8: Double Stump (C++ untangleDoubleStump)
            if self.untangle_rule_8_double_stump(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 2. Rule 6: Outer Stump (C++ untangleOuterStump)
            if self.untangle_rule_6_outer_stump(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 3. Rule 1: Full X (C++ untangleFullX - runs in loop)
            if self.untangle_rule_1(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 4. Rule 2: Full Y (C++ untangleY)
            if self.untangle_rule_2(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 5. Rule 3: Partial Y (C++ untanglePartialY)
            if self.untangle_rule_3(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 6. Rule 4: Full Double Y / DogBone (C++ untangleDogBone)
            if self.untangle_rule_4(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 7. Rule 5: Partial Double Y / PartialDogBone (C++ untanglePartialDogBone)
            if self.untangle_rule_5(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            // 8. Rule 7: Inner Stump (C++ untangleInnerStump)
            if self.untangle_rule_7_inner_stump(graph, &node_adj) {
                changed = true;
                any_changed = true;
                continue;
            }

            if !changed {
                break;
            }
        }
        any_changed
    }

    fn untangle_simple_repeatedly(&self, graph: &mut RenderGraph) -> bool {
        let mut any_changed = false;
        loop {
            let node_adj = self.build_node_adjacency(graph);
            // Rule 1: Full X
            if self.untangle_rule_1(graph, &node_adj) {
                any_changed = true;
                // println!("Applied Untangling Rule 1 (Full X)");
                continue;
            }
            break;
        }
        any_changed
    }

    fn build_node_adjacency(&self, graph: &RenderGraph) -> HashMap<i64, Vec<usize>> {
        let mut adj = HashMap::new();
        for (i, edge) in graph.edges.iter().enumerate() {
            adj.entry(edge.from).or_insert_with(Vec::new).push(i);
            adj.entry(edge.to).or_insert_with(Vec::new).push(i);
        }
        adj
    }

    // Implement Untangling Rules 1 & 2
    fn untangle_rule_1(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Untangling Rule 1: Generalized Full X (Isolated Flow)
        // Matches C++ isFullX/untangleFullX logic.
        // At any node v with deg(v) >= 3, find a pair of edges (ea, eb) where:
        // 1. L(ea) == L(eb)  (same lines)
        // 2. Lines on ea/eb do NOT appear on any other edge at v (isolated flow)
        // 3. No lines from OTHER edges partially continue through ea-eb pair
        // Action: Split v into v' and v''. Connect ea/eb to v', rest to v''.

        let mut nodes: Vec<i64> = node_adj.keys().cloned().collect();
        nodes.sort(); // Determinism

        for v in nodes {
            if let Some(adj) = node_adj.get(&v) {
                if adj.len() < 3 {
                    continue;
                }

                // Try all pairs of edges at v
                for i in 0..adj.len() {
                    for j in (i + 1)..adj.len() {
                        let ea_idx = adj[i];
                        let eb_idx = adj[j];
                        let ea = &graph.edges[ea_idx];
                        let eb = &graph.edges[eb_idx];

                        // Condition 1: L(ea) == L(eb)
                        let mut lines_a: Vec<_> =
                            ea.lines.iter().map(|x| x.line_id.clone()).collect();
                        let mut lines_b: Vec<_> =
                            eb.lines.iter().map(|x| x.line_id.clone()).collect();
                        lines_a.sort();
                        lines_b.sort();

                        if lines_a != lines_b || lines_a.is_empty() {
                            continue;
                        }

                        let flow_lines: HashSet<_> = lines_a.iter().cloned().collect();

                        // Condition 2: These lines do NOT appear on any other edge at v
                        let mut isolated = true;
                        for k in 0..adj.len() {
                            if k == i || k == j {
                                continue;
                            }
                            let ek = &graph.edges[adj[k]];
                            for l in &ek.lines {
                                if flow_lines.contains(&l.line_id) {
                                    isolated = false;
                                    break;
                                }
                            }
                            if !isolated {
                                break;
                            }
                        }
                        if !isolated {
                            continue;
                        }

                        // Condition 3: No lines from OTHER edges partially continue over ea-eb
                        // i.e., no line on some other edge ek also appears on ea or eb
                        // (This is already covered by Condition 2 in a stricter sense)
                        // The C++ logic also checks that remaining edges are "line disjunct"
                        // but here we just need the isolated pair.

                        // Found an isolated flow pair!
                        let max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);
                        let v_prime = max_node_id + 1;
                        let v_double_prime = max_node_id + 2;

                        let v_node = graph.nodes[&v].clone();

                        graph.nodes.insert(
                            v_prime,
                            crate::graph::Node {
                                id: v_prime,
                                x: v_node.x,
                                y: v_node.y,
                                is_cluster: false,
                                name: None,
                            },
                        );

                        graph.nodes.insert(
                            v_double_prime,
                            crate::graph::Node {
                                id: v_double_prime,
                                x: v_node.x,
                                y: v_node.y,
                                is_cluster: false,
                                name: None,
                            },
                        );

                        // Isolated pair -> v_prime
                        for &idx in &[ea_idx, eb_idx] {
                            let e = &mut graph.edges[idx];
                            if e.from == v {
                                e.from = v_prime;
                            }
                            if e.to == v {
                                e.to = v_prime;
                            }
                        }
                        // Rest -> v_double_prime
                        for k in 0..adj.len() {
                            if k == i || k == j {
                                continue;
                            }
                            let e = &mut graph.edges[adj[k]];
                            if e.from == v {
                                e.from = v_double_prime;
                            }
                            if e.to == v {
                                e.to = v_double_prime;
                            }
                        }

                        graph.nodes.remove(&v);

                        debug!(
                            "Applied Untangling Rule 1 (Generalized FullX) at node {} (deg={})",
                            v,
                            adj.len()
                        );
                        return true;
                    }
                }
            }
        }
        false
    }

    fn untangle_rule_2(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Untangling Rule 2: Full Y
        // Simplified implementation: Deg(v)=3, Deg(u)=1 (Major leg terminus)
        // Split v into v1, v2; u into u1, u2.

        let mut changes = false;
        let mut nodes: Vec<i64> = node_adj.keys().cloned().collect();
        nodes.sort();

        for v in nodes {
            if let Some(adj) = node_adj.get(&v) {
                if adj.len() != 3 {
                    continue;
                }

                for &e_idx in adj {
                    // Candidate major leg e
                    // Clone data to avoid borrow issues
                    let (u, v, e_lines_set, e_lines_len, e_geom) = {
                        let e = &graph.edges[e_idx];
                        let u = if e.from == v { e.to } else { e.from };
                        let lines_set: HashSet<_> =
                            e.lines.iter().map(|x| x.line_id.clone()).collect();
                        (u, v, lines_set, e.lines.len(), e.geometry.clone())
                    };

                    if node_adj.get(&u).map_or(0, |x| x.len()) != 1 {
                        continue;
                    }

                    let minors: Vec<usize> = adj.iter().cloned().filter(|&x| x != e_idx).collect();
                    if minors.len() != 2 {
                        continue;
                    }

                    // Extract data from minors to avoid holding refs
                    let (m1_lines_vec, m1_lines_set) = {
                        let m = &graph.edges[minors[0]];
                        let vec = m.lines.clone();
                        let set: HashSet<_> = vec.iter().map(|x| x.line_id.clone()).collect();
                        (vec, set)
                    };
                    let (m2_lines_vec, m2_lines_set) = {
                        let m = &graph.edges[minors[1]];
                        let vec = m.lines.clone();
                        let set: HashSet<_> = vec.iter().map(|x| x.line_id.clone()).collect();
                        (vec, set)
                    };

                    if !m1_lines_set.is_subset(&e_lines_set)
                        || !m2_lines_set.is_subset(&e_lines_set)
                    {
                        continue;
                    }
                    if !m1_lines_set.is_disjoint(&m2_lines_set) {
                        continue;
                    }
                    if m1_lines_set.len() + m2_lines_set.len() != e_lines_len {
                        continue;
                    }

                    // Split
                    let max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);
                    let u1 = max_node_id + 1;
                    let u2 = max_node_id + 2;
                    let v1 = max_node_id + 3;
                    let v2 = max_node_id + 4;

                    let u_node = graph.nodes[&u].clone();
                    let v_node = graph.nodes[&v].clone();

                    graph.nodes.insert(
                        u1,
                        crate::graph::Node {
                            id: u1,
                            x: u_node.x,
                            y: u_node.y,
                            is_cluster: false,
                            name: None,
                        },
                    );
                    graph.nodes.insert(
                        u2,
                        crate::graph::Node {
                            id: u2,
                            x: u_node.x,
                            y: u_node.y,
                            is_cluster: false,
                            name: None,
                        },
                    );
                    graph.nodes.insert(
                        v1,
                        crate::graph::Node {
                            id: v1,
                            x: v_node.x,
                            y: v_node.y,
                            is_cluster: false,
                            name: None,
                        },
                    );
                    graph.nodes.insert(
                        v2,
                        crate::graph::Node {
                            id: v2,
                            x: v_node.x,
                            y: v_node.y,
                            is_cluster: false,
                            name: None,
                        },
                    );

                    // Modify m1 to connect to v1
                    {
                        let min1_edge = &mut graph.edges[minors[0]];
                        if min1_edge.from == v {
                            min1_edge.from = v1;
                        }
                        if min1_edge.to == v {
                            min1_edge.to = v1;
                        }
                    }

                    // Modify m2 to connect to v2
                    {
                        let min2_edge = &mut graph.edges[minors[1]];
                        if min2_edge.from == v {
                            min2_edge.from = v2;
                        }
                        if min2_edge.to == v {
                            min2_edge.to = v2;
                        }
                    }

                    let max_edge_id = graph.edges.iter().map(|edge| edge.id).max().unwrap_or(0);

                    graph.edges.push(Edge {
                        id: max_edge_id + 1,
                        from: u1,
                        to: v1,
                        lines: m1_lines_vec, // Use extracted vec
                        geometry: e_geom.clone(),
                        dir: true,
                    });

                    graph.edges.push(Edge {
                        id: max_edge_id + 2,
                        from: u2,
                        to: v2,
                        lines: m2_lines_vec, // Use extracted vec
                        geometry: e_geom,
                        dir: true,
                    });

                    graph.edges.swap_remove(e_idx);
                    graph.nodes.remove(&u);
                    graph.nodes.remove(&v);

                    changes = true;
                    return true;
                }
            }
        }
        changes
    }

    fn untangle_rule_3(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Iterate over all active edges to find candidates.
        // Rule 3 applies to an edge e = {u, v} where deg(u) = 1.
        // We need to look for such edges.

        let mut to_split: Option<(usize, i64, i64)> = None; // (edge_idx, u_id, v_id)

        'outer: for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }

            // Check ends for degree 1
            let u_candidates = [edge.from, edge.to];
            for &u in &u_candidates {
                let deg_u = node_adj.get(&u).map(|v| v.len()).unwrap_or(0);
                if deg_u != 1 {
                    continue; // Must be 1
                }

                // Identify v (the other node)
                let v = if edge.from == u { edge.to } else { edge.from };

                // Conditions:
                // 1. Each l in L(e) terminates at u.
                //    (This is implicitly true if deg(u)=1 and lines don't disappear into thin air.
                //     We assume valid graph where lines only end at terminuses.
                //     If u is deg 1, lines cannot go anywhere else.)

                // 2. Each l in L(e) uniquely extends over v into one of n > 1 edges e_1...e_n
                //    Wait, "uniquely extends".
                //    Means lines partition into sets L(e_i).

                let v_edges = if let Some(ve) = node_adj.get(&v) {
                    ve
                } else {
                    continue;
                };
                if v_edges.len() <= 1 {
                    continue;
                } // Need minor legs > 1 (so v degree >= 2, excluding e makes >= 1, wait. n>1 edges e_i distinct from e. so deg(v) >= 3 ? No, n>1 usually means at least 2 minor legs.)

                // Groups of lines extending into other edges
                let mut line_groups: HashMap<usize, Vec<String>> = HashMap::new();
                let mut all_lines_accounted = true;

                for line in &edge.lines {
                    // Find which edge at v this line extends to
                    let mut found_ext = false;
                    for &minor_idx in v_edges {
                        if minor_idx == e_idx {
                            continue;
                        }
                        let minor_edge = &graph.edges[minor_idx];
                        if minor_edge.lines.iter().any(|l| l.line_id == line.line_id) {
                            if found_ext {
                                // Extends to MULTIPLE minor legs? Then not "uniquely extends into ONE".
                                // Rule 3 requires 1-to-1 mapping of line to minor leg.
                                all_lines_accounted = false;
                                break;
                            }
                            line_groups
                                .entry(minor_idx)
                                .or_default()
                                .push(line.line_id.clone());
                            found_ext = true;
                        }
                    }
                    if !found_ext || !all_lines_accounted {
                        all_lines_accounted = false;
                        break;
                    }
                }

                if !all_lines_accounted {
                    continue;
                }
                if line_groups.len() < 2 {
                    continue;
                } // Need to split into at least 2 parts

                // Valid candidate found!
                to_split = Some((e_idx, u, v));
                break 'outer;
            }
        }

        if let Some((e_idx, u, v)) = to_split {
            // Apply Split
            // We split u into u', u'', ... for each group of minor legs.
            // Actually Rule 3 says: Split u into u', u''. Connect v-u' with lines of leftmost minor leg. Connect v-u'' with rest.
            // It suggests iterative splitting. But we can split all at once for efficiency?
            // "Split u into nodes u', u''. Connect v and u' with an edge e' ... Connect v and u'' with e'' ..."
            // Let's do 1 split (extract one group vs rest) per pass to be safe and simple.

            // Re-identify groups because borrow checker
            let edge_data = graph.edges[e_idx].clone();
            let v_edges = &node_adj[&v];
            let mut line_groups: HashMap<usize, Vec<String>> = HashMap::new();
            for line in &edge_data.lines {
                for &minor_idx in v_edges {
                    if minor_idx == e_idx {
                        continue;
                    }
                    let minor_edge = &graph.edges[minor_idx];
                    if minor_edge.lines.iter().any(|l| l.line_id == line.line_id) {
                        line_groups
                            .entry(minor_idx)
                            .or_default()
                            .push(line.line_id.clone());
                        break; // assumed unique from check above
                    }
                }
            }

            // Pick one minor leg (e.g. first one found) to split off 'u_prime'
            // The rest stay with 'u' (or u_double_prime)
            let (&split_minor_idx, split_lines) = line_groups.iter().next().unwrap();

            // Create new node u_prime
            // Create new edge e_prime = {v, u_prime}

            // Clone u (geometry)
            let u_node = graph.nodes[&u].clone();
            let mut u_prime = u_node.clone();
            // Generate new ID
            let new_u_id = graph.nodes.keys().min().unwrap_or(&0) - 1; // negative IDs for temp nodes? Or find max+1
            let new_u_id = if new_u_id >= 0 { -1 } else { new_u_id }; // ensure negative to avoid collision with DB ids?
            // Actually active graph might use positive IDs. Let's start from max + 1
            let max_id = graph.nodes.keys().max().cloned().unwrap_or(0);
            let new_u_id = max_id + 1;

            u_prime.id = new_u_id;
            graph.nodes.insert(new_u_id, u_prime);

            // Create e_prime
            let mut e_prime = edge_data.clone();
            let max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);
            e_prime.id = max_edge_id + 1;

            // Fix e_prime connectivity
            if e_prime.from == u {
                e_prime.from = new_u_id;
            }
            if e_prime.to == u {
                e_prime.to = new_u_id;
            }

            // Filter lines on e_prime (only split_lines)
            e_prime.lines.retain(|l| split_lines.contains(&l.line_id));

            // Remove lines from original e (lines staying with "rest")
            graph.edges[e_idx]
                .lines
                .retain(|l| !split_lines.contains(&l.line_id));

            // Add e_prime
            graph.edges.push(e_prime);

            debug!(
                "Splitting edge {} at node {} (deg 1) for minor leg lines {:?}",
                edge_data.id, u, split_lines
            );

            return true;
        }

        false
    }

    fn untangle_rule_4(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        let mut to_split: Option<(usize, i64, i64)> = None;

        'outer: for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }
            let u = edge.from;
            let v = edge.to;

            // Degree checks
            let deg_u = node_adj.get(&u).map(|l| l.len()).unwrap_or(0);
            let deg_v = node_adj.get(&v).map(|l| l.len()).unwrap_or(0);

            if deg_u < 3 || deg_v < 3 {
                continue;
            }

            let u_legs = &node_adj[&u];
            let v_legs = &node_adj[&v];

            // Check flow groups at u
            let mut u_groups: HashMap<usize, Vec<String>> = HashMap::new();
            let mut u_accounted = true;
            for line in &edge.lines {
                let mut found = false;
                for &leg in u_legs {
                    if leg == e_idx {
                        continue;
                    }
                    let leg_edge = &graph.edges[leg];
                    if leg_edge.lines.iter().any(|l| l.line_id == line.line_id) {
                        // Strict uniqueness check: line must go to exactly one minor leg
                        if found {
                            u_accounted = false;
                            break;
                        }
                        u_groups.entry(leg).or_default().push(line.line_id.clone());
                        found = true;
                    }
                }
                if !found || !u_accounted {
                    u_accounted = false;
                    break;
                }
            }
            if !u_accounted || u_groups.len() < 2 {
                continue;
            }

            // Check flow groups at v
            let mut v_groups: HashMap<usize, Vec<String>> = HashMap::new();
            let mut v_accounted = true;
            for line in &edge.lines {
                let mut found = false;
                for &leg in v_legs {
                    if leg == e_idx {
                        continue;
                    }
                    let leg_edge = &graph.edges[leg];
                    if leg_edge.lines.iter().any(|l| l.line_id == line.line_id) {
                        if found {
                            v_accounted = false;
                            break;
                        }
                        v_groups.entry(leg).or_default().push(line.line_id.clone());
                        found = true;
                    }
                }
                if !found || !v_accounted {
                    v_accounted = false;
                    break;
                }
            }
            if !v_accounted || v_groups.len() < 2 {
                continue;
            }

            // Check Bijective Mapping
            let mut u_sets: Vec<Vec<String>> = u_groups.values().cloned().collect();
            for s in &mut u_sets {
                s.sort();
            }
            u_sets.sort();

            let mut v_sets: Vec<Vec<String>> = v_groups.values().cloned().collect();
            for s in &mut v_sets {
                s.sort();
            }
            v_sets.sort();

            if u_sets != v_sets {
                continue;
            }

            to_split = Some((e_idx, u, v));
            break 'outer;
        }

        if let Some((e_idx, u, v)) = to_split {
            // Re-detect one group to peel
            // Clone edge data to release borrow on graph.edges
            let edge_data = graph.edges[e_idx].clone();
            let u_legs = &node_adj[&u];

            let mut target_lines = Vec::new();
            let mut u_leg_idx = 0;

            for &leg in u_legs {
                if leg == e_idx {
                    continue;
                }
                let leg_edge = &graph.edges[leg];
                // Check intersection with e lines
                let common: Vec<String> = leg_edge
                    .lines
                    .iter()
                    .filter(|l| edge_data.lines.iter().any(|el| el.line_id == l.line_id))
                    .map(|l| l.line_id.clone())
                    .collect();
                if !common.is_empty() {
                    target_lines = common;
                    u_leg_idx = leg;
                    break;
                }
            }

            let v_legs = &node_adj[&v];
            let mut v_leg_idx = 0;
            for &leg in v_legs {
                if leg == e_idx {
                    continue;
                }
                let leg_edge = &graph.edges[leg];
                let common_count = leg_edge
                    .lines
                    .iter()
                    .filter(|l| target_lines.contains(&l.line_id))
                    .count();
                if common_count == target_lines.len() {
                    v_leg_idx = leg;
                    break;
                }
            }

            // Create u', v'
            let u_node = graph.nodes[&u].clone();
            let v_node = graph.nodes[&v].clone();
            let max_id = graph.nodes.keys().max().cloned().unwrap_or(0);
            let u_prime_id = max_id + 1;
            let v_prime_id = max_id + 2;

            let mut u_prime = u_node.clone();
            u_prime.id = u_prime_id;
            let mut v_prime = v_node.clone();
            v_prime.id = v_prime_id;

            graph.nodes.insert(u_prime_id, u_prime);
            graph.nodes.insert(v_prime_id, v_prime);

            // Create e'
            let mut e_prime = edge_data.clone();
            let max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);
            e_prime.id = max_edge_id + 1;
            if e_prime.from == u {
                e_prime.from = u_prime_id;
            } else {
                e_prime.from = v_prime_id;
            }
            if e_prime.to == v {
                e_prime.to = v_prime_id;
            } else {
                e_prime.to = u_prime_id;
            }
            e_prime.lines.retain(|l| target_lines.contains(&l.line_id));
            graph.edges.push(e_prime);

            // Fix original e lines
            graph.edges[e_idx]
                .lines
                .retain(|l| !target_lines.contains(&l.line_id));

            // DETACH minor legs from u/v and ATTACH to u'/v'
            {
                let u_leg = &mut graph.edges[u_leg_idx];
                if u_leg.from == u {
                    u_leg.from = u_prime_id;
                } else if u_leg.to == u {
                    u_leg.to = u_prime_id;
                }
            }
            {
                let v_leg = &mut graph.edges[v_leg_idx];
                if v_leg.from == v {
                    v_leg.from = v_prime_id;
                } else if v_leg.to == v {
                    v_leg.to = v_prime_id;
                }
            }

            println!(
                "Applied Untangling Rule 4 on edge {} (lines {:?})",
                edge_data.id, target_lines
            );
            return true;
        }

        false
    }

    fn untangle_rule_5(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Untangling Rule 5: Partial Double Y
        // Requirement:
        // Edge e = {u, v}
        // deg(u) >= 3, deg(v) >= 3
        // Node u is "Full": All lines passing through u are carried by e. (u is a funnel into e).
        // Node v is "Partial": The lines on e account for only a subset of legs at v.
        // Action: Split v into v' and v'', isolating the flow from u.

        let mut actions: Vec<(usize, i64, i64, Vec<usize>, Vec<String>)> = Vec::new();
        let mut touched_nodes: HashSet<i64> = HashSet::new();

        'edge_loop: for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }
            if touched_nodes.contains(&edge.from) || touched_nodes.contains(&edge.to) {
                continue;
            }

            let candidates = [(edge.from, edge.to), (edge.to, edge.from)];

            for &(u, v) in &candidates {
                // Direction: u (Full) -> v (Partial)

                // 1. Degree Checks
                // The rule applies to complex intersections.
                if node_adj[&u].len() < 3 || node_adj[&v].len() < 3 {
                    continue;
                }

                if touched_nodes.contains(&u) || touched_nodes.contains(&v) {
                    continue;
                }

                // 2. Check strict "Full" condition at u
                // Condition: Every line on every OTHER leg at u must go into e.
                // Corollary: No lines from other legs terminate at u (they must extend to e).
                // Corollary: e contains all lines present on other u-legs.

                let u_legs = &node_adj[&u];
                let mut u_lines_union: HashSet<&String> = HashSet::new();
                let mut valid_u = true;

                for &u_leg_idx in u_legs {
                    if u_leg_idx == e_idx {
                        continue;
                    }
                    let leg_edge = &graph.edges[u_leg_idx];
                    if leg_edge.lines.is_empty() {
                        continue;
                    }
                    for l in &leg_edge.lines {
                        u_lines_union.insert(&l.line_id);
                    }
                }

                // Check 2a: e must contain ALL lines found on u_legs
                let e_lines_set: HashSet<&String> = edge.lines.iter().map(|l| &l.line_id).collect();
                if e_lines_set.len() != u_lines_union.len() {
                    // Mismatch implies termination or extra lines not accounted for.
                    valid_u = false;
                } else {
                    for l in &u_lines_union {
                        if !e_lines_set.contains(l) {
                            valid_u = false;
                            break;
                        }
                    }
                }

                if !valid_u {
                    continue; // Try other direction or next edge
                }

                // 3. Check "Partial" condition at v
                // Condition: Lines on e must map to a SUBSET of legs at v.
                // There must be at least one leg at v NOT involved in this flow.

                let v_legs = &node_adj[&v];
                let mut v_legs_involved: Vec<usize> = Vec::new();
                let mut valid_v = true;

                // We need to verify that each line on e extends to exactly one v-leg.
                // And we track coverage.

                let mut e_lines_covered: HashSet<&String> = HashSet::new();

                for &v_leg_idx in v_legs {
                    if v_leg_idx == e_idx {
                        continue;
                    }
                    let leg_edge = &graph.edges[v_leg_idx];

                    // Check intersection
                    let common_count = leg_edge
                        .lines
                        .iter()
                        .filter(|l| e_lines_set.contains(&l.line_id))
                        .count();

                    if common_count > 0 {
                        v_legs_involved.push(v_leg_idx);
                        for l in &leg_edge.lines {
                            if e_lines_set.contains(&l.line_id) {
                                e_lines_covered.insert(&l.line_id);
                            }
                        }
                    }
                }

                // Check 3a: All lines on e must be covered (no termination at v from e perspective)
                if e_lines_covered.len() != e_lines_set.len() {
                    valid_v = false;
                }

                // Check 3b: Strict subset of legs?
                // The number of involved legs must be < deg(v) - 1.
                // (deg(v) - 1 is total legs excluding e).
                if v_legs_involved.len() >= v_legs.len() - 1 {
                    // This would be a Full Double Y (Rule 4), or all legs involved.
                    // Rule 5 specifically targets Partial matches.
                    valid_v = false;
                }

                if valid_v {
                    // Found a valid Partial Double Y!
                    // Isolate flow u <-> v_legs_involved
                    let target_lines: Vec<String> = e_lines_set.into_iter().cloned().collect();
                    actions.push((e_idx, u, v, v_legs_involved, target_lines));

                    touched_nodes.insert(u);
                    touched_nodes.insert(v);

                    continue 'edge_loop;
                }
            }
        }

        if actions.is_empty() {
            return false;
        }

        let count = actions.len();
        let mut next_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);

        for (e_idx, _u, v, v_legs_involved, _target_lines) in actions {
            next_node_id += 1;
            let v_prime_id = next_node_id;

            // Create v'
            let v_node = graph.nodes[&v].clone();
            let mut v_prime = v_node.clone();
            v_prime.id = v_prime_id;
            graph.nodes.insert(v_prime_id, v_prime);

            // Move e: u -> v becomes u -> v'
            // We modify the existing edge e to point to v' instead of v
            {
                let edge = &mut graph.edges[e_idx];
                if edge.from == v {
                    edge.from = v_prime_id;
                } else if edge.to == v {
                    edge.to = v_prime_id;
                }
            }

            // Move involved legs from v to v'
            for leg_idx in v_legs_involved {
                let leg = &mut graph.edges[leg_idx];
                if leg.from == v {
                    leg.from = v_prime_id;
                } else if leg.to == v {
                    leg.to = v_prime_id;
                }
            }
        }

        println!(
            "Applied Untangling Rule 5 (Partial Double Y) on {} edges",
            count
        );
        true
    }

    fn untangle_rule_8_double_stump(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Rule 8: Double Stump
        // Requirement: Edge e = {u, v}
        // Lines L on e such that L are NOT present on any other leg at u AND NOT on any other leg at v.
        // Action: These lines are "isolated" on e. They should be moved to a separate component (e', u', v').
        // Unlike previous implementation which deleted them, we split them off to preserve data.

        let mut actions: Vec<(usize, Vec<String>)> = Vec::new();

        for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }
            let u = edge.from;
            let v = edge.to;

            let u_legs = &node_adj[&u];
            let v_legs = &node_adj[&v];

            // Find lines that are isolated on this edge
            // i.e. not present in any neighbour edge of u or v (excluding e itself)
            let double_stumps: Vec<String> = edge
                .lines
                .iter()
                .filter(|l| {
                    let in_u = u_legs.iter().any(|&leg| {
                        leg != e_idx
                            && graph.edges[leg]
                                .lines
                                .iter()
                                .any(|ll| ll.line_id == l.line_id)
                    });
                    let in_v = v_legs.iter().any(|&leg| {
                        leg != e_idx
                            && graph.edges[leg]
                                .lines
                                .iter()
                                .any(|ll| ll.line_id == l.line_id)
                    });
                    !in_u && !in_v
                })
                .map(|l| l.line_id.clone())
                .collect();

            if !double_stumps.is_empty() {
                // Determine if we should split.
                // If ALL lines are double stumps, the edge is already isolated (just a floating segment).
                // No need to split unless we want to detach it from u/v physically?
                // If the edge is physically connected to u and v, but logically carries isolated lines...
                // If it carries ONLY isolated lines, then u and v are just terminals for this segment.
                // But u and v might have other edges.
                // If u has other edges, this edge is attached to the "station" u.
                // If we split, we create u' and v'. u' is a new node at same loc.
                // This detaches the visual line from the station node if the station node is 'u'.
                // However, for line ordering, we want to separate flows.
                // If we don't split, these trivial lines might interfere with sorting other complex lines?
                // Actually Rule 8 specifically says to untangle them.
                if double_stumps.len() < edge.lines.len() {
                    // Only split if it's a subset. If it's all lines, it's already a separate component effectively
                    // (though sharing nodes).
                    // Actually, if we share nodes, we induce constraints. detaching removes constraints.
                    actions.push((e_idx, double_stumps));
                } else {
                    // If all lines are isolated, we might still want to detach if u/v have other edges.
                    // This reduces degree of u/v for other calculations.
                    if u_legs.len() > 1 || v_legs.len() > 1 {
                        actions.push((e_idx, double_stumps));
                    }
                }
            }
        }

        if actions.is_empty() {
            return false;
        }

        let count = actions.len();
        let mut max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);
        let mut max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);

        for (e_idx, stump_lines) in actions {
            // Split off stump_lines into e' = {u', v'}
            let edge_data = graph.edges[e_idx].clone();
            let u = edge_data.from;
            let v = edge_data.to;

            max_node_id += 1;
            let u_prime_id = max_node_id;
            max_node_id += 1;
            let v_prime_id = max_node_id;

            // Clone nodes
            let mut u_prime = graph.nodes[&u].clone();
            u_prime.id = u_prime_id;
            let mut v_prime = graph.nodes[&v].clone();
            v_prime.id = v_prime_id;

            graph.nodes.insert(u_prime_id, u_prime);
            graph.nodes.insert(v_prime_id, v_prime);

            // Create e'
            max_edge_id += 1;
            let mut e_prime = edge_data.clone();
            e_prime.id = max_edge_id;
            e_prime.from = u_prime_id;
            e_prime.to = v_prime_id;

            // Keep only stump lines on e'
            e_prime.lines.retain(|l| stump_lines.contains(&l.line_id));
            graph.edges.push(e_prime);

            // Remove stump lines from original e
            graph.edges[e_idx]
                .lines
                .retain(|l| !stump_lines.contains(&l.line_id));
        }

        println!(
            "Applied Untangling Rule 8 (Double Stump) on {} edges",
            count
        );
        true
    }

    fn untangle_rule_6_outer_stump(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Rule 6: Outer/Inner Stump
        // Requirement: Edge e = {u, v}
        // A leg at u (L_u) has lines that are a subset of e, AND all those lines terminate at v.
        // Action: Split u->u', v->v', e->e'. Move L_u to u'. e' connects u'-v'.
        // Effectively peels off the terminating flow.

        let mut actions: Vec<(usize, usize, bool)> = Vec::new(); // (e_idx, leg_idx, stump_at_u)
        let mut touched_edges: HashSet<usize> = HashSet::new();
        let mut touched_legs: HashSet<usize> = HashSet::new();

        for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }
            if touched_edges.contains(&e_idx) {
                continue;
            }

            let u = edge.from;
            let v = edge.to;
            let u_legs = &node_adj[&u];
            let v_legs = &node_adj[&v];

            // Check u legs for stumpiness
            // Requirement from C++ Loom: n->getDeg() >= 3 && other->getDeg() >= 2
            if u_legs.len() >= 3 && v_legs.len() >= 2 {
                for &leg in u_legs {
                    if leg == e_idx {
                        continue;
                    }
                    if touched_edges.contains(&leg) || touched_legs.contains(&leg) {
                        continue;
                    }
                    let leg_edge = &graph.edges[leg];
                    if leg_edge.lines.is_empty() {
                        continue;
                    }

                    // 1. Subset of e?
                    let all_in_e = leg_edge
                        .lines
                        .iter()
                        .all(|l| edge.lines.iter().any(|el| el.line_id == l.line_id));

                    if all_in_e {
                        // 2. Terminate at v?
                        // i.e. None of these lines are present on any OTHER leg at v (excluding e)
                        let terminates_at_v = leg_edge.lines.iter().all(|l| {
                            !v_legs.iter().any(|&vl| {
                                vl != e_idx
                                    && graph.edges[vl]
                                        .lines
                                        .iter()
                                        .any(|vll| vll.line_id == l.line_id)
                            })
                        });

                        if terminates_at_v {
                            actions.push((e_idx, leg, true));
                            touched_edges.insert(e_idx);
                            touched_legs.insert(leg);
                            break; // Only one split per edge per pass to avoid complexity
                        }
                    }
                }
            }

            if touched_edges.contains(&e_idx) {
                continue;
            }

            // Check v legs for stumpiness
            // Requirement from C++ Loom: n->getDeg() >= 3 && other->getDeg() >= 2
            if v_legs.len() >= 3 && u_legs.len() >= 2 {
                for &leg in v_legs {
                    if leg == e_idx {
                        continue;
                    }
                    if touched_edges.contains(&leg) || touched_legs.contains(&leg) {
                        continue;
                    }
                    let leg_edge = &graph.edges[leg];
                    if leg_edge.lines.is_empty() {
                        continue;
                    }

                    // 1. Subset of e?
                    let all_in_e = leg_edge
                        .lines
                        .iter()
                        .all(|l| edge.lines.iter().any(|el| el.line_id == l.line_id));

                    if all_in_e {
                        // 2. Terminate at u?
                        let terminates_at_u = leg_edge.lines.iter().all(|l| {
                            !u_legs.iter().any(|&ul| {
                                ul != e_idx
                                    && graph.edges[ul]
                                        .lines
                                        .iter()
                                        .any(|ull| ull.line_id == l.line_id)
                            })
                        });

                        if terminates_at_u {
                            actions.push((e_idx, leg, false));
                            touched_edges.insert(e_idx);
                            touched_legs.insert(leg);
                            break;
                        }
                    }
                }
            }
        }

        if actions.is_empty() {
            return false;
        }

        let count = actions.len();
        let mut max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);
        let mut max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);

        for (e_idx, leg_idx, stump_at_u) in actions {
            let edge_data = graph.edges[e_idx].clone();
            let u = edge_data.from;
            let v = edge_data.to;

            // Lines to peel off
            let leg_lines: Vec<String> = graph.edges[leg_idx]
                .lines
                .iter()
                .map(|l| l.line_id.clone())
                .collect();

            // Create new nodes u', v'
            max_node_id += 1;
            let u_prime_id = max_node_id;
            max_node_id += 1;
            let v_prime_id = max_node_id;

            let mut u_prime = graph.nodes[&u].clone();
            u_prime.id = u_prime_id;
            let mut v_prime = graph.nodes[&v].clone();
            v_prime.id = v_prime_id;

            graph.nodes.insert(u_prime_id, u_prime);
            graph.nodes.insert(v_prime_id, v_prime);

            // Create e' = {u', v'}
            max_edge_id += 1;
            let mut e_prime = edge_data.clone();
            e_prime.id = max_edge_id;
            e_prime.from = u_prime_id;
            e_prime.to = v_prime_id;
            // e' gets ONLY the lines from the stump leg
            e_prime.lines.retain(|l| leg_lines.contains(&l.line_id));
            graph.edges.push(e_prime);

            // Remove those lines from original e
            graph.edges[e_idx]
                .lines
                .retain(|l| !leg_lines.contains(&l.line_id));

            // Re-parent the stump leg
            let leg = &mut graph.edges[leg_idx];
            if stump_at_u {
                // Leg was at u, move to u'
                if leg.from == u {
                    leg.from = u_prime_id;
                } else if leg.to == u {
                    leg.to = u_prime_id;
                }
            } else {
                // Leg was at v, move to v'
                if leg.from == v {
                    leg.from = v_prime_id;
                } else if leg.to == v {
                    leg.to = v_prime_id;
                }
            }
        }

        println!("Applied Untangling Rule 6 (Outer Stump) on {} edges", count);
        true
    }

    fn untangle_rule_7_inner_stump(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Rule 7: Inner Stump (C++ untangleInnerStump)
        // Detect internal dead-end flows between nodes u and v connected by "leg" edge.
        // An "inner stump" occurs when:
        // - Edge e (leg) connects u and v with deg(u) >= 3 and deg(v) >= 3
        // - Some lines on e branch at u into specific edges, and those lines terminate at v
        //   (i.e., they don't continue past v on any other edge)
        // Action: Split u->u', v->v'. Move the inner stump flow to u'-v'.

        let mut actions: Vec<(usize, i64, i64, Vec<String>)> = Vec::new(); // (e_idx, u, v, stump_lines)
        let mut touched_edges: HashSet<usize> = HashSet::new();

        for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() || touched_edges.contains(&e_idx) {
                continue;
            }

            let u = edge.from;
            let v = edge.to;
            let u_legs = node_adj.get(&u).map(|x| x.as_slice()).unwrap_or(&[]);
            let v_legs = node_adj.get(&v).map(|x| x.as_slice()).unwrap_or(&[]);

            // Need deg >= 3 at both ends
            if u_legs.len() < 3 || v_legs.len() < 3 {
                continue;
            }

            // Find lines on e that:
            // 1. Branch from u into specific other edges at u
            // 2. Terminate at v (don't continue on any other edge at v)
            let mut stump_lines: Vec<String> = Vec::new();

            for line in &edge.lines {
                // Check if this line terminates at v
                let terminates_at_v = !v_legs.iter().any(|&vl_idx| {
                    vl_idx != e_idx
                        && graph.edges[vl_idx]
                            .lines
                            .iter()
                            .any(|l| l.line_id == line.line_id)
                });

                // Check if this line comes from another edge at u (not just terminating at u)
                let comes_from_u = u_legs.iter().any(|&ul_idx| {
                    ul_idx != e_idx
                        && graph.edges[ul_idx]
                            .lines
                            .iter()
                            .any(|l| l.line_id == line.line_id)
                });

                if terminates_at_v && comes_from_u {
                    stump_lines.push(line.line_id.clone());
                }
            }

            // Need at least one stump line, and not ALL lines (otherwise use other rules)
            if !stump_lines.is_empty() && stump_lines.len() < edge.lines.len() {
                actions.push((e_idx, u, v, stump_lines));
                touched_edges.insert(e_idx);
            }
        }

        if actions.is_empty() {
            return false;
        }

        let count = actions.len();
        let mut max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);
        let mut max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);

        for (e_idx, u, v, stump_lines) in actions {
            let edge_data = graph.edges[e_idx].clone();

            // Create u' and v'
            max_node_id += 1;
            let u_prime_id = max_node_id;
            max_node_id += 1;
            let v_prime_id = max_node_id;

            let mut u_prime = graph.nodes[&u].clone();
            u_prime.id = u_prime_id;
            let mut v_prime = graph.nodes[&v].clone();
            v_prime.id = v_prime_id;

            graph.nodes.insert(u_prime_id, u_prime);
            graph.nodes.insert(v_prime_id, v_prime);

            // Create e' = {u', v'} with stump lines only
            max_edge_id += 1;
            let mut e_prime = edge_data.clone();
            e_prime.id = max_edge_id;
            e_prime.from = u_prime_id;
            e_prime.to = v_prime_id;
            e_prime.lines.retain(|l| stump_lines.contains(&l.line_id));
            graph.edges.push(e_prime);

            // Remove stump lines from original edge
            graph.edges[e_idx]
                .lines
                .retain(|l| !stump_lines.contains(&l.line_id));

            // Move the source edges at u (that carry stump lines) to u'
            let u_legs = node_adj.get(&u).map(|x| x.clone()).unwrap_or_default();
            for ul_idx in u_legs {
                if ul_idx == e_idx {
                    continue;
                }
                let ul_edge = &graph.edges[ul_idx];
                // Check if this edge carries any stump lines
                let carries_stump = ul_edge
                    .lines
                    .iter()
                    .any(|l| stump_lines.contains(&l.line_id));

                if carries_stump {
                    // Move this edge endpoint from u to u'
                    let ul = &mut graph.edges[ul_idx];
                    if ul.from == u {
                        ul.from = u_prime_id;
                    } else if ul.to == u {
                        ul.to = u_prime_id;
                    }
                }
            }
        }

        println!("Applied Untangling Rule 7 (Inner Stump) on {} edges", count);
        true
    }

    fn split_by_articulation_points(
        &self,
        component: &[usize],
        adj: &HashMap<usize, Vec<usize>>,
    ) -> Vec<Vec<usize>> {
        // Implementation of Tarjan's Articulation Point Algorithm to split large components.
        // If no AP found or component small enough, returns vec![component].
        // If APs found, splits at the "best" AP (most balanced split) and returns sub-components.
        // The AP node itself is duplicated into each sub-component to maintain continuity constraints at the cut.

        let n = component.len();
        if n < 5 {
            // Too small to split
            return vec![component.to_vec()];
        }

        // Map component node ID -> index 0..n
        let mut node_to_idx = HashMap::new();
        for (i, &u) in component.iter().enumerate() {
            node_to_idx.insert(u, i);
        }

        // Tarjan's State
        let mut discovery = vec![None; n];
        let mut low = vec![0; n];
        let mut time = 0;
        let mut aps = HashSet::new(); // Set of articulation points (indices in 'component')

        // DFS Stack: (u_idx, p_idx_opt, iter_neighbors)
        // We use iterative DFS to avoid stack overflow on deep graphs.
        // Actually, recursive is easier to write, let's try iterative if possible or careful recursive.
        // Given stack size defaults, 100-300 depth might be fine.
        // Let's stick to recursive for clarity, assuming N ~ 300-1000 isn't stack blowing.

        // Helper for DFS
        fn dfs(
            u_idx: usize,
            p_idx: Option<usize>,
            time: &mut usize,
            discovery: &mut Vec<Option<usize>>,
            low: &mut Vec<usize>,
            aps: &mut HashSet<usize>,
            component: &[usize],
            adj: &HashMap<usize, Vec<usize>>,
            node_to_idx: &HashMap<usize, usize>,
        ) {
            let children = 0; // handled by counting distinct branches in caller if root
            discovery[u_idx] = Some(*time);
            low[u_idx] = *time;
            *time += 1;

            let u = component[u_idx];
            if let Some(neighbors) = adj.get(&u) {
                let mut child_count = 0;
                for &v in neighbors {
                    if let Some(&v_idx) = node_to_idx.get(&v) {
                        if Some(v_idx) == p_idx {
                            continue;
                        }
                        if discovery[v_idx].is_some() {
                            // Back edge
                            low[u_idx] = std::cmp::min(low[u_idx], discovery[v_idx].unwrap());
                        } else {
                            // Tree edge
                            child_count += 1;
                            dfs(
                                v_idx,
                                Some(u_idx),
                                time,
                                discovery,
                                low,
                                aps,
                                component,
                                adj,
                                node_to_idx,
                            );
                            low[u_idx] = std::cmp::min(low[u_idx], low[v_idx]);

                            if p_idx.is_some() && low[v_idx] >= discovery[u_idx].unwrap() {
                                aps.insert(u_idx);
                            }
                        }
                    }
                }
                if p_idx.is_none() && child_count > 1 {
                    aps.insert(u_idx);
                }
            }
        }

        // Run DFS from 0 (assuming connected component, but loop just in case)
        if let Some(root_idx) = (0..n).find(|&i| discovery[i].is_none()) {
            // We assume 'component' is fully connected as passed by caller.
            // If not, standard AP finds APs in connected component.
            dfs(
                root_idx,
                None,
                &mut time,
                &mut discovery,
                &mut low,
                &mut aps,
                component,
                adj,
                &node_to_idx,
            );
        }

        if aps.is_empty() {
            return vec![component.to_vec()];
        }

        // Find the "Best" AP to split on.
        // Heuristic: removing AP creates largest components that are smaller than original.
        // We want to minimize the max size of resulting connected components.
        // For simplicity, let's just pick the first one, or one that isn't too close to edges?
        // Let's evaluate split sizes for all APs.

        let mut best_ap = None;
        let mut min_max_comp_size = n;

        for &ap_idx in &aps {
            // Simulate removal
            let mut visited = HashSet::new();
            visited.insert(ap_idx); // "Removed"

            let mut max_sub_size = 0;
            // Iterate all nodes, bfs to find component size
            for start_idx in 0..n {
                if visited.contains(&start_idx) {
                    continue;
                }

                let mut q = VecDeque::new();
                q.push_back(start_idx);
                visited.insert(start_idx);
                let mut size = 0;

                while let Some(curr_idx) = q.pop_front() {
                    size += 1;
                    let u = component[curr_idx];
                    if let Some(neighbors) = adj.get(&u) {
                        for &v in neighbors {
                            if let Some(&v_idx) = node_to_idx.get(&v) {
                                if !visited.contains(&v_idx) {
                                    visited.insert(v_idx);
                                    q.push_back(v_idx);
                                }
                            }
                        }
                    }
                }
                if size > max_sub_size {
                    max_sub_size = size;
                }
            }

            // Score: lower max_sub_size is better
            if max_sub_size < min_max_comp_size {
                min_max_comp_size = max_sub_size;
                best_ap = Some(ap_idx);
            }
        }

        if let Some(ap_idx) = best_ap {
            // Perform actual split
            let mut results = Vec::new();
            let mut visited = HashSet::new();
            visited.insert(ap_idx); // Mark AP as visited initially so traversal doesn't cross it

            for start_idx in 0..n {
                if visited.contains(&start_idx) {
                    continue;
                }

                let mut sub_comp = Vec::new();
                // Add AP to this sub-component too (boundary)
                sub_comp.push(component[ap_idx]);

                let mut q = VecDeque::new();
                q.push_back(start_idx);
                visited.insert(start_idx);

                while let Some(curr_idx) = q.pop_front() {
                    sub_comp.push(component[curr_idx]);

                    let u = component[curr_idx];
                    if let Some(neighbors) = adj.get(&u) {
                        for &v in neighbors {
                            if let Some(&v_idx) = node_to_idx.get(&v) {
                                // Important: We can "see" the AP, but we don't traverse *through* it.
                                // If neighbour is AP, we don't push to Q (it's marked visited).
                                // But effectively AP is part of this component.
                                // Our `visited` logic handles this: AP is in `visited`.
                                if !visited.contains(&v_idx) {
                                    visited.insert(v_idx);
                                    q.push_back(v_idx);
                                }
                            }
                        }
                    }
                }

                // If the sub-component is just [AP], ignore it (singleton artifact?)
                if sub_comp.len() > 1 {
                    results.push(sub_comp);
                }
            }

            // log the split
            println!(
                "Split component of size {} into {} parts at AP {}. Max part size: {}",
                n,
                results.len(),
                component[ap_idx],
                min_max_comp_size
            );
            return results;
        }

        vec![component.to_vec()]
    }

    fn cutting_rule_1_single_line_cut(&self, graph: &mut RenderGraph) -> bool {
        // Cutting Rule 1: Single Line Cut
        // If |L(e)| == 1, and deg(u) > 1, deg(v) > 1
        // Cut e into e'={u, v_new1} and e''={v_new2, v}
        // This effectively disconnects the two components connected by this single line.
        // Returns true if changed.

        let node_adj = self.build_node_adjacency(graph);
        let mut edges_to_remove = HashSet::new();
        let mut edges_to_add = Vec::new();
        let mut nodes_to_add = HashMap::new(); // id -> Node
        let mut max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);
        let mut max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);

        let mut changed = false;

        for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.len() != 1 {
                continue;
            }

            // Check degrees
            let deg_u = node_adj.get(&edge.from).map(|v| v.len()).unwrap_or(0);
            let deg_v = node_adj.get(&edge.to).map(|v| v.len()).unwrap_or(0);

            if deg_u > 1 && deg_v > 1 {
                // CUT!
                edges_to_remove.insert(e_idx);

                // New nodes
                max_node_id += 1;
                let v_new1 = max_node_id;
                max_node_id += 1;
                let v_new2 = max_node_id;

                // Split geometry roughly in half?
                // Actually, physically they should meet visually, but graph-wise be distinct.
                // We can just duplicate the midpoint or split segment.
                // For rendering, we want them to look connected.
                // If we introduce a gap, it looks bad.
                // If we overlap, it's fine.
                // Let's use the exact midpoint for both new nodes.

                let geom_len = edge.geometry.len();
                let mid = geom_len / 2;
                let (geom1, geom2) = if geom_len >= 2 {
                    let split_point = edge.geometry[mid];
                    let mut g1 = edge.geometry[0..=mid].to_vec();
                    let mut g2 = edge.geometry[mid..].to_vec();
                    // Include split point in both so they touch
                    (g1, g2)
                } else {
                    (edge.geometry.clone(), edge.geometry.clone())
                };

                // Add new nodes (dummy location, taking midpoint)
                // We assume RenderGraph nodes are for topology mainly, but if they have coords:
                let mid_pt = geom1.last().unwrap_or(&[0.0, 0.0]).clone(); // simplified
                nodes_to_add.insert(
                    v_new1,
                    crate::graph::Node {
                        id: v_new1,
                        x: mid_pt[0],
                        y: mid_pt[1],
                        is_cluster: false,
                        name: None,
                        // Copy other props if any?
                    },
                );
                nodes_to_add.insert(
                    v_new2,
                    crate::graph::Node {
                        id: v_new2,
                        x: mid_pt[0],
                        y: mid_pt[1],
                        is_cluster: false,
                        name: None,
                    },
                );

                max_edge_id += 1;
                let e_prime = Edge {
                    id: max_edge_id,
                    from: edge.from,
                    to: v_new1,
                    lines: edge.lines.clone(),
                    geometry: geom1,
                    dir: edge.dir,
                };

                max_edge_id += 1;
                let e_double_prime = Edge {
                    id: max_edge_id,
                    from: v_new2,
                    to: edge.to,
                    lines: edge.lines.clone(),
                    geometry: geom2,
                    dir: edge.dir,
                };

                edges_to_add.push(e_prime);
                edges_to_add.push(e_double_prime);

                changed = true;
            }
        }

        if changed {
            println!(
                "Cutting Rule 1: Cut {} single-line bridges.",
                edges_to_remove.len()
            );
            // Apply updates

            // Add nodes
            graph.nodes.extend(nodes_to_add);

            // Remove old edges (filter)
            let mut i = 0;
            graph.edges.retain(|_| {
                let keep = !edges_to_remove.contains(&i);
                i += 1;
                keep
            });

            // Add new edges
            graph.edges.extend(edges_to_add);
        }

        changed
    }

    fn cutting_rule_2_terminus_detachment(&self, graph: &mut RenderGraph) -> bool {
        // Cutting Rule 2: Terminus Detachment
        // Detach e={u,v} from v if:
        // 1. v is a terminus for all lines in L(e).
        // 2. deg(v) > 1.

        // We need to know if a node is a terminus for a line.
        // A node v is a terminus for line l if v is one of the endpoints of the geometric path of l.
        // Or in the graph context: l is present on exactly one edge incident to v?
        // Let's check incidence.

        // Build map: LineID -> Set<EdgeID>
        let mut line_edges: HashMap<String, HashSet<i64>> = HashMap::new();
        for edge in &graph.edges {
            for l in &edge.lines {
                line_edges
                    .entry(l.line_id.clone())
                    .or_default()
                    .insert(edge.id);
            }
        }

        let node_adj = self.build_node_adjacency(graph);
        let mut edges_to_modify = Vec::new(); // (edge_index, new_to_node_id)
        let mut nodes_to_add = HashMap::new();
        let mut max_node_id = graph.nodes.keys().max().cloned().unwrap_or(0);

        let mut changed = false;

        for (e_idx, edge) in graph.edges.iter().enumerate() {
            // Check both endpoints u and v
            for &v in &[edge.from, edge.to] {
                // deg(v) > 1
                let deg = node_adj.get(&v).map(|l| l.len()).unwrap_or(0);
                if deg <= 1 {
                    continue;
                }

                // Check if v is terminus for all lines in edge
                let all_terminate = edge.lines.iter().all(|l| {
                    // logic: is v an endpoint of the line?
                    // A node is a terminus if the line only enters v from this edge and goes no further.
                    // i.e., this line is NOT present on any other edge incident to v.
                    if let Some(adj_edges) = node_adj.get(&v) {
                        for &other_e_idx in adj_edges {
                            if other_e_idx == e_idx {
                                continue;
                            }
                            let other_edge = &graph.edges[other_e_idx];
                            if other_edge.lines.iter().any(|ol| ol.line_id == l.line_id) {
                                return false; // Continues on another edge
                            }
                        }
                    }
                    true
                });

                if all_terminate {
                    // Detach!
                    // Create new node v'
                    // Update edge to point to v' instead of v

                    max_node_id += 1;
                    let v_prime = max_node_id;

                    // Get coords from v
                    let v_node = &graph.nodes[&v];
                    nodes_to_add.insert(
                        v_prime,
                        crate::graph::Node {
                            id: v_prime,
                            x: v_node.x, // Co-located initially, or slight offset?
                            y: v_node.y, // Paper says "positioned somewhere along original edge", effectively same place visually
                            is_cluster: false,
                            name: None,
                        },
                    );

                    edges_to_modify.push((e_idx, v, v_prime));
                    changed = true;
                    break; // Only detach one end at a time per edge to avoid confusion in loop
                }
            }
        }

        if changed {
            println!(
                "Cutting Rule 2: Detached {} termini.",
                edges_to_modify.len()
            );
            graph.nodes.extend(nodes_to_add);

            for (e_idx, old_v, new_v) in edges_to_modify {
                let edge = &mut graph.edges[e_idx];
                if edge.from == old_v {
                    edge.from = new_v;
                } else if edge.to == old_v {
                    edge.to = new_v;
                }
            }
        }

        changed
    }

    fn prune_rule_1_node_contraction(&self, graph: &mut RenderGraph) -> bool {
        // Pruning Rule 1: Node Contraction
        println!("Starting Pruning Rule 1 (Node Contraction)...");
        let mut any_changed = false;
        let mut rounds = 0;
        let mut changed = true;
        while changed {
            changed = false;
            rounds += 1;
            if rounds > 100 {
                break;
            }

            let node_adj = self.build_node_adjacency(graph);
            let mut edges_to_add = Vec::new();
            let mut edges_to_remove = HashSet::new(); // indices
            let mut nodes_to_remove = HashSet::new();

            // Collect candidates
            let mut nodes: Vec<i64> = graph.nodes.keys().cloned().collect();
            // Deterministic order
            nodes.sort();

            for &v in &nodes {
                if nodes_to_remove.contains(&v) {
                    continue;
                }
                let adj = if let Some(a) = node_adj.get(&v) {
                    a
                } else {
                    continue;
                };
                if adj.len() != 2 {
                    continue;
                }

                let e1_idx = adj[0];
                let e2_idx = adj[1];
                if edges_to_remove.contains(&e1_idx) || edges_to_remove.contains(&e2_idx) {
                    continue;
                }

                let e1 = &graph.edges[e1_idx];
                let e2 = &graph.edges[e2_idx];

                // Compare lines (must be sorted or set-equal)
                let mut l1_ids: Vec<String> = e1.lines.iter().map(|l| l.line_id.clone()).collect();
                let mut l2_ids: Vec<String> = e2.lines.iter().map(|l| l.line_id.clone()).collect();
                l1_ids.sort();
                l2_ids.sort();

                if l1_ids == l2_ids {
                    // Contract!
                    // u --e1-- v --e2-- w
                    let u = if e1.from == v { e1.to } else { e1.from };
                    let w = if e2.from == v { e2.to } else { e2.from };

                    if u == w {
                        continue;
                    } // Cycle (u--v--u)

                    // Create new edge e_new = {u,w}
                    // Combine geometry: e1(u->v) + e2(v->w)
                    let mut e1_geom = e1.geometry.clone();
                    if e1.to == v { /* already u->v */
                    } else {
                        e1_geom.reverse();
                    }

                    let mut e2_geom = e2.geometry.clone();
                    if e2.from == v { /* already v->w */
                    } else {
                        e2_geom.reverse();
                    }

                    // Concat
                    let mut new_geom = e1_geom;
                    if let Some(last) = new_geom.last() {
                        if let Some(first) = e2_geom.first() {
                            if (last[0] - first[0]).abs() < 1e-6
                                && (last[1] - first[1]).abs() < 1e-6
                            {
                                e2_geom.remove(0);
                            }
                        }
                    }
                    new_geom.extend(e2_geom);

                    let max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);
                    let new_id = max_edge_id + 1 + (edges_to_add.len() as i64);

                    let new_edge = Edge {
                        id: new_id,
                        from: u,
                        to: w,
                        lines: e1.lines.clone(),
                        geometry: new_geom,
                        dir: e1.dir,
                    };

                    edges_to_add.push(new_edge);
                    edges_to_remove.insert(e1_idx);
                    edges_to_remove.insert(e2_idx);
                    nodes_to_remove.insert(v);

                    changed = true;
                    any_changed = true;
                }
            }

            if !changed {
                break;
            }

            // Apply changes
            graph.edges.extend(edges_to_add);

            // Clean up dead nodes/edges
            let mut i = 0;
            graph.edges.retain(|_| {
                let keep = !edges_to_remove.contains(&i);
                i += 1;
                keep
            });
            graph.nodes.retain(|k, _| !nodes_to_remove.contains(k));
        }
        println!("Pruning Rule 1 Complete.");
        any_changed
    }

    fn prune_rule_2_line_partner_collapse(&self, graph: &mut RenderGraph) {
        // Pruning Rule 2: Line Partner Collapse
        println!("Starting Pruning Rule 2 (Line Partner Collapse)...");

        let mut line_paths: HashMap<String, Vec<i64>> = HashMap::new();
        // Just iterate edges and build up the path
        for edge in &graph.edges {
            for l in &edge.lines {
                line_paths
                    .entry(l.line_id.clone())
                    .or_default()
                    .push(edge.id);
            }
        }

        // Sort paths for comparison
        for path in line_paths.values_mut() {
            path.sort();
        }

        // Invert: Path -> Vec<LineID>
        let mut path_to_lines: HashMap<Vec<i64>, Vec<String>> = HashMap::new();
        for (lid, path) in line_paths {
            path_to_lines.entry(path).or_default().push(lid);
        }

        // For groups > 1, bundle!
        let mut collapse_count = 0;

        // Build efficient lookup
        let mut line_to_super: HashMap<String, String> = HashMap::new();
        for (path, lids) in &path_to_lines {
            if lids.len() < 2 {
                continue;
            }
            if path.len() < 2 {
                continue;
            } // Skip collapsing if only 1 edge (optional, but saves complexity)

            let leader_id = &lids[0];
            let super_id = format!("SUPER_{}", leader_id);
            for lid in lids {
                line_to_super.insert(lid.clone(), super_id.clone());
            }
            collapse_count += 1;
        }

        if collapse_count == 0 {
            return;
        }

        // Mutate graph
        for edge in &mut graph.edges {
            let mut new_lines = Vec::new();
            let mut processed = HashSet::new();

            for l in &edge.lines {
                if processed.contains(&l.line_id) {
                    continue;
                }

                if let Some(super_id) = line_to_super.get(&l.line_id) {
                    // This line is part of a bundle.
                    if processed.contains(super_id) {
                        continue;
                    }

                    // Combine weights
                    let mut super_weight = 0;
                    let mut constituents = Vec::new(); // We need to store original lines for unbundling

                    // Scan edge lines to find all parts of this bundle
                    for el in &edge.lines {
                        if let Some(s_id) = line_to_super.get(&el.line_id) {
                            if s_id == super_id {
                                super_weight += el.weight;
                                constituents.push(el.clone());
                                processed.insert(el.line_id.clone());
                            }
                        }
                    }

                    // Store/Cache constituents globally if not done yet
                    // Note: We use the first encounter to store the components.
                    // Since they are identical on all edges (path match), any edge is fine.
                    if !graph.collapsed_lines.contains_key(super_id) {
                        graph.collapsed_lines.insert(super_id.clone(), constituents);
                    }

                    // Create Super Line Object
                    let mut super_line = edge
                        .lines
                        .iter()
                        .find(|el| el.line_id == l.line_id)
                        .unwrap()
                        .clone();
                    super_line.line_id = super_id.clone();
                    super_line.weight = super_weight;

                    new_lines.push(super_line);
                    processed.insert(super_id.clone());
                } else {
                    new_lines.push(l.clone());
                }
            }
            edge.lines = new_lines;
        }

        println!(
            "Pruning Rule 2 Complete. Collapsed {} groups.",
            collapse_count
        );
    }

    // ============================================================================
    // GREEDY + HILL CLIMBING OPTIMIZER (matching C++ loom)
    // ============================================================================

    /// Get edges around a node in clockwise order, starting from the "noon" edge.
    /// Matches C++ loom's OptGraph::clockwEdges
    fn clockwise_edges(
        &self,
        graph: &RenderGraph,
        noon_edge_idx: usize,
        node_id: i64,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> Vec<usize> {
        let connected = match node_to_edges.get(&node_id) {
            Some(c) => c,
            None => return vec![],
        };

        if connected.len() <= 1 {
            return connected
                .iter()
                .filter(|&&e| e != noon_edge_idx)
                .cloned()
                .collect();
        }

        // Calculate angles for each edge relative to the node
        let node = match graph.nodes.get(&node_id) {
            Some(n) => n,
            None => return vec![],
        };

        let mut edge_angles: Vec<(usize, f64)> = connected
            .iter()
            .map(|&edge_idx| {
                let edge = &graph.edges[edge_idx];
                let default = [node.x, node.y];
                let (dx, dy) = if edge.from == node_id {
                    let p1 = edge.geometry.get(0).unwrap_or(&default);
                    let p2 = edge.geometry.get(1).unwrap_or(&default);
                    (p2[0] - p1[0], p2[1] - p1[1])
                } else {
                    let p_last = edge.geometry.last().unwrap_or(&default);
                    let p_prev = edge
                        .geometry
                        .get(edge.geometry.len().saturating_sub(2))
                        .unwrap_or(&default);
                    (p_prev[0] - p_last[0], p_prev[1] - p_last[1])
                };
                let angle = dy.atan2(dx);
                let angle = if angle < 0.0 { angle + 2.0 * PI } else { angle };
                (edge_idx, angle)
            })
            .collect();

        edge_angles.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Find noon edge position
        let noon_pos = edge_angles
            .iter()
            .position(|(idx, _)| *idx == noon_edge_idx);

        // Reorder starting after noon in clockwise order
        let mut result = Vec::new();
        if let Some(pos) = noon_pos {
            // Add edges after noon (clockwise = increasing angle order)
            for i in (pos + 1)..edge_angles.len() {
                if edge_angles[i].0 != noon_edge_idx {
                    result.push(edge_angles[i].0);
                }
            }
            // Wrap around
            for i in 0..pos {
                if edge_angles[i].0 != noon_edge_idx {
                    result.push(edge_angles[i].0);
                }
            }
        } else {
            // noon not found, return all except noon
            result = edge_angles
                .into_iter()
                .filter(|(idx, _)| *idx != noon_edge_idx)
                .map(|(idx, _)| idx)
                .collect();
        }

        result
    }

    /// Get the initial edge for greedy algorithm: highest cardinality edge.
    /// Matches C++ loom's GreedyOptimizer::getInitialEdge
    fn get_initial_edge(&self, component: &[usize], graph: &RenderGraph) -> Option<usize> {
        component
            .iter()
            .filter(|&&idx| graph.edges[idx].lines.len() >= 2)
            .max_by_key(|&&idx| graph.edges[idx].lines.len())
            .cloned()
    }

    /// Get next unsettled edge adjacent to the settled set.
    /// Matches C++ loom's GreedyOptimizer::getNextEdge
    fn get_next_edge(
        &self,
        graph: &RenderGraph,
        settled: &HashSet<usize>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
        component_set: &HashSet<usize>,
    ) -> Option<usize> {
        for &edge_idx in settled.iter() {
            let edge = &graph.edges[edge_idx];

            // Check adjacent edges at 'from' node
            if let Some(adj_edges) = node_to_edges.get(&edge.from) {
                for &adj_idx in adj_edges {
                    if !settled.contains(&adj_idx) && component_set.contains(&adj_idx) {
                        return Some(adj_idx);
                    }
                }
            }

            // Check adjacent edges at 'to' node
            if let Some(adj_edges) = node_to_edges.get(&edge.to) {
                for &adj_idx in adj_edges {
                    if !settled.contains(&adj_idx) && component_set.contains(&adj_idx) {
                        return Some(adj_idx);
                    }
                }
            }
        }
        None
    }

    /// Determine if line A should come before line B based on already-settled edges.
    /// Returns (decision, cost):
    ///   decision: 1 = A < B, -1 = B < A, 0 = undecided
    ///   cost: penalty weight
    /// Matches C++ loom's GreedyOptimizer::smallerThanAt
    fn smaller_than_at(
        &self,
        graph: &RenderGraph,
        line_a: &str,
        line_b: &str,
        start_edge_idx: usize,
        node_id: i64,
        ignore_edge_idx: usize,
        cfg: &HashMap<usize, Vec<LineOnEdge>>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> (i32, f64) {
        let clockwise = self.clockwise_edges(graph, start_edge_idx, node_id, node_to_edges);

        let mut positions_a: Vec<usize> = Vec::new();
        let mut positions_b: Vec<usize> = Vec::new();
        let mut cost = 0.0;
        let mut offset = 0usize;

        for edge_idx in clockwise {
            if edge_idx == ignore_edge_idx {
                continue;
            }

            let edge = &graph.edges[edge_idx];
            let has_a = edge.lines.iter().any(|l| l.line_id == line_a);
            let has_b = edge.lines.iter().any(|l| l.line_id == line_b);

            if has_a && has_b {
                // Both lines on this edge - check settled ordering
                if let Some(ordered_lines) = cfg.get(&edge_idx) {
                    let rev = (edge.from != node_id) ^ edge.dir;

                    let pos_a = ordered_lines.iter().position(|l| l.line_id == line_a);
                    let pos_b = ordered_lines.iter().position(|l| l.line_id == line_b);

                    if let (Some(pa), Some(pb)) = (pos_a, pos_b) {
                        let n = ordered_lines.len();
                        if rev {
                            positions_a.push(offset + pa);
                            positions_b.push(offset + pb);
                        } else {
                            positions_a.push(offset + (n - 1 - pa));
                            positions_b.push(offset + (n - 1 - pb));
                        }
                        // Crossing penalty for same-segment (both lines on edge)
                        cost += 40.0;
                    }
                }
            } else if has_a {
                positions_a.push(offset);
                cost += 10.0; // Different segment penalty
            } else if has_b {
                positions_b.push(offset);
                cost += 10.0;
            }

            offset += edge.lines.len();
        }

        if positions_a.is_empty() || positions_b.is_empty() {
            return (0, 0.0);
        }

        // Check if all A positions come before all B positions
        if let (Some(&max_a), Some(&min_b)) = (positions_a.iter().max(), positions_b.iter().min()) {
            if max_a < min_b {
                return (1, cost); // A < B
            }
        }
        if let (Some(&min_a), Some(&max_b)) = (positions_a.iter().min(), positions_b.iter().max()) {
            if min_a > max_b {
                return (-1, cost); // B < A
            }
        }

        (0, 0.0) // Undecided
    }

    /// Guess whether line A should come before line B on an edge.
    /// Walks through adjacent settled edges to make the decision.
    /// Matches C++ loom's GreedyOptimizer::guess
    fn guess_order(
        &self,
        graph: &RenderGraph,
        line_a: &str,
        line_b: &str,
        start_edge_idx: usize,
        ref_node_id: i64,
        cfg: &HashMap<usize, Vec<LineOnEdge>>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> (bool, f64) {
        let edge = &graph.edges[start_edge_idx];
        let mut dec = 0i32;
        let mut cost = 0.0;
        let mut not_ref = false;

        // Try from ref node first
        let result = self.smaller_than_at(
            graph,
            line_a,
            line_b,
            start_edge_idx,
            ref_node_id,
            start_edge_idx,
            cfg,
            node_to_edges,
        );
        if result.0 != 0 {
            dec = result.0;
            cost = result.1;
        }

        // If undecided, try from other node
        if dec == 0 {
            let other_node_id = if edge.from == ref_node_id {
                edge.to
            } else {
                edge.from
            };
            let result = self.smaller_than_at(
                graph,
                line_a,
                line_b,
                start_edge_idx,
                other_node_id,
                start_edge_idx,
                cfg,
                node_to_edges,
            );
            if result.0 != 0 {
                dec = result.0;
                cost = result.1;
                not_ref = true;
            }
        }

        // Build return value with direction adjustment
        let rev = !edge.dir;

        if dec == 1 {
            return (not_ref ^ rev, cost);
        }
        if dec == -1 {
            return (!not_ref ^ rev, cost);
        }

        // Undecided - fall back to line ID comparison
        (line_a < line_b, 0.0)
    }

    /// Greedy ordering for a component.
    /// Implements C++ loom's GreedyOptimizer::getFlatConfig
    pub fn greedy_order_component(
        &self,
        graph: &RenderGraph,
        component: &[usize],
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> HashMap<usize, Vec<LineOnEdge>> {
        let mut cfg: HashMap<usize, Vec<LineOnEdge>> = HashMap::new();
        let mut settled: HashSet<usize> = HashSet::new();
        let component_set: HashSet<usize> = component.iter().cloned().collect();

        // Get initial edge (highest cardinality)
        let initial = match self.get_initial_edge(component, graph) {
            Some(e) => e,
            None => {
                // No suitable edge, just return lines in existing order
                for &edge_idx in component {
                    cfg.insert(edge_idx, graph.edges[edge_idx].lines.clone());
                }
                return cfg;
            }
        };

        // Process edges one by one
        let mut current_edge = Some(initial);

        while let Some(edge_idx) = current_edge {
            let edge = &graph.edges[edge_idx];

            // Build comparison map for left and right orientations
            // Cmp: (line_a, line_b) -> (should_a_be_first, cost)
            let mut left: HashMap<(String, String), (bool, f64)> = HashMap::new();
            let mut right: HashMap<(String, String), (bool, f64)> = HashMap::new();

            for lo1 in &edge.lines {
                for lo2 in &edge.lines {
                    if lo1.line_id == lo2.line_id {
                        continue;
                    }
                    let key = (lo1.line_id.clone(), lo2.line_id.clone());
                    left.insert(
                        key.clone(),
                        self.guess_order(
                            graph,
                            &lo1.line_id,
                            &lo2.line_id,
                            edge_idx,
                            edge.from,
                            &cfg,
                            node_to_edges,
                        ),
                    );
                    right.insert(
                        key,
                        self.guess_order(
                            graph,
                            &lo1.line_id,
                            &lo2.line_id,
                            edge_idx,
                            edge.to,
                            &cfg,
                            node_to_edges,
                        ),
                    );
                }
            }

            // Calculate costs for left vs right
            let mut cost_left = 0.0;
            let mut cost_right = 0.0;

            for lo1 in &edge.lines {
                for lo2 in &edge.lines {
                    if lo1.line_id == lo2.line_id {
                        continue;
                    }
                    let key = (lo1.line_id.clone(), lo2.line_id.clone());
                    if let (Some(l), Some(r)) = (left.get(&key), right.get(&key)) {
                        if l.0 == r.0 {
                            // Agreement - add other side's cost as penalty
                            cost_left += r.1;
                            cost_right += l.1;
                        }
                    }
                }
            }

            // Choose the cheaper orientation
            let use_left = cost_left < cost_right;

            // Sort lines using insertion sort - avoids Rust's sort_by total order validation
            // which panics on non-transitive comparisons (which can happen with greedy guessing)
            // C++ std::sort has undefined behavior with non-transitive comparisons but doesn't panic;
            // insertion sort gives us similar tolerance.
            let mut lines = edge.lines.clone();
            let cmp_map = if use_left { &left } else { &right };
            let rev = !use_left; // right uses rev=true in C++ loom

            // Custom insertion sort that handles potentially non-transitive comparisons
            for i in 1..lines.len() {
                let mut j = i;
                while j > 0 {
                    let key = (lines[j].line_id.clone(), lines[j - 1].line_id.clone());
                    let should_swap = if lines[j].line_id == lines[j - 1].line_id {
                        false // equal elements stay in place
                    } else if let Some((is_less, _)) = cmp_map.get(&key) {
                        // C++ loom: return _map.find({a, b})->second.first ^ _rev
                        // is_less here means lines[j] < lines[j-1]
                        *is_less ^ rev
                    } else {
                        // Fallback to lexicographic ordering
                        lines[j].line_id < lines[j - 1].line_id
                    };

                    if should_swap {
                        lines.swap(j, j - 1);
                        j -= 1;
                    } else {
                        break;
                    }
                }
            }

            cfg.insert(edge_idx, lines);
            settled.insert(edge_idx);

            // Get next edge
            current_edge = self.get_next_edge(graph, &settled, node_to_edges, &component_set);
        }

        // Handle any edges not reached (disconnected parts)
        for &edge_idx in component {
            if !cfg.contains_key(&edge_idx) {
                cfg.insert(edge_idx, graph.edges[edge_idx].lines.clone());
            }
        }

        cfg
    }

    /// Count crossings at a single node for a given edge ordering config.
    /// Returns (same_segment_crossings, diff_segment_crossings)
    fn count_crossings_at_node(
        &self,
        graph: &RenderGraph,
        node_id: i64,
        cfg: &HashMap<usize, Vec<LineOnEdge>>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> (usize, usize) {
        let edges = match node_to_edges.get(&node_id) {
            Some(e) => e,
            None => return (0, 0),
        };

        let mut same_seg = 0usize;
        let mut diff_seg = 0usize;

        // For each pair of edges at this node
        for i in 0..edges.len() {
            for j in (i + 1)..edges.len() {
                let ea_idx = edges[i];
                let eb_idx = edges[j];
                let ea = &graph.edges[ea_idx];
                let eb = &graph.edges[eb_idx];

                let cfg_a = match cfg.get(&ea_idx) {
                    Some(c) => c,
                    None => continue,
                };
                let cfg_b = match cfg.get(&eb_idx) {
                    Some(c) => c,
                    None => continue,
                };

                // Calculate reversal flags based on direction
                let rev_a = (ea.from != node_id) ^ ea.dir;
                let rev_b = (eb.from != node_id) ^ eb.dir;
                let same_dir = !(rev_a ^ rev_b);

                // Build relative ordering map for edge A
                let mut ordering: HashMap<String, usize> = HashMap::new();
                for (i, line) in cfg_a.iter().enumerate() {
                    let pos = if same_dir { cfg_a.len() - 1 - i } else { i };
                    ordering.insert(line.line_id.clone(), pos);
                }

                // Collect positions of shared lines in edge B's order
                let mut rel_order: Vec<usize> = Vec::new();
                for line in cfg_b.iter() {
                    if let Some(&pos) = ordering.get(&line.line_id) {
                        rel_order.push(pos);
                    }
                }

                // Count inversions (crossings)
                let inversions = self.count_inversions(&rel_order);

                if cfg_a.len() == cfg_b.len() && rel_order.len() == cfg_a.len() {
                    // Same segment crossing (all lines shared)
                    same_seg += inversions;
                } else {
                    // Different segment crossing (partial sharing)
                    diff_seg += inversions;
                }
            }
        }

        (same_seg, diff_seg)
    }

    /// Count inversions in a sequence (used for crossing count)
    fn count_inversions(&self, arr: &[usize]) -> usize {
        let mut count = 0;
        for i in 0..arr.len() {
            for j in (i + 1)..arr.len() {
                if arr[i] > arr[j] {
                    count += 1;
                }
            }
        }
        count
    }

    /// Compute total crossing score for a component
    fn compute_crossing_score(
        &self,
        graph: &RenderGraph,
        component: &[usize],
        cfg: &HashMap<usize, Vec<LineOnEdge>>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> f64 {
        // Collect all nodes in component
        let mut nodes: HashSet<i64> = HashSet::new();
        for &edge_idx in component {
            let edge = &graph.edges[edge_idx];
            nodes.insert(edge.from);
            nodes.insert(edge.to);
        }

        let mut total = 0.0;
        for node_id in nodes {
            let (same, diff) = self.count_crossings_at_node(graph, node_id, cfg, node_to_edges);
            total += same as f64 * 40.0 + diff as f64 * 10.0;
        }

        total
    }

    /// Compute crossing score for a single edge (at both endpoints)
    fn compute_edge_score(
        &self,
        graph: &RenderGraph,
        edge_idx: usize,
        cfg: &HashMap<usize, Vec<LineOnEdge>>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> f64 {
        let edge = &graph.edges[edge_idx];
        let (same_from, diff_from) =
            self.count_crossings_at_node(graph, edge.from, cfg, node_to_edges);
        let (same_to, diff_to) = self.count_crossings_at_node(graph, edge.to, cfg, node_to_edges);

        (same_from + same_to) as f64 * 40.0 + (diff_from + diff_to) as f64 * 10.0
    }

    /// Hill climbing optimization: iteratively swap line pairs to reduce crossings.
    /// Matches C++ loom's HillClimbOptimizer::optimizeComp
    pub fn hill_climb_refine(
        &self,
        graph: &RenderGraph,
        component: &[usize],
        mut cfg: HashMap<usize, Vec<LineOnEdge>>,
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> HashMap<usize, Vec<LineOnEdge>> {
        // Only process edges with >= 2 lines
        let edges: Vec<usize> = component
            .iter()
            .filter(|&&idx| graph.edges[idx].lines.len() >= 2)
            .cloned()
            .collect();

        if edges.is_empty() {
            return cfg;
        }

        loop {
            let mut best_change = 0.0;
            let mut best_edge = None;
            let mut best_order: Option<Vec<LineOnEdge>> = None;

            for &edge_idx in &edges {
                let lines = match cfg.get(&edge_idx) {
                    Some(l) => l.clone(),
                    None => continue,
                };

                if lines.len() < 2 {
                    continue;
                }

                let old_score = self.compute_edge_score(graph, edge_idx, &cfg, node_to_edges);

                // Try all pairwise swaps
                for p1 in 0..lines.len() {
                    for p2 in p1..lines.len() {
                        if p1 == p2 {
                            continue;
                        }

                        // Swap
                        let mut swapped = lines.clone();
                        swapped.swap(p1, p2);
                        cfg.insert(edge_idx, swapped.clone());

                        let new_score =
                            self.compute_edge_score(graph, edge_idx, &cfg, node_to_edges);

                        if new_score < old_score && old_score - new_score > best_change {
                            best_change = old_score - new_score;
                            best_edge = Some(edge_idx);
                            best_order = Some(swapped);
                        }

                        // Swap back
                        cfg.insert(edge_idx, lines.clone());
                    }
                }
            }

            // Apply best improvement if found
            match (best_edge, best_order) {
                (Some(edge_idx), Some(order)) => {
                    cfg.insert(edge_idx, order);
                }
                _ => break, // No improvement found, stop
            }
        }

        cfg
    }

    /// Combined greedy + hill climbing for large components
    /// Use this as fallback when ILP is too expensive
    pub fn greedy_hillclimb_component(
        &self,
        graph: &RenderGraph,
        component: &[usize],
        node_to_edges: &HashMap<i64, Vec<usize>>,
    ) -> HashMap<usize, Vec<LineOnEdge>> {
        // Step 1: Greedy ordering
        let cfg = self.greedy_order_component(graph, component, node_to_edges);

        // Step 2: Hill climbing refinement
        self.hill_climb_refine(graph, component, cfg, node_to_edges)
    }
}
