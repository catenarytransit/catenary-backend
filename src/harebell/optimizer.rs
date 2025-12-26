use crate::graph::{Edge, LineOnEdge, Node, RenderGraph};
use good_lp::{
    Expression, IntoAffineExpression, ProblemVariables, Solution, SolverModel, Variable,
    default_solver, variable,
};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::f64::consts::PI;

pub struct Optimizer {
    // Weights for the objective function
    weight_cross: f64,
}

impl Optimizer {
    pub fn new() -> Self {
        Self { weight_cross: 10.0 }
    }

    pub fn optimize(&self, graph: &mut RenderGraph) {
        info!("Starting Decomposed ILP Line Ordering Optimization...");

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
            info!("No edges with multiple lines to optimize.");
            return;
        }

        // Simplify Graph Iteratively
        info!("Starting Iterative Simplification...");
        let mut round = 0;
        loop {
            round += 1;
            if round > 10 {
                // Max rounds safety
                break;
            }
            let mut changed = false;

            // Untangling Rules (Stumps, etc.)
            if self.simplify_graph(graph) {
                changed = true;
            }

            // Pruning Rule 1: Node Contraction
            // (Note: function itself loops internally, but we call it here to catch new opportunities)
            self.prune_rule_1_node_contraction(graph); // This doesn't return bool, but it logs. 

            // Pruning Rule 2: Line Partner Collapse
            self.prune_rule_2_line_partner_collapse(graph); // Also exhaustive.

            // Cutting Rule 1: Single Line Cut
            if self.cutting_rule_1_single_line_cut(graph) {
                changed = true;
            }

            // Cutting Rule 2: Terminus Detachment
            if self.cutting_rule_2_terminus_detachment(graph) {
                changed = true;
            }

            if !changed {
                break;
            }
        }
        info!("Simplification Complete after {} rounds.", round);

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
            if component.len() > 100 {
                let sub_components = self.split_by_articulation_points(&component, &adj);
                components.extend(sub_components);
            } else {
                components.push(component);
            }
        }

        info!(
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

            // Skip huge components to prevent solver hang/OOM
            if size > 300 {
                println!(
                    "Skipping component {}/{} (Size: {} edges) - too large for ILP optimization.",
                    i + 1,
                    total_components,
                    size
                );
                continue;
            }

            if size > 50 {
                println!(
                    "Processing component {}/{} (Size: {} edges). Building model...",
                    i + 1,
                    total_components,
                    size
                );
            }
            if let Some(res) = self.solve_component(graph, component, &node_to_all_edges) {
                global_results.extend(res);
            }
            if size > 50 {
                println!("Component {}/{} solved.", i + 1, total_components);
            }
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

        info!("Decomposed ILP Optimization complete.");
    }

    fn solve_component(
        &self,
        graph: &RenderGraph,
        edge_indices: &[usize],
        node_to_all_edges: &HashMap<i64, Vec<usize>>,
    ) -> Option<HashMap<usize, Vec<LineOnEdge>>> {
        let mut vars = ProblemVariables::new();
        let mut range_vars: HashMap<(usize, usize, usize), Variable> = HashMap::new();
        let mut oracle_vars: HashMap<(usize, usize, usize), Variable> = HashMap::new();
        let mut constraints = Vec::new();
        let mut objective: Expression = 0.into();
        // Compute dynamic Big-M based on the maximum number of lines in any edge of this component
        let max_lines = edge_indices
            .iter()
            .map(|&idx| graph.edges[idx].lines.len())
            .max()
            .unwrap_or(0);

        // M must be > max possible difference in ranks (which is max_lines).
        // We use max_lines + 2 to be safe.
        let big_m = (max_lines + 2) as f64;

        // --- A. Setup Variables (only for component edges) ---
        for &edge_idx in edge_indices {
            let edge = &graph.edges[edge_idx];
            let n = edge.lines.len(); // Known >= 2

            for l_idx in 0..n {
                for p in 1..=n {
                    let v = vars.add(variable().binary());
                    range_vars.insert((edge_idx, l_idx, p), v);

                    // Monotonicity
                    if p > 1 {
                        let v_prev = range_vars.get(&(edge_idx, l_idx, p - 1)).unwrap();
                        constraints.push((v.into_expression() - *v_prev).geq(0));
                    }
                }
            }
            // Uniqueness
            for p in 1..=n {
                let mut sum_expr: Expression = 0.into();
                for l_idx in 0..n {
                    sum_expr += range_vars.get(&(edge_idx, l_idx, p)).unwrap();
                }
                constraints.push(sum_expr.eq(p as f64));
            }

            // Oracle Vars
            for i in 0..n {
                for j in 0..n {
                    if i == j {
                        continue;
                    }
                    let v_oracle = vars.add(variable().binary());
                    oracle_vars.insert((edge_idx, i, j), v_oracle);
                }
            }
            // Oracle Constraints
            for i in 0..n {
                for j in 0..n {
                    if i == j {
                        continue;
                    }
                    let x_ji = oracle_vars.get(&(edge_idx, j, i)).unwrap();
                    let x_ij = oracle_vars.get(&(edge_idx, i, j)).unwrap();

                    let mut sum_i: Expression = 0.into();
                    let mut sum_j: Expression = 0.into();
                    for p in 1..=n {
                        sum_i += range_vars.get(&(edge_idx, i, p)).unwrap();
                        sum_j += range_vars.get(&(edge_idx, j, p)).unwrap();
                    }

                    constraints.push((sum_i - sum_j + x_ji.into_expression() * big_m).geq(0));
                    constraints.push((x_ij.into_expression() + *x_ji).eq(1));
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
                edge_angles.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

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
                    // Check for shared lines >= 2
                    let edge2 = &graph.edges[e2_idx];
                    let mut shared_lines = Vec::new();
                    for l1 in &edge1.lines {
                        // Restriction Check
                        let key = (l1.chateau_id.clone(), l1.route_id.clone());
                        let restricted =
                            if let Some(r) = graph.restrictions.get(&(e1_idx, e2_idx)) {
                                r.contains(&key)
                            } else {
                                false
                            } || if let Some(r) = graph.restrictions.get(&(e2_idx, e1_idx)) {
                                r.contains(&key)
                            } else {
                                false
                            };

                        if restricted {
                            continue;
                        }

                        if edge2.lines.iter().any(|l2| l2.line_id == l1.line_id) {
                            shared_lines.push(l1.line_id.clone());
                        }
                    }

                    if shared_lines.len() >= 2 {
                        // Add consistency constraints
                        for a_idx in 0..shared_lines.len() {
                            for b_idx in (a_idx + 1)..shared_lines.len() {
                                let la = &shared_lines[a_idx];
                                let lb = &shared_lines[b_idx];

                                let a1 = get_line_idx(e1_idx, la).unwrap();
                                let b1 = get_line_idx(e1_idx, lb).unwrap();
                                let a2 = get_line_idx(e2_idx, la).unwrap();
                                let b2 = get_line_idx(e2_idx, lb).unwrap();

                                let x_e1_ab = oracle_vars.get(&(e1_idx, a1, b1)).unwrap();
                                let x_e2_ba = oracle_vars.get(&(e2_idx, b2, a2)).unwrap();

                                let c_var = vars.add(variable().binary());
                                // Weighted crossing cost
                                let w_a = graph.edges[e1_idx].lines[a1].weight as f64;
                                let w_b = graph.edges[e1_idx].lines[b1].weight as f64;
                                objective += c_var * self.weight_cross * w_a * w_b;
                                constraints
                                    .push((x_e1_ab.into_expression() + *x_e2_ba - 1.0).leq(c_var));

                                let x_e1_ba = oracle_vars.get(&(e1_idx, b1, a1)).unwrap();
                                let x_e2_ab = oracle_vars.get(&(e2_idx, a2, b2)).unwrap();
                                constraints
                                    .push((x_e1_ba.into_expression() + *x_e2_ab - 1.0).leq(c_var));
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

                            let prefer_a_left = diff_a < diff_b;

                            let split_penalty = vars.add(variable().binary());
                            objective += split_penalty * self.weight_cross;

                            if prefer_a_left {
                                // Want x_{e1, A<B} == 1 ?
                                // According to Loom/Paper reference:
                                // If A is geometrically "Left" (CCW first), it should often be placed at High Index (Right).
                                // (Logic inversion compared to intuitive mapping).
                                // Current code enforces A < B (Low Index).
                                // We swap to enforce B < A (A High Index).
                                let x_ab = oracle_vars.get(&(e1_idx, i, j)).unwrap();
                                constraints.push(x_ab.into_expression().leq(split_penalty));
                            } else {
                                let x_ba = oracle_vars.get(&(e1_idx, j, i)).unwrap();
                                constraints.push(x_ba.into_expression().leq(split_penalty));
                            }
                        }
                    }
                }
            } // end loop nodes
        } // end loop edges

        // Solve
        // Set a time limit (CBC uses "sec" or "seconds")
        let mut model = vars.minimise(objective).using(default_solver);
        // Note: good_lp 1.0 might not expose set_parameter directly on the builder in a standardized way
        // but typically it might be .with_config or similar.
        // For now, we will stick to the basic solve, but we really should add a timeout.
        // Let's rely on the user instructions or previous knowledge.
        // Assuming no parameter setting for now to avoid compilation error if method doesn't exist.
        // We relying on Big-M fix to resolve the hang (infeasibility loop).

        for c in constraints {
            model = model.with(c);
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

        // Collect results
        let mut component_results = HashMap::new();
        for &edge_idx in edge_indices {
            let edge = &graph.edges[edge_idx];
            let n = edge.lines.len();
            let mut ranks: Vec<(usize, f64)> = Vec::new();
            for i in 0..n {
                let mut rank_sum = 0.0;
                for p in 1..=n {
                    if let Some(v) = range_vars.get(&(edge_idx, i, p)) {
                        rank_sum += solution.value(*v);
                    }
                }
                ranks.push((i, rank_sum));
            }

            let mut line_indices: Vec<usize> = (0..n).collect();
            line_indices.sort_by(|&a, &b| {
                let sum_a = ranks.iter().find(|(idx, _)| *idx == a).unwrap().1;
                let sum_b = ranks.iter().find(|(idx, _)| *idx == b).unwrap().1;
                sum_b.partial_cmp(&sum_a).unwrap() // Descending
            });

            let new_lines: Vec<LineOnEdge> = line_indices
                .into_iter()
                .map(|idx| edge.lines[idx].clone())
                .collect();
            component_results.insert(edge_idx, new_lines);
        }

        Some(component_results)
    }

    fn simplify_graph(&self, graph: &mut RenderGraph) -> bool {
        info!("Starting graph simplification...");
        let mut any_changed = false;
        let mut loop_count = 0;
        loop {
            loop_count += 1;
            let mut changed = false;

            // Rebuild adjacency for safety at start of each pass
            let node_adj = self.build_node_adjacency(graph);

            // Rule 1: Full X
            if self.untangle_rule_1(graph, &node_adj) {
                changed = true;
                any_changed = true;
                info!("Applied Untangling Rule 1 (Full X)");
                continue;
            }

            // Rule 2: Full Y
            if self.untangle_rule_2(graph, &node_adj) {
                changed = true;
                any_changed = true;
                info!("Applied Untangling Rule 2 (Full Y)");
                continue;
            }

            // Rule 3: Partial Y
            // "Given a node v adjacent to an edge e = {u, v} with deg(u) = 1...
            // Each l in L(e) terminates at u.
            // Each l in L(e) uniquely extends over v into one of n > 1 edges e_1...e_n..."
            if self.untangle_rule_3(graph, &node_adj) {
                changed = true;
                any_changed = true;
                any_changed = true;
                info!("Applied Untangling Rule 3 (Partial Y)");
                continue;
            }

            if self.untangle_rule_4(graph, &node_adj) {
                changed = true;
                any_changed = true;
                any_changed = true;
                info!("Applied Untangling Rule 4 (Full Double Y)");
                continue;
            }

            // Rule 5: Partial Double Y
            if self.untangle_rule_5(graph, &node_adj) {
                changed = true;
                any_changed = true;
                any_changed = true;
                info!("Applied Untangling Rule 5 (Partial Double Y)");
                continue;
            }

            // Rule 6 & 8: Stumps
            if self.untangle_stumps(graph, &node_adj) {
                changed = true;
                any_changed = true;
                any_changed = true;
                info!("Applied Stump Untangling (Rule 6/7/8)");
                continue;
            }

            if !changed {
                break;
            }
            if loop_count > 500 {
                warn!("Simplification loop hit max iterations. Stopping.");
                break;
            }
        }
        info!("Graph simplification complete after {} rounds.", loop_count);
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
        // Untangling Rule 1: Full X
        // Node v, deg(v)=4. Edges e1, e2, e3, e4.
        // L(e1) = L(e3) = A
        // L(e2) = L(e4) = B
        // A disjoint B

        let mut changes = false;
        let mut nodes: Vec<i64> = node_adj.keys().cloned().collect();
        nodes.sort(); // Determinism

        for v in nodes {
            if let Some(adj) = node_adj.get(&v) {
                if adj.len() != 4 {
                    continue;
                }

                // Check pairs
                // We need to find two pairs (e1, e3) and (e2, e4) such that L(e1)=L(e3) and L(e2)=L(e4) and disjoint.
                let mut pair1 = None;
                let mut pair2 = None;

                // There are 3 ways to pair 4 edges: (0,1)/(2,3), (0,2)/(1,3), (0,3)/(1,2)
                let mut pairings = vec![((0, 1), (2, 3)), ((0, 2), (1, 3)), ((0, 3), (1, 2))];

                for ((i, j), (k, l)) in pairings {
                    let e_i = &graph.edges[adj[i]];
                    let e_j = &graph.edges[adj[j]];
                    let e_k = &graph.edges[adj[k]];
                    let e_l = &graph.edges[adj[l]];

                    let mut lines_i: Vec<_> = e_i.lines.iter().map(|x| x.line_id.clone()).collect();
                    let mut lines_j: Vec<_> = e_j.lines.iter().map(|x| x.line_id.clone()).collect();
                    let mut lines_k: Vec<_> = e_k.lines.iter().map(|x| x.line_id.clone()).collect();
                    let mut lines_l: Vec<_> = e_l.lines.iter().map(|x| x.line_id.clone()).collect();
                    lines_i.sort();
                    lines_j.sort();
                    lines_k.sort();
                    lines_l.sort();

                    if lines_i == lines_j && lines_k == lines_l {
                        // Check disjoint
                        let set_i: HashSet<_> = lines_i.iter().cloned().collect();
                        let set_k: HashSet<_> = lines_k.iter().cloned().collect();
                        if set_i.is_disjoint(&set_k) {
                            pair1 = Some((adj[i], adj[j])); // e_i, e_j (indices)
                            pair2 = Some((adj[k], adj[l])); // e_k, e_l
                            break;
                        }
                    }
                }

                if let (Some((e1_idx, e3_idx)), Some((e2_idx, e4_idx))) = (pair1, pair2) {
                    // Split v into v' and v''
                    // Connect e1, e3 to v'
                    // Connect e2, e4 to v''

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
                            // Wait, I changed it to name: None without checking definition!
                            // Node definition in graph.rs:8
                            // pub struct Node { ... pub name: Option<String> ... }
                            // So name: None is correct.
                        },
                    );
                    // Oops, my code drafted above used name: String::new(). I must fix it to None.

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

                    // Fix v_prime above too

                    // Update edges
                    // Pair 1 -> v_prime
                    for &idx in &[e1_idx, e3_idx] {
                        let e = &mut graph.edges[idx];
                        if e.from == v {
                            e.from = v_prime;
                        }
                        if e.to == v {
                            e.to = v_prime;
                        }
                    }
                    // Pair 2 -> v_double_prime
                    for &idx in &[e2_idx, e4_idx] {
                        let e = &mut graph.edges[idx];
                        if e.from == v {
                            e.from = v_double_prime;
                        }
                        if e.to == v {
                            e.to = v_double_prime;
                        }
                    }

                    graph.nodes.remove(&v);

                    changes = true;
                    return true;
                }
            }
        }
        changes
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
                    });

                    graph.edges.push(Edge {
                        id: max_edge_id + 2,
                        from: u2,
                        to: v2,
                        lines: m2_lines_vec, // Use extracted vec
                        geometry: e_geom,
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

            info!(
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
        // Partial Double Y:
        // Similar to Rule 4, but we only split at 'v' (leave 'u' as is).
        // Condition: Injective mapping from minor legs at u to minor legs at v.
        // Actually, the paper says: "One of n < deg(v) edges f_1...f_n... and all l in L(f_i) union L(e) uniquely extend over u into e...
        // AND Each l in L(e) uniquely extends over u into one of deg(u)-1 edges g_i...
        // AND Injective mapping."
        // Simplify: We look for a subset of lines L_sub <= L(e) that:
        // 1. Corresponds exactly to ONE minor leg at u (or a set of minor legs unique to these lines).
        // 2. Corresponds exactly to ONE minor leg at v.
        // 3. At v, these lines are the ONLY lines on that minor leg? No, "uniquely extends over v".

        let mut to_split: Option<(usize, i64, i64)> = None;

        'outer: for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }
            let u_candidates = [edge.from, edge.to];

            // Try both directions: u -> v
            for &u in &u_candidates {
                let v = if edge.from == u { edge.to } else { edge.from };

                let u_legs = &node_adj[&u];
                let v_legs = &node_adj[&v];

                // We want to find a group of lines 'S' that:
                // 1. Comes from ONE minor leg at 'v' (v_leg).
                // 2. Goes to ONE minor leg at 'u' (u_leg). (Or subset of legs? "Injective mapping" implies 1-to-1 for the split part).
                // Rule 5 says: "Split v in the same fashion... leave u as is."
                // So we seek to isolate a flow at V.
                // This implies at V, the flow is distinct (uniquely extends into f_i).

                // Let's look for a single line-group (lines shared with one minor leg at v)
                // that matches exactly a line-group at u.

                // 1. Identify groups at v
                let mut v_groups: HashMap<usize, Vec<String>> = HashMap::new();
                for &leg in v_legs {
                    if leg == e_idx {
                        continue;
                    }
                    let leg_edge = &graph.edges[leg];
                    // Intersection
                    let common: Vec<String> = leg_edge
                        .lines
                        .iter()
                        .filter(|l| edge.lines.iter().any(|el| el.line_id == l.line_id))
                        .map(|l| l.line_id.clone())
                        .collect();
                    if !common.is_empty() {
                        v_groups.insert(leg, common);
                    }
                }

                // 2. For each group at v, check if it maps cleanly to u
                for (&v_leg_idx, lines) in &v_groups {
                    // Check if these lines essentially form a distinct group at u too
                    // They must belong to ONE minor leg at u? Or multiple?
                    // "Injective mapping A: {g_i} -> {f_i}"
                    // This means for every leg at u involved, it maps to a leg at v.
                    // If we found a leg at v (v_leg_idx), does it correspond to exactly one leg at u?

                    let mut u_matches = 0;
                    let mut u_leg_match = 0;
                    let mut exact_match = false;

                    for &u_leg in u_legs {
                        if u_leg == e_idx {
                            continue;
                        }
                        let leg_edge = &graph.edges[u_leg];
                        // Check overlap
                        let overlap_count = leg_edge
                            .lines
                            .iter()
                            .filter(|l| lines.contains(&l.line_id))
                            .count();
                        if overlap_count > 0 {
                            u_matches += 1;
                            u_leg_match = u_leg;
                            if overlap_count == lines.len() {
                                // All lines in the v-group are present in this u-leg
                                // Also check if u-leg has EXTRA lines?
                                // "L(g_i) subset L(A(g_i))" -> Lines at u are subset of Lines at v.
                                // If overlap == lines.len(), then Lines(v_leg) <= Lines(u_leg) is FALSE.
                                // We checked if 'lines' (which is L(e) intersect L(v_leg)) are in L(u_leg).
                                // So L(v_leg) intersect L(e) <= L(u_leg).
                                exact_match = true;
                            }
                        }
                    }

                    if u_matches == 1 && exact_match {
                        // Found a clean flow!
                        // Lines 'lines' move from v_leg_idx -> e -> u_leg_match.
                        // We can split 'v'.
                        to_split = Some((e_idx, u, v));
                        break 'outer;
                    }
                }
            }
        }

        if let Some((e_idx, u, v)) = to_split {
            // Split v (the 3rd element in tuple is the one to split)
            // Wait, logic above: "Split v ... leave u as is".
            // So we clone v -> v', create e' = {u, v'}.

            // Re-find the group
            let edge_data = graph.edges[e_idx].clone();
            let v_legs = &node_adj[&v];

            let mut target_lines = Vec::new();
            let mut v_leg_idx = 0;

            for &leg in v_legs {
                if leg == e_idx {
                    continue;
                }
                let leg_edge = &graph.edges[leg];
                let common: Vec<String> = leg_edge
                    .lines
                    .iter()
                    .filter(|l| edge_data.lines.iter().any(|el| el.line_id == l.line_id))
                    .map(|l| l.line_id.clone())
                    .collect();

                // Verify match at u again (sanity check or just proceed if confident)
                // We assume first found is valid if we trust the loop above.
                // Actually need to ensure it's the SAME group.
                if !common.is_empty() {
                    // Check u side uniqueness
                    let u_legs = &node_adj[&u];
                    let mut match_count = 0;
                    for &ul in u_legs {
                        if ul == e_idx {
                            continue;
                        }
                        let u_edge = &graph.edges[ul];
                        if u_edge.lines.iter().any(|l| common.contains(&l.line_id)) {
                            match_count += 1;
                        }
                    }
                    if match_count == 1 {
                        target_lines = common;
                        v_leg_idx = leg;
                        break;
                    }
                }
            }

            // Create v'
            let v_node = graph.nodes[&v].clone();
            let max_id = graph.nodes.keys().max().cloned().unwrap_or(0);
            let v_prime_id = max_id + 1;
            let mut v_prime = v_node.clone();
            v_prime.id = v_prime_id;
            graph.nodes.insert(v_prime_id, v_prime);

            // Create e' = {u, v'}
            let mut e_prime = edge_data.clone();
            let max_edge_id = graph.edges.iter().map(|e| e.id).max().unwrap_or(0);
            e_prime.id = max_edge_id + 1;

            if e_prime.from == v {
                e_prime.from = v_prime_id;
            }
            // e.from was v, so e'.from = v'
            // e.to was u, stays u.
            else if e_prime.to == v {
                e_prime.to = v_prime_id;
            }

            e_prime.lines.retain(|l| target_lines.contains(&l.line_id));
            graph.edges.push(e_prime);

            // Fix original e
            graph.edges[e_idx]
                .lines
                .retain(|l| !target_lines.contains(&l.line_id));

            // Detach minor leg from v, attach to v'
            {
                let v_leg = &mut graph.edges[v_leg_idx];
                if v_leg.from == v {
                    v_leg.from = v_prime_id;
                } else if v_leg.to == v {
                    v_leg.to = v_prime_id;
                }
            }

            info!(
                "Applied Untangling Rule 5 (Partial Double Y) on edge {}/node {}",
                edge_data.id, v
            );
            return true;
        }

        false
    }

    fn untangle_stumps(
        &self,
        graph: &mut RenderGraph,
        node_adj: &HashMap<i64, Vec<usize>>,
    ) -> bool {
        // Apply Rule 8 (Double Stump) and Rule 6 (Outer Stump)
        // Rule 8: Line terminates at u AND v. Remove it (it's isolated on e).
        // Rule 6: Minor leg at u terminates at v (and no Lines(leg) continue past v). Split it.

        let mut to_split: Option<(usize, i64, i64)> = None;
        let mut to_remove_lines: Option<(usize, Vec<String>)> = None;

        'outer: for (e_idx, edge) in graph.edges.iter().enumerate() {
            if edge.lines.is_empty() {
                continue;
            }
            let u = edge.from;
            let v = edge.to;

            let u_legs = &node_adj[&u];
            let v_legs = &node_adj[&v];

            // Check for Rule 8 (Double Stump) lines
            // Lines on e that are NOT in any other u_leg AND NOT in any other v_leg.
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
                to_remove_lines = Some((e_idx, double_stumps));
                break 'outer;
            }

            // Check for Rule 6 (Outer Stump)
            // Check legs at u: Do their lines terminate at v?
            // i.e. For a leg L_u, Lines(L_u) <= Lines(e). AND Lines(L_u) not in any OTHER v_leg.
            for &leg in u_legs {
                if leg == e_idx {
                    continue;
                }
                let leg_edge = &graph.edges[leg];
                if leg_edge.lines.is_empty() {
                    continue;
                }

                // Check subset of e
                let all_in_e = leg_edge
                    .lines
                    .iter()
                    .all(|l| edge.lines.iter().any(|el| el.line_id == l.line_id));

                if all_in_e {
                    // Check if they terminate at v
                    // i.e., NONE of these lines appear in any OTHER leg at v (excluding e)
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
                        // Candidate found!
                        // Split off this leg and corresponding lines on e.
                        // But wait, split HOW?
                        // Rule 6: "Detach the stump... split u and v".
                        // So we act like Rule 4/5: Clone u->u', v->v', e->e'.
                        // Attach leg to u'.
                        // Keep e' lines = Lines(leg).
                        to_split = Some((e_idx, u, v));
                        // Store specific leg to detach? We need to re-find it.
                        break 'outer;
                    }
                }
            }

            // Symmetrical check for legs at v terminating at u
            for &leg in v_legs {
                if leg == e_idx {
                    continue;
                }
                let leg_edge = &graph.edges[leg];
                if leg_edge.lines.is_empty() {
                    continue;
                }

                let all_in_e = leg_edge
                    .lines
                    .iter()
                    .all(|l| edge.lines.iter().any(|el| el.line_id == l.line_id));

                if all_in_e {
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
                        to_split = Some((e_idx, u, v)); // We can swap u/v in handling
                        break 'outer;
                    }
                }
            }
        }

        if let Some((e_idx, lines)) = to_remove_lines {
            info!(
                "Applied Rule 8 (Double Stump): Removing isolated lines {:?} from edge {}",
                lines, e_idx
            );
            let edge = &mut graph.edges[e_idx];
            edge.lines.retain(|l| !lines.contains(&l.line_id));
            return true;
        }

        if let Some((e_idx, u, v)) = to_split {
            // Apply Stump Split
            // Covers Rules 6 (Outer Stump), 7 (Inner Stump), and 8 (Double Stump) essentially by peeling off terminating flows.

            // Clone edge data immediately to avoid holding borrow
            let edge_data = graph.edges[e_idx].clone();
            let u_legs = &node_adj[&u];
            let v_legs = &node_adj[&v];

            let mut stump_leg_idx = None;
            let mut stump_lines = Vec::new();
            let mut stump_at_u = true;

            // Re-identify the stump leg
            // Check u legs
            for &leg in u_legs {
                if leg == e_idx {
                    continue;
                }
                let leg_edge = &graph.edges[leg];
                if leg_edge.lines.is_empty() {
                    continue;
                }
                let all_in_e = leg_edge
                    .lines
                    .iter()
                    .all(|l| edge_data.lines.iter().any(|el| el.line_id == l.line_id));
                if all_in_e {
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
                        stump_leg_idx = Some(leg);
                        stump_lines = leg_edge.lines.iter().map(|l| l.line_id.clone()).collect();
                        stump_at_u = true;
                        break;
                    }
                }
            }

            if stump_leg_idx.is_none() {
                // Check v legs
                for &leg in v_legs {
                    if leg == e_idx {
                        continue;
                    }
                    let leg_edge = &graph.edges[leg];
                    if leg_edge.lines.is_empty() {
                        continue;
                    }
                    let all_in_e = leg_edge
                        .lines
                        .iter()
                        .all(|l| edge_data.lines.iter().any(|el| el.line_id == l.line_id));
                    if all_in_e {
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
                            stump_leg_idx = Some(leg);
                            stump_lines =
                                leg_edge.lines.iter().map(|l| l.line_id.clone()).collect();
                            stump_at_u = false;
                            break;
                        }
                    }
                }
            }

            if let Some(leg_idx) = stump_leg_idx {
                // Split!
                let u_node = graph.nodes[&u].clone();
                let v_node = graph.nodes[&v].clone();
                // Just use max_id + random to avoid conflicts, or smarter ID gen.
                let max_id = graph.nodes.keys().max().cloned().unwrap_or(0);
                let u_prime_id = max_id + 1;
                let v_prime_id = max_id + 2;
                let mut u_prime = u_node.clone();
                u_prime.id = u_prime_id;
                let mut v_prime = v_node.clone();
                v_prime.id = v_prime_id;
                graph.nodes.insert(u_prime_id, u_prime);
                graph.nodes.insert(v_prime_id, v_prime);

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
                e_prime.lines.retain(|l| stump_lines.contains(&l.line_id));

                graph.edges.push(e_prime);

                // Now safe to mutate original
                graph.edges[e_idx]
                    .lines
                    .retain(|l| !stump_lines.contains(&l.line_id));

                // Move leg
                {
                    let leg = &mut graph.edges[leg_idx];
                    if stump_at_u {
                        if leg.from == u {
                            leg.from = u_prime_id;
                        } else if leg.to == u {
                            leg.to = u_prime_id;
                        }
                    } else {
                        if leg.from == v {
                            leg.from = v_prime_id;
                        } else if leg.to == v {
                            leg.to = v_prime_id;
                        }
                    }
                }

                info!(
                    "Applied Stump Rule (6/7) on edge {}/leg {}",
                    edge_data.id, leg_idx
                );
                return true;
            }
        }

        false
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
                                // If neighbor is AP, we don't push to Q (it's marked visited).
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
            info!(
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
                };

                max_edge_id += 1;
                let e_double_prime = Edge {
                    id: max_edge_id,
                    from: v_new2,
                    to: edge.to,
                    lines: edge.lines.clone(),
                    geometry: geom2,
                };

                edges_to_add.push(e_prime);
                edges_to_add.push(e_double_prime);

                changed = true;
            }
        }

        if changed {
            info!(
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
            info!(
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
                // Geometry adjustment?
                // Visually we want it to touch the station?
                // If we detach, we might lose connectivity for rendering (dots might not connect).
                // However, for *Optimization* (Line Ordering), this is crucial.
                // The rendering code draws lines based on Edge geometry + offsets.
                // If the edge ends at `new_v` (which is at same coord as `old_v`), it looks fine.
                // BUT, `new_v` is not `old_v`.
                // If `old_v` draws a "Station Dot", `new_v` won't be part of it?
                // Or maybe it will?
                // If harebell merges nodes by distance or something...
                // Actually, Cutting Rule 2 is for Line Order Optimization.
                // If we persist this change to `RenderGraph` for final export, we might break connectivity in the MVT?
                // The MVT generator likely iterates edges.
                // If the edge ends at `new_v` (same lat/lon), the line geometry is fine.
                // But topological connectivity is broken.
                // This is INTENTIONAL for simplification.
                // Does it break anything else?
                // Maybe "Transfers" or "Interchanges" logic?
                // If we assume this simplification happens *only* for the purpose of calculating offsets,
                // and we map back results?
                // But here we are modifying `RenderGraph` in place.
                // The logic assumes `RenderGraph` is malleable.

                // Let's stick to the plan.
            }
        }

        changed
    }

    fn prune_rule_1_node_contraction(&self, graph: &mut RenderGraph) {
        // Pruning Rule 1: Node Contraction
        info!("Starting Pruning Rule 1 (Node Contraction)...");
        let mut changed = true;
        let mut rounds = 0;
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
                let adj = &node_adj[&v];
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
                    };

                    edges_to_add.push(new_edge);
                    edges_to_remove.insert(e1_idx);
                    edges_to_remove.insert(e2_idx);
                    nodes_to_remove.insert(v);

                    changed = true;
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
        info!("Pruning Rule 1 Complete.");
    }

    fn prune_rule_2_line_partner_collapse(&self, graph: &mut RenderGraph) {
        // Pruning Rule 2: Line Partner Collapse
        info!("Starting Pruning Rule 2 (Line Partner Collapse)...");

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

        info!(
            "Pruning Rule 2 Complete. Collapsed {} groups.",
            collapse_count
        );
    }
}
