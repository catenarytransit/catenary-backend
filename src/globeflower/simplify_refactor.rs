fn simplify_graph_serial(mut edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
    // Step 0: Consolidate parallel edges (identical u->v) to ensure true graph merging
    let mut edge_map: HashMap<(NodeId, NodeId), GraphEdge> = HashMap::new();
    for e in edges {
        let key = (e.from, e.to);
        if let Some(existing) = edge_map.get_mut(&key) {
            // Merge routes (avoid duplicates)
            for r in e.route_ids {
                existing.route_ids.push(r);
            }
        } else {
            edge_map.insert(key, e);
        }
    }

    // Convert to Vec and sort routes for fast comparison
    let mut edges: Vec<GraphEdge> = edge_map.into_values().collect();
    for e in &mut edges {
        e.route_ids.sort_unstable();
        e.route_ids.dedup();
    }

    // Step 1: Build Adjacency Lists for Intersections
    // Key: NodeId (as usize), Value: (Incoming Edge Indices, Outgoing Edge Indices)
    let mut adj: HashMap<usize, (Vec<usize>, Vec<usize>)> = HashMap::new();
    // Cache euclidean lengths in degrees for fast lookup
    let mut edge_lengths_deg: Vec<f64> = Vec::with_capacity(edges.len());

    for (i, edge) in edges.iter().enumerate() {
        if let NodeId::Intersection(u) = edge.from {
            adj.entry(u).or_default().1.push(i);
        }
        if let NodeId::Intersection(v) = edge.to {
            adj.entry(v).or_default().0.push(i);
        }

        // Calculate length roughly in degrees (Euclidean on lat/lon)
        let geom = convert_to_geo(&edge.geometry);
        #[allow(deprecated)]
        edge_lengths_deg.push(geom.euclidean_length());
    }

    let mut visited = vec![false; edges.len()];
    let mut simplified = Vec::new();

    let merge_chain = |indices: &[usize]| -> GraphEdge {
        let first = &edges[indices[0]];
        let last = &edges[indices[indices.len() - 1]];
        let mut coords = Vec::new();
        for (i, &idx) in indices.iter().enumerate() {
            let geom = &edges[idx].geometry;
            let pts = &geom.points;
            if i == 0 {
                for p in pts {
                    coords.push(Coord { x: p.x, y: p.y });
                }
            } else {
                for p in pts.iter().skip(1) {
                    coords.push(Coord { x: p.x, y: p.y });
                }
            }
        }
        let total_weight: f64 = indices.iter().map(|&i| edges[i].weight).sum();
        let route_vec = first.route_ids.clone();

        GraphEdge {
            from: first.from,
            to: last.to,
            geometry: convert_from_geo(&post_process_line(&GeoLineString::new(coords))),
            route_ids: route_vec,
            weight: total_weight,
            original_edge_index: first.original_edge_index,
        }
    };

    // Helper to count route matches
    let count_route_matches = |indices: &[usize], target_edge: &GraphEdge| -> usize {
        indices
            .iter()
            .filter(|&&idx| routes_equal(&edges[idx], target_edge))
            .count()
    };

    // Helper to find single route match
    let find_route_match = |indices: &[usize], target_edge: &GraphEdge| -> Option<usize> {
        let matches: Vec<usize> = indices
            .iter()
            .filter(|&&idx| routes_equal(&edges[idx], target_edge))
            .cloned()
            .collect();
        if matches.len() == 1 {
            Some(matches[0])
        } else {
            None
        }
    };

    // Step 2: Traverse from valid start nodes
    for i in 0..edges.len() {
        if visited[i] {
            continue;
        }

        let start_node = edges[i].from;
        let is_start = match start_node {
            NodeId::Cluster(_) => true,
            NodeId::Intersection(u_id) => {
                if let Some((in_list, out_list)) = adj.get(&u_id) {
                    // Check if this edge is the START of a unique route flow
                    // It is a start if:
                    // 1. No incoming edges match this route OR >1 incoming match (merge point)
                    // OR
                    // 2. >1 outgoing edges match this route (split point) - (Wait, i is one of them)

                    let in_count = count_route_matches(in_list, &edges[i]);
                    let out_count = count_route_matches(out_list, &edges[i]);

                    in_count != 1 || out_count != 1
                } else {
                    true
                }
            }
        };

        if is_start {
            let mut chain = vec![i];
            visited[i] = true;
            let mut current_chain_len = edge_lengths_deg[i];

            let mut curr = i;
            loop {
                let curr_edge = &edges[curr];
                if let NodeId::Intersection(v) = curr_edge.to {
                    // Check if we can traverse THROUGH v
                    if let Some((v_in, v_out)) = adj.get(&v) {
                        // Must have exactly 1 incoming (me) and 1 outgoing for THIS route
                        let in_count = count_route_matches(v_in, &edges[curr]);
                        let next_match = find_route_match(v_out, &edges[curr]);

                        if in_count == 1 {
                            if let Some(next) = next_match {
                                if !visited[next] {
                                    // Check max segment length
                                    let next_len = edge_lengths_deg[next];
                                    if current_chain_len + next_len
                                        <= MAX_COLLAPSED_SEG_LENGTH_DEGREES
                                    {
                                        visited[next] = true;
                                        chain.push(next);
                                        current_chain_len += next_len;
                                        curr = next;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            }
            simplified.push(merge_chain(&chain));
        }
    }

    // Step 3: Process remaining cycles or unvisited components
    for i in 0..edges.len() {
        if !visited[i] {
            let mut chain = vec![i];
            visited[i] = true;
            let mut current_chain_len = edge_lengths_deg[i];

            let mut curr = i;
            loop {
                let curr_edge = &edges[curr];
                if let NodeId::Intersection(v) = curr_edge.to {
                    if let Some((v_in, v_out)) = adj.get(&v) {
                        let in_count = count_route_matches(v_in, &edges[curr]);
                        let next_match = find_route_match(v_out, &edges[curr]);

                        if in_count == 1 {
                            if let Some(next) = next_match {
                                if !visited[next] {
                                    let next_len = edge_lengths_deg[next];
                                    if current_chain_len + next_len
                                        <= MAX_COLLAPSED_SEG_LENGTH_DEGREES
                                    {
                                        visited[next] = true;
                                        chain.push(next);
                                        current_chain_len += next_len;
                                        curr = next;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            }
            simplified.push(merge_chain(&chain));
        }
    }

    simplified
}
