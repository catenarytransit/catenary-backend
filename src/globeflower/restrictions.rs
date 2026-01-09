use crate::map_matcher::MatchedShape;
use crate::osm_types::LineId;
use crate::support_graph::{SupportEdgeId, SupportGraph, SupportNodeId};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use log::{debug, info};
use serde::{Deserialize, Serialize};

/// A turn restriction entry for the export format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcludedConnection {
    /// First edge endpoint (adjacent node)
    pub node_from: SupportNodeId,
    /// Second edge endpoint (adjacent node)
    pub node_to: SupportNodeId,
    /// Line that cannot make this transition
    pub line: LineId,
}

/// Turn restrictions manager
pub struct TurnRestrictions {
    /// Allowed transitions per (node, line): set of (in_edge, out_edge) pairs
    allowed: HashMap<(SupportNodeId, LineId), HashSet<(SupportEdgeId, SupportEdgeId)>>,
}

impl TurnRestrictions {
    pub fn new() -> Self {
        Self {
            allowed: HashMap::new(),
        }
    }

    /// Record allowed transitions from matched shapes
    pub fn record_from_matches(
        &mut self,
        matches: &[MatchedShape],
        atomic_to_support: &HashMap<crate::osm_types::AtomicEdgeId, Vec<SupportEdgeId>>,
        graph: &SupportGraph,
    ) {
        info!(
            "Recording transitions from {} matched shapes",
            matches.len()
        );

        let mut allowed_count = 0;

        for matched in matches {
            for (osm_node, in_atomic, out_atomic) in &matched.transitions {
                // Find corresponding support edges
                let in_candidates = atomic_to_support.get(in_atomic);
                let out_candidates = atomic_to_support.get(out_atomic);

                if let (Some(in_edges), Some(out_edges)) = (in_candidates, out_candidates) {
                    for &s_in in in_edges {
                        for &s_out in out_edges {
                            // Valid transition only if they share a node
                            if let (Some(e_in), Some(e_out)) =
                                (graph.edges.get(&s_in), graph.edges.get(&s_out))
                            {
                                let shared_node = if e_in.to == e_out.from || e_in.to == e_out.to {
                                    e_in.to
                                } else if e_in.from == e_out.from || e_in.from == e_out.to {
                                    e_in.from
                                } else {
                                    continue;
                                };

                                // Record as allowed
                                if self
                                    .allowed
                                    .entry((shared_node, matched.line_id.clone()))
                                    .or_default()
                                    .insert((s_in, s_out))
                                {
                                    allowed_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        info!("Recorded {} allowed transitions", allowed_count);
    }

    /// Generate excluded_conn list for a node
    pub fn excluded_connections_at_node(
        &self,
        node_id: SupportNodeId,
        graph: &SupportGraph,
    ) -> Vec<ExcludedConnection> {
        let edges = graph.node_edges.get(&node_id);
        if edges.is_none() || edges.unwrap().len() < 2 {
            return vec![];
        }

        let edges = edges.unwrap();
        let mut exclusions = Vec::new();

        // For each line at this node
        let mut lines_at_node: HashSet<LineId> = HashSet::new();
        for &edge_id in edges {
            if let Some(edge) = graph.edges.get(&edge_id) {
                for line_occ in &edge.lines {
                    lines_at_node.insert(line_occ.line.clone());
                }
            }
        }

        // For each line, check all possible transitions
        for line_id in lines_at_node {
            let allowed_key = (node_id, line_id.clone());
            let allowed_set = self.allowed.get(&allowed_key);

            // All edge pairs at this node
            for &in_edge_id in edges {
                for &out_edge_id in edges {
                    if in_edge_id == out_edge_id {
                        continue;
                    }

                    // Check if this transition is allowed
                    let is_allowed = allowed_set
                        .map(|set| set.contains(&(in_edge_id, out_edge_id)))
                        .unwrap_or(false);

                    if !is_allowed {
                        // Get the "far" endpoints of each edge for the exclusion format
                        if let (Some(in_edge), Some(out_edge)) =
                            (graph.edges.get(&in_edge_id), graph.edges.get(&out_edge_id))
                        {
                            let node_from = if in_edge.from == node_id {
                                in_edge.to
                            } else {
                                in_edge.from
                            };
                            let node_to = if out_edge.from == node_id {
                                out_edge.to
                            } else {
                                out_edge.from
                            };

                            exclusions.push(ExcludedConnection {
                                node_from,
                                node_to,
                                line: line_id.clone(),
                            });
                        }
                    }
                }
            }
        }

        exclusions
    }

    /// Generate excluded transitions list for a node (returning raw edge IDs)
    pub fn excluded_transitions_at_node(
        &self,
        node_id: SupportNodeId,
        graph: &SupportGraph,
    ) -> Vec<(SupportEdgeId, SupportEdgeId, LineId)> {
        let edges = graph.node_edges.get(&node_id);
        if edges.is_none() || edges.unwrap().len() < 2 {
            return vec![];
        }

        let edges = edges.unwrap();
        let mut exclusions = Vec::new();

        // For each line at this node
        let mut lines_at_node: HashSet<LineId> = HashSet::new();
        for &edge_id in edges {
            if let Some(edge) = graph.edges.get(&edge_id) {
                for line_occ in &edge.lines {
                    lines_at_node.insert(line_occ.line.clone());
                }
            }
        }

        // For each line, check all possible transitions
        for line_id in lines_at_node {
            let allowed_key = (node_id, line_id.clone());
            let allowed_set = self.allowed.get(&allowed_key);

            // All edge pairs at this node
            for &in_edge_id in edges {
                for &out_edge_id in edges {
                    if in_edge_id == out_edge_id {
                        continue;
                    }

                    // Check if this transition is allowed
                    let is_allowed = allowed_set
                        .map(|set| set.contains(&(in_edge_id, out_edge_id)))
                        .unwrap_or(false);

                    if !is_allowed {
                        exclusions.push((in_edge_id, out_edge_id, line_id.clone()));
                    }
                }
            }
        }

        exclusions
    }

    /// Build restrictions from GTFS-matched edge sequences (simpler approach)
    pub fn build_from_edge_sequences(
        matched_sequences: &[(LineId, Vec<SupportEdgeId>)],
        graph: &SupportGraph,
    ) -> Self {
        let mut restrictions = Self::new();

        for (line_id, edges) in matched_sequences {
            // Record allowed transitions
            for window in edges.windows(2) {
                let in_edge = window[0];
                let out_edge = window[1];

                // Find the shared node
                if let (Some(e1), Some(e2)) =
                    (graph.edges.get(&in_edge), graph.edges.get(&out_edge))
                {
                    let shared_node = if e1.to == e2.from || e1.to == e2.to {
                        e1.to
                    } else if e1.from == e2.from || e1.from == e2.to {
                        e1.from
                    } else {
                        continue;
                    };

                    restrictions
                        .allowed
                        .entry((shared_node, line_id.clone()))
                        .or_default()
                        .insert((in_edge, out_edge));
                }
            }
        }

        restrictions
    }
}

impl Default for TurnRestrictions {
    fn default() -> Self {
        Self::new()
    }
}
