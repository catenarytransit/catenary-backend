// ===========================================================================
// Hierarchical Intercity Rail Processing
// ===========================================================================
// Long intercity rail edges (route_type 2) can be 500+ km, which at 5m
// densification spacing creates 100K+ points per edge, causing OOM.
//
// SOLUTION: Defer long edges from cluster processing entirely.
// They are stored in a registry, then reattached post-processing using
// spatial queries to find matching endpoints.
// ===========================================================================

use crate::edges::{GraphEdge, NodeId};
use geo::Coord;

/// Maximum edge length before deferring (50km in Web Mercator meters)
pub const INTERCITY_DEFER_THRESHOLD: f64 = 50_000.0;

/// Registry of long intercity edges that are deferred from cluster processing.
/// These edges are kept intact and reattached post-processing.
#[derive(Debug)]
pub struct LongEdgeRegistry {
    /// Long edges with their original endpoint positions (for spatial matching)
    pub edges: Vec<DeferredEdge>,
}

/// A long edge that has been deferred from cluster processing
#[derive(Debug, Clone)]
pub struct DeferredEdge {
    /// The original edge (geometry intact)
    pub edge: GraphEdge,
    /// Original 'from' position (for spatial matching after processing)
    pub from_pos: Coord,
    /// Original 'to' position (for spatial matching after processing)
    pub to_pos: Coord,
}

impl LongEdgeRegistry {
    pub fn new() -> Self {
        Self { edges: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

/// Extract long intercity rail edges from the edge list.
///
/// Returns:
/// - Short edges (to be processed in clusters)
/// - LongEdgeRegistry (deferred edges to be reattached post-processing)
///
/// Long edges are identified as:
/// - route_type 2 (intercity rail)
/// - weight > INTERCITY_DEFER_THRESHOLD (50km)
pub fn extract_long_intercity_edges(edges: Vec<GraphEdge>) -> (Vec<GraphEdge>, LongEdgeRegistry) {
    let mut short_edges = Vec::with_capacity(edges.len());
    let mut registry = LongEdgeRegistry::new();

    for edge in edges {
        // Only defer route_type 2 (intercity rail) edges
        let is_intercity = edge.routes.iter().any(|(_, _, rt)| *rt == 2);
        let edge_length = edge.weight; // Already in meters (Web Mercator)

        if is_intercity && edge_length > INTERCITY_DEFER_THRESHOLD {
            // Extract endpoint positions for spatial matching
            let from_pos = edge
                .geometry
                .points
                .first()
                .map(|p| Coord { x: p.x, y: p.y })
                .unwrap_or(Coord { x: 0.0, y: 0.0 });
            let to_pos = edge
                .geometry
                .points
                .last()
                .map(|p| Coord { x: p.x, y: p.y })
                .unwrap_or(Coord { x: 0.0, y: 0.0 });

            registry.edges.push(DeferredEdge {
                edge,
                from_pos,
                to_pos,
            });
        } else {
            short_edges.push(edge);
        }
    }

    if !registry.is_empty() {
        println!(
            "  Deferred {} long intercity rail edges (>{}km) for post-processing",
            registry.len(),
            INTERCITY_DEFER_THRESHOLD / 1000.0
        );
    }

    (short_edges, registry)
}
