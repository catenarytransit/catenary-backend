use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use catenary::postgres_tools::CatenaryPostgresPool;
use geo::prelude::*;
use geo::{Coord, LineString as GeoLineString, Point as GeoPoint};
use postgis_diesel::types::{LineString, Point};
use rayon::prelude::*;
use rstar::{AABB, RTree, RTreeObject};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use crate::edges::*;
use crate::graph_types::*;

/// Bounding box in the format (min_x, min_y, max_x, max_y)
pub type BBox = (f64, f64, f64, f64);

/// Compute bounding box of an edge's geometry
pub fn compute_edge_bbox(edge: &GraphEdge) -> BBox {
    let (mut min_x, mut min_y, mut max_x, mut max_y) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
    for pt in &edge.geometry.points {
        min_x = min_x.min(pt.x);
        min_y = min_y.min(pt.y);
        max_x = max_x.max(pt.x);
        max_y = max_y.max(pt.y);
    }
    (min_x, min_y, max_x, max_y)
}

/// Validate that an edge's geometry stays within expected bounds.
/// Returns true if valid, false if corruption detected.
pub fn validate_edge_geometry(
    edge: &GraphEdge,
    original_bbox: BBox,
    max_expansion: f64, // Maximum allowed expansion in meters (Web Mercator)
    context: &str,
) -> bool {
    let current_bbox = compute_edge_bbox(edge);

    // Check if bbox has expanded beyond threshold
    let dx_min = (current_bbox.0 - original_bbox.0).abs();
    let dy_min = (current_bbox.1 - original_bbox.1).abs();
    let dx_max = (current_bbox.2 - original_bbox.2).abs();
    let dy_max = (current_bbox.3 - original_bbox.3).abs();

    let max_delta = dx_min.max(dy_min).max(dx_max).max(dy_max);

    if max_delta > max_expansion {
        eprintln!("WARNING: Edge geometry corruption detected [{}]", context);
        eprintln!(
            "  Original bbox: ({:.1}, {:.1}) - ({:.1}, {:.1})",
            original_bbox.0, original_bbox.1, original_bbox.2, original_bbox.3
        );
        eprintln!(
            "  Current bbox:  ({:.1}, {:.1}) - ({:.1}, {:.1})",
            current_bbox.0, current_bbox.1, current_bbox.2, current_bbox.3
        );
        eprintln!(
            "  Max delta: {:.1}m, threshold: {:.1}m",
            max_delta, max_expansion
        );
        eprintln!("  Routes: {:?}", edge.routes);
        eprintln!("  From: {:?}, To: {:?}", edge.from, edge.to);
        return false;
    }
    true
}

/// Validate a batch of edges against their original bboxes.
/// Returns count of corrupted edges found.
pub fn validate_edge_batch(
    edges: &[GraphEdge],
    original_bboxes: &HashMap<usize, BBox>,
    max_expansion: f64,
    context: &str,
) -> usize {
    let mut corrupted_count = 0;
    for (idx, edge) in edges.iter().enumerate() {
        if let Some(&original_bbox) = original_bboxes.get(&idx) {
            if !validate_edge_geometry(edge, original_bbox, max_expansion, context) {
                corrupted_count += 1;
            }
        }
    }
    if corrupted_count > 0 {
        eprintln!(
            "=== VALIDATION FAILED [{}]: {} corrupted edges out of {} ===",
            context,
            corrupted_count,
            edges.len()
        );
    }
    corrupted_count
}



