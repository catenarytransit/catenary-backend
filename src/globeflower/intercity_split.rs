// ===========================================================================
// Adaptive Intercity Rail Processing
// ===========================================================================
// Long intercity rail edges (route_type 2) can be 500+ km. Instead of
// deferring them, we use ADAPTIVE DENSIFICATION:
//
// - Short edges (<5km): 5m spacing (dense, accurate curves)
// - Long edges (>5km): Coarser spacing based on length
// - Maximum ~2000 points per edge to bound memory
//
// This allows intercity lines to be processed with everything else,
// enabling proper parallel track merging and geometric averaging.
// ===========================================================================

/// Target maximum points per edge after densification
const MAX_POINTS_PER_EDGE: usize = 2000;

/// Threshold for fine densification (5km)
const FINE_DENSIFY_THRESHOLD: f64 = 5_000.0;

/// Fine densification spacing (5m)
const FINE_SPACING: f64 = 5.0;

/// Calculate adaptive densification spacing for an edge.
///
/// This ensures long intercity edges don't create excessive points
/// while still maintaining sufficient geometric detail for collapse algorithms.
///
/// Returns the spacing in meters to use for densification.
pub fn adaptive_densify_spacing(edge_length: f64) -> f64 {
    if edge_length <= FINE_DENSIFY_THRESHOLD {
        // Short edges: use fine 5m spacing for accurate curves
        FINE_SPACING
    } else {
        // Long edges: calculate spacing to stay under MAX_POINTS_PER_EDGE
        // spacing = edge_length / (target_points - 1)
        // We want enough points for good geometry but not OOM
        let target_points = MAX_POINTS_PER_EDGE as f64;
        let coarse_spacing = edge_length / target_points;
        
        // Never go finer than 5m (pointless) or coarser than 200m (too sparse)
        coarse_spacing.max(FINE_SPACING).min(200.0)
    }
}
