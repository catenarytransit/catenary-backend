// Copyright Catenary Transit Initiatives
// Stop matching utilities for parent-child relationships

use ahash::AHashMap;

/// Checks if an RT stop_id matches a scheduled stop_id,
/// accounting for parent-child relationships and underscore-suffixed patterns.
///
/// This handles cases like SNCB/NMBS where RT stop IDs use platform suffixes:
/// - RT: "8833001_7" (child stop with platform 7)
/// - Scheduled: "8833001" (parent station)
pub fn rt_stop_matches_scheduled(
    rt_stop_id: &str,
    scheduled_stop_id: &str,
    stop_id_to_parent_id: &AHashMap<String, String>,
) -> bool {
    // Case 1: Direct match
    if rt_stop_id == scheduled_stop_id {
        return true;
    }

    // Case 2: RT stop is a child of the scheduled stop (parent)
    if let Some(rt_parent) = stop_id_to_parent_id.get(rt_stop_id) {
        if rt_parent == scheduled_stop_id {
            return true;
        }
    }

    // Case 3: RT stop_id has underscore suffix (e.g., "8833001_7")
    // where the parent part matches the scheduled stop
    if let Some((parent_part, _)) = rt_stop_id.rsplit_once('_') {
        if parent_part == scheduled_stop_id {
            return true;
        }
    }

    false
}

/// Simpler version of stop matching for handlers without mapping access.
/// Falls back to underscore suffix pattern matching only.
pub fn rt_stop_matches_scheduled_simple(rt_stop_id: &str, scheduled_stop_id: &str) -> bool {
    // Case 1: Direct match
    if rt_stop_id == scheduled_stop_id {
        return true;
    }

    // Case 2: RT stop_id has underscore suffix (e.g., "8833001_7")
    // where the parent part matches the scheduled stop
    if let Some((parent_part, _)) = rt_stop_id.rsplit_once('_') {
        if parent_part == scheduled_stop_id {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direct_match() {
        let map = AHashMap::new();
        assert!(rt_stop_matches_scheduled("STOP123", "STOP123", &map));
    }

    #[test]
    fn test_parent_child_mapping() {
        let mut map = AHashMap::new();
        map.insert("STOP123_7".to_string(), "STOP123".to_string());
        assert!(rt_stop_matches_scheduled("STOP123_7", "STOP123", &map));
    }

    #[test]
    fn test_underscore_suffix_fallback() {
        let map = AHashMap::new();
        assert!(rt_stop_matches_scheduled("8833001_7", "8833001", &map));
    }

    #[test]
    fn test_no_match() {
        let map = AHashMap::new();
        assert!(!rt_stop_matches_scheduled("STOP999", "STOP123", &map));
    }

    #[test]
    fn test_simple_direct_match() {
        assert!(rt_stop_matches_scheduled_simple("STOP123", "STOP123"));
    }

    #[test]
    fn test_simple_underscore_suffix() {
        assert!(rt_stop_matches_scheduled_simple("8833001_7", "8833001"));
    }
}
