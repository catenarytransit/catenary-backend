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

/// Find realtime for one scheduled stop occurrence. A sequence identifies the
/// occurrence even when a loop visits the same stop with different headsigns.
/// Only use an unsequenced stop ID when it identifies one scheduled occurrence.
pub fn find_stop_time_update<'a, 's>(
    updates: &'a [crate::aspen_dataset::AspenisedStopTimeUpdate],
    scheduled_sequence: u32,
    scheduled_stop_id: &str,
    scheduled_stop_ids: impl Iterator<Item = &'s str> + Clone,
) -> Option<&'a crate::aspen_dataset::AspenisedStopTimeUpdate> {
    updates
        .iter()
        .find(|update| update.stop_sequence.map(u32::from) == Some(scheduled_sequence))
        .or_else(|| {
            updates.iter().find(|update| {
                update.stop_sequence.is_none()
                    && update.stop_id.as_deref().is_some_and(|stop_id| {
                        rt_stop_matches_scheduled_simple(stop_id, scheduled_stop_id)
                            && scheduled_stop_ids
                                .clone()
                                .filter(|id| rt_stop_matches_scheduled_simple(stop_id, id))
                                .take(2)
                                .count()
                                == 1
                    })
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(
        sequence: Option<u16>,
        stop_id: Option<&str>,
        time: i64,
    ) -> crate::aspen_dataset::AspenisedStopTimeUpdate {
        use crate::aspen_dataset::{AspenStopTimeEvent, AspenisedStopTimeUpdate};
        AspenisedStopTimeUpdate {
            stop_sequence: sequence,
            stop_id: stop_id.map(Into::into),
            departure: Some(AspenStopTimeEvent {
                time: Some(time),
                delay: None,
                uncertainty: None,
            }),
            arrival: None,
            departure_occupancy_status: None,
            schedule_relationship: None,
            stop_time_properties: None,
            platform_string: None,
            old_rt_data: false,
            platform_info: None,
        }
    }

    #[test]
    fn loop_terminus_keeps_start_and_end_predictions_separate() {
        // These visits may have different stop_headsigns, but share the stop ID.
        let stops = ["terminus", "middle", "terminus"];
        let updates = [
            update(Some(1), Some("terminus"), 36000),
            update(Some(3), Some("terminus"), 37500),
        ];
        for (sequence, scheduled_time) in [(1, 36000), (3, 37500)] {
            let prediction =
                find_stop_time_update(&updates, sequence, "terminus", stops.into_iter()).unwrap();
            assert_eq!(
                prediction.departure.as_ref().unwrap().time.unwrap() - scheduled_time,
                0
            );
        }
        let reversed = [updates[1].clone(), updates[0].clone()];
        assert_eq!(
            find_stop_time_update(&reversed, 1, "terminus", stops.into_iter())
                .unwrap()
                .stop_sequence,
            Some(1)
        );
    }

    #[test]
    fn wrong_sequence_cannot_match_by_stop_id() {
        let updates = [update(Some(1), Some("terminus"), 36000)];
        assert!(
            find_stop_time_update(
                &updates,
                3,
                "terminus",
                ["terminus", "terminus"].into_iter()
            )
            .is_none()
        );
        // Comparing as u32 also prevents a large static sequence wrapping to u16.
        assert!(
            find_stop_time_update(&updates, 65537, "terminus", ["terminus"].into_iter()).is_none()
        );
    }

    #[test]
    fn sequence_only_updates_are_supported() {
        let updates = [update(Some(3), None, 37500)];
        assert!(
            find_stop_time_update(
                &updates,
                3,
                "terminus",
                ["terminus", "terminus"].into_iter()
            )
            .is_some()
        );
    }

    #[test]
    fn unsequenced_update_requires_unambiguous_stop() {
        let updates = [update(None, Some("terminus"), 36000)];
        assert!(
            find_stop_time_update(
                &updates,
                1,
                "terminus",
                ["terminus", "middle", "terminus"].into_iter()
            )
            .is_none()
        );
        assert!(
            find_stop_time_update(&updates, 1, "terminus", ["terminus", "middle"].into_iter())
                .is_some()
        );
    }

    #[test]
    fn exact_sequence_precedes_unsequenced_fallback() {
        let updates = [
            update(None, Some("stop"), 36000),
            update(Some(2), Some("stop"), 36120),
        ];
        assert_eq!(
            find_stop_time_update(&updates, 2, "stop", ["stop"].into_iter())
                .unwrap()
                .stop_sequence,
            Some(2)
        );
    }

    #[test]
    fn unsequenced_platform_suffix_preserves_unique_fallback() {
        let updates = [update(None, Some("8833001_7"), 36000)];
        assert!(find_stop_time_update(&updates, 1, "8833001", ["8833001"].into_iter()).is_some());
        assert!(
            find_stop_time_update(&updates, 1, "8833001", ["8833001", "8833001_7"].into_iter())
                .is_none()
        );
    }

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
