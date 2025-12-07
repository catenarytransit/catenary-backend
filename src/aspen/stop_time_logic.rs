use gtfs_realtime::trip_update::StopTimeUpdate;

pub fn find_closest_stop_time_update(
    stop_time_updates: &[StopTimeUpdate],
    current_time_unix_timestamp: u64,
) -> Option<StopTimeUpdate> {
    // Filter out updates with schedule_relationship == 1 (skipped?) - checking the original code logic
    // The original code: .filter(|x| x.schedule_relationship != Some(1))
    // We can do this in the loop.

    let mut best_update: Option<&StopTimeUpdate> = None;
    let mut best_arrival_time: Option<u64> = None;
    let mut last_update_in_list: Option<&StopTimeUpdate> = None;

    // We need to keep track if we found something "active" (happening right now)
    // The original code logic:
    // 1. Check if current time is roughly between arrival and departure. If so, return it immediately.
    // 2. If not, collect all future valid updates.
    // 3. Sort them by time.
    // 4. If all are in the past, return the last one (by sort order).
    // 5. If there are future ones, return the first one (closest future).

    // Let's refine the single pass logic:
    // We want:
    // A. An update where arrival <= now <= departure. Priority #1.
    // B. If no A, the update with the smallest arrival time > now. Priority #2.
    // C. If no A or B, the update with the largest arrival/departure time (since all are in past). Priority #3.

    for update in stop_time_updates {
        // Skip skipped stops
        if update.schedule_relationship == Some(1) {
            continue;
        }

        let arrival_time = update
            .arrival
            .as_ref()
            .and_then(|a| a.time)
            .map(|t| t as u64);
        let departure_time = update
            .departure
            .as_ref()
            .and_then(|d| d.time)
            .map(|t| t as u64);

        // Case A: Active now?
        if let (Some(arr), Some(dep)) = (arrival_time, departure_time) {
            if current_time_unix_timestamp >= arr && current_time_unix_timestamp <= dep {
                return Some(update.clone());
            }
        }

        // Track "closest future" (Case B)
        // We look for smallest time that is > current_time
        // We use arrival time primarily, fallback to departure?
        // Original code sorts by arrival, then departure.

        let comparison_time = arrival_time.or(departure_time);

        if let Some(time) = comparison_time {
            if time > current_time_unix_timestamp {
                // It is in the future
                match best_arrival_time {
                    Some(best_time) => {
                        if time < best_time {
                            best_arrival_time = Some(time);
                            best_update = Some(update);
                        }
                    }
                    None => {
                        // This is the first future one we found
                        best_arrival_time = Some(time);
                        best_update = Some(update);
                    }
                }
            }
        }

        // Track "last valid update" for Case C (all in past)
        // Original code: "closest_stop_time_update = sorted.last()"
        // "Sorted" means sorted by arrival/departure.
        // So we need to keep track of the one with the *largest* time if we don't find a future one.
        // But wait, if we find a future one, we don't care about the past ones.
        // If we *only* find past ones, we want the one that is "latest" in the past.

        // Actually, let's simplify.
        // We can just track "best candidate".
        // Criteria for "best":
        // 1. Active (handled immediately)
        // 2. Future (smallest delta positive)
        // 3. Past (smallest delta negative / largest time)

        // But we have two distinct states: checking for future, and falling back to past.
    }

    // If we found a future update, return it.
    if let Some(best) = best_update {
        return Some(best.clone());
    }

    // If we are here, it means no active update, and no future update.
    // We must return the "last" update (the one with the latest time).
    // Let's do a second pass or integrate it?
    // Integrating is cleaner but tricky with the specific sort logic.
    // Let's just iterate again or keep track of "max_time_update".

    let mut max_time: Option<u64> = None;
    let mut max_time_update: Option<&StopTimeUpdate> = None;

    for update in stop_time_updates {
        if update.schedule_relationship == Some(1) {
            continue;
        }
        let arrival_time = update
            .arrival
            .as_ref()
            .and_then(|a| a.time)
            .map(|t| t as u64);
        let departure_time = update
            .departure
            .as_ref()
            .and_then(|d| d.time)
            .map(|t| t as u64);

        let time = arrival_time.or(departure_time);

        if let Some(t) = time {
            match max_time {
                Some(max) => {
                    if t > max {
                        max_time = Some(t);
                        max_time_update = Some(update);
                    } else if t == max {
                        // tie breaker?
                        // original sort uses departure time as secondary.
                        // let's assume arrival/departure are consistent.
                        // If multiple updates have same max time (unlikely unless same station?), just keep one.
                    }
                }
                None => {
                    max_time = Some(t);
                    max_time_update = Some(update);
                }
            }
        }
    }

    if let Some(max) = max_time_update {
        return Some(max.clone());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtfs_realtime::trip_update::{StopTimeEvent, StopTimeUpdate};

    fn make_update(arrival: Option<i64>, departure: Option<i64>, id: &str) -> StopTimeUpdate {
        StopTimeUpdate {
            stop_sequence: None,
            stop_id: Some(id.to_string()),
            arrival: arrival.map(|t| StopTimeEvent {
                delay: None,
                time: Some(t),
                uncertainty: None,
                scheduled_time: None,
            }),
            departure: departure.map(|t| StopTimeEvent {
                delay: None,
                time: Some(t),
                uncertainty: None,
                scheduled_time: None,
            }),
            schedule_relationship: None,
            departure_occupancy_status: None,
            stop_time_properties: None,
        }
    }

    #[test]
    fn test_active_update() {
        let updates = vec![
            make_update(Some(100), Some(110), "1"),
            make_update(Some(120), Some(130), "2"),
        ];
        // Time 105 is inside "1"
        let res = find_closest_stop_time_update(&updates, 105);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "1");
    }

    #[test]
    fn test_future_update() {
        let updates = vec![
            make_update(Some(100), Some(110), "1"),
            make_update(Some(130), Some(140), "3"),
            make_update(Some(120), Some(130), "2"),
        ];
        // Time 115 is after 1, before 2 and 3. Closest future is 2 (120).
        let res = find_closest_stop_time_update(&updates, 115);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "2");
    }

    #[test]
    fn test_past_update() {
        let updates = vec![
            make_update(Some(100), Some(110), "1"),
            make_update(Some(120), Some(130), "2"),
        ];
        // Time 200 is after all. Should return last one ("2").
        let res = find_closest_stop_time_update(&updates, 200);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "2");
    }

    #[test]
    fn test_mixed_schedule_relationship() {
        let mut skipped = make_update(Some(120), Some(130), "skipped");
        skipped.schedule_relationship = Some(1);

        let updates = vec![
            make_update(Some(100), Some(110), "1"),
            skipped,
            make_update(Some(140), Some(150), "3"),
        ];
        // Time 115. "skipped" is at 120 but should be ignored. Closest future is 3.
        let res = find_closest_stop_time_update(&updates, 115);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "3");
    }
}
