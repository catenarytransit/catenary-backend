use catenary::compact_formats::CompactStopTimeUpdate;

pub fn find_closest_stop_time_update(
    stop_time_updates: &[CompactStopTimeUpdate],
    current_time_unix_timestamp: u64,
    reference_epoch: u64,
) -> Option<CompactStopTimeUpdate> {
    let mut best_update: Option<&CompactStopTimeUpdate> = None;
    let mut best_arrival_time: Option<u64> = None;

    // Helper to extract times
    let get_times = |update: &CompactStopTimeUpdate| -> (Option<u64>, Option<u64>) {
        let arr = update
            .arrival
            .as_deref()
            .and_then(|a| a.time)
            .map(|t| (reference_epoch as i64 + i32::from(t) as i64) as u64);
        let dep = update
            .departure
            .as_deref()
            .and_then(|d| d.time)
            .map(|t| (reference_epoch as i64 + i32::from(t) as i64) as u64);
        (arr, dep)
    };

    for update in stop_time_updates {
        if update.schedule_relationship == Some(1) {
            continue;
        }

        let (arrival_time, departure_time) = get_times(update);

        // Case A: Active now?
        if let (Some(arr), Some(dep)) = (arrival_time, departure_time) {
            if current_time_unix_timestamp >= arr && current_time_unix_timestamp <= dep {
                return Some(update.clone());
            }
        }

        // Case B: Future
        let comparison_time = arrival_time.or(departure_time);

        if let Some(time) = comparison_time {
            if time > current_time_unix_timestamp {
                match best_arrival_time {
                    Some(best_time) => {
                        if time < best_time {
                            best_arrival_time = Some(time);
                            best_update = Some(update);
                        }
                    }
                    None => {
                        best_arrival_time = Some(time);
                        best_update = Some(update);
                    }
                }
            }
        }
    }

    if let Some(best) = best_update {
        return Some(best.clone());
    }

    // Case C: Past (Last update)
    let mut max_time: Option<u64> = None;
    let mut max_time_update: Option<&CompactStopTimeUpdate> = None;

    for update in stop_time_updates {
        if update.schedule_relationship == Some(1) {
            continue;
        }
        let (arrival_time, departure_time) = get_times(update);

        let time = arrival_time.or(departure_time);

        if let Some(t) = time {
            match max_time {
                Some(max) => {
                    if t > max {
                        max_time = Some(t);
                        max_time_update = Some(update);
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
    use i24::I24;

    fn make_update(
        arrival: Option<i64>,
        departure: Option<i64>,
        id: &str,
        ref_epoch: u64,
    ) -> CompactStopTimeUpdate {
        CompactStopTimeUpdate {
            stop_sequence: None,
            stop_id: Some(id.to_string()),
            arrival: arrival.map(|t| {
                Box::new(CompactStopTimeEvent {
                    delay: None,
                    time: Some(I24::wrapping_from_i32((t - ref_epoch as i64) as i32)),
                    uncertainty: None,
                    scheduled_time: None,
                })
            }),
            departure: departure.map(|t| {
                Box::new(CompactStopTimeEvent {
                    delay: None,
                    time: Some(I24::wrapping_from_i32((t - ref_epoch as i64) as i32)),
                    uncertainty: None,
                    scheduled_time: None,
                })
            }),
            schedule_relationship: None,
            departure_occupancy_status: None,
            stop_time_properties: None,
        }
    }

    #[test]
    fn test_active_update() {
        let ref_epoch = 0;
        let updates = vec![
            make_update(Some(100), Some(110), "1", ref_epoch),
            make_update(Some(120), Some(130), "2", ref_epoch),
        ];
        // Time 105 is inside "1"
        let res = find_closest_stop_time_update(&updates, 105, ref_epoch);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "1");
    }

    #[test]
    fn test_future_update() {
        let ref_epoch = 0;
        let updates = vec![
            make_update(Some(100), Some(110), "1", ref_epoch),
            make_update(Some(130), Some(140), "3", ref_epoch),
            make_update(Some(120), Some(130), "2", ref_epoch),
        ];
        // Time 115 is after 1, before 2 and 3. Closest future is 2 (120).
        let res = find_closest_stop_time_update(&updates, 115, ref_epoch);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "2");
    }

    #[test]
    fn test_past_update() {
        let ref_epoch = 0;
        let updates = vec![
            make_update(Some(100), Some(110), "1", ref_epoch),
            make_update(Some(120), Some(130), "2", ref_epoch),
        ];
        // Time 200 is after all. Should return last one ("2").
        let res = find_closest_stop_time_update(&updates, 200, ref_epoch);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "2");
    }

    #[test]
    fn test_mixed_schedule_relationship() {
        let ref_epoch = 0;
        let mut skipped = make_update(Some(120), Some(130), "skipped", ref_epoch);
        skipped.schedule_relationship = Some(1);

        let updates = vec![
            make_update(Some(100), Some(110), "1", ref_epoch),
            skipped,
            make_update(Some(140), Some(150), "3", ref_epoch),
        ];
        // Time 115. "skipped" is at 120 but should be ignored. Closest future is 3.
        let res = find_closest_stop_time_update(&updates, 115, ref_epoch);
        assert!(res.is_some());
        assert_eq!(res.unwrap().stop_id.unwrap(), "3");
    }
}
