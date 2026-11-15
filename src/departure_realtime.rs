//! Shared realtime overlays for GTFS-stop and OSM-station departure boards.
use crate::aspen_dataset::*;
use crate::departures_shared::MovedStopData;
use chrono::NaiveDate;
use compact_str::CompactString;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};

#[derive(Serialize, Clone, Debug)]
pub struct StopEvent {
    #[serde(skip)]
    pub stop_sequence: Option<u32>,
    #[serde(skip)]
    pub trip_start_time: Option<u32>,
    pub scheduled_arrival: Option<u64>,
    pub scheduled_departure: Option<u64>,
    pub realtime_arrival: Option<u64>,
    pub realtime_departure: Option<u64>,
    pub trip_modified: bool,
    pub stop_cancelled: bool,
    pub trip_cancelled: bool,
    pub trip_deleted: bool,
    pub trip_id: String,
    pub headsign: Option<String>,
    pub route_id: String,
    pub chateau: String,
    pub stop_id: String,
    pub uses_primary_stop: bool,
    pub unscheduled_trip: bool,
    pub moved_info: Option<MovedStopData>,
    pub platform_string_realtime: Option<String>,
    pub level_id: Option<String>,
    pub platform_code: Option<String>,
    pub vehicle_number: Option<String>,
    pub trip_short_name: Option<CompactString>,
    pub service_date: Option<NaiveDate>,
    pub last_stop: bool,
    pub scheduled_trip_shape_id: Option<CompactString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_station_name: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DepartureRealtime {
    pub searched_stop_ids: Vec<String>,
    pub realtime_stops: BTreeMap<String, AspenisedStop>,
    pub modifications: BTreeMap<String, AspenisedTripModification>,
    pub updates: Vec<AspenisedTripUpdate>,
}

/// A call in the modified schedule. Original sequence is retained so loop stops
/// and multiple modifications can be resolved against the original GTFS trip.
#[derive(Clone, Debug, PartialEq)]
pub struct ModifiedCall {
    pub stop_id: String,
    pub original_sequence: Option<u32>,
    pub arrival: Option<i64>,
    pub departure: Option<i64>,
}

pub fn parse_start_time(time: &str) -> Option<u32> {
    let fields: Vec<_> = time.split(':').collect();
    if fields.len() != 3 {
        return None;
    }
    let (h, m, s): (u32, u32, u32) = (
        fields[0].parse().ok()?,
        fields[1].parse().ok()?,
        fields[2].parse().ok()?,
    );
    if m >= 60 || s >= 60 {
        return None;
    }
    h.checked_mul(3600)?.checked_add(m * 60 + s)
}

fn selector_index(calls: &[ModifiedCall], selector: &AspenStopSelector) -> Option<usize> {
    // Sequence takes precedence; never match a different visit to a loop stop.
    if let Some(sequence) = selector.stop_sequence {
        return calls.iter().position(|call| {
            call.original_sequence == Some(sequence)
                && selector
                    .stop_id
                    .as_deref()
                    .is_none_or(|id| call.stop_id == id)
        });
    }
    let id = selector.stop_id.as_deref()?;
    let mut matches = calls
        .iter()
        .enumerate()
        .filter(|(_, call)| call.stop_id == id);
    let index = matches.next()?.0;
    if matches.next().is_some() {
        return None;
    }
    Some(index)
}

/// Apply inclusive replacement spans and cumulative downstream delay. Travel
/// times are relative to the original preceding stop (first stop at trip start).
/// Invalid/ambiguous selectors leave the caller's static schedule untouched.
pub fn modified_calls(
    original: &[ModifiedCall],
    modifications: &[AspenModification],
) -> Option<Vec<ModifiedCall>> {
    let mut spans = Vec::new();
    for modification in modifications {
        let start = selector_index(original, modification.start_stop_selector.as_ref()?)?;
        let end = modification
            .end_stop_selector
            .as_ref()
            .map(|selector| selector_index(original, selector))
            .transpose_option()?;
        if end.is_some_and(|end| end < start) {
            return None;
        }
        spans.push((start, end.map_or(start, |end| end + 1), modification));
    }
    spans.sort_by_key(|(start, _, _)| *start);
    let mut output = Vec::new();
    let mut cursor = 0;
    let mut delay = 0i64;
    for (start, end, modification) in spans {
        if start < cursor {
            return None;
        }
        for call in &original[cursor..start] {
            output.push(shift_call(call, delay));
        }
        let reference = &original[start.saturating_sub(1)];
        let reference_time = reference.arrival.or(reference.departure);
        for stop in &modification.replacement_stops {
            let time = reference_time
                .zip(stop.travel_time_to_stop)
                .and_then(|(time, offset)| time.checked_add(delay)?.checked_add(i64::from(offset)));
            output.push(ModifiedCall {
                stop_id: stop.stop_id.clone()?,
                original_sequence: None,
                arrival: time,
                departure: time,
            });
        }
        delay = delay.checked_add(i64::from(
            modification.propagated_modification_delay.unwrap_or(0),
        ))?;
        cursor = end;
    }
    for call in &original[cursor..] {
        output.push(shift_call(call, delay));
    }
    Some(output)
}

// Unlike Option::transpose (for Result), an absent selector is valid while an
// invalid present selector must abort the modification.
trait TransposeOption<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}
impl<T> TransposeOption<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            None => Some(None),
            Some(value) => value.map(Some),
        }
    }
}
fn shift_call(call: &ModifiedCall, delay: i64) -> ModifiedCall {
    ModifiedCall {
        stop_id: call.stop_id.clone(),
        original_sequence: call.original_sequence,
        arrival: call.arrival.and_then(|time| time.checked_add(delay)),
        departure: call.departure.and_then(|time| time.checked_add(delay)),
    }
}

pub fn empty_event(chateau: &str, trip_id: &str, route_id: &str, stop_id: &str) -> StopEvent {
    StopEvent {
        stop_sequence: None,
        trip_start_time: None,
        scheduled_arrival: None,
        scheduled_departure: None,
        realtime_arrival: None,
        realtime_departure: None,
        trip_modified: false,
        stop_cancelled: false,
        trip_cancelled: false,
        trip_deleted: false,
        trip_id: trip_id.into(),
        headsign: None,
        route_id: route_id.into(),
        chateau: chateau.into(),
        stop_id: stop_id.into(),
        uses_primary_stop: true,
        unscheduled_trip: false,
        moved_info: None,
        platform_string_realtime: None,
        level_id: None,
        platform_code: None,
        vehicle_number: None,
        trip_short_name: None,
        service_date: None,
        last_stop: false,
        scheduled_trip_shape_id: None,
        final_station_name: None,
    }
}

/// Update a single modified trip instance atomically. Match against a snapshot
/// of the original sequence numbers before renumbering the resulting calls.
pub fn apply_modified_schedule(
    events: &mut Vec<StopEvent>,
    template: &StopEvent,
    calls: &[ModifiedCall],
    stops: &HashSet<String>,
) {
    let instance = |event: &StopEvent| {
        event.chateau == template.chateau
            && event.trip_id == template.trip_id
            && event.service_date == template.service_date
            && event.trip_start_time == template.trip_start_time
    };
    let original: Vec<_> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| instance(event))
        .map(|(index, event)| (index, event.clone()))
        .collect();
    for (index, event) in &original {
        events[*index].trip_modified = true;
        events[*index].stop_cancelled |= !calls.iter().any(|call| {
            call.original_sequence.is_some() && call.original_sequence == event.stop_sequence
        });
    }
    for (sequence, call) in calls
        .iter()
        .enumerate()
        .filter(|(_, call)| stops.contains(&call.stop_id))
    {
        let existing = original.iter().find(|(_, event)| {
            call.original_sequence.is_some() && event.stop_sequence == call.original_sequence
        });
        let mut event = existing
            .map(|(_, event)| event.clone())
            .unwrap_or_else(|| template.clone());
        event.stop_id = call.stop_id.clone();
        event.stop_sequence = Some(sequence as u32 + 1);
        event.trip_modified = true;
        event.realtime_arrival = None;
        event.realtime_departure = None;
        event.scheduled_arrival = positive_time(call.arrival);
        event.scheduled_departure = positive_time(call.departure);
        event.last_stop = sequence + 1 == calls.len();
        event.scheduled_trip_shape_id = template.scheduled_trip_shape_id.clone();
        if let Some((index, _)) = existing {
            events[*index] = event;
        } else {
            events.push(event);
        }
    }
}

fn positive_time(time: Option<i64>) -> Option<u64> {
    time.and_then(|time| time.try_into().ok())
}

/// Apply realtime-only calls once per occurrence, not once per stop ID. The
/// caller has already applied TripModifications to the scheduled board.
pub fn overlay_updates(
    events: &mut Vec<StopEvent>,
    chateau: &str,
    stops: &HashSet<String>,
    updates: &[AspenisedTripUpdate],
) {
    for update in updates {
        let relationship = update.trip.schedule_relationship.as_ref();
        let is_replacement = relationship == Some(&AspenisedTripScheduleRelationship::Replacement);
        let modified = update.trip.modified_trip.as_ref();
        let trip_id = modified
            .and_then(|m| m.affected_trip_id.as_deref())
            .or_else(|| {
                update
                    .trip_properties
                    .as_ref()
                    .and_then(|p| p.trip_id.as_deref())
                    .or(update.trip.trip_id.as_deref())
            });
        let Some(trip_id) = trip_id else {
            continue;
        };
        let date = update
            .trip_properties
            .as_ref()
            .and_then(|p| p.start_date)
            .or(update.trip.effective_start_date());
        // Without a service date, only match an existing call by its absolute
        // scheduled timestamp. Do not apply an undated update to every day.
        let instance_matches =
            |event: &StopEvent| {
                event.chateau == chateau
                    && event.trip_id == trip_id
                    && (!event.trip_modified
                        || modified.is_some()
                        || is_replacement
                        || matches!(
                            relationship,
                            Some(
                                AspenisedTripScheduleRelationship::Deleted
                                    | AspenisedTripScheduleRelationship::Cancelled
                            )
                        ))
                    && update
                        .trip
                        .effective_start_time()
                        .and_then(parse_start_time)
                        .is_none_or(|start| event.trip_start_time == Some(start))
                    && match date {
                        Some(date) => event.service_date == Some(date),
                        None => update.stop_time_update.iter().any(|stu| {
                            stu.arrival
                                .as_ref()
                                .and_then(|e| {
                                    positive_time(e.time.zip(e.delay).and_then(|(time, delay)| {
                                        time.checked_sub(i64::from(delay))
                                    }))
                                })
                                .is_some_and(|time| event.scheduled_arrival == Some(time))
                                || stu
                                    .departure
                                    .as_ref()
                                    .and_then(|e| {
                                        positive_time(e.time.zip(e.delay).and_then(
                                            |(time, delay)| time.checked_sub(i64::from(delay)),
                                        ))
                                    })
                                    .is_some_and(|time| event.scheduled_departure == Some(time))
                        }),
                    }
            };
        if matches!(
            relationship,
            Some(
                AspenisedTripScheduleRelationship::Deleted
                    | AspenisedTripScheduleRelationship::Cancelled
            )
        ) {
            for event in events.iter_mut().filter(|event| instance_matches(event)) {
                event.trip_deleted =
                    relationship == Some(&AspenisedTripScheduleRelationship::Deleted);
                event.trip_cancelled =
                    relationship == Some(&AspenisedTripScheduleRelationship::Cancelled);
            }
        }
        if is_replacement && date.is_some() {
            for event in events.iter_mut().filter(|event| instance_matches(event)) {
                event.trip_modified = true;
                event.stop_cancelled = true;
            }
        }
        for (index, stu) in update.stop_time_update.iter().enumerate() {
            let stop_id = stu.stop_id.as_deref().map(str::to_owned).or_else(|| {
                let candidates: Vec<_> = events
                    .iter()
                    .filter(|event| {
                        instance_matches(event)
                            && stu.stop_sequence.is_some()
                            && event.stop_sequence == stu.stop_sequence.map(u32::from)
                    })
                    .collect();
                (candidates.len() == 1).then(|| candidates[0].stop_id.clone())
            });
            let Some(stop_id) = stop_id else {
                continue;
            };
            let assigned = stu
                .stop_time_properties
                .as_ref()
                .and_then(|properties| properties.assigned_stop_id.as_deref())
                .unwrap_or(stop_id.as_str());
            if !stops.contains(assigned) {
                continue;
            }
            let arrival = stu
                .arrival
                .as_ref()
                .and_then(|event| positive_time(event.time));
            let departure = stu
                .departure
                .as_ref()
                .and_then(|event| positive_time(event.time));
            // Match repeated stop visits by their scheduled timestamps when present.
            let scheduled_arrival = stu.arrival.as_ref().and_then(|e| {
                positive_time(
                    e.time
                        .zip(e.delay)
                        .and_then(|(time, delay)| time.checked_sub(i64::from(delay))),
                )
            });
            let scheduled_departure = stu.departure.as_ref().and_then(|e| {
                positive_time(
                    e.time
                        .zip(e.delay)
                        .and_then(|(time, delay)| time.checked_sub(i64::from(delay))),
                )
            });
            let candidates: Vec<_> = events
                .iter()
                .enumerate()
                .filter(|(_, event)| {
                    instance_matches(event)
                        && event.stop_id == assigned
                        && (stu.stop_sequence.is_none()
                            || event.stop_sequence == stu.stop_sequence.map(u32::from))
                        && (scheduled_arrival.is_none()
                            || event.scheduled_arrival == scheduled_arrival)
                        && (scheduled_departure.is_none()
                            || event.scheduled_departure == scheduled_departure)
                })
                .map(|(i, _)| i)
                .collect();
            let existing = if candidates.len() == 1 {
                candidates.first().copied()
            } else {
                None
            };
            let realtime_only = matches!(
                relationship,
                Some(
                    AspenisedTripScheduleRelationship::Added
                        | AspenisedTripScheduleRelationship::Unscheduled
                        | AspenisedTripScheduleRelationship::Duplicated
                        | AspenisedTripScheduleRelationship::Replacement
                )
            ) || modified.is_some();
            if existing.is_none() && (!realtime_only || (arrival.is_none() && departure.is_none()))
            {
                continue;
            }
            let mut event = existing.map(|i| events[i].clone()).unwrap_or_else(|| {
                let mut event = empty_event(
                    chateau,
                    trip_id,
                    update.trip.route_id.as_deref().unwrap_or(""),
                    assigned,
                );
                event.unscheduled_trip = true;
                event.service_date = date;
                event
            });
            event.stop_sequence = stu.stop_sequence.map(u32::from).or(event.stop_sequence);
            event.trip_start_time = update
                .trip
                .effective_start_time()
                .and_then(parse_start_time)
                .or(event.trip_start_time);
            event.realtime_arrival = arrival
                .or_else(|| {
                    stu.arrival.as_ref().and_then(|rt| {
                        event
                            .scheduled_arrival
                            .zip(rt.delay)
                            .and_then(|(time, delay)| time.checked_add_signed(i64::from(delay)))
                    })
                })
                .or(event.realtime_arrival);
            event.realtime_departure = departure
                .or_else(|| {
                    stu.departure.as_ref().and_then(|rt| {
                        event
                            .scheduled_departure
                            .zip(rt.delay)
                            .and_then(|(time, delay)| time.checked_add_signed(i64::from(delay)))
                    })
                })
                .or(event.realtime_departure);
            event.trip_modified |= modified.is_some() || is_replacement;
            event.stop_cancelled =
                stu.schedule_relationship == Some(AspenisedStopTimeScheduleRelationship::Skipped);
            event.trip_cancelled =
                relationship == Some(&AspenisedTripScheduleRelationship::Cancelled);
            event.trip_deleted = relationship == Some(&AspenisedTripScheduleRelationship::Deleted);
            event.headsign = update
                .trip_properties
                .as_ref()
                .and_then(|p| p.trip_headsign.clone())
                .or_else(|| update.trip_headsign.as_deref().map(str::to_owned))
                .or(event.headsign);
            event.trip_short_name = update
                .trip_properties
                .as_ref()
                .and_then(|p| p.trip_short_name.as_deref().map(Into::into))
                .or(event.trip_short_name);
            event.vehicle_number = update
                .vehicle
                .as_ref()
                .and_then(|v| v.label.clone())
                .or(event.vehicle_number);
            event.platform_string_realtime = stu
                .platform_string
                .as_ref()
                .map(ToString::to_string)
                .or(event.platform_string_realtime);
            event.scheduled_trip_shape_id = update
                .trip_properties
                .as_ref()
                .and_then(|p| p.shape_id.as_deref().map(Into::into))
                .or(event.scheduled_trip_shape_id);
            if realtime_only {
                event.last_stop = index + 1 == update.stop_time_update.len();
            }
            if let Some(i) = existing {
                events[i] = event;
            } else {
                events.push(event);
            }
        }
    }
}

/// Select nearby calls only after applying modifications and trip-level updates.
pub fn nearby_event_time(event: &StopEvent, start: u64) -> Option<u64> {
    if event.trip_deleted {
        return None;
    }
    let time = event
        .realtime_departure
        .or(event.realtime_arrival)
        .or(event.scheduled_departure)
        .or(event.scheduled_arrival)?;
    (time >= start && time <= start.saturating_add(18 * 3600)).then_some(time)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn call(id: &str, sequence: u32, time: i64) -> ModifiedCall {
        ModifiedCall {
            stop_id: id.into(),
            original_sequence: Some(sequence),
            arrival: Some(time),
            departure: Some(time + 10),
        }
    }
    fn selector(sequence: u32) -> AspenStopSelector {
        AspenStopSelector {
            stop_sequence: Some(sequence),
            stop_id: None,
        }
    }
    fn modification(
        start: u32,
        end: Option<u32>,
        replacement: &[(&str, i32)],
        delay: i32,
    ) -> AspenModification {
        AspenModification {
            start_stop_selector: Some(selector(start)),
            end_stop_selector: end.map(selector),
            propagated_modification_delay: Some(delay),
            replacement_stops: replacement
                .iter()
                .map(|(id, time)| AspenReplacementStop {
                    stop_id: Some((*id).into()),
                    travel_time_to_stop: Some(*time),
                })
                .collect(),
            service_alert_id: None,
            last_modified_time: None,
        }
    }
    fn update(relationship: AspenisedTripScheduleRelationship) -> AspenisedTripUpdate {
        serde_json::from_value(serde_json::json!({
            "trip": { "trip_id": "trip", "route_id": "route", "start_date": "2026-11-14", "schedule_relationship": relationship },
            "stop_time_update": [], "found_schedule_trip_id": false
        })).unwrap()
    }
    fn stu(id: &str, sequence: u16, time: i64) -> AspenisedStopTimeUpdate {
        serde_json::from_value(serde_json::json!({
            "stop_id": id, "stop_sequence": sequence,
            "departure": {"time": time}, "old_rt_data": false
        }))
        .unwrap()
    }
    #[test]
    fn inclusive_span_and_downstream_delay() {
        let calls = [
            call("a", 10, 100),
            call("b", 20, 200),
            call("c", 30, 300),
            call("d", 40, 400),
        ];
        let result = modified_calls(
            &calls,
            &[modification(20, Some(30), &[("x", 50), ("y", 90)], 35)],
        )
        .unwrap();
        assert_eq!(
            result
                .iter()
                .map(|c| c.stop_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "x", "y", "d"]
        );
        assert_eq!(result[1].arrival, Some(150));
        assert_eq!(result[3].departure, Some(445));
    }
    #[test]
    fn insertion_and_first_stop_reference() {
        let calls = [call("a", 1, 100), call("b", 2, 200)];
        let result = modified_calls(&calls, &[modification(1, None, &[("x", -5)], 20)]).unwrap();
        assert_eq!(result[0].arrival, Some(95));
        assert_eq!(result[1].arrival, Some(120));
        assert_eq!(result[2].arrival, Some(220));
    }
    #[test]
    fn multiple_modifications_accumulate_delays() {
        let calls = [
            call("a", 1, 100),
            call("b", 2, 200),
            call("c", 3, 300),
            call("d", 4, 400),
        ];
        let result = modified_calls(
            &calls,
            &[
                modification(3, Some(3), &[("y", 70)], 20),
                modification(2, Some(2), &[("x", 50)], 10),
            ],
        )
        .unwrap();
        assert_eq!(result[2].arrival, Some(280));
        assert_eq!(result[3].arrival, Some(430));
    }
    #[test]
    fn loop_selectors_and_invalid_spans() {
        let calls = [call("a", 1, 100), call("b", 2, 200), call("a", 3, 300)];
        let mut m = modification(3, Some(3), &[], 0);
        let result = modified_calls(&calls, &[m.clone()]).unwrap();
        assert_eq!(result.len(), 2);
        m.start_stop_selector = Some(AspenStopSelector {
            stop_sequence: None,
            stop_id: Some("a".into()),
        });
        assert!(modified_calls(&calls, &[m]).is_none());
        assert!(modified_calls(&calls, &[modification(3, Some(1), &[], 0)]).is_none());
        assert!(
            modified_calls(
                &calls,
                &[
                    modification(2, Some(3), &[], 0),
                    modification(3, Some(3), &[], 0)
                ]
            )
            .is_none()
        );
    }
    #[test]
    fn trip_level_deletion_without_stop_updates_is_instance_scoped() {
        for relationship in [
            AspenisedTripScheduleRelationship::Deleted,
            AspenisedTripScheduleRelationship::Cancelled,
        ] {
            let mut u = update(relationship.clone());
            u.trip.start_time = Some("25:00:00".into());
            let mut event = empty_event("feed", "trip", "route", "a");
            event.service_date = u.trip.start_date;
            event.trip_start_time = Some(25 * 3600);
            event.trip_modified = true;
            let mut other_day = event.clone();
            other_day.service_date = event.service_date.map(|d| d.succ_opt().unwrap());
            let mut other_run = event.clone();
            other_run.trip_start_time = Some(26 * 3600);
            let mut other_feed = event.clone();
            other_feed.chateau = "other".into();
            let mut events = vec![event, other_day, other_run, other_feed];
            overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &[u]);
            assert_eq!(
                events[0].trip_deleted,
                relationship == AspenisedTripScheduleRelationship::Deleted
            );
            assert_eq!(
                events[0].trip_cancelled,
                relationship == AspenisedTripScheduleRelationship::Cancelled
            );
            assert!(
                events[1..]
                    .iter()
                    .all(|e| !e.trip_deleted && !e.trip_cancelled)
            );
        }
    }

    #[test]
    fn nearby_filters_after_detour_added_and_deleted_overlays() {
        let mut template = empty_event("feed", "trip", "route", "a");
        template.service_date = NaiveDate::from_ymd_opt(2026, 11, 14);
        template.scheduled_departure = Some(90);
        let calls = modified_calls(
            &[call("outside", 1, 90)],
            &[modification(1, Some(1), &[("a", 30)], 0)],
        )
        .unwrap();
        let stops = HashSet::from(["a".into()]);
        let mut events = vec![];
        apply_modified_schedule(&mut events, &template, &calls, &stops);
        assert_eq!(nearby_event_time(&events[0], 100), Some(120));
        let deleted = update(AspenisedTripScheduleRelationship::Deleted);
        overlay_updates(&mut events, "feed", &stops, &[deleted]);
        assert_eq!(nearby_event_time(&events[0], 100), None);
        let mut added = update(AspenisedTripScheduleRelationship::Added);
        added.trip.trip_id = Some("new".into());
        added.stop_time_update = vec![stu("a", 1, 130)];
        overlay_updates(&mut events, "feed", &stops, &[added]);
        assert_eq!(nearby_event_time(&events[1], 100), Some(130));
        events[1].trip_cancelled = true;
        assert_eq!(nearby_event_time(&events[1], 100), Some(130));
        assert_eq!(nearby_event_time(&events[1], 131), None);
    }

    #[test]
    fn added_trip_keeps_all_loop_occurrences() {
        let mut u = update(AspenisedTripScheduleRelationship::Added);
        u.stop_time_update = vec![stu("a", 1, 100), stu("a", 3, 300)];
        let mut events = vec![];
        overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &[u]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].realtime_departure, Some(300));
        assert!(events.iter().all(|e| e.unscheduled_trip));
    }
    #[test]
    fn replacement_cancels_removed_static_calls() {
        let mut u = update(AspenisedTripScheduleRelationship::Replacement);
        u.stop_time_update = vec![stu("b", 2, 250)];
        let mut event = empty_event("feed", "trip", "route", "a");
        event.service_date = u.trip.start_date;
        event.stop_sequence = Some(1);
        let mut events = vec![event];
        overlay_updates(
            &mut events,
            "feed",
            &HashSet::from(["a".into(), "b".into()]),
            &[u],
        );
        assert_eq!(events.len(), 2);
        assert!(events[0].stop_cancelled && events[0].trip_modified);
        assert!(!events[1].stop_cancelled);
    }
    #[test]
    fn update_is_scoped_to_service_day_and_frequency_instance() {
        let mut u = update(AspenisedTripScheduleRelationship::Replacement);
        u.trip.start_time = Some("25:00:00".into());
        u.stop_time_update = vec![stu("a", 1, 200)];
        let mut event = empty_event("feed", "trip", "route", "a");
        event.service_date = u.trip.start_date;
        event.trip_start_time = Some(26 * 3600);
        let mut events = vec![event];
        overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &[u]);
        assert!(!events[0].stop_cancelled);
        assert_eq!(events[0].realtime_departure, None);
    }
    #[test]
    fn modified_schedule_renumbering_uses_original_snapshot() {
        let mut template = empty_event("feed", "trip", "route", "");
        template.service_date = NaiveDate::from_ymd_opt(2026, 11, 14);
        template.trip_start_time = Some(0);
        let mut first = template.clone();
        first.stop_id = "a".into();
        first.stop_sequence = Some(1);
        let mut second = template.clone();
        second.stop_id = "b".into();
        second.stop_sequence = Some(2);
        let mut events = vec![first, second];
        let calls = vec![
            ModifiedCall {
                stop_id: "x".into(),
                original_sequence: None,
                arrival: Some(50),
                departure: Some(50),
            },
            call("a", 1, 100),
            call("b", 2, 200),
        ];
        apply_modified_schedule(
            &mut events,
            &template,
            &calls,
            &HashSet::from(["a".into(), "b".into()]),
        );
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].scheduled_arrival, Some(100));
        assert_eq!(events[1].scheduled_arrival, Some(200));
        assert_eq!(events[0].stop_sequence, Some(2));
    }
    #[test]
    fn modified_trip_sequence_only_delay_uses_modified_schedule() {
        let mut u = update(AspenisedTripScheduleRelationship::Scheduled);
        u.trip.trip_id = None;
        u.trip.start_date = None;
        u.trip.modified_trip = Some(ModifiedTripSelector {
            modifications_id: Some("detour".into()),
            affected_trip_id: Some("trip".into()),
            start_time: None,
            start_date: NaiveDate::from_ymd_opt(2026, 11, 14),
        });
        let mut stop = stu("a", 2, 200);
        stop.stop_id = None;
        stop.departure = Some(AspenStopTimeEvent {
            time: None,
            delay: Some(30),
            uncertainty: None,
        });
        u.stop_time_update = vec![stop];
        let mut event = empty_event("feed", "trip", "route", "a");
        event.service_date = u.trip.effective_start_date();
        event.stop_sequence = Some(2);
        event.scheduled_departure = Some(150);
        let mut events = vec![event];
        overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &[u]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].realtime_departure, Some(180));
        assert!(events[0].trip_modified);
    }
    #[test]
    fn replacement_index_includes_trips_already_in_static_gtfs() {
        let replacement = update(AspenisedTripScheduleRelationship::Replacement);
        let ordinary = update(AspenisedTripScheduleRelationship::Scheduled);
        assert!(needs_departure_stop_index(&replacement, true));
        assert!(!needs_departure_stop_index(&ordinary, true));
        assert!(needs_departure_stop_index(&ordinary, false));
    }
    #[test]
    fn compact_and_expanded_departure_index_decisions_agree() {
        for (raw, relationship) in [
            (0, AspenisedTripScheduleRelationship::Scheduled),
            (1, AspenisedTripScheduleRelationship::Added),
            (2, AspenisedTripScheduleRelationship::Unscheduled),
            (3, AspenisedTripScheduleRelationship::Cancelled),
            (5, AspenisedTripScheduleRelationship::Replacement),
            (6, AspenisedTripScheduleRelationship::Duplicated),
            (7, AspenisedTripScheduleRelationship::Deleted),
        ] {
            let expanded = update(relationship);
            let compact = crate::compact_formats::CompactTripUpdate {
                trip: gtfs_realtime::TripDescriptor {
                    schedule_relationship: Some(raw),
                    ..Default::default()
                },
                vehicle: None,
                stop_time_update: vec![],
                timestamp: None,
                delay: None,
                trip_properties: None,
            };
            assert_eq!(
                needs_departure_stop_index_compact(&compact, true),
                needs_departure_stop_index(&expanded, true)
            );
            assert_eq!(
                needs_departure_stop_index_raw(Some(raw), false, true),
                needs_departure_stop_index(&expanded, true)
            );
            assert!(needs_departure_stop_index_raw(Some(raw), false, false));
            assert!(needs_departure_stop_index_raw(Some(raw), true, true));
        }
        assert!(!needs_departure_stop_index_raw(None, false, true));
        assert!(!needs_departure_stop_index_raw(Some(99), false, true));
    }
    #[test]
    fn invalid_and_negative_times_never_wrap() {
        assert_eq!(parse_start_time("25:00:00"), Some(90000));
        assert_eq!(parse_start_time("25:60:00"), None);
        assert_eq!(parse_start_time("999999999:00:00"), None);
        let mut u = update(AspenisedTripScheduleRelationship::Added);
        u.stop_time_update = vec![stu("a", 1, -1)];
        let mut events = vec![];
        overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &[u]);
        assert!(events.is_empty());
    }
}

/// Realtime trips must stay discoverable by stop even when their ID also exists
/// in static GTFS (REPLACEMENT and modified-trip updates commonly do).
pub fn needs_departure_stop_index(update: &AspenisedTripUpdate, found_static: bool) -> bool {
    needs_departure_stop_index_raw(
        update
            .trip
            .schedule_relationship
            .as_ref()
            .map(|relationship| {
                i32::from(crate::aspen_dataset::schedule_relationship_to_u8(
                    relationship,
                ))
            }),
        update.trip.modified_trip.is_some(),
        found_static,
    )
}

/// The fresh-import path receives compact updates, before Aspen expansion.
pub fn needs_departure_stop_index_compact(
    update: &crate::compact_formats::CompactTripUpdate,
    found_static: bool,
) -> bool {
    needs_departure_stop_index_raw(
        update.trip.schedule_relationship,
        update.trip.modified_trip.is_some(),
        found_static,
    )
}

/// Import starts with a compact GTFS-RT descriptor whose relationship is still
/// the protobuf integer. Decide whether to index it without expanding the trip.
pub fn needs_departure_stop_index_raw(
    schedule_relationship: Option<i32>,
    has_modified_trip: bool,
    found_static: bool,
) -> bool {
    !found_static || has_modified_trip || matches!(schedule_relationship, Some(1 | 2 | 5 | 6))
}

/// Select a departure-board subset directly from a compressed Aspen snapshot.
/// Only matching update IDs are expanded; feed-wide trip updates stay compressed.
pub fn select_departure_realtime(
    data: &crate::compressed_aspen_dataset::CompressedAspenisedData,
    trip_ids: Vec<String>,
    stop_ids: Vec<String>,
) -> DepartureRealtime {
    use ahash::AHashSet;
    let metadata = data.metadata();
    // Expand parent stations to static and realtime children using the
    // snapshot's index. No scan of all realtime Stop entities is required.
    let mut stop_ids: AHashSet<String> = stop_ids.into_iter().collect();
    let mut pending: Vec<String> = stop_ids.iter().cloned().collect();
    while let Some(parent) = pending.pop() {
        if let Some(children) = metadata.parent_id_to_children_ids.get(parent.as_str()) {
            for child in children {
                if stop_ids.insert(child.to_string()) {
                    pending.push(child.to_string());
                }
            }
        }
    }
    let mut modification_ids = AHashSet::new();
    for trip in &trip_ids {
        if let Some(ids) = metadata.trip_id_to_trip_modification_ids.get(trip.as_str()) {
            modification_ids.extend(ids.iter().cloned());
        }
    }
    for stop in &stop_ids {
        if let Some(ids) = metadata.stop_id_to_trip_modification_ids.get(stop.as_str()) {
            modification_ids.extend(ids.iter().cloned());
        }
    }
    let modifications: std::collections::BTreeMap<_, _> = modification_ids
        .iter()
        .filter_map(|id| {
            metadata
                .trip_modifications
                .get(id.as_str())
                .map(|value| (id.to_string(), value.clone()))
        })
        .collect();
    let mut all_trip_ids: AHashSet<String> = trip_ids.into_iter().collect();
    for modification in modifications.values() {
        for selected in &modification.selected_trips {
            all_trip_ids.extend(selected.trip_ids.iter().cloned());
        }
    }
    let mut update_ids = AHashSet::new();
    for trip in &all_trip_ids {
        if let Some(ids) = metadata
            .trip_updates_lookup_by_trip_id_to_trip_update_ids
            .get(trip.as_str())
        {
            update_ids.extend(ids.iter().map(|id| id.to_string()));
        }
    }
    for stop in &stop_ids {
        if let Some(ids) = metadata
            .stop_id_to_non_scheduled_trip_ids
            .get(stop.as_str())
        {
            update_ids.extend(ids.iter().map(|id| id.to_string()));
        }
    }
    let updates = update_ids
        .iter()
        .filter_map(|id| {
            data.trip_updates()
                .get(id.as_str())
                .map(|update| update.to_owned())
        })
        .collect();
    let realtime_stops = stop_ids
        .iter()
        .filter_map(|id| {
            metadata
                .stop_id_to_stop
                .get(id.as_str())
                .map(|stop| (id.clone(), stop.clone()))
        })
        .collect();
    DepartureRealtime {
        searched_stop_ids: stop_ids.into_iter().collect(),
        realtime_stops,
        modifications,
        updates,
    }
}
