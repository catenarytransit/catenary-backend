//! Shared realtime overlays for GTFS-stop and OSM-station departure boards.
use crate::aspen_dataset::*;
use crate::departures_shared::MovedStopData;
use chrono::NaiveDate;
use compact_str::CompactString;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

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
/// A feed-supplied headsign takes priority over any inferred destination.
/// Blank strings are missing values, rather than useful display names.
pub fn realtime_trip_headsign(update: &AspenisedTripUpdate) -> Option<&str> {
    update
        .trip_properties
        .as_ref()
        .and_then(|p| p.trip_headsign.as_deref())
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            update
                .trip_headsign
                .as_deref()
                .filter(|name| !name.trim().is_empty())
        })
}

fn terminal_stop_id<'a>(
    calls: impl Iterator<Item = (Option<u16>, bool, Option<&'a str>)>,
) -> Option<&'a str> {
    let mut last = None;
    let mut highest: Option<(u16, Option<&str>)> = None;
    let mut complete_sequences = true;
    for (sequence, skipped, id) in calls {
        if skipped {
            continue;
        }
        last = Some(id);
        if let Some(sequence) = sequence {
            if highest.is_none_or(|(previous, _)| sequence >= previous) {
                highest = Some((sequence, id));
            }
        } else {
            complete_sequences = false;
        }
    }
    // Use feed order if sequences are incomplete, otherwise the highest
    // sequence (including loops and out-of-order arrays). Never use a preceding
    // stop merely because the actual terminal has no ID or name.
    let terminal = if complete_sequences {
        highest.and_then(|(_, id)| id)
    } else {
        last.flatten()
    };
    terminal.filter(|id| !id.trim().is_empty())
}

pub fn headsign_terminal_stop_id(update: &AspenisedTripUpdate) -> Option<&str> {
    if matches!(
        update.trip.schedule_relationship,
        Some(
            AspenisedTripScheduleRelationship::Cancelled
                | AspenisedTripScheduleRelationship::Deleted
        )
    ) {
        return None;
    }
    terminal_stop_id(update.stop_time_update.iter().map(|stop| {
        (
            stop.stop_sequence,
            stop.schedule_relationship == Some(AspenisedStopTimeScheduleRelationship::Skipped),
            stop.stop_time_properties
                .as_ref()
                .and_then(|p| p.assigned_stop_id.as_deref())
                .filter(|id| !id.trim().is_empty())
                .or(stop.stop_id.as_deref()),
        )
    }))
}

/// Use exactly the same terminal selection during compact-feed lookup planning.
pub fn compact_headsign_terminal_stop_id(
    update: &crate::compact_formats::CompactTripUpdate,
) -> Option<&str> {
    terminal_stop_id(update.stop_time_update.iter().map(|stop| {
        (
            stop.stop_sequence,
            stop.schedule_relationship == Some(1),
            stop.stop_time_properties
                .as_ref()
                .and_then(|p| p.assigned_stop_id.as_deref())
                .filter(|id| !id.trim().is_empty())
                .or(stop.stop_id.as_deref()),
        )
    }))
}

pub fn needs_headsign_fallback(update: &AspenisedTripUpdate) -> bool {
    realtime_trip_headsign(update).is_none()
        && (!update.found_schedule_trip_id
            || matches!(
                update.trip.schedule_relationship,
                Some(
                    AspenisedTripScheduleRelationship::Added
                        | AspenisedTripScheduleRelationship::Unscheduled
                        | AspenisedTripScheduleRelationship::Duplicated
                        | AspenisedTripScheduleRelationship::Replacement
                )
            ))
        && headsign_terminal_stop_id(update).is_some()
}

/// Fill Aspen's existing field once from the full trip, before selecting board
/// calls. Every departure then has the same destination, even at an intermediate stop.
pub fn fill_unscheduled_headsign(
    update: &mut AspenisedTripUpdate,
    mut stop_name: impl FnMut(&str) -> Option<String>,
) {
    if !needs_headsign_fallback(update) {
        return;
    }
    if let Some(name) = headsign_terminal_stop_id(update)
        .and_then(&mut stop_name)
        .filter(|name| !name.trim().is_empty())
    {
        update.trip_headsign = Some(std::sync::Arc::from(name));
    }
}

pub fn stop_display_name(stop: &AspenisedStop) -> Option<String> {
    let translations = &stop.stop_name.as_ref()?.translation;
    translations
        .iter()
        .find(|t| t.language.is_none() && !t.text.trim().is_empty())
        .or_else(|| translations.iter().find(|t| !t.text.trim().is_empty()))
        .map(|t| t.text.clone())
}

fn scheduled_time(event: &AspenStopTimeEvent) -> Option<u64> {
    positive_time(
        event
            .time
            .zip(event.delay)
            .and_then(|(time, delay)| time.checked_sub(i64::from(delay))),
    )
}

// Return a match only if it is unique; stop after the second candidate.
fn unique_match<T>(mut candidates: impl Iterator<Item = T>) -> Option<T> {
    let first = candidates.next()?;
    candidates.next().is_none().then_some(first)
}

pub fn overlay_updates(
    events: &mut Vec<StopEvent>,
    chateau: &str,
    stops: &HashSet<String>,
    updates: &[AspenisedTripUpdate],
) {
    if updates.is_empty() {
        return;
    }

    // The vector is append-only during this overlay. Existing events keep
    // their chateau/trip identity, so indices remain valid even when a call
    // changes sequence, platform or instance metadata.
    let mut events_by_trip: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        if event.chateau == chateau {
            events_by_trip
                .entry(event.trip_id.clone())
                .or_default()
                .push(index);
        }
    }

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
        let realtime_only = matches!(
            relationship,
            Some(
                AspenisedTripScheduleRelationship::Added
                    | AspenisedTripScheduleRelationship::Unscheduled
                    | AspenisedTripScheduleRelationship::Duplicated
                    | AspenisedTripScheduleRelationship::Replacement
            )
        ) || modified.is_some();
        // Ordinary updates for trips absent from this board cannot create calls.
        if !realtime_only && !events_by_trip.contains_key(trip_id) {
            continue;
        }
        let trip_events = events_by_trip.entry(trip_id.to_owned()).or_default();

        let start_time = update
            .trip
            .effective_start_time()
            .and_then(parse_start_time);
        // Undated updates can only match by absolute scheduled timestamps.
        // Calculate these once instead of scanning the stop updates for every
        // candidate event, keeping arrival and departure identities separate.
        let mut undated_arrivals = HashSet::new();
        let mut undated_departures = HashSet::new();
        if date.is_none() {
            for stu in &update.stop_time_update {
                if let Some(time) = stu.arrival.as_ref().and_then(scheduled_time) {
                    undated_arrivals.insert(time);
                }
                if let Some(time) = stu.departure.as_ref().and_then(scheduled_time) {
                    undated_departures.insert(time);
                }
            }
        }
        let instance_matches = |event: &StopEvent| {
            (!event.trip_modified
                || modified.is_some()
                || is_replacement
                || matches!(
                    relationship,
                    Some(
                        AspenisedTripScheduleRelationship::Deleted
                            | AspenisedTripScheduleRelationship::Cancelled
                    )
                ))
                && start_time.is_none_or(|start| event.trip_start_time == Some(start))
                && match date {
                    Some(date) => event.service_date == Some(date),
                    None => {
                        event
                            .scheduled_arrival
                            .is_some_and(|time| undated_arrivals.contains(&time))
                            || event
                                .scheduled_departure
                                .is_some_and(|time| undated_departures.contains(&time))
                    }
                }
        };
        if matches!(
            relationship,
            Some(
                AspenisedTripScheduleRelationship::Deleted
                    | AspenisedTripScheduleRelationship::Cancelled
            )
        ) {
            for &i in trip_events.iter() {
                let event = &mut events[i];
                if instance_matches(event) {
                    event.trip_deleted =
                        relationship == Some(&AspenisedTripScheduleRelationship::Deleted);
                    event.trip_cancelled =
                        relationship == Some(&AspenisedTripScheduleRelationship::Cancelled);
                }
            }
        }
        if is_replacement && date.is_some() {
            for &i in trip_events.iter() {
                let event = &mut events[i];
                if instance_matches(event) {
                    event.trip_modified = true;
                    event.stop_cancelled = true;
                }
            }
        }
        for (index, stu) in update.stop_time_update.iter().enumerate() {
            let stop_id = stu.stop_id.as_deref().map(str::to_owned).or_else(|| {
                let sequence = u32::from(stu.stop_sequence?);
                unique_match(trip_events.iter().map(|&i| &events[i]).filter(|event| {
                    instance_matches(event) && event.stop_sequence == Some(sequence)
                }))
                .map(|event| event.stop_id.clone())
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
            let scheduled_arrival = stu.arrival.as_ref().and_then(scheduled_time);
            let scheduled_departure = stu.departure.as_ref().and_then(scheduled_time);
            let existing = unique_match(trip_events.iter().copied().filter(|&i| {
                let event = &events[i];
                instance_matches(event)
                    && event.stop_id == assigned
                    && (stu.stop_sequence.is_none()
                        || event.stop_sequence == stu.stop_sequence.map(u32::from))
                    && (scheduled_arrival.is_none() || event.scheduled_arrival == scheduled_arrival)
                    && (scheduled_departure.is_none()
                        || event.scheduled_departure == scheduled_departure)
            }));
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
            event.trip_start_time = start_time.or(event.trip_start_time);
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
            event.headsign = realtime_trip_headsign(update)
                .map(str::to_owned)
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
                trip_events.push(events.len());
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
    fn unscheduled_headsign_uses_last_reachable_stop_even_for_static_trip_ids() {
        for relationship in [
            AspenisedTripScheduleRelationship::Added,
            AspenisedTripScheduleRelationship::Unscheduled,
            AspenisedTripScheduleRelationship::Duplicated,
            AspenisedTripScheduleRelationship::Replacement,
        ] {
            let mut u = update(relationship);
            u.found_schedule_trip_id = true;
            let mut skipped = stu("closed-terminal", 4, 400);
            skipped.schedule_relationship = Some(AspenisedStopTimeScheduleRelationship::Skipped);
            u.stop_time_update = vec![stu("origin", 1, 100), stu("reachable", 3, 300), skipped];
            fill_unscheduled_headsign(&mut u, |id| Some(format!("Name {id}")));
            assert_eq!(u.trip_headsign.as_deref(), Some("Name reachable"));
            let mut events = vec![];
            overlay_updates(&mut events, "feed", &HashSet::from(["origin".into()]), &[u]);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].headsign.as_deref(), Some("Name reachable"));
        }
    }

    #[test]
    fn explicit_headsign_wins_and_blank_properties_do_not_mask_fallback() {
        let mut u = update(AspenisedTripScheduleRelationship::Added);
        u.stop_time_update = vec![stu("terminal", 1, 100)];
        u.trip_properties = Some(
            serde_json::from_value(serde_json::json!({"trip_headsign":"Express destination"}))
                .unwrap(),
        );
        fill_unscheduled_headsign(&mut u, |_| panic!("explicit headsign must not be replaced"));
        assert_eq!(realtime_trip_headsign(&u), Some("Express destination"));
        u.trip_properties.as_mut().unwrap().trip_headsign = Some("  ".into());
        u.trip_headsign = Some(" ".into());
        fill_unscheduled_headsign(&mut u, |_| Some("Terminal Station".into()));
        let mut events = vec![];
        overlay_updates(
            &mut events,
            "feed",
            &HashSet::from(["terminal".into()]),
            &[u.clone()],
        );
        assert_eq!(events[0].headsign.as_deref(), Some("Terminal Station"));
        u.trip_properties = None;
        u.trip_headsign = Some("Feed destination".into());
        fill_unscheduled_headsign(&mut u, |_| panic!("existing headsign must not be replaced"));
        assert_eq!(realtime_trip_headsign(&u), Some("Feed destination"));
    }

    #[test]
    fn terminal_uses_sequence_and_assigned_stop_and_preserves_loop_return() {
        let mut u = update(AspenisedTripScheduleRelationship::Unscheduled);
        let mut terminal = stu("old-platform", 5, 500);
        terminal.stop_time_properties = Some(
            serde_json::from_value(serde_json::json!({"assigned_stop_id":"replacement-platform"}))
                .unwrap(),
        );
        u.stop_time_update = vec![terminal, stu("origin", 1, 100), stu("middle", 3, 300)];
        assert_eq!(headsign_terminal_stop_id(&u), Some("replacement-platform"));
        u.stop_time_update[0] = stu("origin", 5, 500);
        assert_eq!(headsign_terminal_stop_id(&u), Some("origin"));
        // With partial sequences use the feed's call order instead of inventing an order.
        u.stop_time_update[2].stop_sequence = None;
        assert_eq!(headsign_terminal_stop_id(&u), Some("middle"));
    }

    #[test]
    fn unreachable_or_unresolvable_terminal_does_not_invent_destination() {
        let mut u = update(AspenisedTripScheduleRelationship::Added);
        u.stop_time_update = vec![stu("origin", 1, 100), stu("unknown", 2, 200)];
        fill_unscheduled_headsign(&mut u, |id| (id == "origin").then(|| "Origin".into()));
        assert!(u.trip_headsign.is_none());
        u.stop_time_update[1].stop_id = None;
        assert!(headsign_terminal_stop_id(&u).is_none());
        for stop in &mut u.stop_time_update {
            stop.schedule_relationship = Some(AspenisedStopTimeScheduleRelationship::Skipped);
        }
        assert!(headsign_terminal_stop_id(&u).is_none());
        for relationship in [
            AspenisedTripScheduleRelationship::Cancelled,
            AspenisedTripScheduleRelationship::Deleted,
        ] {
            u.trip.schedule_relationship = Some(relationship);
            assert!(!needs_headsign_fallback(&u));
        }
    }

    #[test]
    fn no_data_terminal_is_reachable_and_scheduled_headsigns_are_unchanged() {
        let mut u = update(AspenisedTripScheduleRelationship::Scheduled);
        u.found_schedule_trip_id = true;
        let mut terminal = stu("terminal", 2, 200);
        terminal.schedule_relationship = Some(AspenisedStopTimeScheduleRelationship::NoData);
        terminal.arrival = None;
        terminal.departure = None;
        u.stop_time_update = vec![stu("origin", 1, 100), terminal];
        fill_unscheduled_headsign(&mut u, |_| {
            panic!("ordinary scheduled trip must keep its static headsign")
        });
        assert!(u.trip_headsign.is_none());
        u.trip.modified_trip = Some(ModifiedTripSelector {
            modifications_id: Some("detour".into()),
            affected_trip_id: Some("trip".into()),
            start_time: None,
            start_date: u.trip.start_date,
        });
        fill_unscheduled_headsign(&mut u, |_| {
            panic!(
                "partial scheduled detour updates must not replace the full schedule destination"
            )
        });
        u.found_schedule_trip_id = false;
        fill_unscheduled_headsign(&mut u, |id| Some(format!("Name {id}")));
        assert_eq!(u.trip_headsign.as_deref(), Some("Name terminal"));
    }

    #[test]
    fn compact_and_expanded_headsign_terminal_selection_agree() {
        let mut u = update(AspenisedTripScheduleRelationship::Added);
        u.stop_time_update = vec![
            stu("origin", 1, 100),
            stu("old-terminal", 3, 300),
            stu("reachable", 2, 200),
        ];
        u.stop_time_update[1].schedule_relationship =
            Some(AspenisedStopTimeScheduleRelationship::Skipped);
        u.stop_time_update[2].stop_time_properties = Some(
            serde_json::from_value(serde_json::json!({"assigned_stop_id":"assigned"})).unwrap(),
        );
        let compact = crate::compact_formats::CompactTripUpdate {
            trip: gtfs_realtime::TripDescriptor::default(),
            vehicle: None,
            timestamp: None,
            delay: None,
            trip_properties: None,
            stop_time_update: u
                .stop_time_update
                .iter()
                .map(|stop| crate::compact_formats::CompactStopTimeUpdate {
                    stop_sequence: stop.stop_sequence,
                    stop_id: stop.stop_id.clone(),
                    arrival: None,
                    departure: None,
                    departure_occupancy_status: None,
                    schedule_relationship: stop.schedule_relationship.as_ref().map(|r| match r {
                        AspenisedStopTimeScheduleRelationship::Skipped => 1,
                        _ => 0,
                    }),
                    stop_time_properties: stop.stop_time_properties.as_ref().map(|p| {
                        Box::new(
                            gtfs_realtime::trip_update::stop_time_update::StopTimeProperties {
                                assigned_stop_id: p
                                    .assigned_stop_id
                                    .as_ref()
                                    .map(ToString::to_string),
                                ..Default::default()
                            },
                        )
                    }),
                })
                .collect(),
        };
        assert_eq!(
            compact_headsign_terminal_stop_id(&compact),
            headsign_terminal_stop_id(&u)
        );
        assert_eq!(headsign_terminal_stop_id(&u), Some("assigned"));
    }

    #[test]
    fn nearby_departures_require_trip_updates_for_cancellation() {
        // Route 93 has a detour advisory at specific stops. Without an
        // authoritative trip update, scheduled calls remain departures.
        let date = NaiveDate::from_ymd_opt(2026, 11, 14);
        let stops = HashSet::from([
            "17123".to_owned(),
            "20177".to_owned(),
            "unaffected".to_owned(),
        ]);
        let mut events: Vec<_> = ["17123", "20177", "unaffected"]
            .into_iter()
            .enumerate()
            .map(|(index, stop)| {
                let mut event = empty_event("feed", "trip", "93", stop);
                event.service_date = date;
                event.stop_sequence = Some(index as u32 + 1);
                event.scheduled_departure = Some(100 + index as u64 * 100);
                event
            })
            .collect();

        overlay_updates(&mut events, "feed", &stops, &[]);
        assert!(events.iter().all(|event| {
            !event.trip_cancelled && !event.stop_cancelled && nearby_event_time(event, 0).is_some()
        }));

        // A SKIPPED stop-time update affects only its matched stop.
        let mut scheduled = update(AspenisedTripScheduleRelationship::Scheduled);
        scheduled.trip.route_id = Some("93".into());
        let mut skipped = stu("17123", 1, 100);
        skipped.schedule_relationship = Some(AspenisedStopTimeScheduleRelationship::Skipped);
        scheduled.stop_time_update = vec![skipped];
        overlay_updates(&mut events, "feed", &stops, &[scheduled]);
        assert!(events[0].stop_cancelled);
        assert!(events.iter().all(|event| !event.trip_cancelled));
        assert!(events[1..].iter().all(|event| !event.stop_cancelled));

        // A matching CANCELED trip update still cancels all of its calls.
        let mut cancelled = update(AspenisedTripScheduleRelationship::Cancelled);
        cancelled.trip.route_id = Some("93".into());
        overlay_updates(&mut events, "feed", &stops, &[cancelled]);
        assert!(events.iter().all(|event| event.trip_cancelled));
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
    fn appended_calls_are_indexed_for_later_updates_and_cancellation() {
        let mut added = update(AspenisedTripScheduleRelationship::Added);
        added.stop_time_update = vec![stu("a", 1, 100), stu("a", 3, 300)];
        let mut scheduled = update(AspenisedTripScheduleRelationship::Scheduled);
        let mut sequence_only = stu("a", 3, 350);
        sequence_only.stop_id = None;
        scheduled.stop_time_update = vec![sequence_only];
        let cancelled = update(AspenisedTripScheduleRelationship::Cancelled);

        let mut other_chateau = empty_event("other", "trip", "route", "a");
        other_chateau.service_date = added.trip.start_date;
        other_chateau.stop_sequence = Some(3);
        let mut events = vec![other_chateau];
        overlay_updates(
            &mut events,
            "feed",
            &HashSet::from(["a".into()]),
            &[added, scheduled, cancelled],
        );
        assert_eq!(events.len(), 3);
        assert!(!events[0].trip_cancelled);
        assert_eq!(events[0].realtime_departure, None);
        assert_eq!(events[1].realtime_departure, Some(100));
        assert_eq!(events[2].realtime_departure, Some(350));
        assert!(events[1..].iter().all(|event| event.trip_cancelled));
    }

    #[test]
    fn ambiguous_sequence_only_update_does_not_select_a_loop_call() {
        let mut u = update(AspenisedTripScheduleRelationship::Scheduled);
        let mut sequence_only = stu("a", 1, 200);
        sequence_only.stop_id = None;
        u.stop_time_update = vec![sequence_only];
        let mut event = empty_event("feed", "trip", "route", "a");
        event.service_date = u.trip.start_date;
        event.stop_sequence = Some(1);
        let mut other = event.clone();
        other.stop_id = "b".into();
        let mut events = vec![event, other];
        overlay_updates(
            &mut events,
            "feed",
            &HashSet::from(["a".into(), "b".into()]),
            &[u],
        );
        assert!(
            events
                .iter()
                .all(|event| event.realtime_departure.is_none())
        );
    }

    #[test]
    fn undated_updates_match_absolute_scheduled_times_only() {
        let mut u = update(AspenisedTripScheduleRelationship::Scheduled);
        u.trip.start_date = None;
        let mut stop = stu("a", 1, 200);
        stop.departure = Some(AspenStopTimeEvent {
            time: Some(200),
            delay: Some(20),
            uncertainty: None,
        });
        u.stop_time_update = vec![stop];

        let mut event = empty_event("feed", "trip", "route", "a");
        event.service_date = NaiveDate::from_ymd_opt(2026, 11, 14);
        event.stop_sequence = Some(1);
        event.scheduled_departure = Some(180);
        let mut other_day = event.clone();
        other_day.service_date = NaiveDate::from_ymd_opt(2026, 11, 15);
        other_day.scheduled_departure = Some(180 + 86400);
        let mut wrong_time_kind = event.clone();
        wrong_time_kind.scheduled_departure = None;
        wrong_time_kind.scheduled_arrival = Some(180);
        let mut events = vec![event, other_day, wrong_time_kind];

        overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &[u]);
        assert_eq!(events[0].realtime_departure, Some(200));
        assert!(
            events[1..]
                .iter()
                .all(|event| event.realtime_departure.is_none())
        );
    }

    #[test]
    fn large_board_updates_only_matching_trips() {
        let date = NaiveDate::from_ymd_opt(2026, 11, 14);
        let mut events: Vec<_> = (0..16_384)
            .map(|i| {
                let mut event = empty_event("feed", &format!("trip-{i}"), "route", "a");
                event.service_date = date;
                event.stop_sequence = Some(1);
                event
            })
            .collect();
        let updates: Vec<_> = (0..512)
            .map(|i| {
                let mut u = update(AspenisedTripScheduleRelationship::Scheduled);
                u.trip.trip_id = Some(format!("trip-{}", i * 16).into());
                u.stop_time_update = vec![stu("a", 1, 200)];
                u
            })
            .collect();
        overlay_updates(&mut events, "feed", &HashSet::from(["a".into()]), &updates);
        assert_eq!(events.len(), 16_384);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(
                event.realtime_departure,
                (i < 512 * 16 && i % 16 == 0).then_some(200)
            );
        }
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
            data.trip_updates().get(id.as_str()).map(|update| {
                let mut update = update.to_owned();
                fill_unscheduled_headsign(&mut update, |id| {
                    let stop = metadata.stop_id_to_stop.get(id)?;
                    stop_display_name(stop).or_else(|| {
                        stop.parent_station
                            .as_deref()
                            .and_then(|parent| metadata.stop_id_to_stop.get(parent))
                            .and_then(stop_display_name)
                    })
                });
                update
            })
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
