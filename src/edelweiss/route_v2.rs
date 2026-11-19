//! OJP-style event and quay names on routed journeys only.
//!
//! This is an additive projection of the existing route shape, not an OJP XML
//! implementation or a second trip-detail lookup service. v1 remains unchanged.
//! A routing query pins one realtime revision through scan, PONG and rendering.
use catenary::routing_common::raptor_query::{Journey, Leg};
use catenary::routing_common::realtime_region::RegionRealtimeSnapshot;
use catenary::routing_common::realtime_status::RtEventKind;
use catenary::routing_common::route_manager::PlatformStopMeta;
use catenary::routing_common::timetable::{
    LocationIdx, PackedStop, RouteIdx, ZeroCopyTimetable,
};
use serde_json::{Value, json};

fn event(scheduled: Option<&str>, effective: Option<&str>, kind: Option<RtEventKind>) -> Value {
    let mut value = json!({});
    if let Some(time) = scheduled {
        value["TimetabledTime"] = json!(time);
    }
    // GTFS-RT times are predictions unless their source explicitly attests
    // that the event was recorded. Do not infer "actual" from elapsed time.
    match kind {
        Some(RtEventKind::Estimated) => {
            if let Some(time) = effective {
                value["EstimatedTime"] = json!(time);
            }
        }
        Some(RtEventKind::Recorded) => {
            if let Some(time) = effective {
                value["RecordedAtTime"] = json!(time);
            }
        }
        _ => {}
    }
    value
}

fn platform(platforms: &[PlatformStopMeta], stop: Option<PackedStop>) -> Option<&str> {
    platforms.get(stop?.location().idx()).map(|meta| meta.platform_code.trim())
        .filter(|code| !code.is_empty())
}

fn platform_at(platforms: &[PlatformStopMeta], location: Option<LocationIdx>) -> Option<&str> {
    platforms.get(location?.idx()).map(|meta| meta.platform_code.trim())
        .filter(|code| !code.is_empty())
}

fn quays(
    call: &mut Value,
    planned: Option<&str>,
    estimated: Option<&str>,
) {
    if let Some(code) = planned {
        call["PlannedQuay"] = json!(code);
    }
    if let Some(code) = estimated.filter(|code| Some(*code) != planned) {
        call["EstimatedQuay"] = json!(code);
    }
}

fn emit_call(
    call: &mut Value,
    arrival: Option<Value>,
    departure: Option<Value>,
    planned_quay: Option<&str>,
    estimated_quay: Option<&str>,
    skipped: bool,
    boarding: bool,
    alighting: bool,
) {
    if let Some(arrival) = arrival {
        call["ServiceArrival"] = arrival;
    }
    if let Some(departure) = departure {
        call["ServiceDeparture"] = departure;
    }
    quays(call, planned_quay, estimated_quay);
    if skipped {
        call["NotServicedStop"] = json!(true);
    }
    if !boarding {
        call["NoBoardingAtStop"] = json!(true);
    }
    if !alighting {
        call["NoAlightingAtStop"] = json!(true);
    }
}

/// Enrich only the journey objects that have just been rendered by MOTIS.
/// The outer itinerary/page/WS envelope and its stable IDs remain compatible
/// with the existing client parser, but the v2 event/quay fields have OJP names.
pub fn enrich(
    response: &mut Value,
    journeys: &[Journey],
    tt: &ZeroCopyTimetable<'_>,
    platforms: &[PlatformStopMeta],
    realtime: Option<&RegionRealtimeSnapshot>,
) {
    for (is_direct, field) in [(false, "itineraries"), (true, "direct")] {
        let Some(items) = response[field].as_array_mut() else { continue };
        for (journey, item) in journeys.iter()
            .filter(|journey| (journey.boardings == 0) == is_direct).zip(items.iter_mut())
        {
            let Some(legs) = item["legs"].as_array_mut() else { continue };
            for (journey_leg, leg) in journey.legs.iter().zip(legs.iter_mut()) {
                let Leg::Ride {
                    route, board_stop_index, alight_stop_index, realtime: rt, ..
                } = journey_leg else { continue };
                let pinned = rt.and_then(|handle| realtime.filter(|snap|
                    snap.publication_seq == handle.publication_seq)
                    .map(|snap| (snap, handle)));
                let effective = if let Some((snap, handle)) = pinned {
                    snap.stops(tt, handle.pattern)
                } else {
                    Some(tt.stops_on(RouteIdx(*route)))
                };
                let original = if let Some((snap, handle)) = pinned {
                    snap.run(handle.run).and_then(|run| run.original).and_then(|original| {
                        tt.transport_route().get(original.transport as usize)
                            .map(|route| tt.stops_on(*route))
                    })
                } else {
                    effective
                };
                // Ordinal event matching is valid only for a stop-preserving
                // pattern. Inserted/deleted calls need an explicit occurrence
                // mapping; do not attach an unrelated TimetabledTime or quay.
                let aligned = original.zip(effective)
                    .filter(|(scheduled, actual)| scheduled.len() == actual.len());
                let board = *board_stop_index;
                let alight = *alight_stop_index;
                let metadata = pinned.and_then(|(snap, handle)| snap.run_metadata(handle.run));
                // The v1 renderer supplies the original GTFS event time.
                // These copies must be made before mutating the JSON object.
                let departure_scheduled = leg["scheduledStartTime"].as_str().map(str::to_owned);
                let arrival_scheduled = leg["scheduledEndTime"].as_str().map(str::to_owned);
                let departure_current = leg["startTime"].as_str().map(str::to_owned);
                let arrival_current = leg["endTime"].as_str().map(str::to_owned);
                let kind = |pos: usize, arrival: bool| {
                    let (snap, handle) = pinned?;
                    if arrival && pos == 0 { return None }
                    let slot = if arrival { pos.checked_mul(2)?.checked_sub(1)? } else { pos.checked_mul(2)? };
                    snap.run_event_kind(handle.run, slot)
                };
                let at = |pos: usize| {
                    let changed = metadata.and_then(|m| m.stop_change(pos));
                    let planned = aligned.and_then(|(stops, _)| stops.get(pos).copied());
                    let current = effective.and_then(|stops| stops.get(pos).copied());
                    let original_location = changed.and_then(|change| change.original_location)
                        .map(LocationIdx);
                    let planned_quay = platform(platforms, planned)
                        .or_else(|| platform_at(platforms, original_location));
                    let current_quay = platform(platforms, current);
                    let estimated_quay = if pinned.is_some()
                        && (planned.is_none() || planned.map(|s| s.location()) != current.map(|s| s.location())) {
                        current_quay
                    } else { None };
                    (planned_quay.map(str::to_owned), estimated_quay.map(str::to_owned),
                        changed.is_some_and(|c| c.skipped),
                        current.is_some_and(|stop| stop.outbound()),
                        current.is_some_and(|stop| stop.inbound()))
                };
                let dep_scheduled = departure_scheduled.filter(|_| aligned.is_some());
                let arr_scheduled = arrival_scheduled.filter(|_| aligned.is_some());
                let service_departure = event(dep_scheduled.as_deref(),
                    departure_current.as_deref(), kind(board, false));
                let service_arrival = event(arr_scheduled.as_deref(),
                    arrival_current.as_deref(), kind(alight, true));
                leg["ServiceDeparture"] = service_departure.clone();
                leg["ServiceArrival"] = service_arrival.clone();
                let (planned, estimated, skipped, boarding, alighting) = at(board);
                emit_call(&mut leg["from"], None, Some(service_departure),
                    planned.as_deref(), estimated.as_deref(), skipped, boarding, alighting);
                let (planned, estimated, skipped, boarding, alighting) = at(alight);
                emit_call(&mut leg["to"], Some(service_arrival), None,
                    planned.as_deref(), estimated.as_deref(), skipped, boarding, alighting);
                if let Some(stops) = leg["intermediateStops"].as_array_mut() {
                    for (offset, stop) in stops.iter_mut().enumerate() {
                        let pos = board + 1 + offset;
                        let arr_scheduled = stop["scheduledArrival"].as_str().map(str::to_owned);
                        let dep_scheduled = stop["scheduledDeparture"].as_str().map(str::to_owned);
                        let arr_current = stop["arrival"].as_str().map(str::to_owned);
                        let dep_current = stop["departure"].as_str().map(str::to_owned);
                        let verified = aligned.is_some();
                        let arrival = (pos > 0).then(|| event(
                            arr_scheduled.as_deref().filter(|_| verified),
                            arr_current.as_deref(), kind(pos, true)));
                        let departure = effective.filter(|stops| pos + 1 < stops.len())
                            .map(|_| event(dep_scheduled.as_deref().filter(|_| verified),
                                dep_current.as_deref(), kind(pos, false)));
                        let (planned, estimated, skipped, boarding, alighting) = at(pos);
                        emit_call(stop, arrival, departure, planned.as_deref(),
                            estimated.as_deref(), skipped, boarding, alighting);
                    }
                }
                // v2 event fields supersede ambiguous v1 status/deviation keys;
                // keep effective startTime/endTime for the journey timeline.
                if let Some(obj) = leg.as_object_mut() {
                    for key in ["scheduledStartTime", "scheduledEndTime",
                        "departureStatus", "arrivalStatus"] {
                        obj.remove(key);
                    }
                }
            }
        }
    }
    response["schemaVersion"] = json!(2);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn respects_prediction_vs_recording_provenance() {
        let time = "2026-11-19T12:45:00Z";
        assert_eq!(event(Some(time), Some(time), None), json!({"TimetabledTime":time}));
        assert_eq!(event(Some(time), Some(time), Some(RtEventKind::Estimated)),
            json!({"TimetabledTime":time,"EstimatedTime":time}));
        assert_eq!(event(None, Some(time), Some(RtEventKind::Recorded)),
            json!({"RecordedAtTime":time}));
        assert_eq!(event(None, Some(time), Some(RtEventKind::NoData)), json!({}));
    }
    #[test]
    fn call_status_does_not_mark_an_entire_trip_cancelled() {
        let mut call = json!({"name":"Union Station"});
        emit_call(&mut call,
            Some(json!({"TimetabledTime":"2026-11-19T11:00:00Z"})),
            Some(json!({"EstimatedTime":"2026-11-19T11:01:00Z"})),
            Some("4"), Some("5"), true, false, true);
        assert_eq!(call["ServiceArrival"]["TimetabledTime"], "2026-11-19T11:00:00Z");
        assert_eq!(call["ServiceDeparture"]["EstimatedTime"], "2026-11-19T11:01:00Z");
        assert_eq!(call["PlannedQuay"], "4");
        assert_eq!(call["EstimatedQuay"], "5");
        assert_eq!(call["NotServicedStop"], true);
        assert_eq!(call["NoBoardingAtStop"], true);
        assert!(call.get("Cancelled").is_none());
    }

    #[test]
    fn keeps_planned_and_estimated_quays_distinct() {
        let mut call = json!({});
        quays(&mut call, Some("1"), Some("2"));
        assert_eq!(call, json!({"PlannedQuay":"1","EstimatedQuay":"2"}));
        let mut unchanged = json!({});
        quays(&mut unchanged, Some("1"), Some("1"));
        assert_eq!(unchanged, json!({"PlannedQuay":"1"}));
    }
}
