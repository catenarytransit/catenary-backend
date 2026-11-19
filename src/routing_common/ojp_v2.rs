//! OJP-compatible v2 trip-detail view over immutable Gentian + RT snapshots.
//!
//! OJP 2 semantics: TimetabledTime remains the published schedule,
//! EstimatedTime is a *prediction*, RecordedAtTime is only supplied with
//! verified observations, PlannedQuay is not replaced by EstimatedQuay,
//! NotServicedStop applies only to a particular skipped call, and Cancelled
//! applies to the entire journey. Detours additionally set Deviation.
//!
//! No expanded static stop-time array is held in RAM. The response is built
//! on demand from the original Gentian timing profile and a pinned RT run.

use crate::routing_common::realtime_identity::{
    GtfsIdentityCatalog, ResolvedInstance,
};
use crate::routing_common::realtime_region::{
    RegionRealtimeSnapshot, RegionRtRun, RegionRtPattern,
};
use crate::routing_common::realtime_status::{
    RtEventKind, StaticRunStatus, scheduled_event_unix,
};
use crate::routing_common::route_manager::PlatformStopMeta;
use crate::routing_common::timetable::{
    DayIdx, LocationIdx, PackedStop, SourceIdx, TransportIdx, ZeroCopyTimetable,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct OjpEventTime {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timetabled_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_at_time: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct OjpCall {
    pub order: u32,
    pub stop_point_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_stop_point_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtfs_stop_sequence: Option<u32>,
    pub unplanned_stop: bool,
    pub not_serviced_stop: bool,
    pub no_boarding_at_stop: bool,
    pub no_alighting_at_stop: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub planned_quay: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_quay: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_arrival: Option<OjpEventTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_departure: Option<OjpEventTime>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct OjpRealtimeContext {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct OjpTripDetail {
    pub trip_ref: String,
    pub chateau: String,
    pub feed_id: String,
    pub service_date: NaiveDate,
    pub scheduled: bool,
    pub real_time: bool,
    pub unplanned: bool,
    pub cancelled: bool,
    pub deviation: bool,
    pub realtime_context: OjpRealtimeContext,
    pub calls: Vec<OjpCall>,
}

fn instant(seconds: i64) -> Result<String> {
    Ok(DateTime::<Utc>::from_timestamp(seconds, 0)
        .context("time outside RFC3339 range")?
        .to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn name(tt: &ZeroCopyTimetable<'_>, location: LocationIdx) -> Result<String> {
    let id = *tt.location_id().get(location.idx())
        .context("invalid GTFS stop location")?;
    tt.get_string(id).map(str::to_owned).context("invalid GTFS stop name")
}

fn quay(platforms: &[PlatformStopMeta], location: LocationIdx) -> Option<String> {
    platforms.get(location.idx())
        .map(|v| v.platform_code.trim())
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

fn event(
    tt: &ZeroCopyTimetable<'_>,
    instance: Option<(TransportIdx, DayIdx)>,
    rt: Option<(&RegionRealtimeSnapshot, RegionRtRun)>,
    slot: usize,
) -> Result<OjpEventTime> {
    let timetabled = instance
        .map(|(transport, day)| {
            scheduled_event_unix(tt, transport, day, slot)
                .context("missing original scheduled event")
                .and_then(instant)
        })
        .transpose()?;
    let (estimate, recorded) = match rt {
        Some((snapshot, handle)) => {
            let kind = snapshot.run_event_kind(handle, slot)
                .context("missing realtime event kind")?;
            let time = snapshot.run_event_time(handle, slot)
                .context("missing realtime event time")?;
            match kind {
                RtEventKind::Estimated => (Some(instant(time)?), None),
                RtEventKind::Recorded => (None, Some(instant(time)?)),
                RtEventKind::NoData | RtEventKind::Timetabled => (None, None),
            }
        }
        None => (None, None),
    };
    Ok(OjpEventTime {
        timetabled_time: timetabled,
        estimated_time: estimate,
        recorded_at_time: recorded,
    })
}

fn call(
    tt: &ZeroCopyTimetable<'_>,
    catalog: Option<&GtfsIdentityCatalog>,
    platforms: &[PlatformStopMeta],
    instance: Option<(TransportIdx, DayIdx)>,
    rt: Option<(&RegionRealtimeSnapshot, RegionRtRun)>,
    original: Option<&[PackedStop]>,
    effective: &[PackedStop],
    index: usize,
) -> Result<OjpCall> {
    let here = effective[index];
    let planned = original.map(|s| s[index]);
    let old_location = planned.map(|s| s.location());
    let actual_location = here.location();
    let metadata = rt.and_then(|(snapshot, handle)| snapshot.run_metadata(handle));
    let changed = metadata.and_then(|m| m.stop_change(index));
    let skipped = changed.is_some_and(|c| c.skipped);
    let original_ref = old_location
        .filter(|loc| *loc != actual_location)
        .map(|loc| name(tt, loc)).transpose()?;
    let planned_quay = old_location.and_then(|loc| quay(platforms, loc));
    let new_quay = if old_location.is_some_and(|loc| loc != actual_location) {
        quay(platforms, actual_location)
    } else {
        None
    };
    let scheduled_number = instance.and_then(|(transport, _)| {
        catalog.and_then(|c| c.stop_sequences(transport))
            .and_then(|seq| seq.get(index)).copied()
    });
    Ok(OjpCall {
        order: u32::try_from(index+1)?,
        stop_point_ref: name(tt, actual_location)?,
        original_stop_point_ref: original_ref,
        gtfs_stop_sequence: scheduled_number,
        unplanned_stop: instance.is_none(),
        not_serviced_stop: skipped,
        no_boarding_at_stop: skipped || !here.outbound(),
        no_alighting_at_stop: skipped || !here.inbound(),
        planned_quay,
        estimated_quay: new_quay,
        service_arrival: if index > 0 {
            Some(event(tt, instance, rt, index * 2 - 1)?)
        } else { None },
        service_departure: if index + 1 < effective.len() {
            Some(event(tt, instance, rt, index * 2)?)
        } else { None },
    })
}

fn realtime_run_for(
    snapshot: &RegionRealtimeSnapshot,
    source: SourceIdx,
    instance: (TransportIdx, DayIdx),
) -> Result<Option<(RegionRtRun, RegionRtPattern)>> {
    let mut found = None;
    for (slot, shard) in snapshot.chateaux.iter().enumerate() {
        for (run_idx, run) in shard.overlay.runs.iter().enumerate() {
            if run.source != source.0 || run.original.is_none_or(|original| {
                original.transport != instance.0.0 || original.day != instance.1.0
            }) { continue }
            ensure!(found.is_none(), "multiple RT runs for one static service instance");
            found = Some((
                RegionRtRun { chateau_slot: u32::try_from(slot)?, run_index: u32::try_from(run_idx)? },
                RegionRtPattern { chateau_slot: u32::try_from(slot)?, pattern: run.pattern },
            ));
        }
    }
    Ok(found)
}

/// Render a scheduled GTFS trip including cancelled status even when the
/// transport has been removed from journey search. GTFS-local serviceDate
/// is the requested identity, not the UTC traffic-day index.
pub fn scheduled_trip(
    tt: &ZeroCopyTimetable<'_>,
    catalog: &GtfsIdentityCatalog,
    platforms: &[PlatformStopMeta],
    snapshot: Option<&RegionRealtimeSnapshot>,
    instance: ResolvedInstance,
    trip_ref: &str,
    service_date: NaiveDate,
) -> Result<OjpTripDetail> {
    let (chateau, feed_id, _) = tt.source(instance.source)
        .context("scheduled trip has no GTFS source")?;
    let original = (instance.transport, instance.service_day_utc);
    let route = *tt.transport_route().get(instance.transport.idx())
        .context("invalid static transport route")?;
    let planned = tt.stops_on(route);
    ensure!(planned.len() >= 2, "scheduled trip has too few stops");
    let ready = snapshot.filter(|rt| rt.is_complete());
    let status = ready.map_or(StaticRunStatus::Scheduled, |rt|
        rt.static_status(instance.transport, instance.service_day_utc)
    );
    let resolved = ready.map(|rt| realtime_run_for(rt, instance.source, original))
        .transpose()?.flatten();
    let (effective, rt) = if let (Some(snapshot), Some((handle, pattern))) = (ready, resolved) {
        let stops = snapshot.stops(tt, pattern)
            .context("missing effective RT stop pattern")?;
        ensure!(stops.len() == planned.len(),
            "inserted or removed stops require explicit original-to-current call mapping");
        (stops, Some((snapshot, handle)))
    } else {
        (planned, None)
    };
    let calls = (0..effective.len())
        .map(|i| call(tt, Some(catalog), platforms, Some(original), rt, Some(planned), effective, i))
        .collect::<Result<Vec<_>>>()?;
    let deviation = calls.iter().any(|c|
        c.not_serviced_stop || c.unplanned_stop || c.estimated_quay.is_some()
    );
    let cancelled = status == StaticRunStatus::Cancelled;
    let source_revision = ready.and_then(|snapshot|
        snapshot.chateau_revisions().find(|(c, _)| *c == chateau).map(|(_, r)| r)
    );
    Ok(OjpTripDetail {
        trip_ref: trip_ref.to_owned(),
        chateau: chateau.to_owned(),
        feed_id: feed_id.to_owned(),
        service_date,
        scheduled: true,
        real_time: cancelled || rt.is_some(),
        unplanned: false,
        cancelled,
        deviation,
        realtime_context: OjpRealtimeContext {
            state: if ready.is_none() { "UNAVAILABLE" } else { "CURRENT" },
            snapshot_revision: ready.map(|r| r.publication_seq),
            source_revision,
        },
        calls,
    })
}

/// Added trips have no static timetable event baseline; do not fabricate
/// TimetabledTime by borrowing a similarly named scheduled transport.
pub fn added_trip(
    tt: &ZeroCopyTimetable<'_>,
    platforms: &[PlatformStopMeta],
    snapshot: &RegionRealtimeSnapshot,
    chateau: &str,
    source: SourceIdx,
    trip_ref: &str,
    service_date: NaiveDate,
    external_id: &str,
) -> Result<Option<OjpTripDetail>> {
    if !snapshot.is_complete() { return Ok(None) }
    let (_, feed_id, _) = tt.source(source).context("invalid GTFS source")?;
    let mut match_run = None;
    for (slot, shard) in snapshot.chateaux.iter().enumerate() {
        if shard.chateau != chateau { continue }
        for (idx, run) in shard.overlay.runs.iter().enumerate() {
            if run.source != source.0 || run.original.is_some() ||
                run.external_id.as_deref() != Some(external_id)
            { continue }
            ensure!(match_run.is_none(), "added trip instance is ambiguous");
            match_run = Some((
                RegionRtRun { chateau_slot: u32::try_from(slot)?, run_index: u32::try_from(idx)? },
                RegionRtPattern { chateau_slot: u32::try_from(slot)?, pattern: run.pattern },
            ));
        }
    }
    let Some((handle, pattern)) = match_run else { return Ok(None) };
    let stops = snapshot.stops(tt, pattern).context("added trip has no stop pattern")?;
    let calls = (0..stops.len())
        .map(|i| call(tt, None, platforms, None, Some((snapshot, handle)), None, stops, i))
        .collect::<Result<Vec<_>>>()?;
    let source_revision = snapshot.chateau_revisions()
        .find(|(c, _)| *c == chateau).map(|(_, r)| r);
    Ok(Some(OjpTripDetail {
        trip_ref: trip_ref.to_owned(),
        chateau: chateau.to_owned(),
        feed_id: feed_id.to_owned(),
        service_date,
        scheduled: false,
        real_time: true,
        unplanned: true,
        cancelled: false,
        deviation: false,
        realtime_context: OjpRealtimeContext {
            state: "CURRENT",
            snapshot_revision: Some(snapshot.publication_seq),
            source_revision,
        },
        calls,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ojp_field_names_keep_planned_and_estimated_distinct() {
        let event = OjpEventTime {
            timetabled_time: Some("2026-11-19T16:00:00Z".into()),
            estimated_time: Some("2026-11-19T16:04:30Z".into()),
            recorded_at_time: None,
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["TimetabledTime"], "2026-11-19T16:00:00Z");
        assert_eq!(value["EstimatedTime"], "2026-11-19T16:04:30Z");
        assert!(value.get("RecordedAtTime").is_none());
        let added = OjpEventTime {
            timetabled_time: None,
            estimated_time: Some("2026-11-19T16:05:00Z".into()),
            recorded_at_time: None,
        };
        let added_value = serde_json::to_value(added).unwrap();
        assert!(added_value.get("TimetabledTime").is_none());
        assert_eq!(added_value["EstimatedTime"], "2026-11-19T16:05:00Z");
    }

    #[test]
    fn cancellation_is_service_level_and_stop_skip_is_call_level() {
        let trip = OjpTripDetail {
            trip_ref: "T".into(), chateau: "A".into(), feed_id: "f".into(),
            service_date: NaiveDate::from_ymd_opt(2026,11,19).unwrap(),
            scheduled: true, real_time: true, unplanned: false,
            cancelled: true, deviation: false,
            realtime_context: OjpRealtimeContext {
                state: "CURRENT", snapshot_revision: Some(2), source_revision: Some(10),
            },
            calls: vec![OjpCall {
                order: 1, stop_point_ref: "S".into(), original_stop_point_ref: None,
                gtfs_stop_sequence: Some(17),
                unplanned_stop: false, not_serviced_stop: false,
                no_boarding_at_stop: false, no_alighting_at_stop: false,
                planned_quay: Some("2".into()), estimated_quay: None,
                service_arrival: None,
                service_departure: Some(OjpEventTime {
                    timetabled_time: Some("2026-11-19T16:00:00Z".into()),
                    estimated_time: None, recorded_at_time: None,
                }),
            }],
        };
        let value = serde_json::to_value(trip).unwrap();
        assert_eq!(value["Cancelled"], true);
        assert_eq!(value["Calls"][0]["NotServicedStop"], false);
        assert_eq!(value["Calls"][0]["PlannedQuay"], "2");
        assert!(value["Calls"][0]["ServiceDeparture"].get("EstimatedTime").is_none());
    }
}
