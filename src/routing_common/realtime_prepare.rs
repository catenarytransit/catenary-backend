//! Exact-second RAM-only realtime run preparation, following Nigiri's
//! separation of scheduled modifications and newly introduced RT transports.
//!
//! This module NEVER activates RAPTOR scans or suppresses a static trip when
//! preparation fails. Identical PackedStop sequences are interned downstream.

use crate::aspen_dataset::{
    AspenisedStopTimeScheduleRelationship as StopRelationship,
    AspenisedTripScheduleRelationship as TripRelationship,
};
use crate::routing_common::realtime_identity::{
    RealtimeGtfsResolver, ResolveIssue, ResolvedInstance,
};
use crate::routing_common::realtime_overlay::{RtPatternInput, RtRunInput};
use crate::routing_common::realtime_source::{
    RoutingStopUpdate, RoutingTimeUpdate, RoutingTripUpdate,
};
use crate::routing_common::realtime_status::{
    RtEventAnnotation, RtEventKind, RtRunMetadata, RtStopChange,
};
use crate::routing_common::timetable::{
    DayIdx, PackedStop, SourceIdx, TransportIdx, ZeroCopyTimetable,
};
use chrono::NaiveDate;

#[derive(Debug, PartialEq, Eq)]
pub enum PrepareError {
    Identity(ResolveIssue),
    MissingServiceDate,
    MissingTripIdentity,
    MissingStop,
    MissingEventTime,
    InvalidStopOrder,
    DuplicateOccurrence,
    InvalidEventTimes,
    InvalidStaticProfile,
    TimeOverflow,
    UnsupportedRelationship,
}

impl PrepareError {
    /// Low-cardinality category for per-chateau rejection reporting.
    pub fn diagnostic_code(&self) -> &'static str {
        match self {
            Self::Identity(issue) => issue.diagnostic_code(),
            Self::MissingServiceDate => "prepare_missing_service_date",
            Self::MissingTripIdentity => "prepare_missing_trip_identity",
            Self::MissingStop => "prepare_missing_stop",
            Self::MissingEventTime => "prepare_missing_event_time",
            Self::InvalidStopOrder => "prepare_invalid_stop_order",
            Self::DuplicateOccurrence => "prepare_duplicate_occurrence",
            Self::InvalidEventTimes => "prepare_invalid_event_times",
            Self::InvalidStaticProfile => "prepare_invalid_static_profile",
            Self::TimeOverflow => "prepare_time_overflow",
            Self::UnsupportedRelationship => "prepare_unsupported_relationship",
        }
    }
}

impl From<ResolveIssue> for PrepareError {
    fn from(e: ResolveIssue) -> Self {
        Self::Identity(e)
    }
}

pub enum PreparedAction {
    NoChange,
    Cancel(TransportIdx, DayIdx),
    Run(RtRunInput),
}

fn civil_day(date: NaiveDate) -> Result<i32, PrepareError> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or(PrepareError::TimeOverflow)?;
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| PrepareError::TimeOverflow)
}

fn validate_events(events: &[i64]) -> Result<(), PrepareError> {
    if events.len() < 2 || events.windows(2).any(|v| v[0] > v[1]) {
        return Err(PrepareError::InvalidEventTimes);
    }
    Ok(())
}

fn updated_time(
    scheduled: i64,
    update: Option<&RoutingTimeUpdate>,
    inherited_delay: &mut i64,
) -> Result<i64, PrepareError> {
    match update {
        Some(event) if event.unix_seconds.is_some() => {
            let actual = event.unix_seconds.expect("guarded");
            *inherited_delay = actual
                .checked_sub(scheduled)
                .ok_or(PrepareError::TimeOverflow)?;
            Ok(actual)
        }
        Some(event) if event.delay_seconds.is_some() => {
            *inherited_delay = i64::from(event.delay_seconds.expect("guarded"));
            scheduled
                .checked_add(*inherited_delay)
                .ok_or(PrepareError::TimeOverflow)
        }
        _ => scheduled
            .checked_add(*inherited_delay)
            .ok_or(PrepareError::TimeOverflow),
    }
}

/// Reconstruct the full event vector: dep(0), arr(1), dep(1), ..., arr(last).
/// A fresh prediction changes the propagated delay; NO_DATA resets it to
/// schedule. Original stop occurrences are matched by raw GTFS sequence,
/// even on circular routes. Explicit Unix times take precedence over delays.
pub fn scheduled_action(
    tt: &ZeroCopyTimetable<'_>,
    resolver: &RealtimeGtfsResolver<'_, '_>,
    source: SourceIdx,
    original: ResolvedInstance,
    update: &RoutingTripUpdate,
) -> Result<PreparedAction, PrepareError> {
    if matches!(
        update.schedule_relationship,
        Some(TripRelationship::Cancelled | TripRelationship::Deleted)
    ) {
        return Ok(PreparedAction::Cancel(
            original.transport,
            original.service_day_utc,
        ));
    }
    if matches!(
        update.schedule_relationship,
        Some(
            TripRelationship::Added | TripRelationship::Unscheduled | TripRelationship::Duplicated
        )
    ) {
        return Err(PrepareError::UnsupportedRelationship);
    }

    let transport = original.transport;
    let route = *tt
        .transport_route()
        .get(transport.idx())
        .ok_or(PrepareError::InvalidStaticProfile)?;
    let static_stops = tt.stops_on(route);
    let n = static_stops.len();
    let deltas = tt
        .events_for_transport(transport)
        .ok_or(PrepareError::InvalidStaticProfile)?;
    if n < 2 || deltas.len() != (n - 1) * 2 {
        return Err(PrepareError::InvalidStaticProfile);
    }
    let first_offset = tt
        .transport_first_dep_offset()
        .get(transport.idx())
        .ok_or(PrepareError::InvalidStaticProfile)?;
    let first_departure = i64::from(tt.header().first_utc_day)
        .checked_add(i64::from(original.service_day_utc.0))
        .and_then(|d| d.checked_mul(86_400))
        .and_then(|d| d.checked_add(i64::from(first_offset.seconds())))
        .ok_or(PrepareError::TimeOverflow)?;
    let scheduled = deltas
        .iter()
        .map(|time| {
            first_departure
                .checked_add(i64::from(time.seconds()))
                .ok_or(PrepareError::TimeOverflow)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut by_occurrence: Vec<Option<&RoutingStopUpdate>> = vec![None; n];
    let mut dynamic_stops: Option<Vec<PackedStop>> = None;
    let mut metadata = RtRunMetadata::default();
    for stop in &update.stops {
        let (index, _old, assigned) = resolver.stop_occurrence(source, transport, stop)?;
        if by_occurrence[index].replace(stop).is_some() {
            return Err(PrepareError::DuplicateOccurrence);
        }
        let base = static_stops[index];
        let skipped = matches!(stop.schedule_relationship, Some(StopRelationship::Skipped));
        let replacement = PackedStop::new(
            assigned.unwrap_or(base.location()),
            if skipped { false } else { base.inbound() },
            if skipped { false } else { base.outbound() },
            if skipped { false } else { base.wheelchair_in() },
            if skipped {
                false
            } else {
                base.wheelchair_out()
            },
        )
        .map_err(|_| PrepareError::MissingStop)?;
        if replacement != base {
            dynamic_stops.get_or_insert_with(|| static_stops.to_vec())[index] = replacement;
        }
        let no_data = matches!(stop.schedule_relationship, Some(StopRelationship::NoData));
        if skipped || no_data || assigned.is_some_and(|loc| loc != base.location()) {
            metadata.stops.push(RtStopChange {
                position: index as u32,
                skipped,
                no_data,
                original_location: assigned
                    .filter(|loc| *loc != base.location())
                    .map(|_| base.location().0),
            });
        }
    }

    metadata.stops.sort_by_key(|s| s.position);
    let affects_timing = update.delay_seconds.is_some()
        || update.stops.iter().any(|stop| {
            stop.arrival.is_some()
                || stop.departure.is_some()
                || matches!(stop.schedule_relationship, Some(StopRelationship::NoData))
        });
    if dynamic_stops.is_none() && !affects_timing {
        return Ok(PreparedAction::NoChange);
    }
    let mut inherited_delay = i64::from(update.delay_seconds.unwrap_or(0));
    let mut estimated = update.delay_seconds.is_some();
    let mut events = Vec::with_capacity(scheduled.len());
    for (index, stop) in by_occurrence.iter().enumerate() {
        let no_data = stop.is_some_and(|stop| {
            matches!(stop.schedule_relationship, Some(StopRelationship::NoData))
        });
        if no_data {
            inherited_delay = 0;
            estimated = false;
        }
        let mut push_event =
            |slot: usize, event: Option<&RoutingTimeUpdate>| -> Result<(), PrepareError> {
                let time = updated_time(scheduled[slot], event, &mut inherited_delay)?;
                if event.is_some_and(|e| e.unix_seconds.is_some() || e.delay_seconds.is_some()) {
                    estimated = true;
                }
                let kind = if no_data && !estimated {
                    RtEventKind::NoData
                } else if estimated {
                    RtEventKind::Estimated
                } else {
                    RtEventKind::Timetabled
                };
                if kind != RtEventKind::Timetabled {
                    metadata.events.push(RtEventAnnotation {
                        slot: slot as u32,
                        kind,
                    });
                }
                events.push(time);
                Ok(())
            };
        if index != 0 {
            push_event(index * 2 - 1, stop.and_then(|stop| stop.arrival.as_ref()))?;
        }
        if index + 1 != n {
            push_event(index * 2, stop.and_then(|stop| stop.departure.as_ref()))?;
        }
    }
    validate_events(&events)?;
    Ok(PreparedAction::Run(RtRunInput {
        source,
        original: Some((transport, original.service_day_utc)),
        service_date: civil_day(resolver.service_date(original)?)?,
        external_id: None,
        pattern: match dynamic_stops {
            Some(stops) => RtPatternInput::Stops(stops),
            None => RtPatternInput::Static(route),
        },
        event_times: events,
        metadata: Some(metadata),
    }))
}

/// ADDED/UNSCHEDULED/DUPLICATED runs have no static timing baseline.
/// Require exact Unix seconds at each routable event; never infer them from
/// a frequency assumption, a global delay or neighboring stop timestamps.
pub fn added_action(
    resolver: &RealtimeGtfsResolver<'_, '_>,
    source: SourceIdx,
    update: &RoutingTripUpdate,
) -> Result<PreparedAction, PrepareError> {
    if !matches!(
        update.schedule_relationship,
        Some(
            TripRelationship::Added | TripRelationship::Unscheduled | TripRelationship::Duplicated
        )
    ) {
        return Err(PrepareError::UnsupportedRelationship);
    }
    let service_date = civil_day(
        update
            .service_date
            .ok_or(PrepareError::MissingServiceDate)?,
    )?;
    let id = update
        .trip_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or(PrepareError::MissingTripIdentity)?;
    if update.stops.len() < 2 {
        return Err(PrepareError::MissingStop);
    }
    let mut previous_sequence: Option<u16> = None;
    let mut metadata = RtRunMetadata::default();
    let mut stops = Vec::with_capacity(update.stops.len());
    let mut events = Vec::with_capacity((update.stops.len() - 1) * 2);
    for (i, stop) in update.stops.iter().enumerate() {
        if let Some(sequence) = stop.sequence {
            if previous_sequence.is_some_and(|last| last >= sequence) {
                return Err(PrepareError::InvalidStopOrder);
            }
            previous_sequence = Some(sequence);
        }
        if matches!(stop.schedule_relationship, Some(StopRelationship::NoData)) {
            return Err(PrepareError::MissingEventTime);
        }
        let stop_id = stop.stop_id.as_deref().ok_or(PrepareError::MissingStop)?;
        let location = resolver.stop(source, stop_id)?;
        let assigned = stop
            .assigned_stop_id
            .as_deref()
            .map(|s| resolver.stop(source, s))
            .transpose()?;
        let boardable = !matches!(stop.schedule_relationship, Some(StopRelationship::Skipped));
        if !boardable || assigned.is_some_and(|loc| loc != location) {
            metadata.stops.push(RtStopChange {
                position: i as u32,
                skipped: !boardable,
                no_data: false,
                original_location: assigned.filter(|loc| *loc != location).map(|_| location.0),
            });
        }
        stops.push(
            PackedStop::new(
                assigned.unwrap_or(location),
                boardable,
                boardable,
                boardable,
                boardable,
            )
            .map_err(|_| PrepareError::MissingStop)?,
        );
        if i != 0 {
            events.push(
                stop.arrival
                    .as_ref()
                    .and_then(|t| t.unix_seconds)
                    .ok_or(PrepareError::MissingEventTime)?,
            );
        }
        if i + 1 != update.stops.len() {
            events.push(
                stop.departure
                    .as_ref()
                    .and_then(|t| t.unix_seconds)
                    .ok_or(PrepareError::MissingEventTime)?,
            );
        }
    }
    validate_events(&events)?;
    Ok(PreparedAction::Run(RtRunInput {
        source,
        original: None,
        service_date,
        // Stable instance identity: same source, trip ID, local service date
        // and start time is one run, even if Aspen has multiple entity IDs.
        // Length prefixes avoid ambiguous concatenations in GTFS IDs.
        external_id: Some(format!(
            "{}:{}{}:{}:{}",
            id.len(),
            id,
            update.start_time.as_deref().unwrap_or("").len(),
            update.start_time.as_deref().unwrap_or(""),
            service_date,
        )),
        pattern: RtPatternInput::Stops(stops),
        event_times: events,
        metadata: Some(metadata),
    }))
}
