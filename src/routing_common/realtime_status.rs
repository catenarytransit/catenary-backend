//! Provenance and stop-call identity for lossless realtime trip responses.
//!
//! The static Gentian timetable remains the authoritative source of
//! timetabled events. This sparse metadata records ONLY exceptions/explicit
//! realtime observations; no per-run copy of the static stop sequence or
//! original timestamp array is required.
//!
//! GTFS-RT StopTimeEvent.time is a prediction or measured event depending on
//! feed semantics; a naked Unix timestamp must NOT automatically be labelled
//! "observed". Without an explicit measurement marker we use estimated.
//! The OJP response adapter can emit TimetabledTime and EstimatedTime
//! separately without making the caller infer status from unequal times.

use crate::routing_common::timetable::{DayIdx, TransportIdx, ZeroCopyTimetable};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RtEventKind {
    /// No feed prediction; the original static event is the fallback.
    Timetabled,
    /// Feed supplied an absolute event or a delay-derived prediction.
    Estimated,
    /// A NO_DATA explicitly reset this event to scheduled time.
    NoData,
    /// Reserved for input feeds with verified observed-event semantics.
    Recorded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtEventAnnotation {
    pub slot: u32,
    pub kind: RtEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtStopChange {
    /// Position in the effective run, not GTFS stop_sequence.
    pub position: u32,
    pub skipped: bool,
    pub no_data: bool,
    /// Packed static LocationIdx before a platform reassignment.
    pub original_location: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtRunMetadata {
    /// Sorted sparse event annotations. Absence means Timetabled for a
    /// scheduled instance, or Estimated for an added unscheduled instance.
    pub events: Vec<RtEventAnnotation>,
    /// Only changed scheduled stops; added trips are identified by original=None.
    pub stops: Vec<RtStopChange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StaticRunStatus {
    Scheduled,
    Cancelled,
    Replaced,
}

impl RtRunMetadata {
    pub fn event_kind(&self, slot: usize, added: bool) -> RtEventKind {
        self.events.binary_search_by_key(&(slot as u32), |a| a.slot)
            .ok()
            .map(|i| self.events[i].kind)
            .unwrap_or(if added { RtEventKind::Estimated } else { RtEventKind::Timetabled })
    }

    pub fn stop_change(&self, occurrence: usize) -> Option<&RtStopChange> {
        self.stops.binary_search_by_key(&(occurrence as u32), |s| s.position)
            .ok().map(|i| &self.stops[i])
    }

    pub fn validate(&self, event_count: usize, stop_count: usize) -> bool {
        self.events.iter().all(|a| (a.slot as usize) < event_count)
            && self.events.windows(2).all(|w| w[0].slot < w[1].slot)
            && self.stops.iter().all(|a| (a.position as usize) < stop_count)
            && self.stops.windows(2).all(|w| w[0].position < w[1].position)
    }
}

/// The original scheduled event must be reconstructed from the immutable
/// static timing profile, never inferred by subtracting a GTFS-RT delay
/// from the *latest* prediction (which could have been propagated).
pub fn scheduled_event_unix(
    tt: &ZeroCopyTimetable<'_>,
    transport: TransportIdx,
    service_day: DayIdx,
    slot: usize,
) -> Option<i64> {
    let first = i64::from(tt.header().first_utc_day)
        .checked_add(i64::from(service_day.0))?
        .checked_mul(86_400)?
        .checked_add(i64::from(tt.transport_first_dep_offset()
            .get(transport.idx())?.seconds()))?;
    first.checked_add(i64::from(tt.events_for_transport(transport)?
        .get(slot)?.seconds()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_metadata_has_different_baselines_for_scheduled_and_added_runs() {
        let data = RtRunMetadata::default();
        assert_eq!(data.event_kind(0, false), RtEventKind::Timetabled);
        assert_eq!(data.event_kind(0, true), RtEventKind::Estimated);
    }
    #[test]
    fn sparse_annotations_preserve_no_data_and_platform_change() {
        let data = RtRunMetadata {
            events: vec![
                RtEventAnnotation { slot: 0, kind: RtEventKind::Estimated },
                RtEventAnnotation { slot: 2, kind: RtEventKind::NoData },
            ],
            stops: vec![RtStopChange {
                position: 1,
                skipped: true,
                no_data: false,
                original_location: Some(123),
            }],
        };
        assert!(data.validate(4, 3));
        assert_eq!(data.event_kind(0, false), RtEventKind::Estimated);
        assert_eq!(data.event_kind(1, false), RtEventKind::Timetabled);
        assert_eq!(data.event_kind(2, false), RtEventKind::NoData);
        assert!(data.stop_change(1).unwrap().skipped);
        assert!(!RtRunMetadata {
            events: vec![data.events[1].clone(), data.events[0].clone()],
            stops: Vec::new(),
        }.validate(4, 3));
    }
}
