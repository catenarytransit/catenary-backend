//! Immutable, RAM-only regional realtime snapshot built from chateau overlays.
//!
//! Each independently refreshed chateau owns a compact RealtimeOverlay.
//! A region snapshot *references* those immutable shards via Arc; it never
//! copies event-time arrays, packed stop patterns, or transport records.
//! Callers pin one Arc<RegionRealtimeSnapshot> for the whole route search.
//!
//! A source-scoped pattern ID is not globally unique. Always carry its
//! chateau_slot with the pattern ID, including during RAPTOR reconstruction.
//! A snapshot with missing chateaux is explicitly incomplete and MUST NOT
//! be treated as complete realtime coverage.

use crate::routing_common::realtime_overlay::{
    RealtimeOverlay, RtPatternRef, RtRun, StaticInstance,
};
use crate::routing_common::timetable::{
    DayIdx, LocationIdx, PackedStop, SourceIdx, TransportIdx, ZeroCopyTimetable,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Identifies a realtime pattern within *one pinned region snapshot*.
/// The slot is an index into RegionRealtimeSnapshot::chateaux.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RegionRtPattern {
    pub chateau_slot: u32,
    pub pattern: RtPatternRef,
}

/// Immutable, independently versioned chateau sub-overlay.
#[derive(Clone)]
pub struct ChateauRealtimeShard {
    pub chateau: String,
    pub overlay: Arc<RealtimeOverlay>,
}

/// One consistent view of all available chateau revisions for a static region.
///
/// Revisions are a *vector*, not a single Aspen timestamp. publication_seq
/// increments for each region-wide composition; it has no relationship to an
/// Aspen revision_ms and must not be used as an Aspen pagination cursor.
pub struct RegionRealtimeSnapshot {
    pub timetable_generation: [u8; 32],
    pub event_epoch: i64,
    pub publication_seq: u64,
    pub chateaux: Vec<ChateauRealtimeShard>,
    pub expected_chateaux: usize,
    pub missing_chateaux: Vec<String>,
    location_patterns: BTreeMap<u32, Vec<RegionRtPattern>>,
    suppressed_static: BTreeSet<StaticInstance>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionSnapshotError {
    UnknownChateau,
    DuplicateChateau,
    WrongGeneration,
    WrongEpoch,
    InvalidSource,
    InvalidPattern,
    SuppressionCollision,
    TooManyChateaux,
}

impl std::fmt::Display for RegionSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RegionSnapshotError {}

impl RegionRealtimeSnapshot {
    /// Build the lightweight regional index over immutable, already prepared
    /// per-chateau overlays. The timetable is mmap-backed and is borrowed
    /// only to verify that run/source indices belong to their actual chateau.
    /// The expected source set comes from the embedded Gentian source table.
    pub fn compose(
        tt: &ZeroCopyTimetable<'_>,
        timetable_generation: [u8; 32],
        publication_seq: u64,
        expected_chateaux: &BTreeSet<String>,
        shards: Vec<ChateauRealtimeShard>,
    ) -> Result<Self, RegionSnapshotError> {
        let event_epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let mut location_patterns = BTreeMap::<u32, Vec<RegionRtPattern>>::new();
        let mut suppressed_static = BTreeSet::new();
        let mut chateaux = Vec::with_capacity(shards.len());
        let mut observed = BTreeSet::new();

        for shard in shards {
            if !expected_chateaux.contains(&shard.chateau) {
                return Err(RegionSnapshotError::UnknownChateau);
            }
            if !observed.insert(shard.chateau.clone()) {
                return Err(RegionSnapshotError::DuplicateChateau);
            }
            if shard.overlay.timetable_generation != timetable_generation {
                return Err(RegionSnapshotError::WrongGeneration);
            }
            if shard.overlay.event_epoch != event_epoch {
                return Err(RegionSnapshotError::WrongEpoch);
            }
            // The sequence of stops is already validated by the chateau
            // overlay builder. Never allow a run to be assigned to the wrong
            // compiled GTFS source merely because stop IDs match elsewhere.
            for run in &shard.overlay.runs {
                if tt.source(SourceIdx(run.source))
                    .is_none_or(|(chateau, _, _)| chateau != shard.chateau)
                {
                    return Err(RegionSnapshotError::InvalidSource);
                }
            }

            let chateau_slot =
                u32::try_from(chateaux.len()).map_err(|_| RegionSnapshotError::TooManyChateaux)?;
            for (&location, patterns) in &shard.overlay.location_patterns {
                if location as usize >= tt.coordinates().len() {
                    return Err(RegionSnapshotError::InvalidPattern);
                }
                let index = location_patterns.entry(location).or_default();
                for &pattern in patterns {
                    if !shard.overlay.pattern_runs.contains_key(&pattern) {
                        return Err(RegionSnapshotError::InvalidPattern);
                    }
                    index.push(RegionRtPattern { chateau_slot, pattern });
                }
            }
            for &instance in &shard.overlay.suppressed_static {
                if !suppressed_static.insert(instance) {
                    // Two independent chateaux must never both suppress the
                    // same transport-day instance. Fail the entire publication.
                    return Err(RegionSnapshotError::SuppressionCollision);
                }
            }
            chateaux.push(shard);
        }
        let missing_chateaux = expected_chateaux
            .difference(&observed)
            .cloned()
            .collect::<Vec<_>>();
        Ok(Self {
            timetable_generation,
            event_epoch,
            publication_seq,
            chateaux,
            expected_chateaux: expected_chateaux.len(),
            missing_chateaux,
            location_patterns,
            suppressed_static,
        })
    }

    pub fn is_complete(&self) -> bool {
        self.missing_chateaux.is_empty()
    }

    pub fn chateau_revisions(&self) -> impl Iterator<Item = (&str, u64)> {
        self.chateaux.iter().map(|shard| {
            (shard.chateau.as_str(), shard.overlay.revision)
        })
    }

    /// Sparse region-level stop -> (chateau shard, pattern) lookup. Returns
    /// valid identifiers only while this exact Arc snapshot is pinned.
    pub fn patterns_at(&self, location: LocationIdx) -> &[RegionRtPattern] {
        self.location_patterns
            .get(&location.0)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn runs_for(
        &self,
        pattern: RegionRtPattern,
    ) -> impl Iterator<Item = &RtRun> {
        self.chateaux
            .get(pattern.chateau_slot as usize)
            .into_iter()
            .flat_map(move |shard| shard.overlay.runs_for(pattern.pattern))
    }

    pub fn stops<'a, 't>(
        &'a self,
        tt: &'t ZeroCopyTimetable<'_>,
        pattern: RegionRtPattern,
    ) -> Option<&'a [PackedStop]>
    where
        't: 'a,
    {
        self.chateaux.get(pattern.chateau_slot as usize)?
            .overlay.stops(tt, pattern.pattern)
    }

    pub fn event_time(&self, pattern: RegionRtPattern, run: &RtRun, slot: usize) -> Option<i64> {
        let overlay = &self.chateaux.get(pattern.chateau_slot as usize)?.overlay;
        overlay.event_time(run, slot)
    }

    pub fn is_static_suppressed(&self, transport: TransportIdx, day: DayIdx) -> bool {
        self.suppressed_static.contains(&StaticInstance {
            transport: transport.0,
            day: day.0,
        })
    }

    pub fn suppressed_count(&self) -> usize {
        self.suppressed_static.len()
    }

    pub fn run_count(&self) -> usize {
        self.chateaux.iter().map(|shard| shard.overlay.runs.len()).sum()
    }

    pub fn indexed_stop_count(&self) -> usize {
        self.location_patterns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::realtime_overlay::{
        RealtimeOverlayBuilder, RtPatternInput, RtRunInput, RtPatternRef,
    };
    use crate::routing_common::timetable::{
        BitfieldIdx, Clasz, Coordinate, PackedStop, Range, StringRef, TimetableOwned,
    };

    fn fixture() -> Vec<u8> {
        let mut tt = TimetableOwned::empty(20_000, 4);
        let a = tt.add_source("chateau-a", "feed-a", "attempt-a").unwrap();
        let b = tt.add_source("chateau-b", "feed-b", "attempt-b").unwrap();
        for (i, source) in [a, a, b, b].into_iter().enumerate() {
            tt.coordinates.push(Coordinate::default());
            tt.location_id.push(StringRef::default());
            tt.location_name.push(StringRef::default());
            tt.location_source.push(source);
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(0);
            tt.location_transfer_seconds.push(120);
            tt.location_routes_offsets.push(0);
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
            if i < 2 {
                tt.route_stops.push(PackedStop::new(
                    LocationIdx(i as u32), true, true, true, true,
                ).unwrap());
            }
        }
        tt.route_stop_offsets.push(2);
        tt.route_transport_ranges.push(Range { from: 0, to: 0 });
        tt.route_clasz.push(Clasz::Bus as u8);
        tt.route_traffic_days.push(BitfieldIdx(0));
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.encode().unwrap()
    }

    fn shard(tt: &ZeroCopyTimetable<'_>, source: SourceIdx, chateau: &str,
             generation: [u8; 32], rev: u64, start: i64) -> ChateauRealtimeShard {
        let mut builder = RealtimeOverlayBuilder::new_sparse(tt, generation, rev);
        let first = if source.0 == 0 { 0 } else { 2 };
        builder.add_run(RtRunInput {
            source,
            original: None,
            service_date: 20_000,
            external_id: Some(format!("{chateau}-{rev}")),
            pattern: RtPatternInput::Stops(vec![
                PackedStop::new(LocationIdx(first), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(first + 1), true, true, true, true).unwrap(),
            ]),
            event_times: vec![start, start + 100],
        }).unwrap();
        ChateauRealtimeShard {
            chateau: chateau.into(),
            overlay: Arc::new(builder.finish().unwrap()),
        }
    }

    #[test]
    fn snapshot_shares_chateau_arrays_and_keeps_pattern_namespaces_distinct() {
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let generation = [7; 32];
        let original = shard(&tt, SourceIdx(0), "chateau-a", generation, 1, epoch + 1);
        let other = shard(&tt, SourceIdx(1), "chateau-b", generation, 1, epoch + 2);
        let keep = Arc::clone(&original.overlay);
        let expected = BTreeSet::from(["chateau-a".to_owned(), "chateau-b".to_owned()]);
        let region = RegionRealtimeSnapshot::compose(
            &tt, generation, 1, &expected, vec![original, other],
        ).unwrap();
        assert!(region.is_complete());
        assert_eq!(region.expected_chateaux, 2);
        assert_eq!(region.run_count(), 2);
        assert!(Arc::ptr_eq(&region.chateaux[0].overlay, &keep));
        let pat = region.patterns_at(LocationIdx(0))[0];
        assert_eq!(pat.chateau_slot, 0);
        assert_eq!(region.runs_for(pat).count(), 1);
        assert_eq!(region.event_time(pat, region.runs_for(pat).next().unwrap(), 0), Some(epoch + 1));
        assert_eq!(region.stops(&tt, pat).unwrap()[0].location(), LocationIdx(0));
        let other_pat = region.patterns_at(LocationIdx(2))[0];
        assert_eq!(other_pat.chateau_slot, 1);
        assert_eq!(other_pat.pattern, RtPatternRef::Dynamic(1).min(other_pat.pattern));
        assert_eq!(region.runs_for(other_pat).count(), 1);
    }

    #[test]
    fn missing_chateau_and_generation_mismatch_are_explicit() {
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let expected = BTreeSet::from(["chateau-a".to_owned(), "chateau-b".to_owned()]);
        let one = shard(&tt, SourceIdx(0), "chateau-a", [1; 32], 1, epoch + 1);
        let partial = RegionRealtimeSnapshot::compose(
            &tt, [1; 32], 1, &expected, vec![one.clone()],
        ).unwrap();
        assert!(!partial.is_complete());
        assert_eq!(partial.missing_chateaux, ["chateau-b"]);
        let wrong = RegionRealtimeSnapshot::compose(
            &tt, [2; 32], 2, &expected, vec![one],
        );
        assert!(matches!(wrong, Err(RegionSnapshotError::WrongGeneration)));
    }

    #[test]
    fn reject_cross_chateau_source_assignment() {
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let expected = BTreeSet::from(["chateau-a".to_owned(), "chateau-b".to_owned()]);
        let wrong = shard(&tt, SourceIdx(0), "chateau-b", [1; 32], 1, epoch + 1);
        let result = RegionRealtimeSnapshot::compose(
            &tt, [1; 32], 1, &expected, vec![wrong],
        );
        assert!(matches!(result, Err(RegionSnapshotError::InvalidSource)));
    }
}
