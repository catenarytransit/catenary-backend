//! RAM-only, lossless realtime overlay building blocks for regional RAPTOR.
//!
//! All identifiers are local to a particular Gentian timetable generation.
//! No filesystem I/O, mmap writes, or expanded stop sequences per RT run.
//! Static stop patterns are borrowed from the existing V6 timetable; only
//! previously unseen dynamic patterns are stored in the overlay.
//!
//! This module provides the data model, canonicalization, inverse indices and
//! atomic-in-memory publication. Aspen RPC import and RAPTOR scans are separate
//! integration steps; neither is silently enabled by this module.

use crate::routing_common::timetable::{
    DayIdx, LocationIdx, PackedStop, RouteIdx, SourceIdx, TransportIdx, ZeroCopyTimetable,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

/// Refers to an immutable V6 route or an interned pattern within this snapshot.
/// Serialize the numeric IDs rather than Rust's private strong-index wrappers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RtPatternRef {
    Static(u32),
    Dynamic(u32),
}

/// A single scheduled transport instance, not a complete transport calendar.
/// Days are indices into the referenced timetable's UTC traffic-day interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StaticInstance {
    pub transport: u32,
    pub day: u16,
}

/// Input from a future Aspen-to-Gentian resolver.
/// A full packed stop contains stop identity AND boarding/accessibility flags.
pub enum RtPatternInput {
    Static(RouteIdx),
    Stops(Vec<PackedStop>),
}

/// All event times are absolute Unix seconds. The snapshot stores exact i32
/// offsets from the timetable epoch; impossible offsets fail, never truncate.
pub struct RtRunInput {
    pub source: SourceIdx,
    pub original: Option<(TransportIdx, DayIdx)>,
    /// Original local GTFS service date, represented as civil days since epoch.
    pub service_date: i32,
    /// Required for an unscheduled run; includes start-time instance identity
    /// where a feed can issue multiple instances of the same trip ID.
    pub external_id: Option<String>,
    pub pattern: RtPatternInput,
    /// dep(0), arr(1), dep(1), ..., arr(last), in exact seconds.
    pub event_times: Vec<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RtRun {
    pub source: u16,
    pub original: Option<StaticInstance>,
    pub service_date: i32,
    pub external_id: Option<String>,
    pub pattern: RtPatternRef,
    pub event_from: u32,
    pub event_to: u32,
}

/// A self-contained, immutable RAM snapshot. Its static references are valid
/// only with the matching timetable generation. Inverse indices are by *pattern*
/// so repeated stops are not indexed once per realtime run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RealtimeOverlay {
    pub timetable_generation: [u8; 32],
    pub revision: u64,
    pub event_epoch: i64,
    pub runs: Vec<RtRun>,
    pub dynamic_pattern_offsets: Vec<u32>,
    pub dynamic_pattern_stops: Vec<u32>,
    pub event_offsets_seconds: Vec<i32>,
    pub suppressed_static: Vec<StaticInstance>,
    pub pattern_runs: BTreeMap<RtPatternRef, Vec<u32>>,
    pub location_patterns: BTreeMap<u32, Vec<RtPatternRef>>,
}

impl RealtimeOverlay {
    /// An empty, query-safe baseline, published without any file writes.
    pub fn empty(generation: [u8; 32], revision: u64, event_epoch: i64) -> Self {
        Self {
            timetable_generation: generation,
            revision,
            event_epoch,
            runs: Vec::new(),
            dynamic_pattern_offsets: vec![0],
            dynamic_pattern_stops: Vec::new(),
            event_offsets_seconds: Vec::new(),
            suppressed_static: Vec::new(),
            pattern_runs: BTreeMap::new(),
            location_patterns: BTreeMap::new(),
        }
    }

    pub fn stops<'s, 't>(
        &'s self,
        tt: &'t ZeroCopyTimetable<'_>,
        pattern: RtPatternRef,
    ) -> Option<&'s [PackedStop]>
    where
        't: 's,
    {
        match pattern {
            RtPatternRef::Static(route) => tt
                .route_transport_ranges()
                .get(route as usize)
                .map(|_| tt.stops_on(RouteIdx(route))),
            RtPatternRef::Dynamic(index) => {
                let from = *self.dynamic_pattern_offsets.get(index as usize)? as usize;
                let to = *self.dynamic_pattern_offsets.get(index as usize + 1)? as usize;
                self.dynamic_pattern_stops
                    .get(from..to)
                    .map(bytemuck::cast_slice::<u32, PackedStop>)
            }
        }
    }

    pub fn event_time(&self, run: &RtRun, slot: usize) -> Option<i64> {
        let from = run.event_from as usize;
        let to = run.event_to as usize;
        self.event_offsets_seconds
            .get(from..to)?
            .get(slot)
            .map(|offset| self.event_epoch + i64::from(*offset))
    }

    pub fn patterns_at(&self, stop: LocationIdx) -> &[RtPatternRef] {
        self.location_patterns
            .get(&stop.0)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn runs_for(&self, pattern: RtPatternRef) -> impl Iterator<Item = &RtRun> {
        self.pattern_runs
            .get(&pattern)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|index| self.runs.get(*index as usize))
    }

    pub fn is_static_suppressed(&self, transport: TransportIdx, day: DayIdx) -> bool {
        self.suppressed_static
            .binary_search(&StaticInstance {
                transport: transport.0,
                day: day.0,
            })
            .is_ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlayError {
    InvalidSource,
    InvalidStaticTransport,
    InvalidDay,
    InvalidRoute,
    InvalidLocation,
    InvalidEvents,
    DuplicateStaticInstance,
    MissingAddedTripIdentity,
    TimeOutOfRange,
    TooManyEntries,
    WrongGeneration,
    StaleRevision,
}

impl std::fmt::Display for OverlayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for OverlayError {}

fn pattern_hash(stops: &[PackedStop]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    stops.hash(&mut hasher);
    hasher.finish()
}

/// Construction-time index stores only hashes and route IDs, never copies
/// of complete static patterns. Hash collisions are resolved by exact comparison.
pub struct RealtimeOverlayBuilder<'tt, 'data> {
    tt: &'tt ZeroCopyTimetable<'data>,
    snapshot: RealtimeOverlay,
    static_patterns: HashMap<(usize, u64), Vec<u32>>,
    dynamic_patterns: Vec<Vec<PackedStop>>,
    dynamic_by_hash: HashMap<(usize, u64), Vec<u32>>,
    suppressed: BTreeSet<StaticInstance>,
}

impl<'tt, 'data> RealtimeOverlayBuilder<'tt, 'data> {
    /// Low-memory mode for building a chateau-local overlay. Dynamic patterns
    /// are interned against each other; unchanged scheduled runs explicitly
    /// borrow Static(RouteIdx). Avoid scanning and hashing every static route
    /// again for each chateau/revision.
    pub fn new_sparse(
        tt: &'tt ZeroCopyTimetable<'data>,
        generation: [u8; 32],
        revision: u64,
    ) -> Self {
        Self {
            tt,
            snapshot: RealtimeOverlay::empty(
                generation, revision,
                i64::from(tt.header().first_utc_day) * 86_400,
            ),
            static_patterns: HashMap::new(),
            dynamic_patterns: Vec::new(),
            dynamic_by_hash: HashMap::new(),
            suppressed: BTreeSet::new(),
        }
    }

    pub fn new(
        tt: &'tt ZeroCopyTimetable<'data>,
        generation: [u8; 32],
        revision: u64,
    ) -> Self {
        let mut static_patterns = HashMap::<(usize, u64), Vec<u32>>::new();
        for route in 0..tt.route_transport_ranges().len() {
            let stops = tt.stops_on(RouteIdx(route as u32));
            static_patterns
                .entry((stops.len(), pattern_hash(stops)))
                .or_default()
                .push(route as u32);
        }
        Self {
            tt,
            snapshot: RealtimeOverlay::empty(
                generation,
                revision,
                i64::from(tt.header().first_utc_day) * 86_400,
            ),
            static_patterns,
            dynamic_patterns: Vec::new(),
            dynamic_by_hash: HashMap::new(),
            suppressed: BTreeSet::new(),
        }
    }

    fn instance(&self, t: TransportIdx, day: DayIdx) -> Result<StaticInstance, OverlayError> {
        if t.idx() >= self.tt.transport_route().len() {
            return Err(OverlayError::InvalidStaticTransport);
        }
        if u32::from(day.0) >= self.tt.header().day_count {
            return Err(OverlayError::InvalidDay);
        }
        Ok(StaticInstance {
            transport: t.0,
            day: day.0,
        })
    }

    pub fn cancel_static(
        &mut self,
        transport: TransportIdx,
        day: DayIdx,
    ) -> Result<(), OverlayError> {
        let instance = self.instance(transport, day)?;
        self.suppressed.insert(instance);
        Ok(())
    }

    fn intern_pattern(&mut self, pattern: RtPatternInput) -> Result<RtPatternRef, OverlayError> {
        match pattern {
            RtPatternInput::Static(route) => {
                if route.idx() >= self.tt.route_transport_ranges().len() {
                    return Err(OverlayError::InvalidRoute);
                }
                Ok(RtPatternRef::Static(route.0))
            }
            RtPatternInput::Stops(stops) => {
                if stops.len() < 2
                    || stops.iter().any(|s| s.location().idx() >= self.tt.coordinates().len())
                {
                    return Err(OverlayError::InvalidLocation);
                }
                let key = (stops.len(), pattern_hash(&stops));
                if let Some(routes) = self.static_patterns.get(&key) {
                    for &route in routes {
                        if self.tt.stops_on(RouteIdx(route)) == stops.as_slice() {
                            return Ok(RtPatternRef::Static(route));
                        }
                    }
                }
                // Avoid hashing the entire regional static timetable for
                // every Chateau. The precompiled stop->route CSR narrows the
                // candidates to routes visiting the pattern's first stop.
                // Compare full PackedStop values (including all flags).
                for &route in self.tt.routes_at(stops[0].location()) {
                    if self.tt.stops_on(route) == stops.as_slice() {
                        return Ok(RtPatternRef::Static(route.0));
                    }
                }
                if let Some(patterns) = self.dynamic_by_hash.get(&key) {
                    for &id in patterns {
                        if self.dynamic_patterns[id as usize] == stops {
                            return Ok(RtPatternRef::Dynamic(id));
                        }
                    }
                }
                let id = u32::try_from(self.dynamic_patterns.len())
                    .map_err(|_| OverlayError::TooManyEntries)?;
                self.dynamic_patterns.push(stops);
                self.dynamic_by_hash.entry(key).or_default().push(id);
                Ok(RtPatternRef::Dynamic(id))
            }
        }
    }

    pub fn add_run(&mut self, input: RtRunInput) -> Result<(), OverlayError> {
        if input.source.idx() >= self.tt.sources().len() {
            return Err(OverlayError::InvalidSource);
        }
        if input.original.is_none() && input.external_id.as_ref().is_none_or(String::is_empty) {
            return Err(OverlayError::MissingAddedTripIdentity);
        }
        let original = input
            .original
            .map(|(t, d)| self.instance(t, d))
            .transpose()?;
        if original.is_some_and(|key| self.suppressed.contains(&key)) {
            return Err(OverlayError::DuplicateStaticInstance);
        }
        let stop_count = match &input.pattern {
            RtPatternInput::Static(route) => {
                if route.idx() >= self.tt.route_transport_ranges().len() {
                    return Err(OverlayError::InvalidRoute);
                }
                self.tt.stops_on(*route).len()
            }
            RtPatternInput::Stops(stops) => stops.len(),
        };
        if stop_count < 2
            || input.event_times.len() != (stop_count - 1) * 2
            || input.event_times.windows(2).any(|times| times[0] > times[1])
        {
            return Err(OverlayError::InvalidEvents);
        }
        // Validate the entire time sequence before mutating the builder.
        let offsets = input
            .event_times
            .iter()
            .map(|t| {
                let diff = t.checked_sub(self.snapshot.event_epoch)
                    .ok_or(OverlayError::TimeOutOfRange)?;
                i32::try_from(diff).map_err(|_| OverlayError::TimeOutOfRange)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let from = u32::try_from(self.snapshot.event_offsets_seconds.len())
            .map_err(|_| OverlayError::TooManyEntries)?;
        let to = from
            .checked_add(u32::try_from(offsets.len()).map_err(|_| OverlayError::TooManyEntries)?)
            .ok_or(OverlayError::TooManyEntries)?;
        let pattern = self.intern_pattern(input.pattern)?;
        let run_id = u32::try_from(self.snapshot.runs.len())
            .map_err(|_| OverlayError::TooManyEntries)?;
        self.snapshot.event_offsets_seconds.extend(offsets);
        self.snapshot.runs.push(RtRun {
            source: input.source.0,
            original,
            service_date: input.service_date,
            external_id: input.external_id,
            pattern,
            event_from: from,
            event_to: to,
        });
        self.snapshot.pattern_runs.entry(pattern).or_default().push(run_id);
        if let Some(key) = original {
            self.suppressed.insert(key);
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<RealtimeOverlay, OverlayError> {
        for stops in self.dynamic_patterns {
            let end = self.snapshot.dynamic_pattern_stops.len()
                .checked_add(stops.len())
                .ok_or(OverlayError::TooManyEntries)?;
            self.snapshot.dynamic_pattern_offsets.push(
                u32::try_from(end).map_err(|_| OverlayError::TooManyEntries)?,
            );
            self.snapshot.dynamic_pattern_stops.extend(stops.into_iter().map(|s| s.0));
        }
        self.snapshot.suppressed_static = self.suppressed.into_iter().collect();
        // A circular route lists a stop once in the inverted index, but its
        // original stop occurrences remain in the canonical pattern.
        for &pattern in self.snapshot.pattern_runs.keys() {
            let mut unique = HashSet::new();
            let stops: Vec<_> = match pattern {
                RtPatternRef::Static(route) => self.tt.stops_on(RouteIdx(route))
                    .iter().map(|s| s.location().0).collect(),
                RtPatternRef::Dynamic(index) => {
                    let start = self.snapshot.dynamic_pattern_offsets[index as usize] as usize;
                    let end = self.snapshot.dynamic_pattern_offsets[index as usize + 1] as usize;
                    self.snapshot.dynamic_pattern_stops[start..end]
                        .iter().map(|s| s & PackedStop::LOCATION_MASK).collect()
                }
            };
            for stop in stops {
                if unique.insert(stop) {
                    self.snapshot.location_patterns.entry(stop).or_default().push(pattern);
                }
            }
        }
        Ok(self.snapshot)
    }
}

/// Publish a whole immutable overlay atomically in RAM. Readers pin an Arc for
/// the duration of an entire RAPTOR/PONG search, without holding the lock.
/// Updaters must provide increasing revisions from the same timetable generation.
pub struct RealtimeOverlayStore {
    snapshot: RwLock<Arc<RealtimeOverlay>>,
}

impl RealtimeOverlayStore {
    pub fn new(generation: [u8; 32], event_epoch: i64) -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(RealtimeOverlay::empty(
                generation, 0, event_epoch,
            ))),
        }
    }

    pub fn load(&self) -> Arc<RealtimeOverlay> {
        Arc::clone(&self.snapshot.read().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn publish(&self, next: RealtimeOverlay) -> Result<(), OverlayError> {
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        if current.timetable_generation != next.timetable_generation
            || current.event_epoch != next.event_epoch
        {
            return Err(OverlayError::WrongGeneration);
        }
        if next.revision <= current.revision {
            return Err(OverlayError::StaleRevision);
        }
        *current = Arc::new(next);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::{
        BitfieldIdx, Clasz, Coordinate, LocationIdx, Range, StringRef,
        TimetableOwned,
    };

    fn fixture() -> Vec<u8> {
        let mut tt = TimetableOwned::empty(20_000, 14);
        let source = tt.add_source("metro", "metro-feed", "attempt").unwrap();
        for _ in 0..4 {
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
        }
        for stop in 0..3 {
            tt.route_stops.push(PackedStop::new(
                LocationIdx(stop), true, true, true, true
            ).unwrap());
        }
        tt.route_stop_offsets.push(3);
        tt.route_transport_ranges.push(Range { from: 0, to: 0 });
        tt.route_clasz.push(Clasz::Bus as u8);
        tt.route_traffic_days.push(BitfieldIdx(0));
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.encode().unwrap()
    }

    fn stops(seq: &[u32]) -> Vec<PackedStop> {
        seq.iter().map(|&id| {
            PackedStop::new(LocationIdx(id), true, true, true, true).unwrap()
        }).collect()
    }

    #[test]
    fn reuses_static_and_dynamic_patterns_and_preserves_exact_seconds() {
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let mut builder = RealtimeOverlayBuilder::new(&tt, [7; 32], 1);
        for (id, seq) in [(0, vec![0, 1, 2]), (1, vec![0, 1, 3]), (2, vec![0, 1, 3])] {
            builder.add_run(RtRunInput {
                source: SourceIdx(0),
                original: None,
                service_date: 20_000,
                external_id: Some(format!("added-{id}")),
                pattern: RtPatternInput::Stops(stops(&seq)),
                event_times: vec![epoch + 11, epoch + 123, epoch + 125, epoch + 299],
            }).unwrap();
        }
        let snap = builder.finish().unwrap();
        assert_eq!(snap.dynamic_pattern_offsets, vec![0, 3]);
        assert_eq!(snap.runs[0].pattern, RtPatternRef::Static(0));
        assert_eq!(snap.runs[1].pattern, RtPatternRef::Dynamic(0));
        assert_eq!(snap.runs[2].pattern, RtPatternRef::Dynamic(0));
        assert_eq!(snap.patterns_at(LocationIdx(1)).len(), 2);
        assert_eq!(snap.runs_for(RtPatternRef::Dynamic(0)).count(), 2);
        assert_eq!(snap.event_time(&snap.runs[2], 3), Some(epoch + 299));
        assert_eq!(snap.stops(&tt, RtPatternRef::Dynamic(0)).unwrap(), stops(&[0, 1, 3]));
    }

    #[test]
    fn packed_stop_flags_are_part_of_pattern_identity() {
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let mut builder = RealtimeOverlayBuilder::new(&tt, [7; 32], 1);
        let mut changed = stops(&[0, 1, 2]);
        changed[1] = PackedStop::new(LocationIdx(1), false, true, true, true).unwrap();
        builder.add_run(RtRunInput {
            source: SourceIdx(0),
            original: None,
            service_date: 20_000,
            external_id: Some("restricted".to_string()),
            pattern: RtPatternInput::Stops(changed),
            event_times: vec![epoch + 1, epoch + 2, epoch + 3, epoch + 4],
        }).unwrap();
        let snap = builder.finish().unwrap();
        assert_eq!(snap.runs[0].pattern, RtPatternRef::Dynamic(0));
    }

    #[test]
    fn publishing_is_ram_only_monotonic_and_keeps_old_reader_alive() {
        let base = RealtimeOverlay::empty([5; 32], 0, 123);
        let store = RealtimeOverlayStore::new([5; 32], 123);
        let reader = store.load();
        let mut next = base;
        next.revision = 1;
        store.publish(next).unwrap();
        assert_eq!(reader.revision, 0);
        assert_eq!(store.load().revision, 1);
        assert_eq!(
            store.publish(RealtimeOverlay::empty([5; 32], 1, 123)),
            Err(OverlayError::StaleRevision)
        );
        assert_eq!(
            store.publish(RealtimeOverlay::empty([4; 32], 2, 123)),
            Err(OverlayError::WrongGeneration)
        );
    }

    #[test]
    fn invalid_time_offset_does_not_truncate() {
        let bytes = fixture();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = i64::from(tt.header().first_utc_day) * 86_400;
        let mut builder = RealtimeOverlayBuilder::new(&tt, [1; 32], 1);
        let err = builder.add_run(RtRunInput {
            source: SourceIdx(0),
            original: None,
            service_date: 20_000,
            external_id: Some("far-future".into()),
            pattern: RtPatternInput::Static(RouteIdx(0)),
            event_times: vec![epoch + i64::from(i32::MAX) + 1; 4],
        }).unwrap_err();
        assert_eq!(err, OverlayError::TimeOutOfRange);
    }
}
