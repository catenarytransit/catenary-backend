//! Bidirectional RT route-scan kernel, following Nigiri's per-run RT scan.
//! Works on a pinned, complete region snapshot; static arrays stay mmap.
//! Every candidate carries a shard-qualified handle for reconstruction.

use crate::routing_common::realtime_region::{
    RegionRealtimeSnapshot, RegionRtPattern, RegionRtRun,
};
use crate::routing_common::timetable::{LocationIdx, ZeroCopyTimetable};
use std::collections::{BTreeSet, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RtScanCandidate {
    pub run: RegionRtRun,
    pub pattern: RegionRtPattern,
    pub from_stop: u32,
    pub to_stop: u32,
    pub board_index: u32,
    pub alight_index: u32,
    pub departure_unix_seconds: i64,
    pub arrival_unix_seconds: i64,
}

/// Previous round entries are absolute earliest ready-to-board Unix seconds
/// for forward scans or latest ready-to-alight Unix seconds for reverse.
/// Unreachable entries must be i64::MAX (forward) or i64::MIN (reverse).
/// Transfer minima, dominance and journey reconstruction belong to caller.
/// All indexed stop visits are deduplicated by shard-qualified pattern.
pub fn scan_realtime_round(
    tt: &ZeroCopyTimetable<'_>,
    snapshot: &RegionRealtimeSnapshot,
    previous: &[i64],
    marked: &[usize],
    reverse: bool,
    mut emit: impl FnMut(RtScanCandidate),
) {
    if marked.is_empty() || !snapshot.is_complete() { return; }
    let marked_set = marked.iter().copied().collect::<HashSet<_>>();
    let mut patterns = BTreeSet::<RegionRtPattern>::new();
    for &stop in marked {
        let Some(&ready) = previous.get(stop) else { continue };
        if ready == if reverse { i64::MIN } else { i64::MAX } { continue; }
        for &pattern in snapshot.patterns_at(LocationIdx(stop as u32)) {
            patterns.insert(pattern);
        }
    }

    for pattern in patterns {
        let Some(stops) = snapshot.stops(tt, pattern) else { continue };
        if stops.len() < 2 { continue; }
        for handle in snapshot.run_handles_for(pattern) {
            let mut carried: Option<(usize, u32, i64)> = None;
            if reverse {
                for pos in (0..stops.len()).rev() {
                    let stop = stops[pos];
                    let loc = stop.location().idx();
                    // Reverse scan: start by alighting at an accessible stop,
                    // then emit earlier boardings on the same RT run.
                    if pos + 1 < stops.len() && stop.outbound() {
                        if let Some((alight, to_stop, arrival)) = carried {
                            if let Some(departure) = snapshot.run_event_time(handle, pos * 2) {
                                if departure <= arrival {
                                    emit(RtScanCandidate {
                                        run: handle, pattern, from_stop: loc as u32, to_stop,
                                        board_index: pos as u32, alight_index: alight as u32,
                                        departure_unix_seconds: departure,
                                        arrival_unix_seconds: arrival,
                                    });
                                }
                            }
                        }
                    }
                    if pos > 0 && stop.inbound() && marked_set.contains(&loc)
                        && let Some(&ready) = previous.get(loc)
                        && ready != i64::MIN
                        && let Some(arrival) = snapshot.run_event_time(handle, pos * 2 - 1)
                        && arrival <= ready
                        && carried.is_none_or(|(_, _, current)| arrival > current)
                    {
                        carried = Some((pos, loc as u32, arrival));
                    }
                }
            } else {
                for (pos, stop) in stops.iter().copied().enumerate() {
                    let loc = stop.location().idx();
                    if pos > 0 && stop.inbound() {
                        if let Some((board, from_stop, departure)) = carried {
                            if let Some(arrival) = snapshot.run_event_time(handle, pos * 2 - 1) {
                                if arrival >= departure {
                                    emit(RtScanCandidate {
                                        run: handle, pattern, from_stop, to_stop: loc as u32,
                                        board_index: board as u32, alight_index: pos as u32,
                                        departure_unix_seconds: departure,
                                        arrival_unix_seconds: arrival,
                                    });
                                }
                            }
                        }
                    }
                    if pos + 1 < stops.len() && stop.outbound() && carried.is_none()
                        && marked_set.contains(&loc)
                        && let Some(&ready) = previous.get(loc)
                        && ready != i64::MAX
                        && let Some(departure) = snapshot.run_event_time(handle, pos * 2)
                        && departure >= ready
                    {
                        carried = Some((pos, loc as u32, departure));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::realtime_overlay::{
        RealtimeOverlayBuilder, RtPatternInput, RtRunInput,
    };
    use crate::routing_common::realtime_region::ChateauRealtimeShard;
    use crate::routing_common::timetable::{
        Coordinate, PackedStop, SourceIdx, StringRef, TimetableOwned,
    };
    use std::sync::Arc;

    #[test]
    fn forward_and_reverse_scan_use_exact_events_and_skip_closed_stops() {
        let mut owned = TimetableOwned::empty(20_000, 10);
        let source = owned.add_source("c", "f", "a").unwrap();
        for _ in 0..4 {
            owned.coordinates.push(Coordinate::default());
            owned.location_id.push(StringRef::default());
            owned.location_name.push(StringRef::default());
            owned.location_source.push(source);
            owned.location_parent.push(LocationIdx::INVALID);
            owned.location_type.push(0);
            owned.location_transfer_seconds.push(0);
            owned.location_routes_offsets.push(0);
            owned.footpaths_out_offsets.push(0);
            owned.footpaths_in_offsets.push(0);
        }
        let bytes = owned.encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let epoch = 20_000_i64 * 86_400;
        let mut builder = RealtimeOverlayBuilder::new_sparse(&tt, [3; 32], 11);
        builder.add_run(RtRunInput {
            source,
            original: None,
            service_date: 20_000,
            external_id: Some("added-trip".into()),
            pattern: RtPatternInput::Stops(vec![
                PackedStop::new(LocationIdx(0), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(1), false, false, false, false).unwrap(),
                PackedStop::new(LocationIdx(2), true, true, true, true).unwrap(),
                PackedStop::new(LocationIdx(3), true, true, true, true).unwrap(),
            ]),
            event_times: vec![epoch+100, epoch+150, epoch+160,
                epoch+200, epoch+210, epoch+260],
            metadata: None,
        }).unwrap();
        let mut expected = BTreeSet::new();
        expected.insert("c".to_owned());
        let rt = RegionRealtimeSnapshot::compose(
            &tt, [3; 32], 1, &expected,
            vec![ChateauRealtimeShard {
                chateau: "c".to_owned(),
                overlay: Arc::new(builder.finish().unwrap()),
            }],
        ).unwrap();

        let mut prev = vec![i64::MAX; 4];
        prev[0] = epoch+90;
        let mut candidates = Vec::new();
        scan_realtime_round(&tt, &rt, &prev, &[0], false, |c| candidates.push(c));
        assert_eq!(candidates.iter().map(|c| c.to_stop).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(candidates[0].departure_unix_seconds, epoch+100);
        assert_eq!(candidates[0].arrival_unix_seconds, epoch+200);

        let mut prev = vec![i64::MIN; 4];
        prev[3] = epoch+270;
        candidates.clear();
        scan_realtime_round(&tt, &rt, &prev, &[3], true, |c| candidates.push(c));
        assert_eq!(candidates.iter().map(|c| c.from_stop).collect::<Vec<_>>(), vec![2, 0]);
        assert_eq!(candidates[1].departure_unix_seconds, epoch+100);
        assert_eq!(candidates[1].arrival_unix_seconds, epoch+260);
        assert!(!candidates.iter().any(|c| c.from_stop == 1 || c.to_stop == 1));
    }
}
