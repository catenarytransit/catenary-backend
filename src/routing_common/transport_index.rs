//! Compressed event columns and event-day lookup adapted from Nigiri.
//! See third_party/nigiri-LICENSE and docs/nigiri-profile-parity.md.
//!
//! The v5 mmap stays in seconds. This immutable view is constructed once at
//! timetable load, not once per query/day. Original transport IDs are retained.
use crate::routing_common::raptor_query::RaptorStats;
use crate::routing_common::routing_index::IndexArray;
use crate::routing_common::timetable::{BitfieldIdx, RouteIdx, TransportIdx, ZeroCopyTimetable};
use std::collections::HashMap;

const DAY: i64 = 86_400;

// Four u32 words per event column: base, GCD unit, bit width, payload word
// offset. Each column is word-aligned. Identical offsets cost zero payload.
// A chain retains the original representation if packing would enlarge it.
fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

pub(crate) fn pack_offsets(values: &[u32], patterns: usize) -> Option<Vec<u32>> {
    if patterns == 0 || values.is_empty() || values.len() % patterns != 0 {
        return None;
    }
    let slots = values.len() / patterns;
    let mut words = vec![0; slots.checked_mul(4)?];
    if words.len() >= values.len() {
        return None;
    }
    for (slot, column) in values.chunks_exact(patterns).enumerate() {
        let base = *column.iter().min()?;
        let unit = column.iter().fold(0, |g, &x| gcd(g, x - base)).max(1);
        let width = 32 - ((*column.iter().max()? - base) / unit).leading_zeros();
        let start = words.len();
        let count = patterns.checked_mul(width as usize)?.div_ceil(32);
        if start.checked_add(count)? >= values.len() {
            return None;
        }
        words[slot * 4..slot * 4 + 4]
            .copy_from_slice(&[base, unit, width, u32::try_from(start).ok()?]);
        words.resize(start + count, 0);
        if width == 0 {
            continue;
        }
        for (pattern, &value) in column.iter().enumerate() {
            let bit = pattern * width as usize;
            let at = start + bit / 32;
            let shift = bit % 32;
            let scaled = u64::from((value - base) / unit) << shift;
            words[at] |= scaled as u32;
            if shift + width as usize > 32 {
                words[at + 1] |= (scaled >> 32) as u32;
            }
        }
    }
    Some(words)
}

pub(crate) fn packed_offset(words: &[u32], patterns: usize, slot: usize, pattern: usize) -> u32 {
    debug_assert!(pattern < patterns);
    let header = &words[slot * 4..slot * 4 + 4];
    let (base, unit, width, start) = (header[0], header[1], header[2] as usize, header[3] as usize);
    if width == 0 {
        return base;
    }
    let bit = pattern * width;
    let at = start + bit / 32;
    let shift = bit % 32;
    let mut value = u64::from(words[at]);
    if shift + width > 32 {
        value |= u64::from(words[at + 1]) << 32;
    }
    let scaled = (value >> shift) & ((1u64 << width) - 1);
    base + unit * scaled as u32
}

pub(crate) fn validate_packed(words: &[u32], patterns: usize, slots: usize) -> bool {
    let Some(mut next) = slots.checked_mul(4) else { return false };
    if patterns == 0 || next > words.len() {
        return false;
    }
    for header in words[..next].chunks_exact(4) {
        let width = header[2] as usize;
        if width > 32 || header[1] == 0 || header[3] as usize != next {
            return false;
        }
        let Some(bits) = patterns.checked_mul(width) else { return false };
        let Some(end) = next.checked_add(bits.div_ceil(32)) else { return false };
        if end > words.len() {
            return false;
        }
        next = end;
    }
    if next != words.len() {
        return false;
    }
    // Validate actual values using widened arithmetic before the hot path.
    for slot in 0..slots {
        let h = &words[slot * 4..slot * 4 + 4];
        let width = h[2] as usize;
        if width == 0 { continue; }
        for pattern in 0..patterns {
            let bit = pattern * width;
            let at = h[3] as usize + bit / 32;
            let shift = bit % 32;
            let mut v = u64::from(words[at]);
            if shift + width > 32 { v |= u64::from(words[at + 1]) << 32; }
            let scaled = (v >> shift) & ((1u64 << width) - 1);
            if u64::from(h[0]) + u64::from(h[1]) * scaled > u64::from(u32::MAX) {
                return false;
            }
        }
    }
    true
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SelectedTransport {
    pub transport: TransportIdx,
    /// UTC day of the first departure, relative to first_utc_day.
    pub start_day: usize,
    pub event: i64,
}

/// Nigiri's event_times: first departure + event-major pattern offset.
/// No expanded stop-times matrix or query-local calendar index is needed.
pub(crate) struct TransportChain {
    pub(crate) transports: IndexArray<TransportIdx>,
    pub(crate) first_departures: IndexArray<u32>,
    pub(crate) patterns: IndexArray<usize>,
    pub(crate) offsets: IndexArray<u32>,
    // Experimental Maple v2-style base/GCD/bit-packed event columns.
    pub(crate) packed_offsets: Option<IndexArray<u32>>,
    pub(crate) n_patterns: usize,
    pub(crate) calendars: IndexArray<BitfieldIdx>,
    /// Union of service calendars shifted to all days on which a vehicle runs.
    /// This can extend past the last start day for overnight vehicles.
    pub(crate) event_days: IndexArray<u64>,
}

impl TransportChain {
    fn event_offset(&self, column: usize, slot: usize) -> i64 {
        let pattern = self.patterns[column];
        let offset = match &self.packed_offsets {
            Some(words) => packed_offset(words, self.n_patterns, slot, pattern),
            None => self.offsets[slot * self.n_patterns + pattern],
        };
        i64::from(self.first_departures[column]) + i64::from(offset)
    }

    /// Same guard and day conversion as raptor::get_earliest_transport:
    /// binary search only on the first event day; then visit chronological
    /// event days, pruning on the candidate event + destination lower bound.
    pub(crate) fn select(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        slot: usize,
        ready: i64,
        reverse: bool,
        destination_bound: i64,
        lower_bound: u32,
        stats: &mut RaptorStats,
    ) -> Option<SelectedTransport> {
        self.select_filtered(
            tt,
            slot,
            ready,
            reverse,
            destination_bound,
            lower_bound,
            stats,
            |_, _| true,
        )
    }

    /// Keep the exact FIFO chain search and calendar tests, but skip a
    /// specific static transport/service-day instance when realtime replaced
    /// or cancelled it. Continue searching the chain for the next eligible
    /// service instead of discarding the entire route.
    pub(crate) fn select_filtered(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        slot: usize,
        ready: i64,
        reverse: bool,
        destination_bound: i64,
        lower_bound: u32,
        stats: &mut RaptorStats,
        allow: impl Fn(TransportIdx, usize) -> bool,
    ) -> Option<SelectedTransport> {
        stats.transport_lookup_calls += 1;
        let base = i64::from(tt.header().first_utc_day) * DAY;
        let first_day = (ready - base).div_euclid(DAY);
        let second = (ready - base).rem_euclid(DAY);
        let limit = if reverse {
            destination_bound.saturating_add(i64::from(lower_bound))
        } else {
            destination_bound.saturating_sub(i64::from(lower_bound))
        };
        if if reverse {
            ready < limit
        } else {
            ready > limit
        } {
            stats.transport_bound_prunes += 1;
            return None;
        }
        let last_day = (limit - base).div_euclid(DAY);
        let mut day = first_day;
        while if reverse {
            day >= last_day
        } else {
            day <= last_day
        } {
            stats.transport_days_tested += 1;
            let active = day >= 0
                && self
                    .event_days
                    .get(day as usize / 64)
                    .is_some_and(|word| word & (1u64 << (day as usize % 64)) != 0);
            if active {
                stats.active_transport_days += 1;
                let n = self.transports.len();
                let column = |pos: usize| if reverse { n - pos - 1 } else { pos };
                let mut first = 0;
                if day == first_day {
                    let mut hi = n;
                    while first < hi {
                        let mid = first + (hi - first) / 2;
                        stats.transport_lookup_probes += 1;
                        let value = self.event_offset(column(mid), slot) % DAY;
                        if if reverse {
                            value > second
                        } else {
                            value < second
                        } {
                            first = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                }
                for pos in first..n {
                    stats.transport_lookup_probes += 1;
                    let col = column(pos);
                    let offset = self.event_offset(col, slot);
                    let event = base + day * DAY + offset % DAY;
                    if if reverse {
                        event < limit
                    } else {
                        event > limit
                    } {
                        stats.transport_bound_prunes += 1;
                        return None;
                    }
                    let start_day = day - offset / DAY;
                    if start_day < 0 || start_day >= i64::from(tt.header().day_count) {
                        continue;
                    }
                    if !tt.bitfields()[self.calendars[col].idx()].test(start_day as usize)
                        || !allow(self.transports[col], start_day as usize)
                    {
                        continue;
                    }
                    return Some(SelectedTransport {
                        transport: self.transports[col],
                        start_day: start_day as usize,
                        event,
                    });
                }
            }
            day += if reverse { -1 } else { 1 };
        }
        None
    }
}

pub(crate) struct TransportIndex {
    pub(crate) routes: Vec<Vec<TransportChain>>,
}

impl TransportIndex {
    pub(crate) fn chains(&self, route: RouteIdx) -> &[TransportChain] {
        &self.routes[route.idx()]
    }

    pub(crate) fn build(tt: &ZeroCopyTimetable<'_>) -> Self {
        let mut routes = Vec::with_capacity(tt.route_transport_ranges().len());
        for range in tt.route_transport_ranges() {
            // Existing files guarantee elapsed-time FIFO, not modulo-day FIFO.
            // Partition as Nigiri's loader/get_index.h does. Also keep the
            // event-day-offset vector equal within a chain: this preserves
            // elapsed ordering even for multi-day trips with differing runtimes.
            let mut groups = Vec::<Vec<TransportIdx>>::new();
            let mut by_day_offsets = HashMap::<Vec<i64>, Vec<usize>>::new();
            for id in range.from..range.to {
                let t = TransportIdx(id);
                let events = tt
                    .events_for_transport(t)
                    .expect("validated timing profile");
                if events.is_empty() {
                    continue;
                }
                let first = i64::from(tt.transport_first_dep_offset()[t.idx()].seconds());
                let days = events
                    .iter()
                    .map(|e| (first + i64::from(e.seconds())) / DAY)
                    .collect::<Vec<_>>();
                let candidates = by_day_offsets.entry(days).or_default();
                let chain = candidates.iter().copied().find(|&index| {
                    let last = *groups[index].last().unwrap();
                    let prev_first =
                        i64::from(tt.transport_first_dep_offset()[last.idx()].seconds());
                    tt.events_for_transport(last)
                        .unwrap()
                        .iter()
                        .zip(events)
                        .all(|(a, b)| {
                            (prev_first + i64::from(a.seconds())) % DAY
                                <= (first + i64::from(b.seconds())) % DAY
                        })
                });
                if let Some(index) = chain {
                    groups[index].push(t);
                } else {
                    candidates.push(groups.len());
                    groups.push(vec![t]);
                }
            }
            let mut chains = Vec::with_capacity(groups.len());
            for transports in groups {
                let slots = tt.events_for_transport(transports[0]).unwrap().len();
                let mut ids = HashMap::new();
                let mut profiles = Vec::new();
                let mut patterns = Vec::with_capacity(transports.len());
                let mut first_departures = Vec::with_capacity(transports.len());
                let mut calendars = Vec::with_capacity(transports.len());
                let max_days = transports
                    .iter()
                    .map(|&t| {
                        (i64::from(tt.transport_first_dep_offset()[t.idx()].seconds())
                            + i64::from(
                                tt.events_for_transport(t)
                                    .unwrap()
                                    .last()
                                    .unwrap()
                                    .seconds(),
                            ))
                            / DAY
                    })
                    .max()
                    .unwrap_or(0) as usize;
                let mut event_days =
                    vec![0u64; (tt.header().day_count as usize + max_days).div_ceil(64)];
                for &t in &transports {
                    let profile = tt.transport_timing_profile()[t.idx()];
                    let index = *ids.entry(profile.idx()).or_insert_with(|| {
                        profiles.push(tt.events_for_transport(t).unwrap());
                        profiles.len() - 1
                    });
                    patterns.push(index);
                    first_departures.push(tt.transport_first_dep_offset()[t.idx()].seconds());
                    let calendar = tt.transport_traffic_days()[t.idx()];
                    calendars.push(calendar);
                    let max_delta = (i64::from(*first_departures.last().unwrap())
                        + i64::from(
                            tt.events_for_transport(t)
                                .unwrap()
                                .last()
                                .unwrap()
                                .seconds(),
                        ))
                        / DAY;
                    // Nigiri rebuild_route_traffic_days: OR shifted calendars.
                    for shift in 0..=max_delta as usize {
                        for (wi, &word) in tt.bitfields()[calendar.idx()].words.iter().enumerate() {
                            let out = wi + shift / 64;
                            let bits = shift % 64;
                            if out < event_days.len() {
                                event_days[out] |= word << bits;
                            }
                            if bits != 0 && out + 1 < event_days.len() {
                                event_days[out + 1] |= word >> (64 - bits);
                            }
                        }
                    }
                }
                let n_patterns = profiles.len();
                let mut offsets = Vec::with_capacity(slots * n_patterns);
                for slot in 0..slots {
                    for profile in &profiles {
                        offsets.push(profile[slot].seconds());
                    }
                }
                chains.push(TransportChain {
                    transports: IndexArray::owned(transports),
                    first_departures: IndexArray::owned(first_departures),
                    patterns: IndexArray::owned(patterns),
                    offsets: IndexArray::owned(offsets),
                    packed_offsets: None,
                    n_patterns,
                    calendars: IndexArray::owned(calendars),
                    event_days: IndexArray::owned(event_days),
                });
            }
            routes.push(chains);
        }
        Self { routes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::profile_compact::ProfileSearchState;
    use crate::routing_common::profile_search::StartLabel;
    use crate::routing_common::raptor_query::{Leg, Query, WalkingEndpoint};
    use crate::routing_common::timetable::*;
    use std::time::Duration;

    /// Profiles list absolute seconds from the vehicle's start-day midnight.
    fn fixture(profiles: &[Vec<u32>], days: &[Vec<usize>], day_count: u32) -> TimetableOwned {
        let mut tt = TimetableOwned::empty(20_000, day_count);
        tt.add_source("test", "calendar", "attempt").unwrap();
        let n = profiles[0].len() / 2 + 1;
        for loc in 0..n {
            tt.coordinates.push(Default::default());
            tt.location_id.push(Default::default());
            tt.location_name.push(Default::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(1);
            tt.location_transfer_seconds.push(0);
            tt.location_routes_offsets.push((loc + 1) as u32);
            tt.location_routes.push(RouteIdx(0));
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
            tt.route_stops
                .push(PackedStop::new(LocationIdx(loc as u32), true, true, true, true).unwrap());
        }
        tt.route_stop_offsets.push(n as u32);
        tt.route_transport_ranges.push(Range {
            from: 0,
            to: profiles.len() as u32,
        });
        tt.route_clasz.push(2);
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        let mut union = TrafficDays::default();
        for (i, (events, active)) in profiles.iter().zip(days).enumerate() {
            tt.timing_profile_events.extend(
                events
                    .iter()
                    .map(|e| Delta::from_seconds(e - events[0]).unwrap()),
            );
            tt.timing_profile_offsets
                .push(tt.timing_profile_events.len() as u32);
            tt.transport_timing_profile.push(TimingProfileIdx(i as u32));
            tt.transport_first_dep_offset
                .push(Delta::from_seconds(events[0]).unwrap());
            tt.transport_route.push(RouteIdx(0));
            tt.transport_initial_day_offset.push(0);
            tt.transport_to_trip_section_offsets.push(0);
            let mut calendar = TrafficDays::default();
            for &d in active {
                calendar.set(d, true).unwrap();
            }
            union.or_assign(&calendar);
            tt.transport_traffic_days
                .push(BitfieldIdx(tt.bitfields.len() as u32));
            tt.bitfields.push(calendar);
        }
        tt.route_traffic_days
            .push(BitfieldIdx(tt.bitfields.len() as u32));
        tt.bitfields.push(union);
        tt
    }

    #[test]
    fn maple_style_packing_preserves_every_offset() {
        let n = 73;
        let values: Vec<u32> = (0..5).flat_map(|slot| {
            (0..n).map(move |i| match slot {
                0 => 0,
                1 => 3600 + (i % 13) as u32 * 60,
                2 => 86400 + (i * 37 % 127) as u32,
                3 => 172800 + (i % 9) as u32 * 7,
                _ => 300 + (i % 3) as u32,
            })
        }).collect();
        let packed = pack_offsets(&values, n).unwrap();
        assert!(packed.len() < values.len());
        assert!(validate_packed(&packed, n, 5));
        for (i, &value) in values.iter().enumerate() {
            assert_eq!(packed_offset(&packed, n, i / n, i % n), value);
        }
        assert!(!validate_packed(&packed[..packed.len()-1], n, 5));
        assert!(pack_offsets(&[1, 2], 2).is_none());
    }

    #[test]
    fn indexed_selection_matches_exhaustive_calendar_search_forward_and_reverse() {
        let profiles = vec![
            vec![60, 180, 200, 500],
            vec![100, 200, 220, 520],
            vec![86_300, 86_390, 86_410, 86_500],
            vec![86_350, 86_420, 86_450, 172_900], // different multi-day runtime
            vec![172_810, 172_830, 172_850, 172_880],
        ];
        let days = vec![
            vec![0, 2, 63, 64],
            vec![1, 3, 64, 65],
            vec![0, 1, 64, 65],
            vec![0, 63],
            vec![1, 65],
        ];
        let bytes = fixture(&profiles, &days, 66).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let index = TransportIndex::build(&tt);
        let base = 20_000 * DAY;
        for reverse in [false, true] {
            for slot in 0..4 {
                for day in [-1, 0, 1, 2, 3, 4, 62, 63, 64, 65, 66, 67, 68] {
                    for second in [
                        0, 59, 60, 99, 100, 199, 200, 499, 500, 86_299, 86_300, 86_399,
                    ] {
                        let ready = base + day * DAY + second;
                        let bound = ready + if reverse { -3 * DAY } else { 3 * DAY };
                        let lb = 31;
                        for chain in index.chains(RouteIdx(0)) {
                            let actual = chain.select(
                                &tt,
                                slot,
                                ready,
                                reverse,
                                bound,
                                lb,
                                &mut RaptorStats::default(),
                            );
                            let mut expected = Vec::new();
                            for &t in &chain.transports {
                                for &d in &days[t.idx()] {
                                    let event =
                                        base + d as i64 * DAY + i64::from(profiles[t.idx()][slot]);
                                    if if reverse {
                                        event <= ready && event - i64::from(lb) >= bound
                                    } else {
                                        event >= ready && event + i64::from(lb) <= bound
                                    } {
                                        expected.push((event, t.0, d));
                                    }
                                }
                            }
                            expected.sort_unstable();
                            let expected = if reverse {
                                expected.last()
                            } else {
                                expected.first()
                            };
                            assert_eq!(
                                actual.map(|x| (x.event, x.transport.0, x.start_day)),
                                expected.copied(),
                                "reverse={reverse}, slot={slot}, day={day}, second={second}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn overnight_boarding_uses_previous_start_day_past_calendar_end() {
        let bytes = fixture(&[vec![86_300, 86_420, 86_430, 86_600]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base + 86_425,
            access: vec![WalkingEndpoint {
                stop: LocationIdx(1),
                seconds: 0,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
            allowed_modes: u16::MAX,
        };
        for reverse in [false, true] {
            let anchor = base + if reverse { 86_605 } else { 86_425 };
            let mut state =
                ProfileSearchState::new(&tt, &q, anchor, anchor, reverse, Duration::from_secs(2))
                    .unwrap();
            state
                .extend_starts(&tt, &q, &StartLabel::point(&q, anchor, reverse))
                .unwrap();
            let result = state.result();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].departure_unix_seconds, base + 86_430);
            assert_eq!(result[0].arrival_unix_seconds, base + 86_600);
            assert!(
                result[0]
                    .legs
                    .iter()
                    .any(|leg| matches!(leg, Leg::Ride { service_day: 0, .. }))
            );
        }
    }

    #[test]
    fn modulo_day_and_multiday_overtaking_are_separate_chains() {
        // Both vehicles have elapsed FIFO, but the later-start vehicle arrives
        // after midnight. A next-day earlier-start vehicle can overtake it.
        let bytes = fixture(
            &[vec![0, 100], vec![86_390, 86_600]],
            &[vec![0, 1], vec![0, 1]],
            2,
        )
        .encode()
        .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let index = TransportIndex::build(&tt);
        assert_eq!(index.chains(RouteIdx(0)).len(), 2);
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base + 86_385,
            access: vec![WalkingEndpoint {
                stop: LocationIdx(0),
                seconds: 0,
            }],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(1),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
            allowed_modes: u16::MAX,
        };
        let mut state = ProfileSearchState::new(
            &tt,
            &q,
            q.departure_unix_seconds,
            q.departure_unix_seconds,
            false,
            Duration::from_secs(2),
        )
        .unwrap();
        state
            .extend_starts(
                &tt,
                &q,
                &StartLabel::point(&q, q.departure_unix_seconds, false),
            )
            .unwrap();
        assert_eq!(
            state.result().iter().map(|j| j.arrival_unix_seconds).min(),
            Some(base + 86_500)
        );
    }

    #[test]
    fn bound_prunes_before_skipping_inactive_calendars() {
        let bytes = fixture(&[vec![100, 200], vec![200, 300]], &[vec![1], vec![0]], 2)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let index = TransportIndex::build(&tt);
        let base = 20_000 * DAY;
        let mut stats = RaptorStats::default();
        assert!(
            index.chains(RouteIdx(0))[0]
                .select(&tt, 0, base, false, base + 50, 0, &mut stats)
                .is_none()
        );
        assert_eq!(stats.transport_bound_prunes, 1);
        assert_eq!(stats.active_transport_days, 1); // reject before testing the inactive first transport
    }

    #[test]
    fn no_lookup_when_marked_stop_cannot_improve_onboard_transport() {
        let bytes = fixture(&[vec![100, 200, 200, 300]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base,
            access: vec![
                WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                },
                WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 250,
                },
            ],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
            allowed_modes: u16::MAX,
        };
        let mut state =
            ProfileSearchState::new(&tt, &q, base, base, false, Duration::from_secs(2)).unwrap();
        state
            .extend_starts(&tt, &q, &StartLabel::point(&q, base, false))
            .unwrap();
        assert_eq!(state.stats().transport_lookup_calls, 1);
        assert_eq!(state.stats().transport_lookup_skipped_onboard, 1);
        assert_eq!(state.result()[0].arrival_unix_seconds, base + 300);
    }

    #[test]
    fn grouped_start_marks_only_its_own_stop_and_rejects_unimplemented_vias() {
        let bytes = fixture(&[vec![100, 200, 200, 300]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base,
            access: vec![
                WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                },
                WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 0,
                },
            ],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
            allowed_modes: u16::MAX,
        };
        let mut state =
            ProfileSearchState::new(&tt, &q, base, base + 200, false, Duration::from_secs(2))
                .unwrap();
        let start = StartLabel {
            time_at_start: base + 100,
            time_at_stop: base + 100,
            stop: LocationIdx(0),
            via_state: 0,
        };
        state.extend_starts(&tt, &q, &[start, start]).unwrap();
        assert_eq!(state.stats().profile_anchor_events, 1);
        assert_eq!(state.stats().profile_start_labels, 1);
        assert_eq!(state.stats().marked_stops, 1);
        assert!(
            state
                .extend_starts(
                    &tt,
                    &q,
                    &[StartLabel {
                        via_state: 1,
                        ..start
                    }]
                )
                .is_err()
        );
    }

    #[test]
    fn boundary_search_dominates_waiting_at_unrelated_access_stop() {
        use crate::routing_common::profile_search::{ProfileOptions, route_profile_with_stats};
        // At start=100 the first platform reaches the destination at 300.
        // Starting outside the interval (>=151) at platform 1 reaches the same
        // vehicle at 200 and arrives at 300, so Nigiri suppresses start=100.
        let bytes = fixture(&[vec![100, 200, 200, 300]], &[vec![0]], 1)
            .encode()
            .unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base + 100,
            access: vec![
                WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 0,
                },
                WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 0,
                },
            ],
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(2),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 1000,
            allowed_modes: u16::MAX,
        };
        let options = ProfileOptions {
            search_window_seconds: 60,
            max_search_window_seconds: 60,
            min_itineraries: 1,
            max_journey_duration_seconds: 1000,
            ..Default::default()
        };
        let (journeys, stats) = route_profile_with_stats(&tt, &q, options, false).unwrap();
        assert_eq!(stats.profile_boundary_events, 1);
        assert_eq!(stats.round_copy_bytes, 0);
        // Recovery must not put a departure outside the allowed window back.
        assert!(journeys.is_empty());
    }

    #[test]
    fn multi_day_profile_matches_exhaustive_direct_trips_in_both_directions() {
        use crate::routing_common::profile_search::{
            ProfileOptions, departure_aware_pareto, route_profile_with_stats,
        };
        use crate::routing_common::raptor_query::Journey;
        let profiles = vec![
            vec![100, 200],
            vec![300, 410],
            vec![86390, 86600],
            vec![86401, 86450],
        ];
        let days = vec![vec![0, 2], vec![0, 1], vec![0, 1], vec![0, 2]];
        let bytes = fixture(&profiles, &days, 3).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        for reverse in [false, true] {
            let reference = if reverse { base + 3 * DAY } else { base };
            let q = Query {
                departure_unix_seconds: reference,
                access: vec![WalkingEndpoint {
                    stop: LocationIdx(0),
                    seconds: 17,
                }],
                egress: vec![WalkingEndpoint {
                    stop: LocationIdx(1),
                    seconds: 23,
                }],
                direct_walk_seconds: None,
                max_boardings: 1,
                max_duration_seconds: 1000,
                allowed_modes: u16::MAX,
            };
            let options = ProfileOptions {
                search_window_seconds: 3 * 86400,
                max_search_window_seconds: 3 * 86400,
                min_itineraries: 16,
                max_journey_duration_seconds: 1000,
                ..Default::default()
            };
            let (actual, stats) = route_profile_with_stats(&tt, &q, options, reverse).unwrap();
            let mut expected = Vec::new();
            for (events, active) in profiles.iter().zip(&days) {
                for &day in active {
                    let dep = base + day as i64 * DAY + i64::from(events[0]) - 17;
                    let arr = base + day as i64 * DAY + i64::from(events[1]) + 23;
                    if if reverse {
                        arr >= base && arr <= base + 3 * DAY
                    } else {
                        dep >= base && dep <= base + 3 * DAY
                    } {
                        expected.push(Journey {
                            departure_unix_seconds: dep,
                            arrival_unix_seconds: arr,
                            boardings: 1,
                            legs: vec![],
                        });
                    }
                }
            }
            let signature = |v: Vec<Journey>| {
                let mut result = v
                    .iter()
                    .map(|j| {
                        (
                            j.departure_unix_seconds,
                            j.arrival_unix_seconds,
                            j.boardings,
                        )
                    })
                    .collect::<Vec<_>>();
                result.sort();
                result
            };
            assert!(!stats.profile_budget_exhausted);
            assert_eq!(stats.profile_fallback_searches, 0);
            assert_eq!(
                signature(departure_aware_pareto(actual)),
                signature(departure_aware_pareto(expected)),
                "reverse={reverse}"
            );
        }
    }

    #[test]
    fn route_scan_avoids_repeated_service_days_and_onboard_lookups() {
        use crate::routing_common::raptor_query::route_with_stats;
        let events = (0..38u32)
            .map(|slot| 100 + slot.div_ceil(2) * 10)
            .collect::<Vec<_>>();
        let bytes = fixture(&[events], &[(0..7).collect()], 7).encode().unwrap();
        let tt = ZeroCopyTimetable::new(&bytes).unwrap();
        let base = 20_000 * DAY;
        let q = Query {
            departure_unix_seconds: base,
            access: (0..19u32)
                .map(|stop| WalkingEndpoint {
                    stop: LocationIdx(stop),
                    seconds: if stop == 0 { 0 } else { 101 + stop * 10 },
                })
                .collect(),
            egress: vec![WalkingEndpoint {
                stop: LocationIdx(19),
                seconds: 0,
            }],
            direct_walk_seconds: None,
            max_boardings: 1,
            max_duration_seconds: 7 * 86400,
            allowed_modes: u16::MAX,
        };
        let (reference, old_stats) = route_with_stats(&tt, &q).unwrap();
        let mut state =
            ProfileSearchState::new(&tt, &q, base, base, false, Duration::from_secs(2)).unwrap();
        state
            .extend_starts(&tt, &q, &StartLabel::point(&q, base, false))
            .unwrap();
        assert_eq!(
            state.result()[0].arrival_unix_seconds,
            reference[0].arrival_unix_seconds
        );
        assert_eq!(state.stats().transport_lookup_calls, 1);
        assert_eq!(state.stats().transport_lookup_skipped_onboard, 18);
        assert_eq!(state.stats().active_transport_days, 1);
        assert!(old_stats.active_transport_days >= 7);
        assert!(old_stats.transport_lookup_probes > state.stats().transport_lookup_probes * 20);
    }
}
