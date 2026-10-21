//! Nigiri-shaped static timetable data model; Catenary wire version 5.
//! Unlike Nigiri's minute-based wire representation, every duration is in seconds.
//! This is NOT binary compatible with Cista's serializer.
//! Compiler and RAPTOR algorithms are deliberately deferred.
//! Nigiri references: include/nigiri/{timetable.h,types.h,stop.h,footpath.h}.
//! SourceIdx follows Nigiri's 16-bit source identity. In Catenary a source is
//! a unique (chateau, onestop_feed_id, attempt_id) triple; never globalize
//! raw GTFS stop IDs, route IDs, trip IDs, or service IDs.

use bytemuck::{Pod, Zeroable};
use std::fmt;

pub const MAGIC: u32 = 0x4341_544E;
pub const VERSION: u32 = 5;
pub const MAX_DAYS: usize = 512;
pub const TIMETABLE_OFFSET_DAYS: i32 = 5;
pub const DEFAULT_TRANSFER_SECONDS: u32 = 120;

macro_rules! strong_idx {
    ($($name:ident : $base:ty;)*) => {$(
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
        pub struct $name(pub $base);
        impl $name {
            pub const INVALID: Self = Self(<$base>::MAX);
            pub fn idx(self) -> usize { self.0 as usize }
        }
    )*};
}
strong_idx! {
    LocationIdx: u32; RouteIdx: u32; TransportIdx: u32; TripIdx: u32;
    BitfieldIdx: u32; RouteIdIdx: u32; TripIdIdx: u32; MergedTripsIdx: u32;
    TimingProfileIdx: u32;
    SourceIdx: u16; DayIdx: u16;
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
pub struct TrafficDays { pub words: [u64; 8] }
impl TrafficDays {
    pub fn test(&self, d: usize) -> bool {
        d < MAX_DAYS && self.words[d / 64] & (1 << (d % 64)) != 0
    }
    pub fn set(&mut self, d: usize, active: bool) -> Result<(), FormatError> {
        if d >= MAX_DAYS { return Err(FormatError::Index); }
        let bit = 1_u64 << (d % 64);
        if active { self.words[d / 64] |= bit; } else { self.words[d / 64] &= !bit; }
        Ok(())
    }
    pub fn or_assign(&mut self, rhs: &Self) {
        for (a,b) in self.words.iter_mut().zip(rhs.words) { *a |= b; }
    }
    pub fn shift_left(&self, days: usize) -> Self {
        let mut out = Self::default();
        if days >= MAX_DAYS { return out; }
        let word = days / 64;
        let rem = days % 64;
        for i in 0..8 {
            if i + word < 8 { out.words[i+word] |= self.words[i] << rem; }
            if rem > 0 && i + word + 1 < 8 {
                out.words[i+word+1] |= self.words[i] >> (64-rem);
            }
        }
        out
    }
}
/// Nigiri: 28-bit location and in/out, wheelchair-in/out flags.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
pub struct PackedStop(pub u32);
impl PackedStop {
    pub const LOCATION_MASK: u32 = (1 << 28) - 1;
    pub fn new(loc: LocationIdx, inbound: bool, outbound: bool,
               wheelchair_in: bool, wheelchair_out: bool) -> Result<Self, FormatError> {
        if loc.0 > Self::LOCATION_MASK { return Err(FormatError::Index); }
        Ok(Self(loc.0 | (u32::from(inbound) << 28) | (u32::from(outbound) << 29)
            | (u32::from(wheelchair_in) << 30) | (u32::from(wheelchair_out) << 31)))
    }
    pub fn location(self) -> LocationIdx { LocationIdx(self.0 & Self::LOCATION_MASK) }
    pub fn inbound(self) -> bool { self.0 & (1 << 28) != 0 }
    pub fn outbound(self) -> bool { self.0 & (1 << 29) != 0 }
    pub fn wheelchair_in(self) -> bool { self.0 & (1 << 30) != 0 }
    pub fn wheelchair_out(self) -> bool { self.0 & (1 << 31) != 0 }
}
/// Catenary v3: 32-bit location index + 32-bit walking duration in seconds.
/// Using a u64 instead of Nigiri's 23+9 packed u32 prevents truncating transfers
/// longer than 511 seconds or losing sub-minute precision.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
pub struct PackedFootpath(pub u64);
impl PackedFootpath {
    pub fn new(target: LocationIdx, seconds: u32) -> Result<Self, FormatError> {
        if target == LocationIdx::INVALID || seconds == 0 {
            return Err(FormatError::Index);
        }
        Ok(Self(u64::from(target.0) | (u64::from(seconds) << 32)))
    }
    pub fn target(self) -> LocationIdx { LocationIdx(self.0 as u32) }
    pub fn seconds(self) -> u32 { (self.0 >> 32) as u32 }
}
/// Catenary v3: 5-bit day offset plus second-of-day in the upper bits.
/// Supports up to 31 days and 23:59:59, preserving exact GTFS seconds.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
pub struct Delta(pub u32);
impl Delta {
    pub const SECONDS_PER_DAY: u32 = 86_400;
    pub const MAX_SECONDS: u32 = 32 * Self::SECONDS_PER_DAY - 1;

    pub fn new(days: u16, seconds_after_midnight: u32) -> Result<Self, FormatError> {
        if days >= 32 || seconds_after_midnight >= Self::SECONDS_PER_DAY {
            return Err(FormatError::Index);
        }
        Ok(Self((seconds_after_midnight << 5) | u32::from(days)))
    }
    pub fn from_seconds(seconds: u32) -> Result<Self, FormatError> {
        if seconds > Self::MAX_SECONDS { return Err(FormatError::Index); }
        Self::new((seconds / Self::SECONDS_PER_DAY) as u16,
                  seconds % Self::SECONDS_PER_DAY)
    }
    pub fn days(self) -> u16 { (self.0 & 31) as u16 }
    pub fn seconds_after_midnight(self) -> u32 { self.0 >> 5 }
    pub fn seconds(self) -> u32 {
        u32::from(self.days()) * Self::SECONDS_PER_DAY + self.seconds_after_midnight()
    }
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteFlag { BikesAllowed, CarsAllowed, WheelchairAccessible, ReservationNotRequired }
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Clasz {
    Air, HighSpeed, LongDistance, Coach, Night, RideSharing, Regional,
    Suburban, Subway, Tram, Bus, Ship, Odm, Funicular, AerialLift, Other
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocationType { GeneratedTrack, Track, Station }
/// A source-scoped GTFS stop-pair transfer constraint (v4).
/// Identifiers use compiled LocationIdx, never unqualified GTFS stop IDs.
/// A blocked rule applies to vehicle changes, not to physical pedestrian access.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct StationTransferRule {
    pub from: LocationIdx,
    pub to: LocationIdx,
    pub min_seconds: u32,
    /// 1: prohibited (GTFS transfer_type=3), 0: permitted.
    pub flags: u32,
}
impl StationTransferRule {
    pub fn prohibited(self) -> bool { self.flags & 1 != 0 }
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct Range { pub from: u32, pub to: u32 }
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct TripTransportRange {
    pub transport: TransportIdx,
    pub stop_from: u16,
    pub stop_to: u16,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Pod, Zeroable)]
pub struct Coordinate { pub lat: f32, pub lon: f32 }
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
pub struct StringRef { pub offset: u32, pub len: u32 }
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct SourceRecord {
    pub chateau: StringRef,
    pub onestop_feed_id: StringRef,
    pub attempt_id: StringRef,
}
/// Source-scoped original GTFS trip ID -> internal TripIdx; many IDs may
/// reference one internal trip. Includes the import attempt via SourceIdx.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct TripIdRecord {
    pub trip: TripIdx,
    pub id: StringRef,
    pub source: SourceIdx,
    pub reserved: u16,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct RouteIdRecord {
    pub id: StringRef,
    pub source: SourceIdx,
    pub reserved: u16,
}
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
pub struct Section { pub offset: u64, pub count: u64 }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatError {
    Magic, Version, Truncated, Alignment, Overflow, Index, Layout, Utf8, SourceOverflow,
}
impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for FormatError {}

// Only constructed from a successfully validated timetable. The header may
// be reused with the same immutable bytes without repeating the full scan.
#[derive(Clone, Copy)]
pub(crate) struct ValidatedTimetableHeader(TimetableHeader);

macro_rules! columns {
    ($($name:ident: $ty:ty,)*) => {
        #[repr(C)]
        #[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
        pub struct TimetableHeader {
            pub magic: u32, pub version: u32,
            pub first_utc_day: i32, pub day_count: u32,
            $(pub $name: Section,)*
        }
        #[derive(Default, Debug)]
        pub struct TimetableOwned {
            pub first_utc_day: i32, pub day_count: u32,
            $(pub $name: Vec<$ty>,)*
        }
        impl TimetableOwned {
            pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
                if self.day_count as usize > MAX_DAYS { return Err(FormatError::Layout); }
                let mut h = TimetableHeader {
                    magic: MAGIC, version: VERSION, first_utc_day: self.first_utc_day,
                    day_count: self.day_count, ..Default::default()
                };
                let mut b = vec![0u8; std::mem::size_of::<TimetableHeader>()];
                $(h.$name = push(&mut b, &self.$name)?;)*
                b[..std::mem::size_of::<TimetableHeader>()].copy_from_slice(bytemuck::bytes_of(&h));
                ZeroCopyTimetable::new(&b)?;
                Ok(b)
            }

            /// Write the V4 column format without allocating a second copy of
            /// the timetable. The caller must supply an empty, truncated output.
            pub fn write_to<W: std::io::Write + std::io::Seek>(
                &self,
                output: &mut W,
            ) -> anyhow::Result<u64> {
                if self.day_count as usize > MAX_DAYS {
                    return Err(FormatError::Layout.into());
                }
                output.seek(std::io::SeekFrom::Start(0))?;
                output.write_all(bytemuck::bytes_of(&TimetableHeader::default()))?;
                let mut h = TimetableHeader {
                    magic: MAGIC, version: VERSION, first_utc_day: self.first_utc_day,
                    day_count: self.day_count, ..Default::default()
                };
                $(h.$name = write_section(output, &self.$name)?;)*
                let len = output.stream_position()?;
                output.seek(std::io::SeekFrom::Start(0))?;
                output.write_all(bytemuck::bytes_of(&h))?;
                output.seek(std::io::SeekFrom::Start(len))?;
                Ok(len)
            }
        }
        #[derive(Clone, Copy)]
        pub struct ZeroCopyTimetable<'a> { b: &'a [u8], h: TimetableHeader }
        impl<'a> ZeroCopyTimetable<'a> {
            pub fn new(b: &'a [u8]) -> Result<Self, FormatError> {
                if b.len() < std::mem::size_of::<TimetableHeader>() {
                    return Err(FormatError::Truncated);
                }
                let h: TimetableHeader = bytemuck::pod_read_unaligned(
                    &b[..std::mem::size_of::<TimetableHeader>()]);
                if h.magic != MAGIC { return Err(FormatError::Magic); }
                if h.version != VERSION { return Err(FormatError::Version); }
                if h.day_count as usize > MAX_DAYS { return Err(FormatError::Layout); }
                $(view::<$ty>(b, h.$name)?;)*
                let tt = Self { b, h };
                tt.validate()?;
                Ok(tt)
            }
            pub fn header(&self) -> TimetableHeader { self.h }

            pub(crate) fn validated_header(&self) -> ValidatedTimetableHeader {
                ValidatedTimetableHeader(self.h)
            }

            /// Construct a view over the same immutable bytes validated at open.
            /// Only the mmap owner may reuse this header; never use it for
            /// arbitrary or replaced file contents.
            pub(crate) fn from_validated_header(
                bytes: &'a [u8],
                header: ValidatedTimetableHeader,
            ) -> Self {
                Self { b: bytes, h: header.0 }
            }

            $(pub fn $name(&self) -> &'a [$ty] {
                view::<$ty>(self.b, self.h.$name).expect("validated column")
            })*
        }
    }
}
fn push<T: Pod>(b: &mut Vec<u8>, a: &[T]) -> Result<Section, FormatError> {
    while b.len() % 8 != 0 { b.push(0); }
    let offset = u64::try_from(b.len()).map_err(|_| FormatError::Overflow)?;
    let count = u64::try_from(a.len()).map_err(|_| FormatError::Overflow)?;
    b.extend_from_slice(bytemuck::cast_slice(a));
    Ok(Section { offset, count })
}
/// Identical alignment and section order to `push`, but writes directly
/// to a seekable destination instead of allocating an output-sized Vec.
fn write_section<W: std::io::Write + std::io::Seek, T: Pod>(
    output: &mut W,
    values: &[T],
) -> anyhow::Result<Section> {
    let at = output.stream_position()?;
    let padding = ((8 - at % 8) % 8) as usize;
    output.write_all(&[0u8; 8][..padding])?;
    let offset = at.checked_add(padding as u64).ok_or(FormatError::Overflow)?;
    let count = u64::try_from(values.len()).map_err(|_| FormatError::Overflow)?;
    output.write_all(bytemuck::cast_slice(values))?;
    Ok(Section { offset, count })
}

fn view<'a, T: Pod>(b: &'a [u8], sec: Section) -> Result<&'a [T], FormatError> {
    let at = usize::try_from(sec.offset).map_err(|_| FormatError::Overflow)?;
    let n = usize::try_from(sec.count).map_err(|_| FormatError::Overflow)?
        .checked_mul(std::mem::size_of::<T>()).ok_or(FormatError::Overflow)?;
    let end = at.checked_add(n).ok_or(FormatError::Overflow)?;
    if at < std::mem::size_of::<TimetableHeader>() { return Err(FormatError::Layout); }
    if end > b.len() { return Err(FormatError::Truncated); }
    bytemuck::try_cast_slice(&b[at..end]).map_err(|_| FormatError::Alignment)
}

columns! {
    sources: SourceRecord,
    source_end_days: i32,
    coordinates: Coordinate,
    location_id: StringRef,
    location_source: SourceIdx,
    location_name: StringRef,
    location_parent: LocationIdx,
    location_type: u8,
    location_transfer_seconds: u32,
    station_transfer_rules: StationTransferRule,
    location_routes_offsets: u32,
    location_routes: RouteIdx,
    footpaths_out_offsets: u32,
    footpaths_out: PackedFootpath,
    footpaths_in_offsets: u32,
    footpaths_in: PackedFootpath,
    route_stop_offsets: u32,
    route_stops: PackedStop,
    route_transport_ranges: Range,
    // Profile events are [departure(0), arrival(1), departure(1), ...,
    // arrival(last)]. The first arrival and final departure are unused.
    timing_profile_offsets: u32,
    timing_profile_events: Delta,
    transport_timing_profile: TimingProfileIdx,
    route_clasz: u8,
    route_section_clasz_offsets: u32,
    route_section_clasz: u8,
    route_traffic_days: BitfieldIdx,
    route_flag_bikes: u64,
    route_flag_cars: u64,
    route_flag_wheelchair: u64,
    route_flag_reservation: u64,
    section_flag_offsets: u32,
    section_flag_bikes: u64,
    section_flag_cars: u64,
    section_flag_wheelchair: u64,
    section_flag_reservation: u64,
    transport_traffic_days: BitfieldIdx,
    transport_route: RouteIdx,
    transport_first_dep_offset: Delta,
    transport_initial_day_offset: u8,
    trip_transport_offsets: u32,
    trip_transport_ranges: TripTransportRange,
    transport_to_trip_section_offsets: u32,
    transport_to_trip_section: MergedTripsIdx,
    merged_trip_offsets: u32,
    merged_trips: TripIdx,
    trip_public_route_ids: RouteIdIdx,
    trip_direction_bits: u64,
    trip_ids: TripIdRecord,
    route_ids: RouteIdRecord,
    bitfields: TrafficDays,
    timezone_names: StringRef,
    string_pool: u8,
}

impl TimetableOwned {
    pub fn empty(first_utc_day: i32, day_count: u32) -> Self {
        let mut tt = Self { first_utc_day, day_count, ..Self::default() };
        tt.bitfields.push(TrafficDays::default()); // Nigiri empty bitfield #0
        tt.location_routes_offsets.push(0);
        tt.footpaths_out_offsets.push(0);
        tt.footpaths_in_offsets.push(0);
        tt.route_stop_offsets.push(0);
        tt.timing_profile_offsets.push(0);
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.trip_transport_offsets.push(0);
        tt.transport_to_trip_section_offsets.push(0);
        tt.merged_trip_offsets.push(0);
        tt
    }
    pub fn add_string(&mut self, value: &str) -> Result<StringRef, FormatError> {
        let offset = u32::try_from(self.string_pool.len()).map_err(|_| FormatError::Overflow)?;
        let len = u32::try_from(value.len()).map_err(|_| FormatError::Overflow)?;
        self.string_pool.extend_from_slice(value.as_bytes());
        Ok(StringRef { offset, len })
    }
    pub fn string(&self, value: StringRef) -> Option<&str> {
        get_string(&self.string_pool, value)
    }
    /// SourceIdx is intentionally a feed/import index, not an agency index.
    /// A château containing two feeds registers two distinct sources.
    /// Source ordering is determined by the future compiler's registration order.
    pub fn add_source(&mut self, chateau: &str, feed: &str, attempt: &str)
        -> Result<SourceIdx, FormatError>
    {
        for (i, s) in self.sources.iter().enumerate() {
            if self.string(s.chateau) == Some(chateau)
                && self.string(s.onestop_feed_id) == Some(feed)
                && self.string(s.attempt_id) == Some(attempt) {
                return Ok(SourceIdx(i as u16));
            }
        }
        let idx = u16::try_from(self.sources.len()).map_err(|_| FormatError::SourceOverflow)?;
        if idx == u16::MAX { return Err(FormatError::SourceOverflow); }
        let chateau = self.add_string(chateau)?;
        let onestop_feed_id = self.add_string(feed)?;
        let attempt_id = self.add_string(attempt)?;
        self.sources.push(SourceRecord { chateau, onestop_feed_id, attempt_id });
        self.source_end_days.push(0);
        Ok(SourceIdx(idx))
    }
    pub fn add_trip_id(&mut self, source: SourceIdx, trip: TripIdx, raw_id: &str)
        -> Result<TripIdIdx, FormatError>
    {
        if source.idx() >= self.sources.len() { return Err(FormatError::Index); }
        let idx = u32::try_from(self.trip_ids.len()).map_err(|_| FormatError::Overflow)?;
        let id = self.add_string(raw_id)?;
        self.trip_ids.push(TripIdRecord { trip, id, source, reserved: 0 });
        Ok(TripIdIdx(idx))
    }
    pub fn add_route_id(&mut self, source: SourceIdx, raw_id: &str)
        -> Result<RouteIdIdx, FormatError>
    {
        if source.idx() >= self.sources.len() { return Err(FormatError::Index); }
        let idx = u32::try_from(self.route_ids.len()).map_err(|_| FormatError::Overflow)?;
        let id = self.add_string(raw_id)?;
        self.route_ids.push(RouteIdRecord { id, source, reserved: 0 });
        Ok(RouteIdIdx(idx))
    }
}
fn get_string(pool: &[u8], r: StringRef) -> Option<&str> {
    let a = r.offset as usize;
    let b = a.checked_add(r.len as usize)?;
    std::str::from_utf8(pool.get(a..b)?).ok()
}
fn csr(o: &[u32], n: usize, rows: usize) -> Result<(), FormatError> {
    if o.len() != rows.checked_add(1).ok_or(FormatError::Overflow)?
        || o.first() != Some(&0) || o.last().copied().map(|x| x as usize) != Some(n)
        || o.windows(2).any(|w| w[0] > w[1])
    { return Err(FormatError::Layout); }
    Ok(())
}
impl ZeroCopyTimetable<'_> {
    /// Sparse, sorted stop-pair interchange constraints. Unlisted stop pairs
    /// inherit the timetable's per-location minimum transfer times.
    pub fn station_transfer_rule(&self, from: LocationIdx, to: LocationIdx)
        -> Option<StationTransferRule>
    {
        let rules = self.station_transfer_rules();
        rules.binary_search_by_key(&(from.0, to.0), |r| (r.from.0, r.to.0))
            .ok().map(|idx| rules[idx])
    }
}
impl ZeroCopyTimetable<'_> {
    fn validate(&self) -> Result<(), FormatError> {
        let n = self.coordinates().len();
        let r = self.route_transport_ranges().len();
        let t = self.transport_route().len();
        let trips = self.trip_transport_offsets().len().checked_sub(1)
            .ok_or(FormatError::Layout)?;
        let src = self.sources().len();
        if src >= u16::MAX as usize || self.source_end_days().len() != src
            || self.bitfields().first() != Some(&TrafficDays::default())
        { return Err(FormatError::Layout); }
        for len in [
            self.location_id().len(), self.location_source().len(),
            self.location_name().len(), self.location_parent().len(),
            self.location_type().len(), self.location_transfer_seconds().len()
        ] { if len != n { return Err(FormatError::Layout); } }
        let mut previous = None;
        for rule in self.station_transfer_rules() {
            if rule.from.idx() >= n || rule.to.idx() >= n || rule.flags > 1 {
                return Err(FormatError::Index);
            }
            let key = (rule.from.0, rule.to.0);
            if previous.is_some_and(|p| p >= key) {
                return Err(FormatError::Layout);
            }
            previous = Some(key);
        }
        csr(self.location_routes_offsets(),self.location_routes().len(),n)?;
        csr(self.footpaths_out_offsets(),self.footpaths_out().len(),n)?;
        csr(self.footpaths_in_offsets(),self.footpaths_in().len(),n)?;
        csr(self.route_stop_offsets(),self.route_stops().len(),r)?;
        csr(self.route_section_clasz_offsets(),self.route_section_clasz().len(),r)?;
        csr(self.section_flag_offsets(),self.route_section_clasz().len(),r)?;
        for len in [self.route_clasz().len(),
                    self.route_traffic_days().len()] {
            if len != r { return Err(FormatError::Layout); }
        }
        for len in [self.transport_traffic_days().len(),
                    self.transport_timing_profile().len(),
                    self.transport_first_dep_offset().len(),
                    self.transport_initial_day_offset().len()] {
            if len != t { return Err(FormatError::Layout); }
        }
        csr(self.trip_transport_offsets(),self.trip_transport_ranges().len(),trips)?;
        csr(self.transport_to_trip_section_offsets(),self.transport_to_trip_section().len(),t)?;
        if self.merged_trip_offsets().first() != Some(&0)
            || self.merged_trip_offsets().last().copied().map(|x| x as usize)
                != Some(self.merged_trips().len())
            || self.merged_trip_offsets().windows(2).any(|w| w[0] > w[1])
        { return Err(FormatError::Layout); }
        if self.trip_public_route_ids().len() != trips { return Err(FormatError::Layout); }
        let pool = self.string_pool();
        for entry in self.sources() {
            for strref in [entry.chateau,entry.onestop_feed_id,entry.attempt_id] {
                if get_string(pool,strref).is_none() { return Err(FormatError::Utf8); }
            }
        }
        for strref in self.location_id().iter().chain(self.location_name())
            .chain(self.timezone_names()) {
            if get_string(pool,*strref).is_none() { return Err(FormatError::Utf8); }
        }
        for s in self.location_source() {
            if s.idx() >= src { return Err(FormatError::Index); }
        }
        for x in self.trip_ids() {
            if x.source.idx() >= src || x.trip.idx() >= trips || x.reserved != 0
                || get_string(pool,x.id).is_none() { return Err(FormatError::Index); }
        }
        for x in self.route_ids() {
            if x.source.idx() >= src || x.reserved != 0
                || get_string(pool,x.id).is_none() { return Err(FormatError::Index); }
        }
        for id in self.trip_public_route_ids() {
            if id.idx() >= self.route_ids().len() { return Err(FormatError::Index); }
        }
        for id in self.transport_traffic_days().iter().chain(self.route_traffic_days()) {
            if id.idx() >= self.bitfields().len() { return Err(FormatError::Index); }
        }
        for id in self.transport_route().iter().chain(self.location_routes()) {
            if id.idx() >= r { return Err(FormatError::Index); }
        }
        for p in self.location_parent() {
            if *p != LocationIdx::INVALID && p.idx() >= n { return Err(FormatError::Index); }
        }
        for p in self.footpaths_out().iter().chain(self.footpaths_in()) {
            if p.target().idx() >= n || p.seconds() == 0 { return Err(FormatError::Index); }
        }
        let profiles = self.timing_profile_offsets().len().checked_sub(1)
            .ok_or(FormatError::Layout)?;
        csr(self.timing_profile_offsets(), self.timing_profile_events().len(), profiles)?;
        for time in self.timing_profile_events().iter().chain(self.transport_first_dep_offset()) {
            if time.seconds_after_midnight() >= Delta::SECONDS_PER_DAY {
                return Err(FormatError::Index);
            }
        }
        for p in self.route_stops() {
            if p.location().idx() >= n { return Err(FormatError::Index); }
        }
        for pair in self.trip_transport_ranges() {
            if pair.transport.idx() >= t || pair.stop_from > pair.stop_to {
                return Err(FormatError::Index);
            }
        }
        for trip in self.merged_trips() {
            if trip.idx() >= trips { return Err(FormatError::Index); }
        }
        for group in self.transport_to_trip_section() {
            if group.idx() >= self.merged_trip_offsets().len()-1 { return Err(FormatError::Index); }
        }
        for (i, rr) in self.route_transport_ranges().iter().enumerate() {
            if rr.from > rr.to || rr.to as usize > t {
                return Err(FormatError::Index);
            }
            let stops = (self.route_stop_offsets()[i+1]-self.route_stop_offsets()[i]) as usize;
            let events = stops.checked_sub(1).and_then(|n| n.checked_mul(2))
                .ok_or(FormatError::Overflow)?;
            for j in rr.from..rr.to {
                let transport = j as usize;
                if self.transport_route()[transport] != RouteIdx(i as u32) {
                    return Err(FormatError::Layout);
                }
                let profile = self.transport_timing_profile()[transport].idx();
                if profile >= profiles { return Err(FormatError::Index); }
                let offsets = self.timing_profile_offsets();
                if (offsets[profile + 1] - offsets[profile]) as usize != events {
                    return Err(FormatError::Layout);
                }
            }
        }
        Ok(())
    }
    pub fn source(&self, s: SourceIdx) -> Option<(&str,&str,&str)> {
        let r = self.sources().get(s.idx())?;
        Some((self.get_string(r.chateau)?, self.get_string(r.onestop_feed_id)?,
              self.get_string(r.attempt_id)?))
    }
    pub fn get_string(&self, r: StringRef) -> Option<&str> {
        get_string(self.string_pool(),r)
    }
    pub fn trip_by_source_id(&self, src: SourceIdx, id: &str) -> Option<TripIdx> {
        self.trip_ids().iter().find(|r| r.source == src && self.get_string(r.id) == Some(id))
            .map(|r| r.trip)
    }
    pub fn route_by_source_id(&self, src: SourceIdx, id: &str) -> Option<RouteIdIdx> {
        self.route_ids().iter().position(|r| r.source == src
            && self.get_string(r.id) == Some(id))
            .and_then(|x| u32::try_from(x).ok()).map(RouteIdIdx)
    }
    pub fn is_transport_active(&self, t: TransportIdx, day: DayIdx) -> bool {
        self.transport_traffic_days().get(t.idx()).and_then(|x| self.bitfields().get(x.idx()))
            .is_some_and(|x| x.test(day.idx()))
    }
    pub fn is_route_active(&self, r: RouteIdx, day: DayIdx) -> bool {
        self.route_traffic_days().get(r.idx()).and_then(|x| self.bitfields().get(x.idx()))
            .is_some_and(|x| x.test(day.idx()))
    }
    pub fn routes_at(&self, loc: LocationIdx) -> &[RouteIdx] {
        let i=loc.idx(); let o=self.location_routes_offsets();
        if i+1 >= o.len() { return &[]; }
        &self.location_routes()[o[i] as usize..o[i+1] as usize]
    }
    pub fn stops_on(&self, r: RouteIdx) -> &[PackedStop] {
        let i=r.idx(); let o=self.route_stop_offsets();
        if i+1 >= o.len() { return &[]; }
        &self.route_stops()[o[i] as usize..o[i+1] as usize]
    }
    pub fn footpaths_from(&self, loc: LocationIdx) -> &[PackedFootpath] {
        let i=loc.idx(); let o=self.footpaths_out_offsets();
        if i+1 >= o.len() { return &[]; }
        &self.footpaths_out()[o[i] as usize..o[i+1] as usize]
    }
    /// Shared, stop-order event offsets for one transport. Resolution is O(1)
    /// and the slice can be reused throughout an entire RAPTOR route scan.
    pub fn events_for_transport(&self, transport: TransportIdx) -> Option<&[Delta]> {
        let profile = self.transport_timing_profile().get(transport.idx())?.idx();
        let from = *self.timing_profile_offsets().get(profile)? as usize;
        let to = *self.timing_profile_offsets().get(profile + 1)? as usize;
        self.timing_profile_events().get(from..to)
    }

    pub fn event_time(&self, route: RouteIdx, transport: TransportIdx,
                      stop: usize, arrival: bool) -> Option<Delta> {
        let rr = *self.route_transport_ranges().get(route.idx())?;
        let n = self.stops_on(route).len();
        if transport.0 < rr.from || transport.0 >= rr.to ||
            (arrival && (stop == 0 || stop >= n)) ||
            (!arrival && stop.checked_add(1)? >= n) { return None; }
        let slot = if arrival { stop * 2 - 1 } else { stop * 2 };
        self.events_for_transport(transport)?.get(slot).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_shared_profile_preserves_second_exact_event_lookups() {
        let mut tt = TimetableOwned::empty(20_000, 1);
        tt.add_source("c", "f", "a").unwrap();
        for _ in 0..3 {
            tt.coordinates.push(Coordinate::default());
            tt.location_id.push(StringRef::default());
            tt.location_name.push(StringRef::default());
            tt.location_source.push(SourceIdx(0));
            tt.location_parent.push(LocationIdx::INVALID);
            tt.location_type.push(0);
            tt.location_transfer_seconds.push(120);
            tt.location_routes_offsets.push(0);
            tt.footpaths_out_offsets.push(0);
            tt.footpaths_in_offsets.push(0);
            tt.route_stops.push(PackedStop::new(LocationIdx(tt.route_stops.len() as u32), true, true, true, true).unwrap());
        }
        tt.route_stop_offsets.push(3);
        tt.route_transport_ranges.push(Range { from: 0, to: 2 });
        tt.route_clasz.push(Clasz::Bus as u8);
        tt.route_traffic_days.push(BitfieldIdx(0));
        tt.route_section_clasz_offsets.push(0);
        tt.section_flag_offsets.push(0);
        tt.timing_profile_events.extend([
            Delta::from_seconds(0).unwrap(),
            Delta::from_seconds(631).unwrap(),
            Delta::from_seconds(725).unwrap(),
            Delta::from_seconds(1409).unwrap(),
        ]);
        tt.timing_profile_offsets.push(4);
        tt.transport_route.extend([RouteIdx(0), RouteIdx(0)]);
        tt.transport_timing_profile.extend([TimingProfileIdx(0), TimingProfileIdx(0)]);
        tt.transport_traffic_days.extend([BitfieldIdx(0), BitfieldIdx(0)]);
        tt.transport_first_dep_offset.extend([
            Delta::from_seconds(28_800).unwrap(),
            Delta::from_seconds(29_700).unwrap(),
        ]);
        tt.transport_initial_day_offset.extend([0, 0]);
        tt.transport_to_trip_section_offsets.extend([0, 0]);
        let bytes = tt.encode().unwrap();
        let view = ZeroCopyTimetable::new(&bytes).unwrap();
        assert_eq!(view.events_for_transport(TransportIdx(0)), view.events_for_transport(TransportIdx(1)));
        assert_eq!(view.event_time(RouteIdx(0), TransportIdx(1), 1, true).unwrap().seconds(), 631);
        assert_eq!(view.event_time(RouteIdx(0), TransportIdx(0), 2, true).unwrap().seconds(), 1409);
        assert!(view.event_time(RouteIdx(0), TransportIdx(1), 2, false).is_none());
        let mut invalid = bytes.clone();
        invalid[4..8].copy_from_slice(&4u32.to_ne_bytes());
        assert!(matches!(ZeroCopyTimetable::new(&invalid), Err(FormatError::Version)));
    }
    #[test]
    fn second_precision_storage_layout() {
        assert_eq!(std::mem::size_of::<SourceIdx>(),2);
        assert_eq!(std::mem::size_of::<TrafficDays>(),64);
        assert_eq!(std::mem::size_of::<PackedStop>(),4);
        assert_eq!(std::mem::size_of::<PackedFootpath>(),8);
        assert_eq!(std::mem::size_of::<Delta>(),4);
        let p=PackedStop::new(LocationIdx(42),true,false,true,false).unwrap();
        assert_eq!(p.0,42|(1<<28)|(1<<30));
        assert_eq!(PackedFootpath::new(LocationIdx(7),9).unwrap().0,7|(9_u64<<32));
        assert_eq!(Delta::from_seconds(86_400 + 55).unwrap().seconds(),86_455);
        assert_eq!(Delta::from_seconds(86_400 + 55).unwrap().days(), 1);
        let mut b=TrafficDays::default();
        for n in [0,63,64,511] {b.set(n,true).unwrap();}
        for n in [1,64,65] {assert!(b.shift_left(1).test(n));}
        assert!(!b.shift_left(1).test(511));
    }
    #[test]
    fn distinct_chateaux_feeds_and_attempts_never_collide() {
        let mut tt=TimetableOwned::empty(20000,400);
        let a=tt.add_source("C1","F1","A1").unwrap();
        let b=tt.add_source("C2","F1","A1").unwrap();
        let c=tt.add_source("C1","F1","A2").unwrap();
        assert_eq!(tt.add_source("C1","F1","A1").unwrap(),a);
        assert_ne!(a,b);assert_ne!(a,c);
        tt.add_trip_id(a,TripIdx(0),"T1").unwrap();
        tt.add_trip_id(b,TripIdx(1),"T1").unwrap();
        tt.add_trip_id(c,TripIdx(2),"T1").unwrap();
        let a_route=tt.add_route_id(a,"R1").unwrap();
        let b_route=tt.add_route_id(b,"R1").unwrap();
        tt.trip_public_route_ids=vec![a_route,b_route,a_route];
        tt.trip_transport_offsets=vec![0,0,0,0];
        let binary=tt.encode().unwrap();
        let decoded=ZeroCopyTimetable::new(&binary).unwrap();
        assert_eq!(decoded.source(a),Some(("C1","F1","A1")));
        assert_eq!(decoded.trip_by_source_id(a,"T1"),Some(TripIdx(0)));
        assert_eq!(decoded.trip_by_source_id(b,"T1"),Some(TripIdx(1)));
        assert_eq!(decoded.trip_by_source_id(c,"T1"),Some(TripIdx(2)));
        assert_ne!(decoded.route_by_source_id(a,"R1"),decoded.route_by_source_id(b,"R1"));
    }

    #[test]
    fn packed_values_reject_out_of_range_indices_and_durations() {
        assert_eq!(TrafficDays::default().set(MAX_DAYS, true), Err(FormatError::Index));
        assert_eq!(
            PackedStop::new(LocationIdx(1 << 28), true, true, false, false),
            Err(FormatError::Index)
        );
        assert_eq!(PackedFootpath::new(LocationIdx(u32::MAX), 1), Err(FormatError::Index));
        assert_eq!(PackedFootpath::new(LocationIdx(1), 0), Err(FormatError::Index));
        assert_eq!(PackedFootpath::new(LocationIdx(1), 512).unwrap().seconds(), 512);
        assert_eq!(PackedFootpath::new(LocationIdx(1), 9_001).unwrap().seconds(), 9_001);
        assert_eq!(Delta::new(32, 0), Err(FormatError::Index));
        assert_eq!(Delta::from_seconds(32 * 86_400), Err(FormatError::Index));
        assert_eq!(Delta::new(0, 86_400), Err(FormatError::Index));
    }
    #[test]
    fn streamed_binary_matches_encoded_v4_bytes() {
        use std::io::Cursor;
        for day_count in [1, 35, 512] {
            let mut tt = TimetableOwned::empty(20_000, day_count);
            let a = tt.add_source("C1", "F1", "A1").unwrap();
            let b = tt.add_source("C2", "F2", "A2").unwrap();
            tt.add_route_id(a, "R1").unwrap();
            tt.add_route_id(b, "R2").unwrap();
            let expected = tt.encode().unwrap();
            let mut output = Cursor::new(Vec::new());
            let written = tt.write_to(&mut output).unwrap();
            assert_eq!(written as usize, expected.len());
            let bytes = output.into_inner();
            assert_eq!(bytes, expected);
            ZeroCopyTimetable::new(&bytes).unwrap();
        }
    }

    #[test]
    fn streaming_rejects_invalid_day_count_before_writing() {
        use std::io::Cursor;
        let mut tt = TimetableOwned::empty(0, 513);
        let mut output = Cursor::new(Vec::new());
        assert!(tt.write_to(&mut output).is_err());
        assert!(output.into_inner().is_empty());
        tt.day_count = 512;
        assert!(tt.write_to(&mut Cursor::new(Vec::new())).is_ok());
    }

    #[test]
    fn rejects_old_version_and_truncation() {
        let b=TimetableOwned::empty(20000,400).encode().unwrap();
        assert_eq!(ZeroCopyTimetable::new(&b).unwrap().header().version,VERSION);
        let mut older=b.clone();older[4..8].copy_from_slice(&1u32.to_ne_bytes());
        assert!(matches!(ZeroCopyTimetable::new(&older),Err(FormatError::Version)));
        let mut v2=b.clone(); v2[4..8].copy_from_slice(&2u32.to_ne_bytes());
        assert!(matches!(ZeroCopyTimetable::new(&v2),Err(FormatError::Version)));
        let mut v3=b.clone(); v3[4..8].copy_from_slice(&3u32.to_ne_bytes());
        assert!(matches!(ZeroCopyTimetable::new(&v3),Err(FormatError::Version)));
        let mut cut=b;cut.pop();
        assert!(ZeroCopyTimetable::new(&cut).is_err());
    }
}
