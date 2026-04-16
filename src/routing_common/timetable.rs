use bytemuck::{Pod, Zeroable};

/// Identifies a unique transit location or platform
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable)]
pub struct LocationIdx(pub u32);

/// Identifies a vehicle trip
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable)]
pub struct TripIdx(pub u32);

/// Identifies a route sequence
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable)]
pub struct RouteIdx(pub u32);

/// Offset referencing string pool items
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct StringOffset {
    pub start: u32,
    pub length: u32,
}

/// Describes the bounding offsets for footpaths departing a specific stop
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct StopFootpaths {
    pub offset: u32,
    pub count: u32,
}

/// Describes a transit route (a sequence of stops visited by a set of trips)
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct RouteData {
    pub stops_offset: u32, // Offset into route_stops array
    pub stops_count: u32,
    pub trips_count: u32,
    pub times_offset: u32, // Offset into route_stop_times array
}

/// A pre-computed footpath from `Avens` routing used in `RAPTOR` bounds.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct Footpath {
    pub target: LocationIdx,
    pub duration_seconds: u16,
    pub padding: u16, // ensures 4-byte total size (u32 align)
}

/// A stop coordinate for the R-Tree search
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct Coordinate {
    pub lat: f32,
    pub lon: f32,
}

#[derive(Clone, Copy, Debug)]
pub struct PointObj {
    pub idx: u32,
    pub point: [f64; 2],
}

impl rstar::PointDistance for PointObj {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.point[0] - point[0];
        let dy = self.point[1] - point[1];
        dx * dx + dy * dy
    }
}

impl rstar::RTreeObject for PointObj {
    type Envelope = rstar::AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        rstar::AABB::from_point(self.point)
    }
}

/// Timetable Header - The root of the Memory Mapped file, containing
/// the byte offsets for all tightly packed flat slices.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct TimetableHeader {
    pub magic: u32,
    pub version: u32,

    // Arrays offsets (each tuple is [byte_offset, count])
    pub location_coords_offset: u32,
    pub location_coords_count: u32,

    pub string_pool_offset: u32,
    pub string_pool_length: u32,

    pub footpath_out_offset: u32,
    pub footpath_out_count: u32,

    pub routes_offset: u32,
    pub routes_count: u32,

    pub route_stops_offset: u32,
    pub route_stops_count: u32,

    pub route_stop_times_offset: u32,
    pub route_stop_times_count: u32,

    pub stop_footpaths_offset: u32,
    pub stop_footpaths_count: u32,

    pub route_id_pool_offset: u32,
    pub route_id_pool_count: u32,

    pub trip_id_pool_offset: u32,
    pub trip_id_pool_count: u32,
}

/// Wraps an underlying mapped memory slice to provide safe zero-copy typed slice views
pub struct ZeroCopyTimetable<'a> {
    pub data: &'a [u8],
}

impl<'a> ZeroCopyTimetable<'a> {
    pub fn new(data: &'a [u8]) -> Option<Self> {
        if data.len() < std::mem::size_of::<TimetableHeader>() {
            return None;
        }
        Some(Self { data })
    }

    pub fn header(&self) -> &TimetableHeader {
        bytemuck::from_bytes(&self.data[0..std::mem::size_of::<TimetableHeader>()])
    }

    pub fn coordinates(&self) -> &[Coordinate] {
        let max_len = self.data.len();
        let header = self.header();
        let start = header.location_coords_offset as usize;
        let end = start + header.location_coords_count as usize * std::mem::size_of::<Coordinate>();
        if start > max_len || end > max_len {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn footpaths_out(&self) -> &[Footpath] {
        let max_len = self.data.len();
        let header = self.header();
        let start = header.footpath_out_offset as usize;
        let end = start + header.footpath_out_count as usize * std::mem::size_of::<Footpath>();
        if start > max_len || end > max_len {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn routes(&self) -> &[RouteData] {
        let header = self.header();
        let start = header.routes_offset as usize;
        let end = start + header.routes_count as usize * std::mem::size_of::<RouteData>();
        if start > self.data.len() || end > self.data.len() {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn route_stops(&self) -> &[LocationIdx] {
        let header = self.header();
        let start = header.route_stops_offset as usize;
        let end = start + header.route_stops_count as usize * std::mem::size_of::<LocationIdx>();
        if start > self.data.len() || end > self.data.len() {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn route_stop_times(&self) -> &[u32] {
        let header = self.header();
        let start = header.route_stop_times_offset as usize;
        let end = start + header.route_stop_times_count as usize * std::mem::size_of::<u32>();
        if start > self.data.len() || end > self.data.len() {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn stop_footpaths(&self) -> &[StopFootpaths] {
        let header = self.header();
        let start = header.stop_footpaths_offset as usize;
        let end = start + header.stop_footpaths_count as usize * std::mem::size_of::<StopFootpaths>();
        if start > self.data.len() || end > self.data.len() {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn route_id_offsets(&self) -> &[StringOffset] {
        let header = self.header();
        let start = header.route_id_pool_offset as usize;
        let end = start + header.route_id_pool_count as usize * std::mem::size_of::<StringOffset>();
        if start > self.data.len() || end > self.data.len() {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn trip_id_offsets(&self) -> &[StringOffset] {
        let header = self.header();
        let start = header.trip_id_pool_offset as usize;
        let end = start + header.trip_id_pool_count as usize * std::mem::size_of::<StringOffset>();
        if start > self.data.len() || end > self.data.len() {
            return &[];
        }
        bytemuck::cast_slice(&self.data[start..end])
    }

    pub fn get_string(&self, offset: &StringOffset) -> Option<&'a str> {
        let pool_start = self.header().string_pool_offset as usize;
        let p_start = pool_start + offset.start as usize;
        let p_end = p_start + offset.length as usize;

        if p_end > self.data.len() {
            return None;
        }

        std::str::from_utf8(&self.data[p_start..p_end]).ok()
    }
}
