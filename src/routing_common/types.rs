use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Index, Range};

macro_rules! strong_index {
    ($name:ident, $inner:ty) => {
        #[repr(transparent)]
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Default,
            Serialize,
            Deserialize,
            bytemuck::Pod,
            bytemuck::Zeroable,
        )]
        pub struct $name(pub $inner);

        impl $name {
            pub const INVALID: Self = Self(<$inner>::MAX);

            pub fn is_valid(self) -> bool {
                self != Self::INVALID
            }

            pub fn idx(self) -> usize {
                self.0 as usize
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<usize> for $name {
            fn from(v: usize) -> Self {
                Self(v as $inner)
            }
        }
    };
}

strong_index!(WayIdx, u32);
strong_index!(NodeIdx, u32);
strong_index!(OsmWayIdx, u64);
strong_index!(OsmNodeIdx, u64);
strong_index!(ComponentIdx, u32);

pub type CostT = u32;
pub type DistanceT = u32;
pub const INFEASIBLE: CostT = CostT::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Forward,
    Backward,
}

impl Direction {
    pub fn opposite(self) -> Self {
        match self {
            Direction::Forward => Direction::Backward,
            Direction::Backward => Direction::Forward,
        }
    }

    pub fn flip(self, search_dir: Direction) -> Self {
        if search_dir == Direction::Forward {
            self
        } else {
            self.opposite()
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Forward => write!(f, "forward"),
            Direction::Backward => write!(f, "backward"),
        }
    }
}

// Level: compact encoding of floor levels (-8.0 to 7.5 in 0.25 steps)
// Matches OSR's level_t: value 0 = no level, 1..N = encoded level
const MIN_LEVEL: f32 = -8.0;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Level(pub u8);

impl Level {
    pub const NO_LEVEL: Self = Self(0);

    pub fn from_float(f: f32) -> Self {
        Self(((f - MIN_LEVEL) / 0.25) as u8 + 1)
    }

    pub fn to_float(self) -> f32 {
        if self.0 == 0 {
            0.0
        } else {
            MIN_LEVEL + ((self.0 - 1) as f32 / 4.0)
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            write!(f, "-")
        } else {
            write!(f, "{}", self.to_float())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SpeedLimit {
    Kmh10 = 0,
    Kmh20 = 1,
    Kmh30 = 2,
    Kmh50 = 3,
    Kmh60 = 4,
    Kmh80 = 5,
    Kmh100 = 6,
    Kmh120 = 7,
}

impl SpeedLimit {
    pub fn from_raw(v: u8) -> Self {
        match v {
            0 => Self::Kmh10,
            1 => Self::Kmh20,
            2 => Self::Kmh30,
            3 => Self::Kmh50,
            4 => Self::Kmh60,
            5 => Self::Kmh80,
            6 => Self::Kmh100,
            7 => Self::Kmh120,
            _ => Self::Kmh50,
        }
    }

    pub fn from_kmh(x: u16) -> Self {
        if x >= 120 {
            Self::Kmh120
        } else if x >= 100 {
            Self::Kmh100
        } else if x >= 80 {
            Self::Kmh80
        } else if x >= 60 {
            Self::Kmh60
        } else if x >= 50 {
            Self::Kmh50
        } else if x >= 30 {
            Self::Kmh30
        } else if x >= 20 {
            Self::Kmh20
        } else {
            Self::Kmh10
        }
    }

    pub fn to_kmh(self) -> u16 {
        match self {
            Self::Kmh10 => 10,
            Self::Kmh20 => 20,
            Self::Kmh30 => 30,
            Self::Kmh50 => 50,
            Self::Kmh60 => 60,
            Self::Kmh80 => 80,
            Self::Kmh100 => 100,
            Self::Kmh120 => 120,
        }
    }

    pub fn to_seconds_per_meter(self) -> f32 {
        3.6 / self.to_kmh() as f32
    }
}

/// Compressed Sparse Row container mirroring OSR's cista::vecvec.
///
/// Stores variable-length sublists contiguously. `bucket_starts[k]` marks
/// where the sublist for key `k` begins in `data`, and `bucket_starts[k+1]`
/// marks where it ends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VecVec<V: Clone> {
    pub bucket_starts: Vec<u64>,
    pub data: Vec<V>,
}

impl<V: Clone> VecVec<V> {
    pub fn new() -> Self {
        Self {
            bucket_starts: vec![0],
            data: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.bucket_starts.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, key: usize) -> &[V] {
        let start = self.bucket_starts[key] as usize;
        let end = self.bucket_starts[key + 1] as usize;
        &self.data[start..end]
    }

    pub fn get_range(&self, key: usize) -> Range<usize> {
        let start = self.bucket_starts[key] as usize;
        let end = self.bucket_starts[key + 1] as usize;
        start..end
    }
}

impl<V: Clone> Default for VecVec<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone> Index<usize> for VecVec<V> {
    type Output = [V];
    fn index(&self, key: usize) -> &Self::Output {
        self.get(key)
    }
}

pub struct MappedVecVec<'a, T> {
    pub bucket_starts: &'a [u64],
    pub data: &'a [T],
}

impl<'a, T> MappedVecVec<'a, T> {
    pub fn len(&self) -> usize {
        self.bucket_starts.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, key: usize) -> &'a [T] {
        let start = self.bucket_starts[key] as usize;
        let end = self.bucket_starts[key + 1] as usize;
        &self.data[start..end]
    }

    pub fn get_range(&self, key: usize) -> Range<usize> {
        let start = self.bucket_starts[key] as usize;
        let end = self.bucket_starts[key + 1] as usize;
        start..end
    }
}

impl<'a, T> std::ops::Index<usize> for MappedVecVec<'a, T> {
    type Output = [T];
    fn index(&self, key: usize) -> &Self::Output {
        self.get(key)
    }
}

/// Builder for constructing a VecVec from unsorted data.
pub struct VecVecBuilder<V: Clone> {
    buckets: Vec<Vec<V>>,
}

impl<V: Clone> VecVecBuilder<V> {
    pub fn new(num_keys: usize) -> Self {
        Self {
            buckets: vec![Vec::new(); num_keys],
        }
    }

    pub fn push(&mut self, key: usize, value: V) {
        self.buckets[key].push(value);
    }

    pub fn extend(&mut self, key: usize, values: impl IntoIterator<Item = V>) {
        self.buckets[key].extend(values);
    }

    pub fn build(self) -> VecVec<V> {
        let mut bucket_starts = Vec::with_capacity(self.buckets.len() + 1);
        let total: usize = self.buckets.iter().map(|b| b.len()).sum();
        let mut data = Vec::with_capacity(total);
        let mut offset = 0u64;
        for bucket in self.buckets {
            bucket_starts.push(offset);
            offset += bucket.len() as u64;
            data.extend(bucket);
        }
        bucket_starts.push(offset);
        VecVec {
            bucket_starts,
            data,
        }
    }
}

#[repr(C)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Default,
    Serialize,
    Deserialize,
    bytemuck::Pod,
    bytemuck::Zeroable,
)]
pub struct Point {
    pub lat_e7: i32,
    pub lng_e7: i32,
}

impl Point {
    pub fn from_latlng(lat: f64, lng: f64) -> Self {
        Self {
            lat_e7: (lat * 1e7) as i32,
            lng_e7: (lng * 1e7) as i32,
        }
    }

    pub fn lat(&self) -> f64 {
        self.lat_e7 as f64 / 1e7
    }

    pub fn lng(&self) -> f64 {
        self.lng_e7 as f64 / 1e7
    }

    pub fn haversine_distance(&self, other: &Point) -> f64 {
        let lat1 = self.lat().to_radians();
        let lat2 = other.lat().to_radians();
        let dlat = (other.lat() - self.lat()).to_radians();
        let dlng = (other.lng() - self.lng()).to_radians();

        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlng / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();
        6_371_000.0 * c
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.lat(), self.lng())
    }
}
