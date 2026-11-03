//! Lossless, snapshot-local trip descriptor columns. Dictionary builders are
//! discarded after construction; resident strings share a contiguous UTF-8 pool.
use super::OptionalColumn;
use ahash::AHashMap;
use gtfs_realtime::TripDescriptor;
use gtfs_realtime::trip_descriptor::ModifiedTripSelector;
use serde::{Deserialize, Serialize};

/// Zero is absent; other values are one-based dictionary references. The width
/// is selected once per column, including the absence sentinel in its range.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DescriptorIndices {
    Absent,
    Uniform(u32),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
}

impl DescriptorIndices {
    fn new(indices: Vec<u32>) -> Self {
        let max = indices.iter().copied().max().unwrap_or(0);
        if max == 0 {
            Self::Absent
        } else if indices.iter().all(|&id| id == indices[0]) {
            Self::Uniform(indices[0])
        } else if max <= u32::from(u8::MAX) {
            Self::U8(indices.into_iter().map(|id| id as u8).collect())
        } else if max <= u32::from(u16::MAX) {
            Self::U16(indices.into_iter().map(|id| id as u16).collect())
        } else {
            Self::U32(indices)
        }
    }

    fn get(&self, row: usize) -> Option<usize> {
        let id = match self {
            Self::Absent => return None,
            Self::Uniform(id) => *id,
            Self::U8(ids) => u32::from(*ids.get(row)?),
            Self::U16(ids) => u32::from(*ids.get(row)?),
            Self::U32(ids) => *ids.get(row)?,
        };
        id.checked_sub(1).map(|id| id as usize)
    }
}

/// Each distinct string occupies bytes plus one u32 end offset, rather than a
/// separate allocation and String header. Empty strings have equal offsets and
/// remain distinct from absent values. No date/time normalization is performed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DescriptorStrings {
    data: String,
    ends: Vec<u32>,
    pub indices: DescriptorIndices,
}

impl DescriptorStrings {
    fn new(values: Vec<Option<String>>) -> Self {
        let mut dictionary = AHashMap::new();
        let mut data = String::new();
        let mut ends = Vec::new();
        let mut indices = Vec::with_capacity(values.len());
        for value in values {
            let id = match value {
                None => 0,
                Some(value) => match dictionary.get(value.as_str()) {
                    Some(&id) => id,
                    None => {
                        let id = u32::try_from(ends.len() + 1)
                            .expect("too many descriptor strings");
                        data.push_str(&value);
                        ends.push(
                            u32::try_from(data.len())
                                .expect("descriptor string pool exceeds 4 GiB"),
                        );
                        dictionary.insert(value, id);
                        id
                    }
                },
            };
            indices.push(id);
        }
        data.shrink_to_fit();
        ends.shrink_to_fit();
        Self {
            data,
            ends,
            indices: DescriptorIndices::new(indices),
        }
    }

    fn get(&self, row: usize) -> Option<&str> {
        let id = self.indices.get(row)?;
        let begin = if id == 0 {
            0
        } else {
            *self.ends.get(id - 1)? as usize
        };
        self.data.get(begin..*self.ends.get(id)? as usize)
    }
}

/// Numeric dictionaries retain the complete protobuf integer domain, including
/// unknown enum values, while repeated direction/relationship values use small
/// indices and a uniform column needs no per-row storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DescriptorValues<T> {
    values: Vec<T>,
    pub indices: DescriptorIndices,
}

impl<T: Copy + Eq + std::hash::Hash> DescriptorValues<T> {
    fn new(rows: Vec<Option<T>>) -> Self {
        let mut dictionary = AHashMap::new();
        let mut values = Vec::new();
        let indices = rows
            .into_iter()
            .map(|value| match value {
                None => 0,
                Some(value) => *dictionary.entry(value).or_insert_with(|| {
                    values.push(value);
                    u32::try_from(values.len()).expect("too many descriptor values")
                }),
            })
            .collect();
        values.shrink_to_fit();
        Self {
            values,
            indices: DescriptorIndices::new(indices),
        }
    }

    fn get(&self, row: usize) -> Option<T> {
        self.values.get(self.indices.get(row)?).copied()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactTripDescriptorStore {
    pub count: u32,
    pub trip_id: DescriptorStrings,
    pub route_id: DescriptorStrings,
    pub direction_id: DescriptorValues<u32>,
    pub start_time: DescriptorStrings,
    pub start_date: DescriptorStrings,
    pub schedule_relationship: DescriptorValues<i32>,
    pub modified_trip: OptionalColumn<ModifiedTripSelector>,
}

/// A descriptor view borrows pooled strings without expanding the feed.
#[derive(Clone, Copy, Debug)]
pub struct TripDescriptorRef<'a> {
    pub trip_id: Option<&'a str>,
    pub route_id: Option<&'a str>,
    pub direction_id: Option<u32>,
    pub start_time: Option<&'a str>,
    pub start_date: Option<&'a str>,
    pub schedule_relationship: Option<i32>,
    pub modified_trip: Option<&'a ModifiedTripSelector>,
}

impl TripDescriptorRef<'_> {
    pub fn to_owned(self) -> TripDescriptor {
        TripDescriptor {
            trip_id: self.trip_id.map(str::to_owned),
            route_id: self.route_id.map(str::to_owned),
            direction_id: self.direction_id,
            start_time: self.start_time.map(str::to_owned),
            start_date: self.start_date.map(str::to_owned),
            schedule_relationship: self.schedule_relationship,
            modified_trip: self.modified_trip.cloned(),
        }
    }
}

impl CompactTripDescriptorStore {
    pub(super) fn new(descriptors: Vec<TripDescriptor>) -> Self {
        let count = u32::try_from(descriptors.len()).expect("too many trip descriptors");
        let mut trip_id = Vec::new();
        let mut route_id = Vec::new();
        let mut direction_id = Vec::new();
        let mut start_time = Vec::new();
        let mut start_date = Vec::new();
        let mut schedule_relationship = Vec::new();
        let mut modified_trip = Vec::new();
        for trip in descriptors {
            trip_id.push(trip.trip_id);
            route_id.push(trip.route_id);
            direction_id.push(trip.direction_id);
            start_time.push(trip.start_time);
            start_date.push(trip.start_date);
            schedule_relationship.push(trip.schedule_relationship);
            modified_trip.push(trip.modified_trip);
        }
        Self {
            count,
            trip_id: DescriptorStrings::new(trip_id),
            route_id: DescriptorStrings::new(route_id),
            direction_id: DescriptorValues::new(direction_id),
            start_time: DescriptorStrings::new(start_time),
            start_date: DescriptorStrings::new(start_date),
            schedule_relationship: DescriptorValues::new(schedule_relationship),
            modified_trip: OptionalColumn::from_values(modified_trip),
        }
    }

    pub fn get(&self, row: usize) -> Option<TripDescriptorRef<'_>> {
        if row >= self.count as usize {
            return None;
        }
        Some(TripDescriptorRef {
            trip_id: self.trip_id.get(row),
            route_id: self.route_id.get(row),
            direction_id: self.direction_id.get(row),
            start_time: self.start_time.get(row),
            start_date: self.start_date.get(row),
            schedule_relationship: self.schedule_relationship.get(row),
            modified_trip: self.modified_trip.get(row),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_round_trip_preserves_presence_and_unusual_values() {
        let trips = vec![
            TripDescriptor::default(),
            TripDescriptor {
                trip_id: Some(String::new()),
                route_id: Some("路線-A".into()),
                direction_id: Some(0),
                start_time: Some("25:01:02".into()),
                start_date: Some("20261103".into()),
                schedule_relationship: Some(0),
                modified_trip: Some(ModifiedTripSelector::default()),
            },
            TripDescriptor {
                trip_id: Some("trip-2".into()),
                route_id: Some("路線-A".into()),
                direction_id: Some(u32::MAX),
                start_time: Some("001:02:03".into()),
                start_date: Some(String::new()),
                schedule_relationship: Some(i32::MIN),
                modified_trip: None,
            },
        ];
        let store = CompactTripDescriptorStore::new(trips.clone());
        for (row, trip) in trips.iter().enumerate() {
            assert_eq!(store.get(row).unwrap().to_owned(), *trip);
        }
        assert!(store.get(trips.len()).is_none());
        assert_eq!(store.route_id.ends.len(), 1);
        let bytes = crate::bincode_serialize(&store).unwrap();
        let decoded: CompactTripDescriptorStore = crate::bincode_deserialize(&bytes).unwrap();
        for (row, trip) in trips.iter().enumerate() {
            assert_eq!(decoded.get(row).unwrap().to_owned(), *trip);
        }
    }

    #[test]
    fn shared_columns_and_absent_modified_trips_have_no_per_row_values() {
        let trip = TripDescriptor {
            route_id: Some("A".into()),
            start_date: Some("20261103".into()),
            ..Default::default()
        };
        let trips = vec![trip; 1000];
        let original_bytes = crate::bincode_serialize(&trips).unwrap().len();
        let store = CompactTripDescriptorStore::new(trips);
        assert!(crate::bincode_serialize(&store).unwrap().len() < original_bytes);
        assert!(matches!(store.route_id.indices, DescriptorIndices::Uniform(1)));
        assert!(matches!(store.start_date.indices, DescriptorIndices::Uniform(1)));
        assert!(matches!(store.trip_id.indices, DescriptorIndices::Absent));
        assert!(store.modified_trip.is_absent());
        assert_eq!(store.start_date.data, "20261103");
        assert!(store.get(1000).is_none());
    }

    #[test]
    fn rare_modified_trips_are_sparse_and_use_descriptor_row_indices() {
        let mut trips = vec![TripDescriptor::default(); 100];
        trips[73].modified_trip = Some(ModifiedTripSelector::default());
        let store = CompactTripDescriptorStore::new(trips.clone());
        assert!(matches!(
            &store.modified_trip,
            OptionalColumn::Sparse { indices, .. } if indices == &[73]
        ));
        for (row, trip) in trips.iter().enumerate() {
            assert_eq!(store.get(row).unwrap().to_owned(), *trip);
        }
    }

    #[test]
    fn index_width_accounts_for_absence_sentinel() {
        for max in [255, 256, 65535, 65536] {
            let indices = DescriptorIndices::new(vec![0, 1, max]);
            match max {
                255 => assert!(matches!(indices, DescriptorIndices::U8(_))),
                256 | 65535 => assert!(matches!(indices, DescriptorIndices::U16(_))),
                _ => assert!(matches!(indices, DescriptorIndices::U32(_))),
            }
            assert_eq!(indices.get(0), None);
            assert_eq!(indices.get(1), Some(0));
            assert_eq!(indices.get(2), Some((max - 1) as usize));
            assert_eq!(indices.get(3), None);
        }
    }

    #[test]
    fn mostly_unique_trip_ids_use_one_contiguous_string_pool() {
        let values: Vec<_> = (0..300).map(|i| Some(format!("trip-{i}"))).collect();
        let strings = DescriptorStrings::new(values.clone());
        assert!(matches!(strings.indices, DescriptorIndices::U16(_)));
        assert_eq!(strings.ends.len(), 300);
        for (row, value) in values.iter().enumerate() {
            assert_eq!(strings.get(row), value.as_deref());
        }
    }
}
