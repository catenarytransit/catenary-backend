//! Snapshot-local columns. Builders are discarded after freezing a snapshot.
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Column<T> {
    Absent,
    All(Box<[T]>),
    Sparse {
        indices: Box<[u32]>,
        values: Box<[T]>,
    },
    Bitmap {
        bits: Box<[u64]>,
        ranks: Box<[u32]>,
        values: Box<[T]>,
    },
    Dense(Box<[Option<T>]>),
}

impl<T> Column<T> {
    pub fn new(rows: Vec<Option<T>>) -> Self {
        let count = rows.iter().filter(|v| v.is_some()).count();
        if count == 0 {
            return Self::Absent;
        }
        if count == rows.len() {
            return Self::All(rows.into_iter().flatten().collect());
        }
        let sparse = count.saturating_mul(size_of::<u32>() + size_of::<T>());
        let bitmap = rows
            .len()
            .div_ceil(64)
            .saturating_mul(12)
            .saturating_add(count.saturating_mul(size_of::<T>()));
        let dense = rows.len().saturating_mul(size_of::<Option<T>>());
        if dense <= sparse.min(bitmap) {
            return Self::Dense(rows.into_boxed_slice());
        }
        if sparse <= bitmap {
            let mut indices = Vec::with_capacity(count);
            let mut values = Vec::with_capacity(count);
            for (i, value) in rows.into_iter().enumerate() {
                if let Some(value) = value {
                    indices.push(u32::try_from(i).expect("too many Aspen rows"));
                    values.push(value);
                }
            }
            return Self::Sparse {
                indices: indices.into_boxed_slice(),
                values: values.into_boxed_slice(),
            };
        }
        let mut bits = vec![0_u64; rows.len().div_ceil(64)];
        let mut values = Vec::with_capacity(count);
        for (i, value) in rows.into_iter().enumerate() {
            if let Some(value) = value {
                bits[i / 64] |= 1_u64 << (i % 64);
                values.push(value);
            }
        }
        let mut rank = 0_u32;
        let ranks = bits
            .iter()
            .map(|word| {
                let before = rank;
                rank = rank
                    .checked_add(word.count_ones())
                    .expect("too many Aspen values");
                before
            })
            .collect();
        Self::Bitmap {
            bits: bits.into_boxed_slice(),
            ranks,
            values: values.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<&T> {
        match self {
            Self::Absent => None,
            Self::All(values) => values.get(row),
            Self::Dense(values) => values.get(row)?.as_ref(),
            Self::Sparse { indices, values } => {
                let row = u32::try_from(row).ok()?;
                values.get(indices.binary_search(&row).ok()?)
            }
            Self::Bitmap {
                bits,
                ranks,
                values,
            } => {
                let word = *bits.get(row / 64)?;
                let bit = 1_u64 << (row % 64);
                if word & bit == 0 {
                    return None;
                }
                let index = ranks
                    .get(row / 64)?
                    .checked_add((word & (bit - 1)).count_ones())?;
                values.get(index as usize)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Indices {
    Uniform(u32),
    U8(Box<[u8]>),
    U16(Box<[u16]>),
    U32(Box<[u32]>),
}

impl Indices {
    pub fn new(rows: Vec<u32>) -> Self {
        let max = rows.iter().copied().max().unwrap_or(0);
        if rows.iter().all(|&id| id == max) {
            return Self::Uniform(max);
        }
        if u8::try_from(max).is_ok() {
            Self::U8(rows.into_iter().map(|id| id as u8).collect())
        } else if u16::try_from(max).is_ok() {
            Self::U16(rows.into_iter().map(|id| id as u16).collect())
        } else {
            Self::U32(rows.into_boxed_slice())
        }
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<u32> {
        Some(match self {
            Self::Uniform(id) => *id,
            Self::U8(ids) => u32::from(*ids.get(row)?),
            Self::U16(ids) => u32::from(*ids.get(row)?),
            Self::U32(ids) => *ids.get(row)?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Values<T> {
    values: Box<[T]>,
    indices: Indices,
}

impl<T: Copy + Eq + std::hash::Hash> Values<T> {
    pub fn new(rows: Vec<Option<T>>) -> Self {
        let mut dictionary = AHashMap::new();
        let mut values = Vec::new();
        let indices = rows
            .into_iter()
            .map(|value| match value {
                None => 0,
                Some(value) => *dictionary.entry(value).or_insert_with(|| {
                    values.push(value);
                    u32::try_from(values.len()).expect("too many Aspen values")
                }),
            })
            .collect();
        Self {
            values: values.into_boxed_slice(),
            indices: Indices::new(indices),
        }
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<T> {
        self.values
            .get(self.indices.get(row)?.checked_sub(1)? as usize)
            .copied()
    }
}

/// One allocation for UTF-8 and one for offsets, with zero reserved for None.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct Strings {
    bytes: String,
    ends: Box<[u32]>,
}

impl Strings {
    #[inline]
    pub fn get(&self, id: u32) -> Option<&str> {
        let index = id.checked_sub(1)? as usize;
        let start = if index == 0 {
            0
        } else {
            *self.ends.get(index - 1)? as usize
        };
        self.bytes.get(start..*self.ends.get(index)? as usize)
    }
}

#[derive(Default)]
pub(super) struct StringBuilder {
    ids: AHashMap<String, u32>,
    bytes: String,
    ends: Vec<u32>,
}

impl StringBuilder {
    pub fn insert(&mut self, value: Option<&str>) -> u32 {
        let Some(value) = value else {
            return 0;
        };
        if let Some(&id) = self.ids.get(value) {
            return id;
        }
        let id = u32::try_from(self.ends.len() + 1).expect("too many Aspen strings");
        self.bytes.push_str(value);
        self.ends
            .push(u32::try_from(self.bytes.len()).expect("Aspen string pool exceeds 4 GiB"));
        self.ids.insert(value.to_owned(), id);
        id
    }

    pub fn finish(mut self) -> Strings {
        self.bytes.shrink_to_fit();
        Strings {
            bytes: self.bytes,
            ends: self.ends.into_boxed_slice(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Delays {
    I16(Column<i16>),
    I24(Column<i24::I24>),
    I32(Column<i32>),
}

impl Delays {
    pub fn new(rows: Vec<Option<i32>>) -> Self {
        if rows.iter().flatten().all(|&v| i16::try_from(v).is_ok()) {
            Self::I16(Column::new(
                rows.into_iter().map(|v| v.map(|v| v as i16)).collect(),
            ))
        } else if rows
            .iter()
            .flatten()
            .all(|v| (-8_388_608..=8_388_607).contains(v))
        {
            Self::I24(Column::new(
                rows.into_iter()
                    .map(|v| v.map(i24::I24::wrapping_from_i32))
                    .collect(),
            ))
        } else {
            Self::I32(Column::new(rows))
        }
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<i32> {
        match self {
            Self::I16(c) => c.get(row).copied().map(i32::from),
            Self::I24(c) => c.get(row).copied().map(i32::from),
            Self::I32(c) => c.get(row).copied(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Times {
    Relative { base: i64, offsets: Column<i32> },
    Absolute(Column<i64>),
}

impl Times {
    pub fn new(rows: Vec<Option<i64>>) -> Self {
        let base = rows.iter().flatten().copied().min().unwrap_or(0);
        if rows.iter().flatten().all(|&v| {
            v.checked_sub(base)
                .and_then(|v| i32::try_from(v).ok())
                .is_some()
        }) {
            Self::Relative {
                base,
                offsets: Column::new(
                    rows.into_iter()
                        .map(|v| v.map(|v| (v - base) as i32))
                        .collect(),
                ),
            }
        } else {
            Self::Absolute(Column::new(rows))
        }
    }

    #[inline]
    pub fn get(&self, row: usize) -> Option<i64> {
        match self {
            Self::Relative { base, offsets } => base.checked_add(i64::from(*offsets.get(row)?)),
            Self::Absolute(c) => c.get(row).copied(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_encodings_preserve_presence_and_word_boundaries() {
        for step in [1, 2, 7, 1000] {
            let rows: Vec<_> = (0..1025)
                .map(|i| (i % step == 0).then_some(i as i64))
                .collect();
            let column = Column::new(rows.clone());
            for (i, value) in rows.iter().enumerate() {
                assert_eq!(column.get(i), value.as_ref());
            }
            assert!(column.get(rows.len()).is_none());
            if step == 2 {
                assert!(matches!(column, Column::Bitmap { .. }));
            }
            if step == 1000 {
                assert!(matches!(column, Column::Sparse { .. }));
            }
        }
        let dense = Column::new(vec![Some(0_u8), None, Some(255)]);
        assert!(matches!(dense, Column::Dense(_)));
        assert_eq!(dense.get(0), Some(&0));
        assert!(dense.get(1).is_none());
        assert!(matches!(Column::<u8>::new(vec![None; 100]), Column::Absent));
    }

    #[test]
    fn numeric_widths_never_truncate() {
        for value in [
            i32::MIN,
            -8_388_609,
            -8_388_608,
            -32769,
            -32768,
            0,
            32767,
            32768,
            8_388_607,
            8_388_608,
            i32::MAX,
        ] {
            let column = Delays::new(vec![None, Some(value)]);
            assert_eq!(column.get(0), None);
            assert_eq!(column.get(1), Some(value));
        }
        for values in [
            vec![None, Some(0), Some(1_793_700_000)],
            vec![Some(i64::MIN), Some(i64::MAX)],
            vec![Some(i64::MAX - 1), Some(i64::MAX)],
        ] {
            let column = Times::new(values.clone());
            for (i, value) in values.into_iter().enumerate() {
                assert_eq!(column.get(i), value);
            }
        }
    }

    #[test]
    fn strings_and_index_widths_preserve_empty_unicode_and_absence() {
        let mut builder = StringBuilder::default();
        let empty = builder.insert(Some(""));
        let unicode = builder.insert(Some("路線-A"));
        assert_eq!(builder.insert(Some("路線-A")), unicode);
        let pool = builder.finish();
        assert_eq!(pool.get(0), None);
        assert_eq!(pool.get(empty), Some(""));
        assert_eq!(pool.get(unicode), Some("路線-A"));
        for max in [255, 256, 65535, 65536] {
            let ids = Indices::new(vec![0, max]);
            assert_eq!(ids.get(0), Some(0));
            assert_eq!(ids.get(1), Some(max));
            assert_eq!(ids.get(2), None);
        }
    }
}
