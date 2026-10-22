//! Reverse indexes for result metadata, built once when a timetable is loaded.
//!
//! The timetable keeps source-scoped trip IDs and many-to-many trip/transport
//! relations. Reconstructing these relations for every returned ride requires
//! scanning all trips; these compact indexes instead use four bytes per entry.
//! Where several trips share a transport (or several IDs name a trip), the first
//! association in timetable order matches the previous metadata lookup.

use crate::routing_common::timetable::{TripIdRecord, TripTransportRange, ZeroCopyTimetable};
use anyhow::{Context, Result};

const MISSING: u32 = u32::MAX;

pub struct MetadataIndex {
    transport_to_trip: Vec<u32>,
    trip_to_id_record: Vec<u32>,
}

impl MetadataIndex {
    pub fn build(tt: &ZeroCopyTimetable<'_>) -> Result<Self> {
        Self::from_columns(
            tt.transport_route().len(),
            tt.trip_transport_offsets(),
            tt.trip_transport_ranges(),
            tt.trip_public_route_ids().len(),
            tt.trip_ids(),
        )
    }

    fn from_columns(
        transport_count: usize,
        trip_offsets: &[u32],
        trip_ranges: &[TripTransportRange],
        trip_count: usize,
        trip_ids: &[TripIdRecord],
    ) -> Result<Self> {
        let mut transport_to_trip = vec![MISSING; transport_count];
        for (trip, offsets) in trip_offsets.windows(2).enumerate() {
            let trip = u32::try_from(trip).context("trip index exceeds u32")?;
            for entry in &trip_ranges[offsets[0] as usize..offsets[1] as usize] {
                let slot = &mut transport_to_trip[entry.transport.idx()];
                if *slot == MISSING {
                    *slot = trip;
                }
            }
        }

        let mut trip_to_id_record = vec![MISSING; trip_count];
        for (record, id) in trip_ids.iter().enumerate() {
            let slot = &mut trip_to_id_record[id.trip.idx()];
            if *slot == MISSING {
                *slot = u32::try_from(record).context("trip ID record index exceeds u32")?;
            }
        }

        Ok(Self {
            transport_to_trip,
            trip_to_id_record,
        })
    }

    pub fn trip_for_transport(&self, transport: u32) -> Option<usize> {
        self.transport_to_trip
            .get(transport as usize)
            .copied()
            .filter(|&idx| idx != MISSING)
            .map(|idx| idx as usize)
    }

    pub fn id_record_for_trip(&self, trip: usize) -> Option<usize> {
        self.trip_to_id_record
            .get(trip)
            .copied()
            .filter(|&idx| idx != MISSING)
            .map(|idx| idx as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::{SourceIdx, StringRef, TransportIdx, TripIdx};

    fn range(transport: u32) -> TripTransportRange {
        TripTransportRange {
            transport: TransportIdx(transport),
            stop_from: 0,
            stop_to: 2,
        }
    }

    fn id(trip: u32) -> TripIdRecord {
        TripIdRecord {
            trip: TripIdx(trip),
            id: StringRef::default(),
            source: SourceIdx(0),
            reserved: 0,
        }
    }

    #[test]
    fn indexes_preserve_first_association_in_timetable_order() {
        // Trip zero and trip one both reference transport 1. The old linear
        // search selected trip zero, regardless of which ID was requested.
        let index = MetadataIndex::from_columns(
            3,
            &[0, 2, 4],
            &[range(0), range(1), range(1), range(2)],
            2,
            &[id(1), id(0), id(0)],
        )
        .unwrap();
        assert_eq!(index.trip_for_transport(0), Some(0));
        assert_eq!(index.trip_for_transport(1), Some(0));
        assert_eq!(index.trip_for_transport(2), Some(1));
        assert_eq!(index.trip_for_transport(3), None);
        assert_eq!(index.id_record_for_trip(0), Some(1));
        assert_eq!(index.id_record_for_trip(1), Some(0));
        assert_eq!(index.id_record_for_trip(2), None);
    }

    #[test]
    fn missing_transport_and_missing_id_remain_unmapped() {
        let index = MetadataIndex::from_columns(3, &[0, 1, 1], &[range(1)], 2, &[id(1)]).unwrap();
        assert_eq!(index.trip_for_transport(0), None);
        assert_eq!(index.trip_for_transport(1), Some(0));
        assert_eq!(index.trip_for_transport(2), None);
        assert_eq!(index.id_record_for_trip(0), None);
        assert_eq!(index.id_record_for_trip(1), Some(0));
    }
}
