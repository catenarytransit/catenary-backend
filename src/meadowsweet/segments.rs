//! Disk-backed logical OSM-station segments over verified physical railway walks.
//!
//! This module NEVER infers the arrival station by geometric proximity. A
//! station anchor must be confirmed by a topology-aware map matcher. Both OSM
//! type and OSM ID are preserved to prevent node/way/relation collisions.
use anyhow::{Result, bail};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

pub const SEGMENT_RECORD_BYTES: usize = 48;
pub const MATCHED_EDGE_RECORD_BYTES: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StationId {
    /// 0: OSM node; 1: OSM way; 2: OSM relation.
    pub kind: u8,
    pub id: i64,
}

impl StationId {
    pub fn new(kind: u8, id: i64) -> Result<Self> {
        if kind > 2 || id < 0 {
            bail!("invalid OSM station identity: type {kind}, id {id}");
        }
        Ok(Self { kind, id })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraversedEdge {
    pub edge_id: u64,
    pub forward: bool,
    pub arrival_station: Option<StationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StationSegment {
    pub from: StationId,
    pub to: StationId,
    /// Byte offset into the append-only 9-byte directed physical-edge records.
    pub path_offset: u64,
    pub edge_count: u64,
}

pub struct SegmentWriter {
    path: BufWriter<File>,
    index: BufWriter<File>,
    offset: u64,
}

impl SegmentWriter {
    pub fn create(path_file: &Path, index_file: &Path) -> Result<Self> {
        Ok(Self {
            path: BufWriter::new(File::create(path_file)?),
            index: BufWriter::new(File::create(index_file)?),
            offset: 0,
        })
    }

    fn add_edge(&mut self, edge_id: u64, forward: bool) -> Result<()> {
        self.path.write_all(&edge_id.to_be_bytes())?;
        self.path.write_all(&[u8::from(forward)])?;
        self.offset = self.offset.checked_add(9)
            .ok_or_else(|| anyhow::anyhow!("path byte offset overflow"))?;
        Ok(())
    }

    fn complete_span(
        &mut self,
        from: &mut StationId,
        to: StationId,
        span_offset: &mut u64,
        span_count: &mut u64,
    ) -> Result<u64> {
        // The matched edge must terminate at the proposed OSM anchor;
        // repeated anchors may still represent a real physical loop.
        let emit = if to != *from || *span_count > 1 {
            self.write_index(StationSegment {
                from: *from,
                to,
                path_offset: *span_offset,
                edge_count: *span_count,
            })?;
            1
        } else { 0 };
        *from = to;
        *span_offset = self.offset;
        *span_count = 0;
        Ok(emit)
    }

    pub fn append<I>(&mut self, start: StationId, walk: I) -> Result<u64>
    where
        I: IntoIterator<Item = TraversedEdge>,
    {
        let mut from = start;
        let mut span_offset = self.offset;
        let mut span_count = 0u64;
        let mut emitted = 0u64;
        for step in walk {
            self.add_edge(step.edge_id, step.forward)?;
            span_count = span_count.checked_add(1).ok_or_else(|| anyhow::anyhow!("span overflow"))?;
            if let Some(to) = step.arrival_station {
                emitted += self.complete_span(&mut from, to, &mut span_offset, &mut span_count)?;
            }
        }
        // Trailing unmatched geometry is not considered an anchored segment.
        Ok(emitted)
    }

    /// Stream exactly edge_count records from a **trusted** map matcher.
    /// Input record v2: edge ID u64, forward u8, arrival kind u8,
    /// 6 reserved bytes, arrival station ID i64. If arrival ID is i64::MIN
    /// there is no station at the edge end; kind is ignored.
    pub fn append_records(
        &mut self, start: StationId, reader: &mut impl Read, edge_count: u64,
    ) -> Result<u64> {
        let mut from = start;
        let mut span_offset = self.offset;
        let mut span_count = 0u64;
        let mut emitted = 0u64;
        for _ in 0..edge_count {
            let mut raw = [0u8; MATCHED_EDGE_RECORD_BYTES];
            reader.read_exact(&mut raw)?;
            let edge_id = u64::from_be_bytes(raw[..8].try_into()?);
            if raw[8] > 1 { bail!("invalid edge direction"); }
            let arrival_id = i64::from_be_bytes(raw[16..24].try_into()?);
            let arrival = if arrival_id == i64::MIN {
                None
            } else {
                Some(StationId::new(raw[9], arrival_id)?)
            };
            self.add_edge(edge_id, raw[8] == 1)?;
            span_count = span_count.checked_add(1).ok_or_else(|| anyhow::anyhow!("span overflow"))?;
            if let Some(to) = arrival {
                emitted += self.complete_span(&mut from, to, &mut span_offset, &mut span_count)?;
            }
        }
        Ok(emitted)
    }

    fn write_index(&mut self, segment: StationSegment) -> Result<()> {
        for station in [segment.from, segment.to] {
            self.index.write_all(&[station.kind])?;
            self.index.write_all(&[0u8; 7])?;
            self.index.write_all(&station.id.to_be_bytes())?;
        }
        self.index.write_all(&segment.path_offset.to_be_bytes())?;
        self.index.write_all(&segment.edge_count.to_be_bytes())?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.path.flush()?;
        self.index.flush()?;
        Ok(())
    }
}

pub fn read_segment(reader: &mut impl Read) -> Result<Option<StationSegment>> {
    let mut bytes = [0u8; SEGMENT_RECORD_BYTES];
    if reader.read(&mut bytes[..1])? == 0 { return Ok(None); }
    reader.read_exact(&mut bytes[1..])?;
    let from = StationId::new(bytes[0], i64::from_be_bytes(bytes[8..16].try_into()?))?;
    let to = StationId::new(bytes[16], i64::from_be_bytes(bytes[24..32].try_into()?))?;
    Ok(Some(StationSegment {
        from,
        to,
        path_offset: u64::from_be_bytes(bytes[32..40].try_into()?),
        edge_count: u64::from_be_bytes(bytes[40..48].try_into()?),
    }))
}

pub fn stream_segment_edges(
    path_file: &mut (impl Read + std::io::Seek),
    segment: StationSegment,
    mut callback: impl FnMut(u64, bool) -> Result<()>,
) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    path_file.seek(SeekFrom::Start(segment.path_offset))?;
    for _ in 0..segment.edge_count {
        let mut id = [0; 8];
        let mut dir = [0; 1];
        path_file.read_exact(&mut id)?;
        path_file.read_exact(&mut dir)?;
        if dir[0] > 1 { bail!("invalid edge direction"); }
        callback(u64::from_be_bytes(id), dir[0] == 1)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn station(kind: u8, id: i64) -> StationId { StationId::new(kind, id).unwrap() }
    fn temp(label: &str) -> Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "meadowsweet-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[test]
    fn very_long_station_span_keeps_one_logical_edge() -> Result<()> {
        let dir = temp("long")?;
        let paths = dir.join("paths.bin");
        let index = dir.join("segments.bin");
        let mut writer = SegmentWriter::create(&paths, &index)?;
        let walk = (0..100_000u64).map(|i| TraversedEdge {
            edge_id: i,
            forward: true,
            arrival_station: (i == 99_999).then_some(station(0, 200)),
        });
        assert_eq!(writer.append(station(1, 100), walk)?, 1);
        writer.finish()?;
        let mut reader = BufReader::new(File::open(index)?);
        let segment = read_segment(&mut reader)?.unwrap();
        assert_eq!((segment.from, segment.to), (station(1, 100), station(0, 200)));
        assert_eq!(segment.edge_count, 100_000);
        assert!(read_segment(&mut reader)?.is_none());
        let mut physical = File::open(paths)?;
        let mut seen = 0u64;
        stream_segment_edges(&mut physical, segment, |id, forward| {
            assert_eq!(id, seen);
            assert!(forward);
            seen += 1;
            Ok(())
        })?;
        assert_eq!(seen, 100_000);
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn skipped_station_is_still_a_boundary() -> Result<()> {
        let dir = temp("skipped")?;
        let mut writer = SegmentWriter::create(&dir.join("paths"), &dir.join("index"))?;
        assert_eq!(writer.append(station(0, 1), [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: Some(station(0, 2)) },
            TraversedEdge { edge_id: 11, forward: true, arrival_station: Some(station(0, 3)) },
        ])?, 2);
        writer.finish()?;
        let mut reader = BufReader::new(File::open(dir.join("index"))?);
        let a = read_segment(&mut reader)?.unwrap();
        let b = read_segment(&mut reader)?.unwrap();
        assert_eq!((a.from, a.to), (station(0, 1), station(0, 2)));
        assert_eq!((b.from, b.to), (station(0, 2), station(0, 3)));
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn osm_node_way_identity_collision_is_not_merged() -> Result<()> {
        let dir = temp("types")?;
        let mut writer = SegmentWriter::create(&dir.join("paths"), &dir.join("index"))?;
        assert_eq!(writer.append(station(0, 123), [
            TraversedEdge { edge_id: 7, forward: true, arrival_station: Some(station(1, 123)) },
        ])?, 1);
        writer.finish()?;
        let mut reader = BufReader::new(File::open(dir.join("index"))?);
        let item = read_segment(&mut reader)?.unwrap();
        assert_eq!(item.from.kind, 0);
        assert_eq!(item.to.kind, 1);
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }
}
