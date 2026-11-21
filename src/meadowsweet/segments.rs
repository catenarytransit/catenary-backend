//! A logical segment spans consecutive **OSM station anchors** on one matched
//! railway traversal. Its geometry may be arbitrarily long: the segment stores
//! offsets into an append-only file of directed physical-edge IDs.
use anyhow::{Result, bail};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraversedEdge {
    pub edge_id: u64,
    /// Whether the physical edge is traversed in its stored direction.
    pub forward: bool,
    /// Station entered at the end of this edge, if any.
    pub arrival_station: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StationSegment {
    pub from_station: i64,
    pub to_station: i64,
    /// Byte offset into the append-only directed-edge path file.
    pub path_offset: u64,
    pub edge_count: u64,
}

/// Each physical edge occupies nine bytes: u64 ID (BE), then direction byte.
/// A separate edge store holds geometry; nothing here depends on station distance.
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

    /// Convert a *confirmed* matched railway walk into station segments.
    ///
    /// `start_station` is the station at the beginning of the walk.
    /// `arrival_station` must come from an anchored railway traversal, not a
    /// nearest-station query. Empty station spans are ignored.
    ///
    /// A path may include station-free sections of any length and may cross
    /// arbitrarily many geographic partitions. Incomplete trailing spans are
    /// intentionally discarded, since their endpoint is unknown.
    pub fn append<I>(&mut self, start_station: i64, walk: I) -> Result<u64>
    where
        I: IntoIterator<Item = TraversedEdge>,
    {
        let mut from = start_station;
        let mut span_offset = self.offset;
        let mut span_count = 0u64;
        let mut emitted = 0u64;

        for step in walk {
            self.path.write_all(&step.edge_id.to_be_bytes())?;
            self.path.write_all(&[u8::from(step.forward)])?;
            self.offset = self.offset.checked_add(9).ok_or_else(|| anyhow::anyhow!("path offset overflow"))?;
            span_count = span_count.checked_add(1).ok_or_else(|| anyhow::anyhow!("path length overflow"))?;

            if let Some(next) = step.arrival_station {
                if next != from || span_count > 1 {
                    self.write_index(StationSegment {
                        from_station: from,
                        to_station: next,
                        path_offset: span_offset,
                        edge_count: span_count,
                    })?;
                    emitted += 1;
                }
                from = next;
                span_offset = self.offset;
                span_count = 0;
            }
        }
        Ok(emitted)
    }

    fn write_index(&mut self, seg: StationSegment) -> Result<()> {
        self.index.write_all(&seg.from_station.to_be_bytes())?;
        self.index.write_all(&seg.to_station.to_be_bytes())?;
        self.index.write_all(&seg.path_offset.to_be_bytes())?;
        self.index.write_all(&seg.edge_count.to_be_bytes())?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.path.flush()?;
        self.index.flush()?;
        Ok(())
    }
}

pub fn read_segment(reader: &mut impl Read) -> Result<Option<StationSegment>> {
    let mut bytes = [0u8; 32];
    if reader.read(&mut bytes[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut bytes[1..])?;
    Ok(Some(StationSegment {
        from_station: i64::from_be_bytes(bytes[0..8].try_into()?),
        to_station: i64::from_be_bytes(bytes[8..16].try_into()?),
        path_offset: u64::from_be_bytes(bytes[16..24].try_into()?),
        edge_count: u64::from_be_bytes(bytes[24..32].try_into()?),
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
        if dir[0] > 1 {
            bail!("invalid edge direction");
        }
        callback(u64::from_be_bytes(id), dir[0] == 1)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn very_long_station_span_keeps_one_logical_edge() -> Result<()> {
        let dir = std::env::temp_dir().join(format!(
            "meadowsweet-segments-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&dir)?;
        let paths = dir.join("paths.bin");
        let index = dir.join("segments.bin");
        let mut writer = SegmentWriter::create(&paths, &index)?;
        let walk = (0..100_000u64).map(|i| TraversedEdge {
            edge_id: i,
            forward: true,
            arrival_station: (i == 99_999).then_some(200),
        });
        assert_eq!(writer.append(100, walk)?, 1);
        writer.finish()?;
        let mut reader = BufReader::new(File::open(index)?);
        let segment = read_segment(&mut reader)?.unwrap();
        assert_eq!((segment.from_station, segment.to_station), (100, 200));
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
        let dir = std::env::temp_dir().join(format!("meadowsweet-skip-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let mut writer = SegmentWriter::create(&dir.join("p"), &dir.join("i"))?;
        assert_eq!(writer.append(1, [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: Some(2) },
            TraversedEdge { edge_id: 11, forward: true, arrival_station: Some(3) },
        ])?, 2);
        writer.finish()?;
        let mut reader = BufReader::new(File::open(dir.join("i"))?);
        let a = read_segment(&mut reader)?.unwrap();
        let b = read_segment(&mut reader)?.unwrap();
        assert_eq!((a.from_station, a.to_station), (1, 2));
        assert_eq!((b.from_station, b.to_station), (2, 3));
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }
}
