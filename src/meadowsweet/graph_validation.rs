//! Verify that each station-to-station path follows a connected, directed
//! OSM railway walk. This validates physical topology, not station identity.
//!
//! Edges are fixed 64-byte records sorted by atomic edge ID, so lookup uses
//! binary search with a small direct-mapped cache rather than materializing
//! the continent-wide graph in a HashMap.
use crate::segments::{StationSegment, read_segment, stream_segment_edges};
use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

const EDGE_BYTES: u64 = 64;
const CACHE_SLOTS: usize = 4096;

#[derive(Debug, Clone, Copy)]
struct PhysicalEdge {
    id: u64,
    from: i64,
    to: i64,
}

struct EdgeLookup {
    file: BufReader<File>,
    count: u64,
    cache: Vec<Option<PhysicalEdge>>,
}

impl EdgeLookup {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let len = file.metadata()?.len();
        if len % EDGE_BYTES != 0 {
            bail!("physical edge store is not a multiple of 64 bytes");
        }
        Ok(Self {
            file: BufReader::new(file),
            count: len / EDGE_BYTES,
            cache: vec![None; CACHE_SLOTS],
        })
    }

    fn find(&mut self, id: u64) -> Result<PhysicalEdge> {
        let slot = (id as usize) % self.cache.len();
        if let Some(found) = self.cache[slot] {
            if found.id == id {
                return Ok(found);
            }
        }

        let mut low = 0u64;
        let mut high = self.count;
        while low < high {
            let mid = low + (high - low) / 2;
            self.file.seek(SeekFrom::Start(mid.checked_mul(EDGE_BYTES)
                .context("physical edge byte offset overflow")?))?;
            let mut raw = [0u8; 24];
            self.file.read_exact(&mut raw)?;
            let mid_id = u64::from_be_bytes(raw[0..8].try_into()?);
            if mid_id < id {
                low = mid + 1;
            } else if mid_id > id {
                high = mid;
            } else {
                let found = PhysicalEdge {
                    id: mid_id,
                    from: i64::from_be_bytes(raw[8..16].try_into()?),
                    to: i64::from_be_bytes(raw[16..24].try_into()?),
                };
                self.cache[slot] = Some(found);
                return Ok(found);
            }
        }
        bail!("path references non-existent physical railway edge {id}");
    }
}

fn validate_one(
    segment: StationSegment,
    paths: &mut File,
    edges: &mut EdgeLookup,
) -> Result<u64> {
    if segment.edge_count == 0 {
        bail!("zero-length station-to-station segment");
    }
    let mut previous_end: Option<i64> = None;
    let mut count = 0u64;
    stream_segment_edges(paths, segment, |edge_id, forward| {
        let e = edges.find(edge_id)?;
        let (start, end) = if forward {
            (e.from, e.to)
        } else {
            (e.to, e.from)
        };
        if let Some(prior) = previous_end {
            if prior != start {
                bail!(
                    "disconnected railway path at edge {edge_id}: previous endpoint {prior}, next start {start}"
                );
            }
        }
        previous_end = Some(end);
        count += 1;
        Ok(())
    })?;
    Ok(count)
}

/// Verify each path's complete physical connectivity and edge references.
///
/// This stage *cannot* yet prove that the first/last edge reaches the
/// supplied station, or that intermediate station anchors were identified.
/// That requires accepted topology-aware station anchors.
pub fn validate(index: &Path, paths: &Path, graph: &Path) -> Result<()> {
    let mut index_file = BufReader::new(File::open(index)?);
    let mut path_file = File::open(paths)?;
    let mut edges = EdgeLookup::open(&graph.join("edges.bin"))?;
    let mut segments = 0u64;
    let mut traversed = 0u64;
    while let Some(seg) = read_segment(&mut index_file)? {
        traversed += validate_one(seg, &mut path_file, &mut edges)
            .with_context(|| format!("station segment {}: {:?} -> {:?}", segments, seg.from, seg.to))?;
        segments += 1;
    }
    println!("validated {segments} connected station-segment paths ({traversed} physical edge traversals)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segments::{SegmentWriter, StationId, TraversedEdge};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn setup() -> Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "meadowsweet-topology-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&dir)?;
        let mut graph = File::create(dir.join("edges.bin"))?;
        // id, from, to, 40 remaining bytes of metadata and coordinates
        for (id, from, to) in [(10u64, 1i64, 2i64), (20, 2, 3), (30, 10, 11)] {
            graph.write_all(&id.to_be_bytes())?;
            graph.write_all(&from.to_be_bytes())?;
            graph.write_all(&to.to_be_bytes())?;
            graph.write_all(&[0u8; 40])?;
        }
        graph.flush()?;
        Ok(dir)
    }

    fn station(id: i64) -> Result<StationId> {
        StationId::new(0, id)
    }

    #[test]
    fn connected_directed_walk_passes() -> Result<()> {
        let dir = setup()?;
        let mut writer = SegmentWriter::create(&dir.join("paths"), &dir.join("index"))?;
        writer.append(station(1)?, [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 20, forward: true, arrival_station: Some(station(3)?) },
        ])?;
        writer.finish()?;
        validate(&dir.join("index"), &dir.join("paths"), &dir)?;
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn disconnected_jump_is_rejected() -> Result<()> {
        let dir = setup()?;
        let mut writer = SegmentWriter::create(&dir.join("paths"), &dir.join("index"))?;
        writer.append(station(1)?, [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 30, forward: true, arrival_station: Some(station(3)?) },
        ])?;
        writer.finish()?;
        assert!(validate(&dir.join("index"), &dir.join("paths"), &dir).is_err());
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn reversed_direction_must_be_connected() -> Result<()> {
        let dir = setup()?;
        let mut writer = SegmentWriter::create(&dir.join("paths"), &dir.join("index"))?;
        writer.append(station(3)?, [
            TraversedEdge { edge_id: 20, forward: false, arrival_station: None },
            TraversedEdge { edge_id: 10, forward: false, arrival_station: Some(station(1)?) },
        ])?;
        writer.finish()?;
        validate(&dir.join("index"), &dir.join("paths"), &dir)?;
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }
}
