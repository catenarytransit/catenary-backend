//! Conservative, exact railway corridor equivalence over disk-backed paths.
//!
//! This FIRST reducer only deduplicates identical physical edge sequences,
//! including the opposite travel direction. It deliberately DOES NOT conflate
//! parallel tracks or two genuinely different paths serving the same stations.
//! Such conflation needs topology-aware correspondence and station anchoring.
use crate::external_sort::{read_record, sort_fixed};
use crate::segments::{StationId, StationSegment, read_segment};
use anyhow::{Context, Result, bail};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const RECORD_SIZE: usize = 88;
const CHUNK_EDGES: usize = 4096;

fn station_lt(a: StationId, b: StationId) -> bool {
    (a.kind, a.id) < (b.kind, b.id)
}

fn normalized(seg: StationSegment) -> (StationId, StationId, bool) {
    if station_lt(seg.to, seg.from) {
        (seg.to, seg.from, true)
    } else {
        (seg.from, seg.to, false)
    }
}

fn write_station_key(out: &mut impl Write, s: StationId) -> Result<()> {
    out.write_all(&[s.kind])?;
    out.write_all(&[0; 7])?;
    out.write_all(&s.id.to_be_bytes())?;
    Ok(())
}

/// Streams an entire physical path without allocating its complete geometry.
/// Reverse orientation inverts the direction of every physical edge.
fn hash_path(file: &mut File, seg: StationSegment, reverse: bool) -> Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; CHUNK_EDGES * 9];
    let mut progress = 0u64;
    while progress < seg.edge_count {
        let take = (seg.edge_count - progress).min(CHUNK_EDGES as u64) as usize;
        let start_index = if reverse {
            seg.edge_count - progress - take as u64
        } else {
            progress
        };
        let offset = seg.path_offset
            .checked_add(start_index.checked_mul(9).context("path index overflow")?)
            .context("path position overflow")?;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buffer[..take * 9])?;
        if reverse {
            for item in buffer[..take * 9].chunks_exact(9).rev() {
                if item[8] > 1 { bail!("invalid edge direction"); }
                hasher.update(&item[..8]);
                hasher.update(&[1 - item[8]]);
            }
        } else {
            for item in buffer[..take * 9].chunks_exact(9) {
                if item[8] > 1 { bail!("invalid edge direction"); }
            }
            hasher.update(&buffer[..take * 9]);
        }
        progress += take as u64;
    }
    Ok(hasher.finalize())
}

/// The orientation bit accompanies the canonical fingerprint so geometry
/// export can reproduce a stable A-to-B line in either input direction.
/// Closed loops select the lexicographically smaller traversal fingerprint.
fn canonical_digest(file: &mut File, seg: StationSegment) -> Result<(blake3::Hash, bool)> {
    let (_, _, reverse) = normalized(seg);
    if seg.from == seg.to {
        let forward = hash_path(file, seg, false)?;
        let backward = hash_path(file, seg, true)?;
        if forward.as_bytes() <= backward.as_bytes() {
            Ok((forward, false))
        } else {
            Ok((backward, true))
        }
    } else {
        Ok((hash_path(file, seg, reverse)?, reverse))
    }
}

#[cfg(test)]
fn path_digest(file: &mut File, seg: StationSegment) -> Result<blake3::Hash> {
    Ok(canonical_digest(file, seg)?.0)
}

/// Exact comparison, even when two paths share a BLAKE3 digest.
/// The chosen representation is canonical under reversal.
fn equivalent(file: &mut File, a: StationSegment, b: StationSegment) -> Result<bool> {
    if a.edge_count != b.edge_count { return Ok(false); }
    let (_, _, rev_a) = normalized(a);
    let (_, _, rev_b) = normalized(b);
    let same = compare_oriented(file, a, rev_a, b, rev_b)?;
    if same { return Ok(true); }
    if a.from == a.to && b.from == b.to {
        // Equal-station loops can have either traversal orientation.
        return compare_oriented(file, a, rev_a, b, !rev_b);
    }
    Ok(false)
}

fn compare_oriented(
    file: &mut File, a: StationSegment, a_reverse: bool,
    b: StationSegment, b_reverse: bool,
) -> Result<bool> {
    // Read contiguous 36 KiB runs, including in the reverse direction.
    // This avoids 2*N random seeks on long duplicate corridors.
    let mut abuf = vec![0u8; CHUNK_EDGES * 9];
    let mut bbuf = vec![0u8; CHUNK_EDGES * 9];
    let mut processed = 0u64;
    while processed < a.edge_count {
        let chunk = (a.edge_count - processed).min(CHUNK_EDGES as u64) as usize;
        for (seg, reverse, dest) in [
            (a, a_reverse, &mut abuf),
            (b, b_reverse, &mut bbuf),
        ] {
            let index = if reverse {
                seg.edge_count - processed - chunk as u64
            } else { processed };
            let off = seg.path_offset
                .checked_add(index.checked_mul(9).context("path offset overflow")?)
                .context("path offset overflow")?;
            file.seek(SeekFrom::Start(off))?;
            file.read_exact(&mut dest[..chunk * 9])?;
            if reverse {
                for item in dest[..chunk * 9].chunks_exact_mut(9) {
                    if item[8] > 1 { bail!("invalid edge direction"); }
                    item[8] ^= 1;
                }
                for i in 0..(chunk / 2) {
                    for j in 0..9 {
                        dest.swap(9*i+j, 9*(chunk-1-i)+j);
                    }
                }
            }
        }
        if abuf[..chunk * 9] != bbuf[..chunk * 9] {
            return Ok(false);
        }
        processed += chunk as u64;
    }
    Ok(true)
}

fn record(seg: StationSegment, hash: blake3::Hash, reversed: bool) -> Result<[u8; RECORD_SIZE]> {
    let mut bytes = [0u8; RECORD_SIZE];
    let (a, b, _) = normalized(seg);
    let mut cursor = &mut bytes[..];
    write_station_key(&mut cursor, a)?;
    write_station_key(&mut cursor, b)?;
    cursor.write_all(hash.as_bytes())?;
    cursor.write_all(&seg.path_offset.to_be_bytes())?;
    cursor.write_all(&seg.edge_count.to_be_bytes())?;
    cursor.write_all(&u64::from(reversed).to_be_bytes())?;
    Ok(bytes)
}

fn from_record(row: &[u8; RECORD_SIZE]) -> Result<StationSegment> {
    let a = StationId::new(row[0], i64::from_be_bytes(row[8..16].try_into()?))?;
    let b = StationId::new(row[16], i64::from_be_bytes(row[24..32].try_into()?))?;
    let reverse = u64::from_be_bytes(row[80..88].try_into()?);
    if reverse > 1 { bail!("invalid corridor path orientation"); }
    Ok(StationSegment {
        from: if reverse == 1 { b } else { a },
        to: if reverse == 1 { a } else { b },
        path_offset: u64::from_be_bytes(row[64..72].try_into()?),
        edge_count: u64::from_be_bytes(row[72..80].try_into()?),
    })
}

/// Exact-path deduplication within canonical endpoint pairs.
/// Writes a fixed 88-byte record per input segment and a 104-byte record per
/// unique physical path: canonical endpoints, digest, first path offset,
/// edge count and supporting traversal count.
pub fn reduce(index: &Path, paths: &Path, output: &Path, scratch: &Path, memory_bytes: usize) -> Result<()> {
    if scratch.exists() { bail!("scratch directory already exists"); }
    fs::create_dir_all(scratch)?;
    let mut path_file = File::open(paths)?;
    let mut reader = BufReader::new(File::open(index)?);
    let mut ungrouped = BufWriter::new(File::create(scratch.join("fingerprints.raw"))?);
    let mut count = 0u64;
    while let Some(seg) = read_segment(&mut reader)? {
        let (digest, reversed) = canonical_digest(&mut path_file, seg)?;
        ungrouped.write_all(&record(seg, digest, reversed)?)?;
        count += 1;
    }
    ungrouped.flush()?;
    drop(ungrouped);
    sort_fixed::<RECORD_SIZE>(
        &scratch.join("fingerprints.raw"),
        &scratch.join("fingerprints.sorted"),
        &scratch.join("sort_runs"), memory_bytes, 16,
    )?;

    let mut sorted = BufReader::new(File::open(scratch.join("fingerprints.sorted"))?);
    let mut out = BufWriter::new(File::create(output)?);
    let mut previous: Option<[u8; RECORD_SIZE]> = None;
    let mut support = 0u64;
    let mut unique = 0u64;
    while let Some(row) = read_record::<RECORD_SIZE>(&mut sorted)? {
        if let Some(prior) = previous {
            if row[..64] == prior[..64] {
                // Hash equality is NOT sufficient to equate corridor topology.
                // Fail closed if two canonical hashes collide.
                if !equivalent(&mut path_file, from_record(&prior)?, from_record(&row)?)? {
                    bail!("physical path fingerprint collision; refusing to merge");
                }
                support += 1;
                continue;
            }
            out.write_all(&prior)?;
            out.write_all(&support.to_be_bytes())?;
            out.write_all(&[0u8; 8])?;
            unique += 1;
        }
        previous = Some(row);
        support = 1;
    }
    if let Some(row) = previous {
        out.write_all(&row)?;
        out.write_all(&support.to_be_bytes())?;
        out.write_all(&[0u8; 8])?;
        unique += 1;
    }
    out.flush()?;
    fs::remove_dir_all(scratch)?;
    eprintln!("Meadowsweet: {count} station traversals -> {unique} exact physical corridors");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn reverse_direction_is_same_corridor() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("meadowsweet-corridor-{}", std::process::id()));
        fs::create_dir_all(&dir)?;
        let mut f = File::create(dir.join("p"))?;
        for (id, direction) in [(11u64, 1u8), (12, 1), (12, 0), (11, 0)] {
            f.write_all(&id.to_be_bytes())?;
            f.write_all(&[direction])?;
        }
        drop(f);
        let mut file = File::open(dir.join("p"))?;
        let a = StationId::new(0, 10)?;
        let b = StationId::new(1, 20)?;
        let forward = StationSegment { from: a, to: b, path_offset: 0, edge_count: 2 };
        let backward = StationSegment { from: b, to: a, path_offset: 18, edge_count: 2 };
        assert_eq!(path_digest(&mut file, forward)?, path_digest(&mut file, backward)?);
        assert!(equivalent(&mut file, forward, backward)?);
        let other = StationSegment { from: a, to: b, path_offset: 9, edge_count: 2 };
        assert!(!equivalent(&mut file, forward, other)?);
        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn reducer_aggregates_reverse_path_but_not_distinct_branch() -> Result<()> {
        use crate::segments::{SegmentWriter, TraversedEdge};
        let dir = std::env::temp_dir().join(format!(
            "meadowsweet-reduce-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
        ));
        fs::create_dir_all(&dir)?;
        let a = StationId::new(0, 1)?;
        let b = StationId::new(0, 2)?;
        let mut writer = SegmentWriter::create(&dir.join("paths"), &dir.join("index"))?;
        assert_eq!(writer.append(a, [
            TraversedEdge { edge_id: 100, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 101, forward: true, arrival_station: Some(b) },
        ])?, 1);
        assert_eq!(writer.append(b, [
            TraversedEdge { edge_id: 101, forward: false, arrival_station: None },
            TraversedEdge { edge_id: 100, forward: false, arrival_station: Some(a) },
        ])?, 1);
        assert_eq!(writer.append(a, [
            TraversedEdge { edge_id: 100, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 102, forward: true, arrival_station: Some(b) },
        ])?, 1);
        writer.finish()?;
        reduce(&dir.join("index"), &dir.join("paths"), &dir.join("reduced"), &dir.join("scratch"), 88 * 2)?;
        let bytes = fs::read(dir.join("reduced"))?;
        assert_eq!(bytes.len(), 104 * 2);
        let mut weights = vec![
            u64::from_be_bytes(bytes[88..96].try_into()?),
            u64::from_be_bytes(bytes[104+88..104+96].try_into()?),
        ];
        weights.sort();
        assert_eq!(weights, vec![1, 2]);
        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn two_different_routes_between_same_stations_remain_distinct() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("meadowsweet-distinct-{}", std::process::id()));
        fs::create_dir_all(&dir)?;
        let mut f = File::create(dir.join("p"))?;
        for id in [1u64, 2, 1, 3] {
            f.write_all(&id.to_be_bytes())?;
            f.write_all(&[1])?;
        }
        drop(f);
        let a = StationId::new(0, 1)?;
        let b = StationId::new(0, 2)?;
        let x = StationSegment { from:a, to:b, path_offset:0, edge_count:2 };
        let y = StationSegment { from:a, to:b, path_offset:18, edge_count:2 };
        let mut f = File::open(dir.join("p"))?;
        assert_ne!(path_digest(&mut f, x)?, path_digest(&mut f, y)?);
        fs::remove_dir_all(dir)?;
        Ok(())
    }
}
