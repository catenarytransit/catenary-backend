//! Immutable, independently published mmap routing indexes.
//! The Gentian timetable is the source of truth. This file contains only derived
//! data: event-major FIFO transport columns, lower-bound CSR graphs, and
//! station metadata. A header and inode/generation stamp reject stale indexes.
use crate::routing_common::profile_compact::{CsrGraph, LowerBoundTopology};
use crate::routing_common::timetable::ZeroCopyTimetable;
use crate::routing_common::timetable_mmap::MappedTimetable;
use crate::routing_common::transfer_optimizer::StationImportance;
use crate::routing_common::transport_index::{TransportChain, TransportIndex};
use anyhow::{Context, Result, bail, ensure};
use bytemuck::{Pod, Zeroable};
use memmap2::{Mmap, MmapOptions};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: u32 = 0x5844_4952; // RIDX
const VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
pub(crate) struct Section {
    offset: u64,
    count: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
struct Edge {
    to: u32,
    cost: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
struct ChainRecord {
    transports: Section,
    first_departures: Section,
    patterns: Section,
    offsets: Section,
    calendars: Section,
    event_days: Section,
    n_patterns: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
struct Header {
    magic: u32,
    version: u32,
    identity: [u8; 32],
    stops: u64,
    routes: u64,
    file_size: u64,
    route_chain_offsets: Section,
    chains: Section,
    forward_offsets: Section,
    forward_edges: Section,
    reverse_offsets: Section,
    reverse_edges: Section,
    stations: Section,
    transfer_sensitive: Section,
    importance_roots: Section,
    importance_values: Section,
    importance_maximum: u64,
}

/// An index column is either built in-memory (tests/offline compilation), or
/// borrowed directly from the validated, immutable mapped companion file.
/// All mmap sections are bounds-checked once at open, never copied per chain.
pub(crate) enum IndexArray<T: Pod> {
    Owned(Vec<T>),
    Mapped {
        bytes: Arc<Mmap>,
        section: Section,
        marker: PhantomData<T>,
    },
}

impl<T: Pod> IndexArray<T> {
    pub(crate) fn owned(data: Vec<T>) -> Self {
        Self::Owned(data)
    }

    fn mapped(bytes: Arc<Mmap>, section: Section) -> Result<Self> {
        read_slice::<T>(&bytes, section)?;
        Ok(Self::Mapped {
            bytes,
            section,
            marker: PhantomData,
        })
    }
}
impl<T: Pod> Deref for IndexArray<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        match self {
            Self::Owned(values) => values,
            Self::Mapped { bytes, section, .. } => {
                read_slice(bytes, *section).expect("validated routing-index section")
            }
        }
    }
}
impl<'a, T: Pod> IntoIterator for &'a IndexArray<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

fn read_slice<T: Pod>(bytes: &[u8], sec: Section) -> Result<&[T]> {
    let start = usize::try_from(sec.offset)?;
    let count = usize::try_from(sec.count)?;
    let size = count
        .checked_mul(std::mem::size_of::<T>())
        .context("routing index section length overflow")?;
    let end = start
        .checked_add(size)
        .context("routing index section offset overflow")?;
    ensure!(
        start % std::mem::align_of::<T>() == 0,
        "unaligned routing index section"
    );
    let slice = bytes
        .get(start..end)
        .context("truncated routing index section")?;
    bytemuck::try_cast_slice(slice)
        .map_err(|_| anyhow::anyhow!("invalid routing index section alignment"))
}

fn write_slice<T: Pod>(out: &mut BufWriter<File>, values: &[T]) -> Result<Section> {
    let position = out.stream_position()?;
    let padding = (8 - position % 8) % 8;
    if padding != 0 {
        out.write_all(&[0u8; 8][..padding as usize])?;
    }
    let offset = out.stream_position()?;
    out.write_all(bytemuck::cast_slice(values))?;
    Ok(Section {
        offset,
        count: u64::try_from(values.len())?,
    })
}

fn write_usize(out: &mut BufWriter<File>, values: &[usize]) -> Result<Section> {
    // Offsets are explicit u64 on disk, regardless of Rust pointer width.
    let owned = values.iter().map(|&v| v as u64).collect::<Vec<_>>();
    write_slice(out, &owned)
}

fn read_usize(bytes: &[u8], section: Section) -> Result<Vec<usize>> {
    read_slice::<u64>(bytes, section)?
        .iter()
        .map(|&value| usize::try_from(value).map_err(Into::into))
        .collect()
}

fn write_edges(out: &mut BufWriter<File>, edges: &[(u32, u32)]) -> Result<Section> {
    let position = out.stream_position()?;
    let padding = (8 - position % 8) % 8;
    if padding != 0 {
        out.write_all(&[0u8; 8][..padding as usize])?;
    }
    let offset = out.stream_position()?;
    for chunk in edges.chunks(4096) {
        let packed = chunk
            .iter()
            .map(|&(to, cost)| Edge { to, cost })
            .collect::<Vec<_>>();
        out.write_all(bytemuck::cast_slice(&packed))?;
    }
    Ok(Section {
        offset,
        count: u64::try_from(edges.len())?,
    })
}

fn read_graph(bytes: &[u8], offsets: Section, edges: Section, n: usize) -> Result<CsrGraph> {
    let offsets = read_usize(bytes, offsets)?;
    let pairs = read_slice::<Edge>(bytes, edges)?;
    ensure!(
        offsets.len() == n + 1,
        "routing-index CSR row count mismatch"
    );
    ensure!(
        offsets.first() == Some(&0) && offsets.last() == Some(&pairs.len()),
        "routing-index CSR offsets mismatch"
    );
    ensure!(
        offsets.windows(2).all(|pair| pair[0] <= pair[1]),
        "unordered CSR offsets"
    );
    ensure!(
        pairs.iter().all(|edge| (edge.to as usize) < n),
        "invalid CSR target"
    );
    Ok(CsrGraph {
        offsets,
        edges: pairs.iter().map(|edge| (edge.to, edge.cost)).collect(),
    })
}

/// A cheap identity for atomically published, immutable Gentian timetables.
/// Includes the full binary header, size, filesystem identity and modification
/// timestamp. Production must not mutate published timetable files in place.
fn source_identity(path: &Path, tt: &ZeroCopyTimetable<'_>) -> Result<[u8; 32]> {
    let meta = fs::metadata(path)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&meta.len().to_le_bytes());
    hasher.update(bytemuck::bytes_of(&tt.header()));
    let changed = meta.modified()?.duration_since(std::time::UNIX_EPOCH)?;
    hasher.update(&changed.as_secs().to_le_bytes());
    hasher.update(&changed.subsec_nanos().to_le_bytes());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        hasher.update(&meta.dev().to_le_bytes());
        hasher.update(&meta.ino().to_le_bytes());
        hasher.update(&meta.mtime_nsec().to_le_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}

pub fn sidecar_path(timetable_path: &Path) -> PathBuf {
    timetable_path.with_extension("routing-index.bin")
}

/// Backfill an already compiled Gentian timetable without a database or OSR
/// dependency. Also called by Gentian on new timetable builds.
pub fn build_for_timetable(timetable_path: &Path) -> Result<PathBuf> {
    let path = sidecar_path(timetable_path);
    build_at(timetable_path, &path)?;
    Ok(path)
}

pub fn build_at(timetable_path: &Path, index_path: &Path) -> Result<()> {
    ensure!(
        cfg!(target_endian = "little"),
        "routing index requires little-endian machine"
    );
    let mapped = MappedTimetable::open_fast(timetable_path)?;
    let tt = mapped.view()?;
    let started = std::time::Instant::now();
    let index = LowerBoundTopology::build(&tt);
    let identity = source_identity(timetable_path, &tt)?;
    let parent = index_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".routing-index.{}.building", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        // The publisher validates the completed index with a read-only mmap.
        // Open the temporary file for reading as well as writing so Linux does
        // not reject the validation mapping with EACCES.
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temp)
            .with_context(|| format!("creating temporary routing index {}", temp.display()))?;
        let mut out = BufWriter::new(file);
        out.write_all(&vec![0u8; std::mem::size_of::<Header>()])?;
        let mut route_offsets = Vec::with_capacity(index.transports.routes.len() + 1);
        let mut chains = Vec::new();
        route_offsets.push(0u64);
        for route in &index.transports.routes {
            for chain in route {
                chains.push(ChainRecord {
                    transports: write_slice(&mut out, &chain.transports)?,
                    first_departures: write_slice(&mut out, &chain.first_departures)?,
                    patterns: write_slice(&mut out, &chain.patterns)?,
                    offsets: write_slice(&mut out, &chain.offsets)?,
                    calendars: write_slice(&mut out, &chain.calendars)?,
                    event_days: write_slice(&mut out, &chain.event_days)?,
                    n_patterns: chain.n_patterns as u64,
                });
            }
            route_offsets.push(chains.len() as u64);
        }
        let mut h = Header {
            magic: MAGIC,
            version: VERSION,
            identity,
            stops: tt.coordinates().len() as u64,
            routes: tt.route_transport_ranges().len() as u64,
            ..Header::default()
        };
        h.route_chain_offsets = write_slice(&mut out, &route_offsets)?;
        h.chains = write_slice(&mut out, &chains)?;
        h.forward_offsets = write_usize(&mut out, &index.forward.offsets)?;
        h.forward_edges = write_edges(&mut out, &index.forward.edges)?;
        h.reverse_offsets = write_usize(&mut out, &index.reverse.offsets)?;
        h.reverse_edges = write_edges(&mut out, &index.reverse.edges)?;
        h.stations = write_usize(&mut out, &index.stations)?;
        let sensitive: Vec<u8> = index
            .transfer_sensitive
            .iter()
            .map(|&v| u8::from(v))
            .collect();
        h.transfer_sensitive = write_slice(&mut out, &sensitive)?;
        h.importance_roots = write_usize(&mut out, &index.importance.roots)?;
        h.importance_values = write_slice(&mut out, &index.importance.values)?;
        h.importance_maximum = index.importance.maximum;
        h.file_size = out.stream_position()?;
        out.seek(SeekFrom::Start(0))?;
        out.write_all(bytemuck::bytes_of(&h))?;
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        // Always inspect the published representation before installation.
        let checked = unsafe { MmapOptions::new().map(&file) }.with_context(|| {
            format!(
                "mapping temporary routing index {} for validation",
                temp.display()
            )
        })?;
        validate(&checked, &tt, identity)?;
        drop(checked);
        fs::rename(&temp, index_path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result?;
    eprintln!(
        "Gentian: routing index {} generated in {:.3}s (offline)",
        index_path.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn validate(bytes: &[u8], tt: &ZeroCopyTimetable<'_>, identity: [u8; 32]) -> Result<Header> {
    ensure!(
        bytes.len() >= std::mem::size_of::<Header>(),
        "routing index too small"
    );
    let h: Header = bytemuck::pod_read_unaligned(&bytes[..std::mem::size_of::<Header>()]);
    ensure!(
        h.magic == MAGIC && h.version == VERSION,
        "unsupported routing index version"
    );
    ensure!(
        h.file_size == bytes.len() as u64,
        "routing index length mismatch"
    );
    ensure!(
        h.identity == identity,
        "routing index belongs to another timetable generation"
    );
    ensure!(
        h.stops == tt.coordinates().len() as u64,
        "routing-index stop count mismatch"
    );
    ensure!(
        h.routes == tt.route_transport_ranges().len() as u64,
        "routing-index route count mismatch"
    );
    let n = tt.coordinates().len();
    let route_offsets = read_slice::<u64>(bytes, h.route_chain_offsets)?;
    let chain_records = read_slice::<ChainRecord>(bytes, h.chains)?;
    ensure!(
        route_offsets.len() == tt.route_transport_ranges().len() + 1,
        "route-chain count mismatch"
    );
    ensure!(
        route_offsets.first() == Some(&0)
            && route_offsets.last() == Some(&(chain_records.len() as u64)),
        "route-chain index boundaries mismatch"
    );
    ensure!(
        route_offsets.windows(2).all(|p| p[0] <= p[1]),
        "invalid route-chain offsets"
    );
    for chain in chain_records {
        let transports =
            read_slice::<crate::routing_common::timetable::TransportIdx>(bytes, chain.transports)?;
        let first = read_slice::<u32>(bytes, chain.first_departures)?;
        let patterns = read_slice::<usize>(bytes, chain.patterns)?;
        let calendars =
            read_slice::<crate::routing_common::timetable::BitfieldIdx>(bytes, chain.calendars)?;
        let offsets = read_slice::<u32>(bytes, chain.offsets)?;
        read_slice::<u64>(bytes, chain.event_days)?;
        ensure!(
            first.len() == transports.len()
                && patterns.len() == transports.len()
                && calendars.len() == transports.len(),
            "transport-chain column length mismatch"
        );
        ensure!(
            chain.n_patterns > 0 && chain.n_patterns <= transports.len() as u64,
            "invalid timing pattern count"
        );
        ensure!(
            patterns.iter().all(|&p| p < chain.n_patterns as usize),
            "timing pattern outside chain"
        );
        ensure!(
            offsets.len() % chain.n_patterns as usize == 0,
            "timing offset table length mismatch"
        );
        ensure!(
            transports
                .iter()
                .all(|t| t.idx() < tt.transport_route().len()),
            "transport index out of bounds"
        );
    }
    for (offsets, edges) in [
        (h.forward_offsets, h.forward_edges),
        (h.reverse_offsets, h.reverse_edges),
    ] {
        let rows = read_slice::<u64>(bytes, offsets)?;
        let links = read_slice::<Edge>(bytes, edges)?;
        ensure!(
            rows.len() == n + 1 && rows[0] == 0 && rows[n] == links.len() as u64,
            "invalid lower-bound graph offsets"
        );
        ensure!(
            rows.windows(2).all(|p| p[0] <= p[1]),
            "unordered lower-bound graph offsets"
        );
        ensure!(
            links.iter().all(|link| (link.to as usize) < n),
            "lower-bound target out of bounds"
        );
    }
    ensure!(
        read_slice::<u64>(bytes, h.stations)?.len() == n,
        "station root count mismatch"
    );
    ensure!(
        read_slice::<u8>(bytes, h.transfer_sensitive)?.len() == n,
        "transfer flags mismatch"
    );
    ensure!(
        read_slice::<u64>(bytes, h.importance_roots)?.len() == n,
        "importance roots mismatch"
    );
    ensure!(
        read_slice::<u64>(bytes, h.importance_values)?.len() == n,
        "importance values mismatch"
    );
    Ok(h)
}

pub(crate) fn load(
    timetable_path: &Path,
    tt: &ZeroCopyTimetable<'_>,
) -> Result<LowerBoundTopology> {
    ensure!(
        cfg!(target_endian = "little") && std::mem::size_of::<usize>() == 8,
        "routing index requires 64-bit little-endian machine"
    );
    let path = sidecar_path(timetable_path);
    let file = File::open(&path)
        .with_context(|| format!("opening precompiled routing index {}", path.display()))?;
    let bytes = Arc::new(unsafe { MmapOptions::new().map(&file)? });
    let h = validate(&bytes, tt, source_identity(timetable_path, tt)?)?;
    let route_offsets = read_slice::<u64>(&bytes, h.route_chain_offsets)?;
    let descriptors = read_slice::<ChainRecord>(&bytes, h.chains)?;
    let mut routes = Vec::with_capacity(tt.route_transport_ranges().len());
    for row in route_offsets.windows(2) {
        let mut chains = Vec::with_capacity((row[1] - row[0]) as usize);
        for descriptor in &descriptors[row[0] as usize..row[1] as usize] {
            chains.push(TransportChain {
                transports: IndexArray::mapped(Arc::clone(&bytes), descriptor.transports)?,
                first_departures: IndexArray::mapped(
                    Arc::clone(&bytes),
                    descriptor.first_departures,
                )?,
                patterns: IndexArray::mapped(Arc::clone(&bytes), descriptor.patterns)?,
                offsets: IndexArray::mapped(Arc::clone(&bytes), descriptor.offsets)?,
                n_patterns: descriptor.n_patterns as usize,
                calendars: IndexArray::mapped(Arc::clone(&bytes), descriptor.calendars)?,
                event_days: IndexArray::mapped(Arc::clone(&bytes), descriptor.event_days)?,
            });
        }
        routes.push(chains);
    }
    let n = tt.coordinates().len();
    let stations = read_usize(&bytes, h.stations)?;
    ensure!(
        stations.iter().all(|&s| s < n),
        "station root outside timetable"
    );
    let transfer_sensitive = read_slice::<u8>(&bytes, h.transfer_sensitive)?
        .iter()
        .map(|&v| v != 0)
        .collect::<Vec<_>>();
    let roots = read_usize(&bytes, h.importance_roots)?;
    ensure!(
        roots.iter().all(|&r| r < n),
        "importance root outside timetable"
    );
    let importance = StationImportance {
        roots,
        values: read_slice::<u64>(&bytes, h.importance_values)?.to_vec(),
        maximum: h.importance_maximum,
    };
    Ok(LowerBoundTopology {
        transports: Arc::new(TransportIndex { routes }),
        forward: read_graph(&bytes, h.forward_offsets, h.forward_edges, n)?,
        reverse: read_graph(&bytes, h.reverse_offsets, h.reverse_edges, n)?,
        stations,
        transfer_sensitive: Arc::new(transfer_sensitive),
        importance: Arc::new(importance),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing_common::timetable::TimetableOwned;

    #[test]
    fn compiled_index_round_trip_and_stale_rejection() {
        let dir = std::env::temp_dir().join(format!("routing-index-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.bin");
        let tt = TimetableOwned::empty(20_000, 4);
        fs::write(&path, tt.encode().unwrap()).unwrap();
        build_for_timetable(&path).unwrap();
        let mapped = MappedTimetable::open_fast(&path).unwrap();
        let cached = load(&path, &mapped.view().unwrap()).unwrap();
        let original = LowerBoundTopology::build(&mapped.view().unwrap());
        assert_eq!(cached.stations, original.stations);
        assert_eq!(cached.forward.offsets, original.forward.offsets);
        assert_eq!(cached.reverse.edges, original.reverse.edges);
        assert_eq!(
            cached.transports.routes.len(),
            original.transports.routes.len()
        );
        // An atomic timetable replacement must invalidate this companion file.
        let replacement = dir.join("replacement.bin");
        fs::write(&replacement, tt.encode().unwrap()).unwrap();
        fs::rename(&replacement, &path).unwrap();
        let changed = MappedTimetable::open_fast(&path).unwrap();
        assert!(load(&path, &changed.view().unwrap()).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn truncated_companion_is_rejected() {
        let dir = std::env::temp_dir().join(format!("routing-index-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.bin");
        let tt = TimetableOwned::empty(20_000, 4);
        fs::write(&path, tt.encode().unwrap()).unwrap();
        build_for_timetable(&path).unwrap();
        fs::write(sidecar_path(&path), [0u8; 4]).unwrap();
        let mapped = MappedTimetable::open_fast(&path).unwrap();
        assert!(load(&path, &mapped.view().unwrap()).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
