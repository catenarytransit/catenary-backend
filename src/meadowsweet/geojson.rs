//! Streaming, topology-checked GeoJSON export of canonical Meadowsweet
//! station-to-station railway corridors.
//!
//! Geometry is read from disk-backed OSM physical edges, not GTFS shapes.
//! Output is one LineString feature per *exact* physical corridor; different
//! parallel alignments remain separate until a safe conflation stage exists.
//!
//! The graph, path and corridor files are read with fixed-size buffers.
//! A full station-to-station geometry is never collected in RAM.
use anyhow::{Context, Result, bail, ensure};
use crate::external_sort::read_record;
use crate::segments::StationId;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const EDGE_RECORD_BYTES: u64 = 64;
const CORRIDOR_RECORD_BYTES: usize = 104;
const PATH_CHUNK_EDGES: usize = 4096;
const EDGE_CACHE_SLOTS: usize = 4096;

#[derive(Clone, Copy)]
struct PhysicalEdge {
    id: u64,
    from_node: i64,
    to_node: i64,
    from: (f64, f64),
    to: (f64, f64),
}

impl PhysicalEdge {
    fn parse(record: &[u8; 64]) -> Result<Self> {
        let at = |start: usize| -> Result<u64> {
            Ok(u64::from_be_bytes(record[start..start + 8].try_into()?))
        };
        let item = Self {
            id: at(0)?,
            from_node: i64::from_be_bytes(record[8..16].try_into()?),
            to_node: i64::from_be_bytes(record[16..24].try_into()?),
            from: (f64::from_bits(at(32)?), f64::from_bits(at(40)?)),
            to: (f64::from_bits(at(48)?), f64::from_bits(at(56)?)),
        };
        for (lon, lat) in [item.from, item.to] {
            ensure!(
                lon.is_finite() && lat.is_finite() && (-180.0..=180.0).contains(&lon)
                    && (-90.0..=90.0).contains(&lat),
                "edge {} has invalid geographic coordinates", item.id
            );
        }
        ensure!(
            matches!(record[24], 1 | 2),
            "edge {} is not eligible for GTFS route_type=2", item.id
        );
        Ok(item)
    }
}

struct GraphLookup {
    reader: BufReader<File>,
    count: u64,
    cache: Vec<Option<PhysicalEdge>>,
}

impl GraphLookup {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let bytes = file.metadata()?.len();
        ensure!(bytes % EDGE_RECORD_BYTES == 0, "truncated railway edges.bin");
        Ok(Self {
            reader: BufReader::new(file),
            count: bytes / EDGE_RECORD_BYTES,
            cache: vec![None; EDGE_CACHE_SLOTS],
        })
    }

    fn lookup(&mut self, id: u64) -> Result<PhysicalEdge> {
        let slot = (id as usize) % self.cache.len();
        if let Some(item) = self.cache[slot] {
            if item.id == id { return Ok(item); }
        }
        let (mut lo, mut hi) = (0u64, self.count);
        while lo < hi {
            let middle = lo + (hi - lo) / 2;
            self.reader.seek(SeekFrom::Start(
                middle.checked_mul(EDGE_RECORD_BYTES).context("graph offset overflow")?
            ))?;
            let mut record = [0u8; 64];
            self.reader.read_exact(&mut record)?;
            let middle_id = u64::from_be_bytes(record[..8].try_into()?);
            match middle_id.cmp(&id) {
                std::cmp::Ordering::Less => lo = middle + 1,
                std::cmp::Ordering::Greater => hi = middle,
                std::cmp::Ordering::Equal => {
                    let edge = PhysicalEdge::parse(&record)?;
                    self.cache[slot] = Some(edge);
                    return Ok(edge);
                }
            }
        }
        bail!("missing physical OSM railway edge {id}");
    }
}

#[derive(Debug, Clone)]
struct Corridor {
    from: StationId,
    to: StationId,
    digest: [u8; 32],
    path_offset: u64,
    edge_count: u64,
    reverse: bool,
    supporting_traversals: u64,
}

impl Corridor {
    fn parse(bytes: [u8; CORRIDOR_RECORD_BYTES]) -> Result<Self> {
        let kind_from = bytes[0];
        let kind_to = bytes[16];
        let from = StationId::new(kind_from, i64::from_be_bytes(bytes[8..16].try_into()?))?;
        let to = StationId::new(kind_to, i64::from_be_bytes(bytes[24..32].try_into()?))?;
        let orientation = u64::from_be_bytes(bytes[80..88].try_into()?);
        ensure!(orientation < 2, "invalid corridor orientation {orientation}");
        let item = Self {
            from,
            to,
            digest: bytes[32..64].try_into()?,
            path_offset: u64::from_be_bytes(bytes[64..72].try_into()?),
            edge_count: u64::from_be_bytes(bytes[72..80].try_into()?),
            reverse: orientation == 1,
            supporting_traversals: u64::from_be_bytes(bytes[88..96].try_into()?),
        };
        ensure!(item.edge_count != 0, "zero-length corridor");
        ensure!(item.supporting_traversals > 0, "corridor has zero supporting traversals");
        Ok(item)
    }

    /// Zero-padded IDs preserve the key ordering of the externally sorted
    /// canonical corridor index. Geometry hash distinguishes two paths
    /// sharing a station pair. The ID remains stable within an OSM snapshot.
    fn stable_id(&self) -> String {
        format!(
            "{:02x}{:016x}-{:02x}{:016x}-{}",
            self.from.kind, self.from.id as u64,
            self.to.kind, self.to.id as u64,
            blake3::Hash::from_bytes(self.digest).to_hex()
        )
    }
}

fn validate_color(raw: &str) -> Result<()> {
    ensure!(
        raw.len() == 7 && raw.starts_with('#')
            && raw.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit),
        "invalid corridor color {raw:?}; expected #RRGGBB"
    );
    Ok(())
}

/// Optional per-corridor color TSV. Rows MUST be sorted by the canonical
/// corridor ID, allowing an O(1) working-set merge with the corridor stream.
/// A missing color simply results in no 'color' property on that feature.
struct Colors {
    reader: BufReader<File>,
    pending: Option<(String, String)>,
    last_read: Option<String>,
}

impl Colors {
    fn open(path: &Path) -> Result<Self> {
        let mut colors = Self {
            reader: BufReader::new(File::open(path)?),
            pending: None,
            last_read: None,
        };
        colors.advance()?;
        Ok(colors)
    }

    fn advance(&mut self) -> Result<()> {
        self.pending = None;
        let mut line = Vec::with_capacity(160);
        loop {
            let buffered = self.reader.fill_buf()?;
            if buffered.is_empty() {
                if line.is_empty() { return Ok(()); }
                break;
            }
            let end = buffered.iter().position(|&b| b == b'\n');
            let take = end.map_or(buffered.len(), |pos| pos + 1);
            ensure!(line.len() + take <= 256, "corridor color TSV line exceeds 256 bytes");
            line.extend_from_slice(&buffered[..take]);
            self.reader.consume(take);
            if end.is_some() { break; }
        }
        let line = String::from_utf8(line).context("invalid UTF-8 in corridor colors")?;
        let line = line.trim_end_matches(['\r', '\n']);
        let (key, color) = line.split_once('\t')
            .context("color TSV requires corridor_id<TAB>#RRGGBB")?;
        ensure!(!key.is_empty() && !key.contains(char::is_whitespace), "invalid corridor ID in color TSV");
        validate_color(color)?;
        if let Some(ref last) = self.last_read {
            ensure!(key > last.as_str(), "color TSV must be strictly sorted by corridor ID without duplicates");
        }
        self.last_read = Some(key.to_owned());
        self.pending = Some((key.to_owned(), color.to_owned()));
        Ok(())
    }

    fn color_for(&mut self, key: &str) -> Result<Option<String>> {
        if let Some((ref candidate, ref color)) = self.pending {
            if candidate == key {
                let value = color.clone();
                self.advance()?;
                return Ok(Some(value));
            }
            ensure!(candidate.as_str() > key, "color assigned to non-existent corridor {candidate}");
        }
        Ok(None)
    }

    fn finish(&self) -> Result<()> {
        if let Some((key, _)) = &self.pending {
            bail!("color assigned to non-existent corridor {key}");
        }
        Ok(())
    }
}

fn write_point(out: &mut impl Write, point: (f64, f64), first: &mut bool) -> Result<()> {
    if !*first { out.write_all(b",")?; }
    *first = false;
    write!(out, "[{},{}]", point.0, point.1)?;
    Ok(())
}

fn write_geometry(
    out: &mut impl Write,
    paths: &mut File,
    graph: &mut GraphLookup,
    corridor: &Corridor,
) -> Result<()> {
    paths.seek(SeekFrom::End(0))?;
    let available = paths.stream_position()?;
    let required = corridor.path_offset
        .checked_add(corridor.edge_count.checked_mul(9).context("path length overflow")?)
        .context("path offset overflow")?;
    ensure!(required <= available, "corridor path outside paths.bin");

    let mut previous_node = None;
    let mut first = true;
    let mut completed = 0u64;
    let mut chunk = [0u8; PATH_CHUNK_EDGES * 9];

    out.write_all(b"[")?;
    while completed < corridor.edge_count {
        let take = ((corridor.edge_count - completed).min(PATH_CHUNK_EDGES as u64)) as usize;
        let index = if corridor.reverse {
            corridor.edge_count - completed - take as u64
        } else {
            completed
        };
        let position = corridor.path_offset
            .checked_add(index.checked_mul(9).context("path position overflow")?)
            .context("path position overflow")?;
        paths.seek(SeekFrom::Start(position))?;
        paths.read_exact(&mut chunk[..take * 9])?;

        // With a reversed canonical traversal, invert the original edge
        // direction and consume the stored edges in reverse order.
        if corridor.reverse {
            for record in chunk[..take * 9].chunks_exact(9).rev() {
                process_path_edge(record, true, &mut previous_node, &mut first, graph, out)?;
            }
        } else {
            for record in chunk[..take * 9].chunks_exact(9) {
                process_path_edge(record, false, &mut previous_node, &mut first, graph, out)?;
            }
        }
        completed += take as u64;
    }
    out.write_all(b"]")?;
    Ok(())
}

fn process_path_edge(
    bytes: &[u8], reversed: bool,
    previous_node: &mut Option<i64>,
    first: &mut bool,
    graph: &mut GraphLookup,
    out: &mut impl Write,
) -> Result<()> {
    let id = u64::from_be_bytes(bytes[..8].try_into()?);
    ensure!(bytes[8] <= 1, "invalid physical edge direction");
    let forward = (bytes[8] == 1) != reversed;
    let edge = graph.lookup(id)?;
    let (start_node, end_node, start, end) = if forward {
        (edge.from_node, edge.to_node, edge.from, edge.to)
    } else {
        (edge.to_node, edge.from_node, edge.to, edge.from)
    };
    if let Some(prev) = previous_node {
        ensure!(
            *prev == start_node,
            "disconnected physical corridor: expected node {prev}, edge {id} begins at node {start_node}"
        );
    } else {
        write_point(out, start, first)?;
    }
    write_point(out, end, first)?;
    *previous_node = Some(end_node);
    Ok(())
}

/// Emit exactly one GeoJSON LineString feature for each distinct physical
/// corridor in `corridors.bin`. This is a streaming FeatureCollection, not
/// a fully materialized GeoJSON tree. Color is optional and does not affect
/// corridor identity, geometry, or the graph.
pub fn export(
    graph_dir: &Path,
    corridor_file: &Path,
    path_file: &Path,
    destination: &Path,
    global_color: Option<&str>,
    colors_file: Option<&Path>,
) -> Result<u64> {
    if let Some(value) = global_color { validate_color(value)?; }
    let mut overrides = colors_file.map(Colors::open).transpose()?;
    let mut graph = GraphLookup::open(&graph_dir.join("edges.bin"))?;
    let mut paths = File::open(path_file)?;
    let mut corridors = BufReader::new(File::open(corridor_file)?);
    // Write to a sibling temporary file so a failed path validation does not
    // replace a previously valid GeoJSON export with an incomplete document.
    let mut tmp_name = destination.as_os_str().to_os_string();
    tmp_name.push(format!(".meadowsweet-{}.partial", std::process::id()));
    let temporary = std::path::PathBuf::from(tmp_name);
    let mut out = BufWriter::new(
        OpenOptions::new().write(true).create_new(true).open(&temporary)
            .with_context(|| format!("creating export staging file {}", temporary.display()))?
    );
    let mut count = 0u64;
    let mut previous_key = None::<String>;

    out.write_all(b"{\"type\":\"FeatureCollection\",\"features\":[")?;
    while let Some(raw) = read_record::<CORRIDOR_RECORD_BYTES>(&mut corridors)? {
        let corridor = Corridor::parse(raw)?;
        let key = corridor.stable_id();
        if let Some(ref previous) = previous_key {
            ensure!(key.as_str() > previous.as_str(), "corridors.bin is not sorted by canonical corridor identity");
        }
        previous_key = Some(key.clone());
        let color = if let Some(overrides) = overrides.as_mut() {
            overrides.color_for(&key)?
                .or_else(|| global_color.map(ToOwned::to_owned))
        } else {
            global_color.map(ToOwned::to_owned)
        };
        if count > 0 { out.write_all(b",")?; }
        write!(
            out,
            "{{\"type\":\"Feature\",\"id\":\"{}\",\"properties\":{{\"corridor_id\":\"{}\",\"route_type\":2,\"from_station\":{{\"osm_type\":{},\"osm_id\":{}}},\"to_station\":{{\"osm_type\":{},\"osm_id\":{}}},\"support_count\":{},\"physical_edge_count\":{}",
            key, key,
            corridor.from.kind, corridor.from.id,
            corridor.to.kind, corridor.to.id,
            corridor.supporting_traversals, corridor.edge_count
        )?;
        if let Some(value) = color {
            write!(out, ",\"color\":\"{}\"", value)?;
        }
        out.write_all(b"},\"geometry\":{\"type\":\"LineString\",\"coordinates\":")?;
        write_geometry(&mut out, &mut paths, &mut graph, &corridor)
            .with_context(|| format!("exporting corridor {key}"))?;
        out.write_all(b"}}")?;
        count += 1;
    }
    if let Some(overrides) = overrides {
        overrides.finish()?;
    }
    out.write_all(b"]}")?;
    out.flush()?;
    drop(out);
    fs::rename(&temporary, destination).with_context(|| {
        format!("publishing staged GeoJSON {} to {}", temporary.display(), destination.display())
    })?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segments::{SegmentWriter, TraversedEdge};
    use crate::corridors::reduce;
    use std::fs;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_dir(label: &str) -> Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "meadowsweet-geojson-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn station(id: i64) -> StationId {
        StationId::new(0, id).unwrap()
    }

    fn graph(dir: &Path) -> Result<()> {
        let mut writer = BufWriter::new(File::create(dir.join("edges.bin"))?);
        // Two physically connected alternatives between the same stations.
        for (id, from, to, a, b) in [
            (10u64, 1i64, 2i64, (8.0f64, 47.0f64), (8.01f64, 47.0f64)),
            (20, 2, 3, (8.01, 47.0), (8.02, 47.01)),
            (30, 2, 4, (8.01, 47.0), (8.02, 46.99)),
            (40, 4, 3, (8.02, 46.99), (8.02, 47.01)),
        ] {
            writer.write_all(&id.to_be_bytes())?;
            writer.write_all(&from.to_be_bytes())?;
            writer.write_all(&to.to_be_bytes())?;
            writer.write_all(&[1u8, 0, 0, 0, 0, 0, 0, 0])?;
            for v in [a.0, a.1, b.0, b.1] {
                writer.write_all(&v.to_bits().to_be_bytes())?;
            }
        }
        writer.flush()?;
        Ok(())
    }

    fn fixture(dir: &Path) -> Result<()> {
        graph(dir)?;
        let mut writer = SegmentWriter::create(&dir.join("paths.bin"), &dir.join("segments.bin"))?;
        writer.append(station(1), [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 20, forward: true, arrival_station: Some(station(3)) },
        ])?;
        writer.append(station(3), [
            TraversedEdge { edge_id: 20, forward: false, arrival_station: None },
            TraversedEdge { edge_id: 10, forward: false, arrival_station: Some(station(1)) },
        ])?;
        writer.append(station(1), [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 30, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 40, forward: true, arrival_station: Some(station(3)) },
        ])?;
        writer.finish()?;
        reduce(
            &dir.join("segments.bin"), &dir.join("paths.bin"), &dir.join("corridors.bin"),
            &dir.join("reduce_tmp"), 88 * 2,
        )?;
        Ok(())
    }

    #[test]
    fn exports_unique_lines_with_optional_color_and_stable_direction() -> Result<()> {
        let dir = sample_dir("features")?;
        fixture(&dir)?;
        let no_color = dir.join("uncolored.geojson");
        assert_eq!(
            export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"), &no_color, None, None)?,
            2
        );
        let data: serde_json::Value = serde_json::from_slice(&fs::read(&no_color)?)?;
        let features = data["features"].as_array().context("missing GeoJSON features")?;
        assert_eq!(features.len(), 2);
        assert!(features.iter().all(|item| item["properties"].get("color").is_none()));
        assert!(features.iter().all(|item| item["properties"]["route_type"] == 2));
        assert!(features.iter().any(|item| item["properties"]["support_count"] == 2));
        assert!(features.iter().any(|item|
            item["geometry"]["coordinates"] == serde_json::json!([
                [8.0, 47.0], [8.01, 47.0], [8.02, 47.01]
            ])
        ));
        let colored = dir.join("colored.geojson");
        export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"), &colored, Some("#E30613"), None)?;
        let data: serde_json::Value = serde_json::from_slice(&fs::read(colored)?)?;
        assert!(data["features"].as_array().unwrap().iter().all(|item|
            item["properties"]["color"] == "#E30613"
        ));
        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn rejects_broken_topology_and_invalid_color() -> Result<()> {
        let dir = sample_dir("invalid")?;
        fixture(&dir)?;
        assert!(export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"),
            &dir.join("bad.geojson"), Some("red"), None).is_err());
        // Remove one physical edge from the indexed graph, preserving the
        // sorted binary format; the exporter must refuse missing references.
        let original = fs::read(dir.join("edges.bin"))?;
        fs::write(dir.join("edges.bin"), [&original[..64], &original[128..]].concat())?;
        assert!(export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"),
            &dir.join("bad.geojson"), None, None).is_err());
        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn opposite_direction_station_loops_export_one_stable_line() -> Result<()> {
        let dir = sample_dir("loops")?;
        let mut edges = BufWriter::new(File::create(dir.join("edges.bin"))?);
        for (id, from, to, start, end) in [
            (10u64, 1i64, 2i64, (8.0f64, 47.0f64), (8.01f64, 47.0f64)),
            (20, 2, 3, (8.01, 47.0), (8.01, 47.01)),
            (40, 3, 1, (8.01, 47.01), (8.0, 47.0)),
        ] {
            edges.write_all(&id.to_be_bytes())?;
            edges.write_all(&from.to_be_bytes())?;
            edges.write_all(&to.to_be_bytes())?;
            edges.write_all(&[1u8, 0, 0, 0, 0, 0, 0, 0])?;
            for coord in [start.0, start.1, end.0, end.1] {
                edges.write_all(&coord.to_bits().to_be_bytes())?;
            }
        }
        edges.flush()?;
        drop(edges);
        let mut writer = SegmentWriter::create(&dir.join("paths.bin"), &dir.join("segments.bin"))?;
        writer.append(station(1), [
            TraversedEdge { edge_id: 10, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 20, forward: true, arrival_station: None },
            TraversedEdge { edge_id: 40, forward: true, arrival_station: Some(station(1)) },
        ])?;
        writer.append(station(1), [
            TraversedEdge { edge_id: 40, forward: false, arrival_station: None },
            TraversedEdge { edge_id: 20, forward: false, arrival_station: None },
            TraversedEdge { edge_id: 10, forward: false, arrival_station: Some(station(1)) },
        ])?;
        writer.finish()?;
        reduce(&dir.join("segments.bin"), &dir.join("paths.bin"), &dir.join("corridors.bin"),
            &dir.join("scratch"), 88 * 2)?;
        assert_eq!(
            export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"),
                &dir.join("loops.geojson"), None, None)?,
            1
        );
        let doc: serde_json::Value = serde_json::from_slice(&fs::read(dir.join("loops.geojson"))?)?;
        assert_eq!(doc["features"][0]["properties"]["support_count"], 2);
        let coordinates = doc["features"][0]["geometry"]["coordinates"].as_array().unwrap();
        assert_eq!(coordinates.len(), 4);
        assert_eq!(coordinates.first(), coordinates.last());
        fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn per_corridor_color_tsv_can_override_default() -> Result<()> {
        let dir = sample_dir("overrides")?;
        fixture(&dir)?;
        export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"),
            &dir.join("plain.geojson"), None, None)?;
        let v: serde_json::Value = serde_json::from_slice(&fs::read(dir.join("plain.geojson"))?)?;
        let id = v["features"][0]["id"].as_str().unwrap();
        fs::write(dir.join("colors.tsv"), format!("{id}\t#0088FF\n"))?;
        export(&dir, &dir.join("corridors.bin"), &dir.join("paths.bin"),
            &dir.join("mapped.geojson"), Some("#111111"), Some(&dir.join("colors.tsv")))?;
        let v: serde_json::Value = serde_json::from_slice(&fs::read(dir.join("mapped.geojson"))?)?;
        assert_eq!(v["features"][0]["properties"]["color"], "#0088FF");
        assert_eq!(v["features"][1]["properties"]["color"], "#111111");
        fs::remove_dir_all(dir)?;
        Ok(())
    }
}
