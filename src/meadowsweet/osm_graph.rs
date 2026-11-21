//! External-memory OSM railway graph materialization.
//!
//! Scans the PBF twice and joins sorted node coordinates to sorted railway
//! endpoints. A railway edge is one adjacent pair of OSM way nodes. Every
//! stage uses fixed-width records and bounded external sorting.
//!
//! This is an *infrastructure* graph, not a GTFS transit line graph.
use crate::external_sort::{read_record, sort_fixed};
use anyhow::{Context, Result, bail};
use osmpbfreader::{OsmObj, OsmPbfReader};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

fn read_i64(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(bytes.try_into().expect("validated record field"))
}
fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("validated record field"))
}
fn is_rail_way(way: &osmpbfreader::Way) -> bool {
    matches!(
        way.tags.get("railway").map(|value| value.as_str()),
        Some("rail" | "narrow_gauge" | "subway" | "light_rail" | "tram")
    ) && !matches!(way.tags.get("service").map(|s| s.as_str()), Some("yard"))
}

pub fn build(pbf: &Path, out: &Path, memory_bytes: usize) -> Result<()> {
    if !out.exists() {
        fs::create_dir_all(out)?;
    }
    let work = out.join("scratch");
    if work.exists() {
        bail!("scratch directory already exists: {}", work.display());
    }
    fs::create_dir_all(&work)?;

    // Edge: edge_id / source node / target node (24 bytes).
    // Endpoint ref: node_id / edge_id / endpoint(0/1) (24 bytes).
    let mut edges = BufWriter::new(File::create(work.join("edges.raw"))?);
    let mut refs = BufWriter::new(File::create(work.join("refs.raw"))?);
    let mut count = 0u64;
    {
        let mut pbf_reader = OsmPbfReader::new(BufReader::new(File::open(pbf)?));
        for obj in pbf_reader.iter() {
            if let OsmObj::Way(way) = obj? {
                if !is_rail_way(&way) { continue; }
                if way.nodes.len().saturating_sub(1) > u16::MAX as usize {
                    bail!("OSM way {} contains more than 65535 segments", way.id.0);
                }
                if way.id.0 < 0 || (way.id.0 as u64) >= (1u64 << 48) {
                    bail!("OSM way ID out of encodable range: {}", way.id.0);
                }
                for (idx, pair) in way.nodes.windows(2).enumerate() {
                    let a = pair[0].0;
                    let b = pair[1].0;
                    if a == b { continue; }
                    let id = ((way.id.0 as u64) << 16) | idx as u64;
                    edges.write_all(&id.to_be_bytes())?;
                    edges.write_all(&a.to_be_bytes())?;
                    edges.write_all(&b.to_be_bytes())?;
                    for (node, side) in [(a, 0u64), (b, 1u64)] {
                        refs.write_all(&node.to_be_bytes())?;
                        refs.write_all(&id.to_be_bytes())?;
                        refs.write_all(&side.to_be_bytes())?;
                    }
                    count += 1;
                }
            }
        }
    }
    edges.flush()?;
    refs.flush()?;
    drop(edges);
    drop(refs);
    eprintln!("Meadowsweet: collected {count} physical railway edges");

    // Scan node coordinates without retaining a global node-id hash table.
    // Node record: id / longitude bits / latitude bits (24 bytes).
    let mut coords = BufWriter::new(File::create(work.join("nodes.raw"))?);
    {
        let mut pbf_reader = OsmPbfReader::new(BufReader::new(File::open(pbf)?));
        for obj in pbf_reader.iter() {
            if let OsmObj::Node(node) = obj? {
                coords.write_all(&node.id.0.to_be_bytes())?;
                coords.write_all(&node.lon().to_bits().to_be_bytes())?;
                coords.write_all(&node.lat().to_bits().to_be_bytes())?;
            }
        }
    }
    coords.flush()?;
    drop(coords);

    for (name, width) in [("nodes",24usize),("refs",24),("edges",24)] {
        // Record lengths are fixed at 24 bytes in these three streams.
        assert_eq!(width, 24);
        sort_fixed::<24>(
            &work.join(format!("{name}.raw")),
            &work.join(format!("{name}.sorted")),
            &work.join(format!("runs_{name}")),
            memory_bytes,
            16,
        )?;
    }

    // Sorted merge join: nodes -> references -> keyed endpoint geometry.
    let mut nodes = BufReader::new(File::open(work.join("nodes.sorted"))?);
    let mut refs = BufReader::new(File::open(work.join("refs.sorted"))?);
    let mut endpoints = BufWriter::new(File::create(work.join("endpoints.raw"))?);
    let mut node = read_record::<24>(&mut nodes)?;
    while let Some(reference) = read_record::<24>(&mut refs)? {
        let target = &reference[0..8];
        while let Some(current) = node {
            if &current[0..8] >= target {
                break;
            }
            node = read_record::<24>(&mut nodes)?;
        }
        let Some(current) = node else {
            bail!("missing OSM node {} from PBF", read_i64(target));
        };
        if &current[0..8] != target {
            bail!("missing referenced OSM railway node {}", read_i64(target));
        }
        // edge_id / side / lon_bits / lat_bits = 32 bytes.
        endpoints.write_all(&reference[8..24])?;
        endpoints.write_all(&current[8..24])?;
    }
    endpoints.flush()?;
    drop(endpoints);
    sort_fixed::<32>(
        &work.join("endpoints.raw"),
        &work.join("endpoints.sorted"),
        &work.join("runs_endpoints"),
        memory_bytes,
        16,
    )?;

    // Merge keyed edge endpoints to a compact edge file and adjacency list.
    // Physical edge record is exactly 56 bytes:
    // id, from OSM node, to OSM node, from lon/lat, to lon/lat.
    let mut endpoint_reader = BufReader::new(File::open(work.join("endpoints.sorted"))?);
    let mut edge_reader = BufReader::new(File::open(work.join("edges.sorted"))?);
    let mut output = BufWriter::new(File::create(out.join("edges.bin"))?);
    let mut adjacency = BufWriter::new(File::create(work.join("adjacency.raw"))?);
    let mut written = 0u64;
    while let Some(edge) = read_record::<24>(&mut edge_reader)? {
        let a = read_record::<32>(&mut endpoint_reader)?.context("missing first edge endpoint")?;
        let b = read_record::<32>(&mut endpoint_reader)?.context("missing second edge endpoint")?;
        let id = read_u64(&edge[0..8]);
        if read_u64(&a[0..8]) != id || read_u64(&b[0..8]) != id ||
            read_u64(&a[8..16]) != 0 || read_u64(&b[8..16]) != 1 {
            bail!("out-of-order or missing edge endpoint at id {id}");
        }
        output.write_all(&edge)?;
        output.write_all(&a[16..32])?;
        output.write_all(&b[16..32])?;
        for (node, neighbor) in [(&edge[8..16], &edge[16..24]), (&edge[16..24], &edge[8..16])] {
            adjacency.write_all(node)?;
            adjacency.write_all(&edge[0..8])?;
            adjacency.write_all(neighbor)?;
        }
        written += 1;
    }
    if read_record::<32>(&mut endpoint_reader)?.is_some() {
        bail!("unexpected trailing endpoint records");
    }
    output.flush()?;
    adjacency.flush()?;
    drop(output);
    drop(adjacency);
    sort_fixed::<24>(
        &work.join("adjacency.raw"),
        &out.join("adjacency.bin"),
        &work.join("runs_adjacency"),
        memory_bytes,
        16,
    )?;
    // Keep complete topology and source identity; no station-pair shortcuts.
    std::fs::write(
        out.join("manifest.txt"),
        format!("format=meadowsweet-osm-v1\nedge_count={written}\nedge_record_bytes=56\nadjacency_record_bytes=24\n"),
    )?;
    fs::remove_dir_all(&work).context("remove successful graph-build scratch data")?;
    eprintln!("Meadowsweet: wrote {written} railway edges and {} adjacency records", written * 2);
    Ok(())
}
