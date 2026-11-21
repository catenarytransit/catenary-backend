//! Bounded-memory station-to-railway *candidate* anchoring.
//!
//! This stage never connects a station to a track merely because it is near.
//! It generates spatially plausible, independently verifiable candidates.
//! Final anchoring needs a connected GTFS traversal and the station type.
//!
//! Stations are streamed from PostGIS, preserving (osm_type, osm_id).
//! Physical edges are distributed to small grid cells in fixed-width files,
//! externally sorted, then joined against one cell's stations at a time.
use crate::external_sort::{read_record, sort_fixed};
use anyhow::{Context, Result, bail};
use postgres::{Client, NoTls};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

const CELL_DEG: f64 = 0.01;
const METERS_PER_DEG_LAT: f64 = 111_320.0;
const MAX_CELL_EDGE_COUNT: usize = 100_000;
const MAX_EDGE_FANOUT: i64 = 4096;
const MAX_CANDIDATES: usize = 8;
const SOURCE_EDGE_LEN: usize = 64;
const TILED_EDGE_LEN: usize = 72;
const TILED_STATION_LEN: usize = 40;

fn tile_key(x: i32, y: i32) -> [u8; 8] {
    let mut result = [0u8; 8];
    result[..4].copy_from_slice(&(x as u32 ^ 0x8000_0000).to_be_bytes());
    result[4..].copy_from_slice(&(y as u32 ^ 0x8000_0000).to_be_bytes());
    result
}

fn grid_cell(lon: f64, lat: f64) -> (i32, i32) {
    ((lon / CELL_DEG).floor() as i32, (lat / CELL_DEG).floor() as i32)
}

fn station_type(value: &str) -> Option<u8> {
    match value {
        "node" => Some(0),
        "way" => Some(1),
        "relation" => Some(2),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
struct Edge {
    id: u64,
    mode: u8,
    from: (f64, f64),
    to: (f64, f64),
}

fn edge_from_record(raw: &[u8]) -> Result<Edge> {
    if raw.len() != SOURCE_EDGE_LEN {
        bail!("wrong physical edge record length");
    }
    let id = u64::from_be_bytes(raw[0..8].try_into()?);
    let mode = raw[24];
    let coords = [
        f64::from_bits(u64::from_be_bytes(raw[32..40].try_into()?)),
        f64::from_bits(u64::from_be_bytes(raw[40..48].try_into()?)),
        f64::from_bits(u64::from_be_bytes(raw[48..56].try_into()?)),
        f64::from_bits(u64::from_be_bytes(raw[56..64].try_into()?)),
    ];
    if coords.iter().any(|v| !v.is_finite()) {
        bail!("non-finite OSM edge coordinate at edge {id}");
    }
    Ok(Edge { id, mode, from: (coords[0], coords[1]), to: (coords[2], coords[3]) })
}

fn edge_range(e: Edge, radius_m: f64) -> Result<(i32, i32, i32, i32)> {
    let max_lat = e.from.1.abs().max(e.to.1.abs()).min(89.0);
    let cos = max_lat.to_radians().cos().max(0.01);
    let dx = radius_m / (METERS_PER_DEG_LAT * cos);
    let dy = radius_m / METERS_PER_DEG_LAT;
    let (min_x, min_y) = grid_cell(e.from.0.min(e.to.0) - dx, e.from.1.min(e.to.1) - dy);
    let (max_x, max_y) = grid_cell(e.from.0.max(e.to.0) + dx, e.from.1.max(e.to.1) + dy);
    let count = (i64::from(max_x) - i64::from(min_x) + 1)
        .checked_mul(i64::from(max_y) - i64::from(min_y) + 1)
        .context("cell replication count overflow")?;
    if count > MAX_EDGE_FANOUT {
        bail!("edge {} covers {count} station cells; subdivide the OSM edge before anchoring", e.id);
    }
    Ok((min_x, min_y, max_x, max_y))
}

/// Distance to a *physical edge* in a local tangent approximation.
/// Stores projection fraction even for stations on a long edge.
fn projection(e: Edge, lon: f64, lat: f64) -> (f64, f64, f64, f64) {
    let cos = lat.to_radians().cos().max(0.01);
    let sx = METERS_PER_DEG_LAT * cos;
    let sy = METERS_PER_DEG_LAT;
    let a = ((e.from.0 - lon) * sx, (e.from.1 - lat) * sy);
    let b = ((e.to.0 - lon) * sx, (e.to.1 - lat) * sy);
    let vx = b.0 - a.0;
    let vy = b.1 - a.1;
    let len2 = vx * vx + vy * vy;
    let fraction = if len2 == 0.0 { 0.0 } else { (-(a.0 * vx + a.1 * vy) / len2).clamp(0.0, 1.0) };
    let near_x = a.0 + fraction * vx;
    let near_y = a.1 + fraction * vy;
    let distance = near_x.hypot(near_y);
    (fraction, lon + near_x / sx, lat + near_y / sy, distance)
}

#[derive(Clone, Copy)]
struct Candidate {
    edge: u64,
    fraction: f64,
    lon: f64,
    lat: f64,
    distance: f64,
}

fn candidates(edges: &[Edge], lon: f64, lat: f64, radius: f64) -> Vec<Candidate> {
    let mut best = Vec::new();
    for &edge in edges {
        // Meadowsweet's first rail build handles mainline / narrow gauge only;
        // urban rail belongs to the local-transport modules.
        if !matches!(edge.mode, 1 | 2) { continue; }
        let (fraction, near_lon, near_lat, distance) = projection(edge, lon, lat);
        if distance > radius { continue; }
        best.push(Candidate { edge: edge.id, fraction, lon: near_lon, lat: near_lat, distance });
    }
    best.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance).then(a.edge.cmp(&b.edge)));
    best.truncate(MAX_CANDIDATES);
    best
}

fn write_candidate(
    out: &mut impl Write,
    station_id: i64,
    osm_type: u8,
    c: Candidate,
) -> Result<()> {
    out.write_all(&station_id.to_be_bytes())?;
    out.write_all(&[osm_type])?;
    out.write_all(&[0; 7])?;
    out.write_all(&c.edge.to_be_bytes())?;
    out.write_all(&c.lon.to_bits().to_be_bytes())?;
    out.write_all(&c.lat.to_bits().to_be_bytes())?;
    out.write_all(&c.fraction.to_bits().to_be_bytes())?;
    out.write_all(&c.distance.to_bits().to_be_bytes())?;
    Ok(())
}

fn flush_stations(
    stations: &mut impl std::io::Read,
    source_edges: &[Edge],
    output: &mut impl Write,
    radius: f64,
    count: &mut u64,
    unresolved: &mut u64,
) -> Result<()> {
    while let Some(row) = read_record::<TILED_STATION_LEN>(stations)? {
        let station_id = i64::from_be_bytes(row[8..16].try_into()?);
        let osm_type = row[16];
        let lon = f64::from_bits(u64::from_be_bytes(row[24..32].try_into()?));
        let lat = f64::from_bits(u64::from_be_bytes(row[32..40].try_into()?));
        let ranked = candidates(source_edges, lon, lat, radius);
        if ranked.is_empty() {
            *unresolved += 1;
        }
        for candidate in ranked {
            write_candidate(output, station_id, osm_type, candidate)?;
            *count += 1;
        }
    }
    Ok(())
}

/// PostgreSQL connection is read-only: this does not update station matches.
/// Excludes child platforms and stop positions, using parent OSM stations.
/// Query uses DISTINCT ON to choose the most recent OSM import per
/// (osm_type,osm_id), but doesn't discard distinct OSM identity types.
pub fn build(
    database_url: &str,
    graph_dir: &Path,
    output_dir: &Path,
    radius_m: f64,
    memory_bytes: usize,
) -> Result<()> {
    if !radius_m.is_finite() || !(0.0..=2_000.0).contains(&radius_m) || radius_m == 0.0 {
        bail!("station candidate radius must be >0 and <=2000 meters");
    }
    fs::create_dir_all(output_dir)?;
    let scratch = output_dir.join("scratch");
    if scratch.exists() {
        bail!("station-anchor scratch directory exists: {}", scratch.display());
    }
    fs::create_dir_all(&scratch)?;
    let mut client = Client::connect(database_url, NoTls)?;

    let mut station_out = BufWriter::new(File::create(scratch.join("stations.raw"))?);
    let query = r#"
        SELECT DISTINCT ON (osm_type, osm_id)
            osm_type, osm_id, ST_X(point)::float8, ST_Y(point)::float8
        FROM gtfs.osm_stations
        WHERE mode_type = 'rail'
          AND parent_osm_id IS NULL
          AND (station_type IN ('station', 'halt', 'stop_area')
               OR railway_tag IN ('station', 'halt'))
        ORDER BY osm_type, osm_id, import_id DESC
    "#;
    let mut station_total = 0u64;
    let rows = client.query_raw(query, &[] as &[&(dyn postgres::types::ToSql + Sync)])?;
    for result in rows {
        let row = result?;
        let name: String = row.get(0);
        let Some(osm_type) = station_type(&name) else { continue; };
        let id: i64 = row.get(1);
        let lon: f64 = row.get(2);
        let lat: f64 = row.get(3);
        if !lon.is_finite() || !lat.is_finite() || !(-180.0..=180.0).contains(&lon)
            || !(-85.0..=85.0).contains(&lat) { continue; }
        let (x, y) = grid_cell(lon, lat);
        station_out.write_all(&tile_key(x, y))?;
        station_out.write_all(&id.to_be_bytes())?;
        station_out.write_all(&[osm_type])?;
        station_out.write_all(&[0; 7])?;
        station_out.write_all(&lon.to_bits().to_be_bytes())?;
        station_out.write_all(&lat.to_bits().to_be_bytes())?;
        station_total += 1;
    }
    station_out.flush()?;
    drop(station_out);
    sort_fixed::<TILED_STATION_LEN>(
        &scratch.join("stations.raw"),
        &scratch.join("stations.sorted"),
        &scratch.join("station_runs"),
        memory_bytes, 16,
    )?;

    let mut edge_reader = BufReader::new(File::open(graph_dir.join("edges.bin"))?);
    let mut tiled = BufWriter::new(File::create(scratch.join("edges.raw"))?);
    let mut total_edges = 0u64;
    while let Some(row) = read_record::<SOURCE_EDGE_LEN>(&mut edge_reader)? {
        let edge = edge_from_record(&row)?;
        // In this stage a station is a mainline rail station, so skip urban
        // infrastructure before generating tile fanout.
        if !matches!(edge.mode, 1 | 2) { continue; }
        let (min_x, min_y, max_x, max_y) = edge_range(edge, radius_m)?;
        for x in min_x..=max_x {
            for y in min_y..=max_y {
                tiled.write_all(&tile_key(x, y))?;
                tiled.write_all(&row)?;
            }
        }
        total_edges += 1;
    }
    tiled.flush()?;
    drop(tiled);
    sort_fixed::<TILED_EDGE_LEN>(
        &scratch.join("edges.raw"),
        &scratch.join("edges.sorted"),
        &scratch.join("edge_runs"),
        memory_bytes, 16,
    )?;

    // Group sorted edge records by tile and stream station rows in lockstep.
    // Only one grid cell of edges is ever retained in RAM. Very dense tiles
    // explicitly error rather than silently exceeding the allocation budget.
    let mut stations = BufReader::new(File::open(scratch.join("stations.sorted"))?);
    let mut edges = BufReader::new(File::open(scratch.join("edges.sorted"))?);
    let mut output = BufWriter::new(File::create(output_dir.join("anchor_candidates.bin"))?);
    let mut station = read_record::<TILED_STATION_LEN>(&mut stations)?;
    let mut edge_row = read_record::<TILED_EDGE_LEN>(&mut edges)?;
    let mut anchors = 0u64;
    let mut unresolved = 0u64;
    let mut cells = 0u64;
    while let Some(current_station) = station {
        let key = &current_station[..8];
        while let Some(e) = edge_row {
            if &e[..8] >= key { break; }
            edge_row = read_record::<TILED_EDGE_LEN>(&mut edges)?;
        }
        let mut local_edges = Vec::<Edge>::new();
        while let Some(e) = edge_row {
            if &e[..8] != key { break; }
            if local_edges.len() == MAX_CELL_EDGE_COUNT {
                bail!("station anchoring grid cell exceeded {} rail edges", MAX_CELL_EDGE_COUNT);
            }
            local_edges.push(edge_from_record(&e[8..])?);
            edge_row = read_record::<TILED_EDGE_LEN>(&mut edges)?;
        }
        // Process the station group in this cell without accumulating stations.
        loop {
            let Some(ref row) = station else { break };
            if &row[..8] != key { break; }
            let mut one = &row[..];
            flush_stations(&mut one, &local_edges, &mut output, radius_m, &mut anchors, &mut unresolved)?;
            station = read_record::<TILED_STATION_LEN>(&mut stations)?;
        }
        cells += 1;
    }
    output.flush()?;
    std::fs::write(
        output_dir.join("manifest.txt"),
        format!(
            "format=meadowsweet-anchor-candidates-v1\nsource_station_count={station_total}\nphysical_edge_count={total_edges}\nstation_cells={cells}\ncandidate_count={anchors}\nunresolved_station_count={unresolved}\nrecord_bytes=56\nradius_m={radius_m}\n"
        ),
    )?;
    fs::remove_dir_all(&scratch)?;
    eprintln!("Meadowsweet: anchored {anchors} candidates for {station_total} stations ({unresolved} with no candidate)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_projection_can_be_inside_a_long_edge() {
        let edge = Edge { id: 1, mode: 1, from: (0.0, 0.0), to: (2.0, 0.0) };
        let (fraction, lon, lat, dist) = projection(edge, 1.0, 0.001);
        assert!((fraction - 0.5).abs() < 1e-8);
        assert!((lon - 1.0).abs() < 1e-8);
        assert!(lat.abs() < 1e-8);
        assert!(dist > 100.0 && dist < 120.0);
    }

    #[test]
    fn competing_tracks_are_not_automatically_conflated() {
        let edges = [
            Edge { id: 1, mode: 1, from: (0.0, 0.0), to: (0.01, 0.0) },
            Edge { id: 2, mode: 1, from: (0.0, 0.0001), to: (0.01, 0.0001) },
        ];
        let found = candidates(&edges, 0.005, 0.00004, 50.0);
        assert_eq!(found.len(), 2);
        assert_eq!((found[0].edge, found[1].edge), (1, 2));
    }

    #[test]
    fn no_candidate_when_line_is_far_away() {
        let edges = [Edge { id: 1, mode: 1, from: (0.0, 0.0), to: (0.01, 0.0) }];
        assert!(candidates(&edges, 10.0, 10.0, 100.0).is_empty());
    }

    #[test]
    fn distinct_osm_types_are_preserved() {
        assert_eq!(station_type("node"), Some(0));
        assert_eq!(station_type("way"), Some(1));
        assert_eq!(station_type("relation"), Some(2));
        assert_eq!(station_type("unknown"), None);
    }
}
