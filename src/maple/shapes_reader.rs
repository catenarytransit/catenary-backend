use ahash::AHashMap;
use anyhow::Context;
use geo::Point;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[derive(Debug, Clone)]
pub struct ShapePoint {
    pub geometry: Point<f64>,
    pub sequence: usize,
    pub dist_traveled: Option<f32>,
}

#[derive(Debug, Clone, Copy)]
pub struct ShapeRange {
    pub start: u64,
    pub end: u64,
}

fn maybe_sort_by_sequence(points: &mut Vec<ShapePoint>) {
    if points.len() < 2 {
        return;
    }

    let mut prev = points[0].sequence;
    let mut sorted = true;

    for p in points.iter().skip(1) {
        if p.sequence < prev {
            sorted = false;
            break;
        }
        prev = p.sequence;
    }

    if !sorted {
        points.sort_unstable_by_key(|p| p.sequence);
    }
}

/// Disk-backed shape reader.
///
/// This intentionally keeps the old `MmapShapeReader` public name so existing call sites do not
/// need to change, but it no longer stores a full-file mmap. The 12GB `shapes.txt` stays on disk;
/// this struct keeps only a byte-range index and uses positioned reads for each requested shape.
pub struct MmapShapeReader {
    file: File,
    index: AHashMap<Box<str>, ShapeRange>,
    col_idx_lat: usize,
    col_idx_lon: usize,
    col_idx_seq: usize,
    col_idx_dist: Option<usize>,
}

impl MmapShapeReader {
    pub fn new(path: PathBuf) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let file = File::open(&path).context("Failed to open shapes.txt")?;

        // Use a separate handle for the one-time index scan. Do not mmap the full file; scanning a
        // 12GB mmap faults the file into process RSS/cgroup accounting and can OOM large feeds.
        let scan_file = File::open(&path).context("Failed to open shapes.txt for scan")?;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(scan_file);

        let headers = rdr.headers()?.clone();
        let col_idx_id = headers
            .iter()
            .position(|h| h == "shape_id")
            .context("Missing shape_id")?;
        let col_idx_lat = headers
            .iter()
            .position(|h| h == "shape_pt_lat")
            .context("Missing shape_pt_lat")?;
        let col_idx_lon = headers
            .iter()
            .position(|h| h == "shape_pt_lon")
            .context("Missing shape_pt_lon")?;
        let col_idx_seq = headers
            .iter()
            .position(|h| h == "shape_pt_sequence")
            .context("Missing shape_pt_sequence")?;
        let col_idx_dist = headers.iter().position(|h| h == "shape_dist_traveled");

        let mut index: AHashMap<Box<str>, ShapeRange> = AHashMap::with_capacity(100_000);
        let mut current_shape_id: Vec<u8> = Vec::new();
        let mut current_start: u64 = rdr.position().byte();

        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            let shape_id_bytes = record
                .get(col_idx_id)
                .context("shape_id column missing from record")?;
            let record_start = record
                .position()
                .context("csv record position unavailable")?
                .byte();

            if current_shape_id.is_empty() {
                current_shape_id.extend_from_slice(shape_id_bytes);
                current_start = record_start;
                continue;
            }

            if current_shape_id.as_slice() != shape_id_bytes {
                let shape_id = String::from_utf8_lossy(&current_shape_id).into_owned();
                index.insert(
                    shape_id.into_boxed_str(),
                    ShapeRange {
                        start: current_start,
                        end: record_start,
                    },
                );

                current_shape_id.clear();
                current_shape_id.extend_from_slice(shape_id_bytes);
                current_start = record_start;
            }
        }

        if !current_shape_id.is_empty() {
            let shape_id = String::from_utf8_lossy(&current_shape_id).into_owned();
            index.insert(
                shape_id.into_boxed_str(),
                ShapeRange {
                    start: current_start,
                    end: file.metadata()?.len(),
                },
            );
        }

        Ok(Self {
            file,
            index,
            col_idx_lat,
            col_idx_lon,
            col_idx_seq,
            col_idx_dist,
        })
    }

    pub fn shape_ids(&self) -> impl Iterator<Item = &str> {
        self.index.keys().map(|shape_id| shape_id.as_ref())
    }

    pub fn get_shape(&self, shape_id: &str) -> Option<Vec<ShapePoint>> {
        let range = *self.index.get(shape_id)?;
        let reader = FileRangeReader {
            file: &self.file,
            pos: range.start,
            end: range.end,
        };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(reader);

        let mut points = Vec::with_capacity(500);
        let mut record = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut record).unwrap_or(false) {
            let lat: f64 = std::str::from_utf8(record.get(self.col_idx_lat)?)
                .ok()?
                .parse()
                .ok()?;
            let lon: f64 = std::str::from_utf8(record.get(self.col_idx_lon)?)
                .ok()?
                .parse()
                .ok()?;
            let seq: usize = std::str::from_utf8(record.get(self.col_idx_seq)?)
                .ok()?
                .parse()
                .ok()?;

            let dist_traveled = self
                .col_idx_dist
                .and_then(|idx| record.get(idx))
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .and_then(|s| s.parse().ok());

            points.push(ShapePoint {
                geometry: Point::new(lon, lat),
                sequence: seq,
                dist_traveled,
            });
        }

        maybe_sort_by_sequence(&mut points);
        Some(points)
    }
}

struct FileRangeReader<'a> {
    file: &'a File,
    pos: u64,
    end: u64,
}

impl Read for FileRangeReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.end || buf.is_empty() {
            return Ok(0);
        }

        let max_len = ((self.end - self.pos) as usize).min(buf.len());
        let n = read_at_portable(self.file, &mut buf[..max_len], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(unix)]
fn read_at_portable(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_at_portable(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.seek_read(buf, offset)
}
