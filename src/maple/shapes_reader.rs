use ahash::AHashMap;
use anyhow::Context;
use geo::Point;
use memmap2::MmapOptions;
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ShapePoint {
    pub geometry: Point<f64>,
    pub sequence: usize,
    pub dist_traveled: Option<f32>,
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

pub struct MmapShapeReader {
    mmap: memmap2::Mmap,
    pub index: AHashMap<String, (u64, u64)>,
    col_idx_lat: usize,
    col_idx_lon: usize,
    col_idx_seq: usize,
    col_idx_dist: Option<usize>,
}

impl MmapShapeReader {
    pub fn new(path: PathBuf) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let file = File::open(&path).context("Failed to open shapes.txt")?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(std::io::Cursor::new(&mmap));

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

        let mut index = AHashMap::with_capacity(100_000);
        let mut current_shape_id: Vec<u8> = Vec::new();
        let mut current_start: u64 = rdr.position().byte();

        let mut record = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut record)? {
            let shape_id_bytes = &record[col_idx_id];

            if !current_shape_id.is_empty() {
                if current_shape_id != shape_id_bytes {
                    let pos = record.position().unwrap().byte();
                    let shape_id_str = String::from_utf8_lossy(&current_shape_id).into_owned();
                    index.insert(shape_id_str, (current_start, pos));
                    current_start = pos;
                    current_shape_id.clear();
                    current_shape_id.extend_from_slice(shape_id_bytes);
                }
            } else {
                current_shape_id.extend_from_slice(shape_id_bytes);
                current_start = record.position().unwrap().byte();
            }
        }

        if !current_shape_id.is_empty() {
            let shape_id_str = String::from_utf8_lossy(&current_shape_id).into_owned();
            index.insert(shape_id_str, (current_start, mmap.len() as u64));
        }

        Ok(Self {
            mmap,
            index,
            col_idx_lat,
            col_idx_lon,
            col_idx_seq,
            col_idx_dist,
        })
    }

    pub fn get_shape(&self, shape_id: &str) -> Option<Vec<ShapePoint>> {
        let &(start, end) = self.index.get(shape_id)?;
        let slice = &self.mmap[start as usize..end as usize];

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(slice);

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
