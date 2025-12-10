use ahash::AHashMap;
use anyhow::Context;
use ecow::EcoString;
use geo::Point;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ShapePoint {
    pub geometry: Point<f64>,
    pub sequence: usize,
    pub dist_traveled: Option<f32>,
}

#[derive(Deserialize)]
struct RawShape {
    #[serde(rename = "shape_id")]
    pub id: EcoString,
    #[serde(rename = "shape_pt_lat")]
    pub latitude: f64,
    #[serde(rename = "shape_pt_lon")]
    pub longitude: f64,
    #[serde(rename = "shape_pt_sequence")]
    pub sequence: usize,
    #[serde(rename = "shape_dist_traveled")]
    pub dist_traveled: Option<f32>,
}

/// O(n) check; only sort if any sequence is out of order.
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
        // In-place, no extra allocation
        points.sort_unstable_by_key(|p| p.sequence);
    }
}

/// Reads a shapes.txt file and aggregates points into a HashMap.
///
/// This implementation assumes the input CSV is sorted by `shape_id`.
/// It reads row-by-row and flushes the accumulated vector to the map
/// only when the ID changes, preventing excessive allocation of intermediate strings.
pub fn faster_shape_reader(
    path: PathBuf,
) -> Result<AHashMap<String, Vec<ShapePoint>>, Box<dyn Error>> {
    let file = File::open(path).context("Failed to open shapes.txt in the faster shapes reader")?;
    let buf_reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(buf_reader);

    // Estimate capacity to reduce re-allocations (tuning required based on data size)
    let mut shapes: AHashMap<String, Vec<ShapePoint>> = AHashMap::with_capacity(1000);

    // State buffers for the streaming aggregation
    let mut current_points: Vec<ShapePoint> = Vec::with_capacity(500);
    let mut current_shape_id: Option<EcoString> = None;

    for result in rdr.deserialize() {
        let record: RawShape = result?;

        // Check if we have transitioned to a new shape_id
        if let Some(ref curr_id) = current_shape_id {
            if *curr_id != record.id {
                // ID changed: finish the current shape
                if !current_points.is_empty() {
                    maybe_sort_by_sequence(&mut current_points);
                    shapes.insert(curr_id.to_string(), current_points);
                    current_points = Vec::with_capacity(500);
                }

                current_shape_id = Some(record.id.clone());
            }
        } else {
            // First iteration initialization
            current_shape_id = Some(record.id.clone());
        }

        // Convert RawShape to ShapePoint and accumulate
        let point = ShapePoint {
            geometry: Point::new(record.longitude, record.latitude),
            sequence: record.sequence,
            dist_traveled: record.dist_traveled,
        };

        current_points.push(point);
    }

    // Commit the final batch of points after the loop finishes
    if let Some(last_id) = current_shape_id {
        if !current_points.is_empty() {
            maybe_sort_by_sequence(&mut current_points);
            shapes.insert(last_id.to_string(), current_points);
        }
    }

    Ok(shapes)
}
