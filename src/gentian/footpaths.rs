//! Compile directed stop-to-stop transfers directly against Gentian location indexes.
//! The OSR graph and stop coordinates are opened once per regional timetable.
use anyhow::{Context, Result, ensure};
use catenary::routing_common::timetable::Coordinate;
use osr_ffi::{Profile, ffi};
use std::collections::HashMap;
use std::path::Path;

/// Haversine distance is only a conservative geographic candidate filter.
/// Native OSR supplies the actual legal walking path and travel duration.
fn meters(a: Coordinate, b: Coordinate) -> f64 {
    let (alat, alon) = (f64::from(a.lat), f64::from(a.lon));
    let (blat, blon) = (f64::from(b.lat), f64::from(b.lon));
    let dlat = (blat - alat).to_radians();
    let dlon = (blon - alon).to_radians();
    let h = (dlat / 2.0).sin().powi(2)
        + alat.to_radians().cos() * blat.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    12_742_000.0 * h.min(1.0).sqrt().asin()
}

/// Compute all directed transfers into compact outgoing adjacency vectors.
/// Each index is the exact LocationIdx produced by Gentian's timetable writer.
pub fn compute(
    osr_dir: &Path,
    stops: &[Coordinate],
    max_seconds: u32,
    candidate_meters: f64,
) -> Result<Vec<Vec<(u32, u16)>>> {
    ensure!((1..=30_660).contains(&max_seconds), "invalid OSR transfer duration");
    ensure!(candidate_meters.is_finite() && candidate_meters > 0.0,
        "candidate radius must be positive and finite");
    ensure!(u32::try_from(stops.len()).is_ok(), "too many locations for packed indexes");
    const CELL: f64 = 0.02;
    let mut tiles: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
    for (idx, stop) in stops.iter().enumerate() {
        ensure!(stop.lat.is_finite() && stop.lon.is_finite(),
            "non-finite timetable stop coordinate");
        ensure!((-90.0..=90.0).contains(&stop.lat)
                && (-180.0..=180.0).contains(&stop.lon),
            "timetable stop coordinate outside WGS84");
        tiles.entry(((f64::from(stop.lat) / CELL).floor() as i32,
                     (f64::from(stop.lon) / CELL).floor() as i32))
            .or_default().push(idx);
    }
    let graph = ffi::open_router(osr_dir.to_str().context("non-UTF8 OSR directory")?)
        .with_context(|| format!("opening OSR graph {}", osr_dir.display()))?;
    let router = graph.as_ref().context("OSR returned null router")?;
    let options = ffi::RoutingOptions {
        max_cost: max_seconds,
        include_segments: false,
        ..ffi::RoutingOptions::default().profile(Profile::Foot)
    };
    let mut out = Vec::with_capacity(stops.len());
    for (i, &origin) in stops.iter().enumerate() {
        let y = (f64::from(origin.lat) / CELL).floor() as i32;
        let x = (f64::from(origin.lon) / CELL).floor() as i32;
        let lat_cells = (candidate_meters / (111_000.0 * CELL)).ceil() as i32 + 1;
        let lon_meters = 111_000.0 * f64::from(origin.lat).to_radians().cos().abs();
        ensure!(lon_meters > 1.0, "near-polar routing needs a different spatial index");
        let lon_cells = (candidate_meters / (lon_meters * CELL)).ceil() as i32 + 1;
        let mut targets = Vec::new();
        let mut indices = Vec::new();
        for dy in -lat_cells..=lat_cells {
            for dx in -lon_cells..=lon_cells {
                if let Some(candidates) = tiles.get(&(y + dy, x + dx)) {
                    for &j in candidates {
                        let target = stops[j];
                        if i != j && meters(origin, target) <= candidate_meters {
                            indices.push(j);
                            targets.push(ffi::Coordinate::outdoor(
                                f64::from(target.lat), f64::from(target.lon),
                            ));
                        }
                    }
                }
            }
        }
        let mut edges = Vec::new();
        if !targets.is_empty() {
            let routes = router.one_to_many(
                &ffi::Coordinate::outdoor(f64::from(origin.lat), f64::from(origin.lon)),
                &targets, &options, &[],
            )?;
            ensure!(routes.len() == indices.len(), "OSR one-to-many result count mismatch");
            for (target, path) in indices.into_iter().zip(routes) {
                if path.found && path.duration_seconds <= max_seconds {
                    let minutes = path.duration_seconds.div_ceil(60).max(1);
                    ensure!(minutes <= 511, "OSR footpath exceeds packed duration");
                    edges.push((u32::try_from(target)?, u16::try_from(minutes)?));
                }
            }
        }
        out.push(edges);
        if i % 1000 == 0 {
            eprintln!("Gentian OSR transfers: {}/{} locations", i, stops.len());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn geographic_candidate_filter_excludes_distant_stops() {
        let a = Coordinate { lat: 33.646, lon: -117.843 };
        let b = Coordinate { lat: 33.647, lon: -117.843 };
        let c = Coordinate { lat: 34.054, lon: -118.243 };
        assert!(meters(a, b) < 1200.0);
        assert!(meters(a, c) > 1200.0);
        assert_eq!(meters(a, a), 0.0);
    }
}
