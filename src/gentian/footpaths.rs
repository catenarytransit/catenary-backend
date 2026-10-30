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
) -> Result<Vec<Vec<(u32, u32)>>> {
    ensure!(
        (1..=30_660).contains(&max_seconds),
        "invalid OSR transfer duration"
    );
    ensure!(
        candidate_meters.is_finite() && candidate_meters > 0.0,
        "candidate radius must be positive and finite"
    );
    ensure!(
        u32::try_from(stops.len()).is_ok(),
        "too many locations for packed indexes"
    );
    const CELL: f64 = 0.02;
    let mut tiles: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
    for (idx, stop) in stops.iter().enumerate() {
        ensure!(
            stop.lat.is_finite() && stop.lon.is_finite(),
            "non-finite timetable stop coordinate"
        );
        ensure!(
            (-90.0..=90.0).contains(&stop.lat) && (-180.0..=180.0).contains(&stop.lon),
            "timetable stop coordinate outside WGS84"
        );
        tiles
            .entry((
                (f64::from(stop.lat) / CELL).floor() as i32,
                (f64::from(stop.lon) / CELL).floor() as i32,
            ))
            .or_default()
            .push(idx);
    }
    let graph = ffi::open_router(osr_dir.to_str().context("non-UTF8 OSR directory")?)
        .with_context(|| format!("opening OSR graph {}", osr_dir.display()))?;
    let router = graph.as_ref().context("OSR returned null router")?;
    let options = ffi::RoutingOptions {
        max_cost: max_seconds,
        // MOTIS timetable.max_matching_distance_ defaults to 25 metres.
        // The general-purpose osr-ffi default is 100 metres.
        max_match_distance_meters: 25.0,
        include_segments: false,
        ..ffi::RoutingOptions::default().profile(Profile::Foot)
    };
    // MOTIS-style match preprocessing: the same stop appears in many searches.
    // Cache its forward-origin and reverse-destination matches once, retaining
    // the native osr::match_result buckets in C++ rather than re-snapping each
    // stop on every one-to-many call.
    let mut origins = router.create_match_store();
    let mut destinations = router.create_match_store();
    let match_origin = ffi::MatchOptions {
        profile: Profile::Foot as u8,
        reverse: false,
        max_match_distance_meters: 25.0,
        ..ffi::MatchOptions::default()
    };
    let match_destination = ffi::MatchOptions {
        reverse: true,
        ..match_origin.clone()
    };
    for (i, stop) in stops.iter().enumerate() {
        let coordinate = ffi::Coordinate::outdoor(f64::from(stop.lat), f64::from(stop.lon));
        // Profile-independent candidates can be reused for both match roles.
        let raw = router.get_raw_match(&coordinate, 250.0)?;
        let origin_idx =
            router.append_lookup_match(origins.pin_mut(), &coordinate, &match_origin, &raw)?;
        let dest_idx = router.append_lookup_match(
            destinations.pin_mut(),
            &coordinate,
            &match_destination,
            &raw,
        )?;
        ensure!(
            origin_idx as usize == i && dest_idx as usize == i,
            "OSR match-store index is not aligned with timetable stops"
        );
        if i % 10_000 == 0 {
            eprintln!("Gentian OSR matching: {}/{} locations", i, stops.len());
        }
    }
    let origins = origins.as_ref().context("OSR origin match store missing")?;
    let destinations = destinations
        .as_ref()
        .context("OSR destination match store missing")?;
    let mut out = Vec::with_capacity(stops.len());
    for (i, &origin) in stops.iter().enumerate() {
        let y = (f64::from(origin.lat) / CELL).floor() as i32;
        let x = (f64::from(origin.lon) / CELL).floor() as i32;
        let lat_cells = (candidate_meters / (111_000.0 * CELL)).ceil() as i32 + 1;
        let lon_meters = 111_000.0 * f64::from(origin.lat).to_radians().cos().abs();
        ensure!(
            lon_meters > 1.0,
            "near-polar routing needs a different spatial index"
        );
        let lon_cells = (candidate_meters / (lon_meters * CELL)).ceil() as i32 + 1;
        let mut indices = Vec::new();
        for dy in -lat_cells..=lat_cells {
            for dx in -lon_cells..=lon_cells {
                if let Some(candidates) = tiles.get(&(y + dy, x + dx)) {
                    for &j in candidates {
                        let target = stops[j];
                        if i != j && meters(origin, target) <= candidate_meters {
                            indices.push(j);
                        }
                    }
                }
            }
        }
        let mut edges = Vec::new();
        if !indices.is_empty() {
            let destination_indices = indices
                .iter()
                .map(|&idx| u32::try_from(idx))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let routes = router.route_one_to_many_cached(
                origins,
                u32::try_from(i)?,
                destinations,
                &destination_indices,
                &options,
            )?;
            ensure!(
                routes.len() == indices.len(),
                "OSR one-to-many result count mismatch"
            );
            for (target, path) in indices.into_iter().zip(routes) {
                if path.found && path.duration_seconds <= max_seconds {
                    // The native router reports seconds. Preserve them in the
                    // compiled timetable instead of rounding to whole minutes.
                    let seconds = path.duration_seconds.max(1);
                    edges.push((u32::try_from(target)?, seconds));
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

/// Bound and transitively close the directed walking graph at compile time.
/// Runtime RAPTOR can relax these shortcuts once per round rather than
/// running a potentially unbounded Dijkstra from every marked stop.
/// Durations are exact seconds. Only paths <= max_seconds are materialized.
/// To avoid quadratic blow-up, the closure fails explicitly if the configured
/// transfer network cannot fit the per-source edge budget.
pub fn bounded_closure(
    edges: &[Vec<(u32, u32)>],
    max_seconds: u32,
) -> Result<Vec<Vec<(u32, u32)>>> {
    use std::cmp::Reverse;
    use std::collections::{BinaryHeap, HashMap};
    ensure!(
        (1..=3600).contains(&max_seconds),
        "invalid closure duration"
    );
    let mut result = Vec::with_capacity(edges.len());
    for source in 0..edges.len() {
        let mut distances = HashMap::<usize, u32>::new();
        let mut queue = BinaryHeap::new();
        distances.insert(source, 0);
        queue.push(Reverse((0u32, source)));
        while let Some(Reverse((elapsed, from))) = queue.pop() {
            if distances.get(&from) != Some(&elapsed) {
                continue;
            }
            for &(target, seconds) in &edges[from] {
                let to = target as usize;
                ensure!(to < edges.len(), "transfer destination out of range");
                if seconds == 0 {
                    continue;
                }
                let Some(next) = elapsed.checked_add(seconds) else {
                    continue;
                };
                if next > max_seconds {
                    continue;
                }
                if next < *distances.get(&to).unwrap_or(&u32::MAX) {
                    distances.insert(to, next);
                    queue.push(Reverse((next, to)));
                }
            }
        }
        distances.remove(&source);
        let mut shortcuts: Vec<_> = distances
            .into_iter()
            .map(|(idx, seconds)| (idx as u32, seconds))
            .collect();
        shortcuts.sort_unstable_by_key(|&(idx, duration)| (duration, idx));
        result.push(shortcuts);
    }
    Ok(result)
}

/// Nigiri also creates local links between the parent station and its
/// platforms. Only fill gaps in native OSR's results, for station members
/// within MOTIS's 100m nearby-stop default. This fallback is deliberately
/// conservative: it never connects unrelated stations, never replaces a
/// routed OSR duration, and requires matching GTFS source identities.
pub fn add_station_links(
    tt: &catenary::routing_common::timetable::TimetableOwned,
    edges: &mut [Vec<(u32, u32)>],
) -> Result<()> {
    use catenary::routing_common::timetable::LocationIdx;
    ensure!(
        edges.len() == tt.coordinates.len(),
        "station transfer count mismatch"
    );
    let mut children: HashMap<u32, Vec<usize>> = HashMap::new();
    for (child, &parent) in tt.location_parent.iter().enumerate() {
        if parent != LocationIdx::INVALID {
            ensure!(parent.idx() < edges.len(), "invalid parent station");
            if tt.location_type[parent.idx()] == 2
                && tt.location_source[child] == tt.location_source[parent.idx()]
            {
                children.entry(parent.0).or_default().push(child);
            }
        }
    }
    for (parent, child_stops) in children {
        let mut members = Vec::with_capacity(child_stops.len() + 1);
        members.push(parent as usize);
        members.extend(child_stops);
        for &from in &members {
            for &to in &members {
                if from == to
                    || edges[from]
                        .iter()
                        .any(|&(existing, _)| existing as usize == to)
                {
                    continue;
                }
                let distance = meters(tt.coordinates[from], tt.coordinates[to]);
                if distance > 100.0 {
                    continue;
                }
                // Nigiri uses a conservative 0.7m/s for missing links.
                let seconds = (distance / 0.7).ceil() as u32;
                let seconds = seconds
                    .max(1)
                    .max(tt.location_transfer_seconds[from])
                    .max(tt.location_transfer_seconds[to]);
                edges[from].push((u32::try_from(to)?, seconds));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closure_is_directed_and_bounded() {
        let edges = vec![vec![(1, 240)], vec![(2, 250)], vec![]];
        let closure = bounded_closure(&edges, 480).unwrap();
        assert!(closure[0].contains(&(1, 240)));
        assert!(!closure[0].iter().any(|&(id, _)| id == 2));
        assert!(closure[2].is_empty());
        let closure = bounded_closure(&edges, 500).unwrap();
        assert!(closure[0].contains(&(2, 490)));
    }

    #[test]
    fn same_station_local_links_fill_unmatched_platforms_only() {
        use catenary::routing_common::timetable::{LocationIdx, SourceIdx, TimetableOwned};
        let mut tt = TimetableOwned::empty(0, 1);
        for (lat, lon, parent, kind) in [
            (33.0, -117.0, LocationIdx::INVALID, 2u8),
            (33.0001, -117.0, LocationIdx(0), 1),
            (33.0002, -117.0, LocationIdx(0), 1),
            (34.0, -118.0, LocationIdx::INVALID, 2),
        ] {
            tt.coordinates.push(Coordinate { lat, lon });
            tt.location_parent.push(parent);
            tt.location_type.push(kind);
            tt.location_transfer_seconds.push(120);
            tt.location_source.push(SourceIdx(0));
        }
        let mut paths = vec![vec![], vec![(2, 188)], vec![], vec![]];
        add_station_links(&tt, &mut paths).unwrap();
        assert!(paths[0].iter().any(|&(to, t)| to == 1 && t >= 120));
        assert!(paths[1].iter().any(|&(to, t)| to == 2 && t == 188));
        assert!(paths[2].iter().any(|&(to, _)| to == 1));
        assert!(paths[3].is_empty());
    }

    #[test]
    fn geographic_candidate_filter_excludes_distant_stops() {
        let a = Coordinate {
            lat: 33.646,
            lon: -117.843,
        };
        let b = Coordinate {
            lat: 33.647,
            lon: -117.843,
        };
        let c = Coordinate {
            lat: 34.054,
            lon: -118.243,
        };
        assert!(meters(a, b) < 1200.0);
        assert!(meters(a, c) > 1200.0);
        assert_eq!(meters(a, a), 0.0);
    }
}
