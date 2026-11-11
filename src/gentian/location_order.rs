//! Experimental numbering only: route/transport order and route travel order stay fixed.
use anyhow::{Result, ensure};
use catenary::routing_common::timetable::{LocationIdx, PackedStop, TimetableOwned};

// Standard xy-to-Hilbert distance on a 16-bit square. Quantization uses the
// regional bounding box (one scale for both axes), with old index as tie-break.
fn distance(mut x: u32, mut y: u32) -> u64 {
    let mut d = 0_u64;
    let mut s = 1_u32 << 15;
    while s != 0 {
        let rx = u32::from(x & s != 0);
        let ry = u32::from(y & s != 0);
        d += u64::from(s) * u64::from(s) * u64::from((3 * rx) ^ ry);
        if ry == 0 {
            if rx == 1 {
                x = 65535 - x;
                y = 65535 - y;
            }
            std::mem::swap(&mut x, &mut y);
        }
        s >>= 1;
    }
    d
}

pub(super) fn apply(tt: &mut TimetableOwned) -> Result<()> {
    let n = tt.coordinates.len();
    ensure!(
        n <= PackedStop::LOCATION_MASK as usize + 1,
        "too many locations"
    );
    ensure!(
        tt.osr_location_matches.is_empty()
            && tt.osr_candidates.is_empty()
            && tt.footpaths_out.is_empty()
            && tt.footpaths_in.is_empty(),
        "Hilbert ordering must precede OSR and footpath generation"
    );
    if n == 0 {
        return Ok(());
    }
    let mut min = [f64::INFINITY; 2];
    let mut max = [f64::NEG_INFINITY; 2];
    for c in &tt.coordinates {
        for (axis, v) in [f64::from(c.lon), f64::from(c.lat)].into_iter().enumerate() {
            ensure!(v.is_finite(), "invalid Hilbert coordinate");
            min[axis] = min[axis].min(v);
            max[axis] = max[axis].max(v);
        }
    }
    let span = (max[0] - min[0]).max(max[1] - min[1]).max(f64::EPSILON);
    let mut order: Vec<usize> = (0..n).collect(); // new -> old
    order.sort_by_cached_key(|&i| {
        let c = tt.coordinates[i];
        let x = ((f64::from(c.lon) - min[0]) / span * 65535.0).round() as u32;
        let y = ((f64::from(c.lat) - min[1]) / span * 65535.0).round() as u32;
        (distance(x.min(65535), y.min(65535)), i)
    });
    let mut inverse = vec![LocationIdx::INVALID; n];
    for (new, &old) in order.iter().enumerate() {
        inverse[old] = LocationIdx(new as u32);
    }
    macro_rules! permute {
        ($($field:ident),+) => { $(tt.$field = order.iter().map(|&old| tt.$field[old]).collect();)+ };
    }
    permute!(
        coordinates,
        location_id,
        location_source,
        location_name,
        location_parent,
        location_type,
        location_transfer_seconds
    );
    for parent in &mut tt.location_parent {
        if *parent != LocationIdx::INVALID {
            *parent = inverse[parent.idx()];
        }
    }
    for stop in &mut tt.route_stops {
        // Preserve all four boarding/alighting/accessibility flags.
        stop.0 = (stop.0 & !PackedStop::LOCATION_MASK) | inverse[stop.location().idx()].0;
    }
    for rule in &mut tt.station_transfer_rules {
        rule.from = inverse[rule.from.idx()];
        rule.to = inverse[rule.to.idx()];
    }
    tt.station_transfer_rules
        .sort_unstable_by_key(|r| (r.from.0, r.to.0));
    let old_offsets = std::mem::take(&mut tt.location_routes_offsets);
    let old_routes = std::mem::take(&mut tt.location_routes);
    tt.location_routes_offsets.push(0);
    for &old in &order {
        tt.location_routes.extend_from_slice(
            &old_routes[old_offsets[old] as usize..old_offsets[old + 1] as usize],
        );
        tt.location_routes_offsets
            .push(u32::try_from(tt.location_routes.len())?);
    }
    eprintln!("Gentian: globally Hilbert-ordered {n} locations (16 bits per axis)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use catenary::routing_common::timetable::{
        Coordinate, RouteIdx, SourceIdx, StationTransferRule, StringRef,
    };

    #[test]
    fn hilbert_grid_is_bijective_and_continuous() {
        let mut cells = Vec::new();
        for x in 0..16 {
            for y in 0..16 {
                cells.push((distance(x * 4096, y * 4096), x as i32, y as i32));
            }
        }
        cells.sort_unstable();
        for pair in cells.windows(2) {
            assert_ne!(pair[0].0, pair[1].0);
            assert_eq!(
                (pair[0].1 - pair[1].1).abs() + (pair[0].2 - pair[1].2).abs(),
                1
            );
        }
    }

    #[test]
    fn remapping_preserves_source_identity_parents_flags_rules_and_adjacency() {
        let mut tt = TimetableOwned::empty(0, 1);
        // Deliberately interleaved feeds and a duplicate coordinate.
        for (i, (lon, lat)) in [(1., 1.), (0., 0.), (1., 0.), (0., 0.)]
            .into_iter()
            .enumerate()
        {
            tt.coordinates.push(Coordinate { lat, lon });
            tt.location_id.push(StringRef::default());
            tt.location_name.push(StringRef::default());
            tt.location_source.push(SourceIdx((i % 2) as u16));
            tt.location_type.push(i as u8);
            tt.location_transfer_seconds.push(100 + i as u32);
            tt.location_parent.push(if i == 2 {
                LocationIdx(0)
            } else {
                LocationIdx::INVALID
            });
            tt.route_stops
                .push(PackedStop::new(LocationIdx(i as u32), true, false, true, false).unwrap());
            tt.location_routes.push(RouteIdx(i as u32));
        }
        tt.location_routes_offsets = vec![0, 1, 2, 3, 4];
        tt.station_transfer_rules.push(StationTransferRule {
            from: LocationIdx(2),
            to: LocationIdx(0),
            min_seconds: 42,
            flags: 1,
        });
        apply(&mut tt).unwrap();
        assert_eq!(tt.location_type[..2], [1, 3]); // stable coordinate tie
        for old in 0..4 {
            let new = tt.route_stops[old].location().idx();
            assert_eq!(tt.location_type[new], old as u8);
            assert_eq!(tt.location_source[new].0, (old % 2) as u16);
            assert_eq!(tt.location_transfer_seconds[new], 100 + old as u32);
            assert_eq!(
                tt.location_routes[tt.location_routes_offsets[new] as usize].0,
                old as u32
            );
            assert_eq!(
                tt.route_stops[old].0 & !PackedStop::LOCATION_MASK,
                0x5000_0000
            );
        }
        let child = tt.route_stops[2].location();
        let parent = tt.route_stops[0].location();
        assert_eq!(tt.location_parent[child.idx()], parent);
        assert_eq!(tt.station_transfer_rules[0].from, child);
        assert_eq!(tt.station_transfer_rules[0].to, parent);
        assert_eq!(tt.station_transfer_rules[0].min_seconds, 42);
        assert!(tt.station_transfer_rules[0].prohibited());
    }
}
