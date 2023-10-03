use std::cmp::Ordering::Equal;

use geo_types::LineString;

//graham scan
pub fn convex_hull(input: &Vec<(f64, f64)>) {
    let pnts = x
    .into_iter()
    .map(|xi| coord! {x: xi.0, y: xi.1})
    .collect::<Vec<Coord>>();

    LineString::new(pnts).convex_hull()
}