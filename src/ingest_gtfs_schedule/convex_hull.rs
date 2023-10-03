use std::cmp::Ordering::Equal;

use geo::LineString;
use geo::Coord;
use geo::coord;

//graham scan
pub fn convex_hull(input: &Vec<(f64, f64)>) {
    let pnts = input
    .into_iter()
    .map(|xi| coord! {x: xi.0, y: xi.1})
    .collect::<Vec<Coord>>();

    LineString::new(pnts).convex_hull()
}