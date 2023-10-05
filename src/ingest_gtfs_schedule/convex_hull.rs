use std::cmp::Ordering::Equal;

use geo::coord;
use geo::ConvexHull;
use geo::Coord;
use geo::LineString;

//graham scan
pub fn convex_hull(input: &Vec<(f64, f64)>) -> geo::Polygon {
    let pnts = input
        .into_iter()
        .map(|xi| coord! {x: xi.0, y: xi.1})
        .collect::<Vec<Coord>>();

    LineString::new(pnts).convex_hull()
}
