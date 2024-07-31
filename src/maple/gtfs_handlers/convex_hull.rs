use geo::coord;
use geo::ConvexHull;
use geo::Coord;
use geo::LineString;

//graham scan
pub fn convex_hull(input: &Vec<(f64, f64)>) -> geo::Polygon {
    let pnts = input
        .into_iter()
        .filter(|coords| {
            let (x, y) = coords;
            x.is_finite() && y.is_finite()
        })
        //remove null island
        .filter(|coords| {
            let (x, y) = coords;
            !(x.abs() < 0.1 && y.abs() < 0.1)
        })
        .map(|xi| coord! {x: xi.0, y: xi.1})
        .collect::<Vec<Coord>>();

    LineString::new(pnts).convex_hull()
}
