use geo::ConcaveHull;
use geo::prelude::*;
use geo::{LineString, MultiPoint, Point, Polygon}; // Try importing this

pub fn test() {
    let points = MultiPoint(vec![
        Point::new(0.0, 0.0),
        Point::new(10.0, 0.0),
        Point::new(10.0, 10.0),
        Point::new(0.0, 10.0),
        Point::new(5.0, 5.0),
    ]);

    // concavity parameter:
    // "The concavity parameter determines how concave the hull will be.
    // A value of 1.0 produces the convex hull. A value of 0.0 produces the most concave hull possible."
    // Wait, let's guess the API. usually it's `concave_hull(factor)`.
    let concave = points.concave_hull(0.5);
    println!("Concave hull: {:?}", concave);
}
