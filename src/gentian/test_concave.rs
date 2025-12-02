use geo::{ConcaveHull, MultiPoint, Point};

fn main() {
    let points = vec![
        Point::new(0.0, 0.0),
        Point::new(1.0, 0.0),
        Point::new(1.0, 1.0),
        Point::new(0.0, 1.0),
        Point::new(0.5, 0.5), // Inner point
    ];

    let multipoint = MultiPoint(points);
    // concavity of 2.0 is a guess, need to check docs or trial
    // In geo 0.26+, concave_hull takes a 'concavity' param.
    // It seems to be related to the 'chi' parameter or similar.
    // Let's try calling it.
    let hull = multipoint.concave_hull(2.0);

    println!("Hull: {:?}", hull);
}
