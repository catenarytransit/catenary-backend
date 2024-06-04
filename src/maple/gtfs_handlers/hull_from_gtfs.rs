use geo::algorithm::concave_hull::ConcaveHull;
use geo::{MultiPoint, Point, Polygon};

pub fn hull_from_gtfs(gtfs: &gtfs_structures::Gtfs) -> Option<Polygon> {
    match gtfs.shapes.len() > 3 {
        // hull shapes with parameter of 50
        // it's still better than convex hull
        true => {
            let points: MultiPoint = gtfs
                .shapes
                .iter()
                .map(|(id, points)| {
                    points
                        .iter()
                        .filter(|point| {
                            let is_null_island = f64::abs(0. - point.latitude) < 0.000001
                                && f64::abs(0. - point.latitude) < 0.000001;
                            !is_null_island
                        })
                        .map(|point| Point::new(point.longitude, point.latitude))
                })
                .flatten()
                .collect::<MultiPoint>();
            Some(points.concave_hull(4.0))
        }
        false => {
            match gtfs.stops.len() > 3 {
                true => {
                    //hull stops with parameter of 10

                    let points: MultiPoint = gtfs
                        .stops
                        .iter()
                        .filter(|(_, stop)| stop.longitude.is_some() && stop.latitude.is_some())
                        .filter(|(_, stop)| {
                            let is_null_island = f64::abs(0. - stop.latitude.unwrap()) < 0.000001
                                && f64::abs(0. - stop.latitude.unwrap()) < 0.000001;
                            !is_null_island
                        })
                        .map(|(_, stop)| {
                            Point::new(stop.longitude.unwrap(), stop.latitude.unwrap())
                        })
                        .collect::<MultiPoint>();
                    Some(points.concave_hull(4.0))
                }
                false => None,
            }
        }
    }
}
