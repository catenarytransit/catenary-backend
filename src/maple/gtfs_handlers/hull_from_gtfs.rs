use catenary::is_null_island;
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
                .flat_map(|(id, points)| {
                    points
                        .iter()
                        .filter(|point| !is_null_island(point.longitude, point.latitude))
                        .map(|point| Point::new(point.longitude, point.latitude))
                })
                .collect::<MultiPoint>();
            Some(points.concave_hull(1.5))
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
                            !is_null_island(stop.latitude.unwrap(), stop.longitude.unwrap())
                        })
                        .map(|(_, stop)| {
                            Point::new(stop.longitude.unwrap(), stop.latitude.unwrap())
                        })
                        .collect::<MultiPoint>();
                    Some(points.concave_hull(1.5))
                }
                false => None,
            }
        }
    }
}
