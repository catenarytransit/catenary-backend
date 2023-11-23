use geo::CoordsIter;
use geo_types::Point;
use postgis::ewkb;
extern crate time;
extern crate travelling_salesman;
use geo::HaversineLength;
use geo::ConvexHull;
use geo::{line_string, polygon,GeodesicDistance};
use geo_postgis::{FromPostgis, ToPostgis};
use geo::EuclideanDistance;
use geo::HaversineDistance;

use std::f64::consts::PI;

fn rotating_calipers_diameter(polygon: &Vec<geo::Point>) -> f64 {
    let n = polygon.len();

    let mut k = 1;

    while abs_area(polygon[n-1], polygon[0], polygon[(k+1)%n]) >
    abs_area(polygon[n-1], polygon[0], polygon[k]) {
        k = k + 1;
    }

    let k = k;

    let mut res = 0.;
    let mut j = k;
    
    for i in 0..(k+1) {
        while abs_area(
            polygon[i],
            polygon[(i+1) % n],
            polygon[(j+1)%n]
        ) >
        abs_area(polygon[i], polygon[(i+1)%n], polygon[j])
        {
            res = f64::max(res, polygon[i].haversine_distance(&polygon[(j+1)%n]));
            j = (j+1) % n;
        }

        res = f64::max(res, polygon[i].haversine_distance(&polygon[j]))
    }

    res
}

pub fn fix_broken_lines(
    linestring: ewkb::LineStringT<ewkb::Point>,
    feed_id: &str,
) -> ewkb::LineStringT<ewkb::Point> {
    let geo_typed = geo_types::LineString::from_postgis(&linestring);

    if feed_id == "f-ezjm-informaciónoficial~consorcioregionaldetransportesdemadrid" {
        let length = geo_typed.haversine_length();

        println!("{}", length);

        let convex_hull = geo_typed.convex_hull();
        
        let convex_hull_points = convex_hull.exterior().clone().into_points();

        let farthest_points_distance = match convex_hull_points.len() {
            0 => 0.,
            1 => 0.,
            2 => convex_hull_points[0].geodesic_distance(&convex_hull_points[1]),
            _ => {
                rotating_calipers_diameter(&convex_hull_points)
            }
        };

        println!("{} point distance and {} line distance", farthest_points_distance, length);

        if length > farthest_points_distance * 20. && length > 1000. {
            println!("doing travelling salesman");
            
            let points = geo_typed.points().map(|point| (point.x(), point.y())).collect::<Vec<(f64,f64)>>();

            let tour = travelling_salesman::simulated_annealing::solve(
                &points,
                time::Duration::milliseconds(5000),
              );
            
            let result:geo::LineString = tour.route.iter().map(|index| 
                 points[*index]
            ).collect::<Vec<(f64,f64)>>().into();

            return result.to_postgis_wgs84();
        }
    }

    linestring
}


fn abs_area(p: geo::Point, q : geo::Point, r: geo::Point) -> f64 {
    return (
      p.x() * q.y() + q.x() * r.y() + r.x() * p.y() - (p.y() * q.x() + q.y() * r.x() + r.y() * p.x())
    ).abs();
  }