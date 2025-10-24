use actix_web::Route;
use catenary::is_null_island;
use geo::BooleanOps;
use geo::Centroid;
use geo::Distance;
use geo::HaversineDistance;
use geo::RhumbBearing;
use geo::RhumbDestination;
use geo::algorithm::concave_hull::ConcaveHull;
use geo::algorithm::convex_hull::ConvexHull;
use geo::coord;
use geo::prelude::*;
use geo::{Coord, MultiPoint, Point, Polygon, convex_hull};
use geo_buffer::buffer_polygon;
use gtfs_structures::RouteType;
use lazy_static::lazy_static;

lazy_static! {
    static ref BANNED_OCEAN_GEO: geo::MultiPolygon<f64> = geo::MultiPolygon::new(vec![
        geo::Polygon::new(
            geo::LineString::new(vec![
                coord! { x: 5.2, y: 3.6 },
                coord! { x: -13.84, y: 3.62 },
                coord! { x: -13.84, y: -15.28 },
                coord! { x: 5.2, y: -15.28 },
                coord! { x: 5.2, y: 3.6 },
            ]),
            vec![],
        ),
        geo::Polygon::new(
            geo::LineString::new(vec![
                coord! { x: 51.8397440696757, y: 11.036088030014866 },
                coord! { x: 51.8397440696757, y: -3.1282014955991713 },
                coord! { x: 71.36372595923046, y: -3.1282014955991713 },
                coord! { x: 71.36372595923046, y: 11.036088030014866 },
                coord! { x: 51.8397440696757, y: 11.036088030014866 },
            ]),
            vec![],
        ),
    ]);
}

pub fn hull_from_gtfs(gtfs: &gtfs_structures::Gtfs, feed_id: &str) -> Option<Polygon> {
    let bus_only = gtfs
        .routes
        .iter()
        .all(|(_, route)| route.route_type == RouteType::Bus);

    let contains_metro_and_bus_only = gtfs.routes.iter().any(|(_, route)| {
        route.route_type == RouteType::Subway || route.route_type == RouteType::Bus
    });

    let shape_point_count = gtfs.shapes.iter().map(|(_, x)| x.len()).sum::<usize>();

    let extremely_large_shape_file = shape_point_count > 10_000_000;

    let list_of_coordinates_to_use_from_shapes = match extremely_large_shape_file {
        true => vec![],
        false => gtfs
            .shapes
            .iter()
            .flat_map(|(id, points)| {
                points
                    .iter()
                    .filter(|point| !is_null_island(point.longitude, point.latitude))
                    .filter(|point| point.longitude.is_finite() && point.latitude.is_finite())
                    .map(|point| Point::new(point.longitude, point.latitude))
            })
            .collect::<Vec<Point>>(),
    };

    let stop_points = gtfs
        .stops
        .iter()
        .filter(|(_, stop)| stop.longitude.is_some() && stop.latitude.is_some())
        .filter(|(_, stop)| !is_null_island(stop.latitude.unwrap(), stop.longitude.unwrap()))
        .filter(|(_, stop)| {
            stop.longitude.unwrap().is_finite() && stop.latitude.unwrap().is_finite()
        })
        .map(|(_, stop)| Point::new(stop.longitude.unwrap(), stop.latitude.unwrap()))
        .collect::<Vec<Point>>();

    //join vecs together

    let new_point_collection = list_of_coordinates_to_use_from_shapes
        .into_iter()
        .chain(stop_points.into_iter())
        .collect::<Vec<Point>>();

    let new_point_collection = match feed_id {
        "f-bus~dft~gov~uk" => new_point_collection
            .into_iter()
            .filter(|point| point.x() < 6. && point.y() > 45.)
            .collect::<Vec<Point>>(),
        _ => new_point_collection,
    };

    if new_point_collection.len() < 4 {
        return None;
    }

    let multi_point = MultiPoint(new_point_collection);

    let concave_hull = multi_point.concave_hull(1.0);
    let convex_hull = multi_point.convex_hull();

    let centroid = convex_hull.centroid().unwrap();

    //buffer the convex hull by 5km if bus only, 10km for metros, but 50km if contains rail or other modes

    let buffer_distance = match extremely_large_shape_file {
        true => 20000.0,
        false => match bus_only {
            true => 5000.0,
            false => match contains_metro_and_bus_only {
                true => 5000.0,
                false => 30000.0,
            },
        },
    };

    let mut buffered_convex_hull =
        buffer_geo_polygon_internal(convex_hull, buffer_distance).unwrap();

    //convert concave hull back into multipoint

    let concave_hull_points = concave_hull.exterior().points().collect::<MultiPoint<_>>();

    Some(buffered_convex_hull)
}

struct PolygonSide {
    starting_index: usize,
    ending_index: usize,
    length: f64,
}

pub fn longest_side_length_metres(polygon: &geo::Polygon<f64>) -> PolygonSide {
    let exterior = polygon.exterior();

    let points = exterior.points().collect::<Vec<_>>();

    let mut longest_side = PolygonSide {
        starting_index: 0,
        ending_index: 1,
        length: 0.0,
    };

    for (index, point) in points.iter().enumerate() {
        //skip the last one
        if index == points.len() - 1 {
            break;
        }

        let next_point = points[index + 1];

        let distance = point.haversine_distance(&next_point);

        if distance > longest_side.length {
            longest_side = PolygonSide {
                starting_index: index,
                ending_index: index + 1,
                length: distance,
            };
        }
    }

    longest_side
}

pub fn buffer_geo_polygon_internal(
    polygon: geo::Polygon<f64>,
    distance_metres: f64,
) -> Option<geo::Polygon<f64>> {
    let centre = polygon.centroid();

    match centre {
        Some(centre) => {
            let mut points = Vec::new();

            let points_of_polygon = polygon.exterior().points().collect::<Vec<_>>();

            for original_point in points_of_polygon {
                //calculate bearing between the centre and the point

                let bearing = centre.rhumb_bearing(original_point);

                // calculate the distance_metres between the centre and the point

                let distance = centre.haversine_distance(&original_point);

                // calculate the new point

                let new_point = centre.rhumb_destination(bearing, distance + distance_metres);

                points.push(new_point);
            }

            let new_polygon = geo::Polygon::new(
                geo::LineString::new(points.into_iter().map(|x| Coord::from(x)).collect()),
                vec![],
            );

            Some(new_polygon)
        }
        None => None,
    }
}
