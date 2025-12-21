use catenary::is_null_island;
use geo::algorithm::convex_hull::ConvexHull;
use geo::coord;
use geo::prelude::*;
use geo::{BoundingRect, Coord, MultiPoint, Point, Polygon};
use geo_buffer::buffer_polygon;
use gtfs_structures::RouteType;
use lazy_static::lazy_static;

use catenary::hull::chi_shape;

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

    let convex_hull = multi_point.convex_hull();

    let bbox = multi_point.bounding_rect().unwrap();
    let width = Point::new(bbox.min().x, bbox.min().y)
        .haversine_distance(&Point::new(bbox.max().x, bbox.min().y));
    let height = Point::new(bbox.min().x, bbox.min().y)
        .haversine_distance(&Point::new(bbox.min().x, bbox.max().y));

    let longest_side = width.max(height);

    let hull = if longest_side > 500_000.0 && !gtfs.shapes.is_empty() {
        let longest_side_geom = longest_side_length_metres(&convex_hull);
        println!(
            "Computing chi shape with {} points and {} maximum side length",
            multi_point.0.len(),
            longest_side_geom.length
        );
        let chi_shape_output = chi_shape(&multi_point.0, 0.5 * longest_side_geom.length);

        if let Some(chi_shape_output) = &chi_shape_output {
            println!(
                "Chi shape computed successfully, {} points",
                chi_shape_output.0.len()
            );
        } else {
            println!("Chi shape failed");
        }

        chi_shape_output.unwrap_or(convex_hull)
    } else {
        convex_hull
    };

    //buffer the hull by 5km if bus only, 10km for metros, but 50km if contains rail or other modes

    let buffer_distance_metres = match extremely_large_shape_file {
        true => 20000.0,
        false => match bus_only {
            true => 5000.0,
            false => match contains_metro_and_bus_only {
                true => 5000.0,
                false => 30000.0,
            },
        },
    };

    // Convert metres to degrees (approximate)
    // 1 degree latitude is ~111km
    let lat_conversion = 111_000.0;
    let buffer_distance_degrees = buffer_distance_metres / lat_conversion;

    let buffered_hull = buffer_polygon(&hull, buffer_distance_degrees);

    println!(
        "Buffered hull computed successfully, {} polygons",
        buffered_hull.len()
    );

    // buffer_polygon returns a MultiPolygon.
    // We want a single Polygon.
    // Since we are buffering a single connected polygon (the hull), the result should usually be a single polygon.
    // If it's not, we'll take the one with the largest area or just the first one.
    // For now, let's take the first one.

    buffered_hull.into_iter().next()
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
