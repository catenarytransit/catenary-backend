use catenary::is_null_island;
use geo::algorithm::convex_hull::ConvexHull;
use geo::coord;
use geo::prelude::*;
use geo::{
    Area, BooleanOps, BoundingRect, Coord, LineString, MultiPoint, MultiPolygon, Point, Polygon,
};
use gtfs_structures::RouteType;
use lazy_static::lazy_static;
use std::f64::consts::PI;

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
                chi_shape_output.exterior().0.len()
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

    // 1. Setup Projection
    // We center the projection on the first point (arbitrary, but sufficient for local area)
    let center_point = hull
        .exterior()
        .points()
        .next()
        .unwrap_or(Point::new(0.0, 0.0));
    let projection = LocalProjection::new(center_point);
    let smoothness = 12; // Points per semi-circle

    let mut buffered_polygons: Vec<Polygon<f64>> = Vec::new();

    // 2. Project and Buffer each segment
    for line in hull.exterior().lines() {
        if line.start == line.end {
            continue;
        }
        let p_start = projection.forward(Point::from(line.start));
        let p_end = projection.forward(Point::from(line.end));

        let stadium = create_stadium(p_start, p_end, buffer_distance_metres, smoothness);
        buffered_polygons.push(stadium);
    }

    // 3. Include the original polygon (projected)
    let projected_exterior_coords: Vec<Coord<f64>> = hull
        .exterior()
        .coords()
        .map(|c| {
            let p = projection.forward(Point::from(*c));
            Coord { x: p.x(), y: p.y() }
        })
        .collect();
    let projected_poly = Polygon::new(LineString::new(projected_exterior_coords), vec![]);
    buffered_polygons.push(projected_poly);

    // 4. Compute Union (Merge the stadiums into one shape)
    // We use the pure-rust 'BooleanOps' trait here.
    let mut result_poly = MultiPolygon::new(vec![buffered_polygons[0].clone()]);

    for poly in &buffered_polygons[1..] {
        let current_mp = MultiPolygon::new(vec![poly.clone()]);
        result_poly = result_poly.union(&current_mp);
    }

    // 5. Reproject back to WGS84
    // We take the largest polygon from the result.
    let largest_poly = result_poly.0.into_iter().max_by(|a, b| {
        a.unsigned_area()
            .partial_cmp(&b.unsigned_area())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if let Some(poly) = largest_poly {
        let final_wgs84_coords: Vec<Coord<f64>> = poly
            .exterior()
            .coords()
            .map(|c| {
                let p = projection.inverse(Point::new(c.x, c.y));
                Coord { x: p.x(), y: p.y() }
            })
            .collect();

        let final_polygon = Polygon::new(LineString::new(final_wgs84_coords), vec![]);
        println!("Buffered hull computed successfully via stadium method, 1 polygon");
        Some(final_polygon)
    } else {
        None
    }
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

/// Constants for WGS84 approximation
const EARTH_RADIUS: f64 = 6_378_137.0; // in metres

/// A struct to handle the projection between WGS84 and a Local Metric Plane
struct LocalProjection {
    center_lat: f64,
    center_lon: f64,
}

impl LocalProjection {
    fn new(center: Point<f64>) -> Self {
        Self {
            center_lat: center.y(),
            center_lon: center.x(),
        }
    }

    /// Converts WGS84 (lon, lat) to Local (x, y) in metres
    fn forward(&self, point: Point<f64>) -> Point<f64> {
        let d_lat = (point.y() - self.center_lat).to_radians();
        let d_lon = (point.x() - self.center_lon).to_radians();

        let lat_rad = self.center_lat.to_radians();

        // precise local approximation
        let x = d_lon * lat_rad.cos() * EARTH_RADIUS;
        let y = d_lat * EARTH_RADIUS;

        Point::new(x, y)
    }

    /// Converts Local (x, y) back to WGS84 (lon, lat)
    fn inverse(&self, point: Point<f64>) -> Point<f64> {
        let lat_rad = self.center_lat.to_radians();

        let d_lon = point.x() / (lat_rad.cos() * EARTH_RADIUS);
        let d_lat = point.y() / EARTH_RADIUS;

        let lon = self.center_lon + d_lon.to_degrees();
        let lat = self.center_lat + d_lat.to_degrees();

        Point::new(lon, lat)
    }
}

/// Generates a "Stadium" polygon (buffered line segment) with rounded caps
fn create_stadium(
    start: Point<f64>,
    end: Point<f64>,
    radius: f64,
    num_points: usize,
) -> Polygon<f64> {
    let dx = end.x() - start.x();
    let dy = end.y() - start.y();
    let len = (dx * dx + dy * dy).sqrt();

    // Normal vector (perpendicular to the line)
    // Avoid division by zero
    let (_nx, _ny) = if len == 0.0 {
        (0.0, 0.0)
    } else {
        (-dy / len, dx / len)
    };

    let mut points = Vec::new();

    // 1. Generate the right-side semi-circle (around the 'end' point)
    // Angle of the line
    let angle = dy.atan2(dx);
    // Start angle for the cap (line angle - 90 degrees)
    let start_angle = angle - PI / 2.0;

    for i in 0..=num_points {
        let theta = start_angle + (PI * i as f64 / num_points as f64);
        let px = end.x() + radius * theta.cos();
        let py = end.y() + radius * theta.sin();
        points.push((px, py));
    }

    // 2. Generate the left-side semi-circle (around the 'start' point)
    // Start angle for the cap (line angle + 90 degrees)
    let start_angle_2 = angle + PI / 2.0;

    for i in 0..=num_points {
        let theta = start_angle_2 + (PI * i as f64 / num_points as f64);
        let px = start.x() + radius * theta.cos();
        let py = start.y() + radius * theta.sin();
        points.push((px, py));
    }

    // Close the ring
    if !points.is_empty() {
        points.push(points[0]);
    }

    Polygon::new(LineString::from(points), vec![])
}
