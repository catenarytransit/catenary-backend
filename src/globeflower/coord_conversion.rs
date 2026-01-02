// ===========================================================================
// Web Mercator Coordinate Conversion (matches C++ util::geo::latLngToWebMerc)
// ===========================================================================
use crate::graph_types::*;
use crate::edges::*;

pub const EARTH_RADIUS: f64 = 6378137.0;

/// Convert lat/lng (EPSG:4326) to Web Mercator (EPSG:3857) - matches C++ latLngToWebMerc
/// Input: (longitude, latitude) in degrees
/// Output: (x, y) in meters
pub fn lat_lng_to_web_merc(lon: f64, lat: f64) -> (f64, f64) {
    let x = EARTH_RADIUS * lon.to_radians();
    let y = EARTH_RADIUS * ((std::f64::consts::FRAC_PI_4 + lat.to_radians() / 2.0).tan()).ln();
    (x, y)
}

/// Convert Web Mercator (EPSG:3857) to lat/lng (EPSG:4326) - matches C++ webMercToLatLng
/// Input: (x, y) in meters
/// Output: (longitude, latitude) in degrees
pub fn web_merc_to_lat_lng(x: f64, y: f64) -> (f64, f64) {
    let lon = (x / EARTH_RADIUS).to_degrees();
    let lat = (2.0 * (y / EARTH_RADIUS).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees();
    (lon, lat)
}

/// Convert a GraphEdge geometry to Web Mercator coordinates
pub fn convert_edge_to_web_merc(edge: &mut GraphEdge) {
    for coord in &mut edge.geometry.points {
        let (x, y) = lat_lng_to_web_merc(coord.x, coord.y);
        coord.x = x;
        coord.y = y;
    }
}

/// Convert a GraphEdge geometry back to lat/lng coordinates
pub fn convert_edge_to_lat_lng(edge: &mut GraphEdge) {
    for coord in &mut edge.geometry.points {
        let (lon, lat) = web_merc_to_lat_lng(coord.x, coord.y);
        coord.x = lon;
        coord.y = lat;
    }
}