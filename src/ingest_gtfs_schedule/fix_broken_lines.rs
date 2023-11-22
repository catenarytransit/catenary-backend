use postgis::ewkb;
extern crate time;
extern crate travelling_salesman;
use geo::HaversineLength;
use geo::ConvexHull;
use geo::{line_string, polygon};
use geo_postgis::{FromPostgis, ToPostgis};

pub fn fix_broken_lines(
    linestring: ewkb::LineStringT<ewkb::Point>,
    feed_id: &str,
) -> ewkb::LineStringT<ewkb::Point> {
    let geo_typed = geo_types::LineString::from_postgis(&linestring);

    if feed_id == "f-ezjm-informaciónoficial~consorcioregionaldetransportesdemadrid" {
        let length = geo_typed.haversine_length();

        println!("{}", length);

        let convex_hulled = geo_typed.convex_hull();
    }

    linestring
}
