use postgis::ewkb;
extern crate time;
extern crate travelling_salesman;
use geo::{line_string, polygon};
use geo_postgis::{FromPostgis,ToPostgis};
use geo::HaversineLength;

pub fn fix_broken_lines(linestring: ewkb::LineStringT<ewkb::Point>, feed_id: &str) -> ewkb::LineStringT<ewkb::Point> {
    let geo_typed = geo_types::LineString::from_postgis(&linestring);

    let length = geo_typed.haversine_length();

    if feed_id == "f-ezjm-informaciónoficial~consorcioregionaldetransportesdemadrid" {
        print({}, length);
    }

    linestring
}