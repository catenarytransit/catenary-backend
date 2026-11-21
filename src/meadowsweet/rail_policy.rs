//! Meadowsweet's single permitted GTFS mode.
//!
//! An OSM track tag is NOT a GTFS route type. The graph can contain
//! mainline or narrow-gauge rail infrastructure, but any GTFS route
//! admitted to Meadowsweet must explicitly have route_type=2.
use anyhow::{Result, bail};

pub const GTFS_RAIL_ROUTE_TYPE: u16 = 2;

pub fn validate_gtfs_route_type(value: u16) -> Result<()> {
    if value != GTFS_RAIL_ROUTE_TYPE {
        bail!(
            "Meadowsweet accepts GTFS route_type=2 (rail) only; got route_type={value}"
        );
    }
    Ok(())
}

/// OSM infrastructure tags eligible for a future GTFS route_type=2 match.
/// This is deliberately not a claim that every OSM railway=rail way
/// carries a type-2 service: service provenance still needs GTFS matching.
pub fn osm_railway_is_eligible(tag: &str) -> bool {
    matches!(tag, "rail" | "narrow_gauge")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_standard_gtfs_rail_type() {
        assert!(validate_gtfs_route_type(2).is_ok());
        for route_type in [0, 1, 3, 4, 5, 6, 7, 100, 109, 400, 700, 1700] {
            assert!(
                validate_gtfs_route_type(route_type).is_err(),
                "unexpected route_type {route_type}"
            );
        }
    }

    #[test]
    fn osm_mainline_tags_only() {
        for tag in ["rail", "narrow_gauge"] {
            assert!(osm_railway_is_eligible(tag), "{tag}");
        }
        for tag in ["subway", "tram", "light_rail", "funicular", "monorail", "preserved", "bus"] {
            assert!(!osm_railway_is_eligible(tag), "{tag}");
        }
    }
}
