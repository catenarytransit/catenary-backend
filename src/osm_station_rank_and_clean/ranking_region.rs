use geo::{BoundingRect, Intersects, Point, Polygon, Rect};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RankingRegion {
    GreaterLondon,
    RestOfUk,
    TargetCountries,
    Other,
}

pub struct GreaterLondon {
    polygon: Polygon<f64>,
    bounds: Rect<f64>,
}

impl GreaterLondon {
    pub fn load() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Keep the boundary in the binary so runs do not need another runtime file.
        let geojson = include_str!("greater-london.geo.json").parse::<geojson::GeoJson>()?;
        let geojson::GeoJson::Feature(feature) = geojson else {
            return Err("Greater London boundary must be a GeoJSON Feature".into());
        };
        let geometry: geo::Geometry<f64> = feature
            .geometry
            .ok_or("Greater London boundary has no geometry")?
            .try_into()?;
        let geo::Geometry::Polygon(polygon) = geometry else {
            return Err("Greater London boundary must be a Polygon".into());
        };
        let bounds = polygon
            .bounding_rect()
            .ok_or("Empty Greater London boundary")?;
        Ok(Self { polygon, bounds })
    }

    pub fn ranking_region(&self, point: &Point<f64>, country: Option<&str>) -> RankingRegion {
        // The more detailed London boundary takes precedence over coarse country data.
        // Intersects includes stations exactly on the boundary in London's cohort.
        if self.bounds.intersects(point) && self.polygon.intersects(point) {
            RankingRegion::GreaterLondon
        } else if country == Some("UK") {
            RankingRegion::RestOfUk
        } else if country.is_some() {
            RankingRegion::TargetCountries
        } else {
            RankingRegion::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn london_uses_the_polygon_including_outer_boroughs() {
        let london = GreaterLondon::load().unwrap();
        for (lon, lat) in [(-0.125, 51.530), (-0.100, 51.372), (0.183, 51.576)] {
            assert_eq!(
                london.ranking_region(&Point::new(lon, lat), Some("UK")),
                RankingRegion::GreaterLondon
            );
        }
        // Watford and Dartford are outside London but inside its bounding rectangle.
        for (lon, lat) in [(-0.397, 51.656), (0.219, 51.447), (-2.588, 51.454)] {
            assert_eq!(
                london.ranking_region(&Point::new(lon, lat), Some("UK")),
                RankingRegion::RestOfUk
            );
        }
    }

    #[test]
    fn detailed_london_boundary_takes_precedence_and_includes_its_edge() {
        let london = GreaterLondon::load().unwrap();
        let vertex = london.polygon.exterior().0[0];
        for point in [Point::new(-0.125, 51.530), Point::new(vertex.x, vertex.y)] {
            assert_eq!(
                london.ranking_region(&point, None),
                RankingRegion::GreaterLondon
            );
        }
    }

    #[test]
    fn other_cohorts_are_preserved() {
        let london = GreaterLondon::load().unwrap();
        assert_eq!(
            london.ranking_region(&Point::new(13.405, 52.520), Some("DE")),
            RankingRegion::TargetCountries
        );
        assert_eq!(
            london.ranking_region(&Point::new(-118.244, 34.052), None),
            RankingRegion::Other
        );
    }
}
