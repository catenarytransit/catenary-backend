use anyhow::{Context, Result, bail, ensure};
use geo::{BoundingRect, Geometry, Intersects, Point, Polygon, Rect};
use geojson::GeoJson;
use std::sync::LazyLock;

struct IndexedPolygon {
    bounds: Rect<f64>,
    polygon: Polygon<f64>,
}

pub(super) struct OceanExclusions {
    polygons: Vec<IndexedPolygon>,
}

static OPEN_OCEAN: LazyLock<OceanExclusions> = LazyLock::new(|| {
    OceanExclusions::from_geojson(include_str!(
        "../../../chateau_exclude_open_oceans.geojson"
    ))
    .expect("invalid chateau_exclude_open_oceans.geojson")
});

pub(super) fn open_ocean_exclusions() -> &'static OceanExclusions {
    &OPEN_OCEAN
}

impl OceanExclusions {
    fn from_geojson(source: &str) -> Result<Self> {
        let GeoJson::FeatureCollection(collection) = source.parse::<GeoJson>()? else {
            bail!("expected a GeoJSON FeatureCollection");
        };

        let mut exclusions = Self {
            polygons: Vec::new(),
        };

        for (index, feature) in collection.features.into_iter().enumerate() {
            let geometry = feature
                .geometry
                .with_context(|| format!("exclusion feature {index} has no geometry"))?;
            let geometry: Geometry<f64> = geometry
                .try_into()
                .with_context(|| format!("invalid geometry in exclusion feature {index}"))?;

            match geometry {
                Geometry::Polygon(polygon) => exclusions.insert(polygon)?,
                Geometry::MultiPolygon(multi_polygon) => {
                    for polygon in multi_polygon.0 {
                        exclusions.insert(polygon)?;
                    }
                }
                _ => bail!("exclusion feature {index} is not a polygon"),
            }
        }

        ensure!(!exclusions.polygons.is_empty(), "no exclusion polygons");
        Ok(exclusions)
    }

    fn insert(&mut self, polygon: Polygon<f64>) -> Result<()> {
        ensure!(
            polygon.exterior().0.len() >= 4,
            "exclusion polygon has fewer than four ring coordinates"
        );
        let bounds = polygon
            .bounding_rect()
            .context("exclusion polygon has no bounds")?;
        ensure!(
            [bounds.min().x, bounds.min().y, bounds.max().x, bounds.max().y]
                .iter()
                .all(|value| value.is_finite()),
            "exclusion polygon has non-finite coordinates"
        );

        self.polygons.push(IndexedPolygon { bounds, polygon });
        Ok(())
    }

    pub(super) fn excludes(&self, longitude: f64, latitude: f64) -> bool {
        self.polygons.iter().any(|candidate| {
            if latitude < candidate.bounds.min().y || latitude > candidate.bounds.max().y {
                return false;
            }

            // Some exclusion rings use longitudes beyond 180 degrees to cross
            // the antimeridian. Compare equivalent longitude representations.
            [longitude, longitude + 360.0, longitude - 360.0]
                .into_iter()
                .any(|x| {
                    x >= candidate.bounds.min().x
                        && x <= candidate.bounds.max().x
                        && candidate.polygon.intersects(&Point::new(x, latitude))
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_ocean_polygons_and_antimeridian() {
        let exclusions = open_ocean_exclusions();

        assert_eq!(exclusions.polygons.len(), 7);
        assert!(exclusions.excludes(-35.0, 25.0));
        assert!(exclusions.excludes(190.0, -30.0));
        assert!(exclusions.excludes(-170.0, -30.0));
        assert!(!exclusions.excludes(-155.0, -30.0));
        assert!(!exclusions.excludes(-122.0, 37.0));
    }

    #[test]
    fn polygon_holes_and_boundaries() {
        let json = r#"{
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[0,0],[10,0],[10,10],[0,10],[0,0]],
                        [[3,3],[7,3],[7,7],[3,7],[3,3]]
                    ]
                }
            }]
        }"#;

        let exclusions = OceanExclusions::from_geojson(json).unwrap();
        assert!(exclusions.excludes(2.0, 2.0));
        assert!(exclusions.excludes(0.0, 5.0));
        assert!(exclusions.excludes(3.0, 5.0));
        assert!(!exclusions.excludes(5.0, 5.0));
        assert!(!exclusions.excludes(12.0, 5.0));
    }

    #[test]
    fn multipolygons_are_indexed_individually() {
        let json = r#"{
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [[[20,20],[22,20],[22,22],[20,22],[20,20]]],
                        [[[30,30],[32,30],[32,32],[30,32],[30,30]]]
                    ]
                }
            }]
        }"#;

        let exclusions = OceanExclusions::from_geojson(json).unwrap();
        assert_eq!(exclusions.polygons.len(), 2);
        assert!(exclusions.excludes(21.0, 21.0));
        assert!(exclusions.excludes(31.0, 31.0));
        assert!(!exclusions.excludes(25.0, 25.0));
    }

    #[test]
    fn malformed_exclusions_fail_instead_of_being_ignored() {
        assert!(OceanExclusions::from_geojson(
            r#"{"type":"FeatureCollection","features":[]}"#
        )
        .is_err());
        assert!(OceanExclusions::from_geojson(
            r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[1,2]}}]}"#
        )
        .is_err());
    }
}
