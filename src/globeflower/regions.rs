// Region definitions for geographic partitioning of rail graph processing.
//
// Each region corresponds to a Geofabrik extract, with a bounding box used
// to filter database queries. This enables processing one region at a time
// to reduce peak memory usage.

use std::str::FromStr;

/// Geographic regions for rail graph processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    NorthAmerica,
    Europe,
    Japan,
    Australia,
    NewZealand,
    MalaysiaSingaporeBrunei,
}

impl Region {
    /// All available regions
    pub const ALL: &'static [Region] = &[
        Region::NorthAmerica,
        Region::Europe,
        Region::Japan,
        Region::Australia,
        Region::NewZealand,
        Region::MalaysiaSingaporeBrunei,
    ];

    /// Get the configuration for this region
    pub fn config(&self) -> RegionConfig {
        match self {
            Region::NorthAmerica => RegionConfig {
                name: "north-america",
                display_name: "North America",
                // Includes Canada, US, Mexico - wide bbox for Amtrak cross-border
                bbox: (-180.0, 5.0, -50.0, 85.0),
                osm_pbf_suffix: "north-america",
            },
            Region::Europe => RegionConfig {
                name: "europe",
                display_name: "Europe",
                // Western European landmass + Britain + Scandinavia
                bbox: (-25.0, 34.0, 46.0, 72.0),
                osm_pbf_suffix: "europe",
            },
            Region::Japan => RegionConfig {
                name: "japan",
                display_name: "Japan",
                // Japanese archipelago
                bbox: (122.0, 20.0, 154.0, 46.0),
                osm_pbf_suffix: "japan",
            },
            Region::Australia => RegionConfig {
                name: "australia",
                display_name: "Australia",
                // Australian continent
                bbox: (112.0, -45.0, 155.0, -9.0),
                osm_pbf_suffix: "australia",
            },
            Region::NewZealand => RegionConfig {
                name: "new-zealand",
                display_name: "New Zealand",
                // NZ islands
                bbox: (165.0, -48.0, 180.0, -33.0),
                osm_pbf_suffix: "new-zealand",
            },
            Region::MalaysiaSingaporeBrunei => RegionConfig {
                name: "malaysia-singapore-brunei",
                display_name: "Malaysia/Singapore/Brunei",
                // Peninsular Malaysia + Singapore
                bbox: (99.0, 0.0, 120.0, 8.0),
                osm_pbf_suffix: "malaysia-singapore-brunei",
            },
        }
    }

    /// Get the expected OSM PBF filename for this region
    ///
    /// Returns patterns like "railonly-europe-latest.osm.pbf"
    pub fn expected_pbf_name(&self) -> String {
        format!("railonly-{}-latest.osm.pbf", self.config().osm_pbf_suffix)
    }

    /// Get the output graph filename for this region
    pub fn output_graph_name(&self) -> String {
        format!("globeflower_graph_{}.bin", self.config().name)
    }

    /// Get the output GeoJSON filename for this region
    pub fn output_geojson_name(&self) -> String {
        format!("globeflower_graph_{}.geojson", self.config().name)
    }
}

impl FromStr for Region {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "north-america" | "northamerica" | "na" => Ok(Region::NorthAmerica),
            "europe" | "eu" => Ok(Region::Europe),
            "japan" | "jp" => Ok(Region::Japan),
            "australia" | "au" => Ok(Region::Australia),
            "new-zealand" | "newzealand" | "nz" => Ok(Region::NewZealand),
            "malaysia-singapore-brunei" | "malaysia" | "sg" => Ok(Region::MalaysiaSingaporeBrunei),
            _ => Err(format!(
                "Unknown region: '{}'. Valid options: north-america, europe, japan, australia, new-zealand, malaysia-singapore-brunei",
                s
            )),
        }
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.config().name)
    }
}

/// Configuration for a geographic region
#[derive(Debug, Clone)]
pub struct RegionConfig {
    /// Short name (used in filenames)
    pub name: &'static str,
    /// Human-readable display name
    pub display_name: &'static str,
    /// Bounding box: (west, south, east, north) in WGS84 degrees
    pub bbox: (f64, f64, f64, f64),
    /// Suffix for OSM PBF file naming (e.g., "europe" for "railonly-europe-latest.osm.pbf")
    pub osm_pbf_suffix: &'static str,
}

impl RegionConfig {
    /// Check if a point (lon, lat) is within this region's bounding box
    pub fn contains_point(&self, lon: f64, lat: f64) -> bool {
        let (west, south, east, north) = self.bbox;
        lon >= west && lon <= east && lat >= south && lat <= north
    }

    /// Get the bounding box as a PostGIS-compatible ST_MakeEnvelope string
    /// Format: (xmin, ymin, xmax, ymax, srid)
    pub fn postgis_envelope(&self) -> String {
        let (west, south, east, north) = self.bbox;
        format!(
            "ST_MakeEnvelope({}, {}, {}, {}, 4326)",
            west, south, east, north
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_from_str() {
        assert_eq!(Region::from_str("europe").unwrap(), Region::Europe);
        assert_eq!(Region::from_str("EU").unwrap(), Region::Europe);
        assert_eq!(
            Region::from_str("north-america").unwrap(),
            Region::NorthAmerica
        );
        assert!(Region::from_str("invalid").is_err());
    }

    #[test]
    fn test_contains_point() {
        let europe = Region::Europe.config();
        // Paris should be in Europe
        assert!(europe.contains_point(2.35, 48.86));
        // New York should not be in Europe
        assert!(!europe.contains_point(-74.0, 40.7));
    }

    #[test]
    fn test_output_names() {
        assert_eq!(
            Region::Europe.output_graph_name(),
            "globeflower_graph_europe.bin"
        );
        assert_eq!(
            Region::NorthAmerica.expected_pbf_name(),
            "railonly-north-america-latest.osm.pbf"
        );
    }
}
