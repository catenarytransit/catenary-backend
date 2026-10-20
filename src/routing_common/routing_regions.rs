//! Resolve timetable regions into complete GTFS chateaux.
//!
//! Geography is a selection mechanism only. Once a chateau intersects a
//! region, the timetable compiler includes that chateau in full.
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, bail, ensure};
use geojson::{Feature, GeoJson, Geometry, Value};
use postgres::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingRegionsConfig {
    pub schema_version: u32,
    /// A local FeatureCollection with ISO 3166-1 alpha-2 country properties.
    /// Required only when a region specifies countries.
    pub country_boundaries: Option<PathBuf>,
    pub regions: Vec<RoutingRegion>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingRegion {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub geojson: Vec<PathBuf>,
    #[serde(default)]
    pub countries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChateauSource {
    pub chateau: String,
    pub onestop_feed_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Serialize)]
pub struct RegionSelection {
    pub id: String,
    pub name: String,
    pub chateaux: Vec<String>,
    pub sources: Vec<ChateauSource>,
}

impl RoutingRegionsConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let data = fs::read_to_string(path)
            .with_context(|| format!("reading routing regions config {}", path.display()))?;
        let config: Self = toml::from_str(&data)
            .with_context(|| format!("parsing {}", path.display()))?;
        ensure!(config.schema_version == 1, "unsupported routing regions schema version {}", config.schema_version);
        ensure!(!config.regions.is_empty(), "routing regions config contains no regions");
        let mut ids = BTreeSet::new();
        for region in &config.regions {
            ensure!(valid_region_id(&region.id), "invalid routing region id {:?}", region.id);
            ensure!(!region.name.trim().is_empty(), "region {} has no name", region.id);
            ensure!(!region.geojson.is_empty() || !region.countries.is_empty(),
                "region {} needs geojson or countries", region.id);
            ensure!(ids.insert(region.id.as_str()), "duplicate routing region {}", region.id);
            for code in &region.countries {
                ensure!(valid_country_code(code), "invalid country code {:?} in {}", code, region.id);
            }
        }
        Ok(config)
    }

    pub fn region(&self, id: &str) -> Result<&RoutingRegion> {
        self.regions.iter().find(|region| region.id == id)
            .with_context(|| format!("unknown routing region {id:?}"))
    }

    /// Build a GeoJSON geometry fragment accepted by ST_GeomFromGeoJSON.
    /// Paths are relative to the config file; country boundaries are never
    /// downloaded implicitly so builds remain reproducible.
    pub fn geometry_json(&self, config_path: &Path, id: &str) -> Result<String> {
        let region = self.region(id)?;
        let root = config_path.parent().unwrap_or_else(|| Path::new("."));
        let mut geometries = Vec::<Geometry>::new();

        for relative in &region.geojson {
            let path = root.join(relative);
            let data = fs::read_to_string(&path)
                .with_context(|| format!("reading region geometry {}", path.display()))?;
            let geojson = GeoJson::from_str(&data)
                .with_context(|| format!("parsing region geometry {}", path.display()))?;
            geometries.extend(geometries_from_geojson(geojson)
                .with_context(|| format!("reading polygons in {}", path.display()))?);
        }

        if !region.countries.is_empty() {
            let path = root.join(self.country_boundaries.as_ref().context(
                "country_boundaries must be set when a region uses countries"
            )?);
            let data = fs::read_to_string(&path)
                .with_context(|| format!("reading country boundaries {}", path.display()))?;
            let geojson = GeoJson::from_str(&data)
                .with_context(|| format!("parsing country boundaries {}", path.display()))?;
            let GeoJson::FeatureCollection(collection) = geojson else {
                bail!("country boundaries must be a GeoJSON FeatureCollection");
            };
            let requested: BTreeSet<_> = region.countries.iter()
                .map(|code| code.to_ascii_uppercase()).collect();
            let mut found = BTreeSet::new();
            for feature in collection.features {
                let Some(code) = feature_country_code(&feature) else { continue };
                if requested.contains(&code) {
                    let geometry = feature.geometry.with_context(|| format!(
                        "country {code} has no geometry in {}", path.display()
                    ))?;
                    validate_polygonal(&geometry)?;
                    geometries.push(geometry);
                    found.insert(code);
                }
            }
            let missing: Vec<_> = requested.difference(&found).cloned().collect();
            ensure!(missing.is_empty(),
                "country boundaries missing requested ISO alpha-2 codes: {}", missing.join(", "));
        }

        ensure!(!geometries.is_empty(), "routing region {id} has no polygons");
        let geometry = Geometry::new(Value::GeometryCollection(
            geometries
        ));
        serde_json::to_string(&geometry).context("encoding routing region geometry")
    }
}

fn valid_region_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase()
        || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-') && !s.ends_with('-')
}

fn valid_country_code(s: &str) -> bool {
    s.len() == 2 && s.bytes().all(|b| b.is_ascii_alphabetic())
}

fn feature_country_code(feature: &Feature) -> Option<String> {
    let properties = feature.properties.as_ref()?;
    for key in ["ISO3166-1-Alpha-2", "ISO_A2", "ISO_A2_EH", "iso_a2"] {
        if let Some(code) = properties.get(key).and_then(serde_json::Value::as_str) {
            if valid_country_code(code) {
                return Some(code.to_ascii_uppercase());
            }
        }
    }
    None
}

fn geometries_from_geojson(geojson: GeoJson) -> Result<Vec<Geometry>> {
    let geometries: Vec<Geometry> = match geojson {
        GeoJson::Geometry(g) => vec![g],
        GeoJson::Feature(feature) => vec![feature.geometry.context("GeoJSON feature has no geometry")?],
        GeoJson::FeatureCollection(collection) => collection.features.into_iter()
            .map(|feature| feature.geometry.context("GeoJSON feature has no geometry"))
            .collect::<Result<_>>()?,
    };
    ensure!(!geometries.is_empty(), "GeoJSON contains no features");
    for geometry in &geometries {
        validate_polygonal(geometry)?;
    }
    Ok(geometries)
}

fn validate_polygonal(geometry: &Geometry) -> Result<()> {
    let polygons = match &geometry.value {
        Value::Polygon(rings) => {
            return validate_rings(rings);
        }
        Value::MultiPolygon(polygons) => polygons,
        _ => bail!("routing region geometries must be Polygon or MultiPolygon"),
    };
    ensure!(!polygons.is_empty(), "empty MultiPolygon");
    for polygon in polygons {
        validate_rings(polygon)?;
    }
    Ok(())
}

fn validate_rings(rings: &[Vec<Vec<f64>>]) -> Result<()> {
    ensure!(!rings.is_empty(), "polygon without an exterior ring");
    for ring in rings {
        ensure!(ring.len() >= 4, "polygon ring must have at least four positions");
        ensure!(ring.first() == ring.last(), "polygon ring is not closed");
        for coordinate in ring {
            ensure!(coordinate.len() == 2, "region coordinates must be [longitude, latitude]");
            let (longitude, latitude) = (coordinate[0], coordinate[1]);
            ensure!(longitude.is_finite() && (-180.0..=180.0).contains(&longitude),
                "invalid longitude {longitude}");
            ensure!(latitude.is_finite() && (-90.0..=90.0).contains(&latitude),
                "invalid latitude {latitude}");
        }
    }
    Ok(())
}

/// Execute exact spatial intersection, not merely bounding-box overlap.
/// The database validates the resulting geometry before selecting chateaux.
pub fn select_chateaux(client: &mut Client, geometry_json: &str) -> Result<Vec<String>> {
    let row = client.query_one(
        "SELECT ST_IsValid(g.geom), ST_IsEmpty(g.geom) \
         FROM (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::text), 4326) AS geom) AS g",
        &[&geometry_json],
    ).context("validating routing region geometry in PostGIS")?;
    let valid: bool = row.get(0);
    let empty: bool = row.get(1);
    ensure!(valid && !empty, "routing region geometry is invalid or empty");

    let rows = client.query(
        "WITH region AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON($1::text), 4326) AS geom) \
         SELECT c.chateau FROM gtfs.chateaus AS c CROSS JOIN region AS r \
         WHERE c.hull IS NOT NULL AND c.hull && r.geom \
         AND ST_Intersects(c.hull, r.geom) ORDER BY c.chateau",
        &[&geometry_json],
    ).context("selecting chateaux intersecting routing region")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Select a single completed production import for each static feed.
/// The result is sorted for stable SourceIdx assignment.
pub fn select_active_sources(client: &mut Client, chateaux: &[String]) -> Result<Vec<ChateauSource>> {
    if chateaux.is_empty() {
        return Ok(Vec::new());
    }
    let rows = client.query(
        "SELECT DISTINCT ON (sf.onestop_feed_id) \
           sf.chateau, sf.onestop_feed_id, ingest.attempt_id \
         FROM gtfs.static_feeds AS sf \
         JOIN gtfs.ingested_static AS ingest \
           ON ingest.onestop_feed_id = sf.onestop_feed_id \
         WHERE sf.chateau = ANY($1::text[]) \
           AND ingest.production AND ingest.ingestion_successfully_finished \
           AND NOT ingest.ingesting_in_progress \
           AND NOT ingest.ingestion_errored AND NOT ingest.deleted \
         ORDER BY sf.onestop_feed_id, ingest.ingest_end_unix_time_ms DESC, ingest.attempt_id DESC",
        &[&chateaux],
    ).context("selecting current production GTFS imports")?;
    let mut sources: Vec<_> = rows.into_iter().map(|row| ChateauSource {
        chateau: row.get(0),
        onestop_feed_id: row.get(1),
        attempt_id: row.get(2),
    }).collect();
    sources.sort_by(|a, b| (&a.chateau, &a.onestop_feed_id, &a.attempt_id)
        .cmp(&(&b.chateau, &b.onestop_feed_id, &b.attempt_id)));
    Ok(sources)
}

pub fn resolve(
    config: &RoutingRegionsConfig,
    config_path: &Path,
    id: &str,
    client: &mut Client,
) -> Result<RegionSelection> {
    let region = config.region(id)?;
    let geometry = config.geometry_json(config_path, id)?;
    let chateaux = select_chateaux(client, &geometry)?;
    let sources = select_active_sources(client, &chateaux)?;
    Ok(RegionSelection {
        id: region.id.clone(),
        name: region.name.clone(),
        chateaux,
        sources,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_dir() -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("routing-regions-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn southern_california_fixture_is_polygonal() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("routing_regions/config.toml");
        let config = RoutingRegionsConfig::load(&path).unwrap();
        let region = config.region("southern-california").unwrap();
        assert_eq!(region.geojson.len(), 1);
        let geometry = config.geometry_json(&path, &region.id).unwrap();
        assert!(geometry.contains("GeometryCollection"));
        assert!(geometry.contains("Polygon"));
    }

    #[test]
    fn unknown_and_duplicate_regions_are_rejected() {
        let root = fixture_dir();
        let path = root.join("config.toml");
        fs::write(&path, "schema_version = 1\n[[regions]]\nid = 'a'\nname = 'A'\ncountries = ['US']\n[[regions]]\nid = 'a'\nname = 'B'\ncountries = ['CA']\n").unwrap();
        assert!(RoutingRegionsConfig::load(&path).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn country_list_loads_only_requested_features() {
        let root = fixture_dir();
        let path = root.join("config.toml");
        fs::write(&path, "schema_version = 1\ncountry_boundaries = 'countries.geojson'\n[[regions]]\nid = 'test'\nname = 'Test'\ncountries = ['us']\n").unwrap();
        let countries = r#"{"type":"FeatureCollection","features":[
          {"type":"Feature","properties":{"ISO_A2":"US"},"geometry":{"type":"Polygon","coordinates":[[[-120,32],[-110,32],[-110,40],[-120,40],[-120,32]]]}},
          {"type":"Feature","properties":{"ISO_A2":"CA"},"geometry":{"type":"Polygon","coordinates":[[[-140,50],[-130,50],[-130,60],[-140,60],[-140,50]]]}}
        ]}"#;
        fs::write(root.join("countries.geojson"), countries).unwrap();
        let config = RoutingRegionsConfig::load(&path).unwrap();
        let result = config.geometry_json(&path, "test").unwrap();
        assert!(result.contains("-120"));
        assert!(!result.contains("-140"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_missing_country_and_invalid_polygon() {
        let root = fixture_dir();
        let path = root.join("config.toml");
        fs::write(&path, "schema_version = 1\ncountry_boundaries = 'countries.geojson'\n[[regions]]\nid = 'test'\nname = 'Test'\ncountries = ['MX']\n").unwrap();
        fs::write(root.join("countries.geojson"),
            r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"ISO_A2":"US"},"geometry":{"type":"Polygon","coordinates":[[[-120,32],[-110,32],[-110,40],[-120,40],[-120,32]]]}}]}"#).unwrap();
        let config = RoutingRegionsConfig::load(&path).unwrap();
        assert!(config.geometry_json(&path, "test").unwrap_err().to_string().contains("MX"));
        let invalid = Geometry::new(Value::Polygon(vec![vec![
            vec![-118.0, 34.0], vec![-117.0, 34.0], vec![-118.0, 35.0]
        ]]));
        assert!(validate_polygonal(&invalid).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
