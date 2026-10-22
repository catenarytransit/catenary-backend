use crate::routing_common::raptor_query::{self, Journey, Query, WalkingEndpoint};
use crate::routing_common::metadata_index::MetadataIndex;
use crate::routing_common::routing_regions::RoutingRegionsConfig;
use crate::routing_common::timetable::{LocationIdx, ZeroCopyTimetable};
use crate::routing_common::timetable_mmap::MappedTimetable;
use anyhow::{Context, Result, bail, ensure};
use geo::{Contains, Point};
use geojson::{GeoJson, Value};
use serde::{Deserialize, Serialize};
use std::{fs, path::{Path, PathBuf}, str::FromStr};

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Coordinates { pub lat: f64, pub lon: f64 }

#[derive(Debug, Deserialize)]
pub struct RouterSettings {
    pub data_dir: PathBuf,
    #[serde(default = "default_access")]
    pub access_radius_meters: f64,
    #[serde(default = "default_boardings")]
    pub max_boardings: usize,
    #[serde(default = "default_duration")]
    pub max_duration_seconds: u32,
}
fn default_access() -> f64 { 1500.0 }
fn default_boardings() -> usize { 5 }
fn default_duration() -> u32 { 6 * 60 * 60 }

#[derive(Debug, Deserialize)]
struct StreetManager { data_dir: PathBuf }
#[derive(Debug, Deserialize)]
struct LocalConfig { osm_manager: StreetManager }

pub struct TransitRegion {
    pub id: String,
    geometry: Vec<geo::Geometry<f64>>,
    pub timetable: MappedTimetable,
    pub metadata: MetadataIndex,
}

#[derive(Clone, Debug, Serialize)]
pub struct RoutingSelection {
    pub transit_region: String,
    pub origin_osr_region: String,
    pub destination_osr_region: String,
}

#[derive(Clone)]
struct StreetRegion {
    id: String,
    geometry: geo::Geometry<f64>,
    area: f64,
}

#[derive(Debug, Serialize)]
pub struct RoutingResponse {
    pub selection: RoutingSelection,
    pub journeys: Vec<Journey>,
    pub raptor_stats: raptor_query::RaptorStats,
}

pub struct Router {
    pub transit: Vec<TransitRegion>,
    streets: Vec<StreetRegion>,
    pub settings: RouterSettings,
}

fn polygon_geometries(value: GeoJson) -> Result<Vec<geo::Geometry<f64>>> {
    let geometries = match value {
        GeoJson::Geometry(g) => vec![g],
        GeoJson::Feature(f) => vec![f.geometry.context("GeoJSON feature lacks geometry")?],
        GeoJson::FeatureCollection(c) => c.features.into_iter()
            .map(|f| f.geometry.context("GeoJSON feature lacks geometry"))
            .collect::<Result<Vec<_>>>()?,
    };
    geometries.into_iter().map(|g| {
        ensure!(matches!(g.value, Value::Polygon(_) | Value::MultiPolygon(_)),
            "region must consist of Polygon or MultiPolygon");
        let shape: geo::Geometry<f64> = g.try_into()?;
        Ok(shape)
    }).collect()
}

fn contains(geometries: &[geo::Geometry<f64>], point: Coordinates) -> bool {
    let p = Point::new(point.lon, point.lat);
    geometries.iter().any(|geometry| geometry.contains(&p))
}

fn bbox_area(geometry: &geo::Geometry<f64>) -> f64 {
    use geo::BoundingRect;
    geometry.bounding_rect().map(|r| r.width() * r.height()).unwrap_or(f64::INFINITY)
}

impl Router {
    pub fn load(
        routing_config: &Path,
        osr_config: &Path,
        settings: RouterSettings,
    ) -> Result<Self> {
        let regions = RoutingRegionsConfig::load(routing_config)?;
        let osr: LocalConfig = toml::from_str(&fs::read_to_string(osr_config)?)?;
        let registry = osr.osm_manager.data_dir.join("regions.geojson");
        let registry_json = GeoJson::from_str(&fs::read_to_string(&registry)?)?;
        let GeoJson::FeatureCollection(collection) = registry_json else {
            bail!("OSR region registry is not a FeatureCollection");
        };
        let streets = collection.features.into_iter().map(|f| {
            let id = f.properties.as_ref().and_then(|p| p.get("id"))
                .and_then(|v| v.as_str()).context("OSR region missing id")?.to_owned();
            let g: geo::Geometry<f64> = f.geometry.context("OSR region has no geometry")?.try_into()?;
            let dir = osr.osm_manager.data_dir.join("regions").join(&id);
            ensure!(dir.join("routing.bin").is_file(), "OSR graph missing for {id}");
            Ok(StreetRegion {id, area: bbox_area(&g), geometry:g})
        }).collect::<Result<Vec<_>>>()?;
        let mut transit = Vec::new();
        for region in &regions.regions {
            let geom_json = regions.geometry_json(routing_config, &region.id)?;
            let parsed: serde_json::Value = serde_json::from_str(&geom_json)?;
            let pieces = parsed.get("geometries").and_then(|v|v.as_array())
                .context("routing region geometry collection missing geometries")?;
            let mut geometries = Vec::new();
            for piece in pieces {
                geometries.extend(polygon_geometries(GeoJson::from_str(&piece.to_string())?)?);
            }
            let path = settings.data_dir.join(format!("{}.bin", region.id));
            let timetable = MappedTimetable::open(&path)
                .with_context(|| format!("mapping transit region {} from {}",region.id,path.display()))?;
            let metadata = MetadataIndex::build(&timetable.view()?)?;
            transit.push(TransitRegion {id:region.id.clone(),geometry:geometries,timetable,metadata});
        }
        ensure!(!transit.is_empty() && !streets.is_empty(), "no routable regions");
        Ok(Self {transit, streets, settings})
    }

    fn street_for(&self, point: Coordinates) -> Result<&str> {
        let p = Point::new(point.lon,point.lat);
        self.streets.iter().filter(|r| r.geometry.contains(&p))
            .min_by(|a,b| a.area.total_cmp(&b.area).then(a.id.cmp(&b.id)))
            .map(|r|r.id.as_str()).context("coordinate is outside all OSR regions")
    }

    pub fn select(&self, from: Coordinates, to: Coordinates) -> Result<RoutingSelection> {
        for point in [from,to] {
            ensure!(point.lat.is_finite() && point.lon.is_finite()
                && (-90.0..=90.0).contains(&point.lat) && (-180.0..=180.0).contains(&point.lon),
                "invalid coordinates");
        }
        let start_osr = self.street_for(from)?.to_owned();
        let end_osr = self.street_for(to)?.to_owned();
        let region = self.transit.iter().find(|r|
            contains(&r.geometry, from) && contains(&r.geometry, to))
            .context("no single compiled transit region contains both endpoints")?;
        Ok(RoutingSelection {
            transit_region: region.id.clone(),
            origin_osr_region:start_osr,
            destination_osr_region:end_osr,
        })
    }

    pub fn timetable_for(&self, name: &str) -> Result<ZeroCopyTimetable<'_>> {
        self.transit.iter().find(|r|r.id == name)
            .context("routing region disappeared")?.timetable.view()
            .map_err(Into::into)
    }

    pub fn metadata_for(&self, name: &str) -> Result<&MetadataIndex> {
        self.transit.iter().find(|r|r.id == name)
            .map(|r| &r.metadata)
            .context("routing region disappeared")
    }

    pub fn route_with_access(
        &self,
        selection: RoutingSelection,
        departure_unix_seconds: i64,
        access: Vec<WalkingEndpoint>,
        egress: Vec<WalkingEndpoint>,
        direct_walk_seconds: Option<u32>,
    ) -> Result<RoutingResponse> {
        let tt = self.timetable_for(&selection.transit_region)?;
        let (journeys, raptor_stats) = raptor_query::route_with_stats(&tt, &Query {
            departure_unix_seconds,
            access,
            egress,
            direct_walk_seconds,
            max_boardings:self.settings.max_boardings,
            max_duration_seconds:self.settings.max_duration_seconds,
        })?;
        Ok(RoutingResponse {selection,journeys,raptor_stats})
    }

    pub fn nearest_stops(&self, region: &str, point: Coordinates, limit: usize) -> Result<Vec<(LocationIdx, f64)>> {
        let tt = self.timetable_for(region)?;
        let mut stops = Vec::new();
        for (i, coordinate) in tt.coordinates().iter().enumerate() {
            let d = haversine_meters(
                point.lat,point.lon,f64::from(coordinate.lat),f64::from(coordinate.lon));
            if d <= self.settings.access_radius_meters {
                stops.push((LocationIdx(i as u32), d));
            }
        }
        stops.sort_by(|a,b|a.1.total_cmp(&b.1));
        stops.truncate(limit);
        Ok(stops)
    }
}

fn haversine_meters(lat1:f64,lon1:f64,lat2:f64,lon2:f64)->f64 {
    let dy=(lat2-lat1).to_radians();
    let dx=(lon2-lon1).to_radians();
    let a=(dy*0.5).sin().powi(2)+lat1.to_radians().cos()*lat2.to_radians().cos()*(dx*0.5).sin().powi(2);
    12_742_000.0*a.sqrt().asin()
}
