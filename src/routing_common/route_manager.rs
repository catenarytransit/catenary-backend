use crate::routing_common::metadata_index::MetadataIndex;
use crate::routing_common::raptor_query::{self, Journey, Query, WalkingEndpoint};
use crate::routing_common::raptor_arrive_by;
use crate::routing_common::profile_search::{self, ProfileOptions};
use crate::routing_common::routing_regions::RoutingRegionsConfig;
use crate::routing_common::timetable::{LocationIdx, ZeroCopyTimetable};
use crate::routing_common::timetable_mmap::MappedTimetable;
use anyhow::{Context, Result, bail, ensure};
use geo::{Contains, Point};
use geojson::{GeoJson, Value};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Coordinates {
    pub lat: f64,
    pub lon: f64,
}

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
fn default_access() -> f64 {
    1500.0
}
fn default_boardings() -> usize {
    5
}
fn default_duration() -> u32 {
    6 * 60 * 60
}

#[derive(Debug, Deserialize)]
struct StreetManager {
    data_dir: PathBuf,
}
#[derive(Debug, Deserialize)]
struct LocalConfig {
    osm_manager: StreetManager,
}

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
        GeoJson::FeatureCollection(c) => c
            .features
            .into_iter()
            .map(|f| f.geometry.context("GeoJSON feature lacks geometry"))
            .collect::<Result<Vec<_>>>()?,
    };
    geometries
        .into_iter()
        .map(|g| {
            ensure!(
                matches!(g.value, Value::Polygon(_) | Value::MultiPolygon(_)),
                "region must consist of Polygon or MultiPolygon"
            );
            let shape: geo::Geometry<f64> = g.try_into()?;
            Ok(shape)
        })
        .collect()
}

fn contains(geometries: &[geo::Geometry<f64>], point: Coordinates) -> bool {
    let p = Point::new(point.lon, point.lat);
    geometries.iter().any(|geometry| geometry.contains(&p))
}

fn bbox_area(geometry: &geo::Geometry<f64>) -> f64 {
    use geo::BoundingRect;
    geometry
        .bounding_rect()
        .map(|r| r.width() * r.height())
        .unwrap_or(f64::INFINITY)
}

/// Choose the most specific compiled transit region for one coordinate.
/// Do not select a larger region simply because it contains both endpoints.
fn transit_region_for<'a>(
    regions: impl Iterator<Item = (&'a str, &'a [geo::Geometry<f64>])>,
    point: Coordinates,
) -> Option<&'a str> {
    regions
        .filter(|(_, geometries)| contains(geometries, point))
        .min_by(|(id_a, geometry_a), (id_b, geometry_b)| {
            let area = |pieces: &[geo::Geometry<f64>]| pieces.iter().map(bbox_area).sum::<f64>();
            area(geometry_a)
                .total_cmp(&area(geometry_b))
                .then(id_a.cmp(id_b))
        })
        .map(|(id, _)| id)
}

fn require_same_transit_region<'a>(
    origin: Option<&'a str>,
    destination: Option<&'a str>,
) -> Result<&'a str> {
    let origin = origin.context("origin is outside all compiled transit regions")?;
    let destination = destination.context("destination is outside all compiled transit regions")?;
    ensure!(
        origin == destination,
        "origin and destination are in different compiled transit regions: {origin} and {destination}"
    );
    Ok(origin)
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
        let streets = collection
            .features
            .into_iter()
            .map(|f| {
                let id = f
                    .properties
                    .as_ref()
                    .and_then(|p| p.get("id"))
                    .and_then(|v| v.as_str())
                    .context("OSR region missing id")?
                    .to_owned();
                let g: geo::Geometry<f64> = f
                    .geometry
                    .context("OSR region has no geometry")?
                    .try_into()?;
                let dir = osr.osm_manager.data_dir.join("regions").join(&id);
                ensure!(
                    dir.join("routing.bin").is_file(),
                    "OSR graph missing for {id}"
                );
                Ok(StreetRegion {
                    id,
                    area: bbox_area(&g),
                    geometry: g,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut transit = Vec::new();
        for region in &regions.regions {
            let geom_json = regions.geometry_json(routing_config, &region.id)?;
            let parsed: serde_json::Value = serde_json::from_str(&geom_json)?;
            let pieces = parsed
                .get("geometries")
                .and_then(|v| v.as_array())
                .context("routing region geometry collection missing geometries")?;
            let mut geometries = Vec::new();
            for piece in pieces {
                geometries.extend(polygon_geometries(GeoJson::from_str(&piece.to_string())?)?);
            }
            let path = regions.timetable_path(routing_config, &region.id, None)?;
            let timetable = MappedTimetable::open(&path).with_context(|| {
                format!(
                    "mapping transit region {} from {}",
                    region.id,
                    path.display()
                )
            })?;
            let metadata = MetadataIndex::build(&timetable.view()?)?;
            transit.push(TransitRegion {
                id: region.id.clone(),
                geometry: geometries,
                timetable,
                metadata,
            });
        }
        ensure!(
            !transit.is_empty() && !streets.is_empty(),
            "no routable regions"
        );
        Ok(Self {
            transit,
            streets,
            settings,
        })
    }

    fn street_for(&self, point: Coordinates) -> Result<&str> {
        let p = Point::new(point.lon, point.lat);
        self.streets
            .iter()
            .filter(|r| r.geometry.contains(&p))
            .min_by(|a, b| a.area.total_cmp(&b.area).then(a.id.cmp(&b.id)))
            .map(|r| r.id.as_str())
            .context("coordinate is outside all OSR regions")
    }

    pub fn select(&self, from: Coordinates, to: Coordinates) -> Result<RoutingSelection> {
        for point in [from, to] {
            ensure!(
                point.lat.is_finite()
                    && point.lon.is_finite()
                    && (-90.0..=90.0).contains(&point.lat)
                    && (-180.0..=180.0).contains(&point.lon),
                "invalid coordinates"
            );
        }
        let origin_region = transit_region_for(
            self.transit
                .iter()
                .map(|r| (r.id.as_str(), r.geometry.as_slice())),
            from,
        );
        let destination_region = transit_region_for(
            self.transit
                .iter()
                .map(|r| (r.id.as_str(), r.geometry.as_slice())),
            to,
        );
        // Reject cross-region requests before contacting OSR or running RAPTOR.
        // Different OSR street-graph regions are still permitted within one
        // transit region, since access and egress can use separate OSR graphs.
        let region = require_same_transit_region(origin_region, destination_region)?;
        let start_osr = self.street_for(from)?.to_owned();
        let end_osr = self.street_for(to)?.to_owned();
        Ok(RoutingSelection {
            transit_region: region.to_owned(),
            origin_osr_region: start_osr,
            destination_osr_region: end_osr,
        })
    }

    pub fn timetable_for(&self, name: &str) -> Result<ZeroCopyTimetable<'_>> {
        self.transit
            .iter()
            .find(|r| r.id == name)
            .context("routing region disappeared")?
            .timetable
            .view()
            .map_err(Into::into)
    }

    pub fn metadata_for(&self, name: &str) -> Result<&MetadataIndex> {
        self.transit
            .iter()
            .find(|r| r.id == name)
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
        let (journeys, raptor_stats) = raptor_query::route_with_stats(
            &tt,
            &Query {
                departure_unix_seconds,
                access,
                egress,
                direct_walk_seconds,
                max_boardings: self.settings.max_boardings,
                max_duration_seconds: self.settings.max_duration_seconds,
            },
        )?;
        Ok(RoutingResponse {
            selection,
            journeys,
            raptor_stats,
        })
    }

    /// MOTIS-style timetable view with an adaptive window and departure-aware
    /// Pareto filtering. OSR access/egress are reused for all profile anchors.
    pub fn route_profile_with_access(
        &self,
        selection: RoutingSelection,
        unix_seconds: i64,
        access: Vec<WalkingEndpoint>,
        egress: Vec<WalkingEndpoint>,
        direct_walk_seconds: Option<u32>,
        arrive_by: bool,
        options: ProfileOptions,
    ) -> Result<RoutingResponse> {
        let tt = self.timetable_for(&selection.transit_region)?;
        let (journeys, raptor_stats) = profile_search::route_profile_with_stats(
            &tt,
            &Query {
                departure_unix_seconds: unix_seconds,
                access,
                egress,
                direct_walk_seconds,
                max_boardings: self.settings.max_boardings,
                max_duration_seconds: options.max_journey_duration_seconds,
            },
            options,
            arrive_by,
        )?;
        Ok(RoutingResponse { selection, journeys, raptor_stats })
    }

    /// Reverse RAPTOR: find the latest departure that reaches the destination by
    /// arrival_unix_seconds. Access and egress retain their original direction.
    pub fn route_arrive_by_with_access(
        &self,
        selection: RoutingSelection,
        arrival_unix_seconds: i64,
        access: Vec<WalkingEndpoint>,
        egress: Vec<WalkingEndpoint>,
        direct_walk_seconds: Option<u32>,
    ) -> Result<RoutingResponse> {
        let tt = self.timetable_for(&selection.transit_region)?;
        let (journeys, raptor_stats) = raptor_arrive_by::route_arrive_by_with_stats(
            &tt,
            &Query {
                departure_unix_seconds: arrival_unix_seconds,
                access,
                egress,
                direct_walk_seconds,
                max_boardings: self.settings.max_boardings,
                max_duration_seconds: self.settings.max_duration_seconds,
            },
        )?;
        Ok(RoutingResponse { selection, journeys, raptor_stats })
    }

    pub fn nearest_stops(
        &self,
        region: &str,
        point: Coordinates,
        limit: usize,
    ) -> Result<Vec<(LocationIdx, f64)>> {
        let tt = self.timetable_for(region)?;
        let mut stops = Vec::new();
        for (i, coordinate) in tt.coordinates().iter().enumerate() {
            let d = haversine_meters(
                point.lat,
                point.lon,
                f64::from(coordinate.lat),
                f64::from(coordinate.lon),
            );
            if d <= self.settings.access_radius_meters {
                stops.push((LocationIdx(i as u32), d));
            }
        }
        stops.sort_by(|a, b| a.1.total_cmp(&b.1));
        stops.truncate(limit);
        Ok(stops)
    }
}

fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dy = (lat2 - lat1).to_radians();
    let dx = (lon2 - lon1).to_radians();
    let a = (dy * 0.5).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dx * 0.5).sin().powi(2);
    12_742_000.0 * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rectangle(west: f64, south: f64, east: f64, north: f64) -> geo::Geometry<f64> {
        geo::Geometry::Polygon(geo::Rect::new((west, south), (east, north)).to_polygon())
    }

    #[test]
    fn same_region_is_accepted() {
        let west = [rectangle(-120.0, 30.0, -115.0, 40.0)];
        let east = [rectangle(-115.0, 30.0, -110.0, 40.0)];
        let regions = [("west", west.as_slice()), ("east", east.as_slice())];
        let from = Coordinates {
            lat: 34.0,
            lon: -118.0,
        };
        let to = Coordinates {
            lat: 35.0,
            lon: -117.0,
        };
        let a = transit_region_for(regions.iter().copied(), from);
        let b = transit_region_for(regions.iter().copied(), to);
        assert_eq!(require_same_transit_region(a, b).unwrap(), "west");
    }

    #[test]
    fn different_regions_are_rejected() {
        let west = [rectangle(-120.0, 30.0, -115.0, 40.0)];
        let east = [rectangle(-115.0, 30.0, -110.0, 40.0)];
        let regions = [("west", west.as_slice()), ("east", east.as_slice())];
        let from = transit_region_for(
            regions.iter().copied(),
            Coordinates {
                lat: 34.0,
                lon: -118.0,
            },
        );
        let to = transit_region_for(
            regions.iter().copied(),
            Coordinates {
                lat: 34.0,
                lon: -112.0,
            },
        );
        let err = require_same_transit_region(from, to)
            .unwrap_err()
            .to_string();
        assert!(err.contains("different compiled transit regions: west and east"));
    }

    #[test]
    fn overlapping_regions_do_not_fall_back_to_larger_region() {
        let large = [rectangle(-120.0, 30.0, -110.0, 40.0)];
        let small = [rectangle(-119.0, 33.0, -117.0, 35.0)];
        let regions = [("large", large.as_slice()), ("small", small.as_slice())];
        let from = transit_region_for(
            regions.iter().copied(),
            Coordinates {
                lat: 34.0,
                lon: -118.0,
            },
        );
        let to = transit_region_for(
            regions.iter().copied(),
            Coordinates {
                lat: 34.0,
                lon: -112.0,
            },
        );
        assert_eq!(from, Some("small"));
        assert_eq!(to, Some("large"));
        assert!(require_same_transit_region(from, to).is_err());
    }

    #[test]
    fn equal_area_overlap_uses_region_id_tiebreaker() {
        let a = [rectangle(-119.0, 33.0, -117.0, 35.0)];
        let b = [rectangle(-119.0, 33.0, -117.0, 35.0)];
        let regions = [("zeta", a.as_slice()), ("alpha", b.as_slice())];
        assert_eq!(
            transit_region_for(
                regions.iter().copied(),
                Coordinates {
                    lat: 34.0,
                    lon: -118.0
                }
            ),
            Some("alpha")
        );
    }

    #[test]
    fn coordinates_outside_compiled_regions_are_rejected() {
        let region = [rectangle(-119.0, 33.0, -117.0, 35.0)];
        let regions = [("socal", region.as_slice())];
        let inside = transit_region_for(
            regions.iter().copied(),
            Coordinates {
                lat: 34.0,
                lon: -118.0,
            },
        );
        let outside = transit_region_for(
            regions.iter().copied(),
            Coordinates {
                lat: 48.0,
                lon: 2.0,
            },
        );
        let err = require_same_transit_region(inside, outside)
            .unwrap_err()
            .to_string();
        assert!(err.contains("destination is outside all compiled transit regions"));
    }
}
