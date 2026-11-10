//! Shared in-process OSR graphs. Mutable match stores/search state belong to a
//! single call. Persisted candidates are accepted only for the identical graph.
use super::route_manager::{Coordinates, PlatformStopMeta};
use super::timetable::{LocationIdx, ZeroCopyTimetable};
use anyhow::{Context, Result, ensure};
use osr_ffi::{Profile, ffi};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Condvar, Mutex},
};

/// Maximum endpoint snap distance, including locations inside large buildings or fields.
pub const MAX_ENDPOINT_MATCH_METERS: f64 = 1000.0;

fn expanding_match<T>(mut attempt: impl FnMut(f64) -> Result<Option<T>>) -> Result<Option<T>> {
    for radius in [100.0, 500.0, MAX_ENDPOINT_MATCH_METERS] {
        if let Some(value) = attempt(radius)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// Append a usable match at the first successful radius, or return None. Saved raw candidates
/// cover the initial 100 m; wider retries always query the live graph. Empty
/// native buckets are retained in the store, so callers must use the returned index.
pub fn append_expanding_match(
    router: &ffi::Router,
    mut store: std::pin::Pin<&mut ffi::MatchStore>,
    point: &ffi::Coordinate,
    options: &ffi::MatchOptions,
    initial_raw: &[ffi::RawWayCandidate],
) -> Result<Option<u32>> {
    // All buckets in a native store must share options. Restrict each attempt
    // through its raw candidates while keeping the store's distance limit fixed.
    let options = ffi::MatchOptions {
        max_match_distance_meters: MAX_ENDPOINT_MATCH_METERS,
        ..options.clone()
    };
    expanding_match(|radius| {
        let mut raw: Vec<ffi::RawWayCandidate> = if radius == 100.0 {
            initial_raw.to_vec()
        } else {
            router.get_raw_match(point, radius)?
        };
        raw.retain(|c| f64::from(c.distance_to_way) <= radius);
        // Empty raw input makes OSR perform its own geometric lookup at the
        // options limit, which would skip our smaller-radius attempts.
        if raw.is_empty() {
            return Ok(None);
        }
        let index = router.append_lookup_match(store.as_mut(), point, &options, &raw)?;
        let matched = router.export_match(store.as_ref().get_ref(), index)?;
        // OSR may expand internally when profile/level/direction filtering
        // rejects every raw candidate. Do not accept those wider matches until
        // their explicit radius is reached (and never accept beyond 1 km).
        Ok((!matched.distance_to_way.is_empty()
            && matched
                .distance_to_way
                .iter()
                .all(|&d| f64::from(d) <= radius))
        .then_some(index))
    })
}

/// Route only endpoints with a usable bounded match, preserving target order.
pub fn route_expanding_matches(
    router: &ffi::Router,
    origins: &ffi::MatchStore,
    origin: Option<u32>,
    destinations: &ffi::MatchStore,
    targets: &[Option<u32>],
    options: &ffi::RoutingOptions,
) -> Result<Vec<ffi::RouteResult>> {
    let missing = || ffi::RouteResult {
        found: false,
        cost: 0,
        duration_seconds: 0,
        distance_meters: 0.0,
        uses_elevator: false,
        elevation_up: 0,
        elevation_down: 0,
        segments: Vec::new(),
    };
    let Some(origin) = origin else {
        return Ok(targets.iter().map(|_| missing()).collect());
    };
    let indices: Vec<_> = targets.iter().flatten().copied().collect();
    if indices.is_empty() {
        return Ok(targets.iter().map(|_| missing()).collect());
    }
    let paths =
        router.route_one_to_many_cached(origins, origin, destinations, &indices, options)?;
    ensure!(paths.len() == indices.len(), "OSR result count mismatch");
    let mut paths = paths.into_iter();
    Ok(targets
        .iter()
        .map(|target| {
            if target.is_some() {
                paths.next().expect("validated OSR result count")
            } else {
                missing()
            }
        })
        .collect())
}

#[derive(Clone, Copy, Debug)]
pub struct Endpoint {
    pub point: Coordinates,
    pub stop: Option<LocationIdx>,
}
impl Endpoint {
    pub fn position(point: Coordinates) -> Self {
        Self { point, stop: None }
    }
    pub fn stop(tt: &ZeroCopyTimetable<'_>, stop: LocationIdx) -> Result<Self> {
        let c = tt
            .coordinates()
            .get(stop.idx())
            .context("OSR stop outside timetable")?;
        Ok(Self {
            point: Coordinates {
                lat: f64::from(c.lat),
                lon: f64::from(c.lon),
            },
            stop: Some(stop),
        })
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LoadMode {
    #[default]
    Read,
    Mmap,
}
#[derive(Default, Deserialize)]
struct WorkerConfig {
    #[serde(default)]
    excluded_regions: BTreeSet<String>,
    #[serde(default)]
    region_load_modes: BTreeMap<String, LoadMode>,
}
#[derive(Deserialize)]
struct Manager {
    data_dir: PathBuf,
}
#[derive(Deserialize)]
struct Config {
    osm_manager: Manager,
    // Keep existing per-region load/exclusion settings useful after embedding.
    #[serde(default)]
    osr_worker: WorkerConfig,
}
struct Graph {
    router: cxx::SharedPtr<ffi::Router>,
    fingerprint: [u8; 32],
    node_count: u64,
    way_count: u64,
}

/// A separate limit bounds simultaneous native searches across HTTP requests.
/// No process-wide graph mutex is held while matching or routing.
pub struct NativeOsr {
    graphs: BTreeMap<String, Graph>,
    active: Mutex<usize>,
    available: Condvar,
    workers: usize,
}
struct Permit<'a>(&'a NativeOsr);
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        *self.0.active.lock().expect("OSR permit lock poisoned") -= 1;
        self.0.available.notify_one();
    }
}
impl NativeOsr {
    pub fn load(config_path: &Path, workers: usize) -> Result<Self> {
        ensure!((1..=16).contains(&workers), "OSR workers must be in 1..=16");
        let config: Config = toml::from_str(&fs::read_to_string(config_path)?)?;
        let registry: serde_json::Value = serde_json::from_slice(&fs::read(
            config.osm_manager.data_dir.join("regions.geojson"),
        )?)?;
        let features = registry["features"]
            .as_array()
            .context("OSR registry missing features")?;
        let mut graphs = BTreeMap::new();
        for feature in features {
            let id = feature["properties"]["id"]
                .as_str()
                .context("OSR region missing id")?;
            ensure!(
                !id.starts_with('/')
                    && !id.split('/').any(|s| s.is_empty() || s == "." || s == ".."),
                "invalid OSR region id"
            );
            if config.osr_worker.excluded_regions.contains(id) {
                continue;
            }
            let dir = config.osm_manager.data_dir.join("regions").join(id);
            eprintln!("Edelweiss startup: fingerprinting and opening native OSR graph {id}");
            let fingerprint = super::osr_graph_identity::fingerprint(&dir)?;
            let metadata_load_mode = match config
                .osr_worker
                .region_load_modes
                .get(id)
                .unwrap_or(&LoadMode::Read)
            {
                LoadMode::Read => ffi::MetadataLoadMode::ReadIntoMemory,
                LoadMode::Mmap => ffi::MetadataLoadMode::MmapPrivate,
            };
            let router = ffi::open_shared_router_with_options(
                dir.to_str().context("non-UTF8 OSR path")?,
                &ffi::RouterOpenOptions { metadata_load_mode },
            )
            .with_context(|| format!("opening native OSR graph {id}"))?;
            ensure!(!router.is_null(), "OSR returned null graph for {id}");
            ensure!(
                fingerprint == super::osr_graph_identity::fingerprint(&dir)?,
                "OSR graph {id} changed while opening; retry with immutable graph artifacts"
            );
            ensure!(
                graphs
                    .insert(
                        id.to_owned(),
                        Graph {
                            node_count: router.node_count(),
                            way_count: router.way_count(),
                            router,
                            fingerprint,
                        }
                    )
                    .is_none(),
                "duplicate OSR region {id}"
            );
        }
        ensure!(!graphs.is_empty(), "no native OSR graphs enabled");
        Ok(Self {
            graphs,
            active: Mutex::new(0),
            available: Condvar::new(),
            workers,
        })
    }

    fn permit(&self) -> Permit<'_> {
        let mut active = self.active.lock().expect("OSR permit lock poisoned");
        while *active >= self.workers {
            active = self
                .available
                .wait(active)
                .expect("OSR permit lock poisoned");
        }
        *active += 1;
        Permit(self)
    }

    /// Validate availability and report whether Gentian candidates can be used.
    /// A different graph is safe: it is matched afresh, never given stale indexes.
    pub fn has_precomputed_matches(
        &self,
        region: &str,
        tt: &ZeroCopyTimetable<'_>,
    ) -> Result<bool> {
        let graph = self
            .graphs
            .get(region)
            .context("OSR region is not loaded")?;
        Ok(graph.can_reuse(tt, 100.0))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn route(
        &self,
        region: &str,
        tt: &ZeroCopyTimetable<'_>,
        metadata: &[PlatformStopMeta],
        origin: Endpoint,
        targets: &[Endpoint],
        profile: Profile,
        direction: u8,
        max_seconds: u32,
        pedestrian_speed: f32,
        cycling_speed: f32,
        geometry: bool,
    ) -> Result<Vec<ffi::RouteResult>> {
        ensure!(direction <= 1, "invalid OSR query direction");
        ensure!(
            (1..=3600).contains(&max_seconds),
            "invalid OSR duration budget"
        );
        ensure!(targets.len() <= 4096, "too many OSR targets");
        let options = routing_options(
            profile,
            direction,
            max_seconds,
            pedestrian_speed,
            cycling_speed,
            geometry,
        )?;
        let graph = self
            .graphs
            .get(region)
            .context("OSR region is not loaded")?;
        let _permit = self.permit();
        let router = graph.router.as_ref().context("null OSR graph")?;
        let match_options = ffi::MatchOptions {
            profile: options.profile,
            direction,
            reverse: false,
            max_match_distance_meters: options.max_match_distance_meters,
            foot_speed_meters_per_second: options.foot_speed_meters_per_second,
            bike_speed_meters_per_second: options.bike_speed_meters_per_second,
        };
        let mut origins = router.create_match_store();
        let mut destinations = router.create_match_store();
        let (point, raw) =
            graph.match_input(tt, metadata, origin, options.max_match_distance_meters)?;
        let origin_index =
            append_expanding_match(router, origins.pin_mut(), &point, &match_options, &raw)?;
        let destination_options = ffi::MatchOptions {
            reverse: true,
            ..match_options
        };
        let mut indices = Vec::with_capacity(targets.len());
        for &target in targets {
            let (point, raw) =
                graph.match_input(tt, metadata, target, options.max_match_distance_meters)?;
            indices.push(append_expanding_match(
                router,
                destinations.pin_mut(),
                &point,
                &destination_options,
                &raw,
            )?);
        }
        if indices.is_empty() {
            return Ok(Vec::new());
        }
        let paths = route_expanding_matches(
            router,
            origins.as_ref().context("null OSR source matches")?,
            origin_index,
            destinations
                .as_ref()
                .context("null OSR destination matches")?,
            &indices,
            &options,
        )?;
        ensure!(paths.len() == targets.len(), "OSR result count mismatch");
        Ok(paths)
    }
}

impl Graph {
    fn can_reuse(&self, tt: &ZeroCopyTimetable<'_>, radius: f64) -> bool {
        tt.osr_match_graphs().first().is_some_and(|saved| {
            saved.fingerprint == self.fingerprint
                && saved.node_count == self.node_count
                && saved.way_count == self.way_count
                && f64::from(saved.raw_radius_meters) >= radius
        })
    }
    fn match_input(
        &self,
        tt: &ZeroCopyTimetable<'_>,
        metadata: &[PlatformStopMeta],
        endpoint: Endpoint,
        radius: f64,
    ) -> Result<(ffi::Coordinate, Vec<ffi::RawWayCandidate>)> {
        let p = endpoint.point;
        ensure!(
            p.lat.is_finite()
                && p.lon.is_finite()
                && (-90.0..=90.0).contains(&p.lat)
                && (-180.0..=180.0).contains(&p.lon),
            "invalid OSR endpoint"
        );
        let mut coordinate = ffi::Coordinate::outdoor(p.lat, p.lon);
        if let Some(stop) = endpoint.stop {
            let expected = Endpoint::stop(tt, stop)?.point;
            ensure!(
                p.lat == expected.lat && p.lon == expected.lon,
                "OSR stop coordinates differ from timetable"
            );
            if self.can_reuse(tt, radius) {
                let matched = tt.osr_location_matches()[stop.idx()];
                if matched.platform_index != u32::MAX {
                    coordinate = ffi::Coordinate::indoor(p.lat, p.lon, matched.level);
                }
                return Ok((
                    coordinate,
                    tt.osr_candidates_for(stop)
                        .iter()
                        .map(|c| ffi::RawWayCandidate {
                            distance_to_way: c.distance_to_way,
                            way_index: c.way_index,
                            left: ffi::RawNodeCandidate {
                                node_index: c.left_node,
                                distance_to_node: c.left_distance,
                            },
                            right: ffi::RawNodeCandidate {
                                node_index: c.right_node,
                                distance_to_node: c.right_distance,
                            },
                        })
                        .collect(),
                ));
            }
            let meta = metadata
                .get(stop.idx())
                .context("missing OSR platform metadata")?;
            let matched = self.router.match_platform(
                &coordinate,
                tt.get_string(tt.location_id()[stop.idx()]).unwrap_or(""),
                tt.get_string(tt.location_name()[stop.idx()]).unwrap_or(""),
                &meta.platform_code,
                &meta.route_names.join("\u{1f}"),
                25.0,
            )?;
            if matched.found {
                coordinate = ffi::Coordinate::indoor(p.lat, p.lon, matched.level);
            }
        }
        let raw = self.router.get_raw_match(&coordinate, 250.0)?;
        Ok((coordinate, raw))
    }
}

pub fn routing_options(
    profile: Profile,
    direction: u8,
    max_seconds: u32,
    pedestrian_speed: f32,
    cycling_speed: f32,
    geometry: bool,
) -> Result<ffi::RoutingOptions> {
    ensure!(
        pedestrian_speed.is_finite() && (0.3..=10.0).contains(&pedestrian_speed),
        "pedestrian speed out of range"
    );
    ensure!(
        cycling_speed.is_finite() && (0.7..=20.0).contains(&cycling_speed),
        "cycling speed out of range"
    );
    ensure!(
        matches!(profile, Profile::Foot | Profile::BikeFast),
        "unsupported Edelweiss street profile"
    );
    Ok(ffi::RoutingOptions {
        direction,
        max_cost: max_seconds,
        include_segments: geometry,
        foot_speed_meters_per_second: if profile == Profile::Foot {
            pedestrian_speed
        } else {
            0.0
        },
        bike_speed_meters_per_second: if profile == Profile::BikeFast {
            cycling_speed
        } else {
            0.0
        },
        ..ffi::RoutingOptions::default().profile(profile)
    })
}

/// Forward geometry, with exact query endpoints retained as in the worker.
pub fn path_geometry(from: Coordinates, to: Coordinates, path: &ffi::RouteResult) -> Vec<[f64; 2]> {
    if !path.found {
        return Vec::new();
    }
    let mut points = vec![[from.lon, from.lat]];
    for segment in &path.segments {
        for point in &segment.geometry {
            let p = [point.lon, point.lat];
            if points.last() != Some(&p) {
                points.push(p);
            }
        }
    }
    let end = [to.lon, to.lat];
    if points.last() != Some(&end) {
        points.push(end);
    }
    points
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Uses the graph and V6 timetable produced by the SoCal integration job.
    #[test]
    #[ignore = "requires a compiled timetable and its matching OSR graph"]
    fn precomputed_matches_equal_fresh_matching_in_both_directions() {
        use super::super::timetable_mmap::MappedTimetable;
        let timetable_path = PathBuf::from(std::env::var("CATENARY_OSR_TEST_TIMETABLE").unwrap());
        let config = PathBuf::from(std::env::var("CATENARY_OSR_TEST_CONFIG").unwrap());
        let region = std::env::var("CATENARY_OSR_TEST_REGION").unwrap();
        let mapped = MappedTimetable::open(&timetable_path).unwrap();
        let tt = mapped.view().unwrap();
        let native = NativeOsr::load(&config, 3).unwrap();
        assert!(native.has_precomputed_matches(&region, &tt).unwrap());
        let metadata: Vec<PlatformStopMeta> = serde_json::from_slice(
            &fs::read(timetable_path.with_extension("platforms.json")).unwrap(),
        )
        .unwrap();
        // Deliberately change only the identity. Any stale native index must be
        // ignored; all other timetable data and stop order stay identical.
        let mut fresh_bytes = fs::read(&timetable_path).unwrap();
        let fingerprint_at = tt.header().osr_match_graphs.offset as usize;
        fresh_bytes[fingerprint_at] ^= 1;
        let fresh = ZeroCopyTimetable::new(&fresh_bytes).unwrap();
        assert!(!native.has_precomputed_matches(&region, &fresh).unwrap());
        // Whole-feed imports can include stops outside the OSR extract.
        // Require an actual compiled short footpath so this exercises a
        // reachable nonzero route rather than only unreachable candidates.
        let pair = (0..tt.coordinates().len())
            .find_map(|i| {
                let from = LocationIdx(i as u32);
                if tt.osr_candidates_for(from).is_empty() {
                    return None;
                }
                tt.footpaths_from(from)
                    .iter()
                    .find(|edge| {
                        edge.seconds() <= 300 && !tt.osr_candidates_for(edge.target()).is_empty()
                    })
                    .map(|edge| (from, edge.target()))
            })
            .expect("fixture needs a short OSR footpath");
        let mut targets = vec![
            Endpoint::stop(&tt, pair.0).unwrap(),
            Endpoint::stop(&tt, pair.1).unwrap(),
        ];
        targets.extend(
            (0..tt.coordinates().len())
                .filter(|&i| !tt.osr_candidates_for(LocationIdx(i as u32)).is_empty())
                .take(30)
                .map(|i| Endpoint::stop(&tt, LocationIdx(i as u32)).unwrap()),
        );
        let mut found = 0;
        for direction in [0, 1] {
            for (profile, foot_speed, bike_speed) in [
                (Profile::Foot, 1.2, 4.2),
                (Profile::Foot, 1.7, 4.2),
                (Profile::BikeFast, 1.2, 4.2),
                (Profile::BikeFast, 1.2, 7.0),
            ] {
                let run = |table: &ZeroCopyTimetable<'_>| {
                    native
                        .route(
                            &region, table, &metadata, targets[0], &targets, profile, direction,
                            300, foot_speed, bike_speed, false,
                        )
                        .unwrap()
                };
                let (cached, uncached) = rayon::join(|| run(&tt), || run(&fresh));
                for (a, b) in cached.iter().zip(&uncached) {
                    assert_eq!(
                        (a.found, a.cost, a.duration_seconds),
                        (b.found, b.cost, b.duration_seconds)
                    );
                    found += usize::from(a.found);
                }
            }
        }
        assert!(
            found > 0,
            "fixture must exercise at least one reachable street route"
        );
        // Geometry uses exactly the same cached levels and raw candidates.
        let mut geometries = Vec::new();
        for table in [&tt, &fresh] {
            let paths = native
                .route(
                    &region,
                    table,
                    &metadata,
                    targets[0],
                    &targets[1..2],
                    Profile::Foot,
                    0,
                    300,
                    1.2,
                    4.2,
                    true,
                )
                .unwrap();
            assert_eq!(paths.len(), 1);
            geometries.push(path_geometry(targets[0].point, targets[1].point, &paths[0]));
        }
        assert_eq!(geometries[0], geometries[1]);
    }

    #[test]
    fn direction_and_custom_speeds_are_applied_to_matching_and_search() {
        let foot = routing_options(Profile::Foot, 1, 600, 1.7, 7.0, false).unwrap();
        assert_eq!(foot.direction, 1);
        assert_eq!(foot.foot_speed_meters_per_second, 1.7);
        assert_eq!(foot.bike_speed_meters_per_second, 0.0);
        assert!(!foot.include_segments);
        let bike = routing_options(Profile::BikeFast, 0, 1200, 1.2, 7.0, true).unwrap();
        assert_eq!(bike.profile, Profile::BikeFast as u8);
        assert_eq!(bike.bike_speed_meters_per_second, 7.0);
        assert_eq!(bike.foot_speed_meters_per_second, 0.0);
        assert!(routing_options(Profile::Foot, 0, 600, f32::NAN, 7.0, false).is_err());
    }
}

#[cfg(test)]
mod expanding_match_tests {
    use super::*;

    #[test]
    fn stops_at_first_usable_radius() {
        for expected in [100.0, 500.0, 1000.0] {
            let mut visited = Vec::new();
            let result = expanding_match(|radius| {
                visited.push(radius);
                Ok((radius >= expected).then_some(radius))
            })
            .unwrap();
            assert_eq!(result, Some(expected));
            assert_eq!(visited.last(), Some(&expected));
        }
    }

    #[test]
    fn empty_matches_stop_at_one_kilometre() {
        let mut visited = Vec::new();
        let result = expanding_match::<()>(|radius| {
            visited.push(radius);
            Ok(None)
        })
        .unwrap();
        assert_eq!(result, None);
        assert_eq!(visited, [100.0, 500.0, 1000.0]);
    }

    #[test]
    fn native_errors_are_propagated() {
        let mut attempts = 0;
        assert!(
            expanding_match::<()>(|_| {
                attempts += 1;
                anyhow::bail!("native lookup failed")
            })
            .is_err()
        );
        assert_eq!(attempts, 1);
    }
}
