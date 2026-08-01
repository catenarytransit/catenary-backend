use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::{Array, Nullable, Text};
use diesel::upsert::excluded;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use futures::{stream, StreamExt, TryStreamExt};
use geo::{BoundingRect, Intersects};
use geo_types::{Geometry, Point as GeoPoint};
use geojson::GeoJson;
use postgis_diesel::types::Point as PgPoint;
use rayon::prelude::*;
use rstar::{AABB, RTree, RTreeObject};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Instant;
use url::{Host, Url};

pub(crate) type PgPolygon = postgis_diesel::types::Polygon<PgPoint>;

#[derive(Debug, Deserialize)]
struct UnifiedAgencyIdRules {
    #[serde(default)]
    generic_names: Vec<String>,
    #[serde(default)]
    exceptions: Vec<UnifiedAgencyIdException>,
}

#[derive(Debug, Deserialize)]
struct UnifiedAgencyIdException {
    unified_id: String,
    #[serde(default)]
    agency_name_equals: Vec<String>,
    #[serde(default)]
    agency_name_contains: Vec<String>,
    #[serde(default)]
    agency_url_host_suffixes: Vec<String>,
}

static UNIFIED_AGENCY_ID_RULES: LazyLock<UnifiedAgencyIdRules> = LazyLock::new(|| {
    toml::from_str(include_str!("unified_agency_id_exceptions.toml"))
        .expect("invalid unified agency ID exception definitions")
});

impl UnifiedAgencyIdException {
    fn matches(&self, agency_name: &str, agency_url_host: Option<&str>) -> bool {
        let normalized_name = agency_name.trim().to_lowercase();
        let has_matcher = !self.agency_name_equals.is_empty()
            || !self.agency_name_contains.is_empty()
            || !self.agency_url_host_suffixes.is_empty();

        has_matcher
            && (self.agency_name_equals.is_empty()
                || self
                    .agency_name_equals
                    .iter()
                    .any(|name| agency_name.trim().eq_ignore_ascii_case(name.trim())))
            && (self.agency_name_contains.is_empty()
                || self
                    .agency_name_contains
                    .iter()
                    .any(|fragment| normalized_name.contains(&fragment.trim().to_lowercase())))
            && (self.agency_url_host_suffixes.is_empty()
                || agency_url_host.is_some_and(|host| {
                    self.agency_url_host_suffixes
                        .iter()
                        .any(|suffix| host_matches_suffix(host, suffix))
                }))
    }
}

fn host_matches_suffix(host: &str, suffix: &str) -> bool {
    let suffix = suffix
        .trim()
        .trim_start_matches('.')
        .trim_end_matches('.')
        .to_ascii_lowercase();

    !suffix.is_empty() && (host == suffix || host.ends_with(&format!(".{suffix}")))
}

fn agency_url_host(agency_url: &str) -> Option<String> {
    let parsed = Url::parse(agency_url)
        .or_else(|_| Url::parse(&format!("https://{agency_url}")))
        .ok()?;

    match parsed.host()? {
        Host::Domain(host) => Some(host.trim_end_matches('.').to_ascii_lowercase()),
        Host::Ipv4(_) | Host::Ipv6(_) => None,
    }
}

fn domain_name_without_tld(host: &str) -> Option<&str> {
    const COMMON_SECOND_LEVEL_DOMAINS: &[&str] =
        &["ac", "co", "com", "edu", "gov", "net", "org"];

    let labels = host
        .split('.')
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>();

    match labels.len() {
        0 => None,
        1 => labels.as_slice().first().copied(),
        len => {
            let tld = labels[len - 1];
            let second_level = labels[len - 2];
            let domain_index = if len >= 3
                && tld.len() == 2
                && COMMON_SECOND_LEVEL_DOMAINS.contains(&second_level)
            {
                len - 3
            } else {
                len - 2
            };

            labels.get(domain_index).copied()
        }
    }
}

fn effective_route_agency_id<'a>(
    route_agency_id: Option<&'a str>,
    sole_agency_id: Option<&'a str>,
) -> Option<&'a str> {
    route_agency_id.or(sole_agency_id)
}

pub fn unified_agency_id_for(agency_name: &str, agency_url: &str) -> String {
    let agency_url_host = agency_url_host(agency_url);

    if let Some(exception) = UNIFIED_AGENCY_ID_RULES
        .exceptions
        .iter()
        .find(|exception| exception.matches(agency_name, agency_url_host.as_deref()))
    {
        return exception.unified_id.clone();
    }

    if UNIFIED_AGENCY_ID_RULES
        .generic_names
        .iter()
        .any(|name| agency_name.trim().eq_ignore_ascii_case(name.trim()))
    {
        if let Some(host) = agency_url_host.as_deref() {
            if let Some(domain_name) = domain_name_without_tld(host) {
                return domain_name.to_string();
            }
        }
    }

    agency_name.replace(' ', "_")
}

#[derive(Clone)]
struct IndexedArea {
    id: String,
    geometry: Geometry<f64>,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedArea {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

#[derive(Clone, Copy, Debug)]
struct CoordinateBounds {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

impl CoordinateBounds {
    fn from_coordinate(longitude: f64, latitude: f64) -> Option<Self> {
        (longitude.is_finite() && latitude.is_finite()).then_some(Self {
            min_x: longitude,
            min_y: latitude,
            max_x: longitude,
            max_y: latitude,
        })
    }

    fn add_coordinate(target: &mut Option<Self>, longitude: f64, latitude: f64) {
        Self::add_bounds(target, Self::from_coordinate(longitude, latitude));
    }

    fn add_bounds(target: &mut Option<Self>, additional: Option<Self>) {
        let Some(additional) = additional else {
            return;
        };

        match target {
            Some(current) => {
                current.min_x = current.min_x.min(additional.min_x);
                current.min_y = current.min_y.min(additional.min_y);
                current.max_x = current.max_x.max(additional.max_x);
                current.max_y = current.max_y.max(additional.max_y);
            }
            None => *target = Some(additional),
        }
    }

    fn from_polygon(polygon: &PgPolygon) -> Option<Self> {
        let mut bounds = None;
        for point in polygon.rings.iter().flatten() {
            Self::add_coordinate(&mut bounds, point.x, point.y);
        }
        bounds
    }

    fn to_polygon(self) -> PgPolygon {
        let srid = Some(4326);
        PgPolygon {
            rings: vec![vec![
                PgPoint::new(self.min_x, self.min_y, srid),
                PgPoint::new(self.max_x, self.min_y, srid),
                PgPoint::new(self.max_x, self.max_y, srid),
                PgPoint::new(self.min_x, self.max_y, srid),
                PgPoint::new(self.min_x, self.min_y, srid),
            ]],
            srid,
        }
    }

    fn as_array(self) -> [f64; 4] {
        [self.min_x, self.min_y, self.max_x, self.max_y]
    }
}

#[derive(Clone, Default)]
struct SpatialAccumulator {
    level_0s: BTreeSet<String>,
    level_1s: BTreeSet<String>,
    bbox: Option<CoordinateBounds>,
}

impl SpatialAccumulator {
    fn merge(&mut self, other: Self) {
        self.level_0s.extend(other.level_0s);
        self.level_1s.extend(other.level_1s);
        CoordinateBounds::add_bounds(&mut self.bbox, other.bbox);
    }

    fn into_metadata(self) -> AgencySpatialMetadata {
        AgencySpatialMetadata {
            level_0s: self.level_0s.into_iter().map(Some).collect(),
            level_1s: self.level_1s.into_iter().map(Some).collect(),
            bbox: self.bbox.map(CoordinateBounds::to_polygon),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AgencySpatialMetadata {
    pub(crate) level_0s: Vec<Option<String>>,
    pub(crate) level_1s: Vec<Option<String>>,
    pub(crate) bbox: Option<PgPolygon>,
}

fn indexed_areas_from_geojson(
    path: &Path,
    id_property: &str,
) -> Result<Vec<IndexedArea>, Box<dyn Error + Send + Sync>> {
    let raw = fs::read_to_string(path)?;
    let geojson = raw.parse::<GeoJson>()?;
    let GeoJson::FeatureCollection(collection) = geojson else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} must contain a GeoJSON FeatureCollection", path.display()),
        )
        .into());
    };

    collection
        .features
        .into_iter()
        .filter_map(|feature| {
            let area_id = feature
                .properties
                .as_ref()
                .and_then(|properties| properties.get(id_property))
                .and_then(|value| value.as_str())?
                .trim();
            if area_id.is_empty() {
                return None;
            }

            let geometry = feature.geometry?;
            Some((area_id.to_string(), geometry))
        })
        .map(|(id, geometry)| {
            let geometry: Geometry<f64> = geometry.try_into()?;
            let bounds = geometry.bounding_rect().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{id_property}={id} has an empty geometry"),
                )
            })?;

            Ok(IndexedArea {
                id,
                envelope: AABB::from_corners(
                    [bounds.min().x, bounds.min().y],
                    [bounds.max().x, bounds.max().y],
                ),
                geometry,
            })
        })
        .collect()
}

fn level_1_geojson_paths(
    directory: &Path,
) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync>> {
    let mut paths = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;

    paths.retain(|path| {
        path.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("gadm41_") && name.ends_with("_1.json"))
    });
    paths.sort();
    Ok(paths)
}

pub struct CountryIndex {
    countries: RTree<IndexedArea>,
    level_1s: RTree<IndexedArea>,
}

impl CountryIndex {
    pub fn from_geojson(
        country_path: &Path,
        level_1_directory: &Path,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let countries = indexed_areas_from_geojson(country_path, "CNTR_ID")?;
        if countries.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "country boundary GeoJSON did not contain any CNTR_ID geometries",
            )
            .into());
        }

        let level_1_paths = level_1_geojson_paths(level_1_directory)?;
        if level_1_paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "{} does not contain any gadm41_*_1.json files",
                    level_1_directory.display()
                ),
            )
            .into());
        }

        let level_1_groups = level_1_paths
            .par_iter()
            .map(|path| indexed_areas_from_geojson(path, "ISO_1"))
            .collect::<Result<Vec<_>, _>>()?;
        let level_1s = level_1_groups.into_iter().flatten().collect::<Vec<_>>();
        if level_1s.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "level-1 boundary files did not contain any ISO_1 geometries",
            )
            .into());
        }

        Ok(Self {
            countries: RTree::bulk_load(countries),
            level_1s: RTree::bulk_load(level_1s),
        })
    }

    fn area_ids_for_coordinate(
        index: &RTree<IndexedArea>,
        longitude: f64,
        latitude: f64,
    ) -> Vec<String> {
        if !longitude.is_finite() || !latitude.is_finite() {
            return Vec::new();
        }

        let point = GeoPoint::new(longitude, latitude);
        let envelope = AABB::from_point([longitude, latitude]);

        index
            .locate_in_envelope_intersecting(&envelope)
            .filter(|area| area.geometry.intersects(&point))
            .map(|area| area.id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn country_ids_for_coordinate(&self, longitude: f64, latitude: f64) -> Vec<String> {
        Self::area_ids_for_coordinate(&self.countries, longitude, latitude)
    }

    fn level_1_ids_for_coordinate(&self, longitude: f64, latitude: f64) -> Vec<String> {
        Self::area_ids_for_coordinate(&self.level_1s, longitude, latitude)
    }

    pub fn spatial_metadata_for_gtfs(
        &self,
        gtfs: &gtfs_structures::Gtfs,
        stop_ids_to_route_ids: &HashMap<String, Vec<String>>,
    ) -> HashMap<String, AgencySpatialMetadata> {
        let mut metadata_by_agency: HashMap<String, SpatialAccumulator> = gtfs
            .agencies
            .iter()
            .map(|agency| {
                (
                    agency.id.clone().unwrap_or_default(),
                    SpatialAccumulator::default(),
                )
            })
            .collect();

        let sole_agency_id = (gtfs.agencies.len() == 1)
            .then(|| gtfs.agencies[0].id.clone().unwrap_or_default());

        for (stop_id, route_ids) in stop_ids_to_route_ids {
            let Some(stop) = gtfs.stops.get(stop_id) else {
                continue;
            };
            let (Some(latitude), Some(longitude)) = (stop.latitude, stop.longitude) else {
                continue;
            };
            if !latitude.is_finite() || !longitude.is_finite() {
                continue;
            }

            let agency_ids = route_ids
                .iter()
                .filter_map(|route_id| gtfs.routes.get(route_id))
                .filter_map(|route| {
                    effective_route_agency_id(
                        route.agency_id.as_deref(),
                        sole_agency_id.as_deref(),
                    )
                    .map(str::to_owned)
                })
                .collect::<HashSet<_>>();
            if agency_ids.is_empty() {
                continue;
            }

            let country_ids = self.country_ids_for_coordinate(longitude, latitude);
            let level_1_ids = self.level_1_ids_for_coordinate(longitude, latitude);

            for agency_id in agency_ids {
                let metadata = metadata_by_agency.entry(agency_id).or_default();
                metadata.level_0s.extend(country_ids.iter().cloned());
                metadata.level_1s.extend(level_1_ids.iter().cloned());
                CoordinateBounds::add_coordinate(&mut metadata.bbox, longitude, latitude);
            }
        }

        metadata_by_agency
            .into_iter()
            .map(|(agency_id, metadata)| (agency_id, metadata.into_metadata()))
            .collect()
    }
}

const STOP_CHUNK_SIZE: i64 = 5_000;
const AGENCY_ROW_CHUNK_SIZE: i64 = 5_000;
const MAX_CONCURRENT_FEEDS: usize = 8;

struct FeedChunkAccumulator {
    spatial_by_agency: Vec<SpatialAccumulator>,
    stop_counts_by_agency: Vec<usize>,
}

impl FeedChunkAccumulator {
    fn new(agency_count: usize) -> Self {
        Self {
            spatial_by_agency: vec![SpatialAccumulator::default(); agency_count],
            stop_counts_by_agency: vec![0; agency_count],
        }
    }

    fn merge(mut self, other: Self) -> Self {
        let Self {
            spatial_by_agency,
            stop_counts_by_agency,
        } = other;

        for (current, additional) in self
            .spatial_by_agency
            .iter_mut()
            .zip(spatial_by_agency)
        {
            current.merge(additional);
        }

        for (current, additional) in self
            .stop_counts_by_agency
            .iter_mut()
            .zip(stop_counts_by_agency)
        {
            *current += additional;
        }

        self
    }
}

const UNIFIED_AGENCY_UPSERT_CHUNK_SIZE: usize = 1_000;

pub(crate) fn unified_agency_row(
    id: String,
    name: String,
    chateaux: Vec<String>,
) -> catenary::models::UnifiedAgency {
    let chateaux = chateaux
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(Some)
        .collect();

    catenary::models::UnifiedAgency {
        id,
        name,
        name_translations: None,
        primary_level_0: None,
        primary_level_1: None,
        has_rail: false,
        has_tram: false,
        has_metro: false,
        has_ferry: false,
        has_bus: false,
        is_national_railway_operator: false,
        no_home_country_europe: false,
        chateaux,
        level_0s: None,
        bbox: None,
        level_1s: None,
    }
}

pub(crate) async fn upsert_unified_agencies(
    conn: &mut AsyncPgConnection,
    rows: &[catenary::models::UnifiedAgency],
) -> diesel::QueryResult<usize> {
    use catenary::schema::gtfs::unified_agency::dsl as unified_agencies;

    let mut affected_rows = 0usize;

    for chunk in rows.chunks(UNIFIED_AGENCY_UPSERT_CHUNK_SIZE) {
        affected_rows += diesel::insert_into(unified_agencies::unified_agency)
            .values(chunk)
            .on_conflict(unified_agencies::id)
            .do_update()
            .set((
                unified_agencies::name.eq(excluded(unified_agencies::name)),
                unified_agencies::chateaux.eq(sql::<Array<Nullable<Text>>>(
                    "ARRAY(SELECT DISTINCT chateau FROM unnest(unified_agency.chateaux || excluded.chateaux) AS merged(chateau) WHERE chateau IS NOT NULL ORDER BY chateau)",
                )),
            ))
            .execute(conn)
            .await?;
    }

    Ok(affected_rows)
}

pub async fn refresh_unified_agency_ids(
    pool: &CatenaryPostgresPool,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::agencies::dsl;

    let mut conn = pool.get().await?;
    let mut updated = 0usize;
    let mut offset = 0i64;

    loop {
        let agency_rows = dsl::agencies
            .order((
                dsl::static_onestop_id.asc(),
                dsl::attempt_id.asc(),
                dsl::agency_id.asc(),
            ))
            .limit(AGENCY_ROW_CHUNK_SIZE)
            .offset(offset)
            .select((
                dsl::static_onestop_id,
                dsl::attempt_id,
                dsl::agency_id,
                dsl::agency_name,
                dsl::agency_url,
                dsl::chateau,
                dsl::unified_agency_id,
            ))
            .load::<(String, String, String, String, String, String, Option<String>)>(&mut conn)
            .await?;

        if agency_rows.is_empty() {
            break;
        }

        let row_count = agency_rows.len();
        let assignments = agency_rows
            .into_par_iter()
            .map(
                |(
                    static_onestop_id,
                    attempt_id,
                    agency_id,
                    agency_name,
                    agency_url,
                    chateau,
                    current_unified_agency_id,
                )| {
                    let generated_unified_agency_id =
                        unified_agency_id_for(&agency_name, &agency_url);

                    (
                        static_onestop_id,
                        attempt_id,
                        agency_id,
                        agency_name,
                        chateau,
                        current_unified_agency_id,
                        generated_unified_agency_id,
                    )
                },
            )
            .collect::<Vec<_>>();

        let mut unified_agencies_by_id =
            HashMap::<String, (String, BTreeSet<String>)>::new();

        for (_, _, _, agency_name, chateau, _, generated_unified_agency_id) in &assignments {
            // Keep the first deterministic name from the ordered agency query, but merge every
            // chateau represented by this unified agency in the current chunk.
            unified_agencies_by_id
                .entry(generated_unified_agency_id.clone())
                .or_insert_with(|| (agency_name.clone(), BTreeSet::new()))
                .1
                .insert(chateau.clone());
        }

        let mut unified_agency_rows = unified_agencies_by_id
            .into_iter()
            .map(|(id, (name, chateaux))| {
                unified_agency_row(id, name, chateaux.into_iter().collect())
            })
            .collect::<Vec<_>>();
        unified_agency_rows.sort_by(|left, right| left.id.cmp(&right.id));

        let pending_updates = assignments
            .into_iter()
            .filter_map(
                |(
                    static_onestop_id,
                    attempt_id,
                    agency_id,
                    _,
                    _,
                    current_unified_agency_id,
                    generated_unified_agency_id,
                )| {
                    (current_unified_agency_id.as_deref()
                        != Some(generated_unified_agency_id.as_str()))
                    .then_some((
                        static_onestop_id,
                        attempt_id,
                        agency_id,
                        generated_unified_agency_id,
                    ))
                },
            )
            .collect::<Vec<_>>();

        updated += conn
            .build_transaction()
            .run::<usize, diesel::result::Error, _>(|conn| {
                Box::pin(async move {
                    upsert_unified_agencies(conn, &unified_agency_rows).await?;

                    let mut updated_in_chunk = 0usize;
                    for (static_onestop_id, attempt_id, agency_id, unified_agency_id) in
                        pending_updates
                    {
                        updated_in_chunk += diesel::update(
                            dsl::agencies
                                .filter(dsl::static_onestop_id.eq(static_onestop_id))
                                .filter(dsl::attempt_id.eq(attempt_id))
                                .filter(dsl::agency_id.eq(agency_id)),
                        )
                        .set(dsl::unified_agency_id.eq(Some(unified_agency_id)))
                        .execute(conn)
                        .await?;
                    }

                    Ok(updated_in_chunk)
                })
            })
            .await?;

        if row_count < AGENCY_ROW_CHUNK_SIZE as usize {
            break;
        }
        offset += row_count as i64;
    }

    Ok(updated)
}

async fn process_feed_agency_spatial_metadata(
    pool: &CatenaryPostgresPool,
    country_index: &CountryIndex,
    static_onestop_id: String,
    attempt_id: String,
    agency_ids: Vec<String>,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::agencies::dsl as agencies;
    use catenary::schema::gtfs::routes::dsl as routes;
    use catenary::schema::gtfs::stops::dsl as stops;

    let started = Instant::now();
    for agency_id in &agency_ids {
        println!(
            "[agency-spatial-metadata] processing agency static_onestop_id={} attempt_id={} agency_id={}",
            static_onestop_id, attempt_id, agency_id
        );
    }

    let mut conn = pool.get().await?;
    let route_rows = routes::routes
        .filter(routes::onestop_feed_id.eq(&static_onestop_id))
        .filter(routes::attempt_id.eq(&attempt_id))
        .select((routes::route_id, routes::agency_id))
        .load::<(String, Option<String>)>(&mut conn)
        .await?;

    let agency_index_by_id = agency_ids
        .iter()
        .enumerate()
        .map(|(index, agency_id)| (agency_id.clone(), index))
        .collect::<HashMap<_, _>>();
    let sole_agency_index = (agency_ids.len() == 1).then_some(0);
    let mut route_counts_by_agency = vec![0usize; agency_ids.len()];
    let mut route_to_agency_index = HashMap::with_capacity(route_rows.len());

    for (route_id, route_agency_id) in route_rows {
        let agency_index = match route_agency_id.as_deref() {
            Some(agency_id) => agency_index_by_id.get(agency_id).copied(),
            None => sole_agency_index,
        };

        if let Some(agency_index) = agency_index {
            route_counts_by_agency[agency_index] += 1;
            route_to_agency_index.insert(route_id, agency_index);
        }
    }

    let agency_count = agency_ids.len();
    let mut aggregate = FeedChunkAccumulator::new(agency_count);
    let mut last_stop_id: Option<String> = None;

    if !route_to_agency_index.is_empty() {
        loop {
            let mut query = stops::stops
                .filter(stops::onestop_feed_id.eq(&static_onestop_id))
                .filter(stops::attempt_id.eq(&attempt_id))
                .filter(stops::point.is_not_null())
                .order(stops::gtfs_id.asc())
                .select((stops::gtfs_id, stops::point, stops::routes))
                .into_boxed();

            if let Some(last_stop_id) = last_stop_id.as_deref() {
                query = query.filter(stops::gtfs_id.gt(last_stop_id));
            }

            let stop_chunk = query
                .limit(STOP_CHUNK_SIZE)
                .load::<(
                    String,
                    Option<postgis_diesel::types::Point>,
                    Vec<Option<String>>,
                )>(&mut conn)
                .await?;

            if stop_chunk.is_empty() {
                break;
            }

            let is_final_chunk = stop_chunk.len() < STOP_CHUNK_SIZE as usize;
            last_stop_id = stop_chunk.last().map(|row| row.0.clone());

            let chunk_accumulator = stop_chunk
                .par_iter()
                .fold(
                    || FeedChunkAccumulator::new(agency_count),
                    |mut accumulator, (_, point, route_ids)| {
                        let mut stop_agency_indices = route_ids
                            .iter()
                            .filter_map(|route_id| route_id.as_deref())
                            .filter_map(|route_id| route_to_agency_index.get(route_id).copied())
                            .collect::<Vec<_>>();
                        stop_agency_indices.sort_unstable();
                        stop_agency_indices.dedup();

                        if stop_agency_indices.is_empty() {
                            return accumulator;
                        }

                        for &agency_index in &stop_agency_indices {
                            accumulator.stop_counts_by_agency[agency_index] += 1;
                        }

                        let Some(point) = point.as_ref() else {
                            return accumulator;
                        };
                        if !point.x.is_finite() || !point.y.is_finite() {
                            return accumulator;
                        }

                        let country_ids =
                            country_index.country_ids_for_coordinate(point.x, point.y);
                        let level_1_ids =
                            country_index.level_1_ids_for_coordinate(point.x, point.y);

                        for agency_index in stop_agency_indices {
                            let metadata =
                                &mut accumulator.spatial_by_agency[agency_index];
                            metadata.level_0s.extend(country_ids.iter().cloned());
                            metadata.level_1s.extend(level_1_ids.iter().cloned());
                            CoordinateBounds::add_coordinate(
                                &mut metadata.bbox,
                                point.x,
                                point.y,
                            );
                        }

                        accumulator
                    },
                )
                .reduce(
                    || FeedChunkAccumulator::new(agency_count),
                    FeedChunkAccumulator::merge,
                );

            aggregate = aggregate.merge(chunk_accumulator);

            if is_final_chunk {
                break;
            }
        }
    }

    let mut updated = 0usize;
    for (agency_index, agency_id) in agency_ids.into_iter().enumerate() {
        let metadata = &aggregate.spatial_by_agency[agency_index];
        let country_ids = metadata.level_0s.iter().cloned().collect::<Vec<_>>();
        let level_1_ids = metadata.level_1s.iter().cloned().collect::<Vec<_>>();
        let level_0s = country_ids.iter().cloned().map(Some).collect::<Vec<_>>();
        let level_1s = level_1_ids.iter().cloned().map(Some).collect::<Vec<_>>();
        let bbox = metadata.bbox.map(CoordinateBounds::to_polygon);
        let bbox_summary = metadata.bbox.map(CoordinateBounds::as_array);

        updated += diesel::update(
            agencies::agencies
                .filter(agencies::static_onestop_id.eq(&static_onestop_id))
                .filter(agencies::attempt_id.eq(&attempt_id))
                .filter(agencies::agency_id.eq(&agency_id)),
        )
        .set((
            agencies::level_0s.eq(Some(level_0s)),
            agencies::level_1s.eq(Some(level_1s)),
            agencies::bbox.eq(bbox),
        ))
        .execute(&mut conn)
        .await?;

        println!(
            "[agency-spatial-metadata] finished agency static_onestop_id={} attempt_id={} agency_id={} routes={} stops={} countries={:?} level_1s={:?} bbox={:?} elapsed_ms={}",
            static_onestop_id,
            attempt_id,
            agency_id,
            route_counts_by_agency[agency_index],
            aggregate.stop_counts_by_agency[agency_index],
            country_ids,
            level_1_ids,
            bbox_summary,
            started.elapsed().as_millis()
        );
    }

    Ok(updated)
}

pub async fn backfill_all_agency_spatial_metadata(
    pool: &CatenaryPostgresPool,
    country_index: &CountryIndex,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::agencies::dsl as agencies;

    let mut conn = pool.get().await?;
    let mut agencies_by_feed = HashMap::<(String, String), Vec<String>>::new();
    let mut offset = 0i64;

    loop {
        let agency_rows = agencies::agencies
            .order((
                agencies::static_onestop_id.asc(),
                agencies::attempt_id.asc(),
                agencies::agency_id.asc(),
            ))
            .limit(AGENCY_ROW_CHUNK_SIZE)
            .offset(offset)
            .select((
                agencies::static_onestop_id,
                agencies::attempt_id,
                agencies::agency_id,
            ))
            .load::<(String, String, String)>(&mut conn)
            .await?;

        if agency_rows.is_empty() {
            break;
        }

        let row_count = agency_rows.len();
        for (static_onestop_id, attempt_id, agency_id) in agency_rows {
            agencies_by_feed
                .entry((static_onestop_id, attempt_id))
                .or_default()
                .push(agency_id);
        }

        if row_count < AGENCY_ROW_CHUNK_SIZE as usize {
            break;
        }
        offset += row_count as i64;
    }
    drop(conn);

    for agency_ids in agencies_by_feed.values_mut() {
        agency_ids.sort();
    }

    let mut feed_work = agencies_by_feed.into_iter().collect::<Vec<_>>();
    feed_work.sort_by(|left, right| left.0.cmp(&right.0));

    let updated_by_feed = stream::iter(feed_work.into_iter().map(
        |((static_onestop_id, attempt_id), agency_ids)| async move {
            process_feed_agency_spatial_metadata(
                pool,
                country_index,
                static_onestop_id,
                attempt_id,
                agency_ids,
            )
            .await
        },
    ))
    .buffer_unordered(MAX_CONCURRENT_FEEDS)
    .try_collect::<Vec<_>>()
    .await?;

    Ok(updated_by_feed.into_iter().sum())
}

pub async fn refresh_unified_agency_spatial_metadata(
    pool: &CatenaryPostgresPool,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::agencies::dsl as agencies;
    use catenary::schema::gtfs::unified_agency::dsl as unified_agencies;

    let mut conn = pool.get().await?;
    let mut metadata_by_unified_id = HashMap::<String, SpatialAccumulator>::new();
    let mut offset = 0i64;

    loop {
        let agency_rows = agencies::agencies
            .filter(agencies::unified_agency_id.is_not_null())
            .order((
                agencies::static_onestop_id.asc(),
                agencies::attempt_id.asc(),
                agencies::agency_id.asc(),
            ))
            .limit(AGENCY_ROW_CHUNK_SIZE)
            .offset(offset)
            .select((
                agencies::unified_agency_id,
                agencies::level_0s,
                agencies::level_1s,
                agencies::bbox,
            ))
            .load::<(
                Option<String>,
                Option<Vec<Option<String>>>,
                Option<Vec<Option<String>>>,
                Option<PgPolygon>,
            )>(&mut conn)
            .await?;

        if agency_rows.is_empty() {
            break;
        }

        let row_count = agency_rows.len();
        for (unified_agency_id, level_0s, level_1s, bbox) in agency_rows {
            let Some(unified_agency_id) = unified_agency_id else {
                continue;
            };

            let metadata = metadata_by_unified_id.entry(unified_agency_id).or_default();
            if let Some(level_0s) = level_0s {
                metadata.level_0s.extend(level_0s.into_iter().flatten());
            }
            if let Some(level_1s) = level_1s {
                metadata.level_1s.extend(level_1s.into_iter().flatten());
            }
            CoordinateBounds::add_bounds(
                &mut metadata.bbox,
                bbox.as_ref().and_then(CoordinateBounds::from_polygon),
            );
        }

        if row_count < AGENCY_ROW_CHUNK_SIZE as usize {
            break;
        }
        offset += row_count as i64;
    }

    diesel::update(unified_agencies::unified_agency)
        .set((
            unified_agencies::level_0s.eq(None::<Vec<Option<String>>>),
            unified_agencies::level_1s.eq(None::<Vec<Option<String>>>),
            unified_agencies::bbox.eq(None::<PgPolygon>),
        ))
        .execute(&mut conn)
        .await?;

    let mut metadata_by_unified_id = metadata_by_unified_id.into_iter().collect::<Vec<_>>();
    metadata_by_unified_id.sort_by(|left, right| left.0.cmp(&right.0));

    let mut updated = 0usize;
    for (unified_agency_id, metadata) in metadata_by_unified_id {
        let level_0s = metadata.level_0s.into_iter().map(Some).collect::<Vec<_>>();
        let level_1s = metadata.level_1s.into_iter().map(Some).collect::<Vec<_>>();
        let bbox = metadata.bbox.map(CoordinateBounds::to_polygon);

        updated += diesel::update(
            unified_agencies::unified_agency
                .filter(unified_agencies::id.eq(unified_agency_id)),
        )
        .set((
            unified_agencies::level_0s.eq(Some(level_0s)),
            unified_agencies::level_1s.eq(Some(level_1s)),
            unified_agencies::bbox.eq(bbox),
        ))
        .execute(&mut conn)
        .await?;
    }

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::{effective_route_agency_id, unified_agency_id_for};

    #[test]
    fn routes_without_agency_id_use_the_only_feed_agency() {
        assert_eq!(effective_route_agency_id(None, Some("")), Some(""));
        assert_eq!(
            effective_route_agency_id(None, Some("sole-agency")),
            Some("sole-agency")
        );
    }

    #[test]
    fn route_agency_fallback_is_not_used_for_multi_agency_feeds() {
        assert_eq!(effective_route_agency_id(None, None), None);
        assert_eq!(
            effective_route_agency_id(Some("explicit-agency"), Some("sole-agency")),
            Some("explicit-agency")
        );
    }

    #[test]
    fn separates_translink_agencies_by_url() {
        assert_eq!(
            unified_agency_id_for("TransLink", "https://translink.com.au/"),
            "translink-au"
        );
        assert_eq!(
            unified_agency_id_for("Translink", "https://www.translink.ca/"),
            "translink-ca"
        );
    }

    #[test]
    fn collapses_sbb_names() {
        assert_eq!(
            unified_agency_id_for("SBB CFF FFS", "https://www.sbb.ch/"),
            "sbb"
        );
    }

    #[test]
    fn generic_names_use_the_url_domain() {
        assert_eq!(
            unified_agency_id_for("Metro", "https://www.metro.net/"),
            "metro"
        );
        assert_eq!(
            unified_agency_id_for("Metrolink", "https://metrolinktrains.com/"),
            "metrolinktrains"
        );
        assert_eq!(
            unified_agency_id_for("Metro", "https://www.example.co.uk/metro"),
            "example"
        );
    }

    #[test]
    fn preserves_the_existing_default_format() {
        assert_eq!(
            unified_agency_id_for("Los Angeles Metro", "https://www.metro.net/"),
            "Los_Angeles_Metro"
        );
    }
}
