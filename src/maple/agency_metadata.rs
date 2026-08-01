use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::{stream, StreamExt, TryStreamExt};
use geo::{BoundingRect, Intersects};
use geo_types::{Geometry, Point};
use geojson::GeoJson;
use rayon::prelude::*;
use rstar::{AABB, RTree, RTreeObject};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Instant;
use url::{Host, Url};

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
struct IndexedCountry {
    id: String,
    geometry: Geometry<f64>,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedCountry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

pub struct CountryIndex {
    countries: RTree<IndexedCountry>,
}

impl CountryIndex {
    pub fn from_geojson(
        path: &Path,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let raw = fs::read_to_string(path)?;
        let geojson = raw.parse::<GeoJson>()?;
        let GeoJson::FeatureCollection(collection) = geojson else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "country boundary GeoJSON must be a FeatureCollection",
            )
            .into());
        };

        let mut countries = Vec::with_capacity(collection.features.len());

        for feature in collection.features {
            let Some(country_id) = feature
                .properties
                .as_ref()
                .and_then(|properties| properties.get("CNTR_ID"))
                .and_then(|value| value.as_str())
            else {
                continue;
            };

            let Some(geometry) = feature.geometry else {
                continue;
            };

            let geometry: Geometry<f64> = geometry.try_into()?;
            let Some(bounds) = geometry.bounding_rect() else {
                continue;
            };

            countries.push(IndexedCountry {
                id: country_id.to_string(),
                envelope: AABB::from_corners(
                    [bounds.min().x, bounds.min().y],
                    [bounds.max().x, bounds.max().y],
                ),
                geometry,
            });
        }

        if countries.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "country boundary GeoJSON did not contain any CNTR_ID geometries",
            )
            .into());
        }

        Ok(Self {
            countries: RTree::bulk_load(countries),
        })
    }

    fn country_ids_for_coordinate(&self, longitude: f64, latitude: f64) -> Vec<String> {
        let point = Point::new(longitude, latitude);
        let envelope = AABB::from_point([longitude, latitude]);

        self.countries
            .locate_in_envelope_intersecting(&envelope)
            .filter(|country| country.geometry.intersects(&point))
            .map(|country| country.id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn level_0s_for_gtfs(
        &self,
        gtfs: &gtfs_structures::Gtfs,
        stop_ids_to_route_ids: &HashMap<String, Vec<String>>,
    ) -> HashMap<String, Vec<Option<String>>> {
        let mut countries_by_agency: HashMap<String, BTreeSet<String>> = gtfs
            .agencies
            .iter()
            .map(|agency| {
                (
                    agency.id.clone().unwrap_or_default(),
                    BTreeSet::<String>::new(),
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

            let country_ids = self.country_ids_for_coordinate(longitude, latitude);
            if country_ids.is_empty() {
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

            for agency_id in agency_ids {
                countries_by_agency
                    .entry(agency_id)
                    .or_default()
                    .extend(country_ids.iter().cloned());
            }
        }

        countries_by_agency
            .into_iter()
            .map(|(agency_id, country_ids)| {
                (
                    agency_id,
                    country_ids.into_iter().map(Some).collect::<Vec<_>>(),
                )
            })
            .collect()
    }
}

const STOP_CHUNK_SIZE: i64 = 5_000;
const MAX_CONCURRENT_FEEDS: usize = 8;

struct FeedChunkAccumulator {
    countries_by_agency: Vec<BTreeSet<String>>,
    stop_counts_by_agency: Vec<usize>,
}

impl FeedChunkAccumulator {
    fn new(agency_count: usize) -> Self {
        Self {
            countries_by_agency: vec![BTreeSet::new(); agency_count],
            stop_counts_by_agency: vec![0; agency_count],
        }
    }

    fn merge(mut self, other: Self) -> Self {
        let Self {
            countries_by_agency,
            stop_counts_by_agency,
        } = other;

        for (current, additional) in self
            .countries_by_agency
            .iter_mut()
            .zip(countries_by_agency)
        {
            current.extend(additional);
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

pub async fn refresh_unified_agency_ids(
    pool: &CatenaryPostgresPool,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::agencies::dsl;

    let mut conn = pool.get().await?;
    let agency_rows = dsl::agencies
        .select((
            dsl::static_onestop_id,
            dsl::attempt_id,
            dsl::agency_id,
            dsl::agency_name,
            dsl::agency_url,
            dsl::unified_agency_id,
        ))
        .load::<(String, String, String, String, String, Option<String>)>(&mut conn)
        .await?;

    let mut updated = 0;

    for (
        static_onestop_id,
        attempt_id,
        agency_id,
        agency_name,
        agency_url,
        current_unified_agency_id,
    ) in agency_rows
    {
        let generated_unified_agency_id = unified_agency_id_for(&agency_name, &agency_url);
        if current_unified_agency_id.as_deref() == Some(generated_unified_agency_id.as_str()) {
            continue;
        }

        updated += diesel::update(
            dsl::agencies
                .filter(dsl::static_onestop_id.eq(static_onestop_id))
                .filter(dsl::attempt_id.eq(attempt_id))
                .filter(dsl::agency_id.eq(agency_id)),
        )
        .set(dsl::unified_agency_id.eq(Some(generated_unified_agency_id)))
        .execute(&mut conn)
        .await?;
    }

    Ok(updated)
}

async fn process_feed_agency_level_0s(
    pool: &CatenaryPostgresPool,
    country_index: &CountryIndex,
    static_onestop_id: String,
    attempt_id: String,
    agency_ids: Vec<String>,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::{agencies, routes, stops};

    let started = Instant::now();
    for agency_id in &agency_ids {
        println!(
            "[agency-level-0s] processing agency static_onestop_id={} attempt_id={} agency_id={}",
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
                        let country_ids =
                            country_index.country_ids_for_coordinate(point.x, point.y);

                        for agency_index in stop_agency_indices {
                            accumulator.countries_by_agency[agency_index]
                                .extend(country_ids.iter().cloned());
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
        let country_ids = aggregate.countries_by_agency[agency_index]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let level_0s = country_ids.iter().cloned().map(Some).collect::<Vec<_>>();

        updated += diesel::update(
            agencies::agencies
                .filter(agencies::static_onestop_id.eq(&static_onestop_id))
                .filter(agencies::attempt_id.eq(&attempt_id))
                .filter(agencies::agency_id.eq(&agency_id)),
        )
        .set(agencies::level_0s.eq(Some(level_0s)))
        .execute(&mut conn)
        .await?;

        println!(
            "[agency-level-0s] finished agency static_onestop_id={} attempt_id={} agency_id={} routes={} stops={} countries={:?} elapsed_ms={}",
            static_onestop_id,
            attempt_id,
            agency_id,
            route_counts_by_agency[agency_index],
            aggregate.stop_counts_by_agency[agency_index],
            country_ids,
            started.elapsed().as_millis()
        );
    }

    Ok(updated)
}

pub async fn backfill_all_agency_level_0s(
    pool: &CatenaryPostgresPool,
    country_index: &CountryIndex,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    use catenary::schema::gtfs::agencies;

    let mut conn = pool.get().await?;
    let agency_rows = agencies::agencies
        .select((
            agencies::static_onestop_id,
            agencies::attempt_id,
            agencies::agency_id,
        ))
        .load::<(String, String, String)>(&mut conn)
        .await?;
    drop(conn);

    let mut agencies_by_feed = HashMap::<(String, String), Vec<String>>::new();
    for (static_onestop_id, attempt_id, agency_id) in agency_rows {
        agencies_by_feed
            .entry((static_onestop_id, attempt_id))
            .or_default()
            .push(agency_id);
    }

    for agency_ids in agencies_by_feed.values_mut() {
        agency_ids.sort();
    }

    let mut feed_work = agencies_by_feed.into_iter().collect::<Vec<_>>();
    feed_work.sort_by(|left, right| left.0.cmp(&right.0));

    let updated_by_feed = stream::iter(feed_work.into_iter().map(
        |((static_onestop_id, attempt_id), agency_ids)| async move {
            process_feed_agency_level_0s(
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
