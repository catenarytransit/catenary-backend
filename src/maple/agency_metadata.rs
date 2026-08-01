use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel::sql_types::{Double, Text};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;
use geo::{BoundingRect, Intersects};
use geo_types::{Geometry, Point};
use geojson::GeoJson;
use rstar::{AABB, RTree, RTreeObject};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::LazyLock;
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
        1 => labels.first().copied(),
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
                    route
                        .agency_id
                        .as_deref()
                        .map(str::to_owned)
                        .or_else(|| sole_agency_id.clone())
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

#[derive(Debug, Hash, PartialEq, Eq)]
struct AgencyKey {
    static_onestop_id: String,
    attempt_id: String,
    agency_id: String,
}

#[derive(QueryableByName)]
struct AgencyStopPoint {
    #[diesel(sql_type = Text)]
    static_onestop_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    agency_id: String,
    #[diesel(sql_type = Double)]
    longitude: f64,
    #[diesel(sql_type = Double)]
    latitude: f64,
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

pub async fn backfill_all_agency_level_0s(
    pool: &CatenaryPostgresPool,
    country_index: &CountryIndex,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    let mut conn = pool.get().await?;

    let refreshed =
        diesel::sql_query("UPDATE gtfs.agencies SET level_0s = ARRAY[]::TEXT[]")
            .execute(&mut conn)
            .await?;

    let query = diesel::sql_query(
        r#"
        WITH agency_counts AS (
            SELECT static_onestop_id, attempt_id, COUNT(*) AS agency_count
            FROM gtfs.agencies
            GROUP BY static_onestop_id, attempt_id
        )
        SELECT DISTINCT
            a.static_onestop_id,
            a.attempt_id,
            a.agency_id,
            ST_X(s.point) AS longitude,
            ST_Y(s.point) AS latitude
        FROM gtfs.agencies AS a
        JOIN agency_counts AS counts
          ON counts.static_onestop_id = a.static_onestop_id
         AND counts.attempt_id = a.attempt_id
        JOIN gtfs.routes AS r
          ON r.onestop_feed_id = a.static_onestop_id
         AND r.attempt_id = a.attempt_id
         AND (
             r.agency_id = a.agency_id
             OR (r.agency_id IS NULL AND counts.agency_count = 1)
         )
        JOIN gtfs.stops AS s
          ON s.onestop_feed_id = r.onestop_feed_id
         AND s.attempt_id = r.attempt_id
         AND r.route_id = ANY(s.routes)
        WHERE s.point IS NOT NULL
        "#,
    );

    let mut rows = query.load_stream::<AgencyStopPoint>(&mut conn).await?;
    let mut countries_by_agency: HashMap<AgencyKey, BTreeSet<String>> = HashMap::new();

    while let Some(row) = rows.try_next().await? {
        let country_ids =
            country_index.country_ids_for_coordinate(row.longitude, row.latitude);
        if country_ids.is_empty() {
            continue;
        }

        countries_by_agency
            .entry(AgencyKey {
                static_onestop_id: row.static_onestop_id,
                attempt_id: row.attempt_id,
                agency_id: row.agency_id,
            })
            .or_default()
            .extend(country_ids);
    }

    drop(rows);

    for (key, country_ids) in countries_by_agency {
        use catenary::schema::gtfs::agencies::dsl;

        diesel::update(
            dsl::agencies
                .filter(dsl::static_onestop_id.eq(key.static_onestop_id))
                .filter(dsl::attempt_id.eq(key.attempt_id))
                .filter(dsl::agency_id.eq(key.agency_id)),
        )
        .set(
            dsl::level_0s.eq(Some(
                country_ids.into_iter().map(Some).collect::<Vec<_>>(),
            )),
        )
        .execute(&mut conn)
        .await?;
    }

    Ok(refreshed)
}

#[cfg(test)]
mod tests {
    use super::unified_agency_id_for;

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
