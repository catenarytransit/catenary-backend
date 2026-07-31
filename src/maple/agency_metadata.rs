use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel::sql_types::{Double, Text};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;
use geo::{BoundingRect, Intersects};
use geo_types::{Geometry, Point};
use geojson::GeoJson;
use rstar::{AABB, RTree, RTreeObject};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

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
    let mut conn = pool.get().await?;

    let updated = diesel::sql_query(
        r#"
        UPDATE gtfs.agencies
        SET unified_agency_id = replace(agency_name, ' ', '_')
        WHERE unified_agency_id IS DISTINCT FROM replace(agency_name, ' ', '_')
        "#,
    )
    .execute(&mut conn)
    .await?;

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
