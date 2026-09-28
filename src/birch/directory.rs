use actix_web::{get, web, HttpResponse, Responder};
use catenary::region_names::{
    AgencyRegionOverrideMode, GeoKind, GeographyIndex, RegionNamesStore,
};
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;
use std::sync::Arc;

const REGION_CACHE_CONTROL: &str =
    "public, max-age=300, s-maxage=21600, stale-while-revalidate=86400";
const AGENCY_CACHE_CONTROL: &str =
    "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";
const ROUTE_CACHE_CONTROL: &str =
    "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";

#[derive(Debug, Serialize)]
struct DirectoryErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
pub struct LocalizedName {
    pub locale: String,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct RegionLink {
    pub id: String,
    pub name: String,
    pub kind: GeoKind,
    pub path: String,
    pub official_languages: Vec<String>,
    pub official_names: Vec<LocalizedName>,
}

#[derive(Debug, Serialize)]
pub struct RegionLocaleLink {
    pub locale: String,
    pub path: String,
    pub name: String,
    pub country_official: bool,
    pub region_official: bool,
}

#[derive(Debug, Serialize)]
pub struct StableLocaleLink {
    pub locale: String,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct AgencyCard {
    pub id: String,
    pub name: String,
    pub path: String,
    pub has_rail: bool,
    pub has_tram: bool,
    pub has_metro: bool,
    pub has_ferry: bool,
    pub has_bus: bool,
    pub is_national_railway_operator: bool,
}

#[derive(Debug, Serialize)]
pub struct DirectoryLocalesResponse {
    pub schema_version: u32,
    pub fallback_locale: String,
    pub public_locales: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DirectoryRootResponse {
    pub schema_version: u32,
    pub locale: String,
    pub countries: Vec<RegionLink>,
}

#[derive(Debug, Serialize)]
pub struct RegionPageRegion {
    pub id: String,
    pub name: String,
    pub kind: GeoKind,
    pub depth: usize,
    pub official_languages: Vec<String>,
    pub official_names: Vec<LocalizedName>,
    pub canonical_path: String,
}

#[derive(Debug, Serialize)]
pub struct CountryContext {
    pub id: String,
    pub name: String,
    pub official_languages: Vec<String>,
    pub official_names: Vec<LocalizedName>,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct RegionPageResponse {
    pub schema_version: u32,
    pub canonical: bool,
    pub locale: String,
    pub region: RegionPageRegion,
    pub country: CountryContext,
    pub breadcrumbs: Vec<RegionLink>,
    pub alternate_locales: Vec<RegionLocaleLink>,
    pub children: Vec<RegionLink>,
    pub railways: Vec<AgencyCard>,
    pub national_operators: Vec<AgencyCard>,
    pub agencies: Vec<AgencyCard>,
}

#[derive(Debug, Serialize)]
pub struct RouteCard {
    pub route_key: String,
    pub chateau: String,
    pub route_id: String,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub route_type: i16,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub gtfs_order: Option<i64>,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct AgencyPageAgency {
    pub id: String,
    pub name: String,
    pub has_rail: bool,
    pub has_tram: bool,
    pub has_metro: bool,
    pub has_ferry: bool,
    pub has_bus: bool,
    pub is_national_railway_operator: bool,
}

#[derive(Debug, Serialize)]
pub struct AgencyPageResponse {
    pub schema_version: u32,
    pub locale: String,
    pub canonical_path: String,
    pub agency: AgencyPageAgency,
    pub primary_region: Option<RegionLink>,
    pub regions: Vec<RegionLink>,
    pub alternate_locales: Vec<StableLocaleLink>,
    pub routes: Vec<RouteCard>,
}

#[derive(Debug, Serialize)]
pub struct RoutePageRoute {
    pub route_key: String,
    pub chateau: String,
    pub route_id: String,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub description: Option<String>,
    pub route_type: i16,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RoutePageAgency {
    pub id: String,
    pub name: String,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct RouteStop {
    pub stop_id: String,
    pub name: String,
    pub code: Option<String>,
    pub sequence: i64,
}

#[derive(Debug, Serialize)]
pub struct RouteDirectionPattern {
    pub id: String,
    pub headsign: String,
    pub direction_id: Option<bool>,
    pub stops: Vec<RouteStop>,
}

#[derive(Debug, Serialize)]
pub struct RoutePageResponse {
    pub schema_version: u32,
    pub locale: String,
    pub canonical_path: String,
    pub route: RoutePageRoute,
    pub agency: RoutePageAgency,
    pub direction_patterns: Vec<RouteDirectionPattern>,
    pub alternate_locales: Vec<StableLocaleLink>,
    pub map_deeplink: String,
}

#[derive(Debug)]
struct UnifiedAgencyDb {
    id: String,
    name: String,
    primary_level_0: Option<String>,
    primary_level_1: Option<String>,
    level_0s: Vec<String>,
    level_1s: Vec<String>,
    has_rail: bool,
    has_tram: bool,
    has_metro: bool,
    has_ferry: bool,
    has_bus: bool,
    is_national_railway_operator: bool,
}

#[derive(Debug)]
struct RouteDb {
    onestop_feed_id: String,
    chateau: String,
    route_id: String,
    short_name: Option<String>,
    long_name: Option<String>,
    description: Option<String>,
    route_type: i16,
    url: Option<String>,
    agency_id: Option<String>,
    color: Option<String>,
    text_color: Option<String>,
    gtfs_order: Option<i64>,
}

fn json_error(status: actix_web::http::StatusCode, message: impl Into<String>) -> HttpResponse {
    HttpResponse::build(status).json(DirectoryErrorResponse {
        error: message.into(),
    })
}

fn json_cached<T: Serialize>(body: T, cache_control: &'static str) -> HttpResponse {
    HttpResponse::Ok()
        .insert_header(("Cache-Control", cache_control))
        .json(body)
}

fn locale_supported(index: &GeographyIndex, locale: &str) -> bool {
    locale == index.config().fallback_locale.as_str()
        || index
            .config()
            .public_locales
            .iter()
            .any(|candidate| candidate == locale)
}

fn official_names(index: &GeographyIndex, id: &str) -> Vec<LocalizedName> {
    let Some(node) = index.node(id) else {
        return Vec::new();
    };

    node.official_languages
        .iter()
        .filter_map(|locale| {
            node.names.get(locale).map(|name| LocalizedName {
                locale: locale.clone(),
                name: name.clone(),
            })
        })
        .collect()
}

fn localized_region_path(index: &GeographyIndex, id: &str, locale: &str) -> Option<String> {
    let parts = index.localized_path(id, locale).ok()?;
    Some(format!("/{locale}/region/{}", parts.join("/")))
}

fn build_region_link(index: &GeographyIndex, id: &str, locale: &str) -> Option<RegionLink> {
    let node = index.node(id)?;
    Some(RegionLink {
        id: id.to_string(),
        name: index.name(id, locale)?.to_string(),
        kind: node.kind.clone(),
        path: localized_region_path(index, id, locale)?,
        official_languages: node.official_languages.clone(),
        official_names: official_names(index, id),
    })
}

fn agency_path(locale: &str, unified_agency_id: &str) -> String {
    format!(
        "/{locale}/agency/{}",
        urlencoding::encode(unified_agency_id)
    )
}

fn route_path(locale: &str, unified_agency_id: &str, route_key: &str) -> String {
    format!(
        "{}/route/{route_key}",
        agency_path(locale, unified_agency_id)
    )
}

fn encode_route_key(chateau: &str, route_id: &str) -> String {
    hex::encode(format!("{chateau}\0{route_id}").as_bytes())
}

fn decode_route_key(route_key: &str) -> Option<(String, String)> {
    let bytes = hex::decode(route_key).ok()?;
    let decoded = String::from_utf8(bytes).ok()?;
    let (chateau, route_id) = decoded.split_once('\0')?;

    if chateau.is_empty() || route_id.is_empty() {
        return None;
    }

    Some((chateau.to_string(), route_id.to_string()))
}

fn region_locale_links(
    index: &GeographyIndex,
    region_id: &str,
    country_id: &str,
) -> Vec<RegionLocaleLink> {
    let Some(region) = index.node(region_id) else {
        return Vec::new();
    };
    let Some(country) = index.node(country_id) else {
        return Vec::new();
    };

    index
        .config()
        .public_locales
        .iter()
        .filter_map(|locale| {
            Some(RegionLocaleLink {
                locale: locale.clone(),
                path: localized_region_path(index, region_id, locale)?,
                name: index.name(region_id, locale)?.to_string(),
                country_official: country.official_languages.contains(locale),
                region_official: region.official_languages.contains(locale),
            })
        })
        .collect()
}

fn stable_locale_links(
    index: &GeographyIndex,
    path_for_locale: impl Fn(&str) -> String,
) -> Vec<StableLocaleLink> {
    index
        .config()
        .public_locales
        .iter()
        .map(|locale| StableLocaleLink {
            locale: locale.clone(),
            path: path_for_locale(locale),
        })
        .collect()
}

fn inferred_direct_region_ids(agency: &UnifiedAgencyDb) -> Vec<String> {
    if !agency.level_1s.is_empty() {
        return agency.level_1s.clone();
    }
    if let Some(primary_level_1) = &agency.primary_level_1 {
        return vec![primary_level_1.clone()];
    }
    if !agency.level_0s.is_empty() {
        return agency.level_0s.clone();
    }
    agency.primary_level_0.iter().cloned().collect()
}

fn configured_region_ids(
    store: &RegionNamesStore,
    agency: &UnifiedAgencyDb,
) -> Vec<String> {
    let inferred = inferred_direct_region_ids(agency);
    let Some(configured) = store.agency_overrides.get(&agency.id) else {
        return inferred;
    };

    match configured.mode {
        AgencyRegionOverrideMode::Lock => configured.regions.clone(),
        AgencyRegionOverrideMode::Augment => {
            let mut result = inferred;
            for region in &configured.regions {
                if !result.contains(region) {
                    result.push(region.clone());
                }
            }
            result
        }
    }
}

fn primary_region_id(store: &RegionNamesStore, agency: &UnifiedAgencyDb) -> Option<String> {
    if let Some(configured) = store.agency_overrides.get(&agency.id) {
        if let Some(primary) = &configured.primary_region {
            return Some(primary.clone());
        }
    }

    agency
        .primary_level_1
        .clone()
        .or_else(|| agency.primary_level_0.clone())
        .or_else(|| configured_region_ids(store, agency).into_iter().next())
}

fn region_is_in_country(index: &GeographyIndex, region_id: &str, country_id: &str) -> bool {
    index
        .lineage_ids(region_id)
        .ok()
        .and_then(|lineage| lineage.into_iter().next())
        .is_some_and(|root| root == country_id)
}

fn agency_relevant_to_country(
    store: &RegionNamesStore,
    agency: &UnifiedAgencyDb,
    country_id: &str,
) -> bool {
    configured_region_ids(store, agency)
        .iter()
        .any(|region| region_is_in_country(&store.geography, region, country_id))
        || agency.primary_level_0.as_deref() == Some(country_id)
        || agency.level_0s.iter().any(|region| region == country_id)
}

fn agency_directly_assigned_to_region(
    store: &RegionNamesStore,
    agency: &UnifiedAgencyDb,
    region_id: &str,
) -> bool {
    let Some(node) = store.geography.node(region_id) else {
        return false;
    };

    if let Some(configured) = store.agency_overrides.get(&agency.id) {
        if configured.regions.iter().any(|region| region == region_id) {
            return true;
        }
        if configured.mode == AgencyRegionOverrideMode::Lock {
            return false;
        }
    }

    match node.depth {
        0 => {
            agency.level_1s.is_empty()
                && agency.primary_level_1.is_none()
                && (agency.primary_level_0.as_deref() == Some(region_id)
                    || agency.level_0s.iter().any(|region| region == region_id))
        }
        1 => {
            agency.primary_level_1.as_deref() == Some(region_id)
                || agency.level_1s.iter().any(|region| region == region_id)
        }
        _ => false,
    }
}

fn agency_card(agency: &UnifiedAgencyDb, locale: &str) -> AgencyCard {
    AgencyCard {
        id: agency.id.clone(),
        name: agency.name.clone(),
        path: agency_path(locale, &agency.id),
        has_rail: agency.has_rail,
        has_tram: agency.has_tram,
        has_metro: agency.has_metro,
        has_ferry: agency.has_ferry,
        has_bus: agency.has_bus,
        is_national_railway_operator: agency.is_national_railway_operator,
    }
}

async fn fetch_country_agency_candidates(
    pool: &PgPool,
    country_id: &str,
) -> Result<Vec<UnifiedAgencyDb>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            name,
            primary_level_0,
            primary_level_1,
            COALESCE(array_remove(level_0s, NULL::text), ARRAY[]::text[]) AS level_0s_clean,
            COALESCE(array_remove(level_1s, NULL::text), ARRAY[]::text[]) AS level_1s_clean,
            has_rail,
            has_tram,
            has_metro,
            has_ferry,
            has_bus,
            is_national_railway_operator
        FROM gtfs.unified_agency
        WHERE primary_level_0 = $1
           OR $1 = ANY(COALESCE(array_remove(level_0s, NULL::text), ARRAY[]::text[]))
           OR primary_level_1 LIKE ($1 || '-%')
           OR EXISTS (
                SELECT 1
                FROM unnest(COALESCE(array_remove(level_1s, NULL::text), ARRAY[]::text[]))
                    AS region_ids(region_id)
                WHERE region_id LIKE ($1 || '-%')
           )
           OR is_national_railway_operator = TRUE
        ORDER BY name, id
        "#,
    )
    .bind(country_id)
    .fetch_all(pool)
    .await?;

    rows.iter().map(unified_agency_from_row).collect()
}

async fn fetch_unified_agency(
    pool: &PgPool,
    unified_agency_id: &str,
) -> Result<Option<UnifiedAgencyDb>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            name,
            primary_level_0,
            primary_level_1,
            COALESCE(array_remove(level_0s, NULL::text), ARRAY[]::text[]) AS level_0s_clean,
            COALESCE(array_remove(level_1s, NULL::text), ARRAY[]::text[]) AS level_1s_clean,
            has_rail,
            has_tram,
            has_metro,
            has_ferry,
            has_bus,
            is_national_railway_operator
        FROM gtfs.unified_agency
        WHERE id = $1
        LIMIT 1
        "#,
    )
    .bind(unified_agency_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(unified_agency_from_row).transpose()
}

fn unified_agency_from_row(row: &sqlx::postgres::PgRow) -> Result<UnifiedAgencyDb, sqlx::Error> {
    Ok(UnifiedAgencyDb {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        primary_level_0: row.try_get("primary_level_0")?,
        primary_level_1: row.try_get("primary_level_1")?,
        level_0s: row.try_get("level_0s_clean")?,
        level_1s: row.try_get("level_1s_clean")?,
        has_rail: row.try_get("has_rail")?,
        has_tram: row.try_get("has_tram")?,
        has_metro: row.try_get("has_metro")?,
        has_ferry: row.try_get("has_ferry")?,
        has_bus: row.try_get("has_bus")?,
        is_national_railway_operator: row.try_get("is_national_railway_operator")?,
    })
}

async fn fetch_routes_for_unified_agency(
    pool: &PgPool,
    unified_agency_id: &str,
) -> Result<Vec<RouteDb>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH target_agencies AS (
            SELECT DISTINCT chateau, static_onestop_id, agency_id
            FROM gtfs.agencies
            WHERE unified_agency_id = $1
        ),
        target_feeds AS (
            SELECT DISTINCT chateau, static_onestop_id
            FROM target_agencies
        ),
        feed_agency_counts AS (
            SELECT a.chateau, a.static_onestop_id, COUNT(DISTINCT a.agency_id) AS agency_count
            FROM gtfs.agencies a
            JOIN target_feeds f
              ON f.chateau = a.chateau
             AND f.static_onestop_id = a.static_onestop_id
            GROUP BY a.chateau, a.static_onestop_id
        )
        SELECT DISTINCT ON (r.chateau, r.route_id)
            r.onestop_feed_id,
            r.chateau,
            r.route_id,
            r.short_name,
            r.long_name,
            r.gtfs_desc,
            r.route_type,
            r.url,
            r.agency_id,
            r.color,
            r.text_color,
            r.gtfs_order::bigint AS gtfs_order
        FROM gtfs.routes r
        JOIN target_agencies a
          ON a.chateau = r.chateau
         AND a.static_onestop_id = r.onestop_feed_id
        JOIN feed_agency_counts c
          ON c.chateau = r.chateau
         AND c.static_onestop_id = r.onestop_feed_id
        WHERE r.agency_id = a.agency_id
           OR (r.agency_id IS NULL AND c.agency_count = 1)
        ORDER BY r.chateau, r.route_id, r.gtfs_order NULLS LAST, r.short_name NULLS LAST
        "#,
    )
    .bind(unified_agency_id)
    .fetch_all(pool)
    .await?;

    rows.iter().map(route_from_row).collect()
}

fn route_from_row(row: &sqlx::postgres::PgRow) -> Result<RouteDb, sqlx::Error> {
    Ok(RouteDb {
        onestop_feed_id: row.try_get("onestop_feed_id")?,
        chateau: row.try_get("chateau")?,
        route_id: row.try_get("route_id")?,
        short_name: row.try_get("short_name")?,
        long_name: row.try_get("long_name")?,
        description: row.try_get("gtfs_desc")?,
        route_type: row.try_get("route_type")?,
        url: row.try_get("url")?,
        agency_id: row.try_get("agency_id")?,
        color: row.try_get("color")?,
        text_color: row.try_get("text_color")?,
        gtfs_order: row.try_get("gtfs_order")?,
    })
}

async fn fetch_route(
    pool: &PgPool,
    chateau: &str,
    route_id: &str,
) -> Result<Option<RouteDb>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT
            onestop_feed_id,
            chateau,
            route_id,
            short_name,
            long_name,
            gtfs_desc,
            route_type,
            url,
            agency_id,
            color,
            text_color,
            gtfs_order::bigint AS gtfs_order
        FROM gtfs.routes
        WHERE chateau = $1 AND route_id = $2
        ORDER BY gtfs_order NULLS LAST, onestop_feed_id
        LIMIT 1
        "#,
    )
    .bind(chateau)
    .bind(route_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(route_from_row).transpose()
}

async fn fetch_route_agency(
    pool: &PgPool,
    route: &RouteDb,
) -> Result<Option<(String, String)>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        WITH feed_agencies AS (
            SELECT DISTINCT a.agency_id, a.unified_agency_id
            FROM gtfs.agencies a
            WHERE a.chateau = $1
              AND a.static_onestop_id = $2
        ),
        feed_count AS (
            SELECT COUNT(*) AS agency_count
            FROM feed_agencies
        )
        SELECT f.unified_agency_id, u.name
        FROM feed_agencies f
        CROSS JOIN feed_count c
        JOIN gtfs.unified_agency u ON u.id = f.unified_agency_id
        WHERE ($3::text IS NOT NULL AND f.agency_id = $3)
           OR ($3::text IS NULL AND c.agency_count = 1)
        LIMIT 1
        "#,
    )
    .bind(&route.chateau)
    .bind(&route.onestop_feed_id)
    .bind(route.agency_id.as_deref())
    .fetch_optional(pool)
    .await?;

    row.map(|row| Ok((row.try_get("unified_agency_id")?, row.try_get("name")?)))
        .transpose()
}

async fn fetch_direction_patterns(
    pool: &PgPool,
    chateau: &str,
    route_id: &str,
) -> Result<Vec<RouteDirectionPattern>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH picked_patterns AS (
            SELECT DISTINCT ON (direction_pattern_id)
                direction_pattern_id,
                headsign_or_destination,
                direction_id,
                onestop_feed_id,
                attempt_id
            FROM gtfs.direction_pattern_meta
            WHERE chateau = $1 AND route_id = $2
            ORDER BY direction_pattern_id, onestop_feed_id, attempt_id
        )
        SELECT
            p.direction_pattern_id,
            p.headsign_or_destination,
            p.direction_id,
            d.stop_id,
            d.stop_sequence::bigint AS stop_sequence,
            COALESCE(s.displayname, s.name, d.stop_id) AS stop_name,
            s.code AS stop_code
        FROM picked_patterns p
        JOIN gtfs.direction_pattern d
          ON d.chateau = $1
         AND d.direction_pattern_id = p.direction_pattern_id
         AND d.onestop_feed_id = p.onestop_feed_id
         AND d.attempt_id = p.attempt_id
        LEFT JOIN gtfs.stops s
          ON s.chateau = d.chateau
         AND s.onestop_feed_id = d.onestop_feed_id
         AND s.attempt_id = d.attempt_id
         AND s.gtfs_id = d.stop_id
        ORDER BY p.direction_pattern_id, d.stop_sequence
        "#,
    )
    .bind(chateau)
    .bind(route_id)
    .fetch_all(pool)
    .await?;

    let mut patterns: BTreeMap<String, RouteDirectionPattern> = BTreeMap::new();
    for row in rows {
        let pattern_id: String = row.try_get("direction_pattern_id")?;
        let pattern = patterns
            .entry(pattern_id.clone())
            .or_insert_with(|| RouteDirectionPattern {
                id: pattern_id,
                headsign: row
                    .try_get::<String, _>("headsign_or_destination")
                    .unwrap_or_default(),
                direction_id: row.try_get("direction_id").ok().flatten(),
                stops: Vec::new(),
            });

        pattern.stops.push(RouteStop {
            stop_id: row.try_get("stop_id")?,
            name: row.try_get("stop_name")?,
            code: row.try_get("stop_code")?,
            sequence: row.try_get("stop_sequence")?,
        });
    }

    Ok(patterns.into_values().collect())
}

#[get("/directory/v1/locales")]
pub async fn directory_locales(
    store: web::Data<Arc<RegionNamesStore>>,
) -> impl Responder {
    json_cached(
        DirectoryLocalesResponse {
            schema_version: 1,
            fallback_locale: store.geography.config().fallback_locale.clone(),
            public_locales: store.geography.config().public_locales.clone(),
        },
        REGION_CACHE_CONTROL,
    )
}

#[get("/directory/v1/regions/{locale}")]
pub async fn directory_regions(
    path: web::Path<String>,
    store: web::Data<Arc<RegionNamesStore>>,
) -> impl Responder {
    let locale = path.into_inner();
    if !locale_supported(&store.geography, &locale) {
        return json_error(
            actix_web::http::StatusCode::NOT_FOUND,
            "unsupported directory locale",
        );
    }

    let mut countries = store
        .geography
        .roots()
        .filter_map(|node| build_region_link(&store.geography, &node.id, &locale))
        .collect::<Vec<_>>();
    countries.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));

    json_cached(
        DirectoryRootResponse {
            schema_version: 1,
            locale,
            countries,
        },
        REGION_CACHE_CONTROL,
    )
}

#[get("/directory/v1/region/{locale}/{tail:.*}")]
pub async fn directory_region(
    path: web::Path<(String, String)>,
    store: web::Data<Arc<RegionNamesStore>>,
    pool: web::Data<Arc<PgPool>>,
) -> impl Responder {
    let (locale, tail) = path.into_inner();
    if !locale_supported(&store.geography, &locale) {
        return json_error(
            actix_web::http::StatusCode::NOT_FOUND,
            "unsupported directory locale",
        );
    }

    let slugs = tail
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if slugs.is_empty() {
        return json_error(
            actix_web::http::StatusCode::BAD_REQUEST,
            "region path is empty",
        );
    }

    let Some(resolved) = store.geography.resolve_path(&locale, &slugs) else {
        return json_error(
            actix_web::http::StatusCode::NOT_FOUND,
            "region path was not found",
        );
    };
    let Some(region_node) = store.geography.node(&resolved.region_id) else {
        return json_error(
            actix_web::http::StatusCode::NOT_FOUND,
            "region was not found",
        );
    };

    let lineage = match store.geography.lineage_ids(&region_node.id) {
        Ok(lineage) => lineage,
        Err(error) => {
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
            );
        }
    };
    let Some(country_id) = lineage.first().cloned() else {
        return json_error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "region has no root country",
        );
    };

    let Some(canonical_path) = localized_region_path(&store.geography, &region_node.id, &locale)
    else {
        return json_error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "region has no localized canonical path",
        );
    };

    let Some(country_link) = build_region_link(&store.geography, &country_id, &locale) else {
        return json_error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "country could not be localized",
        );
    };

    let mut children = store
        .geography
        .children_of(&region_node.id)
        .into_iter()
        .filter_map(|child| build_region_link(&store.geography, &child.id, &locale))
        .collect::<Vec<_>>();
    children.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));

    let breadcrumbs = lineage
        .iter()
        .filter_map(|id| build_region_link(&store.geography, id, &locale))
        .collect::<Vec<_>>();

    let is_country_page = region_node.depth == 0;
    let agency_candidates = match fetch_country_agency_candidates(pool.get_ref().as_ref(), &country_id).await {
        Ok(agencies) => agencies,
        Err(error) => {
            eprintln!("directory region agency query failed: {error}");
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "could not query transit agencies",
            );
        }
    };

    let mut railways = Vec::new();
    let mut national_operators = Vec::new();
    let mut agencies = Vec::new();
    for agency in agency_candidates {
        let relevant_to_country =
            agency_relevant_to_country(store.get_ref().as_ref(), &agency, &country_id);

        // A level-0 page should surface every railway that is relevant to the
        // country before the administrative subdivisions. This intentionally
        // includes regional and cross-border rail operators, not only agencies
        // marked as a national railway operator.
        if is_country_page && agency.has_rail && relevant_to_country {
            railways.push(agency_card(&agency, &locale));
        }

        if agency.is_national_railway_operator && relevant_to_country {
            national_operators.push(agency_card(&agency, &locale));
        } else if !agency.is_national_railway_operator
            && agency_directly_assigned_to_region(store.get_ref().as_ref(), &agency, &region_node.id)
            && !(is_country_page && agency.has_rail)
        {
            agencies.push(agency_card(&agency, &locale));
        }
    }
    railways.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
    national_operators.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
    agencies.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));

    let country_node = store.geography.node(&country_id).expect("lineage root exists");
    let region_name = store
        .geography
        .name(&region_node.id, &locale)
        .unwrap_or(&region_node.id)
        .to_string();

    json_cached(
        RegionPageResponse {
            schema_version: 1,
            canonical: resolved.canonical,
            locale: locale.clone(),
            region: RegionPageRegion {
                id: region_node.id.clone(),
                name: region_name,
                kind: region_node.kind.clone(),
                depth: region_node.depth,
                official_languages: region_node.official_languages.clone(),
                official_names: official_names(&store.geography, &region_node.id),
                canonical_path,
            },
            country: CountryContext {
                id: country_id.clone(),
                name: country_link.name,
                official_languages: country_node.official_languages.clone(),
                official_names: official_names(&store.geography, &country_id),
                path: country_link.path,
            },
            breadcrumbs,
            alternate_locales: region_locale_links(
                &store.geography,
                &region_node.id,
                &country_id,
            ),
            children,
            railways,
            national_operators,
            agencies,
        },
        REGION_CACHE_CONTROL,
    )
}

#[get("/directory/v1/agency/{locale}/{unified_agency_id}")]
pub async fn directory_agency(
    path: web::Path<(String, String)>,
    store: web::Data<Arc<RegionNamesStore>>,
    pool: web::Data<Arc<PgPool>>,
) -> impl Responder {
    let (locale, unified_agency_id) = path.into_inner();
    if !locale_supported(&store.geography, &locale) {
        return json_error(
            actix_web::http::StatusCode::NOT_FOUND,
            "unsupported directory locale",
        );
    }

    let agency = match fetch_unified_agency(pool.get_ref().as_ref(), &unified_agency_id).await {
        Ok(Some(agency)) => agency,
        Ok(None) => {
            return json_error(
                actix_web::http::StatusCode::NOT_FOUND,
                "unified agency was not found",
            );
        }
        Err(error) => {
            eprintln!("directory agency query failed: {error}");
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "could not query unified agency",
            );
        }
    };

    let mut region_ids = configured_region_ids(store.get_ref().as_ref(), &agency);
    region_ids.retain(|id| store.geography.contains(id));
    region_ids.sort();
    region_ids.dedup();

    let mut regions = region_ids
        .iter()
        .filter_map(|id| build_region_link(&store.geography, id, &locale))
        .collect::<Vec<_>>();
    regions.sort_by(|a, b| {
        let depth_a = store.geography.node(&a.id).map_or(usize::MAX, |node| node.depth);
        let depth_b = store.geography.node(&b.id).map_or(usize::MAX, |node| node.depth);
        depth_a
            .cmp(&depth_b)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });

    let primary_region = primary_region_id(store.get_ref().as_ref(), &agency)
        .filter(|id| store.geography.contains(id))
        .and_then(|id| build_region_link(&store.geography, &id, &locale));

    let mut routes = match fetch_routes_for_unified_agency(pool.get_ref().as_ref(), &agency.id).await {
        Ok(routes) => routes,
        Err(error) => {
            eprintln!("directory route list query failed: {error}");
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "could not query agency routes",
            );
        }
    };
    routes.sort_by(|a, b| {
        a.route_type
            .cmp(&b.route_type)
            .then_with(|| a.gtfs_order.cmp(&b.gtfs_order))
            .then_with(|| a.short_name.cmp(&b.short_name))
            .then_with(|| a.long_name.cmp(&b.long_name))
            .then_with(|| a.route_id.cmp(&b.route_id))
    });

    let route_cards = routes
        .into_iter()
        .map(|route| {
            let route_key = encode_route_key(&route.chateau, &route.route_id);
            RouteCard {
                route_key: route_key.clone(),
                chateau: route.chateau,
                route_id: route.route_id,
                short_name: route.short_name,
                long_name: route.long_name,
                route_type: route.route_type,
                color: route.color,
                text_color: route.text_color,
                gtfs_order: route.gtfs_order,
                path: route_path(&locale, &agency.id, &route_key),
            }
        })
        .collect();

    json_cached(
        AgencyPageResponse {
            schema_version: 1,
            locale: locale.clone(),
            canonical_path: agency_path(&locale, &agency.id),
            agency: AgencyPageAgency {
                id: agency.id.clone(),
                name: agency.name.clone(),
                has_rail: agency.has_rail,
                has_tram: agency.has_tram,
                has_metro: agency.has_metro,
                has_ferry: agency.has_ferry,
                has_bus: agency.has_bus,
                is_national_railway_operator: agency.is_national_railway_operator,
            },
            primary_region,
            regions,
            alternate_locales: stable_locale_links(&store.geography, |alternate_locale| {
                agency_path(alternate_locale, &agency.id)
            }),
            routes: route_cards,
        },
        AGENCY_CACHE_CONTROL,
    )
}

#[get("/directory/v1/route/{locale}/{route_key}")]
pub async fn directory_route(
    path: web::Path<(String, String)>,
    store: web::Data<Arc<RegionNamesStore>>,
    pool: web::Data<Arc<PgPool>>,
) -> impl Responder {
    let (locale, route_key) = path.into_inner();
    if !locale_supported(&store.geography, &locale) {
        return json_error(
            actix_web::http::StatusCode::NOT_FOUND,
            "unsupported directory locale",
        );
    }

    let Some((chateau, route_id)) = decode_route_key(&route_key) else {
        return json_error(
            actix_web::http::StatusCode::BAD_REQUEST,
            "invalid route key",
        );
    };

    let route = match fetch_route(pool.get_ref().as_ref(), &chateau, &route_id).await {
        Ok(Some(route)) => route,
        Ok(None) => {
            return json_error(
                actix_web::http::StatusCode::NOT_FOUND,
                "route was not found",
            );
        }
        Err(error) => {
            eprintln!("directory route query failed: {error}");
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "could not query route",
            );
        }
    };

    let (unified_agency_id, agency_name) = match fetch_route_agency(pool.get_ref().as_ref(), &route).await {
        Ok(Some(agency)) => agency,
        Ok(None) => {
            return json_error(
                actix_web::http::StatusCode::NOT_FOUND,
                "route does not resolve to a unified agency",
            );
        }
        Err(error) => {
            eprintln!("directory route agency query failed: {error}");
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "could not resolve route agency",
            );
        }
    };

    let direction_patterns = match fetch_direction_patterns(pool.get_ref().as_ref(), &chateau, &route_id).await {
        Ok(patterns) => patterns,
        Err(error) => {
            eprintln!("directory direction pattern query failed: {error}");
            return json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "could not query route direction patterns",
            );
        }
    };

    let map_deeplink = format!(
        "https://maps.catenarymaps.org/?page=route&chateau={}&route={}",
        urlencoding::encode(&chateau),
        urlencoding::encode(&route_id)
    );

    json_cached(
        RoutePageResponse {
            schema_version: 1,
            locale: locale.clone(),
            canonical_path: route_path(&locale, &unified_agency_id, &route_key),
            route: RoutePageRoute {
                route_key: route_key.clone(),
                chateau: route.chateau,
                route_id: route.route_id,
                short_name: route.short_name,
                long_name: route.long_name,
                description: route.description,
                route_type: route.route_type,
                color: route.color,
                text_color: route.text_color,
                url: route.url,
            },
            agency: RoutePageAgency {
                id: unified_agency_id.clone(),
                name: agency_name,
                path: agency_path(&locale, &unified_agency_id),
            },
            direction_patterns,
            alternate_locales: stable_locale_links(&store.geography, |alternate_locale| {
                route_path(alternate_locale, &unified_agency_id, &route_key)
            }),
            map_deeplink,
        },
        ROUTE_CACHE_CONTROL,
    )
}

#[cfg(test)]
mod tests {
    use super::{decode_route_key, encode_route_key};

    #[test]
    fn route_key_round_trip() {
        let encoded = encode_route_key("deutschland", "ICE 1/42");
        assert_eq!(
            decode_route_key(&encoded),
            Some(("deutschland".to_string(), "ICE 1/42".to_string()))
        );
    }

    #[test]
    fn route_key_rejects_invalid_hex() {
        assert_eq!(decode_route_key("not-hex"), None);
    }
}
