use ahash::{AHashMap, AHashSet};
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::sql_types::{Array, Nullable, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use sha2::{Digest, Sha256};
use std::error::Error;

const UPDATE_CHUNK_SIZE: usize = 2_000;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(QueryableByName)]
struct UnifiedAgencyIdRow {
    #[diesel(sql_type = Text)]
    unified_agency_id: String,
}

#[derive(QueryableByName)]
struct RouteSlugRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    route_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    short_name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    long_name: Option<String>,
    #[diesel(sql_type = Text)]
    unified_agency_id: String,
}

#[derive(Clone, Debug)]
struct PhysicalRouteRow {
    onestop_feed_id: String,
    attempt_id: String,
    route_id: String,
}

#[derive(Clone, Debug)]
struct LogicalRoute {
    route_id: String,
    short_name: Option<String>,
    long_name: Option<String>,
    rows: Vec<PhysicalRouteRow>,
    slug: String,
}

pub async fn unified_agency_ids_for_feed(
    pool: &CatenaryPostgresPool,
    feed_id: &str,
) -> Result<Vec<String>, BoxError> {
    let mut conn = pool.get().await?;
    let rows = sql_query(
        r#"
        SELECT DISTINCT unified_agency_id
        FROM gtfs.agencies
        WHERE static_onestop_id = $1
          AND unified_agency_id IS NOT NULL
        "#,
    )
    .bind::<Text, _>(feed_id)
    .load::<UnifiedAgencyIdRow>(&mut conn)
    .await?;

    Ok(rows.into_iter().map(|row| row.unified_agency_id).collect())
}

pub async fn recompute_all(pool: &CatenaryPostgresPool) -> Result<usize, BoxError> {
    let mut conn = pool.get().await?;
    let rows = sql_query(
        r#"
        SELECT DISTINCT unified_agency_id
        FROM gtfs.agencies
        WHERE unified_agency_id IS NOT NULL
        "#,
    )
    .load::<UnifiedAgencyIdRow>(&mut conn)
    .await?;
    drop(conn);

    let unified_agency_ids = rows
        .into_iter()
        .map(|row| row.unified_agency_id)
        .collect::<Vec<_>>();
    recompute_for_unified_agencies(pool, &unified_agency_ids).await
}

pub async fn recompute_for_unified_agencies(
    pool: &CatenaryPostgresPool,
    unified_agency_ids: &[String],
) -> Result<usize, BoxError> {
    if unified_agency_ids.is_empty() {
        return Ok(0);
    }

    let ids = unified_agency_ids
        .iter()
        .filter(|id| !id.is_empty())
        .cloned()
        .collect::<AHashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if ids.is_empty() {
        return Ok(0);
    }

    let mut conn = pool.get().await?;
    let rows = sql_query(
        r#"
        WITH target_feeds AS (
            SELECT DISTINCT chateau, static_onestop_id, attempt_id
            FROM gtfs.agencies
            WHERE unified_agency_id = ANY($1::text[])
        ),
        feed_agency_counts AS (
            SELECT
                a.chateau,
                a.static_onestop_id,
                a.attempt_id,
                COUNT(DISTINCT a.agency_id) AS agency_count,
                MIN(a.unified_agency_id) FILTER (
                    WHERE a.unified_agency_id IS NOT NULL
                ) AS sole_unified_agency_id
            FROM gtfs.agencies a
            JOIN target_feeds f
              ON f.chateau = a.chateau
             AND f.static_onestop_id = a.static_onestop_id
             AND f.attempt_id = a.attempt_id
            GROUP BY a.chateau, a.static_onestop_id, a.attempt_id
        )
        SELECT DISTINCT
            r.onestop_feed_id,
            r.attempt_id,
            r.route_id,
            r.short_name,
            r.long_name,
            COALESCE(
                specific.unified_agency_id,
                CASE
                    WHEN r.agency_id IS NULL AND counts.agency_count = 1
                    THEN counts.sole_unified_agency_id
                END
            ) AS unified_agency_id
        FROM gtfs.routes r
        JOIN feed_agency_counts counts
          ON counts.chateau = r.chateau
         AND counts.static_onestop_id = r.onestop_feed_id
         AND counts.attempt_id = r.attempt_id
        LEFT JOIN gtfs.agencies specific
          ON specific.chateau = r.chateau
         AND specific.static_onestop_id = r.onestop_feed_id
         AND specific.attempt_id = r.attempt_id
         AND r.agency_id IS NOT NULL
         AND specific.agency_id = r.agency_id
        WHERE (
                r.agency_id IS NOT NULL
            AND specific.unified_agency_id = ANY($1::text[])
        ) OR (
                r.agency_id IS NULL
            AND counts.agency_count = 1
            AND counts.sole_unified_agency_id = ANY($1::text[])
        )
        "#,
    )
    .bind::<Array<Text>, _>(ids)
    .load::<RouteSlugRow>(&mut conn)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    // The outer hash partitions by unified agency. The inner hash collapses
    // multiple physical feed versions of the same route_id into one logical
    // route before collision detection.
    let mut by_agency: AHashMap<String, AHashMap<String, LogicalRoute>> = AHashMap::new();

    for row in rows {
        let agency_routes = by_agency.entry(row.unified_agency_id).or_default();
        let logical_route = agency_routes
            .entry(row.route_id.clone())
            .or_insert_with(|| LogicalRoute {
                route_id: row.route_id.clone(),
                short_name: None,
                long_name: None,
                rows: Vec::new(),
                slug: String::new(),
            });

        prefer_name(&mut logical_route.short_name, row.short_name.as_deref());
        prefer_name(&mut logical_route.long_name, row.long_name.as_deref());
        logical_route.rows.push(PhysicalRouteRow {
            onestop_feed_id: row.onestop_feed_id,
            attempt_id: row.attempt_id,
            route_id: row.route_id,
        });
    }

    let mut assignments = Vec::<(String, String, String, String)>::new();

    for routes_by_id in by_agency.into_values() {
        let mut logical_routes = routes_by_id.into_values().collect::<Vec<_>>();
        assign_slugs(&mut logical_routes);

        for route in logical_routes {
            for row in route.rows {
                assignments.push((
                    row.onestop_feed_id,
                    row.attempt_id,
                    row.route_id,
                    route.slug.clone(),
                ));
            }
        }
    }

    let mut updated = 0usize;
    for chunk in assignments.chunks(UPDATE_CHUNK_SIZE) {
        let feed_ids = chunk.iter().map(|row| row.0.clone()).collect::<Vec<_>>();
        let attempt_ids = chunk.iter().map(|row| row.1.clone()).collect::<Vec<_>>();
        let route_ids = chunk.iter().map(|row| row.2.clone()).collect::<Vec<_>>();
        let slugs = chunk.iter().map(|row| row.3.clone()).collect::<Vec<_>>();

        updated += sql_query(
            r#"
            UPDATE gtfs.routes AS r
            SET url_slug_for_unified_agency = u.slug
            FROM unnest(
                $1::text[],
                $2::text[],
                $3::text[],
                $4::text[]
            ) AS u(onestop_feed_id, attempt_id, route_id, slug)
            WHERE r.onestop_feed_id = u.onestop_feed_id
              AND r.attempt_id = u.attempt_id
              AND r.route_id = u.route_id
            "#,
        )
        .bind::<Array<Text>, _>(feed_ids)
        .bind::<Array<Text>, _>(attempt_ids)
        .bind::<Array<Text>, _>(route_ids)
        .bind::<Array<Text>, _>(slugs)
        .execute(&mut conn)
        .await?;
    }

    Ok(updated)
}

fn prefer_name(slot: &mut Option<String>, candidate: Option<&str>) {
    let Some(candidate) = candidate.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };

    let should_replace = match slot.as_deref() {
        Some(existing) => candidate < existing,
        None => true,
    };
    if should_replace {
        *slot = Some(candidate.to_string());
    }
}

fn assign_slugs(routes: &mut [LogicalRoute]) {
    if routes.is_empty() {
        return;
    }

    let base_candidates = routes.iter().map(base_slug).collect::<Vec<_>>();
    let mut assigned = vec![false; routes.len()];
    let base_counts = candidate_counts(&base_candidates, &assigned);
    let mut used = AHashSet::<String>::with_capacity(routes.len());

    for index in 0..routes.len() {
        let candidate = &base_candidates[index];
        if base_counts.get(candidate).copied() == Some(1) {
            routes[index].slug = candidate.clone();
            assigned[index] = true;
            used.insert(candidate.clone());
        }
    }

    let long_name_candidates = routes
        .iter()
        .enumerate()
        .map(|(index, route)| {
            if assigned[index] {
                String::new()
            } else {
                append_component(&base_candidates[index], route.long_name.as_deref())
            }
        })
        .collect::<Vec<_>>();
    let long_name_counts = candidate_counts(&long_name_candidates, &assigned);

    for index in 0..routes.len() {
        if assigned[index] {
            continue;
        }
        let candidate = &long_name_candidates[index];
        if candidate != &base_candidates[index]
            && long_name_counts.get(candidate).copied() == Some(1)
            && !used.contains(candidate)
        {
            routes[index].slug = candidate.clone();
            assigned[index] = true;
            used.insert(candidate.clone());
        }
    }

    let route_id_candidates = routes
        .iter()
        .enumerate()
        .map(|(index, route)| {
            if assigned[index] {
                String::new()
            } else {
                append_component(&long_name_candidates[index], Some(&route.route_id))
            }
        })
        .collect::<Vec<_>>();
    let route_id_counts = candidate_counts(&route_id_candidates, &assigned);

    for index in 0..routes.len() {
        if assigned[index] {
            continue;
        }
        let candidate = &route_id_candidates[index];
        if candidate != &base_candidates[index]
            && route_id_counts.get(candidate).copied() == Some(1)
            && !used.contains(candidate)
        {
            routes[index].slug = candidate.clone();
            assigned[index] = true;
            used.insert(candidate.clone());
        }
    }

    // This final branch is only for pathological collisions where short name,
    // long name, and the slugified route_id all collide. A SHA-256 suffix keeps
    // the result stable without global counters or ordering dependencies.
    for index in 0..routes.len() {
        if assigned[index] {
            continue;
        }

        let route = &routes[index];
        let prefix = if route_id_candidates[index].is_empty() {
            base_candidates[index].clone()
        } else {
            route_id_candidates[index].clone()
        };
        let digest = hex::encode(Sha256::digest(route.route_id.as_bytes()));
        let mut candidate = format!("{prefix}-{digest}");

        if used.contains(&candidate) {
            let second_digest = hex::encode(Sha256::digest(
                format!("{}\0{}", prefix, route.route_id).as_bytes(),
            ));
            candidate = format!("{prefix}-{digest}-{second_digest}");
        }

        routes[index].slug = candidate.clone();
        used.insert(candidate);
    }
}

fn candidate_counts(candidates: &[String], assigned: &[bool]) -> AHashMap<String, usize> {
    let mut counts = AHashMap::with_capacity(candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        if assigned[index] || candidate.is_empty() {
            continue;
        }
        *counts.entry(candidate.clone()).or_insert(0) += 1;
    }
    counts
}

fn base_slug(route: &LogicalRoute) -> String {
    for value in [
        route.short_name.as_deref(),
        route.long_name.as_deref(),
        Some(route.route_id.as_str()),
    ]
    .into_iter()
    .flatten()
    {
        let candidate = slugify(value);
        if !candidate.is_empty() {
            return candidate;
        }
    }

    format!(
        "route-{}",
        hex::encode(Sha256::digest(route.route_id.as_bytes()))
    )
}

fn append_component(base: &str, value: Option<&str>) -> String {
    let Some(value) = value else {
        return base.to_string();
    };
    let component = slugify(value);
    if component.is_empty() || component == base {
        return base.to_string();
    }

    let base_prefix = format!("{base}-");
    if component.starts_with(&base_prefix) {
        component
    } else {
        format!("{base}-{component}")
    }
}

fn slugify(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    let mut pending_separator = false;

    for character in value.trim().chars() {
        let keep_character =
            character.is_alphanumeric() || (!character.is_ascii() && !character.is_whitespace());

        if keep_character {
            if pending_separator && !slug.is_empty() && !slug.ends_with('-') {
                slug.push('-');
            }
            for lower in character.to_lowercase() {
                slug.push(lower);
            }
            pending_separator = false;
        } else {
            pending_separator = !slug.is_empty();
        }
    }

    while slug.ends_with('-') {
        slug.pop();
    }

    slug
}

#[cfg(test)]
mod tests {
    use super::{LogicalRoute, assign_slugs, slugify};

    fn route(route_id: &str, short_name: Option<&str>, long_name: Option<&str>) -> LogicalRoute {
        LogicalRoute {
            route_id: route_id.to_string(),
            short_name: short_name.map(str::to_string),
            long_name: long_name.map(str::to_string),
            rows: Vec::new(),
            slug: String::new(),
        }
    }

    #[test]
    fn unique_short_name_stays_short() {
        let mut routes = vec![route("1-COM", Some("1"), Some("COMEX Mall"))];
        assign_slugs(&mut routes);
        assert_eq!(routes[0].slug, "1");
    }

    #[test]
    fn duplicate_short_names_use_long_names() {
        let mut routes = vec![
            route("1-COM", Some("1"), Some("COMEX Mall")),
            route("1-UVIC", Some("1"), Some("University")),
        ];
        assign_slugs(&mut routes);

        assert_eq!(routes[0].slug, "1-comex-mall");
        assert_eq!(routes[1].slug, "1-university");
    }

    #[test]
    fn collided_short_name_never_keeps_the_ambiguous_bare_slug() {
        let mut routes = vec![
            route("local", Some("1"), None),
            route("comex", Some("1"), Some("COMEX Mall")),
        ];
        assign_slugs(&mut routes);

        assert_eq!(routes[0].slug, "1-local");
        assert_eq!(routes[1].slug, "1-comex-mall");
    }

    #[test]
    fn route_id_breaks_remaining_name_collisions() {
        let mut routes = vec![
            route("weekday", Some("1"), Some("Main")),
            route("weekend", Some("1"), Some("Main")),
        ];
        assign_slugs(&mut routes);

        assert_eq!(routes[0].slug, "1-main-weekday");
        assert_eq!(routes[1].slug, "1-main-weekend");
    }

    #[test]
    fn slugify_removes_url_significant_ascii_punctuation() {
        assert_eq!(slugify(" 1 / COMEX Mall? "), "1-comex-mall");
        assert_eq!(slugify("Métro 4"), "métro-4");
    }
}
