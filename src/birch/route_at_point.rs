//! Asynchronous, bounded route-shape hit testing for map taps.
//! Keep selection independent of vector-tile feature decoding on mobile.
use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::f64::consts::PI;
use std::sync::Arc;


#[derive(Debug, Deserialize)]
pub struct RouteHitQuery {
    latitude: f64,
    longitude: f64,
    zoom: f64,
    bus: bool,
    local_rail: bool,
    intercity_rail: bool,
    other: bool,
}

#[derive(Serialize)]
struct RouteHit {
    chateau: String,
    route_id: String,
    colour: String,
    name: String,
    route_type: i32,
}

fn hit_radius_m(latitude: f64, zoom: f64) -> f64 {
    // MapLibre's zoom level is based on 512 CSS pixels per world tile.
    // Match the 5dp tap target plus a small allowance for line width.
    let meters_per_px = (2.0 * PI * 6_378_137.0 * latitude.to_radians().cos().abs())
        / (512.0 * 2.0_f64.powf(zoom));
    (meters_per_px * 10.0).clamp(4.0, 1_200.0)
}

#[actix_web::get("/routes_at_point")]
pub async fn routes_at_point(
    query: web::Query<RouteHitQuery>,
    pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
) -> HttpResponse {
    let q = query.into_inner();
    if !q.latitude.is_finite()
        || !q.longitude.is_finite()
        || !q.zoom.is_finite()
        || q.latitude.abs() > 85.051_129
        || q.longitude.abs() > 180.0
        || !(4.0..=20.0).contains(&q.zoom)
    {
        return HttpResponse::BadRequest().body("Invalid map coordinates or zoom");
    }
    if !(q.bus || q.local_rail || q.intercity_rail || q.other) {
        return HttpResponse::Ok().json(Vec::<RouteHit>::new());
    }

    let radius_m = hit_radius_m(q.latitude, q.zoom);
    let lat_delta = radius_m / 111_320.0;
    let lon_delta = lat_delta / q.latitude.to_radians().cos().abs().max(0.01);

    // The && predicate and the separate route-type branches can use the existing
    // partial GiST indexes on gtfs.shapes. A bounded KNN candidate set prevents
    // work from growing with the number of shapes crossing a dense map tap.
    // Geography is only evaluated AFTER this cheap indexed candidate search.
    const SQL: &str = r#"
        WITH candidates AS MATERIALIZED (
            SELECT s.chateau, s.onestop_feed_id, s.attempt_id,
                   s.routes, s.route_type, s.route_label, s.color, s.linestring
              FROM gtfs.shapes AS s
             WHERE s.allowed_spatial_query = true
               AND s.routes IS NOT NULL
               AND s.linestring && ST_MakeEnvelope($3, $4, $5, $6, 4326)
               AND (
                 ($8::boolean AND s.route_type IN (3, 11, 200)
                   AND s.chateau NOT IN ('flixbus~europe','flixbus~america','ouibus'))
                 OR ($9::boolean AND s.route_type IN (0, 1, 5, 7, 11, 12))
                 OR ($10::boolean AND s.route_type = 2)
                 OR ($11::boolean AND s.route_type = 4)
               )
             ORDER BY s.linestring <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
             LIMIT 256
        ), hits AS MATERIALIZED (
            SELECT c.*, ST_Distance(
                c.linestring::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
            ) AS distance_m
              FROM candidates AS c
             WHERE ST_DWithin(
                c.linestring::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                $7
             )
        ), unique_routes AS (
            SELECT DISTINCT ON (h.chateau, ids.route_id)
                   h.chateau, ids.route_id,
                   COALESCE(NULLIF(r.color, ''), NULLIF(h.color, ''), 'FFFFFF') AS colour,
                   COALESCE(NULLIF(r.short_name, ''), NULLIF(r.long_name, ''),
                            NULLIF(h.route_label, ''), ids.route_id) AS name,
                   h.route_type::integer AS route_type,
                   h.distance_m
              FROM hits AS h
              CROSS JOIN LATERAL unnest(h.routes) AS ids(route_id)
              LEFT JOIN gtfs.routes AS r
                ON r.onestop_feed_id = h.onestop_feed_id
               AND r.attempt_id = h.attempt_id
               AND r.route_id = ids.route_id
             WHERE ids.route_id IS NOT NULL AND ids.route_id <> ''
             ORDER BY h.chateau, ids.route_id, h.distance_m
        )
        SELECT chateau, route_id, colour, name, route_type
          FROM unique_routes
         ORDER BY distance_m, chateau, route_id
         LIMIT 32
    "#;

    let mut transaction = match pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            log::error!("route hit test: database unavailable: {err}");
            return HttpResponse::ServiceUnavailable().finish();
        }
    };
    // PostgreSQL aborts the query rather than monopolizing a database connection.
    if let Err(err) = sqlx::query("SET LOCAL statement_timeout = '1500ms'")
        .execute(&mut *transaction)
        .await
    {
        log::error!("route hit test: cannot set timeout: {err}");
        return HttpResponse::ServiceUnavailable().finish();
    }

    let result = sqlx::query(SQL)
        .bind(q.longitude)
        .bind(q.latitude)
        .bind(q.longitude - lon_delta)
        .bind(q.latitude - lat_delta)
        .bind(q.longitude + lon_delta)
        .bind(q.latitude + lat_delta)
        .bind(radius_m)
        .bind(q.bus)
        .bind(q.local_rail)
        .bind(q.intercity_rail)
        .bind(q.other)
        .fetch_all(&mut *transaction)
        .await;

    match result {
        Ok(rows) => {
            let routes: Vec<RouteHit> = rows
                .into_iter()
                .map(|row| RouteHit {
                    chateau: row.get("chateau"),
                    route_id: row.get("route_id"),
                    colour: row.get("colour"),
                    name: row.get("name"),
                    route_type: row.get("route_type"),
                })
                .collect();
            HttpResponse::Ok()
                .insert_header(("Cache-Control", "no-store"))
                .json(routes)
        }
        Err(err) => {
            log::warn!("route hit test failed: {err}");
            HttpResponse::ServiceUnavailable().body("Route lookup unavailable")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tap_radius_is_finite_and_bounded() {
        for lat in [-85.0, 0.0, 65.0, 85.0] {
            for zoom in [4.0, 10.0, 16.0, 20.0] {
                let radius = hit_radius_m(lat, zoom);
                assert!(radius.is_finite());
                assert!((4.0..=1_200.0).contains(&radius));
            }
        }
    }
}
