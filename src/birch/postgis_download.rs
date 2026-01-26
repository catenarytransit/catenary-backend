use actix_web::middleware::DefaultHeaders;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware, web};
use catenary::EtcdConnectionIps;
use catenary::models::IpToGeoAddr;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use geojson::{Feature, GeoJson, JsonValue};
use ordered_float::Pow;
use serde::Deserialize;
use serde_derive::Serialize;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tilejson::TileJSON;

// smaller simplification threshold means more detail

#[derive(Deserialize)]
struct BboxForPostgis {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

#[derive(Serialize)]
struct CountShapesResponse {
    intercityrail_shapes: usize,
    metro_shapes: usize,
    tram_shapes: usize,
}

#[actix_web::get("/countrailinbox")]
pub async fn count_rail_in_box(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
    query: web::Query<BboxForPostgis>,
) -> impl Responder {
    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = "SELECT count(DISTINCT routes) FROM gtfs.shapes WHERE ST_Intersects(linestring, ST_MakeEnvelope($1, $2, $3, $4, 4326)) AND route_type = $5";

    let intercityrail_shapes_fut = sqlx::query_scalar::<_, i64>(query_str)
        .bind(query.min_x)
        .bind(query.min_y)
        .bind(query.max_x)
        .bind(query.max_y)
        .bind(2)
        .fetch_one(sqlx_pool_ref);

    let metro_shapes_fut = sqlx::query_scalar::<_, i64>(query_str)
        .bind(query.min_x)
        .bind(query.min_y)
        .bind(query.max_x)
        .bind(query.max_y)
        .bind(1)
        .fetch_one(sqlx_pool_ref);

    let tram_shapes_fut = sqlx::query_scalar::<_, i64>(query_str)
        .bind(query.min_x)
        .bind(query.min_y)
        .bind(query.max_x)
        .bind(query.max_y)
        .bind(0)
        .fetch_one(sqlx_pool_ref);

    let (intercityrail_shapes, metro_shapes, tram_shapes) =
        futures::join!(intercityrail_shapes_fut, metro_shapes_fut, tram_shapes_fut);

    let resp = CountShapesResponse {
        intercityrail_shapes: intercityrail_shapes.unwrap_or(0) as usize,
        metro_shapes: metro_shapes.unwrap_or(0) as usize,
        tram_shapes: tram_shapes.unwrap_or(0) as usize,
    };

    HttpResponse::Ok().json(resp)
}

#[actix_web::get("/busstops")]
pub async fn bus_stops_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_stops_fields(),
        String::from("busstops"),
        vec![String::from(
            "https://birch_stops_tiles.catenarymaps.org/busstops/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        1000,
    )
}

#[actix_web::get("/busstops/{z}/{x}/{y}")]
pub async fn bus_stops(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();
    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();
    let query_str = build_stops_query(
        z,
        x,
        y,
        "ARRAY[3,11,200,1700,1500,1702]::smallint[] && route_types::smallint[] OR ARRAY[3,11,200,1700,1500,1702]::smallint[] && children_route_types::smallint[]",
    );
    fetch_mvt(sqlx_pool_ref, query_str, 1000).await
}

#[actix_web::get("/station_features")]
pub async fn station_features_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_stops_fields(),
        String::from("station_features"),
        vec![String::from(
            "https://birch_stops_tiles.catenarymaps.org/station_features/{z}/{x}/{y}",
        )],
        Some(7),
        Some(19),
        1000,
    )
}

#[actix_web::get("/station_features/{z}/{x}/{y}")]
pub async fn station_features(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();
    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();
    let query_str = build_stops_query(
        z,
        x,
        y,
        "location_type=2 OR location_type=3 OR location_type=4",
    );
    fetch_mvt(sqlx_pool_ref, query_str, 1000).await
}

#[actix_web::get("/railstops/{z}/{x}/{y}")]
pub async fn rail_stops(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();
    let query_str = build_stops_query(
        z,
        x,
        y,
        "ARRAY[0,1,2,5,12]::smallint[] && route_types::smallint[] OR ARRAY[0,1,2,5,12]::smallint[] && children_route_types::smallint[]",
    );
    fetch_mvt(sqlx_pool_ref, query_str, 1000).await
}

#[actix_web::get("/railstops")]
pub async fn rail_stops_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_stops_fields(),
        String::from("railstops"),
        vec![String::from(
            "https://birch_stops_tiles.catenarymaps.org/railstops/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        10000,
    )
}

#[actix_web::get("/otherstops/{z}/{x}/{y}")]
pub async fn other_stops(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();
    let query_str = build_stops_query(
        z,
        x,
        y,
        "ARRAY[4,6,7]::smallint[] && route_types::smallint[] OR ARRAY[4,6,7]::smallint[] && children_route_types::smallint[]",
    );
    fetch_mvt(sqlx_pool_ref, query_str, 1000).await
}

#[actix_web::get("/otherstops")]
pub async fn other_stops_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_stops_fields(),
        String::from("otherstops"),
        vec![String::from(
            "https://birch_stops_tiles.catenarymaps.org/otherstops/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        10000,
    )
}

#[actix_web::get("/unmatched_railstops/{z}/{x}/{y}")]
pub async fn unmatched_rail_stops(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();
    let query_str = build_stops_query(
        z,
        x,
        y,
        "(ARRAY[0,1,2,5,12]::smallint[] && route_types::smallint[] OR ARRAY[0,1,2,5,12]::smallint[] && children_route_types::smallint[]) AND osm_station_id IS NULL AND NOT (chateau = 'sncf' AND gtfs_id LIKE 'StopArea:%')",
    );
    fetch_mvt(sqlx_pool_ref, query_str, 1000).await
}

#[actix_web::get("/unmatched_railstops")]
pub async fn unmatched_rail_stops_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_stops_fields(),
        String::from("unmatched_railstops"),
        vec![String::from(
            "https://birch_stops_tiles.catenarymaps.org/unmatched_railstops/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        10000,
    )
}

fn tile_width_degrees_from_z(z: u8) -> f32 {
    360.0 / (2.pow(z + 1) as f32)
}

#[actix_web::get("/shapes_intercity_rail/{z}/{x}/{y}")]
pub async fn shapes_intercity_rail(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let eligible_for_cache_hit = z <= 11;

    let category = catenary::shape_fetcher::Category::IntercityRailOriginal;

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    if eligible_for_cache_hit {
        let fetch_tile = catenary::shape_fetcher::fetch_tile(conn, x, y, z, category.clone()).await;

        if let Ok(tile) = fetch_tile {
            if let Some(tile) = tile.get(0) {
                if tile.mvt_data.len() > 0 {
                    return HttpResponse::Ok()
                        .insert_header(("Content-Type", "application/x-protobuf"))
                        .insert_header(("Cache-Control", "max-age=600, public"))
                        .body(tile.mvt_data.clone());
                }
            }
        }
    }

    let tile_width_degrees = tile_width_degrees_from_z(z);

    let simplification_threshold = tile_width_degrees * 0.001;

    // let grid = tile_grid::Grid::wgs84();

    //let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = build_shapes_query(z, x, y, simplification_threshold, "route_type = 2");

    // println!("Performing query \n {}", query_str);

    let max_age = match z {
        4 => 10000,
        5 => 5000,
        6 => 2000,
        _ => 1000,
    };

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            if eligible_for_cache_hit {
                let mvt_bytes_clone = mvt_bytes.clone();
                let category_clone = category.clone();
                let pool_clone = pool.clone();
                actix_web::rt::spawn(async move {
                    let conn_pool = pool_clone.as_ref();
                    let conn_pre = conn_pool.get().await;
                    let mut conn = conn_pre.unwrap();
                    let insert_tile = catenary::shape_fetcher::insert_tile(
                        &mut conn,
                        x,
                        y,
                        z,
                        category_clone,
                        mvt_bytes_clone,
                        chrono::Utc::now(),
                    )
                    .await;
                    if let Err(err) = insert_tile {
                        eprintln!("{:?}", err);
                    }
                });
            }

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", format!("max-age={}, public", max_age)))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
}

#[actix_web::get("/shapes_ferry/{z}/{x}/{y}")]
pub async fn shapes_ferry(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let tile_width_degrees = tile_width_degrees_from_z(z);

    let simplification_threshold = tile_width_degrees * 0.004;

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = build_shapes_query(z, x, y, simplification_threshold, "route_type = 4");

    let max_age = match z {
        4 => 36000,
        5 => 10000,
        6 => 2000,
        _ => 1000,
    };

    fetch_mvt(sqlx_pool_ref, query_str, max_age).await
}

#[actix_web::get("/shapes_local_rail/{z}/{x}/{y}")]
pub async fn shapes_local_rail(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let tile_width_degrees = tile_width_degrees_from_z(z);

    let simplification_threshold = tile_width_degrees * 0.001;

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = build_shapes_query(
        z,
        x,
        y,
        simplification_threshold,
        "route_type IN (0,1,5,7,11,12)",
    );

    let max_age = match z {
        4 => 1000,
        5 => 1000,
        6 => 1000,
        _ => 500,
    };

    fetch_mvt(sqlx_pool_ref, query_str, max_age).await
}

#[actix_web::get("/shapes_bus/{z}/{x}/{y}")]
pub async fn shapes_bus(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let tile_width_degrees = tile_width_degrees_from_z(z);

    let simp_amount: f32 = match z {
        6 => 0.005,
        7 => 0.004,
        8 => 0.004,
        _ => 0.003,
    };

    //lower means better detail
    let simplification_threshold = tile_width_degrees * simp_amount;

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = build_shapes_query(
        z,
        x,
        y,
        simplification_threshold,
        "route_type IN (3,11,200) AND routes != '{}' AND chateau != 'flixbus~europe' AND chateau != 'flixbus~america'",
    );

    fetch_mvt(sqlx_pool_ref, query_str, 1000).await
}

#[actix_web::get("/shapes_ferry")]
pub async fn shapes_ferry_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_shapes_fields(),
        String::from("shapes_not_bus"),
        vec![String::from(
            "https://birch.catenarymaps.org/shapes_ferry/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        1000,
    )
}

#[actix_web::get("/shapes_not_bus")]
pub async fn shapes_not_bus_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_shapes_fields(),
        String::from("shapes_not_bus"),
        vec![String::from(
            "https://birch.catenarymaps.org/shapes_not_bus/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        1000,
    )
}

#[actix_web::get("/shapes_intercity_rail")]
pub async fn shapes_intercity_rail_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_shapes_fields(),
        String::from("shapes_local_rail"),
        vec![
            String::from(
                "https://birch_intercity_rail_shape_1.catenarymaps.org/shapes_intercity_rail/{z}/{x}/{y}",
            ),
            String::from(
                "https://birch_intercity_rail_shape_2.catenarymaps.org/shapes_intercity_rail/{z}/{x}/{y}",
            ),
            String::from(
                "https://birch_intercity_rail_shape_3.catenarymaps.org/shapes_intercity_rail/{z}/{x}/{y}",
            ),
        ],
        None,
        Some(15),
        1000,
    )
}

#[actix_web::get("/shapes_local_rail")]
pub async fn shapes_local_rail_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_shapes_fields(),
        String::from("shapes_local_rail"),
        vec![
            String::from(
                "https://birch_local_rail_shape_1.catenarymaps.org/shapes_local_rail/{z}/{x}/{y}",
            ),
            String::from(
                "https://birch_local_rail_shape_2.catenarymaps.org/shapes_local_rail/{z}/{x}/{y}",
            ),
        ],
        None,
        Some(15),
        1000,
    )
}

#[actix_web::get("/shapes_bus")]
pub async fn shapes_bus_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_shapes_fields(),
        String::from("shapes_bus"),
        vec![String::from(
            "https://birch_shapes_bus.catenarymaps.org/shapes_bus/{z}/{x}/{y}",
        )],
        None,
        Some(15),
        1000,
    )
}

fn get_stops_fields() -> std::collections::BTreeMap<String, String> {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));
    fields.insert(String::from("attempt_id"), String::from("text"));
    fields.insert(String::from("gtfs_id"), String::from("text"));
    fields.insert(String::from("name"), String::from("text"));
    fields.insert(String::from("displayname"), String::from("text"));
    fields.insert(String::from("code"), String::from("text"));
    fields.insert(String::from("gtfs_desc"), String::from("text"));
    fields.insert(String::from("location_type"), String::from("smallint"));
    fields.insert(String::from("parent_station"), String::from("text"));
    fields.insert(String::from("zone_id"), String::from("text"));
    fields.insert(String::from("url"), String::from("text"));
    fields.insert(String::from("timezone"), String::from("text"));
    fields.insert(
        String::from("wheelchair_boarding"),
        String::from("smallint"),
    );
    fields.insert(String::from("level_id"), String::from("text"));
    fields.insert(String::from("platform_code"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_types"), String::from("smallint[]"));
    fields.insert(String::from("children_ids"), String::from("text[]"));
    fields.insert(
        String::from("children_route_types"),
        String::from("smallint[]"),
    );
    fields.insert(String::from("osm_station_id"), String::from("bigint"));
    fields.insert(String::from("osm_platform_id"), String::from("bigint"));
    fields
}

fn get_shapes_fields() -> std::collections::BTreeMap<String, String> {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_type"), String::from("smallint"));
    fields.insert(String::from("route_label"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));
    fields.insert(
        String::from("stop_to_stop_generated"),
        String::from("boolean"),
    );
    fields
}

fn serve_tilejson(
    fields: std::collections::BTreeMap<String, String>,
    name: String,
    tiles_url: Vec<String>,
    minzoom: Option<u8>,
    maxzoom: Option<u8>,
    max_age: u32,
) -> HttpResponse {
    let fields = tilejson::VectorLayer::new(String::from("data"), fields);

    let tile_json = TileJSON {
        vector_layers: Some(vec![fields]),
        tilejson: String::from("3.0.0"),
        bounds: None,
        center: None,
        data: None,
        description: None,
        fillzoom: None,
        grids: None,
        legend: None,
        maxzoom,
        minzoom,
        name: Some(name),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: tiles_url,
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", format!("max-age={}, public", max_age)))
        .body(serde_json::to_string(&tile_json).unwrap())
}

async fn fetch_mvt(
    sqlx_pool: &sqlx::Pool<sqlx::Postgres>,
    query_str: String,
    max_age: u32,
) -> HttpResponse {
    match sqlx::query(query_str.as_str()).fetch_one(sqlx_pool).await {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", format!("max-age={}, public", max_age)))
                .body(mvt_bytes)
        }
        Err(err) => {
            eprintln!("{:?}", err);
            HttpResponse::InternalServerError().body("Failed to fetch from postgres!")
        }
    }
}

fn build_stops_query(z: u8, x: u32, y: u32, where_clause: &str) -> String {
    format!("
    SELECT
    ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
    SELECT
        onestop_feed_id,
        chateau,
        attempt_id,
        gtfs_id,
        name,
        displayname,
        code,
        gtfs_desc,
        location_type,
        parent_station,
        zone_id,
        url,
        timezone,
        wheelchair_boarding,
        level_id,
        platform_code,
        routes,
        route_types,
        children_ids,
        children_route_types,
        osm_station_id,
        osm_platform_id,
        ST_AsMVTGeom(ST_Transform(point, 3857),
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.stops
    WHERE
        (point && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true
        AND ({where_clause})
) q", z = z, x = x, y= y, where_clause = where_clause)
}

fn build_shapes_query(
    z: u8,
    x: u32,
    y: u32,
    simplification_threshold: f32,
    where_clause: &str,
) -> String {
    format!("
    SELECT
    ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
    SELECT
        onestop_feed_id,
        shape_id,
        color,
        routes,
        route_type,
        route_label,
        text_color,
        chateau,
        stop_to_stop_generated,
        ST_AsMVTGeom(ST_Transform(ST_Simplify(linestring, {simplification_threshold}), 3857),
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND ({where_clause})
) q", z = z, x = x, y= y, simplification_threshold = simplification_threshold, where_clause = where_clause)
}

fn get_osm_stations_fields() -> std::collections::BTreeMap<String, String> {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("osm_id"), String::from("bigint"));
    fields.insert(String::from("osm_type"), String::from("text"));
    fields.insert(String::from("name"), String::from("text"));
    fields.insert(String::from("station_type"), String::from("text"));
    fields.insert(String::from("railway_tag"), String::from("text"));
    fields.insert(String::from("mode_type"), String::from("text"));
    fields.insert(String::from("uic_ref"), String::from("text"));
    fields.insert(String::from("ref"), String::from("text"));
    fields.insert(String::from("wikidata"), String::from("text"));
    fields.insert(String::from("operator"), String::from("text"));
    fields.insert(String::from("network"), String::from("text"));
    fields.insert(String::from("level"), String::from("text"));
    fields.insert(String::from("local_ref"), String::from("text"));
    fields.insert(String::from("parent_osm_id"), String::from("bigint"));
    fields.insert(String::from("is_derivative"), String::from("boolean"));
    fields
}

#[actix_web::get("/osm_stations")]
pub async fn osm_stations_meta(req: HttpRequest) -> impl Responder {
    serve_tilejson(
        get_osm_stations_fields(),
        String::from("osm_stations"),
        vec![String::from(
            "https://birch.catenarymaps.org/osm_stations/{z}/{x}/{y}",
        )],
        Some(4),
        Some(19),
        10000,
    )
}

#[actix_web::get("/osm_stations/{z}/{x}/{y}")]
pub async fn osm_stations(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 4 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();
    let query_str = build_osm_stations_query(z, x, y);
    fetch_mvt(sqlx_pool_ref, query_str, 10000).await
}

fn build_osm_stations_query(z: u8, x: u32, y: u32) -> String {
    format!(
        "
    SELECT
    ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
    SELECT
        osm_id,
        osm_type,
        name,
        station_type,
        railway_tag,
        mode_type,
        uic_ref,
        ref as ref,
        wikidata,
        operator,
        network,
        level,
        local_ref,
        parent_osm_id,
        is_derivative,
        ST_AsMVTGeom(ST_Transform(point, 3857),
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.osm_stations
    WHERE
        (point && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326))
) q",
        z = z,
        x = x,
        y = y
    )
}
