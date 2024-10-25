use actix_web::middleware::DefaultHeaders;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use catenary::models::IpToGeoAddr;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use catenary::EtcdConnectionIps;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use geojson::{Feature, GeoJson, JsonValue};
use ordered_float::Pow;
use serde::Deserialize;
use serde_derive::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tilejson::TileJSON;

// smaller simplification threshold means more detail

#[actix_web::get("/busstops")]
pub async fn bus_stops_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();

    fields.insert(String::from("onestop_feed_id"), String::from("text"));
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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("busstops")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/busstops/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000, public"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/busstops/{z}/{x}/{y}")]
pub async fn bus_stops(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    //let grid = tile_grid::Grid::wgs84();

    // let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
    SELECT
    ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
    SELECT
        onestop_feed_id,
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
        ST_AsMVTGeom(ST_Transform(point, 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.stops
    WHERE
        (point && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true
        AND (ARRAY[3,11,200,1700,1500,1702]::smallint[] && route_types::smallint[] OR ARRAY[3,11,200,1700,1500,1702]::smallint[] && children_route_types::smallint[])
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "max-age=1000, public"))
                .body(mvt_bytes)
        }
        Err(err) => {
            eprintln!("{:?}", err);
            HttpResponse::InternalServerError().body("Failed to fetch from postgres!")
        }
    }
}

#[actix_web::get("/station_features")]
pub async fn station_features_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();

    fields.insert(String::from("onestop_feed_id"), String::from("text"));
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
        maxzoom: Some(19),
        minzoom: Some(7),
        name: Some(String::from("station_features")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/station_features/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000, public"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/station_features/{z}/{x}/{y}")]
pub async fn station_features(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    //let grid = tile_grid::Grid::wgs84();

    // let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!(
        "
SELECT
ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
SELECT
    onestop_feed_id,
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
    ST_AsMVTGeom(ST_Transform(point, 3857), 
    ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
FROM
    gtfs.stops
WHERE
    (point && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true
    AND (location_type=2 OR location_type=3 OR location_type=4)
) q",
        z = z,
        x = x,
        y = y
    );

    // println!("Performing query \n {}", query_str);

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "max-age=1000, public"))
                .body(mvt_bytes)
        }
        Err(err) => {
            eprintln!("{:?}", err);
            HttpResponse::InternalServerError().body("Failed to fetch from postgres!")
        }
    }
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

    //let grid = tile_grid::Grid::wgs84();

    // let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
    SELECT
    ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
    SELECT
        onestop_feed_id,
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
        ST_AsMVTGeom(ST_Transform(point, 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.stops
    WHERE
        (point && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true
        AND (ARRAY[0,1,2,5,12]::smallint[] && route_types::smallint[] OR ARRAY[0,1,2,5,12]::smallint[] && children_route_types::smallint[]) 
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "max-age=1000, public"))
                .body(mvt_bytes)
        }
        Err(err) => {
            eprintln!("{:?}", err);
            HttpResponse::InternalServerError().body("Failed to fetch from postgres!")
        }
    }
}

#[actix_web::get("/railstops")]
pub async fn rail_stops_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();

    fields.insert(String::from("onestop_feed_id"), String::from("text"));
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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("railstops")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/railstops/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=10000, public"))
        .body(serde_json::to_string(&tile_json).unwrap())
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

    //let grid = tile_grid::Grid::wgs84();

    // let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
    SELECT
    ST_AsMVT(q, 'data', 4096, 'geom')
FROM (
    SELECT
        onestop_feed_id,
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
        ST_AsMVTGeom(ST_Transform(point, 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.stops
    WHERE
        (point && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true
        AND (ARRAY[4,6,7]::smallint[] && route_types::smallint[] OR ARRAY[4,6,7]::smallint[] && children_route_types::smallint[])
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "max-age=1000, public"))
                .body(mvt_bytes)
        }
        Err(err) => {
            eprintln!("{:?}", err);
            HttpResponse::InternalServerError().body("Failed to fetch from postgres!")
        }
    }
}

#[actix_web::get("/otherstops")]
pub async fn other_stops_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();

    fields.insert(String::from("onestop_feed_id"), String::from("text"));
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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("otherstops")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/otherstops/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=10000, public"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

fn tile_width_degrees_from_z(z: u8) -> f32 {
    360.0 / (2.pow(z + 1) as f32)
}

#[actix_web::get("/shapes_not_bus/{z}/{x}/{y}")]
pub async fn shapes_not_bus(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if z < 5 {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

    let tile_width_degrees = tile_width_degrees_from_z(z);

    let simplification_threshold = tile_width_degrees * 0.006;

    // let grid = tile_grid::Grid::wgs84();

    //let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
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
        ST_AsMVTGeom(ST_Transform(ST_Simplify(linestring, {simplification_threshold}), 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND route_type IN (0,1,2,4,5,7,11,12)
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    let max_age = match z {
        5 => 10000,
        6 => 2000,
        _ => 1000,
    };

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", format!("max-age={}, public", max_age)))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
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

    let tile_width_degrees = tile_width_degrees_from_z(z);

    let simplification_threshold = tile_width_degrees * 0.003;

    // let grid = tile_grid::Grid::wgs84();

    //let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
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
        ST_AsMVTGeom(ST_Transform(ST_Simplify(linestring, {simplification_threshold}), 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND route_type = 2
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    let max_age = match z {
        4 => 36000,
        5 => 10000,
        6 => 2000,
        _ => 1000,
    };

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

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

    let simplification_threshold = tile_width_degrees * 0.005;

    // let grid = tile_grid::Grid::wgs84();

    //let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
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
        ST_AsMVTGeom(ST_Transform(ST_Simplify(linestring, {simplification_threshold}), 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND route_type = 4
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    let max_age = match z {
        4 => 36000,
        5 => 10000,
        6 => 2000,
        _ => 1000,
    };

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", format!("max-age={}, public", max_age)))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
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

    let simplification_threshold = tile_width_degrees * 0.003;

    // let grid = tile_grid::Grid::wgs84();

    //let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
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
        ST_AsMVTGeom(ST_Transform(ST_Simplify(linestring, {simplification_threshold}), 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND route_type IN (0,1,5,7,11,12)
) q", z = z, x = x, y= y);

    // println!("Performing query \n {}", query_str);

    let max_age = match z {
        4 => 36000,
        5 => 10000,
        6 => 2000,
        _ => 1000,
    };

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", format!("max-age={}, public", max_age)))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
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

    //lower means better detail
    let simplification_threshold = tile_width_degrees * 0.005;

    // let grid = tile_grid::Grid::wgs84();
    // let bbox = grid.tile_extent(x, y, z);

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let query_str = format!("
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
        ST_AsMVTGeom(ST_Transform(ST_Simplify(linestring, {simplification_threshold}), 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND route_type IN (3,11,200)
) q", z = z, x = x, y= y);

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "max-age=1000, public"))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
}

#[actix_web::get("/shapes_ferry")]
pub async fn shapes_ferry_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_type"), String::from("smallint"));
    fields.insert(String::from("route_label"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));

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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("shapes_not_bus")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/shapes_ferry/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/shapes_not_bus")]
pub async fn shapes_not_bus_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_type"), String::from("smallint"));
    fields.insert(String::from("route_label"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));

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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("shapes_not_bus")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/shapes_not_bus/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/shapes_intercity_rail")]
pub async fn shapes_intercity_rail_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_type"), String::from("smallint"));
    fields.insert(String::from("route_label"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));

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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("shapes_local_rail")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/shapes_intercity_rail/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/shapes_local_rail")]
pub async fn shapes_local_rail_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_type"), String::from("smallint"));
    fields.insert(String::from("route_label"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));

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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("shapes_local_rail")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/shapes_local_rail/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/shapes_bus")]
pub async fn shapes_bus_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("onestop_feed_id"), String::from("text"));
    fields.insert(String::from("routes"), String::from("text[]"));
    fields.insert(String::from("route_type"), String::from("smallint"));
    fields.insert(String::from("route_label"), String::from("text"));
    fields.insert(String::from("chateau"), String::from("text"));

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
        maxzoom: Some(15),
        minzoom: None,
        name: Some(String::from("shapes_bus")),
        scheme: None,
        template: None,
        version: None,
        other: std::collections::BTreeMap::new(),
        tiles: vec![String::from(
            "https://birch.catenarymaps.org/shapes_bus/{z}/{x}/{y}",
        )],
        attribution: None,
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=1000, public"))
        .body(serde_json::to_string(&tile_json).unwrap())
}
