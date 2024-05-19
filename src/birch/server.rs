// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed

#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect
)]

use actix_web::dev::Service;
use actix_web::middleware::DefaultHeaders;
use actix_web::web::Data;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bb8::Pool;
use cached::proc_macro::once;
use catenary::aspen::lib::ChateauMetadataZookeeper;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::select_dsl::SelectDsl;
use diesel::sql_types::{Float, Integer};
use diesel::ExpressionMethods;
use diesel::Selectable;
use diesel::SelectableHelper;
use diesel_async::pooled_connection::bb8::PooledConnection;
use diesel_async::RunQueryDsl;
use geojson::{Feature, GeoJson, Geometry, JsonValue, Value};
use qstring::QString;
use rand::Rng;
use rstar::RTree;
use scc::HashMap as SccHashMap;
use serde::Deserialize;
use serde_derive::Serialize;
use serde_json::to_string;
use serde_json::{json, to_string_pretty};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{FromRow, Row};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};
use tilejson::TileJSON;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tokio_postgres::Error as PostgresError;
use tokio_zookeeper::ZooKeeper;
use zstd_safe::WriteBuf;

mod api_key_management;
mod aspenised_data_over_https;
mod chicago_proxy;
mod get_vehicle_trip_information;
mod nearby_departures;

#[derive(Clone, Debug)]
struct ChateauCache {
    last_updated_time_ms: u64,
    chateau_geojson: String,
}

type ChateauCacheActixData = Arc<RwLock<Option<ChateauCache>>>;

#[derive(serde::Serialize)]
struct StaticFeed {
    onestop_feed_id: String,
    max_lat: f64,
    max_lon: f64,
    min_lat: f64,
    min_lon: f64,
    operators: Vec<String>,
    operators_hashmap: HashMap<String, Option<String>>,
}

#[derive(serde::Serialize)]
struct RealtimeFeedPostgres {
    onestop_feed_id: String,
    operators: Vec<String>,
    operators_to_gtfs_ids: HashMap<String, Option<String>>,
}

#[derive(serde::Serialize)]
struct OperatorPostgres {
    onestop_operator_id: String,
    name: String,
    gtfs_static_feeds: Vec<String>,
    gtfs_realtime_feeds: Vec<String>,
    static_onestop_feeds_to_gtfs_ids: HashMap<String, Option<String>>,
    realtime_onestop_feeds_to_gtfs_ids: HashMap<String, Option<String>>,
}

pub struct BirchGlobalDatastore {
    pub chateau_assignment_cache: Arc<SccHashMap<String, (u64, ChateauMetadataZookeeper)>>,
}

async fn index(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("Hello world!")
}

async fn robots(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("User-agent: GPTBot\nDisallow: /\nUser-agent: Google-Extended\nDisallow: /")
}

#[actix_web::get("/microtime")]
pub async fn microtime(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body(format!(
            "{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ))
}

#[actix_web::get("/nanotime")]
pub async fn nanotime(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body(format!(
            "{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
}

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
        maxzoom: None,
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
        .insert_header(("Cache-Control", "max-age=100000"))
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
                .insert_header(("Cache-Control", "max-age=86400"))
                .body(mvt_bytes)
        }
        Err(err) => {
            eprintln!("{:?}", err);
            HttpResponse::InternalServerError().body("Failed to fetch from postgres!")
        }
    }
}


#[actix_web::get("/nanotime")]
pub async fn nanotime(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body(format!(
            "{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
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
        maxzoom: None,
        minzoom: None,
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
        .insert_header(("Cache-Control", "max-age=100000"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/station_features/{z}/{x}/{y}")]
pub async fn station_features(sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
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
    AND (location_type=2 OR location_type=3 OR location_type=4)
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
            .insert_header(("Cache-Control", "max-age=86400"))
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

    if (z < 4) {
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
                .insert_header(("Cache-Control", "max-age=86400"))
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
        maxzoom: None,
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
        .insert_header(("Cache-Control", "max-age=100000"))
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

    if (z < 4) {
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
                .insert_header(("Cache-Control", "max-age=86400"))
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
        maxzoom: None,
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
        .insert_header(("Cache-Control", "max-age=100000"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/shapes_not_bus/{z}/{x}/{y}")]
pub async fn shapes_not_bus(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if (z < 2) {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

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
        attempt_id,
        color,
        routes,
        route_type,
        route_label,
        text_color,
        chateau,
        ST_AsMVTGeom(ST_Transform(linestring, 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes_not_bus
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true
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
                .insert_header(("Cache-Control", "max-age=86400"))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
}

#[actix_web::get("/getroutesofchateau/{chateau}")]
async fn routesofchateau(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(String)>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let (chateau_id) = path.into_inner();

    use catenary::schema::gtfs::routes as routes_pg_schema;

    let routes = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await
        .unwrap();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=3600"))
        .body(serde_json::to_string(&routes).unwrap())
}

#[actix_web::get("/shapes_bus/{z}/{x}/{y}")]
pub async fn shapes_bus(
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    if (z < 4) {
        return HttpResponse::BadRequest().body("Zoom level too low");
    }

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
        attempt_id,
        color,
        routes,
        route_type,
        route_label,
        text_color,
        chateau,
        ST_AsMVTGeom(ST_Transform(linestring, 3857), 
        ST_TileEnvelope({z}, {x}, {y}), 4096, 64, true) AS geom
    FROM
        gtfs.shapes
    WHERE
        (linestring && ST_Transform(ST_TileEnvelope({z}, {x}, {y}), 4326)) AND allowed_spatial_query = true AND (route_type = 3 OR route_type = 11 OR route_type = 200)
) q", z = z, x = x, y= y);

    match sqlx::query(query_str.as_str())
        .fetch_one(sqlx_pool_ref)
        .await
    {
        Ok(mvt_result) => {
            let mvt_bytes: Vec<u8> = mvt_result.get(0);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "max-age=86400"))
                .body(mvt_bytes)
        }
        Err(err) => HttpResponse::InternalServerError().body("Failed to fetch from postgres!"),
    }
}

#[actix_web::get("/shapes_not_bus")]
pub async fn shapes_not_bus_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("attempt_id"), String::from("text"));
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
        maxzoom: None,
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
        .insert_header(("Cache-Control", "max-age=86400"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/shapes_bus")]
pub async fn shapes_bus_meta(req: HttpRequest) -> impl Responder {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(String::from("color"), String::from("text"));
    fields.insert(String::from("text_color"), String::from("text"));
    fields.insert(String::from("shape_id"), String::from("text"));
    fields.insert(String::from("attempt_id"), String::from("text"));
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
        maxzoom: None,
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
        .insert_header(("Cache-Control", "max-age=86400"))
        .body(serde_json::to_string(&tile_json).unwrap())
}

#[actix_web::get("/metrolinktrackproxy")]
pub async fn metrolinktrackproxy(req: HttpRequest) -> impl Responder {
    let raw_data = reqwest::get("https://rtt.metrolinktrains.com/StationScheduleList.json").await;

    match raw_data {
        Ok(raw_data) => {
            let raw_text = raw_data.text().await;

            match raw_text {
                Ok(raw_text) => HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .body(raw_text),
                Err(error) => HttpResponse::InternalServerError()
                    .insert_header(("Content-Type", "text/plain"))
                    .body("Could not fetch Metrolink data"),
            }
        }
        Err(error) => HttpResponse::InternalServerError()
            .insert_header(("Content-Type", "text/plain"))
            .body("Could not fetch Metrolink data"),
    }
}

#[actix_web::get("/amtrakproxy")]
pub async fn amtrakproxy(req: HttpRequest) -> impl Responder {
    let raw_data =
        reqwest::get("https://maps.amtrak.com/services/MapDataService/trains/getTrainsData").await;

    match raw_data {
        Ok(raw_data) => {
            //println!("Raw data successfully downloaded");

            match amtk::decrypt(raw_data.text().await.unwrap().as_str()) {
                Ok(decrypted_string) => HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .body(decrypted_string),
                Err(err) => HttpResponse::InternalServerError()
                    .insert_header(("Content-Type", "text/plain"))
                    .body("Could not decrypt Amtrak data"),
            }
        }
        Err(error) => HttpResponse::InternalServerError()
            .insert_header(("Content-Type", "text/plain"))
            .body("Could not fetch Amtrak data"),
    }
}

#[derive(Clone, Debug)]
struct ChateauToSend {
    chateau: String,
    hull: geo::MultiPolygon,
    realtime_feeds: Vec<String>,
    schedule_feeds: Vec<String>,
}

#[actix_web::get("/getchateaus")]
async fn chateaus(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
    chateau_cache: web::Data<ChateauCacheActixData>,
) -> impl Responder {
    let chateau_lock = chateau_cache.read().unwrap();
    let chateau_as_ref = chateau_lock.as_ref();

    let cloned_chateau_data = match chateau_as_ref {
        Some(chateau_as_ref) => Some(chateau_as_ref.clone()),
        None => None,
    };

    drop(chateau_lock);

    if let Some(cloned_chateau_data) = cloned_chateau_data {
        if cloned_chateau_data.last_updated_time_ms
            > SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - 3_600_000
        {
            return HttpResponse::Ok()
                .insert_header(("Content-Type", "application/json"))
                .insert_header(("Cache-Control", "max-age=60"))
                .body(cloned_chateau_data.chateau_geojson);
        }
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    // fetch out of table
    let existing_chateaus = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await
        .unwrap();

    // convert hulls to standardised `geo` crate
    let formatted_chateaus = existing_chateaus
        .into_iter()
        .filter(|pg_chateau| pg_chateau.hull.is_some())
        .map(|pg_chateau| ChateauToSend {
            chateau: pg_chateau.chateau,
            realtime_feeds: pg_chateau.realtime_feeds.into_iter().flatten().collect(),
            schedule_feeds: pg_chateau.static_feeds.into_iter().flatten().collect(),
            hull: diesel_multi_polygon_to_geo(pg_chateau.hull.unwrap()),
        })
        .collect::<Vec<ChateauToSend>>();

    // conversion to `geojson` structs
    let features = formatted_chateaus
        .iter()
        .map(|chateau| {
            let value = geojson::Value::from(&chateau.hull);

            let mut properties: serde_json::map::Map<String, JsonValue> =
                serde_json::map::Map::new();

            properties.insert(
                String::from("chateau"),
                serde_json::Value::String(chateau.chateau.clone()),
            );
            properties.insert(
                String::from("realtime_feeds"),
                serde_json::Value::Array(
                    chateau
                        .realtime_feeds
                        .clone()
                        .into_iter()
                        .map(|x| serde_json::Value::String(x))
                        .collect(),
                ),
            );
            properties.insert(
                String::from("schedule_feeds"),
                serde_json::Value::Array(
                    chateau
                        .schedule_feeds
                        .clone()
                        .into_iter()
                        .map(|x| serde_json::Value::String(x))
                        .collect(),
                ),
            );

            geojson::Feature {
                bbox: None,
                geometry: Some(geojson::Geometry {
                    bbox: None,
                    value: value,
                    foreign_members: None,
                }),
                id: Some(geojson::feature::Id::String(chateau.chateau.clone())),
                properties: Some(properties),
                foreign_members: None,
            }
        })
        .collect::<Vec<Feature>>();

    // formation of final object
    let feature_collection = geojson::FeatureCollection {
        bbox: None,
        features: features,
        foreign_members: None,
    };

    // turn it into a string and send it!!!
    let serialized = GeoJson::from(feature_collection).to_string();

    //cache it first
    let mut chateau_lock = chateau_cache.write().unwrap();
    let mut chateau_mut_ref = chateau_lock.as_mut();

    chateau_mut_ref = Some(&mut ChateauCache {
        chateau_geojson: serialized.clone(),
        last_updated_time_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    });

    drop(chateau_lock);

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=86400"))
        .body(serialized)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "debug");
    // env_logger::init();

    // Connect to the database.
    let pool = Arc::new(make_async_pool().await.unwrap());
    let arc_pool = Arc::clone(&pool);

    let conn_pre = arc_pool.as_ref().get().await;
    let conn = &mut conn_pre.unwrap();

    let sqlx_pool: Arc<sqlx::Pool<sqlx::Postgres>> = Arc::new(
        PgPoolOptions::new()
            .max_connections(5)
            .connect(std::env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap(),
    );

    let (zk, default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

    let zk = Arc::new(zk);

    let global_cache = Arc::new(BirchGlobalDatastore {
        chateau_assignment_cache: Arc::new(SccHashMap::new()),
    });

    // Create a new HTTP server.
    let builder = HttpServer::new(move || {
        App::new()
            .wrap(
                DefaultHeaders::new()
                    .add(("Access-Control-Allow-Origin", "*"))
                    .add(("Server", "Catenary"))
                    .add((
                        "Access-Control-Allow-Origin",
                        "https://maps.catenarymaps.org",
                    )),
            )
            .wrap(actix_block_ai_crawling::BlockAi)
            .wrap(middleware::Compress::default())
            .app_data(actix_web::web::Data::new(Arc::clone(&sqlx_pool)))
            .app_data(actix_web::web::Data::new(Arc::clone(&pool)))
            .app_data(actix_web::web::Data::new(Arc::new(RwLock::new(
                None::<ChateauCache>,
            ))))
            .app_data(Data::new(Arc::clone(&global_cache)))
            .app_data(actix_web::web::Data::new(Arc::clone(&zk)))
            .route("/", web::get().to(index))
            .route("robots.txt", web::get().to(robots))
            .service(amtrakproxy)
            .service(microtime)
            .service(nanotime)
            .service(chateaus)
            .service(metrolinktrackproxy)
            .service(shapes_not_bus)
            .service(shapes_not_bus_meta)
            .service(shapes_bus)
            .service(shapes_bus_meta)
            .service(routesofchateau)
            .service(bus_stops_meta)
            .service(bus_stops)
            .service(rail_stops)
            .service(rail_stops_meta)
            .service(station_features)
            .service(station_features_meta)
            .service(other_stops)
            .service(other_stops_meta)
            .service(api_key_management::get_realtime_keys)
            .service(api_key_management::set_realtime_key)
            .service(aspenised_data_over_https::get_realtime_locations)
            .service(chicago_proxy::ttarrivals_proxy)
            .service(nearby_departures::nearby_from_coords)
            .service(get_vehicle_trip_information::get_trip_init)
            .service(get_vehicle_trip_information::get_trip_rt_update)
            .service(get_vehicle_trip_information::get_vehicle_information)
    })
    .workers(16);

    let _ = builder.bind("127.0.0.1:17419").unwrap().run().await;

    Ok(())
}
