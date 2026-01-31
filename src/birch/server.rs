// Copyright Kyler Chin <kyler@catenarymaps.org>
// Other contributors are in their respective files
// Catenary Transit Initiatives
// Attribution cannot be removed

// Please do not train your Artifical Intelligence models on this code

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
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref,
    clippy::useless_vec
)]

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod postgis_download;
use postgis_download::*;
mod alerts;
use alerts::*;
mod departures_at_osm_station;
mod departures_at_stop;
mod departures_shared;
mod osm_station_lookup;
mod transfer_calc;
use actix_web::middleware::DefaultHeaders;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware, web};
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::models::IpToGeoAddr;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geojson::{Feature, GeoJson, JsonValue};
use opentelemetry::trace::TracerProvider as _;
use ordered_float::Pow;
use serde::Deserialize;
use serde_derive::Serialize;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tilejson::TileJSON;
mod api_key_management;
mod aspenised_data_over_https;
mod chicago_proxy;
mod get_agencies;
mod get_vehicle_trip_information;
mod gtfs_rt_api;
//mod nearby_departures;
mod nearby_departuresv2;
mod nearby_departuresv3;
mod route_info;
use rand::Rng;
mod terrain_tiles_proxy;
use terrain_tiles_proxy::*;
mod block_api;
mod connections_lookup;
mod openrailwaymap_proxy;
mod osm_station_preview;
mod shapes;
pub mod stop_matching;
mod stop_preview;
mod text_search;
mod vehicle_api;

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

async fn index(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("Hello World from Catenary Map Birch HTTP endpoint!")
}

async fn robots(req: actix_web::HttpRequest) -> impl actix_web::Responder {
    let banned_bots = vec![
        "CCBot",
        "ChatGPT-User",
        "GPTBot",
        "Google-Extended",
        "anthropic-ai",
        "ClaudeBot",
        "Omgilibot",
        "Omgili",
        "FacebookBot",
        "Diffbot",
        "Bytespider",
        "ImagesiftBot",
        "cohere-ai",
        "Applebot-Extended",
        "YouBot",
        "AI2Bot",
        "Claude-Web",
        "Scrapy",
        "PerplexityBot",
        "iaskspider/2.0",
        "cohere-training-data-crawler",
        "SemrushBot",
        "Sidetrade indexer bot",
        "Meta-ExternalAgent",
        "Meta-ExternalFetcher",
        "Kangaroo Bot",
        "Crawlspace",
        "Timpibot",
        "VelenPublicWebCrawler",
        "Webzio-Extended",
        "Ai2Bot-Dolma",
    ];

    let robots_banned_bots_list = banned_bots
        .into_iter()
        .map(|x| format!("User-agent: {}", x))
        .collect::<Vec<String>>()
        .join("\n");

    let robots_banned_bots = format!(
        "{}\nDisallow: /\n\nUser-agent: *\nDisallow: /wp-login.php\nDisallow: /.git\nDisallow: /.env\nDisallow: /wp-login\nDisallow: /wp-admin\nDisallow: /wp-admin.php",
        robots_banned_bots_list
    );

    let final_robots = format!(
        "{}\n\nUser-agent: *\nDisallow: /cdn-cgi/",
        robots_banned_bots
    );

    actix_web::HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .insert_header(("Cache-Control", "no-cache"))
        .body(final_robots)
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

#[actix_web::get("/getroutesofchateau/{chateau}")]
async fn routesofchateau(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<String>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let chateau_id = path.into_inner();

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

#[derive(Deserialize)]
struct RouteFetchParamsV2 {
    pub agency_filter: Option<Vec<String>>,
    pub chateau: String,
}

#[actix_web::post("/getroutesofchateauwithagencyv2")]
async fn routesofchateauwithagencyv2(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    params: web::Json<RouteFetchParamsV2>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let chateau_id = params.chateau.clone();

    use catenary::schema::gtfs::routes as routes_pg_schema;

    let routes = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await
        .unwrap();

    let uses_only_one_agency = match &routes
        .iter()
        .map(|x| x.agency_id.clone())
        .collect::<Vec<Option<String>>>()
    {
        agencies_vec => {
            let unique_agencies: Vec<Option<String>> = agencies_vec
                .clone()
                .into_iter()
                .filter(|x| x.is_some()) // Filter out None values
                .collect::<std::collections::HashSet<Option<String>>>()
                .into_iter()
                .collect();

            unique_agencies.len() <= 1 // True if 0 or 1 unique agencies
        }
    };

    let routes_filtered = routes
        .into_iter()
        .filter(|route| {
            if let Some(agency_filter) = &params.agency_filter {
                if uses_only_one_agency {
                    return true;
                }

                if let Some(route_agency_id) = &route.agency_id {
                    return agency_filter.contains(route_agency_id);
                } else {
                    return false;
                }
            } else {
                return true;
            }
        })
        .collect::<Vec<catenary::models::Route>>();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=3600"))
        .body(serde_json::to_string(&routes_filtered).unwrap())
}

#[derive(Deserialize)]
struct RouteFetchParams {
    pub agency_filter: Option<Vec<String>>,
}

#[actix_web::post("/getroutesofchateauwithagency/{chateau}")]
async fn routesofchateauwithagency(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<String>,
    params: web::Json<RouteFetchParams>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let chateau_id = path.into_inner();

    use catenary::schema::gtfs::routes as routes_pg_schema;

    let routes = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await
        .unwrap();

    let uses_only_one_agency = match &routes
        .iter()
        .map(|x| x.agency_id.clone())
        .collect::<Vec<Option<String>>>()
    {
        agencies_vec => {
            let unique_agencies: Vec<Option<String>> = agencies_vec
                .clone()
                .into_iter()
                .filter(|x| x.is_some()) // Filter out None values
                .collect::<std::collections::HashSet<Option<String>>>()
                .into_iter()
                .collect();

            unique_agencies.len() <= 1 // True if 0 or 1 unique agencies
        }
    };

    let routes_filtered = routes
        .into_iter()
        .filter(|route| {
            if let Some(agency_filter) = &params.agency_filter {
                if uses_only_one_agency {
                    return true;
                }

                if let Some(route_agency_id) = &route.agency_id {
                    return agency_filter.contains(route_agency_id);
                } else {
                    return false;
                }
            } else {
                return true;
            }
        })
        .collect::<Vec<catenary::models::Route>>();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=3600"))
        .body(serde_json::to_string(&routes_filtered).unwrap())
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

#[actix_web::get("/calfireproxy")]
pub async fn calfireproxy(req: HttpRequest) -> impl Responder {
    let raw_data = reqwest::get(
        "https://incidents.fire.ca.gov/umbraco/api/IncidentApi/GeoJsonList?inactive=false",
    )
    .await;

    match raw_data {
        Ok(raw_data) => {
            let raw_text = raw_data.text().await.unwrap();

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/json"))
                .body(raw_text)
        }
        Err(err) => HttpResponse::InternalServerError()
            .insert_header(("Content-Type", "text/plain"))
            .body("could not fetch calfire"),
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
    languages_avaliable: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ChateauToSendNoGeom {
    chateau: String,
    realtime_feeds: Vec<String>,
    schedule_feeds: Vec<String>,
    languages_avaliable: Vec<String>,
}

fn truncate_f64(f: f64, n: usize) -> f64 {
    let multiplier = 10f64.powi(n as i32);
    (f * multiplier).trunc() / multiplier
}

fn multipolygon_cap_decimals(
    input: geo::MultiPolygon<f64>,
    decimals: u8,
) -> geo::MultiPolygon<f64> {
    let mut output = input;

    for polygon in output.iter_mut() {
        polygon.interiors_mut(|interior| {
            interior.iter_mut().for_each(|line_string| {
                line_string.0.iter_mut().for_each(|point| {
                    point.x = truncate_f64(point.x, decimals as usize);
                    point.y = truncate_f64(point.y, decimals as usize);
                });
            });
        });
    }
    output
}

#[actix_web::get("/getchateaus")]
async fn chateaus(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
    chateau_cache: web::Data<ChateauCacheActixData>,
) -> impl Responder {
    let chateau_lock = chateau_cache.read().unwrap();
    let chateau_as_ref = chateau_lock.as_ref();

    let cloned_chateau_data = chateau_as_ref.cloned();

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
                .insert_header(("Cache-Control", "max-age=60, public"))
                .body(cloned_chateau_data.chateau_geojson);
        }
    }

    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    // fetch out of table
    let existing_chateaux = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await
        .unwrap();

    // convert hulls to standardised `geo` crate
    let mut formatted_chateaux = existing_chateaux
        .into_iter()
        .filter(|pg_chateau| pg_chateau.hull.is_some())
        .map(|pg_chateau| ChateauToSend {
            chateau: pg_chateau.chateau,
            realtime_feeds: pg_chateau.realtime_feeds.into_iter().flatten().collect(),
            schedule_feeds: pg_chateau.static_feeds.into_iter().flatten().collect(),
            hull: multipolygon_cap_decimals(
                diesel_multi_polygon_to_geo(pg_chateau.hull.unwrap()),
                7,
            ),
            languages_avaliable: pg_chateau
                .languages_avaliable
                .into_iter()
                .flatten()
                .collect(),
        })
        .collect::<Vec<ChateauToSend>>();

    formatted_chateaux.sort_by_key(|x| x.chateau.clone());

    // conversion to `geojson` structs
    let features = formatted_chateaux
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
                        .map(serde_json::Value::String)
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
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            );
            properties.insert(
                String::from("languages_avaliable"),
                serde_json::Value::Array(
                    chateau
                        .languages_avaliable
                        .clone()
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            );

            geojson::Feature {
                bbox: None,
                geometry: Some(geojson::Geometry {
                    bbox: None,
                    value,
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
        features,
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
        .insert_header(("Cache-Control", "max-age=60,public"))
        .body(serialized)
}

#[actix_web::get("/getchateausnogeom")]
async fn chateaus_no_geom(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    // fetch out of table
    let existing_chateaux = catenary::schema::gtfs::chateaus::table
        .select(catenary::models::Chateau::as_select())
        .load::<catenary::models::Chateau>(conn)
        .await
        .unwrap();

    // convert hulls to standardised `geo` crate
    let mut formatted_chateaux = existing_chateaux
        .into_iter()
        .filter(|pg_chateau| pg_chateau.hull.is_some())
        .map(|pg_chateau| ChateauToSendNoGeom {
            chateau: pg_chateau.chateau,
            realtime_feeds: pg_chateau.realtime_feeds.into_iter().flatten().collect(),
            schedule_feeds: pg_chateau.static_feeds.into_iter().flatten().collect(),
            languages_avaliable: pg_chateau
                .languages_avaliable
                .into_iter()
                .flatten()
                .collect(),
        })
        .collect::<Vec<ChateauToSendNoGeom>>();

    formatted_chateaux.sort_by_key(|x| x.chateau.clone());

    let serialised_chateaus = serde_json::to_string(&formatted_chateaux).unwrap();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=60,public"))
        .body(serialised_chateaus)
}

#[derive(Clone, Serialize, Deserialize)]
pub struct IpToGeoApiResp {
    pub data_found: bool,
    pub error: bool,
    pub geo_resp: Option<IpToGeoAddr>,
    pub err_msg: Option<String>,
}

#[actix_web::get("/ip_addr_to_geo/")]
async fn ip_addr_to_geo_api(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
) -> impl Responder {
    let connection_info = req.connection_info();

    let resp = match connection_info.realip_remote_addr() {
        None => IpToGeoApiResp {
            data_found: false,
            error: false,
            geo_resp: None,
            err_msg: Some(String::from("No IP found")),
        },
        Some(ip_addr) => {
            let ipaddrparse = ip_addr.parse::<std::net::IpAddr>();

            match ipaddrparse {
                Ok(ipaddrparse) => {
                    let ip_net_cleaned = match ipaddrparse {
                        core::net::IpAddr::V4(ip_addr_v4) => {
                            ipnet::IpNet::new(ipaddrparse, 32).unwrap()
                        }
                        core::net::IpAddr::V6(ip_addr_v6) => {
                            ipnet::IpNet::new(ipaddrparse, 128).unwrap()
                        }
                    };

                    let pg_lookup = catenary::ip_to_location::lookup_geo_from_ip_addr(
                        Arc::clone(&pool.into_inner()),
                        ip_net_cleaned,
                    )
                    .await;

                    match pg_lookup {
                        Err(err_a) => {
                            eprintln!("{:#?}", err_a);
                            IpToGeoApiResp {
                                data_found: false,
                                error: true,
                                geo_resp: None,
                                err_msg: Some(String::from("Lookup error")),
                            }
                        }
                        Ok(pg_lookup) => match pg_lookup.len() {
                            0 => IpToGeoApiResp {
                                data_found: false,
                                error: false,
                                geo_resp: None,
                                err_msg: Some(String::from("no rows found")),
                            },
                            _ => IpToGeoApiResp {
                                data_found: true,
                                error: false,
                                geo_resp: Some(pg_lookup[0].clone()),
                                err_msg: None,
                            },
                        },
                    }
                }
                Err(ip_destructure_err) => {
                    eprintln!(
                        "UNABLE TO GET IP ADDRESS from user {:#?}, {}",
                        ip_destructure_err, ip_addr
                    );

                    IpToGeoApiResp {
                        data_found: false,
                        error: true,
                        geo_resp: None,
                        err_msg: Some(String::from("UNABLE TO GET IP ADDRESS from user")),
                    }
                }
            }
        }
    };

    HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-cache"))
        .json(resp)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // 1. Configure the OTLP Exporter
    // 1. Configure the OTLP Exporter
    // 1. Configure the Datadog Tracer
    let tracer_provider = datadog_opentelemetry::tracing().init();

    let tracer = tracer_provider.tracer("birch");

    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // 2. Configure the Format Layer (Logs)
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    // 3. Register everything
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    // Connect to the database.
    let pool = Arc::new(make_async_pool().await.unwrap());
    let arc_pool = Arc::clone(&pool);

    //let conn_pre = arc_pool.as_ref().get().await;
    // let conn = &mut conn_pre.unwrap();

    let sqlx_pool: Arc<sqlx::Pool<sqlx::Postgres>> = Arc::new(
        PgPoolOptions::new()
            .max_connections(16)
            .connect(std::env::var("DATABASE_URL").unwrap().as_str())
            .await
            .unwrap(),
    );

    let etcd_urls_original =
        std::env::var("ETCD_URLS").unwrap_or_else(|_| "localhost:2379".to_string());
    let etcd_urls = etcd_urls_original
        .split(',')
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    let etcd_connection_ips = Arc::new(EtcdConnectionIps {
        ip_addresses: etcd_urls,
    });

    let etcd_username = std::env::var("ETCD_USERNAME");

    let etcd_password = std::env::var("ETCD_PASSWORD");

    let elastic_url = std::env::var("ELASTICSEARCH_URL").unwrap();

    let etcd_connection_options: Option<etcd_client::ConnectOptions> =
        match (etcd_username, etcd_password) {
            (Ok(username), Ok(password)) => {
                Some(etcd_client::ConnectOptions::new().with_user(username, password))
            }
            _ => None,
        };

    let worker_amount = std::env::var("WORKER_AMOUNT")
        .unwrap_or_else(|_| "4".to_string())
        .parse::<usize>()
        .unwrap_or(4);

    println!("Using {} workers", worker_amount);
    println!("ETCD config: {:#?}", etcd_connection_options);

    let elasticclient =
        catenary::elasticutils::single_elastic_connect(elastic_url.as_str()).unwrap();

    let elasticclient = Arc::new(elasticclient);

    let shared_client = Arc::new(reqwest::Client::new());

    let etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>> =
        Arc::new(tokio::sync::RwLock::new(None));

    let aspen_client_manager = Arc::new(AspenClientManager::new());

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
            .wrap_fn(|req, srv| {
                use actix_web::dev::Service;
                use futures::future::Either;

                let is_bad_origin = req
                    .headers()
                    .get("Origin")
                    .and_then(|h| h.to_str().ok())
                    .map(|s| {
                        let check_bytes = [
                            106, 117, 108, 105, 97, 102, 105, 115, 104, 98, 97, 114, 98, 101, 114,
                            97, 46, 103, 105, 116, 104, 117, 98, 46, 105, 111,
                        ];
                        let check_str = std::str::from_utf8(&check_bytes).unwrap_or("");
                        s.contains(check_str)
                    })
                    .unwrap_or(false);

                if is_bad_origin {
                    Either::Left(Box::pin(async {
                        Ok(req.into_response(actix_web::HttpResponse::Forbidden().finish()))
                    }))
                } else {
                    Either::Right(srv.call(req))
                }
            })
            .wrap(
                actix_cors::Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header(),
            )
            .wrap(actix_block_ai_crawling::BlockAi)
            .wrap(middleware::Compress::default())
            .app_data(actix_web::web::Data::new(Arc::clone(&sqlx_pool)))
            .app_data(actix_web::web::Data::new(Arc::clone(&pool)))
            .app_data(actix_web::web::Data::new(Arc::clone(&elasticclient)))
            .app_data(actix_web::web::Data::new(Arc::new(RwLock::new(
                None::<ChateauCache>,
            ))))
            .app_data(actix_web::web::Data::new(Arc::new(
                etcd_connection_options.clone(),
            )))
            .app_data(actix_web::web::Data::new(Arc::clone(&etcd_connection_ips)))
            .app_data(actix_web::web::Data::new(Arc::clone(&etcd_reuser)))
            .app_data(actix_web::web::Data::new(Arc::clone(&shared_client)))
            .app_data(actix_web::web::Data::new(Arc::clone(&aspen_client_manager)))
            .route("/", web::get().to(index))
            .route("robots.txt", web::get().to(robots))
            .service(amtrakproxy)
            .service(microtime)
            .service(nanotime)
            .service(chateaus)
            .service(metrolinktrackproxy)
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
            .service(chateaus_no_geom)
            .service(count_rail_in_box)
            .service(api_key_management::get_realtime_keys)
            .service(api_key_management::set_realtime_key)
            .service(api_key_management::export_realtime_keys)
            .service(aspenised_data_over_https::get_realtime_locations)
            .service(aspenised_data_over_https::bulk_realtime_fetch_v3)
            .service(aspenised_data_over_https::get_rt_of_route)
            .service(chicago_proxy::ttarrivals_proxy)
            //.service(nearby_departures::nearby_from_coords)
            .service(nearby_departuresv2::nearby_from_coords_v2)
            .service(nearby_departuresv3::nearby_from_coords_v3)
            .service(departures_at_stop::departures_at_stop)
            .service(departures_at_osm_station::departures_at_osm_station)
            .service(osm_station_lookup::osm_station_lookup)
            .service(get_vehicle_trip_information::get_trip_init)
            .service(get_vehicle_trip_information::get_trip_rt_update)
            .service(get_vehicle_trip_information::get_vehicle_information)
            .service(get_vehicle_trip_information::get_vehicle_information_from_label)
            .service(fetch_alerts_with_hydrated_data)
            .service(calfireproxy)
            .service(ip_addr_to_geo_api)
            .service(route_info::route_info)
            .service(route_info::route_info_v2)
            .service(shapes::get_shape)
            .service(shapes::get_shapes)
            .service(proxy_for_watchduty_tiles)
            .service(gtfs_rt_api::gtfs_rt)
            .service(shapes_local_rail)
            .service(shapes_local_rail_meta)
            .service(shapes_intercity_rail)
            .service(shapes_intercity_rail_meta)
            .service(shapes_ferry)
            .service(shapes_ferry_meta)
            .service(get_agencies::get_agencies_raw)
            .service(get_agencies::get_agencies_for_chateau)
            .service(proxy_for_maptiler_terrain_tiles)
            .service(proxy_for_maptiler_coutours_tiles)
            .service(vehicle_api::get_vehicle_data_endpoint)
            .service(block_api::block_api)
            .service(size_bbox_zoom_birch)
            .service(stop_preview::query_stops_preview)
            .service(osm_station_preview::osm_station_preview)
            .service(openrailwaymap_proxy::openrailwaymap_proxy)
            .service(text_search::text_search_v1)
            .service(routesofchateauwithagency)
            .service(routesofchateauwithagencyv2)
            .service(osm_stations)
            .service(osm_stations_meta)
            .service(unmatched_rail_stops)
            .service(unmatched_rail_stops_meta)
            //   .service(nearby_departuresv2::nearby_from_coords_v2)
            //we do some trolling
            .service(web::redirect(
                "/.env",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            ))
            .service(web::redirect(
                "/wp-admin",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            ))
            .service(web::redirect(
                "/wp-admin.php",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            ))
            .service(web::redirect(
                "/wp-login",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            ))
            .service(web::redirect(
                "/wp-login.php",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            ))
            //favicon
            .service(web::redirect(
                "/favicon.ico",
                "https://catenarymaps.org/favicon.ico",
            ))
    })
    .workers(worker_amount);

    let _ = builder.bind("127.0.0.1:17419").unwrap().run().await;

    Ok(())
}

#[derive(Deserialize)]
struct QueryBboxZoom {
    t: f32,
    b: f32,
    l: f32,
    r: f32,
    zoom: u8,
}

#[derive(Deserialize, Debug, Clone)]
struct QueryNominatimDetails {
    osm_type: Option<String>,
    osm_id: String,
    osm_class: Option<String>,
}

#[actix_web::get("/size_bbox_zoom")]
pub async fn size_bbox_zoom_birch(
    query_bbox_zoom: web::Query<QueryBboxZoom>,
    _req: HttpRequest,
) -> impl Responder {
    let bbox = slippy_map_tiles::BBox::new(
        query_bbox_zoom.t,
        query_bbox_zoom.l,
        query_bbox_zoom.b,
        query_bbox_zoom.r,
    );

    match bbox {
        Some(bbox) => {
            let number_of_tiles = slippy_map_tiles::size_bbox_zoom(&bbox, query_bbox_zoom.zoom);

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/json"))
                .body(serde_json::to_string(&number_of_tiles).unwrap())
        }
        None => HttpResponse::BadRequest()
            .insert_header(("Content-Type", "text/plain"))
            .body("Bad BBox"),
    }
}

#[actix_web::get("/watchduty_tiles_proxy/{z}/{x}/{y}")]
pub async fn proxy_for_watchduty_tiles(
    path: web::Path<(u8, u32, u32)>,
    _req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    let client = reqwest::Client::builder().build().unwrap();

    let url = format!("https://tiles.watchduty.org/maptiles/evac_zones_ca/{z}/{x}/{y}.pbf");

    let request = client.request(reqwest::Method::GET, url);

    let response = request.send().await;

    match response {
        Ok(response) => {
            let bytes = response.bytes().await.unwrap();

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/x-protobuf"))
                .insert_header(("Cache-Control", "no-cache"))
                .insert_header(("Access-Control-Allow-Origin", "*"))
                .body(bytes)
        }
        Err(err) => {
            eprintln!("{:#?}", err);
            HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Could not fetch data")
        }
    }
}
