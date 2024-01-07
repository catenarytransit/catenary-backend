use actix_web::http::StatusCode;
use actix_web::middleware::DefaultHeaders;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use service::quicli::prelude::{error, info, warn};
use sqlx::migrate::MigrateDatabase;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool, Postgres};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

mod database;

#[derive(thiserror::Error, Debug)]
pub enum CatenaryError {
    #[error("Could not connect to database pool: {0}")]
    DatabasePool(#[from] bb8::RunError<tokio_postgres::error::Error>),
    #[error("Could not connect to database: {0}")]
    Database(#[from] tokio_postgres::error::Error),
    #[error("Could not fetch Amtrak data: {0}")]
    AmtrakFetch(#[from] reqwest::Error),
    #[error("Could not decrypt Amtrak data: {0}")]
    AmtrakDecrypt(#[from] amtk::DecryptionError),
}

impl actix_web::ResponseError for CatenaryError {
    fn status_code(&self) -> StatusCode {
        match self {
            CatenaryError::DatabasePool(..) => StatusCode::INTERNAL_SERVER_ERROR,
            CatenaryError::Database(..) => StatusCode::INTERNAL_SERVER_ERROR,
            CatenaryError::AmtrakFetch(..) => StatusCode::INTERNAL_SERVER_ERROR,
            CatenaryError::AmtrakDecrypt(..) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(("Content-Type", "text/plain"))
            .body(self.to_string())
    }
}

#[derive(Serialize, Deserialize)]
struct StaticFeed {
    onestop_feed_id: String,
    max_lat: f64,
    max_lon: f64,
    min_lat: f64,
    min_lon: f64,
    operators: Vec<String>,
    operators_hashmap: HashMap<String, Option<String>>,
}

#[derive(Serialize, Deserialize)]
struct RealtimeFeedPostgres {
    onestop_feed_id: String,
    operators: Vec<String>,
    operators_to_gtfs_ids: HashMap<String, Option<String>>,
}

#[derive(Serialize, Deserialize)]
struct OperatorPostgres {
    onestop_operator_id: String,
    name: String,
    gtfs_static_feeds: Vec<String>,
    gtfs_realtime_feeds: Vec<String>,
    static_onestop_feeds_to_gtfs_ids: HashMap<String, Option<String>>,
    realtime_onestop_feeds_to_gtfs_ids: HashMap<String, Option<String>>,
}

async fn index() -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("Hello world!")
}

async fn robots() -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("User-agent: GPTBot\nDisallow: /\nUser-agent: Google-Extended\nDisallow: /")
}

#[derive(Serialize, Deserialize)]
struct RouteOutPostgres {
    onestop_feed_id: String,
    route_id: String,
    short_name: String,
    long_name: String,
    desc: Option<String>,
    route_type: i16,
    url: Option<String>,
    agency_id: Option<String>,
    gtfs_order: Option<i32>,
    color: String,
    text_color: String,
    continuous_pickup: i16,
    continuous_drop_off: i16,
    shapes_list: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct TripPostgres {
    trip_id: String,
    onestop_feed_id: String,
    route_id: String,
    service_id: String,
    trip_headsign: Option<String>,
    trip_short_name: Option<String>,
    direction_id: Option<i32>,
    block_id: Option<String>,
    shape_id: Option<String>,
    wheelchair_accessible: Option<i32>,
    bikes_allowed: Option<i32>,
}

#[actix_web::get("/microtime")]
pub async fn microtime() -> impl Responder {
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

#[actix_web::get("/amtrakproxy")]
pub async fn amtrak_proxy() -> Result<HttpResponse, CatenaryError> {
    let raw_data =
        reqwest::get("https://maps.amtrak.com/services/MapDataService/trains/getTrainsData")
            .await
            .map_err(CatenaryError::AmtrakFetch)?;

    let decrypted_string = amtk::decrypt(raw_data.text().await.unwrap().as_str())?;

    Ok(HttpResponse::Ok().json(decrypted_string))
}

#[derive(Serialize, Deserialize)]
struct GetTripQuery {
    feed_id: String,
    trip_id: String,
}

#[actix_web::get("/gettrip")]
pub async fn get_trip(
    pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>,
    web::Query(query): web::Query<GetTripQuery>,
) -> Result<HttpResponse, CatenaryError> {
    let client = pool.get().await?;

    let result: Vec<TripPostgres> = client
        .query(
            "SELECT trip_id, onestop_feed_id,
            route_id, service_id, trip_headsign, trip_short_name, direction_id,
            block_id, shape_id, wheelchair_accessible, bikes_allowed FROM gtfs.trips
            WHERE onestop_feed_id = $1 AND trip_id = $2;",
            &[&query.feed_id, &query.trip_id],
        )
        .await?
        .iter()
        .map(|row| TripPostgres {
            trip_id: row.get(0),
            onestop_feed_id: row.get(1),
            route_id: row.get(2),
            service_id: row.get(3),
            trip_headsign: row.get(4),
            trip_short_name: row.get(5),
            direction_id: row.get(6),
            block_id: row.get(7),
            shape_id: row.get(8),
            wheelchair_accessible: row.get(9),
            bikes_allowed: row.get(10),
        })
        .collect();

    Ok(HttpResponse::Ok().json(result))
}

#[derive(Serialize, Deserialize)]
struct InitialData {
    s: Vec<StaticFeed>,
    o: Vec<OperatorPostgres>,
    r: Vec<RealtimeFeedPostgres>,
}

//this endpoint is used to load the initial static feeds, realtime feeds, and operators
#[actix_web::get("/getinitdata")]
pub async fn get_init_data(
    pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>,
) -> Result<HttpResponse, CatenaryError> {
    let client = pool.get().await?;

    // (onestop_feed_id, max_lat, max_lon, min_lat, min_lon, operators, operators_to_gtfs_ids)
    let statics = client.query(
        "SELECT
        onestop_feed_id, max_lat, max_lon, min_lat, min_lon, operators, operators_to_gtfs_ids
        FROM gtfs.static_feeds;",
        &[],
    );

    let operators = client.query(
        "SELECT onestop_operator_id,
        name,
        gtfs_static_feeds,
        gtfs_realtime_feeds,
        static_onestop_feeds_to_gtfs_ids,
        realtime_onestop_feeds_to_gtfs_ids FROM gtfs.operators;",
        &[],
    );

    let realtime = client.query(
        "SELECT onestop_feed_id, operators, operators_to_gtfs_ids FROM gtfs.realtime_feeds;",
        &[],
    );

    let (statics_result, operators_result, realtime_result) =
        futures::try_join!(statics, operators, realtime).unwrap();

    let operators_result: Vec<OperatorPostgres> = operators_result
        .iter()
        .map(|row| OperatorPostgres {
            onestop_operator_id: row.get(0),
            name: row.get(1),
            gtfs_static_feeds: row.get(2),
            gtfs_realtime_feeds: row.get(3),
            static_onestop_feeds_to_gtfs_ids: row.get(4),
            realtime_onestop_feeds_to_gtfs_ids: row.get(5),
        })
        .collect();

    let statics_result: Vec<StaticFeed> = statics_result
        .iter()
        .map(|row| StaticFeed {
            onestop_feed_id: row.get(0),
            max_lat: row.get(1),
            max_lon: row.get(2),
            min_lat: row.get(3),
            min_lon: row.get(4),
            operators: row.get(5),
            operators_hashmap: row.get(6),
        })
        .collect();

    let realtime_result: Vec<RealtimeFeedPostgres> = realtime_result
        .iter()
        .map(|row| RealtimeFeedPostgres {
            onestop_feed_id: row.get(0),
            operators: row.get(1),
            operators_to_gtfs_ids: row.get(2),
        })
        .collect();

    Ok(HttpResponse::Ok().json(InitialData {
        s: statics_result,
        o: operators_result,
        r: realtime_result,
    }))
}

#[derive(Serialize, Deserialize)]
struct GetRoutesPerAgencyQuery {
    feed_id: String,
}

//given a static feed id, return all routes and the basic metadata with it
#[actix_web::get("/getroutesperagency")]
pub async fn get_routes_per_agency(
    pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>,
    web::Query(query): web::Query<GetRoutesPerAgencyQuery>,
) -> Result<HttpResponse, CatenaryError> {
    let client = pool.get().await?;

    let result: Vec<RouteOutPostgres> = client
        .query(
            "SELECT onestop_feed_id, route_id,
            short_name, long_name, gtfs_desc, route_type, url, agency_id,
            gtfs_order,
            color,
            text_color,
            continuous_pickup,
            continuous_drop_off,
            shapes_list FROM gtfs.routes WHERE onestop_feed_id = $1;",
            &[&query.feed_id],
        )
        .await?
        .iter()
        .map(|row| RouteOutPostgres {
            onestop_feed_id: row.get(0),
            route_id: row.get(1),
            short_name: row.get(2),
            long_name: row.get(3),
            desc: row.get(4),
            route_type: row.get(5),
            url: row.get(6),
            agency_id: row.get(7),
            gtfs_order: row.get(8),
            color: row.get(9),
            text_color: row.get(10),
            continuous_pickup: row.get(11),
            continuous_drop_off: row.get(12),
            shapes_list: row.get(13),
        })
        .collect();

    Ok(HttpResponse::Ok().json(result))
}

#[derive(Serialize, Deserialize)]
struct GtfsIngestError {
    onestop_feed_id: String,
    error: String,
}

#[actix_web::get("/gtfsingesterrors")]
pub async fn gtfs_ingest_errors(pool: web::Data<PgPool>) -> Result<HttpResponse, CatenaryError> {
    let client = pool.get().await?;

    let result: Vec<GtfsIngestError> =
        sqlx::query!("SELECT onestop_feed_id, error FROM gtfs.gtfs_errors")
            .fetch_all(&**pool)
            .await?
            .iter()
            .map(|row| GtfsIngestError {
                onestop_feed_id: row.get(0),
                error: row.get(1),
            })
            .collect();

    Ok(HttpResponse::Ok().json(result))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    info!(
        "Starting Catenary on {}",
        dotenvy::var("BIND_ADDR").unwrap()
    );

    database::check_for_migrations()
        .await
        .expect("An error occurred while running migrations.");

    let pool = database::connect()
        .await
        .expect("Database connection failed");

    HttpServer::new(move || {
        App::new()
            .wrap(
                DefaultHeaders::new()
                    .add(("Access-Control-Allow-Origin", "*"))
                    .add(("Server", "Catenary"))
                    .add((
                        "Access-Control-Allow-Origin",
                        "https://maps.catenarymaps.org",
                    ))
                    .add(("Access-Control-Allow-Origin", "https://catenarymaps.org")),
            )
            .wrap(actix_block_ai_crawling::BlockAi)
            .wrap(middleware::Compress::default())
            .app_data(web::Data::new(pool.clone()))
            .route("/", web::get().to(index))
            .service(get_routes_per_agency)
            .route("robots.txt", web::get().to(robots))
            .service(get_trip)
            .service(amtrak_proxy)
            .service(get_init_data)
            .service(gtfs_ingest_errors)
            .service(microtime)
    })
    .workers(16)
    .bind(dotenvy::var("BIND_ADDR").unwrap())?
    .run()
    .await
}
