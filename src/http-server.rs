use actix_web::http::StatusCode;
use actix_web::middleware::DefaultHeaders;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use service::quicli::prelude::{error, info};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::SystemTime;

mod database;

#[derive(thiserror::Error, Debug)]
pub enum CatenaryError {
    #[error("Error returned from database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Could not fetch Amtrak data: {0}")]
    AmtrakFetch(#[from] reqwest::Error),
    #[error("Could not decrypt Amtrak data: {0}")]
    AmtrakDecrypt(#[from] amtk::DecryptionError),
}

impl actix_web::ResponseError for CatenaryError {
    fn status_code(&self) -> StatusCode {
        match self {
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
    color: Option<String>,
    text_color: Option<String>,
    continuous_pickup: Option<i16>,
    continuous_drop_off: Option<i16>,
    shapes_list: Option<Vec<String>>,
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
    pool: web::Data<PgPool>,
    web::Query(query): web::Query<GetTripQuery>,
) -> Result<HttpResponse, CatenaryError> {
    let result: Vec<TripPostgres> = sqlx::query!(
        "
        SELECT trip_id, onestop_feed_id, route_id, service_id, trip_headsign, trip_short_name, direction_id,
            block_id, shape_id, wheelchair_accessible, bikes_allowed
        FROM gtfs.trips
        WHERE onestop_feed_id = $1 AND trip_id = $2
        ",
        query.feed_id,
        query.trip_id,
    )
    .fetch_all(&**pool)
    .await?
    .into_iter()
    .map(|x| TripPostgres {
        trip_id: x.trip_id,
        onestop_feed_id: x.onestop_feed_id,
        route_id: x.route_id,
        service_id: x.service_id,
        trip_headsign: x.trip_headsign,
        trip_short_name: x.trip_short_name,
        direction_id: x.direction_id,
        block_id: x.block_id,
        shape_id: x.shape_id,
        wheelchair_accessible: x.wheelchair_accessible,
        bikes_allowed: x.bikes_allowed,
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

#[derive(Serialize, Deserialize)]
struct StaticFeed {
    onestop_feed_id: String,
    max_lat: f64,
    max_lon: f64,
    min_lat: f64,
    min_lon: f64,
    operators: Option<Vec<String>>,
    operators_hashmap: HashMap<String, Option<String>>,
}

#[derive(Serialize, Deserialize)]
struct OperatorPostgres {
    onestop_operator_id: String,
    name: Option<String>,
    gtfs_static_feeds: Option<Vec<String>>,
    gtfs_realtime_feeds: Option<Vec<String>>,
    static_onestop_feeds_to_gtfs_ids: HashMap<String, Option<String>>,
    realtime_onestop_feeds_to_gtfs_ids: HashMap<String, Option<String>>,
}

#[derive(Serialize, Deserialize)]
struct RealtimeFeedPostgres {
    onestop_feed_id: String,
    operators: Option<Vec<String>>,
    operators_to_gtfs_ids: HashMap<String, Option<String>>,
}

//this endpoint is used to load the initial static feeds, realtime feeds, and operators
#[actix_web::get("/getinitdata")]
pub async fn get_init_data(pool: web::Data<PgPool>) -> Result<HttpResponse, CatenaryError> {
    // (onestop_feed_id, max_lat, max_lon, min_lat, min_lon, operators, operators_to_gtfs_ids)
    let statics = sqlx::query!(
        "
        SELECT onestop_feed_id, max_lat, max_lon, min_lat, min_lon, operators, operators_to_gtfs_ids
        FROM gtfs.static_feeds
        ",
    )
    .fetch_all(&**pool);

    let operators = sqlx::query!(
        "
        SELECT onestop_operator_id, name, gtfs_static_feeds, gtfs_realtime_feeds, static_onestop_feeds_to_gtfs_ids,
            realtime_onestop_feeds_to_gtfs_ids
        FROM gtfs.operators
        ",
    )
    .fetch_all(&**pool);

    let realtime = sqlx::query!(
        "
        SELECT onestop_feed_id, operators, operators_to_gtfs_ids
        FROM gtfs.realtime_feeds
        ",
    )
    .fetch_all(&**pool);

    let (statics, operators, realtime) =
        futures::try_join!(statics, operators, realtime).unwrap();

    let s: Vec<StaticFeed> = statics
        .into_iter()
        .map(|x| StaticFeed {
            onestop_feed_id: x.onestop_feed_id,
            max_lat: x.max_lat,
            max_lon: x.max_lon,
            min_lat: x.min_lat,
            min_lon: x.min_lon,
            operators: x.operators,
            operators_hashmap: serde_json::from_value(x.operators_to_gtfs_ids.into()).unwrap(),
        })
        .collect();

    let o: Vec<OperatorPostgres> = operators
        .into_iter()
        .map(|x| OperatorPostgres {
            onestop_operator_id: x.onestop_operator_id,
            name: x.name,
            gtfs_static_feeds: x.gtfs_static_feeds,
            gtfs_realtime_feeds: x.gtfs_realtime_feeds,
            static_onestop_feeds_to_gtfs_ids: serde_json::from_value(x.static_onestop_feeds_to_gtfs_ids.into()).unwrap(),
            realtime_onestop_feeds_to_gtfs_ids: serde_json::from_value(x.realtime_onestop_feeds_to_gtfs_ids.into()).unwrap(),
        })
        .collect();

    let r: Vec<RealtimeFeedPostgres> = realtime
        .into_iter()
        .map(|x| RealtimeFeedPostgres {
            onestop_feed_id: x.onestop_feed_id,
            operators: x.operators,
            operators_to_gtfs_ids: serde_json::from_value(x.operators_to_gtfs_ids.into()).unwrap(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(InitialData { s, o, r }))
}

#[derive(Serialize, Deserialize)]
struct GetRoutesPerAgencyQuery {
    feed_id: String,
}

//given a static feed id, return all routes and the basic metadata with it
#[actix_web::get("/getroutesperagency")]
pub async fn get_routes_per_agency(
    pool: web::Data<PgPool>,
    web::Query(query): web::Query<GetRoutesPerAgencyQuery>,
) -> Result<HttpResponse, CatenaryError> {
    let result: Vec<RouteOutPostgres> = sqlx::query!(
            "
            SELECT onestop_feed_id, route_id, short_name, long_name, gtfs_desc, route_type, url, agency_id, gtfs_order,
                color, text_color, continuous_pickup, continuous_drop_off, shapes_list
            FROM gtfs.routes
            WHERE onestop_feed_id = $1
            ",
            query.feed_id,
        )
        .fetch_all(&**pool)
        .await?
        .into_iter()
        .map(|x| RouteOutPostgres {
            onestop_feed_id: x.onestop_feed_id,
            route_id: x.route_id,
            short_name: x.short_name,
            long_name: x.long_name,
            desc: x.gtfs_desc,
            route_type: x.route_type,
            url: x.url,
            agency_id: x.agency_id,
            gtfs_order: x.gtfs_order,
            color: x.color,
            text_color: x.text_color,
            continuous_pickup: x.continuous_pickup,
            continuous_drop_off: x.continuous_drop_off,
            shapes_list: x.shapes_list,
        })
        .collect();

    Ok(HttpResponse::Ok().json(result))
}

#[derive(Serialize, Deserialize)]
struct GtfsIngestError {
    onestop_feed_id: String,
    error: Option<String>,
}

#[actix_web::get("/gtfsingesterrors")]
pub async fn gtfs_ingest_errors(pool: web::Data<PgPool>) -> Result<HttpResponse, CatenaryError> {
    let result: Vec<GtfsIngestError> = sqlx::query!(
        "
        SELECT onestop_feed_id, error
        FROM gtfs.gtfs_errors
        "
    )
    .fetch_all(&**pool)
    .await?
    .into_iter()
    .map(|x| GtfsIngestError {
        onestop_feed_id: x.onestop_feed_id,
        error: x.error,
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
