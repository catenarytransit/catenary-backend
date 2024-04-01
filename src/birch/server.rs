use actix_web::dev::Service;
use actix_web::middleware::DefaultHeaders;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, get, Responder};
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use serde::Deserialize;
use std::sync::Arc;
use bb8::Pool;
use qstring::QString;
use serde_json::to_string;
use serde_json::{json, to_string_pretty};
use std::collections::HashMap;
use diesel_async::RunQueryDsl;
use std::time::UNIX_EPOCH;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use std::time::{Duration, SystemTime};
use tokio_postgres::types::private::BytesMut;
use serde_derive::Serialize;
use tokio_postgres::types::ToSql;
use diesel::query_dsl::select_dsl::SelectDsl;
use tokio_postgres::Client;
use diesel::SelectableHelper;
use tokio_postgres::{Error as PostgresError, Row};

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

#[actix_web::get("/metrolinktrackproxy")]
pub async fn metrolinktrackproxy(req: HttpRequest) -> impl Responder {
    let raw_data = reqwest::get("https://metrolinktrains.com/rtt/StationScheduleList.json").await;

    match raw_data {
        Ok(raw_data) => {

            let raw_text = raw_data.text().await;

            match raw_text {
                Ok(raw_text) => {
                    HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .body(raw_text)
                },
                Err(error) => HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Could not fetch Metrolink data")
            }
        },
        Err(error) => HttpResponse::InternalServerError()
            .insert_header(("Content-Type", "text/plain"))
            .body("Could not fetch Metrolink data")
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChateauToSend {
    chateau: String,
    hull: geo::MultiPolygon
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChateauList {
    chateaus: Vec<ChateauToSend>
}

#[actix_web::get("/getchateaus")]
async fn chateaus(pool: web::Data<Arc<CatenaryPostgresPool>>, req: HttpRequest) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let existing_chateaus = catenary::schema::gtfs::chateaus::table
    .select(catenary::models::Chateau::as_select())
    .load::<catenary::models::Chateau>(conn)
    .await.unwrap();

    let formatted_chateaus = existing_chateaus.into_iter()
    .filter(|pg_chateau|
        pg_chateau.hull.is_some()
    )
    .map(|pg_chateau| 
        ChateauToSend {
            chateau: pg_chateau.chateau,
            hull: diesel_multi_polygon_to_geo(pg_chateau.hull.unwrap())
        }
    ).collect::<Vec<ChateauToSend>>();

    let struct_to_send =  ChateauList {
        chateaus: formatted_chateaus
    };

    let response = serde_json::to_string(&struct_to_send).unwrap();

    HttpResponse::Ok()
    .insert_header(("Content-Type", "application/json"))
    .body(response)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Connect to the database.
    let pool = Arc::new(make_async_pool().await.unwrap());
    let arc_pool = Arc::clone(&pool);

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
            .app_data(actix_web::web::Data::new(Arc::clone(&pool)))
            .route("/", web::get().to(index))
            .route("robots.txt", web::get().to(robots))
            .service(amtrakproxy)
            .service(microtime)
            .service(nanotime)
            .service(chateaus)
            .service(metrolinktrackproxy)
    })
    .workers(16);

    let _ = builder.bind("127.0.0.1:17419").unwrap().run().await;

    Ok(())
} 