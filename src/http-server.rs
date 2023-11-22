#![feature(future_join)]
use actix_web::dev::Service;
use actix_web::middleware::DefaultHeaders;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bb8::Pool;
use qstring::QString;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use serde_json::to_string;
use serde_json::{json, to_string_pretty};
use std::collections::HashMap;
use std::future::join;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
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

#[derive(serde::Serialize)]
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

#[derive(serde::Serialize)]
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

#[actix_web::get("/gettrip")]
pub async fn gettrip(
    pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>,
    req: HttpRequest,
) -> impl Responder {
    let mut client = pool.get().await;

    if client.is_ok() {
        let mut client = client.unwrap();
        let query_str = req.query_string(); // "name=ferret"
        let qs = QString::from(query_str);
        let req_feed_id = qs.get("feed_id").unwrap();
        let trip_id = qs.get("trip_id").unwrap();

        let postgresresult = client
            .query(
                "SELECT trip_id, onestop_feed_id, 
        route_id, service_id, trip_headsign, trip_short_name, direction_id, 
        block_id, shape_id, wheelchair_accessible, bikes_allowed FROM gtfs.trips
         WHERE onestop_feed_id = $1 AND trip_id = $2;",
                &[&req_feed_id, &trip_id],
            )
            .await;

        match postgresresult {
            Ok(postgresresult) => {
                let mut result: Vec<TripPostgres> = Vec::new();
                for row in postgresresult {
                    result.push(TripPostgres {
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
                    });
                }

                let json_string = to_string_pretty(&json!(result)).unwrap();

                HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .body(json_string)
            }
            Err(e) => {
                println!("{:?}", e);
                println!("No results from postgres");

                HttpResponse::InternalServerError()
                    .insert_header(("Content-Type", "text/plain"))
                    .body("Postgres Error")
            }
        }
    } else {
        HttpResponse::InternalServerError()
            .insert_header(("Content-Type", "text/plain"))
            .body("Couldn't connect to pool")
    }
}

//this endpoint is used to load the initial static feeds, realtime feeds, and operators
#[actix_web::get("/getinitdata")]
pub async fn getinitdata(
    pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>,
    req: HttpRequest,
) -> impl Responder {
    let mut client = pool.get().await;

    if client.is_ok() {
        let mut client = client.unwrap();

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

        let runqueries = join!(statics, operators, realtime).await;

        let operators_result: Vec<OperatorPostgres> = runqueries
            .1
            .unwrap()
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

        let statics_result: Vec<StaticFeed> = runqueries
            .0
            .unwrap()
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

        let realtime_result: Vec<RealtimeFeedPostgres> = runqueries
            .2
            .unwrap()
            .iter()
            .map(|row| RealtimeFeedPostgres {
                onestop_feed_id: row.get(0),
                operators: row.get(1),
                operators_to_gtfs_ids: row.get(2),
            })
            .collect();

        return HttpResponse::Ok()
            .insert_header(("Content-Type", "application/json"))
            .body(format!(
                "{{\"s\":{},\"o\":{},\"r\":{}}}",
                to_string(&statics_result).unwrap(),
                to_string(&operators_result).unwrap(),
                to_string(&realtime_result).unwrap()
            ));
    }

    HttpResponse::InternalServerError()
        .insert_header(("Content-Type", "application/json"))
        .body("{}")
}

//given a static feed id, return all routes and the basic metadata with it
#[actix_web::get("/getroutesperagency")]
pub async fn getroutesperagency(
    pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>,
    req: HttpRequest,
) -> impl Responder {
    let mut client = pool.get().await;

    if client.is_ok() {
        let mut client = client.unwrap();
        let query_str = req.query_string(); // "name=ferret"
        let qs = QString::from(query_str);
        let req_feed_id = qs.get("feed_id"); // "ferret"

        match req_feed_id {
            None => HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("No feed_id specified"),
            Some(req_feed_id) => {
                let postgresresult = client
                    .query(
                        "SELECT onestop_feed_id, route_id,
     short_name, long_name, gtfs_desc, route_type, url, agency_id,
     gtfs_order,
     color,
     text_color,
     continuous_pickup,
     continuous_drop_off,
     shapes_list FROM gtfs.routes WHERE onestop_feed_id = $1;",
                        &[&req_feed_id],
                    )
                    .await;

                match postgresresult {
                    Ok(postgresresult) => {
                        let mut result: Vec<RouteOutPostgres> = Vec::new();
                        for row in postgresresult {
                            result.push(RouteOutPostgres {
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
                            });
                        }

                        let json_string = to_string(&json!(result)).unwrap();

                        HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(json_string)
                    }
                    Err(e) => {
                        println!("No results from postgres");

                        HttpResponse::InternalServerError()
                            .insert_header(("Content-Type", "text/plain"))
                            .body("Postgres Error")
                    }
                }
            }
        }
    } else {
        HttpResponse::InternalServerError()
            .insert_header(("Content-Type", "text/plain"))
            .body("Couldn't connect to pool")
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let postgresstring = arguments::parse(std::env::args())
        .unwrap()
        .get::<String>("postgres");

    let postgresstring = match postgresstring {
        Some(s) => s,
        None => {
            println!("Postgres string not avaliable, using default");
            "host=localhost user=postgres".to_string()
        }
    };

    // Connect to the database.
    let manager: bb8_postgres::PostgresConnectionManager<NoTls> =
        bb8_postgres::PostgresConnectionManager::new(postgresstring.parse().unwrap(), NoTls);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    // Create a new HTTP server.
    let builder = HttpServer::new(move || {
        App::new()
            .wrap(
                DefaultHeaders::new()
                    .add(("Access-Control-Allow-Origin", "*"))
                    .add(("Server", "Catenary"))
                    .add((
                        "Access-Control-Allow-Origin",
                        "https://transitmap.kylerchin.com",
                    ))
                    .add((
                        "Access-Control-Allow-Origin",
                        "https://maps.catenarymaps.org",
                    ))
                    .add(("Access-Control-Allow-Origin", "https://catenarymaps.org")),
            )
            .wrap(actix_block_ai_crawling::BlockAi)
            .app_data(actix_web::web::Data::new(pool.clone()))
            .route("/", web::get().to(index))
            .service(getroutesperagency)
            .route("robots.txt", web::get().to(robots))
            .service(gettrip)
            .service(amtrakproxy)
            .service(getinitdata)
            .service(microtime)
    })
    .workers(4);

    // Bind the server to port 8080.
    let _ = builder.bind("127.0.0.1:5401").unwrap().run().await;

    Ok(())
}
