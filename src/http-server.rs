use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use serde_json::{json, to_string_pretty};
use tokio_postgres::types::private::BytesMut;
use actix_web::middleware::DefaultHeaders;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tokio_postgres::{Error as PostgresError, Row};
use bb8::Pool;
use qstring::QString;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};

#[derive(serde::Serialize)]
struct StaticFeed {
    feed_id: String,
    operator_id: String,
    agency_id: String,
    name: String,
    url: String,
    timezone:String,
    lang: Option<String>,
    phone: Option<String>,
    fare_url: Option<String>,
    email: Option<String>,
    max_lat: f64,
    min_lat: f64,
    max_lon: f64,
    min_lon: f64,
}

async fn index(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("Hello world!")
}

async fn getfeeds(pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>, req: HttpRequest) -> impl Responder {
    let mut client = pool.get().await.unwrap();
    
    let postgresresult = client.query("SELECT onestop_feed_id, onestop_operator_id, gtfs_agency_id, name, url, timezone, lang, phone, fare_url, email,  max_lat, min_lat, max_lon, min_lon FROM gtfs_static.static_feeds;", &[]).await;

     match postgresresult {
        Ok(postgresresult) => {
            let mut result: Vec<StaticFeed> = Vec::new();
            for row in postgresresult {
                result.push(StaticFeed {
                    feed_id: row.get(0),
                    operator_id: row.get(1),
                    agency_id: row.get(2),
                    name: row.get(3),
                    url: row.get(4),
                    timezone: row.get(5),
                    lang: row.get(6),
                    phone: row.get(7),
                    fare_url: row.get(8),
                    email: row.get(9),
                    max_lat: row.get(10),
                    min_lat: row.get(11),
                    max_lon: row.get(12),
                    min_lon: row.get(13),
                });
            }

            let json_string = to_string_pretty(&json!(result)).unwrap();

            HttpResponse::Ok()
                .insert_header(("Content-Type", "application/json"))
                .body(json_string)
        },
        Err(e) => {
            println!("No results from postgres");

            HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Postgres Error")
        }
     }
}

#[derive(serde::Serialize)]
struct RouteOutPostgres {
    onestop_feed_id: String,
    route_id: String,
    short_name: String,
    long_name: String,
    desc: String,
    route_type: i32,
    url: Option<String>,
    agency_id: Option<String>,
    gtfs_order: Option<i32>,
    color: String,
    text_color: String,
    continuous_pickup: i32,
    continuous_drop_off: i32,
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

#[actix_web::get("/gettrip")]
pub async fn gettrip(pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>, req: HttpRequest) -> impl Responder {
    let mut client = pool.get().await;

    if client.is_ok() {
    let mut client = client.unwrap();
    let query_str = req.query_string(); // "name=ferret"
    let qs = QString::from(query_str);
    let req_feed_id = qs.get("feed_id").unwrap(); 
    let trip_id = qs.get("trip_id").unwrap();
        
        let postgresresult = client.query("SELECT trip_id, onestop_feed_id, 
        route_id, service_id, trip_headsign, trip_short_name, direction_id, 
        block_id, shape_id, wheelchair_accessible, bikes_allowed FROM gtfs.trips
         WHERE onestop_feed_id = $1 AND trip_id = $2;", &[
            &req_feed_id,
            &trip_id
         ]).await;
    
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
            },
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

#[actix_web::get("/getroutesperagency")]
pub async fn getroutesperagency(pool: web::Data<bb8::Pool<bb8_postgres::PostgresConnectionManager<NoTls>>>, req: HttpRequest) -> impl Responder {
    let mut client = pool.get().await;

    if client.is_ok() {
    let mut client = client.unwrap();
    let query_str = req.query_string(); // "name=ferret"
    let qs = QString::from(query_str);
    let req_feed_id = qs.get("feed_id").unwrap(); // "ferret"

        
        let postgresresult = client.query("SELECT onestop_feed_id, route_id,
         short_name, long_name, gtfs_desc, route_type, url, agency_id,
         gtfs_order,
         color,
         text_color,
         continuous_pickup,
         continuous_drop_off,
         shapes_list FROM gtfs.routes WHERE onestop_feed_id = $1;", &[
            &req_feed_id
         ]).await;
    
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
                        shapes_list: row.get(13)
                    });
                }
    
                let json_string = to_string_pretty(&json!(result)).unwrap();
    
                HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .body(json_string)
            },
            Err(e) => {
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
let manager: bb8_postgres::PostgresConnectionManager<NoTls> = bb8_postgres::PostgresConnectionManager::new(
    postgresstring.parse().unwrap(),
    NoTls,
);
let pool  = bb8::Pool::builder().build(manager).await.unwrap();


    // Create a new HTTP server.
    let builder = HttpServer::new(move || {
        App::new()
        .wrap(
            DefaultHeaders::new()
              .add(("Access-Control-Allow-Origin", "*"))
              .add(("Server", "KylerChinCatenary"))
              .add(("Access-Control-Allow-Origin","https://transitmap.kylerchin.com"))
        )
            .app_data(actix_web::web::Data::new(pool.clone()))
            .route("/", web::get().to(index))
            .service(getroutesperagency)
            .service(gettrip)
            .route("/getfeeds", web::get().to(getfeeds))
    })
    .workers(4);

    // Bind the server to port 8080.
    let _ = builder.bind("127.0.0.1:5401").unwrap().run().await;

    Ok(())
}
