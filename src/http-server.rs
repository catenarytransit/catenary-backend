use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use serde_json::{json, to_string_pretty};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tokio_postgres::{Error as PostgresError, NoTls, Row};

#[derive(serde::Serialize)]
struct StaticFeed {
    feed_id: String,
    operator_id: String,
    agency_id: String,
    name: String,
    url: String,
    timezone: String,
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

async fn getfeeds(req: HttpRequest, client: &Client) -> impl Responder {
    let postgresresult = client.query("SELECT onestop_feed_id, onestop_operator_id, gtfs_agency_id, name, url, timezone, lang, phone, fare_url, email, 
    max_lat, min_lat, max_lon, min_lon FROM gtfs_static.static_feeds", &[]).await;

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
        }
        Err(e) => {
            println!("No results from postgres");

            HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Postgres Error")
        }
    }
}

fn getroutesperagency(req: HttpRequest, client: &Client) -> impl Responder {
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain"))
        .body("Hello world!")
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
    let (client, connection) = tokio_postgres::connect(&postgresstring, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create a new HTTP server.
    let builder = HttpServer::new(|| {
        App::new()
            .wrap(
                middleware::DefaultHeaders::new()
                   // .add("Access-Control-Allow-Origin", "*")
                    //.add("Server", "KylerChinCatenary"),
            )
            .route("/", web::get().to(index))
    })
    .workers(4);

    // Bind the server to port 8080.
    let _ = builder.bind("127.0.0.1:5401").unwrap().run().await;

    Ok(())
}
