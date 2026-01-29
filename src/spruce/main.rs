use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, web};
use actix_web_actors::ws;
use std::sync::Arc;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use catenary::EtcdConnectionIps;

mod trip_websocket;
use trip_websocket::TripWebSocket;

async fn index(
    req: HttpRequest,
    stream: web::Payload,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
) -> Result<HttpResponse, Error> {
    ws::start(
        TripWebSocket::new(
            pool.as_ref().clone(),
            etcd_connection_ips.as_ref().clone(),
            etcd_connection_options.as_ref().clone()
        ),
        &req,
        stream
    )
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "debug");
    // env_logger::init();

    let pool = Arc::new(make_async_pool().await.unwrap());
    
    let etcd_urls_string = std::env::var("ETCD_URLS").unwrap();
    let etcd_urls_vec: Vec<String> = etcd_urls_string.split(",").map(|x| x.to_string()).collect();
    let etcd_username = std::env::var("ETCD_USERNAME").unwrap();
    let etcd_password = std::env::var("ETCD_PASSWORD").unwrap();

    let etcd_connection_ips = Arc::new(EtcdConnectionIps {
        ip_addresses: etcd_urls_vec.clone(),
    });

    let etcd_connection_options = Arc::new(Some(
        etcd_client::ConnectOptions::new()
            .with_user(etcd_username, etcd_password)
            .with_keep_alive(std::time::Duration::from_secs(1), std::time::Duration::from_secs(5)),
    ));
    
    let worker_amount = std::env::var("WORKER_AMOUNT")
        .unwrap_or("2".to_string())
        .parse::<usize>()
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(etcd_connection_ips.clone()))
            .app_data(web::Data::new(etcd_connection_options.clone()))
            .route("/ws/", web::get().to(index))
    })
    .workers(worker_amount)
    .bind(("127.0.0.1", 52771))?
    .run()
    .await
}
