use actix_web::dev::Service;
use actix_web::middleware::DefaultHeaders;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use std::sync::Arc;
use std::error::Error;
use std::collections::BTreeMap;
use serde::Serialize;
use serde::Deserialize;

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct EachPasswordRow {
    passwords: catenary::agency_secret::PasswordFormat,
    last_updated_ms: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct KeyResponse {
    passwords: BTreeMap<String,EachPasswordRow>
}

#[actix_web::get("/getrealtimekeys/")]
pub async fn get_realtime_keys(pool: web::Data<Arc<CatenaryPostgresPool>>, req: HttpRequest) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    HttpResponse::Ok()
    .insert_header(("Content-Type", "text/plain"))
    .body("Get Realtime Keys")
}