use actix_web::middleware::DefaultHeaders;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use catenary::models::IpToGeoAddr;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{make_async_pool, CatenaryPostgresPool};
use catenary::EtcdConnectionIps;
use diesel::prelude::*;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use geojson::{Feature, GeoJson, JsonValue};
use ordered_float::Pow;
use serde::Deserialize;
use serde_derive::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tilejson::TileJSON;

#[actix_web::get("/get_agencies")]
pub async fn get_agencies_raw(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    let agencies_pg: Result<Vec<catenary::models::Agency>, _> =
        catenary::schema::gtfs::agencies::dsl::agencies
            .select(catenary::models::Agency::as_select())
            .order(catenary::schema::gtfs::agencies::dsl::agency_name)
            .load(conn)
            .await;

    if let Err(agencies_pg) = &agencies_pg {
        eprintln!("{}", agencies_pg);
        return HttpResponse::InternalServerError().body("Could not fetch agencies");
    }

    let agencies_pg = agencies_pg.unwrap();

    HttpResponse::Ok().json(agencies_pg)
}
