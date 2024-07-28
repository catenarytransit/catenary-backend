use actix_web::web;
use actix_web::HttpResponse;
use actix_web::Responder;
use catenary::EtcdConnectionIps;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::CatenaryPostgresPool;
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;

pub struct RouteInfoResponse {
    pub agency_name: String,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub url: Option<String>,
    pub color: String,
    pub text_color: String,
    pub route_type: i16,
    pub pdf_url: Option<String>,
    pub stops: BTreeMap<String, catenary::models::Stop>,
    pub direction_patterns: BTreeMap<String, DirectionsSummary>,
    pub shapes_polyline: BTreeMap<String, String>,
}

pub struct DirectionsSummary {
    pub direction_patterns: catenary::models::DirectionPatternMeta,
    pub rows: Vec<catenary::models::DirectionPatternRow>,
    pub shape_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct QueryRouteInfo {
    pub chateau: String,
    pub route_id: String,
}

#[actix_web::get("/route_info")]
pub async fn route_info(
    query: web::Query<QueryRouteInfo>,
    
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
) -> impl Responder {
    let query = query.into_inner();

    //connect to etcd

    let etcd =
        etcd_client::Client::connect(etcd_connection_ips.ip_addresses.as_slice(), None).await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    //connect to postgres
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to database");
    }

    let conn = &mut conn_pre.unwrap();

    // fetch route information

    let route_information = catenary::schema::gtfs::routes::dsl::routes
    .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(&query.chateau))
    .filter(catenary::schema::gtfs::routes::dsl::route_id.eq(&query.route_id))
    .select(catenary::models::Route::as_select())
    .load(conn)
    .await;

    // fetch agency name

    // fetch directions

    // fetch stops

    // fetch shapes

    //query realtime data pool for alerts

    //return as struct

    HttpResponse::InternalServerError().body("TODO!")
}
