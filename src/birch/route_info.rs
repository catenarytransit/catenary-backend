use actix_web::web;
use actix_web::HttpResponse;
use actix_web::Responder;
use catenary::EtcdConnectionIps;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::CatenaryPostgresPool;
use diesel_async::RunQueryDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use catenary::aspen_dataset::AspenisedAlert;

use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;

pub struct RouteInfoResponse {
    pub agency_name: String,
    pub agency_id: String,
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
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
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
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    // fetch route information

    let route_information_pg: Result<Vec<catenary::models::Route>, _> = catenary::schema::gtfs::routes::dsl::routes
    .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(&query.chateau))
    .filter(catenary::schema::gtfs::routes::dsl::route_id.eq(&query.route_id))
    .select(catenary::models::Route::as_select())
    .load(conn)
    .await;

    if let Err(route_information_pg) = &route_information_pg {
        eprintln!("{}", route_information_pg);
        return HttpResponse::InternalServerError().body("Could not fetch route information");
    }

    let route_information_pg = route_information_pg.unwrap();

    if route_information_pg.is_empty() {
        return HttpResponse::InternalServerError().body("Error finding route");
    }

    let route = route_information_pg[0].clone();

    // fetch agency name

    let mut agency: Option<catenary::models::Agency> = None;

    match route.agency_id {
        Some(agency_id) => {
            let agency_pg: Result<Vec<catenary::models::Agency>, _> = catenary::schema::gtfs::agencies::dsl::agencies
                .filter(catenary::schema::gtfs::agencies::dsl::chateau.eq(&query.chateau))
                .filter(catenary::schema::gtfs::agencies::dsl::agency_id.eq(agency_id))
                .select(catenary::models::Agency::as_select())
                .load(conn)
                .await;

            if let Err(agency_pg) = &agency_pg {
                eprintln!("{}", agency_pg);
                return HttpResponse::InternalServerError().body("Could not fetch agency information");
            }

            let agency_pg = agency_pg.unwrap();

            if !agency_pg.is_empty() {
                agency = Some(agency_pg[0].clone());
            }
        },
        None => {
            let agency_pg: Result<Vec<catenary::models::Agency>, _> = catenary::schema::gtfs::agencies::dsl::agencies
                .filter(catenary::schema::gtfs::agencies::dsl::static_onestop_id.eq(&route.onestop_feed_id))
                .select(catenary::models::Agency::as_select())
                .load(conn)
                .await;

                if let Err(agency_pg) = &agency_pg {
                    eprintln!("{}", agency_pg);
                    return HttpResponse::InternalServerError().body("Could not fetch agency information");
                }
    
                let agency_pg = agency_pg.unwrap();
    
                if !agency_pg.is_empty() {
                    agency = Some(agency_pg[0].clone());
                }
        }
    };
    
    //get current attempt ids for onestop_feed_id



    // fetch directions

    let direction_patterns_pg: Result<Vec<catenary::models::DirectionPatternMeta>, _> = catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
        .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::route_id.eq(&query.route_id))
        .select(catenary::models::DirectionPatternMeta::as_select())
        .load(conn)
        .await;
    // fetch stops

    // fetch shapes

    //query realtime data pool for alerts

    //return as struct

    HttpResponse::InternalServerError().body("TODO!")
}
