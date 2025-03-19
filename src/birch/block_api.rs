use actix_web::middleware::DefaultHeaders;
use actix_web::web::Query;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware, web};
use catenary::EtcdConnectionIps;
use catenary::models::IpToGeoAddr;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use compact_str::CompactString;
use diesel::SelectableHelper;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geojson::{Feature, GeoJson, JsonValue};
use itertools::Itertools;
use ordered_float::Pow;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tilejson::TileJSON;

#[derive(Deserialize, Clone)]
struct BlockApiQuery {
    chateau: String,
    block_id: String,
    service_date: String,
}

#[actix_web::get("/get_agencies")]
pub async fn block_api(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: Query<BlockApiQuery>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    //query trips compressed using the block id and chateau idx

    let trips: Vec<catenary::models::CompressedTrip> =
        catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::dsl::block_id.eq(&query.block_id))
            .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(&query.chateau))
            .select(catenary::models::CompressedTrip::as_select())
            .load(conn)
            .await
            .unwrap();

    // now find all the relevant itinerary, itinerary rows, direction meta, and service ids

    let itin_id_list = trips
        .iter()
        .map(|x| &x.itinerary_pattern_id)
        .unique()
        .map(|x| x.clone())
        .collect::<BTreeSet<String>>();

    let itin_rows = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
        .filter(
            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern_id
                .eq_any(&itin_id_list),
        )
        .filter(catenary::schema::gtfs::itinerary_pattern::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::ItineraryPatternRow::as_select())
        .load(conn)
        .await
        .unwrap();

    let itin_metas = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
        .filter(
            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_id
                .eq_any(&itin_id_list),
        )
        .filter(catenary::schema::gtfs::itinerary_pattern_meta::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::ItineraryPatternMeta::as_select())
        .load(conn)
        .await
        .unwrap();

    let service_ids = trips
        .iter()
        .map(|x| &x.service_id)
        .unique()
        .map(|x| x.clone())
        .collect::<BTreeSet<CompactString>>();

    HttpResponse::InternalServerError().body("Not implemented")
}
