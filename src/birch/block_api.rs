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
use serde::Serialize;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tilejson::TileJSON;

// query the client sends to the API
#[derive(Deserialize, Clone)]
struct BlockApiQuery {
    chateau: String,
    block_id: String,
    //should be in YYYYMMDD format, but YYYY-MM-DD should also be parsable
    service_date: String
}

#[derive(Deserialize, Serialize, Clone)]
struct TripInBlock {
    start_time: u64,
    end_time: u64,
    first_stop_name: String,
    last_stop_name: String,
    timezone_start: String,
    timezone_end: String,
    trip_id: String,
    route_id: String,
    trip_short_name: String,
    trip_headsign: String,
    stop_count: usize,
}

#[derive(Deserialize, Clone)]
struct BlockResponse {
    trips: Vec<TripInBlock>,
    routes: HashMap<String, catenary::models::Route>
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

    let itin_rows: Vec<catenary::models::ItineraryPatternRow> = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
        .filter(
            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern_id
                .eq_any(&itin_id_list),
        )
        .filter(catenary::schema::gtfs::itinerary_pattern::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::ItineraryPatternRow::as_select())
        .load(conn)
        .await
        .unwrap();

    let itin_metas: Vec<catenary::models::ItineraryPatternMeta> = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
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

    let native_date = query.service_date.chars()
    .filter(|x| x.is_numeric()).collect::<String>();

    let native_date = chrono::NaiveDate::parse_from_str(&native_date, "%Y%m%d");

    if native_date.is_err() {
        return HttpResponse::BadRequest().body("Invalid date format");
    }

    let native_date = native_date.unwrap();

    //get tz

    let tz = chrono_tz::Tz::from_str_insensitive(itin_metas[0].timezone.as_str()).unwrap();

    //sort itin rows into a hashmap of vecs

    let mut itin_row_map: HashMap<String, Vec<catenary::models::ItineraryPatternRow>> = HashMap::new();

    for row in itin_rows {
        if itin_row_map.contains_key(&row.itinerary_pattern_id) {
            itin_row_map.get_mut(&row.itinerary_pattern_id).unwrap().push(row);
        } else {
            itin_row_map.insert(row.itinerary_pattern_id.clone(), vec![row]);
        }
    }

    //get all first and last stop ids
    let mut stop_ids_to_lookup: Vec<CompactString> = vec![];

    for (_, rows) in &itin_row_map {
        stop_ids_to_lookup.push(rows[0].stop_id.clone());
        stop_ids_to_lookup.push(rows[rows.len() - 1].stop_id.clone());
    }

    let stop_ids_to_lookup = stop_ids_to_lookup.iter().unique().map(|x| x.clone()).collect::<BTreeSet<CompactString>>();

    let stops: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&stop_ids_to_lookup))
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await
        .unwrap();

    let reference_midnight = native_date.and_time(chrono::NaiveTime::from_hms(12, 0, 0)) - chrono::Duration::hours(12);

    let reference_midnight = reference_midnight.and_local_timezone(tz).unwrap();

    

    HttpResponse::InternalServerError().body("Not implemented")


}
