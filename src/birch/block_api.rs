use actix_web::middleware::DefaultHeaders;
use actix_web::web::Query;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware, web};
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::models::IpToGeoAddr;
use catenary::postgis_to_diesel::diesel_multi_polygon_to_geo;
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use chrono::Datelike;
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
    service_date: String,
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
    trip_short_name: Option<CompactString>,
    trip_headsign: Option<String>,
    stop_count: usize,
}

#[derive(Deserialize, Clone, Serialize)]
struct BlockResponse {
    trips: Vec<TripInBlock>,
    routes: AHashMap<String, catenary::models::Route>,
}

#[actix_web::get("/get_block")]
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

    if trips.len() == 0 {
        return HttpResponse::NotFound().body("No trips found for block");
    }

    // now find all the relevant itinerary, itinerary rows, direction meta, and service ids

    let itin_id_list = trips
        .iter()
        .map(|x| &x.itinerary_pattern_id)
        .unique()
        .map(|x| x.clone())
        .collect::<BTreeSet<String>>();

    let itin_rows: Vec<catenary::models::ItineraryPatternRow> =
        catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
            .filter(
                catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern_id
                    .eq_any(&itin_id_list),
            )
            .filter(catenary::schema::gtfs::itinerary_pattern::dsl::chateau.eq(&query.chateau))
            .select(catenary::models::ItineraryPatternRow::as_select())
            .load(conn)
            .await
            .unwrap();

    let itin_metas: Vec<catenary::models::ItineraryPatternMeta> =
        catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
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

    let naive_date = query
        .service_date
        .chars()
        .filter(|x| x.is_numeric())
        .collect::<String>();

    let naive_date = chrono::NaiveDate::parse_from_str(&naive_date, "%Y%m%d");

    if naive_date.is_err() {
        return HttpResponse::BadRequest().body("Invalid date format");
    }

    let naive_date = naive_date.unwrap();

    //get tz

    let tz = chrono_tz::Tz::from_str_insensitive(itin_metas[0].timezone.as_str()).unwrap();

    //sort itin rows into a HashMap of vecs

    let mut itin_row_map: AHashMap<String, Vec<catenary::models::ItineraryPatternRow>> =
        AHashMap::new();

    for row in itin_rows {
        if itin_row_map.contains_key(&row.itinerary_pattern_id) {
            itin_row_map
                .get_mut(&row.itinerary_pattern_id)
                .unwrap()
                .push(row);
        } else {
            itin_row_map.insert(row.itinerary_pattern_id.clone(), vec![row]);
        }
    }

    for (_, rows) in itin_row_map.iter_mut() {
        rows.sort_by(|a, b| a.gtfs_stop_sequence.cmp(&b.gtfs_stop_sequence));
    }

    //make itin meta map

    let mut itin_meta_map: AHashMap<String, catenary::models::ItineraryPatternMeta> =
        AHashMap::new();

    for meta in itin_metas {
        itin_meta_map.insert(meta.itinerary_pattern_id.clone(), meta);
    }

    //get all first and last stop ids
    let mut stop_ids_to_lookup: Vec<CompactString> = vec![];

    for (_, rows) in &itin_row_map {
        stop_ids_to_lookup.push(rows[0].stop_id.clone());
        stop_ids_to_lookup.push(rows[rows.len() - 1].stop_id.clone());
    }

    let stop_ids_to_lookup = stop_ids_to_lookup
        .iter()
        .unique()
        .map(|x| x.clone())
        .collect::<BTreeSet<CompactString>>();

    let stops: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&stop_ids_to_lookup))
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await
        .unwrap();

    //sort into HashMap

    let mut stops_map: AHashMap<String, catenary::models::Stop> = AHashMap::new();

    for stop in stops {
        stops_map.insert(stop.gtfs_id.clone(), stop);
    }

    let calendar: Vec<catenary::models::Calendar> = catenary::schema::gtfs::calendar::dsl::calendar
        .filter(catenary::schema::gtfs::calendar::dsl::service_id.eq_any(&service_ids))
        .filter(catenary::schema::gtfs::calendar::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::Calendar::as_select())
        .load(conn)
        .await
        .unwrap();

    let calendar_dates: Vec<catenary::models::CalendarDate> =
        catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
            .filter(catenary::schema::gtfs::calendar_dates::dsl::service_id.eq_any(&service_ids))
            .filter(catenary::schema::gtfs::calendar_dates::dsl::chateau.eq(&query.chateau))
            .select(catenary::models::CalendarDate::as_select())
            .load(conn)
            .await
            .unwrap();

    let calendar_hashmap: AHashMap<String, Vec<catenary::models::Calendar>> = calendar
        .into_iter()
        .chunk_by(|x| x.service_id.clone())
        .into_iter()
        .map(|(key, group)| (key, group.collect()))
        .collect();

    let calendar_dates_hashmap: AHashMap<String, Vec<catenary::models::CalendarDate>> =
        calendar_dates
            .into_iter()
            .chunk_by(|x| x.service_id.clone())
            .into_iter()
            .map(|(key, group)| (key, group.collect()))
            .collect();

    let reference_midnight = naive_date
        .and_time(chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap())
        - chrono::Duration::hours(12);

    let reference_midnight = reference_midnight.and_local_timezone(tz).unwrap();

    let mut trips_in_block: Vec<TripInBlock> = vec![];

    for trip in trips.into_iter().filter(|x| {
        let mut is_active = false;

        let calendar = calendar_hashmap.get(x.service_id.as_str());

        if let Some(calendars) = calendar {
            for cal in calendars {
                if cal.gtfs_start_date <= naive_date && cal.gtfs_end_date >= naive_date {
                    match naive_date.weekday() {
                        chrono::Weekday::Mon => {
                            if cal.monday {
                                is_active = true;
                            }
                        }
                        chrono::Weekday::Tue => {
                            if cal.tuesday {
                                is_active = true;
                            }
                        }
                        chrono::Weekday::Wed => {
                            if cal.wednesday {
                                is_active = true;
                            }
                        }
                        chrono::Weekday::Thu => {
                            if cal.thursday {
                                is_active = true;
                            }
                        }
                        chrono::Weekday::Fri => {
                            if cal.friday {
                                is_active = true;
                            }
                        }
                        chrono::Weekday::Sat => {
                            if cal.saturday {
                                is_active = true;
                            }
                        }
                        chrono::Weekday::Sun => {
                            if cal.sunday {
                                is_active = true;
                            }
                        }
                    }
                }
            }
        }

        let calendar_dates = calendar_dates_hashmap.get(x.service_id.as_str());

        if let Some(calendar_dates) = calendar_dates {
            for cal in calendar_dates {
                if cal.gtfs_date == naive_date {
                    if cal.exception_type == 1 {
                        is_active = true;
                    } else if cal.exception_type == 2 {
                        is_active = false;
                    }
                }
            }
        }

        is_active
    }) {
        let itin_rows = itin_row_map.get(&trip.itinerary_pattern_id).unwrap();
        let itin_meta = itin_meta_map.get(&trip.itinerary_pattern_id).unwrap();

        let first_stop = stops_map.get(itin_rows[0].stop_id.as_str()).unwrap();
        let last_stop = stops_map
            .get(itin_rows[itin_rows.len() - 1].stop_id.as_str())
            .unwrap();

        let start_time = trip.start_time as u64 + reference_midnight.timestamp() as u64;

        let end_time_offset = itin_rows[itin_rows.len() - 1]
            .arrival_time_since_start
            .unwrap_or(
                itin_rows[itin_rows.len() - 1]
                    .departure_time_since_start
                    .unwrap(),
            ) as u64;

        let end_time = start_time + end_time_offset;

        let trip_in_block = TripInBlock {
            start_time: start_time as u64,
            end_time: end_time as u64,
            first_stop_name: first_stop.name.clone().unwrap_or_default(),
            last_stop_name: last_stop.name.clone().unwrap_or_default(),
            timezone_start: stops_map
                .get(itin_rows[0].stop_id.as_str())
                .unwrap()
                .timezone
                .clone()
                .unwrap_or(itin_meta.timezone.clone()),
            timezone_end: stops_map
                .get(itin_rows[itin_rows.len() - 1].stop_id.as_str())
                .unwrap()
                .timezone
                .clone()
                .unwrap_or(itin_meta.timezone.clone()),
            trip_id: trip.trip_id.clone(),
            route_id: trip.route_id.clone(),
            trip_short_name: trip.trip_short_name.clone(),
            trip_headsign: itin_meta.trip_headsign.clone(),
            stop_count: itin_rows.len(),
        };

        trips_in_block.push(trip_in_block);
    }

    //sort by start time

    trips_in_block.sort_by(|a, b| a.start_time.cmp(&b.start_time));

    //get all the routes

    let route_ids = trips_in_block
        .iter()
        .map(|x| x.route_id.clone())
        .unique()
        .collect::<Vec<String>>();

    let routes: Vec<catenary::models::Route> = catenary::schema::gtfs::routes::dsl::routes
        .filter(catenary::schema::gtfs::routes::dsl::route_id.eq_any(&route_ids))
        .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(&query.chateau))
        .select(catenary::models::Route::as_select())
        .load(conn)
        .await
        .unwrap();

    let mut routes_map: AHashMap<String, catenary::models::Route> = AHashMap::new();

    for route in routes {
        routes_map.insert(route.route_id.clone(), route);
    }

    let block_response = BlockResponse {
        trips: trips_in_block,
        routes: routes_map,
    };

    HttpResponse::Ok()
        .append_header(("Access-Control-Allow-Origin", "*"))
        .json(block_response)
}
