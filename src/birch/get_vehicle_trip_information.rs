use actix_web::{web, HttpResponse, Responder};
use catenary::aspen_dataset::AspenStopTimeEvent;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use catenary::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use catenary::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use catenary::schema::gtfs::stops as stops_pg_schema;
use diesel::query_dsl::methods::FilterDsl;
use diesel::ExpressionMethods;
use diesel::query_dsl::methods::SelectDsl;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono_tz::Tz;
use diesel::SelectableHelper;
use std::collections::BTreeMap;
use chrono::TimeZone;

#[actix_web::get("/get_vehicle_metadata/{chateau}/{vehicle_id}")]
pub async fn get_vehicle_metadata(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[actix_web::get("/get_vehicle_information/{chateau}/{gtfs_rt_id}")]
pub async fn get_vehicle_information(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[derive(Deserialize, Serialize)]
struct TripIntroductionInformation {
    stoptimes: Vec<StopTimeIntroduction>,
    tz: Tz,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct StopTimeIntroduction {
    pub stop_id: String,
    pub name: Option<String>,
    pub translations: Option<BTreeMap<String, String>>,
    pub platform_code: Option<String>,
    pub timezone: Option<Tz>,
    pub code: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub scheduled_arrival_time_unix_seconds: Option<u64>,
    pub scheduled_departure_time_unix_seconds: Option<u64>,
    pub rt_arrival: Option<AspenStopTimeEvent>,
    pub rt_departure: Option<AspenStopTimeEvent>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct QueryTripInformationParams {
    pub trip_id: String,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
}

#[actix_web::get("/get_trip_information/{chateau}/")]
pub async fn get_trip(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>,
    zk: web::Data<Arc<tokio_zookeeper::ZooKeeper>>,
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let chateau = path.into_inner();


    let query = query.into_inner();

    // connect to pool
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to database");
    }

    let conn = &mut conn_pre.unwrap();

    //ask postgres first
    let trip_compressed = trips_compressed_pg_schema::dsl::trips_compressed
        .filter(trips_compressed_pg_schema::dsl::chateau.eq(&chateau))
        .filter(trips_compressed_pg_schema::dsl::trip_id.eq(&query.trip_id))
        .select(catenary::models::CompressedTrip::as_select())
        .load(conn)
        .await;

    if let Err(trip_compressed_err) = &trip_compressed {
        eprintln!("{}", trip_compressed_err);
        return HttpResponse::InternalServerError().body("Error fetching trip compressed");
    }

    let trip_compressed = trip_compressed.unwrap();

    if trip_compressed.len() == 0 {
        return HttpResponse::NotFound().body("Compressed trip not found");
    }

    let trip_compressed = trip_compressed[0].clone();
    // get itin data and itin meta data

    let itin_meta = itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_meta
        .filter(itinerary_pattern_meta_pg_schema::dsl::chateau.eq(&chateau))
        .filter(itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_id.eq(&trip_compressed.itinerary_pattern_id))
        .select(catenary::models::ItineraryPatternMeta::as_select())
        .load(conn).await;

        if let Err(itin_meta) = &itin_meta {
            eprintln!("{}", itin_meta);
            return HttpResponse::InternalServerError().body("Error fetching itinerary pattern metadata");
        }

        let itin_meta = itin_meta.unwrap();

        let itin_rows = itinerary_pattern_pg_schema::dsl::itinerary_pattern
        .filter(itinerary_pattern_pg_schema::dsl::chateau.eq(&chateau))
        .filter(itinerary_pattern_pg_schema::dsl::itinerary_pattern_id.eq(&trip_compressed.itinerary_pattern_id))
        .select(catenary::models::ItineraryPatternRow::as_select())
        .load(conn).await;

        if let Err(itin_rows_err) = &itin_rows {
            eprintln!("{}", itin_rows_err);
            return HttpResponse::InternalServerError().body("Error fetching itinerary pattern rows");
        }

        let itin_rows = itin_rows.unwrap();

        if itin_meta.is_empty() {
            return HttpResponse::NotFound().body("Trip Itin not found");
        }

        let itin_meta:catenary::models::ItineraryPatternMeta = itin_meta[0].clone();

        let mut itin_rows_to_use:Vec<catenary::models::ItineraryPatternRow> = itin_rows.into_iter().filter(|row| itin_meta.attempt_id == row.attempt_id).collect::<Vec<_>>();

        itin_rows_to_use.sort_by_key(|x| x.stop_sequence);

        let itin_rows_to_use = itin_rows_to_use;

        let tz = chrono_tz::Tz::from_str_insensitive(itin_meta.timezone.as_str());

        if let Err(tz_parsing_error) = &tz {
            eprintln!("Could not parse timezone {}", itin_meta.timezone.as_str());
            return HttpResponse::InternalServerError().body(
                format!("Could not parse timezone {} from itinerary {}", itin_meta.timezone, itin_meta.itinerary_pattern_id)
            );
        }

        let timezone = tz.unwrap();

        let stop_ids_to_lookup: Vec<String> = itin_rows_to_use.iter().map(|x| x.stop_id.clone()).collect();

        let stops_data = stops_pg_schema::dsl::stops
        .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
        .filter(stops_pg_schema::dsl::gtfs_id.eq_any(stop_ids_to_lookup))
        .select(catenary::models::Stop::as_select())
        .load(conn).await;

        if let Err(stops_data_err) = &stops_data {
            eprintln!("{}", stops_data_err);
            return HttpResponse::InternalServerError().body("Error fetching stops data");
        }

        let stops_data = stops_data.unwrap();

        let mut stops_data_map: BTreeMap<String, catenary::models::Stop> = BTreeMap::new();

        for stop in stops_data {
            stops_data_map.insert(stop.gtfs_id.clone(), stop);
        }

        let stops_data_map = stops_data_map;

        let mut stop_times_for_this_trip:Vec<StopTimeIntroduction> = vec![];
        
    //map start date to a YYYY, MM, DD format
    let start_naive_date = if let Some(start_date) = query.start_date {
        let start_date = chrono::NaiveDate::parse_from_str(&start_date, "%Y%m%d");

        if let Err(start_date_err) = start_date {
            eprintln!("{}", start_date_err);
            return HttpResponse::BadRequest().body("Invalid start date");
        }

        start_date.unwrap()
    } else {
        //get current date under the timezone
       chrono::Utc::now().with_timezone(&timezone).date_naive()
    };

    // get reference time as 12 hours before noon of the starting date
    let reference_time_noon = chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    
    let noon_on_start_date = chrono::NaiveDateTime::new(start_naive_date, reference_time_noon);
    let noon_on_start_date_with_tz = timezone.from_local_datetime(&noon_on_start_date).unwrap();

    //reference time is 12 hours before noon
    let reference_time = noon_on_start_date_with_tz - chrono::Duration::hours(12);

    //calculate start of the trip time

    let start_of_trip_datetime = if let Some(start_time) = query.start_time {
        let start_time = chrono::NaiveTime::parse_from_str(&start_time, "%H:%M:%S");

        if let Err(start_time_err) = start_time {
            eprintln!("{}", start_time_err);
            return HttpResponse::BadRequest().body("Invalid start time");
        }

        let start_time = start_time.unwrap();

        let start_of_trip_datetime = chrono::NaiveDateTime::new(start_naive_date, start_time);
        timezone.from_local_datetime(&start_of_trip_datetime).unwrap()
    } else {
        // number of seconds since midnight + compressed_trip.start_time
        reference_time + chrono::Duration::seconds(trip_compressed.start_time as i64)
    };

    for row in itin_rows_to_use {
        let stop = stops_data_map.get(&row.stop_id);

        if let None = stop {
            eprintln!("Stop {} not found", row.stop_id);
            continue;
        }

        let stop = stop.unwrap();

        let stop_time = StopTimeIntroduction {
            stop_id: stop.gtfs_id.clone(),
            name: stop.name.clone(),
            translations: None,
            platform_code: stop.platform_code.clone(),
            timezone: Some(timezone),
            code: stop.code.clone(),
            longitude: stop.point.map(|point| point.x),
            latitude: stop.point.map(|point| point.y),
            scheduled_arrival_time_unix_seconds: row.arrival_time_since_start.map(|arrival_time_since_start|
                start_of_trip_datetime.timestamp() as u64 + arrival_time_since_start as u64
            ),
            scheduled_departure_time_unix_seconds: row.departure_time_since_start.map(|departure_time_since_start|
                start_of_trip_datetime.timestamp() as u64 + departure_time_since_start as u64
            
            ),
            rt_arrival: None,
            rt_departure: None,
        };

        stop_times_for_this_trip.push(stop_time);
    }

    let response = TripIntroductionInformation {
        stoptimes: stop_times_for_this_trip,
        tz: timezone,
    };

    let text = serde_json::to_string(&response).unwrap();

    HttpResponse::Ok().body(text)
}
