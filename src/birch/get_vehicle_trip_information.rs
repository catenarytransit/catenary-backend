use actix_web::rt;
use actix_web::{web, HttpResponse, Responder};
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::AspenStopTimeEvent;
use catenary::aspen_dataset::AspenisedAlert;
use catenary::aspen_dataset::AspenisedVehicleDescriptor;
use catenary::aspen_dataset::AspenisedVehiclePosition;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::calendar as calendar_pg_schema;
use catenary::schema::gtfs::calendar_dates as calendar_dates_pg_schema;
use catenary::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use catenary::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use catenary::schema::gtfs::routes as routes_pg_schema;
use catenary::schema::gtfs::stops as stops_pg_schema;
use catenary::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use catenary::EtcdConnectionIps;
use chrono::Datelike;
use chrono::TimeZone;
use chrono_tz::Tz;
use compact_str::CompactString;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use geo::coord;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tarpc::context;

#[actix_web::get("/get_vehicle_metadata/{chateau}/{vehicle_id}")]
pub async fn get_vehicle_metadata(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[actix_web::get("/get_vehicle_information_from_label/{chateau}/{vehicle_label}")]
pub async fn get_vehicle_information_from_label(
    path: web::Path<(String, String)>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
) -> impl Responder {
    let (chateau, vehicle_label) = path.into_inner();

    let etcd = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned(),
    )
    .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaus/{}", chateau).as_str(),
            None,
        )
        .await;

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        let fetch_assigned_node_for_this_chateau_kv_first =
            fetch_assigned_node_for_this_chateau.kvs().first();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = bincode::deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            if let Ok(aspen_client) = aspen_client {
                let get_vehicle = aspen_client
                    .get_single_vehicle_location_from_vehicle_label(
                        context::current(),
                        chateau.clone(),
                        vehicle_label.clone(),
                    )
                    .await;

                if let Ok(get_vehicle) = get_vehicle {
                    if let Some(get_vehicle) = get_vehicle {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: true,
                            data: Some(get_vehicle),
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    } else {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: false,
                            data: None,
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    }
                }
            }
        }
    }

    HttpResponse::InternalServerError().body("Could not connect to assigned node")
}

#[actix_web::get("/get_vehicle_information/{chateau}/{gtfs_rt_id}")]
pub async fn get_vehicle_information(
    path: web::Path<(String, String)>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
) -> impl Responder {
    let (chateau, gtfs_id) = path.into_inner();

    let etcd = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned(),
    )
    .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaus/{}", chateau).as_str(),
            None,
        )
        .await;

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        let fetch_assigned_node_for_this_chateau_kv_first =
            fetch_assigned_node_for_this_chateau.kvs().first();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = bincode::deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            if let Ok(aspen_client) = aspen_client {
                let get_vehicle = aspen_client
                    .get_single_vehicle_location_from_gtfsid(
                        context::current(),
                        chateau.clone(),
                        gtfs_id.clone(),
                    )
                    .await;

                if let Ok(get_vehicle) = get_vehicle {
                    if let Some(get_vehicle) = get_vehicle {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: true,
                            data: Some(get_vehicle),
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    } else {
                        let response_struct = ResponseForGtfsVehicle {
                            found_data: false,
                            data: None,
                        };

                        let response = serde_json::to_string(&response_struct).unwrap();
                        return HttpResponse::Ok()
                            .insert_header(("Content-Type", "application/json"))
                            .body(response);
                    }
                }
            }
        }
    }

    HttpResponse::InternalServerError().body("Could not connect to assigned node")
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct ResponseForGtfsVehicle {
    found_data: bool,
    data: Option<AspenisedVehiclePosition>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct ResponseForGtfsRtRefresh {
    pub found_data: bool,
    pub data: Option<GtfsRtRefreshData>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct GtfsRtRefreshData {
    stoptimes: Vec<StopTimeRefresh>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct StopTimeRefresh {
    pub stop_id: Option<compact_str::CompactString>,
    pub rt_arrival: Option<AspenStopTimeEvent>,
    pub rt_departure: Option<AspenStopTimeEvent>,
    pub schedule_relationship: Option<i32>,
    pub gtfs_stop_sequence: Option<u16>,
    pub rt_platform_string: Option<String>,
    pub departure_occupancy_status: Option<i32>,
}

#[derive(Deserialize, Serialize)]
struct TripIntroductionInformation {
    pub stoptimes: Vec<StopTimeIntroduction>,
    pub tz: Tz,
    pub block_id: Option<String>,
    pub bikes_allowed: i16,
    pub wheelchair_accessible: i16,
    pub has_frequencies: bool,
    pub route_id: String,
    pub trip_headsign: Option<String>,
    pub route_short_name: Option<String>,
    pub trip_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub vehicle: Option<AspenisedVehicleDescriptor>,
    pub route_type: i16,
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_ids_for_this_trip: Vec<String>,
    pub shape_polyline: Option<String>,
    pub trip_id_found_in_db: bool,
}
#[derive(Deserialize, Serialize, Clone, Debug)]
struct StopTimeIntroduction {
    pub stop_id: CompactString,
    pub name: Option<String>,
    pub translations: Option<BTreeMap<String, String>>,
    pub platform_code: Option<String>,
    pub rt_platform_string: Option<String>,
    pub timezone: Option<Tz>,
    pub code: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub scheduled_arrival_time_unix_seconds: Option<u64>,
    pub scheduled_departure_time_unix_seconds: Option<u64>,
    pub rt_arrival: Option<AspenStopTimeEvent>,
    pub rt_departure: Option<AspenStopTimeEvent>,
    pub schedule_relationship: Option<i32>,
    pub gtfs_stop_sequence: u16,
    pub interpolated_stoptime_unix_seconds: Option<u64>,
    pub timepoint: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct QueryTripInformationParams {
    pub trip_id: String,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub route_id: Option<String>,
}

#[actix_web::get("/get_trip_information_rt_update/{chateau}/")]
pub async fn get_trip_rt_update(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>, // pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
) -> impl Responder {
    let chateau = path.into_inner();

    let query = query.into_inner();

    let etcd = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned(),
    )
    .await;

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaus/{}", chateau).as_str(),
            None,
        )
        .await;

    match fetch_assigned_node_for_this_chateau {
        Ok(fetch_assigned_node_for_this_chateau) => {
            let fetch_assigned_node_for_this_chateau_kv_first =
                fetch_assigned_node_for_this_chateau.kvs().first();

            if let Some(fetch_assigned_node_for_this_chateau_data) =
                fetch_assigned_node_for_this_chateau_kv_first
            {
                let assigned_chateau_data = bincode::deserialize::<ChateauMetadataEtcd>(
                    fetch_assigned_node_for_this_chateau_data.value(),
                )
                .unwrap();

                let aspen_client =
                    catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                        .await;

                match aspen_client {
                    Ok(aspen_client) => {
                        let get_trip = aspen_client
                            .get_trip_updates_from_trip_id(
                                context::current(),
                                chateau.clone(),
                                query.trip_id.clone(),
                            )
                            .await;

                        match get_trip {
                            Ok(Some(get_trip)) => {
                                println!("recieved {} trip options from aspen", get_trip.len());
                                if !get_trip.is_empty() {
                                    let rt_trip_update = match get_trip.len() {
                                        1 => &get_trip[0],
                                        _ => {
                                            println!(
                                                "Multiple trip updates found for trip id {} {}",
                                                chateau, query.trip_id
                                            );
                                            match &query.start_time {
                                                Some(query_start_time) => {
                                                    let find_trip =
                                                        get_trip.iter().find(|each_update| {
                                                            matches!(
                                                                each_update
                                                                    .trip
                                                                    .start_time
                                                                    .as_ref()
                                                                    .map(|start_time| {
                                                                        start_time
                                                                            == query_start_time
                                                                    }),
                                                                Some(true)
                                                            )
                                                        });

                                                    match find_trip {
                                                        Some(find_trip) => find_trip,
                                                        None => &get_trip[0],
                                                    }
                                                }
                                                None => &get_trip[0],
                                            }
                                        }
                                    };

                                    println!(
                                        "rt data contains {} stop updates",
                                        rt_trip_update.stop_time_update.len()
                                    );

                                    let stop_data: Vec<StopTimeRefresh> = rt_trip_update
                                        .stop_time_update
                                        .iter()
                                        .map(|stop_time_update| StopTimeRefresh {
                                            stop_id: stop_time_update.stop_id.clone(),
                                            rt_arrival: stop_time_update.arrival.clone(),
                                            rt_departure: stop_time_update.departure.clone(),
                                            schedule_relationship: stop_time_update
                                                .schedule_relationship,
                                            gtfs_stop_sequence: stop_time_update
                                                .stop_sequence
                                                .map(|x| x as u16),
                                            rt_platform_string: stop_time_update
                                                .platform_string
                                                .clone(),
                                            departure_occupancy_status: stop_time_update
                                                .departure_occupancy_status,
                                        })
                                        .collect();

                                    HttpResponse::Ok().json(ResponseForGtfsRtRefresh {
                                        found_data: true,
                                        data: Some(GtfsRtRefreshData {
                                            stoptimes: stop_data,
                                        }),
                                    })
                                } else {
                                    HttpResponse::Ok().json(ResponseForGtfsRtRefresh {
                                        found_data: false,
                                        data: None,
                                    })
                                }
                            }
                            _ => HttpResponse::Ok().json(ResponseForGtfsRtRefresh {
                                found_data: false,
                                data: None,
                            }),
                        }
                    }
                    _ => HttpResponse::InternalServerError()
                        .body("Could not connect to realtime data server"),
                }
            } else {
                HttpResponse::InternalServerError()
                    .body("Could not connect to realtime data server")
            }
        }
        _ => HttpResponse::InternalServerError().body("Could not connect to zookeeper"),
    }
}

// TODO!
// - Allow for Swiftly Scheduleless no trip system
// - Allow for detours, looking up of new stops

//How to do it
//Use a centralised table and progressively update it

#[actix_web::get("/get_trip_information_v2/{chateau}/")]
pub async fn get_trip_init_v2(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>,
    // sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
) -> impl Responder {
    let mut timer = simple_server_timing_header::Timer::new();
    let chateau = path.into_inner();

    let query = query.into_inner();

    // connect to pool
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to database");
    }

    let conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    > = &mut conn_pre.unwrap();

    timer.add("open_pg_connection");

    //ask postgres first about the compressed trip
    let trip_compressed = trips_compressed_pg_schema::dsl::trips_compressed
        .filter(trips_compressed_pg_schema::dsl::chateau.eq(&chateau))
        .filter(trips_compressed_pg_schema::dsl::trip_id.eq(&query.trip_id))
        .select(catenary::models::CompressedTrip::as_select())
        .load(conn)
        .await;

    timer.add("query_compressed_trip");

    if let Err(trip_compressed_err) = &trip_compressed {
        eprintln!("{}", trip_compressed_err);
        return HttpResponse::InternalServerError().body("Error fetching trip compressed");
    }

    let trip_compressed: Vec<catenary::models::CompressedTrip> = trip_compressed.unwrap();

    //if the trip compressed cannot be found in the database and the route id is invalid, reject (maybe in the future, keep?)

    if trip_compressed.is_empty() {
        if query.route_id.is_none() {
            return HttpResponse::NotFound()
                .body("Compressed trip not found and route id is empty");
        }

        // build it's own made up trip compressed?
    }

    //also calculate detour information?

    HttpResponse::Ok().body("Not Implemented Yet")
}

#[actix_web::get("/get_trip_information/{chateau}/")]
pub async fn get_trip_init(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>,
    // sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
) -> impl Responder {
    let mut timer = simple_server_timing_header::Timer::new();
    let chateau = path.into_inner();

    let query = query.into_inner();

    // connect to pool
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to database");
    }

    let conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    > = &mut conn_pre.unwrap();

    timer.add("open_pg_connection");

    //ask postgres first
    let trip_compressed = trips_compressed_pg_schema::dsl::trips_compressed
        .filter(trips_compressed_pg_schema::dsl::chateau.eq(&chateau))
        .filter(trips_compressed_pg_schema::dsl::trip_id.eq(&query.trip_id))
        .select(catenary::models::CompressedTrip::as_select())
        .load(conn)
        .await;

    timer.add("query_compressed_trip");

    if let Err(trip_compressed_err) = &trip_compressed {
        eprintln!("{}", trip_compressed_err);
        return HttpResponse::InternalServerError().body("Error fetching trip compressed");
    }

    let trip_compressed: Vec<catenary::models::CompressedTrip> = trip_compressed.unwrap();

    if trip_compressed.is_empty() {
        return HttpResponse::NotFound().body("Compressed trip not found");
    }

    let trip_compressed = trip_compressed[0].clone();
    // get itin data and itin meta data, and calendar data

    let (itin_meta, itin_rows, route, calendar_req, calendar_dates) = futures::join!(
        itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_meta
            .filter(itinerary_pattern_meta_pg_schema::dsl::chateau.eq(&chateau))
            .filter(
                itinerary_pattern_meta_pg_schema::dsl::itinerary_pattern_id
                    .eq(&trip_compressed.itinerary_pattern_id),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load(conn),
        itinerary_pattern_pg_schema::dsl::itinerary_pattern
            .filter(itinerary_pattern_pg_schema::dsl::chateau.eq(&chateau))
            .filter(
                itinerary_pattern_pg_schema::dsl::itinerary_pattern_id
                    .eq(&trip_compressed.itinerary_pattern_id),
            )
            .select(catenary::models::ItineraryPatternRow::as_select())
            .load(conn),
        routes_pg_schema::dsl::routes
            .filter(routes_pg_schema::dsl::chateau.eq(&chateau))
            .filter(routes_pg_schema::dsl::route_id.eq(&trip_compressed.route_id))
            .select(catenary::models::Route::as_select())
            .load(conn),
        calendar_pg_schema::dsl::calendar
            .filter(calendar_pg_schema::dsl::chateau.eq(&chateau))
            .filter(calendar_pg_schema::dsl::service_id.eq(&trip_compressed.service_id))
            .select(catenary::models::Calendar::as_select())
            .load(conn),
        calendar_dates_pg_schema::dsl::calendar_dates
            .filter(calendar_dates_pg_schema::dsl::chateau.eq(&chateau))
            .filter(calendar_dates_pg_schema::dsl::service_id.eq(&trip_compressed.service_id))
            .select(catenary::models::CalendarDate::as_select())
            .load(conn),
    );

    timer.add("query_itin_route_and_itin_rows");

    if let Err(route_err) = &route {
        eprintln!("{}", route_err);
        return HttpResponse::InternalServerError().body("Error fetching route data");
    }

    let route = route.unwrap();

    if route.is_empty() {
        return HttpResponse::NotFound().body("Route not found");
    }

    let route = route[0].clone();

    if let Err(itin_meta) = &itin_meta {
        eprintln!("{}", itin_meta);
        return HttpResponse::InternalServerError()
            .body("Error fetching itinerary pattern metadata");
    }

    let itin_meta = itin_meta.unwrap();

    if let Err(itin_rows_err) = &itin_rows {
        eprintln!("{}", itin_rows_err);
        return HttpResponse::InternalServerError().body("Error fetching itinerary pattern rows");
    }

    let itin_rows = itin_rows.unwrap();

    if itin_meta.is_empty() {
        return HttpResponse::NotFound().body("Trip Itin not found");
    }

    if calendar_req.is_err() {
        eprintln!("{}", calendar_req.unwrap_err());
        return HttpResponse::InternalServerError().body("Error fetching calendar data");
    }

    let calendar_rows = calendar_req.unwrap();

    if calendar_dates.is_err() {
        eprintln!("{}", calendar_dates.unwrap_err());
        return HttpResponse::InternalServerError().body("Error fetching calendar dates data");
    }

    let calendar_dates = calendar_dates.unwrap();

    let itin_meta: catenary::models::ItineraryPatternMeta = itin_meta[0].clone();

    let mut itin_rows_to_use: Vec<catenary::models::ItineraryPatternRow> = itin_rows
        .into_iter()
        .filter(|row| itin_meta.attempt_id == row.attempt_id)
        .collect::<Vec<_>>();

    itin_rows_to_use.sort_by_key(|x| x.stop_sequence);

    let itin_rows_to_use = itin_rows_to_use;

    //query both at the same time

    //fetch shape from dataset

    //convert shape data into polyline

    let stop_ids_to_lookup: Vec<CompactString> =
        itin_rows_to_use.iter().map(|x| x.stop_id.clone()).collect();

    let (stops_data, shape_lookup): (
        Result<Vec<catenary::models::Stop>, diesel::result::Error>,
        Option<catenary::models::Shape>,
    ) = futures::join!(
        stops_pg_schema::dsl::stops
            .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
            .filter(stops_pg_schema::dsl::gtfs_id.eq_any(&stop_ids_to_lookup))
            .select(catenary::models::Stop::as_select())
            .load(conn),
        async {
            match itin_meta.shape_id {
                Some(shape_id_to_lookup) => {
                    let shape_query = catenary::schema::gtfs::shapes::dsl::shapes
                        .filter(
                            catenary::schema::gtfs::shapes::dsl::shape_id.eq(shape_id_to_lookup),
                        )
                        .filter(catenary::schema::gtfs::shapes::dsl::chateau.eq(&chateau))
                        .select(catenary::models::Shape::as_select())
                        .first(conn)
                        .await;

                    match shape_query {
                        Ok(shape_query) => Some(shape_query.clone()),
                        Err(err) => None,
                    }
                }
                None => None,
            }
        }
    );

    timer.add("query_stops_and_shape");

    let shape_polyline = shape_lookup.map(|shape_info| {
        polyline::encode_coordinates(
            geo::LineString::new(
                shape_info
                    .linestring
                    .points
                    .iter()
                    .map(|point| {
                        coord! {
                            x: point.x,
                            y: point.y
                        }
                    })
                    .collect::<Vec<_>>(),
            ),
            6,
        )
        .unwrap()
    });

    timer.add("convert_polyline");

    let tz = chrono_tz::Tz::from_str_insensitive(itin_meta.timezone.as_str());

    if let Err(tz_parsing_error) = &tz {
        eprintln!("Could not parse timezone {}", itin_meta.timezone.as_str());
        return HttpResponse::InternalServerError().body(format!(
            "Could not parse timezone {} from itinerary {}",
            itin_meta.timezone, itin_meta.itinerary_pattern_id
        ));
    }

    let timezone = tz.unwrap();

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

    let mut stop_times_for_this_trip: Vec<StopTimeIntroduction> = vec![];

    let mut alert_id_to_alert: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
    let mut alert_ids_for_this_route: Vec<String> = vec![];
    let mut alert_ids_for_this_trip: Vec<String> = vec![];

    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let added_seconds_to_ref_midnight = match &query.start_time {
        Some(start_time) => {
            let start_split: Vec<&str> = start_time.split(':').collect::<Vec<&str>>();

            if start_split.len() != 3 {
                eprintln!("Invalid start time format");
                return HttpResponse::BadRequest().body("Invalid start time");
            }

            let (h, m, s): (
                Result<u8, std::num::ParseIntError>,
                Result<u8, std::num::ParseIntError>,
                Result<u8, std::num::ParseIntError>,
            ) = (
                start_split[0].parse(),
                start_split[1].parse(),
                start_split[2].parse(),
            );

            match (h, m, s) {
                (Ok(h), Ok(m), Ok(s)) => (h as i64 * 3600) + (m as i64 * 60) + s as i64,
                _ => {
                    eprintln!("Invalid start time format");
                    return HttpResponse::BadRequest().body("Invalid start time");
                }
            }
        }
        None => trip_compressed.start_time as i64,
    };

    //map start date to a YYYY, MM, DD format
    let start_naive_date = if let Some(start_date) = query.start_date {
        let start_date = chrono::NaiveDate::parse_from_str(&start_date, "%Y%m%d");

        if let Err(start_date_err) = start_date {
            eprintln!("{}", start_date_err);
            return HttpResponse::BadRequest().body("Invalid start date");
        }

        start_date.unwrap()
    } else {
        // make an array from 7 days in the past to 7 days in the future, chrono naive days

        let now = chrono::Utc::now();
        let now = timezone.from_utc_datetime(&now.naive_utc());

        let now_date = now.date_naive();

        let start_date_iter = now_date - chrono::Duration::days(10);

        let reference_time_noon = chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap();

        let vec_service_dates = start_date_iter
            .iter_days()
            .take(20)
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|date| {
                let day_of_week = date.weekday();

                //check if the service is active on this day

                let mut service_active = false;

                for calendar in &calendar_rows {
                    if calendar.gtfs_start_date <= *date && date <= &(calendar.gtfs_end_date) {
                        service_active = match day_of_week {
                            chrono::Weekday::Mon => calendar.monday,
                            chrono::Weekday::Tue => calendar.tuesday,
                            chrono::Weekday::Wed => calendar.wednesday,
                            chrono::Weekday::Thu => calendar.thursday,
                            chrono::Weekday::Fri => calendar.friday,
                            chrono::Weekday::Sat => calendar.saturday,
                            chrono::Weekday::Sun => calendar.sunday,
                        };
                    }
                }

                let find_calendar_date = calendar_dates
                    .iter()
                    .find(|calendar_date| calendar_date.gtfs_date == *date);

                if let Some(find_calendar_date) = find_calendar_date {
                    service_active = match find_calendar_date.exception_type {
                        1 => true,
                        2 => false,
                        _ => service_active,
                    }
                }

                service_active
            })
            .map(|date| {
                let noon_on_start_date = chrono::NaiveDateTime::new(date, reference_time_noon);
                let noon_on_start_date_with_tz =
                    timezone.from_local_datetime(&noon_on_start_date).unwrap();

                //reference time is 12 hours before noon
                let reference_time = noon_on_start_date_with_tz - chrono::Duration::hours(12);

                let start_time =
                    reference_time + chrono::Duration::seconds(added_seconds_to_ref_midnight);

                let time_diff_from_now = start_time.signed_duration_since(now);

                (date, start_time, time_diff_from_now)
            })
            .collect::<Vec<_>>();

        let mut vec_service_dates = vec_service_dates;

        vec_service_dates.sort_by_key(|x| x.2.abs().num_seconds());

        let (start_naive_date, _, _) = vec_service_dates[0];

        start_naive_date
    };

    // get reference time as 12 hours before noon of the starting date
    let reference_time_noon = chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap();

    let noon_on_start_date = chrono::NaiveDateTime::new(start_naive_date, reference_time_noon);
    let noon_on_start_date_with_tz = timezone.from_local_datetime(&noon_on_start_date).unwrap();

    //reference time is 12 hours before noon
    let reference_time = noon_on_start_date_with_tz - chrono::Duration::hours(12);

    //calculate start of the trip time

    let start_of_trip_datetime =
        reference_time + chrono::Duration::seconds(added_seconds_to_ref_midnight);

    for row in itin_rows_to_use {
        let stop = stops_data_map.get(row.stop_id.as_str());

        if stop.is_none() {
            eprintln!("Stop {} not found", row.stop_id);
            continue;
        }

        let stop = stop.unwrap();

        let stop_time = StopTimeIntroduction {
            stop_id: (&stop.gtfs_id).into(),
            name: stop.name.clone(),
            translations: None,
            platform_code: stop.platform_code.clone(),
            timezone: match stop.timezone.as_ref() {
                Some(tz) => match chrono_tz::Tz::from_str_insensitive(tz) {
                    Ok(tz) => Some(tz),
                    Err(_) => None,
                },
                None => None,
            },
            code: stop.code.clone(),
            longitude: stop.point.map(|point| point.x),
            latitude: stop.point.map(|point| point.y),
            scheduled_arrival_time_unix_seconds: row.arrival_time_since_start.map(
                |arrival_time_since_start| {
                    start_of_trip_datetime.timestamp() as u64 + arrival_time_since_start as u64
                },
            ),
            scheduled_departure_time_unix_seconds: row.departure_time_since_start.map(
                |departure_time_since_start| {
                    start_of_trip_datetime.timestamp() as u64 + departure_time_since_start as u64
                },
            ),
            interpolated_stoptime_unix_seconds: row.interpolated_time_since_start.map(
                |interpolated_time_since_start| {
                    start_of_trip_datetime.timestamp() as u64 + interpolated_time_since_start as u64
                },
            ),
            gtfs_stop_sequence: row.gtfs_stop_sequence as u16,
            rt_arrival: None,
            rt_departure: None,
            schedule_relationship: None,
            rt_platform_string: None,
            timepoint: row.timepoint,
        };

        stop_times_for_this_trip.push(stop_time);
    }

    timer.add("stop_time_calculation");

    let etcd = etcd_client::Client::connect(
        etcd_connection_ips.ip_addresses.as_slice(),
        etcd_connection_options.as_ref().as_ref().to_owned(),
    )
    .await;

    timer.add("connect_to_etcd");

    if let Err(etcd_err) = &etcd {
        eprintln!("{:#?}", etcd_err);

        return HttpResponse::InternalServerError()
            .append_header(("Cache-Control", "no-cache"))
            .body("Could not connect to etcd");
    }

    let mut etcd = etcd.unwrap();

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaus/{}", chateau).as_str(),
            None,
        )
        .await;

    timer.add("fetch_assigned_aspen_chateau_data_from_etcd");

    let mut vehicle = None;

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        let fetch_assigned_node_for_this_chateau_kv_first =
            fetch_assigned_node_for_this_chateau.kvs().first();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = bincode::deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            timer.add("open_aspen_connection");

            match aspen_client {
                Ok(aspen_client) => {
                    let get_trip = aspen_client
                        .get_trip_updates_from_trip_id(
                            context::current(),
                            chateau.clone(),
                            query.trip_id.clone(),
                        )
                        .await;

                    timer.add("get_trip_rt_from_aspen");

                    if let Ok(get_trip) = get_trip {
                        match get_trip {
                            Some(get_trip) => {
                                println!("recieved {} trip options from aspen", get_trip.len());
                                if !get_trip.is_empty() {
                                    let rt_trip_update = match get_trip.len() {
                                        1 => &get_trip[0],
                                        _ => {
                                            println!(
                                                "Multiple trip updates found for trip id {} {}",
                                                chateau, query.trip_id
                                            );
                                            match &query.start_time {
                                                Some(query_start_time) => {
                                                    let find_trip =
                                                        get_trip.iter().find(|each_update| {
                                                            match each_update
                                                                .trip
                                                                .start_time
                                                                .as_ref()
                                                                .map(|start_time| {
                                                                    start_time.clone()
                                                                        == *query_start_time
                                                                }) {
                                                                Some(true) => true,
                                                                _ => false,
                                                            }
                                                        });

                                                    match find_trip {
                                                        Some(find_trip) => find_trip,
                                                        None => &get_trip[0],
                                                    }
                                                }
                                                None => &get_trip[0],
                                            }
                                        }
                                    };

                                    vehicle = rt_trip_update.vehicle.clone();

                                    println!(
                                        "rt data contains {} stop updates",
                                        rt_trip_update.stop_time_update.len()
                                    );

                                    for stop_time_update in &rt_trip_update.stop_time_update {
                                        // per gtfs rt spec, the stop can be targeted with either stop id or stop sequence
                                        let stop_time =
                                            stop_times_for_this_trip.iter_mut().find(|x| {
                                                match stop_time_update.stop_id.clone() {
                                                    Some(rt_stop_id) => {
                                                        match stop_time_update.stop_sequence {
                                                            Some(rt_stop_sequence) => {
                                                                rt_stop_id == x.stop_id
                                                                    && rt_stop_sequence as u16
                                                                        == x.gtfs_stop_sequence
                                                            }
                                                            None => rt_stop_id == x.stop_id,
                                                        }
                                                    }
                                                    None => match stop_time_update.stop_sequence {
                                                        Some(rt_stop_sequence) => {
                                                            rt_stop_sequence as u16
                                                                == x.gtfs_stop_sequence
                                                        }
                                                        None => false,
                                                    },
                                                }
                                            });

                                        if let Some(stop_time) = stop_time {
                                            if let Some(arrival) = &stop_time_update.arrival {
                                                stop_time.rt_arrival = Some(arrival.clone());
                                            }

                                            if let Some(departure) = &stop_time_update.departure {
                                                stop_time.rt_departure = Some(departure.clone());
                                            }

                                            if let Some(schedule_relationship) =
                                                stop_time_update.schedule_relationship
                                            {
                                                stop_time.schedule_relationship =
                                                    Some(schedule_relationship);
                                            }

                                            if let Some(rt_platform_string) =
                                                stop_time_update.platform_string.clone()
                                            {
                                                stop_time.rt_platform_string =
                                                    Some(rt_platform_string);
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                eprintln!("Trip id not found {} {}", chateau, query.trip_id);
                            }
                        }
                    }

                    // GET ALERTS

                    let alerts_for_route = aspen_client
                        .get_alerts_from_route_id(
                            context::current(),
                            chateau.clone(),
                            route.route_id.clone(),
                        )
                        .await;
                    timer.add("query_alerts_for_route");

                    let alerts_for_trip = aspen_client
                        .get_alert_from_trip_id(
                            context::current(),
                            chateau.clone(),
                            query.trip_id.clone(),
                        )
                        .await;

                    timer.add("query_alerts_for_trip");

                    if let Ok(alerts_for_route) = alerts_for_route {
                        if let Some(alerts_for_route) = alerts_for_route {
                            for (alert_id, alert) in alerts_for_route {
                                alert_id_to_alert.insert(alert_id.clone(), alert.clone());
                                alert_ids_for_this_route.push(alert_id.clone());
                            }
                        }
                    }

                    if let Ok(alerts_for_trip) = alerts_for_trip {
                        if let Some(alerts_for_trip) = alerts_for_trip {
                            for (alert_id, alert) in alerts_for_trip {
                                alert_id_to_alert.insert(alert_id.clone(), alert.clone());
                                alert_ids_for_this_trip.push(alert_id.clone());
                            }
                        }
                    }

                    // GET ALERTS FOR STOPS

                    let alerts_for_stops = aspen_client
                        .get_alert_from_stop_ids(
                            context::current(),
                            chateau.clone(),
                            stop_ids_to_lookup
                                .iter()
                                .map(|x| x.to_string())
                                .collect_vec(),
                        )
                        .await;

                    if let Ok(Some(alerts_for_stops)) = alerts_for_stops {
                        let relevant_alert_ids = alerts_for_stops
                            .alerts
                            .iter()
                            .filter(|(alert_id, alert)| {
                                alert.informed_entity.iter().any(|entity| {
                                    let route_id_covered = match &entity.route_id {
                                        None => true,
                                        Some(route_id) => route_id == &route.route_id,
                                    };

                                    let trip_covered = match &entity.trip {
                                        None => true,
                                        Some(trip) => match &trip.trip_id {
                                            None => true,
                                            Some(entity_trip_id) => {
                                                entity_trip_id == &query.trip_id
                                            }
                                        },
                                    };

                                    route_id_covered && trip_covered
                                })
                            })
                            .map(|(alert_id, _)| alert_id.clone())
                            .collect::<BTreeSet<_>>();

                        for (alert_id, alerts) in alerts_for_stops.alerts {
                            if relevant_alert_ids.contains(&alert_id) {
                                alert_id_to_alert.insert(alert_id.clone(), alerts.clone());
                            }
                        }

                        for (stop_id, alert_ids) in alerts_for_stops.stops_to_alert_ids {
                            stop_id_to_alert_ids.insert(
                                stop_id.clone(),
                                alert_ids
                                    .iter()
                                    .filter(|alert_id| {
                                        relevant_alert_ids.contains(alert_id.as_str())
                                    })
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            );
                        }
                    }
                }
                _ => {
                    eprintln!("Error connecting to assigned node. Failed to connect to tarpc");
                }
            }
        } else {
            eprintln!("No assigned node found for this chateau");
        }
    }

    let response = TripIntroductionInformation {
        stoptimes: stop_times_for_this_trip,
        tz: timezone,
        color: route.color,
        text_color: route.text_color,
        route_id: route.route_id,
        block_id: trip_compressed.block_id,
        bikes_allowed: trip_compressed.bikes_allowed,
        wheelchair_accessible: trip_compressed.wheelchair_accessible,
        has_frequencies: trip_compressed.has_frequencies,
        trip_headsign: itin_meta.trip_headsign,
        trip_short_name: trip_compressed.trip_short_name.map(|x| x.into()),
        route_long_name: route.long_name,
        route_short_name: route.short_name,
        vehicle,
        route_type: route.route_type,
        stop_id_to_alert_ids,
        alert_ids_for_this_route,
        alert_ids_for_this_trip,
        alert_id_to_alert,
        shape_polyline,
        trip_id_found_in_db: true,
    };

    let text = serde_json::to_string(&response).unwrap();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header((
            simple_server_timing_header::Timer::header_key(),
            timer.header_value(),
        ))
        .body(text)
}
