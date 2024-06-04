use actix_web::{web, HttpResponse, Responder};
use catenary::aspen::lib::ChateauMetadataZookeeper;
use catenary::aspen_dataset::AspenStopTimeEvent;
use catenary::aspen_dataset::AspenisedAlert;
use catenary::aspen_dataset::AspenisedVehicleDescriptor;
use catenary::aspen_dataset::AspenisedVehiclePosition;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::itinerary_pattern as itinerary_pattern_pg_schema;
use catenary::schema::gtfs::itinerary_pattern_meta as itinerary_pattern_meta_pg_schema;
use catenary::schema::gtfs::routes as routes_pg_schema;
use catenary::schema::gtfs::stops as stops_pg_schema;
use catenary::schema::gtfs::trips_compressed as trips_compressed_pg_schema;
use chrono::TimeZone;
use chrono_tz::Tz;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tarpc::{client, context, tokio_serde::formats::Bincode};

#[actix_web::get("/get_vehicle_metadata/{chateau}/{vehicle_id}")]
pub async fn get_vehicle_metadata(path: web::Path<(String, String)>) -> impl Responder {
    let (chateau, vehicle_id) = path.into_inner();
    HttpResponse::Ok().body("get_vehicle_metadata")
}

#[actix_web::get("/get_vehicle_information/{chateau}/{gtfs_rt_id}")]
pub async fn get_vehicle_information(
    path: web::Path<(String, String)>,
    zk: web::Data<Arc<tokio_zookeeper::ZooKeeper>>,
) -> impl Responder {
    let (chateau, gtfs_id) = path.into_inner();

    let fetch_assigned_node_for_this_chateau = zk
        .get_data(format!("/aspen_assigned_chateaus/{}", chateau).as_str())
        .await;

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        if let Some((fetch_assigned_node_for_this_chateau_data, stat)) =
            fetch_assigned_node_for_this_chateau
        {
            let assigned_chateau_data = bincode::deserialize::<ChateauMetadataZookeeper>(
                &fetch_assigned_node_for_this_chateau_data,
            )
            .unwrap();

            let socket_addr = std::net::SocketAddr::new(assigned_chateau_data.tailscale_ip, 40427);

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr).await;

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
    pub stop_id: Option<String>,
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
    pub schedule_relationship: Option<i32>,
    pub gtfs_stop_sequence: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct QueryTripInformationParams {
    pub trip_id: String,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
}

#[actix_web::get("/get_trip_information_rt_update/{chateau}/")]
pub async fn get_trip_rt_update(
    path: web::Path<String>,
    query: web::Query<QueryTripInformationParams>,
    zk: web::Data<Arc<tokio_zookeeper::ZooKeeper>>,
    // pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let chateau = path.into_inner();

    let query = query.into_inner();

    let fetch_assigned_node_for_this_chateau = zk
        .get_data(format!("/aspen_assigned_chateaus/{}", chateau).as_str())
        .await;

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        if let Some((fetch_assigned_node_for_this_chateau_data, stat)) =
            fetch_assigned_node_for_this_chateau
        {
            let assigned_chateau_data = bincode::deserialize::<ChateauMetadataZookeeper>(
                &fetch_assigned_node_for_this_chateau_data,
            )
            .unwrap();

            let socket_addr = std::net::SocketAddr::new(assigned_chateau_data.tailscale_ip, 40427);

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr).await;

            if let Ok(aspen_client) = aspen_client {
                let get_trip = aspen_client
                    .get_trip_updates_from_trip_id(
                        context::current(),
                        chateau.clone(),
                        query.trip_id.clone(),
                    )
                    .await;

                if let Ok(Some(get_trip)) = get_trip {
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
                                    Some(ref query_start_time) => {
                                        let find_trip = get_trip.iter().find(|each_update| {
                                            matches!(
                                                each_update.trip.start_time.as_ref().map(
                                                    |start_time| {
                                                        start_time.clone() == *query_start_time
                                                    }
                                                ),
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
                                schedule_relationship: stop_time_update.schedule_relationship,
                                gtfs_stop_sequence: stop_time_update
                                    .stop_sequence
                                    .map(|x| x as u16),
                                rt_platform_string: stop_time_update.platform_string.clone(),
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
                } else {
                    HttpResponse::Ok().json(ResponseForGtfsRtRefresh {
                        found_data: false,
                        data: None,
                    })
                }
            } else {
                HttpResponse::InternalServerError()
                    .body("Could not connect to realtime data server")
            }
        } else {
            HttpResponse::InternalServerError().body("Could not connect to realtime data server")
        }
    } else {
        HttpResponse::InternalServerError().body("Could not connect to zookeeper")
    }
}

#[actix_web::get("/get_trip_information/{chateau}/")]
pub async fn get_trip_init(
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

    if trip_compressed.is_empty() {
        return HttpResponse::NotFound().body("Compressed trip not found");
    }

    let trip_compressed = trip_compressed[0].clone();
    // get itin data and itin meta data

    let (itin_meta, itin_rows, route) = futures::join!(
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
            .load(conn)
    );

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

    let itin_meta: catenary::models::ItineraryPatternMeta = itin_meta[0].clone();

    let mut itin_rows_to_use: Vec<catenary::models::ItineraryPatternRow> = itin_rows
        .into_iter()
        .filter(|row| itin_meta.attempt_id == row.attempt_id)
        .collect::<Vec<_>>();

    itin_rows_to_use.sort_by_key(|x| x.stop_sequence);

    let itin_rows_to_use = itin_rows_to_use;

    let tz = chrono_tz::Tz::from_str_insensitive(itin_meta.timezone.as_str());

    if let Err(tz_parsing_error) = &tz {
        eprintln!("Could not parse timezone {}", itin_meta.timezone.as_str());
        return HttpResponse::InternalServerError().body(format!(
            "Could not parse timezone {} from itinerary {}",
            itin_meta.timezone, itin_meta.itinerary_pattern_id
        ));
    }

    let timezone = tz.unwrap();

    let stop_ids_to_lookup: Vec<String> =
        itin_rows_to_use.iter().map(|x| x.stop_id.clone()).collect();

    let stops_data = stops_pg_schema::dsl::stops
        .filter(stops_pg_schema::dsl::chateau.eq(&chateau))
        .filter(stops_pg_schema::dsl::gtfs_id.eq_any(stop_ids_to_lookup))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await;

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

    let start_of_trip_datetime = if let Some(ref start_time) = query.start_time {
        //  let start_time = chrono::NaiveTime::parse_from_str(&start_time, "%H:%M:%S");

        // split into array based on :
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
            (Ok(h), Ok(m), Ok(s)) => {
                let added_days = h / 24;

                let start_time =
                    chrono::NaiveTime::from_hms_opt((h % 24).into(), m as u32, s as u32).unwrap();

                let new_naive_date = start_naive_date + chrono::Duration::days(added_days as i64);

                let start_of_trip_datetime = chrono::NaiveDateTime::new(new_naive_date, start_time);
                timezone
                    .from_local_datetime(&start_of_trip_datetime)
                    .unwrap()
            }
            _ => {
                eprintln!("Invalid start time format");
                return HttpResponse::BadRequest().body("Invalid start time");
            }
        }
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
            gtfs_stop_sequence: row.gtfs_stop_sequence as u16,
            rt_arrival: None,
            rt_departure: None,
            schedule_relationship: None,
        };

        stop_times_for_this_trip.push(stop_time);
    }

    // find zookeeper node ip
    let fetch_assigned_node_for_this_chateau = zk
        .get_data(format!("/aspen_assigned_chateaus/{}", chateau).as_str())
        .await;

    let mut vehicle = None;

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        if let Some((fetch_assigned_node_for_this_chateau_data, stat)) =
            fetch_assigned_node_for_this_chateau
        {
            let assigned_chateau_data = bincode::deserialize::<ChateauMetadataZookeeper>(
                &fetch_assigned_node_for_this_chateau_data,
            )
            .unwrap();

            let socket_addr = std::net::SocketAddr::new(assigned_chateau_data.tailscale_ip, 40427);

            let aspen_client = catenary::aspen::lib::spawn_aspen_client_from_ip(&socket_addr).await;

            if let Ok(aspen_client) = aspen_client {
                let get_trip = aspen_client
                    .get_trip_updates_from_trip_id(
                        context::current(),
                        chateau.clone(),
                        query.trip_id.clone(),
                    )
                    .await;

                if let Ok(get_trip) = get_trip {
                    if let Some(get_trip) = get_trip {
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
                                        Some(ref query_start_time) => {
                                            let find_trip =
                                                get_trip.iter().find(
                                                    |each_update| match each_update
                                                        .trip
                                                        .start_time
                                                        .as_ref()
                                                        .map(|start_time| {
                                                            start_time.clone() == *query_start_time
                                                        }) {
                                                        Some(true) => true,
                                                        _ => false,
                                                    },
                                                );

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
                                let stop_time = stop_times_for_this_trip.iter_mut().find(|x| {
                                    match stop_time_update.stop_id.clone() {
                                        Some(rt_stop_id) => match stop_time_update.stop_sequence {
                                            Some(rt_stop_sequence) => {
                                                rt_stop_id == x.stop_id
                                                    && rt_stop_sequence as u16
                                                        == x.gtfs_stop_sequence
                                            }
                                            None => rt_stop_id == x.stop_id,
                                        },
                                        None => match stop_time_update.stop_sequence {
                                            Some(rt_stop_sequence) => {
                                                rt_stop_sequence as u16 == x.gtfs_stop_sequence
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
                                }
                            }
                        }
                    } else {
                        eprintln!("Trip id not found {} {}", chateau, query.trip_id);
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

                let alerts_for_trip = aspen_client
                    .get_alert_from_trip_id(
                        context::current(),
                        chateau.clone(),
                        query.trip_id.clone(),
                    )
                    .await;

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
            } else {
                eprintln!("Error connecting to assigned node. Failed to connect to tarpc");
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
        trip_short_name: trip_compressed.trip_short_name,
        route_long_name: route.long_name,
        route_short_name: route.short_name,
        vehicle,
        route_type: route.route_type,
        stop_id_to_alert_ids,
        alert_ids_for_this_route: alert_ids_for_this_route,
        alert_ids_for_this_trip: alert_ids_for_this_trip,
        alert_id_to_alert: alert_id_to_alert,
    };

    let text = serde_json::to_string(&response).unwrap();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "no-cache"))
        .body(text)
}
