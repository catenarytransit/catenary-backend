use crate::CatenaryPostgresPool;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use ahash::AHashMap;
use catenary::EtcdConnectionIps;
use catenary::SerializableStop;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::AspenisedAlert;
use catenary::models::DirectionPatternMeta;
use catenary::models::DirectionPatternRow;
use compact_str::CompactString;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use geo::coord;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tarpc::context;

// NEW: import the helper
use crate::connections_lookup::connections_lookup;

#[derive(Serialize, Deserialize, Clone)]
pub struct RouteInfoResponse {
    pub agency_name: Option<String>,
    pub agency_id: Option<String>,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub url: Option<String>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub route_type: i16,
    pub pdf_url: Option<String>,
    pub stops: HashMap<String, SerializableStop>,
    pub direction_patterns: BTreeMap<String, DirectionsSummary>,
    pub shapes_polyline: BTreeMap<String, String>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
    pub onestop_feed_id: String,
    pub bounding_box: Option<geo::Rect<f64>>,

    pub connecting_routes: Option<BTreeMap<String, BTreeMap<String, catenary::models::Route>>>, //chateau -> route_id -> Route
    pub connections_per_stop: Option<BTreeMap<String, BTreeMap<String, Vec<String>>>>, //stop_id -> chateau -> route_ids
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DirectionsSummary {
    pub direction_pattern: catenary::models::DirectionPatternMeta,
    pub rows: Vec<catenary::models::DirectionPatternRow>,
}

#[derive(Serialize, Deserialize)]
struct QueryRouteInfo {
    pub chateau: String,
    pub route_id: String,
}

#[actix_web::get("/route_info")]
pub async fn route_info(
    query: web::Query<QueryRouteInfo>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
) -> impl Responder {
    let query = query.into_inner();

    //connect to etcd

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

    //connect to postgres
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    let route_id = urlencoding::decode(&query.route_id).unwrap();

    // fetch route information

    let route_information_pg: Result<Vec<catenary::models::Route>, _> =
        catenary::schema::gtfs::routes::dsl::routes
            .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(&query.chateau))
            .filter(catenary::schema::gtfs::routes::dsl::route_id.eq(&route_id))
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

    match &route.agency_id {
        Some(agency_id) => {
            let agency_pg: Result<Vec<catenary::models::Agency>, _> =
                catenary::schema::gtfs::agencies::dsl::agencies
                    .filter(catenary::schema::gtfs::agencies::dsl::chateau.eq(&query.chateau))
                    .filter(catenary::schema::gtfs::agencies::dsl::agency_id.eq(agency_id))
                    .select(catenary::models::Agency::as_select())
                    .load(conn)
                    .await;

            if let Err(agency_pg) = &agency_pg {
                eprintln!("{}", agency_pg);
                return HttpResponse::InternalServerError()
                    .body("Could not fetch agency information");
            }

            let agency_pg = agency_pg.unwrap();

            if !agency_pg.is_empty() {
                agency = Some(agency_pg[0].clone());
            }
        }
        None => {
            let agency_pg: Result<Vec<catenary::models::Agency>, _> =
                catenary::schema::gtfs::agencies::dsl::agencies
                    .filter(
                        catenary::schema::gtfs::agencies::dsl::static_onestop_id
                            .eq(&route.onestop_feed_id),
                    )
                    .select(catenary::models::Agency::as_select())
                    .load(conn)
                    .await;

            if let Err(agency_pg) = &agency_pg {
                eprintln!("{}", agency_pg);
                return HttpResponse::InternalServerError()
                    .body("Could not fetch agency information");
            }

            let agency_pg = agency_pg.unwrap();

            if !agency_pg.is_empty() {
                agency = Some(agency_pg[0].clone());
            }
        }
    };

    // fetch directions

    let direction_patterns_pg: Vec<catenary::models::DirectionPatternMeta> =
        catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
            .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::chateau.eq(&query.chateau))
            .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::route_id.eq(&route_id))
            .select(catenary::models::DirectionPatternMeta::as_select())
            .load(conn)
            .await
            .unwrap();

    let mut list_of_direction_patterns = direction_patterns_pg
        .iter()
        .map(|x| x.direction_pattern_id.clone())
        .collect::<Vec<String>>();

    list_of_direction_patterns.sort();

    list_of_direction_patterns.dedup();

    let mut list_of_shape_ids = direction_patterns_pg
        .iter()
        .filter_map(|x| x.gtfs_shape_id.clone())
        .collect::<Vec<String>>();

    list_of_shape_ids.sort();

    list_of_shape_ids.dedup();

    let direction_rows = catenary::schema::gtfs::direction_pattern::table
        .filter(catenary::schema::gtfs::direction_pattern::dsl::chateau.eq(&query.chateau))
        .filter(
            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern_id
                .eq_any(&list_of_direction_patterns),
        )
        .select(catenary::models::DirectionPatternRow::as_select())
        .load(conn)
        .await
        .unwrap();

    let directions_zipped: BTreeMap<String, DirectionsSummary> = {
        let direction_meta_hashmap: HashMap<String, DirectionPatternMeta> = direction_patterns_pg
            .into_iter()
            .map(|each_direction_metadata| {
                (
                    each_direction_metadata.direction_pattern_id.clone(),
                    each_direction_metadata,
                )
            })
            .collect();

        let direction_rows_hashmap: HashMap<String, Vec<catenary::models::DirectionPatternRow>> = {
            let mut temp: HashMap<String, Vec<DirectionPatternRow>> = HashMap::new();

            for row in &direction_rows {
                temp.entry(row.direction_pattern_id.clone())
                    .and_modify(|x| x.push(row.clone()))
                    .or_insert(vec![row.clone()]);
            }

            temp
        };

        let mut combined_hashmap: BTreeMap<String, DirectionsSummary> = BTreeMap::new();

        for (id, meta_direction) in direction_meta_hashmap {
            combined_hashmap.insert(
                id.clone(),
                DirectionsSummary {
                    direction_pattern: meta_direction,
                    rows: match direction_rows_hashmap.get(&id) {
                        Some(a) => a.clone(),
                        None => vec![],
                    },
                },
            );
        }

        combined_hashmap
    };

    // fetch stops
    let list_of_stop_ids = direction_rows
        .iter()
        .map(|x| x.stop_id.clone())
        .collect::<HashSet<CompactString>>();

    let stops_pg: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&list_of_stop_ids))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await
        .unwrap();

    let known_transfers_same_stop: AHashMap<String, Vec<String>> = stops_pg
        .iter()
        .map(|stop| {
            (
                stop.gtfs_id.clone(),
                stop.routes
                    .clone()
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<String>>(),
            )
        })
        .collect::<AHashMap<String, Vec<String>>>();

    let mut stops_hashmap: HashMap<String, SerializableStop> = HashMap::new();

    let parent_stops: HashSet<String> = stops_pg
        .iter()
        .filter_map(|x| x.parent_station.clone())
        .collect();

    let mut additional_routes_to_lookup: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for stop in stops_pg {
        let lat = stop.point.map(|x| x.y);
        let lon = stop.point.map(|x| x.x);

        additional_routes_to_lookup
            .entry(stop.chateau.clone())
            .or_insert(BTreeSet::new())
            .extend(
                stop.routes
                    .iter()
                    .filter_map(|x| x.clone())
                    .collect::<BTreeSet<String>>(),
            );

        stops_hashmap.insert(
            stop.gtfs_id.clone(),
            catenary::SerializableStop {
                id: stop.gtfs_id.clone(),
                name: stop.name,
                code: stop.code,
                description: stop.gtfs_desc,
                latitude: lat,
                longitude: lon,
                location_type: stop.location_type,
                parent_station: stop.parent_station,
                timezone: stop.timezone,
                zone_id: stop.zone_id,
                routes: stop.routes.iter().filter_map(|x| x.clone()).collect(),
                platform_code: stop.platform_code,
                level_id: stop.level_id,
            },
        );
    }

    let parent_stops_pg: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&parent_stops))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await
        .unwrap();

    for stop in parent_stops_pg {
        let lat = stop.point.map(|x| x.y);
        let lon = stop.point.map(|x| x.x);

        additional_routes_to_lookup
            .entry(stop.chateau.clone())
            .or_insert(BTreeSet::new())
            .extend(
                stop.routes
                    .iter()
                    .filter_map(|x| x.clone())
                    .collect::<BTreeSet<String>>(),
            );

        stops_hashmap.insert(
            stop.gtfs_id.clone(),
            catenary::SerializableStop {
                id: stop.gtfs_id.clone(),
                name: stop.name,
                code: stop.code,
                description: stop.gtfs_desc,
                latitude: lat,
                longitude: lon,
                location_type: stop.location_type,
                parent_station: stop.parent_station,
                timezone: stop.timezone,
                zone_id: stop.zone_id,
                routes: stop.routes.iter().filter_map(|x| x.clone()).collect(),
                platform_code: stop.platform_code,
                level_id: stop.level_id,
            },
        );
    }

    // --- compute close connections / transfers via shared helper ---

    // base stops (for this route) with coordinates
    let stop_positions: Vec<(String, f64, f64)> = stops_hashmap
        .iter()
        .filter_map(|(stop_id, stop)| match (stop.latitude, stop.longitude) {
            (Some(lat), Some(lon)) => Some((stop_id.clone(), lat, lon)),
            _ => None,
        })
        .collect();

    let (response_connecting_routes, connections_per_stop) = if stop_positions.is_empty() {
        (BTreeMap::new(), BTreeMap::new())
    } else {
        // unwrap inner Arc<CatenaryPostgresPool> from web::Data
        let pool_arc: Arc<CatenaryPostgresPool> = pool.get_ref().clone();

        let connections_info = connections_lookup(
            &query.chateau,
            &route.route_id,
            route.route_type,
            stop_positions,
            additional_routes_to_lookup,
            Some(known_transfers_same_stop),
            pool_arc,
        )
        .await;

        (
            connections_info.connecting_routes,
            connections_info.connections_per_stop,
        )
    };

    // --- end connections lookup ---

    // fetch shapes

    let shapes_pg: Vec<catenary::models::Shape> = catenary::schema::gtfs::shapes::dsl::shapes
        .filter(catenary::schema::gtfs::shapes::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::shapes::dsl::shape_id.eq_any(&list_of_shape_ids))
        .select(catenary::models::Shape::as_select())
        .load(conn)
        .await
        .unwrap();

    //query realtime data pool for alerts

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", &query.chateau).as_str(),
            None,
        )
        .await;

    let mut alerts_for_route_send: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut alert_ids = vec![];

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        let fetch_assigned_node_for_this_chateau_kv_first =
            fetch_assigned_node_for_this_chateau.kvs().first();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = catenary::bincode_deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            if let Ok(aspen_client) = aspen_client {
                let alerts_for_route = aspen_client
                    .get_alerts_from_route_id(
                        context::current(),
                        query.chateau.clone(),
                        route.route_id.clone(),
                    )
                    .await;

                if let Ok(Some(alerts_for_route)) = alerts_for_route {
                    for (alert_id, alert) in alerts_for_route {
                        alert_ids.push(alert_id.clone());
                        alerts_for_route_send.insert(alert_id.clone(), alert);
                    }
                }

                let alerts_for_stops = aspen_client
                    .get_alert_from_stop_ids(
                        context::current(),
                        query.chateau.clone(),
                        list_of_stop_ids.iter().map(|x| x.to_string()).collect(),
                    )
                    .await;

                if let Ok(Some(alerts_for_stops)) = alerts_for_stops {
                    for (alert_id, alert) in alerts_for_stops.alerts {
                        alert_ids.push(alert_id.clone());
                    }

                    for (stop_id, alert_ids) in alerts_for_stops.stops_to_alert_ids {
                        stop_id_to_alert_ids.insert(
                            stop_id.clone(),
                            alert_ids.iter().cloned().collect::<Vec<_>>(),
                        );
                    }
                }
            }
        }
    }

    let mut min_x = f64::MAX;
    let mut max_x = f64::MIN;
    let mut min_y = f64::MAX;
    let mut max_y = f64::MIN;

    for shape in &shapes_pg {
        for point in &shape.linestring.points {
            min_x = min_x.min(point.x);
            max_x = max_x.max(point.x);
            min_y = min_y.min(point.y);
            max_y = max_y.max(point.y);
        }
    }

    for stop in stops_hashmap.values() {
        if let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) {
            min_x = min_x.min(lon);
            max_x = max_x.max(lon);
            min_y = min_y.min(lat);
            max_y = max_y.max(lat);
        }
    }

    let bounding_box = if min_x == f64::MAX {
        None
    } else {
        Some(geo::Rect::new(
            coord! { x: min_x, y: min_y },
            coord! { x: max_x, y: max_y },
        ))
    };

    //return as struct
    //pdf is none for now

    let response = RouteInfoResponse {
        agency_name: agency.map(|x| x.agency_name.clone()),
        agency_id: route.agency_id,
        short_name: route.short_name,
        long_name: route.long_name,
        url: route.url,
        color: route.color,
        text_color: route.text_color,
        route_type: route.route_type,
        onestop_feed_id: route.onestop_feed_id,
        pdf_url: None,
        stops: stops_hashmap,
        direction_patterns: directions_zipped,
        shapes_polyline: shapes_pg
            .iter()
            .map(|x| {
                (
                    x.shape_id.clone(),
                    polyline::encode_coordinates(
                        geo::LineString::new(
                            x.linestring
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
                        5,
                    )
                    .unwrap(),
                )
            })
            .collect(),
        alert_ids_for_this_route: alert_ids,
        alert_id_to_alert: alerts_for_route_send,
        stop_id_to_alert_ids,
        bounding_box: bounding_box,
        connecting_routes: Some(response_connecting_routes),
        connections_per_stop: if connections_per_stop.is_empty() {
            None
        } else {
            Some(connections_per_stop)
        },
    };

    HttpResponse::Ok().json(response)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RouteInfoResponseV2 {
    pub agency_name: Option<String>,
    pub agency_id: Option<String>,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub url: Option<String>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub route_type: i16,
    pub pdf_url: Option<String>,
    pub stops: HashMap<String, SerializableStop>,
    pub direction_patterns: BTreeMap<String, DirectionsSummary>,
    pub shape_ids: Vec<String>,
    pub alert_ids_for_this_route: Vec<String>,
    pub alert_id_to_alert: BTreeMap<String, AspenisedAlert>,
    pub stop_id_to_alert_ids: BTreeMap<String, Vec<String>>,
    pub onestop_feed_id: String,
    pub bounding_box: Option<geo::Rect<f64>>,

    pub connecting_routes: Option<BTreeMap<String, BTreeMap<String, catenary::models::Route>>>, //chateau -> route_id -> Route
    pub connections_per_stop: Option<BTreeMap<String, BTreeMap<String, Vec<String>>>>, //stop_id -> chateau -> route_ids
}

#[actix_web::get("/route_info_v2")]
pub async fn route_info_v2(
    query: web::Query<QueryRouteInfo>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
) -> impl Responder {
    let query = query.into_inner();

    //connect to etcd

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

    //connect to postgres
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("{}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    let route_id = urlencoding::decode(&query.route_id).unwrap();

    // fetch route information

    let route_information_pg: Result<Vec<catenary::models::Route>, _> =
        catenary::schema::gtfs::routes::dsl::routes
            .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(&query.chateau))
            .filter(catenary::schema::gtfs::routes::dsl::route_id.eq(&route_id))
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

    match &route.agency_id {
        Some(agency_id) => {
            let agency_pg: Result<Vec<catenary::models::Agency>, _> =
                catenary::schema::gtfs::agencies::dsl::agencies
                    .filter(catenary::schema::gtfs::agencies::dsl::chateau.eq(&query.chateau))
                    .filter(catenary::schema::gtfs::agencies::dsl::agency_id.eq(agency_id))
                    .select(catenary::models::Agency::as_select())
                    .load(conn)
                    .await;

            if let Err(agency_pg) = &agency_pg {
                eprintln!("{}", agency_pg);
                return HttpResponse::InternalServerError()
                    .body("Could not fetch agency information");
            }

            let agency_pg = agency_pg.unwrap();

            if !agency_pg.is_empty() {
                agency = Some(agency_pg[0].clone());
            }
        }
        None => {
            let agency_pg: Result<Vec<catenary::models::Agency>, _> =
                catenary::schema::gtfs::agencies::dsl::agencies
                    .filter(
                        catenary::schema::gtfs::agencies::dsl::static_onestop_id
                            .eq(&route.onestop_feed_id),
                    )
                    .select(catenary::models::Agency::as_select())
                    .load(conn)
                    .await;

            if let Err(agency_pg) = &agency_pg {
                eprintln!("{}", agency_pg);
                return HttpResponse::InternalServerError()
                    .body("Could not fetch agency information");
            }

            let agency_pg = agency_pg.unwrap();

            if !agency_pg.is_empty() {
                agency = Some(agency_pg[0].clone());
            }
        }
    };

    // fetch directions

    let direction_patterns_pg: Vec<catenary::models::DirectionPatternMeta> =
        catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
            .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::chateau.eq(&query.chateau))
            .filter(catenary::schema::gtfs::direction_pattern_meta::dsl::route_id.eq(&route_id))
            .select(catenary::models::DirectionPatternMeta::as_select())
            .load(conn)
            .await
            .unwrap();

    let mut list_of_direction_patterns = direction_patterns_pg
        .iter()
        .map(|x| x.direction_pattern_id.clone())
        .collect::<Vec<String>>();

    list_of_direction_patterns.sort();

    list_of_direction_patterns.dedup();

    let mut list_of_shape_ids = direction_patterns_pg
        .iter()
        .filter_map(|x| x.gtfs_shape_id.clone())
        .collect::<Vec<String>>();

    list_of_shape_ids.sort();

    list_of_shape_ids.dedup();

    let direction_rows = catenary::schema::gtfs::direction_pattern::table
        .filter(catenary::schema::gtfs::direction_pattern::dsl::chateau.eq(&query.chateau))
        .filter(
            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern_id
                .eq_any(&list_of_direction_patterns),
        )
        .select(catenary::models::DirectionPatternRow::as_select())
        .load(conn)
        .await
        .unwrap();

    let directions_zipped: BTreeMap<String, DirectionsSummary> = {
        let direction_meta_hashmap: HashMap<String, DirectionPatternMeta> = direction_patterns_pg
            .into_iter()
            .map(|each_direction_metadata| {
                (
                    each_direction_metadata.direction_pattern_id.clone(),
                    each_direction_metadata,
                )
            })
            .collect();

        let direction_rows_hashmap: HashMap<String, Vec<catenary::models::DirectionPatternRow>> = {
            let mut temp: HashMap<String, Vec<DirectionPatternRow>> = HashMap::new();

            for row in &direction_rows {
                temp.entry(row.direction_pattern_id.clone())
                    .and_modify(|x| x.push(row.clone()))
                    .or_insert(vec![row.clone()]);
            }

            temp
        };

        let mut combined_hashmap: BTreeMap<String, DirectionsSummary> = BTreeMap::new();

        for (id, meta_direction) in direction_meta_hashmap {
            combined_hashmap.insert(
                id.clone(),
                DirectionsSummary {
                    direction_pattern: meta_direction,
                    rows: match direction_rows_hashmap.get(&id) {
                        Some(a) => a.clone(),
                        None => vec![],
                    },
                },
            );
        }

        combined_hashmap
    };

    // fetch stops
    let list_of_stop_ids = direction_rows
        .iter()
        .map(|x| x.stop_id.clone())
        .collect::<HashSet<CompactString>>();

    let stops_pg: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&list_of_stop_ids))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await
        .unwrap();

    let known_transfers_same_stop: AHashMap<String, Vec<String>> = stops_pg
        .iter()
        .map(|stop| {
            (
                stop.gtfs_id.clone(),
                stop.routes
                    .clone()
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<String>>(),
            )
        })
        .collect::<AHashMap<String, Vec<String>>>();

    let mut stops_hashmap: HashMap<String, SerializableStop> = HashMap::new();

    let parent_stops: HashSet<String> = stops_pg
        .iter()
        .filter_map(|x| x.parent_station.clone())
        .collect();

    let mut additional_routes_to_lookup: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for stop in stops_pg {
        let lat = stop.point.map(|x| x.y);
        let lon = stop.point.map(|x| x.x);

        additional_routes_to_lookup
            .entry(stop.chateau.clone())
            .or_insert(BTreeSet::new())
            .extend(
                stop.routes
                    .iter()
                    .filter_map(|x| x.clone())
                    .collect::<BTreeSet<String>>(),
            );

        stops_hashmap.insert(
            stop.gtfs_id.clone(),
            catenary::SerializableStop {
                id: stop.gtfs_id.clone(),
                name: stop.name,
                code: stop.code,
                description: stop.gtfs_desc,
                latitude: lat,
                longitude: lon,
                location_type: stop.location_type,
                parent_station: stop.parent_station,
                timezone: stop.timezone,
                zone_id: stop.zone_id,
                routes: stop.routes.iter().filter_map(|x| x.clone()).collect(),
                platform_code: stop.platform_code,
                level_id: stop.level_id,
            },
        );
    }

    let parent_stops_pg: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&parent_stops))
        .select(catenary::models::Stop::as_select())
        .load(conn)
        .await
        .unwrap();

    for stop in parent_stops_pg {
        let lat = stop.point.map(|x| x.y);
        let lon = stop.point.map(|x| x.x);

        additional_routes_to_lookup
            .entry(stop.chateau.clone())
            .or_insert(BTreeSet::new())
            .extend(
                stop.routes
                    .iter()
                    .filter_map(|x| x.clone())
                    .collect::<BTreeSet<String>>(),
            );

        stops_hashmap.insert(
            stop.gtfs_id.clone(),
            catenary::SerializableStop {
                id: stop.gtfs_id.clone(),
                name: stop.name,
                code: stop.code,
                description: stop.gtfs_desc,
                latitude: lat,
                longitude: lon,
                location_type: stop.location_type,
                parent_station: stop.parent_station,
                timezone: stop.timezone,
                zone_id: stop.zone_id,
                routes: stop.routes.iter().filter_map(|x| x.clone()).collect(),
                platform_code: stop.platform_code,
                level_id: stop.level_id,
            },
        );
    }

    // --- compute close connections / transfers via shared helper ---

    // base stops (for this route) with coordinates
    let stop_positions: Vec<(String, f64, f64)> = stops_hashmap
        .iter()
        .filter_map(|(stop_id, stop)| match (stop.latitude, stop.longitude) {
            (Some(lat), Some(lon)) => Some((stop_id.clone(), lat, lon)),
            _ => None,
        })
        .collect();

    let (response_connecting_routes, connections_per_stop) = if stop_positions.is_empty() {
        (BTreeMap::new(), BTreeMap::new())
    } else {
        // unwrap inner Arc<CatenaryPostgresPool> from web::Data
        let pool_arc: Arc<CatenaryPostgresPool> = pool.get_ref().clone();

        let connections_info = connections_lookup(
            &query.chateau,
            &route.route_id,
            route.route_type,
            stop_positions,
            additional_routes_to_lookup,
            Some(known_transfers_same_stop),
            pool_arc,
        )
        .await;

        (
            connections_info.connecting_routes,
            connections_info.connections_per_stop,
        )
    };

    // --- end connections lookup ---

    // fetch shapes (lightweight)

    let shapes_pg: Vec<catenary::models::Shape> = catenary::schema::gtfs::shapes::dsl::shapes
        .filter(catenary::schema::gtfs::shapes::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::shapes::dsl::shape_id.eq_any(&list_of_shape_ids))
        .select(catenary::models::Shape::as_select())
        .load(conn)
        .await
        .unwrap();

    //query realtime data pool for alerts

    let fetch_assigned_node_for_this_chateau = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", &query.chateau).as_str(),
            None,
        )
        .await;

    let mut alerts_for_route_send: BTreeMap<String, AspenisedAlert> = BTreeMap::new();
    let mut stop_id_to_alert_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut alert_ids = vec![];

    if let Ok(fetch_assigned_node_for_this_chateau) = fetch_assigned_node_for_this_chateau {
        let fetch_assigned_node_for_this_chateau_kv_first =
            fetch_assigned_node_for_this_chateau.kvs().first();

        if let Some(fetch_assigned_node_for_this_chateau_data) =
            fetch_assigned_node_for_this_chateau_kv_first
        {
            let assigned_chateau_data = catenary::bincode_deserialize::<ChateauMetadataEtcd>(
                fetch_assigned_node_for_this_chateau_data.value(),
            )
            .unwrap();

            let aspen_client =
                catenary::aspen::lib::spawn_aspen_client_from_ip(&assigned_chateau_data.socket)
                    .await;

            if let Ok(aspen_client) = aspen_client {
                let alerts_for_route = aspen_client
                    .get_alerts_from_route_id(
                        context::current(),
                        query.chateau.clone(),
                        route.route_id.clone(),
                    )
                    .await;

                if let Ok(Some(alerts_for_route)) = alerts_for_route {
                    for (alert_id, alert) in alerts_for_route {
                        alert_ids.push(alert_id.clone());
                        alerts_for_route_send.insert(alert_id.clone(), alert);
                    }
                }

                let alerts_for_stops = aspen_client
                    .get_alert_from_stop_ids(
                        context::current(),
                        query.chateau.clone(),
                        list_of_stop_ids.iter().map(|x| x.to_string()).collect(),
                    )
                    .await;

                if let Ok(Some(alerts_for_stops)) = alerts_for_stops {
                    for (alert_id, alert) in alerts_for_stops.alerts {
                        alert_ids.push(alert_id.clone());
                    }

                    for (stop_id, alert_ids) in alerts_for_stops.stops_to_alert_ids {
                        stop_id_to_alert_ids.insert(
                            stop_id.clone(),
                            alert_ids.iter().cloned().collect::<Vec<_>>(),
                        );
                    }
                }
            }
        }
    }

    let mut min_x = f64::MAX;
    let mut max_x = f64::MIN;
    let mut min_y = f64::MAX;
    let mut max_y = f64::MIN;

    for shape in &shapes_pg {
        for point in &shape.linestring.points {
            min_x = min_x.min(point.x);
            max_x = max_x.max(point.x);
            min_y = min_y.min(point.y);
            max_y = max_y.max(point.y);
        }
    }

    for stop in stops_hashmap.values() {
        if let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) {
            min_x = min_x.min(lon);
            max_x = max_x.max(lon);
            min_y = min_y.min(lat);
            max_y = max_y.max(lat);
        }
    }

    let bounding_box = if min_x == f64::MAX {
        None
    } else {
        Some(geo::Rect::new(
            coord! { x: min_x, y: min_y },
            coord! { x: max_x, y: max_y },
        ))
    };

    //return as struct
    //pdf is none for now

    let response = RouteInfoResponseV2 {
        agency_name: agency.map(|x| x.agency_name.clone()),
        agency_id: route.agency_id,
        short_name: route.short_name,
        long_name: route.long_name,
        url: route.url,
        color: route.color,
        text_color: route.text_color,
        route_type: route.route_type,
        onestop_feed_id: route.onestop_feed_id,
        pdf_url: None,
        stops: stops_hashmap,
        direction_patterns: directions_zipped,
        shape_ids: list_of_shape_ids,
        alert_ids_for_this_route: alert_ids,
        alert_id_to_alert: alerts_for_route_send,
        stop_id_to_alert_ids,
        bounding_box: bounding_box,
        connecting_routes: Some(response_connecting_routes),
        connections_per_stop: if connections_per_stop.is_empty() {
            None
        } else {
            Some(connections_per_stop)
        },
    };

    HttpResponse::Ok().json(response)
}
