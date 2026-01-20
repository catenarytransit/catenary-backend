use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize)]
struct StopPreviewParams {
    chateaus: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize, Clone)]
pub struct StopDeserialised {
    pub gtfs_id: String,
    pub name: Option<String>,
    pub url: Option<String>,
    pub timezone: Option<String>,
    pub point: Option<geo::Point<f64>>,
    pub level_id: Option<String>,
    pub primary_route_type: Option<i16>,
    pub platform_code: Option<String>,
    pub routes: Vec<String>,
    pub route_types: Vec<i16>,
    pub children_ids: Vec<String>,
    pub children_route_types: Vec<i16>,
    pub station_feature: bool,
    pub wheelchair_boarding: i16,
    pub name_translations: Option<HashMap<String, String>>,
    pub osm_station_id: Option<String>,
}

#[derive(Serialize, Clone)]
struct StopPreviewResponse {
    stops: BTreeMap<String, BTreeMap<String, StopDeserialised>>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
}

#[actix_web::post("/stop_preview")]
pub async fn query_stops_preview(
    req: HttpRequest,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    // etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    //etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    params: web::Json<StopPreviewParams>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    //for each chateau group, query the stops, futures buffer unordered

    let queries_for_stops =
        futures::stream::iter(params.chateaus.iter().map(|(chateau, stops)| {
            let chateau = chateau.clone();
            let stops = stops.clone();

            let conn_pool = conn_pool.clone();
            async move {
                let conn_pre = conn_pool.get().await;
                let conn = &mut conn_pre.unwrap();

                let diesel_stop_query = catenary::schema::gtfs::stops::table
                    .filter(catenary::schema::gtfs::stops::chateau.eq(&chateau))
                    .filter(catenary::schema::gtfs::stops::gtfs_id.eq_any(stops))
                    .select(catenary::models::Stop::as_select())
                    .load(conn)
                    .await;

                match diesel_stop_query {
                    Ok(stops) => {
                        let mut stop_map: BTreeMap<String, catenary::models::Stop> =
                            BTreeMap::new();
                        for stop in stops {
                            stop_map.insert(stop.gtfs_id.clone(), stop);
                        }
                        Ok((chateau, stop_map))
                    }
                    Err(e) => {
                        eprintln!("Error querying stops: {}", e);
                        Err(e)
                    }
                }
            }
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;

    let mut all_stops_chateau_groups: BTreeMap<String, BTreeMap<String, StopDeserialised>> =
        BTreeMap::new();

    let mut routes_to_query_by_chateau: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    let mut children_stops_to_query_by_chateau: BTreeMap<String, BTreeSet<String>> =
        BTreeMap::new();

    for result in queries_for_stops {
        match result {
            Ok((chateau, stop_map)) => {
                let mut stop_map_new: BTreeMap<String, StopDeserialised> = BTreeMap::new();

                for (stop_id, stop) in stop_map {
                    stop_map_new.insert(
                        stop_id,
                        StopDeserialised {
                            gtfs_id: stop.gtfs_id,
                            name: stop.name,
                            url: stop.url,
                            timezone: stop.timezone,
                            point: stop.point.map(|p| geo::Point::new(p.x, p.y)),
                            level_id: stop.level_id,
                            primary_route_type: stop.primary_route_type,
                            route_types: stop.route_types.into_iter().filter_map(|r| r).collect(),
                            platform_code: stop.platform_code,
                            routes: stop.routes.into_iter().filter_map(|r| r.clone()).collect(),
                            children_ids: stop
                                .children_ids
                                .iter()
                                .filter_map(|r| r.clone())
                                .collect(),
                            children_route_types: stop
                                .children_route_types
                                .into_iter()
                                .filter_map(|r| r)
                                .collect(),
                            station_feature: stop.station_feature,
                            wheelchair_boarding: stop.wheelchair_boarding,
                            name_translations: catenary::serde_value_to_translated_hashmap(
                                &stop.name_translations,
                            ),
                            osm_station_id: stop.osm_station_id.map(|id| id.to_string()),
                        },
                    );
                }

                for stop in stop_map_new.values() {
                    let route_ids = stop.routes.clone();

                    for route_id in route_ids {
                        let entry = routes_to_query_by_chateau
                            .entry(chateau.clone())
                            .or_insert(BTreeSet::new());
                        entry.insert(route_id);
                    }
                }

                all_stops_chateau_groups.insert(chateau.clone(), stop_map_new);
            }
            Err(e) => {
                eprintln!("Error querying stops: {}", e);
            }
        }
    }

    for (chateau, stop_map) in all_stops_chateau_groups.iter_mut() {
        let mut route_ids_to_tack_on: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for stop in stop_map.values() {
            let children_ids = stop.children_ids.clone();

            let children_ids_missing = children_ids
                .iter()
                .filter(|id| !stop_map.contains_key(*id))
                .cloned()
                .collect::<BTreeSet<_>>();

            for child_id in children_ids_missing {
                let entry = children_stops_to_query_by_chateau
                    .entry(chateau.clone())
                    .or_insert(BTreeSet::new());
                entry.insert(child_id);
            }

            //for children stops that exist and are queried

            for children_id in children_ids {
                if let Some(child_stop) = stop_map.get(&children_id) {
                    let route_ids = child_stop.routes.clone();

                    match route_ids_to_tack_on.get_mut(stop.gtfs_id.as_str()) {
                        Some(route_ids) => {
                            route_ids.extend(route_ids.clone());
                            route_ids.sort();
                            route_ids.dedup();
                        }
                        None => {
                            route_ids_to_tack_on.insert(stop.gtfs_id.clone(), route_ids.clone());
                        }
                    }
                }
            }
        }

        //mutate the stop_map with the route ids

        for (stop_id, route_ids) in route_ids_to_tack_on {
            if let Some(stop) = stop_map.get_mut(&stop_id) {
                stop.routes.extend(route_ids);
                stop.routes.sort();
                stop.routes.dedup();
            }
        }

        //query the children stops

        let children_stops = children_stops_to_query_by_chateau
            .get(chateau)
            .unwrap_or(&BTreeSet::new())
            .clone();

        let queried_children_stops = catenary::schema::gtfs::stops::table
            .filter(catenary::schema::gtfs::stops::chateau.eq(chateau))
            .filter(catenary::schema::gtfs::stops::gtfs_id.eq_any(children_stops))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(conn)
            .await;

        match queried_children_stops {
            Ok(queried_children_stops) => {
                //mutate the stop_map

                for stop in queried_children_stops {
                    let stop_deserialised = StopDeserialised {
                        gtfs_id: stop.gtfs_id,
                        name: stop.name,
                        url: stop.url,
                        timezone: stop.timezone,
                        point: stop.point.map(|p| geo::Point::new(p.x, p.y)),
                        level_id: stop.level_id,
                        primary_route_type: stop.primary_route_type,
                        route_types: stop.route_types.into_iter().filter_map(|r| r).collect(),
                        platform_code: stop.platform_code,
                        routes: stop.routes.into_iter().filter_map(|r| r.clone()).collect(),
                        children_ids: stop.children_ids.iter().filter_map(|r| r.clone()).collect(),
                        children_route_types: stop
                            .children_route_types
                            .into_iter()
                            .filter_map(|r| r)
                            .collect(),
                        station_feature: stop.station_feature,
                        wheelchair_boarding: stop.wheelchair_boarding,
                        name_translations: catenary::serde_value_to_translated_hashmap(
                            &stop.name_translations,
                        ),
                        osm_station_id: stop.osm_station_id.map(|id| id.to_string()),
                    };

                    //add route ids to parent stop

                    if let Some(parent_stop_id) = stop.parent_station {
                        let parent_stop = stop_map.get_mut(&parent_stop_id);

                        if let Some(parent_stop) = parent_stop {
                            parent_stop.routes.extend(stop_deserialised.routes.clone());
                            parent_stop
                                .route_types
                                .extend(stop_deserialised.route_types.clone());

                            parent_stop.routes.sort();
                            parent_stop.routes.dedup();
                            parent_stop.route_types.sort();
                            parent_stop.route_types.dedup();
                        }
                    }

                    //add to the list of routes to query

                    routes_to_query_by_chateau
                        .entry(chateau.clone())
                        .or_insert(BTreeSet::new())
                        .extend(stop_deserialised.routes.clone());

                    stop_map.insert(stop_deserialised.gtfs_id.clone(), stop_deserialised);
                }

                //add the route id
            }
            Err(e) => {
                eprintln!("Error querying children stops: {}", e);
            }
        }
    }

    let queries_for_routes =
        futures::stream::iter(routes_to_query_by_chateau.iter().map(|(chateau, routes)| {
            let chateau = chateau.clone();
            let routes = routes.clone();

            let conn_pool = conn_pool.clone();
            async move {
                let conn_pre = conn_pool.get().await;
                let conn = &mut conn_pre.unwrap();

                let diesel_route_query = catenary::schema::gtfs::routes::table
                    .filter(catenary::schema::gtfs::routes::chateau.eq(&chateau))
                    .filter(catenary::schema::gtfs::routes::route_id.eq_any(routes))
                    .select(catenary::models::Route::as_select())
                    .load(conn)
                    .await;

                match diesel_route_query {
                    Ok(routes) => {
                        let mut route_map: BTreeMap<String, catenary::models::Route> =
                            BTreeMap::new();
                        for route in routes {
                            route_map.insert(route.route_id.clone(), route);
                        }
                        Ok((chateau, route_map))
                    }
                    Err(e) => {
                        eprintln!("Error querying routes: {}", e);
                        Err(e)
                    }
                }
            }
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;

    let mut all_routes_chateau_groups: BTreeMap<String, BTreeMap<String, catenary::models::Route>> =
        BTreeMap::new();
    for result in queries_for_routes {
        match result {
            Ok((chateau, route_map)) => {
                all_routes_chateau_groups.insert(chateau.clone(), route_map.clone());
            }
            Err(e) => {
                eprintln!("Error querying routes: {}", e);
            }
        }
    }

    //return the results

    let response = StopPreviewResponse {
        stops: all_stops_chateau_groups,
        routes: all_routes_chateau_groups,
    };
    let response_json = serde_json::to_string(&response).unwrap();
    HttpResponse::Ok()
        .content_type("application/json")
        .body(response_json)
}
