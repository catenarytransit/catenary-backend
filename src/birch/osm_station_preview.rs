// Copyright
// Catenary Transit Initiatives
// OSM Station Preview Endpoint
// Attribution cannot be removed

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
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

use crate::stop_preview::StopDeserialised;

#[derive(Deserialize)]
pub struct OsmStationPreviewParams {
    pub osm_station_id: i64,
}

#[derive(Serialize, Clone)]
pub struct OsmStationInfo {
    pub osm_id: i64,
    pub osm_type: String,
    pub name: Option<String>,
    pub name_translations: Option<serde_json::Value>,
    pub station_type: Option<String>,
    pub railway_tag: Option<String>,
    pub mode_type: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Serialize, Clone)]
struct OsmStationPreviewResponse {
    osm_station: Option<OsmStationInfo>,
    stops: BTreeMap<String, BTreeMap<String, StopDeserialised>>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
}

#[actix_web::get("/osm_station_preview")]
pub async fn osm_station_preview(
    req: HttpRequest,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: Query<OsmStationPreviewParams>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    let conn = match conn_pre {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Database connection error: {:?}", e);
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Database connection failed"}));
        }
    };

    let mut conn = conn;

    // Fetch OSM station info
    let osm_station_result: diesel::prelude::QueryResult<Vec<catenary::models::OsmStation>> =
        catenary::schema::gtfs::osm_stations::dsl::osm_stations
            .filter(catenary::schema::gtfs::osm_stations::osm_id.eq(query.osm_station_id))
            .select(catenary::models::OsmStation::as_select())
            .load::<catenary::models::OsmStation>(&mut conn)
            .await;

    let osm_station_info = match &osm_station_result {
        Ok(stations) if !stations.is_empty() => {
            let station = &stations[0];
            Some(OsmStationInfo {
                osm_id: station.osm_id,
                osm_type: station.osm_type.clone(),
                name: station.name.clone(),
                name_translations: station.name_translations.clone(),
                station_type: station.station_type.clone(),
                railway_tag: station.railway_tag.clone(),
                mode_type: station.mode_type.clone(),
                lat: station.point.y,
                lon: station.point.x,
            })
        }
        _ => None,
    };

    // Find all GTFS stops linked to this OSM station
    let stops_result: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
        catenary::schema::gtfs::stops::dsl::stops
            .filter(catenary::schema::gtfs::stops::osm_station_id.eq(query.osm_station_id))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(&mut conn)
            .await;

    let linked_stops = match stops_result {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Stop query error: {:?}", e);
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Failed to query stops"}));
        }
    };

    if linked_stops.is_empty() {
        return HttpResponse::Ok().json(OsmStationPreviewResponse {
            osm_station: osm_station_info,
            stops: BTreeMap::new(),
            routes: BTreeMap::new(),
        });
    }

    // Group stops by chateau
    let mut all_stops_chateau_groups: BTreeMap<String, BTreeMap<String, StopDeserialised>> =
        BTreeMap::new();
    let mut routes_to_query_by_chateau: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut children_stops_to_query_by_chateau: BTreeMap<String, BTreeSet<String>> =
        BTreeMap::new();

    for stop in linked_stops {
        let chateau = stop.chateau.clone();
        let stop_deserialised = StopDeserialised {
            gtfs_id: stop.gtfs_id.clone(),
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
            osm_station_id: stop.osm_station_id.map(|id| id.to_string()),
            name_translations: catenary::serde_value_to_translated_hashmap(&stop.name_translations),
        };

        // Track routes to query
        for route_id in &stop_deserialised.routes {
            routes_to_query_by_chateau
                .entry(chateau.clone())
                .or_insert(BTreeSet::new())
                .insert(route_id.clone());
        }

        // Track children to query
        for child_id in &stop_deserialised.children_ids {
            children_stops_to_query_by_chateau
                .entry(chateau.clone())
                .or_insert(BTreeSet::new())
                .insert(child_id.clone());
        }

        all_stops_chateau_groups
            .entry(chateau)
            .or_insert(BTreeMap::new())
            .insert(stop.gtfs_id, stop_deserialised);
    }

    // Query children stops for each chateau
    for (chateau, children_ids) in children_stops_to_query_by_chateau.iter() {
        // Filter out children already in the map
        let existing_stop_ids: BTreeSet<String> = all_stops_chateau_groups
            .get(chateau)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        let missing_children: Vec<String> = children_ids
            .iter()
            .filter(|id| !existing_stop_ids.contains(*id))
            .cloned()
            .collect();

        if missing_children.is_empty() {
            continue;
        }

        let queried_children = catenary::schema::gtfs::stops::table
            .filter(catenary::schema::gtfs::stops::chateau.eq(chateau))
            .filter(catenary::schema::gtfs::stops::gtfs_id.eq_any(&missing_children))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(&mut conn)
            .await;

        if let Ok(children) = queried_children {
            for stop in children {
                let stop_deserialised = StopDeserialised {
                    gtfs_id: stop.gtfs_id.clone(),
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
                    osm_station_id: stop.osm_station_id.map(|id| id.to_string()),
                    name_translations: catenary::serde_value_to_translated_hashmap(
                        &stop.name_translations,
                    ),
                };

                // Add routes from children to query list
                for route_id in &stop_deserialised.routes {
                    routes_to_query_by_chateau
                        .entry(chateau.clone())
                        .or_insert(BTreeSet::new())
                        .insert(route_id.clone());
                }

                // Add child's routes to parent stop
                if let Some(parent_station) = &stop.parent_station {
                    if let Some(chateau_stops) = all_stops_chateau_groups.get_mut(chateau) {
                        if let Some(parent_stop) = chateau_stops.get_mut(parent_station) {
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
                }

                all_stops_chateau_groups
                    .entry(chateau.clone())
                    .or_insert(BTreeMap::new())
                    .insert(stop.gtfs_id, stop_deserialised);
            }
        }
    }

    // Query routes
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
        if let Ok((chateau, route_map)) = result {
            all_routes_chateau_groups.insert(chateau, route_map);
        }
    }

    let response = OsmStationPreviewResponse {
        osm_station: osm_station_info,
        stops: all_stops_chateau_groups,
        routes: all_routes_chateau_groups,
    };

    HttpResponse::Ok().json(response)
}
