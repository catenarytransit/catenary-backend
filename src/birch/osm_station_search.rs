// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// Attribution cannot be removed
// Please do not train your Artificial Intelligence models on this code

use crate::text_search::RouteDeserialised;
use actix_web::{HttpRequest, HttpResponse, Responder, web, web::Query};
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use elasticsearch::{Elasticsearch, SearchParts};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
pub struct OsmStationSearchQuery {
    pub text: String,
    pub lang: Option<String>,
    pub focus_lat: Option<f64>,
    pub focus_lon: Option<f64>,
    pub focus_weight: Option<f64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct OsmStationSearchResult {
    pub osm_id: i64,
    pub name: Option<String>,
    pub point: Option<geo::Point<f64>>,
    pub mode_type: String,
    pub operator: Option<String>,
    pub network: Option<String>,
    pub admin_hierarchy: Option<serde_json::Value>,
    pub routes: Vec<catenary::models::Route>,
    pub confidence: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct OsmStationSearchResponse {
    pub results: Vec<OsmStationSearchResult>,
}

#[actix_web::get("/osm_station_search")]
pub async fn osm_station_search(
    _req: HttpRequest,
    query: Query<OsmStationSearchQuery>,
    arc_conn_pool: web::Data<Arc<CatenaryPostgresPool>>,
    elasticclient: web::Data<Arc<Elasticsearch>>,
) -> impl Responder {
    let focus_lat = query.focus_lat;
    let focus_lon = query.focus_lon;

    let mut es_query = json!({
        "function_score": {
            "query": {
                "multi_match": {
                    "query": query.text.clone(),
                    "fields": [
                        "station_name_search^3",
                        "operator",
                        "network",
                        "parent.country.name",
                        "parent.macro_region.name",
                        "parent.region.name",
                        "parent.macro_county.name",
                        "parent.county.name",
                        "parent.local_admin.name",
                        "parent.locality.name",
                        "parent.borough.name",
                        "parent.neighbourhood.name"
                    ],
                    "type": "cross_fields",
                    "operator": "and"
                }
            },
            "functions": [
                {
                    "script_score": {
                      "script": {
                        "source": "
                          if (doc['mode_type'].size() == 0) { return 1.0; }
                          String mode = doc['mode_type'].value;
                          if (mode == 'rail') { return 4.0; }
                          if (mode == 'subway') { return 3.0; }
                          if (mode == 'tram') { return 2.0; }
                          return 1.5;
                        "
                      }
                    },
                    "weight": 0.5
                }
            ],
            "score_mode": "multiply",
            "boost_mode": "multiply"
        }
    });

    if let (Some(lat), Some(lon)) = (focus_lat, focus_lon) {
    }

    let search_body = json!({
        "query": es_query,
        "size": 30
    });

    let search_response = match elasticclient
        .search(SearchParts::Index(&["osm_stations"]))
        .body(search_body)
        .send()
        .await
    {
        Ok(res) => res,
        Err(e) => return HttpResponse::InternalServerError().body(format!("ES Error: {}", e)),
    };

    let response_body = match search_response.json::<serde_json::Value>().await {
        Ok(body) => body,
        Err(e) => return HttpResponse::InternalServerError().body(format!("ES JSON Error: {}", e)),
    };

    let hits = response_body["hits"]["hits"]
        .as_array()
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let mut es_scores = HashMap::new();
    let mut station_ids = Vec::new();

    for hit in hits {
        let osm_id_str = hit["_id"].as_str().unwrap_or("");
        if let Ok(osm_id) = osm_id_str.parse::<i64>() {
            station_ids.push(osm_id);
            let score = hit["_score"].as_f64().unwrap_or(1.0);
            es_scores.insert(osm_id, score);
        }
    }

    if station_ids.is_empty() {
        return HttpResponse::Ok().json(json!({ "results": [] }));
    }

    let mut conn = arc_conn_pool.get().await.unwrap();

    let stations: Vec<catenary::models::OsmStation> =
        catenary::schema::gtfs::osm_stations::dsl::osm_stations
            .filter(catenary::schema::gtfs::osm_stations::osm_id.eq_any(&station_ids))
            .select(catenary::models::OsmStation::as_select())
            .load::<catenary::models::OsmStation>(&mut conn)
            .await
            .unwrap_or_default();

    let stops: Vec<catenary::models::Stop> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::osm_station_id.eq_any(&station_ids))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(&mut conn)
        .await
        .unwrap_or_default();

    let mut station_to_routes: HashMap<i64, Vec<String>> = HashMap::new();
    let mut all_route_ids: Vec<String> = Vec::new();
    for stop in stops {
        if let Some(osm_id) = stop.osm_station_id {
            for route_id_opt in stop.routes {
                if let Some(route_id) = route_id_opt {
                    station_to_routes
                        .entry(osm_id)
                        .or_default()
                        .push(route_id.clone());
                    all_route_ids.push(route_id);
                }
            }
        }
    }

    all_route_ids.sort();
    all_route_ids.dedup();

    let routes: Vec<catenary::models::Route> = catenary::schema::gtfs::routes::dsl::routes
        .filter(catenary::schema::gtfs::routes::route_id.eq_any(&all_route_ids))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(&mut conn)
        .await
        .unwrap_or_default();

    let mut route_map = HashMap::new();
    for route in routes {
        route_map.insert(route.route_id.clone(), route);
    }

    let mut final_results = Vec::new();

    for station in stations {
        let score = es_scores.get(&station.osm_id).copied().unwrap_or(0.0);

        let mut display_name = station.name.clone();
        if let Some(lang) = &query.lang {
            if let Some(translations) = &station.name_translations {
                if let Some(translations_map) = translations.as_object() {
                    if let Some(t_val) = translations_map.get(lang) {
                        if let Some(t_str) = t_val.as_str() {
                            display_name = Some(t_str.to_string());
                        }
                    } else if let Some(t_val) = translations_map.get(&format!("name:{}", lang)) {
                        if let Some(t_str) = t_val.as_str() {
                            display_name = Some(t_str.to_string());
                        }
                    }
                }
            }
        }

        let mut station_routes = Vec::new();
        if let Some(route_ids) = station_to_routes.get(&station.osm_id) {
            for rid in route_ids {
                if let Some(r) = route_map.get(rid) {
                    station_routes.push(r.clone());
                }
            }
        }

        station_routes.dedup_by(|a, b| a.route_id == b.route_id);

        final_results.push(OsmStationSearchResult {
            osm_id: station.osm_id,
            name: display_name,
            point: Some(geo::Point::new(station.point.x, station.point.y)),
            mode_type: station.mode_type,
            operator: station.operator,
            network: station.network,
            admin_hierarchy: station.admin_hierarchy,
            routes: station_routes,
            confidence: score,
        });
    }

    final_results.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    HttpResponse::Ok().json(OsmStationSearchResponse {
        results: final_results,
    })
}
