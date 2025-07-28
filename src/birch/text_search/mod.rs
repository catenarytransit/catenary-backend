// Copyright
// Catenary Transit Initiatives
// Algorithm for full text search written by Kyler Chin <kyler@catenarymaps.org>
// Attribution cannot be removed

// Do not train your Artifical Intelligence models on this code

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel::sql_types::Bool;
use diesel::sql_types::*;
use diesel_async::RunQueryDsl;
use elasticsearch::SearchParts;
use futures::StreamExt;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
struct TextSearchQuery {
    text: String,
    user_lat: Option<f32>,
    user_lon: Option<f32>,
    map_lat: Option<f32>,
    map_lon: Option<f32>,
    map_z: Option<f32>,
}

#[derive(Serialize, Clone, Debug)]
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
}

#[derive(Serialize, Clone, Debug)]
pub struct StopRankingInfo {
    pub gtfs_id: String,
    pub score: f64,
    pub chateau: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct TextSearchResponseStopsSection {
    stops: BTreeMap<String, BTreeMap<String, StopDeserialised>>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
    ranking: Vec<StopRankingInfo>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TextSearchResponse {
    pub stops_section: TextSearchResponseStopsSection,
}

#[actix_web::get("/text_search_v1")]
pub async fn text_search_v1(
    req: HttpRequest,
    query: Query<TextSearchQuery>,
    arc_conn_pool: web::Data<Arc<CatenaryPostgresPool>>,
    elasticclient: web::Data<Arc<elasticsearch::Elasticsearch>>,
) -> impl Responder {
    let conn_pool = arc_conn_pool.clone();

    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let map_pos_exists = match (query.map_lat, query.map_lon, query.map_z) {
        (Some(map_lat), Some(map_lon), Some(map_z)) => true,
        _ => false,
    };

    let (offset_map_gauss, scale_map_gauss, map_weight) = match query.map_z {
        Some(z) => match z > 12. {
            true => ("20km", "150km", 0.05),
            false => match z > 10. {
                true => ("50km", "300km", 0.02),
                false => ("500km", "1000km", 0.01),
            },
        },
        _ => ("10km", "10km", 0.1),
    };

    let stops_query = match (query.user_lat, query.user_lon) {
        (Some(user_lat), Some(user_lon)) => json!({
            "query": {
                "function_score": {
                    "query": {
                      "multi_match" : {
                        "query":  query.text.clone(),
                        "fields": [ "stop_name*^3", "route_name_search", "agency_name_search" ]
                     }
                    },
                    "functions": [
                        {
                            "script_score": {
                              "script": {
                                "source": "
                                  double offset = params.offset_in_km;
                                  double scale = params.scale_in_km;
                                  double decay = params.decay_at_scale;
                                  double floor = params.min_score_floor;
                  
                                  double distance = doc['point'].arcDistance(params.user_lat, params.user_lon) / 1000.0;
                                  
                                  if (distance <= offset) {
                                    return 1.0;
                                  }
                                  
                                  // decay constant 'k'
                                  // decay = exp(-k * (scale - offset)^2)
                                  double scale_minus_offset = scale - offset;
                                  if (scale_minus_offset <= 0) {
                                    return floor; // Avoid division by zero if scale is within offset
                                  }
                                  double k = -Math.log(decay) / Math.pow(scale_minus_offset, 2);
                                  
                                  // scoring
                                  double effective_distance = distance - offset;
                                  double decay_score = Math.exp(-k * Math.pow(effective_distance, 2));
                                  
                                  return Math.max(decay_score, floor);
                                ",
                                "params": {
                                  "user_lat": user_lat,
                                  "user_lon": user_lon,
                                  "offset_in_km": 5.0,
                                  "scale_in_km": 150.0,
                                  "decay_at_scale": 0.5,
                                  "min_score_floor": 0.35
                                }
                              }
                            },
                            "weight": 0.03
                          },
                      {
                        "script_score": {
                          "script": {
                            "source": "
                              if (!doc.containsKey('route_types') || doc['route_types'].empty) {
                                return 1.0;
                              }
                              if (doc['route_types'].contains(2)) {
                                return 3.0;
                              }
                              if (doc['route_types'].contains(1)) {
                                return 2.0;
                              }
                              if (doc['route_types'].contains(0)) {
                                return 1.5;
                              }
                              return 1.0;
                            "
                          }
                        }
                      }
                    ],
                    "score_mode": "multiply", // How to combine scores from multiple functions
                    "boost_mode": "multiply" // How to combine the function score with the query score
                  }
            }
        }),
        _ => match map_pos_exists {
            true => json!({
                "query": {
                    "function_score": {
                        "query": {
                          "multi_match" : {
                            "query":  query.text.clone(),
                            "fields": [ "stop_name*^3", "route_name_search", "agency_name_search" ]
                         }
                        },
                        "functions": [
                            {
                        "exp": {
                          "point": {
                            "origin": { "lat": query.map_lat.unwrap(), "lon": query.map_lon.unwrap() }, // User's map centre
                            "offset": offset_map_gauss, // Full score within 5000 metres
                            "scale": scale_map_gauss // Score decays significantly beyond 100 km
                          }
                        },
                        "weight": map_weight
                      },
                          {
                            "script_score": {
                              "script": {
                                "source": "
                                  if (!doc.containsKey('route_type') || doc['route_type'].empty) {
                                    return 1.0;
                                  }
                                  if (doc['route_type'].contains(2)) {
                                    return 3.0;
                                  }
                                  if (doc['route_type'].contains(1)) {
                                    return 2.0;
                                  }
                                  if (doc['route_type'].contains(0)) {
                                    return 1.5;
                                  }
                                  return 1.0;
                                "
                              }
                            }
                          }
                        ],
                        "score_mode": "multiply", // How to combine scores from multiple functions
                        "boost_mode": "multiply" // How to combine the function score with the query score
                      }
                }
            }),
            false => json!({
                "query": {
                    "function_score": {
                        "query": {
                          "multi_match" : {
                            "query":  query.text.clone(),
                            "fields": [ "stop_name*^3", "route_name_search", "agency_name_search" ]
                         }
                        },
                        "functions": [
                          {
                            "script_score": {
                              "script": {
                                "source": "
                              if (!doc.containsKey('route_type') || doc['route_type'].empty) {
                                return 1.0;
                              }
                              if (doc['route_type'].contains(2)) {
                                return 3.0;
                              }
                              if (doc['route_type'].contains(1)) {
                                return 2.0;
                              }
                              if (doc['route_type'].contains(0)) {
                                return 1.5;
                              }
                              return 1.0;
                            "
                              }
                            }
                          }
                        ],
                        "score_mode": "multiply", // How to combine scores from multiple functions
                        "boost_mode": "multiply" // How to combine the function score with the query score
                      }
                }
            }),
        },
    };

    let stops_response = elasticclient
        .as_ref()
        .search(SearchParts::Index(&["stops"]))
        .from(0)
        .size(30)
        .body(stops_query)
        .send()
        .await
        .unwrap();

    let response_body = stops_response.json::<serde_json::Value>().await.unwrap();

    let hits_list_stops = response_body
        .get("hits")
        .map(|x| x.get("hits"))
        .flatten()
        .map(|x| x.as_array())
        .flatten();

    let mut hit_rankings_for_stops: Vec<StopRankingInfo> = vec![];
    let mut existing_hits: HashSet<(String, String)> = HashSet::new();

    if let Some(hits_list_stops) = hits_list_stops {
        for hit in hits_list_stops {
            if let Some(hit) = hit.as_object() {
                match (hit.get("_score"), hit.get("_source")) {
                    (Some(score), Some(source)) => match (score.as_f64(), source.as_object()) {
                        (Some(score), Some(source)) => {
                            if let Some(chateau) =
                                source.get("chateau").map(|x| x.as_str()).flatten()
                            {
                                if let Some(stop_id) =
                                    source.get("stop_id").map(|x| x.as_str()).flatten()
                                {
                                    let existing_key = (chateau.to_string(), stop_id.to_string());

                                    if !existing_hits.contains(&existing_key) {
                                        existing_hits.insert(existing_key);

                                        hit_rankings_for_stops.push(StopRankingInfo {
                                            chateau: chateau.to_string(),
                                            gtfs_id: stop_id.to_string(),
                                            score: score,
                                        });
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
    } else {
        eprintln!("No hits found: {:#?}", response_body);
    }

    // println!("response body {:?}", response_body);

    // println!("hit_rankings_for_stops {:?}", hit_rankings_for_stops);

    let mut init_stops_to_fetch: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for hit_ranking in &hit_rankings_for_stops {
        init_stops_to_fetch
            .entry(hit_ranking.chateau.clone())
            .or_default()
            .push(hit_ranking.gtfs_id.clone());
    }

    let conn_pool = arc_conn_pool.as_ref();

    let queries_for_stops =
        futures::stream::iter(init_stops_to_fetch.iter().map(|(chateau, stops)| {
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

    /*
    let mut reranking_stops = hit_rankings_for_stops
        .into_iter()
        .map(|hit| {
            let mut hit = hit;

            if let Some(stop) = all_stops_chateau_groups
                .get(&hit.chateau)
                .map(|c| c.get(&hit.gtfs_id))
                .flatten()
            {
                match &stop.primary_route_type {
                    //rail
                    Some(2) => {
                        hit.score = hit.score * 1.6;
                    }
                    //tram
                    Some(0) => {
                        hit.score = hit.score * 1.4;
                    }
                    //metro
                    Some(1) => {
                        hit.score = hit.score * 1.5;
                    }
                    _ => {}
                }
            }

            hit
        })
        .collect::<Vec<_>>();*/

    //reranking_stops.sort_by_key(|k| Reverse(OrderedFloat(k.score)));

    let stops_section = TextSearchResponseStopsSection {
        ranking: hit_rankings_for_stops,
        stops: all_stops_chateau_groups,
        routes: all_routes_chateau_groups,
    };

    let response_struct = TextSearchResponse { stops_section };

    HttpResponse::Ok().json(response_struct)
}
