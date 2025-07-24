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
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
struct TextSearchQuery {
    text: String,
    user_lat: Option<f32>,
    user_lon: Option<f32>,
    map_lat: Option<f32>,
    map_lon: Option<f32>,
    map_z: Option<u8>,
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

#[derive(Clone, Debug)]
pub struct TextSearchResponseStopsSection {
    stops: BTreeMap<String, BTreeMap<String, StopDeserialised>>,
    routes: BTreeMap<String, BTreeMap<String, catenary::models::Route>>,
    ranking: Vec<StopRankingInfo>,
}

#[derive(Clone, Debug)]
pub struct TextSearchResponse {
    pub stops_section: TextSearchResponseStopsSection,
}

#[actix_web::get("/text_search_v1")]
pub async fn text_search_v1(
    req: HttpRequest,
    query: Query<TextSearchQuery>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    elasticclient: web::Data<Arc<elasticsearch::Elasticsearch>>,
) -> impl Responder {
    let map_pos_exists = match (query.map_lat, query.map_lon, query.map_z) {
        (Some(map_lat), Some(map_lon), Some(map_z)) => true,
        _ => false,
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
                        "gauss": {
                          "point": {
                            "origin": { "lat": user_lat, "lon": user_lon }, // User's location
                            "offset": "5km", // Full score within 5000 metres
                            "scale": "100km" // Score decays significantly beyond 100 km
                          }
                        },
                        "weight": 1
                      }
                    ],
                    "score_mode": "sum", // How to combine scores from multiple functions
                    "boost_mode": "multiply" // How to combine the function score with the query score
                  }
            }
        }),
        _ => json!({
            "query": {
                "multi_match" : {
                    "query":  query.text.clone(),
                    "fields": [ "stop_name*^3", "route_name_search", "agency_name_search" ]
                  }
            }
        }),
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

    let hits_list_stops = response_body.get("hits").map(|x| x.get("hits")).flatten().map(|x| x.as_array()).flatten();

    let mut hit_rankings_for_stops: Vec<StopRankingInfo> = vec![];

    if let Some(hits_list_stops) = hits_list_stops {
        for hit in hits_list_stops {
            if let Some(hit) = hit.as_object() {
                match (hit.get("_score"), hit.get("_source")) {
                    (Some(score), Some(source)) => {
                        match (score.as_f64(), source.as_object()) {
                            (Some(score), Some(source)) => {
                                if let Some(chateau) = source.get("chateau").map(|x| x.as_str()).flatten() {
                                    if let Some(stop_id) = source.get("stop_id").map(|x| x.as_str()).flatten() {
                                        hit_rankings_for_stops.push(StopRankingInfo {
                                            chateau: chateau.to_string(),
                                            gtfs_id: stop_id.to_string(),
                                            score: score
                                        });
                                    }
                                }
                            },
                            _ => {

                            }
                        }
                    },
                    _ => {}
                }
            }
        }
    }

    println!("response body {:?}", response_body);

    println!("hit_rankings_for_stops {:?}", hit_rankings_for_stops);
 
    HttpResponse::Ok().json(response_body)
}
