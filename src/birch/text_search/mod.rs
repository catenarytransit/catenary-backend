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
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize, Clone)]
struct TextSearchQuery {
    text: String,
    user_lat: Option<f32>,
    user_lon: Option<f32>,
    map_lat: Option<f32>,
    map_lon: Option<f32>,
    map_z: Option<u8>,
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
        _ => false
    };

    let stops_query = match (query.user_lat, query.user_lon) {
        (Some(user_lat), Some(user_lon)) => json!({
            "query": {
                "function_score": {
                    "query": {
                      "multi_match" : {
                        "query":  query.text.clone(),
                        "fields": [ "stop_name*^2", "route_name_search" ]
                     }
                    },
                    "functions": [
                      {
                        "gauss": {
                          "location": {
                            "origin": { "lat": user_lat, "lon": user_lon }, // User's location
                            "offset": "5km", // Full score within 5000 metres
                            "scale": "100km" // Score decays significantly beyond 100 km
                          }
                        },
                        "weight": 2 // Optional: Give more importance to location
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
                    "fields": [ "stop_name*^2", "route_name_search" ]
                  }
            }
        })
    };

    let stops_response = elasticclient
        .as_ref()
        .search(SearchParts::Index(&["stops"]))
        .from(0)
        .size(50)
        .body(stops_query)
        .send()
        .await
        .unwrap();

    let response_body = stops_response.json::<serde_json::Value>().await.unwrap();

    println!("response body {:?}", response_body);

    HttpResponse::Ok().json(response_body)
}
