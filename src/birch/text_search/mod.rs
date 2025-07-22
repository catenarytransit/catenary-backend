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
}

#[actix_web::get("/text_search_v1")]
pub async fn text_search_v1(
    req: HttpRequest,
    query: Query<TextSearchQuery>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    elasticclient: web::Data<Arc<elasticsearch::Elasticsearch>>,
) -> impl Responder {
    let stops_response = elasticclient
        .as_ref()
        .search(SearchParts::Index(&["stops"]))
        .from(0)
        .size(30)
        .body(json!({
            "query": {
                "multi_match" : {
                    "query":  query.text.clone(),
                    "fields": [ "stop_name_search^5", "route_name_search" ]
                  }
            }
        }))
        .send()
        .await
        .unwrap();

    let response_body = stops_response.json::<serde_json::Value>().await.unwrap();

    println!("response body {:?}", response_body);

    HttpResponse::Ok().json(response_body)
}
