use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct TTArrivalsQuery {
    map_id: String,
}

use actix_web::{HttpRequest, HttpResponse, Responder, web};
#[actix_web::get("/cta_ttarrivals_proxy")]
pub async fn ttarrivals_proxy(
    req: HttpRequest,
    query: web::Query<TTArrivalsQuery>,
) -> impl Responder {
    //get query param mapid

    let map_id = &query.map_id;

    //https://lapi.transitchicago.com/api/1.0/ttarrivals.aspx?key=e325bc1ce4ad4ce0a3ad0830739c4993&mapid=40380&outputType=JSON
    let url = format!(
        "https://lapi.transitchicago.com/api/1.0/ttarrivals.aspx?key=e325bc1ce4ad4ce0a3ad0830739c4993&mapid={}&outputType=JSON",
        map_id
    );

    let response = reqwest::get(url).await;

    match response {
        Ok(res) => {
            let body = res.text().await.unwrap();
            HttpResponse::Ok().body(body)
        }
        Err(err) => HttpResponse::InternalServerError().body(format!("Error: {:?}", err)),
    }
}
