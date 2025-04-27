use actix_web::{HttpResponse, Responder};

//write a proxy for the openrailwaymap martin server at https://openrailwaymap.fly.dev/
// when encountering tilejson, replace all tile urls that match https://openrailwaymap.fly.dev/ with https://birch.catenarymaps.org/openrailwaymap/

//forward protobuf or other data without modification

#[actix_web::get("/openrailwaymap_proxy/{path:.*}")]
pub async fn openrailwaymap_proxy(path: actix_web::web::Path<String>) -> impl Responder {
    let url = format!("https://openrailwaymap.fly.dev/{}", path);
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await.unwrap();

    // Check if the response is a tilejson
    if response.headers().get("Content-Type").unwrap() == "application/json" {
        let mut json: serde_json::Value = response.json().await.unwrap();
        if let Some(tiles) = json["tiles"].as_array_mut() {
            for tile in tiles {
                if let Some(tile_url) = tile.as_str() {
                    *tile = tile_url
                        .replace(
                            "https://openrailwaymap.fly.dev/",
                            "https://birch.catenarymaps.org/openrailwaymap_proxy/",
                        )
                        .into();
                }
            }
        }
        return HttpResponse::Ok().json(json);
    } else {
        // Forward the response without modification
        return HttpResponse::Ok().body(response.bytes().await.unwrap());
    }
}
