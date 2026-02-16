use actix_web::Responder;
use actix_web::{HttpRequest, HttpResponse, web};
use rand::Rng;

const API_KEYS: [&str; 2] = ["JfNvPTYZZ91w2EXyyJiq", "B265xPhJaYe2kWHOLHTG"];

#[actix_web::get("/maptiler_terrain_tiles_proxy/{z}/{x}/{y}.webp")]
pub async fn proxy_for_maptiler_terrain_tiles(
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    let client = reqwest::Client::builder().build().unwrap();

    let mut rng = rand::rng();
    let pick_random_key = API_KEYS[rng.random_range(0..API_KEYS.len())];

    let url = format!(
        "https://api.maptiler.com/tiles/terrain-rgb-v2/{z}/{x}/{y}.webp?key={pick_random_key}&mtsid=23671537-53fa-48f2-9ba1-647a217cbdb1"
    );

    let request = client
        .request(reqwest::Method::GET, url)
        .header("Origin", "https://maps.catenarymaps.org")
        .header("Referer", "https://maps.catenarymaps.org");

    let response = request.send().await;

    match response {
        Ok(response) => {
            let status = response.status();

            //get header content type

            let content_type = match (&response).headers().get("content-type") {
                Some(content_type) => content_type.to_str().unwrap_or_default(),
                None => "application/octet-stream",
            }
            .to_owned();

            let bytes = response.bytes().await.unwrap();

            match status.is_success() {
                true => HttpResponse::Ok()
                    .insert_header(("Content-Type", content_type))
                    .insert_header(("Cache-Control", "public, max-age=9999999999"))
                    .insert_header(("Access-Control-Allow-Origin", "*"))
                    .body(bytes),
                false => HttpResponse::NotFound()
                    .insert_header(("Content-Type", content_type))
                    .body(bytes),
            }
        }
        Err(err) => {
            eprintln!("{:#?}", err);
            HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Could not fetch data")
        }
    }
}

#[actix_web::get("/maptiler_contours_tiles_proxy/{z}/{x}/{y}.pbf")]
pub async fn proxy_for_maptiler_coutours_tiles(
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    let client = reqwest::Client::builder().build().unwrap();

    let mut rng = rand::rng();
    let pick_random_key = API_KEYS[rng.random_range(0..API_KEYS.len())];

    let url = format!(
        "https://api.maptiler.com/tiles/contours-v2/{z}/{x}/{y}.pbf?key={pick_random_key}&mtsid=23671537-53fa-48f2-9ba1-647a217cbdb1"
    );

    let request = client
        .request(reqwest::Method::GET, url)
        .header("Origin", "https://maps.catenarymaps.org")
        .header("Referer", "https://maps.catenarymaps.org");

    let response = request.send().await;

    match response {
        Ok(response) => {
            let status = response.status();

            //get header content type

            let content_type = match (&response).headers().get("content-type") {
                Some(content_type) => content_type.to_str().unwrap_or_default(),
                None => "application/octet-stream",
            }
            .to_owned();

            let bytes = response.bytes().await.unwrap();

            match status.is_success() {
                true => HttpResponse::Ok()
                    .insert_header(("Content-Type", content_type))
                    .insert_header(("Cache-Control", "public, max-age=9999999999"))
                    .insert_header(("Access-Control-Allow-Origin", "*"))
                    .body(bytes),
                false => HttpResponse::NotFound()
                    .insert_header(("Content-Type", content_type))
                    .body(bytes),
            }
        }
        Err(err) => {
            eprintln!("{:#?}", err);
            HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Could not fetch data")
        }
    }
}

#[actix_web::get("/mapbox_terrain_tiles_proxy/{z}/{x}/{y}.vector.pbf")]
pub async fn proxy_for_mapbox_terrain_tiles(
    path: web::Path<(u8, u32, u32)>,
    req: HttpRequest,
) -> impl Responder {
    let (z, x, y) = path.into_inner();

    let client = reqwest::Client::builder().build().unwrap();

    let url = format!(
        "https://api.mapbox.com/v4/mapbox.mapbox-terrain-v2/{}/{}/{}.vector.pbf?sku=101kszcOHRvc9&access_token=pk.eyJ1Ijoia3lsZXJzY2hpbiIsImEiOiJjajFsajI0ZHMwMDIzMnFwaXNhbDlrNDhkIn0.VdZpwJyJ8gWA--JNzkU5_Q",
        z, x, y
    );

    let request = client
        .request(reqwest::Method::GET, url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")
        .header("Referer", "https://api.mapbox.com/styles/v1/kylerschin/cmlpld9ek000y01suad7u5odf.html?title=view&access_token=pk.eyJ1Ijoia3lsZXJzY2hpbiIsImEiOiJjajFsajI0ZHMwMDIzMnFwaXNhbDlrNDhkIn0.VdZpwJyJ8gWA--JNzkU5_Q&zoomwheel=true&fresh=true");

    let response = request.send().await;

    match response {
        Ok(response) => {
            let status = response.status();

            //get header content type

            let content_type = match (&response).headers().get("content-type") {
                Some(content_type) => content_type.to_str().unwrap_or_default(),
                None => "application/octet-stream",
            }
            .to_owned();

            let bytes = response.bytes().await.unwrap();

            match status.is_success() {
                true => HttpResponse::Ok()
                    .insert_header(("Content-Type", content_type))
                    .insert_header(("Cache-Control", "public, max-age=9999999999"))
                    .insert_header(("Access-Control-Allow-Origin", "*"))
                    .body(bytes),
                false => HttpResponse::NotFound()
                    .insert_header(("Content-Type", content_type))
                    .body(bytes),
            }
        }
        Err(err) => {
            eprintln!("{:#?}", err);
            HttpResponse::InternalServerError()
                .insert_header(("Content-Type", "text/plain"))
                .body("Could not fetch data")
        }
    }
}
