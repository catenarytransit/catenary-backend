use actix_web::{HttpResponse, Responder, get, web};
use std::fs;
use std::path::Path;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_tile);
}

#[get("/tiles/{z}/{x}/{y}.pbf")]
async fn get_tile(path: web::Path<(u8, u32, u32)>) -> impl Responder {
    let (z, x, y) = path.into_inner();
    let file_path = format!("./tiles_output/{}/{}/{}.pbf", z, x, y);

    if Path::new(&file_path).exists() {
        match fs::read(&file_path) {
            Ok(bytes) => HttpResponse::Ok()
                .content_type("application/x-protobuf")
                .body(bytes),
            Err(_) => HttpResponse::InternalServerError().finish(),
        }
    } else {
        HttpResponse::NotFound().finish()
    }
}
