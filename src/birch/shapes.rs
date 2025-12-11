use crate::CatenaryPostgresPool;
use actix_web::{HttpResponse, Responder, web};
use catenary::models::Shape;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::Simplify;
use geo::algorithm::intersects::Intersects;
use geo::coord;
use geojson::GeoJson;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ShapeQueryParams {
    pub chateau: String,
    pub shape_id: String,
    pub min_x: Option<f64>,
    pub min_y: Option<f64>,
    pub max_x: Option<f64>,
    pub max_y: Option<f64>,
    pub simplify: Option<f64>,
    pub format: Option<String>,
}

#[actix_web::get("/get_shape")]
pub async fn get_shape(
    query: web::Query<ShapeQueryParams>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("Error connecting to postgres: {}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();

    // Fetch the shape using Diesel
    let shape_data: Result<Vec<Shape>, _> = catenary::schema::gtfs::shapes::dsl::shapes
        .filter(catenary::schema::gtfs::shapes::dsl::chateau.eq(&query.chateau))
        .filter(catenary::schema::gtfs::shapes::dsl::shape_id.eq(&query.shape_id))
        .select(Shape::as_select())
        .load(conn)
        .await;

    match shape_data {
        Ok(shapes) => {
            if shapes.is_empty() {
                return HttpResponse::NotFound().body("Shape not found");
            }

            let shape = &shapes[0];
            let linestring_points = &shape.linestring.points;

            // Convert to geo::LineString
            let mut geo_linestring = geo::LineString::new(
                linestring_points
                    .iter()
                    .map(|p| coord! { x: p.x, y: p.y })
                    .collect(),
            );

            // Simplification
            if let Some(tolerance_meters) = query.simplify {
                let tolerance_deg = tolerance_meters / 111_111.0;
                geo_linestring = geo_linestring.simplify(tolerance_deg);
            }

            // Cropping (check intersection with bbox)
            if let (Some(min_x), Some(min_y), Some(max_x), Some(max_y)) =
                (query.min_x, query.min_y, query.max_x, query.max_y)
            {
                let bbox =
                    geo::Rect::new(coord! { x: min_x, y: min_y }, coord! { x: max_x, y: max_y });

                if !geo_linestring.intersects(&bbox) {
                    geo_linestring = geo::LineString::new(vec![]);
                }
            }

            let format = query.format.as_deref().unwrap_or("geojson");

            if format == "polyline" {
                // Encode to polyline
                let poly = polyline::encode_coordinates(geo_linestring, 5).unwrap();
                HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .json(serde_json::json!({ "polyline": poly }))
            } else {
                // Default to GeoJSON
                let geometry = geojson::Geometry::from(&geo_linestring);
                let geojson = GeoJson::Geometry(geometry);
                HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .body(geojson.to_string())
            }
        }
        Err(err) => {
            eprintln!("Error fetching shape: {:?}", err);
            HttpResponse::InternalServerError().body("Error fetching shape")
        }
    }
}
