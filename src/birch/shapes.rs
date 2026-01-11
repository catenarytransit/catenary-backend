use crate::CatenaryPostgresPool;
use actix_web::{HttpResponse, Responder, web};
use catenary::models::Shape;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use geo::Simplify;
use geo::algorithm::intersects::Intersects;
use geo::coord;
use geojson::{Feature, FeatureCollection, GeoJson};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ShapeOptions {
    pub min_x: Option<f64>,
    pub min_y: Option<f64>,
    pub max_x: Option<f64>,
    pub max_y: Option<f64>,
    pub simplify: Option<f64>,
    pub format: Option<String>,
}

#[derive(Deserialize)]
pub struct ShapeQueryParams {
    pub chateau: String,
    pub shape_id: String,
    #[serde(flatten)]
    pub options: ShapeOptions,
}

#[derive(Deserialize, Debug)]
pub struct ShapeRequestItem {
    pub chateau: String,
    pub shape_ids: Vec<String>,
}

pub fn process_shape(shape: &Shape, options: &ShapeOptions) -> Option<geo::LineString> {
    let linestring_points = &shape.linestring.points;

    // Convert to geo::LineString
    let mut geo_linestring = geo::LineString::new(
        linestring_points
            .iter()
            .map(|p| coord! { x: p.x, y: p.y })
            .collect(),
    );

    // Simplification
    if let Some(tolerance_meters) = options.simplify {
        // Approximate conversion: 1 degree ~ 111,111 meters
        let tolerance_deg = tolerance_meters / 111_111.0;
        geo_linestring = geo_linestring.simplify(tolerance_deg);
    }

    // Cropping (check intersection with bbox)
    if let (Some(min_x), Some(min_y), Some(max_x), Some(max_y)) =
        (options.min_x, options.min_y, options.max_x, options.max_y)
    {
        let bbox = geo::Rect::new(coord! { x: min_x, y: min_y }, coord! { x: max_x, y: max_y });

        if !geo_linestring.intersects(&bbox) {
            return None;
        }
    }

    Some(geo_linestring)
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
            let geo_linestring_opt = process_shape(shape, &query.options);

            let geo_linestring = match geo_linestring_opt {
                Some(g) => g,
                None => geo::LineString::new(vec![]),
            };

            let format = query.options.format.as_deref().unwrap_or("geojson");

            if format == "polyline" {
                // Encode to polyline
                let poly = polyline::encode_coordinates(geo_linestring, 5).unwrap();
                HttpResponse::Ok()
                    .insert_header(("Content-Type", "application/json"))
                    .json(json!({ "polyline": poly }))
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

#[actix_web::post("/get_shapes")]
pub async fn get_shapes(
    body: web::Json<Vec<ShapeRequestItem>>,
    query: web::Query<ShapeOptions>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    if let Err(conn_pre) = &conn_pre {
        eprintln!("Error connecting to postgres: {}", conn_pre);
        return HttpResponse::InternalServerError().body("Error connecting to postgres");
    }

    let conn = &mut conn_pre.unwrap();
    let mut all_features = Vec::new();
    let mut polyline_results = Vec::new();

    let format = query.format.as_deref().unwrap_or("geojson");
    let is_polyline = format == "polyline";

    for item in body.iter() {
        // Fetch shapes for this chateau and list of shape_ids
        let shape_data: Result<Vec<Shape>, _> = catenary::schema::gtfs::shapes::dsl::shapes
            .filter(catenary::schema::gtfs::shapes::dsl::chateau.eq(&item.chateau))
            .filter(catenary::schema::gtfs::shapes::dsl::shape_id.eq_any(&item.shape_ids))
            .select(Shape::as_select())
            .load(conn)
            .await;

        match shape_data {
            Ok(shapes) => {
                for shape in shapes {
                    if let Some(geo_linestring) = process_shape(&shape, &query) {
                        if is_polyline {
                            let poly = polyline::encode_coordinates(geo_linestring, 5).unwrap();
                            polyline_results.push(json!({
                                "chateau": shape.chateau,
                                "shape_id": shape.shape_id,
                                "polyline": poly,
                                "color": shape.color
                            }));
                        } else {
                            let geometry = geojson::Geometry::from(&geo_linestring);
                            let feature = Feature {
                                bbox: None,
                                geometry: Some(geometry),
                                id: None,
                                properties: Some(serde_json::Map::from_iter(vec![
                                    ("chateau".to_string(), json!(shape.chateau)),
                                    ("shape_id".to_string(), json!(shape.shape_id)),
                                    ("color".to_string(), json!(shape.color)),
                                ])),
                                foreign_members: None,
                            };
                            all_features.push(feature);
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!(
                    "Error fetching shapes for chateau {}: {:?}",
                    item.chateau, err
                );
                return HttpResponse::InternalServerError().body("Error fetching shapes");
            }
        }
    }

    if is_polyline {
        HttpResponse::Ok()
            .insert_header(("Content-Type", "application/json"))
            .json(polyline_results)
    } else {
        let feature_collection = FeatureCollection {
            bbox: None,
            features: all_features,
            foreign_members: None,
        };
        HttpResponse::Ok()
            .insert_header(("Content-Type", "application/json"))
            .body(feature_collection.to_string())
    }
}
