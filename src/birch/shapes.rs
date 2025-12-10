use actix_web::{HttpRequest, HttpResponse, Responder, web};
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
}

#[actix_web::get("/get_shape")]
pub async fn get_shape(
    query: web::Query<ShapeQueryParams>,
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
) -> impl Responder {
    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let mut query_str = "SELECT ST_AsGeoJSON(".to_string();

    // Logic to wrap the geometry based on options
    // Base: linestring FROM gtfs.shapes

    let mut geom_selector = "linestring".to_string();

    // Simplification
    // ST_Simplify(geom, tolerance)
    if let Some(tolerance_meters) = query.simplify {
        // Approximate degrees. 1 degree ~= 111,111 meters.
        // This makes sense at the equator but less so elsewhere, but standard for simple stuff.
        // For better accuracy, we could cast to geography, but let's stick to geometry simple calc for performance if requested.
        let tolerance_deg = tolerance_meters / 111_111.0;
        geom_selector = format!("ST_Simplify({}, {})", geom_selector, tolerance_deg);
    }

    // Cropping / Intersection
    // ST_Intersection(geom, envelope)
    // Note: ST_Intersection might return MultiLineString or GeometryCollection
    if let (Some(min_x), Some(min_y), Some(max_x), Some(max_y)) =
        (query.min_x, query.min_y, query.max_x, query.max_y)
    {
        geom_selector = format!(
            "ST_Intersection({}, ST_MakeEnvelope({}, {}, {}, {}, 4326))",
            geom_selector, min_x, min_y, max_x, max_y
        );
    }

    query_str.push_str(&geom_selector);
    query_str.push_str(") as geojson FROM gtfs.shapes WHERE chateau = $1 AND shape_id = $2");

    let row: Result<(String,), _> = sqlx::query_as(query_str.as_str())
        .bind(&query.chateau)
        .bind(&query.shape_id)
        .fetch_one(sqlx_pool_ref)
        .await;

    match row {
        Ok((geojson,)) => HttpResponse::Ok()
            .insert_header(("Content-Type", "application/json"))
            .body(geojson),
        Err(err) => {
            eprintln!("Error fetching shape: {:?}", err);
            HttpResponse::InternalServerError().body("Error fetching shape or shape not found")
        }
    }
}
