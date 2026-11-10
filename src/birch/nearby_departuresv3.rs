use actix_web::{HttpResponse, Responder, web};
use catenary::nearby_departures::{NearbyFromCoordsV3, RouteSelection, prepare_nearby};
use catenary::postgres_tools::CatenaryPostgresPool;
use std::sync::Arc;

#[actix_web::get("/nearbydeparturesfromcoordsv3")]
#[tracing::instrument(name = "nearby_from_coords_v3", skip(pool, cache), fields(lat = ?query.lat, lon = ?query.lon))]
pub async fn nearby_from_coords_v3(
    query: web::Query<NearbyFromCoordsV3>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    cache: web::Data<Arc<catenary::aspen::lib::AspenChateauCache>>,
) -> impl Responder {
    let query = query.into_inner();
    if let Err(error) = query.validate() {
        return HttpResponse::BadRequest().body(error);
    }
    match prepare_nearby(query, pool.get_ref().clone()).await {
        Ok(prepared) => match prepared
            .load(pool.get_ref().clone(), cache.get_ref().clone(), RouteSelection::All)
            .await
        {
            Ok(data) => HttpResponse::Ok().json(data),
            Err(error) => HttpResponse::InternalServerError().body(error),
        },
        Err(error) => HttpResponse::InternalServerError().body(error),
    }
}
