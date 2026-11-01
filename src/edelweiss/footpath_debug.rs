//! Read-only diagnostic inspection of the *compiled* Gentian transfer closure.
//! This module is deliberately outside RAPTOR and the OSR worker: it never
//! recomputes transfers or changes the routing timetable.
use actix_web::{HttpResponse, web};
use anyhow::{Context, Result, ensure};
use catenary::routing_common::route_manager::Router;
use catenary::routing_common::timetable::{LocationIdx, PackedFootpath, ZeroCopyTimetable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const DEFAULT_MAX_EDGES: usize = 128;
const HARD_MAX_EDGES: usize = 512;
const STOP_COORDINATE_TOLERANCE_METERS: f64 = 30.0;
const NEARBY_STOP_RADIUS_METERS: f64 = 250.0;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Request {
    stop_index: Option<u32>,
    lat: f64,
    lon: f64,
    max_edges: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Stop {
    stop_index: u32,
    stop_id: String,
    name: String,
    lat: f64,
    lon: f64,
    route_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Footpath {
    target: Stop,
    /// Stored directed edge cost in seconds, not a guessed walking score.
    seconds: u32,
    /// Geodesic endpoint distance only, not the actual OSR walking geometry.
    straight_line_meters: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    transit_region: String,
    stop: Stop,
    outgoing: Vec<Footpath>,
    incoming: Vec<Footpath>,
    outgoing_count: usize,
    incoming_count: usize,
    truncated: bool,
}

fn distance_meters(a_lat: f64, a_lon: f64, b_lat: f64, b_lon: f64) -> f64 {
    let dlat = (b_lat - a_lat).to_radians();
    let dlon = (b_lon - a_lon).to_radians();
    let h = (dlat / 2.0).sin().powi(2)
        + a_lat.to_radians().cos() * b_lat.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * 6_371_000.0 * h.clamp(0.0, 1.0).sqrt().asin()
}

fn stop(tt: &ZeroCopyTimetable<'_>, idx: LocationIdx) -> Result<Stop> {
    let i = idx.idx();
    let point = tt
        .coordinates()
        .get(i)
        .context("footpath target is outside timetable")?;
    Ok(Stop {
        stop_index: idx.0,
        stop_id: tt
            .location_id()
            .get(i)
            .and_then(|v| tt.get_string(*v))
            .unwrap_or("")
            .to_owned(),
        name: tt
            .location_name()
            .get(i)
            .and_then(|v| tt.get_string(*v))
            .unwrap_or("")
            .to_owned(),
        lat: f64::from(point.lat),
        lon: f64::from(point.lon),
        route_count: tt.routes_at(idx).len(),
    })
}

fn edges(
    tt: &ZeroCopyTimetable<'_>,
    origin: &Stop,
    paths: &[PackedFootpath],
    limit: usize,
) -> Result<Vec<Footpath>> {
    let mut sorted = paths.to_vec();
    sorted.sort_unstable_by_key(|edge| (edge.seconds(), edge.target().0));
    sorted
        .into_iter()
        .take(limit)
        .map(|edge| {
            let target = stop(tt, edge.target())?;
            Ok(Footpath {
                straight_line_meters: distance_meters(
                    origin.lat, origin.lon, target.lat, target.lon,
                )
                .round() as u32,
                target,
                seconds: edge.seconds(),
            })
        })
        .collect()
}

fn inspect(router: &Router, request: Request) -> Result<Response> {
    ensure!(
        request.lat.is_finite()
            && request.lon.is_finite()
            && (-90.0..=90.0).contains(&request.lat)
            && (-180.0..=180.0).contains(&request.lon),
        "invalid stop coordinates"
    );
    let limit = request
        .max_edges
        .unwrap_or(DEFAULT_MAX_EDGES)
        .clamp(1, HARD_MAX_EDGES);
    // An itinerary supplies the exact timetable index. A standalone GTFS
    // screen supplies coordinates only: resolve the nearest compiled location
    // within a bounded radius, independently of regular route planning.
    let mut best: Option<(&str, LocationIdx, f64)> = None;
    for region in &router.transit {
        let tt = router.timetable_for(&region.id)?;
        let mut consider = |index: LocationIdx| {
            if let Some(point) = tt.coordinates().get(index.idx()) {
                let distance = distance_meters(
                    request.lat,
                    request.lon,
                    f64::from(point.lat),
                    f64::from(point.lon),
                );
                let radius = if request.stop_index.is_some() {
                    STOP_COORDINATE_TOLERANCE_METERS
                } else {
                    NEARBY_STOP_RADIUS_METERS
                };
                if distance <= radius && best.is_none_or(|(_, _, previous)| distance < previous) {
                    best = Some((&region.id, index, distance));
                }
            }
        };
        if let Some(index) = request.stop_index {
            consider(LocationIdx(index));
        } else {
            for i in 0..tt.coordinates().len() {
                consider(LocationIdx(i as u32));
            }
        }
    }
    let (region, idx, _) =
        best.context("no matching compiled stop found near these coordinates")?;
    let tt = router.timetable_for(region)?;
    let origin = stop(&tt, idx)?;
    let outgoing_paths = tt.footpaths_from(idx);
    let incoming_paths = tt.footpaths_to(idx);
    Ok(Response {
        transit_region: region.to_owned(),
        outgoing: edges(&tt, &origin, outgoing_paths, limit)?,
        incoming: edges(&tt, &origin, incoming_paths, limit)?,
        outgoing_count: outgoing_paths.len(),
        incoming_count: incoming_paths.len(),
        truncated: outgoing_paths.len() > limit || incoming_paths.len() > limit,
        stop: origin,
    })
}

pub(super) async fn handle(
    state: web::Data<super::State>,
    payload: web::Json<Request>,
) -> HttpResponse {
    let router: Arc<Router> = Arc::clone(&state.router);
    let request = payload.into_inner();
    match web::block(move || inspect(&router, request)).await {
        Ok(Ok(result)) => HttpResponse::Ok().json(result),
        Ok(Err(err)) => HttpResponse::UnprocessableEntity().body(err.to_string()),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn geographic_distance_and_direction_are_not_conflated() {
        assert!(distance_meters(34.0, -118.0, 34.0, -118.0) < 0.01);
        let distance = distance_meters(34.0, -118.0, 34.001, -118.0);
        assert!(distance > 110.0 && distance < 112.0);
    }
}
