//! GET /v2/regions/{region}/trips/{chateau}/{trip_id}
//! Uses the same immutably pinned RAM snapshot as the eventual RAPTOR
//! realtime route response. No Aspen RPC per incoming HTTP request.
use super::State;
use actix_web::{HttpResponse, http::StatusCode, web};
use catenary::routing_common::ojp_v2::{OjpTripDetail, added_trip, scheduled_trip};
use catenary::routing_common::realtime_identity::{RealtimeGtfsResolver, ResolveIssue};
use catenary::routing_common::realtime_source::RoutingTripUpdate;
use catenary::routing_common::timetable::SourceIdx;
use chrono::NaiveDate;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone, Deserialize)]
pub struct TripQuery {
    pub feed_id: String,
    pub service_date: NaiveDate,
    pub start_time: Option<String>,
    pub route_id: Option<String>,
    pub attempt_id: Option<String>,
}
type ApiError = (StatusCode, String);

fn lookup(
    router: &catenary::routing_common::route_manager::Router,
    catalog: &catenary::routing_common::realtime_identity::GtfsIdentityCatalog,
    snapshot: Option<&catenary::routing_common::realtime_region::RegionRealtimeSnapshot>,
    region: &str,
    chateau: &str,
    trip_id: &str,
    q: &TripQuery,
) -> Result<OjpTripDetail, ApiError> {
    let transit = router.transit.iter().find(|r| r.id == region)
        .ok_or((StatusCode::NOT_FOUND, "unknown transit region".to_owned()))?;
    let tt = transit.timetable.view()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let candidates = tt.sources().iter().enumerate().filter_map(|(i, _)| {
        let idx = SourceIdx(u16::try_from(i).ok()?);
        let (c, f, a) = tt.source(idx)?;
        (c == chateau && f == q.feed_id
            && q.attempt_id.as_deref().is_none_or(|v| v == a)).then_some(idx)
    }).collect::<Vec<_>>();
    let source = match candidates.as_slice() {
        [one] => *one,
        [] => return Err((StatusCode::NOT_FOUND, "GTFS source not in region".into())),
        _ => return Err((StatusCode::CONFLICT,
            "multiple matching attempts; specify attempt_id".into())),
    };
    let update = RoutingTripUpdate {
        update_id: "v2-query".into(),
        trip_id: Some(trip_id.to_owned()),
        route_id: q.route_id.clone(),
        service_date: Some(q.service_date),
        start_time: q.start_time.clone(),
        schedule_relationship: None,
        delay_seconds: None,
        stops: Vec::new(),
    };
    let updates = [update];
    let resolver = RealtimeGtfsResolver::new(&tt, catalog, source, &updates)
        .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, format!("invalid GTFS identity: {e:?}")))?;
    match resolver.instance(source, &updates[0]) {
        Ok(instance) => scheduled_trip(
            &tt, catalog, &transit.platform_metadata, snapshot,
            instance, trip_id, q.service_date,
        ).map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())),
        Err(ResolveIssue::TripIdNotFound | ResolveIssue::NoActiveService) => {
            // Added/unscheduled runs cannot resolve to a static TransportIdx.
            // Require start_time so a frequency-expanded trip is unique.
            let start = q.start_time.as_deref().ok_or((
                StatusCode::NOT_FOUND, "no static trip; added trips require start_time".to_owned()
            ))?;
            let day = q.service_date.signed_duration_since(
                NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch")
            ).num_days();
            let key = format!("{}:{}{}:{}:{}", trip_id.len(), trip_id,
                start.len(), start, day);
            let pinned = snapshot.ok_or((
                StatusCode::SERVICE_UNAVAILABLE, "realtime snapshot is unavailable".to_owned()
            ))?;
            added_trip(&tt, &transit.platform_metadata, pinned, chateau,
                source, trip_id, q.service_date, &key)
                .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?
                .ok_or((StatusCode::NOT_FOUND, "trip instance not found".to_owned()))
        }
        Err(ResolveIssue::AmbiguousTripInstance) => Err((
            StatusCode::CONFLICT, "ambiguous GTFS trip instance; specify route_id/start_time".into()
        )),
        Err(e) => Err((StatusCode::NOT_FOUND, format!("trip instance not found: {e:?}"))),
    }
}

pub async fn handle(
    state: web::Data<State>,
    path: web::Path<(String, String, String)>,
    q: web::Query<TripQuery>,
) -> HttpResponse {
    let (region, chateau, trip_id) = path.into_inner();
    if q.feed_id.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error":"feed_id is required"
        }));
    }
    let Some(sources) = state.realtime_sources.as_ref() else {
        return HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "error":"realtime identity catalogs are not loaded; enable Aspen source synchronization"
        }));
    };
    let Some(catalog) = sources.identity_catalogs.get(&region).cloned() else {
        return HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "error":"this region is missing a generation-matched GTFS identity catalog"
        }));
    };
    let router = Arc::clone(&state.router);
    let pinned = router.pin_realtime(&region);
    let query = q.into_inner();
    // Large regional trip-ID scans must never block an Actix reactor.
    match web::block(move || {
        lookup(&router, &catalog, pinned.as_deref(),
            &region, &chateau, &trip_id, &query)
    }).await {
        Ok(Ok(trip)) => HttpResponse::Ok().json(trip),
        Ok(Err((status, message))) => HttpResponse::build(status).json(serde_json::json!({
            "error":message
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error":format!("trip lookup worker failed: {e}")
        })),
    }
}
