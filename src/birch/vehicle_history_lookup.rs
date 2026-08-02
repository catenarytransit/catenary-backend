use actix_web::http::StatusCode;
use actix_web::web::Query;
use actix_web::{HttpResponse, Responder, web};
use catenary::models::{BasicVehicleHistory, Route};
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::{Duration, LocalResult, NaiveDate, TimeZone};
use compact_str::CompactString;
use diesel::SelectableHelper;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize)]
pub struct VehicleHistoryLookupQuery {
    vehicle: Option<String>,
    chateau: Option<String>,
    route_id: Option<String>,
    unified_agency_id: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct VehicleHistoryOfRouteQuery {
    chateau: Option<String>,
    route_id: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RouteHistoryRow {
    operation_date: NaiveDate,
    unix_start_time: Option<u64>,
    trip_id: String,
    route_id: String,
    trip_short_name: Option<String>,
    direction_headsign: Option<String>,
    block_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct VehicleHistoryOfRouteRow {
    operation_date: NaiveDate,
    vehicle_label: String,
    trip_id: String,
    trip_short_name: Option<String>,
    direction_headsign: Option<String>,
    block_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct VehicleHistoryLookupResponse {
    trip_history: Vec<RouteHistoryRow>,
    routes: HashMap<String, Route>,
    agency_timezone: String,
}

#[derive(Debug, Serialize)]
pub struct VehicleHistoryOfRouteResponse {
    trip_history: Vec<VehicleHistoryOfRouteRow>,
    agency_timezone: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResolvedAgency {
    unified_agency_id: String,
    timezone: String,
}

#[derive(Clone, Debug)]
struct TripMetadata {
    onestop_feed_id: String,
    attempt_id: String,
    itinerary_pattern_id: String,
    trip_short_name: Option<String>,
    block_id: Option<String>,
    start_time: u32,
}

#[derive(Clone, Debug)]
struct EnrichedHistoryRow {
    operation_date: NaiveDate,
    start_time: Option<u32>,
    vehicle_label: String,
    trip_id: String,
    route_id: String,
    trip_short_name: Option<String>,
    direction_headsign: Option<String>,
    block_id: Option<String>,
}

#[derive(Debug)]
enum LookupError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Database(diesel::result::Error),
    Internal(String),
}

#[derive(Debug, Serialize)]
struct LookupErrorResponse {
    error: LookupErrorBody,
}

#[derive(Debug, Serialize)]
struct LookupErrorBody {
    code: &'static str,
    message: String,
}

impl LookupError {
    fn into_response(self) -> HttpResponse {
        let (status, code, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            Self::Conflict(message) => (StatusCode::CONFLICT, "conflict", message),
            Self::Database(error) => {
                eprintln!("vehicle history lookup database error: {error}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database_error",
                    "Database query failed".to_string(),
                )
            }
            Self::Internal(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
            }
        };

        HttpResponse::build(status).json(LookupErrorResponse {
            error: LookupErrorBody { code, message },
        })
    }
}

impl From<diesel::result::Error> for LookupError {
    fn from(error: diesel::result::Error) -> Self {
        Self::Database(error)
    }
}

fn non_empty_parameter<'a>(
    value: &'a Option<String>,
    field_name: &str,
) -> Result<Option<&'a str>, LookupError> {
    match value.as_deref() {
        Some(value) if value.trim().is_empty() => Err(LookupError::BadRequest(format!(
            "{field_name} cannot be empty"
        ))),
        Some(value) => Ok(Some(value.trim())),
        None => Ok(None),
    }
}

fn required_parameter<'a>(
    value: &'a Option<String>,
    field_name: &str,
) -> Result<&'a str, LookupError> {
    non_empty_parameter(value, field_name)?
        .ok_or_else(|| LookupError::BadRequest(format!("{field_name} is required")))
}

fn parse_optional_date(
    value: Option<&str>,
    field_name: &str,
) -> Result<Option<NaiveDate>, LookupError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();

    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(value, "%Y%m%d"))
        .map(Some)
        .map_err(|_| {
            LookupError::BadRequest(format!("{field_name} must use YYYY-MM-DD or YYYYMMDD"))
        })
}

fn parse_date_range(
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<(Option<NaiveDate>, Option<NaiveDate>), LookupError> {
    let start_date = parse_optional_date(start_date, "start_date")?;
    let end_date = parse_optional_date(end_date, "end_date")?;

    if matches!(
        (start_date.as_ref(), end_date.as_ref()),
        (Some(start), Some(end)) if start > end
    ) {
        return Err(LookupError::BadRequest(
            "start_date cannot be after end_date".to_string(),
        ));
    }

    Ok((start_date, end_date))
}

fn gtfs_start_time_to_unix_start_time(
    operation_date: NaiveDate,
    gtfs_start_time: u32,
    timezone: &chrono_tz::Tz,
) -> Result<u64, LookupError> {
    let local_noon = operation_date.and_hms_opt(12, 0, 0).ok_or_else(|| {
        LookupError::Internal("Invalid vehicle history operation date".to_string())
    })?;
    let local_noon = match timezone.from_local_datetime(&local_noon) {
        LocalResult::Single(noon) => noon,
        LocalResult::Ambiguous(first, _) => first,
        LocalResult::None => {
            return Err(LookupError::Internal(
                "Could not resolve local noon for the agency timezone".to_string(),
            ));
        }
    };

    // Resolve local noon first, then use the instant exactly 12 hours earlier as
    // the service-day reference midnight. GTFS start_time remains elapsed seconds
    // from that reference, including values greater than 24:00:00.
    let reference_midnight = local_noon
        .checked_sub_signed(Duration::hours(12))
        .ok_or_else(|| {
            LookupError::Internal("Vehicle history reference time overflowed".to_string())
        })?;
    let unix_start_time = reference_midnight
        .checked_add_signed(Duration::seconds(i64::from(gtfs_start_time)))
        .ok_or_else(|| LookupError::Internal("Vehicle history start time overflowed".to_string()))?
        .timestamp();

    u64::try_from(unix_start_time).map_err(|_| {
        LookupError::Internal("Vehicle history start time is before the Unix epoch".to_string())
    })
}

type ProductionAgency = (String, String, Option<String>, String);

async fn production_agencies_for_chateau(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
) -> Result<BTreeSet<ProductionAgency>, LookupError> {
    use catenary::schema::gtfs::{agencies, ingested_static};

    let rows = agencies::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(agencies::static_onestop_id)
                .and(ingested_static::attempt_id.eq(agencies::attempt_id))),
        )
        .filter(agencies::chateau.eq(chateau_id))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select((
            agencies::static_onestop_id,
            agencies::agency_id,
            agencies::unified_agency_id,
            agencies::agency_timezone,
        ))
        .load::<ProductionAgency>(conn)
        .await?;

    Ok(rows.into_iter().collect())
}

fn resolve_one_agency(
    agencies: BTreeSet<ProductionAgency>,
    no_match_message: &str,
) -> Result<ResolvedAgency, LookupError> {
    if agencies.is_empty() {
        return Err(LookupError::NotFound(no_match_message.to_string()));
    }

    let candidates = agencies
        .into_iter()
        .filter_map(|(_, _, unified_agency_id, timezone)| {
            unified_agency_id
                .filter(|value| !value.trim().is_empty())
                .map(|unified_agency_id| (unified_agency_id, timezone))
        })
        .collect::<BTreeSet<_>>();

    if candidates.is_empty() {
        return Err(LookupError::NotFound(
            "The resolved agency has no unified_agency_id".to_string(),
        ));
    }

    let unified_agency_ids = candidates
        .iter()
        .map(|(unified_agency_id, _)| unified_agency_id.as_str())
        .collect::<BTreeSet<_>>();
    if unified_agency_ids.len() != 1 {
        return Err(LookupError::Conflict(
            "The lookup resolves to multiple unified agencies".to_string(),
        ));
    }

    if candidates.len() != 1 {
        return Err(LookupError::Conflict(
            "The resolved unified agency has multiple agency timezones".to_string(),
        ));
    }

    let (unified_agency_id, timezone) = candidates
        .into_iter()
        .next()
        .ok_or_else(|| LookupError::NotFound("No production agency was found".to_string()))?;

    Ok(ResolvedAgency {
        unified_agency_id,
        timezone,
    })
}

async fn candidate_agencies_from_chateau(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
    route_id_filter: Option<&str>,
) -> Result<BTreeSet<ProductionAgency>, LookupError> {
    use catenary::schema::gtfs::{ingested_static, routes};

    let agencies = production_agencies_for_chateau(conn, chateau_id).await?;

    let Some(route_id_filter) = route_id_filter else {
        return Ok(agencies);
    };

    let route_owners = routes::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(routes::onestop_feed_id)
                .and(ingested_static::attempt_id.eq(routes::attempt_id))),
        )
        .filter(routes::chateau.eq(chateau_id))
        .filter(routes::route_id.eq(route_id_filter))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select((routes::agency_id, routes::onestop_feed_id))
        .load::<(Option<String>, String)>(conn)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();

    if route_owners.is_empty() {
        return Err(LookupError::NotFound(
            "No production route was found for chateau and route_id".to_string(),
        ));
    }

    let candidates = agencies
        .into_iter()
        .filter(|(static_onestop_id, agency_id, _, _)| {
            route_owners.iter().any(|(route_agency_id, route_feed_id)| {
                if route_feed_id != static_onestop_id {
                    return false;
                }

                route_agency_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map_or(true, |route_agency_id| route_agency_id == agency_id)
            })
        })
        .collect::<BTreeSet<_>>();

    if candidates.is_empty() {
        return Err(LookupError::NotFound(
            "No production agency was found for the route".to_string(),
        ));
    }

    Ok(candidates)
}

async fn resolve_from_chateau(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
    route_id_filter: Option<&str>,
) -> Result<ResolvedAgency, LookupError> {
    let candidates = candidate_agencies_from_chateau(conn, chateau_id, route_id_filter).await?;
    resolve_one_agency(candidates, "No production agency was found for the chateau")
}

async fn resolve_from_unified_agency(
    conn: &mut diesel_async::AsyncPgConnection,
    unified_agency_id: &str,
) -> Result<ResolvedAgency, LookupError> {
    use catenary::schema::gtfs::{agencies, ingested_static};

    let timezones = agencies::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(agencies::static_onestop_id)
                .and(ingested_static::attempt_id.eq(agencies::attempt_id))),
        )
        .filter(agencies::unified_agency_id.eq(unified_agency_id))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select(agencies::agency_timezone)
        .load::<String>(conn)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();

    if timezones.is_empty() {
        return Err(LookupError::NotFound(
            "No production agency was found for unified_agency_id".to_string(),
        ));
    }

    if timezones.len() != 1 {
        return Err(LookupError::Conflict(
            "The unified agency has multiple agency timezones".to_string(),
        ));
    }

    let timezone = timezones
        .into_iter()
        .next()
        .ok_or_else(|| LookupError::NotFound("No agency timezone was found".to_string()))?;

    Ok(ResolvedAgency {
        unified_agency_id: unified_agency_id.to_string(),
        timezone,
    })
}

async fn resolve_vehicle_from_candidates(
    conn: &mut diesel_async::AsyncPgConnection,
    agencies: BTreeSet<ProductionAgency>,
    vehicle: &str,
) -> Result<ResolvedAgency, LookupError> {
    use catenary::schema::gtfs::basic_vehicles;

    let candidate_unified_agency_ids = agencies
        .iter()
        .filter_map(|(_, _, unified_agency_id, _)| unified_agency_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if candidate_unified_agency_ids.is_empty() {
        return Err(LookupError::NotFound(
            "The resolved agency has no unified_agency_id".to_string(),
        ));
    }

    let matching_unified_agency_ids = basic_vehicles::table
        .filter(basic_vehicles::vehicle_label.eq(vehicle))
        .filter(basic_vehicles::unified_agency_id.eq_any(&candidate_unified_agency_ids))
        .select(basic_vehicles::unified_agency_id)
        .load::<String>(conn)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();

    if matching_unified_agency_ids.is_empty() {
        return Err(LookupError::NotFound(
            "No vehicle was found for the requested agency scope and vehicle label".to_string(),
        ));
    }

    if matching_unified_agency_ids.len() != 1 {
        return Err(LookupError::Conflict(
            "The vehicle label exists under multiple unified agency ids".to_string(),
        ));
    }

    let matching_unified_agency_id = matching_unified_agency_ids
        .into_iter()
        .next()
        .ok_or_else(|| LookupError::NotFound("No matching vehicle was found".to_string()))?;
    let matching_agencies = agencies
        .into_iter()
        .filter(|(_, _, unified_agency_id, _)| {
            unified_agency_id.as_deref() == Some(matching_unified_agency_id.as_str())
        })
        .collect::<BTreeSet<_>>();

    resolve_one_agency(matching_agencies, "No matching production agency was found")
}

async fn ensure_vehicle_exists(
    conn: &mut diesel_async::AsyncPgConnection,
    resolved_agency: ResolvedAgency,
    vehicle: &str,
) -> Result<ResolvedAgency, LookupError> {
    use catenary::schema::gtfs::basic_vehicles;

    let vehicle_exists = basic_vehicles::table
        .filter(basic_vehicles::unified_agency_id.eq(&resolved_agency.unified_agency_id))
        .filter(basic_vehicles::vehicle_label.eq(vehicle))
        .select(basic_vehicles::vehicle_label)
        .first::<String>(conn)
        .await
        .optional()?;

    vehicle_exists.map(|_| resolved_agency).ok_or_else(|| {
        LookupError::NotFound(
            "No vehicle was found for unified agency and vehicle label".to_string(),
        )
    })
}

async fn resolve_agency_for_vehicle(
    conn: &mut diesel_async::AsyncPgConnection,
    query: &VehicleHistoryLookupQuery,
    vehicle: &str,
) -> Result<ResolvedAgency, LookupError> {
    let chateau = non_empty_parameter(&query.chateau, "chateau")?;
    let route_id = non_empty_parameter(&query.route_id, "route_id")?;
    let unified_agency_id = non_empty_parameter(&query.unified_agency_id, "unified_agency_id")?;

    match (chateau, route_id, unified_agency_id) {
        (Some(chateau), None, None) => {
            let candidates = candidate_agencies_from_chateau(conn, chateau, None).await?;
            resolve_vehicle_from_candidates(conn, candidates, vehicle).await
        }
        (Some(chateau), Some(route_id), None) => {
            let candidates = candidate_agencies_from_chateau(conn, chateau, Some(route_id)).await?;
            resolve_vehicle_from_candidates(conn, candidates, vehicle).await
        }
        (None, None, Some(unified_agency_id)) => {
            let resolved_agency = resolve_from_unified_agency(conn, unified_agency_id).await?;
            ensure_vehicle_exists(conn, resolved_agency, vehicle).await
        }
        (None, Some(_), None) => Err(LookupError::BadRequest(
            "route_id requires chateau".to_string(),
        )),
        _ => Err(LookupError::BadRequest(
            "Provide exactly one lookup mode: chateau, chateau with route_id, or unified_agency_id"
                .to_string(),
        )),
    }
}

async fn load_trip_metadata(
    conn: &mut diesel_async::AsyncPgConnection,
    history: &[BasicVehicleHistory],
) -> Result<
    (
        BTreeMap<(String, String, String), TripMetadata>,
        BTreeMap<(String, String, String), Option<String>>,
    ),
    LookupError,
> {
    use catenary::schema::gtfs::{ingested_static, itinerary_pattern_meta, trips_compressed};

    if history.is_empty() {
        return Ok((BTreeMap::new(), BTreeMap::new()));
    }

    let chateaux = history
        .iter()
        .map(|row| row.chateau.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let trip_ids = history
        .iter()
        .map(|row| row.trip_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let history_keys = history
        .iter()
        .map(|row| {
            (
                row.chateau.clone(),
                row.trip_id.clone(),
                row.route_id.clone(),
            )
        })
        .collect::<HashSet<_>>();

    let mut trip_rows = trips_compressed::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(trips_compressed::onestop_feed_id)
                .and(ingested_static::attempt_id.eq(trips_compressed::attempt_id))),
        )
        .filter(trips_compressed::chateau.eq_any(&chateaux))
        .filter(trips_compressed::trip_id.eq_any(&trip_ids))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select((
            trips_compressed::onestop_feed_id,
            trips_compressed::attempt_id,
            trips_compressed::chateau,
            trips_compressed::trip_id,
            trips_compressed::route_id,
            trips_compressed::trip_short_name,
            trips_compressed::block_id,
            trips_compressed::itinerary_pattern_id,
            trips_compressed::start_time,
        ))
        .load::<(
            String,
            String,
            String,
            String,
            String,
            Option<CompactString>,
            Option<String>,
            String,
            u32,
        )>(conn)
        .await?;

    trip_rows.sort_by(|left, right| {
        (&left.2, &left.3, &left.4, &left.0, &left.1)
            .cmp(&(&right.2, &right.3, &right.4, &right.0, &right.1))
    });

    let mut trips = BTreeMap::new();
    for (
        onestop_feed_id,
        attempt_id,
        chateau,
        trip_id,
        route_id,
        trip_short_name,
        block_id,
        itinerary_pattern_id,
        start_time,
    ) in trip_rows
    {
        let key = (chateau, trip_id, route_id);
        if !history_keys.contains(&key) {
            continue;
        }

        trips.entry(key).or_insert_with(|| TripMetadata {
            onestop_feed_id,
            attempt_id,
            itinerary_pattern_id,
            trip_short_name: trip_short_name.map(|value| value.to_string()),
            block_id,
            start_time,
        });
    }

    let itinerary_ids = trips
        .values()
        .map(|trip| trip.itinerary_pattern_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if itinerary_ids.is_empty() {
        return Ok((trips, BTreeMap::new()));
    }

    let headsign_rows = itinerary_pattern_meta::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(itinerary_pattern_meta::onestop_feed_id)
                .and(ingested_static::attempt_id.eq(itinerary_pattern_meta::attempt_id))),
        )
        .filter(itinerary_pattern_meta::itinerary_pattern_id.eq_any(&itinerary_ids))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select((
            itinerary_pattern_meta::onestop_feed_id,
            itinerary_pattern_meta::attempt_id,
            itinerary_pattern_meta::itinerary_pattern_id,
            itinerary_pattern_meta::trip_headsign,
        ))
        .load::<(String, String, String, Option<String>)>(conn)
        .await?;

    let headsigns = headsign_rows
        .into_iter()
        .map(
            |(onestop_feed_id, attempt_id, itinerary_pattern_id, trip_headsign)| {
                (
                    (onestop_feed_id, attempt_id, itinerary_pattern_id),
                    trip_headsign,
                )
            },
        )
        .collect::<BTreeMap<(String, String, String), Option<String>>>();

    Ok((trips, headsigns))
}

async fn load_routes(
    conn: &mut diesel_async::AsyncPgConnection,
    history: &[BasicVehicleHistory],
) -> Result<HashMap<String, Route>, LookupError> {
    use catenary::schema::gtfs::{ingested_static, routes};

    if history.is_empty() {
        return Ok(HashMap::new());
    }

    let route_pairs = history
        .iter()
        .map(|row| (row.chateau.clone(), row.route_id.clone()))
        .collect::<HashSet<_>>();
    let chateaux = route_pairs
        .iter()
        .map(|(chateau, _)| chateau.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let route_ids = route_pairs
        .iter()
        .map(|(_, route_id)| route_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let mut route_rows = routes::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(routes::onestop_feed_id)
                .and(ingested_static::attempt_id.eq(routes::attempt_id))),
        )
        .filter(routes::chateau.eq_any(&chateaux))
        .filter(routes::route_id.eq_any(&route_ids))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select(Route::as_select())
        .load::<Route>(conn)
        .await?;

    route_rows
        .retain(|route| route_pairs.contains(&(route.chateau.clone(), route.route_id.clone())));
    route_rows.sort_by(|left, right| {
        (
            &left.route_id,
            &left.chateau,
            &left.onestop_feed_id,
            &left.attempt_id,
        )
            .cmp(&(
                &right.route_id,
                &right.chateau,
                &right.onestop_feed_id,
                &right.attempt_id,
            ))
    });

    let mut route_map = HashMap::new();
    for route in route_rows {
        route_map.entry(route.route_id.clone()).or_insert(route);
    }

    Ok(route_map)
}

enum HistoryLookup<'a> {
    Vehicle {
        unified_agency_id: &'a str,
        vehicle_label: &'a str,
    },
    Route {
        chateau: &'a str,
        route_id: &'a str,
        unified_agency_id: &'a str,
    },
}

async fn load_history(
    conn: &mut diesel_async::AsyncPgConnection,
    lookup: HistoryLookup<'_>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<Vec<BasicVehicleHistory>, LookupError> {
    use catenary::schema::gtfs::basic_vehicle_history;

    let mut query = basic_vehicle_history::table.into_boxed();
    query = match lookup {
        HistoryLookup::Vehicle {
            unified_agency_id,
            vehicle_label,
        } => query
            .filter(basic_vehicle_history::unified_agency_id.eq(unified_agency_id))
            .filter(basic_vehicle_history::vehicle_label.eq(vehicle_label)),
        HistoryLookup::Route {
            chateau,
            route_id,
            unified_agency_id,
        } => query
            .filter(basic_vehicle_history::chateau.eq(chateau))
            .filter(basic_vehicle_history::route_id.eq(route_id))
            .filter(basic_vehicle_history::unified_agency_id.eq(unified_agency_id)),
    };

    if let Some(start_date) = start_date {
        query = query.filter(basic_vehicle_history::operation_date.ge(start_date));
    }
    if let Some(end_date) = end_date {
        query = query.filter(basic_vehicle_history::operation_date.le(end_date));
    }

    query
        .order((
            basic_vehicle_history::operation_date.desc(),
            basic_vehicle_history::vehicle_label.asc(),
            basic_vehicle_history::trip_id.asc(),
            basic_vehicle_history::route_id.asc(),
        ))
        .select(BasicVehicleHistory::as_select())
        .load::<BasicVehicleHistory>(conn)
        .await
        .map_err(LookupError::Database)
}

async fn enrich_history(
    conn: &mut diesel_async::AsyncPgConnection,
    history: &[BasicVehicleHistory],
) -> Result<Vec<EnrichedHistoryRow>, LookupError> {
    let (trip_metadata, headsigns) = load_trip_metadata(conn, history).await?;
    let mut seen_history = HashSet::new();
    let mut enriched_history = Vec::new();

    for history_row in history {
        let metadata_key = (
            history_row.chateau.clone(),
            history_row.trip_id.clone(),
            history_row.route_id.clone(),
        );
        let metadata = trip_metadata.get(&metadata_key);
        let direction_headsign = metadata.and_then(|metadata| {
            headsigns
                .get(&(
                    metadata.onestop_feed_id.clone(),
                    metadata.attempt_id.clone(),
                    metadata.itinerary_pattern_id.clone(),
                ))
                .cloned()
                .flatten()
        });
        let block_id = history_row
            .block_id
            .clone()
            .or_else(|| metadata.and_then(|metadata| metadata.block_id.clone()));
        let dedupe_key = (
            history_row.operation_date,
            history_row.vehicle_label.clone(),
            history_row.trip_id.clone(),
            history_row.route_id.clone(),
            block_id.clone(),
        );

        if !seen_history.insert(dedupe_key) {
            continue;
        }

        enriched_history.push(EnrichedHistoryRow {
            operation_date: history_row.operation_date,
            start_time: metadata.map(|metadata| metadata.start_time),
            vehicle_label: history_row.vehicle_label.clone(),
            trip_id: history_row.trip_id.clone(),
            route_id: history_row.route_id.clone(),
            trip_short_name: metadata.and_then(|metadata| metadata.trip_short_name.clone()),
            direction_headsign,
            block_id,
        });
    }

    Ok(enriched_history)
}

#[actix_web::get("/vehicle_history_lookup")]
pub async fn vehicle_history_lookup(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: Query<VehicleHistoryLookupQuery>,
) -> impl Responder {
    let vehicle = match required_parameter(&query.vehicle, "vehicle") {
        Ok(vehicle) => vehicle,
        Err(error) => return error.into_response(),
    };
    let (start_date, end_date) =
        match parse_date_range(query.start_date.as_deref(), query.end_date.as_deref()) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };

    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("vehicle_history_lookup pool error: {error}");
            return LookupError::Internal("Error connecting to postgres".to_string())
                .into_response();
        }
    };

    let resolved_agency = match resolve_agency_for_vehicle(&mut conn, &query, vehicle).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };

    let history = match load_history(
        &mut conn,
        HistoryLookup::Vehicle {
            unified_agency_id: &resolved_agency.unified_agency_id,
            vehicle_label: vehicle,
        },
        start_date,
        end_date,
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) => return error.into_response(),
    };

    let enriched_history = match enrich_history(&mut conn, &history).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let routes = match load_routes(&mut conn, &history).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let agency_timezone = match resolved_agency.timezone.parse::<chrono_tz::Tz>() {
        Ok(timezone) => timezone,
        Err(_) => {
            return LookupError::Internal(format!(
                "Invalid agency timezone: {}",
                resolved_agency.timezone
            ))
            .into_response();
        }
    };
    let mut trip_history = Vec::with_capacity(enriched_history.len());

    for history_row in enriched_history {
        let unix_start_time = match history_row.start_time {
            Some(start_time) => match gtfs_start_time_to_unix_start_time(
                history_row.operation_date,
                start_time,
                &agency_timezone,
            ) {
                Ok(unix_start_time) => Some(unix_start_time),
                Err(error) => return error.into_response(),
            },
            None => None,
        };

        trip_history.push(RouteHistoryRow {
            operation_date: history_row.operation_date,
            unix_start_time,
            trip_id: history_row.trip_id,
            route_id: history_row.route_id,
            trip_short_name: history_row.trip_short_name,
            direction_headsign: history_row.direction_headsign,
            block_id: history_row.block_id,
        });
    }

    trip_history.sort_by(|left, right| {
        right
            .operation_date
            .cmp(&left.operation_date)
            .then_with(|| {
                left.unix_start_time
                    .unwrap_or(u64::MAX)
                    .cmp(&right.unix_start_time.unwrap_or(u64::MAX))
            })
            .then_with(|| left.trip_id.cmp(&right.trip_id))
    });

    HttpResponse::Ok().json(VehicleHistoryLookupResponse {
        trip_history,
        routes,
        agency_timezone: resolved_agency.timezone,
    })
}

#[actix_web::get("/vehicle_history_of_route")]
pub async fn vehicle_history_of_route(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: Query<VehicleHistoryOfRouteQuery>,
) -> impl Responder {
    let chateau = match required_parameter(&query.chateau, "chateau") {
        Ok(chateau) => chateau,
        Err(error) => return error.into_response(),
    };
    let route_id = match required_parameter(&query.route_id, "route_id") {
        Ok(route_id) => route_id,
        Err(error) => return error.into_response(),
    };
    let (start_date, end_date) =
        match parse_date_range(query.start_date.as_deref(), query.end_date.as_deref()) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };

    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("vehicle_history_of_route pool error: {error}");
            return LookupError::Internal("Error connecting to postgres".to_string())
                .into_response();
        }
    };

    let resolved_agency = match resolve_from_chateau(&mut conn, chateau, Some(route_id)).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let history = match load_history(
        &mut conn,
        HistoryLookup::Route {
            chateau,
            route_id,
            unified_agency_id: &resolved_agency.unified_agency_id,
        },
        start_date,
        end_date,
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) => return error.into_response(),
    };
    let trip_history = match enrich_history(&mut conn, &history).await {
        Ok(rows) => rows
            .into_iter()
            .map(|history_row| VehicleHistoryOfRouteRow {
                operation_date: history_row.operation_date,
                vehicle_label: history_row.vehicle_label,
                trip_id: history_row.trip_id,
                trip_short_name: history_row.trip_short_name,
                direction_headsign: history_row.direction_headsign,
                block_id: history_row.block_id,
            })
            .collect(),
        Err(error) => return error.into_response(),
    };

    HttpResponse::Ok().json(VehicleHistoryOfRouteResponse {
        trip_history,
        agency_timezone: resolved_agency.timezone,
    })
}
