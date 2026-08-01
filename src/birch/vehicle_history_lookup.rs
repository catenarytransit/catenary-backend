use actix_web::web::Query;
use actix_web::{HttpResponse, Responder, web};
use catenary::models::{BasicVehicleHistory, Route};
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::NaiveDate;
use compact_str::CompactString;
use diesel::SelectableHelper;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize)]
pub struct VehicleHistoryLookupQuery {
    vehicle: String,
    chateau: Option<String>,
    route_id: Option<String>,
    unified_agency_id: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RouteHistoryRow {
    operation_date: NaiveDate,
    trip_id: String,
    route_id: String,
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

#[derive(Clone, Debug)]
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
}

#[derive(Debug)]
enum LookupError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Database(diesel::result::Error),
}

impl LookupError {
    fn into_response(self) -> HttpResponse {
        match self {
            Self::BadRequest(message) => HttpResponse::BadRequest().body(message),
            Self::NotFound(message) => HttpResponse::NotFound().body(message),
            Self::Conflict(message) => HttpResponse::Conflict().body(message),
            Self::Database(error) => {
                eprintln!("vehicle_history_lookup database error: {error}");
                HttpResponse::InternalServerError().body("Database query failed")
            }
        }
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

async fn production_agencies_for_chateau(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
) -> Result<BTreeSet<(String, Option<String>, String)>, LookupError> {
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
            agencies::agency_id,
            agencies::unified_agency_id,
            agencies::agency_timezone,
        ))
        .load::<(String, Option<String>, String)>(conn)
        .await?;

    Ok(rows.into_iter().collect())
}

fn resolve_one_agency(
    agencies: BTreeSet<(String, Option<String>, String)>,
    required_agency_id: Option<&str>,
    no_match_message: &str,
) -> Result<ResolvedAgency, LookupError> {
    let mut candidates = agencies
        .into_iter()
        .filter(|(agency_id, _, _)| {
            required_agency_id.map_or(true, |required| agency_id == required)
        })
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return Err(LookupError::NotFound(no_match_message.to_string()));
    }

    candidates.sort();
    candidates.dedup();

    if candidates.len() != 1 {
        return Err(LookupError::Conflict(
            "The lookup resolves to multiple agencies; provide chateau and route_id, or unified_agency_id"
                .to_string(),
        ));
    }

    let (_, unified_agency_id, timezone) = candidates.remove(0);
    let unified_agency_id = unified_agency_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            LookupError::NotFound("The resolved agency has no unified_agency_id".to_string())
        })?;

    Ok(ResolvedAgency {
        unified_agency_id,
        timezone,
    })
}

async fn resolve_from_chateau(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
    route_id_filter: Option<&str>,
) -> Result<ResolvedAgency, LookupError> {
    use catenary::schema::gtfs::{ingested_static, routes};

    let agencies = production_agencies_for_chateau(conn, chateau_id).await?;

    let Some(route_id_filter) = route_id_filter else {
        return resolve_one_agency(
            agencies,
            None,
            "No production agency was found for the chateau",
        );
    };

    let route_agency_ids = routes::table
        .inner_join(
            ingested_static::table.on(ingested_static::onestop_feed_id
                .eq(routes::onestop_feed_id)
                .and(ingested_static::attempt_id.eq(routes::attempt_id))),
        )
        .filter(routes::chateau.eq(chateau_id))
        .filter(routes::route_id.eq(route_id_filter))
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select(routes::agency_id)
        .load::<Option<String>>(conn)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();

    if route_agency_ids.is_empty() {
        return Err(LookupError::NotFound(
            "No production route was found for chateau and route_id".to_string(),
        ));
    }

    if route_agency_ids.len() != 1 {
        return Err(LookupError::Conflict(
            "The route_id resolves to multiple agencies in this chateau".to_string(),
        ));
    }

    let route_agency_id = route_agency_ids.into_iter().next().flatten();
    resolve_one_agency(
        agencies,
        route_agency_id.as_deref(),
        "No production agency was found for the route",
    )
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

async fn resolve_agency(
    conn: &mut diesel_async::AsyncPgConnection,
    query: &VehicleHistoryLookupQuery,
) -> Result<ResolvedAgency, LookupError> {
    let chateau = non_empty_parameter(&query.chateau, "chateau")?;
    let route_id = non_empty_parameter(&query.route_id, "route_id")?;
    let unified_agency_id = non_empty_parameter(&query.unified_agency_id, "unified_agency_id")?;

    match (chateau, route_id, unified_agency_id) {
        (Some(chateau), None, None) => resolve_from_chateau(conn, chateau, None).await,
        (Some(chateau), Some(route_id), None) => {
            resolve_from_chateau(conn, chateau, Some(route_id)).await
        }
        (None, None, Some(unified_agency_id)) => {
            resolve_from_unified_agency(conn, unified_agency_id).await
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

#[actix_web::get("/vehicle_history_lookup")]
pub async fn vehicle_history_lookup(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    query: Query<VehicleHistoryLookupQuery>,
) -> impl Responder {
    use catenary::schema::gtfs::{basic_vehicle_history, basic_vehicles};

    let vehicle = query.vehicle.trim();
    if vehicle.is_empty() {
        return HttpResponse::BadRequest().body("vehicle cannot be empty");
    }

    let start_date = match parse_optional_date(query.start_date.as_deref(), "start_date") {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let end_date = match parse_optional_date(query.end_date.as_deref(), "end_date") {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };

    if matches!(
        (start_date.as_ref(), end_date.as_ref()),
        (Some(start), Some(end)) if start > end
    ) {
        return HttpResponse::BadRequest().body("start_date cannot be after end_date");
    }

    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!("vehicle_history_lookup pool error: {error}");
            return HttpResponse::InternalServerError().body("Error connecting to postgres");
        }
    };

    let resolved_agency = match resolve_agency(&mut conn, &query).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };

    let vehicle_exists = basic_vehicles::table
        .filter(basic_vehicles::unified_agency_id.eq(&resolved_agency.unified_agency_id))
        .filter(basic_vehicles::vehicle_label.eq(vehicle))
        .select(basic_vehicles::vehicle_label)
        .first::<String>(&mut conn)
        .await
        .optional();

    match vehicle_exists {
        Ok(Some(_)) => {}
        Ok(None) => {
            return HttpResponse::NotFound()
                .body("No vehicle was found for unified agency and vehicle label");
        }
        Err(error) => return LookupError::Database(error).into_response(),
    }

    let mut history_query = basic_vehicle_history::table
        .filter(basic_vehicle_history::unified_agency_id.eq(&resolved_agency.unified_agency_id))
        .filter(basic_vehicle_history::vehicle_label.eq(vehicle))
        .into_boxed();

    if let Some(start_date) = start_date {
        history_query = history_query.filter(basic_vehicle_history::operation_date.ge(start_date));
    }
    if let Some(end_date) = end_date {
        history_query = history_query.filter(basic_vehicle_history::operation_date.le(end_date));
    }

    let history = match history_query
        .order((
            basic_vehicle_history::operation_date.desc(),
            basic_vehicle_history::trip_id.asc(),
            basic_vehicle_history::route_id.asc(),
        ))
        .select(BasicVehicleHistory::as_select())
        .load::<BasicVehicleHistory>(&mut conn)
        .await
    {
        Ok(rows) => rows,
        Err(error) => return LookupError::Database(error).into_response(),
    };

    let (trip_metadata, headsigns) = match load_trip_metadata(&mut conn, &history).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let routes = match load_routes(&mut conn, &history).await {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };

    let mut seen_history = HashSet::new();
    let mut trip_history = Vec::new();

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
            history_row.trip_id.clone(),
            history_row.route_id.clone(),
            block_id.clone(),
        );

        if !seen_history.insert(dedupe_key) {
            continue;
        }

        trip_history.push(RouteHistoryRow {
            operation_date: history_row.operation_date,
            trip_id: history_row.trip_id,
            route_id: history_row.route_id,
            trip_short_name: metadata.and_then(|metadata| metadata.trip_short_name.clone()),
            direction_headsign,
            block_id,
        });
    }

    HttpResponse::Ok().json(VehicleHistoryLookupResponse {
        trip_history,
        routes,
        agency_timezone: resolved_agency.timezone,
    })
}
