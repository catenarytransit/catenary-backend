use ahash::AHashMap;
use catenary::aspen_dataset::{
    AspenisedTripUpdate, AspenisedVehiclePosition, AspenisedVehicleRouteCache,
    CompressedTripInternalCache,
};
use catenary::models::{BasicVehicle, BasicVehicleHistory};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::{
    agencies, basic_vehicle_history, basic_vehicles, ingested_static, unified_agency,
};
use chrono::NaiveDate;
use compact_str::CompactString;
use diesel::prelude::*;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

const CANADA_COUNTRY_CODE: &str = "CA";
const CALIFORNIA_LEVEL_1_CODE: &str = "US-CA";
const UPSERT_BATCH_SIZE: usize = 1_000;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug)]
struct VehicleTripObservation {
    vehicle_label: String,
    trip_id: String,
    route_id: String,
    operation_date: NaiveDate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgencyScope {
    unified_agency_id: Option<String>,
    rollout_enabled: bool,
}

#[derive(Clone, Debug, Default)]
struct AgencyScopeAccumulator {
    unified_agency_ids: BTreeSet<String>,
    rollout_enabled: bool,
}

fn contains_area_code(values: &Option<Vec<Option<String>>>, expected: &str) -> bool {
    values
        .as_deref()
        .unwrap_or_default()
        .iter()
        .flatten()
        .any(|value| value.eq_ignore_ascii_case(expected))
}

fn rollout_enabled(
    level_0s: &Option<Vec<Option<String>>>,
    level_1s: &Option<Vec<Option<String>>>,
) -> bool {
    contains_area_code(level_0s, CANADA_COUNTRY_CODE)
        || contains_area_code(level_1s, CALIFORNIA_LEVEL_1_CODE)
}

fn observation_from_trip(
    vehicle_label: Option<&str>,
    trip_id: Option<&str>,
    route_id: Option<&str>,
    operation_date: Option<NaiveDate>,
    compressed_trip_cache: &CompressedTripInternalCache,
) -> Option<VehicleTripObservation> {
    let vehicle_label = vehicle_label.filter(|value| !value.trim().is_empty())?;
    let trip_id = trip_id.filter(|value| !value.trim().is_empty())?;
    let operation_date = operation_date?;
    let compressed_trip = compressed_trip_cache.compressed_trips.get(trip_id);
    let route_id = route_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| compressed_trip.map(|trip| trip.route_id.as_str()))?;

    Some(VehicleTripObservation {
        vehicle_label: vehicle_label.to_string(),
        trip_id: trip_id.to_string(),
        route_id: route_id.to_string(),
        operation_date,
    })
}

fn collect_observations(
    vehicle_positions: &AHashMap<String, AspenisedVehiclePosition>,
    trip_updates: &AHashMap<CompactString, AspenisedTripUpdate>,
    compressed_trip_cache: &CompressedTripInternalCache,
) -> BTreeMap<(String, NaiveDate, String), VehicleTripObservation> {
    let mut observations = BTreeMap::new();

    for vehicle_position in vehicle_positions.values() {
        let observation = vehicle_position.trip.as_ref().and_then(|trip| {
            observation_from_trip(
                vehicle_position
                    .vehicle
                    .as_ref()
                    .and_then(|vehicle| vehicle.label.as_deref().or(vehicle.id.as_deref())),
                trip.trip_id.as_deref(),
                trip.route_id.as_deref(),
                trip.start_date.clone(),
                compressed_trip_cache,
            )
        });

        if let Some(observation) = observation {
            observations.insert(
                (
                    observation.vehicle_label.clone(),
                    observation.operation_date,
                    observation.trip_id.clone(),
                ),
                observation,
            );
        }
    }

    for trip_update in trip_updates.values() {
        let observation = observation_from_trip(
            trip_update
                .vehicle
                .as_ref()
                .and_then(|vehicle| vehicle.label.as_deref().or(vehicle.id.as_deref())),
            trip_update.trip.trip_id.as_deref(),
            trip_update.trip.route_id.as_deref(),
            trip_update.trip.start_date.clone(),
            compressed_trip_cache,
        );

        if let Some(observation) = observation {
            observations.insert(
                (
                    observation.vehicle_label.clone(),
                    observation.operation_date,
                    observation.trip_id.clone(),
                ),
                observation,
            );
        }
    }

    observations
}

async fn load_agency_scopes(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
) -> Result<BTreeMap<String, AgencyScope>, diesel::result::Error> {
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
            agencies::level_0s,
            agencies::level_1s,
        ))
        .load::<(
            String,
            Option<String>,
            Option<Vec<Option<String>>>,
            Option<Vec<Option<String>>>,
        )>(conn)
        .await?;

    // The rollout metadata is sometimes available on gtfs.unified_agency before it has
    // been copied onto every production gtfs.agencies row. Metro Los Angeles is one such
    // case: its unified agency is US-CA, so it must be considered eligible even when an
    // individual static-feed agency row has NULL level_0s/level_1s.
    let unified_agency_ids = rows
        .iter()
        .filter_map(|(_, unified_agency_id, _, _)| unified_agency_id.as_deref())
        .map(str::trim)
        .filter(|unified_agency_id| !unified_agency_id.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let unified_rollout = if unified_agency_ids.is_empty() {
        BTreeMap::new()
    } else {
        unified_agency::table
            .filter(unified_agency::id.eq_any(&unified_agency_ids))
            .select((
                unified_agency::id,
                unified_agency::level_0s,
                unified_agency::level_1s,
            ))
            .load::<(
                String,
                Option<Vec<Option<String>>>,
                Option<Vec<Option<String>>>,
            )>(conn)
            .await?
            .into_iter()
            .map(|(id, level_0s, level_1s)| (id, rollout_enabled(&level_0s, &level_1s)))
            .collect::<BTreeMap<_, _>>()
    };

    // A chateau can contain more than one production static feed with the same agency_id.
    // Missing spatial metadata in one duplicate must not disable a valid agency found in
    // another duplicate. Merge eligibility with OR and only drop the unified ID itself if
    // genuinely conflicting unified IDs are present.
    let mut accumulators = BTreeMap::<String, AgencyScopeAccumulator>::new();
    for (agency_id, unified_agency_id, level_0s, level_1s) in rows {
        let unified_agency_id = unified_agency_id.filter(|value| !value.trim().is_empty());
        let is_rollout_enabled = rollout_enabled(&level_0s, &level_1s)
            || unified_agency_id
                .as_ref()
                .and_then(|id| unified_rollout.get(id))
                .copied()
                .unwrap_or(false);

        let accumulator = accumulators.entry(agency_id).or_default();
        accumulator.rollout_enabled |= is_rollout_enabled;
        if let Some(unified_agency_id) = unified_agency_id {
            accumulator.unified_agency_ids.insert(unified_agency_id);
        }
    }

    Ok(accumulators
        .into_iter()
        .map(|(agency_id, accumulator)| {
            let unified_agency_id = if accumulator.unified_agency_ids.len() == 1 {
                accumulator.unified_agency_ids.into_iter().next()
            } else {
                None
            };

            (
                agency_id,
                AgencyScope {
                    unified_agency_id,
                    rollout_enabled: accumulator.rollout_enabled,
                },
            )
        })
        .collect())
}

fn resolve_agency<'a>(
    route_agency_id: Option<&str>,
    agency_scopes: &'a BTreeMap<String, AgencyScope>,
) -> Option<(&'a str, &'a AgencyScope)> {
    match route_agency_id {
        Some(agency_id) => agency_scopes
            .get_key_value(agency_id)
            .map(|(agency_id, scope)| (agency_id.as_str(), scope)),
        None if agency_scopes.len() == 1 => agency_scopes
            .first_key_value()
            .map(|(agency_id, scope)| (agency_id.as_str(), scope)),
        None => None,
    }
}

async fn upsert_history_rows(
    conn: &mut diesel_async::AsyncPgConnection,
    rows: &[BasicVehicleHistory],
) -> Result<(), diesel::result::Error> {
    for chunk in rows.chunks(UPSERT_BATCH_SIZE) {
        diesel::insert_into(basic_vehicle_history::table)
            .values(chunk)
            .on_conflict((
                basic_vehicle_history::realtime_feed_id,
                basic_vehicle_history::vehicle_label,
                basic_vehicle_history::operation_date,
                basic_vehicle_history::trip_id,
            ))
            .do_update()
            .set((
                basic_vehicle_history::chateau.eq(excluded(basic_vehicle_history::chateau)),
                basic_vehicle_history::route_id.eq(excluded(basic_vehicle_history::route_id)),
                basic_vehicle_history::agency_id.eq(excluded(basic_vehicle_history::agency_id)),
                basic_vehicle_history::unified_agency_id
                    .eq(excluded(basic_vehicle_history::unified_agency_id)),
                basic_vehicle_history::block_id.eq(excluded(basic_vehicle_history::block_id)),
            ))
            .execute(conn)
            .await?;
    }

    Ok(())
}

async fn upsert_vehicle_rows(
    conn: &mut diesel_async::AsyncPgConnection,
    rows: &[BasicVehicle],
) -> Result<(), diesel::result::Error> {
    for chunk in rows.chunks(UPSERT_BATCH_SIZE) {
        diesel::insert_into(basic_vehicles::table)
            .values(chunk)
            .on_conflict((
                basic_vehicles::unified_agency_id,
                basic_vehicles::vehicle_label,
            ))
            .do_update()
            .set((
                basic_vehicles::trip_id.eq(excluded(basic_vehicles::trip_id)),
                basic_vehicles::block_id.eq(excluded(basic_vehicles::block_id)),
            ))
            .execute(conn)
            .await?;
    }

    Ok(())
}

async fn mark_unified_agencies_with_histories(
    conn: &mut diesel_async::AsyncPgConnection,
    unified_agency_ids: &[String],
) -> Result<(), diesel::result::Error> {
    for chunk in unified_agency_ids.chunks(UPSERT_BATCH_SIZE) {
        diesel::update(unified_agency::table.filter(unified_agency::id.eq_any(chunk)))
            .set(unified_agency::has_vehicle_histories.eq(true))
            .execute(conn)
            .await?;
    }

    Ok(())
}

pub async fn upsert_basic_vehicle_history(
    pool: &CatenaryPostgresPool,
    chateau_id: &str,
    realtime_feed_id: &str,
    vehicle_positions: &AHashMap<String, AspenisedVehiclePosition>,
    trip_updates: &AHashMap<CompactString, AspenisedTripUpdate>,
    vehicle_routes_cache: &AHashMap<String, AspenisedVehicleRouteCache>,
    compressed_trip_cache: &CompressedTripInternalCache,
) -> Result<(), DynError> {
    let observations = collect_observations(vehicle_positions, trip_updates, compressed_trip_cache);
    if observations.is_empty() {
        return Ok(());
    }

    let mut conn = pool.get().await?;
    let agency_scopes = load_agency_scopes(&mut conn, chateau_id).await?;
    if agency_scopes.is_empty() {
        return Ok(());
    }

    let mut history_rows = Vec::new();
    let mut vehicle_rows = BTreeMap::new();

    for observation in observations.into_values() {
        let route_agency_id = vehicle_routes_cache
            .get(observation.route_id.as_str())
            .and_then(|route| route.agency_id.as_deref());
        let Some((agency_id, agency_scope)) = resolve_agency(route_agency_id, &agency_scopes)
        else {
            continue;
        };
        if !agency_scope.rollout_enabled {
            continue;
        }

        let block_id = compressed_trip_cache
            .compressed_trips
            .get(observation.trip_id.as_str())
            .and_then(|trip| trip.block_id.clone());

        history_rows.push(BasicVehicleHistory {
            realtime_feed_id: realtime_feed_id.to_string(),
            chateau: chateau_id.to_string(),
            route_id: observation.route_id.clone(),
            agency_id: Some(agency_id.to_string()),
            unified_agency_id: agency_scope.unified_agency_id.clone(),
            vehicle_label: observation.vehicle_label.clone(),
            trip_id: observation.trip_id.clone(),
            block_id: block_id.clone(),
            operation_date: observation.operation_date,
        });

        if let Some(unified_agency_id) = &agency_scope.unified_agency_id {
            // observations is ordered by label, operation date, and trip id, so a later
            // insert deterministically keeps the newest operation date for this vehicle.
            vehicle_rows.insert(
                (unified_agency_id.clone(), observation.vehicle_label.clone()),
                BasicVehicle {
                    unified_agency_id: unified_agency_id.clone(),
                    vehicle_label: observation.vehicle_label,
                    trip_id: Some(observation.trip_id),
                    block_id,
                    model: None,
                    manufacturer: None,
                    manufacture_year: None,
                },
            );
        }
    }

    if history_rows.is_empty() {
        return Ok(());
    }

    let vehicle_rows = vehicle_rows.into_values().collect::<Vec<_>>();
    let unified_agency_ids = vehicle_rows
        .iter()
        .map(|vehicle| vehicle.unified_agency_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    upsert_history_rows(&mut conn, &history_rows).await?;
    upsert_vehicle_rows(&mut conn, &vehicle_rows).await?;
    mark_unified_agencies_with_histories(&mut conn, &unified_agency_ids).await?;

    Ok(())
}
