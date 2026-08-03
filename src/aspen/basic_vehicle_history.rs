use ahash::AHashMap;
use catenary::aspen_dataset::{
    AspenisedTripUpdate, AspenisedVehiclePosition, AspenisedVehicleRouteCache,
    CompressedTripInternalCache,
};
use catenary::models::{BasicVehicle, BasicVehicleHistory};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::{
    agencies, basic_vehicle_history, basic_vehicles, ingested_static, routes, unified_agency,
};
use chrono::{DateTime, Days, Duration, LocalResult, NaiveDate, NaiveTime, TimeZone, Utc};
use compact_str::CompactString;
use diesel::prelude::*;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

const CANADA_COUNTRY_CODE: &str = "CA";
const ENABLED_US_LEVEL_1_CODES: &[&str] = &["US-CA", "US-IL", "US-NY", "US-DC", "US-MA"];
const UPSERT_BATCH_SIZE: usize = 1_000;
const MAX_EARLY_START_SECONDS: i64 = 2 * 60 * 60;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug)]
struct VehicleTripObservation {
    vehicle_label: String,
    trip_id: String,
    route_id: String,
    operation_date: Option<NaiveDate>,
    observed_at_unix: Option<u64>,
    gtfs_start_time: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgencyScope {
    unified_agency_id: Option<String>,
    rollout_enabled: bool,
    agency_timezone: Option<String>,
}

#[derive(Clone, Debug)]
struct AgencyLookups {
    scopes_by_agency_id: BTreeMap<String, AgencyScope>,
    agency_id_by_static_feed: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct AgencyScopeAccumulator {
    unified_agency_ids: BTreeSet<String>,
    agency_timezones: BTreeSet<String>,
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
        || ENABLED_US_LEVEL_1_CODES
            .iter()
            .any(|code| contains_area_code(level_1s, code))
}

fn route_history_is_excluded(chateau_id: &str, route_type: Option<i16>) -> bool {
    match chateau_id {
        "gotransit" | "metrolinktrains" | "upexpress" => true,
        "san-diego-mts" => route_type == Some(0),
        "nyct" => matches!(route_type, Some(1) | Some(2)),
        _ => false,
    }
}

fn parse_gtfs_start_time(value: Option<&str>) -> Option<u32> {
    let mut parts = value?.split(':');
    let hours = parts.next()?.parse::<u32>().ok()?;
    let minutes = parts.next()?.parse::<u32>().ok()?;
    let seconds = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() || minutes >= 60 || seconds >= 60 {
        return None;
    }

    hours
        .checked_mul(60 * 60)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)
}

fn observation_from_trip(
    vehicle_label: Option<&str>,
    trip_id: Option<&str>,
    route_id: Option<&str>,
    start_time: Option<&str>,
    operation_date: Option<NaiveDate>,
    observed_at_unix: Option<u64>,
    compressed_trip_cache: &CompressedTripInternalCache,
) -> Option<VehicleTripObservation> {
    let vehicle_label = vehicle_label.filter(|value| !value.trim().is_empty())?;
    let trip_id = trip_id.filter(|value| !value.trim().is_empty())?;
    let compressed_trip = compressed_trip_cache.compressed_trips.get(trip_id);
    let route_id = route_id
        .filter(|value| !value.trim().is_empty())
        .or_else(|| compressed_trip.map(|trip| trip.route_id.as_str()))?;
    let gtfs_start_time =
        parse_gtfs_start_time(start_time).or_else(|| compressed_trip.map(|trip| trip.start_time));

    Some(VehicleTripObservation {
        vehicle_label: vehicle_label.to_string(),
        trip_id: trip_id.to_string(),
        route_id: route_id.to_string(),
        operation_date,
        observed_at_unix,
        gtfs_start_time,
    })
}

fn merge_observation(
    observations: &mut BTreeMap<(String, String), VehicleTripObservation>,
    observation: VehicleTripObservation,
) {
    let key = (
        observation.vehicle_label.clone(),
        observation.trip_id.clone(),
    );

    match observations.entry(key) {
        Entry::Vacant(entry) => {
            entry.insert(observation);
        }
        Entry::Occupied(mut entry) => {
            let existing = entry.get_mut();
            if existing.operation_date.is_none() {
                existing.operation_date = observation.operation_date;
            }
            if existing.gtfs_start_time.is_none() {
                existing.gtfs_start_time = observation.gtfs_start_time;
            }
            if observation.observed_at_unix > existing.observed_at_unix {
                existing.observed_at_unix = observation.observed_at_unix;
            }
        }
    }
}

fn collect_observations(
    vehicle_positions: &AHashMap<String, AspenisedVehiclePosition>,
    trip_updates: &AHashMap<CompactString, AspenisedTripUpdate>,
    compressed_trip_cache: &CompressedTripInternalCache,
) -> BTreeMap<(String, String), VehicleTripObservation> {
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
                trip.start_time.as_deref(),
                trip.start_date,
                vehicle_position.timestamp,
                compressed_trip_cache,
            )
        });

        if let Some(observation) = observation {
            merge_observation(&mut observations, observation);
        }
    }

    for trip_update in trip_updates.values() {
        let trip_properties = trip_update.trip_properties.as_ref();
        let observation = observation_from_trip(
            trip_update
                .vehicle
                .as_ref()
                .and_then(|vehicle| vehicle.label.as_deref().or(vehicle.id.as_deref())),
            trip_update.trip.trip_id.as_deref(),
            trip_update.trip.route_id.as_deref(),
            trip_update.trip.start_time.as_deref().or_else(|| {
                trip_properties.and_then(|properties| properties.start_time.as_deref())
            }),
            trip_update
                .trip
                .start_date
                .or_else(|| trip_properties.and_then(|properties| properties.start_date)),
            trip_update.timestamp,
            compressed_trip_cache,
        );

        if let Some(observation) = observation {
            merge_observation(&mut observations, observation);
        }
    }

    observations
}

fn local_service_start(
    service_date: NaiveDate,
    gtfs_start_time: u32,
    timezone: chrono_tz::Tz,
) -> Option<DateTime<chrono_tz::Tz>> {
    let service_day_offset = u64::from(gtfs_start_time / (24 * 60 * 60));
    let seconds_after_midnight = gtfs_start_time % (24 * 60 * 60);
    let local_date = service_date.checked_add_days(Days::new(service_day_offset))?;
    let local_time = NaiveTime::from_num_seconds_from_midnight_opt(seconds_after_midnight, 0)?;
    let local_start = local_date.and_time(local_time);

    match timezone.from_local_datetime(&local_start) {
        LocalResult::Single(value) => Some(value),
        LocalResult::Ambiguous(first, second) => Some(first.min(second)),
        LocalResult::None => None,
    }
}

fn infer_operation_date(
    explicit_operation_date: Option<NaiveDate>,
    observed_at_unix: Option<u64>,
    gtfs_start_time: Option<u32>,
    agency_timezone: Option<&str>,
) -> NaiveDate {
    if let Some(operation_date) = explicit_operation_date {
        return operation_date;
    }

    let observed_at = observed_at_unix
        .and_then(|timestamp| i64::try_from(timestamp).ok())
        .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0))
        .unwrap_or_else(Utc::now);
    let Some(timezone) = agency_timezone.and_then(|value| value.parse::<chrono_tz::Tz>().ok())
    else {
        return observed_at.date_naive();
    };
    let local_observed_at = observed_at.with_timezone(&timezone);
    let local_date = local_observed_at.date_naive();
    let Some(gtfs_start_time) = gtfs_start_time else {
        return local_date;
    };

    // A vehicle can appear shortly before its scheduled start, but a trip observed just
    // after midnight may still belong to the previous GTFS service day. Check nearby
    // service dates and choose the latest scheduled start that is not implausibly future.
    let latest_allowed_start = local_observed_at + Duration::seconds(MAX_EARLY_START_SECONDS);
    let mut best_match = None;
    for day_offset in -4_i64..=1 {
        let service_date = if day_offset < 0 {
            local_date.checked_sub_days(Days::new(day_offset.unsigned_abs()))
        } else {
            local_date.checked_add_days(Days::new(day_offset as u64))
        };
        let Some(service_date) = service_date else {
            continue;
        };
        let Some(scheduled_start) = local_service_start(service_date, gtfs_start_time, timezone)
        else {
            continue;
        };
        if scheduled_start > latest_allowed_start {
            continue;
        }

        let should_replace = best_match
            .as_ref()
            .is_none_or(|(_, best_start)| scheduled_start > *best_start);
        if should_replace {
            best_match = Some((service_date, scheduled_start));
        }
    }

    best_match
        .map(|(service_date, _)| service_date)
        .unwrap_or(local_date)
}

async fn load_agency_scopes(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
) -> Result<AgencyLookups, diesel::result::Error> {
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
            agencies::level_0s,
            agencies::level_1s,
            agencies::agency_timezone,
        ))
        .load::<(
            String,
            String,
            Option<String>,
            Option<Vec<Option<String>>>,
            Option<Vec<Option<String>>>,
            String,
        )>(conn)
        .await?;

    // The rollout metadata is sometimes available on gtfs.unified_agency before it has
    // been copied onto every production gtfs.agencies row. Metro Los Angeles is one such
    // case: its unified agency is US-CA, so it must be considered eligible even when an
    // individual static-feed agency row has NULL level_0s/level_1s.
    let unified_agency_ids = rows
        .iter()
        .filter_map(|(_, _, unified_agency_id, _, _, _)| unified_agency_id.as_deref())
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
    let mut feed_agency_ids = BTreeMap::<String, BTreeSet<String>>::new();
    for (static_onestop_id, agency_id, unified_agency_id, level_0s, level_1s, agency_timezone) in
        rows
    {
        feed_agency_ids
            .entry(static_onestop_id)
            .or_default()
            .insert(agency_id.clone());

        let unified_agency_id = unified_agency_id.filter(|value| !value.trim().is_empty());
        let is_rollout_enabled = rollout_enabled(&level_0s, &level_1s)
            || unified_agency_id
                .as_ref()
                .and_then(|id| unified_rollout.get(id))
                .copied()
                .unwrap_or(false);

        let accumulator = accumulators.entry(agency_id).or_default();
        accumulator.rollout_enabled |= is_rollout_enabled;
        let agency_timezone = agency_timezone.trim();
        if !agency_timezone.is_empty() {
            accumulator
                .agency_timezones
                .insert(agency_timezone.to_string());
        }
        if let Some(unified_agency_id) = unified_agency_id {
            accumulator.unified_agency_ids.insert(unified_agency_id);
        }
    }

    let scopes_by_agency_id = accumulators
        .into_iter()
        .map(|(agency_id, accumulator)| {
            let unified_agency_id = if accumulator.unified_agency_ids.len() == 1 {
                accumulator.unified_agency_ids.into_iter().next()
            } else {
                None
            };
            let agency_timezone = if accumulator.agency_timezones.len() == 1 {
                accumulator.agency_timezones.into_iter().next()
            } else {
                None
            };

            (
                agency_id,
                AgencyScope {
                    unified_agency_id,
                    rollout_enabled: accumulator.rollout_enabled,
                    agency_timezone,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let agency_id_by_static_feed = feed_agency_ids
        .into_iter()
        .filter_map(|(static_onestop_id, agency_ids)| {
            if agency_ids.len() != 1 {
                return None;
            }

            let agency_id = agency_ids.into_iter().next()?;
            scopes_by_agency_id
                .contains_key(&agency_id)
                .then_some((static_onestop_id, agency_id))
        })
        .collect();

    Ok(AgencyLookups {
        scopes_by_agency_id,
        agency_id_by_static_feed,
    })
}

async fn load_route_static_feed_ids(
    conn: &mut diesel_async::AsyncPgConnection,
    chateau_id: &str,
    route_ids: &[String],
) -> Result<BTreeMap<String, String>, diesel::result::Error> {
    let mut static_feeds_by_route = BTreeMap::<String, BTreeSet<String>>::new();

    for chunk in route_ids.chunks(UPSERT_BATCH_SIZE) {
        let rows = routes::table
            .inner_join(
                ingested_static::table.on(ingested_static::onestop_feed_id
                    .eq(routes::onestop_feed_id)
                    .and(ingested_static::attempt_id.eq(routes::attempt_id))),
            )
            .filter(routes::chateau.eq(chateau_id))
            .filter(routes::route_id.eq_any(chunk))
            .filter(ingested_static::production.eq(true))
            .filter(ingested_static::deleted.eq(false))
            .select((routes::route_id, routes::onestop_feed_id))
            .load::<(String, String)>(conn)
            .await?;

        for (route_id, static_onestop_id) in rows {
            static_feeds_by_route
                .entry(route_id)
                .or_default()
                .insert(static_onestop_id);
        }
    }

    Ok(static_feeds_by_route
        .into_iter()
        .filter_map(|(route_id, static_feed_ids)| {
            if static_feed_ids.len() != 1 {
                return None;
            }

            static_feed_ids
                .into_iter()
                .next()
                .map(|static_feed_id| (route_id, static_feed_id))
        })
        .collect())
}

fn resolve_agency<'a>(
    route_agency_id: Option<&str>,
    route_static_feed_id: Option<&str>,
    agency_lookups: &'a AgencyLookups,
) -> Option<(&'a str, &'a AgencyScope)> {
    if let Some(agency_id) = route_agency_id {
        return agency_lookups
            .scopes_by_agency_id
            .get_key_value(agency_id)
            .map(|(agency_id, scope)| (agency_id.as_str(), scope));
    }

    if let Some(static_feed_id) = route_static_feed_id {
        if let Some(agency_id) = agency_lookups.agency_id_by_static_feed.get(static_feed_id) {
            return agency_lookups
                .scopes_by_agency_id
                .get_key_value(agency_id)
                .map(|(agency_id, scope)| (agency_id.as_str(), scope));
        }
    }

    if agency_lookups.scopes_by_agency_id.len() == 1 {
        return agency_lookups
            .scopes_by_agency_id
            .first_key_value()
            .map(|(agency_id, scope)| (agency_id.as_str(), scope));
    }

    None
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
    if route_history_is_excluded(chateau_id, None) {
        return Ok(());
    }

    let observations = collect_observations(vehicle_positions, trip_updates, compressed_trip_cache);
    if observations.is_empty() {
        return Ok(());
    }

    let mut conn = pool.get().await?;
    let agency_lookups = load_agency_scopes(&mut conn, chateau_id).await?;
    if agency_lookups.scopes_by_agency_id.is_empty() {
        return Ok(());
    }

    // GTFS permits route.agency_id to be NULL when a static feed contains one agency.
    // Resolve those routes through the production static feed that owns the route instead
    // of requiring the entire chateau to contain only one agency.
    let route_ids_needing_feed_lookup = observations
        .values()
        .filter(|observation| {
            vehicle_routes_cache
                .get(observation.route_id.as_str())
                .and_then(|route| route.agency_id.as_deref())
                .filter(|agency_id| !agency_id.trim().is_empty())
                .is_none()
        })
        .map(|observation| observation.route_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let route_static_feed_ids =
        load_route_static_feed_ids(&mut conn, chateau_id, &route_ids_needing_feed_lookup).await?;

    let mut history_rows = Vec::new();
    let mut vehicle_rows = BTreeMap::new();

    for observation in observations.into_values() {
        let route = vehicle_routes_cache.get(observation.route_id.as_str());
        if route_history_is_excluded(chateau_id, route.map(|route| route.route_type)) {
            continue;
        }

        let route_agency_id = route
            .and_then(|route| route.agency_id.as_deref())
            .filter(|agency_id| !agency_id.trim().is_empty());
        let route_static_feed_id = route_static_feed_ids
            .get(observation.route_id.as_str())
            .map(String::as_str);
        let Some((agency_id, agency_scope)) =
            resolve_agency(route_agency_id, route_static_feed_id, &agency_lookups)
        else {
            continue;
        };
        if !agency_scope.rollout_enabled {
            continue;
        }

        let operation_date = infer_operation_date(
            observation.operation_date,
            observation.observed_at_unix,
            observation.gtfs_start_time,
            agency_scope.agency_timezone.as_deref(),
        );
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
            operation_date,
        });

        if let Some(unified_agency_id) = &agency_scope.unified_agency_id {
            let key = (unified_agency_id.clone(), observation.vehicle_label.clone());
            let vehicle = BasicVehicle {
                unified_agency_id: unified_agency_id.clone(),
                vehicle_label: observation.vehicle_label,
                trip_id: Some(observation.trip_id),
                block_id,
                model: None,
                manufacturer: None,
                manufacture_year: None,
            };
            match vehicle_rows.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert((operation_date, vehicle));
                }
                Entry::Occupied(mut entry) if operation_date >= entry.get().0 => {
                    entry.insert((operation_date, vehicle));
                }
                Entry::Occupied(_) => {}
            }
        }
    }

    if history_rows.is_empty() {
        return Ok(());
    }

    let vehicle_rows = vehicle_rows
        .into_values()
        .map(|(_, vehicle)| vehicle)
        .collect::<Vec<_>>();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn toronto_timestamp(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> u64 {
        let timezone = "America/Toronto".parse::<chrono_tz::Tz>().unwrap();
        timezone
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .timestamp() as u64
    }

    #[test]
    fn preserves_explicit_operation_date() {
        let explicit = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let inferred = infer_operation_date(
            Some(explicit),
            Some(toronto_timestamp(2026, 8, 1, 12, 0)),
            Some(12 * 60 * 60),
            Some("America/Toronto"),
        );
        assert_eq!(inferred, explicit);
    }

    #[test]
    fn infers_toronto_operation_date_when_start_date_is_missing() {
        let inferred = infer_operation_date(
            None,
            Some(toronto_timestamp(2026, 8, 1, 15, 0)),
            Some(14 * 60 * 60),
            Some("America/Toronto"),
        );
        assert_eq!(inferred, NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
    }

    #[test]
    fn assigns_after_midnight_trip_to_previous_service_day() {
        let inferred = infer_operation_date(
            None,
            Some(toronto_timestamp(2026, 8, 2, 0, 30)),
            Some(23 * 60 * 60),
            Some("America/Toronto"),
        );
        assert_eq!(inferred, NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
    }

    #[test]
    fn handles_gtfs_start_times_above_24_hours() {
        let inferred = infer_operation_date(
            None,
            Some(toronto_timestamp(2026, 8, 2, 1, 10)),
            Some(25 * 60 * 60),
            Some("America/Toronto"),
        );
        assert_eq!(inferred, NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
    }

    #[test]
    fn excludes_go_transit_and_up_express() {
        assert!(route_history_is_excluded("gotransit", None));
        assert!(route_history_is_excluded("upexpress", None));
        assert!(!route_history_is_excluded("ttc", Some(3)));
    }
}
