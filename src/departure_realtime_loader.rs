/// Refresh the board using one indexed Aspen lookup per chateau. In particular,
/// a modification indexed by a replacement stop can introduce a trip that did
/// not serve this station in the static schedule.
#[derive(Default, Clone, Debug)]
pub struct DepartureRealtimeTimings {
    pub connection_ms: f64,
    pub rpc_ms: f64,
    pub overlay_ms: f64,
    pub additional_sql_ms: f64,
    pub update_count: usize,
    pub modification_count: usize,
}

pub async fn apply_departure_realtime(
    events: &mut Vec<crate::departure_realtime::StopEvent>,
    stops: &std::collections::BTreeMap<String, std::collections::HashSet<String>>,
    pool: &std::sync::Arc<crate::postgres_tools::CatenaryPostgresPool>,
    cache: &crate::aspen::lib::AspenChateauCache,
    routes: &mut std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, crate::models::Route>,
    >,
    agencies: &mut std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, crate::models::Agency>,
    >,
    shapes: &mut std::collections::BTreeMap<
        ecow::EcoString,
        std::collections::BTreeMap<ecow::EcoString, String>,
    >,
    include_shapes: bool,
) -> std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, crate::aspen_dataset::AspenisedStop>,
> {
    apply_departure_realtime_timed(
        events,
        stops,
        pool,
        cache,
        routes,
        agencies,
        shapes,
        include_shapes,
        &mut DepartureRealtimeTimings::default(),
    )
    .await
}

pub async fn apply_departure_realtime_timed(
    events: &mut Vec<crate::departure_realtime::StopEvent>,
    stops: &std::collections::BTreeMap<String, std::collections::HashSet<String>>,
    pool: &std::sync::Arc<crate::postgres_tools::CatenaryPostgresPool>,
    cache: &crate::aspen::lib::AspenChateauCache,
    routes: &mut std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, crate::models::Route>,
    >,
    agencies: &mut std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, crate::models::Agency>,
    >,
    shapes: &mut std::collections::BTreeMap<
        ecow::EcoString,
        std::collections::BTreeMap<ecow::EcoString, String>,
    >,
    include_shapes: bool,
    timings: &mut DepartureRealtimeTimings,
) -> std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, crate::aspen_dataset::AspenisedStop>,
> {
    use crate::departure_realtime::*;
    use chrono::TimeZone;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;
    use std::collections::{BTreeMap, HashSet};

    use futures::StreamExt;
    let responses: Vec<_> = futures::stream::iter(stops.iter())
        .map(|(chateau, stop_ids)| {
            let metadata = cache.get(chateau);
            let trip_ids = events
                .iter()
                .filter(|event| event.chateau == *chateau)
                .map(|event| event.trip_id.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            async move {
                let mut connection_ms = 0.0;
                let mut rpc_ms = 0.0;
                let mut connection_started = None;
                let mut rpc_started = None;
                // Preserve the existing combined 3-second deadline, and
                // record elapsed time even when the connect or RPC times out.
                let response = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    let metadata = metadata?;
                    connection_started = Some(std::time::Instant::now());
                    let result =
                        crate::aspen::lib::spawn_aspen_client_from_ip(&metadata.socket).await;
                    connection_ms =
                        connection_started.take().unwrap().elapsed().as_secs_f64() * 1000.0;
                    let client = result.ok()?;
                    rpc_started = Some(std::time::Instant::now());
                    let result = client
                        .get_departure_realtime(
                            tarpc::context::current(),
                            chateau.clone(),
                            trip_ids,
                            stop_ids.iter().cloned().collect(),
                        )
                        .await;
                    rpc_ms = rpc_started.take().unwrap().elapsed().as_secs_f64() * 1000.0;
                    let data = result.ok()??;
                    Some((chateau, stop_ids, client, data))
                })
                .await
                .ok()
                .flatten();
                if let Some(started) = connection_started {
                    connection_ms = started.elapsed().as_secs_f64() * 1000.0;
                }
                if let Some(started) = rpc_started {
                    rpc_ms = started.elapsed().as_secs_f64() * 1000.0;
                }
                (response, connection_ms, rpc_ms)
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;
    let mut realtime_stops = BTreeMap::new();
    for (response, connection_ms, rpc_ms) in responses {
        timings.connection_ms += connection_ms;
        timings.rpc_ms += rpc_ms;
        let Some((chateau, _stop_ids, client, data)) = response else {
            continue;
        };
        timings.update_count += data.updates.len();
        timings.modification_count += data.modifications.len();
        let stop_ids: HashSet<String> = data.searched_stop_ids.iter().cloned().collect();
        realtime_stops.insert(chateau.clone(), data.realtime_stops.clone());
        if data.modifications.is_empty() && data.updates.is_empty() {
            continue;
        }

        // Fetch only trips named by applicable modifications, then their complete
        // patterns. The normal station query still fetches selected-stop rows only.
        let affected: Vec<String> = data
            .modifications
            .values()
            .flat_map(|modification| &modification.selected_trips)
            .flat_map(|selected| selected.trip_ids.iter().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !affected.is_empty() {
            let sql_started = std::time::Instant::now();
            if let Ok(mut conn) = pool.get().await {
                use crate::schema::gtfs::{
                    itinerary_pattern_meta as im, trips_compressed as tc,
                };
                let trips = tc::table
                    .filter(tc::chateau.eq(chateau))
                    .filter(tc::trip_id.eq_any(&affected))
                    .select(crate::models::CompressedTrip::as_select())
                    .load::<crate::models::CompressedTrip>(&mut conn)
                    .await
                    .unwrap_or_default();
                let patterns: Vec<_> = trips
                    .iter()
                    .map(|trip| trip.itinerary_pattern_id.clone())
                    .collect();
                let rows = diesel::sql_query(
                    "SELECT * FROM gtfs.itinerary_pattern_trip_reader
                     WHERE chateau = $1 AND itinerary_pattern_id = ANY($2)
                     ORDER BY stop_sequence",
                )
                .bind::<diesel::sql_types::Text, _>(chateau)
                .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&patterns)
                .load::<crate::models::ItineraryPatternRow>(&mut conn)
                .await
                .unwrap_or_default();
                let timezones = im::table
                    .filter(im::chateau.eq(chateau))
                    .filter(im::itinerary_pattern_id.eq_any(&patterns))
                    .select((
                        im::onestop_feed_id,
                        im::attempt_id,
                        im::itinerary_pattern_id,
                        im::timezone,
                        im::trip_headsign,
                    ))
                    .load::<(String, String, String, String, Option<String>)>(&mut conn)
                    .await
                    .unwrap_or_default();
                drop(conn);
                timings.additional_sql_ms += sql_started.elapsed().as_secs_f64() * 1000.0;
                let modification_started = std::time::Instant::now();
                for modification in data.modifications.values() {
                    for selected in &modification.selected_trips {
                        for trip in trips
                            .iter()
                            .filter(|trip| selected.trip_ids.contains(&trip.trip_id))
                        {
                            let Some((_, _, _, timezone, headsign)) =
                                timezones.iter().find(|(feed, attempt, pattern, _, _)| {
                                    feed == &trip.onestop_feed_id
                                        && attempt == &trip.attempt_id
                                        && pattern == &trip.itinerary_pattern_id
                                })
                            else {
                                continue;
                            };
                            let Ok(timezone) = timezone.parse::<catenary_time::Tz>() else {
                                continue;
                            };
                            let trip_rows: Vec<_> = rows
                                .iter()
                                .filter(|row| {
                                    row.onestop_feed_id == trip.onestop_feed_id
                                        && row.attempt_id == trip.attempt_id
                                        && row.itinerary_pattern_id == trip.itinerary_pattern_id
                                })
                                .collect();
                            let start_times: Vec<_> = if modification.start_times.is_empty() {
                                if trip.has_frequencies {
                                    // A frequency template alone is not an actual run.
                                    // Use observed modified-trip instance start times.
                                    data.updates
                                        .iter()
                                        .filter(|update| {
                                            update.trip.trip_id.as_deref()
                                                == Some(trip.trip_id.as_str())
                                                || update
                                                    .trip
                                                    .modified_trip
                                                    .as_ref()
                                                    .and_then(|m| m.affected_trip_id.as_deref())
                                                    == Some(trip.trip_id.as_str())
                                        })
                                        .filter_map(|update| {
                                            update
                                                .trip
                                                .effective_start_time()
                                                .and_then(parse_start_time)
                                        })
                                        .collect::<HashSet<_>>()
                                        .into_iter()
                                        .collect()
                                } else {
                                    vec![trip.start_time]
                                }
                            } else {
                                modification
                                    .start_times
                                    .iter()
                                    .filter_map(|time| parse_start_time(time))
                                    .collect()
                            };
                            for date in &modification.service_dates {
                                let Some(noon) = date.and_hms_opt(12, 0, 0).and_then(|local| {
                                    timezone.from_local_datetime(&local).single()
                                }) else {
                                    continue;
                                };
                                let base = noon.timestamp() - 43200;
                                for start in &start_times {
                                    let original: Vec<_> = trip_rows
                                        .iter()
                                        .map(|row| ModifiedCall {
                                            stop_id: row.stop_id.to_string(),
                                            original_sequence: Some(row.gtfs_stop_sequence),
                                            arrival: row.arrival_time_since_start.map(|offset| {
                                                base + i64::from(*start) + i64::from(offset)
                                            }),
                                            departure: row.departure_time_since_start.map(
                                                |offset| {
                                                    base + i64::from(*start) + i64::from(offset)
                                                },
                                            ),
                                        })
                                        .collect();
                                    let Some(modified) =
                                        modified_calls(&original, &modification.modifications)
                                    else {
                                        continue;
                                    };
                                    let mut template =
                                        empty_event(chateau, &trip.trip_id, &trip.route_id, "");
                                    template.service_date = Some(*date);
                                    template.trip_start_time = Some(*start);
                                    template.trip_short_name = trip.trip_short_name.clone();
                                    template.headsign = headsign.clone();
                                    template.scheduled_trip_shape_id =
                                        selected.shape_id.as_deref().map(Into::into);
                                    apply_modified_schedule(
                                        events, &template, &modified, &stop_ids,
                                    );
                                }
                            }
                        }
                    }
                }
                timings.overlay_ms += modification_started.elapsed().as_secs_f64() * 1000.0;
            } else {
                timings.additional_sql_ms += sql_started.elapsed().as_secs_f64() * 1000.0;
            }
        }
        let overlay_started = std::time::Instant::now();
        overlay_updates(events, chateau, &stop_ids, &data.updates);
        timings.overlay_ms += overlay_started.elapsed().as_secs_f64() * 1000.0;
        // Added trips may name routes absent from the selected static itineraries.
        let missing_routes: Vec<_> = events
            .iter()
            .filter(|event| event.chateau == *chateau)
            .map(|event| event.route_id.clone())
            .filter(|id| {
                !routes
                    .get(chateau)
                    .is_some_and(|routes| routes.contains_key(id))
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !missing_routes.is_empty() {
            let sql_started = std::time::Instant::now();
            if let Ok(mut conn) = pool.get().await {
                use crate::schema::gtfs::{agencies as a, routes as r};
                let new_routes = r::table
                    .filter(r::chateau.eq(chateau))
                    .filter(r::route_id.eq_any(&missing_routes))
                    .select(crate::models::Route::as_select())
                    .load::<crate::models::Route>(&mut conn)
                    .await
                    .unwrap_or_default();
                let agency_ids: Vec<_> = new_routes
                    .iter()
                    .filter_map(|route| route.agency_id.clone())
                    .collect();
                let new_agencies = a::table
                    .filter(a::chateau.eq(chateau))
                    .filter(a::agency_id.eq_any(&agency_ids))
                    .select(crate::models::Agency::as_select())
                    .load::<crate::models::Agency>(&mut conn)
                    .await
                    .unwrap_or_default();
                timings.additional_sql_ms += sql_started.elapsed().as_secs_f64() * 1000.0;
                for route in new_routes {
                    routes
                        .entry(chateau.clone())
                        .or_default()
                        .insert(route.route_id.to_string(), route);
                }
                for agency in new_agencies {
                    agencies
                        .entry(chateau.clone())
                        .or_default()
                        .insert(agency.agency_id.clone(), agency);
                }
            } else {
                timings.additional_sql_ms += sql_started.elapsed().as_secs_f64() * 1000.0;
            }
        }
        if include_shapes {
            let shape_ids: HashSet<_> = events
                .iter()
                .filter(|event| event.chateau == *chateau)
                .filter_map(|event| {
                    event
                        .scheduled_trip_shape_id
                        .as_ref()
                        .map(ToString::to_string)
                })
                .filter(|id| {
                    !shapes
                        .get(chateau.as_str())
                        .is_some_and(|shapes| shapes.contains_key(id.as_str()))
                })
                .collect();
            let shape_rpc_started = std::time::Instant::now();
            let fetched: Vec<_> = futures::stream::iter(shape_ids.iter())
                .map(|id| {
                    let client = &client;
                    async move {
                        let shape = tokio::time::timeout(
                            std::time::Duration::from_secs(3),
                            client.get_shape(
                                tarpc::context::current(),
                                chateau.clone(),
                                id.clone(),
                            ),
                        )
                        .await
                        .ok()?
                        .ok()??;
                        Some((id.clone(), shape.to_string()))
                    }
                })
                .buffer_unordered(8)
                .collect()
                .await;
            timings.rpc_ms += shape_rpc_started.elapsed().as_secs_f64() * 1000.0;
            for (id, shape) in fetched.into_iter().flatten() {
                shapes
                    .entry(chateau.as_str().into())
                    .or_default()
                    .insert(id.into(), shape);
            }
            // A detour shape may also refer to static shapes.txt.
            let missing: Vec<_> = shape_ids
                .into_iter()
                .filter(|id| {
                    !shapes
                        .get(chateau.as_str())
                        .is_some_and(|shapes| shapes.contains_key(id.as_str()))
                })
                .collect();
            if !missing.is_empty() {
                let sql_started = std::time::Instant::now();
                if let Ok(mut conn) = pool.get().await {
                    use crate::schema::gtfs::shapes as sh;
                    let static_shapes = sh::table
                        .filter(sh::chateau.eq(chateau))
                        .filter(sh::shape_id.eq_any(&missing))
                        .load::<crate::models::Shape>(&mut conn)
                        .await
                        .unwrap_or_default();
                    timings.additional_sql_ms += sql_started.elapsed().as_secs_f64() * 1000.0;
                    for shape in static_shapes {
                        if let Ok(polyline) = polyline::encode_coordinates(
                            geo::LineString::new(
                                shape
                                    .linestring
                                    .points
                                    .iter()
                                    .map(|point| geo::Coord {
                                        x: point.x,
                                        y: point.y,
                                    })
                                    .collect(),
                            ),
                            5,
                        ) {
                            shapes
                                .entry(chateau.as_str().into())
                                .or_default()
                                .insert(shape.shape_id.into(), polyline);
                        }
                    }
                } else {
                    timings.additional_sql_ms += sql_started.elapsed().as_secs_f64() * 1000.0;
                }
            }
        }
    }
    realtime_stops
}
