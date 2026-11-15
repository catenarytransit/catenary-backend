pub use catenary::departures_shared::*;

/// Refresh the board using one indexed Aspen lookup per chateau. In particular,
/// a modification indexed by a replacement stop can introduce a trip that did
/// not serve this station in the static schedule.
pub async fn apply_departure_realtime(
    events: &mut Vec<catenary::departure_realtime::StopEvent>,
    stops: &std::collections::BTreeMap<String, std::collections::HashSet<String>>,
    pool: &std::sync::Arc<catenary::postgres_tools::CatenaryPostgresPool>,
    cache: &catenary::aspen::lib::AspenChateauCache,
    routes: &mut std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, catenary::models::Route>,
    >,
    agencies: &mut std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, catenary::models::Agency>,
    >,
    shapes: &mut std::collections::BTreeMap<
        ecow::EcoString,
        std::collections::BTreeMap<ecow::EcoString, String>,
    >,
    include_shapes: bool,
) -> std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, catenary::aspen_dataset::AspenisedStop>,
> {
    use catenary::departure_realtime::*;
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
                let metadata = metadata?;
                // Bound both connection setup and RPC time, including a stale etcd socket.
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    let client = catenary::aspen::lib::spawn_aspen_client_from_ip(&metadata.socket)
                        .await
                        .ok()?;
                    let data = client
                        .get_departure_realtime(
                            tarpc::context::current(),
                            chateau.clone(),
                            trip_ids,
                            stop_ids.iter().cloned().collect(),
                        )
                        .await
                        .ok()??;
                    Some((chateau, stop_ids, client, data))
                })
                .await
                .ok()
                .flatten()
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;
    let mut realtime_stops = BTreeMap::new();
    for (chateau, _stop_ids, client, data) in responses.into_iter().flatten() {
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
            if let Ok(mut conn) = pool.get().await {
                use catenary::schema::gtfs::{
                    itinerary_pattern as ip, itinerary_pattern_meta as im, trips_compressed as tc,
                };
                let trips = tc::table
                    .filter(tc::chateau.eq(chateau))
                    .filter(tc::trip_id.eq_any(&affected))
                    .select(catenary::models::CompressedTrip::as_select())
                    .load::<catenary::models::CompressedTrip>(&mut conn)
                    .await
                    .unwrap_or_default();
                let patterns: Vec<_> = trips
                    .iter()
                    .map(|trip| trip.itinerary_pattern_id.clone())
                    .collect();
                let rows = ip::table
                    .filter(ip::chateau.eq(chateau))
                    .filter(ip::itinerary_pattern_id.eq_any(&patterns))
                    .order(ip::stop_sequence.asc())
                    .select(catenary::models::ItineraryPatternRow::as_select())
                    .load::<catenary::models::ItineraryPatternRow>(&mut conn)
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
            }
        }
        overlay_updates(events, chateau, &stop_ids, &data.updates);
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
            if let Ok(mut conn) = pool.get().await {
                use catenary::schema::gtfs::{agencies as a, routes as r};
                let new_routes = r::table
                    .filter(r::chateau.eq(chateau))
                    .filter(r::route_id.eq_any(&missing_routes))
                    .select(catenary::models::Route::as_select())
                    .load::<catenary::models::Route>(&mut conn)
                    .await
                    .unwrap_or_default();
                let agency_ids: Vec<_> = new_routes
                    .iter()
                    .filter_map(|route| route.agency_id.clone())
                    .collect();
                let new_agencies = a::table
                    .filter(a::chateau.eq(chateau))
                    .filter(a::agency_id.eq_any(&agency_ids))
                    .select(catenary::models::Agency::as_select())
                    .load::<catenary::models::Agency>(&mut conn)
                    .await
                    .unwrap_or_default();
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
                if let Ok(mut conn) = pool.get().await {
                    use catenary::schema::gtfs::shapes as sh;
                    let static_shapes = sh::table
                        .filter(sh::chateau.eq(chateau))
                        .filter(sh::shape_id.eq_any(&missing))
                        .load::<catenary::models::Shape>(&mut conn)
                        .await
                        .unwrap_or_default();
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
                }
            }
        }
    }
    realtime_stops
}
