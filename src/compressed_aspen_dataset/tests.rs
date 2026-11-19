use super::*;
use crate::formation_v1::*;

fn empty_dataset() -> AspenisedData {
    AspenisedData {
        trip_updates: Default::default(),
        vehicle_positions: Default::default(),
        vehicle_routes_cache: Default::default(),
        vehicle_routes_cache_hash: Default::default(),
        vehicle_label_to_gtfs_id: Default::default(),
        trip_updates_lookup_by_trip_id_to_trip_update_ids: Default::default(),
        trip_updates_lookup_by_route_id_to_trip_update_ids: Default::default(),
        vehicle_positions_rtree_by_route_type: Default::default(),
        aspenised_alerts: Default::default(),
        impacted_routes_alerts: Default::default(),
        impacted_stops_alerts: Default::default(),
        impacted_trips_alerts: Default::default(),
        trip_id_to_vehicle_gtfs_rt_id: Default::default(),
        last_updated_time_ms: 1_793_700_000_000,
        itinerary_pattern_internal_cache: ItineraryPatternInternalCache::new(),
        compressed_trip_internal_cache: CompressedTripInternalCache::new(),
        stop_id_to_stop: Default::default(),
        shape_id_to_shape: Default::default(),
        trip_modifications: Default::default(),
        trip_id_to_trip_modification_ids: Default::default(),
        stop_id_to_trip_modification_ids: Default::default(),
        stop_id_to_non_scheduled_trip_ids: Default::default(),
        stop_id_to_parent_id: Default::default(),
        parent_id_to_children_ids: Default::default(),
    }
}

fn empty_stop() -> AspenisedStopTimeUpdate {
    AspenisedStopTimeUpdate {
        stop_sequence: None,
        stop_id: None,
        arrival: None,
        departure: None,
        departure_occupancy_status: None,
        schedule_relationship: None,
        stop_time_properties: None,
        platform_string: None,
        platform_info: None,
        old_rt_data: false,
    }
}

fn trip(stops: Vec<AspenisedStopTimeUpdate>) -> AspenisedTripUpdate {
    AspenisedTripUpdate {
        trip: AspenRawTripInfo {
            trip_id: Some("trip".into()),
            route_id: Some("route".into()),
            direction_id: Some(0),
            start_time: Some("25:01:02".into()),
            start_date: chrono::NaiveDate::from_ymd_opt(2026, 11, 3),
            schedule_relationship: Some(AspenisedTripScheduleRelationship::Scheduled),
            modified_trip: None,
        },
        vehicle: None,
        timestamp: Some(0),
        delay: Some(0),
        stop_time_update: stops,
        trip_properties: None,
        trip_headsign: Some(Arc::from("終点")),
        consist: None,
        found_schedule_trip_id: true,
        last_seen: 1_793_700_001,
    }
}

fn assert_round_trip(data: AspenisedData) {
    let expected = serde_json::to_value(&data).unwrap();
    let compressed = data.into_compressed();
    assert_eq!(
        serde_json::to_value(compressed.to_expanded()).unwrap(),
        expected
    );
    let bytes = crate::bincode_serialize(&compressed).unwrap();
    let decoded: CompressedAspenisedData = crate::bincode_deserialize(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(decoded.into_expanded()).unwrap(),
        expected
    );
}

#[test]
fn empty_snapshot_and_unknown_wire_version() {
    assert_round_trip(empty_dataset());
    let bytes = crate::bincode_serialize(&999_u32).unwrap();
    assert!(crate::bincode_deserialize::<CompressedAspenisedData>(&bytes).is_err());
}

#[test]
fn all_stop_presence_combinations_and_extreme_values_round_trip() {
    let mut data = empty_dataset();
    let mut stops = Vec::new();
    for i in 0..256 {
        let mut stop = empty_stop();
        stop.stop_id = (i % 2 == 0).then(|| Arc::from(if i % 4 == 0 { "" } else { "駅-A" }));
        stop.stop_sequence = (i % 3 != 0).then_some(if i % 4 == 0 { 0 } else { u16::MAX });
        stop.old_rt_data = i % 7 == 0;
        let event = AspenStopTimeEvent {
            delay: if i & 4 == 0 {
                None
            } else {
                Some(match i % 3 {
                    0 => i32::MIN,
                    1 => i32::MAX,
                    _ => 0,
                })
            },
            time: if i & 8 == 0 {
                None
            } else {
                Some(match i % 3 {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    _ => 0,
                })
            },
            uncertainty: if i & 16 == 0 {
                None
            } else {
                Some(match i % 3 {
                    0 => i16::MIN,
                    1 => i16::MAX,
                    _ => 0,
                })
            },
        };
        if i & 1 != 0 {
            stop.arrival = Some(event.clone());
        }
        if i & 2 != 0 {
            stop.departure = Some(event);
        }
        stop.departure_occupancy_status =
            option_u8_to_occupancy_status(&((i % 10 < 9).then_some((i % 10) as u8)));
        stop.schedule_relationship = (i % 5 < 4).then(|| ((i % 5) as u8).into());
        if i % 11 == 0 {
            stop.stop_time_properties = Some(AspenisedStopTimeProperties {
                assigned_stop_id: (i % 22 == 0).then(|| "assigned".into()),
            });
        }
        if i % 13 == 0 {
            stop.platform_string = Some(if i % 26 == 0 { "" } else { "12A" }.into());
        }
        if i % 17 == 0 {
            stop.platform_info = Some(AspenisedPlatformInfo {
                aimed: Some("12".into()),
                expected: Some("13".into()),
                is_changed: true,
                platform_sectors: Some(vec![PlatformSectorDefinition {
                    label: "A".into(),
                    start_meters: 0.0,
                    end_meters: 150.0,
                }]),
            });
        }
        stops.push(stop);
    }
    data.trip_updates.insert("update".into(), trip(stops));
    assert_round_trip(data);
}

#[test]
fn borrowed_views_keep_modified_service_identity_and_all_trip_extras() {
    let mut data = empty_dataset();
    let mut update = trip(vec![empty_stop()]);
    update.trip.start_time = None;
    update.trip.start_date = None;
    update.trip.direction_id = Some(u32::MAX);
    update.trip.modified_trip = Some(ModifiedTripSelector {
        modifications_id: Some("mod".into()),
        affected_trip_id: Some("original".into()),
        start_time: Some("001:02:03".into()),
        start_date: chrono::NaiveDate::from_ymd_opt(2026, 11, 4),
    });
    update.trip.schedule_relationship = Some(AspenisedTripScheduleRelationship::Duplicated);
    update.vehicle = Some(AspenisedVehicleDescriptor {
        id: Some("v".into()),
        label: Some("bus".into()),
        license_plate: Some("plate".into()),
        wheelchair_accessible: Some(2),
    });
    update.trip_properties = Some(AspenTripProperties {
        trip_id: Some("new".into()),
        start_date: update.trip.modified_trip.as_ref().unwrap().start_date,
        start_time: Some("26:00:00".into()),
        shape_id: Some("shape".into()),
        trip_headsign: Some("terminus".into()),
        trip_short_name: Some("12".into()),
    });
    update.consist = Some(Box::new(UnifiedConsist {
        global_journey_id: "journey".into(),
        groups: vec![],
        formation_status: FormationStatus::Unknown,
    }));
    data.trip_updates.insert("update".into(), update);
    let compressed = data.clone().into_compressed();
    let view = compressed.trip_updates().get("update").unwrap();
    assert_eq!(view.trip().effective_start_time(), Some("001:02:03"));
    assert_eq!(
        view.trip().effective_start_date(),
        chrono::NaiveDate::from_ymd_opt(2026, 11, 4)
    );
    assert_eq!(view.stops().len(), 1);
    assert!(compressed.trip_updates().get("missing").is_none());
    assert_eq!(view.trip_headsign(), Some("終点"));
    assert_round_trip(data);
}

#[test]
fn multiple_trips_empty_ranges_and_nontrip_metadata_survive() {
    let mut data = empty_dataset();
    for i in 0..10 {
        let mut update = trip((0..i).map(|_| empty_stop()).collect());
        update.trip.trip_id = if i % 2 == 0 {
            None
        } else {
            Some(format!("trip-{i}"))
        };
        update.timestamp = if i % 2 == 0 { None } else { Some(u64::MAX) };
        update.last_seen = i as u64;
        data.trip_updates
            .insert(format!("update-{i}").into(), update);
    }
    data.vehicle_label_to_gtfs_id
        .insert("label".into(), "vehicle".into());
    data.trip_updates_lookup_by_trip_id_to_trip_update_ids
        .insert("trip-1".into(), vec!["update-1".into()]);
    data.trip_updates_lookup_by_route_id_to_trip_update_ids
        .insert("route".into(), vec!["update-1".into()]);
    data.shape_id_to_shape
        .insert("shape".into(), Some("geometry".into()));
    data.stop_id_to_parent_id
        .insert("child".into(), "parent".into());
    data.parent_id_to_children_ids
        .insert("parent".into(), vec!["child".into()]);
    data.stop_id_to_non_scheduled_trip_ids
        .insert("child".into(), vec!["trip-1".into()]);
    data.trip_modifications.insert(
        "mod".into(),
        AspenisedTripModification {
            selected_trips: vec![],
            start_times: vec!["25:00:00".into()],
            service_dates: vec![],
            modifications: vec![],
        },
    );
    data.trip_id_to_trip_modification_ids
        .insert("trip-1".into(), vec!["mod".into()]);
    data.stop_id_to_trip_modification_ids
        .insert("child".into(), vec!["mod".into()]);
    data.compressed_trip_internal_cache.last_time_full_refreshed = chrono::DateTime::UNIX_EPOCH;
    let compressed = data.clone().into_compressed();
    // The in-memory conversion keeps even fields intentionally skipped on wire.
    assert_eq!(
        compressed
            .metadata()
            .compressed_trip_internal_cache
            .last_time_full_refreshed,
        chrono::DateTime::UNIX_EPOCH
    );
    assert_eq!(
        compressed
            .to_expanded()
            .compressed_trip_internal_cache
            .last_time_full_refreshed,
        chrono::DateTime::UNIX_EPOCH
    );
    assert_eq!(compressed.trip_updates().len(), 10);
    for i in 0..10 {
        assert_eq!(
            compressed
                .trip_updates()
                .get(&format!("update-{i}"))
                .unwrap()
                .stops()
                .len(),
            i
        );
    }
    assert_round_trip(data);
}

#[test]
fn common_bus_snapshot_is_smaller_and_borrowed_reads_work_after_serialization() {
    let mut data = empty_dataset();
    for i in 0..100 {
        let stops = (0..40)
            .map(|s| {
                let mut stop = empty_stop();
                stop.stop_id = Some(Arc::from(format!("stop-{s}")));
                stop.stop_sequence = Some(s);
                stop.arrival = Some(AspenStopTimeEvent {
                    delay: Some(0),
                    time: Some(1_793_700_000 + i * 60 + i64::from(s) * 120),
                    uncertainty: None,
                });
                stop.departure = stop.arrival.clone();
                stop
            })
            .collect();
        data.trip_updates
            .insert(format!("update-{i}").into(), trip(stops));
    }
    let ordinary_bytes = crate::bincode_serialize(&data).unwrap().len();
    let compressed = data.into_compressed();
    let encoded = crate::bincode_serialize(&compressed).unwrap();
    // Serialized size is a regression check, not a claim about allocator RSS.
    assert!(
        encoded.len() < ordinary_bytes / 2,
        "{} versus {}",
        encoded.len(),
        ordinary_bytes
    );
    let decoded: CompressedAspenisedData = crate::bincode_deserialize(&encoded).unwrap();
    let view = decoded.trip_updates().get("update-42").unwrap();
    assert_eq!(view.trip().route_id, Some("route"));
    assert_eq!(
        view.stops().nth(39).unwrap().departure().unwrap().time,
        Some(1_793_700_000 + 42 * 60 + 39 * 120)
    );
    eprintln!(
        "4000-stop bus fixture: ordinary={ordinary_bytes}, compressed={} serialized bytes",
        encoded.len()
    );
}

#[test]
fn descriptor_presence_and_dense_rail_columns_round_trip() {
    let mut data = empty_dataset();
    for i in 0..64 {
        let mut update = trip(vec![empty_stop(); 65]);
        update.trip.trip_id = (i & 1 != 0).then(|| "".into());
        update.trip.route_id = (i & 2 != 0).then(|| "route".into());
        update.trip.start_time = (i & 4 != 0).then(|| "001:00:00".into());
        update.trip.start_date = if i & 8 != 0 {
            update.trip.start_date
        } else {
            None
        };
        update.trip.direction_id = (i & 16 != 0).then_some(0);
        update.trip.schedule_relationship = if i & 32 != 0 {
            update.trip.schedule_relationship
        } else {
            None
        };
        update.delay = (i % 2 == 0).then_some(i32::MAX);
        for (s, stop) in update.stop_time_update.iter_mut().enumerate() {
            stop.stop_id = Some(Arc::from("station"));
            stop.platform_string = Some("12A".into());
            stop.platform_info = Some(AspenisedPlatformInfo {
                aimed: Some("12A".into()),
                expected: Some("12A".into()),
                platform_sectors: Some(vec![]),
                is_changed: false,
            });
            stop.stop_time_properties = Some(AspenisedStopTimeProperties {
                assigned_stop_id: None,
            });
            stop.departure = Some(AspenStopTimeEvent {
                delay: Some(0),
                time: Some(s as i64),
                uncertainty: Some(0),
            });
        }
        data.trip_updates
            .insert(format!("update-{i}").into(), update);
    }
    let compressed = data.clone().into_compressed();
    let first = compressed.trip_updates().get("update-2").unwrap().trip();
    let second = compressed.trip_updates().get("update-3").unwrap().trip();
    assert!(std::ptr::eq(
        first.route_id.unwrap(),
        second.route_id.unwrap()
    ));
    let expanded = compressed.to_expanded();
    let stops = &expanded
        .trip_updates
        .get("update-0")
        .unwrap()
        .stop_time_update;
    assert!(Arc::ptr_eq(
        stops[0].stop_id.as_ref().unwrap(),
        stops[64].stop_id.as_ref().unwrap()
    ));
    assert_round_trip(data);
}

#[test]
fn borrowed_descriptor_matching_preserves_full_service_identity() {
    let mut data = empty_dataset();
    let mut update = trip(vec![]);
    update.trip.modified_trip = Some(ModifiedTripSelector {
        modifications_id: Some("modification".into()),
        affected_trip_id: Some("affected".into()),
        start_time: Some("26:00:00".into()),
        start_date: chrono::NaiveDate::from_ymd_opt(2026, 11, 4),
    });
    let descriptor = update.trip.clone();
    data.trip_updates.insert("update".into(), update);
    let snapshot = data.into_compressed();
    let view = snapshot.trip_updates().get("update").unwrap().trip();
    assert!(view.matches(&descriptor));

    let mut variants = Vec::new();
    let mut changed = descriptor.clone();
    changed.trip_id = None;
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.route_id = Some(String::new());
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.direction_id = None;
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.start_time = Some("01:01:02".into());
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.start_date = None;
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.schedule_relationship = None;
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.modified_trip = None;
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.modified_trip.as_mut().unwrap().start_date = None;
    variants.push(changed);
    let mut changed = descriptor.clone();
    changed.modified_trip.as_mut().unwrap().start_time = None;
    variants.push(changed);

    for changed in variants {
        assert!(!view.matches(&changed), "{changed:?}");
        assert_eq!(view.matches(&changed), view.to_owned() == changed);
    }
}

#[test]
fn borrowed_stop_iterators_support_suffix_merge_and_endpoint_reads() {
    let mut data = empty_dataset();
    let stops: Vec<_> = ["A", "B", "C", "D"]
        .into_iter()
        .enumerate()
        .map(|(index, id)| {
            let mut stop = empty_stop();
            stop.stop_id = Some(Arc::from(id));
            stop.stop_sequence = Some(index as u16);
            stop.departure = Some(AspenStopTimeEvent {
                time: Some(100 + index as i64),
                delay: None,
                uncertainty: None,
            });
            stop
        })
        .collect();
    data.trip_updates.insert("update".into(), trip(stops));
    let snapshot = data.into_compressed();
    let update = snapshot.trip_updates().get("update").unwrap();
    let old_stops = update.stops();
    assert_eq!(old_stops.len(), 4);
    assert_eq!(
        old_stops.clone().next().unwrap().departure().unwrap().time,
        Some(100)
    );
    assert_eq!(
        old_stops
            .clone()
            .next_back()
            .unwrap()
            .departure()
            .unwrap()
            .time,
        Some(103)
    );
    assert_eq!(
        old_stops
            .clone()
            .skip(2)
            .map(|s| s.stop_id().unwrap())
            .collect::<Vec<_>>(),
        ["C", "D"]
    );
    let preserved: Vec<_> = old_stops
        .take(2)
        .map(|s| {
            let mut stop = s.to_owned();
            stop.old_rt_data = true;
            stop
        })
        .collect();
    assert_eq!(preserved.len(), 2);
    assert_eq!(preserved[1].stop_id.as_deref(), Some("B"));
    assert!(preserved.iter().all(|s| s.old_rt_data));
    assert!(update.stops().all(|s| !s.old_rt_data()));
}

#[test]
fn departure_lookup_expands_realtime_children_and_finds_detours_into_new_stops() {
    let mut data = empty_dataset();
    let mut added = trip(vec![AspenisedStopTimeUpdate {
        stop_id: Some(Arc::from("rt-platform")),
        ..empty_stop()
    }]);
    added.trip.schedule_relationship = Some(AspenisedTripScheduleRelationship::Added);
    data.trip_updates.insert("added-entity".into(), added);
    data.stop_id_to_non_scheduled_trip_ids
        .insert("rt-platform".into(), vec!["added-entity".into()]);
    // Include a cycle to verify expansion terminates and de-duplicates.
    data.parent_id_to_children_ids
        .insert("station".into(), vec!["rt-platform".into()]);
    data.parent_id_to_children_ids
        .insert("rt-platform".into(), vec!["station".into()]);
    data.stop_id_to_trip_modification_ids
        .insert("rt-platform".into(), vec!["detour".into()]);
    data.trip_modifications.insert(
        "detour".into(),
        AspenisedTripModification {
            selected_trips: vec![AspenSelectedTrips {
                trip_ids: vec!["outside-static-station".into()],
                shape_id: None,
            }],
            start_times: vec![],
            service_dates: vec![],
            modifications: vec![],
        },
    );
    data.trip_updates_lookup_by_trip_id_to_trip_update_ids
        .insert(
            "outside-static-station".into(),
            vec!["modified-entity".into()],
        );
    data.trip_updates
        .insert("modified-entity".into(), trip(vec![]));
    data.trip_updates
        .insert("unrelated-entity".into(), trip(vec![]));
    let result = crate::departure_realtime::select_departure_realtime(
        &data.into_compressed(),
        vec![],
        vec!["station".into()],
    );
    assert_eq!(result.searched_stop_ids.len(), 2);
    assert!(result.searched_stop_ids.contains(&"rt-platform".into()));
    assert_eq!(result.modifications.len(), 1);
    assert_eq!(result.updates.len(), 2);
}

#[test]
fn departure_lookup_includes_requested_trip_deletions_without_stop_updates() {
    let mut data = empty_dataset();
    let mut deleted = trip(vec![]);
    deleted.trip.trip_id = Some("static-trip".into());
    deleted.trip.schedule_relationship = Some(AspenisedTripScheduleRelationship::Deleted);
    data.trip_updates.insert("deleted-entity".into(), deleted);
    data.trip_updates_lookup_by_trip_id_to_trip_update_ids
        .insert("static-trip".into(), vec!["deleted-entity".into()]);
    data.trip_updates.insert("unrelated".into(), trip(vec![]));
    let result = crate::departure_realtime::select_departure_realtime(
        &data.into_compressed(),
        vec!["static-trip".into()],
        vec!["a".into()],
    );
    assert_eq!(result.updates.len(), 1);
    assert_eq!(
        result.updates[0].trip.schedule_relationship,
        Some(AspenisedTripScheduleRelationship::Deleted)
    );
    assert!(result.updates[0].stop_time_update.is_empty());
}

#[test]
fn departure_headsign_resolves_terminal_outside_selected_board_and_survives_compression() {
    let mut data = empty_dataset();
    let mut update = trip(vec![
        serde_json::from_value(serde_json::json!({"old_rt_data":false, "stop_id":"pico", "stop_sequence":1, "departure":{"time":100}})).unwrap(),
        serde_json::from_value(serde_json::json!({"old_rt_data":false, "stop_id":"rt-terminal", "stop_sequence":10, "arrival":{"time":200}})).unwrap(),
        serde_json::from_value(serde_json::json!({"old_rt_data":false, "stop_id":"closed", "stop_sequence":11, "schedule_relationship":"Skipped"})).unwrap(),
    ]);
    update.trip.trip_id = Some("added-trip".into());
    update.trip.route_id = Some("A".into());
    update.trip.schedule_relationship = Some(AspenisedTripScheduleRelationship::Added);
    update.found_schedule_trip_id = true;
    update.trip_headsign = None;
    update.trip_properties =
        Some(serde_json::from_value(serde_json::json!({"trip_headsign":""})).unwrap());
    data.trip_updates.insert("added".into(), update);
    data.stop_id_to_non_scheduled_trip_ids
        .insert("pico".into(), vec!["added".into()]);
    // A realtime terminal platform can inherit its parent's usable station name.
    data.stop_id_to_stop.insert(
        "rt-terminal".into(),
        serde_json::from_value(serde_json::json!({"parent_station":"terminal-parent"})).unwrap(),
    );
    data.stop_id_to_stop.insert("terminal-parent".into(), serde_json::from_value(serde_json::json!({"stop_name":{"translation":[{"text":"  "},{"text":"Pomona Station"}]}})).unwrap());
    let encoded = crate::bincode_serialize(&data.into_compressed()).unwrap();
    let compressed: CompressedAspenisedData = crate::bincode_deserialize(&encoded).unwrap();
    let result = crate::departure_realtime::select_departure_realtime(
        &compressed,
        vec![],
        vec!["pico".into()],
    );
    assert_eq!(result.searched_stop_ids, ["pico"]);
    assert_eq!(
        result.updates[0].trip_headsign.as_deref(),
        Some("Pomona Station")
    );
    let mut events = vec![];
    crate::departure_realtime::overlay_updates(
        &mut events,
        "feed",
        &std::collections::HashSet::from(["pico".into()]),
        &result.updates,
    );
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].headsign.as_deref(), Some("Pomona Station"));
    assert_eq!(
        crate::departure_realtime::nearby_event_time(&events[0], 90),
        Some(100)
    );
    let serialized = serde_json::to_value(&events[0]).unwrap();
    assert_eq!(serialized["headsign"], "Pomona Station");
}

#[test]
fn identical_stop_sequences_share_pattern_without_sharing_times() {
    let mut data = empty_dataset();
    for i in 0..200 {
        let mut stops = Vec::new();
        for j in 0..40_u16 {
            let mut stop = empty_stop();
            stop.stop_id = Some(Arc::from(format!("stop-{j}")));
            stop.stop_sequence = Some(j * 3 + 2);
            stop.arrival = Some(AspenStopTimeEvent {
                time: Some(1_800_000_000 + i * 1000 + i64::from(j)),
                delay: Some(i as i32),
                uncertainty: None,
            });
            stop.departure = stop.arrival.clone();
            stops.push(stop);
        }
        data.trip_updates
            .insert(format!("t-{i}").into(), trip(stops));
    }
    let expected = serde_json::to_value(&data).unwrap();
    let compressed = data.into_compressed();
    assert!(matches!(compressed, CompressedAspenisedData::V2(_)));
    assert!(compressed.trip_updates().uses_shared_stop_patterns());
    assert_eq!(compressed.trip_updates().stop_pattern_count(), 1);
    assert_ne!(
        compressed
            .trip_updates()
            .get("t-3")
            .unwrap()
            .stops()
            .next()
            .unwrap()
            .arrival()
            .unwrap()
            .time,
        compressed
            .trip_updates()
            .get("t-4")
            .unwrap()
            .stops()
            .next()
            .unwrap()
            .arrival()
            .unwrap()
            .time,
    );
    assert_eq!(
        compressed
            .trip_updates()
            .get("t-3")
            .unwrap()
            .stops()
            .nth(2)
            .unwrap()
            .stop_sequence(),
        Some(8),
    );
    assert_eq!(
        serde_json::to_value(compressed.to_expanded()).unwrap(),
        expected
    );
    let bytes = crate::bincode_serialize(&compressed).unwrap();
    let decoded: CompressedAspenisedData = crate::bincode_deserialize(&bytes).unwrap();
    assert!(decoded.trip_updates().uses_shared_stop_patterns());
    assert_eq!(
        serde_json::to_value(decoded.into_expanded()).unwrap(),
        expected
    );
}

#[test]
fn unique_sequences_use_flat_rows() {
    let mut data = empty_dataset();
    for i in 0..25 {
        let mut stops = Vec::new();
        for j in 0..6 {
            let mut stop = empty_stop();
            stop.stop_id = Some(Arc::from(format!("trip-{i}-stop-{j}")));
            stop.stop_sequence = Some(j + 1);
            stops.push(stop);
        }
        data.trip_updates
            .insert(format!("t-{i}").into(), trip(stops));
    }
    assert!(
        !data
            .clone()
            .into_compressed()
            .trip_updates()
            .uses_shared_stop_patterns()
    );
    assert_round_trip(data);
}

#[test]
fn pattern_distinguishes_missing_sequence_zero_and_repeated_stops() {
    let mut data = empty_dataset();
    for i in 0..80 {
        let mut stops = Vec::new();
        for (j, id) in ["A", "B", "A", ""].into_iter().enumerate() {
            let mut stop = empty_stop();
            stop.stop_id = if j == 3 && i % 4 < 2 {
                None
            } else {
                Some(Arc::from(id))
            };
            stop.stop_sequence = match j {
                0 if i % 2 == 0 => None,
                0 => Some(0),
                1 => Some(3),
                2 => Some(10),
                _ => Some(100),
            };
            stops.push(stop);
        }
        data.trip_updates
            .insert(format!("t-{i}").into(), trip(stops));
    }
    let expected = serde_json::to_value(&data).unwrap();
    let compressed = data.into_compressed();
    assert!(compressed.trip_updates().uses_shared_stop_patterns());
    assert_eq!(compressed.trip_updates().stop_pattern_count(), 4);
    for id in ["t-0", "t-1", "t-2", "t-3"] {
        let stops: Vec<_> = compressed
            .trip_updates()
            .get(id)
            .unwrap()
            .stops()
            .map(|s| (s.stop_id().map(str::to_owned), s.stop_sequence()))
            .collect();
        assert_eq!(stops[0].0.as_deref(), Some("A"));
        assert_eq!(stops[2], (Some("A".to_owned()), Some(10)));
    }
    assert_eq!(
        serde_json::to_value(compressed.to_expanded()).unwrap(),
        expected
    );
}

#[test]
fn historical_v1_snapshots_decode_and_upgrade_on_borrow() {
    let mut data = empty_dataset();
    let mut stop = empty_stop();
    stop.stop_id = Some(Arc::from("old-stop"));
    stop.stop_sequence = Some(42);
    data.trip_updates.insert("legacy".into(), trip(vec![stop]));
    let expected = serde_json::to_value(&data).unwrap();
    let old_trips = legacy_trips::CompressedTripUpdates::from(data.trip_updates.clone());
    let metadata = data.into_compressed().metadata().clone();
    let old = CompressedAspenisedData::V1(Box::new(AspenSnapshotV1 {
        metadata,
        trips: old_trips,
        upgraded: OnceLock::new(),
    }));
    let bytes = crate::bincode_serialize(&old).unwrap();
    let decoded: CompressedAspenisedData = crate::bincode_deserialize(&bytes).unwrap();
    assert_eq!(
        decoded
            .trip_updates()
            .get("legacy")
            .unwrap()
            .stops()
            .next()
            .unwrap()
            .stop_sequence(),
        Some(42),
    );
    assert_eq!(
        serde_json::to_value(decoded.into_expanded()).unwrap(),
        expected
    );
}
