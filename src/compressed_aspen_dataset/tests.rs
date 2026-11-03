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
