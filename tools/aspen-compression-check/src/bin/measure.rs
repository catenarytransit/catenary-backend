//! Synthetic resident-allocation and scan benchmark. Run in release mode.
//! Tracks requested live heap bytes, not allocator overhead or process RSS.
use catenary_aspen_compression_check::aspen_dataset::*;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(ptr, layout);
        }
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                LIVE.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

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

fn fixture(rail: bool) -> AspenisedData {
    let mut data = empty_dataset();
    let stop_ids: Vec<Arc<str>> = (0..40).map(|s| Arc::from(format!("stop-{s}"))).collect();
    for i in 0..2000 {
        let stops = (0..40)
            .map(|s| {
                let mut stop = empty_stop();
                stop.stop_id = Some(Arc::clone(&stop_ids[usize::from(s)]));
                stop.stop_sequence = Some(s);
                stop.arrival = Some(AspenStopTimeEvent {
                    delay: Some(i % 600),
                    time: Some(1_793_700_000 + i64::from(i) * 60 + i64::from(s) * 120),
                    uncertainty: None,
                });
                stop.departure = stop.arrival.clone();
                if rail {
                    stop.platform_string = Some("12A".into());
                    stop.platform_info = Some(
                        catenary_aspen_compression_check::formation_v1::AspenisedPlatformInfo {
                            aimed: Some("12A".into()),
                            expected: Some("12A".into()),
                            platform_sectors: None,
                            is_changed: false,
                        },
                    );
                }
                stop
            })
            .collect();
        let mut update = trip(stops);
        update.trip.trip_id = Some(format!("trip-{i}"));
        data.trip_updates
            .insert(format!("update-{i}").into(), update);
    }
    data
}

fn main() {
    for rail in [false, true] {
        let baseline = LIVE.load(Ordering::Relaxed);
        let data = fixture(rail);
        let raw_bytes = LIVE.load(Ordering::Relaxed) - baseline;
        let mut raw_checksum = 0_i64;
        let start = Instant::now();
        for _ in 0..100 {
            for trip in black_box(&data).trip_updates.values() {
                for stop in &trip.stop_time_update {
                    raw_checksum = raw_checksum
                        .wrapping_add(stop.departure.as_ref().and_then(|e| e.time).unwrap_or(0));
                }
            }
        }
        let raw_scan = start.elapsed();
        let start = Instant::now();
        let compact = data.into_compressed();
        let build = start.elapsed();
        let compact_bytes = LIVE.load(Ordering::Relaxed) - baseline;
        let mut compact_checksum = 0_i64;
        let start = Instant::now();
        for _ in 0..100 {
            for (_, trip) in black_box(&compact).trip_updates().iter() {
                for stop in trip.stops() {
                    compact_checksum = compact_checksum
                        .wrapping_add(stop.departure().and_then(|e| e.time).unwrap_or(0));
                }
            }
        }
        let compact_scan = start.elapsed();
        assert_eq!(black_box(raw_checksum), black_box(compact_checksum));
        println!(
            "{}: 2000 trips / 80000 stops; requested live heap {} -> {} bytes ({:.1}% reduction); build {:?}; 100 timestamp scans ordinary {:?}, compressed {:?}",
            if rail { "rail" } else { "bus" },
            raw_bytes,
            compact_bytes,
            100.0 * (1.0 - compact_bytes as f64 / raw_bytes as f64),
            build,
            raw_scan,
            compact_scan
        );
        drop(compact);
    }
}
