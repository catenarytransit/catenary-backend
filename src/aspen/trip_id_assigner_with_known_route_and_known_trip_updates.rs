use chrono::{Datelike, Timelike};
use chrono_tz::Europe::Berlin;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use scc::HashMap as SccHashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use catenary::aspen_dataset::GtfsRtType;
use catenary::compact_formats::CompactFeedMessage;
use catenary::models::{CompressedTrip, ItineraryPatternMeta, ItineraryPatternRow, Stop};
use catenary::postgres_tools::CatenaryPostgresPool;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VehicleHistoryEntry {
    pub latitude: f32,
    pub longitude: f32,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VehicleState {
    pub positions: Vec<VehicleHistoryEntry>,
    pub last_matched_pattern: Option<String>,
    pub last_stop_index: Option<usize>,
    pub last_update_time: u64,
}

lazy_static::lazy_static! {
    static ref VEHICLE_POSITION_HISTORY: Mutex<HashMap<String, VehicleState>> = Mutex::new(HashMap::new());
}

fn save_vehicle_history(
    history: &HashMap<String, VehicleState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = "data/aspen_data";
    std::fs::create_dir_all(dir)?;

    let file_path = format!("{}/dresden_vehicle_history.bin.zlib", dir);
    let temp_file_path = format!("{}/dresden_vehicle_history.bin.zlib.tmp", dir);

    let file = File::create(&temp_file_path)?;
    let writer = BufWriter::new(file);
    let mut encoder = flate2::write::ZlibEncoder::new(writer, flate2::Compression::default());

    let bytes = catenary::bincode_serialize(history)?;
    encoder.write_all(&bytes)?;

    encoder.finish()?;

    std::fs::rename(temp_file_path, file_path)?;

    Ok(())
}

fn load_vehicle_history()
-> Result<Option<HashMap<String, VehicleState>>, Box<dyn std::error::Error + Send + Sync>> {
    let file_path = "data/aspen_data/dresden_vehicle_history.bin.zlib";
    let path = Path::new(&file_path);

    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut decoder = flate2::read::ZlibDecoder::new(reader);
    let mut buffer = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut buffer)?;

    let history: HashMap<String, VehicleState> = catenary::bincode_deserialize(&buffer)?;

    Ok(Some(history))
}

fn gc_history(history: &mut HashMap<String, VehicleState>, current_time_ms: u64) {
    history.retain(|_, state| current_time_ms.saturating_sub(state.last_update_time) < 300_000);
}

fn get_expected_trip_position(
    trip: &CompressedTrip,
    pattern_stops: &[(ItineraryPatternRow, Stop)],
    adjusted_time: i32,
) -> Option<(f32, f32)> {
    if pattern_stops.is_empty() {
        return None;
    }

    let first_stop = &pattern_stops[0];
    let first_time = trip.start_time as i32
        + first_stop
            .0
            .arrival_time_since_start
            .or(first_stop.0.departure_time_since_start)
            .unwrap_or(0);

    if adjusted_time <= first_time {
        let p = first_stop.1.point.as_ref()?;
        return Some((p.y as f32, p.x as f32));
    }

    let last_stop = &pattern_stops[pattern_stops.len() - 1];
    let last_time = trip.start_time as i32
        + last_stop
            .0
            .arrival_time_since_start
            .or(last_stop.0.departure_time_since_start)
            .unwrap_or(0);

    if adjusted_time >= last_time {
        let p = last_stop.1.point.as_ref()?;
        return Some((p.y as f32, p.x as f32));
    }

    for i in 0..pattern_stops.len() - 1 {
        let s_curr = &pattern_stops[i];
        let s_next = &pattern_stops[i + 1];
        let t_curr = trip.start_time as i32
            + s_curr
                .0
                .arrival_time_since_start
                .or(s_curr.0.departure_time_since_start)
                .unwrap_or(0);
        let t_next = trip.start_time as i32
            + s_next
                .0
                .arrival_time_since_start
                .or(s_next.0.departure_time_since_start)
                .unwrap_or(0);

        if adjusted_time >= t_curr && adjusted_time <= t_next {
            if t_curr == t_next {
                let p = s_curr.1.point.as_ref()?;
                return Some((p.y as f32, p.x as f32));
            }
            let fraction = (adjusted_time - t_curr) as f32 / (t_next - t_curr) as f32;
            let p_curr = s_curr.1.point.as_ref()?;
            let p_next = s_next.1.point.as_ref()?;
            let expected_lat = p_curr.y as f32 + fraction * (p_next.y as f32 - p_curr.y as f32);
            let expected_lon = p_curr.x as f32 + fraction * (p_next.x as f32 - p_curr.x as f32);
            return Some((expected_lat, expected_lon));
        }
    }

    None
}

pub async fn assign_trips_for_dresden(
    authoritative_gtfs_rt: &Arc<SccHashMap<(String, GtfsRtType), CompactFeedMessage>>,
    conn: &mut diesel_async::AsyncPgConnection,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    use log::debug;

    debug!("Starting assign_trips_for_dresden...");
    let now_berlin = chrono::Utc::now().with_timezone(&Berlin);
    let today = now_berlin.date_naive();
    let time_of_day_secs = now_berlin.time().num_seconds_from_midnight() as i32;
    let current_time_ms = now_berlin.timestamp_millis() as u64;

    let tlms_feed_key = ("f-tlms~rt".to_string(), GtfsRtType::VehiclePositions);
    let feed_msg = match authoritative_gtfs_rt.get_async(&tlms_feed_key).await {
        Some(msg) => {
            let msg_val = msg.get().clone();
            debug!(
                "Found 'f-tlms~rt' vehicle positions feed with {} entities",
                msg_val.entity.len()
            );
            msg_val
        }
        None => {
            debug!("'f-tlms~rt' vehicle positions feed not found in authoritative_gtfs_rt");
            return Ok(HashMap::new());
        }
    };

    let mut active_route_ids = HashSet::new();
    for entity in &feed_msg.entity {
        if let Some(vehicle) = &entity.vehicle {
            if let Some(trip) = &vehicle.trip {
                if let Some(route_id) = &trip.route_id {
                    if route_id.starts_with("de:vvo") {
                        active_route_ids.insert(route_id.clone());
                    }
                }
            }
        }
    }

    if active_route_ids.is_empty() {
        debug!("No active route IDs found in 'f-tlms~rt' entities");
        return Ok(HashMap::new());
    }

    let active_routes_vec: Vec<String> = active_route_ids.into_iter().collect();
    debug!("Active route IDs in 'f-tlms~rt': {:?}", active_routes_vec);

    use catenary::schema::gtfs::itinerary_pattern_meta::dsl as ip_meta_dsl;
    let metas = ip_meta_dsl::itinerary_pattern_meta
        .filter(ip_meta_dsl::chateau.eq("deutschland"))
        .filter(ip_meta_dsl::route_id.eq_any(&active_routes_vec))
        .select(ItineraryPatternMeta::as_select())
        .load::<ItineraryPatternMeta>(conn)
        .await?;

    debug!(
        "Loaded {} itinerary pattern metas for active routes",
        metas.len()
    );
    if metas.is_empty() {
        debug!("No itinerary pattern metas found for active routes in 'deutschland' chateau");
        return Ok(HashMap::new());
    }

    let meta_ids: Vec<String> = metas
        .iter()
        .map(|m| m.itinerary_pattern_id.clone())
        .collect();

    use catenary::schema::gtfs::itinerary_pattern::dsl as ip_dsl;
    let ip_rows = ip_dsl::itinerary_pattern
        .filter(ip_dsl::chateau.eq("deutschland"))
        .filter(ip_dsl::itinerary_pattern_id.eq_any(&meta_ids))
        .select(ItineraryPatternRow::as_select())
        .load::<ItineraryPatternRow>(conn)
        .await?;

    debug!("Loaded {} itinerary pattern rows", ip_rows.len());

    let stop_ids: Vec<String> = ip_rows.iter().map(|r| r.stop_id.to_string()).collect();

    use catenary::schema::gtfs::stops::dsl as stops_dsl;
    let stops = stops_dsl::stops
        .filter(stops_dsl::chateau.eq("deutschland"))
        .filter(stops_dsl::gtfs_id.eq_any(&stop_ids))
        .select(Stop::as_select())
        .load::<Stop>(conn)
        .await?;

    debug!("Loaded {} stops", stops.len());

    let stops_by_id: HashMap<String, Stop> =
        stops.into_iter().map(|s| (s.gtfs_id.clone(), s)).collect();

    let mut patterns: HashMap<String, Vec<(ItineraryPatternRow, Stop)>> = HashMap::new();
    for row in ip_rows {
        if let Some(stop) = stops_by_id.get(row.stop_id.as_str()) {
            patterns
                .entry(row.itinerary_pattern_id.clone())
                .or_default()
                .push((row, stop.clone()));
        }
    }

    for stops_list in patterns.values_mut() {
        stops_list.sort_by_key(|(row, _)| row.stop_sequence);
    }
    debug!("Organized {} itinerary patterns with stops", patterns.len());

    use catenary::schema::gtfs::calendar::dsl as cal_dsl;
    let calendars = cal_dsl::calendar
        .filter(cal_dsl::chateau.eq("deutschland"))
        .filter(cal_dsl::gtfs_start_date.le(today))
        .filter(cal_dsl::gtfs_end_date.ge(today))
        .select(catenary::models::Calendar::as_select())
        .load::<catenary::models::Calendar>(conn)
        .await?;

    use catenary::schema::gtfs::calendar_dates::dsl as cal_date_dsl;
    let calendar_dates = cal_date_dsl::calendar_dates
        .filter(cal_date_dsl::chateau.eq("deutschland"))
        .filter(cal_date_dsl::gtfs_date.eq(today))
        .select(catenary::models::CalendarDate::as_select())
        .load::<catenary::models::CalendarDate>(conn)
        .await?;

    debug!(
        "Loaded {} calendars and {} calendar date exceptions for today ({})",
        calendars.len(),
        calendar_dates.len(),
        today
    );

    let mut active_services = HashSet::new();
    for cal in calendars {
        let mut day_active = match today.weekday() {
            chrono::Weekday::Mon => cal.monday,
            chrono::Weekday::Tue => cal.tuesday,
            chrono::Weekday::Wed => cal.wednesday,
            chrono::Weekday::Thu => cal.thursday,
            chrono::Weekday::Fri => cal.friday,
            chrono::Weekday::Sat => cal.saturday,
            chrono::Weekday::Sun => cal.sunday,
        };

        for date_ex in &calendar_dates {
            if date_ex.service_id == cal.service_id {
                if date_ex.exception_type == 2 {
                    day_active = false;
                }
            }
        }

        if day_active {
            active_services.insert(cal.service_id.to_string());
        }
    }

    for date_add in &calendar_dates {
        if date_add.exception_type == 1 {
            active_services.insert(date_add.service_id.to_string());
        }
    }

    debug!(
        "Active service IDs for today: {} services",
        active_services.len()
    );
    if active_services.is_empty() {
        debug!("No active service IDs for today");
        return Ok(HashMap::new());
    }

    let active_services_vec: Vec<String> = active_services.into_iter().collect();

    use catenary::schema::gtfs::trips_compressed::dsl as trip_dsl;
    let trips = trip_dsl::trips_compressed
        .filter(trip_dsl::chateau.eq("deutschland"))
        .filter(trip_dsl::route_id.eq_any(&active_routes_vec))
        .filter(trip_dsl::service_id.eq_any(&active_services_vec))
        .select(CompressedTrip::as_select())
        .load::<CompressedTrip>(conn)
        .await?;

    debug!(
        "Loaded {} compressed trips for active routes and active services",
        trips.len()
    );

    let mut delfi_updates = HashMap::new();
    let delfi_feed_key = ("f-delfi~de~motis~rt".to_string(), GtfsRtType::TripUpdates);
    if let Some(delfi_msg) = authoritative_gtfs_rt.get_async(&delfi_feed_key).await {
        let delfi_msg = delfi_msg.get();
        debug!(
            "Found 'f-delfi~de~motis~rt' trip updates feed with {} entities",
            delfi_msg.entity.len()
        );
        for entity in &delfi_msg.entity {
            if let Some(tu) = &entity.trip_update {
                if let Some(trip_id) = &tu.trip.trip_id {
                    let delay = tu
                        .delay
                        .or_else(|| {
                            tu.stop_time_update.get(0).and_then(|stu| {
                                stu.arrival
                                    .as_ref()
                                    .and_then(|a| a.delay)
                                    .or_else(|| stu.departure.as_ref().and_then(|d| d.delay))
                            })
                        })
                        .unwrap_or(0);
                    delfi_updates.insert(trip_id.clone(), delay);
                }
            }
        }
        debug!(
            "Processed {} delfi trip updates with delay information",
            delfi_updates.len()
        );
    } else {
        debug!("'f-delfi~de~motis~rt' trip updates feed not found in authoritative_gtfs_rt");
    }

    let mut assignments = HashMap::new();

    {
        let mut history_lock = VEHICLE_POSITION_HISTORY.lock().unwrap();

        static LOADED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !std::sync::atomic::AtomicBool::load(&LOADED, std::sync::atomic::Ordering::Relaxed) {
            match load_vehicle_history() {
                Ok(Some(history)) => {
                    *history_lock = history;
                    debug!(
                        "Loaded {} vehicle history entries from disk",
                        history_lock.len()
                    );
                }
                Ok(None) => {
                    debug!("No vehicle history file found on disk, starting fresh");
                }
                Err(e) => {
                    log::error!("Error loading vehicle history from disk: {:?}", e);
                }
            }
            std::sync::atomic::AtomicBool::store(
                &LOADED,
                true,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        gc_history(&mut history_lock, current_time_ms);

        for entity in &feed_msg.entity {
            if let Some(vehicle) = &entity.vehicle {
                let vehicle_id = match &vehicle.vehicle {
                    Some(desc) => match &desc.id {
                        Some(id) => id.clone(),
                        None => {
                            debug!("Skipping vehicle entity because vehicle.id is missing");
                            continue;
                        }
                    },
                    None => {
                        debug!("Skipping vehicle entity because vehicle descriptor is missing");
                        continue;
                    }
                };

                let route_id = match &vehicle.trip {
                    Some(trip) => match &trip.route_id {
                        Some(r_id) => r_id.clone(),
                        None => {
                            debug!(
                                "Skipping vehicle {} because route_id is missing",
                                vehicle_id
                            );
                            continue;
                        }
                    },
                    None => {
                        debug!(
                            "Skipping vehicle {} because trip descriptor is missing",
                            vehicle_id
                        );
                        continue;
                    }
                };

                let pos = match &vehicle.position {
                    Some(p) => p,
                    None => {
                        debug!(
                            "Skipping vehicle {} because position is missing",
                            vehicle_id
                        );
                        continue;
                    }
                };

                let current_lat = pos.latitude;
                let current_lon = pos.longitude;

                debug!(
                    "Processing vehicle_id: {}, route_id: {}, pos: ({}, {})",
                    vehicle_id, route_id, current_lat, current_lon
                );

                let state =
                    history_lock
                        .entry(vehicle_id.clone())
                        .or_insert_with(|| VehicleState {
                            positions: Vec::new(),
                            last_matched_pattern: None,
                            last_stop_index: None,
                            last_update_time: current_time_ms,
                        });

                state.last_update_time = current_time_ms;

                if state.positions.len() >= 5 {
                    state.positions.remove(0);
                }
                state.positions.push(VehicleHistoryEntry {
                    latitude: current_lat,
                    longitude: current_lon,
                    timestamp: current_time_ms,
                });

                debug!(
                    "Vehicle {} history size: {}",
                    vehicle_id,
                    state.positions.len()
                );

                if state.positions.len() >= 3 {
                    let p1 = &state.positions[state.positions.len() - 3];
                    let p2 = &state.positions[state.positions.len() - 2];
                    let p3 = &state.positions[state.positions.len() - 1];
                    let dx1 = p2.longitude - p1.longitude;
                    let dy1 = p2.latitude - p1.latitude;
                    let dx2 = p3.longitude - p2.longitude;
                    let dy2 = p3.latitude - p2.latitude;
                    let len1 = (dx1 * dx1 + dy1 * dy1).sqrt();
                    let len2 = (dx2 * dx2 + dy2 * dy2).sqrt();
                    if len1 > 1e-5 && len2 > 1e-5 {
                        let dot = (dx1 * dx2 + dy1 * dy2) / (len1 * len2);
                        if dot < -0.5 {
                            debug!(
                                "Vehicle {} direction reversal detected (dot: {}). Clearing history.",
                                vehicle_id, dot
                            );
                            state.positions.clear();
                            state.positions.push(VehicleHistoryEntry {
                                latitude: current_lat,
                                longitude: current_lon,
                                timestamp: current_time_ms,
                            });
                        }
                    }
                }

                let candidate_metas: Vec<&ItineraryPatternMeta> = metas
                    .iter()
                    .filter(|m| m.route_id.as_str() == route_id.as_str())
                    .collect();

                debug!(
                    "Vehicle {} has {} candidate itinerary pattern metas for route_id {}",
                    vehicle_id,
                    candidate_metas.len(),
                    route_id
                );
                if candidate_metas.is_empty() {
                    continue;
                }

                let mut best_pattern_id = None;
                let mut best_pattern_score = -1.0f32;
                let mut best_closest_idx = 0;

                for meta in &candidate_metas {
                    let pattern_stops = match patterns.get(&meta.itinerary_pattern_id) {
                        Some(ps) => ps,
                        None => {
                            debug!(
                                "No pattern stops found for itinerary_pattern_id {}",
                                meta.itinerary_pattern_id
                            );
                            continue;
                        }
                    };

                    if pattern_stops.is_empty() {
                        debug!(
                            "Pattern stops are empty for itinerary_pattern_id {}",
                            meta.itinerary_pattern_id
                        );
                        continue;
                    }

                    let num_stops = pattern_stops.len();
                    let mut sum_dist = 0.0;
                    let mut stop_indices = Vec::new();

                    for pos_entry in &state.positions {
                        let mut min_d = f32::MAX;
                        let mut closest_s_idx = 0;
                        for (idx, (_, stop)) in pattern_stops.iter().enumerate() {
                            if let Some(point) = &stop.point {
                                let dist = ((pos_entry.latitude - point.y as f32).powi(2)
                                    + (pos_entry.longitude - point.x as f32).powi(2))
                                .sqrt();
                                if dist < min_d {
                                    min_d = dist;
                                    closest_s_idx = idx;
                                }
                            }
                        }
                        sum_dist += min_d;
                        stop_indices.push(closest_s_idx);
                    }

                    let current_closest_idx = *stop_indices.last().unwrap();

                    if let Some(last_pattern) = &state.last_matched_pattern {
                        if last_pattern == &meta.itinerary_pattern_id {
                            if let Some(last_idx) = state.last_stop_index {
                                if num_stops > 5 {
                                    let was_near_end = last_idx > (num_stops * 8) / 10;
                                    let is_near_start = current_closest_idx < (num_stops * 2) / 10;
                                    if was_near_end && is_near_start {
                                        debug!(
                                            "Vehicle {} was near end ({}/{}) of pattern {} and is now near start ({}/{}) - resetting state.",
                                            vehicle_id,
                                            last_idx,
                                            num_stops,
                                            last_pattern,
                                            current_closest_idx,
                                            num_stops
                                        );
                                        state.positions.clear();
                                        state.positions.push(VehicleHistoryEntry {
                                            latitude: current_lat,
                                            longitude: current_lon,
                                            timestamp: current_time_ms,
                                        });
                                        state.last_matched_pattern = None;
                                        state.last_stop_index = None;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    let avg_dist = sum_dist / state.positions.len() as f32;
                    let dist_score = (-avg_dist / 0.005).exp();

                    let mut progress_score = 1.0f32;
                    if stop_indices.len() >= 2 {
                        let mut forward_count = 0;
                        let mut backward_count = 0;
                        for i in 1..stop_indices.len() {
                            if stop_indices[i] >= stop_indices[i - 1] {
                                forward_count += 1;
                            } else {
                                backward_count += 1;
                            }
                        }
                        if forward_count < backward_count {
                            progress_score = 0.1;
                        }
                    }

                    let score = dist_score * progress_score;
                    debug!(
                        "  Pattern {} stops: {}, avg_dist: {:.6}, dist_score: {:.4}, progress_score: {:.2}, total score: {:.4}",
                        meta.itinerary_pattern_id,
                        num_stops,
                        avg_dist,
                        dist_score,
                        progress_score,
                        score
                    );

                    if score > best_pattern_score {
                        best_pattern_score = score;
                        best_pattern_id = Some(meta.itinerary_pattern_id.clone());
                        best_closest_idx = current_closest_idx;
                    }
                }

                debug!(
                    "Vehicle {} best pattern: {:?} with score: {:.4}",
                    vehicle_id, best_pattern_id, best_pattern_score
                );

                if let Some(pat_id) = best_pattern_id {
                    state.last_matched_pattern = Some(pat_id.clone());
                    state.last_stop_index = Some(best_closest_idx);

                    let pattern_stops = match patterns.get(&pat_id) {
                        Some(ps) => ps,
                        None => continue,
                    };

                    let candidate_trips: Vec<&CompressedTrip> = trips
                        .iter()
                        .filter(|t| t.itinerary_pattern_id == pat_id)
                        .collect();

                    debug!(
                        "Vehicle {} found {} candidate trips for pattern {}",
                        vehicle_id,
                        candidate_trips.len(),
                        pat_id
                    );

                    let mut best_trip_id = None;
                    let mut min_trip_dist = f32::MAX;

                    for trip in candidate_trips {
                        let delay_sec = delfi_updates.get(&trip.trip_id).copied().unwrap_or(0);
                        let adjusted_time = time_of_day_secs - delay_sec;

                        if let Some((expected_lat, expected_lon)) =
                            get_expected_trip_position(trip, pattern_stops, adjusted_time)
                        {
                            let dist = ((current_lat - expected_lat).powi(2)
                                + (current_lon - expected_lon).powi(2))
                            .sqrt();
                            debug!(
                                "  Trip {} delay: {}s, adjusted_time: {}, expected_pos: ({:.6}, {:.6}), current_pos: ({:.6}, {:.6}), dist: {:.6}",
                                trip.trip_id,
                                delay_sec,
                                adjusted_time,
                                expected_lat,
                                expected_lon,
                                current_lat,
                                current_lon,
                                dist
                            );

                            if dist < min_trip_dist {
                                min_trip_dist = dist;
                                best_trip_id = Some(trip.trip_id.clone());
                            }
                        } else {
                            debug!(
                                "  Trip {} could not determine expected position (adjusted_time: {})",
                                trip.trip_id, adjusted_time
                            );
                        }
                    }

                    debug!(
                        "Vehicle {} best trip: {:?} with distance {:.6}",
                        vehicle_id, best_trip_id, min_trip_dist
                    );

                    if let Some(trip_id) = best_trip_id {
                        debug!("Assigning trip_id {} to vehicle {}", trip_id, vehicle_id);
                        assignments.insert(vehicle_id, trip_id);
                    }
                }
            }
        }

        if let Err(e) = save_vehicle_history(&history_lock) {
            log::error!("Error saving vehicle history to disk: {:?}", e);
        }
    }

    debug!(
        "assign_trips_for_dresden finished. Assigned {} trips.",
        assignments.len()
    );

    Ok(assignments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vehicle_history_serialization() {
        let mut history = HashMap::new();
        let state = VehicleState {
            positions: vec![
                VehicleHistoryEntry {
                    latitude: 51.0504,
                    longitude: 13.7373,
                    timestamp: 1625000000,
                },
                VehicleHistoryEntry {
                    latitude: 51.0510,
                    longitude: 13.7380,
                    timestamp: 1625000030,
                },
            ],
            last_matched_pattern: Some("test_pattern".to_string()),
            last_stop_index: Some(3),
            last_update_time: 1625000030,
        };
        history.insert("vehicle_1".to_string(), state);

        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("dresden_vehicle_history_test.bin.zlib");
        let temp_file_path = temp_dir.join("dresden_vehicle_history_test.bin.zlib.tmp");

        let save_res = (|| {
            let file = File::create(&temp_file_path)?;
            let writer = BufWriter::new(file);
            let mut encoder =
                flate2::write::ZlibEncoder::new(writer, flate2::Compression::default());
            let bytes = catenary::bincode_serialize(&history)?;
            encoder.write_all(&bytes)?;
            encoder.finish()?;
            std::fs::rename(&temp_file_path, &file_path)?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })();
        assert!(save_res.is_ok());

        let loaded_history: Result<HashMap<String, VehicleState>, Box<dyn std::error::Error>> =
            (|| {
                let file = File::open(&file_path)?;
                let reader = BufReader::new(file);
                let mut decoder = flate2::read::ZlibDecoder::new(reader);
                let mut buffer = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut buffer)?;
                let hist = catenary::bincode_deserialize(&buffer)?;
                Ok(hist)
            })();

        assert!(loaded_history.is_ok());
        let loaded_history = loaded_history.unwrap();
        assert_eq!(loaded_history.len(), 1);
        let loaded_state = loaded_history.get("vehicle_1").unwrap();
        assert_eq!(loaded_state.positions.len(), 2);
        assert_eq!(loaded_state.positions[0].latitude, 51.0504);
        assert_eq!(
            loaded_state.last_matched_pattern.as_deref(),
            Some("test_pattern")
        );
        assert_eq!(loaded_state.last_stop_index, Some(3));
        assert_eq!(loaded_state.last_update_time, 1625000030);

        let _ = std::fs::remove_file(file_path);
    }
}
