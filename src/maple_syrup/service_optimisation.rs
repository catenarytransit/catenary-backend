use ahash::{AHashMap, AHashSet};
use chrono::{Datelike, Duration, NaiveDate};
use gtfs_structures::{Calendar, CalendarDate, Exception, Gtfs};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ServiceSignature {
    start_date: NaiveDate,
    end_date: NaiveDate,
    // A bitmask or boolean vector representing active days relative to start_date
    // For memory efficiency, we can use a Vec<bool> or just hash the active dates directly.
    // The Go code uses a bool slice.
    active_days_hash: u64,
}

/// Main function to reduce and optimize services
pub fn optimise_services(gtfs: &mut Gtfs) {
    println!("Starting service optimization...");
    let initial_service_count = count_services(gtfs);
    println!("Initial service count: {}", initial_service_count);

    // 1. Identify active dates for all services
    let service_ids: Vec<String> = get_all_service_ids(gtfs);

    // We compute the "signature" for each service in parallel
    let signatures: HashMap<String, ServiceSignature> = service_ids
        .par_iter()
        .map(|id| {
            let (start, end, active_dates) = resolve_active_dates(gtfs, id);
            let mut hasher = ahash::AHasher::default();
            // Sort dates to ensure consistent hashing
            let mut sorted_dates: Vec<NaiveDate> = active_dates.into_iter().collect();
            sorted_dates.sort();
            for date in sorted_dates {
                date.hash(&mut hasher);
            }
            let active_days_hash = hasher.finish();

            (
                id.clone(),
                ServiceSignature {
                    start_date: start,
                    end_date: end,
                    active_days_hash,
                },
            )
        })
        .collect();

    // 2. Group by signature to find duplicates
    let mut signature_to_ids: HashMap<ServiceSignature, Vec<String>> = HashMap::new();
    for (id, sig) in &signatures {
        signature_to_ids
            .entry(sig.clone())
            .or_default()
            .push(id.clone());
    }

    // 3. Create mapping from old ID to canonical ID
    let mut redundancy_map: HashMap<String, String> = HashMap::new();
    let mut services_to_remove: HashSet<String> = HashSet::new();

    for ids in signature_to_ids.values() {
        if ids.is_empty() {
            continue;
        }
        // Heuristic: keep the one being used, or the shortest ID, or just the first.
        // The Go code picks the one with least exceptions as reference.
        // Since we are going to re-optimize exceptions anyway, it doesn't matter much *unless* we skip optimization.
        // Let's pick the first one stably (lexicographically first to be deterministic)
        let mut sorted_ids = ids.clone();
        sorted_ids.sort();
        let canonical = &sorted_ids[0];

        for id in &sorted_ids[1..] {
            redundancy_map.insert(id.clone(), canonical.clone());
            services_to_remove.insert(id.clone());
        }
    }

    println!(
        "Found {} duplicate services to remove.",
        services_to_remove.len()
    );

    // 4. Remap trips
    // We need to iterate over all trips and update service_id if it's in redundancy_map
    for trip in gtfs.trips.values_mut() {
        if let Some(new_id) = redundancy_map.get(&trip.service_id) {
            trip.service_id = new_id.clone();
        }
    }

    // 5. Remove unused services from calendar and calendar_dates
    gtfs.calendar
        .retain(|id, _| !services_to_remove.contains(id));
    gtfs.calendar_dates
        .retain(|id, _| !services_to_remove.contains(id));

    // 6. Minimize/Optimize Calendar for remaining services
    // For each remaining service, we recalculate the optimal calendar/calendar_dates
    // based on the resolved active dates.

    // Collect remaining keys to avoid borrow checker issues
    let remaining_service_ids: Vec<String> = get_all_service_ids(gtfs);

    // Compute optimized representations in parallel
    let optimized_data: Vec<(String, Option<Calendar>, Vec<CalendarDate>)> = remaining_service_ids
        .par_iter()
        .map(|id| {
            let (_, _, active_dates) = resolve_active_dates_from_signatures(gtfs, id, &signatures);
            // Or just re-resolve? Re-resolving is safer as we might have shifted things?
            // Actually signatures computation used the original state.
            // Since we only removed duplicates, the active dates for the canonical service
            // are the same as they were.

            if active_dates.is_empty() {
                // Keep as is? Or empty?
                return (id.clone(), None, Vec::new());
            }

            let (cal, exceptions) = find_optimal_calendar(id, &active_dates);
            (id.clone(), cal, exceptions)
        })
        .collect();

    // Apply optimization
    for (id, cal, exceptions) in optimized_data {
        if let Some(c) = cal {
            gtfs.calendar.insert(id.clone(), c);
        } else {
            gtfs.calendar.remove(&id);
        }

        if exceptions.is_empty() {
            gtfs.calendar_dates.remove(&id);
        } else {
            gtfs.calendar_dates.insert(id, exceptions);
        }
    }

    let final_service_count = count_services(gtfs);
    println!(
        "Service optimization complete. Services: {} -> {}",
        initial_service_count, final_service_count
    );
}

fn count_services(gtfs: &Gtfs) -> usize {
    let mut ids = HashSet::new();
    for id in gtfs.calendar.keys() {
        ids.insert(id);
    }
    for id in gtfs.calendar_dates.keys() {
        ids.insert(id);
    }
    ids.len()
}

fn get_all_service_ids(gtfs: &Gtfs) -> Vec<String> {
    let mut ids = HashSet::new();
    for id in gtfs.calendar.keys() {
        ids.insert(id.clone());
    }
    for id in gtfs.calendar_dates.keys() {
        ids.insert(id.clone());
    }
    for trip in gtfs.trips.values() {
        ids.insert(trip.service_id.clone());
    }
    ids.into_iter().collect()
}

fn resolve_active_dates(
    gtfs: &Gtfs,
    service_id: &str,
) -> (NaiveDate, NaiveDate, HashSet<NaiveDate>) {
    let mut dates = HashSet::new();

    // Start with calendar
    if let Some(cal) = gtfs.calendar.get(service_id) {
        let mut curr = cal.start_date;
        while curr <= cal.end_date {
            let active = match curr.weekday() {
                chrono::Weekday::Mon => cal.monday,
                chrono::Weekday::Tue => cal.tuesday,
                chrono::Weekday::Wed => cal.wednesday,
                chrono::Weekday::Thu => cal.thursday,
                chrono::Weekday::Fri => cal.friday,
                chrono::Weekday::Sat => cal.saturday,
                chrono::Weekday::Sun => cal.sunday,
            };
            if active {
                dates.insert(curr);
            }
            curr = curr + Duration::days(1);
        }
    }

    // Apply exceptions
    if let Some(dates_list) = gtfs.calendar_dates.get(service_id) {
        for d in dates_list {
            match d.exception_type {
                Exception::Added => {
                    dates.insert(d.date);
                }
                Exception::Deleted => {
                    dates.remove(&d.date);
                }
            }
        }
    }

    if dates.is_empty() {
        // Return dummy? Or handles gracefully?
        // Use unix epoch or something safe
        let now = chrono::Utc::now().naive_utc().date();
        return (now, now, dates);
    }

    let min_date = *dates.iter().min().unwrap();
    let max_date = *dates.iter().max().unwrap();
    (min_date, max_date, dates)
}

// Helper to reuse signatures if possible, but for now we just implemented resolve_active_dates
fn resolve_active_dates_from_signatures(
    gtfs: &Gtfs,
    service_id: &str,
    signatures: &HashMap<String, ServiceSignature>,
) -> (NaiveDate, NaiveDate, HashSet<NaiveDate>) {
    resolve_active_dates(gtfs, service_id)
}

fn find_optimal_calendar(
    service_id: &str,
    active_dates: &HashSet<NaiveDate>,
) -> (Option<Calendar>, Vec<CalendarDate>) {
    // Brute force optimization:
    // Try all 127 combinations of days of week.
    // For each combination, determine best start/end date (heuristic: min/max of active_dates)
    // Calculate exceptions (Added/Deleted).
    // Score = number of exceptions.
    // Pick winner.

    if active_dates.is_empty() {
        return (None, Vec::new());
    }

    let min_date = *active_dates.iter().min().unwrap();
    let max_date = *active_dates.iter().max().unwrap();

    let mut best_score = usize::MAX;
    let mut best_cal: Option<Calendar> = None;
    let mut best_exceptions: Vec<CalendarDate> = Vec::new();

    // There are 127 non-empty masks. + 1 empty mask (just exceptions).
    // Mask is 7 bits: Mon..Sun

    // Heuristic: Iterate all masks.
    for mask in 0..128u8 {
        let monday = (mask & 1) != 0;
        let tuesday = (mask & 2) != 0;
        let wednesday = (mask & 4) != 0;
        let thursday = (mask & 8) != 0;
        let friday = (mask & 16) != 0;
        let saturday = (mask & 32) != 0;
        let sunday = (mask & 64) != 0;

        // Candidate Calendar
        let candidate_cal = Calendar {
            id: service_id.to_string(),
            start_date: min_date,
            end_date: max_date,
            monday,
            tuesday,
            wednesday,
            thursday,
            friday,
            saturday,
            sunday,
        };

        // Determine exceptions needed for this calendar to match active_dates
        let (exceptions, score) = calculate_exceptions(&candidate_cal, active_dates);

        if score < best_score {
            best_score = score;
            if mask == 0 {
                best_cal = None;
            } else {
                best_cal = Some(candidate_cal);
            }
            best_exceptions = exceptions;
        }
    }

    (best_cal, best_exceptions)
}

fn calculate_exceptions(
    cal: &Calendar,
    target_dates: &HashSet<NaiveDate>,
) -> (Vec<CalendarDate>, usize) {
    let mut exceptions = Vec::new();
    let mut score = 0;

    // Iterate from start to end
    let mut curr = cal.start_date;
    while curr <= cal.end_date {
        let should_be_active = match curr.weekday() {
            chrono::Weekday::Mon => cal.monday,
            chrono::Weekday::Tue => cal.tuesday,
            chrono::Weekday::Wed => cal.wednesday,
            chrono::Weekday::Thu => cal.thursday,
            chrono::Weekday::Fri => cal.friday,
            chrono::Weekday::Sat => cal.saturday,
            chrono::Weekday::Sun => cal.sunday,
        };

        let is_active = target_dates.contains(&curr);

        if should_be_active && !is_active {
            // Needed, but not in target -> Delete
            exceptions.push(CalendarDate {
                service_id: cal.id.clone(),
                date: curr,
                exception_type: Exception::Deleted,
            });
            score += 1;
        } else if !should_be_active && is_active {
            // Not needed by pattern, but is in target -> Add
            exceptions.push(CalendarDate {
                service_id: cal.id.clone(),
                date: curr,
                exception_type: Exception::Added,
            });
            score += 1;
        }

        curr = curr + Duration::days(1);
    }

    (exceptions, score)
}
