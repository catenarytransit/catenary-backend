// Copyright
// Catenary Transit Initiatives
// Shared types and functions for departure-related endpoints
// Attribution cannot be removed

use catenary::CalendarUnified;
use catenary::aspen_dataset::AspenisedTripUpdate;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;

/// Represents an itinerary stop option with timing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItinOption {
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub interpolated_time_since_start: Option<i32>,
    pub stop_id: CompactString,
    pub gtfs_stop_sequence: u32,
    pub trip_headsign: Option<String>,
    pub trip_headsign_translations: Option<serde_json::Value>,
}

/// A validated trip with all necessary schedule information
#[derive(Clone, Debug, Serialize)]
pub struct ValidTripSet {
    pub chateau_id: String,
    pub trip_id: CompactString,
    pub frequencies: Option<Vec<gtfs_structures::Frequency>>,
    pub trip_service_date: NaiveDate,
    pub itinerary_options: Vec<ItinOption>,
    pub reference_start_of_service_date: chrono::DateTime<chrono_tz::Tz>,
    pub itinerary_pattern_id: String,
    pub direction_pattern_id: String,
    pub route_id: CompactString,
    pub timezone: Option<chrono_tz::Tz>,
    pub trip_start_time: u32,
    pub trip_short_name: Option<CompactString>,
    pub service_id: CompactString,
}

/// Data for a stop that has been moved/reassigned
#[derive(Serialize, Clone, Debug)]
pub struct MovedStopData {
    pub stop_id: String,
    pub scheduled_arrival: Option<u64>,
    pub scheduled_departure: Option<u64>,
    pub realtime_arrival: Option<u64>,
    pub realtime_departure: Option<u64>,
}

/// Index for efficient alert lookups by route or trip
pub struct AlertIndex {
    by_route: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>>,
    by_trip: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>>,
    #[allow(dead_code)]
    general: Vec<(String, catenary::aspen_dataset::AspenisedAlert)>,
}

impl AlertIndex {
    pub fn new(alerts: &BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>) -> Self {
        let mut by_route: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>> =
            HashMap::new();
        let mut by_trip: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>> =
            HashMap::new();
        let general = Vec::new();

        for (id, alert) in alerts {
            for entity in &alert.informed_entity {
                if let Some(r_id) = &entity.route_id {
                    by_route
                        .entry(r_id.clone())
                        .or_default()
                        .push((id.clone(), alert.clone()));
                }
                if let Some(trip) = &entity.trip {
                    if let Some(t_id) = &trip.trip_id {
                        by_trip
                            .entry(t_id.clone())
                            .or_default()
                            .push((id.clone(), alert.clone()));
                    }
                }
            }
        }

        for v in by_route.values_mut() {
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.dedup_by(|a, b| a.0 == b.0);
        }
        for v in by_trip.values_mut() {
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.dedup_by(|a, b| a.0 == b.0);
        }

        Self {
            by_route,
            by_trip,
            general,
        }
    }

    pub fn search(
        &self,
        route_id: &str,
        trip_id: &str,
    ) -> Vec<&catenary::aspen_dataset::AspenisedAlert> {
        let mut candidates = Vec::new();
        if let Some(alerts) = self.by_route.get(route_id) {
            candidates.extend(alerts);
        }
        if let Some(alerts) = self.by_trip.get(trip_id) {
            candidates.extend(alerts);
        }

        candidates.sort_by(|a, b| a.0.cmp(&b.0));
        candidates.dedup_by(|a, b| a.0 == b.0);

        candidates.into_iter().map(|(_, alert)| alert).collect()
    }
}

/// Estimate if a realtime trip update matches a valid scheduled trip's service date
pub fn estimate_service_date(
    valid_trip: &ValidTripSet,
    trip_update: &AspenisedTripUpdate,
    itin_option: &ItinOption,
    calendar_structure: &BTreeMap<String, CalendarUnified>,
    _chateau_id: &str,
) -> bool {
    let trip_offset = itin_option.departure_time_since_start.unwrap_or(
        itin_option
            .arrival_time_since_start
            .unwrap_or(itin_option.interpolated_time_since_start.unwrap_or(0)),
    ) as u64;

    let naive_date_approx_guess = trip_update
        .stop_time_update
        .iter()
        .filter(|x| x.departure.is_some() || x.arrival.is_some())
        .filter_map(|x| {
            if let Some(departure) = &x.departure {
                Some(departure.time)
            } else if let Some(arrival) = &x.arrival {
                Some(arrival.time)
            } else {
                None
            }
        })
        .flatten()
        .min();

    match naive_date_approx_guess {
        Some(least_num) => {
            let tz = match valid_trip.timezone.as_ref() {
                Some(tz) => tz,
                None => return false,
            };
            let rt_least_naive_date = tz.timestamp(least_num as i64, 0);
            let approx_service_date_start =
                rt_least_naive_date - chrono::Duration::seconds(trip_offset as i64);
            let approx_service_date = approx_service_date_start.date();

            let day_before = approx_service_date - chrono::Duration::days(2);

            let mut best_date_score = i64::MAX;
            let mut best_date = None;

            for day in day_before.naive_local().iter_days().take(3) {
                let service_id = valid_trip.service_id.as_str();
                let service_opt = calendar_structure.get(service_id);

                if let Some(service) = service_opt {
                    if catenary::datetime_in_service(service, day) {
                        let day_in_tz_midnight =
                            day.and_hms(12, 0, 0).and_local_timezone(*tz).unwrap()
                                - chrono::Duration::hours(12);
                        let time_delta = rt_least_naive_date
                            .signed_duration_since(day_in_tz_midnight)
                            .num_seconds()
                            .abs();

                        if time_delta < best_date_score {
                            best_date_score = time_delta;
                            best_date = Some(day_in_tz_midnight.date());
                        }
                    }
                }
            }

            if let Some(best_date) = best_date {
                valid_trip.trip_service_date == best_date.naive_local()
            } else {
                false
            }
        }
        None => false,
    }
}
