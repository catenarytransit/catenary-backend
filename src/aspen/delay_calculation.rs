use ahash::AHashSet;
use catenary::CalendarUnified;
use catenary::aspen_dataset::AspenisedStopTimeUpdate;
use catenary::compact_formats::CompactItineraryPatternRow;
use catenary::models::{CompressedTrip, ItineraryPatternMeta};
use chrono::{Datelike, TimeZone, Utc};
use chrono_tz::Tz;
use std::collections::BTreeMap;

pub fn calculate_delay(
    trip_update_delay: Option<i32>,
    trip_start_date: &Option<String>,
    scheduled_stop_ids_hashset: &Option<AHashSet<String>>,
    itinerary_rows: Option<&Vec<CompactItineraryPatternRow>>,
    itinerary_meta: Option<&ItineraryPatternMeta>,
    stop_time_update: &Vec<AspenisedStopTimeUpdate>,
    current_time_unix_timestamp: u64,
    compressed_trip: Option<&CompressedTrip>,
    calendar_structure: &BTreeMap<String, CalendarUnified>,
    timezone: Option<Tz>,
) -> Option<i32> {
    match trip_update_delay {
        Some(delay) => Some(delay),
        None => match (scheduled_stop_ids_hashset, itinerary_rows, itinerary_meta) {
            (Some(scheduled_stop_ids_hashset), Some(itinerary_rows), Some(itinerary_meta)) => {
                let filtered_stu = stop_time_update
                    .clone()
                    .into_iter()
                    .filter(|stu| stu.arrival.is_some() || stu.departure.is_some())
                    .filter(|stu| match &stu.stop_id {
                        Some(stop_id) => scheduled_stop_ids_hashset.contains(stop_id.as_str()),
                        None => false,
                    })
                    .collect::<Vec<AspenisedStopTimeUpdate>>();

                let filtered_stu_after_now = filtered_stu
                    .clone()
                    .into_iter()
                    .filter(|stu| {
                        let mut single_number = None;

                        if let Some(departure) = &stu.departure {
                            if let Some(time) = departure.time {
                                single_number = Some(time);
                            }
                        }

                        if single_number.is_none() {
                            if let Some(arrival) = &stu.arrival {
                                if let Some(time) = arrival.time {
                                    single_number = Some(time);
                                }
                            }
                        }

                        match single_number {
                            None => false,
                            Some(single_number) => {
                                single_number as u64 >= current_time_unix_timestamp
                            }
                        }
                    })
                    .collect::<Vec<_>>();

                match filtered_stu_after_now.len() > 0 {
                    true => {
                        let stu = &filtered_stu_after_now[0];

                        let relevant_stop_rows = itinerary_rows
                            .iter()
                            .filter(|itinerary_row| match &stu.stop_id {
                                Some(stu_stop_id) => itinerary_row.stop_id == stu_stop_id.as_str(),
                                _ => false,
                            })
                            .collect::<Vec<&_>>();

                        let itinerary_stop_row = match relevant_stop_rows.len() {
                            0 => None,
                            1 => Some(relevant_stop_rows[0]),
                            _ => relevant_stop_rows
                                .iter()
                                .filter(|itinerary_row| {
                                    stu.stop_sequence == Some(itinerary_row.stop_sequence as u16)
                                })
                                .collect::<Vec<_>>()
                                .get(0)
                                .map(|v| &***v),
                        };

                        match itinerary_stop_row {
                            Some(itinerary_stop_row) => match compressed_trip {
                                Some(compressed_trip) => {
                                    let start_date = match trip_start_date {
                                        None => {
                                            let timeschedule_number_since_midnight =
                                                match itinerary_stop_row.arrival_time_since_start {
                                                    Some(x) => Some(x),
                                                    None => match itinerary_stop_row
                                                        .departure_time_since_start
                                                    {
                                                        Some(x) => Some(x),
                                                        None => None,
                                                    },
                                                };

                                            match timeschedule_number_since_midnight {
                                                Some(timescheduled) => {
                                                    let mut guesses_date_times = vec![];

                                                    let now = Utc::now();

                                                    let now_in_tz =
                                                        now.with_timezone(&timezone.unwrap());

                                                    let naive_date_now = now_in_tz.date_naive();

                                                    let few_days_before =
                                                        naive_date_now - chrono::Days::new(5);

                                                    for d in few_days_before.iter_days().take(10) {
                                                        let midnight_tz = timezone
                                                            .unwrap()
                                                            .with_ymd_and_hms(
                                                                d.year(),
                                                                d.month(),
                                                                d.day(),
                                                                12,
                                                                0,
                                                                0,
                                                            )
                                                            .unwrap()
                                                            - chrono::Duration::hours(12);

                                                        let schedules = midnight_tz
                                                            + chrono::Duration::seconds(
                                                                timescheduled as i64,
                                                            )
                                                            + chrono::Duration::seconds(
                                                                compressed_trip.start_time as i64,
                                                            );

                                                        let service = calendar_structure.get(
                                                            compressed_trip.service_id.as_str(),
                                                        );

                                                        if let Some(service) = service {
                                                            if catenary::datetime_in_service(
                                                                &service, d,
                                                            ) {
                                                                guesses_date_times.push((
                                                                    d,
                                                                    schedules.timestamp(),
                                                                ));
                                                            }
                                                        }
                                                    }

                                                    let mut chosen_rt_comparison_number = None;

                                                    if let Some(arrival) = &stu.arrival {
                                                        if let Some(a_time) = arrival.time {
                                                            chosen_rt_comparison_number =
                                                                Some(a_time);
                                                        }
                                                    }

                                                    if chosen_rt_comparison_number.is_none() {
                                                        if let Some(departure) = &stu.departure {
                                                            if let Some(d_time) = departure.time {
                                                                chosen_rt_comparison_number =
                                                                    Some(d_time);
                                                            }
                                                        }
                                                    }

                                                    match chosen_rt_comparison_number {
                                                        None => None,
                                                        Some(chosen_rt_comparison_number) => {
                                                            guesses_date_times.sort_by_key(|x| {
                                                                (chosen_rt_comparison_number - x.1)
                                                                    .abs()
                                                            });

                                                            guesses_date_times.get(0).map(|x| x.0)
                                                        }
                                                    }
                                                }
                                                None => None,
                                            }
                                        }
                                        Some(start_date) => {
                                            let chrono_start_date =
                                                chrono::NaiveDate::parse_from_str(
                                                    &start_date,
                                                    "%Y%m%d",
                                                )
                                                .unwrap();

                                            Some(chrono_start_date)
                                        }
                                    };

                                    match &start_date {
                                        None => None,
                                        Some(start_date) => {
                                            let reference_time_noon =
                                                chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap();

                                            let noon_on_start_date = chrono::NaiveDateTime::new(
                                                *start_date,
                                                reference_time_noon,
                                            );

                                            let noon_on_start_date_with_tz = timezone
                                                .unwrap()
                                                .from_local_datetime(&noon_on_start_date)
                                                .unwrap();

                                            //reference time is 12 hours before noon
                                            let reference_time = noon_on_start_date_with_tz
                                                - chrono::Duration::hours(12);

                                            let start_time = reference_time
                                                + chrono::Duration::seconds(
                                                    compressed_trip.start_time.into(),
                                                );

                                            let scheduled_departure_datetime = itinerary_stop_row
                                                .departure_time_since_start
                                                .map(|departure_time_since_start| {
                                                    start_time
                                                        + chrono::Duration::seconds(
                                                            departure_time_since_start.into(),
                                                        )
                                                });

                                            let scheduled_arrival_datetime = itinerary_stop_row
                                                .arrival_time_since_start
                                                .map(|arrival_time_since_start| {
                                                    start_time
                                                        + chrono::Duration::seconds(
                                                            arrival_time_since_start.into(),
                                                        )
                                                });

                                            let scheduled_interpolated_datetime =
                                                itinerary_stop_row
                                                    .interpolated_time_since_start
                                                    .map(|interpolated_time_since_start| {
                                                        start_time
                                                            + chrono::Duration::seconds(
                                                                interpolated_time_since_start
                                                                    .into(),
                                                            )
                                                    });

                                            let delay_arrival = stu
                                                .arrival
                                                .as_ref()
                                                .map(|arrival| {
                                                    arrival.time.map(|arrival_time| {
                                                        let mut delay_arrival = None;

                                                        if let Some(scheduled_arrival_datetime) =
                                                            scheduled_arrival_datetime
                                                        {
                                                            delay_arrival = Some(
                                                                arrival_time
                                                                    - scheduled_arrival_datetime
                                                                        .timestamp(),
                                                            );
                                                        } else {
                                                            if let Some(scheduled_departure_time) =
                                                                scheduled_departure_datetime
                                                            {
                                                                delay_arrival = Some(
                                                                    arrival_time
                                                                        - scheduled_departure_time
                                                                            .timestamp(),
                                                                );
                                                            }
                                                        }

                                                        delay_arrival
                                                    })
                                                })
                                                .flatten()
                                                .flatten();

                                            let delay_departure = stu
                                                .departure
                                                .as_ref()
                                                .map(|departure| {
                                                    departure.time.map(|departure_time| {
                                                        let mut delay_departure = None;

                                                        if let Some(scheduled_departure_datetime) =
                                                            scheduled_departure_datetime
                                                        {
                                                            delay_departure = Some(
                                                                departure_time
                                                                    - scheduled_departure_datetime
                                                                        .timestamp(),
                                                            );
                                                        } else {
                                                            if let Some(scheduled_arrival_time) =
                                                                scheduled_arrival_datetime
                                                            {
                                                                delay_departure = Some(
                                                                    departure_time
                                                                        - scheduled_arrival_time
                                                                            .timestamp(),
                                                                );
                                                            }
                                                        }

                                                        delay_departure
                                                    })
                                                })
                                                .flatten()
                                                .flatten();

                                            let mut delay = match delay_arrival {
                                                Some(delay_arrival) => Some(delay_arrival),
                                                None => delay_departure,
                                            };

                                            if let Some(d) = delay {
                                                //more than 6 hours early should be more like 18 hours late?
                                                if d < -21600 {
                                                    // Prefer positive (late) over negative (early)
                                                    // A train being hours early is almost always a day calculation error
                                                    let candidate = d + 86400;
                                                    if candidate > 0 {
                                                        delay = Some(candidate);
                                                    }
                                                }
                                            }

                                            delay.map(|d| d as i32)
                                        }
                                    }
                                }
                                None => None,
                            },
                            None => None,
                        }
                    }
                    false => None,
                }
            }
            _ => None,
        },
    }
}
