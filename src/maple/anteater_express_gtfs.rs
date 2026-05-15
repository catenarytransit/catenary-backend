use gtfs_structures::Gtfs;
use std::collections::HashMap;

/// Represents a distinct temporal segment of the transit schedule.
struct ServiceWindow {
    start_sec: u32,
    end_sec: u32,
    headway_sec: u32,
    service_id: &'static str,
}

pub fn redo_anteater_express_gtfs(mut gtfs: Gtfs) -> Gtfs {
    // Helper to find a stop by ID or Stop Code (UCI GTFS can be inconsistent)
    let find_stop = |gtfs: &Gtfs, identifier: &str| {
        gtfs.stops.get(identifier).cloned().or_else(|| {
            gtfs.stops
                .values()
                .find(|s| s.code.as_deref() == Some(identifier))
                .cloned()
        })
    };

    // 1. Identify and extract templates
    let mut templates = HashMap::new();
    let route_configs = [
        ("A Line", "107"), // Using codes/IDs common in UCI GTFS
        ("E Line", "100"),
        ("H Line", "100"),
        ("M Line", "100"),
        ("N Line", "107"),
    ];

    for (line_name, primary_stop_id) in route_configs {
        if let Some(route) = gtfs.routes.values().find(|r| {
            r.long_name
                .as_deref()
                .unwrap_or("")
                .eq_ignore_ascii_case(line_name)
        }) {
            // Pick a template trip that actually HAS stop times
            if let Some(mut template) = gtfs
                .trips
                .values()
                .filter(|t| t.route_id == route.id && !t.stop_times.is_empty())
                .max_by_key(|t| t.stop_times.len())
                .cloned()
            {
                // Overwrite H-Line sequence
                if line_name == "H Line" {
                    if let Some(default_st) = template.stop_times.first().cloned() {
                        template.stop_times.clear();
                        let h_stops = vec![
                            ("100", 0),
                            ("116", 240),
                            ("117", 270),
                            ("101", 300),
                            ("102", 600),
                            ("103", 630),
                            ("104", 660),
                            ("105", 840),
                            ("106", 870),
                            ("112", 960),
                            ("113", 990),
                            ("114", 1020),
                            ("118", 1320),
                            ("107", 1440),
                            ("100", 1740),
                        ];
                        for (i, (sid, offset)) in h_stops.into_iter().enumerate() {
                            if let Some(stop) = find_stop(&gtfs, sid) {
                                let mut st = default_st.clone();
                                st.stop = stop;
                                st.arrival_time = Some(offset);
                                st.departure_time = Some(offset);
                                st.stop_sequence = i as u32;
                                template.stop_times.push(st);
                            }
                        }
                    }
                }

                // Final safety: ensure every stop time has an arrival/departure
                // if it was missing in the template, we'll use 0 as a base.
                for st in template.stop_times.iter_mut() {
                    if st.arrival_time.is_none() {
                        st.arrival_time = Some(0);
                    }
                    if st.departure_time.is_none() {
                        st.departure_time = st.arrival_time;
                    }
                }

                templates.insert(line_name, (route.id.clone(), template, primary_stop_id));
            }
        }
    }

    // 2. Clear and Rebuild
    gtfs.trips.clear();
    let mut new_trips = HashMap::new();

    for (line_name, (route_id, template, primary_stop_id)) in templates {
        // Find the anchor offset in the template trip
        let anchor_offset = template
            .stop_times
            .iter()
            .find(|st| {
                st.stop.id == primary_stop_id || st.stop.code.as_deref() == Some(primary_stop_id)
            })
            .and_then(|st| st.arrival_time)
            .unwrap_or(0);

        let windows = match line_name {
            "A Line" => vec![
                ServiceWindow {
                    start_sec: 27480,
                    end_sec: 37980,
                    headway_sec: 480,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 37980,
                    end_sec: 67980,
                    headway_sec: 780,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 27480,
                    end_sec: 37980,
                    headway_sec: 480,
                    service_id: "TL-13",
                },
                ServiceWindow {
                    start_sec: 37980,
                    end_sec: 56700,
                    headway_sec: 780,
                    service_id: "TL-13",
                },
            ],
            "M Line" => vec![
                ServiceWindow {
                    start_sec: 27900,
                    end_sec: 67380,
                    headway_sec: 480,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 67380,
                    end_sec: 71700,
                    headway_sec: 780,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 71700,
                    end_sec: 81000,
                    headway_sec: 1500,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 27900,
                    end_sec: 57900,
                    headway_sec: 480,
                    service_id: "TL-13",
                },
                ServiceWindow {
                    start_sec: 57900,
                    end_sec: 70200,
                    headway_sec: 1500,
                    service_id: "TL-13",
                },
            ],
            "N Line" => vec![
                ServiceWindow {
                    start_sec: 27300,
                    end_sec: 57300,
                    headway_sec: 420,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 57300,
                    end_sec: 68100,
                    headway_sec: 600,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 27300,
                    end_sec: 56880,
                    headway_sec: 420,
                    service_id: "TL-13",
                },
            ],
            "H Line" => vec![
                ServiceWindow {
                    start_sec: 68400,
                    end_sec: 81000,
                    headway_sec: 600,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 57600,
                    end_sec: 70200,
                    headway_sec: 600,
                    service_id: "TL-13",
                },
            ],
            "E Line" => vec![
                ServiceWindow {
                    start_sec: 27600,
                    end_sec: 67800,
                    headway_sec: 600,
                    service_id: "TL-12",
                },
                ServiceWindow {
                    start_sec: 27600,
                    end_sec: 57000,
                    headway_sec: 600,
                    service_id: "TL-13",
                },
            ],
            _ => vec![],
        };

        for window in windows {
            let mut current_start = window.start_sec;
            while current_start <= window.end_sec {
                let mut trip = template.clone();
                let trip_id = format!("{}-{}-{}", route_id, window.service_id, current_start);

                trip.id = trip_id.clone();
                trip.service_id = window.service_id.to_string();
                trip.frequencies.clear();

                let time_shift = current_start as i32 - anchor_offset as i32;

                for st in trip.stop_times.iter_mut() {
                    // Shift arrival and departure, ensuring they are never negative
                    st.arrival_time = st
                        .arrival_time
                        .map(|t| (t as i32 + time_shift).max(0) as u32);
                    st.departure_time = st.arrival_time;
                }

                new_trips.insert(trip_id, trip);
                current_start += window.headway_sec;
            }
        }
    }

    gtfs.trips = new_trips;
    gtfs
}
