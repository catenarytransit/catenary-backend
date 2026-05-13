/// Represents a distinct temporal segment of the transit schedule.
struct ServiceWindow {
    start_sec: u32,
    end_sec: u32,
    headway_sec: u32,
    service_id: &'static str,
}

use gtfs_structures::Gtfs;
use std::collections::HashMap;

pub fn redo_anteater_express_gtfs(gtfs: Gtfs) -> Gtfs {
    let mut gtfs = gtfs;

    // 1. Identify and extract templates BEFORE clearing gtfs.trips
    let mut templates = HashMap::new();
    let route_configs = [
        ("A Line", "107"), //
        ("E Line", "100"), //
        ("H Line", "100"), //
        ("M Line", "100"), //
        ("N Line", "107"), //
    ];

    for (line_name, primary_stop_id) in route_configs {
        if let Some(route) = gtfs.routes.values().find(|r| {
            r.long_name
                .as_deref()
                .unwrap_or("")
                .eq_ignore_ascii_case(line_name)
        }) {
            // Find the trip with the most stops to ensure we have the full loop sequence
            if let Some(template) = gtfs
                .trips
                .values()
                .filter(|t| t.route_id == route.id)
                .max_by_key(|t| t.stop_times.len())
            {
                templates.insert(
                    line_name,
                    (route.id.clone(), template.clone(), primary_stop_id),
                );
            }
        }
    }

    // 2. Now it is safe to clear the existing trips
    gtfs.trips.clear();
    let mut new_trips = HashMap::new();

    for (line_name, (route_id, template, primary_stop_id)) in templates {
        // Calculate the internal time offset of the University Centre anchor in the template
        let anchor_offset = template
            .stop_times
            .iter()
            .find(|st| st.stop.id == primary_stop_id)
            .and_then(|st| st.arrival_time.or(st.departure_time))
            .unwrap_or(0);

        let windows = match line_name {
            "A Line" => vec![
                ServiceWindow {
                    start_sec: 27480,
                    end_sec: 37980,
                    headway_sec: 480,
                    service_id: "TL-12",
                }, // 7:38-10:33 (8m)
                ServiceWindow {
                    start_sec: 37980,
                    end_sec: 67980,
                    headway_sec: 780,
                    service_id: "TL-12",
                }, // 10:33-18:53 (13m)
                ServiceWindow {
                    start_sec: 27480,
                    end_sec: 37980,
                    headway_sec: 480,
                    service_id: "TL-13",
                }, // Fri 8m
                ServiceWindow {
                    start_sec: 37980,
                    end_sec: 56700,
                    headway_sec: 780,
                    service_id: "TL-13",
                }, // Fri 13m ends 15:45
            ],
            "M Line" => vec![
                ServiceWindow {
                    start_sec: 27900,
                    end_sec: 67380,
                    headway_sec: 480,
                    service_id: "TL-12",
                }, // 7:45-18:43 (8m)
                ServiceWindow {
                    start_sec: 67380,
                    end_sec: 71700,
                    headway_sec: 780,
                    service_id: "TL-12",
                }, // 18:43-19:55 (13m)
                ServiceWindow {
                    start_sec: 71700,
                    end_sec: 81000,
                    headway_sec: 1500,
                    service_id: "TL-12",
                }, // 19:55-22:30 (25m)
                ServiceWindow {
                    start_sec: 27900,
                    end_sec: 57900,
                    headway_sec: 480,
                    service_id: "TL-13",
                }, // Fri 8m
                ServiceWindow {
                    start_sec: 57900,
                    end_sec: 70200,
                    headway_sec: 1500,
                    service_id: "TL-13",
                }, // Fri 25m ends 19:30
            ],
            "N Line" => vec![
                ServiceWindow {
                    start_sec: 27300,
                    end_sec: 57300,
                    headway_sec: 420,
                    service_id: "TL-12",
                }, // 7:35-15:55 (7m)
                ServiceWindow {
                    start_sec: 57300,
                    end_sec: 68100,
                    headway_sec: 600,
                    service_id: "TL-12",
                }, // 15:55-18:55 (10m)
                ServiceWindow {
                    start_sec: 27300,
                    end_sec: 56880,
                    headway_sec: 420,
                    service_id: "TL-13",
                }, // Fri 7m ends 15:48
            ],
            "H Line" => vec![
                ServiceWindow {
                    start_sec: 68400,
                    end_sec: 81000,
                    headway_sec: 600,
                    service_id: "TL-12",
                }, // 19:00-22:30 (10m)
                ServiceWindow {
                    start_sec: 57600,
                    end_sec: 70200,
                    headway_sec: 600,
                    service_id: "TL-13",
                }, // Fri 16:00-19:30 (10m)
            ],
            "E Line" => vec![
                ServiceWindow {
                    start_sec: 27600,
                    end_sec: 67800,
                    headway_sec: 600,
                    service_id: "TL-12",
                }, // 7:40-18:50 (10m)
                ServiceWindow {
                    start_sec: 27600,
                    end_sec: 57000,
                    headway_sec: 600,
                    service_id: "TL-13",
                }, // Fri 10m ends 15:50
            ],
            _ => vec![],
        };

        for window in windows {
            let mut current_start = window.start_sec;
            while current_start <= window.end_sec {
                let mut trip = template.clone();

                trip.frequencies = vec![];

                // Constant Trip ID format: {route_id}-{service_id}-{start_seconds}
                let trip_id = format!("{}-{}-{}", route_id, window.service_id, current_start);

                trip.id = trip_id.clone();
                trip.service_id = window.service_id.to_string();

                // Shift all stop times relative to the University Centre scheduled departure
                let time_shift = current_start as i32 - anchor_offset as i32;

                for (idx, st) in trip.stop_times.iter_mut().enumerate() {
                    st.arrival_time = st.arrival_time.map(|t| (t as i32 + time_shift) as u32);
                    st.departure_time = st.departure_time.map(|t| (t as i32 + time_shift) as u32);

                    // Headsign maintenance based on stop sequence position
                    st.stop_headsign = match line_name {
                        "E Line" => Some(String::from(if idx < 2 {
                            "Plaza Verde"
                        } else {
                            "University Centre South"
                        })),
                        "M Line" => Some(String::from(match idx {
                            0..=2 => "East Housing -> Petalson",
                            3..=6 => "Petalson -> University Centre",
                            _ => "University Centre",
                        })),
                        "N Line" => Some(String::from(if idx == 0 {
                            "Vista del Campo Norte"
                        } else {
                            "University Centre"
                        })),
                        "A Line" | "H Line" => Some(String::from(match idx {
                            0..=2 => "AV & CDS & VDC",
                            3..=7 if line_name == "H Line" => "VDC -> University Centre",
                            _ => "University Centre",
                        })),
                        _ => None,
                    };
                }
                new_trips.insert(trip_id, trip);
                current_start += window.headway_sec;
            }
        }
    }

    gtfs.trips = new_trips;

    gtfs
}
