use ahash::AHashMap as HashMap;
use catenary::models::Calendar;
use catenary::routing_common::transit_graph::{
    CompressedTrip, DirectionPattern, TimeDeltaSequence, TripPattern,
};

pub struct ProcessedPattern {
    pub chateau: String,
    pub route_id: String,
    pub stop_indices: Vec<u32>,
    pub trips: Vec<CompressedTrip>,
    pub timezone_idx: u32,
}

pub fn calculate_service_mask(service_id: &str, calendar: &[Calendar]) -> u32 {
    if let Some(cal) = calendar.iter().find(|c| c.service_id == service_id) {
        let mut mask = 0;
        if cal.monday {
            mask |= 1 << 0;
        }
        if cal.tuesday {
            mask |= 1 << 1;
        }
        if cal.wednesday {
            mask |= 1 << 2;
        }
        if cal.thursday {
            mask |= 1 << 3;
        }
        if cal.friday {
            mask |= 1 << 4;
        }
        if cal.saturday {
            mask |= 1 << 5;
        }
        if cal.sunday {
            mask |= 1 << 6;
        }
        mask
    } else {
        0
    }
}

pub fn reindex_deltas(
    mut patterns: Vec<TripPattern>,
    global_deltas: &[TimeDeltaSequence],
    _direction_patterns: &[DirectionPattern],
) -> (Vec<TimeDeltaSequence>, Vec<TripPattern>) {
    let mut new_deltas = Vec::new();
    let mut delta_map = HashMap::new();

    for pat in &mut patterns {
        for trip in &mut pat.trips {
            let old_idx = trip.time_delta_idx;
            if let Some(&new_idx) = delta_map.get(&old_idx) {
                trip.time_delta_idx = new_idx;
            } else {
                let new_idx = new_deltas.len() as u32;
                // Just copy the sequence from global
                if (old_idx as usize) < global_deltas.len() {
                    new_deltas.push(global_deltas[old_idx as usize].clone());
                } else {
                    // Fallback or error? Should be valid.
                    new_deltas.push(TimeDeltaSequence { deltas: vec![] });
                }

                delta_map.insert(old_idx, new_idx);
                trip.time_delta_idx = new_idx;
            }
        }
    }
    (new_deltas, patterns)
}

pub fn lon_lat_to_tile(lon: f64, lat: f64, zoom: u8) -> (u32, u32) {
    let n = 2.0_f64.powi(zoom as i32);
    let x = ((lon + 180.0) / 360.0 * n).floor() as u32;
    let lat_rad = lat.to_radians();
    let y = ((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n).floor() as u32;
    (x, y)
}

pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371000.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}
