use crate::import_alpenrose::MetrolinkPos;
use ahash::AHashMap;
use catenary::aspen_dataset::*;
use compact_str::CompactString;

// Metrolink sometimes reports a fixed off-railway coordinate instead of a
// real vehicle position. Match both f32 coordinates exactly, not a radius.
fn is_invalid_metrolink_position(latitude: f32, longitude: f32) -> bool {
    latitude == 33.5539 && longitude == -117.675
}

pub fn vehicle_pos_supplement(
    pos_aspenised: AspenisedVehiclePosition,
    fetch_supplemental_data_positions_metrolink: &Option<AHashMap<CompactString, MetrolinkPos>>,
    chateau_id: &str,
) -> AspenisedVehiclePosition {
    let mut pos_aspenised = pos_aspenised;

    if chateau_id == "metrolinktrains"
        && pos_aspenised.position.as_ref().is_some_and(|position| {
            is_invalid_metrolink_position(position.latitude, position.longitude)
        })
    {
        // Keep the vehicle and its trip metadata; only discard the bad location.
        pos_aspenised.position = None;
    }

    match chateau_id {
        "metrolinktrains" => match fetch_supplemental_data_positions_metrolink {
            Some(supp_metrolink_data) => {
                if let Some(vehicle_ids) = &pos_aspenised.vehicle {
                    if let Some(vehicle_id) = &vehicle_ids.id {
                        if let Some(metrolink_pos) = supp_metrolink_data
                            .get(vehicle_id.as_str())
                            .filter(|position| {
                                !is_invalid_metrolink_position(position.lat, position.lon)
                            })
                        {
                            if pos_aspenised.position.is_none() {
                                println!("Set new pos for Metrolink {}", vehicle_id);
                                pos_aspenised.position = Some(CatenaryRtVehiclePosition {
                                    latitude: metrolink_pos.lat,
                                    longitude: metrolink_pos.lon,
                                    bearing: None,
                                    odometer: None,
                                    speed: Some(metrolink_pos.speed),
                                });
                            }
                        }
                    }
                }
            }
            None => {}
        },
        _ => {}
    }

    pos_aspenised
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(latitude: f32, longitude: f32) -> CatenaryRtVehiclePosition {
        CatenaryRtVehiclePosition {
            latitude,
            longitude,
            bearing: None,
            odometer: None,
            speed: Some(0.0),
        }
    }

    fn vehicle(position: Option<CatenaryRtVehiclePosition>) -> AspenisedVehiclePosition {
        AspenisedVehiclePosition {
            trip: None,
            vehicle: Some(AspenisedVehicleDescriptor {
                id: Some("M100".to_string()),
                label: Some("100".to_string()),
                license_plate: None,
                wheelchair_accessible: None,
            }),
            position,
            timestamp: Some(123),
            route_type: 2,
            current_stop_sequence: None,
            current_status: None,
            congestion_level: None,
            occupancy_status: None,
            occupancy_percentage: None,
            consist: None,
        }
    }

    fn supplemental(lat: f32, lon: f32) -> Option<AHashMap<CompactString, MetrolinkPos>> {
        let symbol = CompactString::new("M100");
        let mut positions = AHashMap::new();
        positions.insert(
            symbol.clone(),
            MetrolinkPos {
                lat,
                lon,
                speed: 4.0,
                symbol,
            },
        );
        Some(positions)
    }

    #[test]
    fn metrolink_discards_exact_invalid_primary_position() {
        let result = vehicle_pos_supplement(
            vehicle(Some(position(33.5539, -117.675))),
            &None,
            "metrolinktrains",
        );
        assert!(result.position.is_none());
        assert_eq!(result.timestamp, Some(123));
        assert_eq!(result.vehicle.unwrap().id.as_deref(), Some("M100"));
    }

    #[test]
    fn metrolink_discards_exact_invalid_supplemental_position() {
        for primary in [None, Some(position(33.5539, -117.675))] {
            let result = vehicle_pos_supplement(
                vehicle(primary),
                &supplemental(33.5539, -117.675),
                "metrolinktrains",
            );
            assert!(result.position.is_none());
        }
    }

    #[test]
    fn metrolink_can_replace_invalid_primary_with_valid_supplemental_position() {
        let result = vehicle_pos_supplement(
            vehicle(Some(position(33.5539, -117.675))),
            &supplemental(33.5540, -117.675),
            "metrolinktrains",
        );
        let pos = result.position.unwrap();
        assert_eq!(pos.latitude, 33.5540);
        assert_eq!(pos.longitude, -117.675);
        assert_eq!(pos.speed, Some(4.0));
    }

    #[test]
    fn metrolink_keeps_nearby_primary_positions() {
        for (latitude, longitude) in [(33.5540, -117.675), (33.5539, -117.6751)] {
            let result = vehicle_pos_supplement(
                vehicle(Some(position(latitude, longitude))),
                &supplemental(33.5539, -117.675),
                "metrolinktrains",
            );
            let pos = result.position.unwrap();
            assert_eq!(pos.latitude, latitude);
            assert_eq!(pos.longitude, longitude);
        }
    }

    #[test]
    fn other_chateaux_keep_the_same_coordinate() {
        let result = vehicle_pos_supplement(
            vehicle(Some(position(33.5539, -117.675))),
            &None,
            "amtrak",
        );
        let pos = result.position.unwrap();
        assert_eq!(pos.latitude, 33.5539);
        assert_eq!(pos.longitude, -117.675);
    }
}
