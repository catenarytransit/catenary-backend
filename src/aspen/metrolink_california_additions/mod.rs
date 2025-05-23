use crate::import_alpenrose::MetrolinkPos;
use ahash::AHashMap;
use catenary::aspen_dataset::*;
use compact_str::CompactString;

pub fn vehicle_pos_supplement(
    pos_aspenised: AspenisedVehiclePosition,
    fetch_supplemental_data_positions_metrolink: &Option<AHashMap<CompactString, MetrolinkPos>>,
    chateau_id: &str,
) -> AspenisedVehiclePosition {
    let mut pos_aspenised = pos_aspenised;

    match chateau_id {
        "metrolinktrains" => match fetch_supplemental_data_positions_metrolink {
            Some(supp_metrolink_data) => {
                if let Some(vehicle_ids) = &pos_aspenised.vehicle {
                    if let Some(vehicle_id) = &vehicle_ids.id {
                        if let Some(metrolink_pos) = supp_metrolink_data.get(vehicle_id.as_str()) {
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
