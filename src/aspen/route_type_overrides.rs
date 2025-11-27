use catenary::aspen_dataset::AspenisedVehiclePosition;
use gtfs_realtime::VehiclePosition;

pub fn apply_route_type_overrides(
    realtime_feed_id: &str,
    vehicle_pos: &VehiclePosition,
    pos_aspenised: &mut AspenisedVehiclePosition,
) {
    if realtime_feed_id == "f-metro~losangeles~rail~rt" {
        if vehicle_pos.trip.is_none() {
            if let Some(vehicle_info) = vehicle_pos.vehicle.as_ref() {
                if let Some(vehicle_id) = &vehicle_info.id {
                    match vehicle_id.contains("-") {
                        true => {
                            let split = vehicle_id.split('-').collect::<Vec<&str>>();

                            let attempted_number = split[0].parse::<u32>();

                            if let Ok(attempted_number) = attempted_number {
                                let new_route_type = match attempted_number {
                                    //Siemens P2000
                                    201..=250 => 0,
                                    301..=302 => 0,
                                    //AnsaldoBreda P2550
                                    701..=750 => 0,
                                    1001..=1235 => 0,
                                    //Breda A650
                                    500..=699 => 1,
                                    4000..=4064 => 1,
                                    _ => 0,
                                };

                                pos_aspenised.route_type = new_route_type;
                            }
                        }
                        false => {
                            let attempted_number = vehicle_id.parse::<u32>();

                            if let Ok(attempted_number) = attempted_number {
                                let new_route_type = match attempted_number {
                                    //A Line
                                    100..=199 => 0,
                                    //B & D Lines
                                    200..=299 => 1,
                                    //E Line
                                    400..=499 => 0,
                                    //K Line
                                    700..=799 => 0,
                                    //C Line
                                    300..=399 => 0,
                                    //700
                                    _ => 0,
                                };

                                pos_aspenised.route_type = new_route_type;
                            }
                        }
                    }
                }
            }
        }
    }

    if realtime_feed_id == "f-northcountrytransitdistrict~rt" {
        if let Some(vehicle_info) = vehicle_pos.vehicle.as_ref() {
            if let Some(vehicle_id) = &vehicle_info.id {
                if vehicle_id.contains("CSTR") {
                    pos_aspenised.route_type = 2;
                }

                if vehicle_id.contains("SPR") {
                    pos_aspenised.route_type = 0;
                }
            }
        }
    }
}

//Assisted-by: Gemini 3 via Google Antigravity
