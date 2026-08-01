use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SbbFormationApiResponse {
    Error { error: String },
    Data(SbbFormationData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationData {
    pub vehicle_journey_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formations: Option<Vec<SbbFormation>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formations_at_scheduled_stops: Option<Vec<SbbFormationAtScheduledStop>>,
    #[serde(default)]
    pub hints: Vec<serde_json::Value>,
    pub journey_meta_information: Option<SbbJourneyMetaInformation>,
    pub last_update: Option<String>,
    #[serde(default)]
    pub relationships: Vec<serde_json::Value>,
    pub train_meta_information: Option<SbbTrainMetaInformation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormation {
    #[serde(default)]
    pub formation_vehicles: Vec<SbbFormationVehicle>,
    pub meta_information: Option<SbbFormationMetaInformation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationVehicle {
    #[serde(default)]
    pub formation_vehicle_at_scheduled_stops: Vec<SbbFormationVehicleAtScheduledStop>,
    pub number: Option<u32>,
    pub position: Option<u32>,
    pub vehicle_identifier: Option<SbbVehicleIdentifier>,
    pub vehicle_properties: Option<SbbVehicleProperties>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationVehicleAtScheduledStop {
    pub access_to_previous_vehicle: Option<bool>,
    pub sectors: Option<String>,
    pub stop_point: Option<SbbStopPoint>,
    pub stop_time: Option<SbbStopTime>,
    pub track: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbStopPoint {
    pub name: Option<String>,
    pub uic: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbStopTime {
    pub arrival_time: Option<String>,
    pub departure_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbVehicleIdentifier {
    pub build_type_code: Option<String>,
    pub check_number: Option<String>,
    pub country_code: Option<String>,
    pub evn: Option<String>,
    pub parent_evn: Option<String>,
    pub type_code: Option<u32>,
    pub type_code_name: Option<String>,
    pub vehicle_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbVehicleProperties {
    pub accessibility_properties: Option<SbbAccessibilityProperties>,
    pub bike_platform: Option<bool>,
    pub climated: Option<bool>,
    pub closed: Option<bool>,
    pub emergency_call_system: Option<bool>,
    pub from_stop: Option<SbbStopPoint>,
    pub length: Option<f64>,
    pub low_floor_trolley: Option<bool>,
    pub number1class: Option<u32>,
    pub number2class: Option<u32>,
    pub number_beds: Option<u32>,
    pub number_bike_hooks: Option<u32>,
    pub number_restaurant_space: Option<u32>,
    pub picto_properties: Option<SbbPictoProperties>,
    pub to_stop: Option<SbbStopPoint>,
    pub trolley_status: Option<String>,
    pub vehicle_relation: Option<SbbVehicleRelation>,
    pub vehicle_will_be_put_away: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbAccessibilityProperties {
    pub disabled_compartment: Option<bool>,
    pub number_wheelchair_spaces: Option<u32>,
    pub number_wheelchair_spaces1class: Option<u32>,
    pub number_wheelchair_spaces2class: Option<u32>,
    pub wheelchair_accessible_restaurant: Option<bool>,
    pub wheelchair_symbol_properties: Option<SbbWheelchairSymbolProperties>,
    pub wheelchair_toilet: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbWheelchairSymbolProperties {
    pub folding_ramp: Option<bool>,
    pub gap_bridging: Option<bool>,
    pub height_boarding_platform: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbPictoProperties {
    pub bike_picto: Option<bool>,
    pub business_zone_picto: Option<bool>,
    pub family_zone_picto: Option<bool>,
    pub stroller_picto: Option<bool>,
    pub wheelchair_picto: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbVehicleRelation {
    #[serde(default)]
    pub direct_trolleys: Vec<serde_json::Value>,
    pub next_vehicle_journey: Option<SbbVehicleJourneyInfo>,
    pub previous_vehicle_journey: Option<SbbVehicleJourneyInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbVehicleJourneyInfo {
    pub journey_meta_information: Option<SbbJourneyMetaInformation>,
    pub train_meta_information: Option<SbbTrainMetaInformation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbJourneyMetaInformation {
    #[serde(rename = "SJYID")]
    pub sjyid: Option<String>,
    pub operation_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbTrainMetaInformation {
    pub train_number: Option<u64>,
    pub to_code: Option<String>,
    pub runs: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationMetaInformation {
    pub length: Option<f64>,
    pub number_axis: Option<u32>,
    pub number_seats: Option<u32>,
    pub number_vehicles: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationAtScheduledStop {
    pub formation_short: Option<SbbFormationShort>,
    pub scheduled_stop: Option<SbbScheduledStop>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationShort {
    pub formation_short_string: Option<String>,
    #[serde(default)]
    pub vehicle_goals: Vec<SbbVehicleGoal>,
}

impl SbbFormationShort {
    /// Parse the compact CUS formation string returned by the stop-based endpoint.
    ///
    /// The parser is intentionally forgiving. Unknown vehicle and service codes are retained,
    /// while malformed fragments are skipped instead of rejecting the complete API response.
    pub fn parsed_vehicles(&self) -> Vec<SbbFormationShortVehicle> {
        self.formation_short_string
            .as_deref()
            .map(parse_sbb_formation_short)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationShortStatus {
    pub closed: bool,
    pub groups_start_here: bool,
    pub reserved_for_groups: bool,
    pub open_but_not_served: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationShortVehicle {
    pub vehicle_type: String,
    pub ordinal_number: Option<u32>,
    #[serde(default)]
    pub services: Vec<String>,
    pub sector: Option<String>,
    pub belongs_to_train: bool,
    pub status: SbbFormationShortStatus,
    pub no_access_to_previous_vehicle: bool,
    pub no_access_to_next_vehicle: bool,
}

fn is_formation_short_status(character: char) -> bool {
    matches!(character, '-' | '>' | '=' | '%')
}

fn is_formation_short_delimiter(character: char) -> bool {
    matches!(character, ':' | '#' | ',' | '@' | '[' | ']' | '(' | ')')
}

pub fn parse_sbb_formation_short(value: &str) -> Vec<SbbFormationShortVehicle> {
    let source = value.trim();
    if source.is_empty() {
        return Vec::new();
    }

    let characters = source.chars().collect::<Vec<_>>();
    let has_train_group_markers = source.contains('[') || source.contains(']');
    let mut belongs_to_train = !has_train_group_markers;
    let mut current_sector: Option<String> = None;
    let mut index = 0;
    let mut vehicles = Vec::new();

    while index < characters.len() {
        while index < characters.len()
            && (characters[index].is_whitespace() || characters[index] == ',')
        {
            index += 1;
        }
        if index >= characters.len() {
            break;
        }

        match characters[index] {
            '@' => {
                index += 1;
                let sector_start = index;
                while index < characters.len() && characters[index].is_ascii_alphabetic() {
                    index += 1;
                }
                let sector = characters[sector_start..index]
                    .iter()
                    .collect::<String>()
                    .trim()
                    .to_string();
                current_sector = (!sector.is_empty()).then_some(sector);
                continue;
            }
            '[' => {
                belongs_to_train = true;
                index += 1;
                continue;
            }
            ']' => {
                belongs_to_train = false;
                index += 1;
                continue;
            }
            _ => {}
        }

        let mut status = SbbFormationShortStatus::default();
        while index < characters.len() && is_formation_short_status(characters[index]) {
            match characters[index] {
                '-' => status.closed = true,
                '>' => status.groups_start_here = true,
                '=' => status.reserved_for_groups = true,
                '%' => status.open_but_not_served = true,
                _ => {}
            }
            index += 1;
        }

        let no_access_to_previous_vehicle = characters.get(index) == Some(&'(');
        if no_access_to_previous_vehicle {
            index += 1;
        }

        let type_start = index;
        while index < characters.len()
            && !characters[index].is_whitespace()
            && !is_formation_short_delimiter(characters[index])
        {
            index += 1;
        }
        if type_start == index {
            index += 1;
            continue;
        }
        let vehicle_type = characters[type_start..index]
            .iter()
            .collect::<String>()
            .trim()
            .to_ascii_uppercase();

        let mut ordinal_number = None;
        if characters.get(index) == Some(&':') {
            index += 1;
            let number_start = index;
            while index < characters.len() && characters[index].is_ascii_digit() {
                index += 1;
            }
            ordinal_number = characters[number_start..index]
                .iter()
                .collect::<String>()
                .parse::<u32>()
                .ok();
        }

        let mut services = Vec::new();
        if characters.get(index) == Some(&'#') {
            index += 1;
            let services_start = index;
            while index < characters.len()
                && !matches!(characters[index], ',' | '@' | '[' | ']' | '(' | ')')
            {
                index += 1;
            }
            let service_string = characters[services_start..index].iter().collect::<String>();
            for service in service_string.split(';') {
                let service = service.trim().to_ascii_uppercase();
                if !service.is_empty() && !services.contains(&service) {
                    services.push(service);
                }
            }
        }

        let no_access_to_next_vehicle = characters.get(index) == Some(&')');
        if no_access_to_next_vehicle {
            index += 1;
        }

        vehicles.push(SbbFormationShortVehicle {
            vehicle_type,
            ordinal_number,
            services,
            sector: current_sector.clone(),
            belongs_to_train,
            status,
            no_access_to_previous_vehicle,
            no_access_to_next_vehicle,
        });
    }

    vehicles
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbVehicleGoal {
    pub destination_stop_point: Option<SbbStopPoint>,
    pub from_vehicle_at_position: Option<u32>,
    pub to_vehicle_at_position: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbScheduledStop {
    pub stop_modifications: Option<u32>,
    pub stop_point: Option<SbbStopPoint>,
    pub stop_time: Option<SbbStopTime>,
    pub stop_type: Option<String>,
    pub track: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::parse_sbb_formation_short;

    #[test]
    fn parses_stop_based_formation_with_sectors() {
        let vehicles = parse_sbb_formation_short(
            "@A,F,F@B,F,F,[(FA:7#VH;NF@C,2:6#NF,2:5#NF@D,2:4#NF@E,2:3#NF,1:2#NF@F,1:1#BHP;BZ;NF,LK)]@G,F,F,F",
        );

        assert_eq!(vehicles.len(), 15);
        let train = vehicles
            .iter()
            .filter(|vehicle| vehicle.belongs_to_train)
            .collect::<Vec<_>>();
        assert_eq!(
            train
                .iter()
                .map(|vehicle| vehicle.vehicle_type.as_str())
                .collect::<Vec<_>>(),
            vec!["FA", "2", "2", "2", "2", "1", "1", "LK"]
        );
        assert_eq!(train[0].sector.as_deref(), Some("B"));
        assert_eq!(train[1].sector.as_deref(), Some("C"));
        assert_eq!(train[7].sector.as_deref(), Some("F"));
        assert_eq!(train[0].services, vec!["VH", "NF"]);
        assert!(train[0].no_access_to_previous_vehicle);
        assert!(train[7].no_access_to_next_vehicle);
    }

    #[test]
    fn parses_stop_based_formation_without_sectors_and_combined_statuses() {
        let vehicles = parse_sbb_formation_short("[>=(12:1#BHP;NF,%WR:2#VR)]");

        assert_eq!(vehicles.len(), 2);
        assert!(vehicles.iter().all(|vehicle| vehicle.sector.is_none()));
        assert!(vehicles[0].status.groups_start_here);
        assert!(vehicles[0].status.reserved_for_groups);
        assert!(vehicles[0].no_access_to_previous_vehicle);
        assert!(vehicles[1].status.open_but_not_served);
        assert!(vehicles[1].no_access_to_next_vehicle);
        assert_eq!(vehicles[1].services, vec!["VR"]);
    }
}
