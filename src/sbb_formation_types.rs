use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SbbFormationApiResponse {
    Data(SbbFormationData),
    Error { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SbbFormationData {
    pub vehicle_journey_type: Option<String>,
    #[serde(default)]
    pub formations: Vec<SbbFormation>,
    #[serde(default)]
    pub formations_at_scheduled_stops: Vec<SbbFormationAtScheduledStop>,
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
