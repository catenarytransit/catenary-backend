use ecow::EcoString;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UnifiedConsist {
    /// SIRI: CompoundTrainRef | DB: journeyID
    pub global_journey_id: EcoString,

    /// Handles coupled units. If len > 1, the train is "Joined".
    /// Each group can have a different destination (Splitting support).
    pub groups: Vec<ConsistGroup>,

    /// DB: sequenceStatus (e.g., "MATCHES_SCHEDULE", "DIFFERENT_REAR_LOAD")
    pub formation_status: FormationStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConsistGroup {
    /// DB: name (e.g., "ICE9472") | SIRI: TrainBlockRef
    pub group_name: Option<EcoString>,

    /// DB: transport.destination | SIRI: DestinationDisplay
    /// Crucial for splitting trains: which car goes where?
    pub destination: Option<EcoString>,

    /// The physical carriages in this specific unit
    pub vehicles: Vec<VehicleElement>,

    /// DB: orientation (FORWARDS / BACKWARDS)
    pub group_orientation: Option<Orientation>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VehicleElement {
    /// DB: vehicleID (UIC) | SBB: carUic
    pub uic_number: EcoString,

    /// DB: wagonIdentificationNumber | SBB: number
    /// The "Label" on the car for the passenger (e.g., "Car 14")
    pub label: Option<EcoString>,

    pub order: u8,

    /// DB: platformPosition | SBB: Sektor
    /// Unifies meter-based offsets and alphabetical sectors.
    pub position_on_platform: Option<PlatformSpatialPosition>,

    pub facilities: Vec<Amenity>,
    pub occupancy: Option<SiriOccupancy>,
    pub passenger_count: Option<u32>,
    pub passenger_class: Option<PassengerClass>,
    pub is_locomotive: Option<bool>,
    pub is_revenue: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PlatformSpatialPosition {
    /// DB: platformPosition.sector | SBB: name (e.g., "A")
    pub sector: Option<EcoString>,

    /// DB: start/end (In meters from the platform start)
    /// Allows high-precision "Where do I stand?" UI features.
    pub start_meters: Option<f32>,
    pub end_meters: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Amenity {
    pub amenity_type: AmenityType,
    pub status: AmenityStatus,
    /// DB: amount (e.g., number of bike spaces available)
    pub count: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AmenityStatus {
    Available,
    NotAvailable,
    Unknown,
    /// DB: status "UNDEFINED" or "RESTRICTED"
    Restricted,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AmenityType {
    AirCondition,
    WheelchairSpace,
    BikeSpace,
    QuietZone,
    FamilyZone,
    InfoPoint,
    DiningCar,
    Toilet,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AspenisedPlatformInfo {
    /// SIRI: AimedQuayName | DB: departurePlatformSchedule
    pub aimed: Option<EcoString>,

    /// SIRI: ExpectedQuayName | DB: departurePlatform
    pub expected: Option<EcoString>,

    /// Full sector map of the platform (DB: platform.sectors)
    /// Used to draw the platform in the UI independently of the train.
    pub platform_sectors: Vec<PlatformSectorDefinition>,

    pub is_changed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlatformSectorDefinition {
    pub label: EcoString, // "A", "B", etc.
    pub start_meters: f32,
    pub end_meters: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FormationStatus {
    MatchesSchedule,
    DifferentRearLoad,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Orientation {
    Forwards,
    Backwards,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PassengerClass {
    First,
    Second,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SiriOccupancy {
    Low,
    Medium,
    High,
    Empty,
    StandingRoomOnly,
    Full,
    NotAcceptingPassengers,
    Unknown,
}
