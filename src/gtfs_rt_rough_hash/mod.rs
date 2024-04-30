#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughFeedEntity {
    pub is_deleted: Option<bool>,
    pub trip_update: Option<RoughTripUpdate>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughTripUpdate {
    pub trip: Option<RoughTripDescriptor>,
    pub vehicle: Option<RoughVehicleDescriptor>,
    pub stop_time_update: Option<Vec<RoughStopTimeUpdate>>,
    pub delay: Option<i32>,
    pub trip_properties: Option<RoughTripProperties>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughTripDescriptor {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub direction_id: Option<u32>,
    pub start_time: Option<String>,
    pub start_date: Option<String>,
    pub schedule_relationship: Option<i32>,
    pub modified_trip: Option<RoughModifiedTripSelector>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RoughVehicleDescriptor {
    pub id: Option<String>,
    pub label: Option<String>,
    pub license_plate: Option<String>,
    pub wheelchair_accessible: Option<i32>,
}

pub struct RoughModifiedTripSelector {
    pub modifications_id: Option<String>,
    pub affected_trip_id: Option<String>,
}

pub fn rough_hash_of_gtfs_rt(input: &gtfs_rt::FeedEntity) -> u64 {
    0
}