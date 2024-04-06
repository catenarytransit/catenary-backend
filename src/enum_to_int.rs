use gtfs_structures::BikesAllowedType;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::LocationType;
use gtfs_structures::RouteType;
use gtfs_structures::TimepointType;

pub fn location_type_conversion(input: &LocationType) -> i16 {
    match input {
        LocationType::StopPoint => 0,
        LocationType::StopArea => 1,
        LocationType::StationEntrance => 2,
        LocationType::GenericNode => 3,
        LocationType::BoardingArea => 4,
        LocationType::Unknown(i) => *i,
    }
}

pub fn route_type_to_int(input: &RouteType) -> i16 {
    match input {
        RouteType::Tramway => 0,
        RouteType::Subway => 1,
        RouteType::Rail => 2,
        RouteType::Bus => 3,
        RouteType::Ferry => 4,
        RouteType::CableCar => 5,
        RouteType::Gondola => 6,
        RouteType::Funicular => 7,
        RouteType::Coach => 200,
        RouteType::Air => 1100,
        RouteType::Taxi => 1500,
        RouteType::Other(i) => *i,
    }
}

pub fn availability_to_int(input: &gtfs_structures::Availability) -> i16 {
    match input {
        gtfs_structures::Availability::Available => 1,
        gtfs_structures::Availability::NotAvailable => 2,
        gtfs_structures::Availability::Unknown(unknown) => *unknown,
        gtfs_structures::Availability::InformationNotAvailable => 0,
    }
}

pub fn timepoint_to_bool(timepoint: &TimepointType) -> bool {
    match timepoint {
        TimepointType::Exact => true,
        TimepointType::Approximate => false,
    }
}

pub fn pickup_dropoff_to_i16(x: &gtfs_structures::PickupDropOffType) -> i16 {
    match x {
        gtfs_structures::PickupDropOffType::Regular => 0,
        gtfs_structures::PickupDropOffType::NotAvailable => 1,
        gtfs_structures::PickupDropOffType::ArrangeByPhone => 2,
        gtfs_structures::PickupDropOffType::CoordinateWithDriver => 3,
        gtfs_structures::PickupDropOffType::Unknown(x) => *x,
    }
}

pub fn continuous_pickup_drop_off_to_i16(x: &ContinuousPickupDropOff) -> i16 {
    match x {
        ContinuousPickupDropOff::Continuous => 0,
        ContinuousPickupDropOff::NotAvailable => 1,
        ContinuousPickupDropOff::ArrangeByPhone => 2,
        ContinuousPickupDropOff::CoordinateWithDriver => 3,
        ContinuousPickupDropOff::Unknown(x) => *x,
    }
}

pub fn bikes_allowed_to_int(bikes_allowed: &BikesAllowedType) -> i16 {
    match bikes_allowed {
        BikesAllowedType::NoBikeInfo => 0,
        BikesAllowedType::AtLeastOneBike => 1,
        BikesAllowedType::NoBikesAllowed => 2,
        BikesAllowedType::Unknown(unknown) => *unknown,
    }
}
