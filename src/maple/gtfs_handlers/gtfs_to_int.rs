use gtfs_structures::LocationType;
use gtfs_structures::RouteType;

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
        RouteType::Other(i) => (*i),
    }
}

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

pub fn availability_to_int(input: &gtfs_structures::Availability) -> i16 {
    match input {
        gtfs_structures::Availability::Available => 1,
        gtfs_structures::Availability::NotAvailable => 2,
        gtfs_structures::Availability::Unknown(unknown) => *unknown,
        gtfs_structures::Availability::InformationNotAvailable => 0,
    }
}