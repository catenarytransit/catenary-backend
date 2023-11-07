const MAKE_VEHICLES_FEED_LIST: [&str; 9] = [
    "f-mta~nyc~rt~subway~1~2~3~4~5~6~7",
    "f-mta~nyc~rt~subway~a~c~e",
    "f-mta~nyc~rt~subway~b~d~f~m",
    "f-mta~nyc~rt~subway~g",
    "f-mta~nyc~rt~subway~j~z",
    "f-mta~nyc~rt~subway~l",
    "f-mta~nyc~rt~subway~n~q~r~w",
    "f-mta~nyc~rt~subway~sir",
    "f-bart~rt",
];

pub async fn new_rt_kactus(
    realtime_feed_id: String,
    vehicles: Option<Vec<u8>>,
    trips: Option<Vec<u8>>,
    alerts: Option<Vec<u8>>,
) -> bool {
    //decode Vec<u8> into gtfs_rt::FeedMessage

    if (vehicles.is_some()) {
        let decoded_gtfs_vehicles =
            kactus::parse_protobuf_message(vehicles.as_ref().unwrap().as_slice());

        if decoded_gtfs_vehicles.is_ok() {
            if MAKE_VEHICLES_FEED_LIST.contains(&realtime_feed_id.as_str()) {
                //make a vehicles feed

                //get gtfs data from postgres?
            }
        } else {
        }
    }

    true
}
