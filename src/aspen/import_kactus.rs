const MAKE_VEHICLES_FEED_LIST: [&str; 9] = ["f-mta~nyc~rt~subway~1~2~3~4~5~6~7",
"f-mta~nyc~rt~subway~a~c~e",
"f-mta~nyc~rt~subway~b~d~f~m",
"f-mta~nyc~rt~subway~g",
"f-mta~nyc~rt~subway~j~z",
"f-mta~nyc~rt~subway~l",
"f-mta~nyc~rt~subway~n~q~r~w",
"f-mta~nyc~rt~subway~sir",
"f-bart~rt"
];

pub async fn new_rt_kactus(realtime_feed_id: String, data: Vec<u8>) -> bool {

    //decode Vec<u8> into gtfs_rt::FeedMessage

    let decoded_gtfs = kactus::parse_protobuf_message(data.as_slice());

    if MAKE_VEHICLES_FEED_LIST.contains(&realtime_feed_id.as_str()) {
        //make a vehicles feed
        
        //get gtfs data from postgres?
    }

    true
}