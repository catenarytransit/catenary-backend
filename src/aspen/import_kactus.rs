pub async fn new_rt_kactus(realtime_feed_id: String, data: Vec<u8>) -> bool {

    //decode Vec<u8> into gtfs_rt::FeedMessage

    let decoded_gtfs = kactus::parse_protobuf_message(data.as_slice());

    true
}