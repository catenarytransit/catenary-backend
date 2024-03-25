pub fn parse_protobuf_message(
    bytes: &[u8],
) -> Result<gtfs_rt::FeedMessage, Box<dyn std::error::Error>> {
    match prost::Message::decode(bytes) {
        Ok(x) => Ok(x),
        Err(x) => Err(Box::new(x)),
    }
}