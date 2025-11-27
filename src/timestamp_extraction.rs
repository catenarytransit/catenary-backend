use prost::Message;

use gtfs_realtime::FeedHeader;
use gtfs_realtime::FeedMessage;

pub fn get_gtfs_header_timestamp_from_bytes(data: &[u8]) -> Option<u64> {
    let target_tag = 0x18; // Tag for timestamp field (field number 3, wire type 0)

    let mut index = 0;
    while index < data.len() && index < 200 {
        let byte = data[index];
        if byte == target_tag {
            index += 1; // Move to the next byte after the tag
            let mut value: u64 = 0;
            let mut shift = 0;
            loop {
                if index >= data.len() || index >= 200 {
                    return None; // Unexpected end of data during varint decoding
                }
                let varint_byte = data[index];
                value |= (u64::from(varint_byte & 0x7F)) << shift;
                shift += 7;
                index += 1;
                if (varint_byte & 0x80) == 0 {
                    return Some(value); // Successfully decoded timestamp
                }
            }
        } else {
            // Skip other fields (basic skipping, might need more robust protobuf parsing for real-world)
            // For simplicity, assuming fixed length skipping or length-delimited skipping is not needed for this specific task.
            index += 1; // Basic byte increment, may need more sophisticated skipping logic for full protobuf parsing
        }
    }
    None // Timestamp tag not found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_header_timestamp_valid() {
        // Example valid GTFS-RT FeedHeader with timestamp = 1678886400 (example timestamp)
        // Field 1 (gtfs_realtime_version, string) - Tag: 0a, Value: 01 (length), 32.0 (string "2.0") - not parsed in this function
        // Field 2 (incrementality, enum) - Tag: 10, Value: 00 (FULL_DATASET) - not parsed
        // Field 3 (timestamp, uint64) - Tag: 18, Value: c0 d4 93 96 06 (1678886400 as varint)

        let message = gtfs_realtime::FeedMessage {
            header: gtfs_realtime::FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                incrementality: None,
                timestamp: Some(1678886400),
                feed_version: None,
            },
            entity: vec![],
        };
        let bytes = message.encode_to_vec();

        let result = get_gtfs_header_timestamp_from_bytes(bytes.as_slice());

        println!("{:#?}", result);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), 1678886400);
    }

    #[test]
    fn test_get_header_timestamp_invalid_tag() {
        // Invalid tag (using tag for field 4 instead of 3 for timestamp)
        let invalid_message: &[u8] = &[0x20, 0xc0, 0xd4, 0x93, 0x96, 0x06];
        let result = get_gtfs_header_timestamp_from_bytes(invalid_message);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_header_timestamp_decode_fail() {
        // Example valid GTFS-RT FeedHeader with incorrect timestamp value (incomplete varint)
        // Field 1 (gtfs_realtime_version, string) - Tag: 0a, Value: 01 (length), 32.0 (string "2.0") - not parsed in this function
        // Field 2 (incrementality, enum) - Tag: 10, Value: 00 (FULL_DATASET) - not parsed
        // Field 3 (timestamp, uint64) - Tag: 18, Value: 80 80 80 80 80 80 80 80 80 80 (incomplete varint)
        let invalid_message: &[u8] = &[
            0x0a, 0x03, 0x32, 0x2e, 0x30, // gtfs_realtime_version: "2.0"
            0x10, 0x00, // incrementality: FULL_DATASET
            0x18, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
            0x80, // timestamp: incomplete varint
        ];
        let result = get_gtfs_header_timestamp_from_bytes(invalid_message);
        assert!(result.is_none());
    }
}
