//! Compile the actual Aspen data types and compression modules without OSR,
//! Postgres, protobuf generation or the server's other native dependencies.
//! Only the unused internal SQL cache payload types are replaced with unit types.
#[macro_use]
extern crate serde;

pub type RtCacheEntry = ();
pub type RtKey = ();
pub mod models {
    pub type ItineraryPatternMeta = ();
    pub type CompressedTrip = ();
}
#[path = "../../../src/compact_formats.rs"]
pub mod compact_formats;

#[path = "../../../src/aspen_dataset.rs"]
pub mod aspen_dataset;
#[path = "../../../src/compressed_aspen_dataset.rs"]
pub mod compressed_aspen_dataset;
#[path = "../../../src/formation_v1.rs"]
pub mod formation_v1;

pub fn bincode_serialize<T: serde::Serialize>(
    value: &T,
) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy())
}

pub fn bincode_deserialize<T: serde::de::DeserializeOwned>(
    value: &[u8],
) -> Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(value, bincode::config::legacy()).map(|(value, _)| value)
}

// Only the unrelated SQL module is stubbed; board overlays use production RT types.
pub mod departures_shared {
    #[derive(Clone, Debug, serde::Serialize)]
    pub struct MovedStopData {
        pub stop_id: String,
        pub scheduled_arrival: Option<u64>,
        pub scheduled_departure: Option<u64>,
        pub realtime_arrival: Option<u64>,
        pub realtime_departure: Option<u64>,
    }
}
#[path = "../../../src/departure_realtime.rs"]
pub mod departure_realtime;
