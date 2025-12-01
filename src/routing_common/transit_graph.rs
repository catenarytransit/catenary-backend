// src/transit_format.rs
//
// This module defines the binary schema for the Transit Layer (GTFS + Transfer Patterns).
// It is designed to work alongside `graph_format.rs` (Street Layer).
//
// ARCHITECTURE:
// 1. TransitPartition (transit_chunk_X.pbf):
//    - Contains the full "Compressed Timetable" for a specific geographic cluster.
//    - Optimized for "Trip-Based Routing" (contiguous arrays for CPU cache).
//
// 2. GlobalPatternIndex (global_patterns.pbf):
//    - The "Scalable" part of Transfer Patterns.
//    - Stores the pre-computed DAGs connecting "Hub Stops" between partitions.

use prost::Message;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

// ===========================================================================
// IO HELPERS
// ===========================================================================

/// A sequence of time deltas for a trip.
/// Stores interleaved [travel_time, dwell_time] pairs.
/// Index 2*i: Travel time from Stop i-1 (Departure) to Stop i (Arrival). (0 for i=0).
/// Index 2*i+1: Dwell time at Stop i (Departure - Arrival).
#[derive(Clone, PartialEq, Message)]
pub struct TimeDeltaSequence {
    #[prost(uint32, repeated, tag = "1")]
    pub deltas: Vec<u32>,
}

/// Generic helper to save any Protobuf message to a file.
pub fn save_pbf<T: Message>(data: &T, path: &str) -> io::Result<()> {
    let path = Path::new(path);
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    let payload = data.encode_to_vec();
    writer.write_all(&payload)?;
    Ok(())
}

/// Generic helper to load any Protobuf message from a file.
pub fn load_pbf<T: Message + Default>(path: &str) -> io::Result<T> {
    let path = Path::new(path);
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    T::decode(&buffer[..]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Protobuf decode error: {}", e),
        )
    })
}

// ===========================================================================
// 1. TRANSIT PARTITION (The Chunk)
// ===========================================================================

/// The container for a single graph partition (Cluster).
/// Loaded via Memory Mapping (mmap) during query time.
#[derive(Clone, PartialEq, Message)]
pub struct TransitPartition {
    /// Unique ID of this partition (0 to N).
    #[prost(uint32, tag = "1")]
    pub partition_id: u32,

    /// All stops located within this partition.
    #[prost(message, repeated, tag = "2")]
    pub stops: Vec<TransitStop>,

    /// The core routing logic. Trips are grouped by their "Pattern"
    /// (unique sequence of stops). This is more specific than a GTFS "Route".
    #[prost(message, repeated, tag = "3")]
    pub trip_patterns: Vec<TripPattern>,

    /// The "Pool" of time deltas.
    /// To save space, trips do not store their own times. They point to a sequence
    /// in this list.
    #[prost(message, repeated, tag = "4")]
    pub time_deltas: Vec<TimeDeltaSequence>,

    /// The "Pool" of Direction Patterns (Stop Lists).
    /// Many TripPatterns (schedules) can share the same sequence of stops.
    #[prost(message, repeated, tag = "11")]
    pub direction_patterns: Vec<DirectionPattern>,

    /// Static footpaths between stops (e.g., Platform 1 to Platform 2).
    /// These are internal transfers, distinct from street walking.
    #[prost(message, repeated, tag = "5")]
    pub internal_transfers: Vec<StaticTransfer>,

    /// Linkage to the OSM Street Graph.
    /// Maps a Stop Index (local) to an OSM Node ID (from `streets_base.pbf`).
    #[prost(message, repeated, tag = "6")]
    pub osm_links: Vec<OsmLink>,

    /// Mapping of Service Index -> Service ID String (e.g., "c_12345").
    /// Used to look up the actual GTFS Service ID for a trip.
    #[prost(string, repeated, tag = "7")]
    pub service_ids: Vec<String>,

    /// Exceptions to the regular schedule (Calendar Dates).
    /// Used to handle "Added" or "Removed" service on specific dates.
    #[prost(message, repeated, tag = "8")]
    pub service_exceptions: Vec<ServiceException>,

    /// Transfers to other Chateaus (Inter-Chateau links).
    /// MOVED to TransferChunk to save space in the main hot path.
    /// This field is deprecated/unused in the main partition file.
    #[prost(message, repeated, tag = "9")]
    pub _deprecated_external_transfers: Vec<ExternalTransfer>,

    /// Local Transfer Patterns (LTPs).
    #[prost(message, repeated, tag = "10")]
    pub local_transfer_patterns: Vec<LocalTransferPattern>,

    /// Long Distance Trip Patterns.
    /// Stored separately from standard trip patterns.
    #[prost(message, repeated, tag = "15")]
    pub long_distance_trip_patterns: Vec<TripPattern>,

    /// List of unique timezones used in this partition (e.g., "America/Los_Angeles").
    /// Referenced by `TripPattern.timezone_idx`.
    #[prost(string, repeated, tag = "12")]
    pub timezones: Vec<String>,

    /// The geographic boundary of this partition.
    #[prost(message, optional, tag = "13")]
    pub boundary: Option<PartitionBoundary>,

    /// List of unique Chateau IDs used in this partition.
    /// Referenced by `TransitStop.chateau_idx`, `TripPattern.chateau_idx`, etc.
    #[prost(string, repeated, tag = "14")]
    pub chateau_ids: Vec<String>,
}

/// A transfer to a stop in a different Chateau.
#[derive(Clone, PartialEq, Message)]
pub struct ExternalTransfer {
    /// Index of the local stop in this partition.
    #[prost(uint32, tag = "1")]
    pub from_stop_idx: u32,

    /// The Index of the target Chateau in `TransitPartition.chateau_ids`.
    #[prost(uint32, tag = "2")]
    pub to_chateau_idx: u32,

    /// The GTFS Stop ID in the target Chateau.
    #[prost(string, tag = "3")]
    pub to_stop_gtfs_id: String,

    /// Walking time in seconds.
    #[prost(uint32, tag = "4")]
    pub walk_seconds: u32,

    /// Distance in meters.
    #[prost(uint32, tag = "5")]
    pub distance_meters: u32,

    /// Is this transfer wheelchair accessible?
    #[prost(bool, tag = "6")]
    pub wheelchair_accessible: bool,
}

/// A Transit Stop (Platform, Station, or Pole).
#[derive(Clone, PartialEq, Message)]
pub struct TransitStop {
    /// Local Index (0..N) within this partition.
    #[prost(uint64, tag = "1")]
    pub id: u64,

    /// Index of the Chateau in `TransitPartition.chateau_ids`.
    #[prost(uint32, tag = "2")]
    pub chateau_idx: u32,

    /// The original GTFS Stop ID (string).
    /// Required for matching GTFS-Realtime updates.
    #[prost(string, tag = "3")]
    pub gtfs_original_id: String,

    /// Is this a "Hub" (High Centrality / Major Transfer Point)?
    /// Used for Global Transfer Pattern DAGs.
    #[prost(bool, tag = "4")]
    pub is_hub: bool,

    /// Is this a "Border Stop"?
    /// If true, this stop has edges connecting to another cluster/partition.
    #[prost(bool, tag = "7")]
    pub is_border: bool,

    /// Is this an "External Gateway"?
    /// If true, this stop has transfers to another Chateau.
    #[prost(bool, tag = "8")]
    pub is_external_gateway: bool,

    /// Is this a "Long Distance Station"?
    /// If true, this stop is served by long-distance routes.
    #[prost(bool, tag = "9")]
    pub is_long_distance: bool,

    /// Latitude (WGS84).
    #[prost(double, tag = "5")]
    pub lat: f64,

    /// Longitude (WGS84).
    #[prost(double, tag = "6")]
    pub lon: f64,
}

/// A "Direction Pattern" is a unique sequence of stops.
/// It is shared by multiple TripPatterns (schedules).
#[derive(Clone, PartialEq, Message)]
pub struct DirectionPattern {
    /// The sequence of Stop Indices (referencing `stops` vector) for this pattern.
    #[prost(uint32, repeated, tag = "1")]
    pub stop_indices: Vec<u32>,
}

/// A "Trip Pattern" is a collection of trips that visit the EXACT same
/// sequence of stops. This is the primary unit of Trip-Based Routing.
#[derive(Clone, PartialEq, Message)]
pub struct TripPattern {
    /// Index of the Chateau in `TransitPartition.chateau_ids`.
    #[prost(uint32, tag = "1")]
    pub chateau_idx: u32,

    #[prost(string, tag = "2")]
    pub route_id: String,

    /// Index into `TransitPartition.direction_patterns`.
    /// This defines the sequence of stops for this pattern.
    #[prost(uint32, tag = "3")]
    pub direction_pattern_idx: u32,

    /// The list of trips executing this pattern, SORTED by departure time.
    /// This sorting allows for binary search or linear scan during routing.
    #[prost(message, repeated, tag = "4")]
    pub trips: Vec<CompressedTrip>,

    /// Index into `TransitPartition.timezones`.
    #[prost(uint32, tag = "5")]
    pub timezone_idx: u32,
}

/// A single Trip instance (e.g., "The 8:05 AM Bus").
/// Highly compressed to keep the "Hot Path" small.
#[derive(Clone, PartialEq, Message)]
pub struct CompressedTrip {
    /// The original GTFS Trip ID.
    /// Essential for mapping Realtime Delays (TripUpdates) to this static struct.
    #[prost(string, tag = "1")]
    pub gtfs_trip_id: String,

    /// Active Days Bitmask.
    /// Bit 0=Mon, 1=Tue... 6=Sun.
    /// Bit 7-31 can be used for "Exception Dates" or ServiceIDs.
    #[prost(uint32, tag = "2")]
    pub service_mask: u32,

    /// Absolute Start Time (Seconds past reference midnight).
    /// e.g., 08:00 AM = 28800.
    #[prost(uint32, tag = "3")]
    pub start_time: u32,

    /// Index into `TransitPartition.time_deltas`.
    #[prost(uint32, tag = "4")]
    pub time_delta_idx: u32,

    /// Index into `TransitPartition.service_ids`.
    /// Allows looking up the specific Service ID for this trip.
    #[prost(uint32, tag = "5")]
    pub service_idx: u32,

    /// Bikes Allowed (0=Unknown, 1=Yes, 2=No)
    #[prost(uint32, tag = "6")]
    pub bikes_allowed: u32,

    /// Wheelchair Accessible (0=Unknown, 1=Accessible, 2=Not Accessible)
    #[prost(uint32, tag = "7")]
    pub wheelchair_accessible: u32,
}

/// Exception dates for a service (Calendar Dates).
#[derive(Clone, PartialEq, Message)]
pub struct ServiceException {
    /// Index into `TransitPartition.service_ids`.
    #[prost(uint32, tag = "1")]
    pub service_idx: u32,

    /// Dates where service is ADDED (YYYYMMDD or similar integer representation).
    #[prost(uint32, repeated, tag = "2")]
    pub added_dates: Vec<u32>,

    /// Dates where service is REMOVED.
    #[prost(uint32, repeated, tag = "3")]
    pub removed_dates: Vec<u32>,
}

/// A pre-calculated walking connection inside the transit network.
#[derive(Clone, PartialEq, Message)]
pub struct StaticTransfer {
    #[prost(uint32, tag = "1")]
    pub from_stop_idx: u32,
    #[prost(uint32, tag = "2")]
    pub to_stop_idx: u32,
    #[prost(uint32, tag = "3")]
    pub duration_seconds: u32,
    #[prost(uint32, tag = "4")]
    pub distance_meters: u32,
    #[prost(bool, tag = "5")]
    pub wheelchair_accessible: bool,
}

/// Bridges the separate "Transit" and "Street" graph files.
#[derive(Clone, PartialEq, Message)]
pub struct OsmLink {
    #[prost(uint32, tag = "1")]
    pub stop_idx: u32, // Index in `TransitPartition.stops`
    #[prost(uint32, tag = "2")]
    pub osm_node_id: u32, // Index in `StreetData.nodes` (from graph_format.rs)
    #[prost(uint32, tag = "3")]
    pub walk_seconds: u32, // Time to walk from Stop to Street Node
    #[prost(uint32, tag = "4")]
    pub distance_meters: u32,
    #[prost(bool, tag = "5")]
    pub wheelchair_accessible: bool,
}

/// A separate chunk for storing large transfer graphs (e.g. cycling transfers).
/// Loaded on demand.
#[derive(Clone, PartialEq, Message)]
pub struct TransferChunk {
    /// Unique ID of this partition (matches TransitPartition).
    #[prost(uint32, tag = "1")]
    pub partition_id: u32,

    /// Transfers to other Chateaus (Inter-Chateau links).
    #[prost(message, repeated, tag = "2")]
    pub external_transfers: Vec<ExternalTransfer>,
}

// ===========================================================================
// 2. GLOBAL TRANSFER PATTERNS (The Index)
// ===========================================================================

/// The Master Index file (`global_patterns.pbf`).
/// Loaded for every inter-partition query.
#[derive(Clone, PartialEq, Message)]
pub struct GlobalPatternIndex {
    /// A DAG for every pair of partitions that have connectivity.
    #[prost(message, repeated, tag = "1")]
    pub partition_dags: Vec<PartitionDag>,

    /// Long Distance Transfer Patterns.
    /// DAGs connecting long-distance stations between partitions.
    #[prost(message, repeated, tag = "2")]
    pub long_distance_dags: Vec<PartitionDag>,
}

/// The Directed Acyclic Graph connecting two partitions.
/// Represents "How to get from Border A to Border B".
#[derive(Clone, PartialEq, Message)]
pub struct PartitionDag {
    #[prost(uint32, tag = "1")]
    pub from_partition: u32,
    #[prost(uint32, tag = "2")]
    pub to_partition: u32,

    /// The Hub Nodes (Border Stops) involved in this DAG.
    /// Stored as a flat list of Global Stop IDs (hashes) or (PartitionID, StopIdx) tuples.
    #[prost(message, repeated, tag = "3")]
    pub hubs: Vec<GlobalHub>,

    /// The edges of the DAG.
    /// Each edge represents a direct ride or transfer sequence between hubs.
    #[prost(message, repeated, tag = "4")]
    pub edges: Vec<DagEdge>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GlobalHub {
    #[prost(uint32, tag = "1")]
    pub original_partition_id: u32,
    #[prost(uint32, tag = "2")]
    pub stop_idx_in_partition: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct DagEdge {
    /// Index into the `hubs` vector of this DAG.
    #[prost(uint32, tag = "1")]
    pub from_hub_idx: u32,
    /// Index into the `hubs` vector.
    #[prost(uint32, tag = "2")]
    pub to_hub_idx: u32,

    /// The "Transfer Pattern" used here.
    /// Replaces the old string-based signature with a structured reference.
    #[prost(oneof = "EdgeType", tags = "3,4,5")]
    pub edge_type: Option<EdgeType>,
}

#[derive(Clone, PartialEq, Message, serde::Serialize, serde::Deserialize)]
pub struct TransitEdge {
    /// Index into the source partition's `trip_patterns`.
    #[prost(uint32, tag = "1")]
    pub trip_pattern_idx: u32,

    /// Index in the pattern's stop sequence (NOT the global stop index).
    #[prost(uint32, tag = "2")]
    pub start_stop_idx: u32,

    /// Index in the pattern's stop sequence.
    #[prost(uint32, tag = "3")]
    pub end_stop_idx: u32,

    /// Minimum travel time in seconds for this segment across all trips.
    /// Used for static analysis / global graph construction.
    #[prost(uint32, tag = "4")]
    pub min_duration: u32,
}

#[derive(Clone, PartialEq, Message, serde::Serialize, serde::Deserialize)]
pub struct WalkEdge {
    #[prost(uint32, tag = "1")]
    pub duration_seconds: u32,
}

#[derive(Clone, PartialEq, prost::Oneof, serde::Serialize, serde::Deserialize)]
pub enum EdgeType {
    #[prost(message, tag = "3")]
    Transit(TransitEdge),
    #[prost(message, tag = "5")]
    LongDistanceTransit(TransitEdge),
    #[prost(message, tag = "4")]
    Walk(WalkEdge),
}

/// Local Transfer Patterns (LTPs).
/// Precomputed DAGs for optimal routes within the cluster.
/// Used to accelerate long-distance queries and for local queries.
#[derive(Clone, PartialEq, Message)]
pub struct LocalTransferPattern {
    /// The source stop index (local) for this DAG.
    #[prost(uint32, tag = "1")]
    pub from_stop_idx: u32,

    /// The edges of the DAG rooted at `from_stop_idx`.
    /// These edges describe optimal paths to reach other stops (primarily border stops).
    #[prost(message, repeated, tag = "2")]
    pub edges: Vec<DagEdge>,
}

/// Represents an edge between partitions in the global graph.
/// Used for "edges_chunk_X.json".
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub struct EdgeEntry {
    pub from_chateau: String,
    pub from_id: String,
    pub to_chateau: String,
    pub to_id: String,
    pub edge_type: Option<EdgeType>,
}

#[derive(Clone, PartialEq, Message, serde::Serialize, serde::Deserialize)]
pub struct PartitionBoundary {
    #[prost(message, repeated, tag = "1")]
    pub points: Vec<BoundaryPoint>,
}

#[derive(Clone, PartialEq, Message, serde::Serialize, serde::Deserialize)]
pub struct BoundaryPoint {
    #[prost(double, tag = "1")]
    pub lat: f64,
    #[prost(double, tag = "2")]
    pub lon: f64,
}

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub struct Manifest {
    pub chateau_to_partitions: std::collections::HashMap<String, Vec<u32>>,
    pub partition_to_chateaux: std::collections::HashMap<u32, Vec<String>>,
    pub partition_boundaries: std::collections::HashMap<u32, PartitionBoundary>,
}
