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
    /// To save space, trips do not store their own times. They point to a slice
    /// of this massive array.
    /// Values are "Seconds since previous stop".
    /// [Trip A Stop 1 (0), Trip A Stop 2 (300), Trip B Stop 1 (0)...]
    #[prost(uint32, repeated, tag = "4")]
    pub time_deltas: Vec<u32>,

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
}

/// A transfer to a stop in a different Chateau.
#[derive(Clone, PartialEq, Message)]
pub struct ExternalTransfer {
    /// Index of the local stop in this partition.
    #[prost(uint32, tag = "1")]
    pub from_stop_idx: u32,

    /// The ID of the target Chateau.
    #[prost(string, tag = "2")]
    pub to_chateau: String,

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

    /// Linking Chateau
    #[prost(string, tag = "2")]
    pub chateau: String,

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

    /// Latitude (WGS84).
    #[prost(double, tag = "5")]
    pub lat: f64,

    /// Longitude (WGS84).
    #[prost(double, tag = "6")]
    pub lon: f64,
}

/// A "Trip Pattern" is a collection of trips that visit the EXACT same
/// sequence of stops. This is the primary unit of Trip-Based Routing.
#[derive(Clone, PartialEq, Message)]
pub struct TripPattern {
    #[prost(string, tag = "1")]
    pub chateau: String,

    #[prost(string, tag = "2")]
    pub route_id: String,

    /// The sequence of Stop Indices (referencing `stops` vector) for this pattern.
    #[prost(uint32, repeated, tag = "3")]
    pub stop_indices: Vec<u32>,

    /// The list of trips executing this pattern, SORTED by departure time.
    /// This sorting allows for binary search or linear scan during routing.
    #[prost(message, repeated, tag = "4")]
    pub trips: Vec<CompressedTrip>,
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

    /// Absolute Start Time (Seconds past midnight).
    /// e.g., 08:00 AM = 28800.
    #[prost(uint32, tag = "3")]
    pub start_time: u32,

    /// Pointer to the `time_deltas` array in `TransitPartition`.
    /// The trip reads `stop_indices.len()` integers starting at this index.
    #[prost(uint32, tag = "4")]
    pub delta_pointer: u32,

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
    /// Simplification: Just storing the fact that a connection exists.
    /// In full "Scalable Transfer Patterns", this might point to a specific
    /// trip pattern sequence.
    #[prost(string, tag = "3")]
    pub pattern_signature: String,
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
