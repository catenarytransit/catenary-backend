// src/transit_format.rs
//
// This module defines the binary schema for the Transit Layer (GTFS + Transfer Patterns).
// It is designed to work alongside `graph_format.rs` (Street Layer).
//
// ARCHITECTURE:
// 1. TransitPartition (transit_chunk_X.bincode):
//    - Contains the full "Compressed Timetable" for a specific geographic cluster.
//    - Optimized for "Trip-Based Routing" (contiguous arrays for CPU cache).
//
// 2. GlobalPatternIndex (global_patterns.bincode):
//    - The "Scalable" part of Transfer Patterns.
//    - Stores the pre-computed DAGs connecting "Hub Stops" between partitions.

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
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TimeDeltaSequence {
    pub deltas: Vec<u32>,
}

/// Generic helper to save any Serde-compatible struct to a file using bincode.
pub fn save_bincode<T: serde::Serialize>(data: &T, path: &str) -> io::Result<()> {
    let path = Path::new(path);
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    bincode::serde::encode_into_std_write(data, &mut writer, bincode::config::standard()).map_err(
        |e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Bincode serialize error: {}", e),
            )
        },
    )?;
    Ok(())
}

/// Generic helper to load any Serde-compatible struct from a file using bincode.
pub fn load_bincode<T: serde::de::DeserializeOwned>(path: &str) -> io::Result<T> {
    let path = Path::new(path);
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let data = bincode::serde::decode_from_std_read(&mut reader, bincode::config::standard())
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Bincode deserialize error: {}", e),
            )
        })?;
    Ok(data)
}

// ===========================================================================
// 1. TRANSIT PARTITION (The Chunk)
// ===========================================================================

/// The container for a single graph partition (Cluster).
/// Loaded via Memory Mapping (mmap) during query time.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TransitPartition {
    /// Unique ID of this partition (0 to N).
    pub partition_id: u32,

    /// All stops located within this partition.
    pub stops: Vec<TransitStop>,

    /// The core routing logic. Trips are grouped by their "Pattern"
    /// (unique sequence of stops). This is more specific than a GTFS "Route".
    pub trip_patterns: Vec<TripPattern>,

    /// The "Pool" of time deltas.
    /// To save space, trips do not store their own times. They point to a sequence
    /// in this list.
    pub time_deltas: Vec<TimeDeltaSequence>,

    /// The "Pool" of Direction Patterns (Stop Lists).
    /// Many TripPatterns (schedules) can share the same sequence of stops.
    pub direction_patterns: Vec<DirectionPattern>,

    /// Static footpaths between stops (e.g., Platform 1 to Platform 2).
    /// These are internal transfers, distinct from street walking.
    pub internal_transfers: Vec<StaticTransfer>,

    /// Linkage to the OSM Street Graph.
    /// Maps a Stop Index (local) to an OSM Node ID (from `streets_base.pbf`).
    pub osm_links: Vec<OsmLink>,

    /// Mapping of Service Index -> Service ID String (e.g., "c_12345").
    /// Used to look up the actual GTFS Service ID for a trip.
    pub service_ids: Vec<String>,

    /// Exceptions to the regular schedule (Calendar Dates).
    /// Used to handle "Added" or "Removed" service on specific dates.
    pub service_exceptions: Vec<ServiceException>,

    /// Transfers to other Chateaus (Inter-Chateau links).
    /// MOVED to TransferChunk to save space in the main hot path.
    /// This field is deprecated/unused in the main partition file.
    pub _deprecated_external_transfers: Vec<ExternalTransfer>,

    /// Local Transfer Patterns (Union DAG).
    /// Map from Source Stop Index -> List of Outgoing Edges.
    pub local_dag: std::collections::HashMap<u32, DagEdgeList>,

    /// Long Distance Trip Patterns.
    /// Stored separately from standard trip patterns.
    pub long_distance_trip_patterns: Vec<TripPattern>,

    /// List of unique timezones used in this partition (e.g., "America/Los_Angeles").
    /// Referenced by `TripPattern.timezone_idx`.
    pub timezones: Vec<String>,

    /// The geographic boundary of this partition.
    pub boundary: Option<PartitionBoundary>,

    /// List of unique Chateau IDs used in this partition.
    /// Referenced by `TransitStop.chateau_idx`, `TripPattern.chateau_idx`, etc.
    pub chateau_ids: Vec<String>,

    /// External Hubs referenced by long-distance patterns.
    /// These are stops in OTHER partitions that are reachable from this partition's long-distance stations.
    pub external_hubs: Vec<GlobalHub>,

    /// Long Distance Transfer Patterns.
    /// DAGs connecting local long-distance stations to external hubs.
    pub long_distance_transfer_patterns: Vec<LocalTransferPattern>,

    /// Index for direct connections within this partition.
    /// StationID -> List of (PatternIdx, StopIdxInPattern)
    pub direct_connections_index: std::collections::HashMap<String, Vec<DirectionPatternReference>>,
}

/// A transfer to a stop in a different Chateau.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ExternalTransfer {
    /// Index of the local stop in this partition.
    pub from_stop_idx: u32,

    /// The Index of the target Chateau in `TransitPartition.chateau_ids`.
    pub to_chateau_idx: u32,

    /// The GTFS Stop ID in the target Chateau.
    pub to_stop_gtfs_id: String,

    /// Walking time in seconds.
    pub walk_seconds: u32,

    /// Distance in meters.
    pub distance_meters: u32,

    /// Is this transfer wheelchair accessible?
    pub wheelchair_accessible: bool,
}

/// A Transit Stop (Platform, Station, or Pole).
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TransitStop {
    /// Local Index (0..N) within this partition.
    pub id: u64,

    /// Index of the Chateau in `TransitPartition.chateau_ids`.
    pub chateau_idx: u32,

    /// The Station ID (UUID or stable string).
    pub station_id: String,

    /// List of GTFS Stop IDs mapped to this station.
    pub gtfs_stop_ids: Vec<String>,

    /// Is this a "Hub" (High Centrality / Major Transfer Point)?
    /// Used for Global Transfer Pattern DAGs.
    pub is_hub: bool,

    /// Is this a "Border Stop"?
    /// If true, this stop has edges connecting to another cluster/partition.
    pub is_border: bool,

    /// Is this an "External Gateway"?
    /// If true, this stop has transfers to another Chateau.
    pub is_external_gateway: bool,

    /// Is this a "Long Distance Station"?
    /// If true, this stop is served by long-distance routes.
    pub is_long_distance: bool,

    /// Latitude (WGS84).
    pub lat: f64,

    /// Longitude (WGS84).
    pub lon: f64,
}

/// A "Direction Pattern" is a unique sequence of stops.
/// It is shared by multiple TripPatterns (schedules).
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct DirectionPattern {
    /// The sequence of Stop Indices (referencing `stops` vector) for this pattern.
    pub stop_indices: Vec<u32>,
}

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct DirectionPatternReference {
    pub pattern_idx: u32,
    pub stop_idx: u32, // Index in the pattern's stop sequence
}

/// A "Trip Pattern" is a collection of trips that visit the EXACT same
/// sequence of stops. This is the primary unit of Trip-Based Routing.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TripPattern {
    /// Index of the Chateau in `TransitPartition.chateau_ids`.
    pub chateau_idx: u32,

    pub route_id: String,

    /// Index into `TransitPartition.direction_patterns`.
    /// This defines the sequence of stops for this pattern.
    pub direction_pattern_idx: u32,

    /// The list of trips executing this pattern, SORTED by departure time.
    /// This sorting allows for binary search or linear scan during routing.
    pub trips: Vec<CompressedTrip>,

    /// Index into `TransitPartition.timezones`.
    pub timezone_idx: u32,

    pub route_type: u32,

    pub is_border: bool,
}

/// A single Trip instance (e.g., "The 8:05 AM Bus").
/// Highly compressed to keep the "Hot Path" small.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct CompressedTrip {
    /// The original GTFS Trip ID.
    /// Essential for mapping Realtime Delays (TripUpdates) to this static struct.
    pub gtfs_trip_id: String,

    /// Active Days Bitmask.
    /// Bit 0=Mon, 1=Tue... 6=Sun.
    /// Bit 7-31 can be used for "Exception Dates" or ServiceIDs.
    pub service_mask: u32,

    /// Absolute Start Time (Seconds past reference midnight).
    /// e.g., 08:00 AM = 28800.
    pub start_time: u32,

    /// Index into `TransitPartition.time_deltas`.
    pub time_delta_idx: u32,

    /// Index into `TransitPartition.service_ids`.
    /// Allows looking up the specific Service ID for this trip.
    pub service_idx: u32,

    /// Bikes Allowed (0=Unknown, 1=Yes, 2=No)
    pub bikes_allowed: u32,

    /// Wheelchair Accessible (0=Unknown, 1=Accessible, 2=Not Accessible)
    pub wheelchair_accessible: u32,
}

/// Exception dates for a service (Calendar Dates).
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ServiceException {
    /// Index into `TransitPartition.service_ids`.
    pub service_idx: u32,

    /// Dates where service is ADDED (YYYYMMDD or similar integer representation).
    pub added_dates: Vec<u32>,

    /// Dates where service is REMOVED.
    pub removed_dates: Vec<u32>,
}

/// A pre-calculated walking connection inside the transit network.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StaticTransfer {
    pub from_stop_idx: u32,
    pub to_stop_idx: u32,
    pub duration_seconds: u32,
    pub distance_meters: u32,
    pub wheelchair_accessible: bool,
}

/// Bridges the separate "Transit" and "Street" graph files.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct OsmLink {
    pub stop_idx: u32,     // Index in `TransitPartition.stops`
    pub osm_node_id: u32,  // Index in `StreetData.nodes` (from graph_format.rs)
    pub walk_seconds: u32, // Time to walk from Stop to Street Node
    pub distance_meters: u32,
    pub wheelchair_accessible: bool,
}

/// A separate chunk for storing large transfer graphs (e.g. cycling transfers).
/// Loaded on demand.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TransferChunk {
    /// Unique ID of this partition (matches TransitPartition).
    pub partition_id: u32,

    /// Transfers to other Chateaus (Inter-Chateau links).
    pub external_transfers: Vec<ExternalTransfer>,
}

// ===========================================================================
// 2. GLOBAL TRANSFER PATTERNS (The Index)
// ===========================================================================

/// The Master Index file (`global_patterns.bincode`).
/// Loaded for every inter-partition query.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GlobalPatternIndex {
    /// A DAG for every pair of partitions that have connectivity.
    pub partition_dags: Vec<PartitionDag>,

    /// Long Distance Transfer Patterns.
    /// DAGs connecting long-distance stations between partitions.
    pub long_distance_dags: Vec<PartitionDag>,
}

/// The Directed Acyclic Graph connecting two partitions.
/// Represents "How to get from Border A to Border B".
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PartitionDag {
    pub from_partition: u32,
    pub to_partition: u32,

    /// The Hub Nodes (Border Stops) involved in this DAG.
    /// Stored as a flat list of Global Stop IDs (hashes) or (PartitionID, StopIdx) tuples.
    pub hubs: Vec<GlobalHub>,

    /// The edges of the DAG.
    /// Each edge represents a direct ride or transfer sequence between hubs.
    pub edges: Vec<DagEdge>,
}

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GlobalHub {
    pub original_partition_id: u32,
    pub stop_idx_in_partition: u32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct DagEdge {
    /// Index of the source node.
    /// - For Global DAGs: Index into `PartitionDag.hubs`.
    /// - For Local Transfer Patterns: Index into `TransitPartition.stops`.
    pub from_node_idx: u32,
    /// Index of the target node.
    /// - For Global DAGs: Index into `PartitionDag.hubs`.
    /// - For Local Transfer Patterns: Index into `TransitPartition.stops`.
    pub to_node_idx: u32,

    /// The "Transfer Pattern" used here.
    /// Replaces the old string-based signature with a structured reference.
    pub edge_type: Option<EdgeType>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct TransitEdge {
    /// Index into the source partition's `trip_patterns`.
    pub trip_pattern_idx: u32,

    /// Index in the pattern's stop sequence (NOT the global stop index).
    pub start_stop_idx: u32,

    /// Index in the pattern's stop sequence.
    pub end_stop_idx: u32,

    /// Minimum travel time in seconds for this segment across all trips.
    /// Used for static analysis / global graph construction.
    pub min_duration: u32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct WalkEdge {
    pub duration_seconds: u32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub enum EdgeType {
    Transit(TransitEdge),
    LongDistanceTransit(TransitEdge),
    Walk(WalkEdge),
}

/// Local Transfer Patterns.
/// Precomputed DAGs for optimal routes within the cluster.
/// Used to accelerate long-distance queries and for local queries.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct LocalTransferPattern {
    /// The source stop index (local) for this DAG.
    pub from_stop_idx: u32,

    /// The edges of the DAG rooted at `from_stop_idx`.
    /// These edges describe optimal paths to reach other stops (primarily border stops).
    pub edges: Vec<DagEdge>,
}

/// A wrapper for a list of DAG edges, used in the `local_dag` map.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct DagEdgeList {
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

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PartitionBoundary {
    pub points: Vec<BoundaryPoint>,
}

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct BoundaryPoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub struct Manifest {
    pub chateau_to_partitions: std::collections::HashMap<String, Vec<u32>>,
    pub partition_to_chateaux: std::collections::HashMap<u32, Vec<String>>,
    pub partition_boundaries: std::collections::HashMap<u32, PartitionBoundary>,
}

// ===========================================================================
// 3. GLOBAL TIMETABLE (The Schedule)
// ===========================================================================

/// A separate file storing the actual departure times for patterns used in the Global Graph.
/// Used for fast lookup during query time without loading the full partition.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GlobalTimetable {
    pub partition_timetables: Vec<PartitionTimetable>,
}

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PartitionTimetable {
    pub partition_id: u32,
    pub pattern_timetables: Vec<PatternTimetable>,
    pub time_deltas: Vec<TimeDeltaSequence>,
    /// List of unique timezones used in this partition (e.g., "America/Los_Angeles").
    /// Referenced by `PatternTimetable.timezone_idx`.
    pub timezones: Vec<String>,
}

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PatternTimetable {
    /// The index of the pattern in the partition's `trip_patterns`.
    pub pattern_idx: u32,

    /// Sorted list of trip start times (seconds past midnight).
    pub trip_start_times: Vec<u32>,

    /// Index into `PartitionTimetable.time_deltas` for each trip.
    pub trip_time_delta_indices: Vec<u32>,

    /// Service masks for each trip (optional, for calendar filtering).
    /// If present, must match length of trip_start_times.
    pub service_masks: Vec<u32>,

    /// Index into `PartitionTimetable.timezones`.
    pub timezone_idx: u32,
}

// ===========================================================================
// 4. TIMETABLE DATA (Per Chateau)
// ===========================================================================

/// Aggregated timetable data for a Chateau.
/// Saved as `timetable_data_{chateau_id}.bincode`.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TimetableData {
    pub chateau_id: String,
    pub partitions: Vec<PartitionTimetableData>,
}

/// Timetable data for a single partition within a Chateau.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct PartitionTimetableData {
    pub partition_id: u32,
    /// List of GTFS Stop IDs referenced by direction patterns in this data.
    /// Indices in `direction_patterns` refer to this list.
    pub stops: Vec<String>,
    pub trip_patterns: Vec<TripPattern>,
    pub time_deltas: Vec<TimeDeltaSequence>,
    pub service_ids: Vec<String>,
    pub service_exceptions: Vec<ServiceException>,
    pub timezones: Vec<String>,
    pub direction_patterns: Vec<DirectionPattern>,
}

// ===========================================================================
// 5. INTERMEDIATE DATA (Import Pipeline)
// ===========================================================================

/// Intermediate Station representation for the import pipeline.
/// Sharded by Geohash (3 chars).
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct IntermediateStation {
    pub station_id: String,
    pub chateau_id: String, // Added for lineage
    pub lat: f64,
    pub lon: f64,
    pub name: String,
    pub tile_id: String, // 3-char Geohash
    pub gtfs_stop_ids: Vec<String>,
}

/// Intermediate Local Edge for the import pipeline.
/// Represents an undirected edge between two stations with a weight (frequency).
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct IntermediateLocalEdge {
    pub u_station_id: String,
    pub v_station_id: String,
    pub weights: std::collections::HashMap<String, f32>,
}

impl IntermediateLocalEdge {
    pub fn total_weight(&self) -> f32 {
        self.weights.values().sum()
    }
}

// ===========================================================================
// 6. DIRECT CONNECTIONS (Global Timetable)
// ===========================================================================

/// Global lookup for direct connections between two stations.
/// Used to evaluate edges in the query graph.
/// Saved as `direct_connections.bincode`.
#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct DirectConnections {
    /// List of Station IDs referenced by direction patterns.
    pub stops: Vec<String>,

    pub trip_patterns: Vec<TripPattern>,
    pub time_deltas: Vec<TimeDeltaSequence>,
    pub service_ids: Vec<String>,
    pub service_exceptions: Vec<ServiceException>,
    pub timezones: Vec<String>,
    pub direction_patterns: Vec<DirectionPattern>,

    /// Inverted Index: StationID -> List of (PatternIdx, StopIdxInPattern)
    pub index: std::collections::HashMap<String, Vec<DirectionPatternReference>>,
}

#[derive(Clone, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ConnectionList {
    /// Sorted list of (dep_time, arr_time, trip_id).
    pub connections: Vec<(u32, u32, String)>,
}
