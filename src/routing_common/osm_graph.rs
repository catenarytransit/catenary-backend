// This module defines the binary schema for the Multimodal Routing Engine.
// It uses Protocol Buffers (via the `prost` crate) to ensure the data is
// compact, cross-platform, and backwards compatible.
//
// ARCHITECTURE RECAP:
// 1. StreetData (streets_base.pbf):
//    - The "Physical" layer. Contains nodes, edges, and detailed attributes.
//    - Topology is stored as an Adjacency Array (CSR-like) for cache efficiency.

use prost::Message;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

// ===========================================================================
// CONSTANTS & BITMASKS
// ===========================================================================

pub mod permissions {
    /// Bit 0: Walking allowed in forward direction (Source -> Target)
    pub const WALK_FWD: u32 = 0b0000_0001;
    /// Bit 1: Walking allowed in backward direction (Target -> Source)
    pub const WALK_BWD: u32 = 0b0000_0010;
    /// Bit 2: Cycling allowed in forward direction
    pub const CYCL_FWD: u32 = 0b0000_0100;
    /// Bit 3: Cycling allowed in backward direction
    pub const CYCL_BWD: u32 = 0b0000_1000;
}

pub mod edge_flags {
    /// Edge crosses a tile boundary.
    /// The algorithm should fetch the next chunk when traversing this edge.
    pub const EDGE_FLAG_BORDER: u32 = 1 << 0; // 1

    /// Edge has a dedicated sidewalk (or path/footway).
    /// Used to reduce penalties on high-traffic roads.
    pub const EDGE_FLAG_HAS_SIDEWALK: u32 = 1 << 1; // 2

    // Road Class (3 bits: 2-4)
    // 0 = Residential/Unclassified (Default)
    // 1 = Tertiary
    // 2 = Secondary
    // 3 = Primary
    // 4 = Trunk
    // 5 = Motorway
    pub const ROAD_CLASS_MASK: u32 = 0b11100; // Bits 2,3,4
    pub const ROAD_CLASS_SHIFT: u32 = 2;

    pub const CLASS_PATH: u32 = 0; // Footway, Cycleway, Path, etc.
    pub const CLASS_RESIDENTIAL: u32 = 1 << 2; // Residential, Service, Unclassified
    pub const CLASS_TERTIARY: u32 = 2 << 2;
    pub const CLASS_SECONDARY: u32 = 3 << 2;
    pub const CLASS_PRIMARY: u32 = 4 << 2;
    pub const CLASS_TRUNK: u32 = 5 << 2;
    pub const CLASS_MOTORWAY: u32 = 6 << 2;

    /// Edge is a crossing (e.g. footway=crossing).
    pub const EDGE_FLAG_CROSSING: u32 = 1 << 5; // 32
}

pub mod node_flags {
    /// Node is a crossing (e.g. highway=crossing).
    pub const NODE_FLAG_CROSSING: u32 = 1 << 0;
}

// ===========================================================================
// 1. BASE STREET GRAPH (streets_base.pbf)
// ===========================================================================

/// The Root container for the physical street network.
/// Corresponds to `streets_base.pbf`.
#[derive(Clone, PartialEq, Message)]
pub struct StreetData {
    /// All nodes in the graph, sorted by NodeID (0 to N).
    /// Used for coordinate lookup and to find the start of the edge list.
    #[prost(message, repeated, tag = "1")]
    pub nodes: Vec<Node>,

    /// A flattened array of all edges in the graph.
    /// Nodes point into this array using `first_edge_idx`.
    #[prost(message, repeated, tag = "2")]
    pub edges: Vec<Edge>,

    /// Geometries are stored separately to keep the `Edge` struct small (hot path).
    /// An edge refers to this by `geometry_id`.
    #[prost(message, repeated, tag = "3")]
    pub geometries: Vec<Geometry>,

    /// Unique ID of this partition.
    #[prost(uint32, tag = "4")]
    pub partition_id: u32,

    /// Nodes that connect to other partitions.
    /// Used for routing across chunk boundaries.
    #[prost(message, repeated, tag = "5")]
    pub boundary_nodes: Vec<BoundaryNode>,
}

/// A connection point to another Street Graph partition.
#[derive(Clone, PartialEq, Message)]
pub struct BoundaryNode {
    /// The local node index in `nodes`.
    #[prost(uint32, tag = "1")]
    pub local_node_idx: u32,

    /// The ID of the target partition.
    #[prost(uint32, tag = "2")]
    pub target_partition_id: u32,

    /// The node index in the target partition.
    #[prost(uint32, tag = "3")]
    pub target_node_idx: u32,
}

/// A physical location in the world (Intersection, Dead-end, etc.).
#[derive(Clone, PartialEq, Message)]
pub struct Node {
    /// Latitude in degrees (WGS84).
    #[prost(double, tag = "1")]
    pub lat: f64,

    /// Longitude in degrees (WGS84).
    #[prost(double, tag = "2")]
    pub lon: f64,

    /// Elevation in meters above mean sea level.
    /// Critical for calculating slope dynamically if we split edges later.
    #[prost(float, tag = "3")]
    pub elevation: f32,

    /// Index into the `edges` vector where this node's outgoing edges begin.
    /// Index of the first edge starting from this node in the `edges` array.
    #[prost(uint32, tag = "4")]
    pub first_edge_idx: u32,

    /// Node flags (e.g. is_crossing).
    #[prost(uint32, tag = "5")]
    pub flags: u32,
}

/// A physical connection between two nodes.
/// Designed to be compact for the "Street Layer".
#[derive(Clone, PartialEq, Message)]
pub struct Edge {
    /// The destination Node ID.
    #[prost(uint32, tag = "1")]
    pub target_node: u32,

    /// Length in meters. stored as u32 to allow for very long segments if needed,
    /// though u16 (65km) is usually sufficient.
    #[prost(uint32, tag = "2")]
    pub distance_mm: u32, // Stored in millimeters for precision, or centimeters.

    /// Pointer to the `geometries` array for visual rendering.
    #[prost(uint32, tag = "3")]
    pub geometry_id: u32,

    /// Bitmask defining who can use this edge.
    /// See `permissions` module constants.
    /// Type is u32 in Proto, but we treat it as u8 logic.
    #[prost(uint32, tag = "4")]
    pub permissions: u32,

    /// Surface type identifier (0=Asphalt, 1=Gravel, etc.)
    /// Used for calculating rolling resistance penalties.
    #[prost(uint32, tag = "5")]
    pub surface_type: u32,

    /// Percentage Grade (Slope).
    /// Stored as a signed integer.
    /// Value 5 means +5% (Uphill Fwd), -5 means -5% (Downhill Fwd).
    /// Proto doesn't have i8, so we use int32.
    #[prost(int32, tag = "6")]
    pub grade_percent: i32,

    /// General purpose flags for the edge.
    /// See `edge_flags` module constants.
    #[prost(uint32, tag = "7")]
    pub flags: u32,
}

/// Geometry string for map display.
#[derive(Clone, PartialEq, Message)]
pub struct Geometry {
    /// Alternating [lat, lon, lat, lon...]
    /// We use a specialized delta-encoding here usually, but raw float is easier for now.
    #[prost(float, repeated, tag = "1")]
    pub coords: Vec<f32>,
}

// ===========================================================================
// IO HELPERS
// ===========================================================================

/// Generic helper to save any Protobuf message to a file.
/// Uses BufWriter for performance.
pub fn save_pbf<T: Message>(data: &T, path: &str) -> io::Result<()> {
    let path = Path::new(path);
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // We serialize the data into a byte vector first.
    // In production for huge files, you might stream this,
    // but encode_to_vec is safer for ensuring integrity before write.
    let payload = data.encode_to_vec();

    // Write the raw bytes.
    // Note: You might want to wrap `writer` in a GzEncoder (flate2)
    // to compress the file on disk (PBF is binary but not compressed).
    writer.write_all(&payload)?;
    Ok(())
}

/// Generic helper to load any Protobuf message from a file.
pub fn load_pbf<T: Message + Default>(path: &str) -> io::Result<T> {
    let path = Path::new(path);
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read the entire file into a buffer
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;

    // Decode (Parse)
    T::decode(&buffer[..]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Protobuf decode error: {}", e),
        )
    })
}

// ===========================================================================
// USAGE EXAMPLE (TEST)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streets_base_io() {
        // 1. Create Mock Data
        let node1 = Node {
            lat: 34.0,
            lon: -118.0,
            elevation: 100.0,
            first_edge_idx: 0,
            flags: 0,
        };
        let node2 = Node {
            lat: 34.1,
            lon: -118.1,
            elevation: 110.0,
            first_edge_idx: 1,
            flags: 0,
        };

        let edge = Edge {
            target_node: 1,
            distance_mm: 50000, // 50m
            geometry_id: 0,
            permissions: permissions::WALK_FWD | permissions::CYCL_FWD,
            surface_type: 1,  // Gravel
            grade_percent: 5, // 5% Uphill
            flags: 0,
        };

        let graph = StreetData {
            nodes: vec![node1, node2],
            edges: vec![edge],
            geometries: vec![Geometry {
                coords: vec![34.0, -118.0, 34.1, -118.1],
            }],
            partition_id: 0,
            boundary_nodes: vec![],
        };

        // 2. Save
        let filename = "test_streets.pbf";
        save_pbf(&graph, filename).expect("Failed to save");

        // 3. Load
        let loaded: StreetData = load_pbf(filename).expect("Failed to load");

        // 4. Verify
        assert_eq!(loaded.nodes.len(), 2);
        assert_eq!(loaded.edges[0].grade_percent, 5);
        assert_eq!(loaded.edges[0].flags, 0);

        // Cleanup
        std::fs::remove_file(filename).unwrap();
    }
}
