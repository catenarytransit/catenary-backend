use serde::{Deserialize, Serialize};
use std::fmt;

/// Typed wrapper for OSM node IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OsmNodeId(pub i64);

impl fmt::Display for OsmNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Typed wrapper for OSM way IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OsmWayId(pub i64);

impl fmt::Display for OsmWayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "w{}", self.0)
    }
}

/// Grade separation class derived from OSM layer/bridge/tunnel tags.
/// - Positive values indicate above-ground (bridges, viaducts)
/// - Negative values indicate below-ground (tunnels)
/// - Zero is ground level
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct ZClass(pub i8);

impl ZClass {
    pub const GROUND: ZClass = ZClass(0);
    pub const BRIDGE: ZClass = ZClass(1);
    pub const TUNNEL: ZClass = ZClass(-1);

    /// Parse z-class from OSM tags with precedence: layer > bridge/tunnel inference
    pub fn from_tags(layer: Option<i8>, bridge: Option<&str>, tunnel: Option<&str>) -> Self {
        // Explicit layer tag takes precedence
        if let Some(l) = layer {
            return ZClass(l);
        }

        // Infer from bridge/tunnel if layer not present
        if let Some(b) = bridge {
            if b == "yes" || b == "viaduct" || b == "true" {
                return ZClass::BRIDGE;
            }
        }

        if let Some(t) = tunnel {
            if t == "yes" || t == "true" {
                return ZClass::TUNNEL;
            }
        }

        ZClass::GROUND
    }
}

/// Rail transport modes mapped from OSM railway=* values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RailMode {
    /// Heavy rail (mainline, intercity, regional)
    Rail,
    /// Metro/subway (typically grade-separated urban)
    Subway,
    /// Tram/streetcar (typically street-running)
    Tram,
    /// Light rail (hybrid between tram and metro)
    LightRail,
    /// Narrow gauge railways
    NarrowGauge,
    /// Funicular railways
    Funicular,
    /// Monorail
    Monorail,
    /// Preserved/heritage railways
    Preserved,
    /// Miniature/tourist railways
    Miniature,
    /// Unknown or unmapped type
    Unknown,
}

impl RailMode {
    /// Parse from OSM railway=* tag value
    pub fn from_osm_tag(value: &str) -> Option<Self> {
        match value {
            "rail" => Some(RailMode::Rail),
            "subway" => Some(RailMode::Subway),
            "tram" => Some(RailMode::Tram),
            "light_rail" => Some(RailMode::LightRail),
            "narrow_gauge" => Some(RailMode::NarrowGauge),
            "funicular" => Some(RailMode::Funicular),
            "monorail" => Some(RailMode::Monorail),
            "preserved" => Some(RailMode::Preserved),
            "miniature" => Some(RailMode::Miniature),
            // Non-rail values we skip
            "platform" | "station" | "halt" | "tram_stop" | "crossing" | "level_crossing"
            | "signal" | "switch" | "buffer_stop" | "turntable" | "roundhouse" | "traverser"
            | "wash" | "construction" | "disused" | "abandoned" | "razed" => None,
            _ => Some(RailMode::Unknown),
        }
    }

    /// Check if this mode should be included in the track graph
    pub fn is_active_rail(&self) -> bool {
        matches!(
            self,
            RailMode::Rail
                | RailMode::Subway
                | RailMode::Tram
                | RailMode::LightRail
                | RailMode::NarrowGauge
                | RailMode::Funicular
                | RailMode::Monorail
        )
    }

    /// Map to GTFS route_type ranges for compatibility
    pub fn compatible_gtfs_types(&self) -> &'static [i16] {
        match self {
            RailMode::Rail => &[2, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            RailMode::Subway => &[1, 400, 401, 402, 403, 404, 405],
            RailMode::Tram => &[0, 900, 901, 902, 903, 904, 905, 906],
            RailMode::LightRail => &[0, 12, 900],
            RailMode::NarrowGauge => &[2, 100],
            RailMode::Funicular => &[7, 1400],
            RailMode::Monorail => &[12, 405],
            RailMode::Preserved | RailMode::Miniature => &[2],
            RailMode::Unknown => &[],
        }
    }
}

/// Unique identifier for an atomic edge in the track graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AtomicEdgeId(pub u64);

impl AtomicEdgeId {
    /// Generate ID from way ID and segment index
    pub fn from_way_segment(way_id: OsmWayId, segment_idx: u32) -> Self {
        // Pack way ID (48 bits) and segment index (16 bits)
        let id = ((way_id.0 as u64) << 16) | (segment_idx as u64 & 0xFFFF);
        AtomicEdgeId(id)
    }

    /// Extract source way ID
    pub fn source_way(&self) -> OsmWayId {
        OsmWayId((self.0 >> 16) as i64)
    }

    /// Extract segment index within the way
    pub fn segment_index(&self) -> u32 {
        (self.0 & 0xFFFF) as u32
    }
}

/// An atomic segment of track - the fundamental unit of the physical track graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicEdge {
    pub id: AtomicEdgeId,
    /// Start node (OSM node ID)
    pub from: OsmNodeId,
    /// End node (OSM node ID)
    pub to: OsmNodeId,
    /// Transport mode
    pub mode: RailMode,
    /// Grade separation class
    pub z_class: ZClass,
    /// Geometry as coordinate pairs (lon, lat) - WGS84
    pub geometry: Vec<(f64, f64)>,
    /// Source OSM way ID
    pub source_way: OsmWayId,
    /// Length in meters
    pub length_m: f64,
}

impl AtomicEdge {
    /// Get the other endpoint given one endpoint
    pub fn other_end(&self, node: OsmNodeId) -> Option<OsmNodeId> {
        if self.from == node {
            Some(self.to)
        } else if self.to == node {
            Some(self.from)
        } else {
            None
        }
    }

    /// Check if this edge connects to the given node
    pub fn connects_to(&self, node: OsmNodeId) -> bool {
        self.from == node || self.to == node
    }
}

/// Identifier for a transit line (route + direction)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineId {
    pub chateau: String,
    pub route_id: String,
    /// Optional direction ID (0 or 1 for GTFS)
    pub direction: Option<u8>,
}

impl LineId {
    pub fn new(chateau: impl Into<String>, route_id: impl Into<String>) -> Self {
        Self {
            chateau: chateau.into(),
            route_id: route_id.into(),
            direction: None,
        }
    }

    pub fn with_direction(mut self, dir: u8) -> Self {
        self.direction = Some(dir);
        self
    }
}

impl fmt::Display for LineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.direction {
            Some(d) => write!(f, "{}:{}:{}", self.chateau, self.route_id, d),
            None => write!(f, "{}:{}", self.chateau, self.route_id),
        }
    }
}

/// A transit line with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Line {
    pub id: LineId,
    pub label: String,
    pub color: String,
    pub route_type: i16,
    pub agency_id: Option<String>,
}

/// Occurrence of a line on an edge, with optional direction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineOcc {
    pub line: LineId,
    /// Direction node - the node towards which this line travels
    /// None if bidirectional
    pub direction_node: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_z_class_from_tags() {
        assert_eq!(ZClass::from_tags(Some(2), None, None), ZClass(2));
        assert_eq!(ZClass::from_tags(Some(-2), None, None), ZClass(-2));
        assert_eq!(ZClass::from_tags(None, Some("yes"), None), ZClass::BRIDGE);
        assert_eq!(ZClass::from_tags(None, None, Some("yes")), ZClass::TUNNEL);
        assert_eq!(ZClass::from_tags(None, None, None), ZClass::GROUND);
        // Layer takes precedence
        assert_eq!(ZClass::from_tags(Some(3), Some("yes"), None), ZClass(3));
    }

    #[test]
    fn test_rail_mode_parsing() {
        assert_eq!(RailMode::from_osm_tag("rail"), Some(RailMode::Rail));
        assert_eq!(RailMode::from_osm_tag("subway"), Some(RailMode::Subway));
        assert_eq!(RailMode::from_osm_tag("platform"), None);
        assert_eq!(RailMode::from_osm_tag("disused"), None);
    }

    #[test]
    fn test_atomic_edge_id_roundtrip() {
        let way = OsmWayId(123456789);
        let seg = 42u32;
        let id = AtomicEdgeId::from_way_segment(way, seg);
        assert_eq!(id.source_way(), way);
        assert_eq!(id.segment_index(), seg);
    }
}
