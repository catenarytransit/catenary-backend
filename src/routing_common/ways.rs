use crate::routing_common::types::*;
use serde::{Deserialize, Serialize};

/// Way properties packed into 5 bytes, mirroring OSR's way_properties bitfield layout.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct WayProperties {
    /// byte 0: foot, bike, car, destination, oneway_car, oneway_bike, elevator, steps
    b0: u8,
    /// byte 1: speed_limit(3), platform(1), parking(1), ramp(1), sidewalk_separate(1), motor_vehicle_no(1)
    b1: u8,
    /// byte 2: from_level(6), has_toll(1), big_street(1)
    b2: u8,
    /// byte 3: to_level(6), bus_accessible(1), in_route(1)
    b3: u8,
    /// byte 4: railway(1), oneway_bus_psv(1), incline_down(1), bus_penalty(1), ferry(1), railway_penalty(1)
    b4: u8,
}

impl WayProperties {
    pub fn is_foot_accessible(&self) -> bool {
        self.b0 & 0x01 != 0
    }
    pub fn is_bike_accessible(&self) -> bool {
        self.b0 & 0x02 != 0
    }
    pub fn is_car_accessible(&self) -> bool {
        self.b0 & 0x04 != 0
    }
    pub fn is_destination(&self) -> bool {
        self.b0 & 0x08 != 0
    }
    pub fn is_oneway_car(&self) -> bool {
        self.b0 & 0x10 != 0
    }
    pub fn is_oneway_bike(&self) -> bool {
        self.b0 & 0x20 != 0
    }
    pub fn is_elevator(&self) -> bool {
        self.b0 & 0x40 != 0
    }
    pub fn is_steps(&self) -> bool {
        self.b0 & 0x80 != 0
    }

    pub fn speed_limit(&self) -> SpeedLimit {
        SpeedLimit::from_raw(self.b1 & 0x07)
    }
    pub fn is_parking(&self) -> bool {
        self.b1 & 0x10 != 0
    }
    pub fn is_ramp(&self) -> bool {
        self.b1 & 0x20 != 0
    }
    pub fn is_sidewalk_separate(&self) -> bool {
        self.b1 & 0x40 != 0
    }
    pub fn motor_vehicle_no(&self) -> bool {
        self.b1 & 0x80 != 0
    }

    pub fn from_level(&self) -> Level {
        Level(self.b2 & 0x3F)
    }
    pub fn has_toll(&self) -> bool {
        self.b2 & 0x40 != 0
    }
    pub fn is_big_street(&self) -> bool {
        self.b2 & 0x80 != 0
    }

    pub fn to_level(&self) -> Level {
        Level(self.b3 & 0x3F)
    }
    pub fn is_bus_accessible(&self) -> bool {
        self.b3 & 0x40 != 0
    }
    pub fn in_route(&self) -> bool {
        self.b3 & 0x80 != 0
    }

    pub fn is_railway_accessible(&self) -> bool {
        self.b4 & 0x01 != 0
    }
    pub fn is_oneway_bus_psv(&self) -> bool {
        self.b4 & 0x02 != 0
    }
    pub fn is_bus_accessible_with_penalty(&self) -> bool {
        self.b4 & 0x08 != 0
    }
    pub fn is_ferry_accessible(&self) -> bool {
        self.b4 & 0x10 != 0
    }
    pub fn is_railway_accessible_with_penalty(&self) -> bool {
        self.b4 & 0x20 != 0
    }

    pub fn is_accessible(&self) -> bool {
        self.is_car_accessible()
            || self.is_bike_accessible()
            || self.is_foot_accessible()
            || self.is_bus_accessible()
            || self.is_bus_accessible_with_penalty()
            || self.is_railway_accessible()
            || self.is_railway_accessible_with_penalty()
            || self.is_ferry_accessible()
    }

    pub fn max_speed_s_per_m(&self) -> f32 {
        self.speed_limit().to_seconds_per_meter()
    }
}

const _: () = assert!(std::mem::size_of::<WayProperties>() == 5);

/// Builder for WayProperties, sets individual bitfield flags.
#[derive(Default)]
pub struct WayPropertiesBuilder {
    inner: WayProperties,
}

impl WayPropertiesBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn foot_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x01;
        }
        self
    }
    pub fn bike_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x02;
        }
        self
    }
    pub fn car_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x04;
        }
        self
    }
    pub fn destination(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x08;
        }
        self
    }
    pub fn oneway_car(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x10;
        }
        self
    }
    pub fn oneway_bike(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x20;
        }
        self
    }
    pub fn elevator(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x40;
        }
        self
    }
    pub fn steps(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x80;
        }
        self
    }
    pub fn speed_limit(mut self, s: SpeedLimit) -> Self {
        self.inner.b1 = (self.inner.b1 & !0x07) | (s as u8 & 0x07);
        self
    }
    pub fn parking(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x10;
        }
        self
    }
    pub fn ramp(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x20;
        }
        self
    }
    pub fn sidewalk_separate(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x40;
        }
        self
    }
    pub fn from_level(mut self, lvl: Level) -> Self {
        self.inner.b2 = (self.inner.b2 & !0x3F) | (lvl.0 & 0x3F);
        self
    }
    pub fn big_street(mut self, v: bool) -> Self {
        if v {
            self.inner.b2 |= 0x80;
        }
        self
    }
    pub fn to_level(mut self, lvl: Level) -> Self {
        self.inner.b3 = (self.inner.b3 & !0x3F) | (lvl.0 & 0x3F);
        self
    }
    pub fn bus_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b3 |= 0x40;
        }
        self
    }
    pub fn railway_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b4 |= 0x01;
        }
        self
    }
    pub fn oneway_bus_psv(mut self, v: bool) -> Self {
        if v {
            self.inner.b4 |= 0x02;
        }
        self
    }
    pub fn bus_accessible_with_penalty(mut self, v: bool) -> Self {
        if v {
            self.inner.b4 |= 0x08;
        }
        self
    }
    pub fn ferry_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b4 |= 0x10;
        }
        self
    }
    pub fn incline_down(mut self, v: bool) -> Self {
        if v {
            self.inner.b4 |= 0x04;
        }
        self
    }
    pub fn has_toll(mut self, v: bool) -> Self {
        if v {
            self.inner.b2 |= 0x40;
        }
        self
    }

    pub fn build(self) -> WayProperties {
        self.inner
    }
}

/// Node properties packed into 3 bytes.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct NodeProperties {
    /// byte 0: from_level(6), foot(1), bike(1)
    b0: u8,
    /// byte 1: car(1), bus(1), elevator(1), entrance(1), multi_level(1), parking(1)
    b1: u8,
    /// byte 2: to_level(6), bus_with_penalty(1)
    b2: u8,
}

impl NodeProperties {
    pub fn is_foot_accessible(&self) -> bool {
        self.b0 & 0x40 != 0
    }
    pub fn is_bike_accessible(&self) -> bool {
        self.b0 & 0x80 != 0
    }
    pub fn is_car_accessible(&self) -> bool {
        self.b1 & 0x01 != 0
    }
    pub fn is_bus_accessible(&self) -> bool {
        self.b1 & 0x02 != 0
    }
    pub fn is_elevator(&self) -> bool {
        self.b1 & 0x04 != 0
    }
    pub fn is_entrance(&self) -> bool {
        self.b1 & 0x08 != 0
    }
    pub fn is_multi_level(&self) -> bool {
        self.b1 & 0x10 != 0
    }
    pub fn is_parking(&self) -> bool {
        self.b1 & 0x20 != 0
    }
    pub fn from_level(&self) -> Level {
        Level(self.b0 & 0x3F)
    }
    pub fn to_level(&self) -> Level {
        Level(self.b2 & 0x3F)
    }
    pub fn is_bus_accessible_with_penalty(&self) -> bool {
        self.b2 & 0x40 != 0
    }
}

const _: () = assert!(std::mem::size_of::<NodeProperties>() == 3);

#[derive(Default)]
pub struct NodePropertiesBuilder {
    inner: NodeProperties,
}

impl NodePropertiesBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn foot_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x40;
        }
        self
    }
    pub fn bike_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b0 |= 0x80;
        }
        self
    }
    pub fn car_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x01;
        }
        self
    }
    pub fn bus_accessible(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x02;
        }
        self
    }
    pub fn elevator(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x04;
        }
        self
    }
    pub fn entrance(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x08;
        }
        self
    }
    pub fn from_level(mut self, lvl: Level) -> Self {
        self.inner.b0 = (self.inner.b0 & !0x3F) | (lvl.0 & 0x3F);
        self
    }
    pub fn to_level(mut self, lvl: Level) -> Self {
        self.inner.b2 = (self.inner.b2 & !0x3F) | (lvl.0 & 0x3F);
        self
    }
    pub fn bus_accessible_with_penalty(mut self, v: bool) -> Self {
        if v {
            self.inner.b2 |= 0x40;
        }
        self
    }
    pub fn parking(mut self, v: bool) -> Self {
        if v {
            self.inner.b1 |= 0x20;
        }
        self
    }
    pub fn build(self) -> NodeProperties {
        self.inner
    }
}

/// A turn restriction representing "no" or "only" access patterns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Restriction {
    pub is_only: bool,
    pub from: OsmWayIdx,
    pub to: OsmWayIdx,
    pub via: NodeIdx,
    pub applies_to_bus: bool,
}

/// The core routing graph, mirroring OSR's ways::routing struct.
///
/// All data is stored in contiguous arrays indexed by strong types.
/// The way_nodes/node_ways VecVecs form a bidirectional adjacency structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingGraph {
    pub node_properties: Vec<NodeProperties>,
    pub way_properties: Vec<WayProperties>,

    pub way_nodes: VecVec<NodeIdx>,
    pub way_node_dist: VecVec<u16>,

    pub node_ways: VecVec<WayIdx>,
    pub node_in_way_idx: VecVec<u16>,

    pub node_is_restricted: Vec<bool>,
    pub node_restrictions: VecVec<Restriction>,

    pub node_positions: Vec<Point>,
    pub way_component: Vec<ComponentIdx>,
}

impl RoutingGraph {
    pub fn n_ways(&self) -> usize {
        self.way_properties.len()
    }

    pub fn n_nodes(&self) -> usize {
        self.node_properties.len()
    }

    pub fn get_way_node_distance(&self, way: WayIdx, segment: usize) -> DistanceT {
        let dists = &self.way_node_dist[way.idx()];
        if segment < dists.len() {
            dists[segment] as DistanceT
        } else {
            0
        }
    }

    pub fn get_node_pos(&self, node: NodeIdx) -> Point {
        self.node_positions[node.idx()]
    }
}

/// Builder for constructing a RoutingGraph from extracted OSM data.
pub struct RoutingGraphBuilder {
    node_props: Vec<NodeProperties>,
    way_props: Vec<WayProperties>,
    node_positions: Vec<Point>,

    way_node_lists: Vec<Vec<NodeIdx>>,
    way_dist_lists: Vec<Vec<u16>>,
}

impl RoutingGraphBuilder {
    pub fn new() -> Self {
        Self {
            node_props: Vec::new(),
            way_props: Vec::new(),
            node_positions: Vec::new(),
            way_node_lists: Vec::new(),
            way_dist_lists: Vec::new(),
        }
    }

    pub fn add_node(&mut self, pos: Point, props: NodeProperties) -> NodeIdx {
        let idx = NodeIdx(self.node_props.len() as u32);
        self.node_props.push(props);
        self.node_positions.push(pos);
        idx
    }

    pub fn add_way(
        &mut self,
        props: WayProperties,
        nodes: Vec<NodeIdx>,
        distances: Vec<u16>,
    ) -> WayIdx {
        let idx = WayIdx(self.way_props.len() as u32);
        self.way_props.push(props);
        self.way_node_lists.push(nodes);
        self.way_dist_lists.push(distances);
        idx
    }

    pub fn build(self) -> RoutingGraph {
        let n_ways = self.way_props.len();
        let n_nodes = self.node_props.len();

        // Build way_nodes and way_node_dist VecVecs
        let mut way_nodes_builder = VecVecBuilder::new(n_ways);
        let mut way_dist_builder = VecVecBuilder::new(n_ways);
        for (w, (nodes, dists)) in self
            .way_node_lists
            .iter()
            .zip(self.way_dist_lists.iter())
            .enumerate()
        {
            way_nodes_builder.extend(w, nodes.iter().copied());
            way_dist_builder.extend(w, dists.iter().copied());
        }

        // Build the reverse index: node_ways and node_in_way_idx
        let mut node_ways_builder = VecVecBuilder::new(n_nodes);
        let mut node_in_way_builder = VecVecBuilder::new(n_nodes);
        for (w, nodes) in self.way_node_lists.iter().enumerate() {
            for (pos_in_way, node) in nodes.iter().enumerate() {
                node_ways_builder.push(node.idx(), WayIdx(w as u32));
                node_in_way_builder.push(node.idx(), pos_in_way as u16);
            }
        }

        RoutingGraph {
            node_properties: self.node_props,
            way_properties: self.way_props,
            way_nodes: way_nodes_builder.build(),
            way_node_dist: way_dist_builder.build(),
            node_ways: node_ways_builder.build(),
            node_in_way_idx: node_in_way_builder.build(),
            node_is_restricted: vec![false; n_nodes],
            node_restrictions: VecVecBuilder::<Restriction>::new(n_nodes).build(),
            node_positions: self.node_positions,
            way_component: vec![ComponentIdx(0); n_ways],
        }
    }
}
