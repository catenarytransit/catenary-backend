use crate::routing_common::types::*;
use bytemuck::{Pod, Zeroable, cast_slice};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

/// Way properties packed into 5 bytes, mirroring OSR's way_properties bitfield layout.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, Pod, Zeroable)]
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
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, Pod, Zeroable)]
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

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Restriction {
    pub is_only: u8,
    pub applies_to_bus: u8,
    pub _padding1: [u8; 6],
    pub from: OsmWayIdx,
    pub to: OsmWayIdx,
    pub via: NodeIdx,
    pub _padding2: [u8; 4],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct GraphHeader {
    pub magic: u32,
    pub version: u32,
    pub n_nodes: u64,
    pub n_ways: u64,
    pub offset_node_properties: u64,
    pub offset_way_properties: u64,
    pub offset_way_nodes_bucket: u64,
    pub offset_way_nodes_data: u64,
    pub offset_way_node_dist_bucket: u64,
    pub offset_way_node_dist_data: u64,
    pub offset_node_ways_bucket: u64,
    pub offset_node_ways_data: u64,
    pub offset_node_in_way_idx_bucket: u64,
    pub offset_node_in_way_idx_data: u64,
    pub offset_node_is_restricted: u64,
    pub offset_node_restrictions_bucket: u64,
    pub offset_node_restrictions_data: u64,
    pub offset_node_positions: u64,
    pub offset_way_component: u64,
}

pub struct RoutingGraph {
    pub mmap: Arc<Mmap>,
    pub header: GraphHeader,
}

impl RoutingGraph {
    pub fn load(mmap: Arc<Mmap>) -> Self {
        let header: GraphHeader =
            *bytemuck::from_bytes(&mmap[0..std::mem::size_of::<GraphHeader>()]);
        assert_eq!(header.magic, 0x4F535247); // 'OSRG'
        Self { mmap, header }
    }

    fn slice<T: Pod>(&self, offset: u64, len: u64) -> &[T] {
        if len == 0 {
            return &[];
        }
        let start = offset as usize;
        let end = start + (len as usize * std::mem::size_of::<T>());
        cast_slice(&self.mmap[start..end])
    }

    pub fn node_properties(&self) -> &[NodeProperties] {
        self.slice(self.header.offset_node_properties, self.header.n_nodes)
    }

    pub fn way_properties(&self) -> &[WayProperties] {
        self.slice(self.header.offset_way_properties, self.header.n_ways)
    }

    pub fn node_positions(&self) -> &[Point] {
        self.slice(self.header.offset_node_positions, self.header.n_nodes)
    }

    pub fn way_component(&self) -> &[ComponentIdx] {
        self.slice(self.header.offset_way_component, self.header.n_ways)
    }

    pub fn node_is_restricted(&self) -> &[u8] {
        self.slice(self.header.offset_node_is_restricted, self.header.n_nodes)
    }

    pub fn way_nodes(&self) -> MappedVecVec<'_, NodeIdx> {
        let bucket_starts = self.slice(self.header.offset_way_nodes_bucket, self.header.n_ways + 1);
        let len = bucket_starts.last().copied().unwrap_or(0);
        let data = self.slice(self.header.offset_way_nodes_data, len);
        MappedVecVec {
            bucket_starts,
            data,
        }
    }

    pub fn way_node_dist(&self) -> MappedVecVec<'_, u16> {
        let bucket_starts = self.slice(
            self.header.offset_way_node_dist_bucket,
            self.header.n_ways + 1,
        );
        let len = bucket_starts.last().copied().unwrap_or(0);
        let data = self.slice(self.header.offset_way_node_dist_data, len);
        MappedVecVec {
            bucket_starts,
            data,
        }
    }

    pub fn node_ways(&self) -> MappedVecVec<'_, WayIdx> {
        let bucket_starts =
            self.slice(self.header.offset_node_ways_bucket, self.header.n_nodes + 1);
        let len = bucket_starts.last().copied().unwrap_or(0);
        let data = self.slice(self.header.offset_node_ways_data, len);
        MappedVecVec {
            bucket_starts,
            data,
        }
    }

    pub fn node_in_way_idx(&self) -> MappedVecVec<'_, u16> {
        let bucket_starts = self.slice(
            self.header.offset_node_in_way_idx_bucket,
            self.header.n_nodes + 1,
        );
        let len = bucket_starts.last().copied().unwrap_or(0);
        let data = self.slice(self.header.offset_node_in_way_idx_data, len);
        MappedVecVec {
            bucket_starts,
            data,
        }
    }

    pub fn node_restrictions(&self) -> MappedVecVec<'_, Restriction> {
        let bucket_starts = self.slice(
            self.header.offset_node_restrictions_bucket,
            self.header.n_nodes + 1,
        );
        let len = bucket_starts.last().copied().unwrap_or(0);
        let data = self.slice(self.header.offset_node_restrictions_data, len);
        MappedVecVec {
            bucket_starts,
            data,
        }
    }

    pub fn n_ways(&self) -> usize {
        self.header.n_ways as usize
    }

    pub fn n_nodes(&self) -> usize {
        self.header.n_nodes as usize
    }

    pub fn get_way_node_distance(&self, way: WayIdx, segment: usize) -> DistanceT {
        let dists = self.way_node_dist();
        let slice = dists.get(way.idx());
        if segment < slice.len() {
            slice[segment] as DistanceT
        } else {
            0
        }
    }

    pub fn get_node_pos(&self, node: NodeIdx) -> Point {
        self.node_positions()[node.idx()]
    }
}

/// Builder for constructing a RoutingGraph from extracted OSM data.
/// Writes directly to disk to prevent RAM usage spikes!
pub struct RoutingGraphBuilder {
    out_path: std::path::PathBuf,
    node_props: Vec<NodeProperties>,
    way_props: Vec<WayProperties>,
    node_positions: Vec<Point>,

    way_node_lists: Vec<Vec<NodeIdx>>,
    way_dist_lists: Vec<Vec<u16>>,
}

impl RoutingGraphBuilder {
    pub fn new(path: &Path) -> Self {
        Self {
            out_path: path.to_path_buf(),
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

    pub fn get_node_pos(&self, idx: NodeIdx) -> Point {
        self.node_positions[idx.0 as usize]
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

    pub fn build(mut self) {
        let mut file = File::create(&self.out_path).unwrap();
        let n_ways = self.way_props.len();
        let n_nodes = self.node_props.len();

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

        let mut node_ways_builder = VecVecBuilder::new(n_nodes);
        let mut node_in_way_builder = VecVecBuilder::new(n_nodes);
        for (w, nodes) in self.way_node_lists.iter().enumerate() {
            for (pos_in_way, node) in nodes.iter().enumerate() {
                node_ways_builder.push(node.idx(), WayIdx(w as u32));
                node_in_way_builder.push(node.idx(), pos_in_way as u16);
            }
        }

        let way_nodes = way_nodes_builder.build();
        let way_node_dist = way_dist_builder.build();
        let node_ways = node_ways_builder.build();
        let node_in_way_idx = node_in_way_builder.build();
        let node_is_restricted = vec![0u8; n_nodes];
        let node_restrictions = VecVecBuilder::<Restriction>::new(n_nodes).build();
        let way_component = vec![ComponentIdx(0); n_ways];

        let mut offset = std::mem::size_of::<GraphHeader>() as u64;
        let mut header = GraphHeader {
            magic: 0x4F535247,
            version: 1,
            n_nodes: n_nodes as u64,
            n_ways: n_ways as u64,
            offset_node_properties: 0,
            offset_way_properties: 0,
            offset_way_nodes_bucket: 0,
            offset_way_nodes_data: 0,
            offset_way_node_dist_bucket: 0,
            offset_way_node_dist_data: 0,
            offset_node_ways_bucket: 0,
            offset_node_ways_data: 0,
            offset_node_in_way_idx_bucket: 0,
            offset_node_in_way_idx_data: 0,
            offset_node_is_restricted: 0,
            offset_node_restrictions_bucket: 0,
            offset_node_restrictions_data: 0,
            offset_node_positions: 0,
            offset_way_component: 0,
        };

        // Write header placeholder
        file.write_all(bytemuck::bytes_of(&header)).unwrap();

        macro_rules! write_slice {
            ($field:ident, $slice:expr) => {
                header.$field = offset;
                let bytes = bytemuck::cast_slice($slice);
                file.write_all(bytes).unwrap();
                offset += bytes.len() as u64;
            };
        }

        write_slice!(offset_node_properties, &self.node_props);
        write_slice!(offset_way_properties, &self.way_props);
        write_slice!(offset_way_nodes_bucket, &way_nodes.bucket_starts);
        write_slice!(offset_way_nodes_data, &way_nodes.data);
        write_slice!(offset_way_node_dist_bucket, &way_node_dist.bucket_starts);
        write_slice!(offset_way_node_dist_data, &way_node_dist.data);
        write_slice!(offset_node_ways_bucket, &node_ways.bucket_starts);
        write_slice!(offset_node_ways_data, &node_ways.data);
        write_slice!(
            offset_node_in_way_idx_bucket,
            &node_in_way_idx.bucket_starts
        );
        write_slice!(offset_node_in_way_idx_data, &node_in_way_idx.data);
        write_slice!(offset_node_is_restricted, &node_is_restricted);
        write_slice!(
            offset_node_restrictions_bucket,
            &node_restrictions.bucket_starts
        );
        write_slice!(offset_node_restrictions_data, &node_restrictions.data);
        write_slice!(offset_node_positions, &self.node_positions);
        write_slice!(offset_way_component, &way_component);

        // Rewrite header
        file.sync_all().unwrap();
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        file.write_all(bytemuck::bytes_of(&header)).unwrap();
    }
}
