use std::hash::Hash;

use crate::routing_common::types::*;
use crate::routing_common::ways::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchProfile {
    Foot,
    Wheelchair,
    Bike,
    Car,
    Bus,
    Railway,
    Ferry,
}

/// Neighbor discovered during graph expansion.
pub struct Neighbor {
    pub node: NodeIdx,
    pub level: Level,
    pub cost: CostT,
    pub distance: DistanceT,
    pub way: WayIdx,
    pub from_idx: u16,
    pub to_idx: u16,
}

/// Routing profile trait, mirrors OSR's Profile concept.
///
/// Each profile defines how costs are computed for ways and nodes,
/// and how the graph adjacency is expanded.
pub trait RoutingProfile {
    type Params: Default;

    fn way_cost(
        params: &Self::Params,
        props: &WayProperties,
        dir: Direction,
        dist: DistanceT,
    ) -> CostT;
    fn node_cost(params: &Self::Params, props: &NodeProperties) -> CostT;

    fn adjacent(
        params: &Self::Params,
        graph: &RoutingGraph,
        node: NodeIdx,
        level: Level,
        search_dir: Direction,
        out: &mut Vec<Neighbor>,
    );
}

// ---------------------------------------------------------------------------
// Foot profile
// ---------------------------------------------------------------------------

pub struct FootParams {
    pub speed_mps: f32,
    pub is_wheelchair: bool,
}

impl Default for FootParams {
    fn default() -> Self {
        Self {
            speed_mps: 1.2,
            is_wheelchair: false,
        }
    }
}

pub struct FootProfile;

impl RoutingProfile for FootProfile {
    type Params = FootParams;

    fn way_cost(
        params: &FootParams,
        props: &WayProperties,
        _dir: Direction,
        dist: DistanceT,
    ) -> CostT {
        let accessible = props.is_foot_accessible()
            || (!props.is_sidewalk_separate() && props.is_bike_accessible());
        if !accessible || (params.is_wheelchair && props.is_steps()) {
            return INFEASIBLE;
        }
        let penalty: CostT = if !props.is_foot_accessible() || props.is_sidewalk_separate() {
            90
        } else {
            0
        };
        let speed_adj = params.speed_mps
            + if props.is_big_street() { -0.2 } else { 0.0 }
            + if props.motor_vehicle_no() { 0.1 } else { 0.0 };
        penalty + (dist as f32 / speed_adj).round() as CostT
    }

    fn node_cost(params: &FootParams, props: &NodeProperties) -> CostT {
        if !props.is_foot_accessible() && !params.is_wheelchair {
            return INFEASIBLE;
        }
        if !props.is_foot_accessible() {
            return INFEASIBLE;
        }
        if props.is_elevator() {
            90
        } else {
            0
        }
    }

    fn adjacent(
        params: &FootParams,
        graph: &RoutingGraph,
        node: NodeIdx,
        _level: Level,
        search_dir: Direction,
        out: &mut Vec<Neighbor>,
    ) {
        expand_adjacent::<Self>(params, graph, node, search_dir, out);
    }
}

// ---------------------------------------------------------------------------
// Car profile
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct CarParams;

pub struct CarProfile;

impl RoutingProfile for CarProfile {
    type Params = CarParams;

    fn way_cost(
        _params: &CarParams,
        props: &WayProperties,
        dir: Direction,
        dist: DistanceT,
    ) -> CostT {
        if !props.is_car_accessible() {
            return INFEASIBLE;
        }
        if props.is_oneway_car() && dir == Direction::Backward {
            return INFEASIBLE;
        }
        (dist as f32 * props.max_speed_s_per_m()).round() as CostT
    }

    fn node_cost(_params: &CarParams, props: &NodeProperties) -> CostT {
        if props.is_car_accessible() {
            0
        } else {
            INFEASIBLE
        }
    }

    fn adjacent(
        params: &CarParams,
        graph: &RoutingGraph,
        node: NodeIdx,
        _level: Level,
        search_dir: Direction,
        out: &mut Vec<Neighbor>,
    ) {
        expand_adjacent::<Self>(params, graph, node, search_dir, out);
    }
}

// ---------------------------------------------------------------------------
// Bus profile
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct BusParams;

pub struct BusProfile;

impl RoutingProfile for BusProfile {
    type Params = BusParams;

    fn way_cost(
        _params: &BusParams,
        props: &WayProperties,
        dir: Direction,
        dist: DistanceT,
    ) -> CostT {
        let accessible = props.is_bus_accessible() || props.is_bus_accessible_with_penalty();
        if !accessible {
            return INFEASIBLE;
        }
        if props.is_oneway_bus_psv() && dir == Direction::Backward {
            return INFEASIBLE;
        }
        let penalty: CostT = if props.is_bus_accessible_with_penalty() {
            50
        } else {
            0
        };
        penalty + (dist as f32 * props.max_speed_s_per_m()).round() as CostT
    }

    fn node_cost(_params: &BusParams, props: &NodeProperties) -> CostT {
        if props.is_bus_accessible() || props.is_bus_accessible_with_penalty() {
            0
        } else {
            INFEASIBLE
        }
    }

    fn adjacent(
        params: &BusParams,
        graph: &RoutingGraph,
        node: NodeIdx,
        _level: Level,
        search_dir: Direction,
        out: &mut Vec<Neighbor>,
    ) {
        expand_adjacent::<Self>(params, graph, node, search_dir, out);
    }
}

// ---------------------------------------------------------------------------
// Shared expansion logic (mirrors OSR's adjacent template)
// ---------------------------------------------------------------------------

fn expand_adjacent<P: RoutingProfile>(
    params: &P::Params,
    graph: &RoutingGraph,
    node: NodeIdx,
    search_dir: Direction,
    out: &mut Vec<Neighbor>,
) {
    let ways = &graph.node_ways[node.idx()];
    let positions_in_way = &graph.node_in_way_idx[node.idx()];

    for (way, &pos_in_way) in ways.iter().zip(positions_in_way.iter()) {
        let way_nodes = &graph.way_nodes[way.idx()];
        let i = pos_in_way as usize;

        // Expand backward edge (to node at i-1)
        if i > 0 {
            let dir = Direction::Backward.flip(search_dir);
            expand_edge::<P>(params, graph, *way, i, i - 1, dir, search_dir, out);
        }
        // Expand forward edge (to node at i+1)
        if i + 1 < way_nodes.len() {
            let dir = Direction::Forward.flip(search_dir);
            expand_edge::<P>(params, graph, *way, i, i + 1, dir, search_dir, out);
        }
    }
}

fn expand_edge<P: RoutingProfile>(
    params: &P::Params,
    graph: &RoutingGraph,
    way: WayIdx,
    from: usize,
    to: usize,
    way_dir: Direction,
    _search_dir: Direction,
    out: &mut Vec<Neighbor>,
) {
    let way_nodes = &graph.way_nodes[way.idx()];
    let target_node = way_nodes[to];

    let target_node_props = graph.node_properties[target_node.idx()];
    if P::node_cost(params, &target_node_props) == INFEASIBLE {
        return;
    }

    let way_props = graph.way_properties[way.idx()];
    let seg = from.min(to);
    let dist = graph.get_way_node_distance(way, seg);

    let way_cost = P::way_cost(params, &way_props, way_dir, dist);
    if way_cost == INFEASIBLE {
        return;
    }

    let node_cost = P::node_cost(params, &target_node_props);
    let total = way_cost.saturating_add(node_cost);

    out.push(Neighbor {
        node: target_node,
        level: Level::NO_LEVEL,
        cost: total,
        distance: dist,
        way,
        from_idx: from as u16,
        to_idx: to as u16,
    });
}
