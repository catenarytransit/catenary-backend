use super::dijkstra::*;
use super::profiles::*;
use super::transit_graph::*;
use super::types::*;
use super::ways::*;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Existing transit test (kept as-is)
// ---------------------------------------------------------------------------

#[test]
fn test_transit_partition_serialization() {
    let partition = TransitPartition {
        partition_id: 1,
        stops: vec![TransitStop {
            id: 0,
            lat: 34.0,
            lon: -118.0,
            chateau_idx: 0,
            station_id: "stop_1".to_string(),
            gtfs_stop_ids: vec!["stop_1".to_string()],
            is_hub: true,
            is_border: false,
            is_external_gateway: false,
            is_long_distance: false,
        }],
        trip_patterns: vec![TripPattern {
            chateau_idx: 0,
            route_id: "route_1".to_string(),
            direction_pattern_idx: 0,
            trips: vec![CompressedTrip {
                gtfs_trip_id: "trip_1".to_string(),
                service_mask: 1,
                start_time: 3600,
                time_delta_idx: 0,
                service_idx: 0,
                bikes_allowed: 1,
                wheelchair_accessible: 1,
            }],
            timezone_idx: 0,
            route_type: 3,
            is_border: false,
        }],
        time_deltas: vec![],
        direction_patterns: vec![DirectionPattern {
            stop_indices: vec![0],
        }],
        internal_transfers: vec![],
        osm_links: vec![],
        service_ids: vec!["c_12345".to_string(), "c_67890".to_string()],
        service_exceptions: vec![ServiceException {
            service_idx: 0,
            added_dates: vec![20231225],
            removed_dates: vec![20230101],
        }],
        _deprecated_external_transfers: vec![],
        local_dag: HashMap::new(),
        long_distance_trip_patterns: vec![],
        timezones: vec!["UTC".to_string()],
        boundary: None,
        chateau_ids: vec!["test_chateau".to_string()],
        external_hubs: vec![],
        long_distance_transfer_patterns: vec![],
        direct_connections_index: HashMap::new(),
    };

    let config = bincode::config::standard();
    let encoded = bincode::serde::encode_to_vec(&partition, config).expect("Failed to encode");
    let (decoded, _): (TransitPartition, usize) =
        bincode::serde::decode_from_slice(&encoded, config).expect("Failed to decode");

    assert_eq!(decoded.partition_id, 1);
    assert_eq!(decoded.service_ids.len(), 2);
    assert_eq!(decoded.service_ids[0], "c_12345");
    assert_eq!(decoded.service_exceptions.len(), 1);
    assert_eq!(decoded.service_exceptions[0].added_dates[0], 20231225);
    assert_eq!(decoded.trip_patterns.len(), 1);
    assert_eq!(decoded.trip_patterns[0].trips[0].bikes_allowed, 1);
    assert_eq!(decoded.trip_patterns[0].trips[0].wheelchair_accessible, 1);
}

// ---------------------------------------------------------------------------
// OSR-style tests: types
// ---------------------------------------------------------------------------

#[test]
fn test_vecvec_lookup() {
    let mut builder = VecVecBuilder::<u32>::new(3);
    builder.push(0, 10);
    builder.push(0, 20);
    builder.push(1, 30);
    builder.extend(2, [40, 50, 60]);
    let vv = builder.build();

    assert_eq!(vv.len(), 3);
    assert_eq!(vv.get(0), &[10, 20]);
    assert_eq!(vv.get(1), &[30]);
    assert_eq!(vv[2], [40, 50, 60]);
    assert_eq!(vv.get_range(0), 0..2);
    assert_eq!(vv.get_range(1), 2..3);
}

#[test]
fn test_direction_flip() {
    assert_eq!(Direction::Forward.opposite(), Direction::Backward);
    assert_eq!(Direction::Backward.opposite(), Direction::Forward);

    // flip(Forward) is identity
    assert_eq!(
        Direction::Forward.flip(Direction::Forward),
        Direction::Forward
    );
    assert_eq!(
        Direction::Backward.flip(Direction::Forward),
        Direction::Backward
    );

    // flip(Backward) inverts
    assert_eq!(
        Direction::Forward.flip(Direction::Backward),
        Direction::Backward
    );
    assert_eq!(
        Direction::Backward.flip(Direction::Backward),
        Direction::Forward
    );
}

#[test]
fn test_speed_limit_conversion() {
    assert_eq!(SpeedLimit::Kmh10.to_kmh(), 10);
    assert_eq!(SpeedLimit::Kmh50.to_kmh(), 50);
    assert_eq!(SpeedLimit::Kmh120.to_kmh(), 120);

    let spm = SpeedLimit::Kmh50.to_seconds_per_meter();
    let expected = 3.6f32 / 50.0;
    assert!((spm - expected).abs() < 1e-6);

    assert_eq!(SpeedLimit::from_kmh(45), SpeedLimit::Kmh30);
    assert_eq!(SpeedLimit::from_kmh(50), SpeedLimit::Kmh50);
    assert_eq!(SpeedLimit::from_kmh(130), SpeedLimit::Kmh120);
    assert_eq!(SpeedLimit::from_kmh(5), SpeedLimit::Kmh10);
}

#[test]
fn test_level_encoding() {
    let no_level = Level::NO_LEVEL;
    assert_eq!(no_level.0, 0);
    assert_eq!(no_level.to_float(), 0.0);

    let ground = Level::from_float(0.0);
    assert!((ground.to_float() - 0.0).abs() < 0.01);

    let basement = Level::from_float(-1.0);
    assert!((basement.to_float() - (-1.0)).abs() < 0.01);

    let upper = Level::from_float(3.0);
    assert!((upper.to_float() - 3.0).abs() < 0.01);
}

// ---------------------------------------------------------------------------
// OSR-style tests: ways
// ---------------------------------------------------------------------------

#[test]
fn test_way_properties_bitfield() {
    assert_eq!(std::mem::size_of::<WayProperties>(), 5);
    assert_eq!(std::mem::size_of::<NodeProperties>(), 3);

    let wp = WayPropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .oneway_car(true)
        .speed_limit(SpeedLimit::Kmh80)
        .bus_accessible(true)
        .big_street(true)
        .build();

    assert!(wp.is_foot_accessible());
    assert!(!wp.is_bike_accessible());
    assert!(wp.is_car_accessible());
    assert!(wp.is_oneway_car());
    assert_eq!(wp.speed_limit().to_kmh(), 80);
    assert!(wp.is_bus_accessible());
    assert!(wp.is_big_street());
    assert!(!wp.is_elevator());
    assert!(wp.is_accessible());
}

#[test]
fn test_node_properties_bitfield() {
    let np = NodePropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .bus_accessible(true)
        .elevator(true)
        .build();

    assert!(np.is_foot_accessible());
    assert!(np.is_car_accessible());
    assert!(np.is_bus_accessible());
    assert!(np.is_elevator());
    assert!(!np.is_bike_accessible());
    assert!(!np.is_entrance());
}

// ---------------------------------------------------------------------------
// OSR-style tests: graph construction
// ---------------------------------------------------------------------------
/*
/// Builds a simple 4-node linear graph: A --100m-- B --200m-- C --150m-- D
/// All ways are foot+car+bus accessible, not oneway.

fn build_linear_graph() -> RoutingGraph {
    let mut builder = RoutingGraphBuilder::new();
    let default_pos = Point::from_latlng(49.0, 8.0);

    let node_props = NodePropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .bus_accessible(true)
        .build();

    let a = builder.add_node(Point::from_latlng(49.0, 8.0), node_props);
    let b = builder.add_node(Point::from_latlng(49.001, 8.0), node_props);
    let c = builder.add_node(Point::from_latlng(49.002, 8.0), node_props);
    let d = builder.add_node(Point::from_latlng(49.003, 8.0), node_props);

    let way_props = WayPropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .bus_accessible(true)
        .speed_limit(SpeedLimit::Kmh50)
        .build();

    builder.add_way(way_props, vec![a, b], vec![100]);
    builder.add_way(way_props, vec![b, c], vec![200]);
    builder.add_way(way_props, vec![c, d], vec![150]);

    builder.build()
} */

/*
#[test]
fn test_graph_construction() {
    let graph = build_linear_graph();

    assert_eq!(graph.n_nodes(), 4);
    assert_eq!(graph.n_ways(), 3);

    // Node B (idx 1) should be in 2 ways
    let b_ways = &graph.node_ways()[1];
    assert_eq!(b_ways.len(), 2);

    // Way 0 should have nodes [A, B]
    let way0_nodes = &graph.way_nodes()[0];
    assert_eq!(way0_nodes.len(), 2);
    assert_eq!(way0_nodes[0], NodeIdx(0));
    assert_eq!(way0_nodes[1], NodeIdx(1));

    // Distances
    assert_eq!(graph.get_way_node_distance(WayIdx(0), 0), 100);
    assert_eq!(graph.get_way_node_distance(WayIdx(1), 0), 200);
    assert_eq!(graph.get_way_node_distance(WayIdx(2), 0), 150);
}

// ---------------------------------------------------------------------------
// OSR-style tests: routing
// ---------------------------------------------------------------------------

#[test]
fn test_foot_routing_simple() {
    let graph = build_linear_graph();
    let params = FootParams::default();

    // Route A -> D (should traverse all 3 ways)
    let result = route::<FootProfile>(&params, &graph, NodeIdx(0), NodeIdx(3), 10_000);
    assert!(result.is_some());
    let path = result.unwrap();

    assert_eq!(
        path.nodes,
        vec![NodeIdx(0), NodeIdx(1), NodeIdx(2), NodeIdx(3)]
    );
    assert!(path.total_cost > 0);
    assert!(path.total_cost < 10_000);

    // Route D -> A (reverse, should also work since no oneway)
    let reverse = route::<FootProfile>(&params, &graph, NodeIdx(3), NodeIdx(0), 10_000);
    assert!(reverse.is_some());
    let rev_path = reverse.unwrap();
    assert_eq!(
        rev_path.nodes,
        vec![NodeIdx(3), NodeIdx(2), NodeIdx(1), NodeIdx(0)]
    );
    assert_eq!(rev_path.total_cost, path.total_cost);
}

*/

/*

#[test]
fn test_car_oneway() {
    let mut builder = RoutingGraphBuilder::new();

    let node_props = NodePropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .build();

    let a = builder.add_node(Point::from_latlng(49.0, 8.0), node_props);
    let b = builder.add_node(Point::from_latlng(49.001, 8.0), node_props);
    let c = builder.add_node(Point::from_latlng(49.002, 8.0), node_props);

    // A -> B: oneway
    let oneway_props = WayPropertiesBuilder::new()
        .car_accessible(true)
        .oneway_car(true)
        .speed_limit(SpeedLimit::Kmh50)
        .build();
    builder.add_way(oneway_props, vec![a, b], vec![100]);

    // B -> C: bidirectional
    let bidir_props = WayPropertiesBuilder::new()
        .car_accessible(true)
        .speed_limit(SpeedLimit::Kmh50)
        .build();
    builder.add_way(bidir_props, vec![b, c], vec![100]);

    let graph = builder.build();
    let params = CarParams;

    // A -> C: should work (follow oneway direction)
    let fwd = route::<CarProfile>(&params, &graph, NodeIdx(0), NodeIdx(2), 10_000);
    assert!(fwd.is_some());

    // C -> A: should fail (blocked by oneway A->B)
    let rev = route::<CarProfile>(&params, &graph, NodeIdx(2), NodeIdx(0), 10_000);
    assert!(rev.is_none());
}

#[test]
fn test_bus_vs_car_gate() {
    let mut builder = RoutingGraphBuilder::new();

    let open_node = NodePropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .bus_accessible(true)
        .build();

    // Gate node: bus can pass, car cannot
    let gate_node = NodePropertiesBuilder::new()
        .foot_accessible(true)
        .bus_accessible(true)
        .build();

    let a = builder.add_node(Point::from_latlng(49.0, 8.0), open_node);
    let gate = builder.add_node(Point::from_latlng(49.001, 8.0), gate_node);
    let b = builder.add_node(Point::from_latlng(49.002, 8.0), open_node);

    let way_props = WayPropertiesBuilder::new()
        .car_accessible(true)
        .bus_accessible(true)
        .foot_accessible(true)
        .speed_limit(SpeedLimit::Kmh50)
        .build();

    builder.add_way(way_props, vec![a, gate], vec![100]);
    builder.add_way(way_props, vec![gate, b], vec![100]);
    let graph = builder.build();

    // Bus should pass through the gate
    let bus_result = route::<BusProfile>(&BusParams, &graph, NodeIdx(0), NodeIdx(2), 10_000);
    assert!(bus_result.is_some());

    // Car should be blocked at the gate
    let car_result = route::<CarProfile>(&CarParams, &graph, NodeIdx(0), NodeIdx(2), 10_000);
    assert!(car_result.is_none());
}

#[test]
fn test_dijkstra_early_termination() {
    let graph = build_linear_graph();
    let params = FootParams::default();

    let mut router = DijkstraRouter::new(10_000);
    router.add_start(NodeIdx(0), Level::NO_LEVEL);

    // Only search for node B (idx 1), should terminate before exploring C and D
    let completed = router.run::<FootProfile>(
        &params,
        &graph,
        10_000,
        Direction::Forward,
        Some(&[NodeIdx(1)]),
    );

    // Should have found B
    let b_cost = router.get_cost(NodeIdx(1), Level::NO_LEVEL);
    assert!(b_cost < INFEASIBLE);

    // B's path should be A -> B
    let path = router.reconstruct_path(NodeIdx(1), Level::NO_LEVEL);
    assert!(path.is_some());
    assert_eq!(path.unwrap().nodes, vec![NodeIdx(0), NodeIdx(1)]);
}

#[test]
fn test_disconnected_graph() {
    let mut builder = RoutingGraphBuilder::new();

    let node_props = NodePropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .build();

    // Two disconnected components
    let a = builder.add_node(Point::from_latlng(49.0, 8.0), node_props);
    let b = builder.add_node(Point::from_latlng(49.001, 8.0), node_props);
    let c = builder.add_node(Point::from_latlng(49.1, 8.1), node_props);
    let d = builder.add_node(Point::from_latlng(49.101, 8.1), node_props);

    let way_props = WayPropertiesBuilder::new()
        .foot_accessible(true)
        .car_accessible(true)
        .speed_limit(SpeedLimit::Kmh50)
        .build();

    builder.add_way(way_props, vec![a, b], vec![100]);
    builder.add_way(way_props, vec![c, d], vec![100]);
    let graph = builder.build();

    let params = FootParams::default();

    // A -> B should work
    assert!(route::<FootProfile>(&params, &graph, NodeIdx(0), NodeIdx(1), 10_000).is_some());
    // A -> C should fail (disconnected)
    assert!(route::<FootProfile>(&params, &graph, NodeIdx(0), NodeIdx(2), 10_000).is_none());
    // C -> D should work
    assert!(route::<FootProfile>(&params, &graph, NodeIdx(2), NodeIdx(3), 10_000).is_some());
}

#[test]
fn test_lookup_spatial_index() {
    let graph = build_linear_graph();
    let lookup = super::lookup::Lookup::build(&graph);

    // Query near node A (49.0, 8.0) — should find way candidates
    let candidates = lookup.match_location(&graph, 49.0, 8.0, 500.0);
    assert!(!candidates.is_empty());

    // Query far away — should find nothing
    let far = lookup.match_location(&graph, 0.0, 0.0, 100.0);
    assert!(far.is_empty());
}
     */

/*
#[test]
fn test_integration_ballwil_shortcut() {
    let path = "test/ballwill-shortcut.osm.pbf";
    if !std::path::Path::new(path).exists() {
        println!("Skipping integration test: test PBF not found.");
        return;
    }

    let graph = crate::routing_common::extract::load_osm_pbf(path);
    println!(
        "Graph loaded: {} nodes, {} ways",
        graph.n_nodes(),
        graph.n_ways()
    );

    let lookup = super::lookup::Lookup::build(&graph);

    let from_matches = lookup.match_location(&graph, 47.15570087672, 8.315615519882, 1000.0);
    let to_matches = lookup.match_location(&graph, 47.15347392750, 8.316863681682, 1000.0);

    assert!(
        !from_matches.is_empty(),
        "Could not find start node in spatial index. Graph bounds: {} nodes, {} ways",
        graph.n_nodes(),
        graph.n_ways()
    );
    assert!(
        !to_matches.is_empty(),
        "Could not find destination node in spatial index"
    );

    let params = CarParams::default();

    // We try to route from the closest node of the first match to the closest node of the second match
    let from_node = from_matches[0].left.as_ref().unwrap().node;
    let to_node = to_matches[0].left.as_ref().unwrap().node;

    println!("Routing from node {} to {}", from_node.0, to_node.0);

    let result = route::<CarProfile>(&params, &graph, from_node, to_node, 10_000);
    assert!(result.is_some(), "No route found between the points");

    let path = result.unwrap();
    println!(
        "Route path: cost={}, len={}",
        path.total_cost,
        path.nodes.len()
    );
    assert!(path.total_cost > 0, "Route cost should be positive");
}
*/
