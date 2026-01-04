#[cfg(test)]
mod tests {
    use crate::corridor::{CorridorCluster, CorridorId};
    use crate::osm_types::{OsmNodeId, RailMode, ZClass};
    use crate::support_graph::{SupportGraph, SupportGraphConfig};
    use ahash::{HashSet, HashSetExt};

    #[test]
    fn test_process_corridor_splitting_creates_edges() {
        // Setup
        let mut graph = SupportGraph::new();
        let config = SupportGraphConfig::default();

        // Create a simple corridor
        let centerline = vec![(0.0, 0.0), (0.001, 0.001)]; // diagonals approx 150m
        let corridor = CorridorCluster {
            id: CorridorId(1),
            member_edges: vec![],
            centerline: centerline.clone(),
            z_class: ZClass::GROUND,
            mode: RailMode::Rail,
            length_m: 150.0,
            is_mixed_z: false,
        };

        // Pass 1: Create nodes (Endpoints)
        let start = centerline[0];
        let end = centerline[1];
        let empty_ids = HashSet::new();
        graph.find_or_create_node(start, ZClass::GROUND, 5.0, &empty_ids);
        graph.find_or_create_node(end, ZClass::GROUND, 5.0, &empty_ids);

        assert_eq!(graph.nodes.len(), 2, "Pass 1 should create 2 nodes");

        // Pass 2: Split
        graph.process_corridor_splitting(&corridor, &config, None);

        // Assert
        assert!(!graph.edges.is_empty(), "Pass 2 should create edges");
        assert_eq!(
            graph.edges.len(),
            1,
            "Should create 1 edge for simple segment"
        );
    }

    #[test]
    fn test_z_class_connectivity() {
        // Setup
        let mut graph = SupportGraph::new();

        // Create two corridors that meet at a point
        // Corridor A: Ground, (0.0, 0.0) -> (0.01, 0.0)
        // Corridor B: Bridge, (0.01, 0.0) -> (0.02, 0.0)
        let junction = (0.01, 0.0);

        // Pass 1: Create nodes at endpoints
        let mut ids_a = HashSet::new();
        ids_a.insert(OsmNodeId(100)); // Shared ID

        let mut ids_b = HashSet::new();
        ids_b.insert(OsmNodeId(100)); // Shared ID

        // End of A
        graph.find_or_create_node(junction, ZClass::GROUND, 5.0, &ids_a);
        // Start of B (Same location, different Z)
        let node_b = graph.find_or_create_node(junction, ZClass::BRIDGE, 5.0, &ids_b);

        // We expect them to reuse the same node ID because they are at the same location
        // The current implementation likely creates two different nodes because of strict Z-class check.

        let nodes_at_junction: Vec<_> = graph
            .nodes
            .values()
            .filter(|n| {
                (n.position.0 - junction.0).abs() < 1e-6 && (n.position.1 - junction.1).abs() < 1e-6
            })
            .collect();

        assert_eq!(
            nodes_at_junction.len(),
            1,
            "Should have merged nodes at junction despite Z-class difference"
        );

        // Test non-merge without shared ID
        let junction2 = (0.02, 0.0);
        let mut ids_c = HashSet::new();
        ids_c.insert(OsmNodeId(200));
        let mut ids_d = HashSet::new();
        ids_d.insert(OsmNodeId(201)); // Different ID

        graph.find_or_create_node(junction2, ZClass::GROUND, 5.0, &ids_c);
        graph.find_or_create_node(junction2, ZClass::BRIDGE, 5.0, &ids_d);

        let nodes_at_junction2: Vec<_> = graph
            .nodes
            .values()
            .filter(|n| {
                (n.position.0 - junction2.0).abs() < 1e-6
                    && (n.position.1 - junction2.1).abs() < 1e-6
            })
            .collect();

        assert_eq!(
            nodes_at_junction2.len(),
            2,
            "Should NOT have merged nodes if OSM IDs disjoint and Z different"
        );
    }
}
