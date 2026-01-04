use crate::osm_types::{Line, LineId};
use crate::restrictions::TurnRestrictions;
use crate::support_graph::{SupportEdge, SupportEdgeId, SupportGraph, SupportNode, SupportNodeId};
use anyhow::Result;
use catenary::graph_formats::{
    LandMass, NodeId, SerializableExportGraph, SerializableGraphEdge, SerializableStop,
    SerializableStopCluster, TurnRestriction,
};
use geojson::{Feature, FeatureCollection, Geometry, JsonObject, JsonValue};
use log::info;
use serde_json::json;
use std::fs::File;
use std::io::Write;
use std::path::Path;

/// Export the support graph to GeoJSON format matching the paper's spec
pub fn export_geojson(
    graph: &SupportGraph,
    lines: &[Line],
    restrictions: &TurnRestrictions,
    output_path: &Path,
) -> Result<()> {
    info!("Exporting GeoJSON to {:?}", output_path);

    let mut features = Vec::new();

    // Build line lookup
    let line_map: std::collections::HashMap<&LineId, &Line> =
        lines.iter().map(|l| (&l.id, l)).collect();

    // Feature Collection level properties (lines array)
    let lines_array: Vec<JsonValue> = lines
        .iter()
        .map(|line| {
            json!({
                "id": line.id.to_string(),
                "label": line.label,
                "color": line.color,
                "direction": line.id.direction
            })
        })
        .collect();

    // Export nodes as Point features
    for (_, node) in &graph.nodes {
        let mut props = JsonObject::new();
        props.insert("id".to_string(), json!(node.id.0.to_string()));

        if let Some(ref label) = node.station_label {
            props.insert("station_label".to_string(), json!(label));
        }
        if let Some(ref station_id) = node.station_id {
            props.insert("station_id".to_string(), json!(station_id));
        }

        // Add excluded_conn for this node
        let exclusions = restrictions.excluded_connections_at_node(node.id, graph);
        if !exclusions.is_empty() {
            let exc_array: Vec<JsonValue> = exclusions
                .iter()
                .map(|exc| {
                    json!({
                        "node_from": exc.node_from.0.to_string(),
                        "node_to": exc.node_to.0.to_string(),
                        "line": exc.line.to_string()
                    })
                })
                .collect();
            props.insert("excluded_conn".to_string(), json!(exc_array));
        }

        let geometry = Geometry::new(geojson::Value::Point(vec![
            node.position.0,
            node.position.1,
        ]));

        features.push(Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(props),
            foreign_members: None,
        });
    }

    // Export edges as LineString features
    for (_, edge) in &graph.edges {
        let mut props = JsonObject::new();
        props.insert("from".to_string(), json!(edge.from.0.to_string()));
        props.insert("to".to_string(), json!(edge.to.0.to_string()));

        // Lines on this edge
        let edge_lines: Vec<JsonValue> = edge
            .lines
            .iter()
            .map(|occ| {
                let line_info = line_map.get(&occ.line);
                json!({
                    "id": occ.line.to_string(),
                    "label": line_info.map(|l| l.label.as_str()).unwrap_or(""),
                    "color": line_info.map(|l| l.color.as_str()).unwrap_or("888888"),
                    "direction": occ.direction_node.map(|n| n.to_string())
                })
            })
            .collect();
        props.insert("lines".to_string(), json!(edge_lines));

        let coordinates: Vec<Vec<f64>> = edge
            .geometry
            .iter()
            .map(|&(lon, lat)| vec![lon, lat])
            .collect();

        let geometry = Geometry::new(geojson::Value::LineString(coordinates));

        features.push(Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(props),
            foreign_members: None,
        });
    }

    // Create FeatureCollection with top-level properties
    let mut fc_props = JsonObject::new();
    fc_props.insert("lines".to_string(), json!(lines_array));

    let fc = FeatureCollection {
        bbox: None,
        features,
        foreign_members: Some(fc_props),
    };

    // Write to file
    let mut file = File::create(output_path)?;
    let json_str = serde_json::to_string_pretty(&fc)?;
    file.write_all(json_str.as_bytes())?;

    info!("Exported {} features to GeoJSON", fc.features.len());
    Ok(())
}

/// Export to binary format for harebell
pub fn export_binary(
    graph: &SupportGraph,
    restrictions: &TurnRestrictions,
    output_path: &Path,
) -> Result<()> {
    info!("Exporting binary format to {:?}", output_path);

    // Convert support graph to SerializableExportGraph format
    let mut edges = Vec::new();
    let mut clusters = Vec::new();

    // Map SupportEdgeId to index in the edges vector
    let mut support_edge_id_to_index: std::collections::HashMap<SupportEdgeId, usize> =
        std::collections::HashMap::new();

    // Collect station nodes as clusters
    let mut station_to_cluster: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for (_, node) in &graph.nodes {
        if let (Some(station_id), Some(label)) = (&node.station_id, &node.station_label) {
            if !station_to_cluster.contains_key(station_id) {
                let cluster_id = clusters.len();
                station_to_cluster.insert(station_id.clone(), cluster_id);

                clusters.push(SerializableStopCluster {
                    cluster_id,
                    centroid: [node.position.0, node.position.1],
                    stops: vec![SerializableStop {
                        id: station_id.clone(),
                        code: None,
                        name: Some(label.clone()),
                        description: None,
                        location_type: 1,
                        parent_station: None,
                        zone_id: None,
                        longitude: Some(node.position.0),
                        latitude: Some(node.position.1),
                        timezone: None,
                        platform_code: None,
                        level_id: None,
                        routes: vec![],
                    }],
                });
            }
        }
    }

    // Convert edges
    for (idx, (edge_id, edge)) in graph.edges.iter().enumerate() {
        support_edge_id_to_index.insert(*edge_id, idx);

        let from_node_id = convert_support_node_to_catenary(edge.from, &graph.nodes);
        let to_node_id = convert_support_node_to_catenary(edge.to, &graph.nodes);

        let geometry: Vec<[f64; 2]> = edge.geometry.iter().map(|&(lon, lat)| [lon, lat]).collect();

        let route_ids: Vec<(String, String)> = edge
            .lines
            .iter()
            .map(|occ| (occ.line.chateau.clone(), occ.line.route_id.clone()))
            .collect();

        edges.push(SerializableGraphEdge {
            from: from_node_id,
            to: to_node_id,
            geometry,
            route_ids,
            weight: edge.length_m,
            original_edge_index: None,
        });
    }

    // Build land masses (connected components)
    let land_masses = build_land_masses(&edges, &clusters);

    // TurnRestriction uses (from_edge_index, to_edge_index, route_id) format

    // Iterate over all nodes to find restrictions
    let mut turn_restrictions: Vec<TurnRestriction> = Vec::new();

    for (node_id, _) in &graph.nodes {
        let exclusions = restrictions.excluded_transitions_at_node(*node_id, graph);
        for (from_edge_id, to_edge_id, line) in exclusions {
            if let (Some(&from_idx), Some(&to_idx)) = (
                support_edge_id_to_index.get(&from_edge_id),
                support_edge_id_to_index.get(&to_edge_id),
            ) {
                turn_restrictions.push(TurnRestriction {
                    from_edge_index: from_idx,
                    to_edge_index: to_idx,
                    route_id: (line.chateau, line.route_id),
                });
            }
        }
    }

    let export = SerializableExportGraph {
        land_masses,
        edges,
        clusters,
        restrictions: turn_restrictions,
    };

    // Serialize with bincode
    let encoded = bincode::serde::encode_to_vec(&export, bincode::config::standard())?;
    std::fs::write(output_path, encoded)?;

    info!(
        "Exported {} edges, {} clusters to binary",
        export.edges.len(),
        export.clusters.len()
    );
    Ok(())
}

/// Convert SupportNodeId to catenary's NodeId format
fn convert_support_node_to_catenary(
    node_id: SupportNodeId,
    nodes: &ahash::HashMap<SupportNodeId, SupportNode>,
) -> NodeId {
    if let Some(node) = nodes.get(&node_id) {
        if node.station_id.is_some() {
            // Use cluster ID format for station nodes
            NodeId::Cluster(node_id.0 as usize)
        } else {
            // Use intersection format for non-station nodes
            NodeId::Intersection(0, node_id.0 as usize)
        }
    } else {
        NodeId::Intersection(0, node_id.0 as usize)
    }
}

/// Build land masses from connected components
fn build_land_masses(
    edges: &[SerializableGraphEdge],
    clusters: &[SerializableStopCluster],
) -> Vec<LandMass> {
    // Simple single land mass for now - could use Union-Find for true components
    let cluster_ids: Vec<usize> = clusters.iter().map(|c| c.cluster_id).collect();
    let edge_indices: Vec<usize> = (0..edges.len()).collect();

    vec![LandMass {
        id: 0,
        clusters: cluster_ids,
        edges: edge_indices,
        stop_count: clusters.iter().map(|c| c.stops.len()).sum(),
        route_count: edges
            .iter()
            .flat_map(|e| &e.route_ids)
            .collect::<std::collections::HashSet<_>>()
            .len(),
    }]
}
