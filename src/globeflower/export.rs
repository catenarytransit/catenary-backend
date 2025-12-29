use crate::clustering::StopCluster;
use crate::edges::{GraphEdge, NodeId};
use anyhow::Result;
use catenary::graph_formats::{
    LandMass, NodeId as SerialNodeId, SerializableExportGraph, SerializableGraphEdge,
    SerializableStop, SerializableStopCluster, TurnRestriction,
};
use catenary::models::Stop;
use geojson::{Feature, FeatureCollection, GeoJson, Geometry, JsonObject, JsonValue, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufWriter;

pub fn extract_and_export(
    clusters: &[StopCluster],
    edges: &[GraphEdge],
    restrictions: Vec<TurnRestriction>,
    export_geojson: bool,
) -> Result<()> {
    // 1. Calculate Land Masses (using internal types)
    let land_masses = extract_land_masses(clusters, edges);

    println!("Identified {} distinct land masses.", land_masses.len());

    // 2. Export GeoJSON (Debug) - Optional
    if export_geojson {
        export_to_geojson(clusters, edges)?;
    }

    // 3. Convert to Serializable Models
    let serial_clusters: Vec<SerializableStopCluster> = clusters
        .iter()
        .map(|c| SerializableStopCluster {
            cluster_id: c.cluster_id,
            centroid: [c.centroid.x, c.centroid.y],
            stops: c.stops.iter().map(convert_stop).collect(),
        })
        .collect();

    let serial_edges: Vec<SerializableGraphEdge> = edges
        .iter()
        .map(|e| SerializableGraphEdge {
            from: convert_node_id(e.from),
            to: convert_node_id(e.to),
            geometry: e.geometry.points.iter().map(|p| [p.x, p.y]).collect(),
            route_ids: e.routes.iter().map(|(c, r, _)| (c.clone(), r.clone())).collect(),
            weight: e.weight,
            original_edge_index: e.original_edge_index,
        })
        .collect();

    let export_graph = SerializableExportGraph {
        land_masses, // LandMass is shared now
        edges: serial_edges,
        clusters: serial_clusters,
        restrictions,
    };

    let file = File::create("globeflower_graph.bin")?;
    let mut writer = BufWriter::new(file);
    bincode::serde::encode_into_std_write(&export_graph, &mut writer, bincode::config::legacy())
        .map_err(|e| anyhow::anyhow!("Bincode serialization failed: {}", e))?;
    println!("Exported binary graph to globeflower_graph.bin");

    Ok(())
}

fn convert_node_id(id: NodeId) -> SerialNodeId {
    match id {
        NodeId::Cluster(i) => SerialNodeId::Cluster(i),
        NodeId::Intersection(i) => SerialNodeId::Intersection(i),
    }
}

fn convert_stop(s: &Stop) -> SerializableStop {
    SerializableStop {
        id: s.gtfs_id.clone(),
        code: s.code.clone(),
        name: s.name.clone(),
        description: s.gtfs_desc.clone(),
        location_type: s.location_type,
        parent_station: s.parent_station.clone(),
        zone_id: s.zone_id.clone(),
        longitude: s.point.as_ref().map(|p| p.x),
        latitude: s.point.as_ref().map(|p| p.y),
        timezone: s.timezone.clone(),
        platform_code: s.platform_code.clone(),
        level_id: s.level_id.clone(),
        routes: s.routes.iter().flatten().cloned().collect(),
    }
}

fn export_to_geojson(clusters: &[StopCluster], edges: &[GraphEdge]) -> Result<()> {
    let mut features = Vec::new();

    // Export Edges
    for edge in edges {
        let geometry = Geometry::new(Value::LineString(
            edge.geometry
                .points
                .iter()
                .map(|p| {
                    vec![
                        (p.x * 100_000_000.0).round() / 100_000_000.0,
                        (p.y * 100_000_000.0).round() / 100_000_000.0,
                    ]
                })
                .collect(),
        ));

        let mut properties = JsonObject::new();
        // Minimal metadata for visual debugging
        properties.insert("weight".to_string(), JsonValue::from(edge.weight));
        properties.insert(
            "route_ids".to_string(),
            JsonValue::from(
                edge.routes
                    .iter()
                    .map(|(a, b, _)| format!("{}:{}", a, b))
                    .collect::<Vec<_>>(),
            ),
        );

        features.push(Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(properties),
            foreign_members: None,
        });
    }

    // Export Clusters
    for cluster in clusters {
        let geometry = Geometry::new(Value::Point(vec![
            (cluster.centroid.x * 100_000_000.0).round() / 100_000_000.0,
            (cluster.centroid.y * 100_000_000.0).round() / 100_000_000.0,
        ]));

        let mut properties = JsonObject::new();
        properties.insert(
            "cluster_id".to_string(),
            JsonValue::from(cluster.cluster_id as u64),
        );

        features.push(Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(properties),
            foreign_members: None,
        });
    }

    let collection = FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    };

    let geojson = GeoJson::FeatureCollection(collection);
    let file = File::create("globeflower_graph.geojson")?;
    let mut writer = BufWriter::new(file);
    use std::io::Write;
    writer.write_all(geojson.to_string().as_bytes())?;
    println!("Exported graph to globeflower_graph.geojson");
    Ok(())
}

fn extract_land_masses(clusters: &[StopCluster], edges: &[GraphEdge]) -> Vec<LandMass> {
    let mut adj: Vec<Vec<usize>> = vec![vec![]; clusters.len()];
    for (i, edge) in edges.iter().enumerate() {
        if let NodeId::Cluster(u) = edge.from {
            if let NodeId::Cluster(v) = edge.to {
                adj[u].push(i);
                adj[v].push(i);
            }
        }
    }

    let mut visited_clusters = vec![false; clusters.len()];
    let mut land_masses = Vec::new();
    let mut mass_id_counter = 0;

    for i in 0..clusters.len() {
        if !visited_clusters[i] {
            let mut component_clusters = Vec::new();
            let mut component_edges = HashSet::new();
            let mut stack = vec![i];
            visited_clusters[i] = true;

            while let Some(u) = stack.pop() {
                component_clusters.push(u);

                for &edge_idx in &adj[u] {
                    component_edges.insert(edge_idx);
                    let edge = &edges[edge_idx];

                    let v_opt = match (edge.from, edge.to) {
                        (NodeId::Cluster(c1), NodeId::Cluster(c2)) => {
                            if c1 == u {
                                Some(c2)
                            } else if c2 == u {
                                Some(c1)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    if let Some(v) = v_opt {
                        if !visited_clusters[v] {
                            visited_clusters[v] = true;
                            stack.push(v);
                        }
                    }
                }
            }

            let stop_count: usize = component_clusters
                .iter()
                .map(|&c| clusters[c].stops.len())
                .sum();
            let mut unique_routes = HashSet::new();
            for &e_idx in &component_edges {
                for r in &edges[e_idx].routes {
                    unique_routes.insert(r.clone());
                }
            }

            land_masses.push(LandMass {
                id: mass_id_counter,
                clusters: component_clusters,
                edges: component_edges.into_iter().collect(),
                stop_count,
                route_count: unique_routes.len(),
            });
            mass_id_counter += 1;
        }
    }

    land_masses.sort_by(|a, b| b.stop_count.cmp(&a.stop_count));
    land_masses
}
