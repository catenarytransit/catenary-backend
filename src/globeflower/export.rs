use super::clustering::StopCluster;
use super::edges::{GraphEdge, NodeId};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufWriter;
use geojson::{Feature, FeatureCollection, Geometry, Value, JsonObject, JsonValue};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LandMass {
    pub id: usize,
    pub clusters: Vec<usize>,
    pub edges: Vec<usize>, // Indicies into the edges vector
    pub stop_count: usize,
    pub route_count: usize,
}

pub fn extract_and_export(
    clusters: &[StopCluster],
    edges: &[GraphEdge],
) -> Result<()> {
    let land_masses = extract_land_masses(clusters, edges);
    
    println!("Identified {} distinct land masses.", land_masses.len());
    
    // Legacy JSON export
    let file = File::create("globeflower_graph.json")?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &land_masses)?;
    println!("Exported internal graph to globeflower_graph.json");

    // GeoJSON Export
    export_to_geojson(clusters, edges)?;
    
    Ok(())
}

fn export_to_geojson(clusters: &[StopCluster], edges: &[GraphEdge]) -> Result<()> {
    let mut features = Vec::new();

    // 1. Export Edges as LineStrings
    for edge in edges {
        let geometry = Geometry::new(Value::LineString(
            edge.geometry.points.iter().map(|p| vec![p.x, p.y]).collect()
        ));

        let mut properties = JsonObject::new();
        match edge.from {
            NodeId::Cluster(id) => {
                 properties.insert("from_cluster".to_string(), JsonValue::from(id as u64));
                 properties.insert("from_type".to_string(), JsonValue::from("cluster"));
            },
            NodeId::Intersection(id) => {
                 properties.insert("from_intersection".to_string(), JsonValue::from(id as u64));
                 properties.insert("from_type".to_string(), JsonValue::from("intersection"));
            }
        }
        match edge.to {
             NodeId::Cluster(id) => {
                 properties.insert("to_cluster".to_string(), JsonValue::from(id as u64));
                 properties.insert("to_type".to_string(), JsonValue::from("cluster"));
             },
             NodeId::Intersection(id) => {
                 properties.insert("to_intersection".to_string(), JsonValue::from(id as u64));
                 properties.insert("to_type".to_string(), JsonValue::from("intersection"));
             }
        }
        
        properties.insert("weight".to_string(), JsonValue::from(edge.weight));
        properties.insert("route_ids".to_string(), serde_json::to_value(&edge.route_ids)?);
        properties.insert("type".to_string(), JsonValue::from("edge"));

        features.push(Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(properties),
            foreign_members: None,
        });
    }

    // 2. Export Clusters as Points
    for cluster in clusters {
        let geometry = Geometry::new(Value::Point(vec![cluster.centroid.x, cluster.centroid.y]));

        let mut properties = JsonObject::new();
        properties.insert("cluster_id".to_string(), JsonValue::from(cluster.cluster_id as u64));
        properties.insert("stop_count".to_string(), JsonValue::from(cluster.stops.len() as u64));
        
        let stop_names: Vec<String> = cluster.stops.iter().map(|s| s.name.clone().unwrap_or_default()).collect();
        properties.insert("stop_names".to_string(), serde_json::to_value(stop_names)?);
        properties.insert("type".to_string(), JsonValue::from("node"));
        properties.insert("node_type".to_string(), JsonValue::from("cluster"));

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

    let file = File::create("globeflower_graph.geojson")?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &collection)?;
    println!("Exported graph to globeflower_graph.geojson");
    
    Ok(())
}

fn extract_land_masses(
    clusters: &[StopCluster],
    edges: &[GraphEdge],
) -> Vec<LandMass> {
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
                            if c1 == u { Some(c2) } else if c2 == u { Some(c1) } else { None }
                        }
                        _ => None
                    };

                    if let Some(v) = v_opt {
                        if !visited_clusters[v] {
                            visited_clusters[v] = true;
                            stack.push(v);
                        }
                    }
                }
            }

            let stop_count: usize = component_clusters.iter().map(|&c| clusters[c].stops.len()).sum();
            let mut unique_routes = HashSet::new();
            for &e_idx in &component_edges {
                for r in &edges[e_idx].route_ids {
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
