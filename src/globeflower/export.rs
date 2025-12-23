use crate::clustering::StopCluster;
use crate::edges::GraphEdge;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;

#[derive(Serialize)]
pub struct ExportNode {
    pub id: usize, // cluster_id
    pub name: String, // primary stop name
    pub lat: f64,
    pub lon: f64,
    pub stop_ids: Vec<String>,
}

#[derive(Serialize)]
pub struct ExportEdge {
    pub from: usize,
    pub to: usize,
    pub route_ids: Vec<String>,
    pub geometry: Vec<[f64; 2]>, // GeoJSON-like sequence
    pub weight: f64,
}

#[derive(Serialize)]
pub struct LandMass {
    pub id: usize,
    pub nodes: Vec<ExportNode>,
    pub edges: Vec<ExportEdge>,
}

pub fn extract_and_export(clusters: &[StopCluster], edges: &[GraphEdge]) -> Result<()> {
    // 1. Build Adjacency List for Connected Components
    let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
    for edge in edges {
        adj.entry(edge.from_cluster).or_default().push(edge.to_cluster);
        adj.entry(edge.to_cluster).or_default().push(edge.from_cluster); // Undirected for land mass connectivity
    }

    let mut visited = HashSet::new();
    let mut land_masses = Vec::new();
    let mut mass_id_counter = 0;

    // Map ClusterID -> Cluster Ref for easy access
    let cluster_map: HashMap<usize, &StopCluster> = clusters.iter().map(|c| (c.cluster_id, c)).collect();

    for cluster in clusters {
        if visited.contains(&cluster.cluster_id) {
            continue;
        }

        let mut component_nodes = Vec::new();
        let mut stack = vec![cluster.cluster_id];
        visited.insert(cluster.cluster_id);

        while let Some(current_id) = stack.pop() {
            if let Some(&c) = cluster_map.get(&current_id) {
                component_nodes.push(c);
                
                if let Some(neighbors) = adj.get(&current_id) {
                    for &n in neighbors {
                        if !visited.contains(&n) {
                            visited.insert(n);
                            stack.push(n);
                        }
                    }
                }
            }
        }

        if component_nodes.is_empty() { continue; } // Isolated single node? Still valid mass? Yes.

        // Collect edges strictly internal to this component
        let component_ids: HashSet<usize> = component_nodes.iter().map(|c| c.cluster_id).collect();
        let component_edges: Vec<ExportEdge> = edges.iter()
            .filter(|e| component_ids.contains(&e.from_cluster) && component_ids.contains(&e.to_cluster))
            .map(|e| ExportEdge {
                from: e.from_cluster,
                to: e.to_cluster,
                route_ids: e.route_ids.clone(),
                geometry: e.geometry.points.iter().map(|p| [p.x, p.y]).collect(),
                weight: e.weight
            })
            .collect();

        let export_nodes: Vec<ExportNode> = component_nodes.iter().map(|c| {
            // Pick a representative name, e.g. the first stop's name
            let name = c.stops.first().and_then(|s| s.name.clone()).unwrap_or_else(|| "Unknown".to_string());
            ExportNode {
                id: c.cluster_id,
                name,
                lat: c.centroid.y,
                lon: c.centroid.x,
                stop_ids: c.stops.iter().map(|s| s.gtfs_id.clone()).collect(),
            }
        }).collect();

        // Heuristic: If mass is very small (e.g. < 2 nodes), maybe skip? No, keep all for debugging.
        
        land_masses.push(LandMass {
            id: mass_id_counter,
            nodes: export_nodes,
            edges: component_edges
        });
        mass_id_counter += 1;
    }

    // Export to JSON
    println!("Identified {} distinct land masses.", land_masses.len());
    let file = File::create("transit_graph.json")?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &land_masses)?;
    println!("Exported graph to transit_graph.json");

    Ok(())
}
