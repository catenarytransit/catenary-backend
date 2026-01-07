use crate::graph::{Edge as RenderEdge, LineOnEdge, Node as RenderNode, RenderGraph};
use crate::optimizer;
use anyhow::Result;
use catenary::graph_formats::{NodeId, SerializableExportGraph};
use catenary::schema::gtfs::routes::dsl as routes_dsl;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use log::{debug, info};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

pub struct Loader {
    graph_path: String,
    pg_pool: Pool<ConnectionManager<PgConnection>>,
}

impl Loader {
    pub fn new(graph_path: String, pg_pool: Pool<ConnectionManager<PgConnection>>) -> Self {
        Self {
            graph_path,
            pg_pool,
        }
    }

    pub async fn load_graph(&self) -> Result<RenderGraph> {
        println!("Loading graph from {}", self.graph_path);

        // 1. Fetch Route Colors and Short Names
        let mut conn = self.pg_pool.get()?;

        // RouteInfo: (color, short_name)
        let routes_data = tokio::task::block_in_place(
            || -> Result<HashMap<(String, String), (String, Option<String>)>> {
                let results = routes_dsl::routes
                    .select((
                        routes_dsl::chateau,
                        routes_dsl::route_id,
                        routes_dsl::color,
                        routes_dsl::short_name,
                    ))
                    .load::<(String, String, Option<String>, Option<String>)>(&mut conn)?;

                let mut map = HashMap::new();
                for (chateau, route_id, color, short_name) in results {
                    let c = color.unwrap_or_else(|| "000000".to_string());
                    map.insert((chateau, route_id), (c, short_name));
                }
                Ok(map)
            },
        )?;

        // Print some sample keys from DB
        println!(
            "Sample DB keys: {:?}",
            routes_data.keys().take(5).collect::<Vec<_>>()
        );

        println!(
            "Loaded {} routes with color/short_name info",
            routes_data.len()
        );

        // 2. Load Bincode Graph
        let file = File::open(&self.graph_path)?;
        let mut reader = BufReader::new(file);
        let export_graph: SerializableExportGraph =
            bincode::serde::decode_from_std_read(&mut reader, bincode::config::legacy())?;

        println!(
            "Loaded Bincode graph: {} clusters, {} edges",
            export_graph.clusters.len(),
            export_graph.edges.len()
        );

        // 3. Convert to RenderGraph
        let mut nodes = HashMap::new();

        // Convert Clusters to Nodes
        for cluster in export_graph.clusters {
            let id = cluster.cluster_id as i64; // Positive ID for clusters
            // Collect stop names for label
            let name = cluster
                .stops
                .first()
                .and_then(|s| s.name.clone())
                .or_else(|| Some(format!("Cluster {}", id)));

            nodes.insert(
                id,
                RenderNode {
                    id,
                    x: cluster.centroid[0],
                    y: cluster.centroid[1],
                    name,
                    is_cluster: true,
                },
            );
        }

        // We also need to infer geometry for Intersections if they are nodes
        // But ExportGraph doesn't list Intersections explicitly, they are just endpoints in Edges.
        // RenderGraph expects all nodes to be in `nodes` map?
        // Let's collect intersection locations from Edges.

        for edge in &export_graph.edges {
            let from_node = Self::convert_node_id(edge.from);
            let to_node = Self::convert_node_id(edge.to);

            // If it's an intersection (negative ID), we need to ensure it's in `nodes`.
            // Use edge geometry endpoints.
            if let NodeId::Intersection(..) = edge.from {
                if !nodes.contains_key(&from_node) {
                    if let Some(first_pt) = edge.geometry.first() {
                        nodes.insert(
                            from_node,
                            RenderNode {
                                id: from_node,
                                x: first_pt[0],
                                y: first_pt[1],
                                name: None,
                                is_cluster: false,
                            },
                        );
                    }
                }
            }

            if let NodeId::Intersection(..) = edge.to {
                if !nodes.contains_key(&to_node) {
                    if let Some(last_pt) = edge.geometry.last() {
                        nodes.insert(
                            to_node,
                            RenderNode {
                                id: to_node,
                                x: last_pt[0],
                                y: last_pt[1],
                                name: None,
                                is_cluster: false,
                            },
                        );
                    }
                }
            }
        }

        println!("Total nodes (clusters + intersections): {}", nodes.len());

        let mut edges = Vec::new();
        // Track route groups: (chateau, group_key) -> Vec of (route_id, color)
        let mut route_groups: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();

        for (i, edge) in export_graph.edges.iter().enumerate() {
            let from = Self::convert_node_id(edge.from);
            let to = Self::convert_node_id(edge.to);

            let mut lines_on_edge = Vec::new();
            for (chateau_id, route_id) in &edge.route_ids {
                let (color, short_name) = routes_data
                    .get(&(chateau_id.clone(), route_id.clone()))
                    .cloned()
                    .unwrap_or_else(|| {
                        // Log failure once per route to avoid spam
                        if i < 5 {
                            // Only log for first few edges
                            debug!("Missing route info for ({}, {})", chateau_id, route_id);
                        }
                        ("000000".to_string(), None)
                    });

                // Bundle strictly by COLOR to ensure lines of same color are merged (e.g. NYC Subway N/Q/R)
                // This prevents parallel lines of the same color.
                let group_key = color.clone();

                // Track route groups for later use
                route_groups
                    .entry((chateau_id.clone(), group_key.clone()))
                    .or_default()
                    .push((route_id.clone(), color.clone()));

                // line_id uses the group key for bundling
                let line_id = format!("{}:{}", chateau_id, group_key);

                lines_on_edge.push(LineOnEdge {
                    line_id,
                    color,
                    chateau_id: chateau_id.clone(),
                    route_id: route_id.clone(),
                    group_id: Some(group_key),
                    weight: 1,
                });
            }

            edges.push(RenderEdge {
                id: i as i64,
                from,
                to,
                lines: lines_on_edge,
                geometry: edge.geometry.clone(),
                dir: true, // Default direction - will be updated during optimization as needed
            });
        }

        // Build RTree
        let mut geom_items = Vec::new();
        for (i, edge) in edges.iter().enumerate() {
            if edge.geometry.is_empty() {
                continue;
            }
            // Calculate AABB
            let mut min_x = f64::MAX;
            let mut min_y = f64::MAX;
            let mut max_x = f64::MIN;
            let mut max_y = f64::MIN;

            for p in &edge.geometry {
                if p[0] < min_x {
                    min_x = p[0];
                }
                if p[0] > max_x {
                    max_x = p[0];
                }
                if p[1] < min_y {
                    min_y = p[1];
                }
                if p[1] > max_y {
                    max_y = p[1];
                }
            }

            let aabb = rstar::primitives::Rectangle::from_corners([min_x, min_y], [max_x, max_y]);
            geom_items.push(rstar::primitives::GeomWithData::new(aabb, i));
        }
        let tree = rstar::RTree::bulk_load(geom_items);

        // Build Node Tree (only clusters/stations)
        let mut node_items = Vec::new();
        for node in nodes.values() {
            if node.is_cluster {
                node_items.push(rstar::primitives::GeomWithData::new(
                    [node.x, node.y],
                    node.id,
                ));
            }
        }
        let node_tree = rstar::RTree::bulk_load(node_items);

        println!("Node tree loaded");

        // 4. Load Restrictions
        let mut restriction_map: HashMap<(usize, usize), Vec<(String, String)>> = HashMap::new();
        for r in export_graph.restrictions {
            restriction_map
                .entry((r.from_edge_index, r.to_edge_index))
                .or_default()
                .push(r.route_id);
        }

        // 5. Build Node Adjacency
        let mut node_to_edges: HashMap<i64, Vec<usize>> = HashMap::new();
        for (idx, edge) in edges.iter().enumerate() {
            node_to_edges.entry(edge.from).or_default().push(idx);
            node_to_edges.entry(edge.to).or_default().push(idx);
        }

        let render_graph = RenderGraph {
            nodes,
            edges,
            tree,
            node_tree,
            restrictions: restriction_map,
            collapsed_lines: HashMap::new(),
            node_to_edges,
            route_groups,
        };

        // Optimizer is NOT called here anymore. It's called by the specific command in main.rs if needed.

        Ok(render_graph)
    }

    fn convert_node_id(id: NodeId) -> i64 {
        match id {
            NodeId::Cluster(c) => c as i64,
            NodeId::Intersection(cluster_id, local_id) => {
                // Combine cluster_id and local_id into a unique negative ID
                let combined = ((cluster_id as i64) << 32) | (local_id as i64);
                -((combined % 1_000_000_000) + 1) // Ensure it stays in a reasonable range
            }
            // Split nodes: use a hash to create unique negative ID
            // Pack edge_idx and split_idx into a unique number
            NodeId::Split(e, s) => {
                let combined = ((e as i64) << 20) | (s as i64);
                -(combined + 1_000_000_000) // Offset to avoid collision with Intersection IDs
            }
            // OSM Junction nodes: use the OSM node ID with a negative offset
            NodeId::OsmJunction(osm_id) => {
                -(osm_id.abs() + 2_000_000_000) // Offset to avoid collision
            }
        }
    }
}
