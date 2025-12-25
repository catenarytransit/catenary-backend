use super::clustering::StopCluster;
use super::edges::{GraphEdge, NodeId};
use anyhow::Result;
use geojson::{Feature, FeatureCollection, Geometry, JsonObject, JsonValue, Value};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufWriter;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LandMass {
    pub id: usize,
    pub clusters: Vec<usize>,
    pub edges: Vec<usize>, // Indicies into the edges vector
    pub stop_count: usize,
    pub route_count: usize,
}

pub fn extract_and_export(clusters: &[StopCluster], edges: &[GraphEdge]) -> Result<()> {
    let land_masses = extract_land_masses(clusters, edges);

    println!("Identified {} distinct land masses.", land_masses.len());

    // Legacy JSON export
    let file = File::create("globeflower_graph.json")?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &land_masses)?;
    println!("Exported internal graph to globeflower_graph.json");

    // GeoPackage Export (primary format)
    export_to_geopackage(clusters, edges)?;

    Ok(())
}

fn export_to_geojson(clusters: &[StopCluster], edges: &[GraphEdge]) -> Result<()> {
    let mut features = Vec::new();

    // 1. Export Edges as LineStrings
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
        match edge.from {
            NodeId::Cluster(id) => {
                properties.insert("from_cluster".to_string(), JsonValue::from(id as u64));
                properties.insert("from_type".to_string(), JsonValue::from("cluster"));
            }
            NodeId::Intersection(id) => {
                properties.insert("from_intersection".to_string(), JsonValue::from(id as u64));
                properties.insert("from_type".to_string(), JsonValue::from("intersection"));
            }
        }
        match edge.to {
            NodeId::Cluster(id) => {
                properties.insert("to_cluster".to_string(), JsonValue::from(id as u64));
                properties.insert("to_type".to_string(), JsonValue::from("cluster"));
            }
            NodeId::Intersection(id) => {
                properties.insert("to_intersection".to_string(), JsonValue::from(id as u64));
                properties.insert("to_type".to_string(), JsonValue::from("intersection"));
            }
        }

        properties.insert("weight".to_string(), JsonValue::from(edge.weight));
        properties.insert(
            "route_ids".to_string(),
            serde_json::to_value(&edge.route_ids)?,
        );
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
        let geometry = Geometry::new(Value::Point(vec![
            (cluster.centroid.x * 100_000_000.0).round() / 100_000_000.0,
            (cluster.centroid.y * 100_000_000.0).round() / 100_000_000.0,
        ]));

        let mut properties = JsonObject::new();
        properties.insert(
            "cluster_id".to_string(),
            JsonValue::from(cluster.cluster_id as u64),
        );
        properties.insert(
            "stop_count".to_string(),
            JsonValue::from(cluster.stops.len() as u64),
        );

        let stop_names: Vec<String> = cluster
            .stops
            .iter()
            .map(|s| s.name.clone().unwrap_or_default())
            .collect();
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

/// Export to GeoPackage format (SQLite-based spatial database)
fn export_to_geopackage(clusters: &[StopCluster], edges: &[GraphEdge]) -> Result<()> {
    let path = "globeflower_graph.gpkg";

    // Remove existing file if present
    let _ = std::fs::remove_file(path);

    let mut conn = Connection::open(path)?;

    // Set GeoPackage application_id (required by spec)
    conn.execute("PRAGMA application_id = 0x47504B47", [])?; // 'GPKG' in hex
    conn.execute("PRAGMA user_version = 10300", [])?; // GeoPackage version 1.3.0

    // Initialize GeoPackage metadata tables (order matters for foreign keys)
    conn.execute_batch("
        -- Spatial reference systems table (must be first)
        CREATE TABLE gpkg_spatial_ref_sys (
            srs_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL PRIMARY KEY,
            organization TEXT NOT NULL,
            organization_coordsys_id INTEGER NOT NULL,
            definition TEXT NOT NULL,
            description TEXT
        );
        
        -- Required SRS entries per GeoPackage spec
        INSERT INTO gpkg_spatial_ref_sys VALUES(
            'Undefined cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined cartesian coordinate reference system'
        );
        INSERT INTO gpkg_spatial_ref_sys VALUES(
            'Undefined geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic coordinate reference system'
        );
        INSERT INTO gpkg_spatial_ref_sys VALUES(
            'WGS 84 geodetic', 4326, 'EPSG', 4326,
            'GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]]',
            'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid'
        );
        
        -- Contents table
        CREATE TABLE gpkg_contents (
            table_name TEXT NOT NULL PRIMARY KEY,
            data_type TEXT NOT NULL,
            identifier TEXT UNIQUE,
            description TEXT DEFAULT '',
            last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            min_x DOUBLE,
            min_y DOUBLE,
            max_x DOUBLE,
            max_y DOUBLE,
            srs_id INTEGER,
            CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );
        
        -- Geometry columns table
        CREATE TABLE gpkg_geometry_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL,
            z TINYINT NOT NULL,
            m TINYINT NOT NULL,
            CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
            CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
            CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );
    ")?;

    // Create edges table
    conn.execute_batch(
        "
        CREATE TABLE edges (
            fid INTEGER PRIMARY KEY AUTOINCREMENT,
            geom BLOB,
            from_type TEXT,
            from_id INTEGER,
            to_type TEXT,
            to_id INTEGER,
            weight REAL,
            route_ids TEXT
        );
        
        INSERT INTO gpkg_contents VALUES('edges', 'features', 'edges', 'Transit edges', 
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), -180, -90, 180, 90, 4326);
        INSERT INTO gpkg_geometry_columns VALUES('edges', 'geom', 'LINESTRING', 4326, 0, 0);
    ",
    )?;

    // Create clusters table
    conn.execute_batch(
        "
        CREATE TABLE clusters (
            fid INTEGER PRIMARY KEY AUTOINCREMENT,
            geom BLOB,
            cluster_id INTEGER,
            stop_count INTEGER,
            stop_names TEXT
        );
        
        INSERT INTO gpkg_contents VALUES('clusters', 'features', 'clusters', 'Stop clusters',
            strftime('%Y-%m-%dT%H:%M:%fZ','now'), -180, -90, 180, 90, 4326);
        INSERT INTO gpkg_geometry_columns VALUES('clusters', 'geom', 'POINT', 4326, 0, 0);
    ",
    )?;
    // Use transaction for bulk inserts - massive performance improvement
    let tx = conn.transaction()?;

    // Insert edges
    {
        let mut stmt = tx.prepare(
            "INSERT INTO edges (geom, from_type, from_id, to_type, to_id, weight, route_ids) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )?;

        for edge in edges {
            let wkb = linestring_to_gpkg_wkb(&edge.geometry);
            let (from_type, from_id) = match edge.from {
                NodeId::Cluster(id) => ("cluster", id),
                NodeId::Intersection(id) => ("intersection", id),
            };
            let (to_type, to_id) = match edge.to {
                NodeId::Cluster(id) => ("cluster", id),
                NodeId::Intersection(id) => ("intersection", id),
            };
            let routes_json = serde_json::to_string(&edge.route_ids)?;

            stmt.execute(rusqlite::params![
                wkb,
                from_type,
                from_id,
                to_type,
                to_id,
                edge.weight,
                routes_json
            ])?;
        }
    }

    // Insert clusters
    {
        let mut stmt = tx.prepare(
            "INSERT INTO clusters (geom, cluster_id, stop_count, stop_names) VALUES (?, ?, ?, ?)",
        )?;

        for cluster in clusters {
            let wkb = point_to_gpkg_wkb(cluster.centroid.x, cluster.centroid.y);
            let stop_names: Vec<String> = cluster
                .stops
                .iter()
                .map(|s| s.name.clone().unwrap_or_default())
                .collect();
            let names_json = serde_json::to_string(&stop_names)?;

            stmt.execute(rusqlite::params![
                wkb,
                cluster.cluster_id,
                cluster.stops.len(),
                names_json
            ])?;
        }
    }

    tx.commit()?;

    println!("Exported graph to {}", path);
    Ok(())
}

/// Convert a LineString to GeoPackage Binary (GPB) format
fn linestring_to_gpkg_wkb(
    geom: &postgis_diesel::types::LineString<postgis_diesel::types::Point>,
) -> Vec<u8> {
    let mut gpb = Vec::new();

    // GeoPackage Binary header (8 bytes minimum)
    // Magic: 0x47 0x50 ("GP")
    gpb.push(0x47); // 'G'
    gpb.push(0x50); // 'P'

    // Version: 0 for GeoPackage 1.0+
    gpb.push(0x00);

    // Flags byte:
    // bits 0-2: envelope type (0 = no envelope)
    // bit 3: endianness (0 = big endian, 1 = little endian)
    // bit 4: empty geometry flag
    // bits 5-7: reserved
    gpb.push(0b00001000); // bit 3 = little endian, bits 0-2 = 0 = no envelope

    // SRS ID (4 bytes, little endian since flag bit 3 = 1)
    gpb.extend_from_slice(&4326i32.to_le_bytes());

    // Standard WKB follows
    gpb.push(0x01); // WKB little endian byte order
    gpb.extend_from_slice(&2u32.to_le_bytes()); // wkbLineString = 2
    gpb.extend_from_slice(&(geom.points.len() as u32).to_le_bytes());

    for pt in &geom.points {
        gpb.extend_from_slice(&pt.x.to_le_bytes());
        gpb.extend_from_slice(&pt.y.to_le_bytes());
    }

    gpb
}

/// Convert a Point to GeoPackage WKB format
fn point_to_gpkg_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut wkb = Vec::new();

    // GeoPackage header
    wkb.push(0x47); // 'G'
    wkb.push(0x50); // 'P'
    wkb.push(0x00); // version  
    wkb.push(0b00001000); // bit 3 = little endian, bits 0-2 = 0 = no envelope
    wkb.extend_from_slice(&4326i32.to_le_bytes()); // SRID

    // Standard WKB
    wkb.push(0x01); // little endian
    wkb.extend_from_slice(&1u32.to_le_bytes()); // Point type
    wkb.extend_from_slice(&x.to_le_bytes());
    wkb.extend_from_slice(&y.to_le_bytes());

    wkb
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
