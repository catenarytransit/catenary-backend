use anyhow::{Context, Result};
use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::loom_graph::Graph;

/// Cumulative graph sizes after writing all independent components.
#[derive(Default)]
pub struct ExportCounts {
    pub nodes: usize,
    pub edges: usize,
    pub lines: usize,
}

/// Writes GeoJSON features as soon as each topo component is available.
/// Only one feature and a bounded I/O buffer are ever allocated for export.
pub struct GeoJsonWriter {
    writer: BufWriter<File>,
    first_feature: bool,
    counts: ExportCounts,
    // Offsets include vacant graph slots, while counts report live entries.
    node_offset: usize,
    edge_offset: usize,
    // Original edge IDs have a separate namespace from topology edge IDs.
    original_offset: usize,
}

impl GeoJsonWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).with_context(|| format!("create {:?}", path))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        writer.write_all(br#"{"type":"FeatureCollection","features":["#)?;
        Ok(Self {
            writer,
            first_feature: true,
            counts: ExportCounts::default(),
            node_offset: 0,
            edge_offset: 0,
            original_offset: 0,
        })
    }

    fn write_feature(&mut self, feature: &serde_json::Value) -> Result<()> {
        if !self.first_feature {
            self.writer.write_all(b",")?;
        }
        self.first_feature = false;
        serde_json::to_writer(&mut self.writer, feature)?;
        Ok(())
    }

    pub fn write_component(&mut self, graph: &Graph) -> Result<()> {
        let node_offset = self.node_offset;
        let edge_offset = self.edge_offset;
        let original_offset = self.original_offset;

        for edge in graph.edges.iter().flatten() {
            self.write_feature(&json!({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": edge.geom.iter()
                        .map(|point| [point.lon, point.lat])
                        .collect::<Vec<_>>()
                },
                "properties": {
                    "kind": "edge",
                    "id": edge.id + edge_offset,
                    "lines": edge.lines.iter().map(|occurrence| {
                        let line = &graph.lines[occurrence.line];
                        json!({
                            "id": format!("{}:{}", line.chateau, line.route_id),
                            "label": line.label,
                            "color": line.color,
                            "text_color": line.text_color
                        })
                    }).collect::<Vec<_>>(),
                    "original_edges": edge.originals.iter()
                        .map(|id| id + original_offset)
                        .collect::<Vec<_>>()
                }
            }))?;
        }

        for node in graph.nodes.iter().flatten() {
            if node.stops.is_empty() {
                continue;
            }
            self.write_feature(&json!({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [node.pos.lon, node.pos.lat]
                },
                "properties": {
                    "kind": "station",
                    "id": node.id + node_offset,
                    "stops": node.stops.iter().map(|stop| {
                        json!({
                            "id": stop.stop_id,
                            "name": stop.name,
                            "chateau": stop.chateau
                        })
                    }).collect::<Vec<_>>()
                }
            }))?;
        }

        // Match Graph::append's ID namespaces, including vacant node/edge slots.
        self.node_offset += graph.nodes.len();
        self.edge_offset += graph.edges.len();
        self.counts.nodes += graph.nodes.iter().flatten().count();
        self.counts.edges += graph.edges.iter().flatten().count();
        self.counts.lines += graph.lines.len();
        self.original_offset += graph.edges.iter().flatten()
            .flat_map(|edge| edge.originals.iter().copied())
            .max()
            .map_or(0, |id| id + 1);
        Ok(())
    }

    pub fn finish(mut self) -> Result<ExportCounts> {
        self.writer.write_all(b"]}")?;
        self.writer.flush()?;
        Ok(self.counts)
    }
}
