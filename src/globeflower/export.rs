use anyhow::Result;
use serde_json::json;
use std::path::Path;

use crate::loom_graph::Graph;

pub fn geojson(graph: &Graph, path: &Path) -> Result<()> {
    let mut features = Vec::new();

    for edge in graph.edges.iter().filter_map(Option::as_ref) {
        features.push(json!({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": edge.geom
                    .iter()
                    .map(|point| vec![point.lon, point.lat])
                    .collect::<Vec<_>>()
            },
            "properties": {
                "kind": "edge",
                "id": edge.id,
                "lines": edge.lines.iter().map(|occurrence| {
                    let line = &graph.lines[occurrence.line];
                    json!({
                        "id": format!("{}:{}", line.chateau, line.route_id),
                        "label": line.label,
                        "color": line.color,
                        "text_color": line.text_color
                    })
                }).collect::<Vec<_>>(),
                "original_edges": edge.originals.iter().copied().collect::<Vec<_>>()
            }
        }));
    }

    for node in graph.nodes.iter().filter_map(Option::as_ref) {
        if node.stops.is_empty() {
            continue;
        }

        features.push(json!({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [node.pos.lon, node.pos.lat]
            },
            "properties": {
                "kind": "station",
                "id": node.id,
                "stops": node.stops.iter().map(|stop| {
                    json!({
                        "id": stop.stop_id,
                        "name": stop.name,
                        "chateau": stop.chateau
                    })
                }).collect::<Vec<_>>()
            }
        }));
    }

    let collection = json!({
        "type": "FeatureCollection",
        "features": features
    });

    std::fs::write(path, serde_json::to_vec(&collection)?)?;
    Ok(())
}
