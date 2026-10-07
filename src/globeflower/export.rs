use anyhow::Result;
use serde_json::json;
use std::path::Path;
use crate::globeflower::loom_graph::Graph;

pub fn geojson(g:&Graph,path:&Path)->Result<()>{
    let mut features=vec![];
    for e in g.edges.iter().filter_map(Option::as_ref){
        features.push(json!({
            "type":"Feature",
            "geometry":{"type":"LineString","coordinates":e.geom.iter().map(|p|vec![p.lon,p.lat]).collect::<Vec<_>>()},
            "properties":{
                "kind":"edge","id":e.id,
                "lines":e.lines.iter().map(|o|{
                    let l=&g.lines[o.line];
                    json!({"id":format!("{}:{}",l.chateau,l.route_id),"label":l.label,"color":l.color})
                }).collect::<Vec<_>>(),
                "original_edges":e.originals.iter().copied().collect::<Vec<_>>()
            }
        }));
    }
    for n in g.nodes.iter().filter_map(Option::as_ref){
        if n.stops.is_empty(){continue}
        features.push(json!({
            "type":"Feature",
            "geometry":{"type":"Point","coordinates":[n.pos.lon,n.pos.lat]},
            "properties":{"kind":"station","id":n.id,"stops":n.stops.iter().map(|s|json!({"id":s.stop_id,"name":s.name,"chateau":s.chateau})).collect::<Vec<_>>()}
        }));
    }
    std::fs::write(path,serde_json::to_vec(&json!({"type":"FeatureCollection","features":features}))?)?;
    Ok(())
}
