use anyhow::{Context, Result};
use catenary::models::{Route, Shape, Stop, Trip};
use catenary::schema::gtfs::{routes, shapes, stops, trips};
use diesel::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::globeflower::loom_graph::{Graph, Line, LineOcc, Point, Stop as LStop};

/// LOOM gtfs2graph semantics, with PostgreSQL replacing cppgtfs:
/// * every GTFS stop is a preliminary graph node
/// * every consecutive stop pair used by a trip becomes a preliminary edge occurrence
/// * its geometry is the corresponding part of the GTFS shape
/// * occurrences are labelled with their route/line and direction
/// Bus routes are intentionally excluded.
pub fn build(conn: &mut PgConnection) -> Result<Graph> {
    let route_rows: Vec<Route> = routes::table
        .filter(routes::route_type.ne(3))
        .load(conn)
        .context("load non-bus routes")?;

    let mut graph=Graph::default();
    let mut line_by_key=HashMap::<(String,String),usize>::new();
    let mut allowed=BTreeSet::<(String,String)>::new();

    for r in route_rows {
        let key=(r.chateau.clone(), r.route_id.clone());
        allowed.insert(key.clone());
        let id=graph.lines.len();
        line_by_key.insert(key, id);
        graph.lines.push(Line {
            chateau:r.chateau,
            route_id:r.route_id,
            label:r.route_short_name.unwrap_or_default(),
            color:r.route_color.unwrap_or_else(|| "000000".into()),
            text_color:r.route_text_color.unwrap_or_else(|| "FFFFFF".into()),
        });
    }

    let stop_rows: Vec<Stop> = stops::table.load(conn).context("load stops")?;
    let mut node_by_stop=HashMap::<(String,String),usize>::new();
    for s in stop_rows {
        let (Some(lat),Some(lon))=(s.stop_lat,s.stop_lon) else { continue; };
        let nid=graph.add_node(Point{lon,lat});
        graph.nodes[nid].as_mut().unwrap().stops.push(LStop {
            chateau:s.chateau.clone(), stop_id:s.stop_id.clone(),
            name:s.stop_name.unwrap_or_default(), pos:Point{lon,lat},
        });
        node_by_stop.insert((s.chateau,s.stop_id),nid);
    }

    // Catenary stores compact itinerary rows separately in current main.  We deliberately
    // use SQL here so the Chapter-3 port is independent of application-side compressed
    // timetable representations.
    #[derive(QueryableByName)]
    struct SeqRow {
        #[diesel(sql_type=diesel::sql_types::Text)] chateau:String,
        #[diesel(sql_type=diesel::sql_types::Text)] trip_id:String,
        #[diesel(sql_type=diesel::sql_types::Text)] route_id:String,
        #[diesel(sql_type=diesel::sql_types::Nullable<diesel::sql_types::Text>)] shape_id:Option<String>,
        #[diesel(sql_type=diesel::sql_types::Text)] stop_id:String,
        #[diesel(sql_type=diesel::sql_types::Integer)] stop_sequence:i32,
    }

    let rows:Vec<SeqRow>=diesel::sql_query(r#"
        SELECT t.chateau, t.trip_id, t.route_id, t.shape_id,
               st.stop_id, st.stop_sequence
        FROM gtfs.trips t
        JOIN gtfs.stop_times st
          ON st.chateau=t.chateau AND st.trip_id=t.trip_id
        JOIN gtfs.routes r
          ON r.chateau=t.chateau AND r.route_id=t.route_id
        WHERE r.route_type <> 3
        ORDER BY t.chateau,t.trip_id,st.stop_sequence
    "#).load(conn).context("load ordered trip stop sequences")?;

    #[derive(QueryableByName)]
    struct ShapeRow {
        #[diesel(sql_type=diesel::sql_types::Text)] chateau:String,
        #[diesel(sql_type=diesel::sql_types::Text)] shape_id:String,
        #[diesel(sql_type=diesel::sql_types::Double)] lat:f64,
        #[diesel(sql_type=diesel::sql_types::Double)] lon:f64,
        #[diesel(sql_type=diesel::sql_types::Integer)] seq:i32,
    }
    let shape_rows:Vec<ShapeRow>=diesel::sql_query(r#"
        SELECT chateau,shape_id,shape_pt_lat AS lat,shape_pt_lon AS lon,
               shape_pt_sequence AS seq
        FROM gtfs.shapes
        ORDER BY chateau,shape_id,shape_pt_sequence
    "#).load(conn).context("load shapes")?;

    let mut shape_map=BTreeMap::<(String,String),Vec<Point>>::new();
    for p in shape_rows {
        shape_map.entry((p.chateau,p.shape_id)).or_default().push(Point{lon:p.lon,lat:p.lat});
    }

    let mut by_trip=BTreeMap::<(String,String,String,Option<String>),Vec<(i32,String)>>::new();
    for r in rows {
        if !allowed.contains(&(r.chateau.clone(),r.route_id.clone())) { continue; }
        by_trip.entry((r.chateau,r.trip_id,r.route_id,r.shape_id))
            .or_default().push((r.stop_sequence,r.stop_id));
    }

    let mut prelim_id=0usize;
    for ((chateau,_trip,route_id,shape_id), mut seq) in by_trip {
        seq.sort_by_key(|x|x.0);
        let Some(&line)=line_by_key.get(&(chateau.clone(),route_id)) else { continue; };
        let shape=shape_id.as_ref().and_then(|sid|shape_map.get(&(chateau.clone(),sid.clone())));
        for pair in seq.windows(2) {
            let Some(&a)=node_by_stop.get(&(chateau.clone(),pair[0].1.clone())) else {continue};
            let Some(&b)=node_by_stop.get(&(chateau.clone(),pair[1].1.clone())) else {continue};
            if a==b {continue}
            let pa=graph.nodes[a].as_ref().unwrap().pos;
            let pb=graph.nodes[b].as_ref().unwrap().pos;
            let geom=if let Some(s)=shape {
                let (_,ta,_)=crate::globeflower::loom_graph::project_on_polyline(pa,s);
                let (_,tb,_)=crate::globeflower::loom_graph::project_on_polyline(pb,s);
                if ta<=tb { crate::globeflower::loom_graph::subline(s,ta,tb) }
                else { let mut x=crate::globeflower::loom_graph::subline(s,tb,ta); x.reverse(); x }
            } else { vec![pa,pb] };
            let eid=graph.add_edge(a,b,geom);
            let e=graph.edges[eid].as_mut().unwrap();
            e.lines.insert(LineOcc{line,direction:Some(b)});
            e.originals.insert(prelim_id);
            prelim_id+=1;
        }
    }
    Ok(graph)
}
