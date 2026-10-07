use anyhow::{Context, Result};
use catenary::models::{IngestedStatic, Route, Shape, Stop};
use catenary::schema::gtfs::{ingested_static, routes, shapes, stops};
use diesel::prelude::*;
use diesel::sql_types::{Integer, Nullable, Text};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::loom_graph::{
    Graph, Line, LineOcc, Point, Stop as LoomStop, project_on_polyline, subline,
};

/// Keep the same non-bus mode set used by the pre-existing Globeflower loader:
/// tram, subway/metro, rail, cable tram, funicular, monorail.
const RAIL_ROUTE_TYPES: [i16; 6] = [0, 1, 2, 5, 7, 12];

#[derive(QueryableByName, Debug)]
struct PatternStopRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    itinerary_pattern_id: String,
    #[diesel(sql_type = Integer)]
    stop_sequence: i32,
    #[diesel(sql_type = Text)]
    stop_id: String,
    #[diesel(sql_type = Text)]
    chateau: String,
    #[diesel(sql_type = Text)]
    route_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    shape_id: Option<String>,
}

/// PostgreSQL-backed equivalent of LOOM's gtfs2graph input stage.
///
/// Catenary's current schema does not keep a raw `stop_times` table.  The
/// equivalent ordered stop sequence is:
///
///   itinerary_pattern_meta -> itinerary_pattern
///
/// Shapes are already stored as PostGIS LineStrings, so no reconstruction from
/// shape_pt rows is required.
///
/// Only production, non-deleted ingests are accepted and buses stay excluded.
pub fn build(conn: &mut PgConnection) -> Result<Graph> {
    let production_rows: Vec<IngestedStatic> = ingested_static::table
        .filter(ingested_static::production.eq(true))
        .filter(ingested_static::deleted.eq(false))
        .select(IngestedStatic::as_select())
        .load(conn)
        .context("load production GTFS ingest attempts")?;

    let production_attempts: HashSet<(String, String)> = production_rows
        .into_iter()
        .map(|x| (x.onestop_feed_id, x.attempt_id))
        .collect();

    let route_rows: Vec<Route> = routes::table
        .filter(routes::route_type.eq_any(RAIL_ROUTE_TYPES))
        .select(Route::as_select())
        .load(conn)
        .context("load non-bus rail routes")?;

    let mut graph = Graph::default();

    // A route_id is only unique inside a feed/attempt.  Do not collapse routes
    // from different feeds merely because Catenary assigns them the same
    // chateau.
    let mut line_by_key = HashMap::<(String, String, String), usize>::new();

    for route in route_rows {
        if !production_attempts.contains(&(route.onestop_feed_id.clone(), route.attempt_id.clone()))
        {
            continue;
        }

        let key = (
            route.onestop_feed_id.clone(),
            route.attempt_id.clone(),
            route.route_id.clone(),
        );

        if line_by_key.contains_key(&key) {
            continue;
        }

        let line_id = graph.lines.len();
        line_by_key.insert(key, line_id);

        graph.lines.push(Line {
            chateau: route.chateau,
            route_id: route.route_id.clone(),
            label: route
                .short_name
                .or(route.long_name)
                .unwrap_or(route.route_id),
            color: route.color.unwrap_or_else(|| "888888".to_string()),
            text_color: route.text_color.unwrap_or_else(|| "FFFFFF".to_string()),
        });
    }

    // Stops use PostGIS points in current Catenary. allowed_spatial_query marks
    // the currently queryable ingest, and route_types lets us discard bus-only
    // stops before constructing the preliminary graph.
    let stop_rows: Vec<Stop> = stops::table
        .filter(stops::allowed_spatial_query.eq(true))
        .filter(
            stops::location_type
                .eq(0_i16)
                .or(stops::location_type.eq(1_i16)),
        )
        .select(Stop::as_select())
        .load(conn)
        .context("load rail stops")?;

    let rail_types: HashSet<i16> = RAIL_ROUTE_TYPES.into_iter().collect();
    let mut node_by_stop = HashMap::<(String, String, String), usize>::new();

    for stop in stop_rows {
        if !production_attempts.contains(&(stop.onestop_feed_id.clone(), stop.attempt_id.clone())) {
            continue;
        }

        if !stop
            .route_types
            .iter()
            .filter_map(|x| *x)
            .any(|route_type| rail_types.contains(&route_type))
        {
            continue;
        }

        let Some(point) = stop.point else {
            continue;
        };

        let pos = Point {
            lon: point.x,
            lat: point.y,
        };
        let node_id = graph.add_node(pos);

        graph.nodes[node_id]
            .as_mut()
            .expect("newly inserted node")
            .stops
            .push(LoomStop {
                chateau: stop.chateau,
                stop_id: stop.gtfs_id.clone(),
                name: stop.name.or(stop.displayname).unwrap_or_default(),
                pos,
            });

        node_by_stop.insert(
            (stop.onestop_feed_id, stop.attempt_id, stop.gtfs_id),
            node_id,
        );
    }

    // Shapes are already assembled LineStrings.  Key them by feed + attempt +
    // shape_id because shape_id is not globally unique.
    let shape_rows: Vec<Shape> = shapes::table
        .filter(shapes::route_type.eq_any(RAIL_ROUTE_TYPES))
        .filter(shapes::allowed_spatial_query.eq(true))
        .select(Shape::as_select())
        .load(conn)
        .context("load rail shapes")?;

    let mut shape_map = HashMap::<(String, String, String), Vec<Point>>::new();

    for shape in shape_rows {
        if !production_attempts.contains(&(shape.onestop_feed_id.clone(), shape.attempt_id.clone()))
        {
            continue;
        }

        let geometry: Vec<Point> = shape
            .linestring
            .points
            .iter()
            .map(|point| Point {
                lon: point.x,
                lat: point.y,
            })
            .collect();

        if geometry.len() >= 2 {
            shape_map.insert(
                (shape.onestop_feed_id, shape.attempt_id, shape.shape_id),
                geometry,
            );
        }
    }

    // This is the current Catenary equivalent of trip -> stop_times.
    // Querying the two compact-pattern tables directly avoids expanding
    // trips_compressed and keeps one geometrically distinct itinerary pattern
    // as one input occurrence, which is exactly what gtfs2graph needs.
    let pattern_rows: Vec<PatternStopRow> = diesel::sql_query(
        r#"
        SELECT
            ip.onestop_feed_id,
            ip.attempt_id,
            ip.itinerary_pattern_id,
            ip.stop_sequence,
            ip.stop_id,
            ip.chateau,
            meta.route_id,
            meta.shape_id
        FROM gtfs.itinerary_pattern AS ip
        INNER JOIN gtfs.itinerary_pattern_meta AS meta
            ON meta.onestop_feed_id = ip.onestop_feed_id
           AND meta.attempt_id = ip.attempt_id
           AND meta.itinerary_pattern_id = ip.itinerary_pattern_id
        INNER JOIN gtfs.routes AS r
            ON r.onestop_feed_id = meta.onestop_feed_id
           AND r.attempt_id = meta.attempt_id
           AND r.route_id = meta.route_id
        INNER JOIN gtfs.ingested_static AS ingest
            ON ingest.onestop_feed_id = ip.onestop_feed_id
           AND ingest.attempt_id = ip.attempt_id
        WHERE r.route_type IN (0, 1, 2, 5, 7, 12)
          AND ingest.production = TRUE
          AND ingest.deleted = FALSE
        ORDER BY
            ip.onestop_feed_id,
            ip.attempt_id,
            ip.itinerary_pattern_id,
            ip.stop_sequence
        "#,
    )
    .load(conn)
    .context("load ordered non-bus itinerary patterns")?;

    type PatternKey = (String, String, String, String, Option<String>, String);
    let mut patterns = BTreeMap::<PatternKey, Vec<(i32, String)>>::new();

    for row in pattern_rows {
        patterns
            .entry((
                row.onestop_feed_id,
                row.attempt_id,
                row.itinerary_pattern_id,
                row.route_id,
                row.shape_id,
                row.chateau,
            ))
            .or_default()
            .push((row.stop_sequence, row.stop_id));
    }

    let mut preliminary_edge_id = 0usize;

    for ((feed_id, attempt_id, _pattern_id, route_id, shape_id, _chateau), mut stop_sequence) in
        patterns
    {
        stop_sequence.sort_by_key(|x| x.0);

        let Some(&line_id) = line_by_key.get(&(feed_id.clone(), attempt_id.clone(), route_id))
        else {
            continue;
        };

        let shape = shape_id.as_ref().and_then(|shape_id| {
            shape_map.get(&(feed_id.clone(), attempt_id.clone(), shape_id.clone()))
        });

        for pair in stop_sequence.windows(2) {
            let Some(&a) =
                node_by_stop.get(&(feed_id.clone(), attempt_id.clone(), pair[0].1.clone()))
            else {
                continue;
            };

            let Some(&b) =
                node_by_stop.get(&(feed_id.clone(), attempt_id.clone(), pair[1].1.clone()))
            else {
                continue;
            };

            if a == b {
                continue;
            }

            let pa = graph.nodes[a].as_ref().expect("node exists").pos;
            let pb = graph.nodes[b].as_ref().expect("node exists").pos;

            let geometry = if let Some(shape) = shape {
                let (_, ta, _) = project_on_polyline(pa, shape);
                let (_, tb, _) = project_on_polyline(pb, shape);

                if ta <= tb {
                    subline(shape, ta, tb)
                } else {
                    let mut reversed = subline(shape, tb, ta);
                    reversed.reverse();
                    reversed
                }
            } else {
                vec![pa, pb]
            };

            if geometry.len() < 2 {
                continue;
            }

            let edge_id = graph.add_edge(a, b, geometry);
            let edge = graph.edges[edge_id].as_mut().expect("newly inserted edge");

            edge.lines.insert(LineOcc {
                line: line_id,
                direction: Some(b),
            });
            edge.originals.insert(preliminary_edge_id);
            preliminary_edge_id += 1;
        }
    }

    Ok(graph)
}
