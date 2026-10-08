use anyhow::{Context, Result};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Double, Nullable, Text};
use log::{info, warn};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

use crate::loom_graph::{
    Graph, Line, LineOcc, Point, Stop as LoomStop, add_line_occ,
};

/// Globeflower intentionally processes only GTFS route_type 0 (tram) and
/// route_type 1 (subway/metro). Heavy rail is deliberately excluded.
const ROUTE_TYPES_SQL: &str = "(0, 1)";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct PatternKey {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub direction_pattern_id: String,
}

#[derive(Debug, Clone)]
pub struct WorkComponent {
    pub patterns: Vec<PatternKey>,
}

#[derive(Debug, Clone, Copy)]
pub struct PlannerConfig {
    /// Initial world-grid tile width/height. Sparse areas finish at this size.
    pub tile_degrees: f64,
    /// Dense tiles are recursively quartered down to this size.
    pub min_tile_degrees: f64,
    /// A tile returning more rows than this is subdivided before its rows are used.
    pub row_limit: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            tile_degrees: 20.0,
            min_tile_degrees: 0.625,
            row_limit: 100_000,
        }
    }
}

#[derive(QueryableByName, Debug)]
struct RouteSeedRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    route_id: String,
    #[diesel(sql_type = Text)]
    shapes_json: String,
}

#[derive(Debug, Clone, Serialize)]
struct RouteWorkKey {
    onestop_feed_id: String,
    attempt_id: String,
    route_id: String,
}

#[derive(QueryableByName, Debug)]
struct PlannerRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    direction_pattern_id: String,
    #[diesel(sql_type = Text)]
    station_key: String,
}

#[derive(QueryableByName, Debug)]
struct PatternStopRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    direction_pattern_id: String,
    #[diesel(sql_type = BigInt)]
    stop_sequence: i64,
    #[diesel(sql_type = Text)]
    stop_id: String,
    #[diesel(sql_type = Text)]
    stop_name: String,
    #[diesel(sql_type = Double)]
    lon: f64,
    #[diesel(sql_type = Double)]
    lat: f64,
    #[diesel(sql_type = Nullable<BigInt>)]
    osm_station_id: Option<i64>,
    #[diesel(sql_type = Text)]
    chateau: String,
    #[diesel(sql_type = Text)]
    route_id: String,
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = Text)]
    color: String,
    #[diesel(sql_type = Text)]
    text_color: String,
    #[diesel(sql_type = Nullable<Text>)]
    shape_id: Option<String>,
}

/// Catenary preserves trip memberships by itinerary pattern.  Summing
/// trip_ids grouped by direction pattern supplies the C++ Builder::simplify
/// occurrence weight without expanding all stop_times into memory.
#[derive(QueryableByName, Debug)]
struct PatternWeightRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    direction_pattern_id: String,
    #[diesel(sql_type = BigInt)]
    trip_count: i64,
}

#[derive(QueryableByName, Debug)]
struct ShapeRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    shape_id: String,
    #[diesel(sql_type = Text)]
    geojson: String,
}

#[derive(Debug, Clone, Copy)]
struct Tile {
    west: f64,
    south: f64,
    east: f64,
    north: f64,
}

impl Tile {
    fn width(self) -> f64 {
        self.east - self.west
    }

    fn height(self) -> f64 {
        self.north - self.south
    }

    fn quarters(self) -> [Tile; 4] {
        let mid_lon = (self.west + self.east) * 0.5;
        let mid_lat = (self.south + self.north) * 0.5;
        [
            Tile {
                west: self.west,
                south: self.south,
                east: mid_lon,
                north: mid_lat,
            },
            Tile {
                west: mid_lon,
                south: self.south,
                east: self.east,
                north: mid_lat,
            },
            Tile {
                west: self.west,
                south: mid_lat,
                east: mid_lon,
                north: self.north,
            },
            Tile {
                west: mid_lon,
                south: mid_lat,
                east: self.east,
                north: self.north,
            },
        ]
    }
}

#[derive(Default)]
struct DisjointSet {
    index: HashMap<PatternKey, usize>,
    keys: Vec<PatternKey>,
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl DisjointSet {
    fn ensure(&mut self, key: PatternKey) -> usize {
        if let Some(&idx) = self.index.get(&key) {
            return idx;
        }
        let idx = self.parent.len();
        self.index.insert(key.clone(), idx);
        self.keys.push(key);
        self.parent.push(idx);
        self.rank.push(0);
        idx
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let mut ra = self.find(a);
        let mut rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.rank[ra] < self.rank[rb] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb] = ra;
        if self.rank[ra] == self.rank[rb] {
            self.rank[ra] += 1;
        }
    }

    fn into_components(mut self) -> Vec<WorkComponent> {
        let entries: Vec<(PatternKey, usize)> = self
            .keys
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, key)| (key, idx))
            .collect();
        let mut grouped = BTreeMap::<usize, Vec<PatternKey>>::new();
        for (key, idx) in entries {
            let root = self.find(idx);
            grouped.entry(root).or_default().push(key);
        }
        let mut out: Vec<WorkComponent> = grouped
            .into_values()
            .map(|mut patterns| {
                patterns.sort();
                WorkComponent { patterns }
            })
            .collect();
        out.sort_by(|a, b| {
            b.patterns
                .len()
                .cmp(&a.patterns.len())
                .then_with(|| a.patterns.first().cmp(&b.patterns.first()))
        });
        out
    }
}

/// Discover independent transit worksets by starting from gtfs.routes.
///
/// Globeflower only wants GTFS route_type 0 and 1, so the routes table is a much
/// cheaper root than sweeping the entire world through gtfs.stops.  We read the
/// selected routes and their shapes_list first, then resolve direction-pattern
/// station memberships in bounded route batches.  No shape geometry is loaded
/// during planning.
///
/// Patterns are unioned when they share a physical station. osm_station_id is
/// the cross-feed identity; otherwise feed + attempt + stop_id is the fallback.
pub fn discover_components(
    conn: &mut PgConnection,
    cfg: PlannerConfig,
) -> Result<Vec<WorkComponent>> {
    anyhow::ensure!(cfg.row_limit > 0, "planner row limit must be > 0");

    info!("[planner] reading production route_type 0/1 routes first");
    let route_rows: Vec<RouteSeedRow> = diesel::sql_query(format!(
        r#"
        SELECT
            r.onestop_feed_id,
            r.attempt_id,
            r.route_id,
            COALESCE(to_jsonb(r.shapes_list)::text, '[]') AS shapes_json
        FROM gtfs.routes AS r
        INNER JOIN gtfs.ingested_static AS ingest
            ON ingest.onestop_feed_id = r.onestop_feed_id
           AND ingest.attempt_id = r.attempt_id
        WHERE r.route_type IN {route_types}
          AND ingest.production = TRUE
          AND ingest.deleted = FALSE
        ORDER BY r.onestop_feed_id, r.attempt_id, r.route_id
        "#,
        route_types = ROUTE_TYPES_SQL
    ))
    .load(conn)
    .context("load production tram/metro routes")?;

    if route_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut route_keys = Vec::with_capacity(route_rows.len());
    let mut shape_ref_count = 0usize;
    for row in route_rows {
        if let Ok(shape_ids) = serde_json::from_str::<Vec<Option<String>>>(&row.shapes_json) {
            shape_ref_count += shape_ids.into_iter().flatten().count();
        }
        route_keys.push(RouteWorkKey {
            onestop_feed_id: row.onestop_feed_id,
            attempt_id: row.attempt_id,
            route_id: row.route_id,
        });
    }

    info!(
        "[planner] {} routes reference {} shape IDs; geometry remains unloaded",
        route_keys.len(),
        shape_ref_count
    );

    let mut dsu = DisjointSet::default();
    let mut first_pattern_at_station = HashMap::<String, usize>::new();
    let route_batch_size = cfg.row_limit.clamp(64, 1024);
    let mut membership_count = 0usize;

    for route_chunk in route_keys.chunks(route_batch_size) {
        let workset_json = serde_json::to_string(route_chunk)?;
        let rows: Vec<PlannerRow> = diesel::sql_query(format!(
            r#"
            WITH route_workset AS (
                SELECT *
                FROM jsonb_to_recordset($1::jsonb) AS w(
                    onestop_feed_id text,
                    attempt_id text,
                    route_id text
                )
            )
            SELECT DISTINCT
                dp.onestop_feed_id,
                dp.attempt_id,
                dp.direction_pattern_id,
                CASE
                    WHEN s.osm_station_id IS NOT NULL
                        THEN 'osm:' || s.osm_station_id::text
                    ELSE 'gtfs:' || s.onestop_feed_id || '|' || s.attempt_id || '|' || s.gtfs_id
                END AS station_key
            FROM route_workset AS w
            INNER JOIN gtfs.direction_pattern_meta AS dpm
                ON dpm.onestop_feed_id = w.onestop_feed_id
               AND dpm.attempt_id = w.attempt_id
               AND dpm.route_id = w.route_id
            INNER JOIN gtfs.direction_pattern AS dp
                ON dp.onestop_feed_id = dpm.onestop_feed_id
               AND dp.attempt_id = dpm.attempt_id
               AND dp.direction_pattern_id = dpm.direction_pattern_id
            INNER JOIN gtfs.stops AS s
                ON s.onestop_feed_id = dp.onestop_feed_id
               AND s.attempt_id = dp.attempt_id
               AND s.gtfs_id = dp.stop_id
            WHERE dpm.route_type IN {route_types}
              AND s.allowed_spatial_query = TRUE
              AND s.location_type IN (0, 1)
              AND s.point IS NOT NULL
            "#,
            route_types = ROUTE_TYPES_SQL
        ))
        .bind::<Text, _>(&workset_json)
        .load(conn)
        .context("load route-batched direction-pattern station memberships")?;

        membership_count += rows.len();
        for row in rows {
            let idx = dsu.ensure(PatternKey {
                onestop_feed_id: row.onestop_feed_id,
                attempt_id: row.attempt_id,
                direction_pattern_id: row.direction_pattern_id,
            });
            if let Some(&first) = first_pattern_at_station.get(&row.station_key) {
                dsu.union(first, idx);
            } else {
                first_pattern_at_station.insert(row.station_key, idx);
            }
        }
    }

    let pattern_count = dsu.keys.len();
    let components = dsu.into_components();
    let largest = components.first().map_or(0, |c| c.patterns.len());
    info!(
        "[planner] {} station memberships, {} route_type 0/1 direction patterns -> {} connected components; largest={} patterns",
        membership_count,
        pattern_count,
        components.len(),
        largest
    );
    Ok(components)
}

fn scan_tile(
    conn: &mut PgConnection,
    tile: Tile,
    cfg: PlannerConfig,
    dsu: &mut DisjointSet,
    tile_count: &mut usize,
    dense_splits: &mut usize,
) -> Result<()> {
    *tile_count += 1;
    let can_split =
        tile.width() * 0.5 >= cfg.min_tile_degrees && tile.height() * 0.5 >= cfg.min_tile_degrees;

    let rows = load_planner_rows(
        conn,
        tile,
        if can_split {
            Some(cfg.row_limit + 1)
        } else {
            None
        },
    )?;
    if can_split && rows.len() > cfg.row_limit {
        *dense_splits += 1;
        for child in tile.quarters() {
            scan_tile(conn, child, cfg, dsu, tile_count, dense_splits)?;
        }
        return Ok(());
    }

    // station_key only lives for this tile. Cross-tile state consists solely of
    // the much smaller direction-pattern DSU.
    let mut first_pattern_at_station = HashMap::<String, usize>::new();
    for row in rows {
        let idx = dsu.ensure(PatternKey {
            onestop_feed_id: row.onestop_feed_id,
            attempt_id: row.attempt_id,
            direction_pattern_id: row.direction_pattern_id,
        });
        if let Some(&first) = first_pattern_at_station.get(&row.station_key) {
            dsu.union(first, idx);
        } else {
            first_pattern_at_station.insert(row.station_key, idx);
        }
    }
    Ok(())
}

fn load_planner_rows(
    conn: &mut PgConnection,
    tile: Tile,
    limit: Option<usize>,
) -> Result<Vec<PlannerRow>> {
    // The `&& ST_MakeEnvelope(...)` predicate is intentional: it directly uses
    // the existing GiST index on gtfs.stops(point). The direction-pattern meta
    // join filters route_type before any graph/geometry materialization.
    let base = format!(
        r#"
        SELECT DISTINCT
            dp.onestop_feed_id,
            dp.attempt_id,
            dp.direction_pattern_id,
            CASE
                WHEN s.osm_station_id IS NOT NULL
                    THEN 'osm:' || s.osm_station_id::text
                ELSE 'gtfs:' || s.onestop_feed_id || '|' || s.attempt_id || '|' || s.gtfs_id
            END AS station_key
        FROM gtfs.stops AS s
        INNER JOIN gtfs.direction_pattern AS dp
            ON dp.onestop_feed_id = s.onestop_feed_id
           AND dp.attempt_id = s.attempt_id
           AND dp.stop_id = s.gtfs_id
        INNER JOIN gtfs.direction_pattern_meta AS dpm
            ON dpm.onestop_feed_id = dp.onestop_feed_id
           AND dpm.attempt_id = dp.attempt_id
           AND dpm.direction_pattern_id = dp.direction_pattern_id
        WHERE s.allowed_spatial_query = TRUE
          AND s.location_type IN (0, 1)
          AND s.point IS NOT NULL
          AND dpm.route_type IN {route_types}
          AND s.point && ST_MakeEnvelope($1, $2, $3, $4, 4326)
        "#,
        route_types = ROUTE_TYPES_SQL
    );

    let sql = if limit.is_some() {
        format!("{base} LIMIT $5")
    } else {
        base
    };
    let query = diesel::sql_query(sql)
        .bind::<Double, _>(tile.west)
        .bind::<Double, _>(tile.south)
        .bind::<Double, _>(tile.east)
        .bind::<Double, _>(tile.north);

    if let Some(limit) = limit {
        query
            .bind::<BigInt, _>(limit as i64)
            .load(conn)
            .context("scan bounded spatial planner tile")
    } else {
        query
            .load(conn)
            .context("scan minimum spatial planner tile")
    }
}

#[derive(Default)]
struct PhysicalStationAccum {
    sum_lon: f64,
    sum_lat: f64,
    count: usize,
    stops: BTreeMap<(String, String), LoomStop>,
}

#[derive(Default)]
struct PatternAccum {
    route_key: (String, String, String),
    shape_id: Option<String>,
    stops: Vec<(i64, (String, String, String), String)>,
}

/// Materialize exactly one station-connected component and construct its
/// gtfs2graph graph. The caller should run topo immediately and drop this raw
/// component before loading the next one.
pub fn build_component(
    conn: &mut PgConnection,
    component: &WorkComponent,
    prune_threshold: f64,
) -> Result<Graph> {
    if component.patterns.is_empty() {
        return Ok(Graph::default());
    }

    let workset_json = serde_json::to_string(&component.patterns)?;
    let stop_rows: Vec<PatternStopRow> = diesel::sql_query(format!(
        r#"
        WITH workset AS (
            SELECT *
            FROM jsonb_to_recordset($1::jsonb) AS w(
                onestop_feed_id text,
                attempt_id text,
                direction_pattern_id text
            )
        )
        SELECT
            dp.onestop_feed_id,
            dp.attempt_id,
            dp.direction_pattern_id,
            dp.stop_sequence::bigint AS stop_sequence,
            s.gtfs_id AS stop_id,
            COALESCE(s.name, s.displayname, '') AS stop_name,
            ST_X(s.point) AS lon,
            ST_Y(s.point) AS lat,
            s.osm_station_id,
            r.chateau,
            r.route_id,
            COALESCE(r.short_name, r.long_name, r.route_id) AS label,
            COALESCE(r.color, '888888') AS color,
            COALESCE(r.text_color, 'FFFFFF') AS text_color,
            dpm.gtfs_shape_id AS shape_id
        FROM workset AS w
        INNER JOIN gtfs.direction_pattern_meta AS dpm
            ON dpm.onestop_feed_id = w.onestop_feed_id
           AND dpm.attempt_id = w.attempt_id
           AND dpm.direction_pattern_id = w.direction_pattern_id
        INNER JOIN gtfs.direction_pattern AS dp
            ON dp.onestop_feed_id = w.onestop_feed_id
           AND dp.attempt_id = w.attempt_id
           AND dp.direction_pattern_id = w.direction_pattern_id
        INNER JOIN gtfs.stops AS s
            ON s.onestop_feed_id = dp.onestop_feed_id
           AND s.attempt_id = dp.attempt_id
           AND s.gtfs_id = dp.stop_id
        INNER JOIN gtfs.routes AS r
            ON r.onestop_feed_id = dpm.onestop_feed_id
           AND r.attempt_id = dpm.attempt_id
           AND r.route_id = dpm.route_id
        WHERE dpm.route_type IN {route_types}
          AND r.route_type IN {route_types}
          AND s.allowed_spatial_query = TRUE
          AND s.location_type IN (0, 1)
          AND s.point IS NOT NULL
        ORDER BY dp.onestop_feed_id, dp.attempt_id, dp.direction_pattern_id, dp.stop_sequence
        "#,
        route_types = ROUTE_TYPES_SQL
    ))
    .bind::<Text, _>(&workset_json)
    .load(conn)
    .context("load one route_type 0/1 direction-pattern component")?;

    if stop_rows.is_empty() {
        return Ok(Graph::default());
    }

    let shape_rows: Vec<ShapeRow> = diesel::sql_query(format!(
        r#"
        WITH workset AS (
            SELECT *
            FROM jsonb_to_recordset($1::jsonb) AS w(
                onestop_feed_id text,
                attempt_id text,
                direction_pattern_id text
            )
        ),
        route_shapes AS (
            SELECT DISTINCT
                r.onestop_feed_id,
                r.attempt_id,
                route_shape.shape_id
            FROM workset AS w
            INNER JOIN gtfs.direction_pattern_meta AS dpm
                ON dpm.onestop_feed_id = w.onestop_feed_id
               AND dpm.attempt_id = w.attempt_id
               AND dpm.direction_pattern_id = w.direction_pattern_id
            INNER JOIN gtfs.routes AS r
                ON r.onestop_feed_id = dpm.onestop_feed_id
               AND r.attempt_id = dpm.attempt_id
               AND r.route_id = dpm.route_id
            CROSS JOIN LATERAL unnest(COALESCE(r.shapes_list, ARRAY[]::text[])) AS route_shape(shape_id)
            WHERE r.route_type IN {route_types}
        )
        SELECT DISTINCT
            s.onestop_feed_id,
            s.attempt_id,
            s.shape_id,
            ST_AsGeoJSON(s.linestring) AS geojson
        FROM route_shapes AS rs
        INNER JOIN gtfs.shapes AS s
            ON s.onestop_feed_id = rs.onestop_feed_id
           AND s.attempt_id = rs.attempt_id
           AND s.shape_id = rs.shape_id
        WHERE s.route_type IN {route_types}
          AND s.allowed_spatial_query = TRUE
        "#,
        route_types = ROUTE_TYPES_SQL
    ))
    .bind::<Text, _>(&workset_json)
    .load(conn)
    .context("load shapes for one direction-pattern component")?;

    let mut shape_map = HashMap::<(String, String, String), Vec<Point>>::new();
    for row in shape_rows {
        match parse_linestring_geojson(&row.geojson) {
            Some(points) if points.len() >= 2 => {
                shape_map.insert((row.onestop_feed_id, row.attempt_id, row.shape_id), points);
            }
            _ => warn!(
                "[gtfs2graph] ignoring malformed/short shape {}",
                row.shape_id
            ),
        }
    }

    // Read ONLY trip counts for this connected component.  C++ Builder
    // iterates every trip; the compressed schema's `trip_ids` field permits
    // equivalent weighting while we iterate one direction pattern at a time.
    let weight_rows: Vec<PatternWeightRow> = diesel::sql_query(
        r#"WITH workset AS (
              SELECT * FROM jsonb_to_recordset($1::jsonb) AS w(
                  onestop_feed_id text, attempt_id text, direction_pattern_id text)
            )
            SELECT w.onestop_feed_id, w.attempt_id, w.direction_pattern_id,
                   GREATEST(1, COALESCE(SUM(cardinality(ipm.trip_ids)), 0))::bigint AS trip_count
            FROM workset w
            LEFT JOIN gtfs.itinerary_pattern_meta ipm
              ON ipm.onestop_feed_id = w.onestop_feed_id
             AND ipm.attempt_id = w.attempt_id
             AND ipm.direction_pattern_id = w.direction_pattern_id
            GROUP BY w.onestop_feed_id, w.attempt_id, w.direction_pattern_id"#,
    )
    .bind::<Text, _>(&workset_json)
    .load(conn)
    .context("load direction-pattern trip cardinalities for Builder::simplify")?;
    let pattern_weights: HashMap<PatternKey, usize> = weight_rows
        .into_iter()
        .map(|row| {
            (
                PatternKey {
                    onestop_feed_id: row.onestop_feed_id,
                    attempt_id: row.attempt_id,
                    direction_pattern_id: row.direction_pattern_id,
                },
                row.trip_count.max(1) as usize,
            )
        })
        .collect();

    let mut graph = Graph::default();
    let mut line_by_key = HashMap::<(String, String, String), usize>::new();
    let mut physical = BTreeMap::<String, PhysicalStationAccum>::new();
    let mut patterns = BTreeMap::<PatternKey, PatternAccum>::new();

    // First pass: build physical-station aggregates and compact direction patterns.
    for row in &stop_rows {
        let route_key = (
            row.onestop_feed_id.clone(),
            row.attempt_id.clone(),
            row.route_id.clone(),
        );
        line_by_key.entry(route_key.clone()).or_insert_with(|| {
            let id = graph.lines.len();
            graph.lines.push(Line {
                chateau: row.chateau.clone(),
                route_id: row.route_id.clone(),
                label: row.label.clone(),
                color: row.color.clone(),
                text_color: row.text_color.clone(),
            });
            id
        });

        // C++ Builder::addStop keys nodes by GTFS stop identity, not by
        // the parent OSM station. Distinct platforms must remain separate
        // until topo/StatInserter, even if they share osm_station_id.
        let physical_key = format!(
            "gtfs:{}|{}|{}",
            row.onestop_feed_id, row.attempt_id, row.stop_id
        );
        let station = physical.entry(physical_key.clone()).or_default();
        // The same stop occurs in many direction patterns. Count its position
        // exactly once rather than weighting the centroid by pattern frequency.
        if station.count == 0 {
            station.sum_lon = row.lon;
            station.sum_lat = row.lat;
            station.count = 1;
        }
        station
            .stops
            .entry((row.chateau.clone(), row.stop_id.clone()))
            .or_insert_with(|| LoomStop {
                chateau: row.chateau.clone(),
                stop_id: row.stop_id.clone(),
                name: row.stop_name.clone(),
                pos: Point {
                    lon: row.lon,
                    lat: row.lat,
                },
            });

        patterns
            .entry(PatternKey {
                onestop_feed_id: row.onestop_feed_id.clone(),
                attempt_id: row.attempt_id.clone(),
                direction_pattern_id: row.direction_pattern_id.clone(),
            })
            .or_insert_with(|| PatternAccum {
                route_key: route_key.clone(),
                shape_id: row.shape_id.clone(),
                stops: Vec::new(),
            })
            .stops
            .push((
                row.stop_sequence,
                (
                    row.onestop_feed_id.clone(),
                    row.attempt_id.clone(),
                    row.stop_id.clone(),
                ),
                physical_key,
            ));
    }

    let mut node_by_physical = HashMap::<String, usize>::new();
    for (physical_key, station) in physical {
        if station.count == 0 {
            continue;
        }
        let pos = Point {
            lon: station.sum_lon / station.count as f64,
            lat: station.sum_lat / station.count as f64,
        };
        let node_id = graph.add_node(pos);
        graph.nodes[node_id].as_mut().unwrap().stops = station.stops.into_values().collect();
        node_by_physical.insert(physical_key, node_id);
    }

    // C++ Builder::consume calls getEdg(from, to) before addEdg. Reuse
    // canonical edges across repeated direction-pattern occurrences.
    // Geometrically distinct alternatives must not be silently averaged;
    // retain distinct alternatives by shape until a dedicated ETG simplifier
    // can choose the reference geometry as in C++ Builder::simplify.
    let mut edge_by_pair_shape = HashMap::<(usize, usize, Option<String>), usize>::new();
    let mut edge_trip_weight = HashMap::<usize, usize>::new();
    let mut edge_line_trip_weight = HashMap::<(usize, usize), usize>::new();
    let mut preliminary_edge_id = 0usize;
    for (pattern_key, pattern) in &mut patterns {
        pattern.stops.sort_by_key(|x| x.0);
        let Some(&line_id) = line_by_key.get(&pattern.route_key) else {
            continue;
        };
        let shape = pattern.shape_id.as_ref().and_then(|shape_id| {
            shape_map.get(&(
                pattern_key.onestop_feed_id.clone(),
                pattern_key.attempt_id.clone(),
                shape_id.clone(),
            ))
        });

        let mut previous: Option<(usize, usize)> = None; // (end node, edge id)

        for pair in pattern.stops.windows(2) {
            let Some(&a) = node_by_physical.get(&pair[0].2) else {
                continue;
            };
            let Some(&b) = node_by_physical.get(&pair[1].2) else {
                continue;
            };
            if a == b {
                continue;
            }
            let pa = graph.nodes[a].as_ref().unwrap().pos;
            let pb = graph.nodes[b].as_ref().unwrap().pos;
            let geometry = if let Some(shape) = shape {
                crate::loom_polyline::ordered_segment(shape, pa, pb)
            } else {
                vec![pa, pb]
            };
            if geometry.len() < 2 {
                continue;
            }
            // C++ Builder::getSubPolyLine returns projected shape geometry.
            // It DOES NOT force its endpoints onto the GTFS stop positions.
            // MapConstructor handles graph-node endpoints separately.
            let key = (a.min(b), a.max(b), pattern.shape_id.clone());
            let edge_id = if let Some(&id) = edge_by_pair_shape.get(&key) {
                id
            } else {
                let id = graph.add_edge(a, b, geometry);
                edge_by_pair_shape.insert(key, id);
                graph.edges[id]
                    .as_mut()
                    .unwrap()
                    .originals
                    .insert(preliminary_edge_id);
                preliminary_edge_id += 1;
                id
            };
            *edge_trip_weight.entry(edge_id).or_default() +=
                pattern_weights.get(pattern_key).copied().unwrap_or(1);
            *edge_line_trip_weight.entry((edge_id, line_id)).or_default() +=
                pattern_weights.get(pattern_key).copied().unwrap_or(1);
            let edge = graph.edges[edge_id].as_mut().unwrap();
            // A line used in both directions is represented as bidirectional,
            // just as C++ EdgeTripGeom::RouteOccurance aggregates trips.
            add_line_occ(
                &mut edge.lines,
                LineOcc {
                    line: line_id,
                    direction: Some(b),
                },
            );

            // LOOM Builder::consume records each actual consecutive trip
            // transition. Merely sharing a node and line does NOT establish a
            // legal transition (particularly on branching metro services).
            if let Some((previous_end, previous_edge)) = previous {
                if previous_end == a {
                    graph.nodes[a]
                        .as_mut()
                        .unwrap()
                        .allowed_turns
                        .entry(line_id)
                        .or_default()
                        .insert((previous_edge, edge_id));
                }
            }
            previous = Some((b, edge_id));
        }
    }

    crate::loom_builder_simplify::simplify(
        &mut graph,
        &edge_trip_weight,
        &mut edge_line_trip_weight,
        prune_threshold,
    );
    Ok(graph)
}

fn parse_linestring_geojson(raw: &str) -> Option<Vec<Point>> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    if value.get("type")?.as_str()? != "LineString" {
        return None;
    }
    let coords = value.get("coordinates")?.as_array()?;
    let mut out = Vec::with_capacity(coords.len());
    for coord in coords {
        let pair = coord.as_array()?;
        if pair.len() < 2 {
            return None;
        }
        out.push(Point {
            lon: pair[0].as_f64()?,
            lat: pair[1].as_f64()?,
        });
    }
    Some(out)
}

#[cfg(test)]
mod simplifier_tests {
    use super::*;
    #[test]
    fn alternative_shape_edges_become_single_canonical_station_pair() {
        let mut graph = Graph::default();
        let pa = Point { lon: 0.0, lat: 0.0 };
        let pb = Point {
            lon: 0.001,
            lat: 0.0,
        };
        let a = graph.add_node(pa);
        let b = graph.add_node(pb);
        let e0 = graph.add_edge(a, b, vec![pa, pb]);
        let e1 = graph.add_edge(b, a, vec![pb, pa]);
        graph.edges[e0].as_mut().unwrap().originals.insert(0);
        graph.edges[e1].as_mut().unwrap().originals.insert(1);
        graph.edges[e0].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(b),
        });
        graph.edges[e1].as_mut().unwrap().lines.insert(LineOcc {
            line: 0,
            direction: Some(a),
        });
        crate::loom_builder_simplify::simplify(
            &mut graph,
            &HashMap::from([(e0, 10), (e1, 5)]),
            &mut HashMap::from([((e0, 0), 10), ((e1, 0), 5)]),
            0.0,
        );
        let edge = graph.edges.iter().flatten().next().unwrap();
        assert_eq!(graph.edges.iter().flatten().count(), 1);
        assert_eq!(edge.originals.len(), 2);
        assert_eq!(edge.lines.len(), 1);
        assert!(edge.lines.iter().next().unwrap().direction.is_none());
    }
}
