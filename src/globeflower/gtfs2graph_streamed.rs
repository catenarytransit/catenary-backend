use anyhow::{Context, Result};
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, Double, Nullable, Text};
use log::{info, warn};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

use crate::loom_graph::{Graph, Line, Point, Stop as LoomStop};
use crate::loom_shape_alignment::AlignedShape;
use crate::loom_trip_segments::{EdgeRegistry, SegmentKey};

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
    /// LOOM topo/TopoMain.cpp: distConnectedComponents(10000, false).
    /// Web Mercator metres, matching the LOOM geometry operations.
    pub connected_comp_distance: f64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            tile_degrees: 20.0,
            min_tile_degrees: 0.625,
            row_limit: 100_000,
            connected_comp_distance: 10_000.0,
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
    #[diesel(sql_type = Double)]
    lon: f64,
    #[diesel(sql_type = Double)]
    lat: f64,
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

/// Geometry is itinerary-specific. Direction-pattern IDs depend on stop
/// sequence, and multiple itineraries under the same ID may use different
/// GTFS shapes. Weight each distinct shape by the number of represented trips.
#[derive(QueryableByName, Debug)]
struct PatternWeightRow {
    #[diesel(sql_type = Text)]
    onestop_feed_id: String,
    #[diesel(sql_type = Text)]
    attempt_id: String,
    #[diesel(sql_type = Text)]
    direction_pattern_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    shape_id: Option<String>,
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

/// Geographic equivalent of LOOM LineGraph::distConnectedComponents(d, false).
/// LOOM joins graph nodes within d *projected* metres before finding
/// connected components, then deletes the temporary proximity edges.
/// Unioning their pattern owners has exactly the same transitive effect.
/// A cell's diagonal is <= d, so all its points have the same DSU root.
/// Keep every point, however: a boundary point can be within distance of
/// the next cell even when the first point in that cell is not.
struct GeographicComponents {
    bins: HashMap<(i64, i64), Vec<(f64, f64, usize)>>,
    cell_size: f64,
    distance: f64,
}

impl GeographicComponents {
    fn new(distance: f64) -> Self {
        Self {
            bins: HashMap::new(),
            cell_size: distance / std::f64::consts::SQRT_2,
            distance,
        }
    }

    fn add(&mut self, dsu: &mut DisjointSet, pattern: usize, position: Point) {
        if self.distance <= 0.0 || !self.distance.is_finite() {
            return;
        }
        let (x, y) = crate::loom_graph::web_mercator(position);
        if !x.is_finite() || !y.is_finite() {
            return;
        }
        let cell = (
            (x / self.cell_size).floor() as i64,
            (y / self.cell_size).floor() as i64,
        );
        // All points within the same cell are at most `distance` apart.
        if let Some(first) = self.bins.get(&cell).and_then(|v| v.first()) {
            dsu.union(pattern, first.2);
        }
        // Search +/-2 because a radius may span two sqrt(2)-sized cells.
        for dx in -2..=2 {
            for dy in -2..=2 {
                let neighbor = (cell.0 + dx, cell.1 + dy);
                if neighbor == cell {
                    continue;
                }
                let Some(points) = self.bins.get(&neighbor) else {
                    continue;
                };
                // A bucket is one DSU component. No need to scan hundreds
                // of stops once that component has already been joined.
                if dsu.find(pattern) == dsu.find(points[0].2) {
                    continue;
                }
                if let Some((_, _, other)) = points
                    .iter()
                    .find(|&&(px, py, _)| (px - x).hypot(py - y) <= self.distance)
                {
                    dsu.union(pattern, *other);
                }
            }
        }
        self.bins.entry(cell).or_default().push((x, y, pattern));
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
    chateaux: Option<&[String]>,
) -> Result<Vec<WorkComponent>> {
    anyhow::ensure!(cfg.row_limit > 0, "planner row limit must be > 0");

    info!("[planner] reading production route_type 0/1 routes first");
    // Select by chateau at the route seed, before materializing direction
    // patterns, station memberships, or geometry. The SQL suffix is constant;
    // chateau values are bound as an array, never interpolated into SQL.
    let chateau_filter = if chateaux.is_some() {
        "AND r.chateau = ANY($1::text[])"
    } else {
        ""
    };
    let route_sql = format!(
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
          {chateau_filter}
        ORDER BY r.onestop_feed_id, r.attempt_id, r.route_id
        "#,
        route_types = ROUTE_TYPES_SQL
    );
    let route_rows: Vec<RouteSeedRow> = match chateaux {
        Some(ids) => diesel::sql_query(route_sql)
            .bind::<Array<Text>, _>(ids.to_vec())
            .load(conn),
        None => diesel::sql_query(route_sql).load(conn),
    }
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

    anyhow::ensure!(
        cfg.connected_comp_distance.is_finite() && cfg.connected_comp_distance >= 0.0,
        "connected component distance must be finite and >= 0"
    );
    let mut dsu = DisjointSet::default();
    let mut first_pattern_at_station = HashMap::<String, usize>::new();
    let mut seen_positions = HashMap::<(u64, u64), usize>::new();
    let mut geographic = GeographicComponents::new(cfg.connected_comp_distance);
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
                END AS station_key,
                ST_X(s.point) AS lon,
                ST_Y(s.point) AS lat
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
                first_pattern_at_station.insert(row.station_key.clone(), idx);
            }
            // Do not conflate geographic proximity with station identity:
            // independent agencies can overlap without sharing stop IDs.
            let coordinate = (row.lon.to_bits(), row.lat.to_bits());
            if let Some(&first) = seen_positions.get(&coordinate) {
                // Different GTFS stop IDs can have exactly the same point.
                dsu.union(first, idx);
            } else {
                seen_positions.insert(coordinate, idx);
                geographic.add(
                    &mut dsu,
                    idx,
                    Point {
                        lon: row.lon,
                        lat: row.lat,
                    },
                );
            }
        }
    }

    let pattern_count = dsu.keys.len();
    let components = dsu.into_components();
    let largest = components.first().map_or(0, |c| c.patterns.len());
    info!(
        "[planner] {} memberships / {} physical stations, {} route_type 0/1 patterns -> {} connected components (LOOM geographic radius {}m); largest={} patterns",
        membership_count,
        seen_positions.len(),
        pattern_count,
        components.len(),
        cfg.connected_comp_distance,
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
            END AS station_key,
            ST_X(s.point) AS lon,
            ST_Y(s.point) AS lat
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

#[cfg(test)]
mod geographic_components_tests {
    use super::*;

    fn pattern(dsu: &mut DisjointSet, id: usize) -> usize {
        dsu.ensure(PatternKey {
            onestop_feed_id: format!("feed{id}"),
            attempt_id: "0".to_string(),
            direction_pattern_id: "x".to_string(),
        })
    }

    #[test]
    fn nearby_different_agencies_join_without_shared_station_id() {
        let mut dsu = DisjointSet::default();
        let mut geo = GeographicComponents::new(10_000.0);
        let a = pattern(&mut dsu, 0);
        let b = pattern(&mut dsu, 1);
        geo.add(&mut dsu, a, Point { lon: 0.0, lat: 0.0 });
        geo.add(
            &mut dsu,
            b,
            Point {
                lon: 0.015,
                lat: 0.0,
            },
        );
        assert_eq!(dsu.find(a), dsu.find(b));
    }

    #[test]
    fn far_apart_components_stay_separate() {
        let mut dsu = DisjointSet::default();
        let mut geo = GeographicComponents::new(10_000.0);
        let a = pattern(&mut dsu, 0);
        let b = pattern(&mut dsu, 1);
        geo.add(&mut dsu, a, Point { lon: 0.0, lat: 0.0 });
        geo.add(&mut dsu, b, Point { lon: 1.0, lat: 0.0 });
        assert_ne!(dsu.find(a), dsu.find(b));
    }

    #[test]
    fn geographic_connections_are_transitive() {
        let mut dsu = DisjointSet::default();
        let mut geo = GeographicComponents::new(10_000.0);
        let a = pattern(&mut dsu, 0);
        let b = pattern(&mut dsu, 1);
        let c = pattern(&mut dsu, 2);
        geo.add(&mut dsu, a, Point { lon: 0.0, lat: 0.0 });
        geo.add(
            &mut dsu,
            b,
            Point {
                lon: 0.07,
                lat: 0.0,
            },
        );
        geo.add(
            &mut dsu,
            c,
            Point {
                lon: 0.14,
                lat: 0.0,
            },
        );
        assert_eq!(dsu.find(a), dsu.find(c));
    }
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
        referenced_shapes AS (
            SELECT DISTINCT
                w.onestop_feed_id,
                w.attempt_id,
                COALESCE(ipm.shape_id, dpm.gtfs_shape_id) AS shape_id
            FROM workset AS w
            INNER JOIN gtfs.direction_pattern_meta AS dpm
                ON dpm.onestop_feed_id = w.onestop_feed_id
               AND dpm.attempt_id = w.attempt_id
               AND dpm.direction_pattern_id = w.direction_pattern_id
            LEFT JOIN gtfs.itinerary_pattern_meta AS ipm
                ON ipm.onestop_feed_id = w.onestop_feed_id
               AND ipm.attempt_id = w.attempt_id
               AND ipm.direction_pattern_id = w.direction_pattern_id
            WHERE dpm.route_type IN {route_types}
        )
        SELECT DISTINCT
            s.onestop_feed_id,
            s.attempt_id,
            s.shape_id,
            ST_AsGeoJSON(s.linestring) AS geojson
        FROM referenced_shapes AS rs
        INNER JOIN gtfs.shapes AS s
            ON s.onestop_feed_id = rs.onestop_feed_id
           AND s.attempt_id = rs.attempt_id
           AND s.shape_id = rs.shape_id
        WHERE rs.shape_id IS NOT NULL
          AND s.linestring IS NOT NULL
        "#,
        route_types = ROUTE_TYPES_SQL
    ))
    .bind::<Text, _>(&workset_json)
    .load(conn)
    .context("load shapes for one direction-pattern component")?;

    let mut shape_map = HashMap::<(String, String, String), AlignedShape>::new();
    for row in shape_rows {
        match parse_linestring_geojson(&row.geojson).and_then(AlignedShape::new) {
            Some(shape) => {
                shape_map.insert((row.onestop_feed_id, row.attempt_id, row.shape_id), shape);
            }
            None => warn!(
                "[gtfs2graph] ignoring malformed/short shape {}",
                row.shape_id
            ),
        }
    }

    // Unlike direction patterns, itineraries distinguish GTFS shape IDs.
    // Preserve every geometry instead of choosing an arbitrary representative
    // from several itineraries with identical stop sequences.
    let weight_rows: Vec<PatternWeightRow> = diesel::sql_query(
        r#"WITH workset AS (
              SELECT * FROM jsonb_to_recordset($1::jsonb) AS w(
                  onestop_feed_id text, attempt_id text, direction_pattern_id text)
            )
            SELECT w.onestop_feed_id, w.attempt_id, w.direction_pattern_id,
                   COALESCE(ipm.shape_id, dpm.gtfs_shape_id) AS shape_id,
                   GREATEST(1, COALESCE(SUM(cardinality(ipm.trip_ids)), 0))::bigint AS trip_count
            FROM workset w
            INNER JOIN gtfs.direction_pattern_meta dpm
              ON dpm.onestop_feed_id = w.onestop_feed_id
             AND dpm.attempt_id = w.attempt_id
             AND dpm.direction_pattern_id = w.direction_pattern_id
            LEFT JOIN gtfs.itinerary_pattern_meta ipm
              ON ipm.onestop_feed_id = w.onestop_feed_id
             AND ipm.attempt_id = w.attempt_id
             AND ipm.direction_pattern_id = w.direction_pattern_id
            GROUP BY w.onestop_feed_id, w.attempt_id, w.direction_pattern_id,
                     COALESCE(ipm.shape_id, dpm.gtfs_shape_id)"#,
    )
    .bind::<Text, _>(&workset_json)
    .load(conn)
    .context("load itinerary-specific shapes and trip cardinalities")?;
    let mut pattern_weights = HashMap::<PatternKey, Vec<(Option<String>, usize)>>::new();
    for row in weight_rows {
        let key = PatternKey {
            onestop_feed_id: row.onestop_feed_id,
            attempt_id: row.attempt_id,
            direction_pattern_id: row.direction_pattern_id,
        };
        pattern_weights
            .entry(key)
            .or_default()
            .push((row.shape_id, row.trip_count.max(1) as usize));
    }

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

    // Each (itinerary shape, ordered stop sequence) is matched globally.
    // Geometries are extracted from arclength intervals, not independently
    // reprojected endpoints; this preserves circles, lassos and reversals.
    let mut registry = EdgeRegistry::new();
    for (pattern_key, pattern) in &mut patterns {
        pattern.stops.sort_by_key(|x| x.0);
        let Some(&line_id) = line_by_key.get(&pattern.route_key) else {
            continue;
        };
        let Some(stop_nodes) = pattern
            .stops
            .iter()
            .map(|row| node_by_physical.get(&row.2).copied())
            .collect::<Option<Vec<usize>>>()
        else {
            warn!(
                "[gtfs2graph] incomplete stop sequence for {}",
                pattern_key.direction_pattern_id
            );
            continue;
        };
        if stop_nodes.len() < 2 {
            continue;
        }
        let stop_positions: Vec<Point> = stop_nodes
            .iter()
            .map(|&id| graph.nodes[id].as_ref().unwrap().pos)
            .collect();
        let variants = pattern_weights
            .get(pattern_key)
            .cloned()
            .unwrap_or_else(|| vec![(pattern.shape_id.clone(), 1)]);
        for (shape_id, trip_count) in variants {
            let shape = shape_id.as_ref().and_then(|id| {
                shape_map.get(&(
                    pattern_key.onestop_feed_id.clone(),
                    pattern_key.attempt_id.clone(),
                    id.clone(),
                ))
            });
            // A missing referenced shape must not silently become a straight
            // line through a loop. Unshaped GTFS still uses LOOM's fallback.
            if shape_id.is_some() && shape.is_none() {
                warn!(
                    "[gtfs2graph] referenced shape {:?} missing for pattern {}",
                    shape_id, pattern_key.direction_pattern_id
                );
                continue;
            }
            let progression = if let Some(s) = shape {
                let Some(v) = s.match_stops(&stop_positions) else {
                    warn!(
                        "[gtfs2graph] no monotone shape alignment for pattern {} shape {:?}",
                        pattern_key.direction_pattern_id, shape_id
                    );
                    continue;
                };
                Some(v)
            } else {
                None
            };
            let mut previous = None;
            for i in 0..stop_nodes.len() - 1 {
                let (a, b) = (stop_nodes[i], stop_nodes[i + 1]);
                let (geometry, start, end) =
                    if let (Some(s), Some(ds)) = (shape, progression.as_ref()) {
                        let (start, end) = (ds[i], ds[i + 1]);
                        let Some(piece) = s.segment(start, end) else {
                            continue;
                        };
                        (
                            piece,
                            (start * 1000.0).round() as i64,
                            (end * 1000.0).round() as i64,
                        )
                    } else {
                        (vec![stop_positions[i], stop_positions[i + 1]], 0, 0)
                    };
                let key = SegmentKey {
                    from: a,
                    to: b,
                    shape_id: shape_id.clone(),
                    start_mm: start,
                    end_mm: end,
                };
                previous =
                    registry.append(&mut graph, key, geometry, line_id, trip_count, previous);
            }
        }
    }

    crate::loom_builder_simplify::simplify(
        &mut graph,
        &registry.weights,
        &mut registry.line_weights,
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
