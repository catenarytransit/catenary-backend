// Copyright Kyler Chin <kyler@catenarymaps.org>
// Catenary Transit Initiatives
// OSM Station Ranking and Cleaning Tool

use catenary::models::*;
use catenary::postgres_tools::make_async_pool;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use geo::{BoundingRect, Contains, Distance, Haversine, Point};
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::error::Error;

#[derive(QueryableByName, Debug)]
struct TerminalCountRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int8>)]
    osm_station_id: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int8>)]
    terminal_count: Option<i64>,
}

#[derive(QueryableByName, Debug)]
struct AssociatedShapeRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int8>)]
    osm_station_id: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    onestop_feed_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    attempt_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    shape_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    length: Option<f64>,
}

#[derive(QueryableByName, Debug)]
struct CentralityRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int8>)]
    osm_station_id: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int8>)]
    centrality: Option<i64>,
}

#[derive(Clone)]
struct PlacedStation {
    osm_id: i64,
    osm_type: String,
    lon: f64,
    lat: f64,
}

impl RTreeObject for PlacedStation {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.lon, self.lat])
    }
}

impl PointDistance for PlacedStation {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.lon - point[0];
        let dy = self.lat - point[1];
        dx * dx + dy * dy
    }
}

fn compute_mean_and_std(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }
    let sum: f64 = values.iter().sum();
    let mean = sum / (values.len() as f64);
    let variance: f64 = values
        .iter()
        .map(|&v| {
            let diff = v - mean;
            diff * diff
        })
        .sum::<f64>()
        / (values.len() as f64);
    let std = variance.sqrt();
    (mean, std)
}

fn standardise(v: f64, mean: f64, std: f64) -> f64 {
    if std < 1e-9 { 0.0 } else { (v - mean) / std }
}

fn get_icon_min_zoom(level: i16) -> i16 {
    match level {
        1 => 4,
        2 => 6,
        3 => 7,
        4 => 8,
        5 => 9,
        6 => 10,
        7 => 11,
        8 => 12,
        9 => 13,
        _ => 14,
    }
}

const BUFFER_UNORDERED_CONCURRENCY: usize = 7;

enum CountryArea {
    Polygon {
        cntr_id: String,
        poly: geo::Polygon<f64>,
        envelope: AABB<[f64; 2]>,
    },
    MultiPolygon {
        cntr_id: String,
        multi_poly: geo::MultiPolygon<f64>,
        envelope: AABB<[f64; 2]>,
    },
}

impl RTreeObject for CountryArea {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        match self {
            Self::Polygon { envelope, .. } => *envelope,
            Self::MultiPolygon { envelope, .. } => *envelope,
        }
    }
}

impl CountryArea {
    fn contains_point(&self, p: &Point<f64>) -> bool {
        match self {
            Self::Polygon { poly, .. } => poly.contains(p),
            Self::MultiPolygon { multi_poly, .. } => multi_poly.contains(p),
        }
    }

    fn cntr_id(&self) -> &str {
        match self {
            Self::Polygon { cntr_id, .. } => cntr_id,
            Self::MultiPolygon { cntr_id, .. } => cntr_id,
        }
    }
}

struct ScoredStation<'a> {
    station: &'a OsmStation,
    p_val: i32,
    f_val: i32,
    t_val: i32,
    r_val: i32,
    c_val: i32,
    is_intermodal: bool,
    score: f64,
    tram: bool,
    subway: bool,
    rail: bool,
}

struct StationWithTier<'a> {
    scored: ScoredStation<'a>,
    tier: i16,
}

fn score_and_tier_stations<'a>(
    stations: &[&'a OsmStation],
    platform_counts: &HashMap<i64, i32>,
    stops_counts: &HashMap<i64, i32>,
    terminal_counts: &HashMap<i64, i32>,
    route_span_logs: &HashMap<i64, i32>,
    centralities: &HashMap<i64, i32>,
    station_modes: &HashMap<i64, (bool, bool, bool)>,
    station_countries: &HashMap<i64, String>,
    w: &[f64; 6],
) -> Vec<StationWithTier<'a>> {
    if stations.is_empty() {
        return Vec::new();
    }

    let mut p_vals = Vec::new();
    let mut f_vals = Vec::new();
    let mut t_vals = Vec::new();
    let mut r_vals = Vec::new();
    let mut c_vals = Vec::new();

    for s in stations {
        p_vals.push(*platform_counts.get(&s.osm_id).unwrap_or(&0) as f64);
        f_vals.push(*stops_counts.get(&s.osm_id).unwrap_or(&0) as f64);
        t_vals.push(*terminal_counts.get(&s.osm_id).unwrap_or(&0) as f64);
        r_vals.push(*route_span_logs.get(&s.osm_id).unwrap_or(&0) as f64);
        c_vals.push(*centralities.get(&s.osm_id).unwrap_or(&0) as f64);
    }

    let (p_mean, p_std) = compute_mean_and_std(&p_vals);
    let (f_mean, f_std) = compute_mean_and_std(&f_vals);
    let (t_mean, t_std) = compute_mean_and_std(&t_vals);
    let (r_mean, r_std) = compute_mean_and_std(&r_vals);
    let (c_mean, c_std) = compute_mean_and_std(&c_vals);

    let mut scored_stations = Vec::new();
    for (idx, s) in stations.iter().enumerate() {
        let p_raw = p_vals[idx];
        let f_raw = f_vals[idx];
        let t_raw = t_vals[idx];
        let r_raw = r_vals[idx];
        let c_raw = c_vals[idx];

        let zp = standardise(p_raw, p_mean, p_std);
        let zf = standardise(f_raw, f_mean, f_std);
        let zt = standardise(t_raw, t_mean, t_std);
        let zr = standardise(r_raw, r_mean, r_std);
        let zc = standardise(c_raw, c_mean, c_std);

        let modes = station_modes
            .get(&s.osm_id)
            .copied()
            .unwrap_or((false, false, false));
        let active_modes_count = modes.0 as i32 + modes.1 as i32 + modes.2 as i32;
        let is_intermodal = active_modes_count > 1;
        let i_val = if is_intermodal { 1.0 } else { 0.0 };

        let mut adjusted_zr = zr;
        if t_raw == 0.0 {
            adjusted_zr *= 0.1;
        } else if t_raw < 5.0 {
            adjusted_zr *= 0.1 + 0.9 * (t_raw / 5.0);
        }

        let score = w[0] * zp + w[1] * zf + w[2] * zt + w[3] * adjusted_zr + w[4] * zc + w[5] * i_val;

        scored_stations.push(ScoredStation {
            station: *s,
            p_val: p_raw as i32,
            f_val: f_raw as i32,
            t_val: t_raw as i32,
            r_val: r_raw as i32,
            c_val: c_raw as i32,
            is_intermodal,
            score,
            tram: modes.0,
            subway: modes.1,
            rail: modes.2,
        });
    }

    let (mut rail_stations, mut non_rail_stations): (Vec<ScoredStation>, Vec<ScoredStation>) = scored_stations
        .into_iter()
        .partition(|s| s.rail);

    rail_stations.sort_by(|a, b| a.score.total_cmp(&b.score));
    non_rail_stations.sort_by(|a, b| a.score.total_cmp(&b.score));

    let mut stations_with_tiers = Vec::new();

    let num_rail = rail_stations.len();
    for (idx, item) in rail_stations.into_iter().enumerate() {
        let percentile = (idx + 1) as f64 / num_rail as f64;
        let is_ch_or_be = station_countries
            .get(&item.station.osm_id)
            .map(|c| c == "CH" || c == "BE")
            .unwrap_or(false);

        let tier = if is_ch_or_be {
            if percentile > 0.999 {
                1
            } else if percentile > 0.996 {
                2
            } else if percentile > 0.99 {
                3
            } else if percentile > 0.95 {
                4
            } else if percentile > 0.90 {
                5
            } else {
                6
            }
        } else {
            if percentile > 0.998 {
                1
            } else if percentile > 0.995 {
                2
            } else if percentile > 0.98 {
                3
            } else if percentile > 0.95 {
                4
            } else if percentile > 0.90 {
                5
            } else {
                6
            }
        };
        stations_with_tiers.push(StationWithTier { scored: item, tier });
    }

    let num_non_rail = non_rail_stations.len();
    for (idx, item) in non_rail_stations.into_iter().enumerate() {
        let percentile = (idx + 1) as f64 / num_non_rail as f64;
        let tier = if percentile > 0.90 {
            7
        } else if percentile > 0.50 {
            8
        } else {
            9
        };
        stations_with_tiers.push(StationWithTier { scored: item, tier });
    }

    stations_with_tiers
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut geojson_path = std::env::current_dir()?;
    let mut found = false;
    for _ in 0..10 {
        let candidate = geojson_path.join("CNTR_RG_03M_2024_4326.geojson");
        if candidate.exists() {
            geojson_path = std::fs::canonicalize(candidate)?;
            found = true;
            break;
        }
        if !geojson_path.pop() {
            break;
        }
    }

    if !found {
        return Err("Could not find CNTR_RG_03M_2024_4326.geojson in any parent directory".into());
    }

    println!("Loading country boundaries from: {:?}", geojson_path);
    let file = std::fs::File::open(&geojson_path)?;
    let reader = std::io::BufReader::new(file);
    let geojson = serde_json::from_reader::<_, geojson::GeoJson>(reader)?;

    let target_cntr_ids: HashSet<&str> = ["DE", "CH", "NL", "UK", "BE", "CZ", "IE"].iter().copied().collect();
    let mut country_areas = Vec::new();

    if let geojson::GeoJson::FeatureCollection(fc) = geojson {
        for feature in fc.features {
            if let Some(ref props) = feature.properties {
                if let Some(cntr_id_val) = props.get("CNTR_ID") {
                    if let Some(cntr_id_str) = cntr_id_val.as_str() {
                        if target_cntr_ids.contains(cntr_id_str) {
                            if let Some(geometry) = feature.geometry {
                                let geo_geom: geo::Geometry<f64> = geometry.try_into()?;
                                match geo_geom {
                                    geo::Geometry::Polygon(poly) => {
                                        if let Some(bbox) = poly.bounding_rect() {
                                            let envelope = AABB::from_corners(
                                                [bbox.min().x, bbox.min().y],
                                                [bbox.max().x, bbox.max().y],
                                            );
                                            country_areas.push(CountryArea::Polygon {
                                                cntr_id: cntr_id_str.to_string(),
                                                poly,
                                                envelope,
                                            });
                                        }
                                    }
                                    geo::Geometry::MultiPolygon(multi_poly) => {
                                        if let Some(bbox) = multi_poly.bounding_rect() {
                                            let envelope = AABB::from_corners(
                                                [bbox.min().x, bbox.min().y],
                                                [bbox.max().x, bbox.max().y],
                                            );
                                            country_areas.push(CountryArea::MultiPolygon {
                                                cntr_id: cntr_id_str.to_string(),
                                                multi_poly,
                                                envelope,
                                            });
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let country_rtree = RTree::bulk_load(country_areas);
    println!("Loaded {} target country polygons/multipolygons into R-Tree.", country_rtree.size());

    let pool = make_async_pool().await?;
    let mut conn = pool.get().await?;

    // 1. Initialize Run
    use catenary::schema::gtfs::osm_stations_ranking_runs::dsl as runs_dsl;
    let run_id: i32 = runs_dsl::osm_stations_ranking_runs
        .select(diesel::dsl::max(runs_dsl::run_id))
        .first::<Option<i32>>(&mut conn)
        .await?
        .unwrap_or(0)
        + 1;

    let run_start_time = Utc::now();
    let new_run = OsmStationsRankingRunInsert {
        run_id,
        time_start: run_start_time,
        time_end: None,
        allowed_spatial_query: false,
    };
    diesel::insert_into(runs_dsl::osm_stations_ranking_runs)
        .values(&new_run)
        .execute(&mut conn)
        .await?;

    println!("Started ranking run ID: {}", run_id);

    // 2. Fetch Import Groups
    use catenary::schema::gtfs::osm_station_imports::dsl as imports_dsl;
    let imports: Vec<(i32, String)> = imports_dsl::osm_station_imports
        .select((imports_dsl::import_id, imports_dsl::file_name))
        .load::<(i32, String)>(&mut conn)
        .await?;

    println!("Loaded {} OSM station import records.", imports.len());

    let mut groups: HashMap<String, Vec<i32>> = HashMap::new();
    for (import_id, file_name) in imports {
        groups.entry(file_name).or_default().push(import_id);
    }
    println!(
        "Grouped imports into {} distinct file name groups.",
        groups.len()
    );

    use catenary::schema::gtfs::osm_stations_ranked::dsl as ranked_dsl;
    let is_table_empty: bool = ranked_dsl::osm_stations_ranked
        .select(diesel::dsl::count_star())
        .first::<i64>(&mut conn)
        .await?
        == 0;
    println!("Is gtfs.osm_stations_ranked empty? {}", is_table_empty);
    let allowed_spatial_query_val = is_table_empty;

    // empirical weights vector: [P, F, T, R, C, I]
    let w = [2.0, 1.0, 1.5, 2.0, 1.5, 1.0];

    // 3. Phased Processing per file_name
    for (file_name, import_ids) in groups {
        println!(
            "Processing import group: {} (contains import IDs: {:?})",
            file_name, import_ids
        );

        let mut ranked_inserts: Vec<OsmStationRankedInsert> = Vec::new();

        use catenary::schema::gtfs::osm_stations::dsl as stations_dsl;
        let parent_stations: Vec<OsmStation> = stations_dsl::osm_stations
            .filter(stations_dsl::import_id.eq_any(&import_ids))
            .filter(stations_dsl::parent_osm_id.is_null())
            .load::<OsmStation>(&mut conn)
            .await?;

        if parent_stations.is_empty() {
            println!("No parent stations found in group {}", file_name);
            continue;
        }

        println!(
            "Loaded {} parent stations for group {}",
            parent_stations.len(),
            file_name
        );

        let parent_osm_ids: Vec<i64> = parent_stations.iter().map(|s| s.osm_id).collect();

        // Active modes compilation
        let child_modes: Vec<(Option<i64>, String)> = stations_dsl::osm_stations
            .filter(stations_dsl::import_id.eq_any(&import_ids))
            .filter(stations_dsl::parent_osm_id.is_not_null())
            .select((stations_dsl::parent_osm_id, stations_dsl::mode_type))
            .load::<(Option<i64>, String)>(&mut conn)
            .await?;

        println!(
            "Loaded {} child station mode records for mode compilation.",
            child_modes.len()
        );

        let mut parent_to_child_modes: HashMap<i64, Vec<String>> = HashMap::new();
        for (parent_id, m_type) in child_modes {
            if let Some(pid) = parent_id {
                parent_to_child_modes.entry(pid).or_default().push(m_type);
            }
        }

        let mut station_modes: HashMap<i64, (bool, bool, bool)> = HashMap::new();
        for s in &parent_stations {
            let mut tram = false;
            let mut subway = false;
            let mut rail = false;

            if let Some(child_types) = parent_to_child_modes.get(&s.osm_id) {
                for m_type in child_types {
                    match m_type.as_str() {
                        "rail" => rail = true,
                        "subway" => subway = true,
                        "tram" => tram = true,
                        "light_rail" => {
                            tram = true;
                        }
                        _ => {}
                    }
                }
            } else {
                match s.mode_type.as_str() {
                    "rail" => rail = true,
                    "subway" => subway = true,
                    "tram" | "light_rail" => tram = true,
                    _ => {}
                }
            }
            station_modes.insert(s.osm_id, (tram, subway, rail));
        }

        // Feature: Platform Count (P_i) using Diesel DSL query builder
        let platform_counts_vec: Vec<(Option<i64>, i64)> = stations_dsl::osm_stations
            .filter(stations_dsl::import_id.eq_any(&import_ids))
            .filter(stations_dsl::parent_osm_id.is_not_null())
            .filter(
                stations_dsl::ref_
                    .is_not_null()
                    .or(stations_dsl::local_ref.is_not_null()),
            )
            .group_by(stations_dsl::parent_osm_id)
            .select((stations_dsl::parent_osm_id, diesel::dsl::count_star()))
            .load::<(Option<i64>, i64)>(&mut conn)
            .await?;

        let platform_counts: HashMap<i64, i32> = platform_counts_vec
            .into_iter()
            .filter_map(|(pid, cnt)| pid.map(|id| (id, cnt as i32)))
            .collect();
        println!(
            "Retrieved platform counts for {} stations.",
            platform_counts.len()
        );

        let mut stops_counts: HashMap<i64, i32> = HashMap::new();
        let mut terminal_counts: HashMap<i64, i32> = HashMap::new();
        let mut station_shape_logs: HashMap<i64, f64> = HashMap::new();
        let mut centralities: HashMap<i64, i32> = HashMap::new();

        use catenary::schema::gtfs::stops::dsl as stops_dsl;

        let chunks: Vec<Vec<i64>> = parent_osm_ids.chunks(10).map(|c| c.to_vec()).collect();
        let total_chunks = chunks.len();
        println!(
            "Querying features (stops, terminals, shapes, centrality) in {} chunk(s) of 10 stations concurrently...",
            total_chunks
        );

        let mut feature_stream = futures::stream::iter(chunks.into_iter().enumerate().map(|(chunk_idx, chunk)| {
            let pool = pool.clone();
            async move {
                let mut conn = pool.get().await.map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

                // Feature: Associated Stops Count (F_i) using Diesel DSL query builder
                let chunk_stops_counts: Vec<(Option<i64>, i64)> = stops_dsl::stops
                    .filter(stops_dsl::osm_station_id.eq_any(&chunk))
                    .group_by(stops_dsl::osm_station_id)
                    .select((stops_dsl::osm_station_id, diesel::dsl::count_star()))
                    .load::<(Option<i64>, i64)>(&mut conn)
                    .await?;

                let mut stops = Vec::new();
                for (sid, count) in chunk_stops_counts {
                    if let Some(osm_id) = sid {
                        stops.push((osm_id, count as i32));
                    }
                }

                // Feature: Terminal Route Count (T_i) using early filtering and index lookups
                let chunk_terminal_counts = diesel::sql_query(
                    "SELECT \
                         s.osm_station_id, \
                         COUNT(DISTINCT (dpm.chateau, dpm.route_id))::bigint as terminal_count \
                     FROM gtfs.stops s \
                     JOIN gtfs.direction_pattern dp \
                       ON s.chateau = dp.chateau \
                      AND s.gtfs_id = dp.stop_id \
                     JOIN gtfs.direction_pattern_meta dpm \
                       ON dp.onestop_feed_id = dpm.onestop_feed_id \
                      AND dp.attempt_id = dpm.attempt_id \
                      AND dp.direction_pattern_id = dpm.direction_pattern_id \
                     WHERE s.osm_station_id = ANY($1) \
                       AND dpm.route_type IS DISTINCT FROM 3 \
                       AND ( \
                           dp.stop_sequence = ( \
                               SELECT MIN(dp_sub.stop_sequence) \
                               FROM gtfs.direction_pattern dp_sub \
                               WHERE dp_sub.onestop_feed_id = dp.onestop_feed_id \
                                 AND dp_sub.attempt_id = dp.attempt_id \
                                 AND dp_sub.direction_pattern_id = dp.direction_pattern_id \
                           ) \
                           OR \
                           dp.stop_sequence = ( \
                               SELECT MAX(dp_sub.stop_sequence) \
                               FROM gtfs.direction_pattern dp_sub \
                               WHERE dp_sub.onestop_feed_id = dp.onestop_feed_id \
                                 AND dp_sub.attempt_id = dp.attempt_id \
                                 AND dp_sub.direction_pattern_id = dp.direction_pattern_id \
                           ) \
                       ) \
                     GROUP BY s.osm_station_id"
                )
                .bind::<diesel::sql_types::Array<diesel::sql_types::Int8>, _>(&chunk)
                .get_results::<TerminalCountRow>(&mut conn)
                .await?;

                let mut terminals = Vec::new();
                for r in chunk_terminal_counts {
                    if let Some(osm_id) = r.osm_station_id {
                        terminals.push((osm_id, r.terminal_count.unwrap_or(0) as i32));
                    }
                }

                // Feature: Route Span Log (R_i)
                let chunk_shapes = diesel::sql_query(
                    "WITH associated_shape_keys AS ( \
                          SELECT DISTINCT s.osm_station_id, dpm.onestop_feed_id, dpm.attempt_id, dpm.gtfs_shape_id as shape_id \
                          FROM gtfs.direction_pattern_meta dpm \
                          JOIN gtfs.direction_pattern dp \
                            ON dpm.onestop_feed_id = dp.onestop_feed_id \
                           AND dpm.attempt_id = dp.attempt_id \
                           AND dp.direction_pattern_id = dpm.direction_pattern_id \
                          JOIN gtfs.stops s \
                            ON dp.chateau = s.chateau \
                           AND dp.stop_id = s.gtfs_id \
                          WHERE s.osm_station_id = ANY($1) \
                            AND dpm.route_type IS DISTINCT FROM 3 \
                            AND dpm.gtfs_shape_id IS NOT NULL \
                          UNION \
                          SELECT DISTINCT s.osm_station_id, ipm.onestop_feed_id, ipm.attempt_id, ipm.shape_id \
                          FROM gtfs.itinerary_pattern_meta ipm \
                          JOIN gtfs.itinerary_pattern ip \
                            ON ipm.onestop_feed_id = ip.onestop_feed_id \
                           AND ipm.attempt_id = ip.attempt_id \
                           AND ip.itinerary_pattern_id = ipm.itinerary_pattern_id \
                          JOIN gtfs.stops s \
                            ON ip.chateau = s.chateau \
                           AND ip.stop_id = s.gtfs_id \
                          JOIN gtfs.routes r \
                            ON ipm.onestop_feed_id = r.onestop_feed_id \
                           AND ipm.attempt_id = r.attempt_id \
                           AND ipm.route_id = r.route_id \
                          WHERE s.osm_station_id = ANY($1) \
                            AND r.route_type IS DISTINCT FROM 3 \
                            AND ipm.shape_id IS NOT NULL \
                      ) \
                      SELECT ask.osm_station_id, ask.onestop_feed_id, ask.attempt_id, ask.shape_id, ST_Length(sh.linestring::geography)::double precision as length \
                      FROM associated_shape_keys ask \
                      JOIN gtfs.shapes sh \
                        ON ask.onestop_feed_id = sh.onestop_feed_id \
                       AND ask.attempt_id = sh.attempt_id \
                       AND ask.shape_id = sh.shape_id"
                )
                .bind::<diesel::sql_types::Array<diesel::sql_types::Int8>, _>(&chunk)
                .get_results::<AssociatedShapeRow>(&mut conn)
                .await?;

                let mut shapes = Vec::new();
                for row in chunk_shapes {
                    if let Some(osm_id) = row.osm_station_id {
                        let length_m = row.length.unwrap_or(0.0);
                        let ln_val = (length_m + 1.0).ln();
                        let boost = if length_m > 100_000.0 {
                            (length_m / 100_000.0).ln()
                        } else {
                            0.0
                        };
                        let log_val = ln_val * (1.0 + 1.5 * boost);
                        shapes.push((osm_id, log_val));
                    }
                }

                // Feature: Degree Centrality (C_i)
                let chunk_centralities = diesel::sql_query(
                    "WITH our_dp_rows AS ( \
                          SELECT \
                              dp.onestop_feed_id, \
                              dp.attempt_id, \
                              dp.direction_pattern_id, \
                              dp.stop_sequence \
                          FROM gtfs.direction_pattern dp \
                          JOIN gtfs.stops s \
                            ON dp.chateau = s.chateau \
                           AND dp.stop_id = s.gtfs_id \
                          WHERE s.osm_station_id = ANY($1) \
                      ), \
                      ordered_dp AS ( \
                          SELECT \
                              dp.chateau, \
                              dp.onestop_feed_id, \
                              dp.attempt_id, \
                              dp.direction_pattern_id, \
                              dp.stop_sequence, \
                              dp.stop_id, \
                              LAG(dp.stop_id) OVER (PARTITION BY dp.onestop_feed_id, dp.attempt_id, dp.direction_pattern_id ORDER BY dp.stop_sequence) as prev_stop_id, \
                              LEAD(dp.stop_id) OVER (PARTITION BY dp.onestop_feed_id, dp.attempt_id, dp.direction_pattern_id ORDER BY dp.stop_sequence) as next_stop_id \
                          FROM gtfs.direction_pattern dp \
                          WHERE (dp.onestop_feed_id, dp.attempt_id, dp.direction_pattern_id) IN ( \
                              SELECT DISTINCT onestop_feed_id, attempt_id, direction_pattern_id FROM our_dp_rows \
                          ) \
                      ), \
                      adj_stops AS ( \
                          SELECT DISTINCT \
                              r.onestop_feed_id, \
                              r.attempt_id, \
                              r.direction_pattern_id, \
                              r.stop_sequence, \
                              s_curr.osm_station_id as current_osm_id, \
                              unnest(ARRAY[od.prev_stop_id, od.next_stop_id]) as adj_stop_id \
                          FROM ordered_dp od \
                          JOIN our_dp_rows r \
                            ON od.onestop_feed_id = r.onestop_feed_id \
                           AND od.attempt_id = r.attempt_id \
                           AND od.direction_pattern_id = r.direction_pattern_id \
                           AND od.stop_sequence = r.stop_sequence \
                          JOIN gtfs.stops s_curr \
                            ON od.chateau = s_curr.chateau \
                           AND od.stop_id = s_curr.gtfs_id \
                      ) \
                      SELECT \
                          a.current_osm_id as osm_station_id, \
                          COUNT(DISTINCT COALESCE(s.osm_station_id::text, s.gtfs_id))::bigint as centrality \
                      FROM adj_stops a \
                      JOIN gtfs.stops s \
                        ON a.onestop_feed_id = s.onestop_feed_id \
                       AND a.attempt_id = s.attempt_id \
                       AND a.adj_stop_id = s.gtfs_id \
                      WHERE a.adj_stop_id IS NOT NULL \
                        AND s.osm_station_id IS DISTINCT FROM a.current_osm_id \
                      GROUP BY a.current_osm_id"
                )
                .bind::<diesel::sql_types::Array<diesel::sql_types::Int8>, _>(&chunk)
                .get_results::<CentralityRow>(&mut conn)
                .await?;

                let mut cents = Vec::new();
                for r in chunk_centralities {
                    if let Some(osm_id) = r.osm_station_id {
                        cents.push((osm_id, r.centrality.unwrap_or(0) as i32));
                    }
                }

                Ok::<_, Box<dyn Error + Send + Sync>>((chunk_idx, stops, terminals, shapes, cents))
            }
        }))
        .buffer_unordered(BUFFER_UNORDERED_CONCURRENCY);

        while let Some(res) = feature_stream.next().await {
            let (chunk_idx, stops, terminals, shapes, cents) = res?;
            println!(
                "  Processed features chunk {}/{}...",
                chunk_idx + 1,
                total_chunks
            );
            for (osm_id, count) in stops {
                stops_counts.insert(osm_id, count);
            }
            for (osm_id, count) in terminals {
                terminal_counts.insert(osm_id, count);
            }
            for (osm_id, log_val) in shapes {
                *station_shape_logs.entry(osm_id).or_default() += log_val;
            }
            for (osm_id, count) in cents {
                centralities.insert(osm_id, count);
            }
        }

        let route_span_logs: HashMap<i64, i32> = station_shape_logs
            .into_iter()
            .map(|(osm_id, sum_val)| (osm_id, sum_val.round() as i32))
            .collect();

        println!(
            "Feature extraction complete. Extracted feature counts: stops={}, terminals={}, route_spans={}, centralities={}",
            stops_counts.len(),
            terminal_counts.len(),
            route_span_logs.len(),
            centralities.len()
        );

        // 4. Score Calculation & Partitioned Cycle
        let mut station_countries = HashMap::new();
        let mut target_country_stations = Vec::new();
        let mut other_stations = Vec::new();

        for s in &parent_stations {
            let check_point = geo::Point::new(s.point.x, s.point.y);
            let mut matched_country = None;
            let lat_delta = 0.01;
            let lon_delta = 0.01;
            let search_box = rstar::AABB::from_corners(
                [s.point.x - lon_delta, s.point.y - lat_delta],
                [s.point.x + lon_delta, s.point.y + lat_delta],
            );
            for area in country_rtree.locate_in_envelope(&search_box) {
                if area.contains_point(&check_point) {
                    matched_country = Some(area.cntr_id().to_string());
                    break;
                }
            }
            if let Some(country) = matched_country {
                station_countries.insert(s.osm_id, country);
                target_country_stations.push(s);
            } else {
                other_stations.push(s);
            }
        }

        println!(
            "Group partitioning: target_country_stations = {}, other_stations = {}",
            target_country_stations.len(),
            other_stations.len()
        );

        let mut stations_with_tiers_target = score_and_tier_stations(
            &target_country_stations,
            &platform_counts,
            &stops_counts,
            &terminal_counts,
            &route_span_logs,
            &centralities,
            &station_modes,
            &station_countries,
            &w,
        );

        let mut stations_with_tiers_other = score_and_tier_stations(
            &other_stations,
            &platform_counts,
            &stops_counts,
            &terminal_counts,
            &route_span_logs,
            &centralities,
            &station_modes,
            &station_countries,
            &w,
        );

        let mut stations_with_tiers = Vec::new();
        stations_with_tiers.append(&mut stations_with_tiers_target);
        stations_with_tiers.append(&mut stations_with_tiers_other);

        stations_with_tiers.sort_by(|a, b| b.scored.score.total_cmp(&a.scored.score));

        // 5. RDSS Phase
        println!(
            "Starting Resolution-Dependent Spatial Suppression (RDSS) for {} stations...",
            stations_with_tiers.len()
        );
        let mut rtree: RTree<PlacedStation> = RTree::new();
        let mut final_label_zooms: HashMap<i64, i16> = HashMap::new();
        let mut overshadowed_by: HashMap<i64, (i64, String)> = HashMap::new();

        // Evaluate zoom levels 4 to 15
        for z in 4..=15 {
            let mut placed_count = 0;
            let mut overshadowed_count = 0;
            let mut base_zoom_filtered = 0;
            let mut subway_skipped = 0;
            if z >= 9 {
                // Above zoom 9, suppression doesn't apply. Assign remaining stations max(9, base_zoom)
                for item in &stations_with_tiers {
                    let osm_id = item.scored.station.osm_id;
                    if final_label_zooms.contains_key(&osm_id) {
                        continue;
                    }
                    let base_zoom = get_icon_min_zoom(item.tier);
                    let final_zoom = z.max(base_zoom);
                    final_label_zooms.insert(osm_id, final_zoom);
                    placed_count += 1;
                }
            } else {
                // Zoom levels below 9: Apply Resolution-Dependent Spatial Suppression
                let r_z = match z {
                    4 => 100.0,
                    5 => 50.0,
                    6 => 25.0,
                    7 => 11.0,
                    8 => 5.0,
                    _ => 0.0,
                };

                for item in &stations_with_tiers {
                    let osm_id = item.scored.station.osm_id;
                    if final_label_zooms.contains_key(&osm_id) {
                        continue;
                    }

                    let base_zoom = get_icon_min_zoom(item.tier);
                    if z < base_zoom {
                        base_zoom_filtered += 1;
                        continue;
                    }

                    // Subway stations do not participate in spatial suppression/overshadowing
                    if item.scored.subway {
                        final_label_zooms.insert(osm_id, z);
                        subway_skipped += 1;
                        continue;
                    }

                    // Check for spatial collision with previously placed stations
                    let lon = item.scored.station.point.x;
                    let lat = item.scored.station.point.y;
                    let lat_delta = r_z / 110.0;
                    let lon_delta = r_z / (110.0 * lat.to_radians().cos().abs());
                    let search_box = AABB::from_corners(
                        [lon - lon_delta, lat - lat_delta],
                        [lon + lon_delta, lat + lat_delta],
                    );

                    let candidates: Vec<&PlacedStation> =
                        rtree.locate_in_envelope(&search_box).collect();
                    let mut collision_with: Option<&PlacedStation> = None;

                    for cand in candidates {
                        let p1 = Point::new(lon, lat);
                        let p2 = Point::new(cand.lon, cand.lat);
                        let dist_km = Haversine.distance(p1, p2) / 1000.0;
                        if dist_km < r_z {
                            collision_with = Some(cand);
                            break;
                        }
                    }

                    if let Some(parent_station) = collision_with {
                        // Collided at zoom level z, record who overshadowed it
                        overshadowed_by.insert(
                            osm_id,
                            (parent_station.osm_id, parent_station.osm_type.clone()),
                        );
                        overshadowed_count += 1;
                    } else {
                        // No collision: assign label_min_zoom and insert into R-Tree
                        final_label_zooms.insert(osm_id, z);
                        rtree.insert(PlacedStation {
                            osm_id,
                            osm_type: item.scored.station.osm_type.clone(),
                            lon,
                            lat,
                        });
                        placed_count += 1;
                    }
                }
            }
            println!(
                "  Zoom {:>2}: placed={:<4} overshadowed={:<4} subway_skipped={:<4} base_zoom_filtered={:<4}",
                z, placed_count, overshadowed_count, subway_skipped, base_zoom_filtered
            );
        }

        // Collect insertion rows
        for item in stations_with_tiers {
            let osm_id = item.scored.station.osm_id;
            let label_zoom = final_label_zooms.get(&osm_id).copied().unwrap_or(15);
            let base_zoom = get_icon_min_zoom(item.tier);

            let (over_id, over_type) = if label_zoom > base_zoom {
                // If it was overshadowed before being cleared, fetch its last overshadowing station
                overshadowed_by
                    .get(&osm_id)
                    .map(|(id, t)| (Some(*id), Some(t.clone())))
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };

            ranked_inserts.push(OsmStationRankedInsert {
                osm_id,
                osm_type: item.scored.station.osm_type.clone(),
                run_id,
                point: item.scored.station.point.clone(),
                name: item.scored.station.name.clone(),
                name_translations: item.scored.station.name_translations.clone(),
                station_type: item.scored.station.station_type.clone(),
                railway_tag: item.scored.station.railway_tag.clone(),
                mode_type: item.scored.station.mode_type.clone(),
                uic_ref: item.scored.station.uic_ref.clone(),
                wikidata: item.scored.station.wikidata.clone(),
                operator: item.scored.station.operator.clone(),
                network: item.scored.station.network.clone(),
                tram: item.scored.tram,
                subway: item.scored.subway,
                rail: item.scored.rail,
                number_of_associated_stops: Some(item.scored.f_val),
                platform_count: Some(item.scored.p_val),
                terminal_route_count: item.scored.t_val,
                route_span_log: item.scored.r_val,
                degree_centrality: item.scored.c_val,
                importance_level_station: item.tier,
                admin_hierarchy: item.scored.station.admin_hierarchy.clone(),
                label_min_zoom: label_zoom,
                icon_min_zoom: base_zoom,
                overshadowed_by_osm_id: over_id,
                overshadowed_by_osm_type: over_type,
                allowed_spatial_query: allowed_spatial_query_val,
            });
        }
        // 6. Save Results in batches immediately for this group
        let total_save_chunks = (ranked_inserts.len() + 999) / 1000;
        println!(
            "Saving {} ranked stations from group {} to database in {} chunk(s)...",
            ranked_inserts.len(),
            file_name,
            total_save_chunks
        );
        use diesel::pg::upsert::excluded;
        for (chunk_idx, chunk) in ranked_inserts.chunks(1000).enumerate() {
            println!(
                "  Inserting chunk {}/{} (size: {})...",
                chunk_idx + 1,
                total_save_chunks,
                chunk.len()
            );
            diesel::insert_into(ranked_dsl::osm_stations_ranked)
                .values(chunk)
                .on_conflict((ranked_dsl::osm_id, ranked_dsl::osm_type, ranked_dsl::run_id))
                .do_update()
                .set((
                    ranked_dsl::point.eq(excluded(ranked_dsl::point)),
                    ranked_dsl::name.eq(excluded(ranked_dsl::name)),
                    ranked_dsl::name_translations.eq(excluded(ranked_dsl::name_translations)),
                    ranked_dsl::station_type.eq(excluded(ranked_dsl::station_type)),
                    ranked_dsl::railway_tag.eq(excluded(ranked_dsl::railway_tag)),
                    ranked_dsl::mode_type.eq(excluded(ranked_dsl::mode_type)),
                    ranked_dsl::uic_ref.eq(excluded(ranked_dsl::uic_ref)),
                    ranked_dsl::wikidata.eq(excluded(ranked_dsl::wikidata)),
                    ranked_dsl::operator.eq(excluded(ranked_dsl::operator)),
                    ranked_dsl::network.eq(excluded(ranked_dsl::network)),
                    ranked_dsl::tram.eq(excluded(ranked_dsl::tram)),
                    ranked_dsl::subway.eq(excluded(ranked_dsl::subway)),
                    ranked_dsl::rail.eq(excluded(ranked_dsl::rail)),
                    ranked_dsl::number_of_associated_stops.eq(excluded(ranked_dsl::number_of_associated_stops)),
                    ranked_dsl::platform_count.eq(excluded(ranked_dsl::platform_count)),
                    ranked_dsl::terminal_route_count.eq(excluded(ranked_dsl::terminal_route_count)),
                    ranked_dsl::route_span_log.eq(excluded(ranked_dsl::route_span_log)),
                    ranked_dsl::degree_centrality.eq(excluded(ranked_dsl::degree_centrality)),
                    ranked_dsl::importance_level_station.eq(excluded(ranked_dsl::importance_level_station)),
                    ranked_dsl::admin_hierarchy.eq(excluded(ranked_dsl::admin_hierarchy)),
                    ranked_dsl::label_min_zoom.eq(excluded(ranked_dsl::label_min_zoom)),
                    ranked_dsl::icon_min_zoom.eq(excluded(ranked_dsl::icon_min_zoom)),
                    ranked_dsl::overshadowed_by_osm_id.eq(excluded(ranked_dsl::overshadowed_by_osm_id)),
                    ranked_dsl::overshadowed_by_osm_type.eq(excluded(ranked_dsl::overshadowed_by_osm_type)),
                    ranked_dsl::allowed_spatial_query.eq(excluded(ranked_dsl::allowed_spatial_query)),
                ))
                .execute(&mut conn)
                .await?;
        }
    }

    // 7. Transactional Publishing
    println!(
        "Committing transaction to finalize run ID: {} and cleanup previous runs...",
        run_id
    );
    use diesel_async::AsyncConnection;
    conn.transaction::<_, Box<dyn Error + Send + Sync>, _>(|tx| {
        Box::pin(async move {
            // Update run as ended
            diesel::update(runs_dsl::osm_stations_ranking_runs.filter(runs_dsl::run_id.eq(run_id)))
                .set((
                    runs_dsl::time_end.eq(Utc::now()),
                    runs_dsl::allowed_spatial_query.eq(true),
                ))
                .execute(tx)
                .await?;

            // Set allowed_spatial_query = true for the current run's rows
            diesel::update(ranked_dsl::osm_stations_ranked.filter(ranked_dsl::run_id.eq(run_id)))
                .set(ranked_dsl::allowed_spatial_query.eq(true))
                .execute(tx)
                .await?;

            // Delete old runs' rows from gtfs.osm_stations_ranked
            diesel::delete(ranked_dsl::osm_stations_ranked.filter(ranked_dsl::run_id.ne(run_id)))
                .execute(tx)
                .await?;

            // Delete old runs from gtfs.osm_stations_ranking_runs
            diesel::delete(runs_dsl::osm_stations_ranking_runs.filter(runs_dsl::run_id.ne(run_id)))
                .execute(tx)
                .await?;

            Ok(())
        })
    })
    .await?;

    let duration = Utc::now() - run_start_time;
    println!(
        "Ranking run completed successfully in {}s.",
        duration.num_seconds()
    );
    Ok(())
}
