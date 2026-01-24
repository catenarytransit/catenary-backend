use anyhow::Result;
use catenary::models::{Route, Shape, Stop};
use catenary::schema::gtfs::{routes, shapes, stops};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use log::{debug, info};

use crate::osm_types::{Line, LineId, RailMode};
use crate::regions::RegionConfig;

/// Type alias for diesel sync pool
pub type SyncPool = Pool<ConnectionManager<diesel::pg::PgConnection>>;

/// Shape data with extracted geometry points
#[derive(Debug, Clone)]
pub struct GtfsShape {
    pub chateau: String,
    pub shape_id: String,
    pub route_ids: Vec<String>,
    pub route_type: i16,
    pub color: String,
    pub label: Option<String>,
    /// Geometry as (lon, lat) points
    pub geometry: Vec<(f64, f64)>,
}

/// Stop data with position
#[derive(Debug, Clone)]
pub struct GtfsStop {
    pub chateau: String,
    pub stop_id: String,
    pub name: Option<String>,
    pub position: (f64, f64),
    pub route_types: Vec<i16>,
}

/// Loader for GTFS data from PostgreSQL using diesel
pub struct GtfsLoader {
    pool: SyncPool,
    bbox: (f64, f64, f64, f64), // (west, south, east, north)
}

impl GtfsLoader {
    pub fn new(pool: SyncPool, region_config: &RegionConfig) -> Self {
        Self {
            pool,
            bbox: region_config.bbox,
        }
    }

    /// Load all rail shapes within the region bbox
    pub fn load_rail_shapes(&self) -> Result<(Vec<Line>, Vec<GtfsShape>)> {
        let mut conn = self.pool.get()?;

        // Rail route types: 0=tram, 1=subway, 2=rail, 5=cable, 7=funicular, 12=monorail
        let rail_types: Vec<i16> = vec![0, 1, 2, 5, 7, 12];

        info!("Loading rail shapes from database...");

        let shape_results: Vec<Shape> = shapes::table
            .filter(shapes::route_type.eq_any(&rail_types))
            .filter(shapes::allowed_spatial_query.eq(true))
            .load(&mut conn)?;

        info!("Loaded {} shapes from database", shape_results.len());

        // Also load routes for metadata
        let route_results: Vec<Route> = routes::table
            .filter(routes::route_type.eq_any(&rail_types))
            .load(&mut conn)?;

        info!("Loaded {} routes from database", route_results.len());

        // Build route lookup map
        let route_map: std::collections::HashMap<(String, String), &Route> = route_results
            .iter()
            .map(|r| ((r.chateau.clone(), r.route_id.clone()), r))
            .collect();

        // Convert to our types
        let mut lines = Vec::new();
        let mut gtfs_shapes = Vec::new();
        let mut seen_lines: std::collections::HashSet<String> = std::collections::HashSet::new();

        for shape in shape_results {
            // Extract geometry points from postgis linestring
            let geometry: Vec<(f64, f64)> =
                shape.linestring.points.iter().map(|p| (p.x, p.y)).collect();

            // Filter by bbox
            let in_bbox = geometry.iter().any(|(lon, lat)| {
                *lon >= self.bbox.0
                    && *lon <= self.bbox.2
                    && *lat >= self.bbox.1
                    && *lat <= self.bbox.3
            });

            if !in_bbox || geometry.len() < 2 {
                continue;
            }

            let route_ids: Vec<String> = shape
                .routes
                .unwrap_or_default()
                .into_iter()
                .flatten()
                .collect();

            // Create Line entries for each route
            for route_id in &route_ids {
                let line_key = format!("{}:{}", shape.chateau, route_id);
                if !seen_lines.contains(&line_key) {
                    seen_lines.insert(line_key);

                    let (label, color, agency_id) = if let Some(route) =
                        route_map.get(&(shape.chateau.clone(), route_id.clone()))
                    {
                        (
                            route
                                .short_name
                                .clone()
                                .or(route.long_name.clone())
                                .unwrap_or_else(|| route_id.clone()),
                            route.color.clone().unwrap_or_else(|| "888888".to_string()),
                            route.agency_id.clone(),
                        )
                    } else {
                        (route_id.clone(), "888888".to_string(), None)
                    };

                    lines.push(Line {
                        id: LineId::new(shape.chateau.clone(), route_id.clone()),
                        label,
                        color,
                        route_type: shape.route_type,
                        agency_id,
                    });
                }
            }

            gtfs_shapes.push(GtfsShape {
                chateau: shape.chateau,
                shape_id: shape.shape_id,
                route_ids,
                route_type: shape.route_type,
                color: shape.color.unwrap_or_else(|| "888888".to_string()),
                label: shape.route_label,
                geometry,
            });
        }

        info!(
            "Filtered to {} shapes, {} unique lines in bbox",
            gtfs_shapes.len(),
            lines.len()
        );

        Ok((lines, gtfs_shapes))
    }

    /// Load rail stops within the region bbox
    pub fn load_rail_stops(&self) -> Result<Vec<GtfsStop>> {
        let mut conn = self.pool.get()?;

        info!("Loading rail stops from database...");

        // Load stops that have rail route types
        let stop_results: Vec<Stop> = stops::table
            .filter(stops::allowed_spatial_query.eq(true))
            .filter(
                stops::location_type
                    .eq(0_i16)
                    .or(stops::location_type.eq(1_i16)),
            )
            .select(Stop::as_select())
            .load(&mut conn)?;

        info!("Loaded {} stops from database", stop_results.len());

        let rail_types: std::collections::HashSet<i16> =
            [0_i16, 1, 2, 5, 7, 12].iter().cloned().collect();

        let mut gtfs_stops = Vec::new();

        for stop in stop_results {
            // Filter to rail stops only
            let stop_route_types: Vec<i16> = stop
                .route_types
                .iter()
                .filter_map(|t| *t)
                .filter(|t| rail_types.contains(t))
                .collect();

            if stop_route_types.is_empty() {
                continue;
            }

            // Get position from point
            let position = match stop.point {
                Some(p) => (p.x, p.y),
                None => continue,
            };

            // Filter by bbox
            if position.0 < self.bbox.0
                || position.0 > self.bbox.2
                || position.1 < self.bbox.1
                || position.1 > self.bbox.3
            {
                continue;
            }

            gtfs_stops.push(GtfsStop {
                chateau: stop.chateau,
                stop_id: stop.gtfs_id,
                name: stop.name.or(stop.displayname),
                position,
                route_types: stop_route_types,
            });
        }

        info!("Filtered to {} rail stops in bbox", gtfs_stops.len());

        Ok(gtfs_stops)
    }
}

/// Convert RailMode to compatible GTFS route types
pub fn rail_mode_to_gtfs_types(mode: RailMode) -> Vec<i16> {
    mode.compatible_gtfs_types().to_vec()
}
